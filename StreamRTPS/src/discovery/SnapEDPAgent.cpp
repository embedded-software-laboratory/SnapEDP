/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#include "SnapEDPAgentDetail.h"
#include "rtps/discovery/SnapEDPMessages.h"
#include "rtps/entities/Participant.h"
#include "rtps/entities/Reader.h"
#include "rtps/entities/Writer.h"
#include "rtps/messages/MessageTypes.h"
#include "rtps/utils/sysFunctions.h"
#include "ucdr/microcdr.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>

using rtps::SnapEDPAgent;
using namespace rtps::snap_detail;

void SnapEDPAgent::init(Participant &part, const BuiltInEndpoints &endpoints) {

  // Init superclass 
  EDPAgentBase::init(part, endpoints);

  // Set starting value to self
  m_root.set(part.m_guidPrefix);
  m_peers.setSelf(part.m_guidPrefix);
  traceRoot();

  using S = SnapEDPState;
  using E = SnapEDPEvent;
  using A = SnapEDPAgent;

  // election backoff is guid ordered via nextJitterMs
  m_stateDefs = {{
    /* Initial    */ { Config::SNAP_TIMEOUT_INITIAL_MS.load(std::memory_order_relaxed) + nextJitterMs(), E::Timeout,  nullptr              },
    /* Election   */ { 0,                                                                                  E::None,     nullptr              },
    /* Gossip     */ { Config::SNAP_TIMEOUT_RETRANSMIT_MS.load(std::memory_order_relaxed),               E::Timeout,  nullptr              },
    /* Announce   */ { Config::SNAP_TIMEOUT_ANNOUNCING_MS.load(std::memory_order_relaxed),               E::Complete, nullptr              },
    /* Discovered */ { 0,                                                                                  E::None,     &A::enterDiscovered  },
    /* Reconcile  */ { Config::SNAP_TIMEOUT_RETRANSMIT_MS.load(std::memory_order_relaxed),               E::Timeout,  &A::enterReconcile   },
  }};
  const SnapEDPFSM::StateDef *states = m_stateDefs.data();
  const std::size_t numStates = m_stateDefs.size();

  // Create transition table instance
  static const SnapEDPFSM::Transition table[] = {
    // model fsm_initial, timeout with only unconfigured peers elects, a configured peer warm starts a join, TC1, TC2
    { S::Initial,    E::Timeout,                  S::Election,   &A::runElection },
    { S::Initial,    E::ConfiguredPeerDiscovered, S::Snapshot,     &A::startJoin },
    { S::Initial,    E::RequestReceived,          S::Initial,    &A::deferJoin, true },

    // model fsm_election, win claims root and routes through Announce, TC3, TC4, TC5
    { S::Election,   E::WonElection,              S::Announce,   &A::claimRoot },
    { S::Election,   E::LostElection,             S::Initial,    &A::backOffAfterElection },
    { S::Election,   E::NoNewCandidates,          S::Initial,    &A::backOffAfterElection },
    { S::Election,   E::ConfiguredPeerDiscovered, S::Snapshot,     &A::startJoin },
    { S::Election,   E::RequestReceived,          S::Election,   &A::deferJoin, true },

    // join path, response received then announce, timeout self loop retransmits, exhausted retries, TC6 or TC
    { S::Snapshot,     E::ResponseReceived,         S::Announce,   &A::finishJoin },
    { S::Snapshot,     E::Timeout,                  S::Snapshot,     &A::retryJoin },
    { S::Snapshot,     E::RequestsExhausted,        S::Initial,    &A::giveUpJoin },
    { S::Snapshot,   E::RequestReceived,          S::Snapshot,   &A::deferJoin, true },

    // announce always leads to the completeness gate and reconcile state, TC7
    { S::Announce,   E::Complete,                 S::Reconcile,  &A::announce },
    { S::Announce,   E::RequestReceived,          S::Announce,   &A::deferJoin, true },

    // steady state, serve joins in place, repoint on lower root, resync on hash mismatch, TC9
    { S::Discovered, E::RequestReceived,          S::Discovered, &A::serveJoin },
    { S::Discovered, E::Reconcile,                S::Announce,   &A::adoptNewRoot },
    { S::Discovered, E::HashMismatch,             S::Reconcile,  &A::queueResync },
    { S::Discovered, E::Sweep,                    S::Discovered, &A::periodicHashSweep, true },

    // the lc gate, can only pass if complete, TC8 to leave
    { S::Reconcile,  E::ResponseReceived,         S::Reconcile,  &A::finishResync },
    { S::Reconcile,  E::Complete,                 S::Discovered, &A::becomeDiscovered },
    { S::Reconcile,  E::Timeout,                  S::Reconcile,  &A::retryResync },
    { S::Reconcile,  E::RequestReceived,          S::Reconcile,  &A::deferJoin, true },
  };

  constexpr std::size_t numTransitions = sizeof(table) / sizeof(table[0]);
  m_snapSM = std::make_unique<SnapEDPFSM>(S::Initial, states, numStates,table, numTransitions, this);
  m_snapSM->setObserver(&A::onFSMTransition);
  if (m_endpoints.edpSnapReader != nullptr) {m_endpoints.edpSnapReader->registerCallback(receiveSnapshotCallback, this);}
  if (m_endpoints.announcementReader != nullptr) {m_endpoints.announcementReader->registerCallback(receiveAnnouncement, this);}
}

void SnapEDPAgent::start() {
  m_snapSM->start();

  // schedule/setup the periodic anti entropy sweep on the shared timed writer pool
  if (auto *tp = m_part->getThreadPool()) {
    tp->addTimedWorkload(this, rtps::timeNowMs());
  }
}

// the dispatch thread runs actions that publish messages, so it has to be gone before writers and readers are destroyed
void SnapEDPAgent::stop() {
  if (m_snapSM != nullptr) {
    m_snapSM->stop();
  }
}

namespace rtps {
namespace {

enum class ElectionOutcome { NoPeers, WarmStart, NoNewCandidates, Won, Lost };

struct ElectionResult {
  ElectionOutcome outcome = ElectionOutcome::NoPeers;
  GuidPrefix_t warmStartPeer{};
  uint32_t numUnconfigured = 0;
  uint32_t numNewUnconfigured = 0;
};

ElectionResult evaluateElection(const GuidPrefix_t &self, const std::vector<Participant::RemoteSnapView> &remotes, const SnapEDPPeerTable &peers) {
  ElectionResult result;

  // No need for election if we have no peers
  if (remotes.empty()) {
    return result;
  }

  // unacked configured peer so warm start
  for (const auto &part : remotes) {
    if (part.snapState == SPDPDiscoverState::CONFIGURED && peers.getAck(part.prefix) != AckState::AckedConfigured) {
      result.outcome = ElectionOutcome::WarmStart;
      result.warmStartPeer = part.prefix;
      return result;
    }
  }

  // amLowest over configured peers, compare against self
  GuidPrefix_t lowestUnconfigured = self;

  // Check all known remote participants, select the lowest as leader
  for (const auto &part : remotes) {
    
    if (part.snapState != SPDPDiscoverState::UNCONFIGURED) continue;
    ++result.numUnconfigured;
    
    if (peers.getAck(part.prefix) != AckState::AckedUnconfigured) {
      ++result.numNewUnconfigured;
    }

    // amLowest predicate
    if (part.prefix.id < lowestUnconfigured.id) {
      lowestUnconfigured = part.prefix;
    }
  }

  // Back to initial, there is no one
  if (result.numNewUnconfigured == 0) {
    result.outcome = ElectionOutcome::NoNewCandidates;
  } else {
    result.outcome = (self == lowestUnconfigured) ? ElectionOutcome::Won : ElectionOutcome::Lost;
  }
  return result;
}

}
}

// model fsm_initial dispatch
void SnapEDPAgent::runElection() {
  const TracePrefix own(m_part->m_guidPrefix);

  Lock lock{m_mutex};
  const auto remotes = m_part->getRemoteSnapViews();
  const ElectionResult result = evaluateElection(m_part->m_guidPrefix, remotes, m_peers);

  switch (result.outcome) {
  case ElectionOutcome::NoPeers:
    RTPS_TRACE_ALL_EVENT(gossip_election_evaluated, 0ull, own.hi, own.lo, /*won*/ 1, 0u, 0u);
    SEDP_LOG("checkNewLeader: no peers, declaring WonElection");
    m_snapSM->postEvent(SnapEDPEvent::WonElection);
    return;
  case ElectionOutcome::WarmStart:
    RTPS_TRACE_ALL_EVENT(gossip_election_evaluated, 0ull, own.hi, own.lo, /*won*/ 0, 0u, 1u);
    SEDP_LOG("checkNewLeader: unacked CONFIGURED peer found, warm-starting");
    m_snapSM->postEvent(SnapEDPEvent::ConfiguredPeerDiscovered, result.warmStartPeer);
    return;
  case ElectionOutcome::NoNewCandidates:
    RTPS_TRACE_ALL_EVENT(gossip_election_evaluated, 0ull, own.hi, own.lo, /*won*/ 0, result.numUnconfigured, 0u);
    SEDP_LOG("checkNewLeader: no new candidates (unconfigured=%u all acked)", result.numUnconfigured);
    m_snapSM->postEvent(SnapEDPEvent::NoNewCandidates);
    return;
  case ElectionOutcome::Won:
  case ElectionOutcome::Lost:
    break;
  }

  // ack all unconfigured remote participants
  for (const auto &part : remotes) {
    if (part.snapState == SPDPDiscoverState::UNCONFIGURED) {
      m_peers.markAcked(part.prefix, AckState::AckedUnconfigured);
    }
  }

  // Have we won? If yes -> self claim
  const bool won = result.outcome == ElectionOutcome::Won;
  RTPS_TRACE_ALL_EVENT(gossip_election_evaluated, 0ull, own.hi, own.lo, won ? 1 : 0,
                       result.numUnconfigured, 0u);
  SEDP_LOG("checkNewLeader: won=%d unconfigured=%u newUnconfigured=%u",
           won ? 1 : 0, result.numUnconfigured, result.numNewUnconfigured);

  // Post to FSM based on results
  if (won) {
    m_snapSM->postEvent(SnapEDPEvent::WonElection);
  } else {
    m_snapSM->postEvent(SnapEDPEvent::LostElection);
  }
}

void SnapEDPAgent::startJoin() {
  Lock lock{m_mutex};
  GuidPrefix_t partner;

  // Failure, no one available
  if (!selectJoinPartner(partner)) {
    SEDP_LOG("sendGossipRequest: no viable CONFIGURED peer available");
    return;
  }

  // send the request
  beginExchange(partner, false);
}

// model fsm_election win, sanctioned self claim
void SnapEDPAgent::claimRoot() {
  SEDP_LOG("newlyElected: claiming self as cluster root");
  Lock lock{m_mutex};
  m_root.set(m_part->m_guidPrefix);
  traceRoot();
}

void SnapEDPAgent::backOffAfterElection() {
  // backoff for next election to prevent election strom
  armInitialTimeout(3);
}

void SnapEDPAgent::finishJoin() {
  Lock lock{m_mutex};
  endExchange(ExchangeResult::Answered);
}

void SnapEDPAgent::retryJoin() {
  // Timeout snapshot request loop, resend to the same partner until the bound is reached
  Lock lock{m_mutex};
  if (!retransmit(false)) {
    m_snapSM->postEvent(SnapEDPEvent::RequestsExhausted);
  }
}

void SnapEDPAgent::giveUpJoin() {
  // Failure path for exhausted retries on both join and resync requests
  Lock lock{m_mutex};
  SEDP_LOG("abandonGossipRequest: partner left unacked");
  
  // skip this unresponsive participant 
  if (m_exchange.active()) {
    m_exchange.skip(m_exchange.partner());
  }

  endExchange(ExchangeResult::Abandoned);
  armInitialTimeout(1);
}

void SnapEDPAgent::announce() {
  // join or election path enters the gate right after the multicast announce
  Lock lock{m_mutex};
  announceMyEndpoints();
}

void SnapEDPAgent::deferJoin() {
  Lock lock{m_mutex};

  // defer request, we are not discovered atm
  SEDP_LOG("receiveGossipCallback: JOIN REQUEST while not Discovered, deferring");
  deferRequester(m_snapSM->currentPayload().peer);
}

void SnapEDPAgent::serveJoin() {
  assert(m_part->getSPDPAgent().getLocalSnapState() == SPDPDiscoverState::CONFIGURED && "join request served while unconfigured");
  Lock lock{m_mutex};
  const GuidPrefix_t target = m_snapSM->currentPayload().peer;
  sendSnapshotResponseFor(target, /*ownEndpointsOnly=*/false);
}

// merge and adopt new root, follows TC9 
void SnapEDPAgent::adoptNewRoot() {
  {
    Lock lock{m_mutex};
    SEDP_LOG("adoptNewRoot");
    const GuidPrefix_t trigger = m_snapSM->currentPayload().peer;
    Participant::RemoteSnapView peer;
    
    if (m_part->getRemoteSnapView(trigger, peer)) {
      adoptRootIfLower(peer.root);
    }
    
    m_peers.clearStates();
    endExchange(ExchangeResult::Abandoned);
  }
  
  // Reset to unconfigured, FSM state is reset to reconcile to trigger lc gate again
  m_part->getSPDPAgent().setState(SPDPDiscoverState::UNCONFIGURED);
  m_part->getSPDPAgent().requestImmediateResend();
}

// resync endpoints on hash mismatch
void SnapEDPAgent::queueResync() {
  Lock lock{m_mutex};
  const GuidPrefix_t target = m_snapSM->currentPayload().peer;
  
  // Queue a resync
  if (!(target == GUIDPREFIX_UNKNOWN) && m_part->hasRemoteParticipant(target)) {
    m_pendingResync.add(target);
  }
}

void SnapEDPAgent::finishResync() {
  Lock lock{m_mutex};
  const SnapEDPEventData response = m_snapSM->currentPayload();
  if (!(response.peer == m_exchange.partner())) {
    return;
  }
  endExchange(ExchangeResult::Answered);

  // Apply resync policy, might be a good idea to announce in some cases
  const uint32_t reannounce = Config::SNAP_REANNOUNCE_AFTER_RESYNC.load(std::memory_order_relaxed);
  const bool doAnnounce = (reannounce == 1) || (reannounce != 2 && response.rootLowered);
  if (doAnnounce) {
    // announce again so the domain learns our endpoints
    announceMyEndpoints();
  }
}

void SnapEDPAgent::retryResync() {
  Lock lock{m_mutex};

  // Either done or requests exhausted
  if (!retransmit(true)) {
    endExchange(ExchangeResult::Abandoned);
  }
}

// Passed the reconcile state, now become fully discovered
void SnapEDPAgent::becomeDiscovered() {
  
  {
    Lock lock{m_mutex};
    SEDP_LOG("becomeDiscovered: reconcile gate passed");
    m_pendingResync.clear();
    endExchange(ExchangeResult::Abandoned);
  }

  // Only advertise new SPDP state if necessary
  if (m_part->getSPDPAgent().getLocalSnapState() != SPDPDiscoverState::CONFIGURED) {
    m_part->getSPDPAgent().setState(SPDPDiscoverState::CONFIGURED);
    m_part->getSPDPAgent().requestImmediateResend();
  }
}

void SnapEDPAgent::enterReconcile() {
  Lock lock{m_mutex};

  // Check if we got everything or more are missing endpoints
  selectNextResync();
}

void SnapEDPAgent::enterDiscovered() {
  Lock lock{m_mutex};
  sendUnannounced();
  retryOneDeferredResponse();
}


// Log and trace FSM transitions, required for debugging and plots
void SnapEDPAgent::onFSMTransition(SnapEDPState from,
                                      SnapEDPEvent event,
                                      SnapEDPState to) {
  const TracePrefix self(m_part->m_guidPrefix);
  RTPS_TRACE_ALL_EVENT(gossip_state_transition, 0u, self.hi, self.lo, static_cast<int>(from), static_cast<int>(event), static_cast<int>(to));
  SEDP_LOG("FSM %s --(%s)--> %s", stateName(from), eventName(event), stateName(to));
}

void SnapEDPAgent::armInitialTimeout(uint32_t factor) {
  const auto baseMs = Config::SNAP_TIMEOUT_INITIAL_MS.load(std::memory_order_relaxed);
  m_stateDefs[static_cast<size_t>(SnapEDPState::Initial)].timeout_ms = factor * baseMs + nextJitterMs();
}

// Delay/Wait based on participant GUID
uint32_t SnapEDPAgent::nextJitterMs() {
  const uint32_t window = Config::SNAP_JITTER_MAX_MS.load(std::memory_order_relaxed);
  
  if (window == 0) {
    return 0;
  }

  const auto &id = m_part->m_guidPrefix.id;
  const uint32_t hi = (static_cast<uint32_t>(id[0]) << 24) |
                      (static_cast<uint32_t>(id[1]) << 16) |
                      (static_cast<uint32_t>(id[2]) << 8) |
                      static_cast<uint32_t>(id[3]);

  return static_cast<uint32_t>((static_cast<uint64_t>(hi) * window) >> 32);
}

rtps::SnapEDPState SnapEDPAgent::getCurrentState() {
  return m_snapSM->state();
}

uint32_t SnapEDPAgent::getJoinedPeerCounts() {
  if (m_part == nullptr) {
    return 0;
  }
  uint32_t count = 0;
  for (const auto &peer : m_part->getRemoteSnapViews()) {
    if (peer.snapState == SPDPDiscoverState::CONFIGURED) {
      ++count;
    }
  }
  return count;
}

// root can only increase in special case, check those here
bool SnapEDPAgent::adoptRootIfLower(const GuidPrefix_t &candidate) {
  
  if (!isKnownParticipant(candidate)) {
    return false;
  }

  // Is lower or is us
  const bool selfRootUnknown = m_root.get() == GUIDPREFIX_UNKNOWN;
  if (!selfRootUnknown && !(candidate.id < m_root.get().id)) {
    return false;
  }
  
  m_root.set(candidate);
  traceRoot();
  return true;
}

// Check that we know that participant
bool SnapEDPAgent::isKnownParticipant(const GuidPrefix_t &candidate) const {
  return candidate == m_part->m_guidPrefix || m_part->hasRemoteParticipant(candidate);
}

void SnapEDPAgent::traceRoot() {
  const TracePrefix self(m_part->m_guidPrefix);
  const TracePrefix root(m_root.get());
  RTPS_TRACE_ALL_EVENT(gossip_root_changed, self.hi, self.lo, root.hi, root.lo);
}

// SPDP Callback on new info
void SnapEDPAgent::onRemoteParticipantDiscovered( const GuidPrefix_t &prefix, const SPDPDiscoverState &state) {

  SEDP_LOG("onRemoteParticipantDiscovered state=%d", static_cast<int>(state));

  // Wait an SPDP round, give the others some time so we dont start this dance with entirely outdated infos
  const uint16_t round = m_part->getSPDPAgent().getBroadcastRound();
  const uint32_t quiescenceRounds = Config::SNAP_QUIESCENCE_ROUNDS.load(std::memory_order_relaxed);
  if (round < quiescenceRounds) {
    return;
  }

  // Serialise with the gossip reader callback to prevent races between them
  Lock lock{m_mutex};

  // backoff expired, now start the election
  const uint32_t maxRounds = Config::SNAP_QUIESCENCE_MAX_ROUNDS.load(std::memory_order_relaxed);
  if (maxRounds > 0 && round >= maxRounds) {
    m_snapSM->tryStep(SnapEDPState::Initial, SnapEDPEvent::Timeout, m_part->m_guidPrefix);
  }

  const bool changed = m_peers.observePeerState(prefix, state);

  // Dispatch these events to the FSM
  if (changed && state == SPDPDiscoverState::UNCONFIGURED) {
    m_snapSM->step(SnapEDPEvent::UnconfiguredPeerDiscovered, prefix);
  }
  if (changed && state == SPDPDiscoverState::CONFIGURED) {
    m_snapSM->step(SnapEDPEvent::ConfiguredPeerDiscovered, prefix);
  }

  // Are we in discovered, then process the newConf mark on SPDP update
  if (state == SPDPDiscoverState::CONFIGURED && m_snapSM->state() == SnapEDPState::Discovered) {
    checkConfiguredPeer(prefix, changed);
  }

  retryOneDeferredResponse();
}

void SnapEDPAgent::checkConfiguredPeer(const GuidPrefix_t &prefix, bool changed) {
  GuidPrefix_t peerRoot = GUIDPREFIX_UNKNOWN;
  uint64_t peerHash = 0;

  Participant::RemoteSnapView peerData;
  if (m_part->getRemoteSnapView(prefix, peerData)) {
    peerRoot = peerData.root;
    peerHash = peerData.endpointHash;
  }
  const bool peerRootKnown = !(peerRoot == GUIDPREFIX_UNKNOWN);
  const bool selfRootKnown = !(m_root.get() == GUIDPREFIX_UNKNOWN);

  // stale messages must not trigger a merge back onto it
  const bool peerRootAdoptable = peerRootKnown && isKnownParticipant(peerRoot);
  const bool peerRootLower = peerRootAdoptable && selfRootKnown && peerRoot.id < m_root.get().id;
  // the liveness gate can leave a joiner rootless when the claimed root had no proxy yet
  const bool rootlessAdopt = peerRootAdoptable && !selfRootKnown;

  // Check if we have the right view or endpoint of this participant
  const uint64_t peerViewHash = getPeerViewHash(prefix);
  const bool sameClusterHashMismatch = peerRootKnown && selfRootKnown && peerRoot.id == m_root.get().id && peerHash != peerViewHash;
  bool triggeredResync = false;

  if (peerRootLower || rootlessAdopt) {
    // Lower root so repoint and merge, keep O
    m_snapSM->step(SnapEDPEvent::Reconcile, prefix);

  } else if (sameClusterHashMismatch) {
    // Hash mismatch so resync via the gate, debounced per peer
    if (m_peers.canReconcile(prefix)) {
      triggeredResync = true;
      m_snapSM->step(SnapEDPEvent::HashMismatch, prefix);
    }
  } else {
    m_peers.resetReconciliation(prefix);
    if (changed) {
      m_peers.markAcked(prefix, AckState::AckedConfigured);
    }
  }

  // Tracing stuff
  if (peerRootKnown && selfRootKnown && peerRoot.id == m_root.get().id) {
    // per SPDP receive
    traceHashCheck(prefix, peerHash, peerViewHash, sameClusterHashMismatch, triggeredResync);
  }
}

void SnapEDPAgent::traceHashCheck(const GuidPrefix_t &prefix, uint64_t peerHash, uint64_t peerViewHash, bool mismatch, bool triggeredResync) {
  const TracePrefix self(m_part->m_guidPrefix);
  const TracePrefix peer(prefix);
  RTPS_TRACE_PERF_EVENT(gossip_hash_check, self.hi, self.lo, peer.hi, peer.lo,
                       m_ownEndpointHash.load(), peerHash, peerViewHash,
                       mismatch ? 1 : 0,
                       triggeredResync ? 1 : 0);
}

void SnapEDPAgent::forgetPeer(const GuidPrefix_t &prefix) {
  // Clear readers and writers
  m_remote.eraseOwner(prefix);

  // Remove if we store outdated stuff about the removed participant
  m_peers.erase(prefix);
  m_pendingResync.remove(prefix);
  m_exchange.forget(prefix);
  m_unseenEndpointOwners.erase(prefix);
  m_resync.drop(prefix);
  m_deferredRequesters.remove(prefix);
}

void SnapEDPAgent::onRemoteParticipantRemoved(const GuidPrefix_t &prefix) {
  Lock lock{m_mutex};
  forgetPeer(prefix);

  m_peers.eraseAckedUnconfigured();
  m_part->clearRemoteRootsNaming(prefix);

  // Case where deleted participant is root is critical
  if (prefix == m_root.get()) {
    GuidPrefix_t newRoot = m_part->m_guidPrefix;
    for (const auto &peer : m_part->getRemoteSnapViews()) {
      if (peer.prefix == prefix) continue;
      if (peer.snapState != SPDPDiscoverState::CONFIGURED) continue;
      if (peer.prefix.id < newRoot.id) {
        newRoot = peer.prefix;
      }
    }
    SEDP_LOG("onRemoteParticipantRemoved: root died, repointing");
    m_root.set(newRoot);
    traceRoot();
  }
}

void SnapEDPAgent::onAddProxiesForRemoteParticipant( const ParticipantProxyData &proxyData, const Locator &locator) {}

// anti entropy sweep, reenters the gate from Discovered when a peer view drifted and the SPDP driven trigger was missed
void SnapEDPAgent::periodicHashSweep() {
  Lock lock{m_mutex};
  if (m_root.get() == GUIDPREFIX_UNKNOWN) return;

  const auto stale = incompleteParticipants();
  GuidPrefix_t steppedPrefix = GUIDPREFIX_UNKNOWN;
  if (nextResyncTarget(stale, false, steppedPrefix)) {
    // one resync in flight at a time, the gate picks up the rest
    m_snapSM->step(SnapEDPEvent::HashMismatch, steppedPrefix);
  }
#if !defined(EMBRTPS_DISABLE_TRACING) && TRACE_PERF
  for (const auto &prefix : stale) {
    // sweep path counterpart of the SPDP receive hash_check so the hole curve also sees drift the sweep found, mismatch is always 1 here
    uint64_t peerHash = 0;
    Participant::RemoteSnapView peerData;
    if (m_part->getRemoteSnapView(prefix, peerData)) {
      peerHash = peerData.endpointHash;
    }
    traceHashCheck(prefix, peerHash, getPeerViewHash(prefix), true, prefix == steppedPrefix);
  }
#endif
  if (stale.empty()) return;
  if (auto *tp = m_part->getThreadPool()) {
    tp->addTimedWorkload(this, rtps::timeNowMs() + MAINTENANCE_ACTIVE_MS);
  }
}

void SnapEDPAgent::onTimer() {
  removeOwnerlessEndpoints();
  m_snapSM->postEvent(SnapEDPEvent::Sweep);
  if (auto *tp = m_part->getThreadPool()) {
    tp->addTimedWorkload(this, rtps::timeNowMs() + MAINTENANCE_IDLE_MS);
  }
}
