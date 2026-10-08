/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#include "SnapEDPAgentDetail.h"
#include "rtps/discovery/SnapEDPMessages.h"
#include "rtps/entities/Participant.h"
#include "rtps/entities/TransientWriter.h"
#include "rtps/entities/Reader.h"
#include "rtps/entities/Writer.h"
#include "rtps/messages/MessageTypes.h"
#include "rtps/utils/sysFunctions.h"
#include "ucdr/microcdr.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <random>

using rtps::SnapEDPAgent;
using namespace rtps::snap_detail;

std::vector<rtps::GuidPrefix_t> SnapEDPAgent::incompleteParticipants(){
  std::vector<GuidPrefix_t> mismatchedParticipants;
  for (const auto &part : m_part->getRemoteSnapViews()) {
    const GuidPrefix_t prefix = part.prefix;

    // configured same root peers gate completeness
    const bool sameRootConfigured = part.snapState == SPDPDiscoverState::CONFIGURED &&
                                    !(part.root == GUIDPREFIX_UNKNOWN) && part.root.id == m_root.get().id;
    
    if (!sameRootConfigured && !m_pendingResync.contains(prefix)) continue;

    if (part.endpointHash != getPeerViewHash(prefix)) {
      mismatchedParticipants.push_back(prefix);
    }
  }
  return mismatchedParticipants;
}

void SnapEDPAgent::selectNextResync() {
  if (m_exchange.active()) {
    return;
  }

  const auto mismatchedParticipants = incompleteParticipants();
  
  if (mismatchedParticipants.empty()) {
    // No mismtached/incomplete participants -> nothing to do!
    traceGate(0u, /*outcome complete*/ 0, GUIDPREFIX_UNKNOWN);
    m_snapSM->postEvent(SnapEDPEvent::Complete);
    return;
  }

  GuidPrefix_t mismatched;
  if (nextResyncTarget(mismatchedParticipants, true, mismatched)) {
    // Send a snapshot request and retrieve all the endpoints
    traceGate(static_cast<uint32_t>(mismatchedParticipants.size()), /*outcome dispatched*/ 1, mismatched);
    beginExchange(mismatched, true);
    return;
  }

  // mismatchedParticipants remain but none is dispatchable right now
  traceGate(static_cast<uint32_t>(mismatchedParticipants.size()), /*outcome exhausted*/ 2, GUIDPREFIX_UNKNOWN);
}

bool SnapEDPAgent::nextResyncTarget(const std::vector<GuidPrefix_t> &candidates, bool claim, GuidPrefix_t &target) {
  for (const auto &candidate : candidates) {

    // Should we resync with that participant?
    if (!claim) {
      if (!m_peers.canReconcile(candidate)) continue;
    } else {
      if (!m_peers.beginReconciliation(candidate)) continue;
    
      // Get the locator
      Locator dummy;
      if (!resolveSnapLocator(candidate, dummy)) {
        m_peers.endReconciliation(candidate);
        continue;
      }
    }

    target = candidate;
    return true;
  }
  return false;
}

void SnapEDPAgent::traceGate(uint32_t numMismatched, int outcome, const GuidPrefix_t &target) {
  const TracePrefix self(m_part->m_guidPrefix);
  const TracePrefix to(target);
  RTPS_TRACE_ALL_EVENT(gossip_gate_evaluated, self.hi, self.lo, numMismatched,
                       static_cast<uint32_t>(m_pendingResync.size()),
                       outcome, to.hi, to.lo);
}

bool SnapEDPAgent::selectJoinPartner(GuidPrefix_t &partner) {
  std::vector<GuidPrefix_t> viablePartners;
  GuidPrefix_t minRoot = GUIDPREFIX_UNKNOWN;

  auto rootKeyOf = [](const Participant::RemoteSnapView &part) {
    return (part.root == GUIDPREFIX_UNKNOWN) ? part.prefix : part.root;
  };

  // we have multiple policies: 0 random among lowest root, 1 always the root holder, 2 random among all configured (<- this is default)
  const uint32_t policy = Config::SNAP_SERVER_POLICY.load(std::memory_order_relaxed);

  // Operator for the set of viable Participants for Snapshot exchange
  auto collect = [&]() {
    viablePartners.clear();
    minRoot = GUIDPREFIX_UNKNOWN;
    for (const auto &part : m_part->getRemoteSnapViews()) {
      
      // check sanity
      if (part.snapState != SPDPDiscoverState::CONFIGURED || m_exchange.isSkipped(part.prefix)) {
        continue;
      }
      
      if (policy == 1) {
        if (!(m_root.get() == GUIDPREFIX_UNKNOWN) && part.prefix.id == m_root.get().id) {
          viablePartners.push_back(part.prefix);
        }
        continue;
      }

      if (policy == 2) {
        viablePartners.push_back(part.prefix);
        continue;
      }

      const GuidPrefix_t key = rootKeyOf(part);
      if (minRoot == GUIDPREFIX_UNKNOWN || key.id < minRoot.id) {
        minRoot = key;
        viablePartners.clear();
      }
      if (key.id == minRoot.id) {
        viablePartners.push_back(part.prefix);
      }

    }
  };

  collect();
  // If we have no one, we will even try the skip list
  if (viablePartners.empty() && m_exchange.hasSkips()) {
    m_exchange.clearSkips();
    collect();
  }

  if (viablePartners.empty()) {
    return false;
  }

  // sample nondeterministicaly among viable options (for load distribution otherwise, we just have one, depending on policy)
  std::vector<GuidPrefix_t> sampled = {};
  std::sample(viablePartners.begin(), viablePartners.end(), std::back_inserter(sampled), 1, partnerRng());
  partner = sampled[0];
  return true;
}

void SnapEDPAgent::beginExchange(const GuidPrefix_t &partner, bool isResync) {
  m_exchange.begin(partner);
  dispatchSnapshotRequestTo(partner, isResync);
}

void SnapEDPAgent::endExchange(ExchangeResult result) {
  if (result == ExchangeResult::Answered) {
    // ResponseReceived from a participant we requested, mark it acked and done
    m_peers.markAcked(m_exchange.partner(), AckState::AckedConfigured);
    m_exchange.clearSkips();
  }

  // a failed resync must release the in flight flag
  m_peers.endReconciliation(m_exchange.partner());
  m_exchange.clear();
}

bool SnapEDPAgent::retransmit(bool isResync) {
  // Retry request to same partner
  if (!m_exchange.active() || !m_exchange.nextRetry()) {
    return false;
  }

  SEDP_LOG("retryGossipRequest: retransmit %u", m_exchange.retries());
  dispatchSnapshotRequestTo(m_exchange.partner(), isResync);
  return true;
}

// Partner selection RNG
std::mt19937 &SnapEDPAgent::partnerRng() {
  if (!m_rngSeeded) {
    const uint64_t seed = Config::SNAP_RNG_SEED.load(std::memory_order_relaxed);
    if (seed != 0) {
      uint64_t h = seed;
      for (uint8_t b : m_part->m_guidPrefix.id) {
        h = (h ^ b) * 1099511628211ULL;
      }
      m_rng.seed(static_cast<std::mt19937::result_type>(h ^ (h >> 32)));
    } else {
      m_rng.seed(std::random_device{}());
    }
    m_rngSeeded = true;
  }
  return m_rng;
}


void SnapEDPAgent::deferRequester(const GuidPrefix_t &requester) {
  if (m_deferredRequesters.contains(requester)) {
    return;
  }
  // cap 0 disables deferral, drop and let the requester retry
  const uint32_t cap = Config::SNAP_DEFER_CAP.load(std::memory_order_relaxed);
  if (cap == 0 || m_deferredRequesters.size() >= cap) {
    return;
  }
  if (!m_deferredRequesters.add(requester)) {
    SEDP_LOG("deferRequester: list full");
    return;
  }
  const TracePrefix self(m_part->m_guidPrefix);
  const TracePrefix from(requester);
  RTPS_TRACE_ALL_EVENT(gossip_response_deferred, 0ull, self.hi, self.lo, from.hi, from.lo);
  SEDP_LOG("deferRequester: queued (%u entries)",
           (unsigned)m_deferredRequesters.size());
}

void SnapEDPAgent::retryOneDeferredResponse() {
  if (m_snapSM->state() != SnapEDPState::Discovered) {
    return;
  }

  // Find one target which has a valid locator, SPDP caught up wiht it
  for (const auto &req : m_deferredRequesters) {
    Locator dummy;
    if (!resolveSnapLocator(req, dummy)) {
      continue;
    }

    const GuidPrefix_t target = req;
    m_deferredRequesters.remove(target);

    const TracePrefix self(m_part->m_guidPrefix);
    const TracePrefix to(target);
    RTPS_TRACE_ALL_EVENT(gossip_response_retried, 0ull, self.hi, self.lo, to.hi, to.lo);
    SEDP_LOG("retryOneDeferredResponse: retrying");
    m_snapSM->postEvent(SnapEDPEvent::RequestReceived, target);
    return;
  }
}
