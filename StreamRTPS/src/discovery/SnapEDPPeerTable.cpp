/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#include "rtps/discovery/SnapEDPPeerTable.h"
#include "rtps/utils/sysFunctions.h"

using rtps::SnapEDPPeerTable;


SnapEDPPeerTable::PeerStateEntry *SnapEDPPeerTable::observed(const GuidPrefix_t &prefix) {
  PeerStateEntry *e = m_peerStates.find(prefix);
  return (e != nullptr && e->observed) ? e : nullptr;
}

const SnapEDPPeerTable::PeerStateEntry *SnapEDPPeerTable::observed(const GuidPrefix_t &prefix) const {
  const PeerStateEntry *e = m_peerStates.find(prefix);
  return (e != nullptr && e->observed) ? e : nullptr;
}

void SnapEDPPeerTable::track(const GuidPrefix_t &prefix, SPDPDiscoverState state) {
  PeerStateEntry fresh;
  fresh.ack = getAck(prefix);
  fresh.observed = true;
  fresh.state = state;
  fresh.reconciliationIntervalMs = rtps::Config::SNAP_RECONCILE_BASE_MS.load(std::memory_order_relaxed);
  fresh.firstSeenMs = rtps::timeNowMs();
  m_peerStates.insertOrAssign(prefix, fresh);
}

bool SnapEDPPeerTable::observePeerState(const GuidPrefix_t &prefix,
                                       SPDPDiscoverState state) {
  if (PeerStateEntry *e = observed(prefix)) {
    if (e->state == state) {
      return false;
    }
    e->state = state;
    return true;
  }
  // new peer or table full, either way treat it as new
  track(prefix, state);
  return true;
}

// Reconciliation hysterisis to prevent constant spam
bool SnapEDPPeerTable::canReconcile(const GuidPrefix_t &prefix) const {
  const PeerStateEntry *e = observed(prefix);
  if (e == nullptr || e->reconcilicationInFlight) {
    return true;
  }
  
  // first fetch only, hold one grace window after first seeing the peer for its announcements
  if (e->lastResyncMs == 0 &&
      (rtps::timeNowMs() - e->firstSeenMs) < rtps::Config::SNAP_RECONCILE_GRACE_MS.load(std::memory_order_relaxed)) {
    return false;
  }
  
  // Has to have some debouncing otherwise will flood the network
  return (rtps::timeNowMs() - e->lastResyncMs) >= e->reconciliationIntervalMs;
}

bool SnapEDPPeerTable::beginReconciliation(const GuidPrefix_t &prefix) {
  
  PeerStateEntry *e = observed(prefix);
  
  if (e == nullptr) {
    // merge may have cleared the table 
    track(prefix, SPDPDiscoverState::UNCONFIGURED);
    e = observed(prefix);
    if (e == nullptr) {
      return false;
    }
  }
  if (e->reconcilicationInFlight || !canReconcile(prefix)) {
    return false;
  }

  // Update per peer state, so we do not decrease frequency
  e->lastResyncMs = rtps::timeNowMs();
  e->reconcilicationInFlight = true;
  const uint32_t cap = rtps::Config::SNAP_RECONCILE_CAP_MS.load(std::memory_order_relaxed);
  const uint32_t doubled = e->reconciliationIntervalMs * 2;
  e->reconciliationIntervalMs = (doubled < cap) ? doubled : cap;
  return true;
}

void SnapEDPPeerTable::endReconciliation(const GuidPrefix_t &prefix) {
  if (PeerStateEntry *e = observed(prefix)) {
    e->reconcilicationInFlight = false;
  }
}

void SnapEDPPeerTable::resetReconciliation(const GuidPrefix_t &prefix) {
  if (PeerStateEntry *e = observed(prefix)) {
    e->reconciliationIntervalMs = rtps::Config::SNAP_RECONCILE_BASE_MS.load(std::memory_order_relaxed);
    e->reconcilicationInFlight = false;
  }
}

void SnapEDPPeerTable::markAcked(const GuidPrefix_t &peer, AckState state) {
  if (peer == GUIDPREFIX_UNKNOWN || peer == m_self) {
    return;
  }
  if (PeerStateEntry *e = m_peerStates.find(peer)) {
    e->ack = state;
    return;
  }
  PeerStateEntry fresh;
  fresh.ack = state;
  m_peerStates.insertOrAssign(peer, fresh);
}

rtps::AckState SnapEDPPeerTable::getAck(const GuidPrefix_t &peer) const {
  if (const PeerStateEntry *e = m_peerStates.find(peer)) {
    return e->ack;
  }
  return AckState::Unknown;
}

void SnapEDPPeerTable::erase(const GuidPrefix_t &prefix) {
  m_peerStates.erase(prefix);
}

void SnapEDPPeerTable::clearStates() {
  for (auto &entry : m_peerStates) {
    entry.value.observed = false;
  }
}

void SnapEDPPeerTable::eraseAckedUnconfigured() {
  for (auto &entry : m_peerStates) {
    if (entry.value.ack == AckState::AckedUnconfigured) {
      entry.value.ack = AckState::Unknown;
    }
  }
}
