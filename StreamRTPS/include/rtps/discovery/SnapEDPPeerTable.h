/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_SNAPEDPPEERTABLE_H
#define RTPS_SNAPEDPPEERTABLE_H

#include "rtps/common/types.h"
#include "rtps/config.h"
#include "rtps/discovery/SnapEDPTypes.h"
#include "rtps/utils/FixedMap.h"

#include <cstdint>

namespace rtps {

class SnapEDPPeerTable {

  // Entry holding remote participant timing and state information
  struct PeerStateEntry {
    // Per peer ack state
    AckState ack = AckState::Unknown;
    bool observed = false;
    SPDPDiscoverState state = SPDPDiscoverState::UNCONFIGURED;
    uint32_t reconciliationIntervalMs = 0;
    uint32_t lastResyncMs = 0;
    bool reconcilicationInFlight = false;
    uint32_t firstSeenMs = 0;
  };

public:
  void setSelf(const GuidPrefix_t &self) { m_self = self; }

  // Configuration states and ack/unacked, hatted view values in $w_{p_{i}}^{{p_{j}}}$ in the paper
  void markAcked(const GuidPrefix_t &peer, AckState state);
  AckState getAck(const GuidPrefix_t &peer) const;
  bool observePeerState(const GuidPrefix_t &prefix, SPDPDiscoverState state);
  
  // Reconcilation tracking
  bool canReconcile(const GuidPrefix_t &prefix) const;
  bool beginReconciliation(const GuidPrefix_t &prefix);
  void endReconciliation(const GuidPrefix_t &prefix);
  void resetReconciliation(const GuidPrefix_t &prefix);

  // Reset functions
  void erase(const GuidPrefix_t &prefix);
  void clearStates();
  void eraseAckedUnconfigured();

private:
  PeerStateEntry *observed(const GuidPrefix_t &prefix);
  const PeerStateEntry *observed(const GuidPrefix_t &prefix) const;
  void track(const GuidPrefix_t &prefix, SPDPDiscoverState state);

  GuidPrefix_t m_self{};

  // Per peer SPDP state and resync infos
  FixedMap<GuidPrefix_t, PeerStateEntry, Config::SPDP_MAX_NUMBER_FOUND_PARTICIPANTS> m_peerStates;
};

} // namespace rtps

#endif // RTPS_SEDPGOSSIPPEERTABLE_H
