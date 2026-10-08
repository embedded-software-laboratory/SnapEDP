/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_SNAPEDPTYPES_H
#define RTPS_SNAPEDPTYPES_H

#include "rtps/common/types.h"

#include <cstdint>

namespace rtps {

// FSM States, follow paper section 4.1
enum class SnapEDPState : std::uint8_t {
  Initial,             // Startup, waiting for SPDP to deliver a peer
  Election,            // Deciding winner via lowest GUID among unconfigd peers
  Snapshot,              // Join request sent, awaiting response
  Announce,            // Broadcasting local endpoints on join or root change
  Discovered,          // Fully joined the net, steady state
  Reconcile            // Completeness gate, pulls holes via targeted resync
};

// SPDP Ack States, paper uses $w_{p_{i}}^{{p_{j}}}$
enum class AckState : std::uint8_t {
  Unknown,
  AckedUnconfigured,
  AckedConfigured,
};

// FSM Events, these do not follow the paper directly
enum class SnapEDPEvent : std::uint8_t {
  None,                       // not used
  ConfiguredPeerDiscovered,   // SPDP reported a configd remote peer
  UnconfiguredPeerDiscovered, // SPDP reported an new unconfigd remote peer
  WonElection,                // Won an election
  LostElection,               // Lost an election
  NoNewCandidates,            // Election ran with nothing new to decide, got to go back to Initial
  Reconcile,                  // A lower root configd peer appeared, repoint and merge
  HashMismatch,               // Peer hash disagrees with our view of it, resync
  RequestReceived,            // Got a join gossip request
  ResponseReceived,           // Got a gossip response
  Complete,                   // Announce done or reconcile gate passed
  Timeout,                    // FSM inject when the state timer elapses
  RequestsExhausted,          // Bounded gossip request retries used up, give up
  Sweep,
};

struct SnapEDPEventData {
  GuidPrefix_t peer{};
  bool rootLowered = false;

  SnapEDPEventData() = default;
  SnapEDPEventData(const GuidPrefix_t &prefix) : peer(prefix) {}
};

} // namespace rtps

#endif // RTPS_SEDPGOSSIPTYPES_H
