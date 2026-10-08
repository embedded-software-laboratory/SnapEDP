/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/


#ifndef RTPS_SNAPEDPAGENT_DETAIL_H
#define RTPS_SNAPEDPAGENT_DETAIL_H

#include "rtps/discovery/SnapEDPAgent.h"
#include "rtps/utils/Log.h"
#include "trace_control.h"

#include <cstdint>
#include <cstring>

#if SNAP_VERBOSE && RTPS_GLOBAL_VERBOSE
#define SEDP_LOG(...) RTPS_LOG("SEDPGossip", SNAP_VERBOSE, __VA_ARGS__)
#else
#define SEDP_LOG(...) RTPS_TRACE_LOG_EMIT("SEDPGossip", __VA_ARGS__)
#endif

namespace rtps {
namespace snap_detail {

enum SnapDropReason : int {
  DROP_COPY_FAILED = 1,
  DROP_PEEK_FAILED = 2,
  DROP_PARSE_FAILED = 3,
};

inline uint64_t fnv1a64(const uint8_t *data, size_t len) {
  uint64_t h = 14695981039346656037ULL;
  for (size_t i = 0; i < len; ++i) {
    h ^= static_cast<uint64_t>(data[i]);
    h *= 1099511628211ULL;
  }
  return h;
}

inline uint64_t endpointHashAtom(const GuidPrefix_t &prefix,
                                 const Guid_t &endpointGuid) {
  uint8_t buf[sizeof(GuidPrefix_t::id) + sizeof(Guid_t)];
  std::memcpy(buf, prefix.id.data(), sizeof(GuidPrefix_t::id));
  std::memcpy(buf + sizeof(GuidPrefix_t::id), &endpointGuid, sizeof(Guid_t));
  return fnv1a64(buf, sizeof(buf));
}

inline void splitPrefixForTrace(const GuidPrefix_t &prefix, uint64_t &high,
                                uint64_t &low) {
  uint8_t buf[16] = {};
  std::memcpy(buf, prefix.id.data(), prefix.id.size());
  std::memcpy(&high, buf, sizeof(high));
  std::memcpy(&low, buf + sizeof(high), sizeof(low));
}

inline bool isBuiltinEndpoint(const Guid_t &guid) {
  const EntityKind_t kind = guid.entityId.entityKind;
  return kind == EntityKind_t::BUILD_IN_WRITER_WITH_KEY ||
         kind == EntityKind_t::BUILD_IN_WRITER_WITHOUT_KEY ||
         kind == EntityKind_t::BUILD_IN_READER_WITH_KEY ||
         kind == EntityKind_t::BUILD_IN_READER_WITHOUT_KEY;
}

struct TracePrefix {
  uint64_t hi = 0;
  uint64_t lo = 0;
  explicit TracePrefix(const GuidPrefix_t &prefix) { splitPrefixForTrace(prefix, hi, lo); }
};

inline const char *stateName(SnapEDPState s) {
  switch (s) {
  case SnapEDPState::Initial:    return "Initial";
  case SnapEDPState::Election:   return "Election";
  case SnapEDPState::Snapshot:     return "Gossip";
  case SnapEDPState::Announce:   return "Announce";
  case SnapEDPState::Discovered: return "Discovered";
  case SnapEDPState::Reconcile:  return "Reconcile";
  }
  return "?";
}

inline const char *eventName(SnapEDPEvent e) {
  switch (e) {
  case SnapEDPEvent::None:                       return "None";
  case SnapEDPEvent::ConfiguredPeerDiscovered:   return "ConfiguredPeerDiscovered";
  case SnapEDPEvent::UnconfiguredPeerDiscovered: return "UnconfiguredPeerDiscovered";
  case SnapEDPEvent::WonElection:                return "WonElection";
  case SnapEDPEvent::LostElection:               return "LostElection";
  case SnapEDPEvent::NoNewCandidates:            return "NoNewCandidates";
  case SnapEDPEvent::Reconcile:                  return "Reconcile";
  case SnapEDPEvent::HashMismatch:               return "HashMismatch";
  case SnapEDPEvent::RequestReceived:            return "RequestReceived";
  case SnapEDPEvent::ResponseReceived:           return "ResponseReceived";
  case SnapEDPEvent::Complete:                   return "Complete";
  case SnapEDPEvent::Timeout:                    return "Timeout";
  case SnapEDPEvent::RequestsExhausted:          return "RequestsExhausted";
  case SnapEDPEvent::Sweep:                      return "Sweep";
  }
  return "?";
}

} // namespace gossip_detail
} // namespace rtps

#endif // RTPS_SEDPGOSSIPAGENT_DETAIL_H
