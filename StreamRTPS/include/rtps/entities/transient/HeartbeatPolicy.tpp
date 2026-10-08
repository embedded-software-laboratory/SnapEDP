/*
This file is part of streamRTPS.
Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_HEARTBEAT_POLICY_TPP
#define RTPS_HEARTBEAT_POLICY_TPP

#include "rtps/entities/transient/HeartbeatPolicy.h"
#include "rtps/config/FeatureQOS.h"

#include <memory>

namespace rtps {

template <class NetworkDriver>
std::unique_ptr<HeartbeatPolicy> makeHeartbeatPolicy(
    HeartbeatPolicyMode, StatefulWriterT<NetworkDriver>*) {
  return std::make_unique<HeartbeatPolicy>();
}

} // namespace rtps

#endif // RTPS_HEARTBEAT_POLICY_TPP
