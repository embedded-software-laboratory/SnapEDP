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

#include <cassert>
#include <chrono>
#include <cstring>

using rtps::SnapEDPAgent;
using namespace rtps::snap_detail;


uint64_t SnapEDPAgent::getPeerViewHash(const GuidPrefix_t &prefix) {
  return m_remote.ownerHash(prefix);
}

void SnapEDPAgent::xorOwnEndpointHash(const Guid_t &endpointGuid) {
  m_ownEndpointHash ^= endpointHashAtom(m_part->m_guidPrefix, endpointGuid);
}

uint64_t SnapEDPAgent::getLocalViewHash() const {
  Lock lock{const_cast<SnapEDPAgent *>(this)->m_mutex};
  return m_ownEndpointHash.load(std::memory_order_relaxed) ^ m_remote.hash();
}
