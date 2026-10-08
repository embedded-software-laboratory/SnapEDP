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


void SnapEDPAgent::dumpDiscoveryState(uint32_t snapshot_id, bool is_final) {
  if (m_part == nullptr) {
    return;
  }
  // Snapshot the participant discovery view for debugging purposes :D
  Lock lock{m_mutex};

  std::vector<Writer *> localWriters;
  std::vector<Reader *> localReaders;
  {
    Lock localLock{m_localMutex};
    localWriters = m_localWriters;
    localReaders = m_localReaders;
  }

  uint64_t lh = 0, ll = 0;
  splitPrefixForTrace(m_part->m_guidPrefix, lh, ll);

  auto remotes = m_part->getRemoteSnapViews();
  const auto fsm_state = m_snapSM->state();
  const auto spdp_state = m_part->getSPDPAgent().getLocalSnapState();

  const int state_packed = (static_cast<int>(fsm_state) << 16) |
                           (static_cast<int>(spdp_state) & 0xFFFF);
  RTPS_TRACE_ALL_EVENT(
      discovery_snapshot_begin, lh, ll, snapshot_id, is_final ? 1 : 0,
      state_packed,
      static_cast<uint32_t>(remotes.size()),
      static_cast<uint32_t>(localWriters.size()),
      static_cast<uint32_t>(localReaders.size()),
      static_cast<uint32_t>(m_remote.writers().size()),
      static_cast<uint32_t>(m_remote.readers().size()));

  // remote participants and their spdp state 
  for (const auto &peer : remotes) {
    uint64_t ph = 0, pl = 0;
    splitPrefixForTrace(peer.prefix, ph, pl);
    RTPS_TRACE_ALL_EVENT(discovery_snapshot_peer, lh, ll, snapshot_id,
                         ph, pl, static_cast<int>(peer.snapState));
  }

  // our loocal endpoints 
  for (auto *writer : localWriters) {
    if (writer == nullptr) {
      continue;
    }
    const auto &attrs = writer->m_attributes;
    uint32_t entity_id = 0;
    std::memcpy(&entity_id, &attrs.endpointGuid.entityId, sizeof(entity_id));
    RTPS_TRACE_ALL_EVENT(discovery_snapshot_local_endpoint, lh, ll, snapshot_id,
                         /*kind=writer*/ 0, entity_id, attrs.topicName);
  }
  for (auto *reader : localReaders) {
    if (reader == nullptr) {
      continue;
    }
    const auto &attrs = reader->m_attributes;
    uint32_t entity_id = 0;
    std::memcpy(&entity_id, &attrs.endpointGuid.entityId, sizeof(entity_id));
    RTPS_TRACE_ALL_EVENT(discovery_snapshot_local_endpoint, lh, ll, snapshot_id,
                         /*kind=reader*/ 1, entity_id, attrs.topicName);
  }

  // known remote endpoints
  for (const auto &data : m_remote.writers()) {
    uint64_t oh = 0, ol = 0;
    splitPrefixForTrace(data.endpointGuid.prefix, oh, ol);
    uint32_t entity_id = 0;
    std::memcpy(&entity_id, &data.endpointGuid.entityId, sizeof(entity_id));
    const int matched =
        (m_part->getMatchingReader(data) != nullptr) ? 1 : 0;
    RTPS_TRACE_ALL_EVENT(discovery_snapshot_remote_endpoint, lh, ll,
                         snapshot_id, oh, ol, /*kind=writer*/ 0, entity_id,
                         data.topicName, matched);
  }
  for (const auto &data : m_remote.readers()) {
    uint64_t oh = 0, ol = 0;
    splitPrefixForTrace(data.endpointGuid.prefix, oh, ol);
    uint32_t entity_id = 0;
    std::memcpy(&entity_id, &data.endpointGuid.entityId, sizeof(entity_id));
    const int matched =
        (m_part->getMatchingWriter(data) != nullptr) ? 1 : 0;
    RTPS_TRACE_ALL_EVENT(discovery_snapshot_remote_endpoint, lh, ll,
                         snapshot_id, oh, ol, /*kind=reader*/ 1, entity_id,
                         data.topicName, matched);
  }

  RTPS_TRACE_ALL_EVENT(discovery_snapshot_end, lh, ll, snapshot_id);
  SEDP_LOG("dumpDiscoveryState id=%u is_final=%d peers=%u local=%u/%u remote=%u/%u",
           snapshot_id, is_final ? 1 : 0,
           (unsigned)remotes.size(),
           (unsigned)localWriters.size(), (unsigned)localReaders.size(),
           (unsigned)m_remote.writers().size(),
           (unsigned)m_remote.readers().size());
}
