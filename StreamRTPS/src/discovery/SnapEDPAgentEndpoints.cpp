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

void SnapEDPAgent::addWriter(Writer &writer) {
  if (isBuiltinEndpoint(writer.m_attributes.endpointGuid)) {
    return;
  }

  Lock lock{m_mutex};
  {
    Lock localLock{m_localMutex};
    m_localWriters.push_back(&writer);
  }
  // Update Hash values
  SEDP_LOG("Stored local writer %s/%s\n", writer.m_attributes.topicName, writer.m_attributes.typeName);
  xorOwnEndpointHash(writer.m_attributes.endpointGuid);

  // Match against already known remote readers
  for (const auto &remote : m_remote.readers()) {
    TopicData remoteCopy(remote);
    if (remoteCopy.matchesTopicOf(writer.m_attributes)) {
        writer.addNewMatchedReader(ReaderProxy{remoteCopy.endpointGuid, remoteCopy.unicastLocator});
    }
  }

  // outside Discovered, m_localWriters stores the info and transmits when required
  queueAnnouncement(writer.m_attributes);
}

void SnapEDPAgent::addReader(Reader &reader) {
  if (isBuiltinEndpoint(reader.m_attributes.endpointGuid)) {
    return;
  }
  Lock lock{m_mutex};

  {
    Lock localLock{m_localMutex};
    m_localReaders.push_back(&reader);
  }
  SEDP_LOG("Stored local reader %s/%s\n", reader.m_attributes.topicName, reader.m_attributes.typeName);
  xorOwnEndpointHash(reader.m_attributes.endpointGuid);

  // Match against known remote writers
  for (const auto &remote : m_remote.writers()) {
    TopicData remoteCopy(remote);
    if (remoteCopy.matchesTopicOf(reader.m_attributes)) {
      reader.addNewMatchedWriter(WriterProxy{remoteCopy.endpointGuid, remoteCopy.unicastLocator});
    }
  }

  // outside Discovered, m_localReaders is used to announce, when applicable
  queueAnnouncement(reader.m_attributes);
}

void SnapEDPAgent::removeWriter(Writer &writer) {
  if (isBuiltinEndpoint(writer.m_attributes.endpointGuid)) {
    return;
  }
  Lock lock{m_mutex};
  xorOwnEndpointHash(writer.m_attributes.endpointGuid);
  {
    Lock localLock{m_localMutex};
    m_localWriters.erase(std::remove(m_localWriters.begin(), m_localWriters.end(), &writer), m_localWriters.end());
  }
  dropUnannounced(writer.m_attributes.endpointGuid);
  sendEndpointDispose(writer.m_attributes);
}

void SnapEDPAgent::removeReader(Reader &reader) {
  if (isBuiltinEndpoint(reader.m_attributes.endpointGuid)) {
    return;
  }
  Lock lock{m_mutex};
  xorOwnEndpointHash(reader.m_attributes.endpointGuid);
  {
    Lock localLock{m_localMutex};
    m_localReaders.erase(std::remove(m_localReaders.begin(), m_localReaders.end(), &reader),m_localReaders.end());
  }
  dropUnannounced(reader.m_attributes.endpointGuid);
  sendEndpointDispose(reader.m_attributes);
}

void SnapEDPAgent::queueAnnouncement(const TopicData &endpoint) {
  m_unannounced.push_back(endpoint);
  if (m_snapSM->state() == SnapEDPState::Discovered) {
    sendUnannounced();
  }
}

void SnapEDPAgent::dropUnannounced(const Guid_t &endpointGuid) {
  m_unannounced.erase(std::remove_if(m_unannounced.begin(), m_unannounced.end(), [&](const TopicData &d) { return d.endpointGuid == endpointGuid; }), m_unannounced.end());
}

void SnapEDPAgent::sendUnannounced() {
  sendEndpointBatch(m_unannounced, /*disposed=*/false);
  m_unannounced.clear();
}

std::vector<rtps::TopicData> SnapEDPAgent::collectLocalEndpoints() {
  std::vector<TopicData> endpoints;
  {
    Lock localLock{m_localMutex};
    for (auto *writer : m_localWriters) {
      endpoints.push_back(writer->m_attributes);
    }
    for (auto *reader : m_localReaders) {
      endpoints.push_back(reader->m_attributes);
    }
  }
  return endpoints;
}

void SnapEDPAgent::dropOwnerSlice(const GuidPrefix_t &owner) {
  for (const auto &guid : m_remote.ofOwner(owner)) {
    removeRemoteEndpoint(guid);
  }
}

// a join snapshot can hand us endpoints of a participant we never see over SPDP, no proxy means no lease expiry, so store temporarily
void SnapEDPAgent::removeOwnerlessEndpoints() {
  Lock lock{m_mutex};
  if (m_remote.empty()) {
    m_unseenEndpointOwners.clear();
    return;
  }

  const std::vector<GuidPrefix_t> owners = m_remote.owners();

  std::vector<GuidPrefix_t> known;
  for (const auto &peer : m_part->getRemoteSnapViews()) {
    known.push_back(peer.prefix);
  }

  const uint32_t now = rtps::timeNowMs();
  const uint32_t leaseMs = Config::SPDP_LEASE_DURATION_MS.load(std::memory_order_relaxed);
  std::vector<GuidPrefix_t> expired;
  for (const auto &owner : owners) {
    if (std::find(known.begin(), known.end(), owner) != known.end()) {
      m_unseenEndpointOwners.erase(owner);
      continue;
    }
    const uint32_t *firstSeen = m_unseenEndpointOwners.find(owner);
    if (firstSeen == nullptr) {
      m_unseenEndpointOwners.insertOrAssign(owner, now);
    } else if (now - *firstSeen >= leaseMs) {
      expired.push_back(owner);
    }
  }
  for (const auto &owner : expired) {
    SEDP_LOG("removeOwnerlessEndpoints: dropping endpoints of an owner without proxy");
    dropOwnerSlice(owner);
    m_unseenEndpointOwners.erase(owner);
  }

  // forget owners that no longer have endpoints here
  std::vector<GuidPrefix_t> gone;
  for (const auto &entry : m_unseenEndpointOwners) {
    if (std::find(owners.begin(), owners.end(), entry.key) == owners.end()) {
      gone.push_back(entry.key);
    }
  }
  for (const auto &owner : gone) {
    m_unseenEndpointOwners.erase(owner);
  }
}

// process one Frame of the resync
void SnapEDPAgent::processResyncFrame(const GuidPrefix_t &sender, uint32_t resyncId, uint32_t totalEndpoints, const std::vector<TopicData> &frame) {
  if (frame.size() == totalEndpoints) {
    m_resync.drop(sender);
    replaceOwnerEndpoints(sender, frame, /*sliceComplete=*/true);
    return;
  }

  replaceOwnerEndpoints(sender, frame, /*sliceComplete=*/false);

  std::vector<TopicData> whole;
  if (m_resync.add(sender, resyncId, totalEndpoints, frame, whole)) {
    replaceOwnerEndpoints(sender, whole, /*sliceComplete=*/true);
  }
}

void SnapEDPAgent::receiveEndpoints(const TopicData &data) {
  SEDP_LOG("Receive endpoint: Topic %s Type %s\n", data.topicName, data.typeName);

  // If ours discard
  if (data.endpointGuid.prefix == m_part->m_guidPrefix) {
    return;
  }

  uint32_t entityIdU32 = 0;
  std::memcpy(&entityIdU32, &data.endpointGuid.entityId, sizeof(data.endpointGuid.entityId));
  int matched = 0;
  const TracePrefix self(m_part->m_guidPrefix);

  // Read new topic data
  m_remote.upsert(data);
  if (SnapEDPEndpointStore::isWriter(data.endpointGuid)) {

    // record the remote writer once but always retry matching

    Reader *reader = m_part->getMatchingReader(data);
    if (reader != nullptr && data.unicastLocator.isValid()) {
      matched = 1;
      const bool newlyMatched = reader->addNewMatchedWriter( WriterProxy{data.endpointGuid, data.unicastLocator});
      if (newlyMatched && mfp_onNewPublisherCallback != nullptr) {
        mfp_onNewPublisherCallback(m_onNewPublisherArgs);
      }
    }
  } else if (SnapEDPEndpointStore::isReader(data.endpointGuid)) {

    Writer *writer = m_part->getMatchingWriter(data);
    if (writer != nullptr && data.unicastLocator.isValid()) {
      matched = 1;
      const bool newlyMatched = writer->addNewMatchedReader(
          ReaderProxy{data.endpointGuid, data.unicastLocator});
      if (newlyMatched && mfp_onNewSubscriberCallback != nullptr) {
        mfp_onNewSubscriberCallback(m_onNewSubscriberArgs);
      }
    }
  }

  RTPS_TRACE_PERF_EVENT(gossip_endpoint_received, 0ull, self.hi, self.lo, data.topicName, entityIdU32, matched);
}

void SnapEDPAgent::removeRemoteEndpoint(const Guid_t &endpointGuid) {
  if (!m_remote.remove(endpointGuid)) {
    return;
  }

  Lock localLock{m_localMutex};
  if (SnapEDPEndpointStore::isWriter(endpointGuid)) {
    for (auto *reader : m_localReaders) {
      reader->removeWriter(endpointGuid);
    }
  } else {
    for (auto *writer : m_localWriters) {
      writer->removeReader(endpointGuid);
    }
  }
}

// Replace endpoints of some participant endpoints with newly received onces
void SnapEDPAgent::replaceOwnerEndpoints(const GuidPrefix_t &owner, const std::vector<TopicData> &slice, bool sliceComplete) {
  
  for (const auto &data : slice) {
    receiveEndpoints(data);
  }
  
  if (!sliceComplete) {
    return;
  }

  // Remove find stale endpoints operator
  const std::vector<Guid_t> stale = m_remote.staleOf(owner, slice);

  // Remove stale outdated endpoints
  for (const auto &guid : stale) {
    removeRemoteEndpoint(guid);
  }
}

void SnapEDPAgent::receiveParticipant(const GuidPrefix_t &prefix, const GuidPrefix_t &root) {
  if (prefix == m_part->m_guidPrefix || root == GUIDPREFIX_UNKNOWN) {
    return;
  }
  // store the root of participant prefix 
  m_part->setRemoteRoot(prefix, root);
}

void SnapEDPAgent::setRemoteEndpointHash(const GuidPrefix_t &prefix, uint64_t endpointHash) {
  if (prefix == m_part->m_guidPrefix || prefix == GUIDPREFIX_UNKNOWN) {
    return;
  }
  // the announcement carries the senders post change hash, so update
  m_part->setRemoteEndpointHash(prefix, endpointHash);
}
