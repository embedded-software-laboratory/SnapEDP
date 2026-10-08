/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_EDPAGENTBASE_H
#define RTPS_EDPAGENTBASE_H

#include "rtps/common/types.h"
#include "rtps/config.h"
#include "rtps/discovery/BuiltInEndpoints.h"
#include "rtps/discovery/ParticipantProxyData.h"
#include "rtps/discovery/TopicData.h"
#include "rtps/utils/Lock.h"

#include <vector>

namespace rtps {

class Participant;
class Reader;
class Writer;
class ReaderCacheChange;

// Abstract Agent interface, to be able to easily exchange the Discovery implementation
class EDPAgentBase {
public:
  EDPAgentBase() = default;
  virtual ~EDPAgentBase() = default;

  EDPAgentBase(const EDPAgentBase &) = delete;
  EDPAgentBase(EDPAgentBase &&) = delete;
  EDPAgentBase &operator=(const EDPAgentBase &) = delete;
  EDPAgentBase &operator=(EDPAgentBase &&) = delete;

  virtual void init(Participant &part, const BuiltInEndpoints &endpoints);
  virtual void stop() {}
  virtual void start() {}
  virtual void addWriter(Writer &writer) = 0;
  virtual void addReader(Reader &reader) = 0;

  virtual void removeWriter(Writer &writer) = 0;
  virtual void removeReader(Reader &reader) = 0;

  virtual void onRemoteParticipantDiscovered(const GuidPrefix_t & /*prefix*/ , const SPDPDiscoverState& state) {}
  virtual void onRemoteParticipantRemoved(const GuidPrefix_t & /*prefix*/) {}
  virtual void onAddProxiesForRemoteParticipant(const ParticipantProxyData &proxyData, const Locator &locator);
  virtual uint32_t getJoinedPeerCounts() { return 0; }
  
  virtual GuidPrefix_t getCurrentRoot() const { return GUIDPREFIX_UNKNOWN; }
  virtual uint64_t getEndpointHash() const { return 0; }
  virtual void dumpDiscoveryState(uint32_t /*snapshot_id*/, bool /*is_final*/) {}

  void registerOnNewPublisherMatchedCallback(void (*cb)(void *arg), void *args);
  void registerOnNewSubscriberMatchedCallback(void (*cb)(void *arg), void *args);

protected:
  Participant *m_part = nullptr;
  BuiltInEndpoints m_endpoints;
  rtps::Mutex m_mutex;
  uint8_t m_buffer[1472];

  std::vector<TopicDataCompressed> m_unmatchedRemoteWriters;
  std::vector<TopicDataCompressed> m_unmatchedRemoteReaders;

  void (*mfp_onNewPublisherCallback)(void *arg) = nullptr;
  void *m_onNewPublisherArgs = nullptr;
  void (*mfp_onNewSubscriberCallback)(void *arg) = nullptr;
  void *m_onNewSubscriberArgs = nullptr;

  void onNewPublisher(const TopicData &writerData, EventId_t eventId = 0);
  void onNewSubscriber(const TopicData &readerData, EventId_t eventId = 0);
  void addUnmatchedRemoteWriter(const TopicData &writerData);
  void addUnmatchedRemoteReader(const TopicData &readerData);
  void tryMatchUnmatchedEndpoints();
};

} // namespace rtps

#endif // RTPS_SEDPAGENTBASE_H
