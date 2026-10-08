/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_PARTICIPANT_H
#define RTPS_PARTICIPANT_H

#include "rtps/common/types.h"
#include "rtps/config.h"
#include "rtps/config/FeatureQOS.h"
#include "rtps/discovery/EDPAgentBase.h"
#include "rtps/discovery/SPDPAgent.h"
#include "rtps/communication/NetworkDriver.h"
#include "rtps/messages/MessageReceiver.h"
#include "rtps/utils/iptypes.h"
#include "rtps/utils/Lock.h"

#include <memory>
#include <vector>

namespace rtps {

class Writer;
class Reader;
class ThreadPool;

class Participant {
public:
  GuidPrefix_t m_guidPrefix;
  ParticipantId_t m_participantId;

  Participant();
  explicit Participant(const GuidPrefix_t &guidPrefix,
                       ParticipantId_t participantId);

  // not allowed, the message receiver holds a pointer to the participant
  Participant(const Participant &) = delete;
  Participant(Participant &&) = delete;
  Participant &operator=(const Participant &) = delete;
  Participant &operator=(Participant &&) = delete;

  ~Participant();
  bool isValid();

  void reuse(const GuidPrefix_t &guidPrefix, ParticipantId_t participantId);

  std::array<uint8_t, 3> getNextUserEntityKey();

  bool registerOnNewPublisherMatchedCallback(void (*callback)(void *arg),
                                             void *args);
  bool registerOnNewSubscriberMatchedCallback(void (*callback)(void *arg),
                                              void *args);

  // Not thread safe function to add a writer
  Writer *addWriter(Writer *writer);

  // Not thread safe function to add a reader
  Reader *addReader(Reader *reader);

  // drop a local endpoint and let the SEDP agent dispose it, Domain still owns it
  bool removeWriter(Writer *writer);
  bool removeReader(Reader *reader);

  // Probably thread safe if writers cannot be removed
  Writer *getWriter(EntityId_t id) const;
  Writer *getMatchingWriter(const TopicData &topicData) const;
  Writer *getMatchingWriter(const TopicDataCompressed &topicData) const;

  // Probably thread safe if readers cannot be removed
  Reader *getReader(EntityId_t id) const;
  Reader *getReaderByWriterId(const Guid_t &guid) const;
  Reader *getMatchingReader(const TopicData &topicData) const;
  Reader *getMatchingReader(const TopicDataCompressed &topicData) const;

  bool addNewRemoteParticipant(const ParticipantProxyData &remotePart);
  bool removeRemoteParticipant(const GuidPrefix_t &prefix);
  void removeAllEntitiesOfParticipant(const GuidPrefix_t &prefix);
  const ParticipantProxyData *findRemoteParticipant(const GuidPrefix_t &prefix);
  void refreshRemoteParticipantLiveliness(const GuidPrefix_t &prefix);
  uint32_t getRemoteParticipantCount();
  // findRemoteParticipant and getRemoteParticipants hand out pointers into a vector SPDP changes from its own thread, only use them while no other thread can add or remove participants
  const std::vector<ParticipantProxyData*> getRemoteParticipants();

  // gossip relevant fields of one remote participant, copied under the lock
  struct RemoteSnapView {
    GuidPrefix_t prefix;
    SPDPDiscoverState snapState;
    GuidPrefix_t root;
    uint64_t endpointHash;
  };
  std::vector<RemoteSnapView> getRemoteSnapViews();
  bool getRemoteSnapView(const GuidPrefix_t &prefix, RemoteSnapView &out);
  bool hasRemoteParticipant(const GuidPrefix_t &prefix);
  bool copyRemoteParticipant(const GuidPrefix_t &prefix, ParticipantProxyData &out);
  void setRemoteRoot(const GuidPrefix_t &prefix, const GuidPrefix_t &root);
  void setRemoteEndpointHash(const GuidPrefix_t &prefix, uint64_t endpointHash);
  // reset the advertised root of every proxy that still names deadRoot
  void clearRemoteRootsNaming(const GuidPrefix_t &deadRoot);
  // refresh a known remote proxys gossip fields so peers track the latest SPDP broadcast, not first contact values
  void updateRemoteParticipantSnapFields(const GuidPrefix_t &prefix,
                                           DiscoveryMode sedpSupport,
                                           SPDPDiscoverState snapState,
                                           uint32_t snapNetSize,
                                           const GuidPrefix_t &root,
                                           uint64_t endpointHash);


  MessageReceiver *getMessageReceiver();
  void addHeartbeat(GuidPrefix_t sourceGuidPrefix);
  bool checkAndResetHeartbeats();

  bool hasReaderWithMulticastLocator(ip4_struct_t address);

  void addBuiltInEndpoints(BuiltInEndpoints &endpoints, DiscoveryMode mode);
  void newMessage(const uint8_t *data, DataSize_t size, EventId_t eventId = 0);

  void setRuntimeContext(ThreadPool *threadPool, DefaultDriver *transport);
  ThreadPool *getThreadPool() { return m_threadPool; }

  SPDPAgent &getSPDPAgent();
  EDPAgentBase &getSEDPAgent();
  bool hasBuiltInEndpoints() const { return m_hasBuilInEndpoints; }

  DiscoveryMode getDiscoveryMode() const { return m_discoveryMode; }

private:
  DiscoveryMode m_discoveryMode = DiscoveryMode::Standard;
  MessageReceiver m_receiver;
  bool m_hasBuilInEndpoints = false;
  std::array<uint8_t, 3> m_nextUserEntityId{{0, 0, 1}};
  std::vector<Writer *> m_writers;
  std::vector<Reader *> m_readers;

  ThreadPool *m_threadPool = nullptr;
  DefaultDriver *m_transport = nullptr;

  mutable Mutex m_mutex;
  std::vector<ParticipantProxyData> m_remoteParticipants;

  SPDPAgent m_spdpAgent;
  std::unique_ptr<EDPAgentBase> m_sedpAgent;
};
} // namespace rtps

#endif // RTPS_PARTICIPANT_H
