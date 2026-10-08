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

#include "rtps/entities/Participant.h"

#include "rtps/discovery/SEDPAgent.h"
#include "rtps/discovery/SnapEDPAgent.h"
#include "rtps/entities/Reader.h"
#include "rtps/entities/Writer.h"
#include "rtps/messages/MessageReceiver.h"
#include "rtps/utils/Log.h"

#include <algorithm>

#if PARTICIPANT_VERBOSE && RTPS_GLOBAL_VERBOSE
#define PARTICIPANT_LOG(...) RTPS_LOG("Participant", PARTICIPANT_VERBOSE, __VA_ARGS__)
#else
#define PARTICIPANT_LOG(...) RTPS_TRACE_LOG_EMIT("Participant", __VA_ARGS__)
#endif

using rtps::Participant;

Participant::Participant()
    : m_guidPrefix(GUIDPREFIX_UNKNOWN), m_participantId(PARTICIPANT_ID_INVALID),
      m_receiver(this) {
  if (mutex_new(&m_mutex) != EMB_ERR_OK) {
    while (1)
      ;
  }
}
Participant::Participant(const GuidPrefix_t &guidPrefix,
                         ParticipantId_t participantId)
    : m_guidPrefix(guidPrefix), m_participantId(participantId),
      m_receiver(this) {

  if (mutex_new(&m_mutex) != EMB_ERR_OK) {
    while (1)
      ;
  }
}

Participant::~Participant() {
  m_spdpAgent.stop();
}

void Participant::reuse(const GuidPrefix_t &guidPrefix,
                        ParticipantId_t participantId) {
  m_guidPrefix = guidPrefix;
  m_participantId = participantId;
}

bool Participant::isValid() {
  return m_participantId != PARTICIPANT_ID_INVALID;
}

std::array<uint8_t, 3> Participant::getNextUserEntityKey() {
  const auto result = m_nextUserEntityId;

  ++m_nextUserEntityId[2];
  if (m_nextUserEntityId[2] == 0) {
    ++m_nextUserEntityId[1];
    if (m_nextUserEntityId[1] == 0) {
      ++m_nextUserEntityId[0];
    }
  }
  return result;
}

bool Participant::registerOnNewPublisherMatchedCallback(
    void (*callback)(void *arg), void *args) {
  if (!m_hasBuilInEndpoints || m_sedpAgent == nullptr) {
    return false;
  }

  m_sedpAgent->registerOnNewPublisherMatchedCallback(callback, args);
  return true;
}

bool Participant::registerOnNewSubscriberMatchedCallback(
    void (*callback)(void *arg), void *args) {
  if (!m_hasBuilInEndpoints || m_sedpAgent == nullptr) {
    return false;
  }

  m_sedpAgent->registerOnNewSubscriberMatchedCallback(callback, args);
  return true;
}

void Participant::setRuntimeContext(ThreadPool *threadPool,
                                    DefaultDriver *transport) {
  Lock lock{m_mutex};
  m_threadPool = threadPool;
  m_transport = transport;
}

rtps::Writer *Participant::addWriter(Writer *pWriter) {
  if (pWriter == nullptr) {
    return nullptr;
  }

  {
    Lock lock{m_mutex};
    m_writers.push_back(pWriter);
  }
  if (m_hasBuilInEndpoints && m_sedpAgent != nullptr) {
    m_sedpAgent->addWriter(*pWriter);
  }
  return pWriter;
}

rtps::Reader *Participant::addReader(Reader *pReader) {
  if (pReader == nullptr) {
    return nullptr;
  }

  {
    Lock lock{m_mutex};
    m_readers.push_back(pReader);
  }
  if (m_hasBuilInEndpoints && m_sedpAgent != nullptr) {
    m_sedpAgent->addReader(*pReader);
  }
  return pReader;
}

bool Participant::removeWriter(Writer *pWriter) {
  if (pWriter == nullptr) {
    return false;
  }

  bool found = false;
  {
    Lock lock{m_mutex};
    auto it = std::find(m_writers.begin(), m_writers.end(), pWriter);
    if (it != m_writers.end()) {
      m_writers.erase(it);
      found = true;
    }
  }
  if (!found) {
    return false;
  }
  if (m_hasBuilInEndpoints && m_sedpAgent != nullptr) {
    m_sedpAgent->removeWriter(*pWriter);
  }
  return true;
}

bool Participant::removeReader(Reader *pReader) {
  if (pReader == nullptr) {
    return false;
  }

  bool found = false;
  {
    Lock lock{m_mutex};
    auto it = std::find(m_readers.begin(), m_readers.end(), pReader);
    if (it != m_readers.end()) {
      m_readers.erase(it);
      found = true;
    }
  }
  if (!found) {
    return false;
  }
  if (m_hasBuilInEndpoints && m_sedpAgent != nullptr) {
    m_sedpAgent->removeReader(*pReader);
  }
  return true;
}

rtps::Writer *Participant::getWriter(EntityId_t id) const {
  Lock lock{m_mutex};
  for (auto *writer : m_writers) {
    if (writer != nullptr && writer->m_attributes.endpointGuid.entityId == id) {
      return writer;
    }
  }
  return nullptr;
}

rtps::Reader *Participant::getReader(EntityId_t id) const {
  Lock lock{m_mutex};
  for (auto *reader : m_readers) {
    if (reader != nullptr && reader->m_attributes.endpointGuid.entityId == id) {
      return reader;
    }
  }
  return nullptr;
}

rtps::Reader *Participant::getReaderByWriterId(const Guid_t &guid) const {
  Lock lock{m_mutex};
  for (auto *reader : m_readers) {
    if (reader != nullptr && reader->knowWriterId(guid)) {
      return reader;
    }
  }
  return nullptr;
}

rtps::Writer *
Participant::getMatchingWriter(const TopicData &readerTopicData) const {
  Lock lock{m_mutex};
  for (auto *writer : m_writers) {
    if (writer != nullptr && writer->m_attributes.matchesTopicOf(readerTopicData) &&
        (readerTopicData.reliabilityKind == ReliabilityKind_t::BEST_EFFORT ||
         writer->m_attributes.reliabilityKind ==
             ReliabilityKind_t::RELIABLE)) {
      return writer;
    }
  }
  return nullptr;
}

rtps::Reader *
Participant::getMatchingReader(const TopicData &writerTopicData) const {
  Lock lock{m_mutex};
  for (auto *reader : m_readers) {
    if (reader != nullptr && reader->m_attributes.matchesTopicOf(writerTopicData) &&
        (writerTopicData.reliabilityKind == ReliabilityKind_t::RELIABLE ||
         reader->m_attributes.reliabilityKind ==
             ReliabilityKind_t::BEST_EFFORT)) {
      return reader;
    }
  }
  return nullptr;
}

rtps::Writer *Participant::getMatchingWriter(
    const TopicDataCompressed &readerTopicData) const {
  Lock lock{m_mutex};
  for (auto *writer : m_writers) {
    if (writer != nullptr && readerTopicData.matchesTopicOf(writer->m_attributes) &&
        (readerTopicData.reliabilityKind == ReliabilityKind_t::BEST_EFFORT ||
         writer->m_attributes.reliabilityKind ==
             ReliabilityKind_t::RELIABLE)) {
      return writer;
    }
  }
  return nullptr;
}

rtps::Reader *Participant::getMatchingReader(
    const TopicDataCompressed &writerTopicData) const {
  Lock lock{m_mutex};
  for (auto *reader : m_readers) {
    if (reader != nullptr && writerTopicData.matchesTopicOf(reader->m_attributes) &&
        (writerTopicData.reliabilityKind == ReliabilityKind_t::RELIABLE ||
         reader->m_attributes.reliabilityKind == ReliabilityKind_t::BEST_EFFORT)) {
      return reader;
    }
  }
  return nullptr;
}

bool Participant::addNewRemoteParticipant(
    const ParticipantProxyData &remotePart) {
  Lock lock{m_mutex};
  m_remoteParticipants.push_back(remotePart);
  PARTICIPANT_LOG("addNewRemoteParticipant success=1 current_count=%u",
                  static_cast<unsigned>(m_remoteParticipants.size()));
  return true;
}

bool Participant::removeRemoteParticipant(const GuidPrefix_t &prefix) {
  // callbacks run unlocked like in addWriter and addReader, both call back into the participant and would reenter the non recursive m_mutex
  removeAllEntitiesOfParticipant(prefix);
  getSEDPAgent().onRemoteParticipantRemoved(prefix);

  Lock lock{m_mutex};
  for (auto it = m_remoteParticipants.begin(); it != m_remoteParticipants.end(); ++it) {
    if (it->m_guid.prefix == prefix) {
      m_remoteParticipants.erase(it);
      return true;
    }
  }

  return false;
}

void Participant::removeAllEntitiesOfParticipant(const GuidPrefix_t &prefix) {
  // snapshot the endpoint lists so the removal calls, which take their own locks, run without m_mutex held
  std::vector<Writer *> writers;
  std::vector<Reader *> readers;
  {
    Lock lock{m_mutex};
    writers = m_writers;
    readers = m_readers;
  }

  for (auto *writer : writers) {
    if (writer != nullptr) {
      writer->removeReaderOfParticipant(prefix);
    }
  }

  for (auto *reader : readers) {
    if (reader != nullptr) {
      reader->removeWriterOfParticipant(prefix);
    }
  }
}

const rtps::ParticipantProxyData *
Participant::findRemoteParticipant(const GuidPrefix_t &prefix) {
  Lock lock{m_mutex};
  ParticipantProxyData *result = nullptr;
  for (auto &remote : m_remoteParticipants) {
    if (remote.m_guid.prefix == prefix) {
      result = &remote;
      break;
    }
  }
  PARTICIPANT_LOG("findRemoteParticipant found=%u current_count=%u",
                  static_cast<unsigned>(result != nullptr),
                  static_cast<unsigned>(m_remoteParticipants.size()));
  return result;
}

void Participant::refreshRemoteParticipantLiveliness(
    const GuidPrefix_t &prefix) {
  Lock lock{m_mutex};
  for (auto &remote : m_remoteParticipants) {
    if (remote.m_guid.prefix == prefix) {
      remote.onAliveSignal();
      break;
    }
  }
}

void Participant::updateRemoteParticipantSnapFields(
    const GuidPrefix_t &prefix, DiscoveryMode sedpSupport,
    SPDPDiscoverState snapState, uint32_t snapNetSize,
    const GuidPrefix_t &root, uint64_t endpointHash) {
  Lock lock{m_mutex};
  for (auto &remote : m_remoteParticipants) {
    if (remote.m_guid.prefix == prefix) {
      remote.m_sedpSupport = sedpSupport;
      remote.m_snapState = snapState;
      remote.m_snapNetSize = snapNetSize;
      remote.m_root = root;
      remote.m_endpointHash = endpointHash;
      break;
    }
  }
}

bool Participant::hasReaderWithMulticastLocator(ip4_struct_t address) {
  for (auto *reader : m_readers) {
    if (reader != nullptr &&
        reader->m_attributes.multicastLocator.isSameAddress(&address)) {
      return true;
    }
  }
  return false;
}

uint32_t Participant::getRemoteParticipantCount() {
  Lock lock{m_mutex};
  return m_remoteParticipants.size();
}

const std::vector<rtps::ParticipantProxyData *>
Participant::getRemoteParticipants() {
  Lock lock{m_mutex};
  std::vector<ParticipantProxyData *> out;
  out.reserve(m_remoteParticipants.size());
  for (auto &remote : m_remoteParticipants) {
    out.push_back(&remote);
  }
  return out;
}

std::vector<Participant::RemoteSnapView> Participant::getRemoteSnapViews() {
  Lock lock{m_mutex};
  std::vector<RemoteSnapView> out;
  out.reserve(m_remoteParticipants.size());
  for (const auto &remote : m_remoteParticipants) {
    out.push_back({remote.m_guid.prefix, remote.m_snapState, remote.m_root,
                   remote.m_endpointHash});
  }
  return out;
}

bool Participant::getRemoteSnapView(const GuidPrefix_t &prefix,
                                      RemoteSnapView &out) {
  Lock lock{m_mutex};
  for (const auto &remote : m_remoteParticipants) {
    if (remote.m_guid.prefix == prefix) {
      out = {remote.m_guid.prefix, remote.m_snapState, remote.m_root,
             remote.m_endpointHash};
      return true;
    }
  }
  return false;
}

bool Participant::hasRemoteParticipant(const GuidPrefix_t &prefix) {
  Lock lock{m_mutex};
  for (const auto &remote : m_remoteParticipants) {
    if (remote.m_guid.prefix == prefix) {
      return true;
    }
  }
  return false;
}

bool Participant::copyRemoteParticipant(const GuidPrefix_t &prefix,
                                        ParticipantProxyData &out) {
  Lock lock{m_mutex};
  for (const auto &remote : m_remoteParticipants) {
    if (remote.m_guid.prefix == prefix) {
      out = remote;
      return true;
    }
  }
  return false;
}

void Participant::setRemoteRoot(const GuidPrefix_t &prefix,
                                const GuidPrefix_t &root) {
  Lock lock{m_mutex};
  for (auto &remote : m_remoteParticipants) {
    if (remote.m_guid.prefix == prefix) {
      remote.m_root = root;
      return;
    }
  }
}

void Participant::setRemoteEndpointHash(const GuidPrefix_t &prefix,
                                        uint64_t endpointHash) {
  Lock lock{m_mutex};
  for (auto &remote : m_remoteParticipants) {
    if (remote.m_guid.prefix == prefix) {
      remote.m_endpointHash = endpointHash;
      return;
    }
  }
}

void Participant::clearRemoteRootsNaming(const GuidPrefix_t &deadRoot) {
  Lock lock{m_mutex};
  for (auto &remote : m_remoteParticipants) {
    if (remote.m_root == deadRoot) {
      remote.m_root = GUIDPREFIX_UNKNOWN;
    }
  }
}

rtps::MessageReceiver *Participant::getMessageReceiver() { return &m_receiver; }

void Participant::addHeartbeat(GuidPrefix_t sourceGuidPrefix) {
  Lock lock{m_mutex};
  for (auto &remote : m_remoteParticipants) {
    if (remote.m_guid.prefix == sourceGuidPrefix) {
      remote.onAliveSignal();
      break;
    }
  }
}

bool Participant::checkAndResetHeartbeats() {
  // collect first and remove after, removeRemoteParticipant erases from m_remoteParticipants and would invalidate our iterator
  std::vector<GuidPrefix_t> expired;
  {
    Lock lock{m_mutex};
    PARTICIPANT_LOG("Have %u remote participants\n",
                    (unsigned int)m_remoteParticipants.size());
    for (auto &remote : m_remoteParticipants) {
      PARTICIPANT_LOG("remote participant age = %u\n",
                      (unsigned int)remote.getAliveSignalAgeInMilliseconds());
      if (remote.isAlive()) {
        PARTICIPANT_LOG("remote participant is alive\n");
        continue;
      }
      expired.push_back(remote.m_guid.prefix);
    }
  }

  for (const auto &prefix : expired) {
    PARTICIPANT_LOG("!!! REMOVING PARTICIPANT !!!\n");
    if (!removeRemoteParticipant(prefix)) {
      return false;
    }
  }
  return true;
}

rtps::SPDPAgent &Participant::getSPDPAgent() { return m_spdpAgent; }

rtps::EDPAgentBase &Participant::getSEDPAgent() { return *m_sedpAgent; }

void Participant::addBuiltInEndpoints(BuiltInEndpoints &endpoints,
                                      DiscoveryMode mode) {
  m_discoveryMode = mode;
  m_hasBuilInEndpoints = true;
  PARTICIPANT_LOG("Participant: Initializing builtin agents. \n");

  switch (mode) {
  case DiscoveryMode::Snap:
    m_sedpAgent = std::make_unique<SnapEDPAgent>();
    break;
  case DiscoveryMode::Standard:
  default:
    m_sedpAgent = std::make_unique<SEDPAgent>();
    break;
  }

  m_spdpAgent.init(*this, endpoints);
  m_sedpAgent->init(*this, endpoints);

  // This needs to be done after initializing the agents
  addWriter(endpoints.spdpWriter);
  addReader(endpoints.spdpReader);
  addWriter(endpoints.sedpPubWriter);
  addReader(endpoints.sedpPubReader);
  addWriter(endpoints.sedpSubWriter);
  addReader(endpoints.sedpSubReader);

  // announcement channel used by gossip mode
  if (endpoints.announcementWriter != nullptr) {
    addWriter(endpoints.announcementWriter);
  }
  if (endpoints.announcementReader != nullptr) {
    addReader(endpoints.announcementReader);
  }
  if (endpoints.edpSnapWriter != nullptr) {
    addWriter(endpoints.edpSnapWriter);
  }
  if (endpoints.edpSnapReader != nullptr) {
    addReader(endpoints.edpSnapReader);
  }
}

void Participant::newMessage(const uint8_t *data, DataSize_t size,
                             EventId_t eventId) {
  auto result = m_receiver.processMessage(data, size, eventId);
  PARTICIPANT_LOG("processMessage result=%d eventId=%lu",
                  result ? 1 : 0, static_cast<unsigned long>(eventId));
}
