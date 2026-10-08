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

#include "rtps/entities/Domain.h"
#include "rtps/utils/Log.h"
#include "rtps/utils/udpUtils.h"
#include "trace_control.h"
#include <iostream>

#if defined(unix) || defined(__unix__)
#include <unistd.h>
#endif
#include <chrono>
#include <cstdint>
#include <random>

#if DOMAIN_VERBOSE && RTPS_GLOBAL_VERBOSE
#define DOMAIN_LOG(...) RTPS_LOG("Domain", DOMAIN_VERBOSE, __VA_ARGS__)
#else
#define DOMAIN_LOG(...) RTPS_TRACE_LOG_EMIT("Domain", __VA_ARGS__)
#endif

using rtps::Domain;

Domain::Domain(const FeatureQOS &qos)
    : m_featureQos(qos),
      m_threadPool(receiveJumppad, this),
      m_transport(ThreadPool::readCallback, &m_threadPool) {

  m_transport.createUdpConnection(getUserMulticastPort(), /*reuseAddr=*/true);
  DefaultDriver::updateIP();
  m_transport.createUdpConnection(getBuiltInMulticastPort(), /*reuseAddr=*/true);
  m_transport.joinMultiCastGroup(transformIP4ToU32(239, 255, 0, 1));
}

Domain::~Domain() {
  std::cerr << "[Domain] ~dtor: calling stop()" << std::endl;
  stop();
  std::cerr << "[Domain] ~dtor: stop() done, implicit member dtors follow (m_transport then m_threadPool)" << std::endl;
}

bool Domain::completeInit() {
  m_initComplete = m_threadPool.startThreads();

  if (!m_initComplete) {
    DOMAIN_LOG("Failed starting threads\n");
  }

  for (auto& [id, part] : m_participants){
    if (part != nullptr && part->isValid()) {
      part->getSPDPAgent().start();
      if (part->hasBuiltInEndpoints()) {
        part->getSEDPAgent().start();
      }
    }
  }
  return m_initComplete;
}

void Domain::stop() {
  // SPDP broadcast threads publish through m_statelessWriters which is destroyed before m_participants, stop them here or they dereference a freed spdpWriter
  for (auto &[id, part] : m_participants) {
    if (part != nullptr) {
      part->getSPDPAgent().stop();
      // same reason for the discovery agents own thread
      if (part->hasBuiltInEndpoints()) {
        part->getSEDPAgent().stop();
      }
    }
  }
  m_threadPool.stopThreads();
}

void Domain::receiveJumppad(void *callee, const PacketInfo &packet) {
  auto domain = static_cast<Domain *>(callee);
  domain->receiveCallback(packet);
}

void Domain::receiveCallback(const PacketInfo &packet) {
  const uint8_t *payload = packet.buffer.m_buf.data();
  rtps::DataSize_t payloadLen = packet.buffer.m_buf.size();

  RTPS_TRACE_PERF_EVENT(domain_receive_callback, packet.eventId,
             packet.destPort,
             static_cast<uint32_t>(payloadLen),
             isMetaMultiCastPort(packet.destPort) ? 1 : 0,
             isUserMultiCastPort(packet.destPort) ? 1 : 0);

  if (isMetaMultiCastPort(packet.destPort)) {
    // Pass to all
    DOMAIN_LOG("Domain: Multicast to port %u\n", packet.destPort);
    std::size_t activeParticipants = 0;
    for (auto const& [id, part] : m_participants){
      if (part != nullptr && part->isValid()) {
        ++activeParticipants;
        part->newMessage(payload, payloadLen, packet.eventId);
      }
    }
    DOMAIN_LOG("Forward message len=%u to %u of %u participants",
               static_cast<unsigned>(payloadLen),
               static_cast<unsigned>(activeParticipants),
               static_cast<unsigned>(m_participants.size()));
    // First Check if UserTraffic Multicast
  } else if (isUserMultiCastPort(packet.destPort)) {
    // pass to the participant whose reader has this multicast address
    DOMAIN_LOG("Domain: Got user multicast message on port %u\n",
               packet.destPort);
    for (auto const& [id, part] : m_participants){
      if (part == nullptr || !part->isValid()) {
        continue;
      }
      if (part->hasReaderWithMulticastLocator(packet.destAddr)) {
        DOMAIN_LOG("Domain: Forward Multicast only to Participant.\n");
        part->newMessage(payload, payloadLen, packet.eventId);
      }
    }
  } else {
    // Pass to addressed one only, unicast by port
    ParticipantId_t id = getParticipantIdFromUnicastPort(packet.destPort, isUserPort(packet.destPort));
    if (id != PARTICIPANT_ID_INVALID) {
      DOMAIN_LOG("Domain: Got unicast message with PartID %i on port %u\n", id,  packet.destPort);
      if (id >= PARTICIPANT_START_ID && id < PARTICIPANT_END_ID) {
        auto &part = m_participants[id - PARTICIPANT_START_ID];
        if (part != nullptr && part->isValid()) {
          part->newMessage(payload, payloadLen, packet.eventId);
        } else {
          DOMAIN_LOG("Domain: Participant id invalid/uninitialized.\n");
        }
      } else {
        DOMAIN_LOG("Domain: Participant id too high or unplausible.\n");
      }
    } else {
      DOMAIN_LOG("Domain: Got message to port %u: no matching participant\n",
                 packet.destPort);
    }
  }
}

rtps::Participant *Domain::createParticipant(ParticipantId_t requestedId) {
  return createParticipantImpl(requestedId, /*explicitPrefix=*/nullptr);
}

rtps::Participant *
Domain::createParticipant(const GuidPrefix_t &explicitPrefix,
                          ParticipantId_t requestedId) {
  return createParticipantImpl(requestedId, &explicitPrefix);
}

rtps::Participant *
Domain::createParticipantImpl(ParticipantId_t requestedId,
                              const GuidPrefix_t *explicitPrefix) {

  DOMAIN_LOG("Domain: Creating new participant.\n");

  if (m_initComplete) {
    return nullptr;
  }

  // bind and hold the ids unicast ports so a racing process fails its duplicate bind
  auto reservePorts = [&](ParticipantId_t id) {
    // an id we already assigned would dedup in createUdpConnection and look reservable, reject it explicitly
    auto existing = m_participants.find(id);
    if (existing != m_participants.end() && existing->second != nullptr &&
        existing->second->isValid()) {
      return false;
    }
    const Ip4Port_t userPort = getUserUnicastPort(id);
    const Ip4Port_t builtinPort = getBuiltInUnicastPort(id);
    if (m_transport.createUdpConnection(userPort) == nullptr) {
      return false;
    }
    if (m_transport.createUdpConnection(builtinPort) == nullptr) {
      m_transport.removeUdpConnection(userPort);
      return false;
    }
    return true;
  };

  ParticipantId_t chosenId = PARTICIPANT_ID_INVALID;

  if (requestedId != PARTICIPANT_ID_INVALID) {
    DOMAIN_LOG("Domain: Manual participant ID configuration.\n");
    if (requestedId < PARTICIPANT_START_ID ||
        requestedId >= static_cast<ParticipantId_t>(PARTICIPANT_START_ID + m_participants.size())) {
      return nullptr;
    }
    auto &slot = m_participants[requestedId - PARTICIPANT_START_ID];
    if (slot != nullptr && slot->isValid()) {
      DOMAIN_LOG("Domain: Manual participant ID invalid.\n");
      return nullptr;
    }
    if (!reservePorts(requestedId)) {
      DOMAIN_LOG("Domain: Ports for manual participant ID invalid.\n");
      return nullptr;
    }
    chosenId = requestedId;
  } else {
    DOMAIN_LOG("Domain: Automatic, port-based participant ID selection.\n");
    for (std::size_t i = 0; i < PARTICIPANT_END_ID - PARTICIPANT_START_ID; ++i) {
      auto candidate = static_cast<ParticipantId_t>(PARTICIPANT_START_ID + i);
      if (reservePorts(candidate)) {
        chosenId = candidate;
        DOMAIN_LOG("Domain: Found and selected valid participant ID by available port.\n");
        break;
      }
    }
  }

  if (chosenId == PARTICIPANT_ID_INVALID) {
    DOMAIN_LOG("Failed to select valid ParticipantID..\n");
    return nullptr;
  }

  m_participants.insert({chosenId, std::make_unique<Participant>()});
  auto* part = m_participants[chosenId].get();
  const GuidPrefix_t prefix = (explicitPrefix != nullptr)
                                  ? *explicitPrefix
                                  : generateGuidPrefix(chosenId);
  part->reuse(prefix, chosenId);
  part->setRuntimeContext(&m_threadPool, &m_transport);
  registerPort(*part);


  DOMAIN_LOG("Domain: Creating builtin writers and readers. \n");
  createBuiltinWritersAndReaders(*part);
  m_nextParticipantId = chosenId + 1;

  if (m_nextParticipantId >= PARTICIPANT_END_ID) {
    m_nextParticipantId = PARTICIPANT_START_ID;
  }
  return part;
}

void Domain::createBuiltinWritersAndReaders(Participant &part) {
  // SPDP
  m_statelessWriters.emplace_back(std::unique_ptr<StatelessWriter>(new StatelessWriter()));
  StatelessWriter &spdpWriter = *m_statelessWriters.back();
  m_statelessReaders.emplace_back(std::unique_ptr<StatelessReader>(new StatelessReader()));
  StatelessReader &spdpReader = *m_statelessReaders.back();

  TopicData spdpWriterAttributes;
  spdpWriterAttributes.topicName[0] = '\0';
  spdpWriterAttributes.typeName[0] = '\0';
  spdpWriterAttributes.reliabilityKind = ReliabilityKind_t::BEST_EFFORT;
  spdpWriterAttributes.durabilityKind = DurabilityKind_t::TRANSIENT_LOCAL;
  spdpWriterAttributes.endpointGuid.prefix = part.m_guidPrefix;
  spdpWriterAttributes.endpointGuid.entityId =
      ENTITYID_SPDP_BUILTIN_PARTICIPANT_WRITER;
  spdpWriterAttributes.unicastLocator = getBuiltInMulticastLocator();

  spdpWriter.init(spdpWriterAttributes, TopicKind_t::WITH_KEY, &m_threadPool,
                  m_transport);
  spdpWriter.addNewMatchedReader(
      ReaderProxy{{part.m_guidPrefix, ENTITYID_SPDP_BUILTIN_PARTICIPANT_READER},
                  getBuiltInMulticastLocator()});

  TopicData spdpReaderAttributes;
  spdpReaderAttributes.endpointGuid = {
      part.m_guidPrefix, ENTITYID_SPDP_BUILTIN_PARTICIPANT_READER};
  spdpReader.init(spdpReaderAttributes);

  // COLLECT, spdp is always present
  BuiltInEndpoints endpoints{};
  endpoints.spdpWriter = &spdpWriter;
  endpoints.spdpReader = &spdpReader;

  // SEDP reliable endpoints, skipped in gossip mode to avoid heartbeat cost
  if (m_featureQos.discoveryMode != DiscoveryMode::Snap) {
    m_statefulReaders.emplace_back(std::unique_ptr<StatefulReader>(new StatefulReader()));
    StatefulReader &sedpPubReader = *m_statefulReaders.back();
    m_statefulReaders.emplace_back(std::unique_ptr<StatefulReader>(new StatefulReader()));
    StatefulReader &sedpSubReader = *m_statefulReaders.back();
    m_statefulWriters.emplace_back(std::unique_ptr<StatefulWriter>(new StatefulWriter()));
    StatefulWriter &sedpPubWriter = *m_statefulWriters.back();
    m_statefulWriters.emplace_back(std::unique_ptr<StatefulWriter>(new StatefulWriter()));
    StatefulWriter &sedpSubWriter = *m_statefulWriters.back();

    TopicData sedpAttributes;
    sedpAttributes.topicName[0] = '\0';
    sedpAttributes.typeName[0] = '\0';
    sedpAttributes.reliabilityKind = ReliabilityKind_t::RELIABLE;
    sedpAttributes.durabilityKind = DurabilityKind_t::TRANSIENT_LOCAL;
    sedpAttributes.endpointGuid.prefix = part.m_guidPrefix;
    sedpAttributes.unicastLocator =
        getBuiltInUnicastLocator(part.m_participantId);

    // READER
    sedpAttributes.endpointGuid.entityId =
        ENTITYID_SEDP_BUILTIN_PUBLICATIONS_READER;
    sedpPubReader.init(sedpAttributes, m_transport);
    sedpAttributes.endpointGuid.entityId =
        ENTITYID_SEDP_BUILTIN_SUBSCRIPTIONS_READER;
    sedpSubReader.init(sedpAttributes, m_transport);

    // WRITER
    sedpAttributes.endpointGuid.entityId =
        ENTITYID_SEDP_BUILTIN_PUBLICATIONS_WRITER;
    sedpPubWriter.init(sedpAttributes, TopicKind_t::NO_KEY, &m_threadPool,
                       m_transport);

    sedpAttributes.endpointGuid.entityId =
        ENTITYID_SEDP_BUILTIN_SUBSCRIPTIONS_WRITER;
    sedpSubWriter.init(sedpAttributes, TopicKind_t::NO_KEY, &m_threadPool,
                       m_transport);

    endpoints.sedpPubReader = &sedpPubReader;
    endpoints.sedpSubReader = &sedpSubReader;
    endpoints.sedpPubWriter = &sedpPubWriter;
    endpoints.sedpSubWriter = &sedpSubWriter;
  }

  // Gossip mode, multicast best effort announcement channel
  if (m_featureQos.discoveryMode == DiscoveryMode::Snap) {
    m_statelessWriters.emplace_back(std::make_unique<StatelessWriter>());
    StatelessWriter &announcementWriter = *m_statelessWriters.back();
    m_statelessReaders.emplace_back(std::make_unique<StatelessReader>());
    StatelessReader &announcementReader = *m_statelessReaders.back();

    TopicData announcementAttributes;
    announcementAttributes.topicName[0] = '\0';
    announcementAttributes.typeName[0] = '\0';
    announcementAttributes.reliabilityKind = ReliabilityKind_t::BEST_EFFORT;
    announcementAttributes.durabilityKind = DurabilityKind_t::VOLATILE;
    announcementAttributes.endpointGuid.prefix = part.m_guidPrefix;
    announcementAttributes.endpointGuid.entityId =
        ENTITYID_SEDP_BUILTIN_ANNOUNCEMENTS_WRITER;
    announcementAttributes.unicastLocator = getBuiltInMulticastLocator();

    announcementWriter.init(announcementAttributes, TopicKind_t::NO_KEY,
                            &m_threadPool, m_transport);
    announcementWriter.addNewMatchedReader(ReaderProxy{
        {part.m_guidPrefix, ENTITYID_SEDP_BUILTIN_ANNOUNCEMENTS_READER},
        getBuiltInMulticastLocator()});

    announcementAttributes.endpointGuid.entityId =
        ENTITYID_SEDP_BUILTIN_ANNOUNCEMENTS_READER;
    announcementAttributes.multicastLocator = getBuiltInMulticastLocator();
    announcementReader.init(announcementAttributes);

    endpoints.announcementWriter = &announcementWriter;
    endpoints.announcementReader = &announcementReader;
  }

  if (m_featureQos.discoveryMode == DiscoveryMode::Snap) {
    // unicast best effort, agent adds a per target reader proxy per exchange
    m_transientWriters.emplace_back(std::make_unique<TransientWriter>());
    TransientWriter &edpSnapWriter = *m_transientWriters.back();
    m_transientReaders.emplace_back(std::make_unique<TransientReader>());
    TransientReader &edpSnapReader = *m_transientReaders.back();

    TopicData snapAttributes;
    snapAttributes.topicName[0] = '\0';
    snapAttributes.typeName[0] = '\0';
    snapAttributes.reliabilityKind = ReliabilityKind_t::BEST_EFFORT;
    snapAttributes.durabilityKind = DurabilityKind_t::VOLATILE;
    snapAttributes.endpointGuid.prefix = part.m_guidPrefix;
    snapAttributes.unicastLocator = getBuiltInUnicastLocator(part.m_participantId);

    snapAttributes.endpointGuid.entityId = ENTITYID_P2P_BUILTIN_SNAP_WRITER;
    edpSnapWriter.init(snapAttributes, TopicKind_t::NO_KEY, &m_threadPool,
                         m_transport);

    snapAttributes.endpointGuid.entityId = ENTITYID_P2P_BUILTIN_SNAP_READER;
    edpSnapReader.init(snapAttributes);

    endpoints.edpSnapWriter = &edpSnapWriter;
    endpoints.edpSnapReader = &edpSnapReader;
  }

  part.addBuiltInEndpoints(endpoints, m_featureQos.discoveryMode);
}

void Domain::registerPort(const Participant &part) {
  m_transport.createUdpConnection(getUserUnicastPort(part.m_participantId));
  m_transport.createUdpConnection(getBuiltInUnicastPort(part.m_participantId));
}

void Domain::registerMulticastPort(Locator mcastLocator) {
  if (mcastLocator.kind == LocatorKind_t::LOCATOR_KIND_UDPv4) {
    m_transport.createUdpConnection(mcastLocator.getLocatorPort(), /*reuseAddr=*/true);
  }
}

rtps::Reader *Domain::readerExists(Participant &/*part*/, const char *topicName,
                                   const char *typeName, bool reliable) {
  if (reliable) {
    for (unsigned int i = 0; i < m_statefulReaders.size(); i++) {
      if (m_statefulReaders[i]->isInitialized()) {
        if (strncmp(m_statefulReaders[i]->m_attributes.topicName, topicName,
                    Config::MAX_TYPENAME_LENGTH) != 0) {
          continue;
        }

        if (strncmp(m_statefulReaders[i]->m_attributes.typeName, typeName,
                    Config::MAX_TYPENAME_LENGTH) != 0) {
          continue;
        }

        DOMAIN_LOG("StatefulReader exists already [%s, %s]\n", topicName,
                   typeName);

        return m_statefulReaders[i].get();
      }
    }
  } else {
    for (unsigned int i = 0; i < m_statelessReaders.size(); i++) {
      if (m_statelessReaders[i]->isInitialized()) {
        if (strncmp(m_statelessReaders[i]->m_attributes.topicName, topicName,
                    Config::MAX_TYPENAME_LENGTH) != 0) {
          continue;
        }

        if (strncmp(m_statelessReaders[i]->m_attributes.typeName, typeName,
                    Config::MAX_TYPENAME_LENGTH) != 0) {
          continue;
        }

        DOMAIN_LOG("StatelessReader exists [%s, %s]\n", topicName, typeName);

        return m_statelessReaders[i].get();
      }
    }
  }

  return nullptr;
}

rtps::Writer *Domain::writerExists(Participant &/*part*/, const char *topicName,
                                   const char *typeName, bool reliable) {
  if (reliable) {
    for (unsigned int i = 0; i < m_statefulWriters.size(); i++) {
      if (m_statefulWriters[i]->isInitialized()) {
        if (strncmp(m_statefulWriters[i]->m_attributes.topicName, topicName,
                    Config::MAX_TYPENAME_LENGTH) != 0) {
          continue;
        }

        if (strncmp(m_statefulWriters[i]->m_attributes.typeName, typeName,
                    Config::MAX_TYPENAME_LENGTH) != 0) {
          continue;
        }

        DOMAIN_LOG("StatefulWriter exists [%s, %s]\n", topicName, typeName);

        return m_statefulWriters[i].get();
      }
    }
  } else {
    for (unsigned int i = 0; i < m_statelessWriters.size(); i++) {
      if (m_statelessWriters[i]->isInitialized()) {
        if (strncmp(m_statelessWriters[i]->m_attributes.topicName, topicName,
                    Config::MAX_TYPENAME_LENGTH) != 0) {
          continue;
        }

        if (strncmp(m_statelessWriters[i]->m_attributes.typeName, typeName,
                    Config::MAX_TYPENAME_LENGTH) != 0) {
          continue;
        }

        DOMAIN_LOG("StatelessWriter exists [%s, %s]\n", topicName, typeName);

        return m_statelessWriters[i].get();
      }
    }
  }

  return nullptr;
}


// from lwip
#define ip4_addr_ismulticast_impl(addr1) (((addr1)->addr & 0x000000f0UL) == 0x000000e0UL)

rtps::Writer *Domain::createWriter(Participant &part, const char *topicName,
                                   const char *typeName, bool reliable,
                                   bool enforceUnicast) {

  // TODO Distinguish WithKey and NoKey, also changes EntityKind
  TopicData attributes;

  if (strlen(topicName) >= Config::MAX_TOPICNAME_LENGTH ||
      strlen(typeName) >= Config::MAX_TYPENAME_LENGTH) {
    return nullptr;
  }
  strcpy(attributes.topicName, topicName);
  strcpy(attributes.typeName, typeName);
  attributes.endpointGuid.prefix = part.m_guidPrefix;
  attributes.endpointGuid.entityId = {
      part.getNextUserEntityKey(),
      EntityKind_t::USER_DEFINED_WRITER_WITHOUT_KEY};
  attributes.unicastLocator = getUserUnicastLocator(part.m_participantId);
  attributes.durabilityKind = DurabilityKind_t::TRANSIENT_LOCAL;

  DOMAIN_LOG("Creating writer[%s, %s]\n", topicName, typeName);

  if (reliable) {
    attributes.reliabilityKind = ReliabilityKind_t::RELIABLE;

    m_statefulWriters.emplace_back(std::unique_ptr<StatefulWriter>(new StatefulWriter()));
    StatefulWriter* writer = m_statefulWriters.back().get();
    writer->init(attributes, TopicKind_t::NO_KEY, &m_threadPool, m_transport,
                enforceUnicast, m_featureQos);

    if (!part.addWriter(writer)) {
      return nullptr;
    }
    return writer;
  } else {

    attributes.reliabilityKind = ReliabilityKind_t::BEST_EFFORT;

    m_statelessWriters.emplace_back(std::unique_ptr<StatelessWriter>(new StatelessWriter()));
    StatelessWriter* writer = m_statelessWriters.back().get();
    writer->init(attributes, TopicKind_t::NO_KEY, &m_threadPool, m_transport,
                enforceUnicast, m_featureQos);

    if (!part.addWriter(writer)) {
      return nullptr;
    }

    return writer;
  }
}


// from lwip
#define ip4_addr_ismulticast_impl(addr1) (((addr1)->addr & 0x000000f0UL) == 0x000000e0UL)

rtps::Reader *Domain::createReader(Participant &part, const char *topicName,
                                   const char *typeName, bool reliable,
                                   ip4_struct_t mcastaddress) {
 
  // TODO Distinguish WithKey and NoKey, also changes EntityKind
  TopicData attributes;

  if (strlen(topicName) >= Config::MAX_TOPICNAME_LENGTH ||
      strlen(typeName) >= Config::MAX_TYPENAME_LENGTH) {
    return nullptr;
  }
  strcpy(attributes.topicName, topicName);
  strcpy(attributes.typeName, typeName);
  attributes.endpointGuid.prefix = part.m_guidPrefix;
  attributes.endpointGuid.entityId = {
      part.getNextUserEntityKey(),
      EntityKind_t::USER_DEFINED_READER_WITHOUT_KEY};
  attributes.unicastLocator = getUserUnicastLocator(part.m_participantId);
  if (!isZeroAddress(mcastaddress)) {
    if (ip4_addr_ismulticast_impl(&mcastaddress)) {
    uint8_t *mcastaddress_arr = reinterpret_cast<uint8_t*>(&mcastaddress.addr);
      attributes.multicastLocator = rtps::Locator::createUDPv4Locator(
          mcastaddress_arr[0], mcastaddress_arr[1],
          mcastaddress_arr[2], mcastaddress_arr[3],
          getUserMulticastPort());
      m_transport.joinMultiCastGroup(
          attributes.multicastLocator.getIp4Address());
      registerMulticastPort(attributes.multicastLocator);

      DOMAIN_LOG("Multicast enabled!\n");

    } else {

      DOMAIN_LOG("This is not a Multicastaddress!\n");
    }
  }
  attributes.durabilityKind = DurabilityKind_t::VOLATILE;

  DOMAIN_LOG("Creating reader[%s, %s]\n", topicName, typeName);

  if (reliable) {
    attributes.reliabilityKind = ReliabilityKind_t::RELIABLE;

    m_statefulReaders.emplace_back(std::unique_ptr<StatefulReader>(new StatefulReader()));
    StatefulReader &reader = *m_statefulReaders.back();
    reader.init(attributes, m_transport, &m_threadPool, m_featureQos);

    if (!part.addReader(&reader)) {
      return nullptr;
    }
    return &reader;
  } else {

    attributes.reliabilityKind = ReliabilityKind_t::BEST_EFFORT;

    m_statelessReaders.emplace_back(std::unique_ptr<StatelessReader>(new StatelessReader()));
    StatelessReader &reader = *m_statelessReaders.back();
    reader.init(attributes, m_featureQos);

    if (!part.addReader(&reader)) {
      return nullptr;
    }
    return &reader;
  }
}

bool Domain::removeWriter(Participant &part, Writer *writer) {
  if (writer == nullptr) {
    return false;
  }
  // detach first so the dispose goes out while the object is still alive
  if (!part.removeWriter(writer)) {
    return false;
  }
  m_threadPool.removeWorkload(writer);
  return reclaimWriter(writer);
}

bool Domain::removeReader(Participant &part, Reader *reader) {
  if (reader == nullptr) {
    return false;
  }
  if (!part.removeReader(reader)) {
    return false;
  }
  return reclaimReader(reader);
}

bool Domain::reclaimWriter(Writer *writer) {
  
  auto eraseFrom = [&](auto &vec) {
    for (auto it = vec.begin(); it != vec.end(); ++it) {
      if (it->get() == writer) {
        vec.erase(it);
        return true;
      }
    }
    return false;
  };
  
  return eraseFrom(m_statefulWriters) || eraseFrom(m_statelessWriters) || eraseFrom(m_transientWriters);
}

bool Domain::reclaimReader(Reader *reader) {
  auto eraseFrom = [&](auto &vec) {
    for (auto it = vec.begin(); it != vec.end(); ++it) {
      if (it->get() == reader) {
        vec.erase(it);
        return true;
      }
    }
    return false;
  };
  return eraseFrom(m_statefulReaders) || eraseFrom(m_statelessReaders) ||
         eraseFrom(m_transientReaders);
}

rtps::GuidPrefix_t Domain::generateGuidPrefix(ParticipantId_t id) const {
  GuidPrefix_t prefix = Config::BASE_GUID_PREFIX;

  DefaultDriver::updateIP();
  const auto ip = DefaultDriver::m_ip;
  uint32_t host_ip_mix = (static_cast<uint32_t>(ip[0]) << 24) |
                         (static_cast<uint32_t>(ip[1]) << 16) |
                         (static_cast<uint32_t>(ip[2]) << 8) |
                         static_cast<uint32_t>(ip[3]);

  uint32_t pid_mix = 0;
#if defined(unix) || defined(__unix__)
  pid_mix = static_cast<uint32_t>(getpid());
#else
  pid_mix = static_cast<uint32_t>(xTaskGetTickCount());
#endif

  const uint64_t now_mix = static_cast<uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());

  uint32_t rd_mix = 0;
  try {
    std::random_device rd;
    rd_mix = rd();
  } catch (...) {
    rd_mix = 0;
  }

  uint64_t seed = now_mix ^
                  (static_cast<uint64_t>(host_ip_mix) << 32) ^
                  (static_cast<uint64_t>(pid_mix) << 16) ^
                  static_cast<uint64_t>(rd_mix);

  std::mt19937_64 rng(seed);
  for (auto i = 0u; i < prefix.id.size(); i++) {
    prefix.id[i] = static_cast<uint8_t>(rng() & 0xFFu);
  }

  // keep participant id in the last byte to preserve existing routing assumptions
  prefix.id[prefix.id.size() - 1] = static_cast<uint8_t>(id);
  return prefix;
}
