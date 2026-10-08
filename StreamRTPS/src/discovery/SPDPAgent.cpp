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

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <chrono>

#include "rtps/discovery/SPDPAgent.h"
#include "rtps/discovery/SPDPBurstSchedule.h"
#include "rtps/discovery/ParticipantProxyData.h"
#include "rtps/discovery/EDPAgentBase.h"
#include "rtps/entities/Participant.h"
#include "rtps/entities/Reader.h"
#include "rtps/entities/Writer.h"
#include "rtps/messages/MessageTypes.h"
#include "rtps/utils/Log.h"
#include "rtps/utils/udpUtils.h"
#include "rtps/utils/sysFunctions.h"
#include "trace_control.h"

using rtps::SPDPAgent;
using rtps::SMElement::BuildInEndpointSet;
using rtps::SMElement::ParameterId;

#if SPDP_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define SPDP_LOG(...) RTPS_LOG("SPDP", SPDP_VERBOSE, __VA_ARGS__)
#else
#define SPDP_LOG(...) RTPS_TRACE_LOG_EMIT("SPDP", __VA_ARGS__)
#endif

namespace {
using rtps::SPDPBroadcastPhase;
using rtps::getSPDPPhaseForElapsedMs;

const char *phaseToString(SPDPBroadcastPhase phase) {
  switch (phase) {
  case SPDPBroadcastPhase::BURST_PHASE_1:
    return "SPDP burst phase 1";
  case SPDPBroadcastPhase::BURST_PHASE_2:
    return "SPDP burst phase 2";
  case SPDPBroadcastPhase::BURST_PHASE_3:
    return "SPDP burst phase 3";
  case SPDPBroadcastPhase::BURST_PHASE_4:
    return "SPDP burst phase 4";
  case SPDPBroadcastPhase::STEADY:
    return "SPDP steady phase";
  }
  return "SPDP unknown phase";
}
} // namespace

SPDPAgent::~SPDPAgent() {
  // join before releasing the mutex the broadcast thread locks every round
  stop();
  if (initialized) {
    mutex_free(&m_mutex);
  }
}

void SPDPAgent::init(Participant &participant, BuiltInEndpoints &endpoints) {
  if (mutex_new(&m_mutex) != EMB_ERR_OK) {
    SPDP_LOG("Could not alloc mutex");
    return;
  }
  mp_participant = &participant;
  m_buildInEndpoints = endpoints;
  m_buildInEndpoints.spdpReader->registerCallback(receiveCallback, this);

  ucdr_init_buffer(&m_microbuffer, m_outputBuffer.data(),
                   m_outputBuffer.size());
  addParticipantParameters();
  initialized = true;
}

void SPDPAgent::start() {
  if (m_running) {
    return;
  }
  m_running = true;
  m_agentThread = rtps::createThread("SPDPThread", runBroadcast, this);
}

void SPDPAgent::stop() {
  m_running = false;
  wakeBroadcast();
  // joinThread rechecks joinable
  if (m_agentThread) {
    joinThread(m_agentThread);
  }
}

void SPDPAgent::requestImmediateResend() {
  m_immediateResend.store(true, std::memory_order_relaxed);
  wakeBroadcast();
}

void SPDPAgent::wakeBroadcast() {
  std::lock_guard<std::mutex> lock{m_resendMutex};
  m_resendSignal.notify_all();
}

void SPDPAgent::runBroadcast(void *args) {
  
  SPDPAgent &agent = *static_cast<SPDPAgent *>(args);
  
  const DataSize_t size = ucdr_buffer_length(&agent.m_microbuffer);
  const auto start_time = std::chrono::steady_clock::now();
  auto last_heartbeat_check = start_time;
  auto last_logged_phase = SPDPBroadcastPhase::STEADY;
  bool phase_logged = false;

  {
    Lock lock{agent.m_mutex};
    agent.m_buildInEndpoints.spdpWriter->newChange(
        ChangeKind_t::ALIVE, agent.m_microbuffer.init,
        ucdr_buffer_length(&agent.m_microbuffer));
  }

  while (agent.m_running) {
    const auto now = std::chrono::steady_clock::now();
    const uint64_t elapsed_ms = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count());

    uint16_t sleep_interval_ms = static_cast<uint16_t>(Config::SPDP_RESEND_PERIOD_MS.load(std::memory_order_relaxed));
    const auto phase = getSPDPPhaseForElapsedMs(elapsed_ms, sleep_interval_ms);
    if (!phase_logged || phase != last_logged_phase) {
      SPDP_LOG("%s", phaseToString(phase));
      last_logged_phase = phase;
      phase_logged = true;
    }

    uint64_t lh = 0, ll = 0;
    {
      const auto &id = agent.mp_participant->m_guidPrefix.id;
      for (int i = 0; i < 8; ++i) lh |= (static_cast<uint64_t>(id[i]) << (i * 8));
      for (int i = 8; i < 12; ++i) ll |= (static_cast<uint64_t>(id[i]) << ((i - 8) * 8));
    }
    const uint64_t broadcast_hash = agent.mp_participant->getSEDPAgent().getEndpointHash();
    RTPS_TRACE_ALL_EVENT(spdp_broadcast, lh, ll, static_cast<uint32_t>(size),
                         broadcast_hash);

    {
      // hold m_mutex touching the SPDP writer so a concurrent setState cannot swap changes mid broadcast and send a half rebuilt payload
      Lock lock{agent.m_mutex};
      agent.refreshPayloadIfStale();
      agent.m_buildInEndpoints.spdpWriter->setAllChangesToUnsent();
    }
    const uint64_t heartbeat_elapsed_ms = static_cast<uint64_t>( std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat_check).count());

    // Check liveliness of other participants if period demands it
    if (heartbeat_elapsed_ms >= Config::SPDP_HEARTBEAT_CHECK_PERIOD_MS) {
      last_heartbeat_check = now;
      agent.mp_participant->checkAndResetHeartbeats();
    }

    // sleep out the interval but wake early on an immediate resend or stop
    {
      std::unique_lock<std::mutex> lock{agent.m_resendMutex};
      agent.m_resendSignal.wait_for(lock,
          std::chrono::milliseconds{sleep_interval_ms}, [&agent] {
            return !agent.m_running ||
                   agent.m_immediateResend.load(std::memory_order_relaxed);
          });
    }

    agent.m_immediateResend.store(false, std::memory_order_relaxed);
    // one full burst iteration complete, gate clearing signal for SEDP
    if (agent.m_broadcastRound.load(std::memory_order_relaxed) < UINT16_MAX) {
      agent.m_broadcastRound.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

void SPDPAgent::receiveCallback(void *callee,
                                const ReaderCacheChange &cacheChange) {
  auto agent = static_cast<SPDPAgent *>(callee);
  uint64_t lh = 0, ll = 0;
  {
    const auto &id = agent->mp_participant->m_guidPrefix.id;
    for (int i = 0; i < 8; ++i) lh |= (static_cast<uint64_t>(id[i]) << (i * 8));
    for (int i = 8; i < 12; ++i) ll |= (static_cast<uint64_t>(id[i]) << ((i - 8) * 8));
  }
  // always traced, the eval needs socket arrival vs proxy processing to split causes
  RTPS_TRACE_ALL_EVENT(spdp_receive_callback, cacheChange.eventId, lh, ll,
                       static_cast<uint32_t>(cacheChange.size),
                       static_cast<int>(cacheChange.kind));
  agent->handleSPDPPackage(cacheChange);
}

void SPDPAgent::handleSPDPPackage(const ReaderCacheChange &cacheChange) {
  if (!initialized) {
    SPDP_LOG("Callback called without initialization\n");
    return;
  }

  Lock lock{m_mutex};
  if (cacheChange.size > m_inputBuffer.size()) {
    SPDP_LOG("Input buffer to small\n");
    return;
  }

  // Something went wrong deserializing remote participant
  if (!cacheChange.copyInto(m_inputBuffer.data(), m_inputBuffer.size())) {
    return;
  }

  ucdrBuffer buffer;
  ucdr_init_buffer(&buffer, m_inputBuffer.data(), m_inputBuffer.size());

  if (cacheChange.kind == ChangeKind_t::ALIVE) {
    configureEndianessAndOptions(buffer);
    volatile bool success = m_proxyDataBuffer.readFromUcdrBuffer(buffer);
    if (success) {
      // TODO In case we store the history we can free the history mutex here
      processProxyData(cacheChange.eventId);
    }
  } else {
    // TODO RemoveParticipant
  }
}

void SPDPAgent::configureEndianessAndOptions(ucdrBuffer &buffer) {
  std::array<uint8_t, 2> encapsulation{};
  ucdr_deserialize_array_uint8_t(&buffer, encapsulation.data(),
                                 encapsulation.size());
  if (encapsulation == SMElement::SCHEME_PL_CDR_LE) {
    buffer.endianness = UCDR_LITTLE_ENDIANNESS;
  } else {
    buffer.endianness = UCDR_BIG_ENDIANNESS;
  }
  ucdr_deserialize_array_uint8_t(&buffer, encapsulation.data(),
                                 encapsulation.size());
}

void SPDPAgent::processProxyData(EventId_t eventId) {
  if (m_proxyDataBuffer.m_guid.prefix.id == mp_participant->m_guidPrefix.id) {
    SPDP_LOG("SPDP: Rec msg for our own prefix guid.prefix = %u \n",(unsigned int)Guid_t::sum(m_proxyDataBuffer.m_guid));
    return; // Our own packet
  }

  const bool remoteKnown = mp_participant->hasRemoteParticipant(m_proxyDataBuffer.m_guid.prefix);
  
  // Extract GUID prefix for tracing, first 8 and last 4 bytes
  uint64_t guid_prefix_high = 0;
  uint64_t guid_prefix_low = 0;
  for (int i = 0; i < 8 && i < m_proxyDataBuffer.m_guid.prefix.id.size(); i++) {
    guid_prefix_high |= (static_cast<uint64_t>(m_proxyDataBuffer.m_guid.prefix.id[i]) << (i * 8));
  }
  for (int i = 8; i < 12 && i < m_proxyDataBuffer.m_guid.prefix.id.size(); i++) {
    guid_prefix_low |= (static_cast<uint64_t>(m_proxyDataBuffer.m_guid.prefix.id[i]) << ((i - 8) * 8));
  }

  // local participant prefix in the same split format so the trace event tells which participant ran the SPDP receive path
  uint64_t local_high = 0;
  uint64_t local_low = 0;
  for (int i = 0; i < 8 && i < mp_participant->m_guidPrefix.id.size(); i++) {
    local_high |= (static_cast<uint64_t>(mp_participant->m_guidPrefix.id[i]) << (i * 8));
  }
  for (int i = 8; i < 12 && i < mp_participant->m_guidPrefix.id.size(); i++) {
    local_low |= (static_cast<uint64_t>(mp_participant->m_guidPrefix.id[i]) << ((i - 8) * 8));
  }

  if (remoteKnown) {
    SPDP_LOG("Not adding remote participant guid.prefix = %u \n", (unsigned int)Guid_t::sum(m_proxyDataBuffer.m_guid));
    mp_participant->refreshRemoteParticipantLiveliness(m_proxyDataBuffer.m_guid.prefix);

    mp_participant->updateRemoteParticipantSnapFields(
        m_proxyDataBuffer.m_guid.prefix, m_proxyDataBuffer.m_sedpSupport,
        m_proxyDataBuffer.m_snapState, m_proxyDataBuffer.m_snapNetSize,
        m_proxyDataBuffer.m_root, m_proxyDataBuffer.m_endpointHash);

    // Inform SEDP Agent about known Part
    mp_participant->getSEDPAgent().onRemoteParticipantDiscovered(m_proxyDataBuffer.m_guid.prefix, m_proxyDataBuffer.m_snapState);

    RTPS_TRACE_ALL_EVENT(spdp_process_proxy, eventId, local_high, local_low,
                         guid_prefix_high, guid_prefix_low, 0);
    return;
  }

  if (mp_participant->addNewRemoteParticipant(m_proxyDataBuffer)) {

    addProxiesForBuiltInEndpoints();
    m_buildInEndpoints.spdpWriter->setAllChangesToUnsent();
    // gossip mode has no reliable SEDP endpoints
    if (m_buildInEndpoints.sedpPubWriter != nullptr) {
      m_buildInEndpoints.sedpPubWriter->setAllChangesToUnsent();
    }
    if (m_buildInEndpoints.sedpSubWriter != nullptr) {
      m_buildInEndpoints.sedpSubWriter->setAllChangesToUnsent();
    }
    requestImmediateResend();

    mp_participant->getSEDPAgent().onRemoteParticipantDiscovered(m_proxyDataBuffer.m_guid.prefix, m_proxyDataBuffer.m_snapState);

    RTPS_TRACE_ALL_EVENT(spdp_process_proxy, eventId, local_high, local_low,
                         guid_prefix_high, guid_prefix_low, 1);
    SPDP_LOG("Added new participant with guid_sum=%u",
             static_cast<unsigned>(Guid_t::sum(m_proxyDataBuffer.m_guid)));
  } else {
    SPDP_LOG("Failed to add new participant guid_sum=%u remote_count=%u",
             static_cast<unsigned>(Guid_t::sum(m_proxyDataBuffer.m_guid)),
             static_cast<unsigned>(mp_participant->getRemoteParticipantCount()));
    RTPS_TRACE_ALL_EVENT(spdp_process_proxy, eventId, local_high, local_low,
                         guid_prefix_high, guid_prefix_low, 0);
  }
}

void SPDPAgent::refreshPayloadIfStale() {
  if (m_buildInEndpoints.spdpWriter == nullptr) {
    return;
  }
  const uint64_t hash = mp_participant->getSEDPAgent().getEndpointHash();
  const GuidPrefix_t root = mp_participant->getSEDPAgent().getCurrentRoot();
  if (hash == m_lastAdvertisedHash && root == m_lastAdvertisedRoot) {
    return;
  }
  SPDP_LOG("refreshPayloadIfStale: rebuilding SPDP sample");
  ucdr_init_buffer(&m_microbuffer, m_outputBuffer.data(),
                   m_outputBuffer.size());
  addParticipantParameters();
  m_buildInEndpoints.spdpWriter->removeAllChanges();
  m_buildInEndpoints.spdpWriter->newChange(
      ChangeKind_t::ALIVE, m_microbuffer.init,
      ucdr_buffer_length(&m_microbuffer));
}

void SPDPAgent::setState(SPDPDiscoverState state) {
  Lock lock{m_mutex};
  if (m_localSnapState == state) {
    return;
  }
  const SPDPDiscoverState previous = m_localSnapState;
  m_localSnapState = state;
  uint64_t lh = 0, ll = 0;
  {
    const auto &id = mp_participant->m_guidPrefix.id;
    for (int i = 0; i < 8; ++i) lh |= (static_cast<uint64_t>(id[i]) << (i * 8));
    for (int i = 8; i < 12; ++i) ll |= (static_cast<uint64_t>(id[i]) << ((i - 8) * 8));
  }
  RTPS_TRACE_ALL_EVENT(spdp_state_changed, 0ull, lh, ll, static_cast<int>(previous),
                       static_cast<int>(state));
  SPDP_LOG("Local gossip state %d -> %d", static_cast<int>(previous),
           static_cast<int>(state));

  // Rebuild the SPDP payload so the next broadcast carries the updated PID_GOSSIP_STATE
  ucdr_init_buffer(&m_microbuffer, m_outputBuffer.data(),
                   m_outputBuffer.size());
  addParticipantParameters();

  if (m_buildInEndpoints.spdpWriter != nullptr) {
    m_buildInEndpoints.spdpWriter->removeAllChanges();
    m_buildInEndpoints.spdpWriter->newChange(
        ChangeKind_t::ALIVE, m_microbuffer.init,
        ucdr_buffer_length(&m_microbuffer));
  }
}

bool SPDPAgent::addProxiesForBuiltInEndpoints() {

  Locator *locator = nullptr;

  // Check if the remote participants has a locator in our subnet
  SPDP_LOG("Searching locators in list size=%u",
           static_cast<unsigned>(m_proxyDataBuffer.m_metatrafficUnicastLocatorList.size()));
  for (unsigned int i = 0;
       i < m_proxyDataBuffer.m_metatrafficUnicastLocatorList.size(); i++) {
    Locator *l = &(m_proxyDataBuffer.m_metatrafficUnicastLocatorList[i]);
    if (l->isValid() && l->isSameSubnet()) {
      locator = l;
      break;
    } else if(!l->isValid()) {
      SPDP_LOG("Locator %u not valid", i);
    } else if(!l->isSameSubnet()) {
      SPDP_LOG("Locator %u not same subnet", i);
    } else {
      SPDP_LOG("Locator %u rejected for unknown reason", i);
    }
  }

  if (!locator) {
    SPDP_LOG("addProxiesForBuiltInEndpoints failed: no usable locator");
    return false;
  }

  struct in_addr inAddr;
  inAddr.s_addr = locator->getIp4Address().addr; // might be in wrong byte order
  char *addr = inet_ntoa(inAddr);
  SPDP_LOG("Adding IPv4 Locator %s\n", addr);
  (void)addr;

  if (m_proxyDataBuffer.hasPublicationWriter() &&
      m_buildInEndpoints.sedpPubReader != nullptr) {
    const WriterProxy proxy{{m_proxyDataBuffer.m_guid.prefix,
                             ENTITYID_SEDP_BUILTIN_PUBLICATIONS_WRITER},
                            *locator};
    m_buildInEndpoints.sedpPubReader->addNewMatchedWriter(proxy);
  }

  if (m_proxyDataBuffer.hasSubscriptionWriter() &&
      m_buildInEndpoints.sedpSubReader != nullptr) {
    const WriterProxy proxy{{m_proxyDataBuffer.m_guid.prefix,
                             ENTITYID_SEDP_BUILTIN_SUBSCRIPTIONS_WRITER},
                            *locator};
    m_buildInEndpoints.sedpSubReader->addNewMatchedWriter(proxy);
  }

  mp_participant->getSEDPAgent().onAddProxiesForRemoteParticipant(
      m_proxyDataBuffer, *locator);

  return true;
}

void SPDPAgent::addInlineQos() {
  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_KEY_HASH);
  ucdr_serialize_uint16_t(&m_microbuffer, 16);
  ucdr_serialize_array_uint8_t(&m_microbuffer,
                               mp_participant->m_guidPrefix.id.data(),
                               sizeof(GuidPrefix_t::id));
  ucdr_serialize_array_uint8_t(&m_microbuffer,
                               ENTITYID_BUILD_IN_PARTICIPANT.entityKey.data(),
                               sizeof(EntityId_t::entityKey));
  ucdr_serialize_uint8_t(
      &m_microbuffer,
      static_cast<uint8_t>(ENTITYID_BUILD_IN_PARTICIPANT.entityKind));

  endCurrentList();
}

void SPDPAgent::endCurrentList() {
  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_SENTINEL);
  ucdr_serialize_uint16_t(&m_microbuffer, 0);
}

void SPDPAgent::addParticipantParameters() {
  const uint16_t zero_options = 0;
  const uint16_t protocolVersionSize =
      sizeof(PROTOCOLVERSION.major) + sizeof(PROTOCOLVERSION.minor);
  const uint16_t vendorIdSize = Config::VENDOR_ID.vendorId.size();
  const uint16_t locatorSize = sizeof(Locator);
  const uint16_t durationSize =
      sizeof(Duration_t::seconds) + sizeof(Duration_t::fraction);
  const uint16_t entityKeySize = 3;
  const uint16_t entityKindSize = 1;
  const uint16_t entityIdSize = entityKeySize + entityKindSize;
  const uint16_t guidSize = sizeof(GuidPrefix_t::id) + entityIdSize;

  const Locator userUniCastLocator =
      getUserUnicastLocator(mp_participant->m_participantId);
  const Locator builtInUniCastLocator =
      getBuiltInUnicastLocator(mp_participant->m_participantId);
  const Locator builtInMultiCastLocator = getBuiltInMulticastLocator();

  ucdr_serialize_array_uint8_t(&m_microbuffer,
                               rtps::SMElement::SCHEME_PL_CDR_LE.data(),
                               rtps::SMElement::SCHEME_PL_CDR_LE.size());
  ucdr_serialize_uint16_t(&m_microbuffer, zero_options);

  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_PROTOCOL_VERSION);
  ucdr_serialize_uint16_t(&m_microbuffer, protocolVersionSize + 2);
  ucdr_serialize_uint8_t(&m_microbuffer, PROTOCOLVERSION.major);
  ucdr_serialize_uint8_t(&m_microbuffer, PROTOCOLVERSION.minor);
  m_microbuffer.iterator += 2;      // padding
  m_microbuffer.last_data_size = 4; // to 4 byte

  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_VENDORID);
  ucdr_serialize_uint16_t(&m_microbuffer, vendorIdSize + 2);
  ucdr_serialize_array_uint8_t(&m_microbuffer,
                               Config::VENDOR_ID.vendorId.data(), vendorIdSize);
  m_microbuffer.iterator += 2;      // padding
  m_microbuffer.last_data_size = 4; // to 4 byte

  ucdr_serialize_uint16_t(&m_microbuffer,
                          ParameterId::PID_DEFAULT_UNICAST_LOCATOR);
  ucdr_serialize_uint16_t(&m_microbuffer, locatorSize);
  ucdr_serialize_array_uint8_t(
      &m_microbuffer, reinterpret_cast<const uint8_t *>(&userUniCastLocator),
      locatorSize);

  ucdr_serialize_uint16_t(&m_microbuffer,
                          ParameterId::PID_METATRAFFIC_UNICAST_LOCATOR);
  ucdr_serialize_uint16_t(&m_microbuffer, locatorSize);
  ucdr_serialize_array_uint8_t(
      &m_microbuffer, reinterpret_cast<const uint8_t *>(&builtInUniCastLocator),
      locatorSize);

  ucdr_serialize_uint16_t(&m_microbuffer,
                          ParameterId::PID_METATRAFFIC_MULTICAST_LOCATOR);
  ucdr_serialize_uint16_t(&m_microbuffer, locatorSize);
  ucdr_serialize_array_uint8_t(
      &m_microbuffer,
      reinterpret_cast<const uint8_t *>(&builtInMultiCastLocator), locatorSize);

  const uint32_t leaseMs = Config::SPDP_LEASE_DURATION_MS.load(std::memory_order_relaxed);
  ucdr_serialize_uint16_t(&m_microbuffer,
                          ParameterId::PID_PARTICIPANT_LEASE_DURATION);
  ucdr_serialize_uint16_t(&m_microbuffer, durationSize);
  ucdr_serialize_int32_t(&m_microbuffer,
                         static_cast<int32_t>(leaseMs / 1000));
  ucdr_serialize_uint32_t(&m_microbuffer,
                          (leaseMs % 1000) * 1000000u);

  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_PARTICIPANT_GUID);
  ucdr_serialize_uint16_t(&m_microbuffer, guidSize);
  ucdr_serialize_array_uint8_t(&m_microbuffer,
                               mp_participant->m_guidPrefix.id.data(),
                               sizeof(GuidPrefix_t::id));
  ucdr_serialize_array_uint8_t(&m_microbuffer,
                               ENTITYID_BUILD_IN_PARTICIPANT.entityKey.data(),
                               entityKeySize);
  ucdr_serialize_uint8_t(
      &m_microbuffer,
      static_cast<uint8_t>(ENTITYID_BUILD_IN_PARTICIPANT.entityKind));

  ucdr_serialize_uint16_t(&m_microbuffer,
                          ParameterId::PID_BUILTIN_ENDPOINT_SET);
  ucdr_serialize_uint16_t(&m_microbuffer, sizeof(BuildInEndpointSet));
  ucdr_serialize_uint32_t(
      &m_microbuffer, BuildInEndpointSet::DISC_BIE_PARTICIPANT_ANNOUNCER |
                      BuildInEndpointSet::DISC_BIE_PARTICIPANT_DETECTOR |
                      BuildInEndpointSet::DISC_BIE_PUBLICATION_ANNOUNCER |
                      BuildInEndpointSet::DISC_BIE_PUBLICATION_DETECTOR |
                      BuildInEndpointSet::DISC_BIE_SUBSCRIPTION_ANNOUNCER |
                      BuildInEndpointSet::DISC_BIE_SUBSCRIPTION_DETECTOR);

  // SEDP Gossip extension PIDs
  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_SEDP_SUPPORT);
  ucdr_serialize_uint16_t(&m_microbuffer, 4);
  ucdr_serialize_uint8_t(
      &m_microbuffer,
      static_cast<uint8_t>(mp_participant->getDiscoveryMode()));
  m_microbuffer.iterator += 3;      // pad 1 byte enum to 4 byte alignment
  m_microbuffer.last_data_size = 4;

  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_SNAP_NET_SIZE);
  ucdr_serialize_uint16_t(&m_microbuffer, sizeof(uint32_t));
  ucdr_serialize_uint32_t(&m_microbuffer,
                          mp_participant->getSEDPAgent().getJoinedPeerCounts());

  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_SNAP_STATE);
  ucdr_serialize_uint16_t(&m_microbuffer, 4);
  ucdr_serialize_uint8_t(&m_microbuffer,
                         static_cast<uint8_t>(m_localSnapState));
  m_microbuffer.iterator += 3;
  m_microbuffer.last_data_size = 4;

  const GuidPrefix_t root = mp_participant->getSEDPAgent().getCurrentRoot();
  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_ROOT_PREFIX);
  ucdr_serialize_uint16_t(&m_microbuffer, sizeof(GuidPrefix_t::id));
  ucdr_serialize_array_uint8_t(&m_microbuffer, root.id.data(),
                               sizeof(GuidPrefix_t::id));

  const uint64_t endpointHash =
      mp_participant->getSEDPAgent().getEndpointHash();
  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_ENDPOINT_HASH);
  ucdr_serialize_uint16_t(&m_microbuffer, sizeof(uint64_t));
  ucdr_serialize_uint64_t(&m_microbuffer, endpointHash);

  // remember what this sample advertises, drives refreshPayloadIfStale
  m_lastAdvertisedHash = endpointHash;
  m_lastAdvertisedRoot = root;

  endCurrentList();
}

#undef SPDP_VERBOSE
