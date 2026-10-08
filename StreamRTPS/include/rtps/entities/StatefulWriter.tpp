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

#include "rtps/utils/Lock.h"
#include "rtps/entities/StatefulWriter.h"
#include "rtps/messages/MessageFactory.h"
#include "rtps/utils/Log.h"
#include "trace_control.h"
#include <chrono>
#include <cstring>
#include <stdio.h>
#include <thread>

using rtps::StatefulWriterT;

#if SFW_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define SFW_LOG(...) RTPS_LOG("StatefulWriter", SFW_VERBOSE, __VA_ARGS__)
#else
#define SFW_LOG(...) RTPS_TRACE_LOG_EMIT("StatefulWriter", __VA_ARGS__)
#endif

template <class NetworkDriver>
bool StatefulWriterT<NetworkDriver>::init(TopicData attributes,
                                          TopicKind_t topicKind,
                                          ThreadPool *threadPool,
                                          NetworkDriver &driver,
                                          bool enfUnicast,
                                          const FeatureQOS &qos) {

  m_transport = &driver;

  m_attributes = attributes;

  m_heartbeatPolicy = makeHeartbeatPolicy(qos.heartbeatPolicy, this);

  m_topicKind = topicKind;
  m_packetInfo.srcPort = attributes.unicastLocator.port;
  m_enforceUnicast = enfUnicast;
  mp_threadPool = threadPool;

  // heartbeats run as timed work on the shared pool, kick off the first tick now
  if (mp_threadPool != nullptr) {
    mp_threadPool->addTimedWorkload(this, rtps::timeNowMs());
  }

  m_is_initialized_ = true;
  return true;
}

template <class NetworkDriver>
bool StatefulWriterT<NetworkDriver>::addNewMatchedReader(
    const ReaderProxy &newProxy) {
#if SFW_VERBOSE && RTPS_GLOBAL_VERBOSE
  SFW_LOG("New reader added topic=%s reader_key0=%u", this->m_attributes.topicName,
          newProxy.remoteReaderGuid.entityId.entityKey[0]);
#endif

  bool duplicate = false;
  bool locatorRefreshed = false;
  {
    Lock l(memPoolMutex);


    for (auto &proxy : m_proxies) {
      if (proxy.remoteReaderGuid == newProxy.remoteReaderGuid) {
        duplicate = true;
        // refresh locator, the first announcement may have carried a bad one
        if (newProxy.remoteLocator.isValid() &&
            !(proxy.remoteLocator == newProxy.remoteLocator)) {
          proxy.remoteLocator = newProxy.remoteLocator;
          locatorRefreshed = true;
        }
        break;
      }
    }
    if (duplicate && !locatorRefreshed) {
      SFW_LOG("Discarding ReaderProxy topic=%s reader_key0=%u, already known",
              this->m_attributes.topicName,
              newProxy.remoteReaderGuid.entityId.entityKey[0]);
      return false;
    }
    if (!duplicate) {
      m_proxies.push_back(newProxy);
    }
  }
  if (!m_enforceUnicast) {
    manageSendOptions();
  }


  return !duplicate;
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::manageSendOptions() {

  SFW_LOG("Search for Multicast Partners!\n");

  Lock l(memPoolMutex);

  for (auto &proxy : m_proxies) {
    proxy.suppressUnicast = false;
    proxy.useMulticast = false;
    proxy.unknown_eid = false;
  }

  for (auto &proxy : m_proxies) {
    if (proxy.remoteMulticastLocator.kind ==
        LocatorKind_t::LOCATOR_KIND_INVALID) {
      proxy.suppressUnicast = false;
      proxy.useMulticast = false;
    } else {
      bool found = false;
      for (auto &avproxy : m_proxies) {
        if (avproxy.remoteMulticastLocator.kind ==
                LocatorKind_t::LOCATOR_KIND_UDPv4 &&
            avproxy.remoteMulticastLocator.getIp4Address().addr ==
                proxy.remoteMulticastLocator.getIp4Address().addr &&
            avproxy.remoteLocator.getIp4Address().addr !=
                proxy.remoteLocator.getIp4Address().addr) {
          if (avproxy.suppressUnicast == false) {
            avproxy.useMulticast = false;
            avproxy.suppressUnicast = true;
            proxy.useMulticast = true;
            proxy.suppressUnicast = true;

            SFW_LOG("Found Multicast Partner!\n");

            if (avproxy.remoteReaderGuid.entityId !=
                proxy.remoteReaderGuid.entityId) {
              proxy.unknown_eid = true;

              SFW_LOG("Found different EntityIds, using UNKNOWN_ENTITYID\n");
            }
          }
          found = true;
        }
      }
      if (!found) {
        proxy.useMulticast = false;
        proxy.suppressUnicast = false;
      }
    }
  }
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::resetSendOptions() {
  {
    Lock l(memPoolMutex);
    for (auto &proxy : m_proxies) {
      proxy.suppressUnicast = false;
      proxy.useMulticast = false;
      proxy.unknown_eid = false;
    }
  }
  manageSendOptions();
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::removeReader(const Guid_t &guid) {
  {
    Lock l(memPoolMutex);
    for (auto it = m_proxies.begin(); it != m_proxies.end(); ++it) {
      if (it->remoteReaderGuid == guid) {
        m_proxies.erase(it);
        break;
      }
    }
  }
  resetSendOptions();
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::removeReaderOfParticipant(
    const GuidPrefix_t &guidPrefix) {
  {
    Lock l(memPoolMutex);
    for (auto it = m_proxies.begin(); it != m_proxies.end(); ++it) {
      if (it->remoteReaderGuid.prefix == guidPrefix) {
        m_proxies.erase(it);
        break;
      }
    }
  }
  resetSendOptions();
}

template <class NetworkDriver>
const rtps::CacheChange *StatefulWriterT<NetworkDriver>::newChange(
    ChangeKind_t kind, const uint8_t *data, DataSize_t size) {

  if (isIrrelevant(kind)) {
    SFW_LOG("newChange ignored by kind");
    return nullptr;
  }

  Lock lock{m_mutex};

  if (m_history.isFull()) {
    SFW_LOG("history full");
    SequenceNumber_t newMin = ++SequenceNumber_t(m_history.getSeqNumMin());
    if (m_nextSequenceNumberToSend < newMin) {
      m_nextSequenceNumberToSend = newMin; // Make sure we have the correct sn to send
    }
  }

  SFW_LOG("newChange with size=%u SNlow=(%i,%u) SNhigh=(%i,%u)\n", static_cast<unsigned>(size), m_history.getSeqNumMin().high,
                                                                  m_history.getSeqNumMin().low, m_history.getSeqNumMax().high,
                                                                  m_history.getSeqNumMax().low);

 
  auto *result = m_history.addChange(data, size);

  if (m_heartbeatPolicy != nullptr) {
    m_heartbeatPolicy->onNewChange();
  }

  if (mp_threadPool != nullptr) {
    mp_threadPool->addWorkload(this);
  }

  return result;
}

template <class NetworkDriver> void StatefulWriterT<NetworkDriver>::progress() {

  RTPS_TRACE_ALL_EVENT(writer_progress_start, "StatefulWriter",
                       static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
                       0);

  // Send all unsend queued samples
  for (;;) {
    Lock l(memPoolMutex);

    if (m_proxies.size() == 0) {
      SFW_LOG("No proxies, not advancing SN\n");
      break;
    }
    {
      Lock lock{m_mutex};
      if (!m_history.isEmpty()) {
        const SequenceNumber_t histMin = m_history.getSeqNumMin();
        if (m_nextSequenceNumberToSend < histMin) {
          m_nextSequenceNumberToSend = histMin;
        }
      }
    }

    SequenceNumber_t snToSend;
    {
      Lock lock{m_mutex};
      if (m_history.isEmpty() ||
          m_nextSequenceNumberToSend > m_history.getSeqNumMax()) {
        break;
      }
      snToSend = m_nextSequenceNumberToSend;
    }

    bool anySent = false;
    for (const auto &proxy : m_proxies) {
      bool success = false;

      SFW_LOG("progress SN=(%i,%u) %u pkey=%u%u%u remote=%u:%u\n",
              snToSend.high, snToSend.low,
              m_proxies.size(),
              proxy.remoteReaderGuid.entityId.entityKey[0],
              proxy.remoteReaderGuid.entityId.entityKey[1],
              proxy.remoteReaderGuid.entityId.entityKey[2],
              proxy.remoteLocator.address[4],
              proxy.remoteLocator.port);

      if (!m_enforceUnicast) {
        RTPS_TRACE_PERF_EVENT(
            writer_send_to_proxy,
            0,
            static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
            static_cast<uint32_t>(proxy.remoteReaderGuid.entityId.entityKey[0]),
            0,
            0,
            static_cast<uint32_t>(snToSend.high),
            static_cast<uint32_t>(snToSend.low),
            proxy.useMulticast ? 1 : 0,
            0);
        success = sendDataWRMulticast(proxy, snToSend);
      } else {
        RTPS_TRACE_PERF_EVENT(
            writer_send_to_proxy,
            0,
            static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
            static_cast<uint32_t>(proxy.remoteReaderGuid.entityId.entityKey[0]),
            0,
            0,
            static_cast<uint32_t>(snToSend.high),
            static_cast<uint32_t>(snToSend.low),
            0,
            0);
        success = sendData(proxy, snToSend);
      }

      SFW_LOG("progress result=%s proxy_key0=%u\n",
              success ? "ok" : "fail",
              proxy.remoteReaderGuid.entityId.entityKey[0]);
      if (success) {
        anySent = true;
      }
    }

    if (!anySent) {
      break;
    }

    bool moreToSend;
    {
      Lock lock{m_mutex};
      ++m_nextSequenceNumberToSend;
      moreToSend = (m_nextSequenceNumberToSend <= m_history.getSeqNumMax());
    }

    if (!moreToSend) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::microseconds{50});
  }

  RTPS_TRACE_ALL_EVENT(writer_progress_end, "StatefulWriter",
                       static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]));
}

template <typename NetworkDriver>
bool StatefulWriterT<NetworkDriver>::isIrrelevant(ChangeKind_t kind) const {
  // Right now we only allow alive changes
  return kind != ChangeKind_t::ALIVE;
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::setAllChangesToUnsent() {
  Lock lock{m_mutex};

  const SequenceNumber_t minSN = m_history.getSeqNumMin();
  if (minSN != SEQUENCENUMBER_UNKNOWN) {
    m_nextSequenceNumberToSend = minSN;
  }
  m_history.resetMaxSent();

  if (mp_threadPool != nullptr) {
    mp_threadPool->addWorkload(this);
  }
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::removeAllChanges() {
  Lock lock{m_mutex};
  while (!m_history.isEmpty()) {
    m_history.dropOldest();
  }
  m_nextSequenceNumberToSend = m_history.getSeqNumMin();
  m_history.resetMaxSent();
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::onNewAckNack(
    const SubmessageAckNack &msg, const GuidPrefix_t &sourceGuidPrefix) {
  ReaderProxy *reader = nullptr;
  Lock l(memPoolMutex);
  
  for (auto &proxy : m_proxies) {
    if (proxy.remoteReaderGuid.prefix == sourceGuidPrefix &&
        proxy.remoteReaderGuid.entityId == msg.readerId) {
      reader = &proxy;
      break;
    }
  }

  if (reader == nullptr) {
#if SFW_VERBOSE && RTPS_GLOBAL_VERBOSE
    SFW_LOG("No proxy found with id: ");
    printEntityId(msg.readerId);
    SFW_LOG(" Dropping acknack.\n");
#endif
    RTPS_TRACE_ALL_EVENT(writer_acknack_dropped,
        static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
        0,
        static_cast<uint32_t>(msg.readerId.entityKey[0]));
    return;
  }

  if (msg.count.value <= reader->ackNackCount.value) {
    SFW_LOG("Count too small. %l <= %l Dropping acknack.\n", msg.count.value, reader->ackNackCount.value);
    RTPS_TRACE_ALL_EVENT(writer_acknack_dropped,
        static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
        1,
        static_cast<uint32_t>(msg.readerId.entityKey[0]));
    return;
  }

  reader->ackNackCount = msg.count;

  SequenceNumber_t nextSN = msg.readerSNState.base;

  if (nextSN.low == 0 && nextSN.high == 0) {
    SFW_LOG("Received preemptive acknack. Ignored.\n");
    RTPS_TRACE_ALL_EVENT(writer_acknack_dropped,
        static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
        2,
        static_cast<uint32_t>(msg.readerId.entityKey[0]));
    return;
  }

  // Send missing packets
  RTPS_TRACE_PERF_EVENT(writer_recv_acknack,
      static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
      static_cast<uint32_t>(msg.readerId.entityKey[0]),
      static_cast<uint32_t>(msg.count.value),
      static_cast<uint32_t>(msg.readerSNState.base.low),
      static_cast<uint32_t>(msg.readerSNState.numBits));

  SFW_LOG("Received non-preemptive acknack in range.\n");

  // Actual retransmission path for loss
  bool hadNacks = false;
  bool hadUnservable = false;
  for (uint32_t i = 0; i < msg.readerSNState.numBits; ++i, ++nextSN) {
    if (msg.readerSNState.isSet(i)) {
      hadNacks = true;

      SFW_LOG("Send Packet on NACK for SN=(%u,%u) for pkey=%u%u%u.\n", 
                                                nextSN.high, nextSN.low,
                                                reader->remoteReaderGuid.entityId.entityKey[0],
                                                reader->remoteReaderGuid.entityId.entityKey[1],
                                                reader->remoteReaderGuid.entityId.entityKey[2]);

      bool success = sendData(*reader, nextSN);
      if (!success) {
        hadUnservable = true;
      }

      RTPS_TRACE_PERF_EVENT(
          writer_send_to_proxy,
          0, // no per sample eventId available on StatefulWriter
          static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
          static_cast<uint32_t>(reader->remoteReaderGuid.entityId.entityKey[0]),
          0, // guid_prefix_high not tracked here
          0, // guid_prefix_low not tracked here
          static_cast<uint32_t>(nextSN.high),
          static_cast<uint32_t>(nextSN.low),
          0,
          success ? 1 : 0);
    }
  }

  if (hadUnservable) {
    // NACKed SN already evicted, advertise the current floor so the reader advances instead of nacking forever, inline since sendHeartBeat would retake memPoolMutex
    SequenceNumber_t firstSN;
    SequenceNumber_t lastSN;
    {
      Lock lock{m_mutex};
      firstSN = m_history.getSeqNumMin();
      lastSN = m_history.getSeqNumMaxSent();
    }
    if (!(firstSN == SEQUENCENUMBER_UNKNOWN) && !(lastSN == SEQUENCENUMBER_UNKNOWN)) {
      PacketInfo info;
      info.srcPort = m_packetInfo.srcPort;
      info.destAddr = reader->remoteLocator.getIp4Address();
      info.destPort = static_cast<Ip4Port_t>(reader->remoteLocator.port);
      MessageFactory::addHeader(info.buffer, m_attributes.endpointGuid.prefix);
      MessageFactory::addHeartbeat(info.buffer, m_attributes.endpointGuid.entityId,
                                   reader->remoteReaderGuid.entityId, firstSN,
                                   lastSN, m_hbCount);
      m_hbCount.value++;
      m_transport->sendPacket(info);
    }
  }

  if (m_heartbeatPolicy != nullptr) {
    bool allAcknowledged = !hadNacks && (msg.readerSNState.base > m_history.getSeqNumMax());
    m_heartbeatPolicy->onAckNack(hadNacks, allAcknowledged);
  }
}


template <class NetworkDriver>
bool StatefulWriterT<NetworkDriver>::sendData(
    const ReaderProxy &reader, const SequenceNumber_t &snMissing) {

  PacketInfo info;
  info.srcPort = m_packetInfo.srcPort;

  const Locator &locator = reader.remoteLocator;
  info.destAddr = locator.getIp4Address();
  info.destPort = (Ip4Port_t)locator.port;

  SFW_LOG("sendData SN=(%i,%u) port=%u\n", snMissing.high, snMissing.low,
          static_cast<unsigned>(info.destPort));

  MessageFactory::addHeader(info.buffer, m_attributes.endpointGuid.prefix);
  MessageFactory::addSubMessageTimeStamp(info.buffer);
  {
    Lock lock{m_mutex};
    const CacheChange *next = m_history.getChangeBySN(snMissing);
    if (next == nullptr) {
      SFW_LOG("sendData: no CacheChange for SN (%i,%u)\n", snMissing.high,
              snMissing.low);
      return false;
    }
    MessageFactory::addSubMessageData(
        info.buffer, next->data, false, next->sequenceNumber,
        m_attributes.endpointGuid.entityId, reader.remoteReaderGuid.entityId);
  }
  m_transport->sendPacket(info);
  {
    Lock lock{m_mutex};
    m_history.markAsSent(snMissing);
  }
  return true;
}

template <class NetworkDriver>
bool StatefulWriterT<NetworkDriver>::sendDataWRMulticast(
    const ReaderProxy &reader, const SequenceNumber_t &snMissing) {

  RTPS_TRACE_PERF_EVENT(writer_suppression_decision,
      static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
      static_cast<uint32_t>(reader.remoteReaderGuid.entityId.entityKey[0]),
      reader.suppressUnicast ? 1 : 0,
      reader.useMulticast ? 1 : 0,
      static_cast<uint32_t>(reader.remoteLocator.getIp4Address().addr));

  if (reader.suppressUnicast && !reader.useMulticast) {
    SFW_LOG("sendDataWRMulticast SN=(%i,%u) skipped (suppress unicast, no multicast)\n",
            snMissing.high, snMissing.low);
  }

  if (reader.useMulticast || reader.suppressUnicast == false) {
    PacketInfo info;
    info.srcPort = m_packetInfo.srcPort;

    SFW_LOG("sendDataWRMulticast SN=(%i,%u) %s\n", snMissing.high, snMissing.low,
            reader.useMulticast ? "multicast" : "unicast");

    if (reader.useMulticast) {
      const Locator &locator = reader.remoteMulticastLocator;
      info.destAddr = locator.getIp4Address();
      info.destPort = (Ip4Port_t)locator.port;
    } else {
      const Locator &locator = reader.remoteLocator;
      info.destAddr = locator.getIp4Address();
      info.destPort = (Ip4Port_t)locator.port;
    }

    MessageFactory::addHeader(info.buffer, m_attributes.endpointGuid.prefix);
    MessageFactory::addSubMessageTimeStamp(info.buffer);
    {
      Lock lock{m_mutex};
      const CacheChange *next = m_history.getChangeBySN(snMissing);
      if (next == nullptr) {
        SFW_LOG("sendDataWRMulticast: no CacheChange for SN (%i,%u)\n",
                snMissing.high, snMissing.low);
        return false;
      }

      EntityId_t reid;
      if (reader.useMulticast) {
        reid = ENTITYID_UNKNOWN;
      } else {
        reid = reader.remoteReaderGuid.entityId;
      }

      MessageFactory::addSubMessageData(
          info.buffer, next->data, false, next->sequenceNumber,
          m_attributes.endpointGuid.entityId, reid);
    }

    m_transport->sendPacket(info);
    {
      Lock lock{m_mutex};
      m_history.markAsSent(snMissing);
    }
  }
  return true;
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::onTimer() {
  if (!m_running) {
    return;
  }
  uint32_t sleepMs = 5;
  if (m_heartbeatPolicy != nullptr) {
    const uint32_t prevPeriod = m_heartbeatPolicy->getCurrentPeriodMs();

    if (m_heartbeatPolicy->shouldSend()) {
      if (sendHeartBeat()) {
        m_heartbeatPolicy->onHeartbeatTransmitted();

        const uint32_t curPeriod = m_heartbeatPolicy->getCurrentPeriodMs();
        RTPS_TRACE_ALL_EVENT(heartbeat_loop_iteration,
            static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
            curPeriod,
            0, 0, 0);
      }
    }

    const uint32_t curPeriod = m_heartbeatPolicy->getCurrentPeriodMs();
    if (curPeriod != prevPeriod) {
      RTPS_TRACE_ALL_EVENT(heartbeat_period_update,
          static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
          0,
          curPeriod,
          0);
    }

    sleepMs = m_heartbeatPolicy->getRecommendedSleepMs();
  }
  // rearm the next tick on the shared timed writer pool
  if (mp_threadPool != nullptr) {
    mp_threadPool->addTimedWorkload(this, rtps::timeNowMs() + sleepMs);
  }
}

template <class NetworkDriver>
bool StatefulWriterT<NetworkDriver>::sendHeartBeat() {
  Lock l(memPoolMutex);
  if (m_proxies.empty()) {
    return false;
  }

  SequenceNumber_t firstSN;
  SequenceNumber_t lastSN;
  {
    Lock lock{m_mutex};
    firstSN = m_history.getSeqNumMin();
    lastSN = m_history.getSeqNumMaxSent(); // Must be the last SN actually sent to prevent MaxSN vs HB race and spurious retransmission
  }
  if (firstSN == SEQUENCENUMBER_UNKNOWN || lastSN == SEQUENCENUMBER_UNKNOWN) {
    if (strlen(&this->m_attributes.typeName[0]) != 0) {
      SFW_LOG("Skipping heartbeat. No data.\n");
    }
    return false;
  }

  bool anySent = false;
  for (auto &proxy : m_proxies) {

    PacketInfo info;
    info.srcPort = m_packetInfo.srcPort;
    info.destAddr = proxy.remoteLocator.getIp4Address();
    info.destPort = static_cast<Ip4Port_t>(proxy.remoteLocator.port);

    MessageFactory::addHeader(info.buffer, m_attributes.endpointGuid.prefix);

    MessageFactory::addHeartbeat(
        info.buffer, m_attributes.endpointGuid.entityId,
        proxy.remoteReaderGuid.entityId, firstSN, lastSN, m_hbCount);

    RTPS_TRACE_PERF_EVENT(heartbeat_send,
        static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
        static_cast<uint32_t>(firstSN.low),
        static_cast<uint32_t>(lastSN.low),
        0);

    m_transport->sendPacket(info);
    anySent = true;
  }
  if (anySent) {
    m_hbCount.value++;
  }
  return anySent;
}


template <class NetworkDriver>
bool StatefulWriterT<NetworkDriver>::hasLocator(const Locator &loc) {
  Lock l(memPoolMutex);
  for (const auto &proxy : m_proxies) {
    if (proxy.remoteLocator.kind == loc.kind &&
        proxy.remoteLocator.port == loc.port &&
        proxy.remoteLocator.address == loc.address) {
      return true;
    }
  }
  return false;
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::setPlADeadline(uint32_t /*ms*/) {}

#include "rtps/entities/transient/HeartbeatPolicy.tpp"

template <class NetworkDriver>
StatefulWriterT<NetworkDriver>::~StatefulWriterT() {
  m_running = false;
  if (mp_threadPool != nullptr) {
    mp_threadPool->removeTimedWorkload(this);
  }
}

#undef SFW_VERBOSE
