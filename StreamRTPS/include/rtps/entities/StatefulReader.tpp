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
#include "rtps/entities/StatefulReader.h"
#include "rtps/messages/MessageFactory.h"
#include "rtps/utils/Lock.h"
#include "rtps/utils/Log.h"
#include "trace_control.h"

#if SFR_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define SFR_LOG(...) RTPS_LOG("StatefulReader", SFR_VERBOSE, __VA_ARGS__)
#else
#define SFR_LOG(...) RTPS_TRACE_LOG_EMIT("StatefulReader", __VA_ARGS__)
#endif

using rtps::StatefulReaderT;

template <class NetworkDriver>
StatefulReaderT<NetworkDriver>::~StatefulReaderT() {
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::init(const TopicData &attributes,
                                          NetworkDriver &driver,
                                          ThreadPool *threadPool,
                                          const FeatureQOS &qos) {
  (void)qos;
  m_attributes = attributes;
  m_transport = &driver;
  mp_threadPool = threadPool;
  m_packetInfo.srcPort = attributes.unicastLocator.port;
  m_is_initialized_ = true;
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::newChange(
    const ReaderCacheChange &cacheChange) {
  SFR_LOG("newChange topic=%s size=%u writer_key0=%u sn=(%d,%u)",
          &m_attributes.topicName[0],
          static_cast<unsigned>(cacheChange.size),
          cacheChange.writerGuid.entityId.entityKey[0],
          cacheChange.sn.high, cacheChange.sn.low);
  if (m_callback == nullptr) {
    SFR_LOG("newChange callback is null");
    return;
  }

  Lock lock{m_mutex};
  for (auto &proxy : m_proxies) {
    if (proxy.remoteWriterGuid == cacheChange.writerGuid) {

      proxy.ackNackPolicy.onDataReceived();
      if (proxy.ackNackPolicy.shouldSendProactiveAckNack()) {
        sendAckNackToWriter(proxy, 1);
      }

      const CacheChange *slot = proxy.history.addChange(
          cacheChange.getData(), cacheChange.size, cacheChange.sn,
          cacheChange.eventId);

      if (slot == nullptr) {
        SFR_LOG("newChange discarded sn=(%d,%u) — duplicate or out of range",
                cacheChange.sn.high, cacheChange.sn.low);
        return;
      }

      RTPS_TRACE_PERF_EVENT(reader_recv_data,
          static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
          static_cast<uint32_t>(cacheChange.writerGuid.entityId.entityKey[0]),
          static_cast<uint32_t>(cacheChange.sn.low),
          cacheChange.sn < proxy.expectedSN() ? 1u : 0u);

      deliverContiguous(proxy);

      if (proxy.history.isEmpty()) {
        proxy.firstPendingMs = 0;
      } else {
        const SequenceNumber_t leadingSN = proxy.history.getSeqNumMin();
        if (proxy.firstPendingMs == 0 || !(proxy.firstPendingSN == leadingSN)) {
          proxy.firstPendingSN = leadingSN;
          proxy.firstPendingMs = Time_t::now().toMilliseconds();
        } else {
          const int64_t nowMs = Time_t::now().toMilliseconds();
          const int64_t gapAge = nowMs - proxy.firstPendingMs;
          const int64_t sinceLast = nowMs - proxy.lastFastNackMs;
          if (gapAge >= static_cast<int64_t>(WriterProxy::REORDER_GRACE_MS) &&
              (proxy.lastFastNackMs == 0 ||
               sinceLast >= static_cast<int64_t>(WriterProxy::MIN_RENACK_MS))) {
            proxy.lastFastNackMs = nowMs;
            sendAckNackToWriter(proxy, 2);
          }
        }
      }

      return;
    }
  }
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::deliverContiguous(WriterProxy &proxy) {
  while (!proxy.history.isEmpty()) {

    SequenceNumber_t nextSN = proxy.history.getSeqNumMin();
    const CacheChange *change = proxy.history.getChangeBySN(nextSN);
    if (change == nullptr){
        break;
    }

    if (change->kind == ChangeKind_t::INVALID) {
      // tombstoned below the writers history floor, skip without delivering
      proxy.history.removeUntilIncl(nextSN);
      continue;
    }

    if(change->kind != ChangeKind_t::ALIVE_UNDELIVERED) {
      SFR_LOG("deliverContiguous gap at sn=(%d,%u)",
                nextSN.high, nextSN.low);
      break; // gap or pending, stop delivering
    }

    Guid_t writerGuid = proxy.remoteWriterGuid;
    const uint8_t *dataPtr = change->data.m_buf.data();
    DataSize_t dataLen = static_cast<DataSize_t>(change->data.m_buf.size());
    ReaderCacheChange deliverable(
        change->kind, writerGuid, change->sequenceNumber,
        dataPtr, dataLen, change->eventId);

    RTPS_TRACE_PERF_EVENT(reader_deliver,
        static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
        static_cast<uint32_t>(proxy.remoteWriterGuid.entityId.entityKey[0]),
        static_cast<uint32_t>(nextSN.low));

    m_callback(m_callee, deliverable);
    proxy.history.removeUntilIncl(nextSN);
  }
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::registerCallback(ddsReaderCallback_fp cb,
                                                      void *callee) {
  if (cb != nullptr) {
    m_callback = cb;
    m_callee = callee;
  } else {
    SFR_LOG("Passed callback is nullptr\n");
  }
}

template <class NetworkDriver>
bool StatefulReaderT<NetworkDriver>::addNewMatchedWriter(
    const WriterProxy &newProxy) {

  Lock lock{m_mutex};

  for(auto& proxy: m_proxies){
    if(proxy.remoteWriterGuid == newProxy.remoteWriterGuid){
      // refresh locator, the first announcement may have carried a bad one
      if (newProxy.remoteLocator.isValid()) {
        proxy.remoteLocator = newProxy.remoteLocator;
      }
      SFR_LOG("Discarding WriterProxy topic=%s writer_key0=%u, already known", &m_attributes.topicName[0], newProxy.remoteWriterGuid.entityId.entityKey[0]);
      return false;
    }
  }

  SFR_LOG("Adding WriterProxy topic=%s writer_key0=%u", &m_attributes.topicName[0],
          newProxy.remoteWriterGuid.entityId.entityKey[0]);

  m_proxies.push_back(newProxy);

  // builtin readers recover samples below the first received floor, user readers do not
  if (Config::BUILTIN_READER_RECOVER_BELOW_FLOOR) {
    const EntityKind_t kind = m_attributes.endpointGuid.entityId.entityKind;
    if (kind == EntityKind_t::BUILD_IN_READER_WITHOUT_KEY ||
        kind == EntityKind_t::BUILD_IN_READER_WITH_KEY) {
      m_proxies.back().history.setRecoverBelowFloor(true);
    }
  }
  return true;
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::removeWriter(const Guid_t &guid) {
  Lock lock{m_mutex};
  for (auto it = m_proxies.begin(); it != m_proxies.end(); ++it) {
    if (it->remoteWriterGuid == guid) {
      m_proxies.erase(it);
      break;
    }
  }
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::removeWriterOfParticipant(
    const GuidPrefix_t &guidPrefix) {
  Lock lock{m_mutex};
  for (auto it = m_proxies.begin(); it != m_proxies.end(); ++it) {
    if (it->remoteWriterGuid.prefix == guidPrefix) {
      m_proxies.erase(it);
      break;
    }
  }
}

template <class NetworkDriver>
bool StatefulReaderT<NetworkDriver>::onNewHeartbeat(
    const SubmessageHeartbeat &msg, const GuidPrefix_t &sourceGuidPrefix) {
  Lock lock{m_mutex};

  PacketInfo info;
  info.srcPort = m_packetInfo.srcPort;

  WriterProxy *writer = nullptr;
  for (WriterProxy &proxy : m_proxies) {
    if (proxy.remoteWriterGuid.prefix == sourceGuidPrefix &&
        proxy.remoteWriterGuid.entityId == msg.writerId) {
      writer = &proxy;
      break;
    }
  }

  if (writer == nullptr) {
    SFR_LOG("Ignore heartbeat. No matching writer key0=%u", msg.writerId.entityKey[0]);
    return false;
  }

  if (msg.count.value <= writer->hbCount.value) {
    SFR_LOG("Ignore heartbeat. Count too low. \n");
    return false;
  }
  writer->hbCount.value = msg.count.value;
  writer->lastHbFirstSN = msg.firstSN;
  writer->lastHbLastSN = msg.lastSN;

  RTPS_TRACE_PERF_EVENT(reader_recv_heartbeat,
      static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
      static_cast<uint32_t>(msg.writerId.entityKey[0]),
      static_cast<uint32_t>(msg.firstSN.low),
      static_cast<uint32_t>(msg.lastSN.low),
      static_cast<uint32_t>(msg.count.value));

  SequenceNumberSet missingSet = writer->getMissing(msg.firstSN, msg.lastSN);
  const uint32_t numMissing = missingSet.countSet();

  // range update may have tombstoned dead PENDING slots, resume delivery of samples queued behind them
  deliverContiguous(*writer);

  writer->ackNackPolicy.onHeartbeat(numMissing);

  if (!writer->ackNackPolicy.shouldSendAckNack()) {
    return true;
  }

  info.destAddr = writer->remoteLocator.getIp4Address();
  info.destPort = writer->remoteLocator.port;
  rtps::MessageFactory::addHeader(info.buffer,
                                  m_attributes.endpointGuid.prefix);
  rtps::MessageFactory::addAckNack(info.buffer, msg.writerId, msg.readerId,
                                   missingSet,
                                   writer->getNextAckNackCount(), false);

  RTPS_TRACE_PERF_EVENT(reader_send_acknack,
      static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
      static_cast<uint32_t>(msg.writerId.entityKey[0]),
      static_cast<uint32_t>(missingSet.base.low),
      static_cast<uint32_t>(missingSet.numBits),
      0u);

  SFR_LOG("Sending acknack base=(%d,%lu) numBits=%u\n", missingSet.base.high, missingSet.base.low, missingSet.bitMap[0]);
  m_transport->sendPacket(info);
  return true;
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::sendAckNackToWriter(WriterProxy &proxy, uint32_t trigger) {
  // every NACK derives from the last advertised heartbeat range, before the first heartbeat there is nothing authoritative to request
  if (!proxy.hasHeartbeatRange()) {
    return;
  }

  PacketInfo info;
  info.srcPort = m_packetInfo.srcPort;
  info.destAddr = proxy.remoteLocator.getIp4Address();
  info.destPort = proxy.remoteLocator.port;

  rtps::MessageFactory::addHeader(info.buffer,
                                  m_attributes.endpointGuid.prefix);

  SequenceNumberSet set =
      proxy.getMissing(proxy.lastHbFirstSN, proxy.lastHbLastSN);
  if (set.numBits == 0) {
    set.base = proxy.expectedSN();
  }
  rtps::MessageFactory::addAckNack(info.buffer,
                                   proxy.remoteWriterGuid.entityId,
                                   m_attributes.endpointGuid.entityId,
                                   set, proxy.getNextAckNackCount(), false);

  RTPS_TRACE_PERF_EVENT(reader_send_acknack,
      static_cast<uint32_t>(m_attributes.endpointGuid.entityId.entityKey[0]),
      static_cast<uint32_t>(proxy.remoteWriterGuid.entityId.entityKey[0]),
      static_cast<uint32_t>(set.base.low),
      static_cast<uint32_t>(set.numBits),
      trigger);

  SFR_LOG("Sending forced acknack base=(%d,%u) numBits=%u\n",
          set.base.high, set.base.low, set.numBits);
  m_transport->sendPacket(info);
}
