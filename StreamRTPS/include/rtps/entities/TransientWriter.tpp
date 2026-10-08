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

#include <rtps/entities/ReaderProxy.h>

#include "rtps/ThreadPool.h"
#include "rtps/communication/UdpDriver.h"
#include "rtps/messages/MessageFactory.h"
#include "rtps/storages/PBufWrapper.h"
#include "rtps/utils/EventId.h"
#include "rtps/utils/Log.h"
#include "rtps/utils/udpUtils.h"
#include "trace_control.h"

using rtps::TransientCacheChange;
using rtps::TransientWriter;
using rtps::TransientWriterT;

#if SLW_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define TRW_LOG(...) RTPS_LOG("TransientWriter", SLW_VERBOSE, __VA_ARGS__)
#else
#define TRW_LOG(...) RTPS_TRACE_LOG_EMIT("TransientWriter", __VA_ARGS__)
#endif

template <class NetworkDriver>
TransientWriterT<NetworkDriver>::~TransientWriterT() {}

template <typename NetworkDriver>
bool TransientWriterT<NetworkDriver>::init(TopicData attributes,
                                           TopicKind_t topicKind,
                                           ThreadPool *threadPool,
                                           NetworkDriver &driver,
                                           bool enfUnicast,
                                           const FeatureQOS &qos) {
  if (mutex_new(&m_mutex) != EMB_ERR_OK) {
#if SLW_VERBOSE
    TRW_LOG("Failed to create mutex \n");
#endif
    return false;
  }

  (void)qos;
  (void)enfUnicast;
  m_attributes = attributes;
  m_packetInfo.srcPort = attributes.unicastLocator.port;
  m_topicKind = topicKind;
  mp_threadPool = threadPool;
  m_transport = &driver;

  m_is_initialized_ = true;
  return true;
}

// transient writer keeps no reader proxies, destinations travel with the change
template <class NetworkDriver>
bool TransientWriterT<NetworkDriver>::addNewMatchedReader(
    const ReaderProxy & /*newProxy*/) {
  return false;
}

template <class NetworkDriver>
void TransientWriterT<NetworkDriver>::removeReader(const Guid_t & /*guid*/) {}

template <class NetworkDriver>
void TransientWriterT<NetworkDriver>::removeReaderOfParticipant(
    const GuidPrefix_t & /*guidPrefix*/) {}

// no proxies to pair, transient sends are always plain unicast
template <class NetworkDriver>
void TransientWriterT<NetworkDriver>::manageSendOptions() {}

template <class NetworkDriver>
void TransientWriterT<NetworkDriver>::resetSendOptions() {}

// base overload has no destination, unsupported for the transient path
template <typename NetworkDriver>
const rtps::CacheChange *TransientWriterT<NetworkDriver>::newChange(
    rtps::ChangeKind_t /*kind*/, const uint8_t * /*data*/, DataSize_t /*size*/) {
  return nullptr;
}

template <typename NetworkDriver>
const rtps::TransientCacheChange *TransientWriterT<NetworkDriver>::newChange(
    rtps::ChangeKind_t kind, const uint8_t *data, Guid_t rec, Locator loc,
    DataSize_t size) {
  if (isIrrelevant(kind)) {
    return nullptr;
  }
  const EventId_t eventId = generateEventId();
  const TransientCacheChange *result = nullptr;
  {
    Lock lock(m_mutex);
    result = m_history.addChange(data, size, loc, rec, eventId);
  }
  if (mp_threadPool != nullptr) {
    mp_threadPool->addWorkload(this);
  }
  // valid until the slot is overwritten or removed
  return result;
}

template <typename NetworkDriver>
void TransientWriterT<NetworkDriver>::setAllChangesToUnsent() {
  // fire and forget, progress drains everything, nothing to rearm
}

template <typename NetworkDriver>
void TransientWriterT<NetworkDriver>::removeAllChanges() {
  Lock lock(m_mutex);
  if (!m_history.isEmpty()) {
    m_history.removeUntilIncl(m_history.getIDMax());
  }
}

template <typename NetworkDriver>
void TransientWriterT<NetworkDriver>::onNewAckNack(
    const SubmessageAckNack & /*msg*/, const GuidPrefix_t & /*sourceGuidPrefix*/) {
  // best effort, nothing to retransmit
}

template <typename NetworkDriver>
bool TransientWriterT<NetworkDriver>::isIrrelevant(ChangeKind_t kind) const {
  return kind != ChangeKind_t::ALIVE;
}

template <typename NetworkDriver>
void TransientWriterT<NetworkDriver>::progress() {
  Lock lock(m_mutex);

  RTPS_TRACE_ALL_EVENT(writer_progress_start, "TransientWriter",
                       m_attributes.endpointGuid.entityId.entityKey[0],
                       m_history.size());

  // fire and forget, send each queued change to its own destination then drop all
  for (const auto &change : m_history) {
    if (isIrrelevant(change.kind)) {
      continue;
    }

    PacketInfo info;
    info.srcPort = m_packetInfo.srcPort;
    info.eventId = change.eventId;

    MessageFactory::addHeader(info.buffer, m_attributes.endpointGuid.prefix);
    MessageFactory::addSubMessageTimeStamp(info.buffer);

    const EntityId_t reid = change.remoteReader.entityId;

    uint64_t guid_prefix_high = 0;
    uint64_t guid_prefix_low = 0;
    for (uint16_t i = 0; i < 8 && i < change.remoteReader.prefix.id.size(); i++) {
      guid_prefix_high |=
          (static_cast<uint64_t>(change.remoteReader.prefix.id[i]) << (i * 8));
    }
    for (uint16_t i = 8; i < 12 && i < change.remoteReader.prefix.id.size(); i++) {
      guid_prefix_low |=
          (static_cast<uint64_t>(change.remoteReader.prefix.id[i]) << ((i - 8) * 8));
    }

    RTPS_TRACE_ALL_EVENT(writer_send_to_proxy, change.eventId,
                          m_attributes.endpointGuid.entityId.entityKey[0],
                          reid.entityKey[0], guid_prefix_high, guid_prefix_low,
                          SEQUENCENUMBER_UNKNOWN.high, SEQUENCENUMBER_UNKNOWN.low,
                          0, 0);

    MessageFactory::addSubMessageData(info.buffer, change.data, false,
                                      SEQUENCENUMBER_UNKNOWN,
                                      m_attributes.endpointGuid.entityId, reid);

    info.destAddr = change.locator.getIp4Address();
    info.destPort = (Ip4Port_t)change.locator.port;

    TRW_LOG("Sending transient sample to driver.\n");
    m_transport->sendPacket(info);
  }

  if (!m_history.isEmpty()) {
    m_history.removeUntilIncl(m_history.getIDMax());
  }

  RTPS_TRACE_ALL_EVENT(writer_progress_end, "TransientWriter",
                       m_attributes.endpointGuid.entityId.entityKey[0]);
}

template <typename NetworkDriver>
bool TransientWriterT<NetworkDriver>::hasLocator(const Locator & /*loc*/) {
  // no proxies retained, transient writer is not locator addressable
  return false;
}
