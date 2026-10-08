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

#include <cstring>

using rtps::CacheChange;
using rtps::SequenceNumber_t;
using rtps::StatelessWriterT;

#if SLW_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define SLW_LOG(...) RTPS_LOG("StatelessWriter", SLW_VERBOSE, __VA_ARGS__)
#else
#define SLW_LOG(...) RTPS_TRACE_LOG_EMIT("StatelessWriter", __VA_ARGS__)
#endif

template <class NetworkDriver>
StatelessWriterT<NetworkDriver>::~StatelessWriterT() {
}

template <typename NetworkDriver>
bool StatelessWriterT<NetworkDriver>::init(TopicData attributes,
                                           TopicKind_t topicKind,
                                           ThreadPool *threadPool,
                                           NetworkDriver &driver,
                                           bool enfUnicast,
                                           const FeatureQOS &qos) {
  if (mutex_new(&m_mutex) != EMB_ERR_OK) {
#if SLW_VERBOSE
    SLW_LOG("Failed to create mutex \n");
#endif
    return false;
  }

  (void)qos;
  m_attributes = attributes;
  m_packetInfo.srcPort = attributes.unicastLocator.port;
  m_topicKind = topicKind;
  mp_threadPool = threadPool;
  m_transport = &driver;
  m_enforceUnicast = enfUnicast;

  m_is_initialized_ = true;
  return true;
}

template <class NetworkDriver>
bool StatelessWriterT<NetworkDriver>::addNewMatchedReader(
    const ReaderProxy &newProxy) {
#if SLW_VERBOSE && RTPS_GLOBAL_VERBOSE
  SLW_LOG("New reader added topic=%s reader_key0=%u",
          &this->m_attributes.topicName[0],
          newProxy.remoteReaderGuid.entityId.entityKey[0]);
#endif
  bool duplicate = false;
  bool locatorRefreshed = false;
  {
    Lock l(memPoolMutex);
    // early dedup, avoid multiple ReaderProxy entries for the same remote reader GUID
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
      SLW_LOG("Discarding ReaderProxy topic=%s reader_key0=%u, already known",
              &this->m_attributes.topicName[0],
              newProxy.remoteReaderGuid.entityId.entityKey[0]);
      return false;
    }
    if (!duplicate) {
      m_proxies.push_back(newProxy);
    }
  }
  if (!duplicate || locatorRefreshed) {
    // record the locator we will send this readers user data to
    uint64_t wlh = 0, wll = 0, rlh = 0, rll = 0;
    for (uint16_t i = 0; i < 8; ++i) {
      wlh |= (static_cast<uint64_t>(m_attributes.endpointGuid.prefix.id[i]) << (i * 8));
      rlh |= (static_cast<uint64_t>(newProxy.remoteReaderGuid.prefix.id[i]) << (i * 8));
    }
    for (uint16_t i = 8; i < 12; ++i) {
      wll |= (static_cast<uint64_t>(m_attributes.endpointGuid.prefix.id[i]) << ((i - 8) * 8));
      rll |= (static_cast<uint64_t>(newProxy.remoteReaderGuid.prefix.id[i]) << ((i - 8) * 8));
    }
    uint32_t wEnt = 0;
    std::memcpy(&wEnt, &m_attributes.endpointGuid.entityId,
                sizeof(m_attributes.endpointGuid.entityId));
    RTPS_TRACE_ALL_EVENT(reader_proxy_locator_stored, wlh, wll, wEnt, rlh, rll,
                         newProxy.remoteLocator.getIp4Address().addr,
                         static_cast<uint16_t>(newProxy.remoteLocator.port),
                         locatorRefreshed ? 1 : 0);
  }
  if (!m_enforceUnicast) {
    manageSendOptions();
  }
  return !duplicate;
}

template <class NetworkDriver>
void StatelessWriterT<NetworkDriver>::manageSendOptions() {
  SLW_LOG("Search for Multicast Partners!\n");
  Lock l(memPoolMutex);

  // reset flags before recalculating so stale flags dont affect pairing
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
            SLW_LOG("Found Multicast Partner!\n");
            if (avproxy.remoteReaderGuid.entityId !=
                proxy.remoteReaderGuid.entityId) {
              proxy.unknown_eid = true;
              SLW_LOG("Found different EntityIds, using UNKNOWN_ENTITYID\n");
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
void StatelessWriterT<NetworkDriver>::resetSendOptions() {
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
void StatelessWriterT<NetworkDriver>::removeReader(const Guid_t &guid) {
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
void StatelessWriterT<NetworkDriver>::removeReaderOfParticipant(
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

template <typename NetworkDriver>
const CacheChange *StatelessWriterT<NetworkDriver>::newChange(
    rtps::ChangeKind_t kind, const uint8_t *data, DataSize_t size) {
  const EventId_t eventId = generateEventId();
  RTPS_LOG("StatelessWriter", SLW_VERBOSE,
           "newChange kind=%d size=%u eventId=%lu",
           static_cast<int>(kind), static_cast<unsigned>(size),
           static_cast<unsigned long>(eventId));
  RTPS_TRACE_PERF_EVENT(writer_new_change, eventId, "StatelessWriter",
                        m_attributes.endpointGuid.entityId.entityKey[0],
                        static_cast<int>(kind), static_cast<uint32_t>(size), 0,
                        0);
  
  if (isIrrelevant(kind)) {
    RTPS_LOG("StatelessWriter", SLW_VERBOSE, "newChange ignored by kind");
    return nullptr;
  }
  Lock lock(m_mutex);

  if (m_history.isFull()) {
    SequenceNumber_t newMin = ++SequenceNumber_t(m_history.getSeqNumMin());
    if (m_nextSequenceNumberToSend < newMin) {
      m_nextSequenceNumberToSend =
          newMin; // Make sure we have the correct sn to send
    }
  }

  auto *result = m_history.addChange(data, size, eventId);
  if (mp_threadPool != nullptr) {
    mp_threadPool->addWorkload(this);
  }

  SLW_LOG("Adding new data.\n");
  
  if (result != nullptr) {
    RTPS_TRACE_PERF_EVENT(
        writer_new_change, eventId, "StatelessWriter",
        m_attributes.endpointGuid.entityId.entityKey[0], static_cast<int>(kind),
        static_cast<uint32_t>(size), result->sequenceNumber.high,
        result->sequenceNumber.low);
  }
  
  return result;
}

template <typename NetworkDriver>
void StatelessWriterT<NetworkDriver>::setAllChangesToUnsent() {
  Lock lock(m_mutex);

  m_nextSequenceNumberToSend = m_history.getSeqNumMin();

  if (mp_threadPool != nullptr) {
    mp_threadPool->addWorkload(this);
  }
}

template <typename NetworkDriver>
void StatelessWriterT<NetworkDriver>::removeAllChanges() {
  Lock lock(m_mutex);
  while (!m_history.isEmpty()) {
    m_history.dropOldest();
  }
  m_nextSequenceNumberToSend = m_history.getSeqNumMin();
}

template <typename NetworkDriver>
void StatelessWriterT<NetworkDriver>::onNewAckNack(
    const SubmessageAckNack & /*msg*/, const GuidPrefix_t &/*sourceGuidPrefix*/) {
  // Too lazy to respond
}

template <typename NetworkDriver>
bool StatelessWriterT<NetworkDriver>::isIrrelevant(ChangeKind_t kind) const {
  // Right now we only allow alive changes
  return kind != ChangeKind_t::ALIVE;
}

template <typename NetworkDriver>
void StatelessWriterT<NetworkDriver>::progress() {
  // TODO smarter packaging, reusing the pbuf isnt possible, see https://www.nongnu.org/lwip/2_1_x/raw_api.html

  Lock l(memPoolMutex);
  
  RTPS_TRACE_ALL_EVENT(writer_progress_start, "StatelessWriter",
                       m_attributes.endpointGuid.entityId.entityKey[0],
                       m_proxies.size());
  
  if (m_proxies.size() == 0) {
    SLW_LOG("No Proxy!\n");
  }

  // snap the send pointer forward to history min when it lags, like after overflow dropped the oldest entry past it
  {
    Lock lock(m_mutex);
    if (!m_history.isEmpty()) {
      const SequenceNumber_t histMin = m_history.getSeqNumMin();
      if (m_nextSequenceNumberToSend < histMin) {
        m_nextSequenceNumberToSend = histMin;
      }
    }
  }

  for (const auto &proxy : m_proxies) {

    SLW_LOG("Progess.\n");
    // Do nothing if someone else sends for me via multicast
    if (proxy.useMulticast || !proxy.suppressUnicast || m_enforceUnicast) {
      PacketInfo info;
      info.srcPort = m_packetInfo.srcPort;

      {
        Lock lock(m_mutex);
        const CacheChange *next =
            m_history.getChangeBySN(m_nextSequenceNumberToSend);
        if (next == nullptr) {
          SLW_LOG("Couldn't get a new CacheChange with SN "
                  "(%i,%i)\n",
                  m_nextSequenceNumberToSend.high,
                  m_nextSequenceNumberToSend.low);
          return;
        } else {
          SLW_LOG("Sending change with SN (%i,%i)\n",
                  m_nextSequenceNumberToSend.high,
                  m_nextSequenceNumberToSend.low);
        }
        info.eventId = next->eventId;

        MessageFactory::addHeader(info.buffer, m_attributes.endpointGuid.prefix);
        MessageFactory::addSubMessageTimeStamp(info.buffer);
        SLW_LOG("Sending using regular RTPS message path\n");
        EntityId_t reid;
        if (proxy.useMulticast && !m_enforceUnicast && proxy.unknown_eid) {
          reid = ENTITYID_UNKNOWN;
        } else {
          reid = proxy.remoteReaderGuid.entityId;
        }

        uint64_t guid_prefix_high = 0;
        uint64_t guid_prefix_low = 0;
        for (uint16_t i = 0; i < 8 && i < proxy.remoteReaderGuid.prefix.id.size(); i++) {
          guid_prefix_high |= (static_cast<uint64_t>(proxy.remoteReaderGuid.prefix.id[i]) << (i * 8));
        }
        for (uint16_t i = 8; i < 12 && i < proxy.remoteReaderGuid.prefix.id.size(); i++) {
          guid_prefix_low |= (static_cast<uint64_t>(proxy.remoteReaderGuid.prefix.id[i]) << ((i - 8) * 8));
        }

        RTPS_TRACE_PERF_EVENT(
            writer_send_to_proxy, next->eventId,
            m_attributes.endpointGuid.entityId.entityKey[0], reid.entityKey[0],
            guid_prefix_high, guid_prefix_low, next->sequenceNumber.high,
            next->sequenceNumber.low,
            proxy.useMulticast && !m_enforceUnicast ? 1 : 0,
            0);

        MessageFactory::addSubMessageData(info.buffer, next->data, false,
                                          next->sequenceNumber,
                                          m_attributes.endpointGuid.entityId,
                                          reid); // TODO
      }

      const Locator *locator = nullptr;
      if (proxy.useMulticast && !m_enforceUnicast) {
        locator = &proxy.remoteMulticastLocator;
      } else {
        locator = &proxy.remoteLocator;
      }
      info.destAddr = locator->getIp4Address();
      info.destPort = (Ip4Port_t)locator->port;

      {
        // actual locator a user data sample is sent to
        uint64_t wlh = 0, wll = 0, rlh = 0, rll = 0;
        for (uint16_t i = 0; i < 8; ++i) {
          wlh |= (static_cast<uint64_t>(m_attributes.endpointGuid.prefix.id[i]) << (i * 8));
          rlh |= (static_cast<uint64_t>(proxy.remoteReaderGuid.prefix.id[i]) << (i * 8));
        }
        for (uint16_t i = 8; i < 12; ++i) {
          wll |= (static_cast<uint64_t>(m_attributes.endpointGuid.prefix.id[i]) << ((i - 8) * 8));
          rll |= (static_cast<uint64_t>(proxy.remoteReaderGuid.prefix.id[i]) << ((i - 8) * 8));
        }
        uint32_t wEnt = 0;
        std::memcpy(&wEnt, &m_attributes.endpointGuid.entityId,
                    sizeof(m_attributes.endpointGuid.entityId));
        RTPS_TRACE_PERF_EVENT(writer_data_locator, wlh, wll, wEnt, rlh, rll,
                              info.destAddr.addr, info.destPort,
                              (proxy.useMulticast && !m_enforceUnicast) ? 1 : 0);
      }

      SLW_LOG("Sending sample to driver.\n");
      m_transport->sendPacket(info);
    }
  }

  {
    Lock lock(m_mutex);
    ++m_nextSequenceNumberToSend;
  }
  
  RTPS_TRACE_ALL_EVENT(writer_progress_end, "StatelessWriter",
                       m_attributes.endpointGuid.entityId.entityKey[0]);
}

template <typename NetworkDriver>
bool StatelessWriterT<NetworkDriver>::hasLocator(const Locator &loc) {
  Lock lock{memPoolMutex};
  for (const auto &proxy : m_proxies) {
    if (proxy.remoteLocator.kind == loc.kind &&
        proxy.remoteLocator.port == loc.port &&
        proxy.remoteLocator.address == loc.address) {
      return true;
    }
  }
  return false;
}

