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

#include "rtps/entities/StatelessReader.h"
#include "rtps/utils/Lock.h"
#include "rtps/utils/Log.h"
#include "trace_control.h"

using rtps::StatelessReader;

#if SLR_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define SLR_LOG(...) RTPS_LOG("StatelessReader", SLR_VERBOSE, __VA_ARGS__)
#else
#define SLR_LOG(...) RTPS_TRACE_LOG_EMIT("StatelessReader", __VA_ARGS__)
#endif

void StatelessReader::init(const TopicData &attributes,
                           const FeatureQOS &qos) {
  (void)qos;
  m_attributes = attributes;
  m_is_initialized_ = true;
  if (mutex_new(&m_mutex) != EMB_ERR_OK) {
    SLR_LOG("Failed to create mutex.\n");
  }
}

void StatelessReader::newChange(const ReaderCacheChange &cacheChange) {
  RTPS_TRACE_PERF_EVENT(reader_new_change, cacheChange.eventId,
             "StatelessReader",
             m_attributes.endpointGuid.entityId.entityKey[0],
             cacheChange.writerGuid.entityId.entityKey[0],
             cacheChange.sn.high,
             cacheChange.sn.low,
             static_cast<uint32_t>(cacheChange.size));
  
  RTPS_TRACE_PERF_EVENT(reader_callback_invoked, cacheChange.eventId,
             m_attributes.endpointGuid.entityId.entityKey[0],
             m_callback != nullptr ? 1 : 0);
  
  if (m_callback != nullptr) {
    m_callback(m_callee, cacheChange);
  }
}

void StatelessReader::registerCallback(ddsReaderCallback_fp cb, void *callee) {
  if (cb != nullptr) {
    m_callback = cb;
    m_callee = callee; // Its okay if this is null
  } else {
#if (SLR_VERBOSE && RTPS_GLOBAL_VERBOSE)
    SLR_LOG("Passed callback is nullptr\n");
#endif
  }
}

bool StatelessReader::addNewMatchedWriter(const WriterProxy &newProxy) {
  
  for(auto& proxy: m_proxies){
    if(proxy.remoteWriterGuid == newProxy.remoteWriterGuid){
      // refresh locator, the first announcement may have carried a bad one
      if (newProxy.remoteLocator.isValid()) {
        proxy.remoteLocator = newProxy.remoteLocator;
      }
      SLR_LOG("Discarding WriterProxy topic=%s writer_key0=%u, already known", &m_attributes.topicName[0], newProxy.remoteWriterGuid.entityId.entityKey[0]);
      return false;
    }
  }

  SLR_LOG("Adding WriterProxy topic=%s writer_key0=%u", &m_attributes.topicName[0],
          newProxy.remoteWriterGuid.entityId.entityKey[0]);

  m_proxies.push_back(newProxy);
  return true;
}

void StatelessReader::removeWriter(const Guid_t &guid) {
  Lock lock(m_mutex);
  for (auto it = m_proxies.begin(); it != m_proxies.end(); ++it) {
    if (it->remoteWriterGuid == guid) {
      m_proxies.erase(it);
      break;
    }
  }
}

void StatelessReader::removeWriterOfParticipant(
    const GuidPrefix_t &guidPrefix) {
  Lock lock(m_mutex);
  for (auto it = m_proxies.begin(); it != m_proxies.end(); ++it) {
    if (it->remoteWriterGuid.prefix == guidPrefix) {
      m_proxies.erase(it);
      break;
    }
  }
}

bool StatelessReader::onNewHeartbeat(const SubmessageHeartbeat &,
                                     const GuidPrefix_t &) {
  // nothing to do
  return true;
}
