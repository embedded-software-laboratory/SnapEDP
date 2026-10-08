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

#include "rtps/entities/TransientReader.h"
#include "rtps/utils/Lock.h"
#include "rtps/utils/Log.h"
#include "trace_control.h"

using rtps::TransientReader;

#if SLR_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define TRR_LOG(...) RTPS_LOG("TransientReader", SLR_VERBOSE, __VA_ARGS__)
#else
#define TRR_LOG(...) RTPS_TRACE_LOG_EMIT("TransientReader", __VA_ARGS__)
#endif

void TransientReader::init(const TopicData &attributes, const FeatureQOS &qos) {
  (void)qos;
  m_attributes = attributes;
  m_is_initialized_ = true;
  if (mutex_new(&m_mutex) != EMB_ERR_OK) {
    TRR_LOG("Failed to create mutex.\n");
  }
}

// best effort accept from any, deliver straight to the callback
void TransientReader::newChange(const ReaderCacheChange &cacheChange) {
  RTPS_TRACE_PERF_EVENT(reader_new_change, cacheChange.eventId,
                        "TransientReader",
                        m_attributes.endpointGuid.entityId.entityKey[0],
                        cacheChange.writerGuid.entityId.entityKey[0],
                        cacheChange.sn.high, cacheChange.sn.low,
                        static_cast<uint32_t>(cacheChange.size));
  RTPS_TRACE_PERF_EVENT(reader_callback_invoked, cacheChange.eventId,
                        m_attributes.endpointGuid.entityId.entityKey[0],
                        m_callback != nullptr ? 1 : 0);
  if (m_callback != nullptr) {
    m_callback(m_callee, cacheChange);
  }
}

void TransientReader::registerCallback(ddsReaderCallback_fp cb, void *callee) {
  if (cb != nullptr) {
    m_callback = cb;
    m_callee = callee; // Its okay if this is null
  } else {
    TRR_LOG("Passed callback is nullptr\n");
  }
}

bool TransientReader::addNewMatchedWriter(const WriterProxy &newProxy) {
  // accept from any, no proxy kept, same as the TransientWriter side
  (void)newProxy;
  return false;
}

void TransientReader::removeWriter(const Guid_t &guid) {}

void TransientReader::removeWriterOfParticipant(const GuidPrefix_t &guidPrefix) {
}

bool TransientReader::onNewHeartbeat(const SubmessageHeartbeat &,
                                     const GuidPrefix_t &) {
  // best effort, no acknack
  return true;
}
