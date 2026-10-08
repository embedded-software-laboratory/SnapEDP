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

#ifndef RTPS_READER_H
#define RTPS_READER_H

#include "rtps/common/types.h"
#include "rtps/config.h"
#include "rtps/discovery/TopicData.h"
#include "rtps/entities/WriterProxy.h"
#include "rtps/storages/PBufWrapper.h"
#include <cstring>
#include <vector>

namespace rtps {

struct SubmessageHeartbeat;
struct SubmessageSFTMask;

class ReaderCacheChange {
private:
  const uint8_t *data;

public:
  const ChangeKind_t kind;
  const DataSize_t size;
  const EventId_t eventId;
  const Guid_t writerGuid;
  const SequenceNumber_t sn;

  ReaderCacheChange(ChangeKind_t kind, Guid_t &writerGuid, SequenceNumber_t sn,
                    const uint8_t *data, DataSize_t size,
                    EventId_t eventId = 0)
      : data(data), kind(kind), size(size), eventId(eventId),
        writerGuid(writerGuid), sn(sn){};

  ~ReaderCacheChange() =
      default; // no need to free data, its not owned by this object
  // not allowed, this class doesnt own the ptr and its invalid outside the callback scope
  ReaderCacheChange(const ReaderCacheChange &other) = delete;
  ReaderCacheChange(ReaderCacheChange &&other) = delete;
  ReaderCacheChange &operator=(const ReaderCacheChange &other) = delete;
  ReaderCacheChange &operator=(ReaderCacheChange &&other) = delete;

  bool copyInto(uint8_t *buffer, DataSize_t destSize) const {
    if (destSize < size) {
      return false;
    } else {
      memcpy(buffer, data, size);
      return true;
    }
  }

  const uint8_t *getData() const { return data; }

  DataSize_t getDataSize() const { return size; }
};

typedef void (*ddsReaderCallback_fp)(void *callee,
                                     const ReaderCacheChange &cacheChange);


                                     
class Reader {
public:
  TopicData m_attributes;
  virtual void newChange(const ReaderCacheChange &cacheChange) = 0;
  virtual void registerCallback(ddsReaderCallback_fp cb, void *callee) = 0;
  virtual bool onNewHeartbeat(const SubmessageHeartbeat &msg,
                              const GuidPrefix_t &remotePrefix) = 0;
  virtual bool addNewMatchedWriter(const WriterProxy &newProxy) = 0;
  virtual void removeWriter(const Guid_t &guid) = 0;
  virtual void removeWriterOfParticipant(const GuidPrefix_t &guidPrefix) = 0;
  virtual ~Reader() = default;
  bool isInitialized() { return m_is_initialized_; }

  bool knowWriterId(const Guid_t &guid) {
    for (const auto &proxy : m_proxies) {
      if (proxy.remoteWriterGuid.operator==(guid)) {
        return true;
      }
    }
    return false;
  }

protected:
  bool m_is_initialized_ = false;
  std::vector<WriterProxy> m_proxies;
};
} // namespace rtps

#endif // RTPS_READER_H
