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

#ifndef PROJECT_OrderedHistoryCache_H
#define PROJECT_OrderedHistoryCache_H

#include "rtps/config.h"
#include "rtps/storages/CacheChange.h"
#include "rtps/storages/HistoryCacheIterator.h"

namespace rtps {

// simple history cache, auto assigns consecutive SNs so acked changes drop fast, cant dispose arbitrary changes

class OrderedHistoryCache {
public:
  OrderedHistoryCache() = default;

  bool isFull() const;
  bool isEmpty() const;
  uint32_t size() const;
  const CacheChange *addChange(const uint8_t *data, DataSize_t size,
                               EventId_t eventId = 0);
  void dropOldest();
  void removeUntilIncl(SequenceNumber_t sn);
  const CacheChange *getChangeBySN(SequenceNumber_t sn) const;

  const SequenceNumber_t &getSeqNumMin() const;
  const SequenceNumber_t &getSeqNumMax() const;
  // highest SN confirmed sent on the wire, SEQUENCENUMBER_UNKNOWN if nothing sent yet
  SequenceNumber_t getSeqNumMaxSent() const;

  // advance the sent high water mark, call after a confirmed wire send
  void markAsSent(SequenceNumber_t sn);
  // reset sent tracking, needed after setAllChangesToUnsent
  void resetMaxSent();

  HistoryIterator begin();
  HistoryIterator end();
  ConstHistoryIterator begin() const;
  ConstHistoryIterator end() const;

private:
  std::array<CacheChange, HISTORY_BUFFER_SIZE> m_buffer{};
  uint32_t m_head = 0;
  uint32_t m_tail = 0;

  SequenceNumber_t m_lastUsedSequenceNumber{0, 0};
  SequenceNumber_t m_seqNumMaxSent = SEQUENCENUMBER_UNKNOWN;

  void incrementHead();
  void incrementTail();

  uint32_t wrapPos(uint32_t pos) const;
  uint32_t snToPos(SequenceNumber_t sn) const;

protected:
  explicit OrderedHistoryCache(SequenceNumber_t SN);
};
} // namespace rtps

#endif // PROJECT_OrderedHistoryCache_H
