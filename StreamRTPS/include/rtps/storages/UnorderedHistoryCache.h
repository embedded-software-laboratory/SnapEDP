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

#ifndef PROJECT_UnorderedHistoryCache_H
#define PROJECT_UnorderedHistoryCache_H

#include "rtps/config.h"
#include "rtps/storages/CacheChange.h"
#include "rtps/storages/HistoryCacheIterator.h"
#include "rtps/common/types.h"
#include "rtps/utils/Lock.h"

namespace rtps {

class UnorderedHistoryCache {
public:
  UnorderedHistoryCache();
  UnorderedHistoryCache(const UnorderedHistoryCache &other);
  UnorderedHistoryCache &operator=(const UnorderedHistoryCache &other);

  bool isFull() const;
  bool isEmpty() const;
  uint32_t size() const;

  const CacheChange *addChange(const uint8_t *data, DataSize_t size,
                               SequenceNumber_t sn, EventId_t eventId = 0);
  void registerGap(SequenceNumber_t lastAvail);
  void dropOldest();
  void removeUntilIncl(SequenceNumber_t sn);
  const CacheChange *getChangeBySN(SequenceNumber_t sn) const;
  CacheChange *getChangeBySN(SequenceNumber_t sn);
  SequenceNumberSet getMissing() const;
  void ensureRangeInitialized(SequenceNumber_t firstAvail,
                              SequenceNumber_t lastAvail);

  const SequenceNumber_t &getSeqNumMin() const;
  const SequenceNumber_t &getSeqNumMax() const;

  HistoryIterator begin();
  HistoryIterator end();
  ConstHistoryIterator begin() const;
  ConstHistoryIterator end() const;

  rtps::TransmissionPeriod_t detectMeanPeriod(uint32_t minSampleCount = 5);
  bool gapOlderThen(uint32_t backMs) const;
  SequenceNumberSet getMissingForHeartbeatRange(SequenceNumber_t firstAvail,
                                               SequenceNumber_t lastAvail);

  // opt in for builtin readers, recover samples below the first received floor
  void setRecoverBelowFloor(bool enable);

private:
  std::array<CacheChange, HISTORY_BUFFER_SIZE> m_buffer{};
  uint32_t m_head = 0;
  uint32_t m_tail = 0;
  SequenceNumber_t m_highestSequenceNumber{0, 0};
  SequenceNumber_t m_lowestSequenceNumber{0, 1};
  bool m_recoverBelowFloor = false;
  bool m_belowFloorBaselined = false;
  mutable Mutex m_mutex{};

  void incrementHead();
  void incrementTail();

  uint32_t wrapPos(uint32_t pos) const;
  uint32_t snToPos(SequenceNumber_t sn) const;

  const SequenceNumber_t &seqNumMinRefLocked() const;
  const CacheChange *getChangeBySNLocked(SequenceNumber_t sn) const;

  void ensureRangeInitializedImpl(SequenceNumber_t firstAvail,
                                  SequenceNumber_t lastAvail);
  void registerGapImpl(SequenceNumber_t lastAvail);
  void purgeUnavailableBelowImpl(SequenceNumber_t firstAvail);
  void backfillBelowFloorImpl(SequenceNumber_t firstAvail);
  SequenceNumberSet getMissingImpl() const;

protected:
  explicit UnorderedHistoryCache(SequenceNumber_t SN);
};
} // namespace rtps

#endif // PROJECT_UnorderedHistoryCache_H
