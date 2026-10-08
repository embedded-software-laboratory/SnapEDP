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

#include "rtps/storages/UnorderedHistoryCache.h"
#include "rtps/utils/sysFunctions.h"
#include "trace_control.h"
#include <cassert>

using rtps::UnorderedHistoryCache;

UnorderedHistoryCache::UnorderedHistoryCache() {
  mutex_new(&m_mutex);
}

UnorderedHistoryCache::UnorderedHistoryCache(SequenceNumber_t lastUsed)
    : UnorderedHistoryCache() {
  m_highestSequenceNumber = lastUsed;
  m_lowestSequenceNumber = lastUsed + 1;
}

UnorderedHistoryCache::UnorderedHistoryCache(const UnorderedHistoryCache &other) {
  mutex_new(&m_mutex);
  Lock lockOther{other.m_mutex};
  m_buffer = other.m_buffer;
  m_head = other.m_head;
  m_tail = other.m_tail;
  m_highestSequenceNumber = other.m_highestSequenceNumber;
  m_lowestSequenceNumber = other.m_lowestSequenceNumber;
  m_recoverBelowFloor = other.m_recoverBelowFloor;
  m_belowFloorBaselined = other.m_belowFloorBaselined;
}

UnorderedHistoryCache &UnorderedHistoryCache::operator=(
    const UnorderedHistoryCache &other) {
  if (this == &other) {
    return *this;
  }

  // lock in address order to avoid deadlock when two caches assign to each other
  if (this < &other) {
    Lock lockThis{m_mutex};
    Lock lockOther{other.m_mutex};
    m_buffer = other.m_buffer;
    m_head = other.m_head;
    m_tail = other.m_tail;
    m_highestSequenceNumber = other.m_highestSequenceNumber;
    m_lowestSequenceNumber = other.m_lowestSequenceNumber;
    m_recoverBelowFloor = other.m_recoverBelowFloor;
    m_belowFloorBaselined = other.m_belowFloorBaselined;
  } else {
    Lock lockOther{other.m_mutex};
    Lock lockThis{m_mutex};
    m_buffer = other.m_buffer;
    m_head = other.m_head;
    m_tail = other.m_tail;
    m_highestSequenceNumber = other.m_highestSequenceNumber;
    m_lowestSequenceNumber = other.m_lowestSequenceNumber;
    m_recoverBelowFloor = other.m_recoverBelowFloor;
    m_belowFloorBaselined = other.m_belowFloorBaselined;
  }

  return *this;
}

bool UnorderedHistoryCache::isFull() const {
  Lock lock{m_mutex};
  return HistoryIterator::advance(m_head, 1) == m_tail;
}

bool UnorderedHistoryCache::isEmpty() const {
  Lock lock{m_mutex};
  return m_head == m_tail;
}

uint32_t UnorderedHistoryCache::size() const {
  Lock lock{m_mutex};
  return HistoryIterator::distance(m_tail, m_head);
}

const rtps::SequenceNumber_t &UnorderedHistoryCache::getSeqNumMin() const {
  Lock lock{m_mutex};
  if (m_head == m_tail) {
    return SEQUENCENUMBER_UNKNOWN;
  }
  return m_lowestSequenceNumber;
}

const rtps::SequenceNumber_t &UnorderedHistoryCache::getSeqNumMax() const {
  Lock lock{m_mutex};
  return m_highestSequenceNumber;
}

uint32_t UnorderedHistoryCache::wrapPos(uint32_t pos) const {
  return HistoryIterator::wrap(pos);
}

uint32_t UnorderedHistoryCache::snToPos(SequenceNumber_t sn) const {
  // caller must hold m_mutex
  uint32_t offset = sn - m_lowestSequenceNumber;
  return wrapPos(m_tail + offset);
}

const rtps::SequenceNumber_t &UnorderedHistoryCache::seqNumMinRefLocked() const {
  if (m_head == m_tail) {
    return SEQUENCENUMBER_UNKNOWN;
  }
  return m_lowestSequenceNumber;
}

const rtps::CacheChange *
UnorderedHistoryCache::getChangeBySNLocked(SequenceNumber_t sn) const {
  if (m_head == m_tail) {
    return nullptr;
  }
  if (sn < m_lowestSequenceNumber || m_highestSequenceNumber < sn) {
    return nullptr;
  }
  const uint32_t pos = snToPos(sn);
  return &m_buffer[pos];
}

const rtps::CacheChange *UnorderedHistoryCache::addChange(const uint8_t *data,
                                                          DataSize_t dataSize,
                                                          SequenceNumber_t sn,
                                                          EventId_t eventId) {
  Lock lock{m_mutex};
  CacheChange change;
  change.kind = ChangeKind_t::ALIVE_UNDELIVERED;
  change.data.reserve(dataSize);
  change.data.append(data, dataSize);
  change.sequenceNumber = sn;
  change.release = Time_t::fromMilliseconds(static_cast<int64_t>(timeNowMs()));
  change.eventId = eventId;

  if (m_head == m_tail) {
    const bool hasWatermark = m_highestSequenceNumber.high > 0 || m_highestSequenceNumber.low > 0;

    if (hasWatermark) {
      // cache was previously used and drained

      if (sn <= m_highestSequenceNumber) {
        // already delivered, discard
        RTPS_TRACE_PERF_EVENT(history_add_change,
            static_cast<uint64_t>(eventId),
            static_cast<uint32_t>(sn.low),
            static_cast<uint32_t>(5), // path kind too old by watermark
            static_cast<uint32_t>(0)); // stored
        return nullptr;
      }

      if (sn > m_highestSequenceNumber + 1) {
        // gap between watermark and incoming SN, fill with PENDING
        SequenceNumber_t gapStart = m_highestSequenceNumber + 1;
        uint32_t gap = sn - gapStart;
        uint32_t capacity = HISTORY_BUFFER_SIZE - 1;
        if (gap + 1 > capacity) {
          RTPS_TRACE_PERF_EVENT(history_add_change,
              static_cast<uint64_t>(eventId),
              static_cast<uint32_t>(sn.low),
              static_cast<uint32_t>(6), // path kind gap overflow
              static_cast<uint32_t>(0)); // stored
          return nullptr;
        }

        m_lowestSequenceNumber = gapStart;
        for (uint32_t i = 0; i < gap; ++i) {
          ++m_highestSequenceNumber;
          m_buffer[m_head].sequenceNumber = m_highestSequenceNumber;
          m_buffer[m_head].kind = ChangeKind_t::PENDING;
          m_buffer[m_head].data = PBufWrapper{};
          incrementHead();
        }

        m_highestSequenceNumber = sn;
        CacheChange *slot = &m_buffer[m_head];
        incrementHead();
        *slot = std::move(change);
        RTPS_TRACE_PERF_EVENT(history_add_change,
            static_cast<uint64_t>(eventId),
            static_cast<uint32_t>(sn.low),
            static_cast<uint32_t>(2), // path kind gap with PENDING fill
            static_cast<uint32_t>(1)); // stored
        return slot;
      }

      // contiguous with watermark, fall through to normal first entry path
    }

    // first entry, establish the SN range
    m_lowestSequenceNumber = sn;
    m_highestSequenceNumber = sn;
    CacheChange *slot = &m_buffer[m_head];
    incrementHead();
    *slot = std::move(change);
    RTPS_TRACE_PERF_EVENT(history_add_change,
        static_cast<uint64_t>(eventId),
        static_cast<uint32_t>(sn.low),
        static_cast<uint32_t>(0), // path kind first entry
        static_cast<uint32_t>(1)); // stored
    return slot;
  }

  if (sn == m_highestSequenceNumber + 1) {
    // regular next sequence number
    m_highestSequenceNumber = sn;
    CacheChange *slot = &m_buffer[m_head];
    incrementHead();
    *slot = std::move(change);
    RTPS_TRACE_PERF_EVENT(history_add_change,
        static_cast<uint64_t>(eventId),
        static_cast<uint32_t>(sn.low),
        static_cast<uint32_t>(1), // path kind contiguous
        static_cast<uint32_t>(1)); // stored
    return slot;

  } else if (sn > m_highestSequenceNumber + 1) {
    // gap, fill intermediates with PENDING, then store the arrived sample
    uint32_t gap = sn - (m_highestSequenceNumber + 1);

    // cap gap to available capacity to avoid overwriting the entire buffer
    uint32_t capacity = HISTORY_BUFFER_SIZE - 1 - HistoryIterator::distance(m_tail, m_head);
    if (gap + 1 > capacity) {
      // not enough room to represent this gap
      RTPS_TRACE_PERF_EVENT(history_add_change,
          static_cast<uint64_t>(eventId),
          static_cast<uint32_t>(sn.low),
          static_cast<uint32_t>(6), // path kind gap overflow
          static_cast<uint32_t>(0)); // stored
      return nullptr;
    }

    // fill PENDING placeholders for each missing intermediate SN
    for (uint32_t i = 0; i < gap; i++) {
      ++m_highestSequenceNumber;
      m_buffer[m_head].sequenceNumber = m_highestSequenceNumber;
      m_buffer[m_head].kind = ChangeKind_t::PENDING;
      m_buffer[m_head].data = PBufWrapper{};
      incrementHead();
    }

    m_highestSequenceNumber = sn;
    CacheChange *slot = &m_buffer[m_head];
    incrementHead();
    *slot = std::move(change);
    RTPS_TRACE_PERF_EVENT(history_add_change,
        static_cast<uint64_t>(eventId),
        static_cast<uint32_t>(sn.low),
        static_cast<uint32_t>(2), // path kind gap with PENDING fill
        static_cast<uint32_t>(1)); // stored
    return slot;

  } else if (sn >= m_lowestSequenceNumber) {
    // out of order or retransmission within tracked range
    uint32_t pos = snToPos(sn);
    CacheChange *slot = &m_buffer[pos];
    if (slot->kind == ChangeKind_t::PENDING) {
      assert(slot->sequenceNumber == sn);
      *slot = std::move(change);
      RTPS_TRACE_PERF_EVENT(history_add_change,
          static_cast<uint64_t>(eventId),
          static_cast<uint32_t>(sn.low),
          static_cast<uint32_t>(3), // path kind out of order filling PENDING
          static_cast<uint32_t>(1)); // stored
      return slot;
    }
    // already received, duplicate
    RTPS_TRACE_PERF_EVENT(history_add_change,
        static_cast<uint64_t>(eventId),
        static_cast<uint32_t>(sn.low),
        static_cast<uint32_t>(4), // path kind duplicate
        static_cast<uint32_t>(0)); // stored
    return nullptr;
  }

  // below m_lowestSequenceNumber, too old, discard
  RTPS_TRACE_PERF_EVENT(history_add_change,
      static_cast<uint64_t>(eventId),
      static_cast<uint32_t>(sn.low),
      static_cast<uint32_t>(5), // path kind too old
      static_cast<uint32_t>(0)); // stored
  return nullptr;
}

void UnorderedHistoryCache::registerGapImpl(SequenceNumber_t lastAvail) {
  if (m_head == m_tail) {
    return;
  }
  if (lastAvail <= m_highestSequenceNumber) {
    return;
  }

  uint32_t gap = lastAvail - m_highestSequenceNumber;
  uint32_t capacity = HISTORY_BUFFER_SIZE - 1 - HistoryIterator::distance(m_tail, m_head);
  if (gap > capacity) {
    gap = capacity;
  }
  uint32_t originalGap = gap;
  for (uint32_t i = 0; i < gap; i++) {
    ++m_highestSequenceNumber;
    m_buffer[m_head].sequenceNumber = m_highestSequenceNumber;
    m_buffer[m_head].kind = ChangeKind_t::PENDING;
    m_buffer[m_head].data = PBufWrapper{};
    incrementHead();
  }
  RTPS_TRACE_PERF_EVENT(history_register_gap,
      static_cast<uint32_t>(lastAvail.low),
      static_cast<uint32_t>(originalGap),
      static_cast<uint32_t>(gap));
}

void UnorderedHistoryCache::registerGap(SequenceNumber_t lastAvail) {
  Lock lock{m_mutex};
  registerGapImpl(lastAvail);
}

void UnorderedHistoryCache::ensureRangeInitializedImpl(
    SequenceNumber_t firstAvail, SequenceNumber_t lastAvail) {
  // only allowed to initialize an empty cache
  if (m_head != m_tail) {
    return;
  }

  // guard against invalid ranges
  if (firstAvail == SEQUENCENUMBER_UNKNOWN ||
      lastAvail == SEQUENCENUMBER_UNKNOWN ||
      lastAvail < firstAvail) {
    return;
  }

  // treat a zero range as nothing available
  if (firstAvail.high == 0
      && firstAvail.low == 0
      && lastAvail.high == 0
      && lastAvail.low == 0) {
    return;
  }

  // if we previously delivered data dont request SNs at or below the high water mark again, prevents heartbeat triggered retransmit of already delivered samples
  if (m_highestSequenceNumber.high > 0 || m_highestSequenceNumber.low > 0) {
    SequenceNumber_t minStart = m_highestSequenceNumber + 1;
    if (firstAvail < minStart) {
      firstAvail = minStart;
    }
    if (lastAvail < firstAvail) {
      return; // nothing new to request
    }
  }

  uint32_t capacity = HISTORY_BUFFER_SIZE - 1;
  uint32_t total = lastAvail - firstAvail + 1;
  if (total == 0) {
    return;
  }

  // if the advertised range exceeds what we can track, keep only the newest sequence numbers that fit
  SequenceNumber_t start = firstAvail;
  if (total > capacity) {
    start = lastAvail;
    start -= (capacity - 1);
    total = capacity;
  }

  m_lowestSequenceNumber = start;
  m_highestSequenceNumber = lastAvail;
  m_head = m_tail = 0;

  SequenceNumber_t sn = start;
  for (uint32_t i = 0; i < total; ++i, ++sn) {
    m_buffer[m_head].sequenceNumber = sn;
    m_buffer[m_head].kind = ChangeKind_t::PENDING;
    m_buffer[m_head].data = PBufWrapper{};
    incrementHead();
  }

  RTPS_TRACE_PERF_EVENT(history_ensure_range,
      static_cast<uint32_t>(firstAvail.low),
      static_cast<uint32_t>(lastAvail.low),
      static_cast<uint32_t>(start.low),
      static_cast<uint32_t>(total));
}

void UnorderedHistoryCache::ensureRangeInitialized(SequenceNumber_t firstAvail,
                                                   SequenceNumber_t lastAvail) {
  Lock lock{m_mutex};
  ensureRangeInitializedImpl(firstAvail, lastAvail);
}

rtps::SequenceNumberSet UnorderedHistoryCache::getMissingImpl() const {
  SequenceNumberSet set;

  if (m_head == m_tail) {
    set.base = m_lowestSequenceNumber;
    set.numBits = 0;
    RTPS_TRACE_PERF_EVENT(history_get_missing,
        static_cast<uint32_t>(set.base.low),
        static_cast<uint32_t>(set.numBits));
    return set;
  }

  // first PENDING entry blocks contiguous delivery so its the most critical to NACK
  uint32_t firstPending = m_head;
  {
    uint32_t scan = m_tail;
    while (scan != m_head) {
      if (m_buffer[scan].kind == ChangeKind_t::PENDING) {
        firstPending = scan;
        break;
      }
      scan = HistoryIterator::advance(scan, 1);
    }
  }

  if (firstPending == m_head) {
    // no PENDING entries at all, nothing to NACK
    set.base = m_buffer[m_tail].sequenceNumber;
    set.numBits = 0;
    RTPS_TRACE_PERF_EVENT(history_get_missing,
        static_cast<uint32_t>(set.base.low),
        static_cast<uint32_t>(set.numBits));
    return set;
  }

  set.base = m_buffer[firstPending].sequenceNumber;
  set.numBits = 0;

  uint32_t it = firstPending;
  uint32_t bit = 0;
  while (it != m_head && bit < SNS_NUM_BITS) {
    if (m_buffer[it].kind == ChangeKind_t::PENDING) {
      set.set(bit);
    }
    it = HistoryIterator::advance(it, 1);
    ++bit;
  }
  set.setCount();
  RTPS_TRACE_PERF_EVENT(history_get_missing,
      static_cast<uint32_t>(set.base.low),
      static_cast<uint32_t>(set.numBits));
  return set;
}

rtps::SequenceNumberSet UnorderedHistoryCache::getMissing() const {
  Lock lock{m_mutex};
  return getMissingImpl();
}

void UnorderedHistoryCache::setRecoverBelowFloor(bool enable) {
  Lock lock{m_mutex};
  m_recoverBelowFloor = enable;
}

// prepend PENDING slots from firstAvail up to the floor below the tail so lost samples from before the floor become requestable
void UnorderedHistoryCache::backfillBelowFloorImpl(SequenceNumber_t firstAvail) {
  if (m_head == m_tail) {
    return;
  }
  if (firstAvail == SEQUENCENUMBER_UNKNOWN ||
      (firstAvail.high == 0 && firstAvail.low == 0)) {
    return;
  }
  const SequenceNumber_t floor = m_buffer[m_tail].sequenceNumber;
  if (!(firstAvail < floor)) {
    return;
  }

  uint32_t gap = floor - firstAvail;
  const uint32_t used = HistoryIterator::distance(m_tail, m_head);
  const uint32_t capacity = HISTORY_BUFFER_SIZE - 1 - used;
  if (gap > capacity) {
    // only the part nearest the floor fits, the rest stays unrequested
    firstAvail = floor;
    firstAvail -= capacity;
    gap = capacity;
  }
  if (gap == 0) {
    return;
  }

  uint32_t pos = wrapPos(m_tail + HISTORY_BUFFER_SIZE - gap);
  const uint32_t newTail = pos;
  SequenceNumber_t sn = firstAvail;
  for (uint32_t i = 0; i < gap; ++i) {
    m_buffer[pos].sequenceNumber = sn;
    m_buffer[pos].kind = ChangeKind_t::PENDING;
    m_buffer[pos].data = PBufWrapper{};
    pos = HistoryIterator::advance(pos, 1);
    ++sn;
  }
  m_tail = newTail;
  m_lowestSequenceNumber = firstAvail;
}

void UnorderedHistoryCache::purgeUnavailableBelowImpl(SequenceNumber_t firstAvail) {
  // writers firstAvail is its history floor, PENDING slots below it can never fill so tombstone them to stop blocking delivery and NACKs
  uint32_t scan = m_tail;
  while (scan != m_head) {
    CacheChange &c = m_buffer[scan];
    if (!(c.sequenceNumber < firstAvail)) {
      break;
    }
    if (c.kind == ChangeKind_t::PENDING) {
      c.kind = ChangeKind_t::INVALID;
      RTPS_TRACE_PERF_EVENT(history_add_change,
          static_cast<uint64_t>(0),
          static_cast<uint32_t>(c.sequenceNumber.low),
          static_cast<uint32_t>(7), // path kind tombstoned below writer floor
          static_cast<uint32_t>(0)); // stored
    }
    scan = HistoryIterator::advance(scan, 1);
  }
}

rtps::SequenceNumberSet UnorderedHistoryCache::getMissingForHeartbeatRange(
    SequenceNumber_t firstAvail, SequenceNumber_t lastAvail) {
  Lock lock{m_mutex};

  purgeUnavailableBelowImpl(firstAvail);

  // builtin first baseline, make the whole advertised range requestable once, latched so it is not requested again
  if (m_recoverBelowFloor && !m_belowFloorBaselined) {
    m_belowFloorBaselined = true;
    if (m_head == m_tail) {
      m_highestSequenceNumber = SequenceNumber_t{0, 0};
      ensureRangeInitializedImpl(firstAvail, lastAvail);
    } else {
      backfillBelowFloorImpl(firstAvail);
      registerGapImpl(lastAvail);
    }
    return getMissingImpl();
  }

  if (m_head == m_tail) {
    ensureRangeInitializedImpl(firstAvail, lastAvail);
  } else {
    registerGapImpl(lastAvail);
  }
  return getMissingImpl();
}

void UnorderedHistoryCache::dropOldest() {
  Lock lock{m_mutex};
  if (m_head == m_tail) {
    return;
  }
  RTPS_TRACE_PERF_EVENT(history_drop_oldest,
      static_cast<uint32_t>(m_lowestSequenceNumber.low));
  SequenceNumber_t sn = m_lowestSequenceNumber;

  if (m_highestSequenceNumber <= sn) {
    m_head = m_tail;
    return;
  }

  while (m_tail != m_head && m_buffer[m_tail].sequenceNumber <= sn) {
    incrementTail();
  }
  if (m_tail != m_head) {
    m_lowestSequenceNumber = m_buffer[m_tail].sequenceNumber;
  }
}

void UnorderedHistoryCache::removeUntilIncl(SequenceNumber_t sn) {
  Lock lock{m_mutex};
  if (m_head == m_tail) {
    return;
  }

  if (m_highestSequenceNumber <= sn) {
    m_head = m_tail;
    return;
  }

  while (m_tail != m_head && m_buffer[m_tail].sequenceNumber <= sn) {
    incrementTail();
  }
  if (m_tail != m_head) {
    m_lowestSequenceNumber = m_buffer[m_tail].sequenceNumber;
  }
}

const rtps::CacheChange *
UnorderedHistoryCache::getChangeBySN(SequenceNumber_t sn) const {
  Lock lock{m_mutex};
  if (m_head == m_tail) {
    return nullptr;
  }
  if (sn < m_lowestSequenceNumber || m_highestSequenceNumber < sn) {
    return nullptr;
  }
  uint32_t pos = snToPos(sn);
  return &m_buffer[pos];
}

rtps::CacheChange *
UnorderedHistoryCache::getChangeBySN(SequenceNumber_t sn) {
  Lock lock{m_mutex};
  if (m_head == m_tail) {
    return nullptr;
  }
  if (sn < m_lowestSequenceNumber || m_highestSequenceNumber < sn) {
    return nullptr;
  }
  uint32_t pos = snToPos(sn);
  return &m_buffer[pos];
}

void UnorderedHistoryCache::incrementHead() {
  m_head = HistoryIterator::advance(m_head, 1);
  if (m_head == m_tail) {
    // buffer overflow, drop oldest and update lowestSequenceNumber
    m_tail = HistoryIterator::advance(m_tail, 1);
    if (m_tail != m_head) {
      m_lowestSequenceNumber = m_buffer[m_tail].sequenceNumber;
    }
  }
}

void UnorderedHistoryCache::incrementTail() {
  if (m_head != m_tail) {
    m_tail = HistoryIterator::advance(m_tail, 1);
  }
}

rtps::HistoryIterator UnorderedHistoryCache::begin() {
  return HistoryIterator(m_buffer.data(), m_tail, size());
}

rtps::HistoryIterator UnorderedHistoryCache::end() {
  return HistoryIterator(m_buffer.data(), m_head, 0);
}

rtps::ConstHistoryIterator UnorderedHistoryCache::begin() const {
  return ConstHistoryIterator(m_buffer.data(), m_tail, size());
}

rtps::ConstHistoryIterator UnorderedHistoryCache::end() const {
  return ConstHistoryIterator(m_buffer.data(), m_head, 0);
}



rtps::TransmissionPeriod_t UnorderedHistoryCache::detectMeanPeriod(uint32_t minSampleCount){
  Lock lock{m_mutex};
  const SequenceNumber_t &minSN = seqNumMinRefLocked();
  const SequenceNumber_t &maxSN = m_highestSequenceNumber;

  if (minSN == SEQUENCENUMBER_UNKNOWN || maxSN == SEQUENCENUMBER_UNKNOWN) {
    return INVALID_TRANSMISSION_PERIOD;
  }

  const uint32_t count = maxSN.low - minSN.low + 1;
  if (count < minSampleCount) {
    return INVALID_TRANSMISSION_PERIOD;
  }

  uint32_t numDeltas = 0;
  int64_t prevMs = 0;
  int64_t deltas[Config::HISTORY_SIZE]{};

  SequenceNumber_t sn = minSN;
  for (uint32_t i = 0; i < count && numDeltas < Config::HISTORY_SIZE; ++i, ++sn) {
    const CacheChange *change = getChangeBySNLocked(sn);
    
    if (change == nullptr) {
      return INVALID_TRANSMISSION_PERIOD;
    }
    
    const int64_t ms = change->release.toMilliseconds();
    if (i > 0) {
      deltas[numDeltas++] = ms - prevMs;
    }

    prevMs = ms;
  }

  int64_t sum = 0;
  for (uint32_t i = 0; i < numDeltas; ++i) {
    sum += deltas[i];
  }
  const int64_t mean = sum / static_cast<int64_t>(numDeltas);

  if (mean <= 0) {
    return INVALID_TRANSMISSION_PERIOD;
  }

  const int64_t tolerance = mean / 4; // 25 percent tolerance
  for (uint32_t i = 0; i < numDeltas; ++i) {
    const int64_t diff = deltas[i] - mean;
    if (diff > tolerance || diff < -tolerance) {
      return INVALID_TRANSMISSION_PERIOD;
    }
  }
  return mean;
}

bool UnorderedHistoryCache::gapOlderThen(uint32_t backMs) const{
  Lock lock{m_mutex};

  if (m_head == m_tail) {
   return false;
  }

  const SequenceNumber_t maxSN = m_highestSequenceNumber;

  // find the oldest PENDING entry
  uint32_t pendingPos = m_tail;
  bool found = false;
  {
    uint32_t scan = m_head;
    while (scan != m_tail) {
      if (m_buffer[scan].kind == ChangeKind_t::PENDING) {
        pendingPos = scan;
        found = true;
        break;
      }
      scan = wrapPos(scan + 1);
    }
  }

  if (!found) {
    return false;
  }

  const CacheChange* headChange = getChangeBySNLocked(maxSN);
  if (headChange == nullptr) {
    return false;
  }

  const int64_t delta = headChange->release.toMilliseconds()
                      - m_buffer[pendingPos].release.toMilliseconds();
  return delta > static_cast<int64_t>(backMs);
}

