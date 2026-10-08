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

#include "rtps/storages/OrderedHistoryCache.h"
#include "rtps/utils/sysFunctions.h"

using rtps::OrderedHistoryCache;

OrderedHistoryCache::OrderedHistoryCache(SequenceNumber_t lastUsed)
    : OrderedHistoryCache() {
  m_lastUsedSequenceNumber = lastUsed;
}

bool OrderedHistoryCache::isFull() const {
  
  return HistoryIterator::advance(m_head, 1) == m_tail;
}

bool OrderedHistoryCache::isEmpty() const { 
  return m_head == m_tail; 
}

uint32_t OrderedHistoryCache::size() const {
  return HistoryIterator::distance(m_tail, m_head);
}

const rtps::SequenceNumber_t &OrderedHistoryCache::getSeqNumMin() const {
  
  if (isEmpty()) {
    return SEQUENCENUMBER_UNKNOWN;
  }
  return m_buffer[m_tail].sequenceNumber;
}


const rtps::SequenceNumber_t &OrderedHistoryCache::getSeqNumMax() const {
  
  if (isEmpty()) {
    return SEQUENCENUMBER_UNKNOWN;
  }
  return m_lastUsedSequenceNumber;
}

rtps::SequenceNumber_t OrderedHistoryCache::getSeqNumMaxSent() const {
  return m_seqNumMaxSent;
}

void OrderedHistoryCache::markAsSent(SequenceNumber_t sn) {
  if (m_seqNumMaxSent == SEQUENCENUMBER_UNKNOWN || m_seqNumMaxSent < sn) {
    m_seqNumMaxSent = sn;
  }
}

void OrderedHistoryCache::resetMaxSent() {
  m_seqNumMaxSent = SEQUENCENUMBER_UNKNOWN;
}


uint32_t OrderedHistoryCache::wrapPos(uint32_t pos) const {
  return HistoryIterator::wrap(pos);
}

uint32_t OrderedHistoryCache::snToPos(SequenceNumber_t sn) const {
  uint32_t offset = sn - getSeqNumMin();
  return wrapPos(m_tail + offset);
}

const rtps::CacheChange *OrderedHistoryCache::addChange(const uint8_t *data,
                                                       DataSize_t size,
                                                       EventId_t eventId) {
  CacheChange change;
  change.kind = ChangeKind_t::ALIVE_UNDELIVERED;
  change.data.reserve(size);
  change.data.append(data, size);
  change.sequenceNumber = ++m_lastUsedSequenceNumber;
  change.release = Time_t::fromMilliseconds(static_cast<int64_t>(timeNowMs()));
  change.eventId = eventId;

  CacheChange *place = &m_buffer[m_head];
  incrementHead();

  *place = std::move(change);
  return place;
}

void OrderedHistoryCache::dropOldest() { removeUntilIncl(getSeqNumMin()); }

void OrderedHistoryCache::removeUntilIncl(SequenceNumber_t sn) {
  if (isEmpty()) {
    return;
  }

  if (getSeqNumMax() <= sn) {
    m_head = m_tail;
    return;
  }

  while (m_tail != m_head && m_buffer[m_tail].sequenceNumber <= sn) {
    incrementTail();
  }
}

const rtps::CacheChange *
OrderedHistoryCache::getChangeBySN(SequenceNumber_t sn) const {
  if (isEmpty()) {
    return nullptr;
  }
  SequenceNumber_t minSN = getSeqNumMin();
  if (sn < minSN || getSeqNumMax() < sn) {
    return nullptr;
  }
  uint32_t pos = snToPos(sn);
  return &m_buffer[pos];
}

void OrderedHistoryCache::incrementHead() {
  m_head = HistoryIterator::advance(m_head, 1);
  if (m_head == m_tail) {
    // buffer overflow, advance tail directly, incrementTail would do nothing here and the buffer would look empty
    m_tail = HistoryIterator::advance(m_tail, 1);
  }
}

void OrderedHistoryCache::incrementTail() {
  if (m_head != m_tail) {
    m_tail = HistoryIterator::advance(m_tail, 1);
  }
}

rtps::HistoryIterator OrderedHistoryCache::begin() {
  return HistoryIterator(m_buffer.data(), m_tail, size());
}

rtps::HistoryIterator OrderedHistoryCache::end() {
  return HistoryIterator(m_buffer.data(), m_head, 0);
}

rtps::ConstHistoryIterator OrderedHistoryCache::begin() const {
  return ConstHistoryIterator(m_buffer.data(), m_tail, size());
}

rtps::ConstHistoryIterator OrderedHistoryCache::end() const {
  return ConstHistoryIterator(m_buffer.data(), m_head, 0);
}
