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

#include "rtps/storages/SetCache.h"
#include "rtps/utils/sysFunctions.h"

using rtps::SetCache;

bool SetCache::isFull() const {
  return TransientHistoryIterator::advance(m_head, 1) == m_tail;
}

bool SetCache::isEmpty() const { return m_head == m_tail; }

uint32_t SetCache::size() const {
  return TransientHistoryIterator::distance(m_tail, m_head);
}

const rtps::TransientCacheChange *SetCache::addChange(const uint8_t *data,
                                                      DataSize_t size,
                                                      Locator loc, Guid_t guid,
                                                      EventId_t eventId) {
  TransientCacheChange change;
  change.kind = ChangeKind_t::ALIVE;
  change.data.reserve(size);
  change.data.append(data, size);
  change.locator = loc;
  change.remoteReader = guid;
  change.id = ++m_lastUsedId;
  change.release = Time_t::fromMilliseconds(static_cast<int64_t>(timeNowMs()));
  change.eventId = eventId;

  TransientCacheChange *place = &m_buffer[m_head];
  incrementHead();

  *place = std::move(change);
  return place;
}

const rtps::TransientCacheChange *SetCache::getChangeById(ChangeId_t id) const {
  if (isEmpty()) {
    return nullptr;
  }
  if (id < getIDMin() || getIDMax() < id) {
    return nullptr;
  }
  const TransientCacheChange *change = &m_buffer[idToPos(id)];
  if (change->kind == ChangeKind_t::INVALID) {
    return nullptr;
  }
  return change;
}

void SetCache::dropByID(ChangeId_t id) {
  if (isEmpty() || id < getIDMin() || getIDMax() < id) {
    return;
  }
  // tombstone the slot, the id to pos mapping for the rest stays intact
  m_buffer[idToPos(id)].kind = ChangeKind_t::INVALID;
  // reclaim any tombstoned entries that are now at the front
  while (m_tail != m_head && m_buffer[m_tail].kind == ChangeKind_t::INVALID) {
    incrementTail();
  }
}

void SetCache::removeUntilIncl(ChangeId_t id) {
  if (isEmpty()) {
    return;
  }
  if (getIDMax() <= id) {
    m_head = m_tail;
    return;
  }
  while (m_tail != m_head && m_buffer[m_tail].id <= id) {
    incrementTail();
  }
}

rtps::ChangeId_t SetCache::getIDMin() const {
  if (isEmpty()) {
    return 0;
  }
  return m_buffer[m_tail].id;
}

rtps::ChangeId_t SetCache::getIDMax() const {
  if (isEmpty()) {
    return 0;
  }
  return m_lastUsedId;
}

uint32_t SetCache::wrapPos(uint32_t pos) const {
  return TransientHistoryIterator::wrap(pos);
}

uint32_t SetCache::idToPos(ChangeId_t id) const {
  uint32_t offset = static_cast<uint32_t>(id - getIDMin());
  return wrapPos(m_tail + offset);
}

void SetCache::incrementHead() {
  m_head = TransientHistoryIterator::advance(m_head, 1);
  if (m_head == m_tail) {
    // overflow, advance tail directly, incrementTail would do nothing here
    m_tail = TransientHistoryIterator::advance(m_tail, 1);
  }
}

void SetCache::incrementTail() {
  if (m_head != m_tail) {
    m_tail = TransientHistoryIterator::advance(m_tail, 1);
  }
}

rtps::TransientHistoryIterator SetCache::begin() {
  return TransientHistoryIterator(m_buffer.data(), m_tail, size());
}

rtps::TransientHistoryIterator SetCache::end() {
  return TransientHistoryIterator(m_buffer.data(), m_head, 0);
}

rtps::ConstTransientHistoryIterator SetCache::begin() const {
  return ConstTransientHistoryIterator(m_buffer.data(), m_tail, size());
}

rtps::ConstTransientHistoryIterator SetCache::end() const {
  return ConstTransientHistoryIterator(m_buffer.data(), m_head, 0);
}
