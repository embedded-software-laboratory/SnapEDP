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

#ifndef PROJECT_SetCache_H
#define PROJECT_SetCache_H

#include "rtps/config.h"
#include "rtps/storages/TransientCacheChange.h"
#include "rtps/storages/HistoryCacheIterator.h"

namespace rtps {

// transient samples carry no SN, the ring is keyed on the monotonic ChangeId_t
using TransientHistoryIterator =
    HistoryCacheIterator<TransientCacheChange, HISTORY_BUFFER_SIZE>;
using ConstTransientHistoryIterator =
    ConstHistoryCacheIterator<TransientCacheChange, HISTORY_BUFFER_SIZE>;

// simplest history for transient samples with no SN, fire and forget, used in SEDP gossip, a small break with DDS norms but necessary

class SetCache {
public:
  SetCache() = default;

  bool isFull() const;
  bool isEmpty() const;
  uint32_t size() const;
  const TransientCacheChange *addChange(const uint8_t *data, DataSize_t size,
                              Locator loc, Guid_t guid, EventId_t eventId = 0);

  const TransientCacheChange *getChangeById(ChangeId_t id) const;
  void dropByID(ChangeId_t id);
  void removeUntilIncl(ChangeId_t id);

  ChangeId_t getIDMin() const;
  ChangeId_t getIDMax() const;

  TransientHistoryIterator begin();
  TransientHistoryIterator end();
  ConstTransientHistoryIterator begin() const;
  ConstTransientHistoryIterator end() const;

private:
  std::array<TransientCacheChange, HISTORY_BUFFER_SIZE> m_buffer{};
  uint32_t m_head = 0;
  uint32_t m_tail = 0;
  ChangeId_t m_lastUsedId = 0;

  void incrementHead();
  void incrementTail();

  uint32_t wrapPos(uint32_t pos) const;
  uint32_t idToPos(ChangeId_t id) const;
};
} // namespace rtps

#endif // PROJECT_SetCache_H
