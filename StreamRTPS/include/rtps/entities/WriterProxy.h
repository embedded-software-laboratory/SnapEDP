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

#ifndef RTPS_WRITERPROXY_H
#define RTPS_WRITERPROXY_H

#include "rtps/common/Locator.h"
#include "rtps/common/types.h"
#include "rtps/entities/transient/AckNackPolicy.h"
#include "rtps/storages/UnorderedHistoryCache.h"


namespace rtps {
struct WriterProxy {
  static constexpr uint32_t REORDER_GRACE_MS = 5;
  static constexpr uint32_t MIN_RENACK_MS    = 10;

  Guid_t remoteWriterGuid;
  UnorderedHistoryCache history;
  Count_t ackNackCount;
  Count_t hbCount;
  Locator remoteLocator;
  AckNackPolicy ackNackPolicy;

  int64_t lastFastNackMs = 0;
  SequenceNumber_t firstPendingSN{0, 0};
  int64_t firstPendingMs = 0;

  // last advertised heartbeat range, the single source for ACKNACK content
  SequenceNumber_t lastHbFirstSN{0, 0};
  SequenceNumber_t lastHbLastSN{0, 0};

  bool hasHeartbeatRange() const {
    return lastHbLastSN.high != 0 || lastHbLastSN.low != 0;
  }


  WriterProxy() = default;

  WriterProxy(const Guid_t &guid, const Locator &loc)
      : remoteWriterGuid(guid),
        ackNackCount{1}, hbCount{0},
        remoteLocator(loc) {}

  SequenceNumber_t expectedSN() const {
    const SequenceNumber_t maxSN = history.getSeqNumMax();
    if (maxSN.high > 0 || maxSN.low > 0) {
      return maxSN + 1;
    }
    return SequenceNumber_t{0, 1};
  }

  SequenceNumberSet getMissing(const SequenceNumber_t &firstAvail,
                               const SequenceNumber_t &lastAvail) {
    SequenceNumberSet set = history.getMissingForHeartbeatRange(firstAvail, lastAvail);
    if (set.numBits == 0) {
      set.base = {0,0};
    }
    return set;
  }

  Count_t getNextAckNackCount() {
    const Count_t tmp = ackNackCount;
    ++ackNackCount.value;
    return tmp;
  }
};
} // namespace rtps

#endif // RTPS_WRITERPROXY_H
