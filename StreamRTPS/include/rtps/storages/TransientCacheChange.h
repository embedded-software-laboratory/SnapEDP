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

#ifndef PROJECT_TRANSIENTCACHECHANGE_H
#define PROJECT_TRANSIENTCACHECHANGE_H

#include "rtps/common/types.h"
#include "rtps/common/Locator.h"
#include "rtps/storages/PBufWrapper.h"

namespace rtps {
struct TransientCacheChange {
  ChangeKind_t kind = ChangeKind_t::INVALID;
  Locator locator = {.address = LOCATOR_ADDRESS_INVALID};
  Guid_t remoteReader = GUID_UNKNOWN;
  ChangeId_t id = 0;
  Time_t release;
  EventId_t eventId = 0;
  PBufWrapper data{};

  TransientCacheChange() = default;
  TransientCacheChange(ChangeId_t id,  Guid_t rec, Locator loc, EventId_t eventId = 0)
      : locator(loc), remoteReader(rec), id(id), eventId(eventId){};
};
} // namespace rtps

#endif // PROJECT_TRANSIENTCACHECHANGE_H
