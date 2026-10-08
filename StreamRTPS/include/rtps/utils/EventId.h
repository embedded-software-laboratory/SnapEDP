/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University
*/

#ifndef RTPS_EVENT_ID_H
#define RTPS_EVENT_ID_H

#include "rtps/common/types.h"

#include <atomic>
#include <chrono>

namespace rtps {

inline EventId_t generateEventId() {
  static std::atomic<EventId_t> lastId{0};
  const auto now = static_cast<EventId_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  EventId_t expected = lastId.load(std::memory_order_relaxed);
  while (true) {
    EventId_t desired = (now <= expected) ? (expected + 1) : now;
    if (lastId.compare_exchange_weak(expected, desired,
                                     std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
      return desired;
    }
  }
}

} // namespace rtps

#endif // RTPS_EVENT_ID_H
