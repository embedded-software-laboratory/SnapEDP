#ifndef RTPS_THREADSAFEGROWABLEQUEUE_H
#define RTPS_THREADSAFEGROWABLEQUEUE_H

// deque backed drop in for ThreadSafeCircularBuffer, grows on demand to absorb bursts and drops only at MAX_SIZE, T only needs default construction and move assignment so PacketInfo works

#include "rtps/utils/Lock.h"

#include <cstddef>
#include <cstdint>
#include <deque>

namespace rtps {

template <typename T, std::size_t MAX_SIZE> class ThreadSafeGrowableQueue {

public:
  bool init() { return true; }

  // grows the queue, returns false only at MAX_SIZE so the caller can trace the drop
  bool moveElementIntoBuffer(T &&elem) {
    Lock lock(m_mutex);
    if (m_buffer.size() >= MAX_SIZE) {
      return false;
    }
    // default construct then move assign so T needs no move or copy ctor
    m_buffer.emplace_back();
    m_buffer.back() = std::move(elem);
    return true;
  }

  // moves the first element into the hull, transferring ownership of resources
  bool moveFirstInto(T &hull) {
    Lock lock(m_mutex);
    if (m_buffer.empty()) {
      return false;
    }
    hull = std::move(m_buffer.front());
    m_buffer.pop_front();
    return true;
  }

  void clear() {
    Lock lock(m_mutex);
    m_buffer.clear();
  }

  uint16_t size() const {
    Lock lock(m_mutex);
    return static_cast<uint16_t>(m_buffer.size());
  }

private:
  std::deque<T> m_buffer;
  mutable Mutex m_mutex;
};

} // namespace rtps

#endif // RTPS_THREADSAFEGROWABLEQUEUE_H
