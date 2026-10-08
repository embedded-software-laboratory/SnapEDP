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

#ifndef RTPS_THREADSAFEQUEUE_H
#define RTPS_THREADSAFEQUEUE_H

#include "rtps/utils/Lock.h"

#include <array>
#include <limits>

namespace rtps {

template <typename T, uint16_t SIZE> class ThreadSafeCircularBuffer {

public:
  bool init();

  ~ThreadSafeCircularBuffer();

  bool moveElementIntoBuffer(T &&elem);

  // moves the first element into hull and transfers resource ownership, false if empty
  bool moveFirstInto(T &hull);

  void clear();

  // current number of elements, lock free snapshot
  uint16_t size() const;

private:
  std::array<T, SIZE + 1> m_buffer{};
  uint16_t m_head = 0;
  uint16_t m_tail = 0;
  static_assert(SIZE + 1 < std::numeric_limits<decltype(m_head)>::max(),
                "Iterator is large enough for given size");

  Mutex m_mutex;
  bool m_initialized = false;

  inline bool isFull();
  inline void incrementIterator(uint16_t &iterator);
  inline void incrementTail();
  inline void incrementHead();
};

} // namespace rtps

#include "ThreadSafeCircularBuffer.tpp"

#endif // RTPS_THREADSAFEQUEUE_H
