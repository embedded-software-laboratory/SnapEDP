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

#ifndef RTPS_FIXEDSET_H
#define RTPS_FIXEDSET_H

#include <array>
#include <cstdint>

namespace rtps {

// fixed capacity set, array plus count plus linear scan, no heap, T needs equality, order not stable since remove swap pops
template <typename T, uint16_t CAP> class FixedSet {
public:
  bool contains(const T &x) const {
    for (uint16_t i = 0; i < m_count; ++i) {
      if (m_items[i] == x) {
        return true;
      }
    }
    return false;
  }

  // true if newly added, false if already present or full
  bool add(const T &x) {
    if (contains(x)) {
      return false;
    }
    if (m_count >= CAP) {
      return false;
    }
    m_items[m_count++] = x;
    return true;
  }

  // true if removed
  bool remove(const T &x) {
    for (uint16_t i = 0; i < m_count; ++i) {
      if (m_items[i] == x) {
        m_items[i] = m_items[m_count - 1];
        --m_count;
        return true;
      }
    }
    return false;
  }

  void clear() { m_count = 0; }
  bool empty() const { return m_count == 0; }
  uint16_t size() const { return m_count; }

  T *begin() { return m_items.data(); }
  T *end() { return m_items.data() + m_count; }
  const T *begin() const { return m_items.data(); }
  const T *end() const { return m_items.data() + m_count; }

private:
  std::array<T, CAP> m_items{};
  uint16_t m_count = 0;
};

} // namespace rtps

#endif // RTPS_FIXEDSET_H
