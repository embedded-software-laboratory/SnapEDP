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

#ifndef RTPS_FIXEDMAP_H
#define RTPS_FIXEDMAP_H

#include <array>
#include <cstdint>

namespace rtps {

// fixed capacity map, array plus count plus linear scan, no heap, key needs equality, order not stable since erase swap pops
template <typename Key, typename Value, uint16_t CAP> class FixedMap {
public:
  struct Entry {
    Key key;
    Value value;
  };

  // pointer to the value for key, or nullptr if absent
  Value *find(const Key &key) {
    for (uint16_t i = 0; i < m_count; ++i) {
      if (m_items[i].key == key) {
        return &m_items[i].value;
      }
    }
    return nullptr;
  }

  const Value *find(const Key &key) const {
    for (uint16_t i = 0; i < m_count; ++i) {
      if (m_items[i].key == key) {
        return &m_items[i].value;
      }
    }
    return nullptr;
  }

  // overwrite if present, else insert, silently ignored when full
  void insertOrAssign(const Key &key, const Value &value) {
    if (Value *existing = find(key)) {
      *existing = value;
      return;
    }
    if (m_count >= CAP) {
      return;
    }
    m_items[m_count++] = {key, value};
  }

  // true if erased
  bool erase(const Key &key) {
    for (uint16_t i = 0; i < m_count; ++i) {
      if (m_items[i].key == key) {
        m_items[i] = m_items[m_count - 1];
        --m_count;
        return true;
      }
    }
    return false;
  }

  void clear() { m_count = 0; }
  uint16_t size() const { return m_count; }

  Entry *begin() { return m_items.data(); }
  Entry *end() { return m_items.data() + m_count; }
  const Entry *begin() const { return m_items.data(); }
  const Entry *end() const { return m_items.data() + m_count; }

private:
  std::array<Entry, CAP> m_items{};
  uint16_t m_count = 0;
};

} // namespace rtps

#endif // RTPS_FIXEDMAP_H
