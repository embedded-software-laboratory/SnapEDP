#ifndef RTPS_HISTORYCACHEITERATOR_H
#define RTPS_HISTORYCACHEITERATOR_H

#include <cstdint>
#include <iterator>
#include "rtps/storages/CacheChange.h"
#include "rtps/config.h"

namespace rtps {

template <typename T, uint32_t BufferSize>
class HistoryCacheIterator {
public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = T;
  using difference_type = int32_t;
  using pointer = T *;
  using reference = T &;

  HistoryCacheIterator() = default;

  HistoryCacheIterator(T *buffer, uint32_t pos, uint32_t count)
      : m_buffer(buffer), m_pos(pos), m_remaining(count) {}

  reference operator*() const { return m_buffer[m_pos]; }
  pointer operator->() const { return &m_buffer[m_pos]; }

  HistoryCacheIterator &operator++() {
    advance();
    return *this;
  }

  HistoryCacheIterator operator++(int) {
    HistoryCacheIterator tmp(*this);
    advance();
    return tmp;
  }

  bool operator==(const HistoryCacheIterator &other) const {
    return m_remaining == other.m_remaining;
  }

  bool operator!=(const HistoryCacheIterator &other) const {
    return !(*this == other);
  }

  uint32_t pos() const { return m_pos; }

  static uint32_t wrap(uint32_t pos) {
    return pos < BufferSize ? pos : pos - BufferSize;
  }

  static uint32_t distance(uint32_t from, uint32_t to) {
    if (to >= from) {
      return to - from;
    }
    return BufferSize - from + to;
  }

  static uint32_t advance(uint32_t pos, uint32_t n) {
    pos += n;
    if (pos >= BufferSize) {
      pos -= BufferSize;
    }
    return pos;
  }


private:
  T *m_buffer = nullptr;
  uint32_t m_pos = 0;
  uint32_t m_remaining = 0;

  void advance() {
    if (m_remaining > 0) {
      --m_remaining;
      if (++m_pos >= BufferSize) {
        m_pos = 0;
      }
    }
  }
};

template <typename T, uint32_t BufferSize>
class ConstHistoryCacheIterator {
public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = T;
  using difference_type = int32_t;
  using pointer = const T *;
  using reference = const T &;

  ConstHistoryCacheIterator() = default;

  ConstHistoryCacheIterator(const T *buffer, uint32_t pos, uint32_t count)
      : m_buffer(buffer), m_pos(pos), m_remaining(count) {}

  reference operator*() const { return m_buffer[m_pos]; }
  pointer operator->() const { return &m_buffer[m_pos]; }

  ConstHistoryCacheIterator &operator++() {
    advance();
    return *this;
  }

  ConstHistoryCacheIterator operator++(int) {
    ConstHistoryCacheIterator tmp(*this);
    advance();
    return tmp;
  }

  bool operator==(const ConstHistoryCacheIterator &other) const {
    return m_remaining == other.m_remaining;
  }

  bool operator!=(const ConstHistoryCacheIterator &other) const {
    return !(*this == other);
  }

  uint32_t pos() const { return m_pos; }

private:
  const T *m_buffer = nullptr;
  uint32_t m_pos = 0;
  uint32_t m_remaining = 0;

  void advance() {
    if (m_remaining > 0) {
      --m_remaining;
      if (++m_pos >= BufferSize) {
        m_pos = 0;
      }
    }
  }
};

static constexpr uint32_t HISTORY_BUFFER_SIZE = Config::HISTORY_SIZE + 1;

using HistoryIterator = HistoryCacheIterator<CacheChange, HISTORY_BUFFER_SIZE>;
using ConstHistoryIterator =
    ConstHistoryCacheIterator<CacheChange, HISTORY_BUFFER_SIZE>;

} // namespace rtps

#endif // RTPS_HISTORYCACHEITERATOR_H
