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

#include "rtps/storages/PBufWrapper.h"
#include "rtps/utils/Log.h"

using rtps::PBufWrapper;

#if PBUF_WRAP_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define PBUF_WRAP_LOG(...)                                                     \
  if (true) {                                                                  \
    printf("[PBUF Wrapper] ");                                                 \
    printf(__VA_ARGS__);                                                       \
    printf("\n");                                                              \
  }
#else
#define PBUF_WRAP_LOG(...) RTPS_TRACE_LOG_EMIT("PBufWrapper", __VA_ARGS__)
#endif

#include <algorithm>

PBufWrapper::PBufWrapper(DataSize_t length)
    : m_buf(length) {
  if (isValid()) {
    m_freeSpace = length;
  }
}

PBufWrapper::PBufWrapper(std::vector<uint8_t> &bufferToWrap) {
  m_buf = bufferToWrap;
  m_freeSpace = 0;
}

PBufWrapper::PBufWrapper(std::vector<uint8_t> &&bufferToWrap) {
  m_buf = std::move(bufferToWrap);
  m_freeSpace = 0;
}

PBufWrapper PBufWrapper::deepCopy() const {
  return *this;
}


bool PBufWrapper::isValid() const { return !m_buf.empty(); }

rtps::DataSize_t PBufWrapper::spaceLeft() const { return m_freeSpace; }

rtps::DataSize_t PBufWrapper::spaceUsed() const {
  return m_buf.size() - m_freeSpace;
}

bool PBufWrapper::append(const uint8_t *data, DataSize_t length) {
  if (data == nullptr) {
    return false;
  }

  if(m_freeSpace >= length) {
    std::copy(data, data + length, m_buf.data() + m_buf.size() - m_freeSpace);
    m_freeSpace -= length;
    return true;
  } else {
    return false;
  }
}

void PBufWrapper::append(PBufWrapper &&other) {
  if (this == &other) {
    return;
  }
  if (m_buf.empty()) {
    *this = std::move(other);
    return;
  }

  // free space will be kept and free space of other buffer will be copied
  auto newBufBegin = m_buf.size();
  m_buf.resize(m_buf.size() + other.m_buf.size());
  std::copy(other.m_buf.begin(), other.m_buf.end(), m_buf.begin() + newBufBegin);

  m_freeSpace = other.m_freeSpace;
  other.m_buf.clear();
  other.m_freeSpace = 0;
}

bool PBufWrapper::reserve(DataSize_t length) {
  auto additionalAllocation = length - m_freeSpace;
  if (additionalAllocation <= 0) {
    return true;
  }

  return increaseSizeBy(additionalAllocation);
}

void PBufWrapper::reset() {
  m_freeSpace = m_buf.size();
}

bool PBufWrapper::increaseSizeBy(uint16_t length) {
  m_buf.resize(m_buf.size() + length);
  m_freeSpace += length;

  return true;
}

#undef PBUF_WRAP_VERBOSE
