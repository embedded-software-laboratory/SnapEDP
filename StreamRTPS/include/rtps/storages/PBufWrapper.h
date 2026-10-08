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

#ifndef RTPS_PBUFWRAPPER_H
#define RTPS_PBUFWRAPPER_H

#include "rtps/common/types.h"
#include <vector>
#include <cstdint>

namespace rtps {

struct PBufWrapper {

  std::vector<uint8_t> m_buf;

  PBufWrapper() = default;
  explicit PBufWrapper(std::vector<uint8_t> &bufferToWrap);
  explicit PBufWrapper(std::vector<uint8_t> &&bufferToWrap);
  explicit PBufWrapper(DataSize_t length);

  PBufWrapper(const PBufWrapper &other) = default;
  PBufWrapper &operator=(const PBufWrapper &other) = default;

  PBufWrapper(PBufWrapper &&other) noexcept = default;
  PBufWrapper &operator=(PBufWrapper &&other) noexcept = default;

  ~PBufWrapper() = default;

  PBufWrapper deepCopy() const;

  bool isValid() const;

  bool append(const uint8_t *data, DataSize_t length);

  // unused reserved memory stays part of the wrapper, later appends continue behind the appended wrapper
  void append(PBufWrapper &&other);

  bool reserve(DataSize_t length);

  // after this, data is added from the beginning again, does not revert reserve
  void reset();

  DataSize_t spaceLeft() const;
  DataSize_t spaceUsed() const;

private:
  DataSize_t m_freeSpace = 0;

  bool increaseSizeBy(uint16_t length);

  void copySimpleMembersAndResetBuffer(const PBufWrapper &other);
};

} // namespace rtps

#endif // RTPS_PBUFWRAPPER_H
