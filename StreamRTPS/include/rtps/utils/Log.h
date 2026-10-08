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

#ifndef RTPS_LOG_H
#define RTPS_LOG_H

#include <cstdio>
#include <cstring>
#include <stdarg.h>

// default OFF, RTPS_LOG is debug only printf that floods the hot recv path, enable with RTPS_GLOBAL_VERBOSE
#ifndef RTPS_GLOBAL_VERBOSE
#define RTPS_GLOBAL_VERBOSE 0
#endif

#ifndef TRACE_ALL
#define TRACE_ALL 1
#endif

// default OFF, PERF events fire per packet and blow up CTF traces, enable with TRACE_PERF
#ifndef TRACE_PERF
#define TRACE_PERF 0
#endif

#ifndef TRACE_LOG
#define TRACE_LOG 0
#endif

#ifndef SFW_VERBOSE
#define SFW_VERBOSE 0
#endif
#ifndef SPDP_VERBOSE
#define SPDP_VERBOSE 1
#endif
#ifndef PBUF_WRAP_VERBOSE
#define PBUF_WRAP_VERBOSE 0
#endif
#ifndef AGR_VERBOSE
#define AGR_VERBOSE 0
#endif
#ifndef SEDP_VERBOSE
#define SEDP_VERBOSE 1
#endif
#ifndef SNAP_VERBOSE
#define SNAP_VERBOSE 1
#endif
#ifndef RECV_VERBOSE
#define RECV_VERBOSE 0
#endif
#ifndef PARTICIPANT_VERBOSE
#define PARTICIPANT_VERBOSE 1
#endif
#ifndef DOMAIN_VERBOSE
#define DOMAIN_VERBOSE 1
#endif
#ifndef UDP_DRIVER_VERBOSE
#define UDP_DRIVER_VERBOSE 0
#endif
#ifndef TSCB_VERBOSE
#define TSCB_VERBOSE 0
#endif
#ifndef SLW_VERBOSE
#define SLW_VERBOSE 0
#endif
#ifndef SFR_VERBOSE
#define SFR_VERBOSE 0
#endif
#ifndef SLR_VERBOSE
#define SLR_VERBOSE 0
#endif
#ifndef SERDE_VERBOSE
#define SERDE_VERBOSE 0
#endif
#ifndef MSG_TYPES_VERBOSE
#define MSG_TYPES_VERBOSE 0
#endif
#ifndef THREAD_POOL_VERBOSE
#define THREAD_POOL_VERBOSE 0
#endif
#ifndef INIT_VERBOSE
#define INIT_VERBOSE 0
#endif

#define RTPS_FILENAME (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#define RTPS_LOG_ENABLED(SUBSYS_FLAG) ((RTPS_GLOBAL_VERBOSE) && (SUBSYS_FLAG))

#ifndef RTPS_TRACE_LOG_EMIT
#define RTPS_TRACE_LOG_EMIT(SUBSYS, FMT, ...) ((void)0)
#endif

#define RTPS_LOG(SUBSYS, SUBSYS_FLAG, FMT, ...)                                \
  do {                                                                          \
    if (RTPS_LOG_ENABLED(SUBSYS_FLAG)) {                                        \
      std::printf("[%s][%s:%d][%s]\n", (SUBSYS), RTPS_FILENAME, __LINE__,      \
                  rtps::logFormat(FMT, ##__VA_ARGS__));                         \
    }                                                                           \
    RTPS_TRACE_LOG_EMIT(SUBSYS, FMT, ##__VA_ARGS__);                            \
  } while (0)

#define RTPS_LOGD(SUBSYS, SUBSYS_FLAG, FMT, ...) RTPS_LOG(SUBSYS, SUBSYS_FLAG, FMT, ##__VA_ARGS__)
#define RTPS_LOGI(SUBSYS, SUBSYS_FLAG, FMT, ...) RTPS_LOG(SUBSYS, SUBSYS_FLAG, FMT, ##__VA_ARGS__)
#define RTPS_LOGW(SUBSYS, SUBSYS_FLAG, FMT, ...) RTPS_LOG(SUBSYS, SUBSYS_FLAG, FMT, ##__VA_ARGS__)
#define RTPS_LOGE(SUBSYS, SUBSYS_FLAG, FMT, ...) RTPS_LOG(SUBSYS, SUBSYS_FLAG, FMT, ##__VA_ARGS__)

namespace rtps {
template <typename... Args>
const char *logFormat(const char *fmt, Args... args) {
  static thread_local char buffer[1024];
  std::snprintf(buffer, sizeof(buffer), fmt, args...);
  for (char *c = buffer; *c != '\0'; ++c) {
    if (*c == '\n' || *c == '\r') {
      *c = ' ';
    }
  }
  return buffer;
}
inline const char *logFormat(const char *fmt) {
  static thread_local char buffer[1024];
  std::snprintf(buffer, sizeof(buffer), "%s", fmt);
  for (char *c = buffer; *c != '\0'; ++c) {
    if (*c == '\n' || *c == '\r') {
      *c = ' ';
    }
  }
  return buffer;
}
} // namespace rtps

#endif // RTPS_LOG_H
