#ifndef STREAMRTPS_TRACE_CONTROL_H
#define STREAMRTPS_TRACE_CONTROL_H

#include "rtps/utils/Log.h"

#ifdef EMBRTPS_DISABLE_TRACING
#define RTPS_TRACE_ALL_EVENT(EVENT, ...) ((void)0)
#define RTPS_TRACE_PERF_EVENT(EVENT, ...) ((void)0)
#else
#include "streamrtps_trace.h"

#if TRACE_ALL
#define RTPS_TRACE_ALL_EVENT(EVENT, ...) tracepoint(streamrtps_trace, EVENT, ##__VA_ARGS__)
#else
#define RTPS_TRACE_ALL_EVENT(EVENT, ...) ((void)0)
#endif

#if TRACE_PERF
#define RTPS_TRACE_PERF_EVENT(EVENT, ...) tracepoint(streamrtps_trace, EVENT, ##__VA_ARGS__)
#else
#define RTPS_TRACE_PERF_EVENT(EVENT, ...) ((void)0)
#endif

#if TRACE_LOG
#undef RTPS_TRACE_LOG_EMIT
#define RTPS_TRACE_LOG_EMIT(SUBSYS, FMT, ...) \
  tracepoint(streamrtps_trace, log_message, SUBSYS, rtps::logFormat(FMT, ##__VA_ARGS__))
#endif
#endif

#endif
