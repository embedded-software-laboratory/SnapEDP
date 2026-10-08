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

#include "rtps/ThreadPool.h"

#include "rtps/entities/Writer.h"
#include "rtps/utils/Log.h"
#include "rtps/utils/udpUtils.h"
#include "rtps/utils/EventId.h"

#include "trace_control.h"


#if THREAD_POOL_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define THREAD_POOL_LOG(...) RTPS_LOG("ThreadPool", THREAD_POOL_VERBOSE, __VA_ARGS__)
#else
#define THREAD_POOL_LOG(...) RTPS_TRACE_LOG_EMIT("ThreadPool", __VA_ARGS__)
#endif

namespace rtps {

ThreadPool::ThreadPool(receiveJumppad_fp receiveCallback, void *callee)
    : m_receiveJumppad(receiveCallback), m_callee(callee) {

  if (!m_queueOutgoing.init() || !m_queueIncoming.init()) {
    return;
  }
  auto inputErr = m_readerNotificationSem.sem_new(0);
  auto outputErr = m_writerNotificationSem.sem_new(0);

  if (inputErr != 0 || outputErr != 0) {
    THREAD_POOL_LOG("ThreadPool: Failed to create Semaphores.\n");
  }

  if (mutex_new(&m_timedOutgoingMutex) != EMB_ERR_OK) {
    THREAD_POOL_LOG("ThreadPool: Failed to create timed queue mutex.\n");
  }
}

ThreadPool::~ThreadPool() {
  std::cerr << "[ThreadPool] ~dtor running=" << (m_running ? 1 : 0) << std::endl;
  if (m_running) {
    std::cerr << "[ThreadPool] ~dtor: stopping threads" << std::endl;
    stopThreads();
    rtps::sleep_for(500);
    std::cerr << "[ThreadPool] ~dtor: stop complete" << std::endl;
  }

  if (m_readerNotificationSem.sem_valid()) {
    m_readerNotificationSem.sem_free();
  }
  if (m_writerNotificationSem.sem_valid()) {
    m_writerNotificationSem.sem_free();
  }
  if (mutex_valid(&m_timedOutgoingMutex)) {
    mutex_free(&m_timedOutgoingMutex);
  }
}

bool ThreadPool::startThreads() {
  if (m_running) {
    return true;
  }

  if(!m_readerNotificationSem.sem_valid() || !m_writerNotificationSem.sem_valid()) {
    return false;
  }

  THREAD_POOL_LOG("start %u reader/writer threads",
                  static_cast<unsigned>(m_writers.size()));
  m_running = true;
  for (auto &thread : m_writers) {
    // TODO ID, err check, waitOnStop
    thread = rtps::createThread("WriterThread", writerThreadFunction, this);
  }

  for (auto &thread : m_readers) {
    // TODO ID, err check, waitOnStop
    thread = rtps::createThread("ReaderThread", readerThreadFunction, this);
  }

  for (auto &thread : m_timedWriters) {
    thread = rtps::createThread("TimedWriterThread", timedWriterThreadFunction,
                                this);
  }
  return true;
}

void ThreadPool::stopThreads() {
  m_running = false;

  for(auto i = 0u; i < m_writers.size(); ++i) {
    m_writerNotificationSem.sem_signal();
  }
  for(auto i = 0u; i < m_readers.size(); ++i) {
    m_readerNotificationSem.sem_signal();
  }
  {
    // notify under the lock so the timed thread cant miss the stop signal
    Lock lock{m_timedOutgoingMutex};
    m_timedWork.notify_all();
  }
  for (auto &thread : m_writers) {
    rtps::joinThread(thread);
  }
  for (auto &thread : m_readers) {
    rtps::joinThread(thread);
  }
  for (auto &thread : m_timedWriters) {
    rtps::joinThread(thread);
  }
  THREAD_POOL_LOG("stop threads done waiting");
}

void ThreadPool::clearQueues() {
  m_queueOutgoing.clear();
  {
    Lock lock{m_timedOutgoingMutex};
    m_queueTimedOutgoing.clear();
  }
  m_queueIncoming.clear();
}

bool ThreadPool::addWorkload(Writer *workload) {
  bool res = m_queueOutgoing.moveElementIntoBuffer(std::move(workload));
  if (res) {
    m_writerNotificationSem.sem_signal();
  } else {
    RTPS_TRACE_ALL_EVENT(threadpool_queue_drop, 0, 0ULL);
  }

  return res;
}

bool ThreadPool::removeWorkload(Writer *workload) {
  // drain and requeue so a writer thread cant pop a soon to be freed pointer
  std::unique_lock<Mutex> flight{m_writerFlightMutex};
  bool removed = false;
  std::vector<Writer *> keep;
  Writer *item = nullptr;
  while (m_queueOutgoing.moveFirstInto(item)) {
    if (item == workload) {
      removed = true;
    } else {
      keep.push_back(item);
    }
  }
  for (auto *w : keep) {
    m_queueOutgoing.moveElementIntoBuffer(std::move(w));
  }
  // a writer thread may already be inside progress with it, wait for that call to return before the caller frees it, unless we are that thread
  const auto self = std::this_thread::get_id();
  m_writerIdle.wait(flight, [&] {
    for (const auto &entry : m_writerInFlight) {
      if (entry.first == workload && entry.second != self) {
        return false;
      }
    }
    return true;
  });
  return removed;
}

bool ThreadPool::addTimedWorkload(TimedWork *workload, uint32_t wakeup) {
  if (workload == nullptr) {
    return false;
  }

  Lock lock{m_timedOutgoingMutex};
  // being removed right now, do not let its own onTimer put it back
  for (auto *removing : m_timedRemoving) {
    if (removing == workload) {
      return false;
    }
  }
  uint32_t effectiveWakeup = wakeup;

  // one entry per aggregator, keep the earliest scheduled activation time
  for (auto it = m_queueTimedOutgoing.begin(); it != m_queueTimedOutgoing.end();) {
    if (it->workload == workload) {
      if (timeEarlier(it->wakeup, effectiveWakeup)) {
        effectiveWakeup = it->wakeup;
      }
      it = m_queueTimedOutgoing.erase(it);
    } else {
      ++it;
    }
  }

  auto insertPos = m_queueTimedOutgoing.begin();
  while (insertPos != m_queueTimedOutgoing.end() &&
         !timeEarlier(effectiveWakeup, insertPos->wakeup)) {
    ++insertPos;
  }
  m_queueTimedOutgoing.insert(insertPos, TimedWork_t{workload, effectiveWakeup});
  m_timedWork.notify_one();
  return true;
}

bool ThreadPool::addNewPacket(PacketInfo &&packet) {
  const uint64_t eventId = packet.eventId;
  bool res = m_queueIncoming.moveElementIntoBuffer(std::move(packet));
  if (res) {
    m_readerNotificationSem.sem_signal();
  } else {
    RTPS_TRACE_ALL_EVENT(threadpool_queue_drop, 1, eventId);
  }
  RTPS_TRACE_PERF_EVENT(threadpool_queue_state,
      m_queueIncoming.size(),
      static_cast<uint16_t>(Config::THREAD_POOL_WORKLOAD_QUEUE_LENGTH),
      res ? 0 : 1,
      eventId);
  return res;
}

void ThreadPool::writerThreadFunction(void *arg) {
  auto pool = static_cast<ThreadPool *>(arg);
  if (pool == nullptr) {

    THREAD_POOL_LOG("nullptr passed to writer function\n");

    return;
  }

  pool->doWriterWork();
}

void ThreadPool::timedWriterThreadFunction(void *arg) {
  auto pool = static_cast<ThreadPool *>(arg);
  if (pool == nullptr) {
    THREAD_POOL_LOG("nullptr passed to timed writer function\n");
    return;
  }

  pool->doTimedWriterWork();
}

void ThreadPool::doWriterWork() {
  while (m_running) {
    Writer *workload;
    bool isWorkToDo = false;
    {
      // pop and register in one step so removeWorkload sees the item either in the queue or in flight, never in between
      Lock flight{m_writerFlightMutex};
      isWorkToDo = m_queueOutgoing.moveFirstInto(workload);
      if (isWorkToDo) {
        m_writerInFlight.emplace_back(workload, std::this_thread::get_id());
      }
    }
    if (!isWorkToDo) {
      m_writerNotificationSem.sem_wait();
      continue;
    }

    workload->progress();

    {
      Lock flight{m_writerFlightMutex};
      const auto self = std::this_thread::get_id();
      for (auto it = m_writerInFlight.begin(); it != m_writerInFlight.end(); ++it) {
        if (it->first == workload && it->second == self) {
          m_writerInFlight.erase(it);
          break;
        }
      }
    }
    m_writerIdle.notify_all();
  }
}

void ThreadPool::doTimedWriterWork() {
  std::unique_lock<Mutex> lock{m_timedOutgoingMutex};
  while (m_running) {
    // sleep until the front item is due or addTimedWorkload or stop signals us
    if (m_queueTimedOutgoing.empty()) {
      m_timedWork.wait(lock, [this] {
        return !m_running || !m_queueTimedOutgoing.empty();
      });
      continue;
    }

    const uint32_t now = rtps::timeNowMs();
    const uint32_t wakeup = m_queueTimedOutgoing.front().wakeup;
    if (!rtps::timeReached(now, wakeup)) {
      // wake early if stop hits or a nearer item displaces the front
      m_timedWork.wait_for(lock,
          std::chrono::milliseconds{static_cast<int32_t>(wakeup - now)},
          [this, wakeup] {
            return !m_running || m_queueTimedOutgoing.empty() ||
                   m_queueTimedOutgoing.front().wakeup != wakeup;
          });
      continue;
    }

    TimedWork_t dueWork = m_queueTimedOutgoing.front();
    m_queueTimedOutgoing.pop_front();
    const auto self = std::this_thread::get_id();
    // still under the lock, from here the item is visible as in flight
    m_timedInFlight.emplace_back(dueWork.workload, self);
    lock.unlock();
    if (dueWork.workload != nullptr) {
      dueWork.workload->onTimer();
    }
    lock.lock();
    for (auto it = m_timedInFlight.begin(); it != m_timedInFlight.end(); ++it) {
      if (it->first == dueWork.workload && it->second == self) {
        m_timedInFlight.erase(it);
        break;
      }
    }
    m_timedIdle.notify_all();
  }
}

bool ThreadPool::removeTimedWorkload(TimedWork *workload){
  std::unique_lock<Mutex> lock{m_timedOutgoingMutex};
  auto sizeBefore = m_queueTimedOutgoing.size();
  m_queueTimedOutgoing.remove_if([workload] (auto &element) {
    return element.workload == workload;
  });
  const bool removed = m_queueTimedOutgoing.size() < sizeBefore;

  // a running item is no longer in the queue, wait until its onTimer returned or the caller frees an object in use, skipped when called from the timed thread itself
  m_timedRemoving.push_back(workload);
  const auto self = std::this_thread::get_id();
  m_timedIdle.wait(lock, [&] {
    for (const auto &entry : m_timedInFlight) {
      if (entry.first == workload && entry.second != self) {
        return false;
      }
    }
    return true;
  });
  // onTimer may have tried to rearm before it saw the removal mark
  m_queueTimedOutgoing.remove_if([workload] (auto &element) {
    return element.workload == workload;
  });
  for (auto it = m_timedRemoving.begin(); it != m_timedRemoving.end(); ++it) {
    if (*it == workload) {
      m_timedRemoving.erase(it);
      break;
    }
  }
  return removed;
}

void ThreadPool::readCallback(void *args, Ip4Port_t destPort, std::vector<uint8_t> &&p, const ip_struct_t * /*addr*/, Ip4Port_t port) {
  auto &pool = *static_cast<ThreadPool *>(args);

  PacketInfo packet;

  packet.destAddr = {0}; // not relevant
  packet.destPort = destPort;
  packet.srcPort = port;
  packet.eventId = generateEventId();
  packet.buffer = PBufWrapper{std::move(p)};

  THREAD_POOL_LOG("readCallback packet size=%zu eventId=%lu",
                  p.size(), static_cast<unsigned long>(packet.eventId));
  RTPS_TRACE_PERF_EVENT(threadpool_recv_packet, packet.eventId,
                        destPort, port, static_cast<uint32_t>(p.size()));

  if (!pool.addNewPacket(std::move(packet))) {
    THREAD_POOL_LOG("ThreadPool: dropped packet\n");
  }
}

void ThreadPool::readerThreadFunction(void *arg) {
  auto pool = static_cast<ThreadPool *>(arg);
  if (pool == nullptr) {

    THREAD_POOL_LOG("nullptr passed to reader function\n");

    return;
  }
  pool->doReaderWork();
}

void ThreadPool::doReaderWork() {

  while (m_running) {
    PacketInfo packet;
    auto isWorkToDo = m_queueIncoming.moveFirstInto(packet);
    if (!isWorkToDo) {
      m_readerNotificationSem.sem_wait();
      continue;
    }

    m_receiveJumppad(m_callee, const_cast<const PacketInfo &>(packet));
  }
}

} // namespace rtps
