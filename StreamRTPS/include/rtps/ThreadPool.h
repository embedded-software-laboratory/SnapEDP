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

#ifndef RTPS_THREADPOOL_H
#define RTPS_THREADPOOL_H

#include "rtps/communication/PacketInfo.h"
#include "rtps/communication/UdpDriver.h"
#include "rtps/config.h"
#include "rtps/storages/PBufWrapper.h"
#include "rtps/storages/ThreadSafeCircularBuffer.h"
#include "rtps/storages/ThreadSafeGrowableQueue.h"
#include "rtps/utils/Lock.h"
#include "rtps/utils/sysFunctions.h"

#include <array>
#include <list>
#include <atomic>
#include <condition_variable>
#include <thread>
#include <utility>

#include <vector>

namespace rtps {

class Writer;
class TimedWork;

class TimedWork {
public:
  virtual ~TimedWork() = default;
  virtual void onTimer() = 0;
};

struct TimedWork_t {
  TimedWork *workload;
  uint32_t wakeup;
};

class ThreadPool {
public:
  using receiveJumppad_fp = void (*)(void *callee, const PacketInfo &packet);

  ThreadPool(receiveJumppad_fp receiveCallback, void *callee);

  ~ThreadPool();

  bool startThreads();
  void stopThreads();

  void clearQueues();
  bool addWorkload(Writer *workload);
  bool removeWorkload(Writer *workload);
  bool addTimedWorkload(TimedWork *workload, uint32_t wakeup);
  bool removeTimedWorkload(TimedWork *workload);
  bool addNewPacket(PacketInfo &&packet);
  

  static void readCallback(void *arg, Ip4Port_t destPort, std::vector<uint8_t> &&p, const ip_struct_t *addr, Ip4Port_t port);

private:
  receiveJumppad_fp m_receiveJumppad;
  void *m_callee;
  std::atomic<bool> m_running{false};
  std::array<Thread, Config::THREAD_POOL_NUM_WRITERS> m_writers;
  std::array<Thread, Config::THREAD_POOL_NUM_TIMED_WRITERS> m_timedWriters;
  std::array<Thread, Config::THREAD_POOL_NUM_READERS> m_readers;

  Semaphore m_readerNotificationSem;
  Semaphore m_writerNotificationSem;


  ThreadSafeCircularBuffer<Writer *, Config::THREAD_POOL_WORKLOAD_QUEUE_LENGTH>m_queueOutgoing;
  std::list<TimedWork_t>m_queueTimedOutgoing;
  Mutex m_timedOutgoingMutex;
  std::condition_variable m_timedWork;
  // items a timed thread took out of the queue and is running right now, removeTimedWorkload waits on these so the owner is not freed mid call
  std::vector<std::pair<TimedWork *, std::thread::id>> m_timedInFlight;
  // items being removed, addTimedWorkload refuses them so onTimer cannot rearm
  std::vector<TimedWork *> m_timedRemoving;
  std::condition_variable m_timedIdle;

  // same guard for the writer threads, the mutex spans pop plus registration
  Mutex m_writerFlightMutex;
  std::vector<std::pair<Writer *, std::thread::id>> m_writerInFlight;
  std::condition_variable m_writerIdle;
  ThreadSafeGrowableQueue<PacketInfo, Config::THREAD_POOL_INCOMING_QUEUE_MAX> m_queueIncoming;

  static void writerThreadFunction(void *arg);
  static void timedWriterThreadFunction(void *arg);
  static void readerThreadFunction(void *arg);
  void doWriterWork();
  void doTimedWriterWork();
  void doReaderWork();
};
} // namespace rtps

#endif // RTPS_THREADPOOL_H
