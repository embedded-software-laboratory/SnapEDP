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

#ifndef PROJECT_SYSFUNCTIONS_H
#define PROJECT_SYSFUNCTIONS_H

#include <chrono>
#include <thread>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <iostream>
#include "rtps/common/types.h"

namespace rtps {
inline uint32_t timeNowMs() {
  using namespace std::chrono;
  return static_cast<uint32_t>(
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count());
}

inline bool timeReached(uint32_t now, uint32_t wakeup) {
  return static_cast<int32_t>(now - wakeup) >= 0;
}

inline bool timeEarlier(uint32_t lhs, uint32_t rhs) {
  return static_cast<int32_t>(lhs - rhs) < 0;
}

inline Time_t getCurrentTimeStamp() {
  return Time_t::fromMilliseconds(static_cast<int64_t>(timeNowMs()));
}

inline void sleep_for(int ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds{ms});
}

// from https://stackoverflow.com/a/4793662, newer standards can use binary_semaphore
class SemaphoreImpl {
    std::mutex mutex_{};
    std::condition_variable condition_{};
    unsigned long count_ = 0; // initialized as locked

public:
    SemaphoreImpl(uint8_t count = 0) : count_(count) {}

    void release() {
        std::lock_guard<decltype(mutex_)> lock(mutex_);
        ++count_;
        condition_.notify_one();
    }

    void acquire() {
        std::unique_lock<decltype(mutex_)> lock(mutex_);
        while(!count_) // handle spurious wake ups
            condition_.wait(lock);
        --count_;
    }
};

struct Semaphore
{
  int8_t sem_new(uint8_t count) {
    sem = std::unique_ptr<SemaphoreImpl>(new SemaphoreImpl(count));
    return 0;
  }
  void sem_signal() {
    sem->release();
  }
  void sem_wait() {
    sem->acquire();
  }
  void sem_free() {
    sem.reset();
  }
  int sem_valid() {
    return sem != nullptr;
  }
private:
  std::unique_ptr<SemaphoreImpl> sem = nullptr;
};


struct ThreadJoiner {
    void operator()(std::thread* t) const {
        if (t) {
            if (t->joinable()) {
                auto id = t->get_id();
                std::cerr << "[ThreadJoiner] deleter: joining tid=" << id << std::endl;
                t->join();
                std::cerr << "[ThreadJoiner] deleter: joined tid=" << id << std::endl;
            } else {
                std::cerr << "[ThreadJoiner] deleter: thread not joinable, skipping" << std::endl;
            }
            delete t;
        }
    }
};
using Thread = std::unique_ptr<std::thread, ThreadJoiner>;
typedef void (*thread_fn)(void *arg);
inline Thread createThread(const char *, thread_fn thread, void *arg) {
  return Thread(new std::thread(thread, arg));
}
inline void joinThread(Thread &t) {
  if(t) {
    if(t->joinable()) {
      auto id = t->get_id();
      std::cerr << "[joinThread] joining tid=" << id << std::endl;
      t->join();
      std::cerr << "[joinThread] joined tid=" << id << std::endl;
    } else {
      std::cerr << "[joinThread] thread not joinable, skipping" << std::endl;
    }
  } else {
    std::cerr << "[joinThread] thread is null, skipping" << std::endl;
  }
}
inline Time_t Time_t::now() {
  return Time_t::fromMilliseconds(static_cast<int64_t>(timeNowMs()));
}

inline Time_t timeNow() {
  return Time_t::now();
}

} // namespace rtps

#endif // PROJECT_SYSFUNCTIONS_H
