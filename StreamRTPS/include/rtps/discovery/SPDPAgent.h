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

#ifndef RTPS_SPDP_H
#define RTPS_SPDP_H

#include "rtps/utils/Lock.h"
#include "rtps/common/types.h"
#include "rtps/config.h"
#include "rtps/discovery/BuiltInEndpoints.h"
#include "rtps/discovery/ParticipantProxyData.h"
#include "ucdr/microcdr.h"
#include "rtps/utils/sysFunctions.h"
#include <atomic>
#include <condition_variable>
#include <mutex>

namespace rtps {

class Participant;
class Writer;
class Reader;
class ReaderCacheChange;

// Standard confrom SPDP Agent
class SPDPAgent {
public:
  ~SPDPAgent();
  
  void init(Participant &participant, BuiltInEndpoints &endpoints);
  void start();
  void stop();

  // Retransmits/broadcasts a new SPDP message directly
  void requestImmediateResend();
  // Stores the Snap configuration state for next SPDP message
  void setState(SPDPDiscoverState state);

  // snapshot access to dump the state, for debugging and tracing
  SPDPDiscoverState getLocalSnapState() const { return m_localSnapState; }

  // completed SPDP broadcast iterations since startup
  uint16_t getBroadcastRound() const {
    return m_broadcastRound.load(std::memory_order_relaxed);
  }

private:
  Mutex m_mutex;
  bool initialized = false;
  Participant *mp_participant = nullptr;
  BuiltInEndpoints m_buildInEndpoints;
  
  // SPDP thread and state variables
  Thread m_agentThread{};
  std::atomic<bool> m_running{false};
  std::atomic<bool> m_immediateResend{false};
  std::atomic<uint16_t> m_broadcastRound{0};
  std::mutex m_resendMutex;
  std::condition_variable m_resendSignal;
  
  // Deserialization state
  std::array<uint8_t, 400> m_outputBuffer{};
  std::array<uint8_t, 400> m_inputBuffer{};
  ParticipantProxyData m_proxyDataBuffer{};
  ucdrBuffer m_microbuffer{};

  // current local Snap configuration state
  SPDPDiscoverState m_localSnapState = SPDPDiscoverState::UNCONFIGURED;

  // last advertised hash, rebuilt on demand
  uint64_t m_lastAdvertisedHash = 0;
  GuidPrefix_t m_lastAdvertisedRoot = GUIDPREFIX_UNKNOWN;
  void refreshPayloadIfStale();

  // handlers and parsing
  static void receiveCallback(void *callee,const ReaderCacheChange &cacheChange);
  void handleSPDPPackage(const ReaderCacheChange &cacheChange);
  void processProxyData(EventId_t eventId);

  // Message assembly functions
  bool addProxiesForBuiltInEndpoints();
  void addInlineQos();
  void addParticipantParameters();
  void configureEndianessAndOptions(ucdrBuffer &buffer);
  void endCurrentList();
  
  // Broadcast functions
  void wakeBroadcast();
  static void runBroadcast(void *args);
};
} // namespace rtps

#endif // RTPS_SPDP_H
