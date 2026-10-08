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

#ifndef RTPS_CONFIG_H
#define RTPS_CONFIG_H

#include <atomic>
#include <cstddef>

#include "rtps/common/types.h"

namespace rtps {

#define IS_LITTLE_ENDIAN 1

namespace Config {

const VendorId_t VENDOR_ID = {13, 37};
const GuidPrefix_t BASE_GUID_PREFIX{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

const uint8_t DOMAIN_ID = 0;

const uint8_t HISTORY_SIZE = 255;
const bool BUILTIN_READER_RECOVER_BELOW_FLOOR = true;

const uint8_t MAX_TYPENAME_LENGTH = 64;
const uint8_t MAX_TOPICNAME_LENGTH = 64;

const uint16_t SF_WRITER_HB_PERIOD_MS = 500;
inline std::atomic<uint32_t> SPDP_RESEND_PERIOD_MS{5000};
const uint16_t SPDP_MAX_NUMBER_FOUND_PARTICIPANTS = 1024;
const uint8_t SPDP_MAX_NUM_LOCATORS = 5;
const Duration_t SPDP_DEFAULT_REMOTE_LEASE_DURATION = {180, 0};
const Duration_t SPDP_MAX_REMOTE_LEASE_DURATION = {180, 0};
// runtime lease announced in our SPDP messages, also caps any advertised remote lease so lowering it on all nodes bounds dead peer purge time
inline std::atomic<uint32_t> SPDP_LEASE_DURATION_MS{180000};
const uint16_t SPDP_HEARTBEAT_CHECK_PERIOD_MS = 5000;

// SPDP burst discovery phases
const uint16_t SPDP_BURST_PHASE_1_MS = 500;
const uint16_t SPDP_BURST_PHASE_2_MS = 1000;
const uint16_t SPDP_BURST_PHASE_3_MS = 2000;
const uint16_t SPDP_BURST_PHASE_4_MS = 5000;
const uint16_t SPDP_BURST_INTERVAL_1_MS = 50;
const uint16_t SPDP_BURST_INTERVAL_2_MS = 100;
const uint16_t SPDP_BURST_INTERVAL_3_MS = 250;
const uint16_t SPDP_BURST_INTERVAL_4_MS = 500;
inline std::atomic<uint32_t> SPDP_BURST_WINDOW_MS{20000};

const int MAX_NUM_UDP_CONNECTIONS = 255;

const int THREAD_POOL_NUM_WRITERS = 1;
const int THREAD_POOL_NUM_READERS = 4;
const int THREAD_POOL_NUM_TIMED_WRITERS = 1;
const int THREAD_POOL_WORKLOAD_QUEUE_LENGTH = 4096;
const std::size_t THREAD_POOL_INCOMING_QUEUE_MAX = 65535;

inline std::atomic<float> UDP_PACKET_LOSS_RATE{0.0f};


inline std::atomic<uint32_t> SNAP_TIMEOUT_INITIAL_MS{30};
inline std::atomic<uint32_t> SNAP_TIMEOUT_RETRANSMIT_MS{25};
inline std::atomic<uint32_t> SNAP_TIMEOUT_ANNOUNCING_MS{10};
inline std::atomic<uint32_t> SNAP_JITTER_MAX_MS{50};

inline std::atomic<uint32_t> REQUEST_RETRY_BOUND{4};

// per peer reconcile debounce, base interval doubles up to the cap per resync
inline std::atomic<uint32_t> SNAP_RECONCILE_BASE_MS{50};
inline std::atomic<uint32_t> SNAP_RECONCILE_CAP_MS{1600};

// grace before a peers first reconcile fetch, lets in flight announcements land first
inline std::atomic<uint32_t> SNAP_RECONCILE_GRACE_MS{25};

// SPDP burst interval multiplier in percent, 100 is nominal, higher is slower
inline std::atomic<uint32_t> SPDP_BURST_SCALE_PCT{100};

// quiescence gate, SEDP ignores discovery until this many SPDP rounds elapse and forces an election once max rounds elapse even if peers still churn
inline std::atomic<uint32_t> SNAP_QUIESCENCE_ROUNDS{1};
inline std::atomic<uint32_t> SNAP_QUIESCENCE_MAX_ROUNDS{0};

// join server selection, 0 random among lowest root, 1 always the root holder, 2 random among all configured, default 2 spreads join load off the root
inline std::atomic<uint32_t> SNAP_SERVER_POLICY{2};

// announce again after a plain resync, 0 only on lowered root, 1 always, 2 never
inline std::atomic<uint32_t> SNAP_REANNOUNCE_AFTER_RESYNC{2};

// deferred requester cap, 0 disables deferral so the requester has to retry
inline std::atomic<uint32_t> SNAP_DEFER_CAP{4};

// partner selection RNG seed, 0 seeds from random_device, otherwise each agent mixes it with its GUID prefix, test hook
inline std::atomic<uint64_t> SNAP_RNG_SEED{0};

} // namespace Config
} // namespace rtps

#endif // RTPS_CONFIG_H
