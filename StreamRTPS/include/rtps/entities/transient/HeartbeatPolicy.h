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

#ifndef RTPS_HEARTBEAT_POLICY
#define RTPS_HEARTBEAT_POLICY

#include <rtps/config.h>
#include <rtps/common/types.h>

#include <atomic>
#include <cstdint>

namespace rtps {

// period walks toward a multiple of the estimated sample period, clamped between HB_PERIOD_MIN_MS and HB_PERIOD_MAX_MS, any NACK resets it to base
class HeartbeatPolicy {
public:
    HeartbeatPolicy() = default;

    bool shouldSend();
    void onAckNack(bool hadNacks, bool allAcknowledged = false);
    void onNewChange();
    void onHeartbeatTransmitted();

    uint32_t getCurrentPeriodMs() const {
        return m_currentPeriodMs.load(std::memory_order_relaxed);
    }

    uint32_t getRecommendedSleepMs() const;

protected:
    static uint32_t clampPeriod(uint32_t ms) {
        if (ms > HB_PERIOD_MAX_MS) return HB_PERIOD_MAX_MS;
        if (ms < HB_PERIOD_MIN_MS) return HB_PERIOD_MIN_MS;
        return ms;
    }

    uint32_t remainingSleepMs(int64_t refMs) const;
    void adaptTowards();

    std::atomic<uint32_t> m_currentPeriodMs{Config::SF_WRITER_HB_PERIOD_MS};
    Time_t m_lastHbSent = TIME_ZERO;
    Time_t m_lastChangeSeen = TIME_ZERO;
    std::atomic<uint32_t> m_estimatedSamplePeriodMs{0};

    static constexpr uint32_t HB_PERIOD_GROWTH_VAL = 25;
    static constexpr uint32_t HB_PERIOD_MAX_MS = 20000;
    // floor for the adaptive shrink, bounds the HB flood when a reader cant converge
    static constexpr uint32_t HB_PERIOD_MIN_MS = 25;
    static constexpr uint32_t HB_ADAPTIVE_TARGET = 3;
};

inline void HeartbeatPolicy::adaptTowards() {
    const uint32_t estimated = m_estimatedSamplePeriodMs.load(std::memory_order_relaxed);
    if (estimated == 0) return;
    const uint32_t candidate = HB_ADAPTIVE_TARGET * estimated;
    uint32_t period = m_currentPeriodMs.load(std::memory_order_relaxed);

    if (candidate < period && period > HB_PERIOD_GROWTH_VAL) {
        period -= HB_PERIOD_GROWTH_VAL;
    } else if (candidate > period) {
        period += HB_PERIOD_GROWTH_VAL;
    }

    m_currentPeriodMs.store(clampPeriod(period), std::memory_order_relaxed);
}

inline uint32_t HeartbeatPolicy::remainingSleepMs(int64_t refMs) const {
    if (refMs == 0) return 1;
    const int64_t nowMs = Time_t::now().toMilliseconds();
    const uint32_t period = m_currentPeriodMs.load(std::memory_order_relaxed);
    const int64_t rem = static_cast<int64_t>(period) - (nowMs - refMs);
    if (rem <= 0) return 1;
    if (rem > static_cast<int64_t>(HB_PERIOD_MAX_MS)) return HB_PERIOD_MAX_MS;
    return static_cast<uint32_t>(rem);
}

inline uint32_t HeartbeatPolicy::getRecommendedSleepMs() const {
    return remainingSleepMs(m_lastHbSent.toMilliseconds());
}

} // namespace rtps

#endif // RTPS_HEARTBEAT_POLICY
