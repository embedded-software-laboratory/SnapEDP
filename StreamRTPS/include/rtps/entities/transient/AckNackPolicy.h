/*
This file is part of streamRTPS.
Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_ACKNACK_POLICY_H
#define RTPS_ACKNACK_POLICY_H

#include <rtps/common/types.h>
#include <rtps/entities/transient/RateLockConstants.h>
#include <cstdint>

namespace rtps {

class AckNackPolicy {
public:
    AckNackPolicy() = default;

    void onDataReceived() {
        const int64_t nowMs = Time_t::now().toMilliseconds();
        m_proactive = false;

        if (m_lastSampleMs > 0) {
            const int64_t delta = nowMs - m_lastSampleMs;
            if (delta > 0) {
              
                const bool     wasLocked = m_locked;
                const uint32_t prevMean  = m_lockedMeanMs;

                pushInterval(static_cast<uint32_t>(delta));
                evaluateLock();

                if (wasLocked && prevMean > 0 &&
                    delta > static_cast<int64_t>(prevMean) * RateLock::VIOLATION_FACTOR) {
                    m_proactive = true;
                }
            }
        }
        m_lastSampleMs = nowMs;
    }

    void onHeartbeat(uint32_t /*numMissing*/) {
        const int64_t nowMs = Time_t::now().toMilliseconds();
        if (m_lastHeartbeatMs > 0) {
            const int64_t hbDelta = nowMs - m_lastHeartbeatMs;
            if (hbDelta > 0) {
                m_hbInterarrivalMs = static_cast<uint32_t>(hbDelta);
            }
        }
        m_lastHeartbeatMs = nowMs;
        m_shouldSend = true;
    }

    bool shouldSendAckNack() {
        if (m_shouldSend) {
            m_shouldSend = false;
            return true;
        }
        return false;
    }

    bool shouldSendProactiveAckNack() {
        if (m_proactive) {
            m_proactive = false;
            return true;
        }
        return false;
    }

    bool     isLocked()      const { return m_locked; }
    uint32_t lockedMeanMs()  const { return m_lockedMeanMs; }

private:
    int64_t  m_lastSampleMs     = 0;
    int64_t  m_lastHeartbeatMs  = 0;
    uint32_t m_hbInterarrivalMs = 0;
    bool     m_shouldSend       = false;
    bool     m_proactive        = false;
    bool     m_locked           = false;
    uint32_t m_lockedMeanMs     = 0;

    uint32_t m_intervals[RateLock::WINDOW_SIZE]{};
    uint8_t  m_intervalHead  = 0;
    uint8_t  m_intervalCount = 0;

    void pushInterval(uint32_t deltaMs) {
        m_intervals[m_intervalHead] = deltaMs;
        m_intervalHead = static_cast<uint8_t>((m_intervalHead + 1) % RateLock::WINDOW_SIZE);
        if (m_intervalCount < RateLock::WINDOW_SIZE) ++m_intervalCount;
    }

    // compute mean and variance over the last n samples
    bool varianceBelowThreshold(uint8_t n, uint32_t &outMeanMs) const {
        if (m_intervalCount < n) return false;

        uint64_t sum = 0;
        for (uint8_t k = 0; k < n; ++k) {
            sum += m_intervals[RateLock::ringIndex(m_intervalHead, k)];
        }
        const uint32_t meanMs = static_cast<uint32_t>(sum / n);
        outMeanMs = meanMs;
        if (meanMs == 0) return false;

        uint64_t varSum = 0;
        for (uint8_t k = 0; k < n; ++k) {
            const uint32_t s = m_intervals[RateLock::ringIndex(m_intervalHead, k)];
            const int32_t diff = static_cast<int32_t>(s) - static_cast<int32_t>(meanMs);
            varSum += static_cast<uint64_t>(static_cast<int64_t>(diff) * diff);
        }
        return (varSum / n) < RateLock::VARIANCE_THRESH_MS2;
    }

    void evaluateLock() {
        const uint8_t required =
            (m_hbInterarrivalMs > RateLock::IMPLICIT_HB_HINT_MS)
                ? RateLock::MIN_REQUIRED_HINTED
                : RateLock::MIN_REQUIRED;

        uint32_t meanMs = 0;
        if (varianceBelowThreshold(required, meanMs)) {
            m_locked = true;
            m_lockedMeanMs = meanMs;
        } else {
            m_locked = false;
            m_lockedMeanMs = 0;
        }
    }
};

} // namespace rtps

#endif // RTPS_ACKNACK_POLICY_H
