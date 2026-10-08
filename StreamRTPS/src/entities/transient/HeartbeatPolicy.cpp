#include <rtps/entities/transient/HeartbeatPolicy.h>
#include <rtps/utils/sysFunctions.h>

using namespace rtps;

bool HeartbeatPolicy::shouldSend() {
    const int64_t lastHbMs = m_lastHbSent.toMilliseconds();
    if (lastHbMs == 0) return true;
    return (Time_t::now().toMilliseconds() - lastHbMs) >=
           static_cast<int64_t>(m_currentPeriodMs.load(std::memory_order_relaxed));
}

void HeartbeatPolicy::onNewChange() {
    Time_t now = Time_t::now();
    if (m_lastChangeSeen != TIME_ZERO) {
        int64_t delta = now.toMilliseconds() - m_lastChangeSeen.toMilliseconds();
        if (delta > 0) {
            uint32_t estimated = m_estimatedSamplePeriodMs.load(std::memory_order_relaxed);
            estimated = (estimated == 0)
                ? static_cast<uint32_t>(delta)
                : (estimated * 3 + static_cast<uint32_t>(delta)) / 4;
            m_estimatedSamplePeriodMs.store(estimated, std::memory_order_relaxed);
        }
    }
    m_lastChangeSeen = now;
    adaptTowards();
}

void HeartbeatPolicy::onHeartbeatTransmitted() {
    m_lastHbSent = Time_t::now();
}

void HeartbeatPolicy::onAckNack(bool hadNacks, bool /*allAcknowledged*/) {
    if (hadNacks) {
        // on NACK reset to base cadence, never faster, shrinking amplified gaps into a flood
        m_currentPeriodMs.store(Config::SF_WRITER_HB_PERIOD_MS,
                                std::memory_order_relaxed);
    }
}
