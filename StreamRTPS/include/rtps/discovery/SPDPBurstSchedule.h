#ifndef RTPS_SPDPBURSTSCHEDULE_H
#define RTPS_SPDPBURSTSCHEDULE_H

#include "rtps/config.h"

#include <atomic>
#include <cstdint>

namespace rtps {

// SPDP Accelerated Schedule after start
enum class SPDPBroadcastPhase : uint8_t {
  BURST_PHASE_1 = 0,
  BURST_PHASE_2,
  BURST_PHASE_3,
  BURST_PHASE_4,
  STEADY
};

// picks the SPDP broadcast phase and its send interval from elapsed time
inline SPDPBroadcastPhase getSPDPPhaseForElapsedMs(uint64_t elapsed_ms, uint16_t &interval_ms) {

  //Compute phase ends
  const uint64_t phase1_end = Config::SPDP_BURST_PHASE_1_MS;
  const uint64_t phase2_end = phase1_end + Config::SPDP_BURST_PHASE_2_MS;
  const uint64_t phase3_end = phase2_end + Config::SPDP_BURST_PHASE_3_MS;
  const uint64_t phase4_end = phase3_end + Config::SPDP_BURST_PHASE_4_MS;

  // scale burst intervals in percent
  const uint32_t scale =Config::SPDP_BURST_SCALE_PCT.load(std::memory_order_relaxed);
  const uint64_t window = Config::SPDP_BURST_WINDOW_MS.load(std::memory_order_relaxed);
  auto scaled = [scale](uint32_t base) -> uint16_t {
    const uint64_t v = static_cast<uint64_t>(base) * scale / 100u;
    return static_cast<uint16_t>(v > 65535u ? 65535u : v);
  };

  if (elapsed_ms < phase1_end) {
    interval_ms = scaled(Config::SPDP_BURST_INTERVAL_1_MS);
    return SPDPBroadcastPhase::BURST_PHASE_1;
  }
  if (elapsed_ms < phase2_end) {
    interval_ms = scaled(Config::SPDP_BURST_INTERVAL_2_MS);
    return SPDPBroadcastPhase::BURST_PHASE_2;
  }
  if (elapsed_ms < phase3_end) {
    interval_ms = scaled(Config::SPDP_BURST_INTERVAL_3_MS);
    return SPDPBroadcastPhase::BURST_PHASE_3;
  }
  if (elapsed_ms < phase4_end && elapsed_ms < window) {
    interval_ms = scaled(Config::SPDP_BURST_INTERVAL_4_MS);
    return SPDPBroadcastPhase::BURST_PHASE_4;
  }

  interval_ms = static_cast<uint16_t>(
      Config::SPDP_RESEND_PERIOD_MS.load(std::memory_order_relaxed));
  return SPDPBroadcastPhase::STEADY;
}

} // namespace rtps

#endif // RTPS_SPDPBURSTSCHEDULE_H
