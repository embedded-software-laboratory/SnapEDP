#include "harness.h"
#include "rtps/discovery/SPDPBurstSchedule.h"

using namespace rtps;

namespace {
struct ConfigGuard {
  uint32_t scale = Config::SPDP_BURST_SCALE_PCT.load();
  uint32_t window = Config::SPDP_BURST_WINDOW_MS.load();
  uint32_t resend = Config::SPDP_RESEND_PERIOD_MS.load();
  ~ConfigGuard() {
    Config::SPDP_BURST_SCALE_PCT = scale;
    Config::SPDP_BURST_WINDOW_MS = window;
    Config::SPDP_RESEND_PERIOD_MS = resend;
  }
};

const uint64_t P1 = Config::SPDP_BURST_PHASE_1_MS;
const uint64_t P2 = P1 + Config::SPDP_BURST_PHASE_2_MS;
const uint64_t P3 = P2 + Config::SPDP_BURST_PHASE_3_MS;
const uint64_t P4 = P3 + Config::SPDP_BURST_PHASE_4_MS;

struct Result {
  SPDPBroadcastPhase phase;
  uint16_t interval;
};
Result at(uint64_t t) {
  Result r{};
  r.interval = 0xFFFF;
  r.phase = getSPDPPhaseForElapsedMs(t, r.interval);
  return r;
}
}

TEST(spd_phase_intervals) {
  ConfigGuard g;
  Config::SPDP_BURST_WINDOW_MS = 1000000;
  Config::SPDP_RESEND_PERIOD_MS = 5000;
  const uint64_t mids[] = {P1 / 2, (P1 + P2) / 2, (P2 + P3) / 2, (P3 + P4) / 2};
  const SPDPBroadcastPhase phases[] = {
      SPDPBroadcastPhase::BURST_PHASE_1, SPDPBroadcastPhase::BURST_PHASE_2,
      SPDPBroadcastPhase::BURST_PHASE_3, SPDPBroadcastPhase::BURST_PHASE_4};
  const uint16_t intervals[] = {
      Config::SPDP_BURST_INTERVAL_1_MS, Config::SPDP_BURST_INTERVAL_2_MS,
      Config::SPDP_BURST_INTERVAL_3_MS, Config::SPDP_BURST_INTERVAL_4_MS};
  for (int i = 0; i < 4; ++i) {
    Result r = at(mids[i]);
    REQUIRE_TRUE(r.phase == phases[i]);
    REQUIRE_TRUE(r.interval == intervals[i]);
  }
}

TEST(spd_phase_boundaries) {
  ConfigGuard g;
  Config::SPDP_BURST_WINDOW_MS = 1000000;
  const uint64_t ends[] = {P1, P2, P3, P4};
  for (int i = 0; i < 4; ++i) {
    Result before = at(ends[i] - 1);
    Result after = at(ends[i]);
    REQUIRE_TRUE(static_cast<int>(before.phase) == i);
    REQUIRE_TRUE(static_cast<int>(after.phase) == i + 1);
  }
  REQUIRE_TRUE(at(0).phase == SPDPBroadcastPhase::BURST_PHASE_1);
}

TEST(spd_scale_pct) {
  ConfigGuard g;
  Config::SPDP_BURST_WINDOW_MS = 1000000;
  const uint16_t base[] = {
      Config::SPDP_BURST_INTERVAL_1_MS, Config::SPDP_BURST_INTERVAL_2_MS,
      Config::SPDP_BURST_INTERVAL_3_MS, Config::SPDP_BURST_INTERVAL_4_MS};
  const uint64_t mids[] = {P1 / 2, (P1 + P2) / 2, (P2 + P3) / 2, (P3 + P4) / 2};
  for (uint32_t pct : {50u, 100u, 200u}) {
    Config::SPDP_BURST_SCALE_PCT = pct;
    for (int i = 0; i < 4; ++i) {
      REQUIRE_TRUE(at(mids[i]).interval == base[i] * pct / 100);
    }
  }
  Config::SPDP_BURST_SCALE_PCT = 1000000;
  REQUIRE_TRUE(at(0).interval == 65535);
  Config::SPDP_BURST_SCALE_PCT = 200;
  Config::SPDP_RESEND_PERIOD_MS = 1234;
  Config::SPDP_BURST_WINDOW_MS = 1;
  REQUIRE_TRUE(at(P4 + 1).interval == 1234);
}

TEST(spd_steady_state) {
  ConfigGuard g;
  Config::SPDP_RESEND_PERIOD_MS = 4321;
  Config::SPDP_BURST_WINDOW_MS = 1000000;
  Result r = at(P4);
  REQUIRE_TRUE(r.phase == SPDPBroadcastPhase::STEADY);
  REQUIRE_TRUE(r.interval == 4321);
  Config::SPDP_BURST_WINDOW_MS = static_cast<uint32_t>((P3 + P4) / 2);
  REQUIRE_TRUE(at(P3 + 1).phase == SPDPBroadcastPhase::BURST_PHASE_4);
  r = at(Config::SPDP_BURST_WINDOW_MS);
  REQUIRE_TRUE(r.phase == SPDPBroadcastPhase::STEADY);
  REQUIRE_TRUE(r.interval == 4321);
  Config::SPDP_BURST_WINDOW_MS = 0;
  REQUIRE_TRUE(at(0).phase == SPDPBroadcastPhase::BURST_PHASE_1);
  REQUIRE_TRUE(at(P3 + 1).phase == SPDPBroadcastPhase::STEADY);
  REQUIRE_TRUE(at(UINT64_MAX).phase == SPDPBroadcastPhase::STEADY);
}

TEST(spd_runtime_change) {
  ConfigGuard g;
  Config::SPDP_BURST_WINDOW_MS = 100;
  Config::SPDP_RESEND_PERIOD_MS = 5000;
  REQUIRE_TRUE(at(P4 + 1).interval == 5000);
  Config::SPDP_RESEND_PERIOD_MS = 700;
  REQUIRE_TRUE(at(P4 + 1).interval == 700);
  Config::SPDP_BURST_SCALE_PCT = 300;
  REQUIRE_TRUE(at(0).interval == Config::SPDP_BURST_INTERVAL_1_MS * 3);
}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }
