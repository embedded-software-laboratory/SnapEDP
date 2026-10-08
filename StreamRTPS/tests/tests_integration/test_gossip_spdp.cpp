#include "gedp_tap_e4.h"

using namespace tests;
using namespace tests::e4;
using namespace rtps;

namespace {

constexpr int kBudgetMs = 30000;

void quiescenceRounds(uint32_t rounds) {
  Cluster c;
  c.cfg.set(Config::SNAP_QUIESCENCE_ROUNDS, rounds);
  size_t a = c.add(0x20), b = c.add(0xA0);
  c.start(a);
  c.start(b);
  std::atomic<bool> stop{false};
  std::atomic<int> minRoundAtLeave{1 << 20};
  auto watch = [&](size_t i) {
    while (!stop) {
      const auto st = stateOf(c, i);
      if (st != SnapEDPState::Initial && st != SnapEDPState::Election) {
        int r = c.node(i).part->getSPDPAgent().getBroadcastRound();
        int cur = minRoundAtLeave.load();
        while (r < cur && !minRoundAtLeave.compare_exchange_weak(cur, r)) {}
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  };
  std::thread ta(watch, a), tb(watch, b);
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  stop = true;
  ta.join(); tb.join();
  std::cerr << "  rounds=" << rounds << " min round when leaving Initial=" << minRoundAtLeave.load() << std::endl;
  REQUIRE_TRUE(minRoundAtLeave.load() >= static_cast<int>(rounds));
}
TEST(quiescence_rounds_1) { quiescenceRounds(1); }

void maxRoundsRun(uint32_t maxRounds, bool expectElection) {
  Cluster c;
  c.cfg.set(Config::SNAP_TIMEOUT_INITIAL_MS, 60000);
  c.cfg.set(Config::SNAP_QUIESCENCE_ROUNDS, 1);
  c.cfg.set(Config::SNAP_QUIESCENCE_MAX_ROUNDS, maxRounds);
  size_t n[3];
  const uint8_t pre[3] = {0x20, 0x70, 0xB0};
  for (int i = 0; i < 3; ++i) n[i] = c.add(pre[i]);
  for (int i = 0; i < 3; ++i) {
    c.start(n[i]);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  if (expectElection) {
    REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  } else {
    std::this_thread::sleep_for(std::chrono::milliseconds(scaled(2500)));
    for (int i = 0; i < 3; ++i) REQUIRE_TRUE(stateOf(c, n[i]) == SnapEDPState::Initial);
  }
}
TEST(quiescence_max_rounds) { maxRoundsRun(4, true); }
TEST(quiescence_max_rounds_control) { maxRoundsRun(0, false); }

TEST(one_way_spdp) {
  std::atomic<MockNetworkDriver *> from{nullptr}, to{nullptr};
  std::atomic<bool> block{true};
  Cluster c;
  size_t a = c.add(0x20), b = c.add(0x90);
  MockNetworkRouter::instance().setDropFilter(
      [&](MockNetworkDriver *s, MockNetworkDriver *d, const PacketInfo &) {
        if (!from.load() || !to.load()) return true;
        return block && s == from && d == to;
      });
  c.start(a);
  c.start(b);
  from = c.driver(a);
  to = c.driver(b);
  REQUIRE_TRUE(waitFor([&] { return listsPeer(c, a, b); }, kBudgetMs));
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(2000)));
  REQUIRE_TRUE(!listsPeer(c, b, a));
  REQUIRE_TRUE(!oracleCheck(c).empty());
  block = false;
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
}

TEST(lease_boundary) {
  const uint32_t lease = 3000, resend = 300;
  Cluster c;
  fastLease(c, resend, lease);
  size_t a = c.add(0x20), b = c.add(0x90);
  c.addWriter(b, "LB");
  c.addReader(a, "LB");
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  const auto t0 = std::chrono::steady_clock::now();
  c.crash(b);
  auto since = [&] {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
  };
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(lease / 2)));
  REQUIRE_TRUE(listsPeer(c, a, b));
  REQUIRE_TRUE(waitFor([&] { return !listsPeer(c, a, b); }, 30000, 10));
  const long purged = since();
  std::cerr << "  purged " << purged << " ms after last contact (lease " << lease << " ms)" << std::endl;
  REQUIRE_TRUE(purged >= static_cast<long>(scaled(lease - resend - 100)));
  REQUIRE_TRUE(c.agent(a)->getLocalViewHash() == groundTruthHash(c, {a}));
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(2 * resend + 500)));
  REQUIRE_TRUE(!listsPeer(c, a, b));
  REQUIRE_TRUE(c.node(a).part->getRemoteParticipantCount() == 0);
}

TEST(return_after_purge) {
  Cluster c;
  fastLease(c, 300, 1500);
  size_t a = c.add(0x20), b = c.add(0x60), d = c.add(0xB0);
  Endpoint *oldW = c.addWriter(d, "OldLife");
  c.addReader(a, "OldLife");
  c.addWriter(b, "Stay");
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  c.crash(d);
  REQUIRE_TRUE(waitFor([&] { return !listsPeer(c, a, d) && !listsPeer(c, b, d); }, 30000, 20));
  c.removeEndpoint(d, oldW);
  c.addWriter(d, "NewLife");
  c.addReader(a, "NewLife");
  MockNetworkRouter::instance().heal();
  c.node(d).blackholed = false;
  REQUIRE_TRUE(waitConverged(c, 40000));
  REQUIRE_TRUE(c.agent(a)->getLocalViewHash() == groundTruthHash(c, {a, b, d}));
  REQUIRE_TRUE(dataRoundTrip(c));
}

TEST(election_backoff_order) {
  Cluster c;
  c.cfg.set(Config::SNAP_JITTER_MAX_MS, 900);
  c.cfg.set(Config::SNAP_TIMEOUT_INITIAL_MS, 30);
  size_t n[3];
  const uint8_t pre[3] = {0x10, 0x80, 0xF0};
  for (int i = 0; i < 3; ++i) n[i] = c.add(pre[i]);
  std::atomic<long long> left[3] = {{0}, {0}, {0}};
  std::atomic<bool> stop{false};
  auto nowMs = [] {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
  };
  std::vector<std::thread> ths;
  for (int i = 0; i < 3; ++i)
    ths.emplace_back([&, i] {
      while (!stop && left[i] == 0) {
        if (c.node(n[i]).started && stateOf(c, n[i]) != SnapEDPState::Initial) left[i] = nowMs();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    });
  c.startAll();
  REQUIRE_TRUE(waitFor([&] { return left[0] && left[1] && left[2]; }, kBudgetMs));
  stop = true;
  for (auto &t : ths) t.join();
  std::cerr << "  left Initial at +" << left[1] - left[0] << " ms, +" << left[2] - left[0] << " ms rel. to lowest" << std::endl;
  REQUIRE_TRUE(left[0] <= left[1]);
  REQUIRE_TRUE(left[0] <= left[2]);
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
}

}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }
