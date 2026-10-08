#include "gedp_tap_e4.h"

using namespace tests;
using namespace tests::e4;
using namespace rtps;

namespace {

constexpr int kBudgetMs = 30000;

TEST(root_lowered_by_response) {
  std::atomic<int> mode{0};
  std::atomic<MockNetworkDriver *> bridgeA{nullptr}, bridgeB{nullptr};
  std::atomic<MockNetworkDriver *> lowSide[2] = {{nullptr}, {nullptr}};
  std::atomic<uint64_t> announcesFromBridgeB{0};
  std::atomic<MockNetworkDriver *> sideB[2] = {{nullptr}, {nullptr}};
  Cluster c;
  size_t a1 = c.add(0x10), a2 = c.add(0x20), b1 = c.add(0x80), b2 = c.add(0x90);
  auto inA = [&](MockNetworkDriver *d) { return d == lowSide[0] || d == lowSide[1]; };
  auto inB = [&](MockNetworkDriver *d) { return d == sideB[0] || d == sideB[1]; };
  MockNetworkRouter::instance().setDropFilter(
      [&](MockNetworkDriver *s, MockNetworkDriver *d, const PacketInfo &info) {
        if (!lowSide[0].load() || !sideB[1].load()) return true;
        const bool cross = (inA(s) && inB(d)) || (inB(s) && inA(d));
        if (!cross) return false;
        const int m = mode.load();
        if (m == 2) return false;
        if (m == 1) {
          const bool bridge = (s == bridgeA && d == bridgeB) || (s == bridgeB && d == bridgeA);
          if (bridge) {
            if (s == bridgeB && classify(info) == Pkt::Announce) ++announcesFromBridgeB;
            return false;
          }
        }
        return true;
      });
  c.addWriter(b1, "RL"); c.addReader(a2, "RL");
  c.startAll();
  lowSide[0] = c.driver(a1); lowSide[1] = c.driver(a2);
  sideB[0] = c.driver(b1); sideB[1] = c.driver(b2);
  bridgeA = c.driver(a1); bridgeB = c.driver(b1);

  OracleOptions oa; oa.only = {a1, a2};
  OracleOptions ob; ob.only = {b1, b2};
  REQUIRE_TRUE(waitConverged(c, kBudgetMs, oa));
  REQUIRE_TRUE(waitConverged(c, kBudgetMs, ob));
  REQUIRE_TRUE(c.agent(b1)->getCurrentRoot() == c.node(b1).prefix);

  announcesFromBridgeB = 0;
  mode = 1;
  REQUIRE_TRUE(waitFor([&] { return c.agent(b1)->getCurrentRoot() == c.node(a1).prefix; }, kBudgetMs));
  REQUIRE_TRUE(waitFor([&] { return announcesFromBridgeB.load() > 0; }, kBudgetMs));
  mode = 2;
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
}

TEST(root_never_rises) {
  std::atomic<bool> partitioned{false};
  std::vector<MockNetworkDriver *> lo, hi;
  std::atomic<bool> ready{false};
  Cluster c;
  size_t n[4];
  const uint8_t pre[4] = {0x20, 0x40, 0x80, 0xA0};
  for (int i = 0; i < 4; ++i) n[i] = c.add(pre[i]);
  Sampler s(c, {n[0], n[1], n[2], n[3]});
  MockNetworkRouter::instance().setDropFilter(
      [&](MockNetworkDriver *a, MockNetworkDriver *b, const PacketInfo &) {
        if (!ready || !partitioned) return false;
        auto in = [](const std::vector<MockNetworkDriver *> &v, MockNetworkDriver *d) {
          for (auto *x : v) if (x == d) return true;
          return false;
        };
        return (in(lo, a) && in(hi, b)) || (in(hi, a) && in(lo, b));
      });
  c.start(n[0]);
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  c.start(n[2]);
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  c.start(n[1]);
  c.start(n[3]);
  lo = {c.driver(n[0]), c.driver(n[1])};
  hi = {c.driver(n[2]), c.driver(n[3])};
  ready = true;
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  partitioned = true;
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(3000)));
  partitioned = false;
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  s.stop();
  if (s.rootRose) std::cerr << "  root rose on node " << s.riseNode.load() << std::endl;
  REQUIRE_TRUE(!s.rootRose);
}

void deadRootJoiner(uint8_t joinerByte) {
  Cluster c;
  fastLease(c, 500, 4000);
  size_t r = c.add(0x10), m = c.add(0x60), h = c.add(0xE0);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  const GuidPrefix_t pr = c.node(r).prefix;

  c.crash(r);
  size_t j = c.add(joinerByte);
  Sampler s(c, {j});
  s.setForbidden(pr);
  c.start(j);
  const bool stale = listsPeer(c, m, r) && listsPeer(c, h, r);
  REQUIRE_TRUE(waitFor([&] { return stateOf(c, j) == SnapEDPState::Discovered; }, kBudgetMs));
  const bool staleAtDiscovered = stale && listsPeer(c, m, r);
  std::cerr << "  stale claim window at join discovered: " << staleAtDiscovered << std::endl;
  REQUIRE_TRUE(staleAtDiscovered);
  REQUIRE_TRUE(!s.sawForbidden);

  OracleOptions o; o.requireNoDead = true;
  REQUIRE_TRUE(waitConverged(c, 40000, o));
  s.stop();
  REQUIRE_TRUE(!s.sawForbidden);
  const GuidPrefix_t want = joinerByte < 0x60 ? c.node(j).prefix : c.node(m).prefix;
  for (size_t i : c.live()) REQUIRE_TRUE(c.agent(i)->getCurrentRoot() == want);
}

TEST(dead_root_not_adopted_joiner) { deadRootJoiner(0x90); }

TEST(rootless_joiner_recovers) { deadRootJoiner(0x30); }

TEST(dead_root_not_adopted_member) {
  Cluster c;
  fastLease(c, 500, 2500);
  size_t r = c.add(0x10), m = c.add(0x60), h = c.add(0xE0);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  const GuidPrefix_t pr = c.node(r).prefix;
  REQUIRE_TRUE(c.agent(m)->getCurrentRoot() == pr);

  Sampler s(c, {m});
  MockNetworkRouter::instance().blackout(c.driver(r), c.driver(m), 120000);
  const auto t0 = std::chrono::steady_clock::now();
  REQUIRE_TRUE(waitFor([&] { return !listsPeer(c, m, r); }, 30000));
  REQUIRE_TRUE(waitFor([&] { return c.agent(m)->getCurrentRoot() == c.node(m).prefix; }, kBudgetMs));
  const bool hStillHasR = listsPeer(c, h, r);
  std::cerr << "  M purged R after "
            << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count()
            << " ms, H still lists R: " << hStillHasR << std::endl;
  s.setForbidden(pr);
  if (hStillHasR) std::this_thread::sleep_for(std::chrono::milliseconds(scaled(300)));
  c.crash(r);
  OracleOptions o; o.only = {m, h}; o.requireNoDead = true;
  REQUIRE_TRUE(waitConverged(c, 40000, o));
  s.stop();
  REQUIRE_TRUE(!s.sawForbidden);
  REQUIRE_TRUE(c.agent(m)->getCurrentRoot() == c.node(m).prefix);
  REQUIRE_TRUE(c.agent(h)->getCurrentRoot() == c.node(m).prefix);
}

TEST(root_death_takeover) {
  Cluster c;
  fastLease(c);
  size_t n[4];
  const uint8_t pre[4] = {0x10, 0x30, 0x70, 0xB0};
  for (int i = 0; i < 4; ++i) n[i] = c.add(pre[i]);
  c.addWriter(n[3], "RT"); c.addReader(n[2], "RT");
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  c.crash(n[0]);
  OracleOptions o; o.requireNoDead = true;
  REQUIRE_TRUE(waitConverged(c, 40000, o));
  for (size_t i : c.live()) REQUIRE_TRUE(c.agent(i)->getCurrentRoot() == c.node(n[1]).prefix);
  REQUIRE_TRUE(dataRoundTrip(c));
}

}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }
