#include "oracle.h"
#include "profiles.h"

#include <map>

using namespace tests;
using namespace rtps;

namespace {

uint8_t prefixByte(size_t i, size_t n) {
  return static_cast<uint8_t>(0x20 + i * (0xA0 / n));
}

void addBase(Cluster &c, size_t n, size_t eps = 1) {
  for (size_t i = 0; i < n; ++i) c.add(prefixByte(i, n));
  for (size_t i = 0; i < n; ++i)
    for (size_t e = 0; e < eps; ++e) {
      c.addWriter(i, "J" + std::to_string(i) + "_" + std::to_string(e));
      c.addReader(i, "J" + std::to_string((i + 1) % n) + "_" + std::to_string(e));
    }
}

size_t addJoiner(Cluster &c, uint8_t first, size_t eps = 1, const std::string &tag = "N") {
  size_t j = c.add(first);
  if (tag == "H") {
    for (size_t e = 0; e < eps; ++e) {
      const std::string t = "JH_" + std::to_string(e);
      c.addWriter(j, t);
      c.addReader(j, t + "b");
      c.addReader(1, t);
      c.addWriter(1, t + "b");
    }
    return j;
  }
  for (size_t e = 0; e < eps; ++e) {
    c.addReader(j, "J0_" + std::to_string(e % 1));
    c.addWriter(j, "JW" + tag + std::to_string(j) + "_" + std::to_string(e));
  }
  return j;
}

bool existingRanElection(Cluster &c, size_t existing) {
  for (size_t i = 0; i < existing; ++i) {
    auto s = c.agent(i)->getCurrentState();
    if (s == SnapEDPState::Initial || s == SnapEDPState::Election) return true;
  }
  return false;
}

void earlyJoin() {
  Cluster c;
  addBase(c, 8);
  for (size_t i = 0; i < 8; ++i) c.start(i);
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(20)));
  size_t j = addJoiner(c, 0x71);
  c.start(j);
  REQUIRE_TRUE(waitConverged(c, 30000));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void lateJoin(size_t n, uint8_t joinerByte, bool joinerBecomesRoot) {
  Cluster c;
  addBase(c, n);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 30000));
  const GuidPrefix_t oldRoot = c.agent(0)->getCurrentRoot();
  size_t j = addJoiner(c, joinerByte);
  c.start(j);
  bool election = false;
  bool ok = waitFor(
      [&] {
        if (!joinerBecomesRoot && existingRanElection(c, n)) election = true;
        return oracleCheck(c).empty();
      },
      30000, 10);
  REQUIRE_TRUE(ok);
  if (joinerBecomesRoot) {
    for (size_t i = 0; i < c.size(); ++i)
      REQUIRE_TRUE(c.agent(i)->getCurrentRoot() == c.node(j).prefix);
  } else {
    for (size_t i = 0; i < c.size(); ++i)
      REQUIRE_TRUE(c.agent(i)->getCurrentRoot() == oldRoot);
    REQUIRE_TRUE(!election);
  }
  REQUIRE_TRUE(dataRoundTrip(c));
}

void massLateJoin(size_t n, size_t k) {
  Cluster c;
  addBase(c, n);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 30000));
  std::vector<size_t> joiners;
  for (size_t i = 0; i < k; ++i)
    joiners.push_back(addJoiner(c, static_cast<uint8_t>(0xC0 + i * 3), 1, "M"));
  for (size_t j : joiners) c.start(j);
  REQUIRE_TRUE(waitConverged(c, 90000));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void joinLossy(const std::string &profile) {
  Cluster c;
  addBase(c, 16);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 30000));
  applyProfile(profile);
  size_t j = addJoiner(c, 0x71);
  c.start(j);
  REQUIRE_TRUE(waitConverged(c, 120000));
  MockNetworkRouter::instance().setModel(LinkModel{});
  REQUIRE_TRUE(dataRoundTrip(c, 30000));
}

struct Groups {
  std::mutex m;
  std::map<MockNetworkDriver *, int> group;
  std::atomic<bool> active{true};
  void install() {
    MockNetworkRouter::instance().setDropFilter(
        [this](MockNetworkDriver *s, MockNetworkDriver *d, const PacketInfo &) {
          if (!active.load()) return false;
          std::lock_guard<std::mutex> g(m);
          auto a = group.find(s), b = group.find(d);
          if (a == group.end() || b == group.end()) return true;
          return a->second != b->second;
        });
  }
  void assign(Cluster &c, size_t i, int g) {
    std::lock_guard<std::mutex> l(m);
    group[c.driver(i)] = g;
  }
};

void joinDuringPartition() {
  Cluster c;
  addBase(c, 8);
  Groups g;
  g.install();
  c.startAll();
  for (size_t i = 0; i < 8; ++i) g.assign(c, i, i < 4 ? 0 : 1);
  OracleOptions left, right;
  for (size_t i = 0; i < 4; ++i) left.only.push_back(i);
  for (size_t i = 4; i < 8; ++i) right.only.push_back(i);
  REQUIRE_TRUE(waitConverged(c, 30000, left));
  REQUIRE_TRUE(waitConverged(c, 30000, right));
  size_t j = addJoiner(c, 0x71);
  c.start(j);
  g.assign(c, j, 0);
  OracleOptions leftJ = left;
  leftJ.only.push_back(j);
  REQUIRE_TRUE(waitConverged(c, 30000, leftJ));
  g.active.store(false);
  REQUIRE_TRUE(waitConverged(c, 60000));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void joinWhileRootDies() {
  Cluster c;
  c.cfg.set(Config::SPDP_RESEND_PERIOD_MS, 500);
  c.cfg.set(Config::SPDP_LEASE_DURATION_MS, 2000);
  addBase(c, 8);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 30000));
  size_t j = addJoiner(c, 0x71);
  c.crash(0);
  c.start(j);
  OracleOptions o;
  o.requireNoDead = true;
  bool conv = waitConverged(c, 60000, o);
  if (!conv) {
    uint64_t deadAtoms = 0;
    for (auto &ep : c.node(0).eps)
      deadAtoms ^= snap_detail::endpointHashAtom(c.node(0).prefix, ep->guid());
    for (size_t i : c.live())
      std::cerr << "  node[" << i << "] hash^truth=0x" << std::hex
                << (c.agent(i)->getLocalViewHash() ^ groundTruthHash(c, c.live()))
                << " deadRootAtoms=0x" << deadAtoms << std::dec << std::endl;
  }
  REQUIRE_TRUE(conv);
  REQUIRE_TRUE(c.agent(1)->getCurrentRoot() == c.node(1).prefix);
  REQUIRE_TRUE(dataRoundTrip(c));
}

void joinerManyEndpoints() {
  Cluster c;
  addBase(c, 8);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 30000));
  size_t j = addJoiner(c, 0x71, 40, "H");
  c.resetTraffic();
  c.start(j);
  REQUIRE_TRUE(waitConverged(c, 60000));
  REQUIRE_TRUE(dataRoundTrip(c, 30000));
}

void registerAll() {
  ADD_TEST_FN(std::string("early_join_8"), [] { earlyJoin(); });
  for (size_t n : {8, 32}) {
    ADD_TEST_FN("late_join_mid_" + std::to_string(n), [n] { lateJoin(n, 0x20 + 0xA0 / 2 + 1, false); });
    ADD_TEST_FN("late_join_high_" + std::to_string(n), [n] { lateJoin(n, 0xF0, false); });
    ADD_TEST_FN("late_join_low_" + std::to_string(n), [n] { lateJoin(n, 0x05, true); });
  }
  ADD_TEST_FN(std::string("mass_late_join_16"), [] { massLateJoin(16, 16); });
  for (const char *p : {"lossy-10", "reorder"}) {
    std::string name = p;
    ADD_TEST_FN("join_lossy_" + name, [name] { joinLossy(name); });
  }
  ADD_TEST_FN(std::string("join_during_partition"), [] { joinDuringPartition(); });
  ADD_TEST_FN(std::string("join_while_root_dies"), [] { joinWhileRootDies(); });
  ADD_TEST_FN(std::string("joiner_with_many_endpoints"), [] { joinerManyEndpoints(); });
}

}

int main(int argc, char **argv) {
  registerAll();
  RUN_TESTS(argc, argv);
}
