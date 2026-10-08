#include "e2e_util.h"

using namespace tests;
using namespace rtps;

namespace {

constexpr int kBudget = 40000;

void startSplit(Cluster &c, size_t n, const std::vector<std::vector<size_t>> &groups) {
  for (size_t i = 0; i < n; ++i) c.add(prefixByte(i, n));
  for (size_t i = 0; i < n; ++i) addRingEndpoints(c, i, n);
  Gate gate;
  c.startAll();
  std::vector<std::vector<MockNetworkDriver *>> dg;
  for (auto &g : groups) dg.push_back(drivers(c, g));
  MockNetworkRouter::instance().partition(dg);
  gate.open();
  for (auto &g : groups) REQUIRE_TRUE(waitConverged(c, kBudget, only(g)));
}

void healAndConverge(Cluster &c, bool noDead = false) {
  MockNetworkRouter::instance().heal();
  REQUIRE_TRUE(waitConverged(c, kBudget, only(c.live(), noDead)));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void mergeKPlusK(size_t k) {
  const size_t n = 2 * k;
  Cluster c;
  std::vector<size_t> a, b;
  for (size_t i = 0; i < n; ++i) (i % 2 == 0 ? a : b).push_back(i);
  startSplit(c, n, {a, b});
  healAndConverge(c);
}

void mergeUneven() {
  const size_t n = 16;
  Cluster c;
  const size_t lone = 7;
  startSplit(c, n, {{lone}, [&] {
    std::vector<size_t> v;
    for (size_t i = 0; i < n; ++i) if (i != lone) v.push_back(i);
    return v;
  }()});
  healAndConverge(c);
}

void threeWaySplit() {
  Cluster c;
  std::vector<size_t> g0, g1, g2;
  for (size_t i = 0; i < 12; ++i) (i % 3 == 0 ? g0 : i % 3 == 1 ? g1 : g2).push_back(i);
  startSplit(c, 12, {g0, g1, g2});
  healAndConverge(c);
}

void splitHalves(Cluster &c, size_t n, std::vector<size_t> &a, std::vector<size_t> &b) {
  for (size_t i = 0; i < n; ++i) (i % 2 == 0 ? a : b).push_back(i);
  MockNetworkRouter::instance().partition({drivers(c, a), drivers(c, b)});
}

void splitBeforeExpiry() {
  const size_t n = 8;
  Cluster c;
  shortLease(c, 500, 6000);
  startConverged(c, n, kBudget);
  std::vector<size_t> a, b;
  splitHalves(c, n, a, b);
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(1500)));
  for (size_t i = 0; i < n; ++i)
    REQUIRE_TRUE(c.node(i).part->getRemoteParticipantCount() == n - 1);
  healAndConverge(c);
  for (size_t i = 0; i < n; ++i)
    REQUIRE_TRUE(c.node(i).part->getRemoteParticipantCount() == n - 1);
}

void splitAfterExpiry() {
  const size_t n = 8;
  Cluster c;
  shortLease(c);
  startConverged(c, n, kBudget);
  std::vector<size_t> a, b;
  splitHalves(c, n, a, b);
  REQUIRE_TRUE(waitConverged(c, kBudget, only(a, true)));
  REQUIRE_TRUE(waitConverged(c, kBudget, only(b, true)));
  healAndConverge(c, true);
}

void oneWayPartition() {
  const size_t n = 4;
  Cluster c;
  for (size_t i = 0; i < n; ++i) c.add(prefixByte(i, n));
  for (size_t i = 0; i < n; ++i) addRingEndpoints(c, i, n);
  Gate gate;
  c.startAll();
  MockNetworkDriver *from = c.driver(1), *to = c.driver(2);
  gate.open([=](MockNetworkDriver *s, MockNetworkDriver *d) { return s == from && d == to; });
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(3000)));
  REQUIRE_TRUE(!oracleCheck(c).empty());
  gate.setRule(nullptr);
  REQUIRE_TRUE(waitConverged(c, kBudget));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void flapping() {
  const size_t n = 8;
  Cluster c;
  startConverged(c, n, kBudget);
  const int period = static_cast<int>(4 * Config::SNAP_RECONCILE_BASE_MS.load());
  std::vector<size_t> a, b;
  for (size_t i = 0; i < n; ++i) (i % 2 == 0 ? a : b).push_back(i);
  c.resetTraffic();
  for (int cycle = 0; cycle < 12; ++cycle) {
    MockNetworkRouter::instance().partition({drivers(c, a), drivers(c, b)});
    std::this_thread::sleep_for(std::chrono::milliseconds(scaled(period)));
    MockNetworkRouter::instance().heal();
    std::this_thread::sleep_for(std::chrono::milliseconds(scaled(period)));
  }
  REQUIRE_TRUE(waitConverged(c, kBudget));
  REQUIRE_TRUE(dataRoundTrip(c));
  auto t = c.traffic();
  std::cerr << "  gedp packets=" << t.snapEdp << " bytes=" << t.snapEdpBytes << std::endl;
  REQUIRE_TRUE(t.snapEdp < 200ull * n * n);
}

void rootIsolated() {
  const size_t n = 8;
  Cluster c;
  shortLease(c);
  startConverged(c, n, kBudget);
  MockNetworkRouter::instance().isolate(c.driver(0));
  std::vector<size_t> rest;
  for (size_t i = 1; i < n; ++i) rest.push_back(i);
  REQUIRE_TRUE(waitConverged(c, kBudget, only(rest, true)));
  healAndConverge(c);
  REQUIRE_TRUE(c.agent(1)->getCurrentRoot() == c.node(0).prefix);
}

void churnDuringPartition() {
  const size_t n = 8;
  Cluster c;
  shortLease(c);
  startConverged(c, n, kBudget);
  std::vector<size_t> a, b;
  splitHalves(c, n, a, b);
  Endpoint *extraA = c.addWriter(a[1], "ChurnA");
  c.addReader(a[2], "ChurnB");
  c.addWriter(b[1], "ChurnB");
  c.addReader(b[2], "ChurnA");
  c.removeEndpoint(a[3], c.node(a[3]).eps[0].get());
  (void)extraA;
  REQUIRE_TRUE(waitConverged(c, kBudget, only(a, true)));
  REQUIRE_TRUE(waitConverged(c, kBudget, only(b, true)));
  healAndConverge(c);
}

void registerAll() {
  for (size_t k : {2, 4, 16})
    ADD_TEST_FN("merge_k_plus_k_" + std::to_string(k), [k] { mergeKPlusK(k); });
  ADD_TEST_FN(std::string("merge_uneven"), [] { mergeUneven(); });
  ADD_TEST_FN(std::string("three_way_split"), [] { threeWaySplit(); });
  ADD_TEST_FN(std::string("split_then_heal_before_expiry"), [] { splitBeforeExpiry(); });
  ADD_TEST_FN(std::string("split_then_heal_after_expiry"), [] { splitAfterExpiry(); });
  ADD_TEST_FN(std::string("one_way_partition"), [] { oneWayPartition(); });
  ADD_TEST_FN(std::string("flapping"), [] { flapping(); });
  ADD_TEST_FN(std::string("root_isolated"), [] { rootIsolated(); });
  ADD_TEST_FN(std::string("churn_during_partition"), [] { churnDuringPartition(); });
}

}

int main(int argc, char **argv) {
  registerAll();
  RUN_TESTS(argc, argv);
}
