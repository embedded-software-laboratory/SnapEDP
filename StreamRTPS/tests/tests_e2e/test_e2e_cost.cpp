#include "scenario_util.h"

#include <algorithm>

using namespace tests;
using namespace rtps;

namespace {

uint64_t envOr(const char *name, uint64_t dflt) {
  const char *e = std::getenv(name);
  return e ? std::strtoull(e, nullptr, 0) : dflt;
}

void quiescence(size_t n) {
  Cluster c;
  addNodes(c, n);
  addRing(c, n);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 30000 + static_cast<int>(n) * 500));
  REQUIRE_TRUE(dataRoundTrip(c));
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  bool silent = c.quiet(5000);
  auto t = c.traffic();
  std::cout << "  n=" << n << " window: total=" << t.total << " gedp=" << t.snapEdp << std::endl;
  REQUIRE_TRUE(t.total > 0);
  REQUIRE_TRUE(silent);
  REQUIRE_TRUE(waitConverged(c, 5000));
}

void resyncBounded() {
  Cluster c;
  MockNetworkRouter::instance().setModel(lossyModel(10));
  addNodes(c, 16);
  addRing(c, 16);
  c.resetTraffic();
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 120000));
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(5000)));
  REQUIRE_TRUE(waitConverged(c, 30000));
  uint64_t maxPair = c.maxSnapEDPUnicastPerPair();
  std::cout << "  maxPair=" << maxPair << " unicast=" << c.traffic().snapEdpUnicast << std::endl;
  const uint64_t bound = envOr("EMBRTPS_COST_RESYNC_PAIR_BOUND", 20);
  REQUIRE_TRUE(maxPair <= bound);
}

void messageBudget(size_t n) {
  Cluster c;
  addNodes(c, n);
  addRing(c, n);
  c.resetTraffic();
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 30000 + static_cast<int>(n) * 500));
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(1000)));
  auto t = c.traffic();
  std::cout << "  n=" << n << " gedp=" << t.snapEdp << " bytes=" << t.snapEdpBytes
            << " unicast=" << t.snapEdpUnicast << std::endl;
  const std::string tag = std::to_string(n);
  const uint64_t pktBudget = envOr(("EMBRTPS_COST_GEDP_PKTS_" + tag).c_str(), n == 8 ? 150 : 2400);
  const uint64_t byteBudget = envOr(("EMBRTPS_COST_GEDP_BYTES_" + tag).c_str(), n == 8 ? 50000 : 1100000);
  REQUIRE_TRUE(t.snapEdp <= pktBudget);
  REQUIRE_TRUE(t.snapEdpBytes <= byteBudget);
}

void convergenceTime(size_t n) {
  std::vector<double> ms;
  for (int run = 0; run < 5; ++run) {
    Cluster c;
    addNodes(c, n);
    addRing(c, n);
    auto t0 = std::chrono::steady_clock::now();
    c.startAll();
    REQUIRE_TRUE(waitFor([&] { return oracleCheck(c).empty(); },
                         30000 + static_cast<int>(n) * 500, 5));
    ms.push_back(std::chrono::duration<double, std::milli>(
                     std::chrono::steady_clock::now() - t0).count());
  }
  std::sort(ms.begin(), ms.end());
  std::cout << "  n=" << n << " median_ms=" << ms[2] << " min=" << ms[0] << " max=" << ms[4] << std::endl;
  REQUIRE_TRUE(ms[2] <= static_cast<double>(envOr(("EMBRTPS_COST_CONV_MS_" + std::to_string(n)).c_str(), n == 8 ? 500 : 2000)));
}

void registerAll() {
  for (size_t n : {8, 32}) {
    ADD_TEST_FN("quiescence_" + std::to_string(n), [n] { quiescence(n); });
    ADD_TEST_FN("message_budget_" + std::to_string(n), [n] { messageBudget(n); });
    ADD_TEST_FN("convergence_time_budget_" + std::to_string(n), [n] { convergenceTime(n); });
  }
  ADD_TEST_FN(std::string("resync_bounded"), resyncBounded);
}

}

int main(int argc, char **argv) {
  registerAll();
  RUN_TESTS(argc, argv);
}
