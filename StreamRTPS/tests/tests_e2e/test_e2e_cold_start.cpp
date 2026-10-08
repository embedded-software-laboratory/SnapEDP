#include "oracle.h"
#include "profiles.h"

using namespace tests;
using namespace rtps;

namespace {

uint8_t prefixByte(size_t i, size_t n) {
  return static_cast<uint8_t>(0x10 + i * (0xD0 / n));
}

void addEndpoints(Cluster &c, size_t n, size_t eps) {
  for (size_t i = 0; i < n; ++i)
    for (size_t e = 0; e < eps; ++e) {
      c.addWriter(i, "T" + std::to_string(i) + "_" + std::to_string(e));
      c.addReader(i, "T" + std::to_string((i + 1) % n) + "_" + std::to_string(e));
    }
}

void makeNodes(Cluster &c, size_t n, DiscoveryMode m = DiscoveryMode::Snap) {
  for (size_t i = 0; i < n; ++i) c.add(prefixByte(i, n), m);
}

int budgetFor(size_t n) { return 20000 + static_cast<int>(n) * 500; }

void simultaneous(size_t n) {
  Cluster c;
  makeNodes(c, n);
  addEndpoints(c, n, n <= 8 ? 1 : 0);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, budgetFor(n)));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void simultaneousStandard(size_t n) {
  Cluster c;
  makeNodes(c, n, DiscoveryMode::Standard);
  addEndpoints(c, n, 1);
  c.resetTraffic();
  c.startAll();
  REQUIRE_TRUE(waitFor(
      [&] {
        for (size_t i = 0; i < n; ++i)
          if (c.node(i).part->getRemoteParticipantCount() < n - 1) return false;
        return true;
      },
      budgetFor(n)));
  REQUIRE_TRUE(dataRoundTrip(c, budgetFor(n)));
  std::cout << "  standard N=" << n << " packets=" << c.traffic().total << std::endl;
}

void lowestStartsLast(size_t n) {
  Cluster c;
  makeNodes(c, n);
  addEndpoints(c, n, 1);
  for (size_t i = 1; i < n; ++i) c.start(i);
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(100)));
  c.start(0);
  REQUIRE_TRUE(waitConverged(c, budgetFor(n)));
  REQUIRE_TRUE(c.agent(1)->getCurrentRoot() == c.node(0).prefix);
  REQUIRE_TRUE(dataRoundTrip(c));
}

void lowestStartsFirst() {
  const size_t n = 8;
  Cluster c;
  makeNodes(c, n);
  addEndpoints(c, n, 1);
  c.start(0);
  REQUIRE_TRUE(waitFor(
      [&] { return c.agent(0)->getCurrentState() == SnapEDPState::Discovered; }, 5000));
  const GuidPrefix_t root = c.agent(0)->getCurrentRoot();
  REQUIRE_TRUE(root == c.node(0).prefix);
  for (size_t i = 1; i < n; ++i) c.start(i);
  bool changed = false;
  bool ok = waitFor(
      [&] {
        for (size_t i = 0; i < n; ++i)
          if (!(c.agent(i)->getCurrentRoot() == root) &&
              !(c.agent(i)->getCurrentRoot() == GUIDPREFIX_UNKNOWN) &&
              !(c.agent(i)->getCurrentRoot() == c.node(i).prefix))
            changed = true;
        return oracleCheck(c).empty();
      },
      budgetFor(n), 20);
  REQUIRE_TRUE(ok);
  REQUIRE_TRUE(!changed);
  REQUIRE_TRUE(c.agent(0)->getCurrentRoot() == root);
  REQUIRE_TRUE(dataRoundTrip(c));
}

void twoWaves() {
  const size_t n = 16;
  Cluster c;
  makeNodes(c, n);
  addEndpoints(c, n, 1);
  for (size_t i = 0; i < n / 2; ++i) c.start(i);
  OracleOptions first;
  for (size_t i = 0; i < n / 2; ++i) first.only.push_back(i);
  REQUIRE_TRUE(waitConverged(c, budgetFor(n), first));
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(
      static_cast<int>(Config::SNAP_TIMEOUT_ANNOUNCING_MS.load()) * 4 + 100)));
  for (size_t i = n / 2; i < n; ++i) c.start(i);
  bool bad = false;
  bool ok = waitFor(
      [&] {
        for (size_t i = 0; i < n / 2; ++i) {
          auto s = c.agent(i)->getCurrentState();
          if (s != SnapEDPState::Discovered && s != SnapEDPState::Reconcile) bad = true;
        }
        return oracleCheck(c).empty();
      },
      budgetFor(n), 10);
  REQUIRE_TRUE(ok);
  REQUIRE_TRUE(!bad);
  REQUIRE_TRUE(dataRoundTrip(c));
}

void staggered() {
  const size_t n = 32;
  Cluster c;
  makeNodes(c, n);
  addEndpoints(c, n, 1);
  for (size_t i = 0; i < n; ++i) {
    c.start(i);
    std::this_thread::sleep_for(std::chrono::milliseconds(scaled(50)));
  }
  REQUIRE_TRUE(waitConverged(c, budgetFor(n)));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void profileN16(const std::string &profile) {
  const size_t n = 16;
  Cluster c;
  makeNodes(c, n);
  addEndpoints(c, n, 1);
  applyProfile(profile);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 120000));
  MockNetworkRouter::instance().setModel(LinkModel{});
  REQUIRE_TRUE(dataRoundTrip(c, 30000));
}

void asymmetric() {
  const size_t n = 8;
  Cluster c;
  makeNodes(c, n);
  addEndpoints(c, n, 1);
  c.startAll();
  applyAsymmetricUplink(c.driver(3), 0.30);
  REQUIRE_TRUE(waitConverged(c, 60000));
  MockNetworkRouter::instance().clearLinkModels();
  REQUIRE_TRUE(dataRoundTrip(c, 30000));
}

void endpointHeavy(size_t eps, bool expectMultiFrame) {
  const size_t n = 8;
  Cluster c;
  makeNodes(c, n);
  addEndpoints(c, n, eps);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 60000));
  REQUIRE_TRUE(dataRoundTrip(c, 60000));
  if (expectMultiFrame) {
    std::cout << "  gedpMax=" << c.traffic().snapEdpMax << std::endl;
    REQUIRE_TRUE(c.traffic().snapEdpMax > 1000);
  }
}

void simultaneous128() {
  Cluster c;
  makeNodes(c, 128);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 180000));
}

void registerAll() {
  for (size_t n : {2, 3, 8, 16, 32, 64})
    ADD_TEST_FN("simultaneous_" + std::to_string(n), [n] { simultaneous(n); });
  for (size_t n : {8, 32})
    ADD_TEST_FN("simultaneous_standard_" + std::to_string(n),
                [n] { simultaneousStandard(n); });
  for (size_t n : {8, 32})
    ADD_TEST_FN("lowest_starts_last_" + std::to_string(n), [n] { lowestStartsLast(n); });
  ADD_TEST_FN(std::string("lowest_starts_first_8"), [] { lowestStartsFirst(); });
  ADD_TEST_FN(std::string("two_waves_16"), [] { twoWaves(); });
  ADD_TEST_FN(std::string("staggered_32"), [] { staggered(); });
  for (const char *p : {"lan", "wifi", "lossy-5", "lossy-10", "lossy-20", "burst",
                        "reorder", "bursty", "duplicate"}) {
    std::string name = p;
    ADD_TEST_FN("profile_N16_" + name, [name] { profileN16(name); });
  }
  ADD_TEST_FN(std::string("asymmetric_8"), [] { asymmetric(); });
  ADD_TEST_FN(std::string("endpoint_heavy_0"), [] { endpointHeavy(0, false); });
  ADD_TEST_FN(std::string("endpoint_heavy_16"), [] { endpointHeavy(16, false); });
  ADD_TEST_FN(std::string("endpoint_heavy_multi"), [] { endpointHeavy(40, true); });
  ADD_TEST_FN(std::string("simultaneous_128"), [] { simultaneous128(); });
}

}

int main(int argc, char **argv) {
  registerAll();
  RUN_TESTS(argc, argv);
}
