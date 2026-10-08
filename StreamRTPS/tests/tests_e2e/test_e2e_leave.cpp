#include "e2e_util.h"

using namespace tests;
using namespace rtps;

namespace {

constexpr int kBudget = 30000;

std::vector<size_t> without(size_t n, const std::vector<size_t> &gone) {
  std::vector<size_t> v;
  for (size_t i = 0; i < n; ++i)
    if (std::find(gone.begin(), gone.end(), i) == gone.end()) v.push_back(i);
  return v;
}

void leaveNonRoot(size_t n, bool crash) {
  Cluster c;
  shortLease(c);
  startConverged(c, n);
  const size_t victim = n / 2;
  if (crash) c.crash(victim); else c.kill(victim);
  REQUIRE_TRUE(waitConverged(c, kBudget, only(c.live(), true)));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void rootLeaves(size_t n, bool crash) {
  Cluster c;
  shortLease(c);
  startConverged(c, n);
  if (crash) c.crash(0); else c.kill(0);
  REQUIRE_TRUE(waitConverged(c, kBudget, only(c.live(), true)));
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(3000)));
  REQUIRE_TRUE(oracleCheck(c, only(c.live(), true)).empty());
  REQUIRE_TRUE(dataRoundTrip(c));
}

void massLeave() {
  const size_t n = 16;
  Cluster c;
  shortLease(c);
  startConverged(c, n);
  for (size_t i = 0; i < n; i += 2) {
    if (i % 4 == 0) c.kill(i); else c.crash(i);
  }
  REQUIRE_TRUE(waitConverged(c, kBudget, only(c.live(), true)));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void leaveAsJoinPartner() {
  Cluster c;
  shortLease(c);
  startConverged(c, 4);
  c.cfg.set(Config::SNAP_SERVER_POLICY, 1);
  c.crash(0);
  size_t j = c.add(0xF0);
  addRingEndpoints(c, j, 4);
  c.start(j);
  REQUIRE_TRUE(waitConverged(c, kBudget, only(c.live(), true)));
  REQUIRE_TRUE(dataRoundTrip(c));
}

size_t restartSame(Cluster &c, size_t i, size_t n) {
  uint8_t b = c.node(i).prefix.id[0];
  c.kill(i);
  (void)n;
  size_t k = c.add(b);
  c.addWriter(k, "R" + std::to_string(i));
  c.start(k);
  return k;
}

void rejoinSame(bool afterExpiry) {
  const size_t n = 8;
  Cluster c;
  shortLease(c);
  startConverged(c, n);
  const size_t victim = 3;
  if (afterExpiry) {
    uint8_t b = c.node(victim).prefix.id[0];
    c.kill(victim);
    REQUIRE_TRUE(waitConverged(c, kBudget, only(c.live(), true)));
    size_t k = c.add(b);
    c.addWriter(k, "R3");
    c.start(k);
  } else {
    restartSame(c, victim, n);
  }
  REQUIRE_TRUE(waitConverged(c, kBudget, only(c.live(), true)));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void rejoinNewPrefix() {
  const size_t n = 8;
  Cluster c;
  shortLease(c);
  startConverged(c, n);
  c.kill(3);
  size_t k = c.add(static_cast<uint8_t>(c.node(3).prefix.id[0] + 3));
  addRingEndpoints(c, k, n);
  c.start(k);
  REQUIRE_TRUE(waitConverged(c, kBudget, only(c.live(), true)));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void rollingRestart() {
  const size_t n = 8;
  Cluster c;
  shortLease(c);
  startConverged(c, n);
  for (size_t i = 0; i < n; ++i) {
    uint8_t b = prefixByte(i, n);
    size_t idx = c.size();
    for (size_t j = 0; j < c.size(); ++j)
      if (c.node(j).alive && c.node(j).prefix.id[0] == b) idx = j;
    REQUIRE_TRUE(idx < c.size());
    c.kill(idx);
    size_t k = c.add(b);
    addRingEndpoints(c, k, n);
    c.start(k);
    REQUIRE_TRUE(waitConverged(c, kBudget));
  }
  REQUIRE_TRUE(waitConverged(c, kBudget, only(c.live(), true)));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void lastTwo() {
  const size_t n = 4;
  Cluster c;
  shortLease(c);
  startConverged(c, n);
  c.kill(0);
  c.kill(2);
  REQUIRE_TRUE(waitConverged(c, kBudget, only(c.live(), true)));
  REQUIRE_TRUE(dataRoundTrip(c));
  c.kill(1);
  REQUIRE_TRUE(waitConverged(c, kBudget, only(c.live(), true)));
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(2000)));
  REQUIRE_TRUE(oracleCheck(c, only(c.live(), true)).empty());
}

void registerAll() {
  for (size_t n : {8, 32}) {
    ADD_TEST_FN("clean_non_root_" + std::to_string(n), [n] { leaveNonRoot(n, false); });
  }
  ADD_TEST_FN(std::string("crash_non_root_8"), [] { leaveNonRoot(8, true); });
  for (size_t n : {8, 32}) {
    ADD_TEST_FN("root_leaves_clean_" + std::to_string(n), [n] { rootLeaves(n, false); });
    ADD_TEST_FN("root_leaves_crash_" + std::to_string(n), [n] { rootLeaves(n, true); });
  }
  ADD_TEST_FN(std::string("mass_leave_16"), [] { massLeave(); });
  ADD_TEST_FN(std::string("leave_as_join_partner"), [] { leaveAsJoinPartner(); });
  ADD_TEST_FN(std::string("rejoin_same_prefix_before_expiry"), [] { rejoinSame(false); });
  ADD_TEST_FN(std::string("rejoin_same_prefix_after_expiry"), [] { rejoinSame(true); });
  ADD_TEST_FN(std::string("rejoin_new_prefix"), [] { rejoinNewPrefix(); });
  ADD_TEST_FN(std::string("rolling_restart"), [] { rollingRestart(); });
  ADD_TEST_FN(std::string("last_two"), [] { lastTwo(); });
}

}

int main(int argc, char **argv) {
  registerAll();
  RUN_TESTS(argc, argv);
}
