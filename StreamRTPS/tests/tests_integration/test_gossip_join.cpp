#include "gedp_tap.h"
#include "oracle.h"

#include <atomic>
#include <optional>
#include <set>

using namespace tests;
using namespace rtps;

namespace {

using State = SnapEDPState;

bool isDiscovered(Cluster &c, size_t i) {
  return c.agent(i)->getCurrentState() == State::Discovered;
}

void scaleTimeouts(Cluster &c) {
  const double s = timeScale();
  if (s <= 1.0) return;
  c.cfg.set(Config::SNAP_TIMEOUT_INITIAL_MS, Config::SNAP_TIMEOUT_INITIAL_MS.load() * s);
  c.cfg.set(Config::SNAP_TIMEOUT_RETRANSMIT_MS, Config::SNAP_TIMEOUT_RETRANSMIT_MS.load() * s);
  c.cfg.set(Config::SNAP_TIMEOUT_ANNOUNCING_MS, Config::SNAP_TIMEOUT_ANNOUNCING_MS.load() * s);
  c.cfg.set(Config::SNAP_JITTER_MAX_MS, Config::SNAP_JITTER_MAX_MS.load() * s);
  c.cfg.set(Config::SPDP_RESEND_PERIOD_MS, Config::SPDP_RESEND_PERIOD_MS.load());
  c.cfg.set(Config::SPDP_BURST_SCALE_PCT, 100 * s);
}

size_t startServer(Cluster &c, uint8_t prefixByte) {
  size_t s = c.add(prefixByte);
  c.start(s);
  REQUIRE_TRUE(waitFor([&] { return isDiscovered(c, s); }, 10000, 10));
  return s;
}

void request_lost_once() {
  SnapEDPLog log;
  std::atomic<int> seen{0};
  Cluster c;
  scaleTimeouts(c);
  size_t srv = startServer(c, 0x10);
  size_t jn = c.add(0x80);
  installLoggingFilter(log, [&](const SnapEDPMsg &m, auto *, auto *, const PacketInfo &) {
    return m.kind == SnapEDP::JoinRequest && seen.fetch_add(1) == 0;
  });
  c.start(jn);
  REQUIRE_TRUE(waitConverged(c, 15000));
  const size_t reqs = log.count(SnapEDP::JoinRequest, &c.node(jn).prefix);
  std::cerr << "  join requests: " << reqs << std::endl;
  REQUIRE_TRUE(reqs >= 2 && reqs <= 3);
  (void)srv;
}

void request_lost_until_bound() {
  SnapEDPLog log;
  std::atomic<bool> dropAll{true};
  Cluster c;
  scaleTimeouts(c);
  startServer(c, 0x10);
  size_t jn = c.add(0x80);
  installLoggingFilter(log, [&](const SnapEDPMsg &m, auto *, auto *, const PacketInfo &) {
    return dropAll.load() && m.kind == SnapEDP::JoinRequest;
  });
  c.start(jn);
  const size_t bound = Config::REQUEST_RETRY_BOUND.load();
  bool sawSnap = false, exhausted = false;
  size_t atExhaustion = 0;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(scaled(15000));
  while (!exhausted && std::chrono::steady_clock::now() < deadline) {
    State st = c.agent(jn)->getCurrentState();
    if (st == State::Snapshot) {
      sawSnap = true;
    } else if (sawSnap) {
      atExhaustion = log.count(SnapEDP::JoinRequest, &c.node(jn).prefix);
      exhausted = true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  std::cerr << "  requests at exhaustion: " << atExhaustion << " (bound " << bound << ")" << std::endl;
  REQUIRE_TRUE(exhausted);
  REQUIRE_TRUE(atExhaustion == bound + 1);
  dropAll.store(false);
  REQUIRE_TRUE(waitConverged(c, 20000));
}

void skip_list_next_partner() {
  SnapEDPLog log;
  std::mutex mu;
  std::optional<GuidPrefix_t> muted;
  Cluster c;
  scaleTimeouts(c);
  startServer(c, 0x10);
  startServer(c, 0x20);
  REQUIRE_TRUE(waitConverged(c, 15000));
  size_t jn = c.add(0x80);
  installLoggingFilter(log, [&](const SnapEDPMsg &m, auto *, auto *, const PacketInfo &) {
    if (m.kind != SnapEDP::JoinRequest) return false;
    std::lock_guard<std::mutex> g(mu);
    if (!muted) muted = m.target;
    return m.target == *muted;
  });
  c.start(jn);
  REQUIRE_TRUE(waitConverged(c, 25000));
  std::vector<GuidPrefix_t> targets;
  for (auto &e : log.entries())
    if (e.msg.kind == SnapEDP::JoinRequest) targets.push_back(e.msg.target);
  const size_t bound = Config::REQUEST_RETRY_BOUND.load();
  REQUIRE_TRUE(targets.size() > bound + 1);
  for (size_t i = 0; i <= bound; ++i) REQUIRE_TRUE(targets[i] == targets[0]);
  REQUIRE_TRUE(!(targets[bound + 1] == targets[0]));
}

void response_lost() {
  SnapEDPLog log;
  std::atomic<int> seen{0};
  Cluster c;
  scaleTimeouts(c);
  size_t srv = startServer(c, 0x10);
  c.addWriter(srv, "JoinTopic");
  size_t jn = c.add(0x80);
  installLoggingFilter(log, [&](const SnapEDPMsg &m, auto *, auto *, const PacketInfo &) {
    return m.kind == SnapEDP::JoinResponse && seen.fetch_add(1) == 0;
  });
  c.start(jn);
  c.addReader(jn, "JoinTopic");
  REQUIRE_TRUE(waitConverged(c, 15000));
  REQUIRE_TRUE(log.count(SnapEDP::JoinResponse) >= 2);
  REQUIRE_TRUE(log.count(SnapEDP::JoinRequest, &c.node(jn).prefix) >= 2);
  REQUIRE_TRUE(dataRoundTrip(c));
}

void response_late_duplicate() {
  SnapEDPLog log;
  Cluster c;
  scaleTimeouts(c);
  c.cfg.set(Config::SNAP_TIMEOUT_INITIAL_MS, 600 * timeScale());
  size_t srv = startServer(c, 0x10);
  c.addWriter(srv, "JoinTopic");
  size_t jn = c.add(0x80);
  installLoggingFilter(log, nullptr);
  LinkModel m;
  m.min_delay_ms = m.max_delay_ms = 3.0 * Config::SNAP_TIMEOUT_RETRANSMIT_MS.load();
  MockNetworkRouter::instance().setLinkModel(c.driver(srv), nullptr, m);
  c.start(jn);
  REQUIRE_TRUE(waitConverged(c, 20000));
  REQUIRE_TRUE(waitFor([&] { return log.count(SnapEDP::JoinResponse) >= 2; }, 5000, 10));
  REQUIRE_TRUE(MockNetworkRouter::instance().drain(5000));
  const uint64_t h = c.agent(jn)->getLocalViewHash();
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(300)));
  REQUIRE_TRUE(c.agent(jn)->getLocalViewHash() == h);
  REQUIRE_TRUE(waitConverged(c, 10000));
}

void response_duplicated() {
  Cluster c;
  scaleTimeouts(c);
  size_t srv = startServer(c, 0x10);
  c.addWriter(srv, "JoinTopic");
  size_t jn = c.add(0x80);
  LinkModel m;
  m.duplicate_rate = 1.0;
  MockNetworkRouter::instance().setLinkModel(c.driver(srv), nullptr, m);
  c.start(jn);
  REQUIRE_TRUE(waitConverged(c, 20000));
  REQUIRE_TRUE(MockNetworkRouter::instance().drain(5000));
  REQUIRE_TRUE(MockNetworkRouter::instance().linkStats(c.driver(srv), c.driver(jn)).duplicated > 0);
  REQUIRE_TRUE(waitConverged(c, 5000));
}

struct BusyServer {
  SnapEDPLog log;
  std::atomic<bool> hold{false};
  std::atomic<bool> dropReqToOther{true};
  MockNetworkDriver *drv0 = nullptr;
  GuidPrefix_t p0{}, p1{};
};

void runBusyServer(int deferCap, size_t numJoiners) {
  BusyServer b;
  Cluster c;
  scaleTimeouts(c);
  c.cfg.set(Config::SNAP_DEFER_CAP, deferCap);
  size_t n0 = c.add(0x10), n1 = c.add(0x20);
  b.p0 = c.node(n0).prefix;
  b.p1 = c.node(n1).prefix;
  std::vector<size_t> joiners;
  for (size_t j = 0; j < numJoiners; ++j) joiners.push_back(c.add(static_cast<uint8_t>(0x40 + 0x10 * j)));
  c.start(n0);
  c.start(n1);
  b.drv0 = c.driver(n0);
  REQUIRE_TRUE(waitConverged(c, 15000));
  installLoggingFilter(b.log, [&b](const SnapEDPMsg &m, MockNetworkDriver *, MockNetworkDriver *dst,
                                   const PacketInfo &) {
    if (m.kind == SnapEDP::JoinRequest && m.target == b.p1 && b.dropReqToOther.load()) return true;
    if (!b.hold.load()) return false;
    if (m.kind == SnapEDP::Announce && dst == b.drv0) return true;
    if (m.kind == SnapEDP::ResyncResponse && m.target == b.p0) return true;
    return false;
  });
  b.hold.store(true);
  c.addWriter(n1, "BusyTopic");
  REQUIRE_TRUE(waitFor([&] { return c.agent(n0)->getCurrentState() == State::Reconcile; }, 10000, 10));
  for (size_t j : joiners) c.start(j);
  REQUIRE_TRUE(waitFor([&] {
    std::set<std::array<uint8_t, 12>> senders;
    for (auto &e : b.log.entries())
      if (e.msg.kind == SnapEDP::JoinRequest && e.msg.target == b.p0) senders.insert(e.msg.sender.id);
    return senders.size() == numJoiners;
  }, 15000, 20));
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(3 * Config::SNAP_TIMEOUT_RETRANSMIT_MS.load())));
  REQUIRE_TRUE(c.agent(n0)->getCurrentState() != State::Discovered);
  REQUIRE_TRUE(b.log.count(SnapEDP::JoinResponse, &b.p0) == 0);
  b.hold.store(false);
  REQUIRE_TRUE(waitConverged(c, 40000));
  REQUIRE_TRUE(b.log.count(SnapEDP::JoinResponse, &b.p0) > 0);
  REQUIRE_TRUE(dataRoundTrip(c));
}

void request_while_busy() { runBusyServer(4, 1); }
void defer_cap_overflow() { runBusyServer(4, 6); }
void defer_disabled() { runBusyServer(0, 1); }

void server_policy(uint32_t policy) {
  SnapEDPLog log;
  std::atomic<bool> ready{false}, split{true}, dropReq{true};
  std::atomic<MockNetworkDriver *> dX0{nullptr}, dX1{nullptr}, dY{nullptr};
  Cluster c;
  scaleTimeouts(c);
  c.cfg.set(Config::SNAP_SERVER_POLICY, policy);
  c.cfg.set(Config::SNAP_RNG_SEED, 7);
  MockNetworkRouter::instance().setSeed(7);
  size_t x0 = c.add(0x10), x1 = c.add(0x20), y = c.add(0x80), jn = c.add(0xF0);
  installLoggingFilter(log, [&](const SnapEDPMsg &m, MockNetworkDriver *s, MockNetworkDriver *d,
                                const PacketInfo &) {
    if (!ready.load()) return true;
    if (m.kind == SnapEDP::JoinRequest && dropReq.load()) return true;
    if (!split.load()) return false;
    auto inX = [&](MockNetworkDriver *p) { return p == dX0.load() || p == dX1.load(); };
    auto inY = [&](MockNetworkDriver *p) { return p == dY.load(); };
    return (inX(s) && inY(d)) || (inY(s) && inX(d));
  });
  c.start(x0);
  c.start(x1);
  c.start(y);
  dX0 = c.driver(x0);
  dX1 = c.driver(x1);
  dY = c.driver(y);
  ready.store(true);
  OracleOptions ox; ox.only = {x0, x1};
  OracleOptions oy; oy.only = {y};
  REQUIRE_TRUE(waitConverged(c, 20000, ox));
  REQUIRE_TRUE(waitConverged(c, 20000, oy));
  c.start(jn);

  const GuidPrefix_t px0 = c.node(x0).prefix, px1 = c.node(x1).prefix, py = c.node(y).prefix;
  auto targets = [&] {
    std::set<std::array<uint8_t, 12>> t;
    for (auto &e : log.entries())
      if (e.msg.kind == SnapEDP::JoinRequest) t.insert(e.msg.target.id);
    return t;
  };
  REQUIRE_TRUE(waitFor([&] { return targets().size() == 3; }, 15000, 20));
  const auto t = targets();
  auto has = [&](const GuidPrefix_t &p) { return t.count(p.id) > 0; };
  std::cerr << "  policy " << policy << " targets: X0=" << has(px0) << " X1=" << has(px1)
            << " Y=" << has(py) << std::endl;
  if (policy == 0) {
    REQUIRE_TRUE(has(px0) && has(px1));
    const size_t bound = Config::REQUEST_RETRY_BOUND.load();
    size_t toX0 = 0, toX1 = 0;
    for (auto &e : log.entries()) {
      if (e.msg.kind != SnapEDP::JoinRequest) continue;
      if (e.msg.target == px0) ++toX0;
      else if (e.msg.target == px1) ++toX1;
      else if (e.msg.target == py) {
        REQUIRE_TRUE(toX0 >= bound + 1 && toX1 >= bound + 1);
        break;
      }
    }
  } else {
    REQUIRE_TRUE(has(px0) && has(px1) && has(py));
  }
  dropReq.store(false);
  split.store(false);
  REQUIRE_TRUE(waitConverged(c, 40000));
}

void partner_dies_mid_handshake() {
  SnapEDPLog log;
  std::mutex mu;
  std::optional<GuidPrefix_t> victim;
  Cluster c;
  scaleTimeouts(c);
  size_t a = c.add(0x10), b = c.add(0x20), jn = c.add(0x80);
  c.start(a);
  c.start(b);
  REQUIRE_TRUE(waitConverged(c, 15000));
  installLoggingFilter(log, [&](const SnapEDPMsg &m, auto *, auto *, const PacketInfo &) {
    std::lock_guard<std::mutex> g(mu);
    if (m.kind == SnapEDP::JoinRequest && !victim) victim = m.target;
    return m.kind == SnapEDP::JoinResponse && victim && m.sender == *victim;
  });
  c.start(jn);
  REQUIRE_TRUE(waitFor([&] { std::lock_guard<std::mutex> g(mu); return victim.has_value(); }, 15000, 5));
  GuidPrefix_t v;
  { std::lock_guard<std::mutex> g(mu); v = *victim; }
  size_t vi = (c.node(a).prefix == v) ? a : b;
  size_t other = (vi == a) ? b : a;
  c.crash(vi);
  OracleOptions o;
  o.only = {other, jn};
  o.checkRoot = false;
  REQUIRE_TRUE(waitConverged(c, 30000, o));
  size_t toSurvivor = log.count(SnapEDP::JoinRequest, &c.node(jn).prefix, &c.node(other).prefix);
  REQUIRE_TRUE(toSurvivor >= 1);
}

void unresolvable_locator() {
  SnapEDPLog log;
  std::atomic<bool> block{true};
  std::atomic<MockNetworkDriver *> srvDrv{nullptr};
  Cluster c;
  scaleTimeouts(c);
  c.cfg.set(Config::REQUEST_RETRY_BOUND, 40);
  size_t srv = startServer(c, 0x10);
  srvDrv = c.driver(srv);
  size_t jn = c.add(0x80);
  installLoggingFilter(log, [&](const SnapEDPMsg &m, MockNetworkDriver *s, MockNetworkDriver *d,
                                const PacketInfo &) {
    return block.load() && d == srvDrv.load() && s != srvDrv.load() && m.kind != SnapEDP::JoinRequest;
  });
  c.start(jn);
  REQUIRE_TRUE(waitFor([&] { return log.count(SnapEDP::JoinRequest, &c.node(jn).prefix) >= 1; }, 15000, 10));
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(5 * Config::SNAP_TIMEOUT_RETRANSMIT_MS.load())));
  REQUIRE_TRUE(log.count(SnapEDP::JoinResponse) == 0);
  REQUIRE_TRUE(c.agent(jn)->getCurrentState() != State::Discovered);
  block.store(false);
  REQUIRE_TRUE(waitConverged(c, 30000));
  REQUIRE_TRUE(log.count(SnapEDP::JoinResponse) >= 1);
}

void registerAll() {
  registry().push_back({"request_lost_once", request_lost_once});
  registry().push_back({"request_lost_until_bound", request_lost_until_bound});
  registry().push_back({"skip_list_next_partner", skip_list_next_partner});
  registry().push_back({"response_lost", response_lost});
  registry().push_back({"response_late_duplicate", response_late_duplicate});
  registry().push_back({"response_duplicated", response_duplicated});
  registry().push_back({"request_while_busy", request_while_busy});
  registry().push_back({"defer_cap_overflow", defer_cap_overflow});
  registry().push_back({"defer_disabled", defer_disabled});
  for (uint32_t p : {0u, 2u})
    registry().push_back({"server_policy_" + std::to_string(p), [p] { server_policy(p); }});
  registry().push_back({"partner_dies_mid_handshake", partner_dies_mid_handshake});
  registry().push_back({"unresolvable_locator", unresolvable_locator});
}

}

int main(int argc, char **argv) {
  registerAll();
  RUN_TESTS(argc, argv);
}
