#include "gedp_tap_e3.h"

using namespace tests;
using namespace tests::e3;
using namespace rtps;

namespace {

struct State {
  std::atomic<int> dropped{0};
  std::atomic<int> seen{0};
  std::atomic<bool> hold{false};
  std::atomic<bool> mute{false};
};

LinkModel delayModel(double ms) {
  LinkModel m;
  m.min_delay_ms = ms;
  m.max_delay_ms = ms;
  return m;
}

void bootWithEndpoints(Cluster &c, std::initializer_list<std::pair<uint8_t, int>> nodes) {
  for (auto &n : nodes) {
    size_t i = c.add(n.first);
    if (n.second > 0) addMany(c, i, n.second, "rc");
  }
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 40000));
}

TEST(multi_frame_response) {
  Cluster c;
  speedUpSpdp(c);
  bootWithEndpoints(c, {{0x10, 14}, {0x50, 14}});
  Recorder rec(c);
  const size_t j = c.add(0x90);
  c.start(j);
  REQUIRE_TRUE(waitConverged(c, 40000));
  size_t best = 0;
  for (int s = 0; s < 2; ++s) {
    size_t n = rec.count(K::JoinResp, s, static_cast<int>(j), true);
    if (n > best) best = n;
  }
  std::cerr << "  join response frames=" << best << std::endl;
  REQUIRE_TRUE(best >= 3);
}

TEST(multi_frame_frame_lost) {
  Cluster c;
  speedUpSpdp(c);
  bootWithEndpoints(c, {{0x10, 14}, {0x50, 14}});
  Recorder rec(c);
  auto st = std::make_shared<State>();
  const int jIdx = 2;
  rec.setDrop([st, jIdx](int, int d, const Msg &m) {
    if (m.kind == K::JoinResp && d == jIdx && ++st->seen == 2) {
      st->dropped++;
      return true;
    }
    return false;
  });
  const size_t j = c.add(0x90);
  c.start(j);
  REQUIRE_TRUE(waitConverged(c, 40000));
  REQUIRE_TRUE(st->dropped.load() == 1);
  REQUIRE_TRUE(rec.count(K::ResyncReq, jIdx) >= 1);
  rec.clearDrop();
}

TEST(multi_frame_shrink) {
  Cluster c;
  speedUpSpdp(c);
  bootWithEndpoints(c, {{0x10, 0}, {0x50, 10}, {0x90, 0}});
  Recorder rec(c);
  rec.mapAll();
  auto st = std::make_shared<State>();
  rec.setDrop([st](int s, int d, const Msg &m) {
    if (m.kind == K::Announce && m.numEndpoints > 0 && s == 1 && d == 2 && st->dropped.load() == 0) {
      st->dropped++;
      return true;
    }
    return false;
  });
  MockNetworkRouter::instance().setLinkModel(c.driver(1), c.driver(2), delayModel(400));
  rec.clear();
  c.addWriter(1, "rc03_trigger");
  REQUIRE_TRUE(waitFor([&] { return rec.count(K::ResyncResp, 1, 2, true) >= 2; }, 10000));
  for (auto &e : rec.select(K::ResyncResp, 1, 2, true)) REQUIRE_TRUE(e.msg.numEndpoints < e.msg.totalEndpoints);
  std::vector<Endpoint *> victims;
  for (auto &ep : c.node(1).eps)
    if (victims.size() < 5 && ep->topic != "rc03_trigger") victims.push_back(ep.get());
  for (auto *v : victims) c.removeEndpoint(1, v);
  REQUIRE_TRUE(waitConverged(c, 40000));
  MockNetworkRouter::instance().clearLinkModels();
  rec.clearDrop();
}

TEST(single_frame_replacement) {
  Cluster c;
  speedUpSpdp(c);
  boot(c, {0x10, 0x50, 0x90});
  auto *a = c.addWriter(1, "rc04a");
  auto *b = c.addWriter(1, "rc04b");
  c.addReader(2, "rc04a");
  c.addReader(2, "rc04b");
  REQUIRE_TRUE(waitConverged(c, 20000));
  REQUIRE_TRUE(dataRoundTrip(c));
  const Guid_t ag = a->guid(), bg = b->guid();
  Reader *ra = c.node(2).eps[0]->reader;
  Reader *rb = c.node(2).eps[1]->reader;
  REQUIRE_TRUE(ra->knowWriterId(ag) && rb->knowWriterId(bg));

  Recorder rec(c);
  rec.mapAll();
  auto st = std::make_shared<State>();
  rec.setDrop([st](int s, int d, const Msg &m) {
    if (m.kind == K::Dispose && s == 1 && d == 2) {
      st->dropped++;
      return true;
    }
    return false;
  });
  c.removeEndpoint(1, a);
  REQUIRE_TRUE(waitFor([&] { return st->dropped.load() > 0; }, 5000));
  REQUIRE_TRUE(waitConverged(c, 20000));
  REQUIRE_TRUE(waitFor([&] { return !ra->knowWriterId(ag); }, 5000));
  REQUIRE_TRUE(rb->knowWriterId(bg));
  bool single = false;
  for (auto &e : rec.select(K::ResyncResp, 1, 2, true))
    if (e.msg.numEndpoints == e.msg.totalEndpoints) single = true;
  REQUIRE_TRUE(single);
  rec.clearDrop();
}

TEST(target_unresponsive) {
  Cluster c;
  c.cfg.set(Config::SNAP_RECONCILE_BASE_MS, scaled(200));
  c.cfg.set(Config::SNAP_RECONCILE_CAP_MS, scaled(1600));
  c.cfg.set(Config::SNAP_TIMEOUT_RETRANSMIT_MS, scaled(10));
  speedUpSpdp(c);
  boot(c, {0x10, 0x50, 0x90});
  Recorder rec(c);
  rec.mapAll();
  auto st = std::make_shared<State>();
  rec.setDrop([st](int s, int d, const Msg &m) {
    if (st->mute.load() && m.kind == K::ResyncReq && s == 2 && d == 1) return true;
    if (m.kind == K::Announce && m.numEndpoints > 0 && s == 1 && d == 2 && st->dropped.load() == 0) {
      st->dropped++;
      return true;
    }
    return false;
  });
  st->mute.store(true);
  rec.clear();
  c.addWriter(1, "rc05");
  const double burstGap = 100.0 * timeScale();
  std::vector<std::chrono::steady_clock::time_point> starts;
  auto collect = [&] {
    starts.clear();
    std::chrono::steady_clock::time_point last{};
    for (auto &e : rec.select(K::ResyncReq, 2, 1)) {
      if (starts.empty() || msBetween(last, e.at) > burstGap) starts.push_back(e.at);
      last = e.at;
    }
    return starts.size();
  };
  REQUIRE_TRUE(waitFor([&] { return collect() >= 5; }, 30000, 100));
  std::vector<double> gaps;
  for (size_t i = 1; i < starts.size(); ++i) gaps.push_back(msBetween(starts[i - 1], starts[i]));
  for (double g : gaps) std::cerr << "  gap_ms=" << g << std::endl;
  REQUIRE_TRUE(gaps[1] >= 1.5 * gaps[0]);
  REQUIRE_TRUE(gaps[2] >= 1.5 * gaps[1]);
  REQUIRE_TRUE(gaps[3] <= 1.3 * gaps[2]);
  REQUIRE_TRUE(gaps[3] <= (1600.0 + 300.0) * timeScale());
  st->mute.store(false);
  REQUIRE_TRUE(waitConverged(c, 30000));
  rec.clearDrop();
}

TEST(grace_suppresses_resync) {
  Cluster c;
  const int graceMs = scaled(700);
  c.cfg.set(Config::SNAP_RECONCILE_GRACE_MS, graceMs);
  speedUpSpdp(c);
  boot(c, {0x10, 0x50});
  Recorder rec(c);
  rec.setDrop([](int s, int, const Msg &m) {
    return m.kind == K::Announce && m.numEndpoints > 0 && s == 2;
  });
  const size_t j = c.add(0x90);
  c.addWriter(j, "rc06");
  const auto t0 = std::chrono::steady_clock::now();
  c.start(j);
  REQUIRE_TRUE(waitConverged(c, 30000));
  auto reqs = rec.select(K::ResyncReq, -1, static_cast<int>(j));
  REQUIRE_TRUE(!reqs.empty());
  double first = 1e18;
  for (auto &e : reqs) first = std::min(first, msBetween(t0, e.at));
  std::cerr << "  first resync request toward the joiner after " << first << " ms (grace " << graceMs << ")" << std::endl;
  REQUIRE_TRUE(first >= graceMs - 20);
  rec.clearDrop();
}

TEST(in_flight_flag_released) {
  Cluster c;
  c.cfg.set(Config::SNAP_RECONCILE_BASE_MS, scaled(100));
  c.cfg.set(Config::SNAP_RECONCILE_CAP_MS, scaled(400));
  speedUpSpdp(c);
  boot(c, {0x10, 0x50, 0x90});
  Recorder rec(c);
  rec.mapAll();
  auto st = std::make_shared<State>();
  st->mute.store(true);
  rec.setDrop([st](int s, int d, const Msg &m) {
    if (st->mute.load() && m.kind == K::ResyncResp && s == 1 && d == 2) return true;
    if (m.kind == K::Announce && m.numEndpoints > 0 && s == 1 && d == 2 && st->dropped.load() == 0) {
      st->dropped++;
      return true;
    }
    return false;
  });
  rec.clear();
  c.addWriter(1, "rc07");
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(1500)));
  const size_t during = rec.count(K::ResyncReq, 2, 1);
  std::cerr << "  requests while answers are lost=" << during << std::endl;
  REQUIRE_TRUE(during > Config::REQUEST_RETRY_BOUND.load() + 1);
  st->mute.store(false);
  REQUIRE_TRUE(waitConverged(c, 30000));
  rec.clearDrop();
}

TEST(sticky_target_cleared) {
  Cluster c;
  c.cfg.set(Config::SPDP_LEASE_DURATION_MS, scaled(2500));
  speedUpSpdp(c, 200);
  boot(c, {0x10, 0x50, 0x90});
  Recorder rec(c);
  rec.mapAll();
  auto st = std::make_shared<State>();
  rec.setDrop([st](int s, int d, const Msg &m) {
    if (m.kind == K::ResyncResp && s == 1 && d == 2) return true;
    if (m.kind == K::Announce && m.numEndpoints > 0 && s == 1 && d == 2 && st->dropped.load() == 0) {
      st->dropped++;
      return true;
    }
    return false;
  });
  c.addWriter(1, "rc08");
  REQUIRE_TRUE(waitFor([&] { return rec.count(K::ResyncReq, 2, 1) >= 1; }, 10000));
  c.kill(1);
  OracleOptions o;
  o.requireNoDead = true;
  REQUIRE_TRUE(waitConverged(c, 40000, o));
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(1000)));
  REQUIRE_TRUE(c.agent(2)->getCurrentState() == SnapEDPState::Discovered);
  REQUIRE_TRUE(c.agent(0)->getCurrentState() == SnapEDPState::Discovered);
  rec.clearDrop();
}

}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }
