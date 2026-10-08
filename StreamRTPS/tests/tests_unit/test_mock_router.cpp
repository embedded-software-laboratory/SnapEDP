#include "harness.h"

#include "rtps/communication/MockNetworkDriver.h"
#include "rtps/communication/MockNetworkRouter.h"
#include "rtps/utils/udpUtils.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <memory>
#include <mutex>
#include <thread>

using namespace rtps;
using Clock = std::chrono::steady_clock;
using tests::scaled;
using tests::waitFor;

namespace {

constexpr uint8_t TYPE_DATA = 0, TYPE_PING = 1, TYPE_PONG = 2;

Ip4Port_t ucast(int i) { return getUserUnicastPort(static_cast<ParticipantId_t>(i)); }
Ip4Port_t mcast() { return getBuiltInMulticastPort(); }

std::vector<uint8_t> makePayload(uint32_t seq, size_t size = 16, uint8_t type = TYPE_DATA) {
  size = std::max<size_t>(size, 5);
  std::vector<uint8_t> v(size);
  v[0] = type;
  for (int i = 0; i < 4; ++i) v[1 + i] = static_cast<uint8_t>(seq >> (8 * i));
  for (size_t i = 5; i < size; ++i) v[i] = static_cast<uint8_t>(seq * 7 + i);
  return v;
}

uint32_t seqOf(const std::vector<uint8_t> &p) {
  uint32_t s = 0;
  for (int i = 0; i < 4; ++i) s |= static_cast<uint32_t>(p[1 + i]) << (8 * i);
  return s;
}

struct Rec {
  Ip4Port_t port;
  std::vector<uint8_t> data;
  Clock::time_point t;
};

struct Recorder {
  std::mutex m;
  std::vector<Rec> rx;
  std::atomic<size_t> count{0};
  std::function<void(const Rec &)> hook;
  bool store = true;

  size_t size() { return count.load(); }
  std::vector<Rec> snapshot() {
    std::lock_guard<std::mutex> g(m);
    return rx;
  }
};

void rxCb(void *arg, Ip4Port_t port, std::vector<uint8_t> &&p, const ip_struct_t *, Ip4Port_t) {
  auto *r = static_cast<Recorder *>(arg);
  Rec rec{port, std::move(p), Clock::now()};
  if (r->hook) r->hook(rec);
  if (r->store) {
    std::lock_guard<std::mutex> g(r->m);
    r->rx.push_back(std::move(rec));
  }
  r->count++;
}

struct Node {
  std::shared_ptr<Recorder> rec = std::make_shared<Recorder>();
  std::unique_ptr<MockNetworkDriver> drv;
  Ip4Port_t port;

  explicit Node(int idx, bool bindMcast = false) : port(ucast(idx)) {
    drv = std::make_unique<MockNetworkDriver>(&rxCb, rec.get());
    REQUIRE_TRUE(drv->createUdpConnection(port) != nullptr);
    if (bindMcast) REQUIRE_TRUE(drv->createUdpConnection(mcast(), true) != nullptr);
  }

  void send(Ip4Port_t dst, uint32_t seq, size_t size = 16, uint8_t type = TYPE_DATA) {
    PacketInfo info;
    info.srcPort = port;
    info.destPort = dst;
    info.destAddr = ip4_struct_t{0};
    info.buffer = PBufWrapper(makePayload(seq, size, type));
    drv->sendPacket(info);
  }
  MockNetworkDriver *d() { return drv.get(); }
};

MockNetworkRouter &R() { return MockNetworkRouter::instance(); }

void freshRouter(uint64_t seed = 1) {
  R().reset();
  R().setSeed(seed);
}

double ms(Clock::time_point a, Clock::time_point b) {
  return std::chrono::duration<double, std::milli>(b - a).count();
}

LinkModel delayModel(double lo, double hi, double mean = -1.0, bool order = true) {
  LinkModel m;
  m.min_delay_ms = lo;
  m.max_delay_ms = hi;
  m.mean_delay_ms = mean;
  m.preserve_order = order;
  return m;
}

std::vector<double> latencies(Recorder &r, const std::vector<Clock::time_point> &sent) {
  std::vector<double> out;
  for (auto &x : r.snapshot()) out.push_back(ms(sent[seqOf(x.data)], x.t));
  return out;
}

double mean(const std::vector<double> &v) {
  double s = 0;
  for (double x : v) s += x;
  return v.empty() ? 0 : s / v.size();
}

double meanLossRun(Recorder &r, uint32_t n) {
  std::vector<bool> got(n, false);
  for (auto &x : r.snapshot()) got[seqOf(x.data)] = true;
  size_t runs = 0, lost = 0;
  for (uint32_t i = 0; i < n; ++i) {
    if (!got[i]) {
      ++lost;
      if (i == 0 || got[i - 1]) ++runs;
    }
  }
  return runs ? static_cast<double>(lost) / runs : 0.0;
}

}

TEST(rtr_zero_model_sync) {
  freshRouter();
  Node a(0), b(1);
  for (uint32_t i = 0; i < 200; ++i) {
    a.send(b.port, i);
    REQUIRE_TRUE(b.rec->size() == i + 1);
  }
  auto rx = b.rec->snapshot();
  for (uint32_t i = 0; i < 200; ++i) REQUIRE_TRUE(seqOf(rx[i].data) == i);
  REQUIRE_TRUE(R().inFlight() == 0);
}

TEST(rtr_unicast_routing) {
  freshRouter();
  Node a(0), b(1), c(2);
  a.send(b.port, 1);
  REQUIRE_TRUE(b.rec->size() == 1 && c.rec->size() == 0 && a.rec->size() == 0);
  REQUIRE_TRUE(b.rec->snapshot()[0].port == b.port);
  a.send(a.port, 2);
  REQUIRE_TRUE(a.rec->size() == 0);
  a.send(ucast(9), 3);
  REQUIRE_TRUE(b.rec->size() == 1 && c.rec->size() == 0);
}

TEST(rtr_multicast_fanout) {
  freshRouter();
  Node a(0, true), b(1, true), c(2, true), d(3);
  a.send(mcast(), 1);
  REQUIRE_TRUE(a.rec->size() == 1 && b.rec->size() == 1 && c.rec->size() == 1);
  REQUIRE_TRUE(d.rec->size() == 0);
}

TEST(rtr_port_bind_unique) {
  freshRouter();
  Node a(0), b(1);
  REQUIRE_TRUE(b.drv->createUdpConnection(a.port) == nullptr);
  REQUIRE_TRUE(!b.drv->canBindPort(a.port));
  REQUIRE_TRUE(a.drv->createUdpConnection(mcast(), true) != nullptr);
  REQUIRE_TRUE(b.drv->createUdpConnection(mcast(), true) != nullptr);
  REQUIRE_TRUE(R().driverBoundToPort(a.port) == a.d());
  REQUIRE_TRUE(R().driverBoundToPort(ucast(5)) == nullptr);
}

TEST(rtr_drop_filter_per_receiver) {
  freshRouter();
  Node a(0, true), b(1, true), c(2, true);
  MockNetworkDriver *pa = a.d(), *pb = b.d();
  R().setDropFilter([=](MockNetworkDriver *s, MockNetworkDriver *d, const PacketInfo &) {
    return s == pa && d == pb;
  });
  a.send(mcast(), 1);
  REQUIRE_TRUE(b.rec->size() == 0);
  REQUIRE_TRUE(c.rec->size() == 1 && a.rec->size() == 1);
  b.send(mcast(), 2);
  REQUIRE_TRUE(a.rec->size() == 2 && c.rec->size() == 2 && b.rec->size() == 1);
  R().clearDropPolicy();
  a.send(mcast(), 3);
  REQUIRE_TRUE(b.rec->size() == 2);
}

TEST(rtr_drop_rate_converges) {
  const uint32_t n = 20000;
  for (double rate : {0.1, 0.5}) {
    freshRouter(7);
    Node a(0), b(1);
    R().setDropRate(rate);
    for (uint32_t i = 0; i < n; ++i) a.send(b.port, i);
    double observed = 1.0 - static_cast<double>(b.rec->size()) / n;
    REQUIRE_TRUE(std::abs(observed - rate) < 0.02);
    REQUIRE_TRUE(R().linkStats(a.d(), b.d()).dropped == n - b.rec->size());
  }
}

TEST(rtr_delay_bounds) {
  freshRouter();
  Node a(0), b(1);
  R().setModel(delayModel(5, 50, 15, false));
  const uint32_t n = 300;
  std::vector<Clock::time_point> sent(n);
  for (uint32_t i = 0; i < n; ++i) {
    sent[i] = Clock::now();
    a.send(b.port, i);
  }
  REQUIRE_TRUE(R().drain(scaled(10000)));
  REQUIRE_TRUE(b.rec->size() == n);
  const double slack = 40.0 * tests::timeScale();
  for (double l : latencies(*b.rec, sent)) {
    REQUIRE_TRUE(l >= 5.0 - 0.5);
    REQUIRE_TRUE(l <= 50.0 + slack);
  }
}

TEST(rtr_delay_mean) {
  for (double target : {40.0, 160.0}) {
    freshRouter(3);
    Node a(0), b(1);
    R().setModel(delayModel(0, 200, target, false));
    const uint32_t n = 5000;
    std::vector<Clock::time_point> sent(n);
    for (uint32_t i = 0; i < n; ++i) {
      sent[i] = Clock::now();
      a.send(b.port, i);
    }
    REQUIRE_TRUE(R().drain(scaled(20000)));
    auto l = latencies(*b.rec, sent);
    REQUIRE_TRUE(l.size() == n);
    REQUIRE_TRUE(std::abs(mean(l) - target) < 0.1 * target);
  }
}

TEST(rtr_delay_degenerate) {
  freshRouter();
  Node a(0), b(1);
  R().setModel(delayModel(20, 20, 20, false));
  std::vector<Clock::time_point> sent(50);
  for (uint32_t i = 0; i < 50; ++i) {
    sent[i] = Clock::now();
    a.send(b.port, i);
  }
  REQUIRE_TRUE(R().drain(scaled(5000)));
  const double slack = 30.0 * tests::timeScale();
  for (double l : latencies(*b.rec, sent)) REQUIRE_TRUE(l >= 19.5 && l <= 20.0 + slack);

  for (auto m : {delayModel(10, 10, 3, false), delayModel(0, 10, 0, false),
                 delayModel(0, 10, 10, false), delayModel(0, 10, 50, false),
                 delayModel(0, 10, 5, false)}) {
    R().setModel(m);
    size_t before = b.rec->size();
    for (uint32_t i = 0; i < 20; ++i) a.send(b.port, i);
    REQUIRE_TRUE(R().drain(scaled(5000)));
    REQUIRE_TRUE(b.rec->size() == before + 20);
  }

  R().setModel(delayModel(0, 0));
  size_t before = b.rec->size();
  a.send(b.port, 0);
  REQUIRE_TRUE(b.rec->size() == before + 1);
}

TEST(rtr_preserve_order_true) {
  freshRouter();
  Node a(0), b(1);
  R().setModel(delayModel(0, 50, -1, true));
  const uint32_t n = 500;
  for (uint32_t i = 0; i < n; ++i) a.send(b.port, i);
  REQUIRE_TRUE(R().drain(scaled(10000)));
  auto rx = b.rec->snapshot();
  REQUIRE_TRUE(rx.size() == n);
  for (uint32_t i = 0; i < n; ++i) REQUIRE_TRUE(seqOf(rx[i].data) == i);
  REQUIRE_TRUE(R().linkStats(a.d(), b.d()).reordered == 0);
}

TEST(rtr_reorder_happens) {
  freshRouter();
  Node a(0), b(1);
  R().setModel(delayModel(0, 50, -1, false));
  const uint32_t n = 1000;
  for (uint32_t i = 0; i < n; ++i) a.send(b.port, i);
  REQUIRE_TRUE(R().drain(scaled(10000)));
  auto rx = b.rec->snapshot();
  REQUIRE_TRUE(rx.size() == n);
  uint64_t inversions = 0;
  uint32_t maxSeen = 0;
  for (size_t i = 0; i < rx.size(); ++i) {
    uint32_t s = seqOf(rx[i].data);
    if (i > 0 && s < maxSeen) ++inversions;
    maxSeen = std::max(maxSeen, s);
  }
  REQUIRE_TRUE(inversions >= 1);
  REQUIRE_TRUE(R().linkStats(a.d(), b.d()).reordered == inversions);
}

TEST(rtr_frame_loss_rate) {
  freshRouter(5);
  Node a(0), b(1);
  LinkModel m;
  m.frame_loss_rate = 0.05;
  R().setModel(m);
  const uint32_t n = 20000;
  for (uint32_t i = 0; i < n; ++i) a.send(b.port, i, 100);
  double loss = 1.0 - static_cast<double>(b.rec->size()) / n;
  REQUIRE_TRUE(std::abs(loss - 0.05) < 0.01);
}

TEST(rtr_frame_loss_fragments) {
  freshRouter(5);
  Node a(0), b(1);
  LinkModel m;
  m.frame_loss_rate = 0.05;
  m.mtu = 1500;
  R().setModel(m);
  const uint32_t n = 20000;
  for (uint32_t i = 0; i < n; ++i) a.send(b.port, i, 4000);
  double loss = 1.0 - static_cast<double>(b.rec->size()) / n;
  double expected = 1.0 - std::pow(0.95, 3);
  REQUIRE_TRUE(std::abs(loss - expected) < 0.015);
  REQUIRE_TRUE(loss > 0.05 + 0.04);
}

TEST(rtr_bitrate_spacing) {
  freshRouter();
  Node a(0), b(1);
  LinkModel m;
  m.bitrate_bps = 800000;
  R().setModel(m);
  const int n = 10;
  auto t0 = Clock::now();
  for (int i = 0; i < n; ++i) a.send(b.port, i, 1000);
  REQUIRE_TRUE(R().drain(scaled(5000)));
  auto rx = b.rec->snapshot();
  REQUIRE_TRUE(rx.size() == static_cast<size_t>(n));
  for (int i = 0; i < n; ++i) REQUIRE_TRUE(ms(t0, rx[i].t) >= (i + 1) * 10.0 - 1.0);
}

TEST(rtr_queue_limit_drops) {
  freshRouter();
  Node a(0), b(1);
  LinkModel m;
  m.bitrate_bps = 40000;
  m.queue_limit_bytes = 5000;
  R().setModel(m);
  for (int i = 0; i < 20; ++i) a.send(b.port, i, 1000);
  REQUIRE_TRUE(R().modelDropped() == 15);
  REQUIRE_TRUE(R().linkStats(a.d(), b.d()).queue_dropped == 15);
  REQUIRE_TRUE(R().queuedBytes() == 5000);
  REQUIRE_TRUE(R().drain(scaled(10000)));
  REQUIRE_TRUE(R().queuedBytes() == 0);
  REQUIRE_TRUE(b.rec->size() == 5);
}

TEST(rtr_duplicate_rate) {
  freshRouter(9);
  Node a(0), b(1);
  LinkModel m;
  m.duplicate_rate = 0.2;
  R().setModel(m);
  const uint32_t n = 5000;
  for (uint32_t i = 0; i < n; ++i) a.send(b.port, i, 32);
  auto rx = b.rec->snapshot();
  double dupFrac = static_cast<double>(rx.size() - n) / n;
  REQUIRE_TRUE(std::abs(dupFrac - 0.2) < 0.03);
  REQUIRE_TRUE(R().linkStats(a.d(), b.d()).duplicated == rx.size() - n);
  std::vector<int> count(n, 0);
  for (auto &x : rx) {
    uint32_t s = seqOf(x.data);
    ++count[s];
    REQUIRE_TRUE(x.data == makePayload(s, 32));
  }
  for (uint32_t i = 0; i < n; ++i) REQUIRE_TRUE(count[i] == 1 || count[i] == 2);
}

TEST(rtr_burst_loss_runs) {
  const uint32_t n = 20000;
  double geRun, uniRun;
  {
    freshRouter(11);
    Node a(0), b(1);
    LinkModel m;
    m.p_good_to_bad = 0.05;
    m.p_bad_to_good = 0.25;
    m.loss_good = 0.0;
    m.loss_bad = 1.0;
    R().setModel(m);
    for (uint32_t i = 0; i < n; ++i) a.send(b.port, i);
    geRun = meanLossRun(*b.rec, n);
  }
  double rate;
  {
    freshRouter(11);
    Node a(0), b(1);
    rate = 1.0 / 6.0;
    R().setDropRate(rate);
    for (uint32_t i = 0; i < n; ++i) a.send(b.port, i);
    uniRun = meanLossRun(*b.rec, n);
  }
  REQUIRE_TRUE(std::abs(geRun - 4.0) < 0.6);
  REQUIRE_TRUE(uniRun < 1.5);
  REQUIRE_TRUE(geRun > uniRun + 1.0);
}

TEST(rtr_burst_gate) {
  freshRouter();
  Node a(0), b(1);
  LinkModel m;
  m.burst_period_ms = 50;
  m.preserve_order = false;
  R().setModel(m);
  const int n = 80;
  auto t0 = Clock::now();
  for (int i = 0; i < n; ++i) {
    a.send(b.port, i);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  REQUIRE_TRUE(R().drain(scaled(5000)));
  auto rx = b.rec->snapshot();
  REQUIRE_TRUE(rx.size() == static_cast<size_t>(n));
  std::vector<std::vector<double>> clusters;
  double last = -1e9;
  for (auto &x : rx) {
    double t = ms(t0, x.t);
    if (t - last > 20.0) clusters.emplace_back();
    clusters.back().push_back(t);
    last = t;
  }
  const double slack = 15.0 * tests::timeScale();
  REQUIRE_TRUE(clusters.size() >= 4 && clusters.size() <= 12);
  for (auto &c : clusters) REQUIRE_TRUE(c.back() - c.front() < slack);
  for (size_t i = 1; i < clusters.size(); ++i) {
    double gap = clusters[i].front() - clusters[i - 1].front();
    REQUIRE_TRUE(gap > 50.0 - slack);
  }
}

TEST(rtr_late_tail) {
  freshRouter(13);
  Node a(0), b(1);
  LinkModel m;
  m.late_rate = 0.1;
  m.late_delay_ms = 150;
  m.preserve_order = false;
  R().setModel(m);
  const uint32_t n = 1000;
  std::vector<Clock::time_point> sent(n);
  for (uint32_t i = 0; i < n; ++i) {
    sent[i] = Clock::now();
    a.send(b.port, i);
  }
  REQUIRE_TRUE(R().drain(scaled(10000)));
  auto l = latencies(*b.rec, sent);
  REQUIRE_TRUE(l.size() == n);
  size_t late = 0;
  for (double x : l) {
    if (x >= 140.0) {
      ++late;
    } else {
      REQUIRE_TRUE(x < 140.0);
    }
  }
  REQUIRE_TRUE(std::abs(static_cast<double>(late) / n - 0.1) < 0.04);
}

TEST(rtr_blackout_window) {
  freshRouter();
  Node a(0), b(1), c(2);
  R().blackout(a.d(), b.d(), scaled(300));
  a.send(b.port, 1);
  a.send(c.port, 2);
  REQUIRE_TRUE(b.rec->size() == 0 && c.rec->size() == 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(300) + 50));
  a.send(b.port, 3);
  REQUIRE_TRUE(b.rec->size() == 1);
  REQUIRE_TRUE(seqOf(b.rec->snapshot()[0].data) == 3);
  REQUIRE_TRUE(R().linkStats(a.d(), b.d()).dropped == 1);
}

TEST(rtr_per_link_override) {
  freshRouter(17);
  Node a(0, true), b(1, true), c(2, true);
  R().setDropRate(0.1);
  LinkModel m;
  m.drop_rate = 0.5;
  R().setLinkModel(a.d(), b.d(), m);
  const uint32_t n = 6000;
  for (uint32_t i = 0; i < n; ++i) a.send(mcast(), i);
  double fb = static_cast<double>(b.rec->size()) / n;
  double fc = static_cast<double>(c.rec->size()) / n;
  REQUIRE_TRUE(std::abs(fb - 0.5) < 0.04);
  REQUIRE_TRUE(std::abs(fc - 0.9) < 0.04);
  R().clearLinkModels();
  LinkModel total;
  total.drop_rate = 1.0;
  R().setLinkModel(nullptr, c.d(), total);
  size_t cb = c.rec->size();
  for (uint32_t i = 0; i < 100; ++i) a.send(mcast(), i);
  REQUIRE_TRUE(c.rec->size() == cb);
}

TEST(rtr_asymmetric_link) {
  freshRouter();
  Node a(0), b(1);
  LinkModel m;
  m.drop_rate = 1.0;
  R().setLinkModel(a.d(), b.d(), m);
  for (int i = 0; i < 50; ++i) {
    a.send(b.port, i);
    b.send(a.port, i);
  }
  REQUIRE_TRUE(b.rec->size() == 0);
  REQUIRE_TRUE(a.rec->size() == 50);
}

TEST(rtr_partition_helpers) {
  freshRouter();
  Node a(0, true), b(1, true), c(2, true), d(3, true);
  auto counts = [&] {
    return std::vector<size_t>{a.rec->size(), b.rec->size(), c.rec->size(), d.rec->size()};
  };
  R().partition({{a.d(), b.d()}, {c.d(), d.d()}});
  a.send(mcast(), 1);
  REQUIRE_TRUE((counts() == std::vector<size_t>{1, 1, 0, 0}));
  c.send(mcast(), 2);
  REQUIRE_TRUE((counts() == std::vector<size_t>{1, 1, 1, 1}));
  a.send(c.port, 3);
  REQUIRE_TRUE(c.rec->size() == 1);
  R().heal();
  a.send(mcast(), 4);
  REQUIRE_TRUE((counts() == std::vector<size_t>{2, 2, 2, 2}));
  R().isolate(d.d());
  a.send(mcast(), 5);
  REQUIRE_TRUE((counts() == std::vector<size_t>{3, 3, 3, 2}));
  d.send(mcast(), 6);
  REQUIRE_TRUE((counts() == std::vector<size_t>{3, 3, 3, 3}));
  R().heal();
  d.send(mcast(), 7);
  REQUIRE_TRUE((counts() == std::vector<size_t>{4, 4, 4, 4}));
}

TEST(rtr_corruption) {
  freshRouter(19);
  Node a(0), b(1);
  LinkModel m;
  m.corrupt_rate = 0.3;
  R().setModel(m);
  const uint32_t n = 5000;
  for (uint32_t i = 0; i < n; ++i) a.send(b.port, i, 64);
  auto rx = b.rec->snapshot();
  REQUIRE_TRUE(rx.size() == n);
  size_t differ = 0;
  for (auto &x : rx) {
    REQUIRE_TRUE(x.data.size() <= 64);
  }
  for (uint32_t i = 0; i < n; ++i) {
    if (rx[i].data != makePayload(i, 64)) ++differ;
  }
  REQUIRE_TRUE(std::abs(static_cast<double>(differ) / n - 0.3) < 0.04);
  REQUIRE_TRUE(R().linkStats(a.d(), b.d()).corrupted == differ);
}

TEST(rtr_packet_tap) {
  freshRouter();
  Node a(0, true), b(1, true), c(2, true);
  MockNetworkDriver *pa = a.d(), *pb = b.d();
  R().setDropFilter([=](MockNetworkDriver *s, MockNetworkDriver *d, const PacketInfo &) {
    return s == pa && d == pb;
  });
  using V = TapEvent::Verdict;
  struct Seen { MockNetworkDriver *s, *d; Ip4Port_t port; size_t size; V v; };
  std::vector<Seen> seen;
  R().setTap([&](const TapEvent &e) { seen.push_back({e.sender, e.dst, e.destPort, e.size, e.verdict}); });
  a.send(mcast(), 1, 40);
  REQUIRE_TRUE(seen.size() == 3);
  size_t fwd = 0, filt = 0;
  for (auto &s : seen) {
    REQUIRE_TRUE(s.s == pa && s.port == mcast() && s.size == 40);
    if (s.v == V::Forwarded) {
      ++fwd;
      REQUIRE_TRUE(s.d == a.d() || s.d == c.d());
    } else {
      ++filt;
      REQUIRE_TRUE(s.v == V::DroppedFilter && s.d == pb);
    }
  }
  REQUIRE_TRUE(fwd == 2 && filt == 1);
  for (uint32_t i = 0; i < 20; ++i) b.send(mcast(), i, 8);
  size_t tapFwd = 0, tapDrop = 0;
  for (auto &s : seen) (s.v == V::Forwarded ? tapFwd : tapDrop)++;
  auto t = R().totalStats();
  REQUIRE_TRUE(t.sent == seen.size());
  REQUIRE_TRUE(t.delivered == tapFwd);
  REQUIRE_TRUE(t.dropped == tapDrop);
  REQUIRE_TRUE(R().linkStats(a.d(), b.d()).dropped == 1);
  R().setTap(nullptr);
}

TEST(rtr_unregister_in_flight_sender) {
  freshRouter();
  Node b(1);
  {
    Node a(0);
    R().setModel(delayModel(100, 100, 100, true));
    for (int i = 0; i < 20; ++i) a.send(b.port, i);
    REQUIRE_TRUE(R().inFlight() == 20);
  }
  REQUIRE_TRUE(R().inFlight() == 0);
  REQUIRE_TRUE(R().queuedBytes() == 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(250)));
  REQUIRE_TRUE(b.rec->size() == 0);
}

TEST(rtr_unregister_in_flight_receiver) {
  freshRouter();
  Node a(0);
  std::shared_ptr<Recorder> rec;
  {
    Node b(1);
    rec = b.rec;
    R().setModel(delayModel(100, 100, 100, true));
    for (int i = 0; i < 20; ++i) a.send(b.port, i);
    REQUIRE_TRUE(R().inFlight() == 20);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(250)));
  REQUIRE_TRUE(rec->size() == 0);

  Node c(2);
  a.send(c.port, 1);
  c.drv->removeUdpConnection(c.port);
  REQUIRE_TRUE(R().drain(scaled(5000)));
  REQUIRE_TRUE(c.rec->size() == 0);
  REQUIRE_TRUE(R().linkStats(a.d(), c.d()).dropped == 1);
}

TEST(rtr_drain) {
  freshRouter();
  Node a(0), b(1);
  REQUIRE_TRUE(R().drain(10));
  R().setModel(delayModel(300, 300, 300, true));
  a.send(b.port, 1);
  REQUIRE_TRUE(R().inFlight() == 1);
  REQUIRE_TRUE(!R().drain(30));
  REQUIRE_TRUE(R().inFlight() == 1);
  REQUIRE_TRUE(R().drain(scaled(5000)));
  REQUIRE_TRUE(R().inFlight() == 0 && b.rec->size() == 1);
}

TEST(rtr_seed_reproducible) {
  freshRouter();
  Node a(0), b(1);
  LinkModel m;
  m.drop_rate = 0.3;
  m.duplicate_rate = 0.1;
  R().setModel(m);
  auto run = [&](uint64_t seed) {
    R().setSeed(seed);
    {
      std::lock_guard<std::mutex> g(b.rec->m);
      b.rec->rx.clear();
    }
    for (uint32_t i = 0; i < 500; ++i) a.send(b.port, i);
    std::vector<uint32_t> seqs;
    for (auto &x : b.rec->snapshot()) seqs.push_back(seqOf(x.data));
    return seqs;
  };
  auto r1 = run(123), r2 = run(123), r3 = run(124);
  REQUIRE_TRUE(r1 == r2);
  REQUIRE_TRUE(r1 != r3);
  REQUIRE_TRUE(R().seed() == 124);
}

TEST(rtr_reset_with_live_drivers) {
  freshRouter();
  auto a = std::make_unique<Node>(0);
  auto b = std::make_unique<Node>(1);
  R().setModel(delayModel(100, 100, 100, true));
  for (int i = 0; i < 20; ++i) a->send(b->port, i);
  REQUIRE_TRUE(R().inFlight() == 20);
  R().reset();
  REQUIRE_TRUE(R().inFlight() == 0 && R().queuedBytes() == 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(200)));
  REQUIRE_TRUE(b->rec->size() == 0);
  a.reset();
  b.reset();
  Node c(0), d(1);
  R().setModel(delayModel(5, 5, 5, true));
  c.send(d.port, 1);
  REQUIRE_TRUE(R().drain(scaled(5000)));
  REQUIRE_TRUE(d.rec->size() == 1);
}

TEST(rtr_concurrent_senders) {
  freshRouter();
  const int senders = 8;
  const uint32_t perThread = 10000;
  Node rcv(100);
  rcv.rec->store = false;
  std::vector<std::unique_ptr<Node>> nodes;
  for (int i = 0; i < senders; ++i) nodes.push_back(std::make_unique<Node>(i));
  LinkModel m = delayModel(0, 5, -1, false);
  m.drop_rate = 0.1;
  R().setModel(m);
  std::vector<std::thread> ts;
  for (int i = 0; i < senders; ++i) {
    ts.emplace_back([&, i] {
      for (uint32_t k = 0; k < perThread; ++k) nodes[i]->send(rcv.port, k, 8);
    });
  }
  for (auto &t : ts) t.join();
  REQUIRE_TRUE(R().drain(scaled(30000)));
  auto t = R().totalStats();
  REQUIRE_TRUE(t.sent == static_cast<uint64_t>(senders) * perThread);
  REQUIRE_TRUE(t.delivered + t.dropped == t.sent);
  REQUIRE_TRUE(rcv.rec->size() == t.delivered);
  REQUIRE_TRUE(t.dropped == R().modelDropped());
}

TEST(rtr_callback_sends_inline) {
  for (int delayed = 0; delayed < 2; ++delayed) {
    freshRouter();
    Node a(0), b(1);
    if (delayed) R().setModel(delayModel(0, 2, -1, false));
    a.rec->store = b.rec->store = false;
    std::atomic<int> pingsA{0}, pingsB{0}, pongsA{0}, pongsB{0};
    a.rec->hook = [&](const Rec &r) {
      if (r.data[0] == TYPE_PING) {
        ++pingsA;
        a.send(b.port, 0, 8, TYPE_PONG);
      } else {
        ++pongsA;
      }
    };
    b.rec->hook = [&](const Rec &r) {
      if (r.data[0] == TYPE_PING) {
        ++pingsB;
        b.send(a.port, 0, 8, TYPE_PONG);
      } else {
        ++pongsB;
      }
    };
    const int n = 2000;
    std::thread t1([&] { for (int i = 0; i < n; ++i) a.send(b.port, i, 8, TYPE_PING); });
    std::thread t2([&] { for (int i = 0; i < n; ++i) b.send(a.port, i, 8, TYPE_PING); });
    bool done = waitFor([&] {
      return pongsA == n && pongsB == n && pingsA == n && pingsB == n;
    }, 30000);
    REQUIRE_TRUE(done);
    t1.join();
    t2.join();
  }
}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }
