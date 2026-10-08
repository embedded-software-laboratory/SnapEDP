#include "rtps/communication/MockNetworkRouter.h"
#include "rtps/communication/MockNetworkDriver.h"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <iostream>

using rtps::LinkModel;
using rtps::LinkStats;
using rtps::MockNetworkRouter;
using rtps::TapEvent;

namespace {
// mean fraction of a truncated exponential for a given rate times range
double truncExpMeanFraction(double x) {
  if (x < 1e-3) {
    return 0.5 - x / 12.0;
  }
  return 1.0 / x - 1.0 / std::expm1(x);
}
} // namespace

MockNetworkRouter &MockNetworkRouter::instance() {
  static MockNetworkRouter router;
  return router;
}

MockNetworkRouter::MockNetworkRouter() {
  if (const char *e = std::getenv("MOCK_NET_SEED")) {
    m_seed = std::strtoull(e, nullptr, 10);
  }
  m_rng.seed(m_seed);
  m_epoch = Clock::now();
  std::cerr << "[MockNetworkRouter] seed=" << m_seed << std::endl;
}

MockNetworkRouter::~MockNetworkRouter() { shutdownThread(); }

void MockNetworkRouter::registerDriver(MockNetworkDriver *driver) {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_drivers.push_back(driver);
}

void MockNetworkRouter::unregisterDriver(MockNetworkDriver *driver) {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_drivers.erase(std::remove(m_drivers.begin(), m_drivers.end(), driver),
                  m_drivers.end());
  // purge in flight packets by sender and by destination
  for (auto it = m_inflight.begin(); it != m_inflight.end();) {
    const Link &l = it->second.link;
    if (l.first == driver || l.second == driver) {
      auto ls = m_links.find(l);
      if (ls != m_links.end()) {
        ls->second.queuedBytes -=
            std::min(ls->second.queuedBytes, it->second.data.size());
      }
      it = m_inflight.erase(it);
    } else {
      ++it;
    }
  }
  for (auto it = m_links.begin(); it != m_links.end();) {
    if (it->first.first == driver || it->first.second == driver) {
      it = m_links.erase(it);
    } else {
      ++it;
    }
  }
  for (auto it = m_linkModels.begin(); it != m_linkModels.end();) {
    if (it->first.first == driver || it->first.second == driver) {
      it = m_linkModels.erase(it);
    } else {
      ++it;
    }
  }
  m_groupOf.erase(driver);
  m_blackouts.erase(
      std::remove_if(m_blackouts.begin(), m_blackouts.end(),
                     [&](const Blackout &b) {
                       return b.sender == driver || b.dst == driver;
                     }),
      m_blackouts.end());
  if (m_inflight.empty()) {
    m_drained.notify_all();
  }
}

bool MockNetworkRouter::isPortBound(Ip4Port_t port,
                                     const MockNetworkDriver *excludeDriver) const {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  if (isMultiCastPort(port)) {
    return false;
  }
  for (const auto *driver : m_drivers) {
    if (driver == excludeDriver) {
      continue;
    }
    if (driver->hasConnection(port)) {
      return true;
    }
  }
  return false;
}

void MockNetworkRouter::setDropFilter(DropFilter filter) {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_dropFilter = std::move(filter);
}

void MockNetworkRouter::setDropRate(double rate) {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_model.drop_rate = rate;
}

void MockNetworkRouter::clearDropPolicy() {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_dropFilter = nullptr;
  m_model.drop_rate = 0.0;
}

void MockNetworkRouter::setModel(const LinkModel &model) {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_model = model;
  m_epoch = Clock::now();
}

void MockNetworkRouter::setLinkModel(MockNetworkDriver *sender,
                                      MockNetworkDriver *dst,
                                      const LinkModel &model) {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_linkModels[Link{sender, dst}] = model;
}

void MockNetworkRouter::clearLinkModels() {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_linkModels.clear();
}

void MockNetworkRouter::blackout(MockNetworkDriver *sender,
                                  MockNetworkDriver *dst, int durationMs) {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_blackouts.push_back(
      {sender, dst, Clock::now() + std::chrono::milliseconds(durationMs)});
}

void MockNetworkRouter::clearBlackouts() {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_blackouts.clear();
}

void MockNetworkRouter::partition(
    const std::vector<std::vector<MockNetworkDriver *>> &groups) {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_groupOf.clear();
  int id = 1;
  for (const auto &g : groups) {
    for (auto *d : g) {
      m_groupOf[d] = id;
    }
    ++id;
  }
}

void MockNetworkRouter::isolate(MockNetworkDriver *node) {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  int id = -1;
  for (const auto &kv : m_groupOf) {
    id = std::min(id, kv.second - 1);
  }
  m_groupOf[node] = id; // negative ids mark isolated nodes, unique per node
}

void MockNetworkRouter::heal() {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_groupOf.clear();
}

void MockNetworkRouter::setTap(Tap tap) {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_tap = std::move(tap);
}

LinkStats MockNetworkRouter::linkStats(MockNetworkDriver *sender,
                                        MockNetworkDriver *dst) const {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  auto it = m_links.find(Link{sender, dst});
  return it == m_links.end() ? LinkStats{} : it->second.stats;
}

LinkStats MockNetworkRouter::totalStats() const {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  LinkStats t;
  for (const auto &kv : m_links) {
    const auto &s = kv.second.stats;
    t.sent += s.sent;
    t.delivered += s.delivered;
    t.dropped += s.dropped;
    t.duplicated += s.duplicated;
    t.reordered += s.reordered;
    t.corrupted += s.corrupted;
    t.queue_dropped += s.queue_dropped;
  }
  return t;
}

void MockNetworkRouter::resetStats() {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  for (auto &kv : m_links) {
    kv.second.stats = LinkStats{};
  }
  m_modelDropped = 0;
}

void MockNetworkRouter::setSeed(uint64_t seed) {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_seed = seed;
  m_rng.seed(seed);
  for (auto &kv : m_links) {
    kv.second.geBad = false;
  }
  std::cerr << "[MockNetworkRouter] seed=" << m_seed << std::endl;
}

uint64_t MockNetworkRouter::seed() const {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  return m_seed;
}

bool MockNetworkRouter::drain(int timeoutMs) {
  std::unique_lock<std::recursive_mutex> lk(m_mutex);
  return m_drained.wait_for(lk, std::chrono::milliseconds(timeoutMs),
                            [&] { return m_inflight.empty(); });
}

size_t MockNetworkRouter::inFlight() const {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  return m_inflight.size();
}

size_t MockNetworkRouter::queuedBytes() const {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  size_t n = 0;
  for (const auto &kv : m_links) {
    n += kv.second.queuedBytes;
  }
  return n;
}

uint64_t MockNetworkRouter::modelDropped() const {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  return m_modelDropped;
}

LinkModel MockNetworkRouter::wifiTestbed() {
  LinkModel m;
  m.min_delay_ms = 1.0;
  m.max_delay_ms = 40.0;
  m.mean_delay_ms = 5.0;
  m.preserve_order = false;
  m.frame_loss_rate = 0.01;
  m.mtu = 1500;
  m.bitrate_bps = 54e6;
  m.queue_limit_bytes = 256 * 1024;
  m.late_rate = 0.005;
  m.late_delay_ms = 100.0;
  return m;
}

void MockNetworkRouter::shutdownThread() {
  std::thread t;
  {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    m_stop = true;
    t = std::move(m_thread);
    m_wake.notify_all();
  }
  if (t.joinable()) {
    t.join();
  }
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_stop = false;
}

void MockNetworkRouter::reset() {
  shutdownThread();
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  m_drivers.clear();
  m_dropFilter = nullptr;
  m_model = LinkModel{};
  m_linkModels.clear();
  m_blackouts.clear();
  m_groupOf.clear();
  m_tap = nullptr;
  m_links.clear();
  m_inflight.clear();
  m_modelDropped = 0;
  m_rng.seed(m_seed);
  m_epoch = Clock::now();
  m_drained.notify_all();
}

const LinkModel &MockNetworkRouter::modelFor(const Link &link) const {
  if (m_linkModels.empty()) {
    return m_model;
  }
  for (const Link &k : {link, Link{link.first, nullptr}, Link{nullptr, link.second}}) {
    auto it = m_linkModels.find(k);
    if (it != m_linkModels.end()) {
      return it->second;
    }
  }
  return m_model;
}

bool MockNetworkRouter::inBlackout(const Link &link, Clock::time_point now) const {
  for (const auto &b : m_blackouts) {
    if (now < b.end && (b.sender == nullptr || b.sender == link.first) &&
        (b.dst == nullptr || b.dst == link.second)) {
      return true;
    }
  }
  return false;
}

bool MockNetworkRouter::partitioned(MockNetworkDriver *s, MockNetworkDriver *d) const {
  if (m_groupOf.empty()) {
    return false;
  }
  auto gs = m_groupOf.find(s);
  auto gd = m_groupOf.find(d);
  bool hs = gs != m_groupOf.end(), hd = gd != m_groupOf.end();
  if (!hs && !hd) {
    return false;
  }
  if (hs && hd) {
    return gs->second != gd->second;
  }
  // one side named and the other free, only isolated nodes with negative id are cut off
  return (hs ? gs->second : gd->second) < 0;
}

double MockNetworkRouter::uniform() {
  return std::uniform_real_distribution<double>(0.0, 1.0)(m_rng);
}

double MockNetworkRouter::sampleDelayMs(const LinkModel &m) {
  const double lo = std::max(0.0, m.min_delay_ms);
  const double R = m.max_delay_ms - lo;
  if (R <= 0.0) {
    return lo;
  }
  const double mean = m.mean_delay_ms - lo;
  if (m.mean_delay_ms < 0.0) {
    return lo + R * uniform();
  }
  if (mean <= 0.0) {
    return lo;
  }
  if (mean >= R) {
    return lo + R;
  }
  const bool mirror = mean > R / 2.0;
  const double q = (mirror ? R - mean : mean) / R; // target fraction, always below one half
  if (q >= 0.5 - 1e-4) {
    return lo + R * uniform();
  }
  // bisection, truncExpMeanFraction is decreasing in x
  double a = 1e-3, b = 700.0;
  for (int i = 0; i < 60; ++i) {
    double mid = 0.5 * (a + b);
    if (truncExpMeanFraction(mid) > q) {
      a = mid;
    } else {
      b = mid;
    }
  }
  const double x = 0.5 * (a + b);
  const double u = uniform();
  double s = -std::log1p(-u * (-std::expm1(-x))) * (R / x);
  s = std::min(std::max(s, 0.0), R);
  return lo + (mirror ? R - s : s);
}

void MockNetworkRouter::emit(MockNetworkDriver *s, MockNetworkDriver *d,
                              Ip4Port_t port, const uint8_t *data, size_t size,
                              TapEvent::Verdict v) {
  if (m_tap) {
    m_tap(TapEvent{s, d, port, data, size, v});
  }
}

void MockNetworkRouter::deliver(const Link &link, LinkState *st, Ip4Port_t port,
                                 const uint8_t *data, size_t len,
                                 ip4_struct_t srcAddr, Ip4Port_t srcPort,
                                 uint64_t seq) {
  if (st != nullptr) {
    st->stats.delivered++;
    if (st->anyDelivered && seq < st->maxDeliveredSeq) {
      st->stats.reordered++;
    }
    if (!st->anyDelivered || seq > st->maxDeliveredSeq) {
      st->maxDeliveredSeq = seq;
    }
    st->anyDelivered = true;
  }
  link.second->deliverPacket(port, data, len, srcAddr, srcPort);
}

void MockNetworkRouter::scheduleCopy(const Link &link, const LinkModel &m,
                                      LinkState &st, const PacketInfo &info,
                                      std::vector<uint8_t> data) {
  const auto now = Clock::now();
  auto start = std::max(now, st.busyUntil);
  if (m.bitrate_bps > 0.0) {
    start += std::chrono::duration_cast<Clock::duration>(
        std::chrono::duration<double>(data.size() * 8.0 / m.bitrate_bps));
  }
  st.busyUntil = start;
  double delay = sampleDelayMs(m);
  if (m.late_rate > 0.0 && uniform() < m.late_rate) {
    delay += m.late_delay_ms;
  }
  auto due = start + std::chrono::duration_cast<Clock::duration>(
                         std::chrono::duration<double, std::milli>(delay));
  if (m.burst_period_ms > 0.0) {
    const auto period = std::chrono::duration_cast<Clock::duration>(
        std::chrono::duration<double, std::milli>(m.burst_period_ms));
    auto k = (due - m_epoch + period - Clock::duration(1)) / period;
    due = m_epoch + k * period;
  }
  if (m.preserve_order) {
    due = std::max(due, st.lastDue);
    st.lastDue = due;
  }
  st.queuedBytes += data.size();
  InFlight f{link, info.destPort, info.destAddr, info.srcPort, std::move(data),
             st.nextSeq++};
  m_inflight.emplace(due, std::move(f));
  ensureThread();
  m_wake.notify_all();
}

void MockNetworkRouter::ensureThread() {
  if (!m_thread.joinable()) {
    m_thread = std::thread([this] { threadLoop(); });
  }
}

void MockNetworkRouter::threadLoop() {
  std::unique_lock<std::recursive_mutex> lk(m_mutex);
  while (!m_stop) {
    if (m_inflight.empty()) {
      m_drained.notify_all();
      m_wake.wait(lk);
      continue;
    }
    const auto due = m_inflight.begin()->first;
    if (Clock::now() < due) {
      m_wake.wait_until(lk, due);
      continue;
    }
    auto node = m_inflight.extract(m_inflight.begin());
    InFlight &f = node.mapped();
    LinkState &st = m_links[f.link];
    st.queuedBytes -= std::min(st.queuedBytes, f.data.size());
    // recheck the receiver, it may have been unbound since the send
    bool alive = std::find(m_drivers.begin(), m_drivers.end(), f.link.second) !=
                     m_drivers.end() &&
                 f.link.second->hasConnection(f.destPort) &&
                 !inBlackout(f.link, Clock::now());
    if (!alive) {
      st.stats.dropped++;
      m_modelDropped++;
    } else {
      // delivered while holding the router lock, fine because the receive callback only enqueues and the lock is recursive anyway
      deliver(f.link, &st, f.destPort, f.data.data(), f.data.size(), f.srcAddr,
              f.srcPort, f.seq);
    }
    if (m_inflight.empty()) {
      m_drained.notify_all();
    }
  }
}

rtps::MockNetworkDriver *
MockNetworkRouter::driverBoundToPort(Ip4Port_t port) const {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  for (auto *driver : m_drivers) {
    if (driver->hasConnection(port)) {
      return driver;
    }
  }
  return nullptr;
}

void MockNetworkRouter::routePacket(MockNetworkDriver *sender,
                                     PacketInfo &info) {
  std::lock_guard<std::recursive_mutex> lock(m_mutex);
  using V = TapEvent::Verdict;

  const bool isMulticast = isMultiCastPort(info.destPort);
  const std::vector<uint8_t> payload = info.buffer.m_buf; // stable across reentrant sends
  const auto drivers = m_drivers;                         // snapshot, callbacks may reenter

  for (auto *driver : drivers) {
    if (!isMulticast && driver == sender) {
      continue;
    }
    if (!driver->hasConnection(info.destPort)) {
      continue;
    }
    if (std::find(m_drivers.begin(), m_drivers.end(), driver) == m_drivers.end()) {
      continue; // unregistered by an earlier inline callback
    }
    const Link link{sender, driver};
    LinkState &st = m_links[link];
    const LinkModel &m = modelFor(link);
    const auto now = Clock::now();
    const uint8_t *data = payload.data();
    const size_t len = payload.size();
    st.stats.sent++;

    auto drop = [&](V verdict, bool model) {
      st.stats.dropped++;
      if (model) {
        m_modelDropped++;
      }
      emit(sender, driver, info.destPort, data, len, verdict);
    };

    if (m_dropFilter && m_dropFilter(sender, driver, info)) {
      drop(V::DroppedFilter, false);
      continue;
    }
    if (partitioned(sender, driver)) {
      drop(V::DroppedPartition, false);
      continue;
    }
    if (inBlackout(link, now)) {
      drop(V::DroppedBlackout, true);
      continue;
    }
    bool lost = false;
    if (m.drop_rate > 0.0 && uniform() < m.drop_rate) {
      lost = true;
    }
    if (m.p_good_to_bad > 0.0) {
      // loss decided by the current state, then the state advances
      const double p = st.geBad ? m.loss_bad : m.loss_good;
      if (p > 0.0 && uniform() < p) {
        lost = true;
      }
      if (st.geBad) {
        if (uniform() < m.p_bad_to_good) st.geBad = false;
      } else if (uniform() < m.p_good_to_bad) {
        st.geBad = true;
      }
    }
    if (m.frame_loss_rate > 0.0) {
      const size_t mtu = std::max<size_t>(m.mtu, 1);
      const double frames = static_cast<double>(std::max<size_t>(1, (len + mtu - 1) / mtu));
      if (uniform() < 1.0 - std::pow(1.0 - m.frame_loss_rate, frames)) {
        lost = true;
      }
    }
    if (lost) {
      drop(V::DroppedLoss, true);
      continue;
    }

    const bool sync = m.isSynchronous();
    if (!sync && m.queue_limit_bytes > 0 &&
        st.queuedBytes + len > m.queue_limit_bytes) {
      st.stats.queue_dropped++;
      drop(V::DroppedQueue, true);
      continue;
    }

    int copies = 1;
    if (m.duplicate_rate > 0.0 && uniform() < m.duplicate_rate) {
      copies = 2;
    }
    emit(sender, driver, info.destPort, data, len, V::Forwarded);
    for (int c = 0; c < copies; ++c) {
      std::vector<uint8_t> bytes(payload);
      if (m.corrupt_rate > 0.0 && uniform() < m.corrupt_rate && !bytes.empty()) {
        if (uniform() < 0.5) {
          bytes.resize(static_cast<size_t>(uniform() * bytes.size()) % bytes.size());
        } else {
          size_t bit = static_cast<size_t>(uniform() * bytes.size() * 8) % (bytes.size() * 8);
          bytes[bit / 8] ^= static_cast<uint8_t>(1u << (bit % 8));
        }
        st.stats.corrupted++;
      }
      if (c == 1) {
        if (!sync && m.queue_limit_bytes > 0 &&
            st.queuedBytes + bytes.size() > m.queue_limit_bytes) {
          st.stats.queue_dropped++;
          st.stats.dropped++;
          m_modelDropped++;
          emit(sender, driver, info.destPort, data, len, V::DroppedQueue);
          break;
        }
        st.stats.duplicated++;
        emit(sender, driver, info.destPort, data, len, V::Duplicated);
      }
      if (sync) {
        const uint64_t seq = st.nextSeq++;
        deliver(link, &st, info.destPort, bytes.data(), bytes.size(),
                info.destAddr, info.srcPort, seq);
      } else {
        scheduleCopy(link, m, st, info, std::move(bytes));
      }
    }
  }
}
