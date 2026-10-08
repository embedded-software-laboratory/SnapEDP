#ifndef EMBRTPS_TESTS_GEDP_TAP_E3_H
#define EMBRTPS_TESTS_GEDP_TAP_E3_H

#include "oracle.h"

#include "rtps/discovery/SnapEDPMessages.h"
#include "ucdr/microcdr.h"

#include <chrono>
#include <mutex>

namespace tests {
namespace e3 {

enum class K { Other, Spdp, JoinReq, ResyncReq, JoinResp, ResyncResp, Announce, Dispose };

struct Msg {
  K kind = K::Other;
  rtps::GuidPrefix_t sender{};
  rtps::GuidPrefix_t target{};
  uint32_t numEndpoints = 0;
  uint32_t totalEndpoints = 0;
  uint64_t hash = 0;
};

inline long find(const uint8_t *d, size_t n, const rtps::EntityId_t &id) {
  const uint8_t pat[4] = {id.entityKey[0], id.entityKey[1], id.entityKey[2],
                          static_cast<uint8_t>(id.entityKind)};
  for (size_t i = 0; i + 4 <= n; ++i)
    if (std::memcmp(d + i, pat, 4) == 0) return static_cast<long>(i);
  return -1;
}

inline Msg decode(const uint8_t *d, size_t n) {
  using namespace rtps;
  Msg m;
  long i = find(d, n, ENTITYID_P2P_BUILTIN_SNAP_WRITER);
  if (i >= 0) {
    const size_t p = static_cast<size_t>(i) + 12;
    if (p + 8 > n) return m;
    SnapEDPMessageKind kind;
    if (!peekSnapEDPMessageKind(d + p, n - p, kind)) return m;
    ucdrBuffer buf;
    ucdr_init_buffer(&buf, const_cast<uint8_t *>(d + p), static_cast<uint32_t>(n - p));
    if (kind == SnapEDPMessageKind::REQUEST) {
      SnapEDPRequest r;
      if (!r.readFromUcdrBuffer(buf)) return m;
      m.kind = r.isReconciliation ? K::ResyncReq : K::JoinReq;
      m.sender = r.sender;
      m.target = r.target;
    } else if (kind == SnapEDPMessageKind::SNAPSHOT) {
      SnapEDPResponse r;
      if (!r.readHeader(buf)) return m;
      m.kind = r.isResync ? K::ResyncResp : K::JoinResp;
      m.sender = r.sender;
      m.target = r.target;
      m.numEndpoints = r.numEndpoints;
      m.totalEndpoints = r.totalEndpoints;
    }
    return m;
  }
  i = find(d, n, ENTITYID_SEDP_BUILTIN_ANNOUNCEMENTS_WRITER);
  if (i >= 0) {
    const size_t p = static_cast<size_t>(i) + 12;
    if (p + 8 > n) return m;
    ucdrBuffer buf;
    ucdr_init_buffer(&buf, const_cast<uint8_t *>(d + p), static_cast<uint32_t>(n - p));
    SnapEDPAnnouncement a;
    if (!a.readHeader(buf)) return m;
    m.kind = a.disposed ? K::Dispose : K::Announce;
    m.sender = a.sender;
    m.numEndpoints = a.numEndpoints;
    m.hash = a.endpointHash;
    return m;
  }
  if (find(d, n, ENTITYID_SPDP_BUILTIN_PARTICIPANT_WRITER) >= 0) m.kind = K::Spdp;
  return m;
}

struct Event {
  std::chrono::steady_clock::time_point at;
  int src = -1;
  int dst = -1;
  Msg msg;
  bool accepted = false;
};

class Recorder {
public:
  using Pred = std::function<bool(int src, int dst, const Msg &)>;

  explicit Recorder(Cluster &c) : m_c(c) {
    auto &r = rtps::MockNetworkRouter::instance();
    r.setTap([this](const rtps::TapEvent &e) { onTap(e); });
    r.setDropFilter([this](rtps::MockNetworkDriver *s, rtps::MockNetworkDriver *d,
                           const rtps::PacketInfo &info) {
      Pred p;
      {
        std::lock_guard<std::mutex> g(m_mu);
        p = m_pred;
      }
      if (!p) return false;
      Msg m = decode(info.buffer.m_buf.data(), info.buffer.m_buf.size());
      return p(idxOf(s), idxOf(d), m);
    });
  }
  ~Recorder() {
    auto &r = rtps::MockNetworkRouter::instance();
    r.setTap(nullptr);
    r.setDropFilter(nullptr);
  }

  void setDrop(Pred p) {
    std::lock_guard<std::mutex> g(m_mu);
    m_pred = std::move(p);
  }
  void clearDrop() { setDrop(nullptr); }

  void clear() {
    std::lock_guard<std::mutex> g(m_mu);
    m_ev.clear();
  }
  std::vector<Event> events() const {
    std::lock_guard<std::mutex> g(m_mu);
    return m_ev;
  }
  std::vector<Event> select(K k, int src = -1, int dst = -1, bool acceptedOnly = false) const {
    std::lock_guard<std::mutex> g(m_mu);
    std::vector<Event> out;
    for (auto &e : m_ev)
      if (e.msg.kind == k && (src < 0 || e.src == src) && (dst < 0 || e.dst == dst) &&
          (!acceptedOnly || e.accepted))
        out.push_back(e);
    return out;
  }
  size_t count(K k, int src = -1, int dst = -1, bool acceptedOnly = false) const {
    return select(k, src, dst, acceptedOnly).size();
  }
  size_t countAlive(int src, int dst = -1) const {
    size_t n = 0;
    for (auto &e : select(K::Announce, src, dst, true))
      if (e.msg.numEndpoints > 0) ++n;
    return n;
  }

  void mapAll() {
    std::lock_guard<std::mutex> g(m_mu);
    for (size_t i = 0; i < m_c.size(); ++i)
      if (m_c.node(i).drv) m_map[m_c.node(i).drv] = static_cast<int>(i);
  }

private:
  int idxOf(rtps::MockNetworkDriver *d) {
    std::lock_guard<std::mutex> g(m_mu);
    auto it = m_map.find(d);
    if (it != m_map.end()) return it->second;
    for (size_t i = 0; i < m_c.size(); ++i)
      if (m_c.node(i).drv == d) {
        m_map[d] = static_cast<int>(i);
        return static_cast<int>(i);
      }
    return -1;
  }
  void onTap(const rtps::TapEvent &e) {
    using V = rtps::TapEvent::Verdict;
    Msg m = decode(e.data, e.size);
    if (m.kind == K::Other || m.kind == K::Spdp) return;
    const int s = idxOf(e.sender), d = idxOf(e.dst);
    std::lock_guard<std::mutex> g(m_mu);
    m_ev.push_back({std::chrono::steady_clock::now(), s, d, m,
                    e.verdict == V::Forwarded || e.verdict == V::Duplicated});
  }

  Cluster &m_c;
  mutable std::mutex m_mu;
  std::vector<Event> m_ev;
  std::map<rtps::MockNetworkDriver *, int> m_map;
  Pred m_pred;
};

inline std::string longName(const std::string &tag, int i, size_t len = 44) {
  std::string s = tag + "_" + std::to_string(i) + "_";
  while (s.size() < len) s += 'x';
  return s;
}

inline void addMany(Cluster &c, size_t i, int n, const std::string &tag) {
  for (int k = 0; k < n; ++k) {
    c.addWriter(i, longName(tag + "w" + std::to_string(i), k));
    c.addReader(i, longName(tag + "r" + std::to_string(i), k));
  }
}

inline void speedUpSpdp(Cluster &c, int resendMs = 300) {
  c.cfg.set(rtps::Config::SPDP_RESEND_PERIOD_MS, scaled(resendMs));
}

inline void boot(Cluster &c, std::initializer_list<uint8_t> prefixes, int budgetMs = 20000) {
  for (uint8_t p : prefixes) c.add(p);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, budgetMs));
}

inline double msBetween(std::chrono::steady_clock::time_point a, std::chrono::steady_clock::time_point b) {
  return std::chrono::duration<double, std::milli>(b - a).count();
}

}
}

#endif
