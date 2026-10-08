#ifndef EMBRTPS_TESTS_GEDP_TAP_E4_H
#define EMBRTPS_TESTS_GEDP_TAP_E4_H

#include <mutex>
#include "oracle.h"

#include "rtps/discovery/SnapEDPMessages.h"

#include <atomic>
#include <thread>

namespace tests {
namespace e4 {

enum class Pkt { Other, Spdp, Request, Response, Announce };

inline long findPattern(const uint8_t *d, size_t n, const rtps::EntityId_t &id) {
  const uint8_t pat[4] = {id.entityKey[0], id.entityKey[1], id.entityKey[2],
                          static_cast<uint8_t>(id.entityKind)};
  for (size_t i = 0; i + 4 <= n; ++i)
    if (std::memcmp(d + i, pat, 4) == 0) return static_cast<long>(i);
  return -1;
}

inline Pkt classify(const uint8_t *d, size_t n) {
  using namespace rtps;
  if (findPattern(d, n, ENTITYID_SPDP_BUILTIN_PARTICIPANT_WRITER) >= 0) return Pkt::Spdp;
  long p = findPattern(d, n, ENTITYID_P2P_BUILTIN_SNAP_WRITER);
  bool ann = false;
  if (p < 0) {
    p = findPattern(d, n, ENTITYID_SEDP_BUILTIN_ANNOUNCEMENTS_WRITER);
    ann = true;
  }
  if (p < 0) return Pkt::Other;
  size_t kindOff = static_cast<size_t>(p) + 4 + 8 + 4;
  if (kindOff + 4 > n) return Pkt::Other;
  uint32_t raw = d[kindOff] | (d[kindOff + 1] << 8) | (d[kindOff + 2] << 16) |
                 (static_cast<uint32_t>(d[kindOff + 3]) << 24);
  if (raw == static_cast<uint32_t>(SnapEDPMessageKind::REQUEST)) return Pkt::Request;
  if (raw == static_cast<uint32_t>(SnapEDPMessageKind::SNAPSHOT)) return Pkt::Response;
  if (raw == static_cast<uint32_t>(SnapEDPMessageKind::ANNOUNCEMENT)) return Pkt::Announce;
  (void)ann;
  return Pkt::Other;
}

inline Pkt classify(const rtps::PacketInfo &info) {
  return classify(info.buffer.m_buf.data(), info.buffer.m_buf.size());
}

inline void fastLease(Cluster &c, uint32_t resendMs = 500, uint32_t leaseMs = 2000) {
  c.cfg.set(rtps::Config::SPDP_RESEND_PERIOD_MS, resendMs);
  c.cfg.set(rtps::Config::SPDP_LEASE_DURATION_MS, leaseMs);
}

inline bool listsPeer(Cluster &c, size_t self, size_t peer) {
  if (!c.node(self).alive) return false;
  return c.node(self).part->hasRemoteParticipant(c.node(peer).prefix);
}

inline rtps::SnapEDPState stateOf(Cluster &c, size_t i) {
  return c.agent(i)->getCurrentState();
}

class Sampler {
public:
  Sampler(Cluster &c, std::vector<size_t> nodes) : m_c(c), m_nodes(std::move(nodes)) {
    m_last.resize(m_nodes.size());
    m_th = std::thread([this] { loop(); });
  }
  ~Sampler() { stop(); }
  void stop() {
    if (m_th.joinable()) { m_stop = true; m_th.join(); }
  }
  std::atomic<bool> rootRose{false};
  std::atomic<int> riseNode{-1};
  std::atomic<bool> sawForbidden{false};
  void setForbidden(const rtps::GuidPrefix_t &p) {
    std::lock_guard<std::mutex> g(m_forbiddenMutex);
    forbidden = p;
    useForbidden = true;
  }
  rtps::GuidPrefix_t forbidden{};
  std::atomic<bool> useForbidden{false};
  std::mutex m_forbiddenMutex;
  std::vector<size_t> onlyForbiddenNodes;

private:
  void loop() {
    while (!m_stop) {
      for (size_t k = 0; k < m_nodes.size(); ++k) {
        size_t i = m_nodes[k];
        if (!m_c.node(i).alive || !m_c.node(i).started) continue;
        auto r = m_c.agent(i)->getCurrentRoot();
        rtps::GuidPrefix_t forbiddenNow{};
        {
          std::lock_guard<std::mutex> g(m_forbiddenMutex);
          forbiddenNow = forbidden;
        }
        if (useForbidden && r == forbiddenNow) {
          bool watch = onlyForbiddenNodes.empty();
          for (size_t w : onlyForbiddenNodes) if (w == i) watch = true;
          if (watch) sawForbidden = true;
        }
        if (m_haveLast && !(m_last[k] == rtps::GUIDPREFIX_UNKNOWN) &&
            m_last[k].id < r.id) {
          bool holderAlive = false;
          for (size_t j = 0; j < m_c.size(); ++j)
            if (m_c.node(j).alive && !m_c.node(j).blackholed &&
                m_c.node(j).prefix == m_last[k])
              holderAlive = true;
          if (holderAlive) { riseNode = static_cast<int>(i); rootRose = true; }
        }
        m_last[k] = r;
      }
      m_haveLast = true;
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
  }
  Cluster &m_c;
  std::vector<size_t> m_nodes;
  std::vector<rtps::GuidPrefix_t> m_last;
  bool m_haveLast = false;
  std::atomic<bool> m_stop{false};
  std::thread m_th;
};

}
}

#endif
