#ifndef EMBRTPS_TESTS_GEDP_TAP_H
#define EMBRTPS_TESTS_GEDP_TAP_H

#include "cluster.h"

#include "rtps/discovery/SnapEDPMessages.h"
#include "ucdr/microcdr.h"

#include <chrono>
#include <mutex>

namespace tests {

enum class SnapEDP {
  None,
  JoinRequest,
  ResyncRequest,
  JoinResponse,
  ResyncResponse,
  Announce,
  Dispose,
};

struct SnapEDPMsg {
  SnapEDP kind = SnapEDP::None;
  rtps::GuidPrefix_t sender{};
  rtps::GuidPrefix_t target{};
  rtps::GuidPrefix_t root{};
  uint32_t numEndpoints = 0;
  uint32_t totalEndpoints = 0;
};

inline const char *snapEdpName(SnapEDP k) {
  switch (k) {
  case SnapEDP::None: return "none";
  case SnapEDP::JoinRequest: return "join_req";
  case SnapEDP::ResyncRequest: return "resync_req";
  case SnapEDP::JoinResponse: return "join_resp";
  case SnapEDP::ResyncResponse: return "resync_resp";
  case SnapEDP::Announce: return "announce";
  case SnapEDP::Dispose: return "dispose";
  }
  return "?";
}

inline long findEntityId(const uint8_t *d, size_t n, const rtps::EntityId_t &id) {
  const uint8_t pat[4] = {id.entityKey[0], id.entityKey[1], id.entityKey[2],
                          static_cast<uint8_t>(id.entityKind)};
  for (size_t i = 0; i + 4 <= n; ++i)
    if (std::memcmp(d + i, pat, 4) == 0) return static_cast<long>(i);
  return -1;
}

inline SnapEDPMsg classifySnapEDP(const uint8_t *d, size_t n) {
  using namespace rtps;
  SnapEDPMsg m;
  long i = findEntityId(d, n, ENTITYID_P2P_BUILTIN_SNAP_WRITER);
  if (i >= 0) {
    const size_t p = static_cast<size_t>(i) + 12;
    if (p + 8 > n) return m;
    SnapEDPMessageKind kind;
    if (!peekSnapEDPMessageKind(d + p, n - p, kind)) return m;
    ucdrBuffer buf;
    ucdr_init_buffer(&buf, const_cast<uint8_t *>(d + p), n - p);
    if (kind == SnapEDPMessageKind::REQUEST) {
      SnapEDPRequest r;
      if (!r.readFromUcdrBuffer(buf)) return m;
      m.kind = r.isReconciliation ? SnapEDP::ResyncRequest : SnapEDP::JoinRequest;
      m.sender = r.sender;
      m.target = r.target;
    } else if (kind == SnapEDPMessageKind::SNAPSHOT) {
      SnapEDPResponse r;
      if (!r.readHeader(buf)) return m;
      m.kind = r.isResync ? SnapEDP::ResyncResponse : SnapEDP::JoinResponse;
      m.sender = r.sender;
      m.target = r.target;
      m.root = r.root;
      m.numEndpoints = r.numEndpoints;
      m.totalEndpoints = r.totalEndpoints;
    }
    return m;
  }
  i = findEntityId(d, n, ENTITYID_SEDP_BUILTIN_ANNOUNCEMENTS_WRITER);
  if (i >= 0) {
    const size_t p = static_cast<size_t>(i) + 12;
    if (p + 8 > n) return m;
    ucdrBuffer buf;
    ucdr_init_buffer(&buf, const_cast<uint8_t *>(d + p), n - p);
    SnapEDPAnnouncement a;
    if (!a.readHeader(buf)) return m;
    m.kind = a.disposed ? SnapEDP::Dispose : SnapEDP::Announce;
    m.sender = a.sender;
    m.numEndpoints = a.numEndpoints;
  }
  return m;
}

inline SnapEDPMsg classifySnapEDP(const rtps::PacketInfo &info) {
  return classifySnapEDP(info.buffer.m_buf.data(), info.buffer.m_buf.size());
}

class SnapEDPLog {
public:
  struct Entry {
    SnapEDPMsg msg;
    rtps::MockNetworkDriver *sender;
    rtps::MockNetworkDriver *dst;
    std::chrono::steady_clock::time_point at;
  };
  void record(const SnapEDPMsg &m, rtps::MockNetworkDriver *s, rtps::MockNetworkDriver *d) {
    if (m.kind == SnapEDP::None) return;
    std::lock_guard<std::mutex> g(mu);
    v.push_back({m, s, d, std::chrono::steady_clock::now()});
  }
  std::vector<Entry> entries() const {
    std::lock_guard<std::mutex> g(mu);
    return v;
  }
  size_t count(SnapEDP k, const rtps::GuidPrefix_t *sender = nullptr,
               const rtps::GuidPrefix_t *target = nullptr) const {
    std::lock_guard<std::mutex> g(mu);
    size_t c = 0;
    for (auto &e : v)
      if (e.msg.kind == k && (!sender || e.msg.sender == *sender) &&
          (!target || e.msg.target == *target))
        ++c;
    return c;
  }
  void clear() {
    std::lock_guard<std::mutex> g(mu);
    v.clear();
  }

private:
  mutable std::mutex mu;
  std::vector<Entry> v;
};

using DropRule = std::function<bool(const SnapEDPMsg &, rtps::MockNetworkDriver *sender,
                                    rtps::MockNetworkDriver *dst,
                                    const rtps::PacketInfo &)>;
inline void installLoggingFilter(SnapEDPLog &log, DropRule rule) {
  rtps::MockNetworkRouter::instance().setDropFilter(
      [&log, rule](rtps::MockNetworkDriver *s, rtps::MockNetworkDriver *d,
                   const rtps::PacketInfo &info) {
        const SnapEDPMsg m = classifySnapEDP(info);
        log.record(m, s, d);
        return rule ? rule(m, s, d, info) : false;
      });
}

}

#endif
