#include "harness.h"
#include "rtps/discovery/ParticipantProxyData.h"

#include <chrono>
#include <cstring>
#include <memory>
#include <vector>

using namespace rtps;
using SMElement::ParameterId;

namespace {

struct PlBuilder {
  std::vector<uint8_t> bytes;
  std::vector<size_t> starts;

  void raw(const void *p, size_t n) {
    const uint8_t *b = static_cast<const uint8_t *>(p);
    bytes.insert(bytes.end(), b, b + n);
  }
  void param(ParameterId pid, const void *value, size_t n, bool align8 = false) {
    starts.push_back(bytes.size());
    uint16_t id = static_cast<uint16_t>(pid);
    size_t padded = (n + 3) & ~size_t(3);
    uint16_t len = static_cast<uint16_t>(padded);
    raw(&id, 2);
    raw(&len, 2);
    if (align8 && (bytes.size() % 8) != 0) bytes.insert(bytes.end(), 8 - bytes.size() % 8, 0);
    raw(value, n);
    for (size_t i = n; i < padded; ++i) bytes.push_back(0);
  }
  void u8(ParameterId pid, uint8_t v) { param(pid, &v, 1); }
  void u32(ParameterId pid, uint32_t v) { param(pid, &v, 4); }
  void u64(ParameterId pid, uint64_t v) { param(pid, &v, 8, true); }
  void sedpSupport(DiscoveryMode m) { u8(ParameterId::PID_SEDP_SUPPORT, static_cast<uint8_t>(m)); }
  void state(SPDPDiscoverState s) { u8(ParameterId::PID_SNAP_STATE, static_cast<uint8_t>(s)); }
  void root(const GuidPrefix_t &r) { param(ParameterId::PID_ROOT_PREFIX, r.id.data(), r.id.size()); }
  void hash(uint64_t h) { u64(ParameterId::PID_ENDPOINT_HASH, h); }
  void lease(int32_t sec, uint32_t frac) {
    uint8_t v[8];
    std::memcpy(v, &sec, 4);
    std::memcpy(v + 4, &frac, 4);
    param(ParameterId::PID_PARTICIPANT_LEASE_DURATION, v, 8);
  }
  void protocol() {
    uint8_t v[2] = {PROTOCOLVERSION.major, PROTOCOLVERSION.minor};
    param(ParameterId::PID_PROTOCOL_VERSION, v, 2);
  }
  void guid(const Guid_t &g) {
    uint8_t v[16];
    std::memcpy(v, g.prefix.id.data(), 12);
    std::memcpy(v + 12, g.entityId.entityKey.data(), 3);
    v[15] = static_cast<uint8_t>(g.entityId.entityKind);
    param(ParameterId::PID_PARTICIPANT_GUID, v, 16);
  }
  void sentinel() {
    starts.push_back(bytes.size());
    uint16_t id = static_cast<uint16_t>(ParameterId::PID_SENTINEL), len = 0;
    raw(&id, 2);
    raw(&len, 2);
  }
};

bool parse(ParticipantProxyData &d, const std::vector<uint8_t> &b, size_t len) {
  std::vector<uint8_t> copy(b.begin(), b.begin() + len);
  ucdrBuffer buf;
  ucdr_init_buffer(&buf, copy.empty() ? nullptr : copy.data(), copy.size());
  return d.readFromUcdrBuffer(buf);
}

GuidPrefix_t prefix(uint8_t first) {
  GuidPrefix_t p{};
  for (size_t i = 0; i < p.id.size(); ++i) p.id[i] = static_cast<uint8_t>(first + i);
  return p;
}

struct LeaseGuard {
  uint32_t v = Config::SPDP_LEASE_DURATION_MS.load();
  ~LeaseGuard() { Config::SPDP_LEASE_DURATION_MS = v; }
};

void setAge(ParticipantProxyData &d, int64_t ageMs) {
  d.m_lastLivelinessReceivedTimestamp =
      std::chrono::high_resolution_clock::now() - std::chrono::milliseconds(ageMs);
}

}

TEST(ppd_sedp_support_roundtrip) {
  for (DiscoveryMode m : {DiscoveryMode::Snap, DiscoveryMode::Standard}) {
    PlBuilder b;
    b.protocol();
    b.sedpSupport(m);
    b.sentinel();
    ParticipantProxyData d;
    d.m_sedpSupport = (m == DiscoveryMode::Snap) ? DiscoveryMode::Standard : DiscoveryMode::Snap;
    REQUIRE_TRUE(parse(d, b.bytes, b.bytes.size()));
    REQUIRE_TRUE(d.m_sedpSupport == m);
  }
}

TEST(ppd_sedp_support_absent) {
  PlBuilder b;
  b.protocol();
  b.sentinel();
  ParticipantProxyData d;
  d.m_sedpSupport = DiscoveryMode::Snap;
  REQUIRE_TRUE(parse(d, b.bytes, b.bytes.size()));
  REQUIRE_TRUE(d.m_sedpSupport == DiscoveryMode::Standard);
}

TEST(ppd_root_and_hash_roundtrip) {
  const uint64_t hashes[] = {0, 1, 0xDEADBEEFCAFEF00DULL, UINT64_MAX};
  for (uint64_t h : hashes) {
    PlBuilder b;
    b.protocol();
    b.root(prefix(0x10));
    b.hash(h);
    b.sentinel();
    ParticipantProxyData d;
    REQUIRE_TRUE(parse(d, b.bytes, b.bytes.size()));
    REQUIRE_TRUE(d.m_root == prefix(0x10));
    REQUIRE_TRUE(d.m_endpointHash == h);
  }
  PlBuilder b;
  b.root(GUIDPREFIX_UNKNOWN);
  b.hash(0);
  b.sentinel();
  ParticipantProxyData d;
  d.m_root = prefix(9);
  d.m_endpointHash = 77;
  REQUIRE_TRUE(parse(d, b.bytes, b.bytes.size()));
  REQUIRE_TRUE(d.m_root == GUIDPREFIX_UNKNOWN);
  REQUIRE_TRUE(d.m_endpointHash == 0);
  PlBuilder c;
  c.sentinel();
  d.m_root = prefix(9);
  d.m_endpointHash = 77;
  REQUIRE_TRUE(parse(d, c.bytes, c.bytes.size()));
  REQUIRE_TRUE(d.m_root == GUIDPREFIX_UNKNOWN);
  REQUIRE_TRUE(d.m_endpointHash == 0);
}

TEST(ppd_gossip_state_roundtrip) {
  for (SPDPDiscoverState s : {SPDPDiscoverState::CONFIGURED, SPDPDiscoverState::UNCONFIGURED}) {
    PlBuilder b;
    b.state(s);
    b.u32(ParameterId::PID_SNAP_NET_SIZE, 42);
    b.sentinel();
    ParticipantProxyData d;
    d.m_snapState = (s == SPDPDiscoverState::CONFIGURED) ? SPDPDiscoverState::UNCONFIGURED
                                                           : SPDPDiscoverState::CONFIGURED;
    REQUIRE_TRUE(parse(d, b.bytes, b.bytes.size()));
    REQUIRE_TRUE(d.m_snapState == s);
    REQUIRE_TRUE(d.m_snapNetSize == 42);
  }
}

TEST(ppd_lease_clamped) {
  LeaseGuard g;
  PlBuilder b;
  b.lease(180, 0);
  b.sentinel();
  ParticipantProxyData d;
  REQUIRE_TRUE(parse(d, b.bytes, b.bytes.size()));
  REQUIRE_TRUE(d.m_leaseDuration.seconds == 180);

  Config::SPDP_LEASE_DURATION_MS = 200;
  setAge(d, 100);
  REQUIRE_TRUE(d.isAlive());
  setAge(d, 300);
  REQUIRE_TRUE(!d.isAlive());

  PlBuilder b2;
  b2.lease(1, 0);
  b2.sentinel();
  ParticipantProxyData d2;
  REQUIRE_TRUE(parse(d2, b2.bytes, b2.bytes.size()));
  Config::SPDP_LEASE_DURATION_MS = 180000;
  setAge(d2, 500);
  REQUIRE_TRUE(d2.isAlive());
  setAge(d2, 1500);
  REQUIRE_TRUE(!d2.isAlive());
}

TEST(ppd_alive_at_boundary) {
  LeaseGuard g;
  ParticipantProxyData d;
  d.m_leaseDuration = Duration_t{2, 0};
  Config::SPDP_LEASE_DURATION_MS = 180000;
  setAge(d, 1900);
  REQUIRE_TRUE(d.isAlive());
  setAge(d, 2100);
  REQUIRE_TRUE(!d.isAlive());
  d.onAliveSignal();
  REQUIRE_TRUE(d.isAlive());
  REQUIRE_TRUE(d.getAliveSignalAgeInMilliseconds() < 100);
  setAge(d, 20LL * 24 * 3600 * 1000);
  REQUIRE_TRUE(!d.isAlive());
}

TEST(ppd_truncated) {
  PlBuilder b;
  b.protocol();
  b.guid(Guid_t{prefix(3), ENTITYID_UNKNOWN});
  b.lease(30, 0);
  b.sedpSupport(DiscoveryMode::Snap);
  b.state(SPDPDiscoverState::CONFIGURED);
  b.root(prefix(1));
  b.hash(0x1122334455667788ULL);
  b.sentinel();

  const std::vector<size_t> &starts = b.starts;
  REQUIRE_TRUE(parse(*std::make_unique<ParticipantProxyData>(), b.bytes, b.bytes.size()));

  for (size_t cut = 0; cut < b.bytes.size(); ++cut) {
    ParticipantProxyData d;
    bool ok = parse(d, b.bytes, cut);
    size_t lastStart = 0;
    for (size_t s : starts) {
      if (s <= cut) lastStart = s;
    }
    size_t into = cut - lastStart;
    if (into >= 4) {
      REQUIRE_TRUE(!ok);
    } else {
      REQUIRE_TRUE(ok);
    }
  }
}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }
