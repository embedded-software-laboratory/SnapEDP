#ifndef EMBRTPS_TESTS_CLUSTER_H
#define EMBRTPS_TESTS_CLUSTER_H

#include "harness.h"

#include "rtps/communication/MockNetworkDriver.h"
#include "rtps/communication/MockNetworkRouter.h"
#include "rtps/config.h"
#include "rtps/config/FeatureQOS.h"
#include "rtps/discovery/SnapEDPAgent.h"
#include "rtps/entities/Domain.h"
#include "rtps/entities/Participant.h"
#include "rtps/rtps.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace tests {

class ConfigGuard {
public:
  template <class T, class V> void set(std::atomic<T> &a, V v) {
    T old = a.load();
    m_restore.push_back([&a, old] { a.store(old); });
    a.store(static_cast<T>(v));
  }
  ~ConfigGuard() {
    for (auto it = m_restore.rbegin(); it != m_restore.rend(); ++it) (*it)();
  }

private:
  std::vector<std::function<void()>> m_restore;
};

inline rtps::GuidPrefix_t makePrefix(uint8_t first, uint8_t second = 0) {
  rtps::GuidPrefix_t p{};
  p.id[0] = first;
  p.id[1] = second ? second : 0x11;
  for (int i = 2; i < 11; ++i) p.id[i] = static_cast<uint8_t>(0x22 + 0x11 * (i - 2));
  p.id[11] = 0xBB;
  return p;
}

struct RxCount {
  std::atomic<uint32_t> count{0};
};
inline void rxCounter(void *callee, const rtps::ReaderCacheChange &) {
  static_cast<RxCount *>(callee)->count.fetch_add(1);
}

inline constexpr size_t PAYLOAD_SIZE = 32;

struct Endpoint {
  bool isWriter;
  std::string topic;
  std::string type;
  rtps::Writer *writer = nullptr;
  rtps::Reader *reader = nullptr;
  std::unique_ptr<RxCount> rx;
  rtps::Guid_t guid() const {
    return isWriter ? writer->m_attributes.endpointGuid
                    : reader->m_attributes.endpointGuid;
  }
};

struct Node {
  std::unique_ptr<rtps::Domain> dom;
  rtps::Participant *part = nullptr;
  rtps::GuidPrefix_t prefix{};
  rtps::DiscoveryMode mode = rtps::DiscoveryMode::Snap;
  std::atomic<rtps::MockNetworkDriver *> drv{nullptr};
  std::atomic<bool> started{false};
  std::atomic<bool> alive{false};
  std::atomic<bool> blackholed{false};
  std::vector<std::unique_ptr<Endpoint>> eps;
};

inline bool isSnapEDPPacket(const uint8_t *data, size_t size) {
  using namespace rtps;
  const EntityId_t ids[2] = {ENTITYID_SEDP_BUILTIN_ANNOUNCEMENTS_WRITER,
                             ENTITYID_P2P_BUILTIN_SNAP_WRITER};
  for (const auto &id : ids) {
    uint8_t pat[4] = {id.entityKey[0], id.entityKey[1], id.entityKey[2],
                      static_cast<uint8_t>(id.entityKind)};
    for (size_t i = 0; i + 4 <= size; ++i) {
      if (std::memcmp(data + i, pat, 4) == 0) return true;
    }
  }
  return false;
}

class Cluster {
public:
  Cluster() {
    nodes.reserve(1024);
    rtps::init();
    auto &r = rtps::MockNetworkRouter::instance();
    r.clearDropPolicy();
    r.clearLinkModels();
    r.clearBlackouts();
    r.heal();
    const char *s = std::getenv("EMBRTPS_TEST_SEED");
    uint64_t seed = s ? std::strtoull(s, nullptr, 0) : 0;
    if (seed) {
      cfg.set(rtps::Config::SNAP_RNG_SEED, seed);
      r.setSeed(seed);
    }
    std::cerr << "[seed] router=" << r.seed()
              << " gossip=" << rtps::Config::SNAP_RNG_SEED.load() << std::endl;
    r.setTap([this](const rtps::TapEvent &e) { onTap(e); });
  }

  ~Cluster() {
    auto &r = rtps::MockNetworkRouter::instance();
    r.setTap(nullptr);
    for (auto &n : nodes) destroy(*n);
    r.clearDropPolicy();
    r.clearLinkModels();
    r.clearBlackouts();
    r.heal();
  }

  ConfigGuard cfg;

  size_t add(uint8_t prefixByte,
             rtps::DiscoveryMode mode = rtps::DiscoveryMode::Snap) {
    auto n = std::make_unique<Node>();
    n->mode = mode;
    n->prefix = makePrefix(prefixByte);
    n->dom = std::make_unique<rtps::Domain>(rtps::FeatureQOS(
        mode, rtps::HeartbeatPolicyMode::AdaptiveFrequency));
    n->part = n->dom->createParticipant(n->prefix);
    REQUIRE_TRUE(n->part != nullptr);
    n->prefix = n->part->m_guidPrefix;
    n->alive = true;
    nodes.push_back(std::move(n));
    count.store(nodes.size());
    return nodes.size() - 1;
  }

  size_t addPrefix(const rtps::GuidPrefix_t &prefix,
                   rtps::DiscoveryMode mode = rtps::DiscoveryMode::Snap) {
    auto n = std::make_unique<Node>();
    n->mode = mode;
    n->dom = std::make_unique<rtps::Domain>(rtps::FeatureQOS(
        mode, rtps::HeartbeatPolicyMode::AdaptiveFrequency));
    n->part = n->dom->createParticipant(prefix);
    REQUIRE_TRUE(n->part != nullptr);
    n->prefix = n->part->m_guidPrefix;
    n->alive = true;
    nodes.push_back(std::move(n));
    count.store(nodes.size());
    return nodes.size() - 1;
  }

  void start(size_t i) {
    Node &n = *nodes[i];
    REQUIRE_TRUE(n.alive && !n.started);
    REQUIRE_TRUE(n.dom->completeInit());
    n.started = true;
    auto loc = rtps::getBuiltInUnicastLocator(n.part->m_participantId);
    n.drv = rtps::MockNetworkRouter::instance().driverBoundToPort(loc.port);
    REQUIRE_TRUE(n.drv != nullptr);
  }
  void startAll() { for (size_t i = 0; i < nodes.size(); ++i) if (!nodes[i]->started) start(i); }

  void kill(size_t i) { destroy(*nodes[i]); }

  void crash(size_t i) {
    Node &n = *nodes[i];
    REQUIRE_TRUE(n.alive && n.drv);
    n.blackholed = true;
    rtps::MockNetworkRouter::instance().isolate(n.drv);
  }

  rtps::MockNetworkDriver *driver(size_t i) { return nodes[i]->drv; }
  Node &node(size_t i) { return *nodes[i]; }
  size_t size() const { return count.load(); }

  rtps::SnapEDPAgent *agent(size_t i) {
    return static_cast<rtps::SnapEDPAgent *>(&nodes[i]->part->getSEDPAgent());
  }

  Endpoint *addWriter(size_t i, const std::string &topic,
                      const std::string &type = "T", bool reliable = true) {
    Node &n = *nodes[i];
    auto ep = std::make_unique<Endpoint>();
    ep->isWriter = true; ep->topic = topic; ep->type = type;
    ep->writer = n.dom->createWriter(*n.part, topic.c_str(), type.c_str(),
                                     reliable, true);
    REQUIRE_TRUE(ep->writer != nullptr);
    n.eps.push_back(std::move(ep));
    return n.eps.back().get();
  }
  Endpoint *addReader(size_t i, const std::string &topic,
                      const std::string &type = "T", bool reliable = true) {
    Node &n = *nodes[i];
    auto ep = std::make_unique<Endpoint>();
    ep->isWriter = false; ep->topic = topic; ep->type = type;
    ep->reader = n.dom->createReader(*n.part, topic.c_str(), type.c_str(), reliable);
    REQUIRE_TRUE(ep->reader != nullptr);
    ep->rx = std::make_unique<RxCount>();
    ep->reader->registerCallback(rxCounter, ep->rx.get());
    n.eps.push_back(std::move(ep));
    return n.eps.back().get();
  }
  void removeEndpoint(size_t i, Endpoint *ep) {
    Node &n = *nodes[i];
    bool ok = ep->isWriter ? n.dom->removeWriter(*n.part, ep->writer)
                           : n.dom->removeReader(*n.part, ep->reader);
    REQUIRE_TRUE(ok);
    for (auto it = n.eps.begin(); it != n.eps.end(); ++it)
      if (it->get() == ep) { n.eps.erase(it); break; }
  }

  std::vector<size_t> live() const {
    std::vector<size_t> v;
    for (size_t i = 0; i < nodes.size(); ++i)
      if (nodes[i]->alive && nodes[i]->started && !nodes[i]->blackholed) v.push_back(i);
    return v;
  }

  struct Traffic {
    uint64_t total = 0, snapEdp = 0, snapEdpBytes = 0;
    uint64_t snapEdpMax = 0;
    uint64_t snapEdpUnicast = 0;
  };
  Traffic traffic() const {
    std::lock_guard<std::mutex> g(tapMutex);
    return tr;
  }
  void resetTraffic() {
    std::lock_guard<std::mutex> g(tapMutex);
    tr = {};
    unicastPairs.clear();
  }
  uint64_t maxSnapEDPUnicastPerPair() const {
    std::lock_guard<std::mutex> g(tapMutex);
    uint64_t m = 0;
    for (auto &kv : unicastPairs) m = std::max(m, kv.second);
    return m;
  }

  bool quiet(int windowMs) {
    resetTraffic();
    std::this_thread::sleep_for(std::chrono::milliseconds(scaled(windowMs)));
    return traffic().snapEdp == 0;
  }

  std::vector<std::unique_ptr<Node>> nodes;
  std::atomic<size_t> count{0};

private:
  void destroy(Node &n) {
    if (!n.alive) return;
    n.eps.clear();
    n.dom.reset();
    n.part = nullptr;
    n.drv = nullptr;
    n.alive = false;
  }
  void onTap(const rtps::TapEvent &e) {
    using V = rtps::TapEvent::Verdict;
    if (e.verdict != V::Forwarded && e.verdict != V::Duplicated) return;
    std::lock_guard<std::mutex> g(tapMutex);
    ++tr.total;
    if (isSnapEDPPacket(e.data, e.size)) {
      ++tr.snapEdp;
      tr.snapEdpBytes += e.size;
      if (e.size > tr.snapEdpMax) tr.snapEdpMax = e.size;
      if (!rtps::isMultiCastPort(e.destPort)) {
        ++tr.snapEdpUnicast;
        ++unicastPairs[{e.sender, e.dst}];
      }
    }
  }
  mutable std::mutex tapMutex;
  Traffic tr;
  std::map<std::pair<rtps::MockNetworkDriver *, rtps::MockNetworkDriver *>, uint64_t> unicastPairs;
};

}

#endif
