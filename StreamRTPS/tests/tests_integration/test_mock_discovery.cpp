#include "oracle.h"

#include <array>
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

using namespace rtps;
using namespace tests;

namespace {

struct Gate {
  std::atomic<bool> open{false};
};

std::string requireStable(Cluster &c, int holdMs, const OracleOptions &o = {}) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(scaled(holdMs));
  while (std::chrono::steady_clock::now() < deadline) {
    std::string e = oracleCheck(c, o);
    if (!e.empty()) return e;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return "";
}

TEST(two_standard_baseline) {
  Cluster c;
  size_t a = c.add(0x10, DiscoveryMode::Standard);
  size_t b = c.add(0x20, DiscoveryMode::Standard);
  c.startAll();

  c.addWriter(a, "BaselineTopic", "BaselineType");
  c.addReader(b, "BaselineTopic", "BaselineType");

  REQUIRE_TRUE(waitFor([&] { return c.node(a).part->getRemoteParticipantCount() >= 1; }));
  REQUIRE_TRUE(waitFor([&] { return c.node(b).part->getRemoteParticipantCount() >= 1; }));
  REQUIRE_TRUE(dataRoundTrip(c, 15000));
}

TEST(spdp_advertises_gossip) {
  Cluster c;
  size_t a = c.add(0xA1);
  size_t b = c.add(0xB2);
  c.startAll();

  auto advertisesSnap = [&](size_t self, size_t peer) {
    return [&, self, peer] {
      ParticipantProxyData p;
      return c.node(self).part->copyRemoteParticipant(c.node(peer).prefix, p) &&
             p.m_sedpSupport == DiscoveryMode::Snap;
    };
  };
  REQUIRE_TRUE(waitFor(advertisesSnap(a, b)));
  REQUIRE_TRUE(waitFor(advertisesSnap(b, a)));
}

TEST(two_gossip_cold_start) {
  Cluster c;
  c.add(0xCC);
  c.add(0x33);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 15000));
}

TEST(three_gossip_cold_start) {
  Cluster c;
  c.add(0xF0);
  c.add(0x80);
  c.add(0x20);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 20000));
}

TEST(bifurcation_recovery) {
  Cluster c;
  size_t a = c.add(0x10);
  size_t b = c.add(0xE0);

  std::atomic<bool> blocking{true};
  MockNetworkRouter::instance().setDropFilter(
      [&blocking](MockNetworkDriver *, MockNetworkDriver *, const PacketInfo &) {
        return blocking.load();
      });
  c.startAll();

  REQUIRE_TRUE(waitFor(
      [&] {
        return c.agent(a)->getCurrentState() == SnapEDPState::Discovered &&
               c.agent(b)->getCurrentState() == SnapEDPState::Discovered;
      },
      3000, 25));
  REQUIRE_TRUE(c.node(a).part->getRemoteParticipantCount() == 0);
  REQUIRE_TRUE(c.node(b).part->getRemoteParticipantCount() == 0);

  c.addWriter(b, "BifurcationTopic", "BifurcationType");
  c.addReader(a, "BifurcationTopic", "BifurcationType");

  blocking.store(false);

  REQUIRE_TRUE(waitConverged(c, 20000));
  REQUIRE_TRUE(c.agent(b)->getCurrentRoot() == c.node(a).prefix);
  REQUIRE_TRUE(dataRoundTrip(c, 15000));
}

TEST(forced_spdp_race_three) {
  Cluster c;
  c.add(0x20);
  c.add(0x80);
  c.add(0xE0);

  std::atomic<bool> blocking{true};
  MockNetworkRouter::instance().setDropFilter(
      [&blocking](MockNetworkDriver *, MockNetworkDriver *, const PacketInfo &) {
        return blocking.load();
      });
  c.startAll();

  REQUIRE_TRUE(waitFor(
      [&] {
        for (size_t i = 0; i < c.size(); ++i) {
          if (c.agent(i)->getCurrentState() != SnapEDPState::Discovered ||
              c.node(i).part->getRemoteParticipantCount() != 0)
            return false;
        }
        return true;
      },
      3000, 25));

  blocking.store(false);
  REQUIRE_TRUE(waitConverged(c, 20000));
}

TEST(late_joiner_mid_guid) {
  Cluster c;
  size_t low = c.add(0x10);
  size_t high = c.add(0xE0);
  c.start(low);
  c.start(high);
  REQUIRE_TRUE(waitConverged(c, 15000));

  size_t mid = c.add(0x80);
  c.start(mid);
  REQUIRE_TRUE(waitConverged(c, 20000));
  REQUIRE_TRUE(c.agent(mid)->getCurrentRoot() == c.node(low).prefix);
}

TEST(low_guid_late_joiner_cascade) {
  Cluster c;
  size_t n2 = c.add(0x40);
  c.add(0x80);
  c.add(0xC0);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 15000));
  for (size_t i = 0; i < 3; ++i)
    REQUIRE_TRUE(c.agent(i)->getCurrentRoot() == c.node(n2).prefix);

  size_t low = c.add(0x10);
  c.start(low);
  REQUIRE_TRUE(waitConverged(c, 25000));
  for (size_t i = 0; i < c.size(); ++i)
    REQUIRE_TRUE(c.agent(i)->getCurrentRoot() == c.node(low).prefix);
}

TEST(cluster_merge_2plus2) {
  Cluster c;
  size_t a1 = c.add(0x10), a2 = c.add(0x30), b1 = c.add(0x80), b2 = c.add(0xA0);

  std::atomic<bool> partitioned{true};
  Gate gate;
  MockNetworkRouter::instance().setDropFilter(
      [&](MockNetworkDriver *sender, MockNetworkDriver *dst,
          const PacketInfo &) -> bool {
        if (!gate.open.load()) return true;
        if (!partitioned.load()) return false;
        auto side = [&](MockNetworkDriver *d) {
          if (d == c.driver(a1) || d == c.driver(a2)) return 1;
          if (d == c.driver(b1) || d == c.driver(b2)) return 2;
          return 0;
        };
        const int s = side(sender), d = side(dst);
        return s != 0 && d != 0 && s != d;
      });
  c.startAll();
  gate.open.store(true);

  OracleOptions sideA, sideB;
  sideA.only = {a1, a2};
  sideB.only = {b1, b2};
  REQUIRE_TRUE(waitConverged(c, 15000, sideA));
  REQUIRE_TRUE(waitConverged(c, 15000, sideB));

  partitioned.store(false);
  REQUIRE_TRUE(waitConverged(c, 25000));
}

TEST(endpoint_hash_converges) {
  Cluster c;
  size_t a = c.add(0x10);
  size_t b = c.add(0xE0);
  c.startAll();
  c.addWriter(a, "HashTopic", "HashType");
  c.addReader(b, "HashTopic", "HashType");

  REQUIRE_TRUE(dataRoundTrip(c, 15000));
  REQUIRE_TRUE(waitConverged(c, 10000));
  REQUIRE_TRUE(c.agent(a)->getLocalViewHash() != 0);
}

TEST(endpoint_removal) {
  Cluster c;
  size_t a = c.add(0x24);
  size_t b = c.add(0xD4);
  c.startAll();
  Endpoint *w = c.addWriter(a, "RemovalTopic", "RemovalType");
  Endpoint *r = c.addReader(b, "RemovalTopic", "RemovalType");
  Writer *staleWriter = w->writer;

  REQUIRE_TRUE(dataRoundTrip(c, 15000));
  REQUIRE_TRUE(waitConverged(c, 15000));
  const uint64_t hashFull = c.agent(a)->getLocalViewHash();
  REQUIRE_TRUE(hashFull != 0);

  c.removeEndpoint(b, r);
  REQUIRE_TRUE(c.node(b).dom->readerExists(*c.node(b).part, "RemovalTopic",
                                           "RemovalType", true) == nullptr);
  REQUIRE_TRUE(waitConverged(c, 15000));
  const uint64_t hashWriterOnly = c.agent(a)->getLocalViewHash();
  REQUIRE_TRUE(hashWriterOnly != 0 && hashWriterOnly != hashFull);

  c.removeEndpoint(a, w);
  REQUIRE_TRUE(c.node(a).dom->writerExists(*c.node(a).part, "RemovalTopic",
                                           "RemovalType", true) == nullptr);
  REQUIRE_TRUE(waitConverged(c, 15000));
  REQUIRE_TRUE(c.agent(a)->getLocalViewHash() == 0);
  REQUIRE_TRUE(c.agent(b)->getLocalViewHash() == 0);

  REQUIRE_TRUE(!c.node(a).dom->removeWriter(*c.node(a).part, staleWriter));
  REQUIRE_TRUE(!c.node(b).dom->removeReader(*c.node(b).part, nullptr));
}

TEST(steady_state_quiescence) {
  Cluster c;
  size_t a = c.add(0x10);
  size_t b = c.add(0x50);
  size_t cc = c.add(0x90);
  c.startAll();
  c.addWriter(a, "QuietTopic", "QuietType");
  c.addReader(b, "QuietTopic", "QuietType");
  c.addReader(cc, "QuietTopic", "QuietType");

  REQUIRE_TRUE(waitConverged(c, 20000));

  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(1000)));
  c.resetTraffic();
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(2000)));
  const auto t = c.traffic();
  std::cout << "  totalPackets=" << t.total << " gedpPackets=" << t.snapEdp << std::endl;
  REQUIRE_TRUE(t.total < 500);
  REQUIRE_TRUE(t.snapEdp < 12);
  REQUIRE_TRUE(oracleCheck(c).empty());
}

TEST(resync_debounce_bounded) {
  Cluster c;
  size_t a = c.add(0x10);
  size_t b = c.add(0xE0);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 15000));

  MockNetworkDriver *drvA = c.driver(a);
  MockNetworkDriver *drvB = c.driver(b);
  const Ip4Port_t builtinA = getBuiltInUnicastLocator(c.node(a).part->m_participantId).port;
  const Ip4Port_t builtinB = getBuiltInUnicastLocator(c.node(b).part->m_participantId).port;

  enum BlackholeMode { OFF = 0, FULL = 1, UNICAST_ONLY = 2 };
  std::atomic<int> blackhole{OFF};
  std::atomic<uint32_t> requestsFromA{0};
  MockNetworkRouter::instance().setDropFilter(
      [&](MockNetworkDriver *sender, MockNetworkDriver *dst,
          const PacketInfo &info) -> bool {
        if (sender == drvA && info.destPort == builtinB) {
          requestsFromA.fetch_add(1);
        }
        switch (blackhole.load()) {
        case FULL:
          return sender == drvB && dst == drvA;
        case UNICAST_ONLY:
          return sender == drvB && info.destPort == builtinA;
        default:
          return false;
        }
      });
  blackhole.store(FULL);

  c.addWriter(b, "DebounceTopic", "DebounceType");
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(500)));

  blackhole.store(UNICAST_ONLY);
  requestsFromA.store(0);
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(4000)));
  const uint32_t requestsDuringBlackhole = requestsFromA.load();
  std::cout << "  requestsDuringBlackhole=" << requestsDuringBlackhole << std::endl;
  REQUIRE_TRUE(requestsDuringBlackhole > 0);
  REQUIRE_TRUE(requestsDuringBlackhole <= 60);

  blackhole.store(OFF);
  REQUIRE_TRUE(waitConverged(c, 15000));
}

TEST(concurrent_joiners_race) {
  Cluster c;
  size_t a = c.add(0x10), b = c.add(0x40), cj = c.add(0x90), d = c.add(0xC0);

  std::atomic<bool> joinersIsolated{true};
  Gate gate;
  MockNetworkRouter::instance().setDropFilter(
      [&](MockNetworkDriver *sender, MockNetworkDriver *dst,
          const PacketInfo &) -> bool {
        if (!gate.open.load()) return true;
        if (!joinersIsolated.load()) return false;
        return sender == c.driver(cj) || sender == c.driver(d) ||
               dst == c.driver(cj) || dst == c.driver(d);
      });
  c.startAll();
  gate.open.store(true);

  c.addWriter(a, "RaceTopic", "RaceType");
  c.addReader(b, "RaceTopic", "RaceType");
  c.addWriter(cj, "RaceTopicC", "RaceType");
  c.addWriter(d, "RaceTopicD", "RaceType");

  OracleOptions ab;
  ab.only = {a, b};
  REQUIRE_TRUE(waitConverged(c, 15000, ab));
  REQUIRE_TRUE(waitFor(
      [&] {
        return c.agent(cj)->getCurrentState() == SnapEDPState::Discovered &&
               c.agent(d)->getCurrentState() == SnapEDPState::Discovered;
      },
      15000));

  joinersIsolated.store(false);

  REQUIRE_TRUE(waitConverged(c, 30000));
  REQUIRE_TRUE(c.agent(d)->getCurrentRoot() == c.node(a).prefix);
}

TEST(root_death_no_readoption) {
  Cluster c;
  c.cfg.set(Config::SPDP_RESEND_PERIOD_MS, 500);
  c.cfg.set(Config::SPDP_LEASE_DURATION_MS, 2000);

  size_t root = c.add(0x10);
  size_t mid = c.add(0x60);
  size_t high = c.add(0xE0);
  c.startAll();

  REQUIRE_TRUE(waitConverged(c, 15000));

  c.crash(root);

  REQUIRE_TRUE(waitFor(
      [&] {
        return !c.node(mid).part->hasRemoteParticipant(c.node(root).prefix) &&
               !c.node(high).part->hasRemoteParticipant(c.node(root).prefix);
      },
      15000, 100));

  OracleOptions opts;
  opts.requireNoDead = true;
  REQUIRE_TRUE(waitConverged(c, 15000, opts));

  std::string err = requireStable(c, 3000, opts);
  if (!err.empty()) std::cerr << "  oracle: " << err << std::endl;
  REQUIRE_TRUE(err.empty());
  REQUIRE_TRUE(c.agent(mid)->getCurrentRoot() == c.node(mid).prefix);
  REQUIRE_TRUE(c.agent(high)->getCurrentRoot() == c.node(mid).prefix);
}

}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }
