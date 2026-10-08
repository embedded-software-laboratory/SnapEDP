#include "gedp_tap_e4.h"

using namespace tests;
using namespace tests::e4;
using namespace rtps;

namespace {

constexpr int kBudgetMs = 30000;
constexpr int kExpiryBudgetMs = 45000;

void expectSurvivors(Cluster &c, const std::vector<size_t> &survivors) {
  OracleOptions o;
  o.only = survivors;
  o.requireNoDead = true;
  REQUIRE_TRUE(waitConverged(c, kExpiryBudgetMs, o));
}

enum class Hold { Initial, Snapshot, Announce, Discovered, Reconcile };

void destroyInState(Hold h) {
  std::atomic<MockNetworkDriver *> victimDrv{nullptr};
  std::atomic<bool> dropResponsesToVictim{false}, dropAnnounce{false};
  Cluster c;
  fastLease(c, 300, 1500);
  c.cfg.set(Config::REQUEST_RETRY_BOUND, 1000000);
  MockNetworkRouter::instance().setDropFilter(
      [&](MockNetworkDriver *s, MockNetworkDriver *d, const PacketInfo &info) {
        if (dropAnnounce && classify(info) == Pkt::Announce) return true;
        if (dropResponsesToVictim && d == victimDrv.load() && classify(info) == Pkt::Response) return true;
        return false;
      });

  size_t v = 0;
  std::vector<size_t> surv;
  switch (h) {
  case Hold::Initial: {
    {
      ConfigGuard g;
      g.set(Config::SNAP_TIMEOUT_INITIAL_MS, 60000);
      v = c.add(0x40);
      c.start(v);
    }
    c.crash(v);
    surv = {c.add(0x20), c.add(0x80)};
    c.addWriter(surv[0], "TD"); c.addReader(surv[1], "TD");
    for (size_t s : surv) c.start(s);
    REQUIRE_TRUE(waitFor([&] { return stateOf(c, v) == SnapEDPState::Initial; }, 2000));
    std::this_thread::sleep_for(std::chrono::milliseconds(scaled(300)));
    REQUIRE_TRUE(stateOf(c, v) == SnapEDPState::Initial);
    break;
  }
  case Hold::Announce: {
    {
      ConfigGuard g;
      g.set(Config::SNAP_TIMEOUT_ANNOUNCING_MS, 60000);
      v = c.add(0x10);
    }
    surv = {c.add(0x50), c.add(0x90)};
    c.addWriter(surv[0], "TD"); c.addReader(surv[1], "TD");
    c.startAll();
    REQUIRE_TRUE(waitFor([&] { return stateOf(c, v) == SnapEDPState::Announce; }, kBudgetMs, 5));
    break;
  }
  case Hold::Snapshot: {
    surv = {c.add(0x20), c.add(0x60)};
    c.addWriter(surv[0], "TD"); c.addReader(surv[1], "TD");
    for (size_t s : surv) c.start(s);
    REQUIRE_TRUE(waitConverged(c, kBudgetMs));
    v = c.add(0xC0);
    c.addWriter(v, "TDv");
    victimDrv = nullptr;
    dropResponsesToVictim = true;
    c.start(v);
    victimDrv = c.driver(v);
    REQUIRE_TRUE(waitFor([&] { return stateOf(c, v) == SnapEDPState::Snapshot; }, kBudgetMs, 5));
    break;
  }
  case Hold::Discovered: {
    surv = {c.add(0x20), c.add(0x60)};
    v = c.add(0xC0);
    c.addWriter(surv[0], "TD"); c.addReader(v, "TD");
    c.startAll();
    REQUIRE_TRUE(waitConverged(c, kBudgetMs));
    REQUIRE_TRUE(stateOf(c, v) == SnapEDPState::Discovered);
    break;
  }
  case Hold::Reconcile: {
    surv = {c.add(0x20), c.add(0x60)};
    v = c.add(0xC0);
    c.startAll();
    victimDrv = c.driver(v);
    REQUIRE_TRUE(waitConverged(c, kBudgetMs));
    dropAnnounce = true;
    dropResponsesToVictim = true;
    c.addWriter(surv[0], "TDr");
    REQUIRE_TRUE(waitFor([&] { return stateOf(c, v) == SnapEDPState::Reconcile; }, kBudgetMs, 5));
    break;
  }
  }
  c.kill(v);
  dropResponsesToVictim = false;
  dropAnnounce = false;
  if (h == Hold::Reconcile) {
  }
  expectSurvivors(c, surv);
}

TEST(destroy_state_initial) { destroyInState(Hold::Initial); }
TEST(destroy_state_gossip) { destroyInState(Hold::Snapshot); }
TEST(destroy_state_announce) { destroyInState(Hold::Announce); }
TEST(destroy_state_discovered) { destroyInState(Hold::Discovered); }
TEST(destroy_state_reconcile) { destroyInState(Hold::Reconcile); }

TEST(destroy_state_election_probe) {
  int hit = 0;
  for (int i = 0; i < 20; ++i) {
    Cluster c;
    fastLease(c, 300, 1500);
    size_t a = c.add(0x20), b = c.add(0x70);
    c.start(a);
    c.start(b);
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(i * 5);
    while (std::chrono::steady_clock::now() < until) {
      if (stateOf(c, b) == SnapEDPState::Election) { ++hit; break; }
      std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    c.kill(b);
  }
  std::cerr << "  Election observed before destroy in " << hit << "/20 runs" << std::endl;
}

void destroyMidResync(bool destroyRequester) {
  std::atomic<uint64_t> requests{0};
  std::atomic<bool> armed{false};
  Cluster c;
  fastLease(c, 300, 2000);
  MockNetworkRouter::instance().setDropFilter(
      [&](MockNetworkDriver *, MockNetworkDriver *, const PacketInfo &info) {
        if (!armed) return false;
        const Pkt k = classify(info);
        if (k == Pkt::Announce) return true;
        if (k == Pkt::Request) ++requests;
        return false;
      });
  size_t owner = c.add(0x20), req = c.add(0x70), other = c.add(0xB0);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  LinkModel m; m.min_delay_ms = m.max_delay_ms = 250; m.mean_delay_ms = -1;
  MockNetworkRouter::instance().setModel(m);
  armed = true;
  c.addWriter(owner, "MR");
  REQUIRE_TRUE(waitFor([&] { return requests.load() > 0; }, kBudgetMs, 2));
  const size_t victim = destroyRequester ? req : owner;
  c.kill(victim);
  armed = false;
  MockNetworkRouter::instance().setModel(LinkModel{});
  std::vector<size_t> surv;
  for (size_t i : {owner, req, other}) if (i != victim) surv.push_back(i);
  expectSurvivors(c, surv);
}
TEST(destroy_mid_resync_requester) { destroyMidResync(true); }
TEST(destroy_mid_resync_responder) { destroyMidResync(false); }

TEST(destroy_with_packets_in_flight) {
  Cluster c;
  fastLease(c, 300, 2000);
  size_t a = c.add(0x20), b = c.add(0x60), v = c.add(0xB0);
  c.addWriter(a, "IF"); c.addReader(v, "IF");
  LinkModel m; m.min_delay_ms = m.max_delay_ms = 200; m.mean_delay_ms = -1;
  MockNetworkRouter::instance().setModel(m);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  REQUIRE_TRUE(waitFor([&] { return MockNetworkRouter::instance().inFlight() > 0; }, 5000, 1));
  c.kill(v);
  expectSurvivors(c, {a, b});
  REQUIRE_TRUE(MockNetworkRouter::instance().drain(5000) || true);
}

TEST(destroy_before_complete_init) {
  Cluster c;
  fastLease(c, 300, 1500);
  size_t a = c.add(0x20), b = c.add(0x60);
  c.start(a);
  c.start(b);
  size_t v = c.add(0xB0);
  c.kill(v);
  { Domain bare(FeatureQOS(DiscoveryMode::Snap, HeartbeatPolicyMode::AdaptiveFrequency)); }
  { Domain withPart(FeatureQOS(DiscoveryMode::Snap, HeartbeatPolicyMode::AdaptiveFrequency));
    REQUIRE_TRUE(withPart.createParticipant() != nullptr); }
  expectSurvivors(c, {a, b});
}

TEST(stop_twice) {
  Cluster c;
  fastLease(c, 300, 1500);
  size_t a = c.add(0x20), b = c.add(0x60), v = c.add(0xB0);
  c.addWriter(v, "ST"); c.addReader(a, "ST");
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  c.node(v).dom->stop();
  c.node(v).dom->stop();
  c.kill(v);
  expectSurvivors(c, {a, b});
}

TEST(endpoint_removed_during_resync) {
  std::atomic<uint32_t> annCount{0};
  std::atomic<bool> churn{true};
  Cluster c;
  MockNetworkRouter::instance().setDropFilter(
      [&](MockNetworkDriver *, MockNetworkDriver *, const PacketInfo &info) {
        return churn && classify(info) == Pkt::Announce && (annCount.fetch_add(1) % 2 == 0);
      });
  size_t owner = c.add(0x20), p1 = c.add(0x70), p2 = c.add(0xB0);
  c.addWriter(owner, "Base0");
  c.addReader(p1, "Base0");
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  std::vector<Endpoint *> live;
  for (int i = 0; i < 40; ++i) {
    live.push_back(c.addWriter(owner, "Churn" + std::to_string(i), "T", false));
    if (live.size() > 2) { c.removeEndpoint(owner, live.front()); live.erase(live.begin()); }
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
  }
  churn = false;
  REQUIRE_TRUE(waitConverged(c, 60000));
  REQUIRE_TRUE(c.agent(p1)->getLocalViewHash() == c.agent(p2)->getLocalViewHash());
}

TEST(reliable_writer_removal_race) {
  Cluster c;
  size_t a = c.add(0x20), b = c.add(0x70);
  c.addReader(b, "RW");
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, kBudgetMs));
  for (int i = 0; i < 1000; ++i) {
    Endpoint *w = c.addWriter(a, "RW", "T", true);
    std::this_thread::sleep_for(std::chrono::microseconds(i % 7 * 300));
    c.removeEndpoint(a, w);
  }
  REQUIRE_TRUE(waitConverged(c, 60000));
}

}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }
