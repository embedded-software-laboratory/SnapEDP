#include "scenario_util.h"

using namespace tests;
using namespace rtps;

namespace {

void createBeforeStart() {
  Cluster c;
  addNodes(c, 8);
  addRing(c, 8);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 30000));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void createDuringConvergence() {
  Cluster c;
  addNodes(c, 8);
  c.startAll();
  for (size_t i = 0; i < 8; ++i) {
    c.addWriter(i, "R" + std::to_string(i));
    c.addReader(i, "R" + std::to_string((i + 1) % 8));
    std::this_thread::sleep_for(std::chrono::milliseconds(scaled(10)));
  }
  REQUIRE_TRUE(waitConverged(c, 30000));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void createAfterConvergence(size_t n) {
  Cluster c;
  addNodes(c, n);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 30000 + static_cast<int>(n) * 500));
  c.resetTraffic();
  addRing(c, n);
  REQUIRE_TRUE(waitConverged(c, 30000));
  REQUIRE_TRUE(dataRoundTrip(c));
  auto t = c.traffic();
  std::cout << "  gedp=" << t.snapEdp << " gedpUnicast=" << t.snapEdpUnicast << std::endl;
  REQUIRE_TRUE(t.snapEdp > 0);
  REQUIRE_TRUE(t.snapEdpUnicast == 0);
}

void removeOnManyNodes() {
  Cluster c;
  addNodes(c, 8);
  addRing(c, 8, 2);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 30000));
  REQUIRE_TRUE(dataRoundTrip(c));
  std::vector<std::thread> th;
  for (size_t i = 0; i < 8; ++i)
    th.emplace_back([&c, i] {
      auto &eps = c.node(i).eps;
      std::vector<Endpoint *> victims;
      for (auto &ep : eps)
        if (!ep->isWriter || (i % 2)) victims.push_back(ep.get());
      for (auto *ep : victims) c.removeEndpoint(i, ep);
    });
  for (auto &t : th) t.join();
  REQUIRE_TRUE(waitConverged(c, 30000));
  for (size_t i : c.live())
    for (auto &ep : c.node(i).eps) REQUIRE_TRUE(ep->isWriter && i % 2 == 0);
}

void churnWhileJoin() {
  Cluster c;
  addNodes(c, 8);
  for (size_t i = 0; i < 7; ++i) c.start(i);
  REQUIRE_TRUE(waitConverged(c, 30000));
  c.addReader(1, "Join");
  c.start(7);
  for (int k = 0; k < 20; ++k) {
    auto *w = c.addWriter(0, "Join");
    std::this_thread::sleep_for(std::chrono::milliseconds(scaled(20)));
    if (k % 2) c.removeEndpoint(0, w);
  }
  c.addWriter(7, "Join");
  REQUIRE_TRUE(waitConverged(c, 30000));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void churnLossy() {
  Cluster c;
  MockNetworkRouter::instance().setModel(lossyModel(std::getenv("EMBRTPS_CHURN_LOSS") ? std::atof(std::getenv("EMBRTPS_CHURN_LOSS")) : 10));
  addNodes(c, 8);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 60000));
  std::mt19937 rng(1234);
  auto until = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  size_t seq = 0;
  while (std::chrono::steady_clock::now() < until) {
    size_t i = rng() % 8;
    auto &eps = c.node(i).eps;
    if (!eps.empty() && rng() % 2) {
      c.removeEndpoint(i, eps[rng() % eps.size()].get());
    } else if (rng() % 2) {
      c.addWriter(i, "C" + std::to_string(rng() % 4), "T" + std::to_string(seq++));
    } else {
      c.addReader(i, "C" + std::to_string(rng() % 4), "T" + std::to_string(seq++));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(scaled(50)));
  }
  bool ok = waitConverged(c, 90000);
  if (!ok) {
    uint64_t truth = groundTruthHash(c, c.live());
    for (size_t i : c.live())
      std::cerr << "  node[" << i << "] state=" << static_cast<int>(c.agent(i)->getCurrentState())
                << " root=0x" << std::hex << static_cast<int>(c.agent(i)->getCurrentRoot().id[0])
                << " view=0x" << c.agent(i)->getLocalViewHash() << " truth=0x" << truth
                << " own=0x" << c.agent(i)->getEndpointHash() << " expectedOwn=0x" << groundTruthHash(c, {i}) << std::dec
                << " endpoints=" << c.node(i).eps.size() << std::endl;
  }
  REQUIRE_TRUE(ok);
}

void registerAll() {
  ADD_TEST_FN(std::string("create_before_start"), createBeforeStart);
  ADD_TEST_FN(std::string("create_during_convergence"), createDuringConvergence);
  for (size_t n : {8, 32})
    ADD_TEST_FN("create_after_convergence_" + std::to_string(n),
                [n] { createAfterConvergence(n); });
  ADD_TEST_FN(std::string("remove_on_many_nodes"), removeOnManyNodes);
  ADD_TEST_FN(std::string("churn_while_join"), churnWhileJoin);
  ADD_TEST_FN(std::string("churn_lossy"), churnLossy);
}

}

int main(int argc, char **argv) {
  registerAll();
  RUN_TESTS(argc, argv);
}
