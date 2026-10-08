#include "scenario_util.h"
#include "discovery/SnapEDPAgentDetail.h"

#include <random>
#include <sstream>

using namespace tests;
using namespace rtps;

namespace {

uint64_t envU64(const char *name, uint64_t dflt) {
  const char *e = std::getenv(name);
  return e ? std::strtoull(e, nullptr, 0) : dflt;
}

LinkModel soakProfile() {
  LinkModel m;
  m.drop_rate = 0.05;
  m.min_delay_ms = 0;
  m.max_delay_ms = 10;
  m.mean_delay_ms = 3;
  m.preserve_order = false;
  return m;
}

struct Slot {
  size_t node = SIZE_MAX;
};

bool runSeed(uint64_t seed, size_t maxN, int events) {
  std::vector<std::string> schedule;
  Cluster c;
  c.cfg.set(Config::SNAP_RNG_SEED, seed);
  MockNetworkRouter::instance().setSeed(seed);
  MockNetworkRouter::instance().setModel(soakProfile());
  const double ts = timeScale();
  c.cfg.set(Config::SPDP_RESEND_PERIOD_MS, static_cast<uint32_t>(500 * ts));
  c.cfg.set(Config::SPDP_LEASE_DURATION_MS, static_cast<uint32_t>(2000 * ts));

  std::mt19937_64 rng(seed);
  std::vector<uint64_t> deadAtoms;
  std::vector<Slot> slots(maxN);
  auto note = [&](const std::string &s) { schedule.push_back(s); };
  auto liveSlots = [&] {
    std::vector<size_t> v;
    for (size_t i = 0; i < maxN; ++i) if (slots[i].node != SIZE_MAX) v.push_back(i);
    return v;
  };
  auto deadSlots = [&] {
    std::vector<size_t> v;
    for (size_t i = 0; i < maxN; ++i) if (slots[i].node == SIZE_MAX) v.push_back(i);
    return v;
  };
  auto join = [&](size_t slot) {
    size_t n = c.add(prefixByte(slot, maxN));
    c.start(n);
    slots[slot].node = n;
    note("join slot " + std::to_string(slot));
  };

  size_t initial = std::max<size_t>(3, maxN / 2);
  for (size_t i = 0; i < initial && i < maxN; ++i) join(i);

  bool partitioned = false;
  const char *topics[] = {"S0", "S1", "S2", "S3"};
  for (int e = 0; e < events; ++e) {
    auto live = liveSlots();
    auto dead = deadSlots();
    int kind = static_cast<int>(rng() % 100);
    if (kind < 15 && !dead.empty()) {
      join(dead[rng() % dead.size()]);
    } else if (kind < 30 && live.size() > 2) {
      size_t s = live[rng() % live.size()];
      for (auto &ep : c.node(slots[s].node).eps)
        deadAtoms.push_back(snap_detail::endpointHashAtom(c.node(slots[s].node).prefix, ep->guid()));
      c.kill(slots[s].node);
      slots[s].node = SIZE_MAX;
      note("leave slot " + std::to_string(s));
    } else if (kind < 42 && !partitioned && live.size() >= 4) {
      std::vector<MockNetworkDriver *> a, b;
      std::string desc = "partition";
      for (size_t s : live) {
        bool left = rng() % 2;
        (left ? a : b).push_back(c.driver(slots[s].node));
        desc += (left ? " L" : " R") + std::to_string(s);
      }
      if (!a.empty() && !b.empty()) {
        MockNetworkRouter::instance().partition({a, b});
        partitioned = true;
        note(desc);
      }
    } else if (kind < 54 && partitioned) {
      MockNetworkRouter::instance().heal();
      partitioned = false;
      note("heal");
    } else if (kind < 80 && !live.empty()) {
      size_t s = live[rng() % live.size()];
      const char *t = topics[rng() % 4];
      if (rng() % 2) c.addWriter(slots[s].node, t); else c.addReader(slots[s].node, t);
      note(std::string("add endpoint slot ") + std::to_string(s) + " topic " + t);
    } else if (!live.empty()) {
      size_t s = live[rng() % live.size()];
      auto &eps = c.node(slots[s].node).eps;
      if (!eps.empty()) {
        size_t k = rng() % eps.size();
        note("remove endpoint slot " + std::to_string(s) + " topic " + eps[k]->topic);
        c.removeEndpoint(slots[s].node, eps[k].get());
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(scaled(100 + static_cast<int>(rng() % 400))));
  }

  MockNetworkRouter::instance().heal();
  note("faults stop");
  OracleOptions o;
  o.requireNoDead = true;
  bool ok = waitConverged(c, 90000, o) && dataRoundTrip(c, 30000);
  if (!ok) {
    std::cerr << "SOAK FAILURE seed=" << seed << " maxN=" << maxN << "\nschedule:\n";
    for (size_t i = 0; i < schedule.size(); ++i)
      std::cerr << "  " << i << ": " << schedule[i] << "\n";
    {
      const uint64_t truth = groundTruthHash(c, c.live());
      for (size_t sl = 0; sl < maxN; ++sl) {
        if (slots[sl].node == SIZE_MAX) continue;
        const uint64_t diff = c.agent(slots[sl].node)->getLocalViewHash() ^ truth;
        if (diff == 0) continue;
        bool stale = false;
        for (size_t m = 0; m < (1u << std::min<size_t>(deadAtoms.size(), 12)); ++m) {
          uint64_t x = 0;
          for (size_t b = 0; b < std::min<size_t>(deadAtoms.size(), 12); ++b)
            if (m & (1u << b)) x ^= deadAtoms[b];
          if (x == diff) { stale = true; break; }
        }
        std::cerr << "  slot " << sl << " view differs from truth by "
                  << (stale ? "a subset of departed nodes' endpoints (stale)" : "something else") << "\n";
      }
    }
    std::cerr << "endpoints at failure:\n";
    for (size_t sl = 0; sl < maxN; ++sl) {
      if (slots[sl].node == SIZE_MAX) continue;
      for (auto &ep : c.node(slots[sl].node).eps)
        std::cerr << "  slot " << sl << (ep->isWriter ? " W " : " R ") << ep->topic
                  << (ep->isWriter ? "" : " rx=" + std::to_string(ep->rx->count.load())) << "\n";
    }
  }
  return ok;
}

void runSeeds(size_t maxN, uint64_t fullCount, int events) {
  if (const char *one = std::getenv("EMBRTPS_SOAK_SEED")) {
    REQUIRE_TRUE(runSeed(std::strtoull(one, nullptr, 0), maxN, events));
    return;
  }
  uint64_t count = envU64("EMBRTPS_SOAK_SEEDS", envU64("EMBRTPS_SOAK_FULL", 0) ? fullCount : 3);
  for (uint64_t s = 1; s <= count; ++s) {
    std::cout << "[soak] seed " << s << "/" << count << std::endl;
    REQUIRE_TRUE(runSeed(s, maxN, events));
  }
}

void longRun() {
  const bool full = envU64("EMBRTPS_SOAK_LONG", 0) != 0;
  const int totalMs = full ? 30 * 60 * 1000 : 20 * 1000;
  const int warmMs = full ? 60 * 1000 : 5 * 1000;
  Cluster c;
  addNodes(c, 8);
  addRing(c, 8);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, 30000));
  std::mt19937 rng(7);
  auto start = std::chrono::steady_clock::now();
  long rssWarm = 0;
  size_t seq = 0;
  while (true) {
    auto el = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - start).count();
    if (el > scaled(totalMs)) break;
    if (!rssWarm && el > scaled(warmMs)) rssWarm = rssKb();
    size_t i = rng() % 8;
    auto &eps = c.node(i).eps;
    if (eps.size() > 4) {
      c.removeEndpoint(i, eps.back().get());
    } else if (rng() % 2) {
      c.addWriter(i, "L" + std::to_string(seq++ % 6));
    } else {
      c.addReader(i, "L" + std::to_string(seq++ % 6));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(scaled(500)));
  }
  REQUIRE_TRUE(waitConverged(c, 30000));
  long rssEnd = rssKb();
  std::cout << "  rss warm=" << rssWarm << "kB end=" << rssEnd << "kB" << std::endl;
  REQUIRE_TRUE(rssWarm == 0 || rssEnd <= rssWarm + rssWarm / 4 + 30 * 1024);
}

void registerAll() {
  ADD_TEST_FN(std::string("random_schedule"), [] { runSeeds(16, 1000, 14); });
  ADD_TEST_FN(std::string("random_schedule_asan"), [] { runSeeds(16, 100, 14); });
  ADD_TEST_FN(std::string("random_schedule_tsan"), [] { runSeeds(8, 20, 10); });
  ADD_TEST_FN(std::string("long_run"), longRun);
}

}

int main(int argc, char **argv) {
  registerAll();
  RUN_TESTS(argc, argv);
}
