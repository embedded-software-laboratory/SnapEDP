#include "gedp_tap_e3.h"

using namespace tests;
using namespace tests::e3;
using namespace rtps;

namespace {

struct Flags {
  std::atomic<int> dropped{0};
  std::atomic<bool> armed{true};
};

LinkModel delayModel(double ms) {
  LinkModel m;
  m.min_delay_ms = ms;
  m.max_delay_ms = ms;
  return m;
}

bool proxyHash(Cluster &c, size_t viewer, size_t peer, uint64_t &out) {
  Participant::RemoteSnapView v;
  if (!c.node(viewer).part->getRemoteSnapView(c.node(peer).prefix, v)) return false;
  out = v.endpointHash;
  return true;
}

TEST(announce_lost_one_peer) {
  Cluster c;
  speedUpSpdp(c);
  boot(c, {0x10, 0x50, 0x90});
  Recorder rec(c);
  rec.mapAll();
  auto f = std::make_shared<Flags>();
  rec.setDrop([f](int s, int d, const Msg &m) {
    if (m.kind == K::Announce && m.numEndpoints > 0 && s == 1 && d == 2 &&
        f->dropped.load() == 0) {
      f->dropped++;
      return true;
    }
    return false;
  });
  rec.clear();
  c.addWriter(1, "ann01");
  REQUIRE_TRUE(waitFor([&] { return f->dropped.load() > 0; }, 5000));
  REQUIRE_TRUE(waitConverged(c, 20000));
  REQUIRE_TRUE(rec.count(K::ResyncReq, 2, 1) >= 1);
  for (auto &e : rec.select(K::ResyncReq, 2)) REQUIRE_TRUE(e.dst == 1);
  rec.clearDrop();
}

TEST(dispose_lost) {
  Cluster c;
  speedUpSpdp(c);
  boot(c, {0x10, 0x50, 0x90});
  auto *w = c.addWriter(1, "ann02");
  c.addReader(2, "ann02");
  REQUIRE_TRUE(waitConverged(c, 20000));
  REQUIRE_TRUE(dataRoundTrip(c));
  const Guid_t wg = w->guid();
  Reader *r = c.node(2).eps[0]->reader;
  REQUIRE_TRUE(r->knowWriterId(wg));

  Recorder rec(c);
  rec.mapAll();
  auto f = std::make_shared<Flags>();
  rec.setDrop([f](int s, int d, const Msg &m) {
    if (m.kind == K::Dispose && s == 1 && d == 2) {
      f->dropped++;
      return true;
    }
    return false;
  });
  c.removeEndpoint(1, w);
  REQUIRE_TRUE(waitFor([&] { return f->dropped.load() > 0; }, 5000));
  REQUIRE_TRUE(waitConverged(c, 20000));
  REQUIRE_TRUE(waitFor([&] { return !r->knowWriterId(wg); }, 5000));
  REQUIRE_TRUE(rec.count(K::ResyncReq, 2, 1) >= 1);
  rec.clearDrop();
}

TEST(announce_dispose_reordered) {
  Cluster c;
  speedUpSpdp(c);
  boot(c, {0x10, 0x50, 0x90});
  Recorder rec(c);
  rec.mapAll();
  auto &router = MockNetworkRouter::instance();
  router.setLinkModel(c.driver(1), c.driver(2), delayModel(400));
  auto *w = c.addWriter(1, "ann03");
  REQUIRE_TRUE(waitFor([&] { return rec.countAlive(1, 2) >= 1; }, 5000));
  router.clearLinkModels();
  c.removeEndpoint(1, w);
  REQUIRE_TRUE(waitFor([&] { return rec.count(K::Dispose, 1, 2, true) >= 1; }, 5000));
  REQUIRE_TRUE(waitConverged(c, 20000));
  for (size_t i : c.live()) REQUIRE_TRUE(c.agent(i)->getLocalViewHash() == 0);
}

TEST(create_remove_create) {
  Cluster c;
  speedUpSpdp(c);
  boot(c, {0x10, 0x50, 0x90});
  c.addReader(0, "ann04");
  auto *w1 = c.addWriter(1, "ann04");
  c.removeEndpoint(1, w1);
  c.addWriter(1, "ann04");
  REQUIRE_TRUE(waitConverged(c, 20000));
  REQUIRE_TRUE(dataRoundTrip(c));
}

TEST(announcement_duplicated) {
  Cluster c;
  speedUpSpdp(c);
  boot(c, {0x10, 0x50, 0x90});
  Recorder rec(c);
  rec.mapAll();
  LinkModel dup;
  dup.duplicate_rate = 1.0;
  MockNetworkRouter::instance().setLinkModel(c.driver(1), c.driver(2), dup);
  c.addReader(2, "ann05");
  c.addWriter(1, "ann05");
  REQUIRE_TRUE(waitFor([&] { return rec.countAlive(1, 2) >= 2; }, 5000));
  REQUIRE_TRUE(waitConverged(c, 20000));
  REQUIRE_TRUE(dataRoundTrip(c));
  REQUIRE_TRUE(c.agent(2)->getLocalViewHash() != 0);
}

TEST(multi_frame_announce) {
  Cluster c;
  speedUpSpdp(c);
  boot(c, {0x10, 0x90});
  Recorder rec(c);
  const size_t j = c.add(0x50);
  addMany(c, j, 24, "mf");
  c.start(j);
  REQUIRE_TRUE(waitConverged(c, 30000));
  REQUIRE_TRUE(rec.countAlive(static_cast<int>(j), 0) >= 3);
}

void reannouncePolicy(uint32_t policy) {
  Cluster c;
  c.cfg.set(Config::SNAP_REANNOUNCE_AFTER_RESYNC, policy);
  speedUpSpdp(c);
  boot(c, {0x10, 0x50, 0x90});
  c.addReader(2, "ann07");
  REQUIRE_TRUE(waitConverged(c, 20000));
  Recorder rec(c);
  rec.mapAll();
  auto f = std::make_shared<Flags>();
  rec.setDrop([f](int s, int d, const Msg &m) {
    if (m.kind == K::Announce && m.numEndpoints > 0 && s == 1 && d == 2 &&
        f->dropped.load() == 0) {
      f->dropped++;
      return true;
    }
    return false;
  });
  rec.clear();
  c.addWriter(1, "ann07");
  REQUIRE_TRUE(waitFor([&] { return f->dropped.load() > 0; }, 5000));
  REQUIRE_TRUE(waitConverged(c, 20000));
  REQUIRE_TRUE(rec.count(K::ResyncReq, 2, 1) >= 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(600)));
  const size_t reann = rec.countAlive(2);
  std::cerr << "  policy=" << policy << " re-announcements by the resyncing node=" << reann << std::endl;
  if (policy == 1) REQUIRE_TRUE(reann >= 1);
  else REQUIRE_TRUE(reann == 0);
  rec.clearDrop();
}

TEST(hash_in_announcement) {
  Cluster c;
  speedUpSpdp(c);
  boot(c, {0x10, 0x90});
  Recorder rec(c);
  rec.mapAll();
  rec.setDrop([](int, int, const Msg &m) { return m.kind == K::Spdp; });
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(100)));
  rec.clear();
  c.addWriter(1, "ann08");
  REQUIRE_TRUE(waitFor([&] { return rec.countAlive(1, 0) >= 1; }, 5000));
  const uint64_t own = c.agent(1)->getEndpointHash();
  REQUIRE_TRUE(own != 0);
  REQUIRE_TRUE(waitFor(
      [&] {
        uint64_t h = 0;
        return proxyHash(c, 0, 1, h) && h == own;
      },
      3000));
  REQUIRE_TRUE(c.agent(0)->getLocalViewHash() == c.agent(1)->getLocalViewHash());
  std::this_thread::sleep_for(std::chrono::milliseconds(scaled(1000)));
  REQUIRE_TRUE(rec.count(K::ResyncReq) == 0);
  rec.clearDrop();
  REQUIRE_TRUE(waitConverged(c, 20000));
}

TEST(matching_after_announce) {
  Cluster c;
  speedUpSpdp(c);
  boot(c, {0x10, 0x50, 0x90});
  auto *w = c.addWriter(0, "ann09");
  auto *r = c.addReader(1, "ann09");
  REQUIRE_TRUE(waitConverged(c, 20000));
  REQUIRE_TRUE(waitFor([&] { return r->reader->knowWriterId(w->guid()); }, 5000));
  REQUIRE_TRUE(dataRoundTrip(c));
}

void registerParam() {
  for (uint32_t p : {0u, 1u, 2u})
    ADD_TEST_FN("reannounce_policy_" + std::to_string(p), [p] { reannouncePolicy(p); });
}

}

int main(int argc, char **argv) {
  registerParam();
  RUN_TESTS(argc, argv);
}
