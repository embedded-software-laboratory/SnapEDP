#include "harness.h"

#include "rtps/storages/SetCache.h"

#include <cstring>
#include <memory>
#include <vector>

using namespace rtps;

namespace {

constexpr uint32_t kCap = HISTORY_BUFFER_SIZE - 1;

Locator loc(uint8_t port_lo = 1) {
  Locator l{};
  l.kind = LocatorKind_t::LOCATOR_KIND_UDPv4;
  l.port = 7400 + port_lo;
  l.address[15] = port_lo;
  return l;
}

Guid_t guid(uint8_t b) {
  Guid_t g{};
  g.prefix.id[0] = b;
  g.entityId.entityKey[0] = b;
  return g;
}

const TransientCacheChange *addTagged(SetCache &c, uint32_t tag) {
  uint8_t d[4];
  std::memcpy(d, &tag, 4);
  return c.addChange(d, 4, loc(static_cast<uint8_t>(tag)), guid(static_cast<uint8_t>(tag)));
}

uint32_t tagOf(const TransientCacheChange *ch) {
  uint32_t t = 0xFFFFFFFF;
  if (ch != nullptr && ch->data.m_buf.size() >= 4) std::memcpy(&t, ch->data.m_buf.data(), 4);
  return t;
}

}

TEST(set_cache_empty_state) {
  auto c = std::make_unique<SetCache>();
  REQUIRE_TRUE(c->isEmpty());
  REQUIRE_TRUE(!c->isFull());
  REQUIRE_TRUE(c->size() == 0);
  REQUIRE_TRUE(c->begin() == c->end());
  REQUIRE_TRUE(c->getChangeById(0) == nullptr);
  REQUIRE_TRUE(c->getChangeById(1) == nullptr);
  const SetCache &cc = *c;
  REQUIRE_TRUE(cc.begin() == cc.end());
}

TEST(add_and_get) {
  auto c = std::make_unique<SetCache>();
  const uint8_t d1[] = {1, 2, 3, 4, 5};
  const uint8_t d2[] = {9, 8};
  const auto *a = c->addChange(d1, sizeof(d1), loc(1), guid(1), 7);
  const auto *b = c->addChange(d2, sizeof(d2), loc(2), guid(2));
  REQUIRE_TRUE(a != nullptr && b != nullptr);
  REQUIRE_TRUE(a->id == 1 && b->id == 2);
  REQUIRE_TRUE(a->kind == ChangeKind_t::ALIVE);
  REQUIRE_TRUE(a->eventId == 7 && b->eventId == 0);
  REQUIRE_TRUE(a->data.m_buf.size() == sizeof(d1));
  REQUIRE_TRUE(a->data.spaceUsed() == sizeof(d1));
  REQUIRE_TRUE(std::memcmp(a->data.m_buf.data(), d1, sizeof(d1)) == 0);
  REQUIRE_TRUE(b->data.spaceUsed() == sizeof(d2));
  REQUIRE_TRUE(std::memcmp(b->data.m_buf.data(), d2, sizeof(d2)) == 0);
  REQUIRE_TRUE(a->locator.port == loc(1).port && b->locator.port == loc(2).port);
  REQUIRE_TRUE(a->remoteReader == guid(1) && b->remoteReader == guid(2));
  REQUIRE_TRUE(c->size() == 2 && !c->isEmpty());
  REQUIRE_TRUE(c->getChangeById(1) == a && c->getChangeById(2) == b);
  REQUIRE_TRUE(c->getChangeById(3) == nullptr);
  REQUIRE_TRUE(c->getChangeById(0) == nullptr);
  REQUIRE_TRUE(c->getIDMin() == 1 && c->getIDMax() == 2);
}

TEST(fill_to_capacity) {
  auto c = std::make_unique<SetCache>();
  for (uint32_t i = 0; i < kCap; ++i) {
    REQUIRE_TRUE(!c->isFull());
    REQUIRE_TRUE(addTagged(*c, i) != nullptr);
  }
  REQUIRE_TRUE(c->isFull());
  REQUIRE_TRUE(c->size() == kCap);
  REQUIRE_TRUE(c->getIDMin() == 1 && c->getIDMax() == kCap);
}

TEST(add_when_full) {
  auto c = std::make_unique<SetCache>();
  for (uint32_t i = 0; i < kCap; ++i) addTagged(*c, i);
  const auto *n = addTagged(*c, 1000);
  REQUIRE_TRUE(n != nullptr);
  REQUIRE_TRUE(c->isFull() && c->size() == kCap);
  REQUIRE_TRUE(c->getChangeById(1) == nullptr);
  REQUIRE_TRUE(c->getIDMin() == 2);
  REQUIRE_TRUE(c->getIDMax() == kCap + 1);
  REQUIRE_TRUE(tagOf(c->getChangeById(2)) == 1);
  REQUIRE_TRUE(tagOf(c->getChangeById(kCap + 1)) == 1000);
}

TEST(wraparound_lookup) {
  auto c = std::make_unique<SetCache>();
  uint32_t added = 0;
  for (int round = 0; round < 5; ++round) {
    for (uint32_t i = 0; i < kCap / 2 + 13; ++i) {
      const auto *ch = addTagged(*c, added);
      REQUIRE_TRUE(ch != nullptr && ch->id == added + 1);
      ++added;
    }
    c->removeUntilIncl(c->getIDMin() + 20);
  }
  for (uint32_t i = 0; i < kCap * 3; ++i) {
    addTagged(*c, added++);
  }
  ChangeId_t lo = c->getIDMin(), hi = c->getIDMax();
  REQUIRE_TRUE(hi == added);
  REQUIRE_TRUE(hi - lo + 1 == c->size());
  for (ChangeId_t id = lo; id <= hi; ++id) {
    const auto *ch = c->getChangeById(id);
    REQUIRE_TRUE(ch != nullptr && ch->id == id);
    REQUIRE_TRUE(tagOf(ch) == id - 1);
  }
  for (ChangeId_t id = 1; id < lo; id += 7) {
    REQUIRE_TRUE(c->getChangeById(id) == nullptr);
  }
  REQUIRE_TRUE(c->getChangeById(hi + 1) == nullptr);
}

TEST(drop_by_id) {
  for (int which = 0; which < 3; ++which) {
    auto c = std::make_unique<SetCache>();
    for (uint32_t i = 0; i < 10; ++i) addTagged(*c, i);
    ChangeId_t victim = which == 0 ? 5 : which == 1 ? 1 : 10;
    c->dropByID(victim);
    REQUIRE_TRUE(c->getChangeById(victim) == nullptr);
    for (ChangeId_t id = 1; id <= 10; ++id) {
      if (id == victim) continue;
      REQUIRE_TRUE(c->getChangeById(id) != nullptr && tagOf(c->getChangeById(id)) == id - 1);
    }
    if (which == 1) REQUIRE_TRUE(c->getIDMin() == 2 && c->size() == 9);
    c->dropByID(victim);
    c->dropByID(9999);
    c->dropByID(0);
    for (ChangeId_t id = 1; id <= 10; ++id) {
      if (id == victim) continue;
      REQUIRE_TRUE(c->getChangeById(id) != nullptr);
    }
  }
  SetCache empty;
  empty.dropByID(1);
  REQUIRE_TRUE(empty.isEmpty());
}

TEST(remove_until_incl) {
  auto c = std::make_unique<SetCache>();
  for (uint32_t i = 0; i < 10; ++i) addTagged(*c, i);
  c->removeUntilIncl(4);
  REQUIRE_TRUE(c->getIDMin() == 5 && c->getIDMax() == 10 && c->size() == 6);
  for (ChangeId_t id = 1; id <= 4; ++id) REQUIRE_TRUE(c->getChangeById(id) == nullptr);
  for (ChangeId_t id = 5; id <= 10; ++id) REQUIRE_TRUE(tagOf(c->getChangeById(id)) == id - 1);
  c->removeUntilIncl(2);
  REQUIRE_TRUE(c->size() == 6);
  c->removeUntilIncl(1000);
  REQUIRE_TRUE(c->isEmpty() && c->size() == 0);
  REQUIRE_TRUE(c->getChangeById(7) == nullptr);
  c->removeUntilIncl(5);
  const auto *n = addTagged(*c, 77);
  REQUIRE_TRUE(n->id == 11 && c->getIDMin() == 11 && c->getIDMax() == 11);
  REQUIRE_TRUE(tagOf(c->getChangeById(11)) == 77);
  c->removeUntilIncl(11);
  REQUIRE_TRUE(c->isEmpty());
}

TEST(id_min_max) {
  auto c = std::make_unique<SetCache>();
  REQUIRE_TRUE(c->getIDMin() == 0 && c->getIDMax() == 0);
  addTagged(*c, 0);
  REQUIRE_TRUE(c->getIDMin() == 1 && c->getIDMax() == 1);
  for (uint32_t i = 0; i < kCap * 2 + 5; ++i) addTagged(*c, i);
  REQUIRE_TRUE(c->getIDMax() == kCap * 2 + 6);
  REQUIRE_TRUE(c->getIDMin() == c->getIDMax() - kCap + 1);
  c->removeUntilIncl(c->getIDMax());
  REQUIRE_TRUE(c->getIDMin() == 0 && c->getIDMax() == 0);
}

TEST(set_cache_iteration) {
  auto c = std::make_unique<SetCache>();
  for (uint32_t i = 0; i < kCap + 40; ++i) addTagged(*c, i);
  c->dropByID(c->getIDMin() + 3);
  ChangeId_t expect = c->getIDMin();
  uint32_t n = 0;
  for (auto it = c->begin(); it != c->end(); ++it) {
    REQUIRE_TRUE(it->id == expect);
    ++expect;
    ++n;
  }
  REQUIRE_TRUE(n == c->size());
  const SetCache &cc = *c;
  expect = cc.getIDMin();
  n = 0;
  for (const auto &ch : cc) {
    REQUIRE_TRUE(ch.id == expect++);
    ++n;
  }
  REQUIRE_TRUE(n == cc.size());
}

TEST(max_payload) {
  auto c = std::make_unique<SetCache>();
  for (DataSize_t sz : {static_cast<DataSize_t>(1472), static_cast<DataSize_t>(65535)}) {
    std::vector<uint8_t> big(sz);
    for (size_t i = 0; i < big.size(); ++i) big[i] = static_cast<uint8_t>(i * 31 + 7);
    const auto *ch = c->addChange(big.data(), sz, loc(), guid(1));
    REQUIRE_TRUE(ch != nullptr);
    REQUIRE_TRUE(ch->data.spaceUsed() == sz);
    REQUIRE_TRUE(ch->data.m_buf.size() == sz);
    REQUIRE_TRUE(std::memcmp(ch->data.m_buf.data(), big.data(), sz) == 0);
  }
}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }
