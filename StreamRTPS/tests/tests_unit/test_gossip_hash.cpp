#include "discovery/SnapEDPAgentDetail.h"
#include "gedp_test_util.h"
#include "harness.h"

#include <algorithm>
#include <numeric>
#include <random>
#include <set>

using namespace gedp_test;
using rtps::snap_detail::endpointHashAtom;
using rtps::snap_detail::fnv1a64;

namespace {

Guid_t guidOf(const GuidPrefix_t &prefix, uint8_t key, bool reader = false) {
  Guid_t g;
  g.prefix = prefix;
  g.entityId.entityKey = {key, uint8_t(key >> 3), 0};
  g.entityId.entityKind = reader ? EntityKind_t::USER_DEFINED_READER_WITHOUT_KEY
                                 : EntityKind_t::USER_DEFINED_WRITER_WITHOUT_KEY;
  return g;
}

uint64_t hashOf(const std::vector<Guid_t> &set) {
  uint64_t h = 0;
  for (const auto &g : set) h ^= endpointHashAtom(g.prefix, g);
  return h;
}

uint64_t fnv(const char *s) {
  return fnv1a64(reinterpret_cast<const uint8_t *>(s), std::strlen(s));
}

}

TEST(fnv1a64_vectors) {
  REQUIRE_TRUE(fnv("") == 0xcbf29ce484222325ULL);
  REQUIRE_TRUE(fnv("a") == 0xaf63dc4c8601ec8cULL);
  REQUIRE_TRUE(fnv("foobar") == 0x85944171f73967e8ULL);
}

TEST(atom_distinguishes_owner) {
  Guid_t g = guidOf(prefixOf(1), 1);
  GuidPrefix_t other = prefixOf(1);
  other.id[11] ^= 1;
  REQUIRE_TRUE(endpointHashAtom(prefixOf(1), g) != endpointHashAtom(other, g));
  Guid_t g2 = g;
  g2.prefix = other;
  REQUIRE_TRUE(endpointHashAtom(prefixOf(1), g) != endpointHashAtom(prefixOf(1), g2));
}

TEST(atom_distinguishes_entity) {
  GuidPrefix_t p = prefixOf(2);
  REQUIRE_TRUE(endpointHashAtom(p, guidOf(p, 1)) != endpointHashAtom(p, guidOf(p, 2)));
  REQUIRE_TRUE(endpointHashAtom(p, guidOf(p, 1, false)) != endpointHashAtom(p, guidOf(p, 1, true)));
}

TEST(xor_order_independent) {
  std::vector<Guid_t> set;
  for (uint8_t i = 0; i < 16; ++i) set.push_back(guidOf(prefixOf(uint8_t(i % 4)), i, i & 1));
  const uint64_t ref = hashOf(set);
  std::mt19937 rng(7);
  for (int i = 0; i < 50; ++i) {
    std::shuffle(set.begin(), set.end(), rng);
    REQUIRE_TRUE(hashOf(set) == ref);
  }
}

TEST(add_remove_restores) {
  std::vector<Guid_t> set = {guidOf(prefixOf(1), 1), guidOf(prefixOf(1), 2), guidOf(prefixOf(3), 1)};
  const uint64_t before = hashOf(set);
  Guid_t extra = guidOf(prefixOf(9), 7);
  uint64_t h = before ^ endpointHashAtom(extra.prefix, extra);
  REQUIRE_TRUE(h != before);
  h ^= endpointHashAtom(extra.prefix, extra);
  REQUIRE_TRUE(h == before);
}

TEST(empty_is_zero) { REQUIRE_TRUE(hashOf({}) == 0); }

TEST(no_trivial_collisions) {
  std::set<uint64_t> atoms;
  for (uint32_t i = 0; i < 10000; ++i) {
    GuidPrefix_t p = prefixOf(uint8_t(i >> 8));
    Guid_t g = guidOf(p, uint8_t(i & 0xff), (i & 0x100) != 0);
    g.entityId.entityKey[2] = uint8_t(i >> 9);
    atoms.insert(endpointHashAtom(p, g));
  }
  REQUIRE_TRUE(atoms.size() == 10000);
}

TEST(stable_value) {
  static_assert(sizeof(Guid_t) == 16, "atom input layout is part of the wire format");
  std::vector<Guid_t> set;
  for (uint8_t i = 0; i < 4; ++i) set.push_back(guidOf(prefixOf(0x10), i, i & 1));
  set.push_back(guidOf(prefixOf(0x20), 9));
  const uint64_t h = hashOf(set);
  REQUIRE_TRUE(h == 0x8739a310de592906ULL);
}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }
