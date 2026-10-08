#include "harness.h"

#include "rtps/common/types.h"
#include "rtps/utils/FixedMap.h"
#include "rtps/utils/FixedSet.h"

#include <set>

using namespace rtps;

namespace {

GuidPrefix_t prefix(uint8_t first, uint8_t last = 0) {
  GuidPrefix_t p{};
  p.id[0] = first;
  p.id[11] = last;
  return p;
}

template <class M> std::set<int> keysOf(const M &m) {
  std::set<int> s;
  for (const auto &e : m) {
    s.insert(e.key);
  }
  return s;
}

}

TEST(map_insert_find) {
  FixedMap<int, int, 8> m;
  REQUIRE_TRUE(m.size() == 0);
  REQUIRE_TRUE(m.find(1) == nullptr);
  for (int i = 0; i < 5; ++i) {
    m.insertOrAssign(i, i * 10);
  }
  REQUIRE_TRUE(m.size() == 5);
  for (int i = 0; i < 5; ++i) {
    int *v = m.find(i);
    REQUIRE_TRUE(v != nullptr && *v == i * 10);
  }
  REQUIRE_TRUE(m.find(99) == nullptr);
  const auto &cm = m;
  REQUIRE_TRUE(cm.find(3) != nullptr && *cm.find(3) == 30);
  REQUIRE_TRUE(cm.find(99) == nullptr);
}

TEST(map_assign_existing) {
  FixedMap<int, int, 4> m;
  m.insertOrAssign(1, 10);
  m.insertOrAssign(2, 20);
  m.insertOrAssign(1, 11);
  REQUIRE_TRUE(m.size() == 2);
  REQUIRE_TRUE(*m.find(1) == 11);
  REQUIRE_TRUE(*m.find(2) == 20);
}

TEST(map_erase_positions) {
  for (int victim : {0, 2, 4}) {
    FixedMap<int, int, 8> m;
    for (int i = 0; i < 5; ++i) {
      m.insertOrAssign(i, i + 100);
    }
    REQUIRE_TRUE(m.erase(victim));
    REQUIRE_TRUE(m.size() == 4);
    REQUIRE_TRUE(m.find(victim) == nullptr);
    for (int i = 0; i < 5; ++i) {
      if (i == victim) continue;
      REQUIRE_TRUE(m.find(i) != nullptr && *m.find(i) == i + 100);
    }
    REQUIRE_TRUE(!m.erase(victim));
  }
  FixedMap<int, int, 4> m;
  REQUIRE_TRUE(!m.erase(1));
  m.insertOrAssign(1, 1);
  REQUIRE_TRUE(!m.erase(2));
  REQUIRE_TRUE(m.size() == 1);
}

TEST(map_iteration) {
  FixedMap<int, int, 8> m;
  REQUIRE_TRUE(m.begin() == m.end());
  for (int i = 0; i < 6; ++i) {
    m.insertOrAssign(i, i);
  }
  REQUIRE_TRUE(keysOf(m) == (std::set<int>{0, 1, 2, 3, 4, 5}));
  size_t n = 0;
  for (const auto &e : m) { (void)e; ++n; }
  REQUIRE_TRUE(n == m.size());

  m.erase(2);
  m.erase(5);
  m.erase(0);
  REQUIRE_TRUE(keysOf(m) == (std::set<int>{1, 3, 4}));
  n = 0;
  for (auto &e : m) { (void)e; ++n; }
  REQUIRE_TRUE(n == 3);

  m.clear();
  REQUIRE_TRUE(m.size() == 0);
  REQUIRE_TRUE(m.begin() == m.end());
  REQUIRE_TRUE(m.find(1) == nullptr);
  m.insertOrAssign(7, 7);
  REQUIRE_TRUE(keysOf(m) == (std::set<int>{7}));
}

TEST(map_at_capacity) {
  FixedMap<int, int, 3> m;
  m.insertOrAssign(1, 1);
  m.insertOrAssign(2, 2);
  m.insertOrAssign(3, 3);
  REQUIRE_TRUE(m.size() == 3);
  m.insertOrAssign(4, 4);
  REQUIRE_TRUE(m.size() == 3);
  REQUIRE_TRUE(m.find(4) == nullptr);
  REQUIRE_TRUE(keysOf(m) == (std::set<int>{1, 2, 3}));
  m.insertOrAssign(2, 22);
  REQUIRE_TRUE(m.size() == 3 && *m.find(2) == 22);
  REQUIRE_TRUE(m.erase(1));
  m.insertOrAssign(4, 4);
  REQUIRE_TRUE(m.size() == 3 && m.find(4) != nullptr);
}

TEST(set_add_contains_remove) {
  FixedSet<int, 8> s;
  REQUIRE_TRUE(s.empty() && s.size() == 0);
  REQUIRE_TRUE(s.add(1));
  REQUIRE_TRUE(s.add(2));
  REQUIRE_TRUE(!s.add(1));
  REQUIRE_TRUE(s.size() == 2 && !s.empty());
  REQUIRE_TRUE(s.contains(1) && s.contains(2) && !s.contains(3));
  REQUIRE_TRUE(!s.remove(3));
  REQUIRE_TRUE(s.size() == 2);
  REQUIRE_TRUE(s.remove(1));
  REQUIRE_TRUE(!s.contains(1) && s.contains(2) && s.size() == 1);
  REQUIRE_TRUE(!s.remove(1));
  s.clear();
  REQUIRE_TRUE(s.empty() && !s.contains(2));
}

TEST(set_iteration_after_remove) {
  FixedSet<int, 8> s;
  for (int i = 0; i < 6; ++i) {
    s.add(i);
  }
  s.remove(0);
  s.remove(3);
  s.remove(5);
  std::multiset<int> seen(s.begin(), s.end());
  REQUIRE_TRUE(seen == (std::multiset<int>{1, 2, 4}));
  const auto &cs = s;
  size_t n = 0;
  for (auto it = cs.begin(); it != cs.end(); ++it) ++n;
  REQUIRE_TRUE(n == 3);
}

TEST(set_at_capacity) {
  FixedSet<int, 3> s;
  REQUIRE_TRUE(s.add(1) && s.add(2) && s.add(3));
  REQUIRE_TRUE(!s.add(4));
  REQUIRE_TRUE(s.size() == 3);
  REQUIRE_TRUE(!s.contains(4));
  REQUIRE_TRUE(s.contains(1) && s.contains(2) && s.contains(3));
  REQUIRE_TRUE(!s.add(2));
  REQUIRE_TRUE(s.remove(2));
  REQUIRE_TRUE(s.add(4));
  REQUIRE_TRUE(s.size() == 3);
}

TEST(guid_prefix_keys) {
  FixedMap<GuidPrefix_t, int, 4> m;
  m.insertOrAssign(prefix(1, 1), 1);
  m.insertOrAssign(prefix(1, 2), 2);
  REQUIRE_TRUE(m.size() == 2);
  REQUIRE_TRUE(*m.find(prefix(1, 1)) == 1);
  REQUIRE_TRUE(*m.find(prefix(1, 2)) == 2);
  REQUIRE_TRUE(m.find(prefix(1, 3)) == nullptr);

  FixedSet<GuidPrefix_t, 4> s;
  REQUIRE_TRUE(s.add(prefix(5, 0)));
  REQUIRE_TRUE(s.add(prefix(5, 1)));
  REQUIRE_TRUE(!s.add(prefix(5, 1)));
  REQUIRE_TRUE(s.size() == 2);
}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }
