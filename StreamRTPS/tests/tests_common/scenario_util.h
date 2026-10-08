#ifndef EMBRTPS_TESTS_SCENARIO_UTIL_H
#define EMBRTPS_TESTS_SCENARIO_UTIL_H

#include "oracle.h"

#include <algorithm>
#include <cstdio>
#include <string>

namespace tests {

inline uint8_t prefixByte(size_t i, size_t n) {
  size_t step = std::max<size_t>(1, 0xD0 / std::max<size_t>(n, 1));
  return static_cast<uint8_t>(0x10 + i * step);
}

inline void addNodes(Cluster &c, size_t n) {
  for (size_t i = 0; i < n; ++i) c.add(prefixByte(i, n));
}

inline rtps::LinkModel lossyModel(double pct) {
  rtps::LinkModel m;
  m.drop_rate = pct / 100.0;
  return m;
}

inline void addRing(Cluster &c, size_t n, size_t perNode = 1) {
  for (size_t i = 0; i < n; ++i)
    for (size_t e = 0; e < perNode; ++e) {
      c.addWriter(i, "R" + std::to_string(i) + "_" + std::to_string(e));
      c.addReader(i, "R" + std::to_string((i + 1) % n) + "_" + std::to_string(e));
    }
}

inline long rssKb() {
  long pages = 0, rss = 0;
  FILE *f = std::fopen("/proc/self/statm", "r");
  if (!f) return 0;
  if (std::fscanf(f, "%ld %ld", &pages, &rss) != 2) rss = 0;
  std::fclose(f);
  return rss * 4;
}

}

#endif
