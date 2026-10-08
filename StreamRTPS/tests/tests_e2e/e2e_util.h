#ifndef EMBRTPS_TESTS_E2E_UTIL_H
#define EMBRTPS_TESTS_E2E_UTIL_H

#include "oracle.h"

#include <mutex>

namespace tests {

inline void shortLease(Cluster &c, uint32_t resendMs = 500, uint32_t leaseMs = 2500) {
  c.cfg.set(rtps::Config::SPDP_RESEND_PERIOD_MS, scaled(static_cast<int>(resendMs)));
  c.cfg.set(rtps::Config::SPDP_LEASE_DURATION_MS, scaled(static_cast<int>(leaseMs)));
}

inline uint8_t prefixByte(size_t i, size_t n) {
  return static_cast<uint8_t>(0x10 + i * (0xE0 / n));
}

inline void addRingEndpoints(Cluster &c, size_t i, size_t n) {
  c.addWriter(i, "R" + std::to_string(i));
  c.addReader(i, "R" + std::to_string((i + 1) % n));
}

inline std::vector<size_t> allNodes(size_t n) {
  std::vector<size_t> v;
  for (size_t i = 0; i < n; ++i) v.push_back(i);
  return v;
}

inline void startConverged(Cluster &c, size_t n, int budgetMs = 30000) {
  for (size_t i = 0; i < n; ++i) c.add(prefixByte(i, n));
  for (size_t i = 0; i < n; ++i) addRingEndpoints(c, i, n);
  c.startAll();
  REQUIRE_TRUE(waitConverged(c, budgetMs));
}

class Gate {
public:
  Gate() {
    rtps::MockNetworkRouter::instance().setDropFilter(
        [this](rtps::MockNetworkDriver *s, rtps::MockNetworkDriver *d,
               const rtps::PacketInfo &) {
          if (!m_open.load()) return true;
          std::lock_guard<std::mutex> g(m_mutex);
          return m_rule ? m_rule(s, d) : false;
        });
  }
  ~Gate() { rtps::MockNetworkRouter::instance().clearDropPolicy(); }
  using Rule = std::function<bool(rtps::MockNetworkDriver *, rtps::MockNetworkDriver *)>;
  void open(Rule rule = nullptr) {
    std::lock_guard<std::mutex> g(m_mutex);
    m_rule = std::move(rule);
    m_open.store(true);
  }
  void setRule(Rule rule) {
    std::lock_guard<std::mutex> g(m_mutex);
    m_rule = std::move(rule);
  }

private:
  std::atomic<bool> m_open{false};
  std::mutex m_mutex;
  Rule m_rule;
};

inline std::vector<rtps::MockNetworkDriver *> drivers(Cluster &c, const std::vector<size_t> &idx) {
  std::vector<rtps::MockNetworkDriver *> v;
  for (size_t i : idx) v.push_back(c.driver(i));
  return v;
}

inline OracleOptions only(const std::vector<size_t> &idx, bool noDead = false) {
  OracleOptions o;
  o.only = idx;
  o.requireNoDead = noDead;
  return o;
}

}

#endif
