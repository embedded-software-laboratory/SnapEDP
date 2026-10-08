#ifndef EMBRTPS_TESTS_ORACLE_H
#define EMBRTPS_TESTS_ORACLE_H

#include "cluster.h"

#include "discovery/SnapEDPAgentDetail.h"

#include <sstream>

namespace tests {

struct OracleOptions {
  bool requireNoDead = false;
  bool checkRoot = true;
  std::vector<size_t> only;
};

inline uint64_t groundTruthHash(Cluster &c, const std::vector<size_t> &nodes) {
  uint64_t h = 0;
  for (size_t i : nodes)
    for (auto &ep : c.node(i).eps)
      h ^= rtps::snap_detail::endpointHashAtom(c.node(i).prefix, ep->guid());
  return h;
}

inline std::string oracleCheck(Cluster &c, const OracleOptions &o = {}) {
  std::vector<size_t> nodes = o.only.empty() ? c.live() : o.only;
  std::ostringstream err;
  if (nodes.empty()) return "";

  rtps::GuidPrefix_t lowest = c.node(nodes[0]).prefix;
  for (size_t i : nodes)
    if (c.node(i).prefix.id < lowest.id) lowest = c.node(i).prefix;

  const uint64_t truth = groundTruthHash(c, nodes);

  for (size_t i : nodes) {
    auto *a = c.agent(i);
    const int b0 = c.node(i).prefix.id[0];
    if (a->getCurrentState() != rtps::SnapEDPState::Discovered) {
      err << "node[" << i << "/0x" << std::hex << b0 << std::dec << "] state="
          << static_cast<int>(a->getCurrentState()) << " (want Discovered)";
      return err.str();
    }
    if (o.checkRoot && !(a->getCurrentRoot() == lowest)) {
      err << "node[" << i << "] root=0x" << std::hex
          << static_cast<int>(a->getCurrentRoot().id[0]) << " want lowest=0x"
          << static_cast<int>(lowest.id[0]) << std::dec;
      return err.str();
    }
    size_t configured = 0;
    for (const auto &p : c.node(i).part->getRemoteSnapViews()) {
      bool isLive = false;
      for (size_t j : nodes)
        if (j != i && c.node(j).prefix == p.prefix) isLive = true;
      if (isLive && p.snapState == rtps::SPDPDiscoverState::CONFIGURED) ++configured;
      if (!isLive && o.requireNoDead && !(p.prefix == c.node(i).prefix)) {
        err << "node[" << i << "] still lists dead peer 0x" << std::hex
            << static_cast<int>(p.prefix.id[0]) << std::dec;
        return err.str();
      }
    }
    if (configured != nodes.size() - 1) {
      err << "node[" << i << "] sees " << configured << "/" << nodes.size() - 1
          << " peers CONFIGURED";
      return err.str();
    }
    if (a->getLocalViewHash() != truth) {
      err << "node[" << i << "] view hash 0x" << std::hex << a->getLocalViewHash()
          << " != ground truth 0x" << truth;
      return err.str();
    }
  }
  return "";
}

inline bool waitConverged(Cluster &c, int timeoutMs, const OracleOptions &o = {}) {
  std::string last;
  bool ok = waitFor(
      [&] { last = oracleCheck(c, o); return last.empty(); }, timeoutMs, 50);
  if (!ok) std::cerr << "  oracle: " << last << std::endl;
  return ok;
}

inline bool dataRoundTrip(Cluster &c, int timeoutMs = 15000) {
  struct R { Endpoint *ep; uint32_t base; size_t node; };
  std::vector<R> readers;
  std::vector<std::pair<size_t, Endpoint *>> writers;
  for (size_t i : c.live())
    for (auto &ep : c.node(i).eps) {
      if (ep->isWriter) writers.push_back({i, ep.get()});
      else readers.push_back({ep.get(), ep->rx->count.load(), i});
    }
  std::vector<R> expect;
  for (auto &r : readers)
    for (auto &w : writers)
      if (w.first != r.node && w.second->topic == r.ep->topic && w.second->type == r.ep->type) {
        expect.push_back(r);
        break;
      }
  uint8_t buf[PAYLOAD_SIZE] = {};
  uint32_t seq = 0;
  bool ok = waitFor(
      [&] {
        bool all = true;
        for (auto &r : expect)
          if (r.ep->rx->count.load() <= r.base) { all = false; break; }
        if (all) return true;
        ++seq;
        std::memcpy(buf, &seq, sizeof(seq));
        for (auto &w : writers)
          w.second->writer->newChange(rtps::ChangeKind_t::ALIVE, buf, PAYLOAD_SIZE);
        return false;
      },
      timeoutMs, 200);
  if (!ok) std::cerr << "  data round trip failed" << std::endl;
  return ok;
}

}

#endif
