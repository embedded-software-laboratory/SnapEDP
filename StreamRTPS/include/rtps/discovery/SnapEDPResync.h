/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_SNAPEDPRESYNC_H
#define RTPS_SNAPEDPRESYNC_H

#include "rtps/common/types.h"
#include "rtps/discovery/TopicData.h"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace rtps {

class SnapEDPResyncAssembler {

  // multi frame resync answers are collected per sender, the owner slice is only replaced once the whole slice is present
  struct ResyncAssembly {
    GuidPrefix_t sender{};
    uint32_t resyncId = 0;
    std::vector<TopicData> endpoints;
  };

public:
  uint32_t nextId();
  void drop(const GuidPrefix_t &sender);
  bool add(const GuidPrefix_t &sender, uint32_t resyncId, uint32_t totalEndpoints, const std::vector<TopicData> &frame, std::vector<TopicData> &whole);

private:
  static constexpr size_t MAX_RESYNC_ASSEMBLIES = 4;
  std::vector<ResyncAssembly> m_resyncAssemblies;
  uint32_t m_nextResyncId = 1;
};

} // namespace rtps

#endif // RTPS_SEDPGOSSIPRESYNC_H
