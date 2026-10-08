/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#include "rtps/discovery/SnapEDPResync.h"

#include <algorithm>

uint32_t rtps::SnapEDPResyncAssembler::nextId() {
  const uint32_t id = m_nextResyncId++;
  if (m_nextResyncId == 0) {
    m_nextResyncId = 1;
  }
  return id;
}

void rtps::SnapEDPResyncAssembler::drop(const GuidPrefix_t &sender) {
  m_resyncAssemblies.erase(
      std::remove_if(m_resyncAssemblies.begin(), m_resyncAssemblies.end(), [&](const ResyncAssembly &a) { return a.sender == sender; }),
      m_resyncAssemblies.end());
}

bool rtps::SnapEDPResyncAssembler::add(const GuidPrefix_t &sender, uint32_t resyncId,
                                          uint32_t totalEndpoints,
                                          const std::vector<TopicData> &frame,
                                          std::vector<TopicData> &whole) {
  auto it = std::find_if(m_resyncAssemblies.begin(), m_resyncAssemblies.end(), [&](const ResyncAssembly &a) { return a.sender == sender; });
  if (it == m_resyncAssemblies.end() || it->resyncId != resyncId) {
    if (it != m_resyncAssemblies.end()) {
      m_resyncAssemblies.erase(it);
    }
    if (m_resyncAssemblies.size() >= MAX_RESYNC_ASSEMBLIES) {
      m_resyncAssemblies.erase(m_resyncAssemblies.begin());
    }
    m_resyncAssemblies.push_back(ResyncAssembly{sender, resyncId, {}});
    it = m_resyncAssemblies.end() - 1;
  }
  for (const auto &data : frame) {
    const bool present = std::any_of( it->endpoints.begin(), it->endpoints.end(),
        [&](const TopicData &d) { return d.endpointGuid == data.endpointGuid; });
    
    if (!present) {
      it->endpoints.push_back(data);
    }
  
  }

  if (it->endpoints.size() >= totalEndpoints) {
    whole = std::move(it->endpoints);
    m_resyncAssemblies.erase(it);
    return true;
  }

  return false;
}
