/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_SNAPEDPENDPOINTSTORE_H
#define RTPS_SNAPEDPENDPOINTSTORE_H

#include "rtps/common/types.h"
#include "rtps/discovery/TopicData.h"

#include <cstdint>
#include <vector>

namespace rtps {

class SnapEDPEndpointStore {
public:
  static bool isWriter(const Guid_t &guid);
  static bool isReader(const Guid_t &guid);

  const std::vector<TopicData> &writers() const { return m_writers; }
  const std::vector<TopicData> &readers() const { return m_readers; }
  bool empty() const { return m_writers.empty() && m_readers.empty(); }

  void upsert(const TopicData &data);
  bool remove(const Guid_t &guid);
  void eraseOwner(const GuidPrefix_t &owner);

  std::vector<GuidPrefix_t> owners() const;
  std::vector<Guid_t> ofOwner(const GuidPrefix_t &owner) const;
  std::vector<Guid_t> staleOf(const GuidPrefix_t &owner, const std::vector<TopicData> &fresh) const;

  uint64_t ownerHash(const GuidPrefix_t &owner) const;
  uint64_t hash() const;

private:
  std::vector<TopicData> m_writers;
  std::vector<TopicData> m_readers;
};

}

#endif
