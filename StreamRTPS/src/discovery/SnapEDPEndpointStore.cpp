/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#include "rtps/discovery/SnapEDPEndpointStore.h"
#include "SnapEDPAgentDetail.h"

#include <algorithm>

using rtps::SnapEDPEndpointStore;
using namespace rtps::snap_detail;

bool SnapEDPEndpointStore::isWriter(const Guid_t &guid) {
  const EntityKind_t kind = guid.entityId.entityKind;
  return kind == EntityKind_t::USER_DEFINED_WRITER_WITH_KEY || kind == EntityKind_t::USER_DEFINED_WRITER_WITHOUT_KEY;
}

bool SnapEDPEndpointStore::isReader(const Guid_t &guid) {
  const EntityKind_t kind = guid.entityId.entityKind;
  return kind == EntityKind_t::USER_DEFINED_READER_WITH_KEY || kind == EntityKind_t::USER_DEFINED_READER_WITHOUT_KEY;
}

void SnapEDPEndpointStore::upsert(const TopicData &data) {
  if (!isWriter(data.endpointGuid) && !isReader(data.endpointGuid)) {
    return;
  }
  auto &list = isWriter(data.endpointGuid) ? m_writers : m_readers;

  // update locators
  for (auto &known : list) {
    if (known.endpointGuid == data.endpointGuid) {
      if (data.unicastLocator.isValid()) {
        known.unicastLocator = data.unicastLocator;
      }
      return;
    }
  }
  // store as known
  list.push_back(TopicData(data));
}

bool SnapEDPEndpointStore::remove(const Guid_t &guid) {
  auto &list = isWriter(guid) ? m_writers : m_readers;
  const auto before = list.size();
  list.erase(std::remove_if(list.begin(), list.end(), [&](const TopicData &d) { return d.endpointGuid == guid; }), list.end());
  return list.size() != before;
}

void SnapEDPEndpointStore::eraseOwner(const GuidPrefix_t &owner) {
  auto ofPrefix = [&](const TopicData &d) { return d.endpointGuid.prefix == owner; };
  m_writers.erase(std::remove_if(m_writers.begin(), m_writers.end(), ofPrefix), m_writers.end());
  m_readers.erase(std::remove_if(m_readers.begin(), m_readers.end(), ofPrefix), m_readers.end());
}

std::vector<rtps::GuidPrefix_t> SnapEDPEndpointStore::owners() const {
  std::vector<GuidPrefix_t> owners;
  auto noteOwner = [&](const GuidPrefix_t &owner) {
    if (std::find(owners.begin(), owners.end(), owner) == owners.end()) {
      owners.push_back(owner);
    }
  };
  
  for (const auto &data : m_writers) noteOwner(data.endpointGuid.prefix);
  for (const auto &data : m_readers) noteOwner(data.endpointGuid.prefix);
  return owners;
}

std::vector<rtps::Guid_t> SnapEDPEndpointStore::ofOwner(const GuidPrefix_t &owner) const {
  std::vector<Guid_t> guids;
  for (const auto &data : m_writers) {
    if (data.endpointGuid.prefix == owner) guids.push_back(data.endpointGuid);
  }
  for (const auto &data : m_readers) {
    if (data.endpointGuid.prefix == owner) guids.push_back(data.endpointGuid);
  }
  return guids;
}

std::vector<rtps::Guid_t> SnapEDPEndpointStore::staleOf(const GuidPrefix_t &owner, const std::vector<TopicData> &fresh) const {
  std::vector<Guid_t> stale;
  for (const auto &guid : ofOwner(owner)) {
    const bool present = std::any_of(fresh.begin(), fresh.end(), [&](const TopicData &d) { return d.endpointGuid == guid; });
    if (!present) {
      stale.push_back(guid);
    }
  }
  return stale;
}

uint64_t SnapEDPEndpointStore::ownerHash(const GuidPrefix_t &owner) const {
  uint64_t h = 0;
  for (const auto &guid : ofOwner(owner)) {
    h ^= endpointHashAtom(owner, guid);
  }
  return h;
}

uint64_t SnapEDPEndpointStore::hash() const {
  uint64_t h = 0;
  for (const auto &data : m_writers) {
    h ^= endpointHashAtom(data.endpointGuid.prefix, data.endpointGuid);
  }
  for (const auto &data : m_readers) {
    h ^= endpointHashAtom(data.endpointGuid.prefix, data.endpointGuid);
  }
  return h;
}
