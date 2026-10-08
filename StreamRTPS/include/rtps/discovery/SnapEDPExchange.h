/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_SNAPEDPEXCHANGE_H
#define RTPS_SNAPEDPEXCHANGE_H

#include "rtps/common/types.h"
#include "rtps/config.h"
#include "rtps/utils/FixedSet.h"

#include <cstdint>

namespace rtps {

// Represents on exchange, manages retries etc
class SnapEDPExchange {
public:
  bool active() const { return !(m_partner == GUIDPREFIX_UNKNOWN); }
  const GuidPrefix_t &partner() const { return m_partner; }
  uint32_t retries() const { return m_retries; }

  void begin(const GuidPrefix_t &partner) {
    m_partner = partner;
    m_retries = 0;
  }

  bool nextRetry() {
    if (!(m_retries < Config::REQUEST_RETRY_BOUND)) {
      return false;
    }
    m_retries += 1;
    return true;
  }

  void clear() {
    m_partner = GUIDPREFIX_UNKNOWN;
    m_retries = 0;
  }

  void skip(const GuidPrefix_t &prefix) {
    if (!m_skip.add(prefix)) {
      m_skip.clear();
      m_skip.add(prefix);
    }
  }

  bool isSkipped(const GuidPrefix_t &prefix) const { return m_skip.contains(prefix); }
  bool hasSkips() const { return m_skip.size() > 0; }
  void clearSkips() { m_skip.clear(); }
  void forget(const GuidPrefix_t &prefix) { m_skip.remove(prefix); }

private:
  GuidPrefix_t m_partner{};
  uint32_t m_retries = 0;
  FixedSet<GuidPrefix_t, Config::SPDP_MAX_NUMBER_FOUND_PARTICIPANTS> m_skip;
};

}

#endif
