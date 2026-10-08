/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_SNAPEDPROOT_H
#define RTPS_SNAPEDPROOT_H

#include "rtps/common/types.h"
#include "rtps/utils/Lock.h"

namespace rtps {

// Container for root and publishedRoot
class SnapEDPRoot {
public:

  // Getter and setter
  const GuidPrefix_t &get() const { return m_root; }
  GuidPrefix_t getPublished() const {
    Lock lock{m_rootMutex};
    return m_rootPublished;
  }
  void set(const GuidPrefix_t &root);

private:
  // domain root we currently are using, only written through setRoot with m_mutex held
  GuidPrefix_t m_root = GUIDPREFIX_UNKNOWN;
  
  // Thread safe copy of m_root 
  mutable Mutex m_rootMutex;
  GuidPrefix_t m_rootPublished = GUIDPREFIX_UNKNOWN;
};

} // namespace rtps

#endif // RTPS_SEDPGOSSIPROOT_H
