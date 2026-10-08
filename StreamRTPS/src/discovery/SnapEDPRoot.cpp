/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#include "rtps/discovery/SnapEDPRoot.h"

void rtps::SnapEDPRoot::set(const GuidPrefix_t &root) {
  m_root = root;
  Lock lock{m_rootMutex};
  m_rootPublished = root;
}
