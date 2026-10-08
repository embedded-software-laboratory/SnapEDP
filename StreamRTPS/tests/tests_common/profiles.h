#ifndef EMBRTPS_TESTS_PROFILES_H
#define EMBRTPS_TESTS_PROFILES_H

#include "rtps/communication/MockNetworkRouter.h"

#include <cstdio>
#include <cstdlib>
#include <string>

namespace tests {

inline bool profileModel(const std::string &name, rtps::LinkModel &out) {
  rtps::LinkModel m;
  if (name == "ideal") {
  } else if (name == "lan") {
    m.min_delay_ms = 1; m.max_delay_ms = 1; m.mean_delay_ms = -1;
  } else if (name == "wifi") {
    m = rtps::MockNetworkRouter::wifiTestbed();
  } else if (name == "lossy-5") {
    m.drop_rate = 0.05;
  } else if (name == "lossy-10") {
    m.drop_rate = 0.10;
  } else if (name == "lossy-20") {
    m.drop_rate = 0.20;
  } else if (name == "burst") {
    m.p_good_to_bad = 0.05; m.p_bad_to_good = 0.4;
    m.loss_good = 0.0; m.loss_bad = 0.8;
  } else if (name == "reorder") {
    m.min_delay_ms = 1; m.max_delay_ms = 60; m.mean_delay_ms = 20;
    m.preserve_order = false;
  } else if (name == "bursty") {
    m.burst_period_ms = 40;
  } else if (name == "duplicate") {
    m.duplicate_rate = 0.5;
    m.min_delay_ms = 1; m.max_delay_ms = 10; m.mean_delay_ms = 3;
    m.preserve_order = false;
  } else {
    return false;
  }
  out = m;
  return true;
}

inline void applyProfile(const std::string &name) {
  rtps::LinkModel m;
  if (!profileModel(name, m)) {
    std::fprintf(stderr, "unknown profile %s\n", name.c_str());
    std::exit(2);
  }
  rtps::MockNetworkRouter::instance().setModel(m);
}

inline void applyAsymmetricUplink(rtps::MockNetworkDriver *sender, double dropRate) {
  rtps::LinkModel m;
  m.drop_rate = dropRate;
  rtps::MockNetworkRouter::instance().setLinkModel(sender, nullptr, m);
}

}

#endif
