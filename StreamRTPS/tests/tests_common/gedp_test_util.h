#ifndef EMBRTPS_TESTS_GEDP_UTIL_H
#define EMBRTPS_TESTS_GEDP_UTIL_H

#include "rtps/communication/UdpDriver.h"
#include "rtps/discovery/SnapEDPMessages.h"
#include "rtps/discovery/TopicData.h"
#include "rtps/messages/MessageTypes.h"
#include "ucdr/microcdr.h"

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace gedp_test {

using namespace rtps;

constexpr size_t kFrameBytes = 1472;

inline GuidPrefix_t prefixOf(uint8_t first) {
  GuidPrefix_t p{};
  for (size_t i = 0; i < p.id.size(); ++i) {
    p.id[i] = static_cast<uint8_t>(first + i * 3);
  }
  return p;
}

inline Locator localLocator(uint32_t port) {
  const auto &ip = DefaultDriver::m_ip;
  return Locator::createUDPv4Locator(ip[0], ip[1], ip[2], ip[3], port);
}

inline TopicData makeTopic(const GuidPrefix_t &owner, uint8_t key,
                           const std::string &topic, const std::string &type,
                           bool reader = false, bool reliable = true) {
  TopicData td;
  td.endpointGuid.prefix = owner;
  td.endpointGuid.entityId.entityKey = {key, 0, 0};
  td.endpointGuid.entityId.entityKind =
      reader ? EntityKind_t::USER_DEFINED_READER_WITHOUT_KEY
             : EntityKind_t::USER_DEFINED_WRITER_WITHOUT_KEY;
  std::memset(td.topicName, 0, sizeof(td.topicName));
  std::memset(td.typeName, 0, sizeof(td.typeName));
  std::memcpy(td.topicName, topic.data(),
              std::min(topic.size(), sizeof(td.topicName) - 1));
  std::memcpy(td.typeName, type.data(),
              std::min(type.size(), sizeof(td.typeName) - 1));
  td.reliabilityKind =
      reliable ? ReliabilityKind_t::RELIABLE : ReliabilityKind_t::BEST_EFFORT;
  td.durabilityKind = DurabilityKind_t::TRANSIENT_LOCAL;
  td.unicastLocator = localLocator(7411 + key);
  return td;
}

inline bool sameTopic(const TopicData &a, const TopicData &b) {
  return a.endpointGuid == b.endpointGuid &&
         std::strcmp(a.topicName, b.topicName) == 0 &&
         std::strcmp(a.typeName, b.typeName) == 0 &&
         a.reliabilityKind == b.reliabilityKind &&
         a.durabilityKind == b.durabilityKind &&
         a.unicastLocator == b.unicastLocator;
}

struct Exact {
  std::vector<uint8_t> bytes;
  explicit Exact(const uint8_t *p, size_t n) : bytes(p, p + n) {}
  ucdrBuffer buf() {
    ucdrBuffer b;
    ucdr_init_buffer(&b, bytes.data(), static_cast<uint32_t>(bytes.size()));
    return b;
  }
};

inline ucdrBuffer writer(std::vector<uint8_t> &store,
                         size_t size = kFrameBytes) {
  store.assign(size, 0);
  ucdrBuffer b;
  ucdr_init_buffer(&b, store.data(), static_cast<uint32_t>(size));
  return b;
}

inline size_t used(const ucdrBuffer &b) { return ucdr_buffer_length(&b); }

}

#endif
