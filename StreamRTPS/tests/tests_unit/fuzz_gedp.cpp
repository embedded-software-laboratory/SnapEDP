#include "rtps/discovery/SnapEDPMessages.h"

#include <cstddef>
#include <cstdint>
#include <vector>

using namespace rtps;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  std::vector<uint8_t> in(data, data + size);
  auto init = [&](ucdrBuffer &b) {
    ucdr_init_buffer(&b, in.data(), static_cast<uint32_t>(in.size()));
  };
  SnapEDPMessageKind kind;
  peekSnapEDPMessageKind(in.data(), in.size(), kind);
  {
    ucdrBuffer b; init(b);
    SnapEDPRequest r; r.readFromUcdrBuffer(b);
  }
  {
    ucdrBuffer b; init(b);
    SnapEDPAnnouncement a;
    if (a.readHeader(b)) {
      TopicData td;
      for (uint32_t i = 0; i < a.numEndpoints && i < size; ++i)
        if (!SnapEDPAnnouncement::readNextEndpoint(b, td)) break;
    }
  }
  {
    ucdrBuffer b; init(b);
    SnapEDPResponse r;
    if (r.readHeader(b)) {
      GuidPrefix_t g;
      for (uint32_t i = 0; i < r.numParticipants && i < size; ++i)
        if (!SnapEDPResponse::readNextParticipant(b, g)) break;
      TopicData td;
      for (uint32_t i = 0; i < r.numEndpoints && i < size; ++i)
        if (!SnapEDPResponse::readNextEndpoint(b, td)) break;
    }
  }
  {
    ucdrBuffer b; init(b);
    TopicData td; td.readFromUcdrBuffer(b);
  }
  return 0;
}
