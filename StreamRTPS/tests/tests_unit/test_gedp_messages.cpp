#include "gedp_test_util.h"
#include "harness.h"

#include <algorithm>
#include <chrono>

using namespace gedp_test;

namespace {

constexpr size_t kRequestBytes = 8 + 12 + 12 + 1;
constexpr size_t kAnnounceHeaderBytes = 8 + 8 + 12 + 4 + 4;
constexpr size_t kResponseHeaderBytes = 8 + 36 + 4 * 4;

std::vector<uint8_t> trimmed(const std::vector<uint8_t> &store, const ucdrBuffer &b) {
  return std::vector<uint8_t>(store.begin(), store.begin() + used(b));
}

SnapEDPResponse makeResponseHdr(uint32_t parts, uint32_t eps) {
  SnapEDPResponse r;
  r.sender = prefixOf(1);
  r.target = prefixOf(2);
  r.root = prefixOf(3);
  r.isResync = true;
  r.totalEndpoints = eps + 7;
  r.numParticipants = parts;
  r.numEndpoints = eps;
  return r;
}

std::vector<uint8_t> buildResponse(uint32_t parts, uint32_t eps,
                                   std::vector<TopicData> *sent = nullptr) {
  std::vector<uint8_t> store;
  ucdrBuffer b = writer(store);
  SnapEDPResponse r = makeResponseHdr(parts, eps);
  REQUIRE_TRUE(r.serializeHeader(b));
  for (uint32_t i = 0; i < parts; ++i)
    REQUIRE_TRUE(r.serializeAppendParticipant(b, prefixOf(40 + i)));
  for (uint32_t i = 0; i < eps; ++i) {
    TopicData td = makeTopic(prefixOf(60), uint8_t(i), "topic" + std::to_string(i), "Type", i & 1);
    REQUIRE_TRUE(r.serializeAppendEndpoint(b, td));
    if (sent) sent->push_back(td);
  }
  return trimmed(store, b);
}

std::vector<uint8_t> buildAnnouncement(uint32_t eps, bool disposed) {
  std::vector<uint8_t> store;
  ucdrBuffer b = writer(store);
  SnapEDPAnnouncement a;
  a.sender = prefixOf(5);
  a.endpointHash = 0x0123456789abcdefULL;
  a.disposed = disposed;
  a.numEndpoints = eps;
  REQUIRE_TRUE(a.serializeHeader(b));
  for (uint32_t i = 0; i < eps; ++i)
    REQUIRE_TRUE(a.serializeAppendEndpoint(b, makeTopic(prefixOf(5), uint8_t(i), "t", "T")));
  return trimmed(store, b);
}

std::vector<uint8_t> buildRequest(bool recon) {
  std::vector<uint8_t> store;
  ucdrBuffer b = writer(store);
  SnapEDPRequest q;
  q.sender = prefixOf(8);
  q.target = prefixOf(9);
  q.isReconciliation = recon;
  REQUIRE_TRUE(q.serializeIntoUcdrBuffer(b));
  return trimmed(store, b);
}

struct Drained {
  bool header = false;
  uint32_t participants = 0, endpoints = 0;
};

Drained drainResponse(const uint8_t *p, size_t n) {
  Exact e(p, n);
  ucdrBuffer b = e.buf();
  Drained d;
  SnapEDPResponse r;
  d.header = r.readHeader(b);
  if (!d.header) return d;
  GuidPrefix_t g;
  for (uint32_t i = 0; i < r.numParticipants && i < n; ++i) {
    if (!SnapEDPResponse::readNextParticipant(b, g)) break;
    ++d.participants;
  }
  TopicData td;
  for (uint32_t i = 0; i < r.numEndpoints && i < n; ++i) {
    if (!SnapEDPResponse::readNextEndpoint(b, td)) break;
    ++d.endpoints;
  }
  return d;
}

Drained drainAnnouncement(const uint8_t *p, size_t n) {
  Exact e(p, n);
  ucdrBuffer b = e.buf();
  Drained d;
  SnapEDPAnnouncement a;
  d.header = a.readHeader(b);
  if (!d.header) return d;
  TopicData td;
  for (uint32_t i = 0; i < a.numEndpoints && i < n; ++i) {
    if (!SnapEDPAnnouncement::readNextEndpoint(b, td)) break;
    ++d.endpoints;
  }
  return d;
}

}

TEST(request_roundtrip) {
  for (bool recon : {false, true}) {
    auto bytes = buildRequest(recon);
    REQUIRE_TRUE(bytes.size() == kRequestBytes);
    Exact e(bytes.data(), bytes.size());
    ucdrBuffer b = e.buf();
    SnapEDPRequest out;
    REQUIRE_TRUE(out.readFromUcdrBuffer(b));
    REQUIRE_TRUE(out.sender == prefixOf(8));
    REQUIRE_TRUE(out.target == prefixOf(9));
    REQUIRE_TRUE(out.isReconciliation == recon);
  }
}

TEST(response_roundtrip) {
  std::vector<TopicData> sent;
  auto bytes = buildResponse(3, 3, &sent);
  Exact e(bytes.data(), bytes.size());
  ucdrBuffer b = e.buf();
  SnapEDPResponse r;
  REQUIRE_TRUE(r.readHeader(b));
  REQUIRE_TRUE(r.sender == prefixOf(1) && r.target == prefixOf(2) && r.root == prefixOf(3));
  REQUIRE_TRUE(r.isResync);
  REQUIRE_TRUE(r.totalEndpoints == 10 && r.numParticipants == 3 && r.numEndpoints == 3);
  for (uint32_t i = 0; i < 3; ++i) {
    GuidPrefix_t g;
    REQUIRE_TRUE(SnapEDPResponse::readNextParticipant(b, g));
    REQUIRE_TRUE(g == prefixOf(uint8_t(40 + i)));
  }
  for (uint32_t i = 0; i < 3; ++i) {
    TopicData td;
    REQUIRE_TRUE(SnapEDPResponse::readNextEndpoint(b, td));
    REQUIRE_TRUE(sameTopic(sent[i], td));
  }
  TopicData none;
  REQUIRE_TRUE(!SnapEDPResponse::readNextEndpoint(b, none));
}

TEST(response_empty) {
  auto bytes = buildResponse(0, 0);
  REQUIRE_TRUE(bytes.size() == kResponseHeaderBytes);
  Drained d = drainResponse(bytes.data(), bytes.size());
  REQUIRE_TRUE(d.header && d.participants == 0 && d.endpoints == 0);
  Exact e(bytes.data(), bytes.size());
  ucdrBuffer b = e.buf();
  GuidPrefix_t g;
  TopicData td;
  SnapEDPResponse r;
  REQUIRE_TRUE(r.readHeader(b));
  REQUIRE_TRUE(!SnapEDPResponse::readNextParticipant(b, g));
  REQUIRE_TRUE(!SnapEDPResponse::readNextEndpoint(b, td));
}

TEST(announcement_roundtrip) {
  for (bool disposed : {false, true}) {
    auto bytes = buildAnnouncement(2, disposed);
    Exact e(bytes.data(), bytes.size());
    ucdrBuffer b = e.buf();
    SnapEDPAnnouncement a;
    REQUIRE_TRUE(a.readHeader(b));
    REQUIRE_TRUE(a.sender == prefixOf(5));
    REQUIRE_TRUE(a.endpointHash == 0x0123456789abcdefULL);
    REQUIRE_TRUE(a.disposed == disposed);
    REQUIRE_TRUE(a.numEndpoints == 2);
    for (uint8_t i = 0; i < 2; ++i) {
      TopicData td;
      REQUIRE_TRUE(SnapEDPAnnouncement::readNextEndpoint(b, td));
      REQUIRE_TRUE(sameTopic(makeTopic(prefixOf(5), i, "t", "T"), td));
    }
  }
}

TEST(peek_kind) {
  SnapEDPMessageKind k;
  auto q = buildRequest(false);
  REQUIRE_TRUE(peekSnapEDPMessageKind(q.data(), q.size(), k) && k == SnapEDPMessageKind::REQUEST);
  auto a = buildAnnouncement(0, false);
  REQUIRE_TRUE(peekSnapEDPMessageKind(a.data(), a.size(), k) && k == SnapEDPMessageKind::ANNOUNCEMENT);
  auto r = buildResponse(0, 0);
  REQUIRE_TRUE(peekSnapEDPMessageKind(r.data(), r.size(), k) && k == SnapEDPMessageKind::SNAPSHOT);
  auto copy = r;
  peekSnapEDPMessageKind(r.data(), r.size(), k);
  REQUIRE_TRUE(copy == r);
}

TEST(peek_kind_invalid) {
  SnapEDPMessageKind k;
  uint8_t buf[8] = {0, 3, 0, 0, 99, 0, 0, 0};
  for (size_t n = 0; n < 8; ++n) {
    Exact e(buf, n);
    REQUIRE_TRUE(!peekSnapEDPMessageKind(e.bytes.data(), n, k));
  }
  Exact e(buf, 8);
  REQUIRE_TRUE(peekSnapEDPMessageKind(e.bytes.data(), 8, k));
  REQUIRE_TRUE(static_cast<uint32_t>(k) == 99);
  Exact e2(buf, 8);
  ucdrBuffer b = e2.buf();
  SnapEDPRequest q;
  REQUIRE_TRUE(!q.readFromUcdrBuffer(b));
}

TEST(frame_exact_fit) {
  std::vector<uint8_t> store;
  ucdrBuffer b = writer(store);
  SnapEDPAnnouncement a;
  a.sender = prefixOf(5);
  REQUIRE_TRUE(a.serializeHeader(b));
  uint32_t n = 0;
  while (a.serializeAppendEndpoint(b, makeTopic(prefixOf(5), uint8_t(n), "topic", "Type"))) {
    ++n;
    REQUIRE_TRUE(n < 100);
  }
  REQUIRE_TRUE(n >= 2);
  REQUIRE_TRUE(used(b) <= kFrameBytes);
  a.numEndpoints = n;
  std::vector<uint8_t> s2;
  ucdrBuffer hb = writer(s2);
  REQUIRE_TRUE(a.serializeHeader(hb));
  std::copy(s2.begin(), s2.begin() + used(hb), store.begin());
  std::vector<uint8_t> exact = trimmed(store, b);
  Drained d = drainAnnouncement(exact.data(), exact.size());
  REQUIRE_TRUE(d.header && d.endpoints == n);
}

TEST(frame_overflow) {
  std::vector<uint8_t> store;
  ucdrBuffer b = writer(store);
  SnapEDPResponse r = makeResponseHdr(0, 0);
  REQUIRE_TRUE(r.serializeHeader(b));
  uint32_t n = 0;
  while (r.serializeAppendEndpoint(b, makeTopic(prefixOf(7), uint8_t(n), "t", "T"))) ++n;
  const size_t before = used(b);
  REQUIRE_TRUE(!r.serializeAppendEndpoint(b, makeTopic(prefixOf(7), 99, "t", "T")));
  REQUIRE_TRUE(used(b) == before);
  std::vector<uint8_t> tiny;
  ucdrBuffer tb = writer(tiny, 11);
  REQUIRE_TRUE(!r.serializeAppendParticipant(tb, prefixOf(1)));
  REQUIRE_TRUE(used(tb) == 0);
  r.numEndpoints = n;
  std::vector<uint8_t> s2;
  ucdrBuffer hb = writer(s2);
  r.serializeHeader(hb);
  std::copy(s2.begin(), s2.begin() + used(hb), store.begin());
  auto bytes = trimmed(store, b);
  Drained d = drainResponse(bytes.data(), bytes.size());
  REQUIRE_TRUE(d.header && d.endpoints == n);
}

TEST(max_length_names) {
  const std::string topic(Config::MAX_TOPICNAME_LENGTH - 2, 'n');
  const std::string type(Config::MAX_TYPENAME_LENGTH - 2, 't');
  std::vector<uint8_t> store;
  ucdrBuffer b = writer(store);
  SnapEDPAnnouncement a;
  a.numEndpoints = 1;
  REQUIRE_TRUE(a.serializeHeader(b));
  TopicData in = makeTopic(prefixOf(5), 1, topic, type);
  REQUIRE_TRUE(a.serializeAppendEndpoint(b, in));
  auto bytes = trimmed(store, b);
  Exact e(bytes.data(), bytes.size());
  ucdrBuffer rb = e.buf();
  SnapEDPAnnouncement out;
  REQUIRE_TRUE(out.readHeader(rb));
  TopicData td;
  REQUIRE_TRUE(SnapEDPAnnouncement::readNextEndpoint(rb, td));
  REQUIRE_TRUE(sameTopic(in, td));
}

TEST(truncated_every_length) {
  auto q = buildRequest(true);
  for (size_t n = 0; n < q.size(); ++n) {
    Exact e(q.data(), n);
    ucdrBuffer b = e.buf();
    SnapEDPRequest out;
    REQUIRE_TRUE(!out.readFromUcdrBuffer(b));
  }
  auto a = buildAnnouncement(2, false);
  for (size_t n = 0; n < a.size(); ++n) {
    Drained d = drainAnnouncement(a.data(), n);
    if (n < kAnnounceHeaderBytes) REQUIRE_TRUE(!d.header);
    REQUIRE_TRUE(d.endpoints <= 2);
  }
  auto r = buildResponse(2, 2);
  for (size_t n = 0; n < r.size(); ++n) {
    Drained d = drainResponse(r.data(), n);
    if (n < kResponseHeaderBytes) REQUIRE_TRUE(!d.header);
    REQUIRE_TRUE(d.participants <= 2 && d.endpoints <= 2);
  }
  auto h = buildResponse(0, 0);
  for (size_t n = 0; n < h.size(); ++n) REQUIRE_TRUE(!drainResponse(h.data(), n).header);
}

TEST(count_exceeds_payload) {
  auto bytes = buildResponse(2, 2);
  const uint32_t lie = 1000;
  std::memcpy(bytes.data() + 8 + 36 + 8, &lie, 4);
  std::memcpy(bytes.data() + 8 + 36 + 12, &lie, 4);
  Drained d = drainResponse(bytes.data(), bytes.size());
  REQUIRE_TRUE(d.header);
  REQUIRE_TRUE(d.participants <= bytes.size() / 12);

  auto a = buildAnnouncement(2, false);
  std::memcpy(a.data() + kAnnounceHeaderBytes - 4, &lie, 4);
  Drained da = drainAnnouncement(a.data(), a.size());
  REQUIRE_TRUE(da.header && da.endpoints == 2);

  auto r = buildResponse(0, 3);
  std::memcpy(r.data() + 8 + 36 + 12, &lie, 4);
  Drained dr = drainResponse(r.data(), r.size());
  REQUIRE_TRUE(dr.header && dr.endpoints == 3);
}

TEST(count_huge) {
  const auto t0 = std::chrono::steady_clock::now();
  auto r = buildResponse(1, 2);
  const uint32_t huge = 0xFFFFFFFFu;
  std::memcpy(r.data() + 8 + 36 + 8, &huge, 4);
  std::memcpy(r.data() + 8 + 36 + 12, &huge, 4);
  Exact e(r.data(), r.size());
  ucdrBuffer b = e.buf();
  SnapEDPResponse out;
  REQUIRE_TRUE(out.readHeader(b));
  REQUIRE_TRUE(out.numParticipants == huge && out.numEndpoints == huge);
  uint64_t iterations = 0;
  GuidPrefix_t g;
  while (iterations < huge && SnapEDPResponse::readNextParticipant(b, g)) ++iterations;
  TopicData td;
  while (iterations < huge && SnapEDPResponse::readNextEndpoint(b, td)) ++iterations;
  REQUIRE_TRUE(iterations <= r.size());
  REQUIRE_TRUE(std::chrono::steady_clock::now() - t0 < std::chrono::seconds(5));
}

TEST(oversized_string) {
  std::vector<uint8_t> store;
  ucdrBuffer b = writer(store);
  SnapEDPAnnouncement a;
  a.numEndpoints = 1;
  REQUIRE_TRUE(a.serializeHeader(b));
  const size_t hdr = used(b);
  REQUIRE_TRUE(a.serializeAppendEndpoint(b, makeTopic(prefixOf(5), 1, "abc", "T")));
  auto bytes = trimmed(store, b);
  size_t pos = hdr;
  bool patched = false;
  while (pos + 4 <= bytes.size()) {
    uint16_t pid, len;
    std::memcpy(&pid, &bytes[pos], 2);
    std::memcpy(&len, &bytes[pos + 2], 2);
    if (pid == uint16_t(SMElement::ParameterId::PID_TOPIC_NAME)) {
      uint32_t big = 0x10000;
      std::memcpy(&bytes[pos + 4], &big, 4);
      patched = true;
      break;
    }
    pos += 4 + len;
  }
  REQUIRE_TRUE(patched);
  Exact e(bytes.data(), bytes.size());
  ucdrBuffer rb = e.buf();
  SnapEDPAnnouncement out;
  REQUIRE_TRUE(out.readHeader(rb));
  TopicData td;
  REQUIRE_TRUE(!SnapEDPAnnouncement::readNextEndpoint(rb, td));
  REQUIRE_TRUE(td.topicName[0] == '\0');
}

TEST(total_endpoints_mismatch) {
  auto bytes = buildResponse(0, 2);
  const uint32_t total = 5000;
  std::memcpy(bytes.data() + 8 + 36 + 4, &total, 4);
  Exact e(bytes.data(), bytes.size());
  ucdrBuffer b = e.buf();
  SnapEDPResponse r;
  REQUIRE_TRUE(r.readHeader(b));
  REQUIRE_TRUE(r.totalEndpoints == 5000 && r.numEndpoints == 2);
  Drained d = drainResponse(bytes.data(), bytes.size());
  REQUIRE_TRUE(d.endpoints == 2);
}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }

TEST(frame_writer_single_frame) {
  std::vector<TopicData> sent;
  const std::vector<uint8_t> expected = buildResponse(3, 4, &sent);
  SnapEDPResponse r = makeResponseHdr(0, 0);
  r.totalEndpoints = 4 + 7;
  std::vector<uint8_t> store(kFrameBytes);
  std::vector<std::vector<uint8_t>> frames;
  SnapEDPFrameWriter frame(r, store.data(), store.size(), [&](const uint8_t *p, size_t n) {
    frames.emplace_back(p, p + n);
  });
  for (uint32_t i = 0; i < 3; ++i) frame.addParticipant(prefixOf(40 + i));
  for (const auto &td : sent) frame.addEndpoint(td);
  frame.finish();
  REQUIRE_TRUE(frames.size() == 1);
  REQUIRE_TRUE(frames[0] == expected);
}

TEST(frame_writer_splits) {
  const uint32_t numParticipants = 30;
  const uint32_t numEndpoints = 40;
  std::vector<TopicData> sent;
  for (uint32_t i = 0; i < numEndpoints; ++i) {
    sent.push_back(makeTopic(prefixOf(60), uint8_t(i), "topic" + std::to_string(i), "Type", i & 1));
  }
  SnapEDPResponse r = makeResponseHdr(0, 0);
  r.totalEndpoints = numEndpoints;
  std::vector<uint8_t> store(kFrameBytes);
  std::vector<std::vector<uint8_t>> frames;
  SnapEDPFrameWriter frame(r, store.data(), store.size(), [&](const uint8_t *p, size_t n) {
    frames.emplace_back(p, p + n);
  });
  for (uint32_t i = 0; i < numParticipants; ++i) frame.addParticipant(prefixOf(uint8_t(40 + i)));
  for (const auto &td : sent) frame.addEndpoint(td);
  frame.finish();
  REQUIRE_TRUE(frames.size() > 1);

  std::vector<GuidPrefix_t> participants;
  std::vector<TopicData> endpoints;
  for (auto &bytes : frames) {
    REQUIRE_TRUE(bytes.size() <= kFrameBytes);
    Exact e(bytes.data(), bytes.size());
    ucdrBuffer b = e.buf();
    SnapEDPResponse out;
    REQUIRE_TRUE(out.readHeader(b));
    REQUIRE_TRUE(out.totalEndpoints == numEndpoints);
    const size_t partsBefore = participants.size();
    const size_t epsBefore = endpoints.size();
    out.readBody(b, participants, endpoints);
    REQUIRE_TRUE(participants.size() - partsBefore == out.numParticipants);
    REQUIRE_TRUE(endpoints.size() - epsBefore == out.numEndpoints);
  }
  REQUIRE_TRUE(participants.size() == numParticipants);
  REQUIRE_TRUE(endpoints.size() == numEndpoints);
  for (uint32_t i = 0; i < numParticipants; ++i) REQUIRE_TRUE(participants[i] == prefixOf(uint8_t(40 + i)));
  for (uint32_t i = 0; i < numEndpoints; ++i) REQUIRE_TRUE(sameTopic(endpoints[i], sent[i]));
}

TEST(frame_writer_announcement) {
  const uint32_t numEndpoints = 40;
  SnapEDPAnnouncement a;
  a.sender = prefixOf(5);
  a.endpointHash = 0x0123456789abcdefULL;
  std::vector<TopicData> sent;
  for (uint32_t i = 0; i < numEndpoints; ++i) {
    sent.push_back(makeTopic(prefixOf(5), uint8_t(i), "topic" + std::to_string(i), "Type", i & 1));
  }
  std::vector<uint8_t> store(kFrameBytes);
  std::vector<std::vector<uint8_t>> frames;
  SnapEDPFrameWriter frame(a, store.data(), store.size(), [&](const uint8_t *p, size_t n) {
    frames.emplace_back(p, p + n);
  });
  for (const auto &td : sent) frame.addEndpoint(td);
  frame.finish();
  REQUIRE_TRUE(frames.size() > 1);

  std::vector<TopicData> endpoints;
  for (auto &bytes : frames) {
    Exact e(bytes.data(), bytes.size());
    ucdrBuffer b = e.buf();
    SnapEDPAnnouncement out;
    REQUIRE_TRUE(out.readHeader(b));
    REQUIRE_TRUE(out.sender == a.sender && out.endpointHash == a.endpointHash);
    const size_t epsBefore = endpoints.size();
    out.readEndpoints(b, endpoints);
    REQUIRE_TRUE(endpoints.size() - epsBefore == out.numEndpoints);
  }
  REQUIRE_TRUE(endpoints.size() == numEndpoints);
  for (uint32_t i = 0; i < numEndpoints; ++i) REQUIRE_TRUE(sameTopic(endpoints[i], sent[i]));
}
