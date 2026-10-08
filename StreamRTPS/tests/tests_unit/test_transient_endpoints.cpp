#include "harness.h"

#include "rtps/entities/TransientReader.h"
#include "rtps/entities/TransientWriter.h"

#include <algorithm>
#include <cstring>
#include <vector>

using namespace rtps;

namespace {

struct Sent {
  ip4_struct_t addr;
  Ip4Port_t port;
  std::vector<uint8_t> bytes;
};

class StubDriver {
public:
  bool sendPacket(const PacketInfo &info) {
    Sent s;
    s.addr = info.destAddr;
    s.port = info.destPort;
    s.bytes.assign(info.buffer.m_buf.begin(),
                   info.buffer.m_buf.begin() + info.buffer.spaceUsed());
    sent.push_back(std::move(s));
    return true;
  }
  std::vector<Sent> sent;
};

using Writer_t = TransientWriterT<StubDriver>;

Guid_t readerGuid(uint8_t key, uint8_t prefixByte = 0xA0) {
  Guid_t g{};
  g.prefix.id.fill(prefixByte);
  g.entityId.entityKey[0] = key;
  g.entityId.entityKind = EntityKind_t::USER_DEFINED_READER_WITHOUT_KEY;
  return g;
}

Locator loc(uint8_t last, uint32_t port) {
  return Locator::createUDPv4Locator(10, 0, 0, last, port);
}

TopicData topic() {
  TopicData td;
  td.endpointGuid.prefix.id.fill(0x01);
  td.endpointGuid.entityId.entityKey[0] = 9;
  td.unicastLocator = loc(1, 7400);
  return td;
}

bool contains(const std::vector<uint8_t> &hay, const std::vector<uint8_t> &needle) {
  if (needle.empty()) return true;
  auto it = std::search(hay.begin(), hay.end(), needle.begin(), needle.end());
  return it != hay.end();
}

std::vector<uint8_t> pattern(size_t n, uint8_t seed = 1) {
  std::vector<uint8_t> v(n);
  for (size_t i = 0; i < n; ++i) v[i] = static_cast<uint8_t>(seed + i * 7);
  return v;
}

struct Fixture {
  StubDriver driver;
  Writer_t writer;
  Fixture() { REQUIRE_TRUE(writer.init(topic(), TopicKind_t::NO_KEY, nullptr, driver)); }
};

}

TEST(trn_unicast_destination) {
  Fixture f;
  auto data = pattern(16);
  REQUIRE_TRUE(f.writer.newChange(ChangeKind_t::ALIVE, data.data(), readerGuid(1), loc(55, 7411),
                                  data.size()) != nullptr);
  REQUIRE_TRUE(f.writer.newChange(ChangeKind_t::ALIVE, data.data(), readerGuid(2), loc(66, 7422),
                                  data.size()) != nullptr);
  REQUIRE_TRUE(f.driver.sent.empty());
  f.writer.progress();
  REQUIRE_TRUE(f.driver.sent.size() == 2);
  REQUIRE_TRUE(f.driver.sent[0].port == 7411);
  REQUIRE_TRUE(f.driver.sent[0].addr.addr == loc(55, 0).getIp4Address().addr);
  REQUIRE_TRUE(f.driver.sent[1].port == 7422);
  REQUIRE_TRUE(f.driver.sent[1].addr.addr == loc(66, 0).getIp4Address().addr);
  REQUIRE_TRUE(!f.writer.addNewMatchedReader(ReaderProxy{readerGuid(3), loc(77, 7433)}));
}

TEST(trn_payload_intact) {
  for (size_t n : {size_t(1), size_t(2), size_t(100), size_t(1000)}) {
    Fixture f;
    auto data = pattern(n, static_cast<uint8_t>(n));
    REQUIRE_TRUE(f.writer.newChange(ChangeKind_t::ALIVE, data.data(), readerGuid(1), loc(2, 7400),
                                    data.size()) != nullptr);
    f.writer.progress();
    REQUIRE_TRUE(f.driver.sent.size() == 1);
    const auto &b = f.driver.sent[0].bytes;
    REQUIRE_TRUE(b.size() > n + 20);
    REQUIRE_TRUE(std::equal(data.begin(), data.end(), b.end() - n));
    REQUIRE_TRUE(std::memcmp(b.data(), "RTPS", 4) == 0);
  }
}

TEST(trn_payload_size_limits) {
  {
    Fixture f;
    uint8_t dummy = 0;
    REQUIRE_TRUE(f.writer.newChange(ChangeKind_t::ALIVE, &dummy, readerGuid(1), loc(2, 7400), 0) !=
                 nullptr);
    f.writer.progress();
    REQUIRE_TRUE(f.driver.sent.size() == 1);
  }
  {
    Fixture f;
    auto data = pattern(1400);
    REQUIRE_TRUE(f.writer.newChange(ChangeKind_t::ALIVE, data.data(), readerGuid(1), loc(2, 7400),
                                    data.size()) != nullptr);
    f.writer.progress();
    REQUIRE_TRUE(f.driver.sent.size() == 1);
    REQUIRE_TRUE(contains(f.driver.sent[0].bytes, data));
  }
  Fixture f;
  uint8_t d = 1;
  REQUIRE_TRUE(f.writer.newChange(ChangeKind_t::PENDING, &d, readerGuid(1), loc(2, 7400),
                                  1) == nullptr);
  f.writer.progress();
  REQUIRE_TRUE(f.driver.sent.empty());
}

TEST(trn_history_full) {
  Fixture f;
  const size_t cap = Config::HISTORY_SIZE;
  const size_t total = cap + 40;
  for (size_t i = 0; i < total; ++i) {
    uint8_t tag[2] = {static_cast<uint8_t>(i & 0xFF), static_cast<uint8_t>(i >> 8)};
    REQUIRE_TRUE(f.writer.newChange(ChangeKind_t::ALIVE, tag, readerGuid(1),
                                    loc(2, static_cast<uint32_t>(10000 + i)), 2) != nullptr);
  }
  f.writer.progress();
  REQUIRE_TRUE(f.driver.sent.size() == cap);
  REQUIRE_TRUE(f.driver.sent.front().port == 10000 + (total - cap));
  REQUIRE_TRUE(f.driver.sent.back().port == 10000 + total - 1);
}

TEST(trn_sent_changes_released) {
  Fixture f;
  auto data = pattern(8);
  for (int round = 0; round < 3; ++round) {
    for (int i = 0; i < 5; ++i) {
      REQUIRE_TRUE(f.writer.newChange(ChangeKind_t::ALIVE, data.data(), readerGuid(1), loc(2, 7400),
                                      data.size()) != nullptr);
    }
    f.writer.progress();
    REQUIRE_TRUE(f.driver.sent.size() == static_cast<size_t>(5 * (round + 1)));
  }
  f.writer.progress();
  REQUIRE_TRUE(f.driver.sent.size() == 15);
  for (int i = 0; i < 1000; ++i) {
    REQUIRE_TRUE(f.writer.newChange(ChangeKind_t::ALIVE, data.data(), readerGuid(1), loc(2, 7400),
                                    data.size()) != nullptr);
    f.writer.progress();
  }
  REQUIRE_TRUE(f.driver.sent.size() == 1015);
  REQUIRE_TRUE(f.writer.newChange(ChangeKind_t::ALIVE, data.data(), readerGuid(1), loc(2, 7400),
                                  data.size()) != nullptr);
  f.writer.removeAllChanges();
  f.writer.progress();
  REQUIRE_TRUE(f.driver.sent.size() == 1015);
}

TEST(trn_remove_reader_of_participant) {
  Fixture f;
  auto data = pattern(8);
  Guid_t r = readerGuid(1, 0xB0);
  REQUIRE_TRUE(f.writer.newChange(ChangeKind_t::ALIVE, data.data(), r, loc(2, 7400), data.size()) !=
               nullptr);
  f.writer.removeReaderOfParticipant(r.prefix);
  f.writer.removeReader(r);
  f.writer.progress();
  REQUIRE_TRUE(f.driver.sent.size() == 1);
  REQUIRE_TRUE(f.writer.newChange(ChangeKind_t::ALIVE, data.data(), r, loc(2, 7400), data.size()) !=
               nullptr);
  f.writer.progress();
  REQUIRE_TRUE(f.driver.sent.size() == 2);
}

TEST(trn_acknack_ignored) {
  Fixture f;
  auto data = pattern(8);
  REQUIRE_TRUE(f.writer.newChange(ChangeKind_t::ALIVE, data.data(), readerGuid(1), loc(2, 7400),
                                  data.size()) != nullptr);
  f.writer.progress();
  REQUIRE_TRUE(f.driver.sent.size() == 1);
  SubmessageAckNack ack{};
  f.writer.onNewAckNack(ack, readerGuid(1).prefix);
  f.writer.setAllChangesToUnsent();
  f.writer.progress();
  REQUIRE_TRUE(f.driver.sent.size() == 1);
}

namespace {
struct Rec {
  int calls = 0;
  std::vector<uint8_t> last;
  Guid_t lastWriter{};
};
void onData(void *callee, const ReaderCacheChange &c) {
  Rec *r = static_cast<Rec *>(callee);
  ++r->calls;
  r->last.assign(c.getData(), c.getData() + c.getDataSize());
  r->lastWriter = c.writerGuid;
}
Guid_t writerGuid(uint8_t key) {
  Guid_t g{};
  g.prefix.id.fill(0xC0);
  g.entityId.entityKey[0] = key;
  g.entityId.entityKind = EntityKind_t::USER_DEFINED_WRITER_WITHOUT_KEY;
  return g;
}
}

TEST(trn_reader_callback) {
  TransientReader reader;
  reader.init(topic());
  Rec rec;
  reader.registerCallback(onData, &rec);
  auto d1 = pattern(10, 3), d2 = pattern(20, 5);
  Guid_t w = writerGuid(4);
  {
    ReaderCacheChange c(ChangeKind_t::ALIVE, w, SEQUENCENUMBER_UNKNOWN, d1.data(), d1.size());
    reader.newChange(c);
  }
  REQUIRE_TRUE(rec.calls == 1);
  REQUIRE_TRUE(rec.last == d1);
  REQUIRE_TRUE(rec.lastWriter == w);
  {
    ReaderCacheChange c(ChangeKind_t::ALIVE, w, SEQUENCENUMBER_UNKNOWN, d2.data(), d2.size());
    reader.newChange(c);
  }
  REQUIRE_TRUE(rec.calls == 2);
  REQUIRE_TRUE(rec.last == d2);
  reader.registerCallback(nullptr, nullptr);
  {
    ReaderCacheChange c(ChangeKind_t::ALIVE, w, SEQUENCENUMBER_UNKNOWN, d1.data(), d1.size());
    reader.newChange(c);
  }
  REQUIRE_TRUE(rec.calls == 3);
}

TEST(trn_reader_no_callback) {
  TransientReader reader;
  reader.init(topic());
  auto d = pattern(10);
  Guid_t w = writerGuid(1);
  ReaderCacheChange c(ChangeKind_t::ALIVE, w, SEQUENCENUMBER_UNKNOWN, d.data(), d.size());
  reader.newChange(c);
  SubmessageHeartbeat hb{};
  REQUIRE_TRUE(reader.onNewHeartbeat(hb, w.prefix));
}

TEST(trn_reader_duplicate) {
  TransientReader reader;
  reader.init(topic());
  Rec rec;
  reader.registerCallback(onData, &rec);
  auto d = pattern(10);
  Guid_t w = writerGuid(1);
  for (int i = 0; i < 2; ++i) {
    ReaderCacheChange c(ChangeKind_t::ALIVE, w, SEQUENCENUMBER_UNKNOWN, d.data(), d.size());
    reader.newChange(c);
  }
  REQUIRE_TRUE(rec.calls == 2);
}

TEST(trn_reader_remove_writer) {
  TransientReader reader;
  reader.init(topic());
  Rec rec;
  reader.registerCallback(onData, &rec);
  Guid_t w = writerGuid(1);
  REQUIRE_TRUE(!reader.addNewMatchedWriter(WriterProxy{w, loc(2, 7400)}));
  reader.removeWriter(w);
  reader.removeWriterOfParticipant(w.prefix);
  auto d = pattern(4);
  ReaderCacheChange c(ChangeKind_t::ALIVE, w, SEQUENCENUMBER_UNKNOWN, d.data(), d.size());
  reader.newChange(c);
  REQUIRE_TRUE(rec.calls == 1);
}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }
