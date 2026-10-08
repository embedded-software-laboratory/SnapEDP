#include "gedp_test_util.h"
#include "harness.h"

using namespace gedp_test;

namespace {

std::vector<uint8_t> serialize(const TopicData &td) {
  std::vector<uint8_t> store;
  ucdrBuffer b = writer(store);
  REQUIRE_TRUE(td.serializeIntoUcdrBuffer(b));
  store.resize(used(b));
  return store;
}

void putPid(std::vector<uint8_t> &out, uint16_t pid, uint16_t len,
            const std::vector<uint8_t> &payload) {
  out.push_back(pid & 0xff);
  out.push_back(pid >> 8);
  out.push_back(len & 0xff);
  out.push_back(len >> 8);
  out.insert(out.end(), payload.begin(), payload.end());
  while (out.size() % 4) out.push_back(0);
}

std::vector<uint8_t> strPayload(uint32_t declaredLen, size_t bytes) {
  std::vector<uint8_t> p(4 + bytes, 'a');
  std::memcpy(p.data(), &declaredLen, 4);
  if (bytes) p.back() = 0;
  return p;
}

}

TEST(topic_roundtrip) {
  for (bool reader : {false, true}) {
    for (bool reliable : {false, true}) {
      TopicData in = makeTopic(prefixOf(0x10), 3, "chatter", "std_msgs::String",
                               reader, reliable);
      auto bytes = serialize(in);
      Exact e(bytes.data(), bytes.size());
      ucdrBuffer b = e.buf();
      TopicData out;
      REQUIRE_TRUE(out.readFromUcdrBuffer(b));
      REQUIRE_TRUE(sameTopic(in, out));
    }
  }
}

TEST(topic_compressed_roundtrip) {
  TopicData in = makeTopic(prefixOf(0x20), 1, "scan", "sensor_msgs::LaserScan");
  in.deadlineMs = 250;
  in.multicastLocator = Locator::createUDPv4Locator(239, 255, 0, 1, 7400);
  TopicDataCompressed c(in);
  REQUIRE_TRUE(c.endpointGuid == in.endpointGuid);
  REQUIRE_TRUE(c.reliabilityKind == in.reliabilityKind);
  REQUIRE_TRUE(c.durabilityKind == in.durabilityKind);
  REQUIRE_TRUE(c.unicastLocator == in.unicastLocator);
  REQUIRE_TRUE(c.multicastLocator == in.multicastLocator);
  REQUIRE_TRUE(c.deadlineMs == 250);
  REQUIRE_TRUE(c.matchesTopicOf(in));
  TopicData other = in;
  other.topicName[0] = 'X';
  REQUIRE_TRUE(!c.matchesTopicOf(other));
  other = in;
  other.typeName[0] = 'X';
  REQUIRE_TRUE(!c.matchesTopicOf(other));
}

TEST(topic_truncated) {
  TopicData in = makeTopic(prefixOf(0x30), 2, "truncate_me", "T");
  auto bytes = serialize(in);
  size_t failures = 0;
  for (size_t n = 0; n < bytes.size(); ++n) {
    Exact e(bytes.data(), n);
    ucdrBuffer b = e.buf();
    TopicData out;
    if (!out.readFromUcdrBuffer(b)) ++failures;
  }
  REQUIRE_TRUE(failures > bytes.size() / 2);
  Exact full(bytes.data(), bytes.size());
  ucdrBuffer b = full.buf();
  TopicData out;
  REQUIRE_TRUE(out.readFromUcdrBuffer(b));
}

TEST(topic_oversized_name) {
  for (uint32_t len : {uint32_t(Config::MAX_TOPICNAME_LENGTH),
                       uint32_t(Config::MAX_TOPICNAME_LENGTH) + 1, 200u,
                       0xFFFFFFFFu}) {
    std::vector<uint8_t> w;
    size_t payload = std::min<size_t>(len, 200);
    putPid(w, uint16_t(SMElement::ParameterId::PID_TOPIC_NAME),
           uint16_t(4 + payload), strPayload(len, payload));
    putPid(w, uint16_t(SMElement::ParameterId::PID_SENTINEL), 0, {});
    Exact e(w.data(), w.size());
    ucdrBuffer b = e.buf();
    TopicData out;
    REQUIRE_TRUE(!out.readFromUcdrBuffer(b));
    REQUIRE_TRUE(out.topicName[0] == '\0');
  }
  std::vector<uint8_t> w;
  putPid(w, uint16_t(SMElement::ParameterId::PID_TYPE_NAME), 4 + 100,
         strPayload(100, 100));
  putPid(w, uint16_t(SMElement::ParameterId::PID_SENTINEL), 0, {});
  Exact e(w.data(), w.size());
  ucdrBuffer b = e.buf();
  TopicData out;
  REQUIRE_TRUE(!out.readFromUcdrBuffer(b));
  REQUIRE_TRUE(out.typeName[0] == '\0');

  TopicData ok = makeTopic(prefixOf(1), 1, std::string(Config::MAX_TOPICNAME_LENGTH - 2, 'n'),
                           std::string(Config::MAX_TYPENAME_LENGTH - 2, 't'));
  auto bytes = serialize(ok);
  Exact e2(bytes.data(), bytes.size());
  ucdrBuffer b2 = e2.buf();
  TopicData out2;
  REQUIRE_TRUE(out2.readFromUcdrBuffer(b2));
  REQUIRE_TRUE(sameTopic(ok, out2));

  TopicData edge = makeTopic(prefixOf(1), 1, std::string(Config::MAX_TOPICNAME_LENGTH - 1, 'n'), "T");
  auto eb = serialize(edge);
  Exact e3(eb.data(), eb.size());
  ucdrBuffer b3 = e3.buf();
  TopicData out3;
  REQUIRE_TRUE(!out3.readFromUcdrBuffer(b3));
}

TEST(topic_unknown_pid_skipped) {
  TopicData in = makeTopic(prefixOf(0x40), 5, "fwd", "Compat");
  auto good = serialize(in);
  std::vector<uint8_t> w;
  putPid(w, 0x7abc, 8, std::vector<uint8_t>(8, 0xEE));
  w.insert(w.end(), good.begin(), good.end());
  Exact e(w.data(), w.size());
  ucdrBuffer b = e.buf();
  TopicData out;
  REQUIRE_TRUE(out.readFromUcdrBuffer(b));
  REQUIRE_TRUE(sameTopic(in, out));
}

TEST(topic_cursor_overrun_tail) {
  {
    std::vector<uint8_t> w;
    putPid(w, uint16_t(SMElement::ParameterId::PID_RELIABILITY), 4,
           std::vector<uint8_t>{2, 0, 0, 0});
    Exact e(w.data(), w.size());
    ucdrBuffer b = e.buf();
    TopicData out;
    (void)out.readFromUcdrBuffer(b);
    REQUIRE_TRUE(ucdr_buffer_length(&b) <= w.size());
  }
  {
    std::vector<uint8_t> w = {0xbc, 0x7a, 0x01, 0x00, 0xEE};
    Exact e(w.data(), w.size());
    ucdrBuffer b = e.buf();
    TopicData out;
    (void)out.readFromUcdrBuffer(b);
    REQUIRE_TRUE(ucdr_buffer_length(&b) <= w.size());
  }
}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }
