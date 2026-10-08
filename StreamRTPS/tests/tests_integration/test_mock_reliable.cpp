#include "rtps/entities/Domain.h"
#include "rtps/communication/MockNetworkRouter.h"
#include "rtps/config/FeatureQOS.h"
#include "rtps/rtps.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>

using namespace rtps;

static constexpr size_t PAYLOAD_SIZE = 64;

struct TestPayload {
  uint32_t sequence;
  uint8_t padding[PAYLOAD_SIZE - sizeof(uint32_t)];

  void serialize(uint8_t *buf) const { std::memcpy(buf, this, PAYLOAD_SIZE); }
  void deserialize(const uint8_t *buf) { std::memcpy(this, buf, PAYLOAD_SIZE); }
};

static bool waitFor(std::function<bool()> cond, int timeoutMs = 10000,
                    int pollMs = 50) {
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  while (std::chrono::steady_clock::now() < deadline) {
    if (cond()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(pollMs));
  }
  return cond();
}

struct RxContext {
  std::atomic<uint32_t> count{0};
  uint32_t lastSeq = 0;
};

static void countingCallback(void *callee,
                             const ReaderCacheChange & ) {
  static_cast<std::atomic<uint32_t> *>(callee)->fetch_add(1);
}

static void trackingCallback(void *callee,
                              const ReaderCacheChange &change) {
  auto *ctx = static_cast<RxContext *>(callee);
  uint8_t buf[PAYLOAD_SIZE];
  if (change.copyInto(buf, PAYLOAD_SIZE)) {
    TestPayload payload;
    payload.deserialize(buf);
    ctx->lastSeq = payload.sequence;
  }
  ctx->count.fetch_add(1);
}

void test_basic_reliable_delivery() {
  std::cout << "[test] basic reliable delivery between two domains..."
            << std::endl;

  MockNetworkRouter::instance().clearDropPolicy();

  Domain domainA(FeatureQOS(DiscoveryMode::Standard,
                            HeartbeatPolicyMode::AdaptiveFrequency));
  Domain domainB(FeatureQOS(DiscoveryMode::Standard,
                            HeartbeatPolicyMode::AdaptiveFrequency));

  auto *partA = domainA.createParticipant();
  auto *partB = domainB.createParticipant();
  assert(partA != nullptr);
  assert(partB != nullptr);

  domainA.completeInit();
  domainB.completeInit();

  auto *writer =
      domainA.createWriter(*partA, "TestTopic", "TestType", true, true);
  auto *reader =
      domainB.createReader(*partB, "TestTopic", "TestType", true);
  assert(writer != nullptr);
  assert(reader != nullptr);

  RxContext rxCtx;
  reader->registerCallback(trackingCallback, &rxCtx);

  std::cout << "  Waiting for discovery..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(6));

  TestPayload payload{};
  payload.sequence = 42;
  uint8_t buf[PAYLOAD_SIZE];
  payload.serialize(buf);
  auto *change = writer->newChange(ChangeKind_t::ALIVE, buf, PAYLOAD_SIZE);
  assert(change != nullptr);

  bool ok = waitFor([&] { return rxCtx.count.load() > 0; }, 10000);
  std::cout << "  Received: " << rxCtx.count.load()
            << ", lastSeq: " << rxCtx.lastSeq << std::endl;
  assert(ok);
  assert(rxCtx.lastSeq == 42);

  std::cout << "  PASSED" << std::endl;
}

void test_multiple_messages() {
  std::cout << "[test] multiple reliable messages..." << std::endl;

  MockNetworkRouter::instance().clearDropPolicy();

  Domain domainA(FeatureQOS(DiscoveryMode::Standard,
                            HeartbeatPolicyMode::AdaptiveFrequency));
  Domain domainB(FeatureQOS(DiscoveryMode::Standard,
                            HeartbeatPolicyMode::AdaptiveFrequency));

  auto *partA = domainA.createParticipant();
  auto *partB = domainB.createParticipant();
  assert(partA != nullptr);
  assert(partB != nullptr);

  domainA.completeInit();
  domainB.completeInit();

  auto *writer =
      domainA.createWriter(*partA, "MultiTopic", "TestType", true, true);
  auto *reader =
      domainB.createReader(*partB, "MultiTopic", "TestType", true);
  assert(writer != nullptr);
  assert(reader != nullptr);

  std::atomic<uint32_t> received{0};
  reader->registerCallback(countingCallback, &received);

  std::cout << "  Waiting for discovery..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(6));

  constexpr uint32_t NUM_MESSAGES = 10;
  for (uint32_t i = 0; i < NUM_MESSAGES; ++i) {
    TestPayload payload{};
    payload.sequence = i + 1;
    uint8_t buf[PAYLOAD_SIZE];
    payload.serialize(buf);
    writer->newChange(ChangeKind_t::ALIVE, buf, PAYLOAD_SIZE);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  bool ok = waitFor([&] { return received.load() >= NUM_MESSAGES; }, 15000);
  std::cout << "  Received " << received.load() << "/" << NUM_MESSAGES
            << std::endl;
  assert(ok);

  std::cout << "  PASSED" << std::endl;
}

void test_reliable_with_packet_loss() {
  std::cout << "[test] reliable delivery with 30% packet loss..." << std::endl;

  MockNetworkRouter::instance().clearDropPolicy();

  Domain domainA(FeatureQOS(DiscoveryMode::Standard,
                            HeartbeatPolicyMode::AdaptiveFrequency));
  Domain domainB(FeatureQOS(DiscoveryMode::Standard,
                            HeartbeatPolicyMode::AdaptiveFrequency));

  auto *partA = domainA.createParticipant();
  auto *partB = domainB.createParticipant();
  assert(partA != nullptr);
  assert(partB != nullptr);

  domainA.completeInit();
  domainB.completeInit();

  auto *writer =
      domainA.createWriter(*partA, "LossyTopic", "TestType", true, true);
  auto *reader =
      domainB.createReader(*partB, "LossyTopic", "TestType", true);
  assert(writer != nullptr);
  assert(reader != nullptr);

  std::atomic<uint32_t> received{0};
  reader->registerCallback(countingCallback, &received);

  std::cout << "  Waiting for discovery..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(6));

  MockNetworkRouter::instance().setDropRate(0.3);

  constexpr uint32_t NUM_MESSAGES = 20;
  for (uint32_t i = 0; i < NUM_MESSAGES; ++i) {
    TestPayload payload{};
    payload.sequence = i + 1;
    uint8_t buf[PAYLOAD_SIZE];
    payload.serialize(buf);
    writer->newChange(ChangeKind_t::ALIVE, buf, PAYLOAD_SIZE);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  bool ok = waitFor([&] { return received.load() >= NUM_MESSAGES; }, 30000);
  std::cout << "  Received " << received.load() << "/" << NUM_MESSAGES
            << std::endl;

  MockNetworkRouter::instance().clearDropPolicy();

  assert(ok);
  std::cout << "  PASSED" << std::endl;
}

void test_selective_drop_filter() {
  std::cout << "[test] selective drop filter (every 3rd packet)..." << std::endl;

  MockNetworkRouter::instance().clearDropPolicy();

  Domain domainA(FeatureQOS(DiscoveryMode::Standard,
                            HeartbeatPolicyMode::AdaptiveFrequency));
  Domain domainB(FeatureQOS(DiscoveryMode::Standard,
                            HeartbeatPolicyMode::AdaptiveFrequency));

  auto *partA = domainA.createParticipant();
  auto *partB = domainB.createParticipant();
  assert(partA != nullptr);
  assert(partB != nullptr);

  domainA.completeInit();
  domainB.completeInit();

  auto *writer =
      domainA.createWriter(*partA, "FilterTopic", "TestType", true, true);
  auto *reader =
      domainB.createReader(*partB, "FilterTopic", "TestType", true);
  assert(writer != nullptr);
  assert(reader != nullptr);

  std::atomic<uint32_t> received{0};
  reader->registerCallback(countingCallback, &received);

  std::cout << "  Waiting for discovery..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(6));

  std::atomic<int> filterCallCount{0};
  MockNetworkRouter::instance().setDropFilter(
      [&filterCallCount](MockNetworkDriver *, MockNetworkDriver *,
                         const PacketInfo &info) -> bool {
        if (info.buffer.m_buf.size() <= 80) {
          int n = filterCallCount.fetch_add(1);
          return (n % 3) == 0;
        }
        return false;
      });

  constexpr uint32_t NUM_MESSAGES = 15;
  for (uint32_t i = 0; i < NUM_MESSAGES; ++i) {
    TestPayload payload{};
    payload.sequence = i + 1;
    uint8_t buf[PAYLOAD_SIZE];
    payload.serialize(buf);
    writer->newChange(ChangeKind_t::ALIVE, buf, PAYLOAD_SIZE);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  bool ok = waitFor([&] { return received.load() >= NUM_MESSAGES; }, 30000);
  std::cout << "  Received " << received.load() << "/" << NUM_MESSAGES
            << ", filterCalls=" << filterCallCount.load() << std::endl;

  MockNetworkRouter::instance().clearDropPolicy();

  assert(ok);
  std::cout << "  PASSED" << std::endl;
}

void test_bidirectional_exchange() {
  std::cout << "[test] bidirectional reliable exchange..." << std::endl;

  MockNetworkRouter::instance().clearDropPolicy();

  Domain domainA(FeatureQOS(DiscoveryMode::Standard,
                            HeartbeatPolicyMode::AdaptiveFrequency));
  Domain domainB(FeatureQOS(DiscoveryMode::Standard,
                            HeartbeatPolicyMode::AdaptiveFrequency));

  auto *partA = domainA.createParticipant();
  auto *partB = domainB.createParticipant();
  assert(partA != nullptr);
  assert(partB != nullptr);

  domainA.completeInit();
  domainB.completeInit();

  auto *writerAB =
      domainA.createWriter(*partA, "TopicAB", "TestType", true, true);
  auto *readerAB =
      domainB.createReader(*partB, "TopicAB", "TestType", true);
  auto *writerBA =
      domainB.createWriter(*partB, "TopicBA", "TestType", true, true);
  auto *readerBA =
      domainA.createReader(*partA, "TopicBA", "TestType", true);

  assert(writerAB && readerAB && writerBA && readerBA);

  std::atomic<uint32_t> receivedOnB{0};
  std::atomic<uint32_t> receivedOnA{0};

  readerAB->registerCallback(countingCallback, &receivedOnB);
  readerBA->registerCallback(countingCallback, &receivedOnA);

  std::cout << "  Waiting for discovery..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(6));

  constexpr uint32_t NUM_MESSAGES = 5;
  for (uint32_t i = 0; i < NUM_MESSAGES; ++i) {
    TestPayload payload{};
    payload.sequence = i + 1;
    uint8_t buf[PAYLOAD_SIZE];
    payload.serialize(buf);
    writerAB->newChange(ChangeKind_t::ALIVE, buf, PAYLOAD_SIZE);
    writerBA->newChange(ChangeKind_t::ALIVE, buf, PAYLOAD_SIZE);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  bool okB =
      waitFor([&] { return receivedOnB.load() >= NUM_MESSAGES; }, 15000);
  bool okA =
      waitFor([&] { return receivedOnA.load() >= NUM_MESSAGES; }, 15000);

  std::cout << "  A->B received " << receivedOnB.load() << "/" << NUM_MESSAGES
            << std::endl;
  std::cout << "  B->A received " << receivedOnA.load() << "/" << NUM_MESSAGES
            << std::endl;

  assert(okB);
  assert(okA);
  std::cout << "  PASSED" << std::endl;
}

namespace {
struct NamedTest {
  const char *name;
  void (*fn)();
};

constexpr NamedTest kTests[] = {
    {"basic_reliable_delivery", &test_basic_reliable_delivery},
    {"multiple_messages", &test_multiple_messages},
    {"reliable_with_packet_loss", &test_reliable_with_packet_loss},
    {"selective_drop_filter", &test_selective_drop_filter},
    {"bidirectional_exchange", &test_bidirectional_exchange},
};

void run_all() {
  for (const auto &t : kTests) {
    t.fn();
  }
}

void print_usage(const char *prog) {
  std::cerr << "Usage: " << prog << " [all|";
  for (size_t i = 0; i < sizeof(kTests) / sizeof(kTests[0]); ++i) {
    std::cerr << kTests[i].name;
    if (i + 1 < sizeof(kTests) / sizeof(kTests[0])) std::cerr << '|';
  }
  std::cerr << "]\n";
}
}

int main(int argc, char **argv) {
  rtps::init();

  if (argc < 2 || std::strcmp(argv[1], "all") == 0) {
    std::cout << "Running mock network integration tests..." << std::endl;
    run_all();
    std::cout << "All integration tests passed." << std::endl;
    return 0;
  }

  for (const auto &t : kTests) {
    if (std::strcmp(argv[1], t.name) == 0) {
      t.fn();
      return 0;
    }
  }

  print_usage(argv[0]);
  return 1;
}
