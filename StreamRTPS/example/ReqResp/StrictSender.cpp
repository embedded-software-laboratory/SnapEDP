#include "ChainMessage.h"
#include "rtps/config/FeatureQOS.h"
#include "rtps/entities/Domain.h"
#include "rtps/rtps.h"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <condition_variable>

using namespace rtps::example;

namespace {
constexpr uint32_t kNumSamples = 200;
constexpr std::chrono::seconds kDiscoveryTimeout{20};
constexpr std::chrono::seconds kReceiveTimeout{30};
constexpr std::chrono::milliseconds kSendInterval{5};

void fill_payload(uint8_t data[64], char source_tag, uint32_t seq) {
  for (uint32_t i = 0; i < 64; ++i) {
    data[i] = static_cast<uint8_t>((static_cast<uint32_t>(source_tag) * 31u + seq * 17u + i * 7u) & 0xFFu);
  }
}

bool verify_payload(const uint8_t data[64], char source_tag, uint32_t seq) {
  uint8_t expected[64];
  fill_payload(expected, source_tag, seq);
  return std::memcmp(data, expected, sizeof(expected)) == 0;
}
} // namespace

class StrictSender {
 public:
  int run() {
    if (!prepareRTPS()) {
      return 1;
    }

    std::cout << "[StrictSender] Waiting for discovery/stream setup..." << std::endl;
    std::this_thread::sleep_for(kDiscoveryTimeout);

    for (uint32_t seq = 1; seq <= kNumSamples; ++seq) {
      if (hasFailed()) {
        break;
      }
      if (!sendMessage(seq)) {
        break;
      }
      std::this_thread::sleep_for(kSendInterval);
    }

    if (hasFailed()) {
      return 1;
    }

    if (!waitForReception()) {
      return 1;
    }

    {
      std::lock_guard<std::mutex> lock(m_mutex);
      if (m_sent != kNumSamples || m_received != kNumSamples) {
        std::ostringstream oss;
        oss << "completeness mismatch sent=" << m_sent << " received=" << m_received
            << " expected=" << kNumSamples;
        setFailureLocked(oss.str());
      }
    }

    if (hasFailed()) {
      return 1;
    }

    std::cout << "TEST_PASS" << std::endl;
    return 0;
  }

 private:
  rtps::Domain m_domain{rtps::FeatureQOS(
      rtps::DiscoveryMode::Snap, rtps::HeartbeatPolicyMode::AdaptiveFrequency)};
  rtps::Writer *m_writer = nullptr;
  rtps::Reader *m_reader = nullptr;

  std::mutex m_mutex;
  std::condition_variable m_cv;
  bool m_failed = false;
  std::string m_failReason;
  uint32_t m_sent = 0;
  uint32_t m_received = 0;
  uint32_t m_expectedSeq = 1;

  bool prepareRTPS() {
    auto *part = m_domain.createParticipant();
    if (part == nullptr) {
      fail("failed to create participant");
      return false;
    }

    m_domain.completeInit();

    m_writer = m_domain.createWriter(*part, "Exchange_A2B", "ChainMessageType", false, true);
    m_reader = m_domain.createReader(*part, "Exchange_B2A", "ChainMessageType", false);

    if (m_writer == nullptr || m_reader == nullptr) {
      fail("failed to create endpoints");
      return false;
    }

    m_reader->registerCallback(&StrictSender::readerCallback, this);
    return true;
  }

  bool sendMessage(uint32_t seq) {
    ChainMessage msg;
    msg.sequenceNum = seq;
    msg.timestamp = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch())
        .count());
    fill_payload(msg.data, 'S', seq);

    uint8_t buffer[ChainMessage::SIZE];
    msg.serialize(buffer);

    auto *change = m_writer->newChange(rtps::ChangeKind_t::ALIVE, buffer, ChainMessage::SIZE);
    if (change == nullptr) {
      std::ostringstream oss;
      oss << "newChange failed for seq=" << seq;
      fail(oss.str());
      return false;
    }

    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_sent = seq;
    }
    return true;
  }

  bool waitForReception() {
    std::unique_lock<std::mutex> lock(m_mutex);
    const auto deadline = std::chrono::steady_clock::now() + kReceiveTimeout;
    const bool done = m_cv.wait_until(lock, deadline, [&]() { return m_failed || m_received >= kNumSamples; });

    if (!done && !m_failed) {
      setFailureLocked("timeout waiting for all samples");
      return false;
    }
    return !m_failed;
  }

  static void readerCallback(void *callee, const rtps::ReaderCacheChange &cacheChange) {
    static_cast<StrictSender *>(callee)->handleMessage(cacheChange);
  }

  void handleMessage(const rtps::ReaderCacheChange &cacheChange) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_failed) {
      return;
    }

    if (cacheChange.size != ChainMessage::SIZE) {
      std::ostringstream oss;
      oss << "size mismatch size=" << cacheChange.size << " expected=" << ChainMessage::SIZE;
      setFailureLocked(oss.str());
      m_cv.notify_all();
      return;
    }

    if (cacheChange.sn.high != 0 || cacheChange.sn.low != 0) {
      std::ostringstream oss;
      oss << "stream evidence missing sn=(" << cacheChange.sn.high << "," << cacheChange.sn.low << ")";
      setFailureLocked(oss.str());
      m_cv.notify_all();
      return;
    }

    uint8_t buffer[ChainMessage::SIZE];
    if (!cacheChange.copyInto(buffer, ChainMessage::SIZE)) {
      setFailureLocked("copyInto failed");
      m_cv.notify_all();
      return;
    }

    ChainMessage msg;
    msg.deserialize(buffer);

    if (msg.sequenceNum != m_expectedSeq) {
      std::ostringstream oss;
      oss << "sequence mismatch expected=" << m_expectedSeq << " got=" << msg.sequenceNum;
      setFailureLocked(oss.str());
      m_cv.notify_all();
      return;
    }

    if (!verify_payload(msg.data, 'R', msg.sequenceNum)) {
      std::ostringstream oss;
      oss << "payload mismatch seq=" << msg.sequenceNum;
      setFailureLocked(oss.str());
      m_cv.notify_all();
      return;
    }

    ++m_expectedSeq;
    ++m_received;
    m_cv.notify_all();
  }

  bool hasFailed() {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_failed;
  }

  void fail(const std::string &reason) {
    std::lock_guard<std::mutex> lock(m_mutex);
    setFailureLocked(reason);
    m_cv.notify_all();
  }

  void setFailureLocked(const std::string &reason) {
    if (!m_failed) {
      m_failed = true;
      m_failReason = reason;
      std::cout << "TEST_FAIL: " << reason << std::endl;
    }
  }
};

int main() {
  rtps::init();
  StrictSender app;
  return app.run();
}
