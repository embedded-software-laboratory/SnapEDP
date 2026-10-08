// Sends periodic messages to Node B and receives responses

#include "ChainMessage.h"
#include "rtps/config/FeatureQOS.h"
#include "rtps/entities/Domain.h"
#include "rtps/rtps.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>

using namespace rtps::example;

class NodeA {
 public:
  NodeA() : m_sent(0), m_received(0) { prepareRTPS(); }

  void run() {
    std::cout << "=== Node A - Message Exchange Peer ===" << std::endl;
    std::cout << "Waiting 5 seconds for discovery..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cout << "Starting periodic message exchange with Node B..." << std::endl;

    while (true) {
      const uint32_t seq = ++m_sent;
      sendMessage(seq);
      std::this_thread::sleep_for(std::chrono::seconds(1));

      if (seq % 10 == 0) {
        std::cout << "Stats - sent: " << m_sent.load() << ", received: " << m_received.load() << std::endl;
      }
    }
  }

 private:
  rtps::Domain m_domain{rtps::FeatureQOS(
      rtps::DiscoveryMode::Standard, rtps::HeartbeatPolicyMode::AdaptiveFrequency)};
  rtps::Writer* mp_writerToB = nullptr;
  rtps::Reader* mp_readerFromB = nullptr;

  std::atomic<uint32_t> m_sent;
  std::atomic<uint32_t> m_received;

  void prepareRTPS() {
    auto* part = m_domain.createParticipant();
    if (part == nullptr) {
      std::cerr << "Node A: failed to create participant" << std::endl;
      return;
    }

    m_domain.completeInit();

    mp_writerToB = m_domain.createWriter(*part, "Exchange_A2B",
                                         "ChainMessageType", false, true);
    mp_readerFromB = m_domain.createReader(*part, "Exchange_B2A", "ChainMessageType", false);

    if (mp_writerToB == nullptr || mp_readerFromB == nullptr) {
      std::cerr << "Node A: failed to create endpoints" << std::endl;
      return;
    }

    mp_readerFromB->registerCallback(readerCallback, this);
    std::cout << "Node A initialized" << std::endl;
  }

  void sendMessage(uint32_t sequence) {
    if (mp_writerToB == nullptr) {
      return;
    }

    ChainMessage msg;
    msg.sequenceNum = sequence;
    msg.timestamp = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                              std::chrono::steady_clock::now().time_since_epoch())
                                              .count());
    std::memset(msg.data, 0, sizeof(msg.data));
    msg.data[0] = 'A';

    uint8_t buffer[ChainMessage::SIZE];
    msg.serialize(buffer);

    auto* change = mp_writerToB->newChange(rtps::ChangeKind_t::ALIVE, buffer, ChainMessage::SIZE);
    if (change == nullptr) {
      std::cerr << "Node A: failed to send message #" << sequence << std::endl;
      return;
    }

    std::cout << "Node A -> B: seq=" << sequence << std::endl;
  }

  static void readerCallback(void* callee, const rtps::ReaderCacheChange& cacheChange) {
    static_cast<NodeA*>(callee)->handleMessage(cacheChange);
  }

  void handleMessage(const rtps::ReaderCacheChange& cacheChange) {
    uint8_t buffer[ChainMessage::SIZE];
    if (!cacheChange.copyInto(buffer, ChainMessage::SIZE)) {
      std::cerr << "Node A: failed to copy received message" << std::endl;
      return;
    }

    ChainMessage msg;
    msg.deserialize(buffer);
    ++m_received;

    std::cout << "Node A <- B: seq=" << msg.sequenceNum << std::endl;
  }
};

int main() {
  rtps::init();
  NodeA nodeA;
  nodeA.run();
  return 0;
}
