// Sends messages to Node B and measures RTT

#include "ChainMessage.h"
#include "rtps/rtps.h"
#include "rtps/entities/Domain.h"

#include <iostream>
#include <chrono>
#include <vector>
#include <map>
#include <mutex>
#include <condition_variable>
#include <thread>

using namespace rtps::example;

class NodeA {
public:
    NodeA() : m_receivedResponse(false), m_totalSent(0), m_totalReceived(0) {
        prepareRTPS();
    }

    void run() {
        std::cout << "=== Node A - Chain Test Initiator ===" << std::endl;
        std::cout << "Waiting 5 seconds for discovery..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(5));
        
        std::cout << "Starting message transmission..." << std::endl;
        
        const uint32_t numMessages = 100;
        
        for (uint32_t i = 1; i <= numMessages; i++) {
            sendMessage(i);
            
            // Wait a bit between messages to avoid overwhelming the system
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        
        // Wait for remaining responses
        std::cout << "\nWaiting 10 seconds for remaining responses..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));
        
        printStatistics();
        
        // Keep running to maintain discovery
        std::cout << "\nTest complete. Press Ctrl+C to exit." << std::endl;
        while(true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

private:
    rtps::Domain m_domain;
    rtps::Writer* mp_writerToB;
    rtps::Reader* mp_readerFromB;
    
    std::mutex m_mutex;
    std::condition_variable m_condVar;
    bool m_receivedResponse;
    
    uint32_t m_totalSent;
    uint32_t m_totalReceived;
    
    // Map to track sent messages by sequenceNum with their send time
    std::map<uint32_t, std::chrono::steady_clock::time_point> m_sentMessages;
    
    // RTT measurements
    std::vector<double> m_rttMicroseconds;

    void prepareRTPS() {
        auto part = m_domain.createParticipant();
        if (part == nullptr) {
            std::cerr << "Failed to create participant" << std::endl;
            return;
        }
        
        m_domain.completeInit();
        
        mp_writerToB = m_domain.createWriter(*part, "ChainTest_A2B", "ChainMessageType", false, true);
        
        mp_readerFromB = m_domain.createReader(*part, "ChainTest_B2A", "ChainMessageType", false);
        
        if (mp_writerToB == nullptr || mp_readerFromB == nullptr) {
            std::cerr << "Failed to create endpoints" << std::endl;
            return;
        }
        
        mp_readerFromB->registerCallback(readerCallback, this);
        
        std::cout << "Node A initialized successfully" << std::endl;
    }
    
    void sendMessage(uint32_t seqNum) {
        ChainMessage msg;
        msg.sequenceNum = seqNum;
        
        auto now = std::chrono::steady_clock::now();
        auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();
        msg.timestamp = micros;
        
        // Fill data with pattern
        for (size_t i = 0; i < sizeof(msg.data); i++) {
            msg.data[i] = static_cast<uint8_t>((seqNum + i) % 256);
        }
        
        uint8_t buffer[ChainMessage::SIZE];
        msg.serialize(buffer);
        
        auto change = mp_writerToB->newChange(rtps::ChangeKind_t::ALIVE, buffer, ChainMessage::SIZE);
        if (change == nullptr) {
            std::cerr << "Failed to send message " << seqNum << std::endl;
            return;
        }
        
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_sentMessages[seqNum] = now;
            m_totalSent++;
        }
        
        std::cout << "Sent message #" << seqNum << " to Node B" << std::endl;
    }
    
    static void readerCallback(void* callee, const rtps::ReaderCacheChange& cacheChange) {
        auto nodeA = static_cast<NodeA*>(callee);
        nodeA->handleResponse(cacheChange);
    }
    
    void handleResponse(const rtps::ReaderCacheChange& cacheChange) {
        uint8_t buffer[ChainMessage::SIZE];
        
        if (!cacheChange.copyInto(buffer, ChainMessage::SIZE)) {
            std::cerr << "Failed to copy received message" << std::endl;
            return;
        }
        
        ChainMessage msg;
        msg.deserialize(buffer);
        
        auto now = std::chrono::steady_clock::now();
        
        std::lock_guard<std::mutex> lock(m_mutex);
        
        auto it = m_sentMessages.find(msg.sequenceNum);
        if (it != m_sentMessages.end()) {
            auto rtt = std::chrono::duration_cast<std::chrono::microseconds>(
                now - it->second).count();
            m_rttMicroseconds.push_back(rtt);
            
            std::cout << "Received response #" << msg.sequenceNum 
                     << " from Node B - RTT: " << rtt << " us" << std::endl;
            
            m_sentMessages.erase(it);
            m_totalReceived++;
        } else {
            std::cout << "Received unexpected message #" << msg.sequenceNum << std::endl;
        }
    }
    
    void printStatistics() {
        std::lock_guard<std::mutex> lock(m_mutex);
        
        std::cout << "\n========== FINAL STATISTICS ==========" << std::endl;
        std::cout << "Total messages sent: " << m_totalSent << std::endl;
        std::cout << "Total responses received: " << m_totalReceived << std::endl;
        std::cout << "Missing messages: " << (m_totalSent - m_totalReceived) << std::endl;
        
        if (!m_sentMessages.empty()) {
            std::cout << "\nMissing sequence numbers: ";
            for (const auto& pair : m_sentMessages) {
                std::cout << pair.first << " ";
            }
            std::cout << std::endl;
        }
        
        if (!m_rttMicroseconds.empty()) {
            double sum = 0;
            double min = m_rttMicroseconds[0];
            double max = m_rttMicroseconds[0];
            
            for (double rtt : m_rttMicroseconds) {
                sum += rtt;
                if (rtt < min) min = rtt;
                if (rtt > max) max = rtt;
            }
            
            double mean = sum / m_rttMicroseconds.size();
            
            std::cout << "\n--- RTT Statistics (microseconds) ---" << std::endl;
            std::cout << "Mean RTT: " << mean << " us" << std::endl;
            std::cout << "Min RTT: " << min << " us" << std::endl;
            std::cout << "Max RTT: " << max << " us" << std::endl;
        }
        
        std::cout << "======================================" << std::endl;
    }
};

int main() {
    rtps::init();
    
    NodeA nodeA;
    nodeA.run();
    
    return 0;
}
