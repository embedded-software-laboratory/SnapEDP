// Forwards messages between Node A and Node C

#include "ChainMessage.h"
#include "rtps/rtps.h"
#include "rtps/entities/Domain.h"

#include <iostream>
#include <thread>
#include <atomic>

using namespace rtps::example;

class NodeB {
public:
    NodeB() : m_messagesForwarded(0), m_responsesForwarded(0) {
        prepareRTPS();
    }

    void run() {
        std::cout << "=== Node B - Chain Test Forwarder ===" << std::endl;
        std::cout << "Waiting for messages..." << std::endl;
        
        // Just run and forward messages
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            
            // Periodically print statistics
            static int counter = 0;
            if (++counter % 10 == 0) {
                std::cout << "Stats - Messages forwarded to C: " << m_messagesForwarded
                         << ", Responses forwarded to A: " << m_responsesForwarded << std::endl;
            }
        }
    }

private:
    rtps::Domain m_domain;
    rtps::Writer* mp_writerToC;
    rtps::Writer* mp_writerToA;
    rtps::Reader* mp_readerFromA;
    rtps::Reader* mp_readerFromC;
    
    std::atomic<uint32_t> m_messagesForwarded;
    std::atomic<uint32_t> m_responsesForwarded;

    void prepareRTPS() {
        auto part = m_domain.createParticipant();
        if (part == nullptr) {
            std::cerr << "Failed to create participant" << std::endl;
            return;
        }
        
        m_domain.completeInit();
        
        mp_readerFromA = m_domain.createReader(*part, "ChainTest_A2B", "ChainMessageType", false);
        
        mp_writerToC = m_domain.createWriter(*part, "ChainTest_B2C", "ChainMessageType", false);
        
        mp_readerFromC = m_domain.createReader(*part, "ChainTest_C2B", "ChainMessageType", false);
        
        mp_writerToA = m_domain.createWriter(*part, "ChainTest_B2A", "ChainMessageType", false);
        
        if (mp_readerFromA == nullptr || mp_writerToC == nullptr ||
            mp_readerFromC == nullptr || mp_writerToA == nullptr) {
            std::cerr << "Failed to create endpoints" << std::endl;
            return;
        }
        
        mp_readerFromA->registerCallback(callbackFromA, this);
        mp_readerFromC->registerCallback(callbackFromC, this);
        
        std::cout << "Node B initialized successfully" << std::endl;
        std::cout << "Ready to forward messages A->C and C->A" << std::endl;
    }
    
    static void callbackFromA(void* callee, const rtps::ReaderCacheChange& cacheChange) {
        auto nodeB = static_cast<NodeB*>(callee);
        nodeB->handleMessageFromA(cacheChange);
    }
    
    static void callbackFromC(void* callee, const rtps::ReaderCacheChange& cacheChange) {
        auto nodeB = static_cast<NodeB*>(callee);
        nodeB->handleMessageFromC(cacheChange);
    }
    
    void handleMessageFromA(const rtps::ReaderCacheChange& cacheChange) {
        uint8_t buffer[ChainMessage::SIZE];
        
        if (!cacheChange.copyInto(buffer, ChainMessage::SIZE)) {
            std::cerr << "Failed to copy message from A" << std::endl;
            return;
        }
        
        ChainMessage msg;
        msg.deserialize(buffer);
        
        std::cout << "Received message #" << msg.sequenceNum 
                 << " from A, forwarding to C" << std::endl;
        
        // Forward to Node C
        auto change = mp_writerToC->newChange(rtps::ChangeKind_t::ALIVE, buffer, ChainMessage::SIZE);
        if (change == nullptr) {
            std::cerr << "Failed to forward message to C" << std::endl;
            return;
        }
        
        m_messagesForwarded++;
    }
    
    void handleMessageFromC(const rtps::ReaderCacheChange& cacheChange) {
        uint8_t buffer[ChainMessage::SIZE];
        
        if (!cacheChange.copyInto(buffer, ChainMessage::SIZE)) {
            std::cerr << "Failed to copy message from C" << std::endl;
            return;
        }
        
        ChainMessage msg;
        msg.deserialize(buffer);
        
        std::cout << "Received response #" << msg.sequenceNum 
                 << " from C, forwarding to A" << std::endl;
        
        // Forward back to Node A
        auto change = mp_writerToA->newChange(rtps::ChangeKind_t::ALIVE, buffer, ChainMessage::SIZE);
        if (change == nullptr) {
            std::cerr << "Failed to forward response to A" << std::endl;
            return;
        }
        
        m_responsesForwarded++;
    }
};

int main() {
    rtps::init();
    
    NodeB nodeB;
    nodeB.run();
    
    return 0;
}
