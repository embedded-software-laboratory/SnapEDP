// Echoes messages back to Node B, the chain endpoint

#include "ChainMessage.h"
#include "rtps/rtps.h"
#include "rtps/entities/Domain.h"

#include <iostream>
#include <thread>
#include <atomic>

using namespace rtps::example;

class NodeC {
public:
    NodeC() : m_messagesReceived(0), m_responsesSent(0) {
        prepareRTPS();
    }

    void run() {
        std::cout << "=== Node C - Chain Test Responder ===" << std::endl;
        std::cout << "Waiting for messages from Node B..." << std::endl;
        
        // Just run and respond to messages
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            
            // Periodically print statistics
            static int counter = 0;
            if (++counter % 10 == 0) {
                std::cout << "Stats - Messages received: " << m_messagesReceived
                         << ", Responses sent: " << m_responsesSent << std::endl;
            }
        }
    }

private:
    rtps::Domain m_domain;
    rtps::Writer* mp_writerToB;
    rtps::Reader* mp_readerFromB;
    
    std::atomic<uint32_t> m_messagesReceived;
    std::atomic<uint32_t> m_responsesSent;

    void prepareRTPS() {
        auto part = m_domain.createParticipant();
        if (part == nullptr) {
            std::cerr << "Failed to create participant" << std::endl;
            return;
        }
        
        m_domain.completeInit();
        
        mp_readerFromB = m_domain.createReader(*part, "ChainTest_B2C", "ChainMessageType", false);
        
        mp_writerToB = m_domain.createWriter(*part, "ChainTest_C2B", "ChainMessageType", false);
        
        if (mp_readerFromB == nullptr || mp_writerToB == nullptr) {
            std::cerr << "Failed to create endpoints" << std::endl;
            return;
        }
        
        mp_readerFromB->registerCallback(readerCallback, this);
        
        std::cout << "Node C initialized successfully" << std::endl;
        std::cout << "Ready to respond to messages from B" << std::endl;
    }
    
    static void readerCallback(void* callee, const rtps::ReaderCacheChange& cacheChange) {
        auto nodeC = static_cast<NodeC*>(callee);
        nodeC->handleMessage(cacheChange);
    }
    
    void handleMessage(const rtps::ReaderCacheChange& cacheChange) {
        uint8_t buffer[ChainMessage::SIZE];
        
        if (!cacheChange.copyInto(buffer, ChainMessage::SIZE)) {
            std::cerr << "Failed to copy received message" << std::endl;
            return;
        }
        
        ChainMessage msg;
        msg.deserialize(buffer);
        
        std::cout << "Received message #" << msg.sequenceNum 
                 << " from B, sending response back" << std::endl;
        
        m_messagesReceived++;
        
        // echo the same message back, it still carries the original timestamp of Node A
        auto change = mp_writerToB->newChange(rtps::ChangeKind_t::ALIVE, buffer, ChainMessage::SIZE);
        if (change == nullptr) {
            std::cerr << "Failed to send response to B" << std::endl;
            return;
        }
        
        m_responsesSent++;
    }
};

int main() {
    rtps::init();
    
    NodeC nodeC;
    nodeC.run();
    
    return 0;
}
