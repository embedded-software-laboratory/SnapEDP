// Message type for the chain test examples

#ifndef CHAIN_MESSAGE_H
#define CHAIN_MESSAGE_H

#include <cstdint>
#include <cstring>

namespace rtps {
namespace example {

struct ChainMessage {
    uint32_t sequenceNum;    // Sequence number for tracking messages
    uint64_t timestamp;      // Timestamp from Node A in microseconds
    uint8_t data[64];        // Payload data
    
    ChainMessage() : sequenceNum(0), timestamp(0) {
        memset(data, 0, sizeof(data));
    }
    
    static constexpr size_t SIZE = sizeof(sequenceNum) + sizeof(timestamp) + sizeof(data);
    
    void serialize(uint8_t* buffer) const {
        memcpy(buffer, &sequenceNum, sizeof(sequenceNum));
        memcpy(buffer + sizeof(sequenceNum), &timestamp, sizeof(timestamp));
        memcpy(buffer + sizeof(sequenceNum) + sizeof(timestamp), data, sizeof(data));
    }
    
    void deserialize(const uint8_t* buffer) {
        memcpy(&sequenceNum, buffer, sizeof(sequenceNum));
        memcpy(&timestamp, buffer + sizeof(sequenceNum), sizeof(timestamp));
        memcpy(data, buffer + sizeof(sequenceNum) + sizeof(timestamp), sizeof(data));
    }
};

} // namespace example
} // namespace rtps

#endif // CHAIN_MESSAGE_H
