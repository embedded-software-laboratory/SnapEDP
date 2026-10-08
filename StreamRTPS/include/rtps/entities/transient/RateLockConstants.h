
#ifndef RTPS_RATE_LOCK_CONSTANTS_H
#define RTPS_RATE_LOCK_CONSTANTS_H

#include <cstdint>

namespace rtps {

struct RateLock {
    static constexpr uint8_t  WINDOW_SIZE         = 8;
    static constexpr uint8_t  MIN_REQUIRED        = 5;
    static constexpr uint8_t  MIN_REQUIRED_HINTED = 3;
    static constexpr uint64_t VARIANCE_THRESH_MS2 = 400;
    static constexpr uint8_t  VIOLATION_FACTOR    = 3;
    static constexpr uint32_t IMPLICIT_HB_HINT_MS = 500;

   
    static inline uint8_t ringIndex(uint8_t head, uint8_t k) {
        return static_cast<uint8_t>((head + WINDOW_SIZE - 1 - k) % WINDOW_SIZE);
    }
};

} // namespace rtps

#endif // RTPS_RATE_LOCK_CONSTANTS_H
