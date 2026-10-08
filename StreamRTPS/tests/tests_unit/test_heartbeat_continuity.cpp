#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>

#include "rtps/entities/transient/HeartbeatPolicy.h"
#include "rtps/entities/transient/AckNackPolicy.h"
#include "rtps/entities/transient/RateLockConstants.h"
#include "rtps/common/types.h"
#include "rtps/config.h"
#include "rtps/utils/sysFunctions.h"

using namespace rtps;

namespace {

void test_first_send_then_period_gate() {
    std::printf("  test_first_send_then_period_gate...");

    HeartbeatPolicy pol;
    assert(pol.shouldSend() && "First heartbeat must fire immediately");

    pol.onHeartbeatTransmitted();
    assert(!pol.shouldSend() &&
           "Right after a transmit the base period gates the next send");

    std::printf(" PASS\n");
}

void test_adapts_toward_sample_rate() {
    std::printf("  test_adapts_toward_sample_rate...");

    HeartbeatPolicy pol;
    const uint32_t base = pol.getCurrentPeriodMs();
    for (uint32_t i = 0; i < 8; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        pol.onNewChange();
    }
    assert(pol.getCurrentPeriodMs() < base &&
           "Steady 10 ms traffic must shrink the period below the base");

    std::printf(" PASS\n");
}

void test_period_floor_holds() {
    std::printf("  test_period_floor_holds...");

    HeartbeatPolicy pol;
    for (uint32_t i = 0; i < 40; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        pol.onNewChange();
    }
    assert(pol.getCurrentPeriodMs() >= 25 &&
           "Period must never shrink below the 25 ms flood floor");

    std::printf(" PASS\n");
}

void test_nack_resets_to_base() {
    std::printf("  test_nack_resets_to_base...");

    HeartbeatPolicy pol;
    for (uint32_t i = 0; i < 8; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        pol.onNewChange();
    }
    const uint32_t shrunk = pol.getCurrentPeriodMs();
    assert(shrunk < Config::SF_WRITER_HB_PERIOD_MS);

    pol.onAckNack(true);
    assert(pol.getCurrentPeriodMs() == Config::SF_WRITER_HB_PERIOD_MS &&
           "NACK must reset the period to the base cadence");

    pol.onAckNack(false);
    assert(pol.getCurrentPeriodMs() == Config::SF_WRITER_HB_PERIOD_MS &&
           "A clean ACK must not change the period");

    std::printf(" PASS\n");
}

void test_acknack_always_responds() {
    std::printf("  test_acknack_always_responds...");

    AckNackPolicy pol;

    pol.onHeartbeat(0);
    assert(pol.shouldSendAckNack() && "Should always ACK on heartbeat");

    pol.onHeartbeat(3);
    assert(pol.shouldSendAckNack() && "Should always ACK on heartbeat with missing");

    pol.onDataReceived();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    pol.onHeartbeat(0);
    assert(pol.shouldSendAckNack() && "Should always ACK regardless of timing");

    std::printf(" PASS\n");
}

void test_acknack_no_proactive_while_unlocked() {
    std::printf("  test_acknack_no_proactive_while_unlocked...");

    AckNackPolicy pol;

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    pol.onDataReceived();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    pol.onDataReceived();
    assert(!pol.isLocked() && "Should not be locked with only 1 interval");
    assert(!pol.shouldSendProactiveAckNack() &&
           "Unlocked policy must never fire proactive");

    std::printf(" PASS\n");
}

void test_acknack_locks_on_stable_stream() {
    std::printf("  test_acknack_locks_on_stable_stream...");

    AckNackPolicy pol;
    for (uint32_t i = 0; i < RateLock::MIN_REQUIRED + 1; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        pol.onDataReceived();
    }
    assert(pol.isLocked() && "Should lock onto stable 10 ms stream");
    assert(pol.lockedMeanMs() >= 8 && pol.lockedMeanMs() <= 14 &&
           "Locked mean should be around 10 ms");

    std::printf(" PASS\n");
}

void test_acknack_proactive_on_violation_alone() {
    std::printf("  test_acknack_proactive_on_violation_alone...");

    AckNackPolicy pol;
    for (uint32_t i = 0; i < RateLock::MIN_REQUIRED + 1; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        pol.onDataReceived();
    }
    assert(pol.isLocked());

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    pol.onDataReceived();
    assert(pol.shouldSendProactiveAckNack() &&
           "Locked policy must fire proactive on delta > mean * VIOLATION_FACTOR");

    std::printf(" PASS\n");
}

void test_acknack_hb_hint_fast_lock() {
    std::printf("  test_acknack_hb_hint_fast_lock...");

    AckNackPolicy pol;

    pol.onHeartbeat(0);
    std::this_thread::sleep_for(
        std::chrono::milliseconds(RateLock::IMPLICIT_HB_HINT_MS + 50));
    pol.onHeartbeat(0);

    for (uint32_t i = 0; i < RateLock::MIN_REQUIRED_HINTED + 1; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        pol.onDataReceived();
    }
    assert(pol.isLocked() &&
           "With IMPLICIT HB hint, MIN_REQUIRED_HINTED samples must be enough to lock");

    std::printf(" PASS\n");
}

}

int main() {
    std::printf("=== Heartbeat & AckNack Policy Tests ===\n");

    test_first_send_then_period_gate();
    test_adapts_toward_sample_rate();
    test_period_floor_holds();
    test_nack_resets_to_base();
    test_acknack_always_responds();
    test_acknack_no_proactive_while_unlocked();
    test_acknack_locks_on_stable_stream();
    test_acknack_proactive_on_violation_alone();
    test_acknack_hb_hint_fast_lock();

    std::printf("=== All tests passed ===\n");
    return 0;
}
