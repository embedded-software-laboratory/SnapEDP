#include <cassert>
#include <cstdint>
#include <iostream>

#include "rtps/storages/OrderedHistoryCache.h"

using namespace rtps;

static const uint8_t DUMMY_DATA[4] = {0, 1, 2, 3};

void test_ordered_basic() {
  std::cout << "[test] basic add and lookup..." << std::endl;
  OrderedHistoryCache cache;

  for (uint32_t i = 0; i < 5; ++i) {
    const CacheChange *cc = cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA));
    assert(cc != nullptr);
    assert(cc->sequenceNumber.low == i + 1);
  }

  assert(cache.size() == 5);
  assert(cache.getSeqNumMin().low == 1);
  assert(cache.getSeqNumMax().low == 5);

  for (uint32_t i = 1; i <= 5; ++i) {
    const CacheChange *cc = cache.getChangeBySN(SequenceNumber_t{0, i});
    assert(cc != nullptr);
    assert(cc->sequenceNumber.low == i);
  }
}

void test_ordered_overflow_keeps_data_accessible() {
  std::cout << "[test] overflow at HISTORY_SIZE boundary (SN 257 bug)..." << std::endl;
  OrderedHistoryCache cache;

  uint32_t capacity = HISTORY_BUFFER_SIZE - 1;

  for (uint32_t i = 0; i < capacity; ++i) {
    assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA)) != nullptr);
  }
  assert(cache.isFull());
  assert(cache.size() == capacity);
  assert(cache.getSeqNumMin().low == 1);
  assert(cache.getSeqNumMax().low == capacity);

  const CacheChange *overflow = cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA));
  assert(overflow != nullptr);
  assert(overflow->sequenceNumber.low == capacity + 1);

  assert(!cache.isEmpty());
  assert(cache.size() == capacity);
  assert(cache.getSeqNumMin().low == 2);
  assert(cache.getSeqNumMax().low == capacity + 1);

  const CacheChange *found =
      cache.getChangeBySN(SequenceNumber_t{0, capacity + 1});
  assert(found != nullptr);
  assert(found->sequenceNumber.low == capacity + 1);

  assert(cache.getChangeBySN(SequenceNumber_t{0, 1}) == nullptr);

  for (uint32_t i = 2; i <= capacity + 1; ++i) {
    const CacheChange *cc = cache.getChangeBySN(SequenceNumber_t{0, i});
    assert(cc != nullptr);
    assert(cc->sequenceNumber.low == i);
  }
}

void test_ordered_sustained_overflow() {
  std::cout << "[test] sustained writes past capacity..." << std::endl;
  OrderedHistoryCache cache;

  uint32_t capacity = HISTORY_BUFFER_SIZE - 1;
  uint32_t totalWrites = capacity * 3;

  for (uint32_t i = 0; i < totalWrites; ++i) {
    const CacheChange *cc = cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA));
    assert(cc != nullptr);
    assert(cc->sequenceNumber.low == i + 1);
  }

  assert(!cache.isEmpty());
  assert(cache.size() == capacity);
  assert(cache.getSeqNumMax().low == totalWrites);
  assert(cache.getSeqNumMin().low == totalWrites - capacity + 1);

  for (uint32_t sn = cache.getSeqNumMin().low; sn <= cache.getSeqNumMax().low; ++sn) {
    const CacheChange *cc = cache.getChangeBySN(SequenceNumber_t{0, sn});
    assert(cc != nullptr);
    assert(cc->sequenceNumber.low == sn);
  }
}

void test_ordered_removeUntilIncl() {
  std::cout << "[test] removeUntilIncl..." << std::endl;
  OrderedHistoryCache cache;

  for (uint32_t i = 0; i < 10; ++i) {
    cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA));
  }

  cache.removeUntilIncl(SequenceNumber_t{0, 5});
  assert(cache.size() == 5);
  assert(cache.getSeqNumMin().low == 6);
  assert(cache.getSeqNumMax().low == 10);

  assert(cache.getChangeBySN(SequenceNumber_t{0, 5}) == nullptr);
  assert(cache.getChangeBySN(SequenceNumber_t{0, 6}) != nullptr);
}

void test_ordered_removeUntilIncl_all() {
  std::cout << "[test] removeUntilIncl all..." << std::endl;
  OrderedHistoryCache cache;

  for (uint32_t i = 0; i < 5; ++i) {
    cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA));
  }

  cache.removeUntilIncl(SequenceNumber_t{0, 100});
  assert(cache.isEmpty());
}

void test_ordered_getChangeBySN_out_of_range() {
  std::cout << "[test] getChangeBySN out of range..." << std::endl;
  OrderedHistoryCache cache;

  for (uint32_t i = 0; i < 5; ++i) {
    cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA));
  }

  assert(cache.getChangeBySN(SequenceNumber_t{0, 0}) == nullptr);
  assert(cache.getChangeBySN(SequenceNumber_t{0, 6}) == nullptr);
  assert(cache.getChangeBySN(SequenceNumber_t{0, 3}) != nullptr);
}

void test_ordered_getChangeBySN_empty() {
  std::cout << "[test] getChangeBySN on empty cache..." << std::endl;
  OrderedHistoryCache cache;
  assert(cache.getChangeBySN(SequenceNumber_t{0, 1}) == nullptr);
}

void test_ordered_dropOldest() {
  std::cout << "[test] dropOldest..." << std::endl;
  OrderedHistoryCache cache;

  for (uint32_t i = 0; i < 5; ++i) {
    cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA));
  }

  cache.dropOldest();
  assert(cache.size() == 4);
  assert(cache.getSeqNumMin().low == 2);
}

void test_ordered_overflow_then_remove_then_refill() {
  std::cout << "[test] overflow, remove, refill cycle..." << std::endl;
  OrderedHistoryCache cache;

  uint32_t capacity = HISTORY_BUFFER_SIZE - 1;

  for (uint32_t i = 0; i < capacity + 10; ++i) {
    cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA));
  }
  assert(cache.size() == capacity);
  assert(cache.getSeqNumMin().low == 11);
  assert(cache.getSeqNumMax().low == capacity + 10);

  uint32_t midSN = cache.getSeqNumMin().low + capacity / 2;
  cache.removeUntilIncl(SequenceNumber_t{0, midSN});
  uint32_t sizeAfterRemove = cache.size();
  assert(sizeAfterRemove < capacity);

  uint32_t nextExpectedSN = cache.getSeqNumMax().low + 1;
  for (uint32_t i = 0; i < capacity; ++i) {
    const CacheChange *cc = cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA));
    assert(cc != nullptr);
    assert(cc->sequenceNumber.low == nextExpectedSN + i);
  }

  assert(!cache.isEmpty());
  const CacheChange *latest = cache.getChangeBySN(cache.getSeqNumMax());
  assert(latest != nullptr);
}

void test_ordered_single_element() {
  std::cout << "[test] single element operations..." << std::endl;
  OrderedHistoryCache cache;

  const CacheChange *cc = cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA));
  assert(cc != nullptr);
  assert(cache.size() == 1);
  assert(!cache.isEmpty());
  assert(!cache.isFull());

  assert(cache.getChangeBySN(SequenceNumber_t{0, 1}) != nullptr);

  cache.dropOldest();
  assert(cache.isEmpty());
  assert(cache.size() == 0);
}

void test_ordered_snToPos_after_multiple_wraps() {
  std::cout << "[test] snToPos after multiple wraps..." << std::endl;
  OrderedHistoryCache cache;

  uint32_t capacity = HISTORY_BUFFER_SIZE - 1;

  for (uint32_t i = 0; i < capacity * 5; ++i) {
    const CacheChange *cc = cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA));
    assert(cc != nullptr);
  }

  uint32_t totalWritten = capacity * 5;
  assert(cache.getSeqNumMax().low == totalWritten);
  assert(cache.getSeqNumMin().low == totalWritten - capacity + 1);

  for (uint32_t sn = cache.getSeqNumMin().low; sn <= cache.getSeqNumMax().low; ++sn) {
    const CacheChange *cc = cache.getChangeBySN(SequenceNumber_t{0, sn});
    assert(cc != nullptr);
    assert(cc->sequenceNumber.low == sn);
  }
}

int main() {
  std::cout << "Running OrderedHistoryCache tests..." << std::endl;

  test_ordered_basic();
  test_ordered_overflow_keeps_data_accessible();
  test_ordered_sustained_overflow();
  test_ordered_removeUntilIncl();
  test_ordered_removeUntilIncl_all();
  test_ordered_getChangeBySN_out_of_range();
  test_ordered_getChangeBySN_empty();
  test_ordered_dropOldest();
  test_ordered_overflow_then_remove_then_refill();
  test_ordered_single_element();
  test_ordered_snToPos_after_multiple_wraps();

  std::cout << "All OrderedHistoryCache tests passed." << std::endl;
  return 0;
}
