 #include <cassert>
 #include <cstdint>
 #include <iostream>
 #include <thread>
 #include <vector>

 #include "rtps/storages/UnorderedHistoryCache.h"

 using namespace rtps;

 static const uint8_t DUMMY_DATA[4] = {0, 1, 2, 3};

 void test_inorder_reception() {
   std::cout << "[test] In-order reception..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 1; i <= 5; ++i) {
     SequenceNumber_t sn{0, i};
     const CacheChange *cc =
         cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn, i);
     assert(cc != nullptr);
   }

   assert(cache.size() == 5);
   assert(cache.getSeqNumMin().low == 1);
   assert(cache.getSeqNumMax().low == 5);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.numBits == 0);
 }

 void test_duplicate_reception() {
   std::cout << "[test] Duplicate reception..." << std::endl;
   UnorderedHistoryCache cache;

   SequenceNumber_t sn{0, 1};
   const CacheChange *first =
       cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn, 1);
   assert(first != nullptr);

   const CacheChange *dup =
       cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn, 2);
   assert(dup == nullptr);

   assert(cache.size() == 1);
   assert(cache.getSeqNumMin().low == 1);
   assert(cache.getSeqNumMax().low == 1);
 }

 void test_out_of_order_with_gap_fill() {
   std::cout << "[test] Out-of-order reception with gap fill..." << std::endl;
   UnorderedHistoryCache cache;

   SequenceNumber_t sn1{0, 1};
   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn1, 1) != nullptr);

   SequenceNumber_t sn3{0, 3};
   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn3, 3) != nullptr);

   assert(cache.getSeqNumMin().low == 1);
   assert(cache.getSeqNumMax().low == 3);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.base.low == 2);
   assert(missing.numBits > 0);

   SequenceNumber_t sn2{0, 2};
   const CacheChange *gapFill =
       cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn2, 2);
   assert(gapFill != nullptr);

   missing = cache.getMissing();
   assert(missing.numBits == 0);
 }

 void test_gap_overflow_discard() {
   std::cout << "[test] Gap overflow discard..." << std::endl;
   UnorderedHistoryCache cache;

   SequenceNumber_t sn1{0, 1};
   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn1, 1) != nullptr);

   const uint32_t big_gap = HISTORY_BUFFER_SIZE;
   SequenceNumber_t sn_far{0, 1u + big_gap};

   const CacheChange *far =
       cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn_far, 2);
   assert(far == nullptr);
 }

 void test_ensure_range_initialized() {
   std::cout << "[test] ensureRangeInitialized + missing..." << std::endl;
   UnorderedHistoryCache cache;

   SequenceNumber_t first{0, 1};
   SequenceNumber_t last{0, 5};
   cache.ensureRangeInitialized(first, last);

   assert(cache.getSeqNumMin().low == 1);
   assert(cache.getSeqNumMax().low == 5);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.base.low == 1);
   assert(missing.numBits > 0);
 }

 void test_delivered_watermark_prevents_re_request() {
   std::cout << "[test] delivered watermark prevents re-request..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 1; i <= 5; ++i) {
     SequenceNumber_t sn{0, i};
     assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn, i) != nullptr);
   }

   cache.removeUntilIncl(SequenceNumber_t{0, 5});
   assert(cache.isEmpty());

   assert(cache.getSeqNumMax().low == 5);

   cache.ensureRangeInitialized(SequenceNumber_t{0, 1}, SequenceNumber_t{0, 10});

   assert(cache.getSeqNumMin().low == 6);
   assert(cache.getSeqNumMax().low == 10);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.base.low == 6);
   assert(missing.numBits > 0);

   for (uint32_t i = 1; i <= 5; ++i) {
     assert(cache.getChangeBySN(SequenceNumber_t{0, i}) == nullptr);
   }
 }

 void test_watermark_gap_creates_pending() {
   std::cout << "[test] watermark gap creates PENDING on addChange..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 1; i <= 5; ++i) {
     SequenceNumber_t sn{0, i};
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn, i);
   }
   cache.removeUntilIncl(SequenceNumber_t{0, 5});
   assert(cache.isEmpty());

   const CacheChange *cc =
       cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 7}, 7);
   assert(cc != nullptr);

   assert(cache.getSeqNumMin().low == 6);
   assert(cache.getSeqNumMax().low == 7);
   assert(cache.size() == 2);

   const CacheChange *pending = cache.getChangeBySN(SequenceNumber_t{0, 6});
   assert(pending != nullptr);
   assert(pending->kind == ChangeKind_t::PENDING);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.base.low == 6);
   assert(missing.numBits > 0);
 }

 void test_watermark_discards_old_sn() {
   std::cout << "[test] watermark discards already-delivered SN..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 1; i <= 5; ++i) {
     SequenceNumber_t sn{0, i};
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn, i);
   }
   cache.removeUntilIncl(SequenceNumber_t{0, 5});
   assert(cache.isEmpty());

   const CacheChange *dup =
       cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 3}, 3);
   assert(dup == nullptr);
   assert(cache.isEmpty());
 }

 void test_watermark_no_new_data() {
   std::cout << "[test] watermark skips ensureRange when nothing new..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 1; i <= 5; ++i) {
     SequenceNumber_t sn{0, i};
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn, i);
   }
   cache.removeUntilIncl(SequenceNumber_t{0, 5});
   assert(cache.isEmpty());

   cache.ensureRangeInitialized(SequenceNumber_t{0, 1}, SequenceNumber_t{0, 5});

   assert(cache.isEmpty());
 }

 void test_register_gap_does_not_evict_pending() {
   std::cout << "[test] registerGap does not evict PENDING entries..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 1; i <= 5; ++i) {
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, i}, i);
   }
   cache.removeUntilIncl(SequenceNumber_t{0, 5});
   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 7}, 7);

   assert(cache.getSeqNumMin().low == 6);
   assert(cache.size() == 2);

   for (uint32_t hb = 0; hb < 20; ++hb) {
     SequenceNumber_t lastAvail{0, 8 + hb * 50};
     cache.registerGap(lastAvail);
   }

   const CacheChange *sn6 = cache.getChangeBySN(SequenceNumber_t{0, 6});
   assert(sn6 != nullptr);
   assert(sn6->kind == ChangeKind_t::PENDING);
 }

 void test_drop_oldest_and_missing() {
   std::cout << "[test] dropOldest + missing behaviour..." << std::endl;
   UnorderedHistoryCache cache;

   SequenceNumber_t sn1{0, 1};
   SequenceNumber_t sn3{0, 3};
   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn1, 1) != nullptr);
   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn3, 3) != nullptr);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.base.low == 2);
   assert(missing.numBits > 0);

   cache.dropOldest();
   missing = cache.getMissing();
   assert(missing.base.low >= 2);
   assert(missing.numBits > 0);

   SequenceNumber_t sn2{0, 2};
   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn2, 2) != nullptr);

   missing = cache.getMissing();
   assert(missing.numBits == 0);
 }

 void test_buffer_rollover_and_missing_window() {
   std::cout << "[test] buffer rollover + missing window..." << std::endl;
   UnorderedHistoryCache cache;

   const uint32_t N = HISTORY_BUFFER_SIZE * 2;
   for (uint32_t i = 1; i <= N; ++i) {
     SequenceNumber_t sn{0, i};
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn, i);
     if (i % (HISTORY_BUFFER_SIZE / 4) == 0) {
       std::cout << "  inserted SN=" << i
                 << " (current size=" << cache.size() << ")\n";
     }
   }

   assert(cache.size() <= HISTORY_BUFFER_SIZE - 1);
   SequenceNumberSet missing = cache.getMissing();
   assert(missing.numBits == 0);
 }

 void test_multithreaded_add_and_read() {
   std::cout << "[test] multithreaded add + read..." << std::endl;

   UnorderedHistoryCache cache;
   const uint32_t numThreads = 4;
   const uint32_t perThread = 1000;

   auto worker = [&](uint32_t tid) {
     uint32_t start = tid * perThread + 1;
     uint32_t end = start + perThread - 1;
     for (uint32_t i = start; i <= end; ++i) {
       SequenceNumber_t sn{0, i};
       cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn, i);
       if ((i - start) % 250 == 0) {
         std::cout << "  [thread " << tid << "] up to SN=" << i << std::endl;
       }
     }
   };

   std::vector<std::thread> threads;
   for (uint32_t t = 0; t < numThreads; ++t) {
     threads.emplace_back(worker, t);
   }

   for (auto &th : threads) {
     th.join();
   }

   std::cout << "  multithreaded insert complete, cache size=" << cache.size()
             << std::endl;

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.numBits == 0);
 }

 void test_single_element_operations() {
   std::cout << "[test] single element operations..." << std::endl;
   UnorderedHistoryCache cache;

   SequenceNumber_t sn{0, 42};
   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn, 1) != nullptr);
   assert(cache.size() == 1);
   assert(cache.getSeqNumMin().low == 42);
   assert(cache.getSeqNumMax().low == 42);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.numBits == 0);

   const CacheChange *cc = cache.getChangeBySN(sn);
   assert(cc != nullptr);
   assert(cc->sequenceNumber == sn);
   assert(cc->kind == ChangeKind_t::ALIVE_UNDELIVERED);

   cache.dropOldest();
   assert(cache.isEmpty() || cache.size() == 0);
 }

 void test_removeUntilIncl_partial() {
   std::cout << "[test] removeUntilIncl partial removal..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 1; i <= 10; ++i) {
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, i}, i);
   }
   assert(cache.size() == 10);

   cache.removeUntilIncl(SequenceNumber_t{0, 5});
   assert(cache.size() == 5);
   assert(cache.getSeqNumMin().low == 6);
   assert(cache.getSeqNumMax().low == 10);

   assert(cache.getChangeBySN(SequenceNumber_t{0, 5}) == nullptr);
   assert(cache.getChangeBySN(SequenceNumber_t{0, 6}) != nullptr);
 }

 void test_removeUntilIncl_beyond_max() {
   std::cout << "[test] removeUntilIncl beyond max empties cache..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 1; i <= 5; ++i) {
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, i}, i);
   }
   cache.removeUntilIncl(SequenceNumber_t{0, 100});
   assert(cache.isEmpty());
   assert(cache.getSeqNumMax().low == 5);
 }

 void test_addChange_too_old_nonEmpty() {
   std::cout << "[test] addChange below lowestSN is discarded..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 5; i <= 10; ++i) {
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, i}, i);
   }

   const CacheChange *old =
       cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 3}, 3);
   assert(old == nullptr);
   assert(cache.size() == 6);
   assert(cache.getSeqNumMin().low == 5);
 }

 void test_getChangeBySN_out_of_range() {
   std::cout << "[test] getChangeBySN out of range returns null..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 5; i <= 10; ++i) {
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, i}, i);
   }

   assert(cache.getChangeBySN(SequenceNumber_t{0, 4}) == nullptr);
   assert(cache.getChangeBySN(SequenceNumber_t{0, 11}) == nullptr);
   assert(cache.getChangeBySN(SequenceNumber_t{0, 7}) != nullptr);
 }

 void test_getMissing_all_alive() {
   std::cout << "[test] getMissing with all ALIVE entries..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 1; i <= 10; ++i) {
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, i}, i);
   }

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.numBits == 0);
 }

 void test_getMissing_exactly_32_pending() {
   std::cout << "[test] getMissing with exactly SNS_NUM_BITS PENDING entries..." << std::endl;
   UnorderedHistoryCache cache;

   cache.ensureRangeInitialized(SequenceNumber_t{0, 1},
                                 SequenceNumber_t{0, SNS_NUM_BITS});
   assert(cache.size() == SNS_NUM_BITS);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.base.low == 1);
   assert(missing.numBits == SNS_NUM_BITS);

   for (uint32_t i = 0; i < SNS_NUM_BITS; ++i) {
     assert(missing.isSet(i));
   }
 }

 void test_getMissing_more_than_32_pending() {
   std::cout << "[test] getMissing with >32 PENDING, window is 32..." << std::endl;
   UnorderedHistoryCache cache;

   cache.ensureRangeInitialized(SequenceNumber_t{0, 1}, SequenceNumber_t{0, 64});
   assert(cache.size() == 64);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.base.low == 1);
   assert(missing.numBits <= SNS_NUM_BITS);
   assert(missing.numBits > 0);
 }

 void test_multiple_gaps() {
   std::cout << "[test] multiple non-contiguous gaps..." << std::endl;
   UnorderedHistoryCache cache;

   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 1}, 1);
   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 4}, 4);
   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 7}, 7);

   assert(cache.size() == 7);
   assert(cache.getChangeBySN(SequenceNumber_t{0, 2})->kind == ChangeKind_t::PENDING);
   assert(cache.getChangeBySN(SequenceNumber_t{0, 3})->kind == ChangeKind_t::PENDING);
   assert(cache.getChangeBySN(SequenceNumber_t{0, 5})->kind == ChangeKind_t::PENDING);
   assert(cache.getChangeBySN(SequenceNumber_t{0, 6})->kind == ChangeKind_t::PENDING);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.base.low == 2);
   assert(missing.numBits >= 5);
   assert(missing.isSet(0));
   assert(missing.isSet(1));
   assert(!missing.isSet(2));
   assert(missing.isSet(3));
   assert(missing.isSet(4));

   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 2}, 2);
   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 3}, 3);

   missing = cache.getMissing();
   assert(missing.base.low == 5);
   assert(missing.isSet(0));
   assert(missing.isSet(1));

   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 5}, 5);
   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 6}, 6);
   missing = cache.getMissing();
   assert(missing.numBits == 0);
 }

 void test_registerGap_idempotent() {
   std::cout << "[test] registerGap idempotent..." << std::endl;
   UnorderedHistoryCache cache;

   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 1}, 1);
   cache.registerGap(SequenceNumber_t{0, 5});
   uint32_t sizeAfterFirst = cache.size();

   cache.registerGap(SequenceNumber_t{0, 5});
   assert(cache.size() == sizeAfterFirst);

   cache.registerGap(SequenceNumber_t{0, 3});
   assert(cache.size() == sizeAfterFirst);
 }

 void test_registerGap_on_empty_cache() {
   std::cout << "[test] registerGap on empty cache is no-op..." << std::endl;
   UnorderedHistoryCache cache;

   cache.registerGap(SequenceNumber_t{0, 100});
   assert(cache.isEmpty());
 }

 void test_registerGap_fills_to_capacity() {
   std::cout << "[test] registerGap fills exactly to capacity..." << std::endl;
   UnorderedHistoryCache cache;

   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 1}, 1);
   assert(cache.size() == 1);

   uint32_t maxCapacity = HISTORY_BUFFER_SIZE - 1;
   cache.registerGap(SequenceNumber_t{0, maxCapacity});

   assert(cache.size() == maxCapacity);
   assert(cache.isFull());

   const CacheChange *sn1 = cache.getChangeBySN(SequenceNumber_t{0, 1});
   assert(sn1 != nullptr);
   assert(sn1->kind == ChangeKind_t::ALIVE_UNDELIVERED);
 }

 void test_ensureRange_on_nonempty_is_noop() {
   std::cout << "[test] ensureRangeInitialized on non-empty cache is no-op..." << std::endl;
   UnorderedHistoryCache cache;

   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 1}, 1);
   uint32_t sizeBefore = cache.size();

   cache.ensureRangeInitialized(SequenceNumber_t{0, 1}, SequenceNumber_t{0, 100});
   assert(cache.size() == sizeBefore);
 }

 void test_ensureRange_larger_than_capacity() {
   std::cout << "[test] ensureRangeInitialized truncates to capacity..." << std::endl;
   UnorderedHistoryCache cache;

   uint32_t capacity = HISTORY_BUFFER_SIZE - 1;
   cache.ensureRangeInitialized(SequenceNumber_t{0, 1},
                                 SequenceNumber_t{0, capacity * 3});
   assert(cache.size() == capacity);
   assert(cache.getSeqNumMax().low == capacity * 3);
   assert(cache.getSeqNumMin().low == capacity * 3 - capacity + 1);
 }

 void test_watermark_exact_boundary() {
   std::cout << "[test] addChange exactly at watermark is discarded..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 1; i <= 5; ++i) {
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, i}, i);
   }
   cache.removeUntilIncl(SequenceNumber_t{0, 5});
   assert(cache.isEmpty());

   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA),
                           SequenceNumber_t{0, 5}, 5) == nullptr);
   assert(cache.isEmpty());

   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA),
                           SequenceNumber_t{0, 6}, 6) != nullptr);
   assert(cache.size() == 1);
   assert(cache.getSeqNumMin().low == 6);
 }

 void test_watermark_gap_overflow_empty() {
   std::cout << "[test] watermark gap overflow on empty cache..." << std::endl;
   UnorderedHistoryCache cache;

   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 1}, 1);
   cache.removeUntilIncl(SequenceNumber_t{0, 1});
   assert(cache.isEmpty());

   uint32_t capacity = HISTORY_BUFFER_SIZE - 1;
   SequenceNumber_t farSn{0, 2 + capacity};
   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), farSn, 2) == nullptr);
   assert(cache.isEmpty());
 }

 void test_gap_exactly_at_capacity_limit() {
   std::cout << "[test] gap exactly at capacity boundary..." << std::endl;
   UnorderedHistoryCache cache;

   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 1}, 1);

   uint32_t capacity = HISTORY_BUFFER_SIZE - 1;
   uint32_t gap = capacity - 2;
   SequenceNumber_t snFit{0, 2 + gap};
   const CacheChange *cc =
       cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), snFit, 2);
   assert(cc != nullptr);
   assert(cache.size() == capacity);
 }

 void test_full_retransmit_cycle() {
   std::cout << "[test] full deliver-gap-retransmit-deliver cycle..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 1; i <= 10; ++i) {
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, i}, i);
   }
   cache.removeUntilIncl(SequenceNumber_t{0, 10});
   assert(cache.isEmpty());
   assert(cache.getSeqNumMax().low == 10);

   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 12}, 12);
   assert(cache.getSeqNumMin().low == 11);
   assert(cache.size() == 2);

   const CacheChange *p11 = cache.getChangeBySN(SequenceNumber_t{0, 11});
   assert(p11 != nullptr && p11->kind == ChangeKind_t::PENDING);

   cache.registerGap(SequenceNumber_t{0, 15});
   assert(cache.size() == 5);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.base.low == 11);
   assert(missing.isSet(0));

   const CacheChange *filled =
       cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 11}, 11);
   assert(filled != nullptr);
   assert(filled->kind == ChangeKind_t::ALIVE_UNDELIVERED);

   cache.removeUntilIncl(SequenceNumber_t{0, 12});
   assert(cache.getSeqNumMin().low == 13);
   assert(cache.size() == 3);

   for (uint32_t i = 13; i <= 15; ++i) {
     assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA),
                             SequenceNumber_t{0, i}, i) != nullptr);
   }
   missing = cache.getMissing();
   assert(missing.numBits == 0);

   cache.removeUntilIncl(SequenceNumber_t{0, 15});
   assert(cache.isEmpty());
   assert(cache.getSeqNumMax().low == 15);
 }

 void test_snToPos_at_wrap_boundary() {
   std::cout << "[test] snToPos correctness at ring buffer wrap..." << std::endl;
   UnorderedHistoryCache cache;

   uint32_t capacity = HISTORY_BUFFER_SIZE - 1;
   for (uint32_t i = 1; i <= capacity; ++i) {
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, i}, i);
   }
   assert(cache.isFull());

   for (uint32_t i = 1; i <= capacity; ++i) {
     const CacheChange *cc = cache.getChangeBySN(SequenceNumber_t{0, i});
     assert(cc != nullptr);
     assert(cc->sequenceNumber.low == i);
   }

   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA),
                           SequenceNumber_t{0, capacity + 1}, capacity + 1) != nullptr);
   assert(cache.getSeqNumMin().low == 2);
   assert(cache.getSeqNumMax().low == capacity + 1);
   assert(cache.getChangeBySN(SequenceNumber_t{0, 1}) == nullptr);

   for (uint32_t i = 2; i <= capacity + 1; ++i) {
     const CacheChange *cc = cache.getChangeBySN(SequenceNumber_t{0, i});
     assert(cc != nullptr);
     assert(cc->sequenceNumber.low == i);
   }
 }

 void test_copy_constructor() {
   std::cout << "[test] copy constructor..." << std::endl;
   UnorderedHistoryCache original;

   original.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 1}, 1);
   original.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 3}, 3);

   UnorderedHistoryCache copy(original);
   assert(copy.size() == original.size());
   assert(copy.getSeqNumMin().low == 1);
   assert(copy.getSeqNumMax().low == 3);
   assert(copy.getChangeBySN(SequenceNumber_t{0, 2})->kind == ChangeKind_t::PENDING);

   copy.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 2}, 2);
   assert(copy.getChangeBySN(SequenceNumber_t{0, 2})->kind == ChangeKind_t::ALIVE_UNDELIVERED);
   assert(original.getChangeBySN(SequenceNumber_t{0, 2})->kind == ChangeKind_t::PENDING);
 }

 void test_repeated_deliver_gap_cycles() {
   std::cout << "[test] repeated deliver-gap cycles..." << std::endl;
   UnorderedHistoryCache cache;

   uint32_t sn = 1;
   for (int round = 0; round < 5; ++round) {
     for (uint32_t i = 0; i < 10; ++i, ++sn) {
       cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, sn}, sn);
     }

     cache.removeUntilIncl(SequenceNumber_t{0, sn - 1});
     assert(cache.isEmpty());
     assert(cache.getSeqNumMax().low == sn - 1);

     uint32_t lostSn = sn;
     ++sn;
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, sn}, sn);
     ++sn;

     assert(!cache.isEmpty());
     assert(cache.getChangeBySN(SequenceNumber_t{0, lostSn})->kind ==
            ChangeKind_t::PENDING);

     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA),
                     SequenceNumber_t{0, lostSn}, lostSn);
     assert(cache.getChangeBySN(SequenceNumber_t{0, lostSn})->kind ==
            ChangeKind_t::ALIVE_UNDELIVERED);

     SequenceNumberSet missing = cache.getMissing();
     assert(missing.numBits == 0);

     cache.removeUntilIncl(SequenceNumber_t{0, sn - 1});
     assert(cache.isEmpty());
   }
 }

 void test_getMissing_pending_only_at_end() {
   std::cout << "[test] getMissing when PENDING is only at the end..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 1; i <= 5; ++i) {
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, i}, i);
   }
   cache.registerGap(SequenceNumber_t{0, 8});
   assert(cache.size() == 8);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.base.low == 6);
   assert(missing.numBits == 3);
   assert(missing.isSet(0));
   assert(missing.isSet(1));
   assert(missing.isSet(2));
 }

 void test_getMissing_pending_in_middle() {
   std::cout << "[test] getMissing when PENDING is sandwiched between ALIVE..." << std::endl;
   UnorderedHistoryCache cache;

   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 1}, 1);
   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 3}, 3);
   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 4}, 4);
   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 6}, 6);
   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 7}, 7);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.base.low == 2);

   assert(missing.isSet(0));
   assert(!missing.isSet(1));
   assert(!missing.isSet(2));
   assert(missing.isSet(3));
   assert(!missing.isSet(4));
   assert(!missing.isSet(5));
 }

 void test_history_size_boundary() {
   std::cout << "[test] exact HISTORY_SIZE boundary (SN 257 scenario)..." << std::endl;
   UnorderedHistoryCache cache;
   uint32_t capacity = HISTORY_BUFFER_SIZE - 1;

   for (uint32_t i = 1; i <= capacity; ++i) {
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, i}, i);
   }
   cache.removeUntilIncl(SequenceNumber_t{0, capacity});
   assert(cache.isEmpty());
   assert(cache.getSeqNumMax().low == capacity);

   uint32_t lostSn = capacity + 1;
   uint32_t arrivedSn = capacity + 2;
   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA),
                           SequenceNumber_t{0, arrivedSn}, arrivedSn) != nullptr);

   assert(cache.getSeqNumMin().low == lostSn);
   assert(cache.size() == 2);
   assert(cache.getChangeBySN(SequenceNumber_t{0, lostSn})->kind ==
          ChangeKind_t::PENDING);

   cache.registerGap(SequenceNumber_t{0, capacity + 50});

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.base.low == lostSn);
   assert(missing.isSet(0));

   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA),
                           SequenceNumber_t{0, lostSn}, lostSn) != nullptr);

   const CacheChange *cc = cache.getChangeBySN(SequenceNumber_t{0, lostSn});
   assert(cc != nullptr && cc->kind == ChangeKind_t::ALIVE_UNDELIVERED);
 }

 void test_ensureRange_zero_range() {
   std::cout << "[test] ensureRangeInitialized with zero/invalid ranges..." << std::endl;
   UnorderedHistoryCache cache;

   cache.ensureRangeInitialized(SequenceNumber_t{0, 0}, SequenceNumber_t{0, 0});
   assert(cache.isEmpty());

   cache.ensureRangeInitialized(SequenceNumber_t{0, 10}, SequenceNumber_t{0, 5});
   assert(cache.isEmpty());

   cache.ensureRangeInitialized(SEQUENCENUMBER_UNKNOWN, SequenceNumber_t{0, 5});
   assert(cache.isEmpty());
 }

 void test_addChange_duplicate_alive() {
   std::cout << "[test] addChange duplicate for ALIVE slot returns null..." << std::endl;
   UnorderedHistoryCache cache;

   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 1}, 1);
   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 3}, 3);

   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA),
                           SequenceNumber_t{0, 2}, 2) != nullptr);
   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA),
                           SequenceNumber_t{0, 2}, 99) == nullptr);
 }

 void test_get_missing_covers_oldest_pending() {
   std::cout << "[test] getMissing bitmap starts at oldest PENDING..." << std::endl;
   UnorderedHistoryCache cache;

   for (uint32_t i = 1; i <= 5; ++i) {
     SequenceNumber_t sn{0, i};
     cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), sn, i);
   }
   cache.removeUntilIncl(SequenceNumber_t{0, 5});
   assert(cache.isEmpty());

   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 7}, 7);
   assert(cache.getChangeBySN(SequenceNumber_t{0, 6}) != nullptr);
   assert(cache.getChangeBySN(SequenceNumber_t{0, 6})->kind ==
          ChangeKind_t::PENDING);

   cache.registerGap(SequenceNumber_t{0, 50});

   assert(cache.size() > SNS_NUM_BITS);

   SequenceNumberSet missing = cache.getMissing();
   assert(missing.base.low == 6);
   assert(missing.numBits > 0);
   assert(missing.isSet(0));
 }

 void test_builtin_recovers_below_floor() {
   std::cout << "[test] builtin recovers below floor..." << std::endl;
   UnorderedHistoryCache cache;
   cache.setRecoverBelowFloor(true);
   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 2}, 2) != nullptr);
   SequenceNumberSet missing =
       cache.getMissingForHeartbeatRange(SequenceNumber_t{0, 1}, SequenceNumber_t{0, 4});
   assert(missing.base.low == 1);
   assert(missing.numBits > 0);
   assert(missing.isSet(0));
 }

 void test_default_suppresses_below_floor() {
   std::cout << "[test] default suppresses below floor..." << std::endl;
   UnorderedHistoryCache cache;
   assert(cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 2}, 2) != nullptr);
   SequenceNumberSet missing =
       cache.getMissingForHeartbeatRange(SequenceNumber_t{0, 1}, SequenceNumber_t{0, 4});
   assert(!(missing.base.low == 1 && missing.isSet(0)));
 }

 void test_builtin_below_floor_converges() {
   std::cout << "[test] builtin below-floor converges..." << std::endl;
   UnorderedHistoryCache cache;
   cache.setRecoverBelowFloor(true);
   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 2}, 2);
   cache.removeUntilIncl(SequenceNumber_t{0, 2});
   SequenceNumberSet m1 =
       cache.getMissingForHeartbeatRange(SequenceNumber_t{0, 1}, SequenceNumber_t{0, 2});
   assert(m1.base.low == 1 && m1.isSet(0));
   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 1}, 1);
   cache.addChange(DUMMY_DATA, sizeof(DUMMY_DATA), SequenceNumber_t{0, 2}, 2);
   cache.removeUntilIncl(SequenceNumber_t{0, 2});
   SequenceNumberSet m2 =
       cache.getMissingForHeartbeatRange(SequenceNumber_t{0, 1}, SequenceNumber_t{0, 2});
   assert(m2.numBits == 0);
 }

 int main() {
   std::cout << "Running UnorderedHistoryCache tests..." << std::endl;

   test_builtin_recovers_below_floor();
   test_default_suppresses_below_floor();
   test_builtin_below_floor_converges();

   test_inorder_reception();
   test_duplicate_reception();
   test_out_of_order_with_gap_fill();
   test_gap_overflow_discard();
   test_ensure_range_initialized();
   test_delivered_watermark_prevents_re_request();
   test_watermark_gap_creates_pending();
   test_watermark_discards_old_sn();
   test_watermark_no_new_data();
   test_register_gap_does_not_evict_pending();
   test_drop_oldest_and_missing();
   test_buffer_rollover_and_missing_window();

   test_single_element_operations();
   test_removeUntilIncl_partial();
   test_removeUntilIncl_beyond_max();
   test_addChange_too_old_nonEmpty();
   test_getChangeBySN_out_of_range();
   test_getMissing_all_alive();
   test_getMissing_exactly_32_pending();
   test_getMissing_more_than_32_pending();
   test_multiple_gaps();
   test_registerGap_idempotent();
   test_registerGap_on_empty_cache();
   test_registerGap_fills_to_capacity();
   test_ensureRange_on_nonempty_is_noop();
   test_ensureRange_larger_than_capacity();
   test_watermark_exact_boundary();
   test_watermark_gap_overflow_empty();
   test_gap_exactly_at_capacity_limit();
   test_full_retransmit_cycle();
   test_snToPos_at_wrap_boundary();
   test_copy_constructor();
   test_repeated_deliver_gap_cycles();
   test_getMissing_pending_only_at_end();
   test_getMissing_pending_in_middle();
   test_history_size_boundary();
   test_ensureRange_zero_range();
   test_addChange_duplicate_alive();
   test_get_missing_covers_oldest_pending();
   test_multithreaded_add_and_read();

   std::cout << "All UnorderedHistoryCache tests passed." << std::endl;
   return 0;
 }

