#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

#include "rtps/entities/StatefulWriter.h"
#include "rtps/entities/ReaderProxy.h"
#include "rtps/config/FeatureQOS.h"
#include "rtps/common/Locator.h"
#include "rtps/common/types.h"
#include "rtps/ThreadPool.h"

using namespace rtps;

namespace {

class RecordingDriver {
public:
  bool sendPacket(const PacketInfo & ) {
    ++send_count;
    return true;
  }

  void reset() { send_count = 0; }

  int send_count = 0;
};

class InlinePool : public ThreadPool {
public:
  InlinePool() : ThreadPool(nullptr, nullptr) {}
};

TopicData make_topic() {
  TopicData td{};
  const char *name = "reliable_loss_test";
  std::snprintf(td.topicName, sizeof(td.topicName), "%s", name);
  td.unicastLocator.kind = LocatorKind_t::LOCATOR_KIND_UDPv4;
  td.unicastLocator.port = 7400;
  td.unicastLocator.address[12] = 127;
  td.unicastLocator.address[13] = 0;
  td.unicastLocator.address[14] = 0;
  td.unicastLocator.address[15] = 1;
  return td;
}

Guid_t make_reader_guid() {
  Guid_t g{};
  g.prefix = GUIDPREFIX_UNKNOWN;
  g.entityId.entityKind = EntityKind_t::USER_DEFINED_READER_WITHOUT_KEY;
  g.entityId.entityKey[0] = 1;
  g.entityId.entityKey[1] = 0;
  g.entityId.entityKey[2] = 0;
  return g;
}

Locator make_reader_loc() {
  Locator loc;
  loc.kind = LocatorKind_t::LOCATOR_KIND_UDPv4;
  loc.port = 7500;
  loc.address[12] = 10;
  loc.address[13] = 0;
  loc.address[14] = 0;
  loc.address[15] = 1;
  return loc;
}

void fill_payload(uint8_t *buf, size_t size, uint32_t value) {
  for (size_t i = 0; i < size; ++i) {
    buf[i] = static_cast<uint8_t>((value + i) & 0xFFu);
  }
}

void setup_writer_with_samples(StatefulWriterT<RecordingDriver> &writer,
                               InlinePool &pool,
                               RecordingDriver &driver,
                               uint32_t num_samples) {
  FeatureQOS qos{};

  TopicData td = make_topic();
  bool ok = writer.init(td, TopicKind_t::NO_KEY, &pool, driver, false, qos);
  assert(ok);

  std::this_thread::sleep_for(std::chrono::milliseconds(2));

  ReaderProxy proxy(make_reader_guid(), make_reader_loc());
  bool add_ok = writer.addNewMatchedReader(proxy);
  assert(add_ok);

  uint8_t payload[16];
  for (uint32_t i = 0; i < num_samples; ++i) {
    fill_payload(payload, sizeof(payload), i + 1);
    const CacheChange *cc =
        writer.newChange(ChangeKind_t::ALIVE, payload, sizeof(payload));
    assert(cc != nullptr);
  }
}

SubmessageAckNack make_acknack(SequenceNumber_t base, uint32_t numBits,
                               const std::vector<uint32_t> &set_bits,
                               uint32_t count) {
  SubmessageAckNack ack{};
  ack.readerId = make_reader_guid().entityId;
  ack.count.value = count;
  ack.readerSNState.base = base;
  ack.readerSNState.numBits = numBits;
  for (uint32_t bit : set_bits) {
    ack.readerSNState.set(bit);
  }
  return ack;
}

void test_all_acked_no_retransmission() {
  std::cout << "[test] all acked, no retransmission..." << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;
  setup_writer_with_samples(writer, pool, driver, 5);

  SubmessageAckNack ack = make_acknack({0, 1}, 5, {}, 1);

  driver.reset();
  writer.onNewAckNack(ack, make_reader_guid().prefix);
  assert(driver.send_count == 0);
}

void test_single_missing_sample() {
  std::cout << "[test] single missing sample (bit 2)..." << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;
  setup_writer_with_samples(writer, pool, driver, 5);

  SubmessageAckNack ack = make_acknack({0, 1}, 5, {2}, 1);

  driver.reset();
  writer.onNewAckNack(ack, make_reader_guid().prefix);
  assert(driver.send_count == 1);
}

void test_first_and_last_missing() {
  std::cout << "[test] first and last missing (bits 0, 4)..." << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;
  setup_writer_with_samples(writer, pool, driver, 5);

  SubmessageAckNack ack = make_acknack({0, 1}, 5, {0, 4}, 1);

  driver.reset();
  writer.onNewAckNack(ack, make_reader_guid().prefix);
  assert(driver.send_count == 2);
}

void test_partial_loss_at_start() {
  std::cout << "[test] partial loss at start (bits 0, 1, 2)..." << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;
  setup_writer_with_samples(writer, pool, driver, 5);

  SubmessageAckNack ack = make_acknack({0, 1}, 5, {0, 1, 2}, 1);

  driver.reset();
  writer.onNewAckNack(ack, make_reader_guid().prefix);
  assert(driver.send_count == 3);
}

void test_all_missing() {
  std::cout << "[test] all 5 samples missing..." << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;
  setup_writer_with_samples(writer, pool, driver, 5);

  SubmessageAckNack ack = make_acknack({0, 1}, 5, {0, 1, 2, 3, 4}, 1);

  driver.reset();
  writer.onNewAckNack(ack, make_reader_guid().prefix);
  assert(driver.send_count == 5);
}

void test_duplicate_acknack_ignored() {
  std::cout << "[test] duplicate acknack (same count) ignored..." << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;
  setup_writer_with_samples(writer, pool, driver, 5);

  SubmessageAckNack ack = make_acknack({0, 1}, 5, {0, 1, 2, 3, 4}, 1);

  driver.reset();
  writer.onNewAckNack(ack, make_reader_guid().prefix);
  assert(driver.send_count == 5);

  driver.reset();
  writer.onNewAckNack(ack, make_reader_guid().prefix);
  assert(driver.send_count == 0);
}

void test_higher_count_processed() {
  std::cout << "[test] higher count acknack processed..." << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;
  setup_writer_with_samples(writer, pool, driver, 5);

  SubmessageAckNack ack1 = make_acknack({0, 1}, 5, {0, 1, 2}, 1);
  driver.reset();
  writer.onNewAckNack(ack1, make_reader_guid().prefix);
  assert(driver.send_count == 3);

  SubmessageAckNack ack2 = make_acknack({0, 1}, 5, {3, 4}, 2);
  driver.reset();
  writer.onNewAckNack(ack2, make_reader_guid().prefix);
  assert(driver.send_count == 2);
}

void test_preemptive_acknack_ignored() {
  std::cout << "[test] preemptive acknack (base=0,0) ignored..." << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;
  setup_writer_with_samples(writer, pool, driver, 5);

  SubmessageAckNack ack = make_acknack({0, 0}, 5, {0, 1, 2, 3, 4}, 1);

  driver.reset();
  writer.onNewAckNack(ack, make_reader_guid().prefix);
  assert(driver.send_count == 0);
}

Guid_t make_reader_guid_k(uint8_t key0) {
  Guid_t g{};
  g.prefix = GUIDPREFIX_UNKNOWN;
  g.entityId.entityKind = EntityKind_t::USER_DEFINED_READER_WITHOUT_KEY;
  g.entityId.entityKey[0] = key0;
  g.entityId.entityKey[1] = 0;
  g.entityId.entityKey[2] = 0;
  return g;
}

Locator make_loc(uint16_t port, uint8_t d) {
  Locator loc;
  loc.kind = LocatorKind_t::LOCATOR_KIND_UDPv4;
  loc.port = port;
  loc.address[12] = 10;
  loc.address[13] = 0;
  loc.address[14] = 0;
  loc.address[15] = d;
  return loc;
}

void test_acknack_numbits_zero() {
  std::cout << "[test] acknack with numBits=0, no retransmission..."
            << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;
  setup_writer_with_samples(writer, pool, driver, 5);

  SubmessageAckNack ack = make_acknack({0, 1}, 0, {}, 1);

  driver.reset();
  writer.onNewAckNack(ack, make_reader_guid().prefix);
  assert(driver.send_count == 0);
}

void test_two_readers_nack_targets_correct_reader() {
  std::cout << "[test] two readers, NACK targets correct reader..."
            << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;

  FeatureQOS qos{};

  TopicData td = make_topic();
  bool ok =
      writer.init(td, TopicKind_t::NO_KEY, &pool, driver, true , qos);
  assert(ok);
  std::this_thread::sleep_for(std::chrono::milliseconds(2));

  ReaderProxy rp1(make_reader_guid_k(1), make_loc(7500, 1));
  ReaderProxy rp2(make_reader_guid_k(2), make_loc(7501, 2));
  assert(writer.addNewMatchedReader(rp1));
  assert(writer.addNewMatchedReader(rp2));

  uint8_t payload[16];
  for (uint32_t i = 0; i < 3; ++i) {
    fill_payload(payload, sizeof(payload), i + 1);
    writer.newChange(ChangeKind_t::ALIVE, payload, sizeof(payload));
  }

  SubmessageAckNack ack{};
  ack.readerId = make_reader_guid_k(1).entityId;
  ack.count.value = 1;
  ack.readerSNState.base = {0, 1};
  ack.readerSNState.numBits = 3;
  ack.readerSNState.set(0);
  ack.readerSNState.set(1);
  ack.readerSNState.set(2);

  driver.reset();
  writer.onNewAckNack(ack, make_reader_guid_k(1).prefix);
  assert(driver.send_count == 3);
}

void test_nack_from_unknown_reader_dropped() {
  std::cout << "[test] NACK from unknown reader GUID dropped..." << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;
  setup_writer_with_samples(writer, pool, driver, 5);

  SubmessageAckNack ack{};
  ack.readerId = make_reader_guid_k(99).entityId;
  ack.count.value = 1;
  ack.readerSNState.base = {0, 1};
  ack.readerSNState.numBits = 5;
  for (uint32_t i = 0; i < 5; ++i) ack.readerSNState.set(i);

  driver.reset();
  writer.onNewAckNack(ack, make_reader_guid_k(99).prefix);
  assert(driver.send_count == 0);
}

void test_nack_after_remove_reader() {
  std::cout << "[test] NACK after removeReader, no sends..." << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;
  setup_writer_with_samples(writer, pool, driver, 5);

  writer.removeReader(make_reader_guid());

  SubmessageAckNack ack = make_acknack({0, 1}, 5, {0, 1, 2, 3, 4}, 1);

  driver.reset();
  writer.onNewAckNack(ack, make_reader_guid().prefix);
  assert(driver.send_count == 0);
}

void test_progress_multiple_readers() {
  std::cout << "[test] progress sends one packet per reader..." << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;

  FeatureQOS qos{};

  TopicData td = make_topic();
  bool ok =
      writer.init(td, TopicKind_t::NO_KEY, &pool, driver, true , qos);
  assert(ok);
  std::this_thread::sleep_for(std::chrono::milliseconds(2));

  ReaderProxy rp1(make_reader_guid_k(1), make_loc(7500, 1));
  ReaderProxy rp2(make_reader_guid_k(2), make_loc(7501, 2));
  ReaderProxy rp3(make_reader_guid_k(3), make_loc(7502, 3));
  assert(writer.addNewMatchedReader(rp1));
  assert(writer.addNewMatchedReader(rp2));
  assert(writer.addNewMatchedReader(rp3));

  uint8_t payload[8];
  fill_payload(payload, sizeof(payload), 1);
  writer.newChange(ChangeKind_t::ALIVE, payload, sizeof(payload));

  driver.reset();
  writer.progress();
  assert(driver.send_count == 3);
}

void test_progress_beyond_history_no_send() {
  std::cout << "[test] progress beyond history sends nothing..." << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;
  setup_writer_with_samples(writer, pool, driver, 2);

  driver.reset();
  writer.progress();
  writer.progress();
  assert(driver.send_count == 2);

  driver.reset();
  writer.progress();
  assert(driver.send_count == 0);
}

void test_set_all_changes_to_unsent_empty_history() {
  std::cout << "[test] setAllChangesToUnsent on empty history, no crash..."
            << std::endl;

  InlinePool pool;
  RecordingDriver driver;
  StatefulWriterT<RecordingDriver> writer;

  FeatureQOS qos{};

  TopicData td = make_topic();
  bool ok = writer.init(td, TopicKind_t::NO_KEY, &pool, driver, true, qos);
  assert(ok);
  std::this_thread::sleep_for(std::chrono::milliseconds(2));

  ReaderProxy proxy(make_reader_guid(), make_reader_loc());
  assert(writer.addNewMatchedReader(proxy));

  writer.setAllChangesToUnsent();

}

}

int main() {
  std::cout << "Running reliable loss tests..." << std::endl;
  test_all_acked_no_retransmission();
  test_single_missing_sample();
  test_first_and_last_missing();
  test_partial_loss_at_start();
  test_all_missing();
  test_duplicate_acknack_ignored();
  test_higher_count_processed();
  test_preemptive_acknack_ignored();
  test_acknack_numbits_zero();
  test_two_readers_nack_targets_correct_reader();
  test_nack_from_unknown_reader_dropped();
  test_nack_after_remove_reader();
  test_progress_multiple_readers();
  test_progress_beyond_history_no_send();
  test_set_all_changes_to_unsent_empty_history();
  std::cout << "All reliable loss tests passed." << std::endl;
  return 0;
}
