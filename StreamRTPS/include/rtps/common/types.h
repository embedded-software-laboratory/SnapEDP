/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_TYPES_H
#define RTPS_TYPES_H

#include <array>
#include <cstdint>
#include <initializer_list>

namespace rtps {

typedef uint16_t Ip4Port_t;
typedef uint16_t DataSize_t;
typedef int16_t ParticipantId_t; // int16 so more than 127 participants per host do not overflow
typedef uint64_t EventId_t;
typedef uint64_t ChangeId_t;

typedef uint32_t TransmissionPeriod_t;
const TransmissionPeriod_t INVALID_TRANSMISSION_PERIOD = 0;


enum class EntityKind_t : uint8_t {
  USER_DEFINED_UNKNOWN = 0x00,
  // No user define participant
  USER_DEFINED_WRITER_WITH_KEY = 0x02,
  USER_DEFINED_WRITER_WITHOUT_KEY = 0x03,
  USER_DEFINED_READER_WITHOUT_KEY = 0x04,
  USER_DEFINED_READER_WITH_KEY = 0x07,

  BUILD_IN_UNKNOWN = 0xc0,
  BUILD_IN_PARTICIPANT = 0xc1,
  BUILD_IN_WRITER_WITH_KEY = 0xc2,
  BUILD_IN_WRITER_WITHOUT_KEY = 0xc3,
  BUILD_IN_READER_WITHOUT_KEY = 0xc4,
  BUILD_IN_READER_WITH_KEY = 0xc7,

  VENDOR_SPEC_UNKNOWN = 0x40,
  VENDOR_SPEC_PARTICIPANT = 0x41,
  VENDOR_SPEC_WRITER_WITH_KEY = 0x42,
  VENDOR_SPEC_WRITER_WITHOUT_KEY = 0x43,
  VENDOR_SPEC_READER_WITHOUT_KEY = 0x44,
  VENDOR_SPEC_READER_WITH_KEY = 0x47
};

enum class TopicKind_t : uint8_t { NO_KEY = 1, WITH_KEY = 2 };

enum class SPDPDiscoverState : uint8_t {
  UNCONFIGURED = 1,
  CONFIGURED = 2,
  DECONFLICTING = 3,
};

enum class ChangeKind_t : uint8_t {
  INVALID,
  ALIVE,
  ALIVE_UNDELIVERED,
  PENDING,
};


enum class ReliabilityKind_t : uint32_t {
  BEST_EFFORT = 1,
  RELIABLE = 2 // Specification says 3 but eprosima sends 2
};

enum class DurabilityKind_t : uint32_t {
  VOLATILE = 0,
  TRANSIENT_LOCAL = 1,
  TRANSIENT = 2,
  PERSISTENT = 3
};

struct GuidPrefix_t {
  std::array<uint8_t, 12> id;

  bool operator==(const GuidPrefix_t &other) const {
    return this->id == other.id;
  }
};

struct EntityId_t {
  std::array<uint8_t, 3> entityKey;
  EntityKind_t entityKind;

  bool operator==(const EntityId_t &other) const {
    return this->entityKey == other.entityKey &&
           this->entityKind == other.entityKind;
  }

  bool operator!=(const EntityId_t &other) const { return !(*this == other); }
};

struct Guid_t {
  GuidPrefix_t prefix;
  EntityId_t entityId;

  bool operator==(const Guid_t &other) const {
    return this->prefix == other.prefix && this->entityId == other.entityId;
  }

  bool operator<(const Guid_t &other) const {
    if(Guid_t::sum(*this) < Guid_t::sum(other)){
      return true;
    }else{
      return false;
    }
  }

  static uint32_t sum(const Guid_t &other) {
    uint32_t ret = 0;
    for (const auto &i : other.prefix.id) {
      ret += i;
    }
    for (const auto &i : other.entityId.entityKey) {
      ret += i;
    }
    return ret;
  }
  };

struct Time_t {
  int32_t seconds;
  uint32_t fraction; // fraction of a second as 32 bit fixed point

  static Time_t create(int32_t s, uint32_t ns) {
    static constexpr double factor =
        (static_cast<uint64_t>(1) << 32) / 1000000000.;
    auto fraction = static_cast<uint32_t>(ns * factor);
    return Time_t{s, fraction};
  }

  static Time_t fromMilliseconds(int64_t ms) {
    int64_t s = ms / 1000;
    int64_t remMs = ms % 1000;
    if (remMs < 0) {
      remMs += 1000;
      --s;
    }
    const uint64_t fracUnits =
        (static_cast<uint64_t>(remMs) << 32) / 1000ULL;
    return Time_t{static_cast<int32_t>(s), static_cast<uint32_t>(fracUnits)};
  }

  int64_t toMilliseconds() const {
    const int64_t base = static_cast<int64_t>(seconds) * 1000;
    const int64_t fracMs =
        static_cast<int64_t>((static_cast<uint64_t>(fraction) * 1000ULL) >> 32);
    return base + fracMs;
  }

  Time_t addMilliseconds(int64_t deltaMs) const {
    return fromMilliseconds(toMilliseconds() + deltaMs);
  }

  bool operator==(const Time_t &other) const {
    return seconds == other.seconds && fraction == other.fraction;
  }

  bool operator!=(const Time_t &other) const { return !(*this == other); }

  bool operator<(const Time_t &other) const {
    return seconds < other.seconds ||
           (seconds == other.seconds && fraction < other.fraction);
  }

  bool operator>(const Time_t &other) const { return other < *this; }

  bool operator<=(const Time_t &other) const {
    return *this == other || *this < other;
  }

  bool operator>=(const Time_t &other) const {
    return *this == other || *this > other;
  }

  static Time_t now();
};

struct VendorId_t {
  std::array<uint8_t, 2> vendorId;
};


struct SequenceNumber_t {
  int32_t high;
  uint32_t low;

  int64_t toInt64() const {
    // shift as unsigned, left shifting a negative high is undefined in older standards
    return static_cast<int64_t>((static_cast<uint64_t>(static_cast<int64_t>(high)) << 32) | low);
  }

  static SequenceNumber_t fromInt64(int64_t v) {
    return SequenceNumber_t{static_cast<int32_t>(v >> 32),
                            static_cast<uint32_t>(v & 0xFFFFFFFF)};
  }

  bool operator==(const SequenceNumber_t &other) const {
    return high == other.high && low == other.low;
  }

  bool operator!=(const SequenceNumber_t &other) const {
    return !(*this == other);
  }

  bool operator<(const SequenceNumber_t &other) const {
    return toInt64() < other.toInt64();
  }

  bool operator>(const SequenceNumber_t &other) const {
    return toInt64() > other.toInt64();
  }

  bool operator<=(const SequenceNumber_t &other) const {
    return toInt64() <= other.toInt64();
  }

  bool operator>=(const SequenceNumber_t &other) const {
    return toInt64() >= other.toInt64();
  }

  SequenceNumber_t &operator++() {
    if (++low == 0) {
      ++high;
    }
    return *this;
  }

  SequenceNumber_t operator++(int) {
    SequenceNumber_t tmp(*this);
    ++*this;
    return tmp;
  }

  SequenceNumber_t &operator--() {
    if (low-- == 0) {
      --high;
    }
    return *this;
  }

  SequenceNumber_t operator--(int) {
    SequenceNumber_t tmp(*this);
    --*this;
    return tmp;
  }

  SequenceNumber_t &operator+=(uint32_t n) {
    uint32_t oldLow = low;
    low += n;
    if (low < oldLow) {
      ++high;
    }
    return *this;
  }

  SequenceNumber_t &operator-=(uint32_t n) {
    uint32_t oldLow = low;
    low -= n;
    if (low > oldLow) {
      --high;
    }
    return *this;
  }

  friend SequenceNumber_t operator+(SequenceNumber_t lhs, uint32_t rhs) {
    lhs += rhs;
    return lhs;
  }

  friend SequenceNumber_t operator+(uint32_t lhs, SequenceNumber_t rhs) {
    rhs += lhs;
    return rhs;
  }

  friend uint32_t operator-(const SequenceNumber_t &lhs,
                             const SequenceNumber_t &rhs) {
    return static_cast<uint32_t>(lhs.toInt64() - rhs.toInt64());
  }
};

struct SequenceNumberRange_t
{ 
  // lower is inclusive, upper is exclusive
  SequenceNumber_t upper;
  SequenceNumber_t lower;
  
  bool overlaps(const SequenceNumberRange_t& other){
    if (other.lower <= lower && other.upper >= lower){ // overlaps bottom
      return true;
    }
    if (other.lower <= lower && other.upper >= upper){ // complete overlap
      return true;
    }
    if (other.lower >= lower && other.upper <= upper){ // complete within
      return true;
    }
    if (other.lower <= upper && other.upper > upper){ // overlaps top
      return true;
    }
    return false;
  }

  bool empty(){
    return upper == lower;
  }

  bool within(const SequenceNumber_t& num){
    if (num < upper && num > lower){
      return true;
    }else{
      return false;
    }
  }
};


const uint32_t SNS_NUM_BITS = 32;
struct SequenceNumberSet {

  SequenceNumberSet() = default;
  explicit SequenceNumberSet(const SequenceNumber_t &firstMissing)
      : base(firstMissing) {}

  SequenceNumber_t base = {0, 0};
  // Cannot be static because of packed
  uint32_t numBits = SNS_NUM_BITS;
  std::array<uint32_t, 8> bitMap{};

  // only need 1 byte because atm we dont store packets
  bool isSet(uint32_t bit) const {
    if (bit >= SNS_NUM_BITS) {
      return true;
    }
    const auto bucket = static_cast<uint8_t>(bit / 32);
    const auto pos = static_cast<uint8_t>(bit % 32);
    return (bitMap[bucket] & (1 << (31 - pos))) != 0;
  }

  void setCount(){
    // adjust numBits to include the highest set bit, capped by SNS_NUM_BITS
    int32_t lastSet = -1;
    for (uint32_t bit = 0; bit < SNS_NUM_BITS; ++bit) {
      if (isSet(bit)) {
        lastSet = static_cast<int32_t>(bit);
      }
    }
    numBits = (lastSet < 0) ? 0u : static_cast<uint32_t>(lastSet + 1);
  }

  uint32_t countSet() const {
    uint32_t count = 0;
    for (uint32_t bit = 0; bit < numBits; ++bit) {
      if (isSet(bit)) ++count;
    }
    return count;
  }

  void set(){
    set(0);
  }

  void set(uint32_t bit) {
    if (bit >= SNS_NUM_BITS) {
      return;
    }
    const auto bucket = static_cast<uint8_t>(bit / 32);
    const auto pos = static_cast<uint8_t>(bit % 32);
    bitMap[bucket] |= (1u << (31 - pos));
    if (bit + 1 > numBits) {
      numBits = bit + 1;
    }
  }
};

struct FragmentNumber_t {
  uint32_t value;
};

struct Count_t {
  int32_t value;
};



struct ProtocolVersion_t {
  uint8_t major;
  uint8_t minor;
};

typedef Time_t Duration_t; // TODO

enum class ChangeForReaderStatusKind {
  UNSENT,
  UNACKNOWLEDGED,
  REQURESTED,
  ACKNOWLEDGED,
  UNDERWAY
};

enum class ChangeFromWriterStatusKind { LOST, MISSING, RECEIVED, UNKNOWN };

struct InstanceHandle_t { // TODO
  uint64_t value;
};

struct ParticipantMessageData { // TODO
};

const EntityId_t ENTITYID_UNKNOWN{};
const EntityId_t ENTITYID_BUILD_IN_PARTICIPANT = {
    {00, 00, 01}, EntityKind_t::BUILD_IN_PARTICIPANT};
const EntityId_t ENTITYID_SEDP_BUILTIN_TOPIC_WRITER = {
    {00, 00, 02}, EntityKind_t::BUILD_IN_WRITER_WITH_KEY};
const EntityId_t ENTITYID_SEDP_BUILTIN_TOPIC_READER = {
    {00, 00, 02}, EntityKind_t::BUILD_IN_READER_WITH_KEY};
const EntityId_t ENTITYID_SEDP_BUILTIN_PUBLICATIONS_WRITER = {
    {00, 00, 03}, EntityKind_t::BUILD_IN_WRITER_WITH_KEY};
const EntityId_t ENTITYID_SEDP_BUILTIN_PUBLICATIONS_READER = {
    {00, 00, 03}, EntityKind_t::BUILD_IN_READER_WITH_KEY};
const EntityId_t ENTITYID_SEDP_BUILTIN_SUBSCRIPTIONS_WRITER = {
    {00, 00, 04}, EntityKind_t::BUILD_IN_WRITER_WITH_KEY};
const EntityId_t ENTITYID_SEDP_BUILTIN_SUBSCRIPTIONS_READER = {
    {00, 00, 04}, EntityKind_t::BUILD_IN_READER_WITH_KEY};
const EntityId_t ENTITYID_SPDP_BUILTIN_PARTICIPANT_WRITER = {
    {00, 01, 00}, EntityKind_t::BUILD_IN_WRITER_WITH_KEY};
const EntityId_t ENTITYID_SPDP_BUILTIN_PARTICIPANT_READER = {
    {00, 01, 00}, EntityKind_t::BUILD_IN_READER_WITH_KEY};
const EntityId_t ENTITYID_P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER = {
    {00, 02, 00}, EntityKind_t::BUILD_IN_WRITER_WITH_KEY};
const EntityId_t ENTITYID_P2P_BUILTIN_PARTICIPANT_MESSAGE_READER = {
    {00, 02, 00}, EntityKind_t::BUILD_IN_READER_WITH_KEY};


const EntityId_t ENTITYID_SEDP_BUILTIN_ANNOUNCEMENTS_WRITER = {
    {00, 05, 00}, EntityKind_t::BUILD_IN_WRITER_WITH_KEY};
const EntityId_t ENTITYID_SEDP_BUILTIN_ANNOUNCEMENTS_READER = {
    {00, 05, 00}, EntityKind_t::BUILD_IN_READER_WITH_KEY};
const EntityId_t ENTITYID_P2P_BUILTIN_SNAP_WRITER = {
    {00, 06, 00}, EntityKind_t::BUILD_IN_WRITER_WITH_KEY};
const EntityId_t ENTITYID_P2P_BUILTIN_SNAP_READER = {
    {00, 06, 00}, EntityKind_t::BUILD_IN_READER_WITH_KEY};

const GuidPrefix_t GUIDPREFIX_UNKNOWN{};
const Guid_t GUID_UNKNOWN{};

const ParticipantId_t PARTICIPANT_ID_INVALID = -1;

const ProtocolVersion_t PROTOCOLVERSION_1_0 = {1, 0};
const ProtocolVersion_t PROTOCOLVERSION_1_1 = {1, 1};
const ProtocolVersion_t PROTOCOLVERSION_2_0 = {2, 0};
const ProtocolVersion_t PROTOCOLVERSION_2_1 = {2, 1};
const ProtocolVersion_t PROTOCOLVERSION_2_2 = {2, 2};
const ProtocolVersion_t PROTOCOLVERSION = PROTOCOLVERSION_2_2;

const SequenceNumber_t SEQUENCENUMBER_UNKNOWN = {-1, 0};

const Time_t TIME_ZERO = {};
const Time_t TIME_INVALID = {-1, 0xFFFFFFFF};
const Time_t TIME_INFINITE = {0x7FFFFFFF, 0xFFFFFFFF};

const VendorId_t VENDOR_UNKNOWN = {};
} // namespace rtps

#endif // RTPS_TYPES_H
