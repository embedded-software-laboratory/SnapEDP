/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_SNAPEDPMESSAGES_H
#define RTPS_SNAPEDPMESSAGES_H

#include "rtps/common/types.h"
#include "rtps/discovery/TopicData.h"
#include "ucdr/microcdr.h"

#include <cstdint>
#include <cstring>
#include <vector>

namespace rtps {

// Type Enum to allow deserializer to distinguish by type
enum class SnapEDPMessageKind : uint32_t {
  REQUEST      = 1,
  ANNOUNCEMENT = 2,
  SNAPSHOT     = 3,
};

// SnapEDP Announcement Message Struct 
struct SnapEDPAnnouncement {
  
  // GUID of participant of endpoints in the frame
  GuidPrefix_t sender{};
  // sender endpoint hash after applying this announcement
  uint64_t endpointHash = 0;
  // Is dispose or regular update?
  bool disposed = false;
  // Enclosed endpoint count
  uint32_t numEndpoints = 0;

  // Serialize/Deserialize
  bool serializeHeader(ucdrBuffer &buf) const;
  bool serializeAppendEndpoint(ucdrBuffer &buf, const TopicData &ep) const;
  bool readHeader(ucdrBuffer &buf);
  static bool readNextEndpoint(ucdrBuffer &buf, TopicData &out);
  void readEndpoints(ucdrBuffer &buf, std::vector<TopicData> &out) const;
  static void patchCounts(uint8_t *headerEnd, uint32_t numParticipants, uint32_t numEndpoints);
};

// SnapEDP Request Message Struct 
struct SnapEDPRequest {
  GuidPrefix_t sender{};
  GuidPrefix_t target{};
  bool isReconciliation = false;
  bool serializeIntoUcdrBuffer(ucdrBuffer &buf) const;
  bool readFromUcdrBuffer(ucdrBuffer &buf);
};


// SnapEDP Response Message Struct
struct SnapEDPResponse {
  GuidPrefix_t sender{};
  GuidPrefix_t target{};
  GuidPrefix_t root{};
  bool isResync = false;

  // Resync ID and endpoint counts, total endpoints may be split accross multiple messages
  uint32_t resyncId{0};
  uint32_t totalEndpoints{0};
  uint32_t numParticipants{0};
  uint32_t numEndpoints{0};

  // Serialize/Deserialize iteratively
  bool serializeHeader(ucdrBuffer &buf) const;
  bool serializeAppendParticipant(ucdrBuffer &buf, const GuidPrefix_t &prefix) const;
  bool serializeAppendEndpoint(ucdrBuffer &buf, const TopicData &ep) const;
  bool readHeader(ucdrBuffer &buf);
  static bool readNextParticipant(ucdrBuffer &buf, GuidPrefix_t &out);
  static bool readNextEndpoint(ucdrBuffer &buf, TopicData &out);
  void readBody(ucdrBuffer &buf, std::vector<GuidPrefix_t> &participants, std::vector<TopicData> &endpoints) const;
  static void patchCounts(uint8_t *headerEnd, uint32_t numParticipants, uint32_t numEndpoints);
};

template <class Msg, class Sink>
class SnapEDPFrameWriter {
public:
  SnapEDPFrameWriter(const Msg &msg, uint8_t *buffer, size_t capacity, Sink sink)
      : m_msg(msg), m_buffer(buffer), m_capacity(capacity), m_sink(sink) {
    open();
  }

  void addParticipant(const GuidPrefix_t &prefix) {
    if (!m_msg.serializeAppendParticipant(m_cdr, prefix)) {
      flush();
      open();
      m_msg.serializeAppendParticipant(m_cdr, prefix);
    }
    ++m_numParticipants;
  }

  void addEndpoint(const TopicData &ep) {
    if (!m_msg.serializeAppendEndpoint(m_cdr, ep)) {
      flush();
      open();
      m_msg.serializeAppendEndpoint(m_cdr, ep);
    }
    ++m_numEndpoints;
  }

  void finish() { flush(); }

private:
  void open() {
    ucdr_init_buffer(&m_cdr, m_buffer, m_capacity);
    m_msg.serializeHeader(m_cdr);
    m_headerLength = ucdr_buffer_length(&m_cdr);
    m_numParticipants = 0;
    m_numEndpoints = 0;
  }

  void flush() {
    Msg::patchCounts(m_buffer + m_headerLength, m_numParticipants, m_numEndpoints);
    m_sink(m_buffer, ucdr_buffer_length(&m_cdr));
  }

  const Msg &m_msg;
  uint8_t *m_buffer;
  size_t m_capacity;
  Sink m_sink;
  ucdrBuffer m_cdr;
  size_t m_headerLength = 0;
  uint32_t m_numParticipants = 0;
  uint32_t m_numEndpoints = 0;
};

// Peek into incoming buffer and determine SnapEDP message type
bool peekSnapEDPMessageKind(const uint8_t *buffer, size_t size, SnapEDPMessageKind &out);

} // namespace rtps

#endif // RTPS_SEDPGOSSIPMESSAGES_H
