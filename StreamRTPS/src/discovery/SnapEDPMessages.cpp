/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#include "rtps/discovery/SnapEDPMessages.h"
#include "rtps/messages/MessageTypes.h"

namespace rtps {

namespace {

bool writeEncapsulation(ucdrBuffer &buf) {
  ucdr_serialize_array_uint8_t(&buf,
                               SMElement::SCHEME_PL_CDR_LE.data(),
                               SMElement::SCHEME_PL_CDR_LE.size());
  ucdr_serialize_uint16_t(&buf, 0);
  return true;
}

bool readEncapsulation(ucdrBuffer &buf) {
  uint8_t scheme[2];
  uint16_t options;
  ucdr_deserialize_array_uint8_t(&buf, scheme, 2);
  ucdr_deserialize_uint16_t(&buf, &options);
  return true;
}

bool writeKind(ucdrBuffer &buf, SnapEDPMessageKind kind) {
  ucdr_serialize_uint32_t(&buf, static_cast<uint32_t>(kind));
  return true;
}

bool readKind(ucdrBuffer &buf, SnapEDPMessageKind &kind) {
  uint32_t raw = 0;
  ucdr_deserialize_uint32_t(&buf, &raw);
  kind = static_cast<SnapEDPMessageKind>(raw);
  return true;
}

bool writePrefix(ucdrBuffer &buf, const GuidPrefix_t &prefix) {
  ucdr_serialize_array_uint8_t(&buf, prefix.id.data(), prefix.id.size());
  return true;
}

bool readPrefix(ucdrBuffer &buf, GuidPrefix_t &out) {
  ucdr_deserialize_array_uint8_t(&buf, out.id.data(), out.id.size());
  return true;
}

constexpr size_t kSnapChannelHeaderBytes = 4 + 4;

} // namespace

bool SnapEDPAnnouncement::serializeHeader(ucdrBuffer &buf) const {
  writeEncapsulation(buf);
  writeKind(buf, SnapEDPMessageKind::ANNOUNCEMENT);
  ucdr_serialize_uint64_t(&buf, endpointHash);
  writePrefix(buf, sender);
  ucdr_serialize_uint32_t(&buf, disposed ? 1 : 0);
  return ucdr_serialize_uint32_t(&buf, numEndpoints);
}

bool SnapEDPAnnouncement::serializeAppendEndpoint(ucdrBuffer &buf,
                                               const TopicData &ep) const {
  const size_t estimate = 128 + sizeof(ep.topicName) + sizeof(ep.typeName) + 128;
  if (ucdr_buffer_remaining(&buf) < estimate) {
    return false;
  }
  return ep.serializeIntoUcdrBuffer(buf);
}

bool SnapEDPAnnouncement::readHeader(ucdrBuffer &buf) {
  readEncapsulation(buf);
  SnapEDPMessageKind kind;
  readKind(buf, kind);
  if (kind != SnapEDPMessageKind::ANNOUNCEMENT) {
    return false;
  }
  ucdr_deserialize_uint64_t(&buf, &endpointHash);
  readPrefix(buf, sender);
  uint32_t disposedFlag = 0;
  ucdr_deserialize_uint32_t(&buf, &disposedFlag);
  disposed = (disposedFlag != 0);
  return ucdr_deserialize_uint32_t(&buf, &numEndpoints);
}

bool SnapEDPAnnouncement::readNextEndpoint(ucdrBuffer &buf, TopicData &out) {
  if (ucdr_buffer_remaining(&buf) == 0) {
    return false;
  }
  return out.readFromUcdrBuffer(buf);
}

void SnapEDPAnnouncement::readEndpoints(ucdrBuffer &buf, std::vector<TopicData> &out) const {
  for (uint32_t i = 0; i < numEndpoints; ++i) {
    TopicData endpoint;
    if (!readNextEndpoint(buf, endpoint)) {
      break;
    }
    out.push_back(endpoint);
  }
}

void SnapEDPAnnouncement::patchCounts(uint8_t *headerEnd, uint32_t, uint32_t numEndpoints) {
  std::memcpy(headerEnd - 4, &numEndpoints, sizeof(numEndpoints));
}

bool SnapEDPRequest::serializeIntoUcdrBuffer(ucdrBuffer &buf) const {
  writeEncapsulation(buf);
  writeKind(buf, SnapEDPMessageKind::REQUEST);
  writePrefix(buf, sender);
  writePrefix(buf, target);
  return ucdr_serialize_uint8_t(&buf, isReconciliation ? 1 : 0);
}

bool SnapEDPRequest::readFromUcdrBuffer(ucdrBuffer &buf) {
  readEncapsulation(buf);
  SnapEDPMessageKind kind;
  readKind(buf, kind);
  if (kind != SnapEDPMessageKind::REQUEST) {
    return false;
  }
  readPrefix(buf, sender);
  readPrefix(buf, target);
  uint8_t resyncFlag = 0;
  const bool ok = ucdr_deserialize_uint8_t(&buf, &resyncFlag);
  isReconciliation = (resyncFlag != 0);
  return ok;
}


bool SnapEDPResponse::serializeHeader(ucdrBuffer &buf) const {
  writeEncapsulation(buf);
  writeKind(buf, SnapEDPMessageKind::SNAPSHOT);
  writePrefix(buf, sender);
  writePrefix(buf, target);
  writePrefix(buf, root);
  ucdr_serialize_uint32_t(&buf, isResync ? (resyncId != 0 ? resyncId : 1u) : 0u);
  ucdr_serialize_uint32_t(&buf, totalEndpoints);
  ucdr_serialize_uint32_t(&buf, numParticipants);
  return ucdr_serialize_uint32_t(&buf, numEndpoints);
}

bool SnapEDPResponse::serializeAppendParticipant(ucdrBuffer &buf,
                                              const GuidPrefix_t &prefix) const {
  if (ucdr_buffer_remaining(&buf) < prefix.id.size()) {
    return false;
  }
  return writePrefix(buf, prefix);
}

bool SnapEDPResponse::serializeAppendEndpoint(ucdrBuffer &buf,
                                         const TopicData &ep) const {
  const size_t estimate = 128 + sizeof(ep.topicName) + sizeof(ep.typeName) + 128;
  if (ucdr_buffer_remaining(&buf) < estimate) {
    return false;
  }
  return ep.serializeIntoUcdrBuffer(buf);
}

bool SnapEDPResponse::readHeader(ucdrBuffer &buf) {
  readEncapsulation(buf);
  SnapEDPMessageKind kind;
  readKind(buf, kind);
  if (kind != SnapEDPMessageKind::SNAPSHOT) {
    return false;
  }
  readPrefix(buf, sender);
  readPrefix(buf, target);
  readPrefix(buf, root);
  uint32_t resyncFlag = 0;
  ucdr_deserialize_uint32_t(&buf, &resyncFlag);
  isResync = (resyncFlag != 0);
  resyncId = resyncFlag;
  ucdr_deserialize_uint32_t(&buf, &totalEndpoints);
  ucdr_deserialize_uint32_t(&buf, &numParticipants);
  return ucdr_deserialize_uint32_t(&buf, &numEndpoints);
}

bool SnapEDPResponse::readNextParticipant(ucdrBuffer &buf, GuidPrefix_t &out) {
  if (ucdr_buffer_remaining(&buf) < out.id.size()) {
    return false;
  }
  return readPrefix(buf, out);
}

bool SnapEDPResponse::readNextEndpoint(ucdrBuffer &buf, TopicData &out) {
  if (ucdr_buffer_remaining(&buf) == 0) {
    return false;
  }
  return out.readFromUcdrBuffer(buf);
}

void SnapEDPResponse::readBody(ucdrBuffer &buf, std::vector<GuidPrefix_t> &participants, std::vector<TopicData> &endpoints) const {
  for (uint32_t i = 0; i < numParticipants; ++i) {
    GuidPrefix_t participant{};
    if (!readNextParticipant(buf, participant)) {
      break;
    }
    participants.push_back(participant);
  }
  for (uint32_t i = 0; i < numEndpoints; ++i) {
    TopicData endpoint;
    if (!readNextEndpoint(buf, endpoint)) {
      break;
    }
    endpoints.push_back(endpoint);
  }
}

void SnapEDPResponse::patchCounts(uint8_t *headerEnd, uint32_t numParticipants, uint32_t numEndpoints) {
  std::memcpy(headerEnd - 8, &numParticipants, sizeof(numParticipants));
  std::memcpy(headerEnd - 4, &numEndpoints, sizeof(numEndpoints));
}

bool peekSnapEDPMessageKind(const uint8_t *buffer, size_t size,
                               SnapEDPMessageKind &out) {
  if (size < kSnapChannelHeaderBytes) {
    return false;
  }
  uint32_t raw = static_cast<uint32_t>(buffer[4]) |
                 (static_cast<uint32_t>(buffer[5]) << 8) |
                 (static_cast<uint32_t>(buffer[6]) << 16) |
                 (static_cast<uint32_t>(buffer[7]) << 24);
  out = static_cast<SnapEDPMessageKind>(raw);
  return true;
}

} // namespace rtps
