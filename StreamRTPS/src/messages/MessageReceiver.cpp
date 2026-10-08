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

#include "rtps/messages/MessageReceiver.h"
#include <rtps/entities/Participant.h>

#include "rtps/entities/Reader.h"
#include "rtps/entities/Writer.h"
#include "rtps/messages/MessageFactory.h"
#include "rtps/messages/MessageTypes.h"
#include "rtps/utils/Log.h"
#include "trace_control.h"

using rtps::MessageReceiver;

#if RECV_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define RECV_LOG(...) RTPS_LOG("RECV", RECV_VERBOSE, __VA_ARGS__)
#else
#define RECV_LOG(...) RTPS_TRACE_LOG_EMIT("RECV", __VA_ARGS__)
#endif

MessageReceiver::MessageReceiver(Participant *part) : mp_part(part) {}

bool MessageReceiver::processMessage(const uint8_t *data, DataSize_t size,
                                     EventId_t eventId) {
  MessageState state{};
  state.eventId = eventId;
  MessageProcessingInfo msgInfo(data, size);

  RTPS_LOG("MessageReceiver", RECV_VERBOSE, "processMessage size=%u eventId=%lu",
           static_cast<unsigned>(size), static_cast<unsigned long>(eventId));
  RTPS_TRACE_PERF_EVENT(msg_receiver_process_start, eventId,
                        static_cast<uint32_t>(size));

  RTPS_LOG("MessageReceiver", RECV_VERBOSE,
           "regular RTPS processing eventId=%lu",
           static_cast<unsigned long>(eventId));
  if (!processHeader(msgInfo, state)) {
    RTPS_LOG("MessageReceiver", RECV_VERBOSE,
             "processHeader failed eventId=%lu",
             static_cast<unsigned long>(eventId));
    return false;
  }
  SubmessageHeader submsgHeader;
  while (msgInfo.nextPos < msgInfo.size) {
    if (!deserializeMessage(msgInfo, submsgHeader)) {
      RTPS_LOG("MessageReceiver", RECV_VERBOSE,
               "submessage header deserialize failed eventId=%lu",
               static_cast<unsigned long>(eventId));
      return false;
    }
    processSubmessage(msgInfo, submsgHeader, state);
  }

  return true;
}

bool MessageReceiver::processHeader(MessageProcessingInfo &msgInfo,
                                    MessageState &state) {
  Header header;
  if (!deserializeMessage(msgInfo, header)) {
    RTPS_LOG("MessageReceiver", RECV_VERBOSE, "processHeader deserialize failed");
    return false;
  }

  // extract guid prefix for tracing
  uint64_t guid_prefix_high = 0;
  uint64_t guid_prefix_low = 0;
  for (uint16_t i = 0; i < 8 && i < header.guidPrefix.id.size(); i++) {
    guid_prefix_high |= (static_cast<uint64_t>(header.guidPrefix.id[i]) << (i * 8));
  }
  for (uint16_t i = 8; i < 12 && i < header.guidPrefix.id.size(); i++) {
    guid_prefix_low |= (static_cast<uint64_t>(header.guidPrefix.id[i]) << ((i - 8) * 8));
  }

  if (header.guidPrefix.id == mp_part->m_guidPrefix.id) {
    RECV_LOG("Received own message");
    RTPS_TRACE_PERF_EVENT(msg_receiver_process_header, state.eventId,
               guid_prefix_high, guid_prefix_low, 1, 0);
    return false; // dont process our own packet
  }

  if (header.protocolName != RTPS_PROTOCOL_NAME ||
      header.protocolVersion.major != PROTOCOLVERSION.major) {
    RTPS_LOG("MessageReceiver", RECV_VERBOSE, "wrong protocol version");
    RTPS_TRACE_PERF_EVENT(msg_receiver_process_header, state.eventId,
               guid_prefix_high, guid_prefix_low, 0, 0);
    return false;
  }

  state.sourceGuidPrefix = header.guidPrefix;
  state.sourceVendor = header.vendorId;
  state.sourceVersion = header.protocolVersion;

  msgInfo.nextPos += Header::getRawSize();

  RTPS_TRACE_PERF_EVENT(msg_receiver_process_header, state.eventId,
             guid_prefix_high, guid_prefix_low, 0, 1);
  return true;
}

bool MessageReceiver::processSubmessage(MessageProcessingInfo &msgInfo,
                                        const SubmessageHeader &submsgHeader,
                                        MessageState &state) {
  bool success = false;

  switch (submsgHeader.submessageId) {
  case SubmessageKind::ACKNACK:
    RECV_LOG("Processing AckNack submessage");
    success = processAckNackSubmessage(msgInfo, state);
    break;
  case SubmessageKind::DATA:
    RECV_LOG("Processing Data submessage");
    success = processDataSubmessage(msgInfo, submsgHeader, state);
    break;
  case SubmessageKind::HEARTBEAT:
    RECV_LOG("Processing Heartbeat submessage");
    success = processHeartbeatSubmessage(msgInfo, state);
    break;
  case SubmessageKind::INFO_DST:
    RECV_LOG("Info_DST submessage not relevant");
    success = true; // not relevant
    break;
  case SubmessageKind::INFO_TS:
    RECV_LOG("Info_TS submessage not relevant");
    success = true; // not relevant now
    break;
  default:
    RECV_LOG("Submessage type=%u not supported. Skipping",
             static_cast<uint8_t>(submsgHeader.submessageId));
    success = false;
  }
  
  RTPS_TRACE_PERF_EVENT(msg_receiver_process_submsg, state.eventId,
             static_cast<int>(submsgHeader.submessageId),
             static_cast<uint32_t>(submsgHeader.octetsToNextHeader),
             success ? 1 : 0);
  
  msgInfo.nextPos +=
      submsgHeader.octetsToNextHeader + SubmessageHeader::getRawSize();
  return success;
}

bool MessageReceiver::processDataSubmessage(
    MessageProcessingInfo &msgInfo, const SubmessageHeader &submsgHeader,
    MessageState &state) {
  SubmessageData dataSubmsg;
  if (!deserializeMessage(msgInfo, dataSubmsg)) {
    return false;
  }

  const uint8_t *serializedData =
      msgInfo.getPointerToCurrentPos() + SubmessageData::getRawSize();

  const DataSize_t size = submsgHeader.octetsToNextHeader -
                          SubmessageData::getRawSize() +
                          SubmessageHeader::getRawSize();

  RECV_LOG("Received data message size %u", (int)size);

  Reader *reader;
  if (dataSubmsg.readerId == ENTITYID_UNKNOWN) {
#if RECV_VERBOSE
    RECV_LOG("Received ENTITYID_UNKNOWN readerID; searching by writer key0=%u",
             dataSubmsg.writerId.entityKey[0]);
#endif
    reader = mp_part->getReaderByWriterId(
        Guid_t{state.sourceGuidPrefix, dataSubmsg.writerId});
    if (reader != nullptr) {
      RECV_LOG("Found reader!");
    }
  } else {
    reader = mp_part->getReader(dataSubmsg.readerId);
#if RECV_VERBOSE
    auto reader_by_writer = mp_part->getReaderByWriterId(
        Guid_t{state.sourceGuidPrefix, dataSubmsg.writerId});

    if (reader_by_writer == nullptr && reader != nullptr) {
      RECV_LOG("Found by reader ID only, writer key0=%u",
               dataSubmsg.writerId.entityKey[0]);
    }
#endif
  }
  RTPS_TRACE_PERF_EVENT(msg_receiver_process_data, state.eventId,
             dataSubmsg.writerId.entityKey[0],
             dataSubmsg.readerId.entityKey[0],
             dataSubmsg.writerSN.high,
             dataSubmsg.writerSN.low,
             static_cast<uint32_t>(size),
             reader != nullptr ? 1 : 0);

  if (reader != nullptr) {
    Guid_t writerGuid{state.sourceGuidPrefix, dataSubmsg.writerId};
    ReaderCacheChange change{ChangeKind_t::ALIVE, writerGuid,
                             dataSubmsg.writerSN, serializedData, size,
                             state.eventId};
    reader->newChange(change);
  } else {
#if RECV_VERBOSE && RTPS_GLOBAL_VERBOSE
    RECV_LOG("Couldn't find a reader with id key0=%u",
             dataSubmsg.readerId.entityKey[0]);
#endif
  }

  return true;
}


bool MessageReceiver::processHeartbeatSubmessage(
    MessageProcessingInfo &msgInfo, MessageState &state) {
  SubmessageHeartbeat submsgHB;
  if (!deserializeMessage(msgInfo, submsgHB)) {
    return false;
  }

  Reader *reader = mp_part->getReader(submsgHB.readerId);
  if (reader != nullptr) {
    reader->onNewHeartbeat(submsgHB, state.sourceGuidPrefix);
    mp_part->addHeartbeat(state.sourceGuidPrefix);
    return true;
  } else {
    return false;
  }
}


bool MessageReceiver::processAckNackSubmessage(MessageProcessingInfo &msgInfo,
                                               MessageState &state) {
  SubmessageAckNack submsgAckNack;
  if (!deserializeMessage(msgInfo, submsgAckNack)) {
    return false;
  }

  Writer *writer = mp_part->getWriter(submsgAckNack.writerId);
  if (writer != nullptr) {
    writer->onNewAckNack(submsgAckNack, state.sourceGuidPrefix);
    return true;
  } else {
    return false;
  }
}
