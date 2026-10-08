/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#include "rtps/discovery/SEDPAgent.h"
#include "rtps/discovery/TopicData.h"
#include "rtps/entities/Participant.h"
#include "rtps/entities/Reader.h"
#include "rtps/entities/Writer.h"
#include "rtps/messages/MessageTypes.h"
#include "rtps/utils/Log.h"
#include "ucdr/microcdr.h"
#include "trace_control.h"

using rtps::SEDPAgent;

#if SEDP_VERBOSE && RTPS_GLOBAL_VERBOSE
#define SEDP_LOG(...) RTPS_LOG("SEDP", SEDP_VERBOSE, __VA_ARGS__)
#else
#define SEDP_LOG(...) RTPS_TRACE_LOG_EMIT("SEDP", __VA_ARGS__)
#endif

void SEDPAgent::init(Participant &part, const BuiltInEndpoints &endpoints) {
  
  // Init subclass
  EDPAgentBase::init(part, endpoints);
  
  if (m_endpoints.sedpPubReader != nullptr) {
    m_endpoints.sedpPubReader->registerCallback(receiveCallbackPublisher, this);
  }
  if (m_endpoints.sedpSubReader != nullptr) {
    m_endpoints.sedpSubReader->registerCallback(receiveCallbackSubscriber,
                                                this);
  }
}

void SEDPAgent::receiveCallbackPublisher(void *callee, const ReaderCacheChange &cacheChange) {
  auto *agent = static_cast<SEDPAgent *>(callee);
  agent->onNewPublisherFromChange(cacheChange);
}

void SEDPAgent::receiveCallbackSubscriber( void *callee, const ReaderCacheChange &cacheChange) {
  auto *agent = static_cast<SEDPAgent *>(callee);
  agent->onNewSubscriberFromChange(cacheChange);
}

void SEDPAgent::onNewPublisherFromChange(const ReaderCacheChange &change) {
  Lock lock{m_mutex};
  SEDP_LOG("New publisher\n");

  if (!change.copyInto(m_buffer, sizeof(m_buffer) / sizeof(m_buffer[0]))) {
    SEDP_LOG("EDPAgent: Buffer too small.\n");
    return;
  }
  ucdrBuffer cdrBuffer;
  ucdr_init_buffer(&cdrBuffer, m_buffer, sizeof(m_buffer));

  TopicData topicData;
  if (topicData.readFromUcdrBuffer(cdrBuffer)) {
    onNewPublisher(topicData, change.eventId);
  } else {
    SEDP_LOG("readFromUcdrBuffer failed for publisher data");
  }
}

void SEDPAgent::onNewSubscriberFromChange(const ReaderCacheChange &change) {
  Lock lock{m_mutex};
  SEDP_LOG("New subscriber\n");

  if (!change.copyInto(m_buffer, sizeof(m_buffer) / sizeof(m_buffer[0]))) {
    SEDP_LOG("SEDPAgent: Buffer too small.");
    return;
  }
  ucdrBuffer cdrBuffer;
  ucdr_init_buffer(&cdrBuffer, m_buffer, sizeof(m_buffer));

  TopicData topicData;
  if (topicData.readFromUcdrBuffer(cdrBuffer)) {
    onNewSubscriber(topicData, change.eventId);
  } else {
    SEDP_LOG("readFromUcdrBuffer failed for subscriber data");
  }
}

void SEDPAgent::addWriter(Writer &writer) {
  if (m_endpoints.sedpPubWriter == nullptr) {
    return;
  }

  EntityKind_t writerKind = writer.m_attributes.endpointGuid.entityId.entityKind;
  if (writerKind == EntityKind_t::BUILD_IN_WRITER_WITH_KEY ||
      writerKind == EntityKind_t::BUILD_IN_WRITER_WITHOUT_KEY) {
    return; // No need to announce builtin endpoints
  }

  SEDP_LOG("Beginning SEDP Disc for %s\n", writer.m_attributes.topicName);
  RTPS_TRACE_ALL_EVENT(sedp_add_writer, 0, writer.m_attributes.topicName,
                       writer.m_attributes.endpointGuid.entityId.entityKey[0]);

  Lock lock{m_mutex};

  tryMatchUnmatchedEndpoints();

  ucdrBuffer microbuffer;
  ucdr_init_buffer(&microbuffer, m_buffer,
                   sizeof(m_buffer) / sizeof(m_buffer[0]));
  const uint16_t zero_options = 0;

  ucdr_serialize_array_uint8_t(&microbuffer,
                               rtps::SMElement::SCHEME_PL_CDR_LE.data(),
                               rtps::SMElement::SCHEME_PL_CDR_LE.size());
  ucdr_serialize_uint16_t(&microbuffer, zero_options);
  writer.m_attributes.serializeIntoUcdrBuffer(microbuffer);
  m_endpoints.sedpPubWriter->newChange(ChangeKind_t::ALIVE, m_buffer,
                                       ucdr_buffer_length(&microbuffer));
  SEDP_LOG("Added new change to sedpPubWriter.\n");
}

void SEDPAgent::addReader(Reader &reader) {
  if (m_endpoints.sedpSubWriter == nullptr) {
    return;
  }

  EntityKind_t readerKind =
      reader.m_attributes.endpointGuid.entityId.entityKind;
  if (readerKind == EntityKind_t::BUILD_IN_READER_WITH_KEY ||
      readerKind == EntityKind_t::BUILD_IN_READER_WITHOUT_KEY) {
    return; // No need to announce builtin endpoints
  }

  RTPS_TRACE_ALL_EVENT(sedp_add_reader, 0, reader.m_attributes.topicName,
                       reader.m_attributes.endpointGuid.entityId.entityKey[0]);

  Lock lock{m_mutex};

  tryMatchUnmatchedEndpoints();

  ucdrBuffer microbuffer;
  ucdr_init_buffer(&microbuffer, m_buffer,
                   sizeof(m_buffer) / sizeof(m_buffer[0]));
  const uint16_t zero_options = 0;

  ucdr_serialize_array_uint8_t(&microbuffer,
                               rtps::SMElement::SCHEME_PL_CDR_LE.data(),
                               rtps::SMElement::SCHEME_PL_CDR_LE.size());
  ucdr_serialize_uint16_t(&microbuffer, zero_options);
  reader.m_attributes.serializeIntoUcdrBuffer(microbuffer);
  m_endpoints.sedpSubWriter->newChange(ChangeKind_t::ALIVE, m_buffer,
                                       ucdr_buffer_length(&microbuffer));
  SEDP_LOG("Added new change to sedpSubWriter.\n");
}

void SEDPAgent::removeWriter(Writer &writer) {
  (void)writer;
}

void SEDPAgent::removeReader(Reader &reader) {
  (void)reader;
}
