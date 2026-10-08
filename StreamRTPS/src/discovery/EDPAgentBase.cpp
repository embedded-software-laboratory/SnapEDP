/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#include "rtps/discovery/EDPAgentBase.h"
#include "rtps/entities/Participant.h"
#include "rtps/entities/Reader.h"
#include "rtps/entities/Writer.h"
#include "rtps/utils/Log.h"
#include "trace_control.h"

using rtps::EDPAgentBase;

#if SEDP_VERBOSE && RTPS_GLOBAL_VERBOSE
#define SEDP_LOG(...) RTPS_LOG("SEDP", SEDP_VERBOSE, __VA_ARGS__)
#else
#define SEDP_LOG(...) RTPS_TRACE_LOG_EMIT("SEDP", __VA_ARGS__)
#endif

void EDPAgentBase::init(Participant &part, const BuiltInEndpoints &endpoints) {
  if (mutex_new(&m_mutex) != EMB_ERR_OK) {
    SEDP_LOG("SEDPAgentBase failed to create mutex\n");
    return;
  }
  m_part = &part;
  m_endpoints = endpoints;
}

void EDPAgentBase::registerOnNewPublisherMatchedCallback(
    void (*callback)(void *arg), void *args) {
  mfp_onNewPublisherCallback = callback;
  m_onNewPublisherArgs = args;
}

void EDPAgentBase::registerOnNewSubscriberMatchedCallback(
    void (*callback)(void *arg), void *args) {
  mfp_onNewSubscriberCallback = callback;
  m_onNewSubscriberArgs = args;
}

void EDPAgentBase::onAddProxiesForRemoteParticipant(
    const ParticipantProxyData &proxyData, const Locator &locator) {
  // guard the unmatched vectors, the SEDP data path also holds m_mutex
  Lock lock{m_mutex};

  if (proxyData.hasPublicationReader() &&
      m_endpoints.sedpPubWriter != nullptr) {
    const ReaderProxy proxy{{proxyData.m_guid.prefix,
                             ENTITYID_SEDP_BUILTIN_PUBLICATIONS_READER},
                            locator};
    m_endpoints.sedpPubWriter->addNewMatchedReader(proxy);
  }
  if (proxyData.hasSubscriptionReader() &&
      m_endpoints.sedpSubWriter != nullptr) {
    const ReaderProxy proxy{{proxyData.m_guid.prefix,
                             ENTITYID_SEDP_BUILTIN_SUBSCRIPTIONS_READER},
                            locator};
    m_endpoints.sedpSubWriter->addNewMatchedReader(proxy);
  }

  // a late SPDP can supply the peer for already buffered SEDP, retry the backlog to fix
  tryMatchUnmatchedEndpoints();
}

void EDPAgentBase::addUnmatchedRemoteWriter(const TopicData &writerData) {
  SEDP_LOG("Adding unmatched remote writer %s %s.\n", writerData.topicName,
           writerData.typeName);
  m_unmatchedRemoteWriters.push_back(TopicDataCompressed(writerData));
}

void EDPAgentBase::addUnmatchedRemoteReader(const TopicData &readerData) {
  SEDP_LOG("Adding unmatched remote reader %s %s.\n", readerData.topicName,
           readerData.typeName);
  m_unmatchedRemoteReaders.push_back(TopicDataCompressed(readerData));
}

void EDPAgentBase::onNewPublisher(const TopicData &writerData,
                                    EventId_t eventId) {
  RTPS_TRACE_ALL_EVENT(sedp_new_publisher, eventId, writerData.topicName,
                       writerData.endpointGuid.entityId.entityKey[0]);

  // SEDP can race ahead of the participants SPDP, dropping it would lose the endpoint forever, so match or buffer
  if (m_part == nullptr) {
    return;
  }
  SEDP_LOG("PUB T/D %s/%s", writerData.topicName, writerData.typeName);
  Reader *reader = m_part->getMatchingReader(writerData);
  if (reader == nullptr) {
    SEDP_LOG("SEDPAgent: Couldn't find reader for new Publisher[%s, %s] \n",
             writerData.topicName, writerData.typeName);
    addUnmatchedRemoteWriter(writerData);
  } else {
    reader->addNewMatchedWriter(
        WriterProxy{writerData.endpointGuid, writerData.unicastLocator});
    if (mfp_onNewPublisherCallback != nullptr) {
      mfp_onNewPublisherCallback(m_onNewPublisherArgs);
    }
  }

  // a receive can also unblock endpoints left unmatched from an earlier receipt, retry the backlog
  tryMatchUnmatchedEndpoints();
}

void EDPAgentBase::onNewSubscriber(const TopicData &readerData,
                                     EventId_t eventId) {
  RTPS_TRACE_ALL_EVENT(sedp_new_subscriber, eventId, readerData.topicName,
                       readerData.endpointGuid.entityId.entityKey[0]);

  // see onNewPublisher, SEDP can arrive before SPDP so match or buffer instead of dropping
  if (m_part == nullptr) {
    return;
  }

  Writer *writer = m_part->getMatchingWriter(readerData);
  SEDP_LOG("SUB T/D %s/%s", readerData.topicName, readerData.typeName);
  if (writer == nullptr) {
    SEDP_LOG("SEDPAgent: Couldn't find writer for new subscriber[%s, %s]\n",
             readerData.topicName, readerData.typeName);
    addUnmatchedRemoteReader(readerData);
  } else if (readerData.multicastLocator.kind ==
             rtps::LocatorKind_t::LOCATOR_KIND_UDPv4) {
    writer->addNewMatchedReader(ReaderProxy{readerData.endpointGuid,
                                            readerData.unicastLocator,
                                            readerData.multicastLocator});
    if (mfp_onNewSubscriberCallback != nullptr) {
      mfp_onNewSubscriberCallback(m_onNewSubscriberArgs);
    }
  } else {
    writer->addNewMatchedReader(
        ReaderProxy{readerData.endpointGuid, readerData.unicastLocator});
    if (mfp_onNewSubscriberCallback != nullptr) {
      mfp_onNewSubscriberCallback(m_onNewSubscriberArgs);
    }
  }

  // a receive can also unblock endpoints left unmatched from an earlier receipt, retry the backlog
  tryMatchUnmatchedEndpoints();
}

void EDPAgentBase::tryMatchUnmatchedEndpoints() {
  // match remote readers with local writers, drop the ones matched
  for (auto it = m_unmatchedRemoteReaders.begin();
       it != m_unmatchedRemoteReaders.end();) {
    auto writer = m_part->getMatchingWriter(*it);
    if (writer != nullptr) {
      writer->addNewMatchedReader(ReaderProxy{
          it->endpointGuid, it->unicastLocator, it->multicastLocator});
      it = m_unmatchedRemoteReaders.erase(it);
    } else {
      ++it;
    }
  }

  // match remote writers with local readers, drop the ones matched
  for (auto it = m_unmatchedRemoteWriters.begin();
       it != m_unmatchedRemoteWriters.end();) {
    auto reader = m_part->getMatchingReader(*it);
    if (reader != nullptr) {
      reader->addNewMatchedWriter(
          WriterProxy{it->endpointGuid, it->unicastLocator});
      it = m_unmatchedRemoteWriters.erase(it);
    } else {
      ++it;
    }
  }
}
