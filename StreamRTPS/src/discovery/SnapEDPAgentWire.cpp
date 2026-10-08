/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#include "SnapEDPAgentDetail.h"
#include "rtps/discovery/SnapEDPMessages.h"
#include "rtps/entities/Participant.h"
#include "rtps/entities/TransientWriter.h"
#include "rtps/entities/Reader.h"
#include "rtps/entities/Writer.h"
#include "rtps/messages/MessageTypes.h"
#include "rtps/utils/sysFunctions.h"
#include "ucdr/microcdr.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <random>

using rtps::SnapEDPAgent;
using namespace rtps::snap_detail;

bool SnapEDPAgent::readFrame(const ReaderCacheChange &cacheChange, const char *caller, SnapEDPMessageKind &kind, ucdrBuffer &cdrBuffer) {
  const TracePrefix self(m_part->m_guidPrefix);
  if (!cacheChange.copyInto(m_buffer, sizeof(m_buffer))) {
    RTPS_TRACE_ALL_EVENT(gossip_message_dropped, 0ull, self.hi, self.lo, DROP_COPY_FAILED);
    SEDP_LOG("%s: copyInto failed", caller);
    return false;
  }

  if (!peekSnapEDPMessageKind(m_buffer, cacheChange.size, kind)) {
    RTPS_TRACE_ALL_EVENT(gossip_message_dropped, 0ull, self.hi, self.lo, DROP_PEEK_FAILED);
    SEDP_LOG("%s: peek kind failed", caller);
    return false;
  }

  ucdr_init_buffer(&cdrBuffer, m_buffer, cacheChange.size);
  return true;
}

// Receive and process announcement
void SnapEDPAgent::receiveAnnouncement( void *callee, const ReaderCacheChange &cacheChange) {
  auto *agent = static_cast<SnapEDPAgent *>(callee);
  Lock lock{agent->m_mutex};

  // tracing
  const TracePrefix self(agent->m_part->m_guidPrefix);

  // Check type, is it an announcement?
  SnapEDPMessageKind kind;
  ucdrBuffer cdrBuffer;
  if (!agent->readFrame(cacheChange, "receiveAnnouncement", kind, cdrBuffer)) {
    return;
  }
  if (kind != SnapEDPMessageKind::ANNOUNCEMENT) {
    return;
  }

  // Deserialize message
  SnapEDPAnnouncement announcement;
  if (!announcement.readHeader(cdrBuffer)) {
    RTPS_TRACE_ALL_EVENT(gossip_message_dropped, 0ull, self.hi, self.lo, DROP_PARSE_FAILED);
    SEDP_LOG("receiveAnnouncement: ANNOUNCEMENT parse failed");
    return;
  }
  std::vector<TopicData> endpoints;
  announcement.readEndpoints(cdrBuffer, endpoints);
  for (const auto &endpoint : endpoints) {
    
    // Dispose or regular announcement
    if (announcement.disposed) {
      agent->removeRemoteEndpoint(endpoint.endpointGuid);
    } else {
      agent->receiveEndpoints(endpoint);
    }
  }

  // Update hash to what they sent us -> otherwise we might try and resync due to the oudated hash
  agent->setRemoteEndpointHash(announcement.sender, announcement.endpointHash);

  const TracePrefix sender(announcement.sender);
  RTPS_TRACE_ALL_EVENT(gossip_announcement_received, 0ull, self.hi, self.lo, static_cast<uint32_t>(endpoints.size()),
                       announcement.disposed ? 1 : 0, sender.hi, sender.lo, announcement.endpointHash);
}

// Receive Snapshot response
void SnapEDPAgent::receiveSnapshotCallback( void *callee, const ReaderCacheChange &cacheChange) {
  auto *agent = static_cast<SnapEDPAgent *>(callee);
  Lock lock{agent->m_mutex};

  // Once again tracing
  const TracePrefix self(agent->m_part->m_guidPrefix);

  // Check type
  SnapEDPMessageKind kind;
  ucdrBuffer cdrBuffer;
  if (!agent->readFrame(cacheChange, "receiveGossipCallback", kind, cdrBuffer)) {
    return;
  }

  // Can be request, or response
  switch (kind) {
  case SnapEDPMessageKind::REQUEST: {
    SnapEDPRequest request;

    if (!request.readFromUcdrBuffer(cdrBuffer)) {
      RTPS_TRACE_ALL_EVENT(gossip_message_dropped, 0ull, self.hi, self.lo, DROP_PARSE_FAILED);
      SEDP_LOG("receiveGossipCallback: REQUEST parse failed");
      return;
    }

    agent->handleRequest(request);
    break;
  }
  case SnapEDPMessageKind::SNAPSHOT: {
    SnapEDPResponse snap;

    if (!snap.readHeader(cdrBuffer)) {
      RTPS_TRACE_ALL_EVENT(gossip_message_dropped, 0ull, self.hi, self.lo, DROP_PARSE_FAILED);
      SEDP_LOG("receiveGossipCallback: GOSSIP header parse failed");
      return;
    }

    agent->handleSnapshot(snap, cdrBuffer);
    break;
  }
  case SnapEDPMessageKind::ANNOUNCEMENT:
    // should not happen, those use multicast channel
    break;
  }
}

void SnapEDPAgent::handleRequest(const SnapEDPRequest &request) {
  // only do stuff if we are the target
  if (!(request.target == m_part->m_guidPrefix)) {
    return;
  }

  const TracePrefix self(m_part->m_guidPrefix);
  const TracePrefix sender(request.sender);
  RTPS_TRACE_ALL_EVENT(gossip_request_received, 0ull, self.hi, self.lo, sender.hi, sender.lo);
  
  if (request.isReconciliation) {
    // resync requests
    SEDP_LOG("receiveGossipCallback: RESYNC REQUEST, answering own endpoints");
    sendSnapshotResponseFor(request.sender, /*ownEndpointsOnly=*/true);

  } else {
    // join requests, check if we are discovered
    SEDP_LOG("receiveGossipCallback: JOIN REQUEST from peer");
    m_snapSM->postEvent(SnapEDPEvent::RequestReceived, request.sender);
  }
}

void SnapEDPAgent::handleSnapshot(const SnapEDPResponse &snap, ucdrBuffer &cdrBuffer) {
  // is this meant for us? Should be but ignore otherwise!
  if (!(snap.target == m_part->m_guidPrefix)) {
    return;
  }

  std::vector<GuidPrefix_t> participants;
  std::vector<TopicData> endpoints;
  snap.readBody(cdrBuffer, participants, endpoints);
  const uint32_t numEndpoints = static_cast<uint32_t>(endpoints.size());

  // Read the participant infos
  for (const auto &participant : participants) {
    receiveParticipant(participant, snap.root);
  }

  // Read the endpoint info in this message
  if (snap.isResync) {
    // resync has senders own endpoints, do per owner replacement
    processResyncFrame(snap.sender, snap.resyncId, snap.totalEndpoints, endpoints);
  } else {
    // regular joins may have many particpants endpoints
    for (const auto &endpoint : endpoints) {
      receiveEndpoints(endpoint);
    }
  }

  // Tracing
  const TracePrefix self(m_part->m_guidPrefix);
  const TracePrefix sender(snap.sender);
  RTPS_TRACE_ALL_EVENT(gossip_response_received, 0ull, self.hi, self.lo, sender.hi, sender.lo, numEndpoints);
  SEDP_LOG("receiveGossipCallback: GOSSIP with %u endpoints (resync=%d)", numEndpoints, snap.isResync ? 1 : 0);
  
  // ack the responder with the newConf mark, then take its root if lower
  SnapEDPEventData response{snap.sender};
  response.rootLowered = adoptRootIfLower(snap.root);
  if (snap.sender == m_exchange.partner()) {
    m_snapSM->step(SnapEDPEvent::ResponseReceived, response);
  }
}

void SnapEDPAgent::sendEndpointBatch(const std::vector<TopicData> &endpoints,
                                        bool disposed) {
  // multicast best effort over announcementWriter
  if (m_endpoints.announcementWriter == nullptr) {
    SEDP_LOG("sendEndpointBatch: announcementWriter is null");
    return;
  }
  if (endpoints.empty()) {
    return;
  }

  const TracePrefix self(m_part->m_guidPrefix);

  SnapEDPAnnouncement ann;
  ann.disposed = disposed;
  ann.sender = m_part->m_guidPrefix;
  ann.endpointHash = m_ownEndpointHash.load(std::memory_order_relaxed);

  // cram endpoints into a frame, flush and start a fresh one when full
  SnapEDPFrameWriter frame(ann, m_buffer, sizeof(m_buffer), [this](const uint8_t *data, size_t size) {
    m_endpoints.announcementWriter->newChange(ChangeKind_t::ALIVE, data, size);
  });

  //Iteratively add endpoints, then send
  for (const auto &endpoint : endpoints) {
    frame.addEndpoint(endpoint);

    uint32_t entityIdU32 = 0;
    std::memcpy(&entityIdU32, &endpoint.endpointGuid.entityId, sizeof(endpoint.endpointGuid.entityId));
    RTPS_TRACE_ALL_EVENT(gossip_announcement_sent, 0ull, self.hi, self.lo, endpoint.topicName, entityIdU32);
  }
  // Send the last batch
  frame.finish();
}

void SnapEDPAgent::sendEndpointDispose(const TopicData &endpoint) {
  sendEndpointBatch({endpoint}, /*disposed=*/true);
}

void SnapEDPAgent::announceMyEndpoints() {
  SEDP_LOG("Announcing endpoints via multicast\n");
  sendEndpointBatch(collectLocalEndpoints(), /*disposed=*/false);
  m_unannounced.clear();
}

bool SnapEDPAgent::resolveSnapLocator(GuidPrefix_t target, Locator &out) const {
  // work on a copy due to threading
  ParticipantProxyData remoteCopy;
  const bool found = m_part->copyRemoteParticipant(target, remoteCopy);
  const ParticipantProxyData *remoteParticipant = &remoteCopy;
  const TracePrefix self(m_part->m_guidPrefix);

                
  if (!found) {
    const TracePrefix to(target);
    RTPS_TRACE_ALL_EVENT(gossip_locator_resolve_failed, 0ull, self.hi, self.lo, to.hi, to.lo);
    SEDP_LOG("resolveGossipLocator: target participant not found");
    return false;
  }

  // Choose/select the routable same subnet locator
  for (const auto &loc : remoteParticipant->m_metatrafficUnicastLocatorList) {
    if (loc.isValid() && loc.isSameSubnet()) {
      out = loc;
      return true;
    }
  }
  for (const auto &loc : remoteParticipant->m_defaultUnicastLocatorList) {
    if (loc.isValid() && loc.isSameSubnet()) {
      out = loc;
      return true;
    }
  }

  const TracePrefix to(target);
  RTPS_TRACE_ALL_EVENT(gossip_locator_resolve_failed, 0ull, self.hi, self.lo, to.hi, to.lo);
  SEDP_LOG("resolveGossipLocator: no valid locator on target");
  return false;
}

void SnapEDPAgent::dispatchSnapshotRequestTo(const GuidPrefix_t &partner, bool isResync) {
  Locator targetLocator;

  if (!resolveSnapLocator(partner, targetLocator)) {
    SEDP_LOG("dispatchGossipRequestTo: locator resolution failed");
    return;
  }

  // Serialize the request
  ucdrBuffer microbuffer;
  ucdr_init_buffer(&microbuffer, m_buffer, sizeof(m_buffer));
  SnapEDPRequest request;
  request.sender = m_part->m_guidPrefix;
  request.target = partner;
  request.isReconciliation = isResync;
  request.serializeIntoUcdrBuffer(microbuffer);

  // Send using transient writer (transient writers do not maintain a proxy list, but require a locator each time)
  static_cast<TransientWriter *>(m_endpoints.edpSnapWriter)->newChange(ChangeKind_t::ALIVE, m_buffer,
                  Guid_t{partner, ENTITYID_P2P_BUILTIN_SNAP_READER},
                  targetLocator, ucdr_buffer_length(&microbuffer));

  // again tracing stuff
  const TracePrefix self(m_part->m_guidPrefix);
  const TracePrefix to(partner);
  RTPS_TRACE_ALL_EVENT(gossip_request_sent, 0ull, self.hi, self.lo, to.hi, to.lo);
  SEDP_LOG("dispatchGossipRequestTo: dispatched");
}

bool SnapEDPAgent::sendSnapshotResponseFor(const GuidPrefix_t &target, bool ownEndpointsOnly) {
  if (m_endpoints.edpSnapWriter == nullptr) {
    SEDP_LOG("sendGossipResponseFor: edpGossipWriter is null");
    return false;
  }

  // we may be still waiting for SPDP discovery, so defer response if necessary
  Locator targetLocator;
  if (!resolveSnapLocator(target, targetLocator)) {
    SEDP_LOG("sendGossipResponseFor: locator resolution failed, deferring");
    deferRequester(target);
    return false;
  }

  // Unicast using transient writer straight to this gossip reader
  auto *tw = static_cast<TransientWriter *>(m_endpoints.edpSnapWriter);
  const Guid_t targetReader{target, ENTITYID_P2P_BUILTIN_SNAP_READER};

  // Ack Participant if necessary
  Participant::RemoteSnapView peer;
  if (m_part->getRemoteSnapView(target, peer)) {
    const bool peerRootKnown = !(peer.root == GUIDPREFIX_UNKNOWN);
    const bool selfRootKnown = !(m_root.get() == GUIDPREFIX_UNKNOWN);
    if (!(peerRootKnown && selfRootKnown && peer.root.id < m_root.get().id)) {
      m_peers.markAcked(target, AckState::AckedConfigured);
    }
  } else {
    m_peers.markAcked(target, AckState::AckedConfigured);
  }

  // Build response message
  SnapEDPResponse snap;
  snap.sender = m_part->m_guidPrefix;
  snap.target = target;
  snap.root = m_root.get();
  snap.isResync = ownEndpointsOnly;
  if (ownEndpointsOnly) {
    snap.resyncId = m_resync.nextId();
  }

  // resync answers with the own endpoints only, join request gets snapshot plus view
  std::vector<GuidPrefix_t> participants;
  std::vector<TopicData> endpoints = collectLocalEndpoints();
  if (!ownEndpointsOnly) {
    for (const auto &remote : m_part->getRemoteSnapViews()) {
      if (!(remote.prefix == target)) {
        participants.push_back(remote.prefix);
      }
    }
    for (const auto &data : m_remote.writers()) {
      if (!(data.endpointGuid.prefix == target)) {
        endpoints.push_back(data);
      }
    }
    for (const auto &data : m_remote.readers()) {
      if (!(data.endpointGuid.prefix == target)) {
        endpoints.push_back(data);
      }
    }
  }
  snap.totalEndpoints = static_cast<uint32_t>(endpoints.size());

  // pack participants then endpoints into one udp, when full flush
  SnapEDPFrameWriter frame(snap, m_buffer, sizeof(m_buffer), [&](const uint8_t *data, size_t size) {
    tw->newChange(ChangeKind_t::ALIVE, data, targetReader, targetLocator, size);
  });
  for (const auto &participant : participants) {
    frame.addParticipant(participant);
  }
  for (const auto &endpoint : endpoints) {
    frame.addEndpoint(endpoint);
  }
  frame.finish();

  const TracePrefix self(m_part->m_guidPrefix);
  const TracePrefix to(target);
  RTPS_TRACE_ALL_EVENT(gossip_response_sent, 0ull, self.hi, self.lo, to.hi, to.lo, static_cast<uint32_t>(endpoints.size()));
  SEDP_LOG("sendGossipResponseFor: %u participants, %u endpoints sent", static_cast<unsigned>(participants.size()), static_cast<unsigned>(endpoints.size()));
  return true;
}
