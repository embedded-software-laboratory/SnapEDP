/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_SNAPEDPAGENT_H
#define RTPS_SNAPEDPAGENT_H

#include "rtps/ThreadPool.h"
#include "rtps/discovery/EDPAgentBase.h"
#include "rtps/discovery/SnapEDPEndpointStore.h"
#include "rtps/discovery/SnapEDPExchange.h"
#include "rtps/discovery/SnapEDPMessages.h"
#include "rtps/discovery/SnapEDPPeerTable.h"
#include "rtps/discovery/SnapEDPResync.h"
#include "rtps/discovery/SnapEDPRoot.h"
#include "rtps/discovery/SnapEDPTypes.h"
#include "rtps/utils/FSM.h"
#include "rtps/utils/FixedMap.h"
#include "rtps/utils/FixedSet.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <random>
#include <vector>

namespace rtps {

class SnapEDPAgent;
using SnapEDPFSM = FSM<SnapEDPAgent, SnapEDPState, SnapEDPEvent, SnapEDPEventData>;

class SnapEDPAgent : public EDPAgentBase, public TimedWork {

public:
  // Start, init, stop the agend
  void init(Participant &part, const BuiltInEndpoints &endpoints) override;
  void start() override;
  void stop() override;

  // Add or remove writer and reader
  void addWriter(Writer &writer) override;
  void addReader(Reader &reader) override;
  void removeWriter(Writer &writer) override;
  void removeReader(Reader &reader) override;

  // Callbacks for some events
  void onRemoteParticipantDiscovered(const GuidPrefix_t &prefix, const SPDPDiscoverState& state) override;
  void onRemoteParticipantRemoved(const GuidPrefix_t &prefix) override;
  void onAddProxiesForRemoteParticipant(const ParticipantProxyData &proxyData, const Locator &locator) override;

  // Periodic anti entropic sweep, operates on a shared timer, not directly modeled 
  void onTimer() override;

  // Returns count of currently configured, same root participants, but basically unused
  uint32_t getJoinedPeerCounts() override;

  // Returns current root
  GuidPrefix_t getCurrentRoot() const override { return m_root.getPublished(); }

  // Returns current endpoint hash over own endpoitns
  uint64_t getEndpointHash() const override { return m_ownEndpointHash.load(std::memory_order_relaxed); }

  // Full local view hash, own endpoints XOR all known remote endpoints
  uint64_t getLocalViewHash() const;

  // FSM Monitoring functions, onFSMTransition is used for monitoring
  void onFSMTransition(SnapEDPState from, SnapEDPEvent event, SnapEDPState to);
  SnapEDPState getCurrentState();

  //Debugging to access current complete state, used for debugging
  void dumpDiscoveryState(uint32_t snapshot_id, bool is_final) override;

private:
  // Gossip Callback
  static void receiveSnapshotCallback(void *callee, const ReaderCacheChange &cacheChange);
  static void receiveAnnouncement( void *callee, const ReaderCacheChange &cacheChange);
  bool readFrame(const ReaderCacheChange &cacheChange, const char *caller, SnapEDPMessageKind &kind, ucdrBuffer &cdrBuffer);
  void handleRequest(const SnapEDPRequest &request);
  void handleSnapshot(const SnapEDPResponse &snap, ucdrBuffer &cdrBuffer);

  // FSM transition functions
  void runElection();
  void startJoin();
  void claimRoot();
  void backOffAfterElection();
  void finishJoin();
  void retryJoin();
  void giveUpJoin();
  void announce();
  void deferJoin();
  void serveJoin();
  void adoptNewRoot();
  void queueResync();
  void finishResync();
  void retryResync();
  void becomeDiscovered();

  // FSM entry points for the ones above
  void enterReconcile();
  void enterDiscovered();
  void armInitialTimeout(uint32_t factor);

  // completeness gate, pick a missing endpoint target and pull from it
  void selectNextResync();
  std::vector<GuidPrefix_t> incompleteParticipants();
  bool nextResyncTarget(const std::vector<GuidPrefix_t> &candidates, bool claim, GuidPrefix_t &target);
  void traceGate(uint32_t numMismatched, int outcome, const GuidPrefix_t &target);

  enum class ExchangeResult { Answered, Abandoned };
  bool selectJoinPartner(GuidPrefix_t &partner);
  void beginExchange(const GuidPrefix_t &partner, bool isResync);
  void endExchange(ExchangeResult result);
  bool retransmit(bool isResync);
  void dispatchSnapshotRequestTo(const GuidPrefix_t &partner, bool isResync);
  bool sendSnapshotResponseFor(const GuidPrefix_t &target, bool ownEndpointsOnly);
  
  // Generate random millisecond jitter to space some operations in time
  uint32_t nextJitterMs();
  std::mt19937 &partnerRng();
  std::mt19937 m_rng;
  bool m_rngSeeded = false;

  // periodic anti entropic sweep, poll fast while peers are stale
  static constexpr uint32_t MAINTENANCE_ACTIVE_MS = 25;
  static constexpr uint32_t MAINTENANCE_IDLE_MS = 500;
  void periodicHashSweep();
  void checkConfiguredPeer(const GuidPrefix_t &prefix, bool changed);
  void traceHashCheck(const GuidPrefix_t &prefix, uint64_t peerHash, uint64_t peerViewHash, bool mismatch, bool triggeredResync);
  void removeOwnerlessEndpoints();

  // EndpointHash functions
  void xorOwnEndpointHash(const Guid_t &endpointGuid);
  uint64_t getPeerViewHash(const GuidPrefix_t &prefix);
  void setRemoteEndpointHash(const GuidPrefix_t &prefix, uint64_t endpointHash);

  // Root handling functions
  bool adoptRootIfLower(const GuidPrefix_t &candidate);
  bool isKnownParticipant(const GuidPrefix_t &candidate) const;
  void traceRoot();

  // Endpoint transmission functions
  void announceMyEndpoints();
  std::vector<TopicData> collectLocalEndpoints();
  void queueAnnouncement(const TopicData &endpoint);
  void dropUnannounced(const Guid_t &endpointGuid);
  void sendUnannounced();
  void sendEndpointDispose(const TopicData &endpoint);
  void sendEndpointBatch(const std::vector<TopicData> &endpoints, bool disposed);
  bool resolveSnapLocator(GuidPrefix_t target, Locator &out) const;
 
  // Store incoming endpoint processing while SM is busy
  void deferRequester(const GuidPrefix_t &requester);
  void retryOneDeferredResponse();

  // Endpoint reception
  void receiveEndpoints(const TopicData &data);
  void receiveParticipant(const GuidPrefix_t &prefix, const GuidPrefix_t &root);
  void removeRemoteEndpoint(const Guid_t &endpointGuid);
  void replaceOwnerEndpoints(const GuidPrefix_t &owner, const std::vector<TopicData> &slice, bool sliceComplete);
  void dropOwnerSlice(const GuidPrefix_t &owner);
  void forgetPeer(const GuidPrefix_t &prefix);
  void processResyncFrame(const GuidPrefix_t &sender, uint32_t resyncId, uint32_t totalEndpoints, const std::vector<TopicData> &frame);

  // Local readers and writers
  std::vector<Writer *> m_localWriters;
  std::vector<Reader *> m_localReaders;
  std::vector<TopicData> m_unannounced;
  mutable Mutex m_localMutex;

  // Requesters whose locator was not resolvable or who arrived while busy
  static constexpr uint8_t MAX_DEFERRED_REQUESTERS = 4;
  FixedSet<GuidPrefix_t, MAX_DEFERRED_REQUESTERS> m_deferredRequesters;

  // Known remote readers, not yet matched so we store them for later processing, embeddedrtps analog
  SnapEDPEndpointStore m_remote;

  // Snapshot request state, current partner, retry count, skip list and resync target
  SnapEDPExchange m_exchange;
  FixedSet<GuidPrefix_t, Config::SPDP_MAX_NUMBER_FOUND_PARTICIPANTS> m_pendingResync;

  // Internal Snap state and peer View Table
  SnapEDPRoot m_root;
  std::atomic<uint64_t> m_ownEndpointHash{0};
  SnapEDPPeerTable m_peers;

  // Snap FSM instance 
  std::array<SnapEDPFSM::StateDef, 6> m_stateDefs{};
  std::unique_ptr<SnapEDPFSM> m_snapSM = nullptr;

  //Slice assembler for resyncs
  SnapEDPResyncAssembler m_resync;

  // owners whose endpoints we hold without a participant proxy and when we first saw them that way, reaped after one lease
  FixedMap<GuidPrefix_t, uint32_t, Config::SPDP_MAX_NUMBER_FOUND_PARTICIPANTS> m_unseenEndpointOwners;
};

} // namespace rtps

#endif // RTPS_SEDPGOSSIPAGENT_H
