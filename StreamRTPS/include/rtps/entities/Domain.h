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

#ifndef RTPS_DOMAIN_H
#define RTPS_DOMAIN_H

#include "rtps/ThreadPool.h"
#include "rtps/config/FeatureQOS.h"
#include "rtps/communication/NetworkDriver.h"
#include "rtps/common/Locator.h"
#include "rtps/config.h"
#include "rtps/entities/Participant.h"
#include "rtps/entities/StatefulReader.h"
#include "rtps/entities/StatefulWriter.h"
#include "rtps/entities/StatelessReader.h"
#include "rtps/entities/StatelessWriter.h"
#include "rtps/entities/TransientReader.h"
#include "rtps/entities/TransientWriter.h"
#include "rtps/storages/PBufWrapper.h"
#include "rtps/utils/iptypes.h"

#include <new>
#include <cstddef>
#include <memory>
#include <map>
#include <vector>

namespace rtps {

class Domain {
public:
  explicit Domain(const FeatureQOS &qos = FeatureQOS{});
  ~Domain();

  bool completeInit();
  void stop();

  Participant *createParticipant(ParticipantId_t requestedId = PARTICIPANT_ID_INVALID);
  Participant *createParticipant(const GuidPrefix_t &explicitPrefix,
                                 ParticipantId_t requestedId = PARTICIPANT_ID_INVALID);
                                 
  Writer *createWriter(Participant &part, const char *topicName,
                       const char *typeName, bool reliable,
                       bool enforceUnicast = false);
  Reader *createReader(Participant &part, const char *topicName,
                       const char *typeName, bool reliable,
                       ip4_struct_t mcastaddress = {0});
  
  
  Writer *writerExists(Participant &part, const char *topicName,
                       const char *typeName, bool reliable);
  Reader *readerExists(Participant &part, const char *topicName,
                       const char *typeName, bool reliable);

  // detach, dispose to peers and free the endpoint, pointer dangles after
  bool removeWriter(Participant &part, Writer *writer);
  bool removeReader(Participant &part, Reader *reader);

private:
  FeatureQOS m_featureQos;
  ThreadPool m_threadPool;
  DefaultDriver m_transport;

  std::map<ParticipantId_t, std::unique_ptr<Participant>> m_participants;
  const uint16_t PARTICIPANT_START_ID = 0;
  const uint16_t PARTICIPANT_END_ID = 256;
  ParticipantId_t m_nextParticipantId = PARTICIPANT_START_ID;

  std::vector<std::unique_ptr<StatelessWriter>> m_statelessWriters;
  std::vector<std::unique_ptr<StatelessReader>> m_statelessReaders;

  std::vector<std::unique_ptr<TransientWriter>> m_transientWriters;
  std::vector<std::unique_ptr<TransientReader>> m_transientReaders;

  std::vector<std::unique_ptr<StatefulReader>> m_statefulReaders;
  std::vector<std::unique_ptr<StatefulWriter>> m_statefulWriters;
  bool m_initComplete = false;

  void receiveCallback(const PacketInfo &packet);
  GuidPrefix_t generateGuidPrefix(ParticipantId_t id) const;

  Participant *createParticipantImpl(ParticipantId_t requestedId,
                                     const GuidPrefix_t *explicitPrefix);
  void createBuiltinWritersAndReaders(Participant &part);
  bool reclaimWriter(Writer *writer);
  bool reclaimReader(Reader *reader);
  void registerPort(const Participant &part);
  void registerMulticastPort(Locator mcastLocator);
  static void receiveJumppad(void *callee, const PacketInfo &packet);

};



} // namespace rtps

#endif // RTPS_DOMAIN_H
