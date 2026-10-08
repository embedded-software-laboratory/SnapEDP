/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University

This file is part of streamRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#ifndef RTPS_SEDPAGENT_H
#define RTPS_SEDPAGENT_H

#include "rtps/discovery/EDPAgentBase.h"

namespace rtps {

// Regular SEDP Agent 
class SEDPAgent : public EDPAgentBase {
public:
  void init(Participant &part, const BuiltInEndpoints &endpoints) override;
  void addWriter(Writer &writer) override;
  void addReader(Reader &reader) override;

  void removeWriter(Writer &writer) override;
  void removeReader(Reader &reader) override;

private:
  static void receiveCallbackPublisher(void *callee,
                                       const ReaderCacheChange &cacheChange);
  static void receiveCallbackSubscriber(void *callee,
                                        const ReaderCacheChange &cacheChange);
  void onNewPublisherFromChange(const ReaderCacheChange &change);
  void onNewSubscriberFromChange(const ReaderCacheChange &change);
};

} // namespace rtps

#endif // RTPS_SEDPAGENT_H
