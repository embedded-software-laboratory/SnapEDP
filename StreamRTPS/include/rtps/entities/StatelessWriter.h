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

#ifndef RTPS_RTPSWRITER_H
#define RTPS_RTPSWRITER_H

#include "rtps/utils/Lock.h"
#include "rtps/common/types.h"
#include "rtps/communication/NetworkDriver.h"
#include "rtps/config.h"
#include "rtps/config/FeatureQOS.h"
#include "rtps/entities/Writer.h"
#include "rtps/storages/MemoryPool.h"
#include "rtps/storages/OrderedHistoryCache.h"

namespace rtps {

struct PBufWrapper;

template <typename NetworkDriver> class StatelessWriterT : public Writer {
public:
  StatelessWriterT() = default;
  ~StatelessWriterT() override;
  bool init(TopicData attributes, TopicKind_t topicKind, ThreadPool *threadPool,
            NetworkDriver &driver, bool enfUnicast = false,
            const FeatureQOS &qos = FeatureQOS{});

  bool addNewMatchedReader(const ReaderProxy &newProxy) override;
  void removeReader(const Guid_t &guid) override;
  void removeReaderOfParticipant(const GuidPrefix_t &guidPrefix) override;
  void progress() override;
  const CacheChange *newChange(ChangeKind_t kind, const uint8_t *data,
                               DataSize_t size) override;
  void setAllChangesToUnsent() override;
  void removeAllChanges() override;
  void onNewAckNack(const SubmessageAckNack &msg,
                    const GuidPrefix_t &sourceGuidPrefix) override;
  bool hasLocator(const Locator& loc) override;

private:
  Mutex m_mutex;
  ThreadPool *mp_threadPool = nullptr;

  PacketInfo m_packetInfo;
  NetworkDriver *m_transport;
  bool m_enforceUnicast;

  TopicKind_t m_topicKind = TopicKind_t::NO_KEY;
  SequenceNumber_t m_nextSequenceNumberToSend = {0, 1};
  OrderedHistoryCache m_history;

  bool isIrrelevant(ChangeKind_t kind) const;

  void manageSendOptions();
  void resetSendOptions();
};

using StatelessWriter = StatelessWriterT<DefaultDriver>;

} // namespace rtps

#include "StatelessWriter.tpp"

#endif // RTPS_RTPSWRITER_H
