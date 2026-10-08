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

#ifndef RTPS_UDPDRIVER_H
#define RTPS_UDPDRIVER_H

#include <vector>

#include "UdpConnection.h"
#include "rtps/common/types.h"
#include "rtps/communication/PacketInfo.h"
#include "rtps/config.h"
#include "rtps/storages/PBufWrapper.h"
#include "rtps/utils/sysFunctions.h"

namespace rtps {

class UdpDriver {

public:
  UdpDriver(udpRxFunc_fp callback, void *args);

  // reuseAddr true sets SO_REUSEADDR so processes share multicast group ports, unicast must leave it false
  const rtps::UdpConnection *createUdpConnection(Ip4Port_t receivePort,
                                                 bool reuseAddr = false);
  // tear down a connection from createUdpConnection, rollback on a partially reserved participant id
  void removeUdpConnection(Ip4Port_t port);
  // any already open socket, used to send when the srcPort socket cant be bound
  const rtps::UdpConnection *fallbackConnection() const;
  bool canBindPort(Ip4Port_t port) const;
  bool joinMultiCastGroup(ip4_struct_t addr) const;
  void sendPacket(PacketInfo &info);

  static bool isSameSubnet(ip4_struct_t addr);
  static std::array<uint8_t, 4> m_ip;
  static void updateIP();

  // cumulative middleware TX accounting, emitted as udp_tx_total at experiment end, off host means multicast or a dest other than own IP
  static void traceTxTotals();

private:
  std::vector<UdpConnection> m_conns;
  udpRxFunc_fp m_rxCallback = nullptr;
  void *m_callbackArgs = nullptr;

  bool sendPacket(const UdpConnection &conn, ip4_struct_t &destAddr,
                  Ip4Port_t destPort, PBufWrapper &buffer,
                  EventId_t eventId = 0);
};
} // namespace rtps



#endif // RTPS_UDPDRIVER_H
