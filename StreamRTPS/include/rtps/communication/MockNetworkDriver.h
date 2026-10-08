#ifndef RTPS_MOCKNETWORKDRIVER_H
#define RTPS_MOCKNETWORKDRIVER_H

#include "rtps/communication/MockConnection.h"
#include "rtps/communication/UdpConnection.h" // for udpRxFunc_fp
#include "rtps/communication/PacketInfo.h"
#include "rtps/common/types.h"
#include "rtps/config.h"
#include "rtps/storages/PBufWrapper.h"
#include "rtps/utils/iptypes.h"

#include <array>
#include <mutex>
#include <vector>

namespace rtps {

class MockNetworkRouter;

class MockNetworkDriver {
public:
  MockNetworkDriver(udpRxFunc_fp callback, void *args);
  ~MockNetworkDriver();

  const MockConnection *createUdpConnection(Ip4Port_t receivePort,
                                            bool reuseAddr = false);
  void removeUdpConnection(Ip4Port_t port);
  bool canBindPort(Ip4Port_t port) const;
  bool joinMultiCastGroup(ip4_struct_t addr) const;
  void sendPacket(PacketInfo &info);

  static bool isSameSubnet(ip4_struct_t addr);
  static std::array<uint8_t, 4> m_ip;
  static void updateIP();

  bool hasConnection(Ip4Port_t port) const;

private:
  // the router reads this from sender threads while the owner still binds ports
  mutable std::mutex m_connMutex;
  std::vector<MockConnection> m_conns;
  udpRxFunc_fp m_rxCallback = nullptr;
  void *m_callbackArgs = nullptr;

  friend class MockNetworkRouter;
  void deliverPacket(Ip4Port_t destPort, const uint8_t *data, size_t len,
                     ip4_struct_t srcAddr, Ip4Port_t srcPort);
};

} // namespace rtps

#endif // RTPS_MOCKNETWORKDRIVER_H
