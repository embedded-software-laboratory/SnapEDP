#include "rtps/communication/MockNetworkDriver.h"
#include "rtps/communication/MockNetworkRouter.h"
#include "rtps/utils/udpUtils.h"

using rtps::MockNetworkDriver;

std::array<uint8_t, 4> MockNetworkDriver::m_ip = {10, 0, 0, 1};

MockNetworkDriver::MockNetworkDriver(udpRxFunc_fp callback, void *args)
    : m_rxCallback(callback), m_callbackArgs(args) {
  MockNetworkRouter::instance().registerDriver(this);
}

MockNetworkDriver::~MockNetworkDriver() {
  MockNetworkRouter::instance().unregisterDriver(this);
}

const rtps::MockConnection *
MockNetworkDriver::createUdpConnection(Ip4Port_t receivePort, bool reuseAddr) {
  {
    std::lock_guard<std::mutex> lock(m_connMutex);
    for (auto &conn : m_conns) {
      if (conn.port == receivePort) {
        return &conn;
      }
    }
  }

  // mirror a real UDP bind, unicast ports stay unique across drivers so port based id selection works in multi domain tests, multicast ports may be shared
  if (!reuseAddr && !canBindPort(receivePort)) {
    return nullptr;
  }

  // not held across canBindPort above, that takes the router lock and the router takes this one while routing
  std::lock_guard<std::mutex> lock(m_connMutex);
  m_conns.emplace_back(receivePort);
  return &m_conns.back();
}

void MockNetworkDriver::removeUdpConnection(Ip4Port_t port) {
  // rollback always targets the just added last connection
  std::lock_guard<std::mutex> lock(m_connMutex);
  if (!m_conns.empty() && m_conns.back().port == port) {
    m_conns.pop_back();
  }
}

bool MockNetworkDriver::canBindPort(Ip4Port_t port) const {
  // true unless another driver already bound this port
  return !MockNetworkRouter::instance().isPortBound(port, this);
}

bool MockNetworkDriver::joinMultiCastGroup(ip4_struct_t /*addr*/) const {
  return true;
}

void MockNetworkDriver::sendPacket(PacketInfo &info) {
  MockNetworkRouter::instance().routePacket(this, info);
}

bool MockNetworkDriver::isSameSubnet(ip4_struct_t addr) {
  ip4_struct_t dev_ip_addr =
      transformIP4ToU32(m_ip[0], m_ip[1], m_ip[2], m_ip[3]);
  return (addr.addr & 0x00FFFFFF) == (dev_ip_addr.addr & 0x00FFFFFF);
}

void MockNetworkDriver::updateIP() {
  // static mock ip, nothing to update
}

bool MockNetworkDriver::hasConnection(Ip4Port_t port) const {
  std::lock_guard<std::mutex> lock(m_connMutex);
  for (const auto &conn : m_conns) {
    if (conn.port == port) {
      return true;
    }
  }
  return false;
}

void MockNetworkDriver::deliverPacket(Ip4Port_t destPort, const uint8_t *data,
                                       size_t len, ip4_struct_t srcAddr,
                                       Ip4Port_t srcPort) {
  if (m_rxCallback == nullptr) {
    return;
  }

  std::vector<uint8_t> buf(data, data + len);
  ip_struct_t addr{};
  addr.u_addr.ip4 = srcAddr;
  addr.type = ip_addr_type_enum::IPADDR_TYPE_V4;

  m_rxCallback(m_callbackArgs, destPort, std::move(buf), &addr, srcPort);
}
