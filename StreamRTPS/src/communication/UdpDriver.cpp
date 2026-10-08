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

#include "rtps/communication/UdpDriver.h"
#include "rtps/communication/TcpipCoreLock.h"
#include "rtps/utils/Lock.h"
#include "rtps/utils/Log.h"
#include "rtps/config.h"
#include "rtps/utils/udpUtils.h"
#include "trace_control.h"

#include <atomic>
#include <cstring>
#include <unistd.h>

using rtps::UdpDriver;

std::array<uint8_t, 4> UdpDriver::m_ip = {0, 0, 0, 0};

// process wide TX accounting, incremented on every successful sendto
static std::atomic<uint64_t> s_txBytes{0};
static std::atomic<uint64_t> s_txPackets{0};
static std::atomic<uint64_t> s_txBytesOffHost{0};
static std::atomic<uint64_t> s_txPacketsOffHost{0};

static bool isOffHostDest(const ip4_struct_t &destAddr) {
  // network byte order, first octet is the low byte on little endian
  const uint8_t first_octet = static_cast<uint8_t>(destAddr.addr & 0xFFu);
  if (first_octet >= 224 && first_octet <= 239) {
    return true;
  }
  uint32_t own = 0;
  std::memcpy(&own, UdpDriver::m_ip.data(), sizeof(own));
  return destAddr.addr != own;
}

#if UDP_DRIVER_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define UDP_DRIVER_LOG(...) RTPS_LOG("UdpDriver", UDP_DRIVER_VERBOSE, __VA_ARGS__)
#else
#define UDP_DRIVER_LOG(...) RTPS_TRACE_LOG_EMIT("UdpDriver", __VA_ARGS__)
#endif

#include <sys/types.h>
#include <ifaddrs.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>


#ifdef RTPS_UDP_PACKET_LOSS_ENABLED
#include <random>
#include <cstdio>
static thread_local std::mt19937 s_lossRng{std::random_device{}()};
static thread_local std::uniform_real_distribution<float> s_lossDist{0.0f, 1.0f};
#endif

UdpDriver::UdpDriver(rtps::udpRxFunc_fp callback, void *args)
    : m_rxCallback(callback), m_callbackArgs(args) {}

const rtps::UdpConnection *
UdpDriver::createUdpConnection(Ip4Port_t receivePort, bool reuseAddr) {
  for(auto &con : m_conns) {
    if (con.port == receivePort) {
      RTPS_TRACE_PERF_EVENT(udp_create_connection, receivePort, 1);
      return &con;
    }
  }

  if (m_conns.size() == Config::MAX_NUM_UDP_CONNECTIONS) {
    RTPS_TRACE_PERF_EVENT(udp_create_connection, receivePort, 0);
    return nullptr;
  }

  m_conns.emplace_back(receivePort, m_rxCallback, m_callbackArgs, reuseAddr);
  if (m_conns.back().socket_fd < 0) {
    m_conns.pop_back();
    RTPS_TRACE_PERF_EVENT(udp_create_connection, receivePort, 0);
    return nullptr;
  }

  // benIMPROVEOLD check success
  UDP_DRIVER_LOG("Successfully created UDP connection on port %u \n",
                 receivePort);

  RTPS_TRACE_PERF_EVENT(udp_create_connection, receivePort, 1);
  return &m_conns.back();
}

void UdpDriver::removeUdpConnection(Ip4Port_t port) {
  // UdpConnection isnt move assignable so the vector only supports pop_back, rollback always targets the last connection
  if (!m_conns.empty() && m_conns.back().port == port) {
    // UdpConnection destructor closes the fd and joins its recv thread
    m_conns.pop_back();
  }
}

bool UdpDriver::canBindPort(Ip4Port_t port) const {
  int probe_fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (probe_fd < 0) {
    return false;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = INADDR_ANY;

  bool success = (bind(probe_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
  close(probe_fd);
  return success;
}

bool UdpDriver::isSameSubnet(ip4_struct_t addr) {
  ip4_struct_t dev_ip_addr = transformIP4ToU32(m_ip[0], m_ip[1], m_ip[2], m_ip[3]);
  return (addr.addr & 0x00FFFFFF) == (dev_ip_addr.addr & 0x00FFFFFF) ;
}

bool UdpDriver::joinMultiCastGroup(ip4_struct_t addr) const {

  bool success = true;

  for(auto &con : m_conns) {
    if (con.socket_fd < 0) {
      continue;
    }

#if 1
    in_addr local_if_addr;
    local_if_addr.s_addr = transformIP4ToU32(m_ip[0], m_ip[1], m_ip[2], m_ip[3]).addr;
    if (setsockopt(con.socket_fd, IPPROTO_IP, IP_MULTICAST_IF, &local_if_addr, sizeof(local_if_addr)) < 0) {
      UDP_DRIVER_LOG("setsockopt(IP_MULTICAST_IF) failed");
      success = false;
      // continue trying other sockets
    }
#endif

    ip_mreq mreq{};
    mreq.imr_multiaddr.s_addr = addr.addr;
    mreq.imr_interface.s_addr = local_if_addr.s_addr;

    if (setsockopt(con.socket_fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
      UDP_DRIVER_LOG("Failed to join multicast group %s on fd %d: %s\n",
                     inet_ntoa(*(in_addr*)&addr), con.socket_fd, strerror(errno));
      success = false;
      // continue trying other sockets
    } else {
      UDP_DRIVER_LOG("Successfully joined multicast group %s on fd %d\n",
                     inet_ntoa(*(in_addr*)&addr), con.socket_fd);
    }
  }

  // benIMPROVEOLD restart receive loop

  // benIMPROVEOLD kinda duplicate
  if (!success) {

    UDP_DRIVER_LOG("Failed to join IGMP multicast group %s\n",
                   inet_ntoa(*(in_addr*)&addr));

    RTPS_TRACE_ALL_EVENT(udp_join_multicast, addr.addr, 0);
    return false;
  } else {

    UDP_DRIVER_LOG("Succesfully joined IGMP multicast group %s\n",
                   inet_ntoa(*(in_addr*)&addr));
  }
  RTPS_TRACE_ALL_EVENT(udp_join_multicast, addr.addr, 1);
  return true;
}

bool UdpDriver::sendPacket(const UdpConnection &conn, ip4_struct_t &destAddr,
                           Ip4Port_t destPort, PBufWrapper &buffer,
                           EventId_t eventId) {

  if(conn.socket_fd < 0 || buffer.m_buf.empty()) {
    RTPS_TRACE_PERF_EVENT(udp_send_packet, eventId, conn.port, destPort, destAddr.addr,
                          static_cast<uint32_t>(buffer.m_buf.size()), 0);
    return false;
  }

#ifdef RTPS_UDP_PACKET_LOSS_ENABLED
  {
    // independent per packet loss at the configured rate
    const float lossRate = Config::UDP_PACKET_LOSS_RATE.load(std::memory_order_relaxed);
    const bool drop = (lossRate > 0.0f && s_lossDist(s_lossRng) < lossRate);

    if (drop) {
      std::fprintf(stderr, "[WARNING] PACKET DROPPED (simulated loss) dest=%s:%u size=%zu\n",
                   inet_ntoa(*(in_addr*)&destAddr), static_cast<unsigned>(destPort),
                   buffer.m_buf.size());
      RTPS_TRACE_PERF_EVENT(udp_send_packet, eventId, conn.port, destPort, destAddr.addr,
                            static_cast<uint32_t>(buffer.m_buf.size()), 0);
      return false;
    }
  }
#endif

  sockaddr_in dst{};
  dst.sin_family = AF_INET;
  dst.sin_port = htons(destPort);
  dst.sin_addr.s_addr = destAddr.addr; // already in network byte order

  RTPS_LOG("UdpDriver", UDP_DRIVER_VERBOSE,
           "sendPacket len=%zu dest=%s:%u", buffer.m_buf.size(),
           inet_ntoa(*(in_addr*)&destAddr), static_cast<unsigned>(destPort));

  ssize_t sent = sendto(conn.socket_fd,
                        buffer.m_buf.data(),
                        buffer.m_buf.size(),
                        0,
                        reinterpret_cast<const sockaddr*>(&dst),
                        sizeof(dst));

  if(sent != static_cast<ssize_t>(buffer.m_buf.size())) {
    UDP_DRIVER_LOG("UDP TRANSMIT NOT SUCCESSFUL %s:%u size: %lu: %s\n",
                   inet_ntoa(*(in_addr*)&destAddr), destPort, buffer.m_buf.size(), strerror(errno));

    RTPS_TRACE_PERF_EVENT(udp_send_packet, eventId, conn.port, destPort, destAddr.addr,
                          static_cast<uint32_t>(buffer.m_buf.size()), 0);
    return false;
  }
  RTPS_TRACE_PERF_EVENT(udp_send_packet, eventId, conn.port, destPort, destAddr.addr,
                        static_cast<uint32_t>(buffer.m_buf.size()), 1);

  const uint64_t n = static_cast<uint64_t>(buffer.m_buf.size());
  s_txBytes.fetch_add(n, std::memory_order_relaxed);
  s_txPackets.fetch_add(1, std::memory_order_relaxed);
  if (isOffHostDest(destAddr)) {
    s_txBytesOffHost.fetch_add(n, std::memory_order_relaxed);
    s_txPacketsOffHost.fetch_add(1, std::memory_order_relaxed);
  }
  return true;
}

void UdpDriver::traceTxTotals() {
  uint32_t own = 0;
  std::memcpy(&own, m_ip.data(), sizeof(own));
  RTPS_TRACE_ALL_EVENT(udp_tx_total,
                       static_cast<uint32_t>(getpid()),
                       own,
                       s_txBytes.load(std::memory_order_relaxed),
                       s_txPackets.load(std::memory_order_relaxed),
                       s_txBytesOffHost.load(std::memory_order_relaxed),
                       s_txPacketsOffHost.load(std::memory_order_relaxed));
}

const rtps::UdpConnection *UdpDriver::fallbackConnection() const {
  for (const auto &con : m_conns) {
    if (con.socket_fd >= 0) {
      return &con;
    }
  }
  return nullptr;
}

void UdpDriver::sendPacket(PacketInfo &packet) {
  auto p_conn = createUdpConnection(packet.srcPort);
  if (p_conn == nullptr) {
    // srcPort bind can fail at high colocated density when another process owns that port, replies route to the advertised locator so any owned socket works, fall back instead of dropping
    p_conn = fallbackConnection();
    if (p_conn == nullptr) {
      UDP_DRIVER_LOG("Failed to create connection on port %u \n", packet.srcPort);
      return;
    }
  }

  sendPacket(*p_conn, packet.destAddr, packet.destPort, packet.buffer,
             packet.eventId);
}

void UdpDriver::updateIP() {
    struct ifaddrs *ifaddr, *ifa;
    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs");
        return;
    }

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL)
            continue;

        if (ifa->ifa_addr->sa_family == AF_INET) {
            // skip loopback
            if (strcmp(ifa->ifa_name, "lo") == 0) continue;

            struct sockaddr_in *pAddr = (struct sockaddr_in *)ifa->ifa_addr;
            uint32_t ip = pAddr->sin_addr.s_addr;
            m_ip[0] = ip & 0xFF;
            m_ip[1] = (ip >> 8) & 0xFF;
            m_ip[2] = (ip >> 16) & 0xFF;
            m_ip[3] = (ip >> 24) & 0xFF;

            break;
        }
    }
    freeifaddrs(ifaddr);
}
