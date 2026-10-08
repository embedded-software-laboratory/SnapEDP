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

#ifndef RTPS_UDPCONNECTION_H
#define RTPS_UDPCONNECTION_H

#include <cstdint>

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdexcept>
#include <iostream>
#include "rtps/common/types.h"
#include "rtps/utils/Log.h"
#include "rtps/utils/udpUtils.h"
#include "rtps/utils/sysFunctions.h"
#include "rtps/utils/iptypes.h"

#include <sys/types.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>

#include "trace_control.h"

#include <string.h>
#include <vector>

namespace rtps {

typedef void (*udpRxFunc_fp)(void *arg, Ip4Port_t destPort, std::vector<uint8_t> &&p,
                               const ip_struct_t *addr, Ip4Port_t port);

struct UdpConnection {
  int socket_fd = -1;
  Ip4Port_t port = 0;
  udpRxFunc_fp m_rxCallback = nullptr;
  void *m_callbackArgs = nullptr;
  Thread recvThread{};

  UdpConnection() = default; // required for static allocation

  explicit UdpConnection(Ip4Port_t pport, udpRxFunc_fp rxCallback, void *callbackArgs,
                         bool reuseAddr = false) :
      port(pport), m_rxCallback(rxCallback), m_callbackArgs(callbackArgs) {
    socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_fd < 0) {
      throw std::runtime_error("socket call returned invalid fd");
    }

    // enlarge socket buffers to absorb microbursts without kernel drops
    {
      constexpr int SOCKET_BUF_SIZE = 4 * 1024 * 1024;
      int val = SOCKET_BUF_SIZE;
      if (setsockopt(socket_fd, SOL_SOCKET, SO_RCVBUF, &val, sizeof(val)) < 0) {
        RTPS_LOG("UdpConnection", UDP_DRIVER_VERBOSE,
                 "setsockopt(SO_RCVBUF) failed: %s", strerror(errno));
      }
      val = SOCKET_BUF_SIZE;
      if (setsockopt(socket_fd, SOL_SOCKET, SO_SNDBUF, &val, sizeof(val)) < 0) {
        RTPS_LOG("UdpConnection", UDP_DRIVER_VERBOSE,
                 "setsockopt(SO_SNDBUF) failed: %s", strerror(errno));
      }
    }

    // SO_REUSEADDR only on multicast sockets so processes share the group port, unicast omits it so a duplicate bind fails and we detect a colliding participant id, never SO_REUSEPORT
    if (reuseAddr) {
      int opt = 1;
      if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        RTPS_LOG("UdpConnection", UDP_DRIVER_VERBOSE,
                 "setsockopt(SO_REUSEADDR) failed");
        close(socket_fd);
        socket_fd = -1;
        return;
      }
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(socket_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
      RTPS_LOG("UdpConnection", UDP_DRIVER_VERBOSE, "bind failed: %s",
               strerror(errno));
      close(socket_fd);
      socket_fd = -1;
      return;
    }

    // copy so that move ctor doesnt mess this up
    auto fd = socket_fd;
    auto rxCallback_copy = m_rxCallback;
    auto callbackArgs_copy = m_callbackArgs;
    auto port_copy = port;
    recvThread = Thread(new std::thread([fd, rxCallback_copy, callbackArgs_copy, port_copy](){
      constexpr size_t BUF_SZ = 65535;
      std::vector<uint8_t> recvBuf(BUF_SZ);

      std::cerr << "[UdpConnection] recv thread started fd=" << fd
                << " port=" << port_copy << " tid=" << std::this_thread::get_id() << std::endl;

      while (true) {
        RTPS_LOG("UdpConnection", UDP_DRIVER_VERBOSE,
                 "receive loop fd=%d port=%u", fd,
                 static_cast<unsigned>(port_copy));

        sockaddr_in src{};
        socklen_t srclen = sizeof(src);
        ssize_t n = recvfrom(fd, recvBuf.data(), recvBuf.size(), 0,
                             reinterpret_cast<sockaddr*>(&src), &srclen);

        if (n < 0) {
          if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            continue;
          }
          std::cerr << "[UdpConnection] recvfrom error fd=" << fd
                    << " port=" << port_copy << ": " << strerror(errno) << std::endl;
          break;
        }

        if (n == 0) {
          std::cerr << "[UdpConnection] recvfrom returned 0, exiting fd=" << fd
                    << " port=" << port_copy << std::endl;
          break;
        }

        // bytes still pending in the kernel socket buffer after this read
        int pending = 0;
        ioctl(fd, FIONREAD, &pending);
        RTPS_TRACE_PERF_EVENT(socket_buffer_state,
            port_copy,
            static_cast<uint32_t>(n),
            static_cast<uint32_t>(pending > 0 ? pending : 0));

        std::vector<uint8_t> pkt(recvBuf.data(),
                                 recvBuf.data() + static_cast<std::size_t>(n));

        ip_struct_t src_addr;
        if(src.sin_family == AF_INET) {
          src_addr.u_addr.ip4.addr = ntohl(src.sin_addr.s_addr);
          src_addr.type = ip_addr_type_enum::IPADDR_TYPE_V4;
        } else {
          std::cerr << "[UdpConnection] received non-ipv4, exiting fd=" << fd << std::endl;
          break;
        }
        RTPS_LOG("UdpConnection", UDP_DRIVER_VERBOSE, "recv callback start");
        rxCallback_copy(callbackArgs_copy, port_copy, std::move(pkt), &src_addr, ntohs(src.sin_port));
        RTPS_LOG("UdpConnection", UDP_DRIVER_VERBOSE, "recv callback finish");
      }

      std::cerr << "[UdpConnection] recv thread exiting fd=" << fd
                << " port=" << port_copy << " tid=" << std::this_thread::get_id() << std::endl;
    }));
  }

  UdpConnection(UdpConnection &&other) noexcept :
      socket_fd(other.socket_fd), port(other.port), m_rxCallback(other.m_rxCallback), m_callbackArgs(other.m_callbackArgs), recvThread(std::move(other.recvThread)) {
    other.socket_fd = -1;
  }

  ~UdpConnection() {
    if (socket_fd >= 0) {
      std::cerr << "[UdpConnection] ~dtor: shutting down fd=" << socket_fd
                << " port=" << port << std::endl;
      shutdown(socket_fd, SHUT_RDWR);
      std::cerr << "[UdpConnection] ~dtor: shutdown() returned, joining recv thread fd="
                << socket_fd << std::endl;
      joinThread(recvThread);
      std::cerr << "[UdpConnection] ~dtor: thread joined, closing fd=" << socket_fd << std::endl;
      close(socket_fd);
      std::cerr << "[UdpConnection] ~dtor: closed fd=" << socket_fd << std::endl;
    }
  }
};
} // namespace rtps

#endif // RTPS_UDPCONNECTION_H
