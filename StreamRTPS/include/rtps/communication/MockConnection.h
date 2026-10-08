#ifndef RTPS_MOCKCONNECTION_H
#define RTPS_MOCKCONNECTION_H

#include "rtps/common/types.h"

namespace rtps {

struct MockConnection {
  Ip4Port_t port = 0;
  MockConnection() = default;
  explicit MockConnection(Ip4Port_t p) : port(p) {}
};

} // namespace rtps

#endif // RTPS_MOCKCONNECTION_H
