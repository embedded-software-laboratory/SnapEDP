#ifndef RTPS_NETWORKDRIVER_H
#define RTPS_NETWORKDRIVER_H

#ifdef EMBRTPS_USE_MOCK_DRIVER
#include "rtps/communication/MockNetworkDriver.h"
#else
#include "rtps/communication/UdpDriver.h"
#endif

namespace rtps {

#ifdef EMBRTPS_USE_MOCK_DRIVER
using DefaultDriver = MockNetworkDriver;
#else
using DefaultDriver = UdpDriver;
#endif

} // namespace rtps

#endif // RTPS_NETWORKDRIVER_H
