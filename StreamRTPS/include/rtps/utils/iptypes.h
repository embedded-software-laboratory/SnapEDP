#ifndef RTPS_IPTYPES_H
#define RTPS_IPTYPES_H

#include <cstdint>

#include <netinet/in.h>

struct ip4_struct{
  in_addr_t addr;
};
typedef struct ip4_struct ip4_struct_t;

struct ip6_struct {
  uint32_t addr[4];
  uint8_t zone;
};
typedef struct ip6_struct ip6_struct_t;

// from lwip
enum ip_addr_type_enum : uint8_t {
  IPADDR_TYPE_V4 =   0U,
  IPADDR_TYPE_V6 =   6U,
  IPADDR_TYPE_ANY = 46U
};

typedef struct ip_struct {
  union {
    ip6_struct_t ip6;
    ip4_struct_t ip4;
  } u_addr;
  ip_addr_type_enum type;
} ip_struct_t;

#endif // RTPS_IPTYPES_H
