#ifndef RTPS_MOCKNETWORKROUTER_H
#define RTPS_MOCKNETWORKROUTER_H

#include "rtps/communication/PacketInfo.h"
#include "rtps/utils/udpUtils.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <random>
#include <thread>
#include <utility>
#include <vector>

namespace rtps {

class MockNetworkDriver;

// link model applied per sender receiver pair, default is synchronous, lossless and order preserving
struct LinkModel {
  // uniform per packet loss
  double drop_rate = 0.0;

  // delay is a truncated exponential between min and max with the given mean, mirrored if the mean is in the upper half, negative mean means uniform
  double min_delay_ms = 0.0;
  double max_delay_ms = 0.0;
  double mean_delay_ms = -1.0;
  bool preserve_order = true; // if false every packet draws its own delay and can reorder

  // per frame loss, a packet only survives if all its frames do
  double frame_loss_rate = 0.0;
  size_t mtu = 1500;

  // serialisation delay and byte limited tail drop queue, 0 means unlimited
  double bitrate_bps = 0.0;
  size_t queue_limit_bytes = 0;

  double duplicate_rate = 0.0; // second copy draws its own delay

  // Gilbert Elliott burst loss, active when p_good_to_bad is nonzero
  double p_good_to_bad = 0.0;
  double p_bad_to_good = 0.0;
  double loss_good = 0.0;
  double loss_bad = 1.0;

  // hold and release, deliveries are aligned to multiples of this period
  double burst_period_ms = 0.0;

  // rare long tail on top of the base delay
  double late_rate = 0.0;
  double late_delay_ms = 0.0;

  // bit flip or truncation, RTPS has no checksum
  double corrupt_rate = 0.0;

  bool isSynchronous() const {
    return max_delay_ms <= 0.0 && min_delay_ms <= 0.0 && bitrate_bps <= 0.0 &&
           burst_period_ms <= 0.0 && late_rate <= 0.0;
  }
};

struct LinkStats {
  uint64_t sent = 0;       // packets that reached the router for this link
  uint64_t delivered = 0;  // callbacks actually invoked
  uint64_t dropped = 0;    // dropped for any reason, also at delivery time
  uint64_t duplicated = 0; // extra copies created
  uint64_t reordered = 0;  // deliveries that overtook an earlier sent packet
  uint64_t corrupted = 0;
  uint64_t queue_dropped = 0;
};

struct TapEvent {
  enum class Verdict {
    Forwarded,         // accepted, delivered now or scheduled
    Duplicated,        // an extra copy was created and accepted
    DroppedFilter,
    DroppedPartition,
    DroppedBlackout,
    DroppedLoss,       // drop_rate, frame loss or burst loss
    DroppedQueue,
  };
  MockNetworkDriver *sender;
  MockNetworkDriver *dst;
  Ip4Port_t destPort;
  const uint8_t *data; // valid only during the callback
  size_t size;
  Verdict verdict;
};

class MockNetworkRouter {
public:
  static MockNetworkRouter &instance();
  ~MockNetworkRouter();

  void registerDriver(MockNetworkDriver *driver);
  void unregisterDriver(MockNetworkDriver *driver);

  bool isPortBound(Ip4Port_t port, const MockNetworkDriver *excludeDriver) const;

  // drop predicate gets sender and dst driver pointers so tests can partition by participant identity, not just port or locator
  using DropFilter =
      std::function<bool(MockNetworkDriver *sender, MockNetworkDriver *dst,
                         const PacketInfo &info)>;
  void setDropFilter(DropFilter filter);
  void setDropRate(double rate);
  void clearDropPolicy();

  // global link model, per link overrides win, nullptr in setLinkModel is a wildcard, most specific match wins
  void setModel(const LinkModel &model);
  void setLinkModel(MockNetworkDriver *sender, MockNetworkDriver *dst,
                    const LinkModel &model);
  void clearLinkModels();

  // scheduled outage on a link starting now, nullptr is a wildcard
  void blackout(MockNetworkDriver *sender, MockNetworkDriver *dst,
                int durationMs);
  void clearBlackouts();

  // partition helpers built on the same per receiver decision as the filter, drivers not named in any group are unaffected
  void partition(const std::vector<std::vector<MockNetworkDriver *>> &groups);
  void isolate(MockNetworkDriver *node);
  void heal();

  // observer called once per sender receiver decision under the router lock, must not send or call back into the router
  using Tap = std::function<void(const TapEvent &)>;
  void setTap(Tap tap);
  LinkStats linkStats(MockNetworkDriver *sender, MockNetworkDriver *dst) const;
  LinkStats totalStats() const;
  void resetStats();

  // all random decisions are taken on the sending thread so a single sender thread replays exactly, default seed 42 or env MOCK_NET_SEED
  void setSeed(uint64_t seed);
  uint64_t seed() const;

  void routePacket(MockNetworkDriver *sender, PacketInfo &info);

  // true once nothing is in flight, false on timeout
  bool drain(int timeoutMs = 10000);
  size_t inFlight() const;
  size_t queuedBytes() const;
  uint64_t modelDropped() const; // all drops decided by the router

  // look up the driver bound to a unicast port, nullptr if none, used by partition filters
  MockNetworkDriver *driverBoundToPort(Ip4Port_t port) const;

  // stops the delivery thread and restores the default empty model, safe with live drivers
  void reset();

  // preset approximating a wifi testbed link
  static LinkModel wifiTestbed();

private:
  MockNetworkRouter();

  using Clock = std::chrono::steady_clock;
  using Link = std::pair<MockNetworkDriver *, MockNetworkDriver *>;

  struct LinkState {
    LinkStats stats;
    bool geBad = false;
    Clock::time_point lastDue{};
    Clock::time_point busyUntil{};
    size_t queuedBytes = 0;
    uint64_t nextSeq = 0;
    uint64_t maxDeliveredSeq = 0;
    bool anyDelivered = false;
  };

  struct InFlight {
    Link link;
    Ip4Port_t destPort;
    ip4_struct_t srcAddr;
    Ip4Port_t srcPort;
    std::vector<uint8_t> data;
    uint64_t seq;
  };

  struct Blackout {
    MockNetworkDriver *sender;
    MockNetworkDriver *dst;
    Clock::time_point end;
  };

  mutable std::recursive_mutex m_mutex;
  std::condition_variable_any m_wake;    // delivery thread
  std::condition_variable_any m_drained; // drain
  std::vector<MockNetworkDriver *> m_drivers;
  DropFilter m_dropFilter;
  LinkModel m_model;
  std::map<Link, LinkModel> m_linkModels;
  std::vector<Blackout> m_blackouts;
  std::map<MockNetworkDriver *, int> m_groupOf;
  Tap m_tap;
  std::map<Link, LinkState> m_links;
  std::multimap<Clock::time_point, InFlight> m_inflight;
  Clock::time_point m_epoch;
  uint64_t m_seed = 42;
  std::mt19937_64 m_rng;
  uint64_t m_modelDropped = 0;
  std::thread m_thread;
  bool m_stop = false;

  const LinkModel &modelFor(const Link &link) const;
  bool inBlackout(const Link &link, Clock::time_point now) const;
  bool partitioned(MockNetworkDriver *s, MockNetworkDriver *d) const;
  double uniform();
  double sampleDelayMs(const LinkModel &m);
  void emit(MockNetworkDriver *s, MockNetworkDriver *d, Ip4Port_t port,
            const uint8_t *data, size_t size, TapEvent::Verdict v);
  void scheduleCopy(const Link &link, const LinkModel &m, LinkState &st,
                    const PacketInfo &info, std::vector<uint8_t> data);
  void deliver(const Link &link, LinkState *st, Ip4Port_t port,
               const uint8_t *data, size_t len, ip4_struct_t srcAddr,
               Ip4Port_t srcPort, uint64_t seq);
  void ensureThread();
  void threadLoop();
  void shutdownThread();
};

} // namespace rtps

#endif // RTPS_MOCKNETWORKROUTER_H
