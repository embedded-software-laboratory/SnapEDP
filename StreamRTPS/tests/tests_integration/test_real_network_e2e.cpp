#include "rtps/config.h"
#include "rtps/config/FeatureQOS.h"
#include "rtps/entities/Domain.h"
#include "rtps/rtps.h"

#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

using namespace rtps;

namespace {

constexpr size_t PAYLOAD_SIZE = 64;

struct TestPayload {
  uint32_t sequence;
  uint8_t padding[PAYLOAD_SIZE - sizeof(uint32_t)];

  void serialize(uint8_t *buf) const { std::memcpy(buf, this, PAYLOAD_SIZE); }
  void deserialize(const uint8_t *buf) { std::memcpy(this, buf, PAYLOAD_SIZE); }
};

struct Args {
  bool reliable = true;
  float lossRate = 0.0f;
  uint32_t count = 20;
};

bool parseArgs(int argc, char **argv, Args &out) {
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--reliable") {
      out.reliable = true;
    } else if (a == "--best-effort") {
      out.reliable = false;
    } else if (a == "--loss-pct" && i + 1 < argc) {
      out.lossRate = std::atof(argv[++i]) / 100.0f;
    } else if (a == "--count" && i + 1 < argc) {
      out.count = static_cast<uint32_t>(std::atoi(argv[++i]));
    } else {
      std::cerr << "Unknown arg: " << a << std::endl;
      return false;
    }
  }
  return true;
}

std::atomic<uint32_t> g_received{0};

void countingCallback(void *callee, const ReaderCacheChange & ) {
  static_cast<std::atomic<uint32_t> *>(callee)->fetch_add(1);
}

[[noreturn]] void runReceiver(const Args &args, int resultWriteFd,
                               int stopReadFd) {
  rtps::init();

  Domain domain(FeatureQOS(DiscoveryMode::Standard,
                           HeartbeatPolicyMode::AdaptiveFrequency));
  auto *part = domain.createParticipant();
  if (part == nullptr) {
    std::cerr << "[receiver] createParticipant failed" << std::endl;
    _exit(2);
  }
  domain.completeInit();

  auto *reader =
      domain.createReader(*part, "E2ETopic", "E2EType", args.reliable);
  if (reader == nullptr) {
    std::cerr << "[receiver] createReader failed" << std::endl;
    _exit(2);
  }
  reader->registerCallback(countingCallback, &g_received);

  char byte;
  while (true) {
    ssize_t n = read(stopReadFd, &byte, 1);
    if (n == 0) break;
    if (n < 0 && errno == EINTR) continue;
    if (n > 0) break;
    if (n < 0) break;
  }

  const uint32_t finalCount = g_received.load();
  std::cerr << "[receiver] final count = " << finalCount << std::endl;

  const uint32_t netCount = finalCount;
  ssize_t w = write(resultWriteFd, &netCount, sizeof(netCount));
  (void)w;
  close(resultWriteFd);

  _exit(0);
}

int runSender(const Args &args, pid_t childPid, int resultReadFd,
              int stopWriteFd) {
  rtps::init();

  Config::UDP_PACKET_LOSS_RATE.store(args.lossRate, std::memory_order_relaxed);

  Domain domain(FeatureQOS(DiscoveryMode::Standard,
                           HeartbeatPolicyMode::AdaptiveFrequency));
  auto *part = domain.createParticipant();
  if (part == nullptr) {
    std::cerr << "[sender] createParticipant failed" << std::endl;
    return 2;
  }
  domain.completeInit();

  auto *writer = domain.createWriter(*part, "E2ETopic", "E2EType",
                                     args.reliable, true);
  if (writer == nullptr) {
    std::cerr << "[sender] createWriter failed" << std::endl;
    return 2;
  }

  std::cout << "[sender] reliable=" << args.reliable
            << " loss=" << args.lossRate << " count=" << args.count
            << std::endl;

  std::cout << "[sender] waiting for discovery (6s)..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(6));

  for (uint32_t i = 0; i < args.count; ++i) {
    TestPayload payload{};
    payload.sequence = i + 1;
    uint8_t buf[PAYLOAD_SIZE];
    payload.serialize(buf);
    writer->newChange(ChangeKind_t::ALIVE, buf, PAYLOAD_SIZE);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  int drainSec = args.reliable ? (args.lossRate >= 0.05f ? 25 : 10) : 3;
  std::cout << "[sender] draining for " << drainSec << "s..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(drainSec));

  const char stop = 1;
  ssize_t w = write(stopWriteFd, &stop, 1);
  (void)w;
  close(stopWriteFd);

  uint32_t childCount = 0;
  ssize_t r = read(resultReadFd, &childCount, sizeof(childCount));
  if (r != sizeof(childCount)) {
    std::cerr << "[sender] failed to read result from receiver (r=" << r
              << ")" << std::endl;
  }
  close(resultReadFd);

  int status = 0;
  waitpid(childPid, &status, 0);

  std::cout << "[sender] receiver reported " << childCount << " / "
            << args.count << " messages" << std::endl;

  bool pass;
  if (args.reliable) {
    pass = (childCount >= args.count);
  } else {
    if (args.lossRate == 0.0f) {
      pass = (childCount * 10 >= args.count * 9);
    } else {
      pass = (childCount > 0);
    }
  }

  std::cout << "[sender] result: " << (pass ? "PASS" : "FAIL") << std::endl;
  return pass ? 0 : 1;
}

}

int main(int argc, char **argv) {
  Args args;
  if (!parseArgs(argc, argv, args)) {
    std::cerr << "Usage: " << argv[0]
              << " [--reliable|--best-effort] [--loss-pct N] [--count N]"
              << std::endl;
    return 2;
  }

  int resultPipe[2];
  int stopPipe[2];
  if (pipe(resultPipe) != 0 || pipe(stopPipe) != 0) {
    perror("pipe");
    return 2;
  }

  pid_t pid = fork();
  if (pid < 0) {
    perror("fork");
    return 2;
  }

  if (pid == 0) {
    close(resultPipe[0]);
    close(stopPipe[1]);
    runReceiver(args, resultPipe[1], stopPipe[0]);
    _exit(0);
  }

  close(resultPipe[1]);
  close(stopPipe[0]);
  return runSender(args, pid, resultPipe[0], stopPipe[1]);
}
