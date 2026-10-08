#ifndef EMBRTPS_TESTS_HARNESS_H
#define EMBRTPS_TESTS_HARNESS_H

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#define REQUIRE_TRUE(cond)                                                     \
  do {                                                                         \
    if (!(cond)) {                                                             \
      std::cerr << "  FAIL: " #cond " at " __FILE__ ":" << __LINE__            \
                << std::endl;                                                  \
      std::exit(1);                                                            \
    }                                                                          \
  } while (0)

namespace tests {

struct TestEntry {
  std::string name;
  std::function<void()> fn;
};

inline std::vector<TestEntry> &registry() {
  static std::vector<TestEntry> r;
  return r;
}

struct Registrar {
  Registrar(const std::string &name, std::function<void()> fn) {
    registry().push_back({name, std::move(fn)});
  }
};

inline double timeScale() {
  static const double s = [] {
    const char *e = std::getenv("EMBRTPS_TEST_TIME_SCALE");
    double v = e ? std::atof(e) : 1.0;
    return v > 0.0 ? v : 1.0;
  }();
  return s;
}

inline int scaled(int ms) { return static_cast<int>(ms * timeScale()); }

inline bool waitFor(const std::function<bool()> &cond, int timeoutMs = 10000,
                    int pollMs = 20) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(scaled(timeoutMs));
  while (std::chrono::steady_clock::now() < deadline) {
    if (cond()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(pollMs));
  }
  return cond();
}

inline int runTests(int argc, char **argv) {
  if (argc < 2) {
    std::cerr << "usage: " << argv[0] << " <test_name>|--list\n";
    for (auto &t : registry()) std::cerr << "  " << t.name << "\n";
    return 2;
  }
  std::string want = argv[1];
  if (want == "--list") {
    for (auto &t : registry()) std::cout << t.name << "\n";
    return 0;
  }
  for (auto &t : registry()) {
    if (want == t.name) {
      std::cout << "[test] " << t.name << std::endl;
      t.fn();
      std::cout << "[pass] " << t.name << std::endl;
      return 0;
    }
  }
  std::cerr << "unknown test: " << want << "\n";
  return 2;
}

}

#define TEST(name)                                                             \
  static void test_##name();                                                   \
  static ::tests::Registrar registrar_##name(#name, test_##name);              \
  static void test_##name()

#define ADD_TEST_FN(name, fn) ::tests::registry().push_back({(name), fn})

#define RUN_TESTS(argc, argv) return ::tests::runTests(argc, argv)

#endif
