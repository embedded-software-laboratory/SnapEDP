#include "harness.h"

#include "rtps/common/types.h"
#include "rtps/utils/FSM.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

using namespace rtps;
using Clock = std::chrono::steady_clock;

namespace {

enum class S { A = 0, B, C, kCount };
enum class E { Go, Back, Skip, Tout, Ping, Chain, Inc, kCount };

void sleepMs(int ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(tests::scaled(ms)));
}

long long msSince(Clock::time_point t) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t)
      .count();
}

struct Owner {
  using Fsm = FSM<Owner, S, E, GuidPrefix_t>;

  std::mutex m;
  std::vector<int> log;
  std::atomic<int> counter{0};
  std::atomic<int> toutActions{0};
  std::vector<GuidPrefix_t> payloads;
  struct Trans { S from; E ev; S to; };
  std::vector<Trans> observed;
  Clock::time_point lastAction;
  Fsm *fsm = nullptr;
  bool chainInside = false;
  bool chainFirstDone = false;
  bool chainSecondSawFirstDone = false;

  void onGo() { std::lock_guard<std::mutex> g(m); log.push_back(1); lastAction = Clock::now(); }
  void onBack() { std::lock_guard<std::mutex> g(m); log.push_back(2); }
  void onTout() { ++toutActions; lastAction = Clock::now(); }
  void onPing() { ++counter; }
  void onInc() { ++counter; }
  void onPayload() {
    std::lock_guard<std::mutex> g(m);
    payloads.push_back(fsm->currentPayload());
  }
  void onChain1() {
    chainInside = true;
    fsm->postEvent(E::Skip);
    sleepMs(50);
    chainFirstDone = true;
    chainInside = false;
  }
  void onChain2() { chainSecondSawFirstDone = chainFirstDone; }
  void onSeq() { std::lock_guard<std::mutex> g(m); log.push_back(static_cast<int>(counter++)); }

  void observe(S from, E ev, S to) {
    std::lock_guard<std::mutex> g(m);
    observed.push_back({from, ev, to});
  }
};

GuidPrefix_t pfx(uint8_t b) {
  GuidPrefix_t p{};
  p.id[0] = b;
  return p;
}

}

TEST(table_transition) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{0, E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Go, S::B, &Owner::onGo},
                                 {S::B, E::Back, S::A, &Owner::onBack}};
  Owner::Fsm fsm(S::A, states, 3, tr, 2, &o);
  fsm.start();
  REQUIRE_TRUE(fsm.state() == S::A);
  fsm.postEvent(E::Go);
  REQUIRE_TRUE(tests::waitFor([&] { return fsm.state() == S::B; }));
  REQUIRE_TRUE(tests::waitFor([&] { std::lock_guard<std::mutex> g(o.m); return o.log.size() == 1; }));
  sleepMs(50);
  {
    std::lock_guard<std::mutex> g(o.m);
    REQUIRE_TRUE(o.log.size() == 1 && o.log[0] == 1);
  }
  fsm.postEvent(E::Back);
  REQUIRE_TRUE(tests::waitFor([&] { return fsm.state() == S::A; }));
  fsm.stop();
}

TEST(unknown_event_ignored) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{0, E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Go, S::B, &Owner::onGo}};
  Owner::Fsm fsm(S::A, states, 3, tr, 1, &o);
  fsm.start();
  fsm.postEvent(E::Skip);
  fsm.postEvent(E::Back);
  sleepMs(150);
  REQUIRE_TRUE(fsm.state() == S::A);
  {
    std::lock_guard<std::mutex> g(o.m);
    REQUIRE_TRUE(o.log.empty());
  }
  fsm.postEvent(E::Go);
  REQUIRE_TRUE(tests::waitFor([&] { return fsm.state() == S::B; }));
  fsm.stop();
}

TEST(event_order) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{0, E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Inc, S::A, &Owner::onSeq}};
  Owner::Fsm fsm(S::A, states, 3, tr, 1, &o);
  fsm.start();
  for (int i = 0; i < 1000; ++i) {
    fsm.postEvent(E::Inc);
  }
  REQUIRE_TRUE(tests::waitFor([&] { return o.counter.load() == 1000; }));
  fsm.stop();
  std::lock_guard<std::mutex> g(o.m);
  REQUIRE_TRUE(o.log.size() == 1000);
  for (int i = 0; i < 1000; ++i) {
    REQUIRE_TRUE(o.log[i] == i);
  }
}

TEST(timeout_fires) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{tests::scaled(200), E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Tout, S::B, &Owner::onTout}};
  Owner::Fsm fsm(S::A, states, 3, tr, 1, &o);
  auto t0 = Clock::now();
  fsm.start();
  REQUIRE_TRUE(tests::waitFor([&] { return o.toutActions.load() == 1; }));
  long long elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                          o.lastAction - t0).count();
  REQUIRE_TRUE(elapsed >= tests::scaled(200) - 1);
  REQUIRE_TRUE(fsm.state() == S::B);
  fsm.stop();
}

TEST(timeout_zero_waits) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{0, E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Tout, S::B, &Owner::onTout}};
  Owner::Fsm fsm(S::A, states, 3, tr, 1, &o);
  fsm.start();
  sleepMs(400);
  REQUIRE_TRUE(o.toutActions.load() == 0);
  REQUIRE_TRUE(fsm.state() == S::A);
  fsm.stop();
}

TEST(timeout_rearmed_on_reentry) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{tests::scaled(400), E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Ping, S::A, &Owner::onPing},
                                 {S::A, E::Tout, S::B, &Owner::onTout}};
  Owner::Fsm fsm(S::A, states, 3, tr, 2, &o);
  fsm.start();
  sleepMs(250);
  auto pingAt = Clock::now();
  fsm.postEvent(E::Ping);
  REQUIRE_TRUE(tests::waitFor([&] { return o.counter.load() == 1; }));
  sleepMs(250);
  REQUIRE_TRUE(o.toutActions.load() == 0);
  REQUIRE_TRUE(tests::waitFor([&] { return o.toutActions.load() == 1; }));
  long long sincePing = std::chrono::duration_cast<std::chrono::milliseconds>(
                            o.lastAction - pingAt).count();
  REQUIRE_TRUE(sincePing >= tests::scaled(400) - 1);
  fsm.stop();
}

TEST(timeout_cancelled_by_transition) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{tests::scaled(300), E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Go, S::B, &Owner::onGo},
                                 {S::A, E::Tout, S::C, &Owner::onTout},
                                 {S::B, E::Tout, S::C, &Owner::onTout}};
  Owner::Fsm fsm(S::A, states, 3, tr, 3, &o);
  fsm.start();
  sleepMs(50);
  fsm.postEvent(E::Go);
  REQUIRE_TRUE(tests::waitFor([&] { return fsm.state() == S::B; }));
  sleepMs(600);
  REQUIRE_TRUE(fsm.state() == S::B);
  REQUIRE_TRUE(o.toutActions.load() == 0);
  fsm.stop();
}

TEST(timeout_changed_in_observer) {
  struct Ctx {
    FSM<Ctx, S, E>::StateDef *tbl = nullptr;
    std::atomic<bool> reachedC{false};
    void entered(S, E, S to) {
      if (to == S::B) tbl[static_cast<int>(S::B)].timeout_ms = tests::scaled(150);
    }
    void markC() { reachedC = true; }
  } ctx;
  FSM<Ctx, S, E>::StateDef st[] = {{0, E::Tout}, {0, E::Tout}, {0, E::Tout}};
  ctx.tbl = st;
  FSM<Ctx, S, E>::Transition trc[] = {{S::A, E::Go, S::B, nullptr},
                                      {S::B, E::Tout, S::C, &Ctx::markC}};
  FSM<Ctx, S, E> fsm(S::A, st, 3, trc, 2, &ctx);
  fsm.setObserver(&Ctx::entered);
  fsm.start();
  fsm.postEvent(E::Go);
  REQUIRE_TRUE(tests::waitFor([&] { return ctx.reachedC.load(); }));
  REQUIRE_TRUE(fsm.state() == S::C);
  fsm.stop();
}

TEST(post_from_action) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{0, E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Chain, S::B, &Owner::onChain1},
                                 {S::B, E::Skip, S::C, &Owner::onChain2}};
  Owner::Fsm fsm(S::A, states, 3, tr, 2, &o);
  o.fsm = &fsm;
  fsm.start();
  fsm.postEvent(E::Chain);
  REQUIRE_TRUE(tests::waitFor([&] { return fsm.state() == S::C; }));
  REQUIRE_TRUE(o.chainFirstDone);
  REQUIRE_TRUE(o.chainSecondSawFirstDone);
  fsm.stop();
}

TEST(payload_delivered) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{0, E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Inc, S::A, &Owner::onPayload}};
  Owner::Fsm fsm(S::A, states, 3, tr, 1, &o);
  o.fsm = &fsm;
  fsm.start();
  for (uint8_t i = 1; i <= 10; ++i) {
    fsm.postEvent(E::Inc, pfx(i));
  }
  REQUIRE_TRUE(tests::waitFor([&] { std::lock_guard<std::mutex> g(o.m); return o.payloads.size() == 10; }));
  fsm.stop();
  std::lock_guard<std::mutex> g(o.m);
  for (uint8_t i = 1; i <= 10; ++i) {
    REQUIRE_TRUE(o.payloads[i - 1] == pfx(i));
  }
}

TEST(observer_called) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{0, E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Go, S::B, nullptr},
                                 {S::B, E::Back, S::A, &Owner::onBack},
                                 {S::A, E::Skip, S::C, nullptr}};
  Owner::Fsm fsm(S::A, states, 3, tr, 3, &o);
  fsm.setObserver(&Owner::observe);
  fsm.start();
  fsm.postEvent(E::Go);
  fsm.postEvent(E::Back);
  fsm.postEvent(E::Skip);
  fsm.postEvent(E::Ping);
  REQUIRE_TRUE(tests::waitFor([&] { return fsm.state() == S::C; }));
  sleepMs(50);
  fsm.stop();
  std::lock_guard<std::mutex> g(o.m);
  REQUIRE_TRUE(o.observed.size() == 3);
  REQUIRE_TRUE(o.observed[0].from == S::A && o.observed[0].ev == E::Go && o.observed[0].to == S::B);
  REQUIRE_TRUE(o.observed[1].from == S::B && o.observed[1].ev == E::Back && o.observed[1].to == S::A);
  REQUIRE_TRUE(o.observed[2].from == S::A && o.observed[2].ev == E::Skip && o.observed[2].to == S::C);
}

TEST(stop_while_blocked) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{0, E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Go, S::B, nullptr}};
  Owner::Fsm fsm(S::A, states, 3, tr, 1, &o);
  fsm.start();
  sleepMs(100);
  auto t0 = Clock::now();
  fsm.stop();
  REQUIRE_TRUE(msSince(t0) < tests::scaled(1000));
  fsm.stop();
  Owner::Fsm::StateDef st2[] = {{60000, E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm fsm2(S::A, st2, 3, tr, 1, &o);
  fsm2.start();
  sleepMs(50);
  t0 = Clock::now();
  fsm2.stop();
  REQUIRE_TRUE(msSince(t0) < tests::scaled(1000));
}

TEST(post_after_stop) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{0, E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Go, S::B, &Owner::onGo}};
  Owner::Fsm fsm(S::A, states, 3, tr, 1, &o);
  fsm.start();
  fsm.stop();
  fsm.postEvent(E::Go);
  REQUIRE_TRUE(fsm.tryStep(S::A, E::Go));
  sleepMs(150);
  REQUIRE_TRUE(fsm.state() == S::A);
  std::lock_guard<std::mutex> g(o.m);
  REQUIRE_TRUE(o.log.empty());
}

TEST(concurrent_post) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{0, E::Tout}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Inc, S::A, &Owner::onInc}};
  Owner::Fsm fsm(S::A, states, 3, tr, 1, &o);
  fsm.start();
  constexpr int kThreads = 4, kEach = 10000;
  std::vector<std::thread> ts;
  for (int t = 0; t < kThreads; ++t) {
    ts.emplace_back([&] {
      for (int i = 0; i < kEach; ++i) fsm.postEvent(E::Inc);
    });
  }
  for (auto &t : ts) t.join();
  REQUIRE_TRUE(tests::waitFor([&] { return o.counter.load() >= kThreads * kEach; }, 30000));
  sleepMs(100);
  REQUIRE_TRUE(o.counter.load() == kThreads * kEach);
  fsm.stop();
}

int main(int argc, char **argv) { RUN_TESTS(argc, argv); }

TEST(entry_after_action) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{0, E::Tout}, {0, E::Tout, &Owner::onBack}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Go, S::B, &Owner::onGo}};
  Owner::Fsm fsm(S::A, states, 3, tr, 1, &o);
  fsm.setObserver(&Owner::observe);
  fsm.start();
  sleepMs(50);
  {
    std::lock_guard<std::mutex> g(o.m);
    REQUIRE_TRUE(o.log.empty());
  }
  fsm.postEvent(E::Go);
  REQUIRE_TRUE(tests::waitFor([&] { std::lock_guard<std::mutex> g(o.m); return o.observed.size() == 1; }));
  fsm.stop();
  std::lock_guard<std::mutex> g(o.m);
  REQUIRE_TRUE(o.log.size() == 2 && o.log[0] == 1 && o.log[1] == 2);
}

TEST(entry_on_self_loop) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{0, E::Tout, &Owner::onPing}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Inc, S::A, nullptr},
                                 {S::A, E::Go, S::B, nullptr}};
  Owner::Fsm fsm(S::A, states, 3, tr, 2, &o);
  fsm.start();
  for (int i = 0; i < 3; ++i) {
    fsm.postEvent(E::Inc);
  }
  REQUIRE_TRUE(tests::waitFor([&] { return o.counter.load() == 3; }));
  fsm.postEvent(E::Go);
  REQUIRE_TRUE(tests::waitFor([&] { return fsm.state() == S::B; }));
  sleepMs(50);
  fsm.stop();
  REQUIRE_TRUE(o.counter.load() == 3);
}

TEST(internal_transition) {
  Owner o;
  Owner::Fsm::StateDef states[] = {{tests::scaled(400), E::Tout, &Owner::onPing}, {0, E::Tout}, {0, E::Tout}};
  Owner::Fsm::Transition tr[] = {{S::A, E::Inc, S::A, &Owner::onGo, true},
                                 {S::A, E::Tout, S::B, nullptr}};
  Owner::Fsm fsm(S::A, states, 3, tr, 2, &o);
  fsm.setObserver(&Owner::observe);
  auto t0 = Clock::now();
  fsm.start();
  for (int i = 0; i < 3; ++i) {
    sleepMs(100);
    fsm.postEvent(E::Inc);
  }
  REQUIRE_TRUE(tests::waitFor([&] { return fsm.state() == S::B; }));
  REQUIRE_TRUE(msSince(t0) < tests::scaled(650));
  fsm.stop();
  std::lock_guard<std::mutex> g(o.m);
  REQUIRE_TRUE(o.log.size() == 3);
  REQUIRE_TRUE(o.counter.load() == 0);
  REQUIRE_TRUE(o.observed.size() == 1 && o.observed[0].ev == E::Tout);
}
