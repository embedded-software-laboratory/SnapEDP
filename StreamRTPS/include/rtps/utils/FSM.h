#ifndef RTPS_FSM_H
#define RTPS_FSM_H

#include "rtps/utils/Lock.h"

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <queue>
#include <thread>
#include <utility>

namespace rtps {

struct FSMNoPayload {};

// generic FSM over Ctx with its own dispatch thread, postEvent is thread safe and reentrant since actions run without the mutex, each event snapshots its payload
template <class Ctx, class State, class Event, class Payload = FSMNoPayload>
class FSM {
public:
  using Action = void (Ctx::*)();
  using Observer = void (Ctx::*)(State from, Event event, State to);

  struct StateDef {
    long long timeout_ms;    // 0 disables the timer for this state
    Event timeout_event;     // event posted by the dispatcher when timeout_ms elapses
    Action on_enter = nullptr;
  };

  struct Transition {
    State from;
    Event event;
    State to;
    Action action;           // nullptr means no action
    bool internal = false;
  };

  FSM(State initial, const StateDef *states, std::size_t numStates,
      const Transition *transitions, std::size_t numTransitions, Ctx *ctx)
      : m_current(initial), m_states(states), m_numStates(numStates),
        m_transitions(transitions), m_numTransitions(numTransitions),
        m_ctx(ctx), m_stateEnteredMs(nowMs()) {}

  ~FSM() { stop(); }

  FSM(const FSM &) = delete;
  FSM &operator=(const FSM &) = delete;
  FSM(FSM &&) = delete;
  FSM &operator=(FSM &&) = delete;

  State state() {
    std::lock_guard<std::mutex> g(m_mutex);
    return m_current;
  }

  // enqueue an event and wake the dispatcher, safe from any thread incl inside an action
  void postEvent(Event e) { postEvent(e, Payload{}); }
  void postEvent(Event e, Payload p) {
    {
      std::lock_guard<std::mutex> g(m_mutex);
      m_queue.push(QueuedEvent{e, std::move(p)});
    }
    m_cv.notify_one();
  }

  // backwards compat alias for callers that still spell it step
  void step(Event e) { postEvent(e); }
  void step(Event e, Payload p) { postEvent(e, std::move(p)); }

  // atomic conditional post, enqueue only if current state matches expected else return false, closes the race between state and postEvent
  bool tryStep(State expected, Event e) { return tryStep(expected, e, Payload{}); }
  bool tryStep(State expected, Event e, Payload p) {
    {
      std::lock_guard<std::mutex> g(m_mutex);
      if (m_current != expected) {
        return false;
      }
      m_queue.push(QueuedEvent{e, std::move(p)});
    }
    m_cv.notify_one();
    return true;
  }

  const Payload &currentPayload() const { return m_currentPayload; }

  // Launch the dispatch thread
  void start() {
    std::lock_guard<std::mutex> g(m_mutex);
    if (m_running) {
      return;
    }
    m_running = true;
    m_stateEnteredMs = nowMs();
    m_thread = std::thread(&FSM::runDispatch, this);
  }

  // Signal the dispatch thread to exit and join it
  void stop() {
    {
      std::lock_guard<std::mutex> g(m_mutex);
      if (!m_running) {
        return;
      }
      m_running = false;
    }
    m_cv.notify_all();
    if (m_thread.joinable()) {
      m_thread.join();
    }
  }

  void setObserver(Observer obs) {
    std::lock_guard<std::mutex> g(m_mutex);
    m_observer = obs;
  }

private:
  static long long nowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
               steady_clock::now().time_since_epoch())
        .count();
  }

  void runDispatch() {
    std::unique_lock<std::mutex> lock(m_mutex);
    while (m_running) {
      const StateDef &sd = m_states[static_cast<std::size_t>(m_current)];
      auto haveWork = [&] { return !m_running || !m_queue.empty(); };

      if (sd.timeout_ms == 0) {
        m_cv.wait(lock, haveWork);
      } else {
        const auto deadline = std::chrono::steady_clock::time_point(
            std::chrono::milliseconds(m_stateEnteredMs + sd.timeout_ms));
        m_cv.wait_until(lock, deadline, haveWork);
      }

      if (!m_running) {
        break;
      }

      Event e;
      Payload p{};
      if (!m_queue.empty()) {
        e = m_queue.front().event;
        p = std::move(m_queue.front().payload);
        m_queue.pop();
      } else {
        // CV returned without a queued event so this is the timeout path
        const StateDef &cur = m_states[static_cast<std::size_t>(m_current)];
        if (cur.timeout_ms == 0 ||
            nowMs() - m_stateEnteredMs < cur.timeout_ms) {
          continue;
        }
        e = cur.timeout_event;
      }

      const Transition *tr = findTransition(m_current, e);
      if (tr == nullptr) {
        continue;
      }

      const State from = m_current;
      const State to = tr->to;
      const Action action = tr->action;
      const bool internal = tr->internal;
      const Action onEnter = internal ? nullptr : m_states[static_cast<std::size_t>(to)].on_enter;
      const Observer observer = internal ? nullptr : m_observer;
      Ctx *const ctx = m_ctx;

      if (!internal) {
        m_current = to;
        m_stateEnteredMs = nowMs();
      }
      m_currentPayload = std::move(p);

      lock.unlock();
      if (action != nullptr) {
        (ctx->*action)();
      }
      if (onEnter != nullptr) {
        (ctx->*onEnter)();
      }
      if (observer != nullptr) {
        (ctx->*observer)(from, e, to);
      }
      lock.lock();
    }
  }

  const Transition *findTransition(State current, Event e) const {
    for (std::size_t i = 0; i < m_numTransitions; ++i) {
      const Transition &tr = m_transitions[i];
      if (tr.from != current || tr.event != e) {
        continue;
      }
      return &tr;
    }
    return nullptr;
  }

  struct QueuedEvent {
    Event event;
    Payload payload;
  };

  std::mutex m_mutex;
  std::condition_variable m_cv;
  std::queue<QueuedEvent> m_queue;
  bool m_running = false;
  std::thread m_thread;

  State m_current;
  const StateDef *m_states;
  std::size_t m_numStates;
  const Transition *m_transitions;
  std::size_t m_numTransitions;
  Ctx *m_ctx;
  long long m_stateEnteredMs;
  Observer m_observer = nullptr;
  Payload m_currentPayload{};
};

} // namespace rtps

#endif // RTPS_FSM_H
