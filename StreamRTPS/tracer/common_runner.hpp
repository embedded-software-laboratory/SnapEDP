#pragma once

#include "common_config.hpp"

#include "rtps/config.h"
#include "rtps/config/FeatureQOS.h"
#include "rtps/entities/Domain.h"
#include "rtps/rtps.h"
#include "soatracer_tp.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace tracer {

constexpr int EXIT_OK = 0;
constexpr int EXIT_TIMEOUT = 2;
constexpr int EXIT_SEND_FAIL = 3;
constexpr int EXIT_RECV_FAIL = 4;
constexpr int EXIT_CONFIG = 5;
constexpr uint32_t WARMUP_SEQ_PREFIX = 0x80000000u;
constexpr uint32_t WARMUP_SEQ_MASK = 0x7FFFFFFFu;
#ifdef TRACER_KEEP_WARMUP
constexpr std::chrono::seconds WARMUP_DURATION(3);
constexpr std::chrono::milliseconds PARTICIPANT_ENDPOINT_DELAY(1000);
#else
constexpr std::chrono::seconds WARMUP_DURATION(0);
constexpr std::chrono::milliseconds PARTICIPANT_ENDPOINT_DELAY(0);
#endif
constexpr int TIMEOUT_SLACK_MS = 10000;
constexpr int POST_PUBLISH_GRACE_MS = 5000;

inline rtps::HeartbeatPolicyMode tracer_heartbeat_policy_mode(
    bool , bool ) {
  return rtps::HeartbeatPolicyMode::AdaptiveFrequency;
}

inline rtps::DiscoveryMode tracer_discovery_mode(const std::string &name) {
  if (name == "gossip") return rtps::DiscoveryMode::Snap;
  return rtps::DiscoveryMode::Standard;
}

inline const std::chrono::steady_clock::time_point &process_start_time() {
  static const std::chrono::steady_clock::time_point kStart =
      std::chrono::steady_clock::now();
  return kStart;
}

inline uint64_t since_start_ms() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - process_start_time())
          .count());
}

inline bool mark_first(std::atomic<bool> &flag) {
  bool expected = false;
  return flag.compare_exchange_strong(expected, true);
}

inline void emit_log_line(const std::string &line) {
  std::cerr << line;
}

inline void emit_diag_event(const char *role, const std::string &topic,
                            const std::string &event,
                            const nlohmann::json &extra = nlohmann::json::object()) {
  nlohmann::json obj;
  obj["t_ms"] = since_start_ms();
  obj["role"] = role;
  obj["topic"] = topic;
  obj["event"] = event;
  if (extra.is_object()) {
    for (auto it = extra.begin(); it != extra.end(); ++it) {
      obj[it.key()] = it.value();
    }
  }
  emit_log_line("TEST_DIAG: " + obj.dump() + "\n");
}

struct RuntimeOptions {
  int samples = -1;
  std::string config_json;
  int timeout_ms = -1;
  double min_recv_pct = 0.9;
  int64_t start_epoch_ms = -1;
};

struct TraceConfig {
  bool enabled = false;
  int test_id = 0;
};

struct SharedState {
  std::atomic<bool> stop{false};
  std::atomic<bool> failed{false};
  std::atomic<int> fail_code{EXIT_OK};

  std::mutex reason_mutex;
  std::string fail_reason;

  void fail_once(int code, const std::string &reason) {
    bool expected = false;
    if (!failed.compare_exchange_strong(expected, true)) {
      return;
    }

    fail_code.store(code);
    {
      std::lock_guard<std::mutex> lock(reason_mutex);
      fail_reason = reason;
    }
    stop.store(true);
    emit_log_line("TEST_FAIL: " + reason + "\n");
  }
};

inline bool parse_runtime_options(int argc, char **argv, RuntimeOptions &opts,
                                  std::string &error) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg(argv[i]);
    if ((arg == "--samples" || arg == "-s") && i + 1 < argc) {
      opts.samples = std::atoi(argv[++i]);
      continue;
    }
    if ((arg == "--config" || arg == "-c") && i + 1 < argc) {
      opts.config_json = argv[++i];
      continue;
    }
    if (arg == "--timeout-ms" && i + 1 < argc) {
      opts.timeout_ms = std::atoi(argv[++i]);
      continue;
    }
    if (arg == "--min-recv-pct" && i + 1 < argc) {
      opts.min_recv_pct = std::atof(argv[++i]);
      continue;
    }
    if (arg == "--start-epoch-ms" && i + 1 < argc) {
      opts.start_epoch_ms = std::strtoll(argv[++i], nullptr, 10);
      continue;
    }
  }

  if (opts.min_recv_pct < 0.0 || opts.min_recv_pct > 1.0) {
    error = "--min-recv-pct must be in [0.0, 1.0]";
    return false;
  }

  if (opts.samples <= 0) {
    error = "Missing or invalid --samples";
    return false;
  }
  if (opts.config_json.empty()) {
    error = "Missing --config JSON argument";
    return false;
  }
  if (opts.timeout_ms == 0 || opts.timeout_ms < -1) {
    error = "--timeout-ms must be positive when provided";
    return false;
  }
  return true;
}

inline void wait_for_start_barrier(int64_t start_epoch_ms, const char *role,
                                   const std::string &label) {
  if (start_epoch_ms <= 0) {
    return;
  }
  const std::chrono::system_clock::time_point target{
      std::chrono::milliseconds(start_epoch_ms)};
  if (std::chrono::system_clock::now() >= target) {
    emit_diag_event(role, label, "start_barrier_late", {
        {"target_epoch_ms", start_epoch_ms},
    });
    return;
  }
  std::this_thread::sleep_until(target);
}

inline bool is_warmup_seq(uint32_t seq) {
  return (seq & WARMUP_SEQ_PREFIX) != 0u;
}

inline void encode_payload(std::vector<uint8_t> &buffer, uint32_t seq) {
  std::memset(buffer.data(), static_cast<int>(seq & 0xFFU), buffer.size());
  std::memcpy(buffer.data(), &seq, sizeof(seq));
}

inline void encode_warmup_payload(std::vector<uint8_t> &buffer, uint32_t warmup_id) {
  const uint32_t warmup_seq = WARMUP_SEQ_PREFIX | (warmup_id & WARMUP_SEQ_MASK);
  std::memset(buffer.data(), static_cast<int>(warmup_seq & 0xFFU), buffer.size());
  std::memcpy(buffer.data(), &warmup_seq, sizeof(warmup_seq));
}

inline bool is_warmup_payload(const uint8_t *data, size_t size) {
  if (size < sizeof(uint32_t)) {
    return false;
  }
  uint32_t seq = 0;
  std::memcpy(&seq, data, sizeof(seq));
  if (!is_warmup_seq(seq)) {
    return false;
  }
  const uint8_t expected = static_cast<uint8_t>(seq & 0xFFU);
  for (size_t i = sizeof(uint32_t); i < size; ++i) {
    if (data[i] != expected) {
      return false;
    }
  }
  return true;
}

inline bool verify_payload(const uint8_t *data, size_t size, uint32_t seq) {
  if (size < sizeof(uint32_t)) {
    return false;
  }

  uint32_t encoded_seq = 0;
  std::memcpy(&encoded_seq, data, sizeof(encoded_seq));
  if (encoded_seq != seq) {
    return false;
  }

  const uint8_t expected = static_cast<uint8_t>(seq & 0xFFU);
  for (size_t i = sizeof(uint32_t); i < size; ++i) {
    if (data[i] != expected) {
      return false;
    }
  }
  return true;
}

inline std::chrono::microseconds frequency_interval(int hz) {
  const int bounded_hz = std::max(1, hz);
  return std::chrono::microseconds(std::max(1, 1000000 / bounded_hz));
}

inline bool read_process_stats_raw(
    uint64_t &proc_ticks_out,
    uint64_t &total_ticks_out,
    uint64_t &rss_bytes_out) {
  std::ifstream stat_file("/proc/self/stat");
  if (!stat_file.is_open()) {
    return false;
  }
  std::string stat_line;
  std::getline(stat_file, stat_line);
  stat_file.close();

  std::istringstream stat_iss(stat_line);
  std::string token;
  for (int i = 0; i < 13; ++i) {
    if (!(stat_iss >> token)) {
      return false;
    }
  }
  uint64_t utime = 0;
  uint64_t stime = 0;
  if (!(stat_iss >> utime >> stime)) {
    return false;
  }
  proc_ticks_out = utime + stime;

  std::ifstream cpu_file("/proc/stat");
  if (!cpu_file.is_open()) {
    return false;
  }
  std::string cpu_line;
  std::getline(cpu_file, cpu_line);
  cpu_file.close();

  std::istringstream cpu_iss(cpu_line);
  std::string cpu_label;
  cpu_iss >> cpu_label;
  uint64_t user = 0, nice = 0, system = 0, idle = 0, iowait = 0, irq = 0,
           softirq = 0, steal = 0;
  cpu_iss >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal;
  total_ticks_out =
      user + nice + system + idle + iowait + irq + softirq + steal;

  std::ifstream statm_file("/proc/self/statm");
  if (!statm_file.is_open()) {
    return false;
  }
  uint64_t size_pages = 0;
  uint64_t rss_pages = 0;
  statm_file >> size_pages >> rss_pages;
  statm_file.close();

  long page_size = ::sysconf(_SC_PAGESIZE);
  if (page_size <= 0) {
    page_size = 4096;
  }
  rss_bytes_out = rss_pages * static_cast<uint64_t>(page_size);

  return true;
}

inline void emit_experiment_begin(const TraceConfig &trace_cfg) {
  if (!trace_cfg.enabled) {
    return;
  }
  lttng_ust_tracepoint(soatracer_trace_provider, experiment_begin,
                       static_cast<uint32_t>(trace_cfg.test_id),
                       static_cast<uint64_t>(trace_cfg.test_id));
}

inline void emit_experiment_end(const TraceConfig &trace_cfg) {
  if (!trace_cfg.enabled) {
    return;
  }
  lttng_ust_tracepoint(soatracer_trace_provider, experiment_end,
                       static_cast<uint32_t>(trace_cfg.test_id),
                       static_cast<uint64_t>(trace_cfg.test_id));
}

struct PublisherWorker {
  ServiceConfig cfg;
  int samples;
  int64_t start_epoch_ms = -1;
  bool reliable_transport;
  rtps::DiscoveryMode discovery_mode = rtps::DiscoveryMode::Standard;
  TraceConfig trace_cfg;
  std::shared_ptr<SharedState> state;
  std::atomic<bool> done{false};
  std::atomic<bool> first_warmup_sent_logged{false};
  std::atomic<bool> first_regular_sent_logged{false};

  std::vector<int> topic_timing_offsets_us;

  std::vector<uint32_t> sent_counts;
  std::vector<uint32_t> last_seq_per_topic;

  void operator()() {
    const std::string &diag_label = cfg.topics.front();
    const size_t num_topics = cfg.topics.size();

    if (cfg.udp_loss_random_pct >= 0.0f) {
      rtps::Config::UDP_PACKET_LOSS_RATE.store(
          cfg.udp_loss_random_pct / 100.0f, std::memory_order_relaxed);
      emit_diag_event("publisher", diag_label, "loss_config", {
          {"random_pct", cfg.udp_loss_random_pct},
      });
    }

    if (cfg.gossip_fsm_initial_ms > 0) {
      rtps::Config::SNAP_TIMEOUT_INITIAL_MS.store(
          cfg.gossip_fsm_initial_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_announcing_ms > 0) {
      rtps::Config::SNAP_TIMEOUT_ANNOUNCING_MS.store(
          cfg.gossip_fsm_announcing_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_jitter_ms > 0) {
      rtps::Config::SNAP_JITTER_MAX_MS.store(
          cfg.gossip_fsm_jitter_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_rerequest_ms > 0) {
      rtps::Config::SNAP_TIMEOUT_RETRANSMIT_MS.store(
          cfg.gossip_fsm_rerequest_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_rerequest_bound > 0) {
      rtps::Config::REQUEST_RETRY_BOUND.store(
          cfg.gossip_fsm_rerequest_bound, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_reconcile_base_ms > 0) {
      rtps::Config::SNAP_RECONCILE_BASE_MS.store(
          cfg.gossip_fsm_reconcile_base_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_reconcile_cap_ms > 0) {
      rtps::Config::SNAP_RECONCILE_CAP_MS.store(
          cfg.gossip_fsm_reconcile_cap_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_spdp_resend_ms > 0) {
      rtps::Config::SPDP_RESEND_PERIOD_MS.store(
          cfg.gossip_fsm_spdp_resend_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_spdp_lease_ms > 0) {
      rtps::Config::SPDP_LEASE_DURATION_MS.store(
          cfg.gossip_fsm_spdp_lease_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_spdp_burst_scale > 0) {
      rtps::Config::SPDP_BURST_SCALE_PCT.store(
          cfg.gossip_fsm_spdp_burst_scale, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_spdp_burst_window_ms > 0) {
      rtps::Config::SPDP_BURST_WINDOW_MS.store(
          cfg.gossip_fsm_spdp_burst_window_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_edp_defer_rounds > 0) {
      rtps::Config::SNAP_QUIESCENCE_ROUNDS.store(
          cfg.gossip_fsm_edp_defer_rounds, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_edp_force_after_rounds > 0) {
      rtps::Config::SNAP_QUIESCENCE_MAX_ROUNDS.store(
          cfg.gossip_fsm_edp_force_after_rounds, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_reconcile_grace_ms != 0xFFFFFFFFu) {
      rtps::Config::SNAP_RECONCILE_GRACE_MS.store(
          cfg.gossip_fsm_reconcile_grace_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_request_target_policy != 0xFFFFFFFFu) {
      rtps::Config::SNAP_SERVER_POLICY.store(
          cfg.gossip_fsm_request_target_policy, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_reannounce_after_resync > 0) {
      rtps::Config::SNAP_REANNOUNCE_AFTER_RESYNC.store(
          cfg.gossip_fsm_reannounce_after_resync, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_defer_response_cap >= 0) {
      rtps::Config::SNAP_DEFER_CAP.store(
          static_cast<uint32_t>(cfg.gossip_fsm_defer_response_cap), std::memory_order_relaxed);
    }

    emit_diag_event("gossip_fsm", diag_label, "gossip_fsm_config", {
        {"initial_ms", rtps::Config::SNAP_TIMEOUT_INITIAL_MS.load(std::memory_order_relaxed)},
        {"announcing_ms", rtps::Config::SNAP_TIMEOUT_ANNOUNCING_MS.load(std::memory_order_relaxed)},
        {"jitter_ms", rtps::Config::SNAP_JITTER_MAX_MS.load(std::memory_order_relaxed)},
        {"rerequest_ms", rtps::Config::SNAP_TIMEOUT_RETRANSMIT_MS.load(std::memory_order_relaxed)},
        {"rerequest_bound", rtps::Config::REQUEST_RETRY_BOUND.load(std::memory_order_relaxed)},
        {"reconcile_base_ms", rtps::Config::SNAP_RECONCILE_BASE_MS.load(std::memory_order_relaxed)},
        {"reconcile_cap_ms", rtps::Config::SNAP_RECONCILE_CAP_MS.load(std::memory_order_relaxed)},
        {"reconcile_grace_ms", rtps::Config::SNAP_RECONCILE_GRACE_MS.load(std::memory_order_relaxed)},
        {"spdp_resend_ms", rtps::Config::SPDP_RESEND_PERIOD_MS.load(std::memory_order_relaxed)},
        {"spdp_lease_ms", rtps::Config::SPDP_LEASE_DURATION_MS.load(std::memory_order_relaxed)},
        {"spdp_burst_scale", rtps::Config::SPDP_BURST_SCALE_PCT.load(std::memory_order_relaxed)},
        {"spdp_burst_window_ms", rtps::Config::SPDP_BURST_WINDOW_MS.load(std::memory_order_relaxed)},
        {"edp_defer_rounds", rtps::Config::SNAP_QUIESCENCE_ROUNDS.load(std::memory_order_relaxed)},
        {"edp_force_after_rounds", rtps::Config::SNAP_QUIESCENCE_MAX_ROUNDS.load(std::memory_order_relaxed)},
        {"request_target_policy", rtps::Config::SNAP_SERVER_POLICY.load(std::memory_order_relaxed)},
        {"reannounce_after_resync", rtps::Config::SNAP_REANNOUNCE_AFTER_RESYNC.load(std::memory_order_relaxed)},
        {"defer_response_cap", rtps::Config::SNAP_DEFER_CAP.load(std::memory_order_relaxed)},
    });

    if (cfg.participant_delay_ms > 0) {
      wait_for_start_barrier(start_epoch_ms, "publisher", diag_label);
      emit_diag_event("publisher", diag_label, "late_join", {
          {"sleeping_ms", cfg.participant_delay_ms},
      });
      std::this_thread::sleep_for(
          std::chrono::milliseconds(cfg.participant_delay_ms));
    }

    rtps::FeatureQOS qos(discovery_mode,
                         rtps::HeartbeatPolicyMode::AdaptiveFrequency);
    rtps::Domain domain(qos);
    rtps::Participant *part = domain.createParticipant();
    if (part == nullptr) {
      state->fail_once(EXIT_SEND_FAIL,
                       "publisher failed to create participant for " + diag_label);
      done.store(true);
      return;
    }

    std::vector<rtps::Writer *> writers;
    writers.reserve(num_topics);
    for (size_t t = 0; t < num_topics; ++t) {
      const bool effective_reliable = cfg.reliable || reliable_transport;
      rtps::Writer *writer =
          domain.createWriter(*part, cfg.topics[t].c_str(), "TracerPayload",
                              effective_reliable, true);
      if (writer == nullptr) {
        state->fail_once(EXIT_SEND_FAIL,
                         "publisher failed to create writer for " + cfg.topics[t]);
        done.store(true);
        return;
      }
      writer->setPlADeadline(static_cast<uint32_t>(cfg.aggregation_deadline_ms));
      writers.push_back(writer);
      emit_diag_event("publisher", cfg.topics[t], "writer_created", {
          {"idx", t},
          {"plad_ms", cfg.aggregation_deadline_ms},
      });
    }

    if (cfg.participant_delay_ms == 0) {
      wait_for_start_barrier(start_epoch_ms, "publisher", diag_label);
    }

    if (trace_cfg.enabled) {
      lttng_ust_tracepoint(soatracer_trace_provider, participant_join,
                           cfg.idx, trace_cfg.test_id, "publisher");
    }
    if (!domain.completeInit()) {
      state->fail_once(EXIT_SEND_FAIL,
                       "publisher failed to complete init for " + diag_label);
      done.store(true);
      return;
    }

    emit_diag_event("publisher", diag_label, "participant_ready", {
        {"topics", num_topics},
    });

    if (cfg.participant_kill_ms >= 0) {
      auto st = state;
      const auto tc = trace_cfg;
      const int svc_idx = cfg.idx;
      const bool only_root = cfg.kill_if_root != 0;
      arm_kill_at(start_epoch_ms, cfg.participant_kill_ms,
          [st, tc, svc_idx, only_root, part]() {
            if (st->stop.load()) return false;
            const bool is_root =
                part->getSEDPAgent().getCurrentRoot() == part->m_guidPrefix;
            if (only_root && !is_root) return false;
            if (tc.enabled) {
              lttng_ust_tracepoint(soatracer_trace_provider, participant_kill,
                                   svc_idx, tc.test_id, "publisher",
                                   is_root ? 1 : 0);
            }
            std::cout << "participant_kill: idx=" << svc_idx
                      << " was_root=" << (is_root ? 1 : 0) << std::endl;
            return true;
          });
    }

    const bool ep_delete_armed = cfg.endpoint_delete_ms >= 0 &&
        start_epoch_ms > 0 &&
        static_cast<size_t>(cfg.endpoint_delete_idx) < writers.size();
    const std::chrono::system_clock::time_point ep_delete_due{
        std::chrono::milliseconds(start_epoch_ms +
                                  std::max(cfg.endpoint_delete_ms, 0))};
    bool ep_deleted = false;

    const auto publisher_start = std::chrono::steady_clock::now();
    std::vector<bool> first_publish_logged(num_topics, false);
    sent_counts.assign(num_topics, 0);
    last_seq_per_topic.assign(num_topics, 0);

    std::vector<uint8_t> payload(static_cast<size_t>(cfg.payload_size), 0);

    const std::chrono::microseconds interval = frequency_interval(cfg.frequency_hz);
    const std::chrono::microseconds offset(cfg.publish_offset_us);
    const std::chrono::microseconds timing_offset(cfg.timing_offset_us);
    const bool has_per_topic_offsets = !topic_timing_offsets_us.empty() &&
        topic_timing_offsets_us.size() == writers.size();
    std::chrono::steady_clock::time_point next_deadline =
        std::chrono::steady_clock::now() + interval;

    const std::chrono::steady_clock::time_point warmup_end =
        std::chrono::steady_clock::now() + WARMUP_DURATION;
    uint32_t warmup_id = 0;
    while (!state->stop.load() && std::chrono::steady_clock::now() < warmup_end) {
      if (timing_offset.count() > 0) {
        std::this_thread::sleep_for(timing_offset);
      }
      const auto period_start_wu = std::chrono::steady_clock::now();
      for (size_t t = 0; t < writers.size(); ++t) {
        if (has_per_topic_offsets && topic_timing_offsets_us[t] > 0) {
          std::this_thread::sleep_until(
              period_start_wu + std::chrono::microseconds(topic_timing_offsets_us[t]));
        }
        const uint32_t current_warmup_id = warmup_id++;
        encode_warmup_payload(payload, current_warmup_id);
        if (current_warmup_id == 0 && mark_first(first_warmup_sent_logged)) {
          emit_diag_event("publisher", cfg.topics[t], "first_warmup_sent", {
              {"warmup_id", 0},
          });
        }
        if (writers[t]->newChange(rtps::ChangeKind_t::ALIVE, payload.data(),
                              static_cast<rtps::DataSize_t>(payload.size())) == nullptr) {
          state->fail_once(EXIT_SEND_FAIL,
                           "publisher warmup newChange failed topic=" + cfg.topics[t]);
          done.store(true);
          return;
        }
        if (!has_per_topic_offsets && offset.count() > 0 && t + 1 < writers.size()) {
          std::this_thread::sleep_for(offset);
        }
      }
      std::this_thread::sleep_until(next_deadline);
      next_deadline += interval;
    }

    uint32_t seq = 0;
    for (int i = 0; i < samples; ++i) {
      if (state->stop.load()) {
        done.store(true);
        return;
      }

      if (ep_delete_armed && !ep_deleted &&
          std::chrono::system_clock::now() >= ep_delete_due) {
        ep_deleted = true;
        const size_t di = static_cast<size_t>(cfg.endpoint_delete_idx);
        if (writers[di] != nullptr) {
          if (trace_cfg.enabled) {
            lttng_ust_tracepoint(soatracer_trace_provider, endpoint_delete,
                                 cfg.topics[di].c_str(), cfg.idx,
                                 trace_cfg.test_id);
          }
          emit_diag_event("publisher", cfg.topics[di], "endpoint_delete", {
              {"slot", di},
          });
          domain.removeWriter(*part, writers[di]);
          writers[di] = nullptr;
        }
      }

      if (timing_offset.count() > 0) {
        std::this_thread::sleep_for(timing_offset);
      }

      seq = static_cast<uint32_t>(i);
      encode_payload(payload, seq);

      const auto period_start = std::chrono::steady_clock::now();
      for (size_t t = 0; t < writers.size(); ++t) {
        if (writers[t] == nullptr) {
          continue;
        }
        if (has_per_topic_offsets && topic_timing_offsets_us[t] > 0) {
          std::this_thread::sleep_until(
              period_start + std::chrono::microseconds(topic_timing_offsets_us[t]));
        }

        if (seq == 0 && t == 0 && mark_first(first_regular_sent_logged)) {
          emit_diag_event("publisher", cfg.topics[t], "first_regular_sent", {
              {"seq", 0},
          });
        }

        if (!first_publish_logged[t]) {
          first_publish_logged[t] = true;
          const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now() - publisher_start).count();
          if (trace_cfg.enabled) {
            lttng_ust_tracepoint(soatracer_trace_provider, first_publish,
                                 cfg.topics[t].c_str(), cfg.idx,
                                 trace_cfg.test_id,
                                 static_cast<uint64_t>(elapsed));
          }
        }

        if (trace_cfg.enabled) {
          lttng_ust_tracepoint(soatracer_trace_provider, pre_publish,
                               cfg.topics[t].c_str(), cfg.idx, cfg.type_index,
                               static_cast<int>(seq), trace_cfg.test_id);
        }

        if (writers[t]->newChange(rtps::ChangeKind_t::ALIVE, payload.data(),
                              static_cast<rtps::DataSize_t>(payload.size())) == nullptr) {
          state->fail_once(EXIT_SEND_FAIL,
                           "publisher newChange failed topic=" + cfg.topics[t] +
                               " seq=" + std::to_string(seq));
          done.store(true);
          return;
        }

        if (trace_cfg.enabled) {
          lttng_ust_tracepoint(soatracer_trace_provider, after_publish,
                               cfg.topics[t].c_str(), cfg.idx, cfg.type_index,
                               static_cast<int>(seq), trace_cfg.test_id);
        }

        if (t < sent_counts.size()) {
          sent_counts[t] += 1;
          last_seq_per_topic[t] = seq;
        }

        if (!has_per_topic_offsets && offset.count() > 0 && t + 1 < writers.size()) {
          std::this_thread::sleep_for(offset);
        }
      }

      if (cfg.sending_pattern == "sporadic") {
        static thread_local std::mt19937 sporadic_rng{std::random_device{}()};
        const auto min_us = interval.count() / 4;
        const auto max_us = interval.count() * 5 / 2;
        std::uniform_int_distribution<long> dist(
            std::max(1L, min_us), std::max(2L, max_us));
        std::this_thread::sleep_for(std::chrono::microseconds(dist(sporadic_rng)));
        next_deadline = std::chrono::steady_clock::now() + interval;
      } else {
        std::this_thread::sleep_until(next_deadline);
        next_deadline += interval;
      }
    }


    if (trace_cfg.enabled) {
      for (size_t t = 0; t < writers.size(); ++t) {
        if (writers[t] == nullptr) {
          continue;
        }
        lttng_ust_tracepoint(soatracer_trace_provider, published_all,
                             cfg.topics[t].c_str(), cfg.idx, cfg.type_index,
                             static_cast<int>(seq), trace_cfg.test_id);
      }
    }

    {
      const auto grace_end = std::chrono::steady_clock::now() +
          std::chrono::milliseconds(POST_PUBLISH_GRACE_MS);
      while (!state->stop.load() &&
             std::chrono::steady_clock::now() < grace_end) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }

    if (trace_cfg.enabled) {
      part->getSEDPAgent().dumpDiscoveryState(0, true);
      rtps::UdpDriver::traceTxTotals();
    }

    done.store(true);
  }
};

struct TopicRecvCtx {
  std::string topic;
  int payload_size;
  int idx;
  int type_index;
  TraceConfig trace_cfg;
  int target_samples;
  std::shared_ptr<SharedState> shared_state;
  std::condition_variable *completion_cv;
  std::chrono::steady_clock::time_point subscriber_start;

  std::mutex mutex;
  uint32_t expected_seq = 0;
  uint32_t received = 0;
  uint32_t initial_loss = 0;
  uint32_t mid_stream_loss = 0;
  std::atomic<bool> topic_done{false};
  std::atomic<bool> first_change_logged{false};
  std::atomic<bool> first_warmup_received_logged{false};
  std::atomic<bool> first_regular_received_logged{false};

  static void callback(void *callee, const rtps::ReaderCacheChange &change) {
    TopicRecvCtx *self = static_cast<TopicRecvCtx *>(callee);
    if (self != nullptr) {
      self->handle(change);
    }
  }

  void handle(const rtps::ReaderCacheChange &change) {
    if (shared_state->stop.load()) {
      return;
    }

    emit_diag_event("subscriber", topic, "received_change", {
        {"sn_high", change.sn.high},
        {"sn_low", change.sn.low},
        {"size", change.size},
    });

    if (mark_first(first_change_logged)) {
      emit_diag_event("subscriber", topic, "first_cache_change_received", {
          {"sn_high", change.sn.high},
          {"sn_low", change.sn.low},
          {"size", change.size},
      });
    }

    if (change.sn.high == 0 && change.sn.low == 0) {
      shared_state->fail_once(EXIT_RECV_FAIL,
                       "regular-path evidence mismatch topic=" + topic +
                           " sn=(0,0)");
      completion_cv->notify_all();
      return;
    }

    if (change.size != static_cast<rtps::DataSize_t>(payload_size)) {
      shared_state->fail_once(EXIT_RECV_FAIL,
                       "payload size mismatch topic=" + topic +
                           " expected=" + std::to_string(payload_size) +
                           " got=" + std::to_string(change.size));
      completion_cv->notify_all();
      return;
    }

    std::vector<uint8_t> buffer(static_cast<size_t>(payload_size), 0);
    if (!change.copyInto(buffer.data(), static_cast<rtps::DataSize_t>(buffer.size()))) {
      shared_state->fail_once(EXIT_RECV_FAIL, "copyInto failed for topic " + topic);
      completion_cv->notify_all();
      return;
    }

    uint32_t seq = 0;
    std::memcpy(&seq, buffer.data(), sizeof(seq));

    if (is_warmup_payload(buffer.data(), buffer.size())) {
      if (mark_first(first_warmup_received_logged)) {
        emit_diag_event("subscriber", topic, "first_warmup_received", {
            {"warmup_id", seq & WARMUP_SEQ_MASK},
        });
      }
      return;
    }
    if (mark_first(first_regular_received_logged)) {
      const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - subscriber_start).count();
      emit_diag_event("subscriber", topic, "first_regular_received", {
          {"seq", seq},
          {"elapsed_ms", static_cast<int64_t>(elapsed / 1000000)},
      });
      if (trace_cfg.enabled) {
        lttng_ust_tracepoint(soatracer_trace_provider, first_receive,
                             topic.c_str(), idx, trace_cfg.test_id,
                             static_cast<uint64_t>(elapsed),
                             static_cast<int>(seq));
      }
    }

    {
      std::lock_guard<std::mutex> lock(mutex);

      if (seq < expected_seq) {
        emit_diag_event("subscriber", topic, "out_of_order", {
            {"seq", seq},
            {"expected", expected_seq},
        });
        return;
      }
      if (seq > expected_seq) {
        uint32_t gap = seq - expected_seq;
        if (received == 0) {
          initial_loss = gap;
          emit_diag_event("subscriber", topic, "initial_loss", {
              {"skipped", gap},
              {"first_seq", seq},
          });
        } else {
          mid_stream_loss += gap;
          emit_diag_event("subscriber", topic, "mid_stream_loss", {
              {"skipped", gap},
              {"expected", expected_seq},
              {"got", seq},
          });
        }
        expected_seq = seq;
      }

      if (!verify_payload(buffer.data(), buffer.size(), seq)) {
        shared_state->fail_once(EXIT_RECV_FAIL,
                         "payload mismatch topic=" + topic +
                             " seq=" + std::to_string(seq));
        completion_cv->notify_all();
        return;
      }

      ++expected_seq;
      ++received;

      if (trace_cfg.enabled) {
        lttng_ust_tracepoint(soatracer_trace_provider, after_receiving,
                             topic.c_str(), idx,
                             static_cast<int>(received), type_index,
                             0, static_cast<int>(seq), trace_cfg.test_id);
      }

      emit_diag_event("subscriber", topic, "received", {
          {"count", received},
          {"seq", seq},
      });

      const uint32_t target = static_cast<uint32_t>(target_samples);
      const uint32_t total_observed = received + initial_loss + mid_stream_loss;
      const bool saw_last_seq = (target > 0) && (seq + 1 >= target);
      if (saw_last_seq || total_observed >= target) {
        if (trace_cfg.enabled) {
          lttng_ust_tracepoint(soatracer_trace_provider, received_all,
                               topic.c_str(), idx,
                               static_cast<int>(received), type_index,
                               static_cast<int>(seq), trace_cfg.test_id);
        }
        emit_diag_event("subscriber", topic, "done", {
            {"received", received},
            {"initial_loss", initial_loss},
            {"mid_stream_loss", mid_stream_loss},
            {"last_seq", seq},
        });
        topic_done.store(true);
      }
    }

    completion_cv->notify_all();
  }
};

struct SubscriberWorker {
  ServiceConfig cfg;
  int samples;
  int64_t start_epoch_ms = -1;
  bool reliable_transport;
  rtps::DiscoveryMode discovery_mode = rtps::DiscoveryMode::Standard;
  TraceConfig trace_cfg;
  std::shared_ptr<SharedState> state;
  std::atomic<bool> done{false};

  std::mutex wait_mutex;
  std::condition_variable cv;
  std::vector<std::unique_ptr<TopicRecvCtx> > topic_ctxs;

  void operator()() {
    const std::string &diag_label = cfg.topics.front();
    const size_t num_topics = cfg.topics.size();

    if (cfg.udp_loss_random_pct >= 0.0f) {
      rtps::Config::UDP_PACKET_LOSS_RATE.store(
          cfg.udp_loss_random_pct / 100.0f, std::memory_order_relaxed);
      emit_diag_event("subscriber", diag_label, "loss_config", {
          {"random_pct", cfg.udp_loss_random_pct},
      });
    }

    if (cfg.gossip_fsm_initial_ms > 0) {
      rtps::Config::SNAP_TIMEOUT_INITIAL_MS.store(
          cfg.gossip_fsm_initial_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_announcing_ms > 0) {
      rtps::Config::SNAP_TIMEOUT_ANNOUNCING_MS.store(
          cfg.gossip_fsm_announcing_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_jitter_ms > 0) {
      rtps::Config::SNAP_JITTER_MAX_MS.store(
          cfg.gossip_fsm_jitter_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_rerequest_ms > 0) {
      rtps::Config::SNAP_TIMEOUT_RETRANSMIT_MS.store(
          cfg.gossip_fsm_rerequest_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_rerequest_bound > 0) {
      rtps::Config::REQUEST_RETRY_BOUND.store(
          cfg.gossip_fsm_rerequest_bound, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_reconcile_base_ms > 0) {
      rtps::Config::SNAP_RECONCILE_BASE_MS.store(
          cfg.gossip_fsm_reconcile_base_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_reconcile_cap_ms > 0) {
      rtps::Config::SNAP_RECONCILE_CAP_MS.store(
          cfg.gossip_fsm_reconcile_cap_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_spdp_resend_ms > 0) {
      rtps::Config::SPDP_RESEND_PERIOD_MS.store(
          cfg.gossip_fsm_spdp_resend_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_spdp_lease_ms > 0) {
      rtps::Config::SPDP_LEASE_DURATION_MS.store(
          cfg.gossip_fsm_spdp_lease_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_spdp_burst_scale > 0) {
      rtps::Config::SPDP_BURST_SCALE_PCT.store(
          cfg.gossip_fsm_spdp_burst_scale, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_spdp_burst_window_ms > 0) {
      rtps::Config::SPDP_BURST_WINDOW_MS.store(
          cfg.gossip_fsm_spdp_burst_window_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_edp_defer_rounds > 0) {
      rtps::Config::SNAP_QUIESCENCE_ROUNDS.store(
          cfg.gossip_fsm_edp_defer_rounds, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_edp_force_after_rounds > 0) {
      rtps::Config::SNAP_QUIESCENCE_MAX_ROUNDS.store(
          cfg.gossip_fsm_edp_force_after_rounds, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_reconcile_grace_ms != 0xFFFFFFFFu) {
      rtps::Config::SNAP_RECONCILE_GRACE_MS.store(
          cfg.gossip_fsm_reconcile_grace_ms, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_request_target_policy != 0xFFFFFFFFu) {
      rtps::Config::SNAP_SERVER_POLICY.store(
          cfg.gossip_fsm_request_target_policy, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_reannounce_after_resync > 0) {
      rtps::Config::SNAP_REANNOUNCE_AFTER_RESYNC.store(
          cfg.gossip_fsm_reannounce_after_resync, std::memory_order_relaxed);
    }
    if (cfg.gossip_fsm_defer_response_cap >= 0) {
      rtps::Config::SNAP_DEFER_CAP.store(
          static_cast<uint32_t>(cfg.gossip_fsm_defer_response_cap), std::memory_order_relaxed);
    }

    emit_diag_event("gossip_fsm", diag_label, "gossip_fsm_config", {
        {"initial_ms", rtps::Config::SNAP_TIMEOUT_INITIAL_MS.load(std::memory_order_relaxed)},
        {"announcing_ms", rtps::Config::SNAP_TIMEOUT_ANNOUNCING_MS.load(std::memory_order_relaxed)},
        {"jitter_ms", rtps::Config::SNAP_JITTER_MAX_MS.load(std::memory_order_relaxed)},
        {"rerequest_ms", rtps::Config::SNAP_TIMEOUT_RETRANSMIT_MS.load(std::memory_order_relaxed)},
        {"rerequest_bound", rtps::Config::REQUEST_RETRY_BOUND.load(std::memory_order_relaxed)},
        {"reconcile_base_ms", rtps::Config::SNAP_RECONCILE_BASE_MS.load(std::memory_order_relaxed)},
        {"reconcile_cap_ms", rtps::Config::SNAP_RECONCILE_CAP_MS.load(std::memory_order_relaxed)},
        {"reconcile_grace_ms", rtps::Config::SNAP_RECONCILE_GRACE_MS.load(std::memory_order_relaxed)},
        {"spdp_resend_ms", rtps::Config::SPDP_RESEND_PERIOD_MS.load(std::memory_order_relaxed)},
        {"spdp_lease_ms", rtps::Config::SPDP_LEASE_DURATION_MS.load(std::memory_order_relaxed)},
        {"spdp_burst_scale", rtps::Config::SPDP_BURST_SCALE_PCT.load(std::memory_order_relaxed)},
        {"spdp_burst_window_ms", rtps::Config::SPDP_BURST_WINDOW_MS.load(std::memory_order_relaxed)},
        {"edp_defer_rounds", rtps::Config::SNAP_QUIESCENCE_ROUNDS.load(std::memory_order_relaxed)},
        {"edp_force_after_rounds", rtps::Config::SNAP_QUIESCENCE_MAX_ROUNDS.load(std::memory_order_relaxed)},
        {"request_target_policy", rtps::Config::SNAP_SERVER_POLICY.load(std::memory_order_relaxed)},
        {"reannounce_after_resync", rtps::Config::SNAP_REANNOUNCE_AFTER_RESYNC.load(std::memory_order_relaxed)},
        {"defer_response_cap", rtps::Config::SNAP_DEFER_CAP.load(std::memory_order_relaxed)},
    });

    if (cfg.participant_delay_ms > 0) {
      wait_for_start_barrier(start_epoch_ms, "subscriber", diag_label);
      emit_diag_event("subscriber", diag_label, "late_join", {
          {"sleeping_ms", cfg.participant_delay_ms},
      });
      std::this_thread::sleep_for(
          std::chrono::milliseconds(cfg.participant_delay_ms));
    }

    rtps::FeatureQOS qos(discovery_mode,
                         rtps::HeartbeatPolicyMode::AdaptiveFrequency);
    rtps::Domain domain(qos);
    rtps::Participant *part = domain.createParticipant();
    if (part == nullptr) {
      state->fail_once(EXIT_RECV_FAIL,
                       "subscriber failed to create participant for " + diag_label);
      done.store(true);
      return;
    }

    const auto subscriber_start = std::chrono::steady_clock::now();
    topic_ctxs.reserve(num_topics);
    for (size_t t = 0; t < num_topics; ++t) {
      std::unique_ptr<TopicRecvCtx> ctx(new TopicRecvCtx());
      ctx->topic = cfg.topics[t];
      ctx->payload_size = cfg.payload_size;
      ctx->idx = cfg.idx;
      ctx->type_index = cfg.type_index;
      ctx->trace_cfg = trace_cfg;
      ctx->target_samples = samples;
      ctx->shared_state = state;
      ctx->completion_cv = &cv;
      ctx->subscriber_start = subscriber_start;

      const bool effective_reliable = cfg.reliable || reliable_transport;
      rtps::Reader *reader =
          domain.createReader(*part, cfg.topics[t].c_str(), "TracerPayload",
                              effective_reliable, {0});
      if (reader == nullptr) {
        state->fail_once(EXIT_RECV_FAIL,
                         "subscriber failed to create reader for " + cfg.topics[t]);
        done.store(true);
        return;
      }
      reader->registerCallback(&TopicRecvCtx::callback, ctx.get());
      emit_diag_event("subscriber", cfg.topics[t], "reader_created", {
          {"idx", t},
      });
      topic_ctxs.push_back(std::move(ctx));
    }

    if (cfg.participant_delay_ms == 0) {
      wait_for_start_barrier(start_epoch_ms, "subscriber", diag_label);
    }

    if (trace_cfg.enabled) {
      lttng_ust_tracepoint(soatracer_trace_provider, participant_join,
                           cfg.idx, trace_cfg.test_id, "subscriber");
    }
    if (!domain.completeInit()) {
      state->fail_once(EXIT_RECV_FAIL,
                       "subscriber failed to complete init for " + diag_label);
      done.store(true);
      return;
    }

    emit_diag_event("subscriber", diag_label, "participant_ready", {
        {"topics", num_topics},
    });

    if (cfg.participant_kill_ms >= 0) {
      auto st = state;
      const auto tc = trace_cfg;
      const int svc_idx = cfg.idx;
      const bool only_root = cfg.kill_if_root != 0;
      arm_kill_at(start_epoch_ms, cfg.participant_kill_ms,
          [st, tc, svc_idx, only_root, part]() {
            if (st->stop.load()) return false;
            const bool is_root =
                part->getSEDPAgent().getCurrentRoot() == part->m_guidPrefix;
            if (only_root && !is_root) return false;
            if (tc.enabled) {
              lttng_ust_tracepoint(soatracer_trace_provider, participant_kill,
                                   svc_idx, tc.test_id, "subscriber",
                                   is_root ? 1 : 0);
            }
            std::cout << "participant_kill: idx=" << svc_idx
                      << " was_root=" << (is_root ? 1 : 0) << std::endl;
            return true;
          });
    }

    std::unique_lock<std::mutex> lock(wait_mutex);
    while (!state->stop.load()) {
      bool all_topics_done = true;
      for (std::vector<std::unique_ptr<TopicRecvCtx> >::const_iterator it =
               topic_ctxs.begin();
           it != topic_ctxs.end(); ++it) {
        if (!(*it)->topic_done.load()) {
          all_topics_done = false;
          break;
        }
      }
      if (all_topics_done && !done.load()) {
        done.store(true);
      }
      cv.wait_for(lock, std::chrono::milliseconds(50));
    }

    bool all_topics_done = true;
    for (std::vector<std::unique_ptr<TopicRecvCtx> >::const_iterator it =
             topic_ctxs.begin();
         it != topic_ctxs.end(); ++it) {
      if (!(*it)->topic_done.load()) {
        all_topics_done = false;
        break;
      }
    }
    if (trace_cfg.enabled) {
      part->getSEDPAgent().dumpDiscoveryState(0, true);
      rtps::UdpDriver::traceTxTotals();
    }
    done.store(all_topics_done);
  }
};

inline int run_mode(int argc, char **argv,
                    const TracerNodeOptions &node_options) {
  RuntimeOptions options;
  std::string error;
  if (!parse_runtime_options(argc, argv, options, error)) {
    std::cerr << "Config/arg error: " << error << std::endl;
    return EXIT_CONFIG;
  }

  AppConfig app_config;
  if (!parse_app_config(options.config_json, app_config, error)) {
    std::cerr << "Config/arg error: " << error << std::endl;
    return EXIT_CONFIG;
  }

  rtps::DiscoveryMode discovery_mode =
      tracer_discovery_mode(node_options.default_discovery_mode);
  if (app_config.feature_qos_from_config) {
    discovery_mode = tracer_discovery_mode(app_config.feature_qos.discovery_mode);
  }

  const int max_hz = app_config.max_frequency_hz();
  const bool has_sporadic = [&]() {
    for (const auto &svc : app_config.services)
      if (svc.sending_pattern == "sporadic") return true;
    return false;
  }();
  const int timing_multiplier = has_sporadic ? 3 : 1;
  int max_participant_delay_ms = 0;
  for (const auto &svc : app_config.services)
    max_participant_delay_ms = std::max(max_participant_delay_ms, svc.participant_delay_ms);
  const int default_timeout_ms =
      std::max(15000, timing_multiplier * ((options.samples * 1000) / std::max(1, max_hz)) + 10000 + TIMEOUT_SLACK_MS +
                           POST_PUBLISH_GRACE_MS + max_participant_delay_ms +
                           static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                WARMUP_DURATION)
                                                .count()));
  const int timeout_ms = options.timeout_ms > 0 ? options.timeout_ms : default_timeout_ms;

  rtps::init();

  std::shared_ptr<SharedState> state(new SharedState());

  TraceConfig trace_cfg;
  trace_cfg.enabled = node_options.emit_tracepoints;
  if (app_config.test_id > 0) {
    trace_cfg.test_id = app_config.test_id;
  } else {
    trace_cfg.test_id = app_config.services.front().idx + 1;
  }

  emit_experiment_begin(trace_cfg);

  std::thread stats_thread;
  if (trace_cfg.enabled) {
    stats_thread = std::thread([state]() {
      uint64_t prev_proc_ticks = 0;
      uint64_t prev_total_ticks = 0;
      uint64_t prev_rss_bytes = 0;
      if (!read_process_stats_raw(prev_proc_ticks, prev_total_ticks, prev_rss_bytes)) {
        return;
      }

      const unsigned int hw_threads = std::thread::hardware_concurrency();
      const double num_cpus = hw_threads > 0 ? static_cast<double>(hw_threads) : 1.0;

      while (!state->stop.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        uint64_t proc_ticks = 0;
        uint64_t total_ticks = 0;
        uint64_t rss_bytes = 0;
        if (!read_process_stats_raw(proc_ticks, total_ticks, rss_bytes)) {
          continue;
        }

        const uint64_t delta_proc = proc_ticks - prev_proc_ticks;
        const uint64_t delta_total = total_ticks - prev_total_ticks;
        double cpu_percent = 0.0;
        if (delta_total > 0) {
          cpu_percent =
              (static_cast<double>(delta_proc) / static_cast<double>(delta_total)) *
              num_cpus * 100.0;
        }
        const double mem_mb = static_cast<double>(rss_bytes) / (1024.0 * 1024.0);

        lttng_ust_tracepoint(soatracer_trace_provider, machine_stats, cpu_percent, mem_mb);

        prev_proc_ticks = proc_ticks;
        prev_total_ticks = total_ticks;
      }
    });
  }

  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);

  std::vector<std::unique_ptr<PublisherWorker> > pubs;
  std::vector<std::unique_ptr<SubscriberWorker> > subs;
  std::vector<std::thread> workers;

  struct PubGroup {
    ServiceConfig merged;
    std::vector<int> timing_offsets;
    bool has = false;
  };
  std::map<int, PubGroup> pub_groups;
  std::map<int, ServiceConfig> sub_groups;
  std::vector<int> pub_group_order;
  std::vector<int> sub_group_order;

  for (std::vector<ServiceConfig>::const_iterator it = app_config.services.begin();
       it != app_config.services.end(); ++it) {
    const ServiceConfig &svc = *it;
    if (svc.type == "pub") {
      PubGroup &g = pub_groups[svc.participant_group];
      if (!g.has) {
        g.merged = svc;
        g.has = true;
        pub_group_order.push_back(svc.participant_group);
      } else {
        for (std::vector<std::string>::const_iterator t = svc.topics.begin();
             t != svc.topics.end(); ++t) {
          g.merged.topics.push_back(*t);
        }
      }
      for (size_t i = 0; i < svc.topics.size(); ++i) {
        g.timing_offsets.push_back(svc.timing_offset_us);
      }
    } else if (svc.type == "sub") {
      std::map<int, ServiceConfig>::iterator sit =
          sub_groups.find(svc.participant_group);
      if (sit == sub_groups.end()) {
        sub_groups[svc.participant_group] = svc;
        sub_group_order.push_back(svc.participant_group);
      } else {
        for (std::vector<std::string>::const_iterator t = svc.topics.begin();
             t != svc.topics.end(); ++t) {
          sit->second.topics.push_back(*t);
        }
      }
    }
  }

  for (std::vector<int>::const_iterator git = pub_group_order.begin();
       git != pub_group_order.end(); ++git) {
    PubGroup &g = pub_groups[*git];
    if (!g.timing_offsets.empty()) {
      int min_offset = *std::min_element(g.timing_offsets.begin(),
                                         g.timing_offsets.end());
      g.merged.timing_offset_us = min_offset;
      for (auto &v : g.timing_offsets) v -= min_offset;
    }
    std::unique_ptr<PublisherWorker> worker(new PublisherWorker());
    worker->cfg = g.merged;
    worker->topic_timing_offsets_us = std::move(g.timing_offsets);
    worker->samples = options.samples;
    worker->start_epoch_ms = options.start_epoch_ms;
    worker->reliable_transport = node_options.reliable_transport;
    worker->discovery_mode = discovery_mode;
    worker->trace_cfg = trace_cfg;
    worker->state = state;
    workers.push_back(std::thread(std::ref(*worker)));
    pubs.push_back(std::move(worker));
  }

  for (std::vector<int>::const_iterator git = sub_group_order.begin();
       git != sub_group_order.end(); ++git) {
    std::unique_ptr<SubscriberWorker> worker(new SubscriberWorker());
    worker->cfg = sub_groups[*git];
    worker->samples = options.samples;
    worker->start_epoch_ms = options.start_epoch_ms;
    worker->reliable_transport = node_options.reliable_transport;
    worker->discovery_mode = discovery_mode;
    worker->trace_cfg = trace_cfg;
    worker->state = state;
    workers.push_back(std::thread(std::ref(*worker)));
    subs.push_back(std::move(worker));
  }

  while (!state->stop.load()) {
    bool all_done = true;

    for (std::vector<std::unique_ptr<PublisherWorker> >::const_iterator it = pubs.begin(); it != pubs.end(); ++it) {
      if (!(*it)->done.load()) {
        all_done = false;
        break;
      }
    }

    if (all_done) {
      for (std::vector<std::unique_ptr<SubscriberWorker> >::const_iterator it = subs.begin(); it != subs.end(); ++it) {
        if (!(*it)->done.load()) {
          all_done = false;
          break;
        }
      }
    }

    if (all_done) {
      break;
    }

    if (std::chrono::steady_clock::now() > deadline) {
      for (std::vector<std::unique_ptr<PublisherWorker> >::const_iterator it = pubs.begin();
           it != pubs.end(); ++it) {
        const PublisherWorker *pub = it->get();
        const size_t topics = pub->cfg.topics.size();
        for (size_t t = 0; t < topics; ++t) {
          uint32_t sent = 0;
          uint32_t last_seq = 0;
          if (t < pub->sent_counts.size()) {
            sent = pub->sent_counts[t];
            last_seq = pub->last_seq_per_topic[t];
          }
          emit_diag_event("publisher", pub->cfg.topics[t], "timeout_progress", {
              {"sent", sent},
              {"expected", pub->samples},
              {"last_seq", last_seq},
          });
        }
      }

      bool worst_zero = false;
      bool worst_below_threshold = false;
      for (std::vector<std::unique_ptr<SubscriberWorker> >::const_iterator it = subs.begin();
           it != subs.end(); ++it) {
        const SubscriberWorker *sub = it->get();
        const size_t topics = sub->topic_ctxs.size();
        for (size_t t = 0; t < topics; ++t) {
          const TopicRecvCtx *ctx = sub->topic_ctxs[t].get();
          const uint32_t received = ctx->received;
          const uint32_t expected = static_cast<uint32_t>(ctx->target_samples);
          const bool done = ctx->topic_done.load();
          const double pct = expected > 0
              ? static_cast<double>(received) / static_cast<double>(expected)
              : 1.0;
          if (received == 0 && expected > 0) {
            worst_zero = true;
          } else if (pct < options.min_recv_pct) {
            worst_below_threshold = true;
          }
          emit_diag_event("subscriber", ctx->topic, "timeout_progress", {
              {"received", received},
              {"expected", expected},
              {"done", done},
              {"recv_pct", pct},
          });
        }
      }

      if (worst_zero) {
        state->fail_once(EXIT_TIMEOUT,
                         "global timeout exceeded with zero receives on at least "
                         "one topic (" + std::to_string(timeout_ms) + " ms)");
      } else if (worst_below_threshold) {
        state->fail_once(EXIT_TIMEOUT,
                         "global timeout exceeded; receive rate below min_recv_pct=" +
                             std::to_string(options.min_recv_pct) +
                             " (" + std::to_string(timeout_ms) + " ms)");
      } else {
        emit_diag_event("publisher", "", "timeout_within_tolerance", {
            {"min_recv_pct", options.min_recv_pct},
        });
        for (std::vector<std::unique_ptr<SubscriberWorker> >::iterator it = subs.begin();
             it != subs.end(); ++it) {
          (*it)->done.store(true);
          for (size_t t = 0; t < (*it)->topic_ctxs.size(); ++t) {
            (*it)->topic_ctxs[t]->topic_done.store(true);
          }
        }
      }

      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  state->stop.store(true);
  for (std::vector<std::unique_ptr<SubscriberWorker> >::const_iterator it = subs.begin(); it != subs.end(); ++it) {
    (*it)->cv.notify_all();
  }

  for (std::vector<std::thread>::iterator it = workers.begin(); it != workers.end(); ++it) {
    if (it->joinable()) {
      it->join();
    }
  }

  if (stats_thread.joinable()) {
    stats_thread.join();
  }

  emit_experiment_end(trace_cfg);

  if (state->failed.load()) {
    return state->fail_code.load();
  }

  for (std::vector<std::unique_ptr<PublisherWorker> >::const_iterator it = pubs.begin(); it != pubs.end(); ++it) {
    if (!(*it)->done.load()) {
      emit_log_line("TEST_FAIL: publisher did not complete\n");
      return EXIT_SEND_FAIL;
    }
  }
  for (std::vector<std::unique_ptr<SubscriberWorker> >::const_iterator it = subs.begin(); it != subs.end(); ++it) {
    if (!(*it)->done.load()) {
      emit_log_line("TEST_FAIL: subscriber did not complete\n");
      return EXIT_RECV_FAIL;
    }
  }

  std::cout << "TEST_PASS" << std::endl;
  return EXIT_OK;
}

}
