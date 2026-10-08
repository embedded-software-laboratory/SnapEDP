#pragma once

#include <nlohmann/json.hpp>

#include <chrono>
#include <csignal>
#include <cstdint>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rtps/config/FeatureQOS.h"

namespace tracer {

using json = nlohmann::json;

struct ServiceConfig {
  std::string name;
  std::string type;
  int idx = 0;
  int type_index = 0;
  std::string topic;
  std::vector<std::string> topics;
  int frequency_hz = 10;
  int payload_size = 16;
  int aggregation_deadline_ms = 100;
  int publish_offset_us = 0;
  int timing_offset_us = 0;
  bool reliable = false;
  std::string sending_pattern = "periodic";

  int participant_delay_ms = 0;
  int participant_group = 0;

  float udp_loss_random_pct = -1.0f;

  uint32_t gossip_fsm_initial_ms = 0;
  uint32_t gossip_fsm_announcing_ms = 0;
  uint32_t gossip_fsm_jitter_ms = 0;
  uint32_t gossip_fsm_rerequest_ms = 0;
  uint32_t gossip_fsm_rerequest_bound = 0;
  uint32_t gossip_fsm_reconcile_base_ms = 0;
  uint32_t gossip_fsm_reconcile_cap_ms = 0;
  uint32_t gossip_fsm_spdp_resend_ms = 0;
  uint32_t gossip_fsm_spdp_lease_ms = 0;
  uint32_t gossip_fsm_spdp_burst_scale = 0;
  uint32_t gossip_fsm_spdp_burst_window_ms = 0;
  uint32_t gossip_fsm_edp_defer_rounds = 0;
  uint32_t gossip_fsm_edp_force_after_rounds = 0;
  uint32_t gossip_fsm_reconcile_grace_ms = 0xFFFFFFFFu;
  uint32_t gossip_fsm_request_target_policy = 0xFFFFFFFFu;
  uint32_t gossip_fsm_reannounce_after_resync = 0;
  int32_t gossip_fsm_defer_response_cap = -1;

  std::string server_ip;
  int server_port = 0;

  int dds_client_sync_ms = -1;
  int dds_initial_announce_count = -1;
  int dds_initial_announce_period_ms = -1;
  int dds_lease_announce_ms = -1;

  int participant_kill_ms = -1;
  int kill_if_root = 0;
  int endpoint_delete_ms = -1;
  int endpoint_delete_idx = 0;
};

struct FeatureQOSConfig {
  std::string discovery_mode = "standard";
};

struct TracerNodeOptions {
  bool emit_tracepoints = true;
  bool reliable_transport = false;
  std::string default_discovery_mode = "standard";

  static TracerNodeOptions be_embedded() {
    return {true, false, "standard"};
  }

  static TracerNodeOptions rl_embedded() {
    return {true, true, "standard"};
  }

  static TracerNodeOptions be_gossip() {
    return {true, false, "gossip"};
  }

  static TracerNodeOptions be_baseline() {
    return {true, false, "standard"};
  }
};

struct AppConfig {
  int test_id = -1;
  std::vector<ServiceConfig> services;
  FeatureQOSConfig feature_qos;
  bool feature_qos_from_config = false;

  int max_frequency_hz() const {
    int max_hz = 1;
    for (std::vector<ServiceConfig>::const_iterator it = services.begin(); it != services.end(); ++it) {
      if (it->frequency_hz > max_hz) {
        max_hz = it->frequency_hz;
      }
    }
    return max_hz;
  }
};

inline bool parse_string_param(const json &node, const char *key, std::string &out) {
  if (!node.is_object() || !node.contains(key)) return false;
  const json &value = node.at(key);
  if (value.is_string()) { out = value.get<std::string>(); return true; }
  return false;
}

inline bool parse_float_param(const json &node, const char *key, float &out) {
  if (!node.is_object() || !node.contains(key)) return false;
  const json &value = node.at(key);
  try {
    if (value.is_number_float()) {
      out = static_cast<float>(value.get<double>());
      return true;
    }
    if (value.is_number_integer()) {
      out = static_cast<float>(value.get<int>());
      return true;
    }
    if (value.is_string()) {
      out = std::stof(value.get<std::string>());
      return true;
    }
  } catch (...) {
    return false;
  }
  return false;
}

inline bool parse_int_param(const json &node, const char *key, int &out) {
  if (!node.is_object() || !node.contains(key)) {
    return false;
  }
  const json &value = node.at(key);
  try {
    if (value.is_number_integer()) {
      out = value.get<int>();
      return true;
    }
    if (value.is_number_float()) {
      out = static_cast<int>(value.get<double>());
      return true;
    }
    if (value.is_string()) {
      out = std::stoi(value.get<std::string>());
      return true;
    }
  } catch (...) {
    return false;
  }
  return false;
}

inline std::vector<std::string> expand_topic_pattern(const std::string &pattern) {
  std::vector<std::string> result;

  const std::string::size_type open = pattern.find('{');
  if (open == std::string::npos) {
    result.push_back(pattern);
    return result;
  }
  const std::string::size_type close = pattern.find('}', open + 1);
  if (close == std::string::npos || close <= open + 1) {
    result.push_back(pattern);
    return result;
  }

  const std::string prefix = pattern.substr(0, open);
  const std::string suffix = pattern.substr(close + 1);
  const std::string inner = pattern.substr(open + 1, close - open - 1);

  int range_start = 0;
  int range_end = 0;
  const std::string::size_type dash = inner.find('-');
  try {
    if (dash != std::string::npos) {
      range_start = std::stoi(inner.substr(0, dash));
      range_end = std::stoi(inner.substr(dash + 1));
    } else {
      range_start = 0;
      range_end = std::stoi(inner) - 1;
    }
  } catch (...) {
    result.push_back(pattern);
    return result;
  }

  for (int i = range_start; i <= range_end; ++i) {
    result.push_back(prefix + std::to_string(i) + suffix);
  }
  return result;
}

inline bool parse_app_config(const std::string &json_config, AppConfig &out,
                             std::string &error) {
  json root;
  try {
    root = json::parse(json_config);
  } catch (const std::exception &e) {
    error = std::string("Config parse error: ") + e.what();
    return false;
  }

  if (!root.contains("services") || !root["services"].is_array()) {
    error = "Config must contain array field 'services'";
    return false;
  }

  AppConfig cfg;
  if (root.contains("id")) {
    try {
      if (root["id"].is_number_integer()) {
        cfg.test_id = root["id"].get<int>();
      } else if (root["id"].is_string()) {
        cfg.test_id = std::stoi(root["id"].get<std::string>());
      }
    } catch (...) {
    }
  }

  if (root.contains("feature_qos") && root["feature_qos"].is_object()) {
    cfg.feature_qos_from_config = true;
    const json &fq = root["feature_qos"];
    cfg.feature_qos.discovery_mode = fq.value("discovery_mode", std::string{"standard"});
  }

  for (json::const_iterator svc_it = root["services"].begin(); svc_it != root["services"].end(); ++svc_it) {
    const json &svc_json = *svc_it;
    if (!svc_json.is_object()) {
      error = "Each service entry must be an object";
      return false;
    }

    ServiceConfig svc;
    svc.name = svc_json.value("name", std::string());
    svc.type = svc_json.value("type", std::string());
    svc.idx = svc_json.value("idx", 0);
    svc.type_index = svc_json.value("type_index", 0);
    svc.participant_group = svc_json.value("participant_group", 0);
    svc.topic = svc_json.value("topic", std::string());

    if (svc_json.contains("topics") && svc_json["topics"].is_array()) {
      for (json::const_iterator t_it = svc_json["topics"].begin();
           t_it != svc_json["topics"].end(); ++t_it) {
        if (t_it->is_string()) {
          std::vector<std::string> expanded =
              expand_topic_pattern(t_it->get<std::string>());
          for (std::vector<std::string>::const_iterator e = expanded.begin();
               e != expanded.end(); ++e) {
            svc.topics.push_back(*e);
          }
        }
      }
    }

    if (svc.topics.empty() && !svc.topic.empty()) {
      svc.topics.push_back(svc.topic);
    }

    if (svc.type != "pub" && svc.type != "sub" && svc.type != "server") {
      error = "Service type must be 'pub', 'sub' or 'server'";
      return false;
    }
    if (svc.type != "server" && svc.topics.empty()) {
      error = "Service must specify 'topic' or non-empty 'topics'";
      return false;
    }

    if (svc.topic.empty() && !svc.topics.empty()) {
      svc.topic = svc.topics.front();
    }

    if (svc_json.contains("parameters") && svc_json["parameters"].is_object()) {
      const json &params = svc_json["parameters"];

      int frequency = svc.frequency_hz;
      if (parse_int_param(params, "frequency", frequency) && frequency > 0) {
        svc.frequency_hz = frequency;
      }

      int size = svc.payload_size;
      bool size_found = parse_int_param(params, "size", size);
      if (!size_found && params.contains("data") && params["data"].is_object()) {
        size_found = parse_int_param(params["data"], "size", size);
      }
      if (size_found) {
        svc.payload_size = size;
      }

      int deadline = svc.aggregation_deadline_ms;
      bool deadline_found = parse_int_param(params, "aggregation_deadline_ms", deadline);
      if (!deadline_found) {
        deadline_found = parse_int_param(params, "deadline", deadline);
      }
      if (!deadline_found && params.contains("timing") && params["timing"].is_object()) {
        deadline_found = parse_int_param(params["timing"], "deadline", deadline);
      }
      if (!deadline_found && params.contains("transmission_timing") && params["transmission_timing"].is_object()) {
        deadline_found = parse_int_param(params["transmission_timing"], "deadline", deadline);
      }
      if (!deadline_found && params.contains("topic_topology") && params["topic_topology"].is_object()) {
        deadline_found = parse_int_param(params["topic_topology"], "deadline", deadline);
      }
      if (deadline_found && deadline > 0) {
        svc.aggregation_deadline_ms = deadline;
      }

      int offset = svc.publish_offset_us;
      if (parse_int_param(params, "publish_offset_us", offset) && offset >= 0) {
        svc.publish_offset_us = offset;
      }

      int timing_offset = svc.timing_offset_us;
      if (parse_int_param(params, "timing_offset_us", timing_offset) && timing_offset >= 0) {
        svc.timing_offset_us = timing_offset;
      }

      parse_string_param(params, "sending_pattern", svc.sending_pattern);

      if (params.contains("reliable") && params["reliable"].is_boolean()) {
        svc.reliable = params["reliable"].get<bool>();
      } else if (params.contains("reliability") && params["reliability"].is_string()) {
        const std::string r = params["reliability"].get<std::string>();
        svc.reliable = (r == "reliable" || r == "RELIABLE");
      }

      int delay = svc.participant_delay_ms;
      if (parse_int_param(params, "participant_delay_ms", delay) && delay >= 0) {
        svc.participant_delay_ms = delay;
      }

      parse_string_param(params, "server_ip", svc.server_ip);
      int server_port = svc.server_port;
      if (parse_int_param(params, "server_port", server_port) && server_port > 0) {
        svc.server_port = server_port;
      }

      float loss_random = 0.0f;
      if (parse_float_param(params, "udp_loss_random_pct", loss_random) && loss_random >= 0.0f) {
        svc.udp_loss_random_pct = loss_random;
      }

      int dds_client_sync = -1;
      if (parse_int_param(params, "dds_discovery_client_sync_ms", dds_client_sync) && dds_client_sync >= 0) {
        svc.dds_client_sync_ms = dds_client_sync;
      }
      int dds_announce_count = -1;
      if (parse_int_param(params, "dds_discovery_initial_announce_count", dds_announce_count) && dds_announce_count >= 0) {
        svc.dds_initial_announce_count = dds_announce_count;
      }
      int dds_announce_period = -1;
      if (parse_int_param(params, "dds_discovery_initial_announce_period_ms", dds_announce_period) && dds_announce_period >= 0) {
        svc.dds_initial_announce_period_ms = dds_announce_period;
      }
      int dds_lease_announce = -1;
      if (parse_int_param(params, "dds_discovery_lease_announce_ms", dds_lease_announce) && dds_lease_announce >= 0) {
        svc.dds_lease_announce_ms = dds_lease_announce;
      }

      int kill_ms = -1;
      if (parse_int_param(params, "participant_kill_ms", kill_ms) && kill_ms >= 0) {
        svc.participant_kill_ms = kill_ms;
      }
      int kill_root = 0;
      if (parse_int_param(params, "kill_if_root", kill_root) && kill_root > 0) {
        svc.kill_if_root = 1;
      }
      int ep_delete_ms = -1;
      if (parse_int_param(params, "endpoint_delete_ms", ep_delete_ms) && ep_delete_ms >= 0) {
        svc.endpoint_delete_ms = ep_delete_ms;
      }
      int ep_delete_idx = 0;
      if (parse_int_param(params, "endpoint_delete_idx", ep_delete_idx) && ep_delete_idx >= 0) {
        svc.endpoint_delete_idx = ep_delete_idx;
      }

      int fsm_initial = 0;
      if (parse_int_param(params, "gossip_fsm_initial_ms", fsm_initial) && fsm_initial > 0) {
        svc.gossip_fsm_initial_ms = static_cast<uint32_t>(fsm_initial);
      }
      int fsm_announcing = 0;
      if (parse_int_param(params, "gossip_fsm_announcing_ms", fsm_announcing) && fsm_announcing > 0) {
        svc.gossip_fsm_announcing_ms = static_cast<uint32_t>(fsm_announcing);
      }
      int fsm_jitter = 0;
      if (parse_int_param(params, "gossip_fsm_jitter_ms", fsm_jitter) && fsm_jitter > 0) {
        svc.gossip_fsm_jitter_ms = static_cast<uint32_t>(fsm_jitter);
      }
      int fsm_rerequest = 0;
      if (parse_int_param(params, "gossip_fsm_rerequest_ms", fsm_rerequest) && fsm_rerequest > 0) {
        svc.gossip_fsm_rerequest_ms = static_cast<uint32_t>(fsm_rerequest);
      }
      int fsm_rerequest_bound = 0;
      if (parse_int_param(params, "gossip_fsm_rerequest_bound", fsm_rerequest_bound) && fsm_rerequest_bound > 0) {
        svc.gossip_fsm_rerequest_bound = static_cast<uint32_t>(fsm_rerequest_bound);
      }
      int fsm_reconcile_base = 0;
      if (parse_int_param(params, "gossip_fsm_reconcile_base_ms", fsm_reconcile_base) && fsm_reconcile_base > 0) {
        svc.gossip_fsm_reconcile_base_ms = static_cast<uint32_t>(fsm_reconcile_base);
      }
      int fsm_reconcile_cap = 0;
      if (parse_int_param(params, "gossip_fsm_reconcile_cap_ms", fsm_reconcile_cap) && fsm_reconcile_cap > 0) {
        svc.gossip_fsm_reconcile_cap_ms = static_cast<uint32_t>(fsm_reconcile_cap);
      }
      int fsm_spdp_resend = 0;
      if (parse_int_param(params, "gossip_fsm_spdp_resend_ms", fsm_spdp_resend) && fsm_spdp_resend > 0) {
        svc.gossip_fsm_spdp_resend_ms = static_cast<uint32_t>(fsm_spdp_resend);
      }
      int fsm_spdp_lease = 0;
      if (parse_int_param(params, "gossip_fsm_spdp_lease_ms", fsm_spdp_lease) && fsm_spdp_lease > 0) {
        svc.gossip_fsm_spdp_lease_ms = static_cast<uint32_t>(fsm_spdp_lease);
      }
      int fsm_spdp_burst_scale = 0;
      if (parse_int_param(params, "gossip_fsm_spdp_burst_scale", fsm_spdp_burst_scale) && fsm_spdp_burst_scale > 0) {
        svc.gossip_fsm_spdp_burst_scale = static_cast<uint32_t>(fsm_spdp_burst_scale);
      }
      int fsm_spdp_burst_window = 0;
      if (parse_int_param(params, "gossip_fsm_spdp_burst_window_ms", fsm_spdp_burst_window) && fsm_spdp_burst_window > 0) {
        svc.gossip_fsm_spdp_burst_window_ms = static_cast<uint32_t>(fsm_spdp_burst_window);
      }
      int fsm_edp_defer_rounds = 0;
      if (parse_int_param(params, "gossip_fsm_edp_defer_rounds", fsm_edp_defer_rounds) && fsm_edp_defer_rounds > 0) {
        svc.gossip_fsm_edp_defer_rounds = static_cast<uint32_t>(fsm_edp_defer_rounds);
      }
      int fsm_edp_force_after = 0;
      if (parse_int_param(params, "gossip_fsm_edp_force_after_rounds", fsm_edp_force_after) && fsm_edp_force_after > 0) {
        svc.gossip_fsm_edp_force_after_rounds = static_cast<uint32_t>(fsm_edp_force_after);
      }
      int fsm_reconcile_grace = 0;
      if (parse_int_param(params, "gossip_fsm_reconcile_grace_ms", fsm_reconcile_grace) && fsm_reconcile_grace >= 0) {
        svc.gossip_fsm_reconcile_grace_ms = static_cast<uint32_t>(fsm_reconcile_grace);
      }
      int fsm_request_target = 0;
      if (parse_int_param(params, "gossip_fsm_request_target_policy", fsm_request_target) && fsm_request_target >= 0) {
        svc.gossip_fsm_request_target_policy = static_cast<uint32_t>(fsm_request_target);
      }
      int fsm_reannounce = 0;
      if (parse_int_param(params, "gossip_fsm_reannounce_after_resync", fsm_reannounce) && fsm_reannounce > 0) {
        svc.gossip_fsm_reannounce_after_resync = static_cast<uint32_t>(fsm_reannounce);
      }
      int fsm_defer_response = -1;
      if (parse_int_param(params, "gossip_fsm_defer_response_cap", fsm_defer_response) && fsm_defer_response >= 0) {
        svc.gossip_fsm_defer_response_cap = fsm_defer_response;
      }
    }

    if (svc.payload_size < 4) {
      error = "Payload size must be >= 4 bytes";
      return false;
    }

    cfg.services.push_back(svc);
  }

  if (cfg.services.empty()) {
    error = "Config contains no services";
    return false;
  }

  out = cfg;
  return true;
}

inline void arm_kill_at(int64_t start_epoch_ms, int kill_ms,
                        std::function<bool()> fire) {
  if (kill_ms < 0) return;
  if (start_epoch_ms <= 0) {
    std::cerr << "participant_kill_ms set but no start barrier, kill disarmed"
              << std::endl;
    return;
  }
  std::thread([start_epoch_ms, kill_ms, fire = std::move(fire)]() {
    const std::chrono::system_clock::time_point due{
        std::chrono::milliseconds(start_epoch_ms + kill_ms)};
    if (std::chrono::system_clock::now() >= due) return;
    std::this_thread::sleep_until(due);
    if (!fire()) return;
    std::cout.flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::raise(SIGKILL);
  }).detach();
}

template <typename DurationT>
inline void set_duration_ms(DurationT &d, int ms) {
  d.seconds = ms / 1000;
  d.nanosec = static_cast<uint32_t>((ms % 1000) * 1000000);
}

template <typename DiscoverySettingsT>
inline void apply_dds_discovery_tuning(DiscoverySettingsT &dc, const ServiceConfig &svc) {
  if (svc.dds_client_sync_ms >= 0) {
    set_duration_ms(dc.discoveryServer_client_syncperiod, svc.dds_client_sync_ms);
  }
  if (svc.dds_initial_announce_count >= 0) {
    dc.initial_announcements.count = static_cast<uint32_t>(svc.dds_initial_announce_count);
  }
  if (svc.dds_initial_announce_period_ms >= 0) {
    set_duration_ms(dc.initial_announcements.period, svc.dds_initial_announce_period_ms);
  }
  if (svc.dds_lease_announce_ms >= 0) {
    set_duration_ms(dc.leaseDuration_announcementperiod, svc.dds_lease_announce_ms);
  }
  if (svc.dds_client_sync_ms >= 0 || svc.dds_initial_announce_count >= 0 ||
      svc.dds_initial_announce_period_ms >= 0 || svc.dds_lease_announce_ms >= 0) {
    std::cout << "dds_discovery: client_sync_ms=" << svc.dds_client_sync_ms
              << " initial_announce_count=" << svc.dds_initial_announce_count
              << " initial_announce_period_ms=" << svc.dds_initial_announce_period_ms
              << " lease_announce_ms=" << svc.dds_lease_announce_ms << std::endl;
  }
}

}
