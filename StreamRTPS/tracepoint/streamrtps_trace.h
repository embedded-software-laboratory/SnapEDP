#undef TRACEPOINT_PROVIDER
#define TRACEPOINT_PROVIDER streamrtps_trace

#undef TRACEPOINT_INCLUDE
#define TRACEPOINT_INCLUDE <streamrtps_trace.h>

#if !defined(STREAMRTPS_TRACE_H) || defined(TRACEPOINT_HEADER_MULTI_READ)
#define STREAMRTPS_TRACE_H

#include <lttng/tracepoint.h>

TRACEPOINT_EVENT(
    streamrtps_trace,
    udp_create_connection,
    TP_ARGS(
        uint16_t, port,
        int, success
    ),
    TP_FIELDS(
        ctf_integer(uint16_t, port, port)
        ctf_integer(int, success, success)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    udp_send_packet,
    TP_ARGS(
        uint64_t, eventId,
        uint16_t, src_port,
        uint16_t, dest_port,
        uint32_t, dest_addr,
        uint32_t, size,
        int, success
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint16_t, src_port, src_port)
        ctf_integer(uint16_t, dest_port, dest_port)
        ctf_integer(uint32_t, dest_addr, dest_addr)
        ctf_integer(uint32_t, size, size)
        ctf_integer(int, success, success)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    udp_tx_total,
    TP_ARGS(
        uint32_t, pid,
        uint32_t, own_addr,
        uint64_t, bytes,
        uint64_t, packets,
        uint64_t, bytes_offhost,
        uint64_t, packets_offhost
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, pid, pid)
        ctf_integer(uint32_t, own_addr, own_addr)
        ctf_integer(uint64_t, bytes, bytes)
        ctf_integer(uint64_t, packets, packets)
        ctf_integer(uint64_t, bytes_offhost, bytes_offhost)
        ctf_integer(uint64_t, packets_offhost, packets_offhost)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    udp_join_multicast,
    TP_ARGS(
        uint32_t, multicast_addr,
        int, success
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, multicast_addr, multicast_addr)
        ctf_integer(int, success, success)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    ip_send_packet,
    TP_ARGS(
        uint16_t, src_port,
        uint16_t, dest_port,
        uint32_t, dest_addr,
        uint32_t, size,
        int, success,
        int, errno_val
    ),
    TP_FIELDS(
        ctf_integer(uint16_t, src_port, src_port)
        ctf_integer(uint16_t, dest_port, dest_port)
        ctf_integer(uint32_t, dest_addr, dest_addr)
        ctf_integer(uint32_t, size, size)
        ctf_integer(int, success, success)
        ctf_integer(int, errno_val, errno_val)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    spdp_broadcast,
    TP_ARGS(
        uint64_t, local_high,
        uint64_t, local_low,
        uint32_t, data_size,
        uint64_t, endpoint_hash
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint32_t, data_size, data_size)
        ctf_integer(uint64_t, endpoint_hash, endpoint_hash)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_hash_check,
    TP_ARGS(
        uint64_t, local_high,
        uint64_t, local_low,
        uint64_t, peer_high,
        uint64_t, peer_low,
        uint64_t, self_hash,
        uint64_t, peer_hash,
        uint64_t, peer_view_hash,
        int, mismatch,
        int, triggered_resync
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint64_t, peer_high, peer_high)
        ctf_integer(uint64_t, peer_low, peer_low)
        ctf_integer(uint64_t, self_hash, self_hash)
        ctf_integer(uint64_t, peer_hash, peer_hash)
        ctf_integer(uint64_t, peer_view_hash, peer_view_hash)
        ctf_integer(int, mismatch, mismatch)
        ctf_integer(int, triggered_resync, triggered_resync)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_gate_evaluated,
    TP_ARGS(
        uint64_t, local_high,
        uint64_t, local_low,
        uint32_t, num_deficient,
        uint32_t, num_pending_resync,
        int, outcome,
        uint64_t, target_high,
        uint64_t, target_low
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint32_t, num_deficient, num_deficient)
        ctf_integer(uint32_t, num_pending_resync, num_pending_resync)
        ctf_integer(int, outcome, outcome)
        ctf_integer(uint64_t, target_high, target_high)
        ctf_integer(uint64_t, target_low, target_low)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    spdp_receive_callback,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        uint32_t, change_size,
        int, change_kind
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint32_t, change_size, change_size)
        ctf_integer(int, change_kind, change_kind)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    spdp_process_proxy,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        uint64_t, guid_prefix_high,
        uint64_t, guid_prefix_low,
        int, is_new_participant
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint64_t, guid_prefix_high, guid_prefix_high)
        ctf_integer(uint64_t, guid_prefix_low, guid_prefix_low)
        ctf_integer(int, is_new_participant, is_new_participant)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    sedp_new_publisher,
    TP_ARGS(
        uint64_t, eventId,
        const char *, topic_name,
        uint32_t, entity_id
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_string(topic_name, topic_name)
        ctf_integer(uint32_t, entity_id, entity_id)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    sedp_new_subscriber,
    TP_ARGS(
        uint64_t, eventId,
        const char *, topic_name,
        uint32_t, entity_id
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_string(topic_name, topic_name)
        ctf_integer(uint32_t, entity_id, entity_id)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    sedp_add_writer,
    TP_ARGS(
        uint64_t, eventId,
        const char *, topic_name,
        uint32_t, entity_id
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_string(topic_name, topic_name)
        ctf_integer(uint32_t, entity_id, entity_id)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    sedp_add_reader,
    TP_ARGS(
        uint64_t, eventId,
        const char *, topic_name,
        uint32_t, entity_id
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_string(topic_name, topic_name)
        ctf_integer(uint32_t, entity_id, entity_id)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_state_transition,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        int, from_state,
        int, event,
        int, to_state
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(int, from_state, from_state)
        ctf_integer(int, event, event)
        ctf_integer(int, to_state, to_state)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_root_changed,
    TP_ARGS(
        uint64_t, local_high,
        uint64_t, local_low,
        uint64_t, root_high,
        uint64_t, root_low
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint64_t, root_high, root_high)
        ctf_integer(uint64_t, root_low, root_low)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_election_evaluated,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        int, won,
        uint32_t, num_unconfigured_peers,
        uint32_t, num_configured_peers
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(int, won, won)
        ctf_integer(uint32_t, num_unconfigured_peers, num_unconfigured_peers)
        ctf_integer(uint32_t, num_configured_peers, num_configured_peers)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_request_sent,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        uint64_t, target_high,
        uint64_t, target_low
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint64_t, target_high, target_high)
        ctf_integer(uint64_t, target_low, target_low)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_request_received,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        uint64_t, sender_high,
        uint64_t, sender_low
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint64_t, sender_high, sender_high)
        ctf_integer(uint64_t, sender_low, sender_low)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_response_sent,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        uint64_t, target_high,
        uint64_t, target_low,
        uint32_t, num_endpoints
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint64_t, target_high, target_high)
        ctf_integer(uint64_t, target_low, target_low)
        ctf_integer(uint32_t, num_endpoints, num_endpoints)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_response_received,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        uint64_t, sender_high,
        uint64_t, sender_low,
        uint32_t, num_endpoints
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint64_t, sender_high, sender_high)
        ctf_integer(uint64_t, sender_low, sender_low)
        ctf_integer(uint32_t, num_endpoints, num_endpoints)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_endpoint_received,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        const char *, topic_name,
        uint32_t, entity_id,
        int, matched
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_string(topic_name, topic_name)
        ctf_integer(uint32_t, entity_id, entity_id)
        ctf_integer(int, matched, matched)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_announcement_sent,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        const char *, topic_name,
        uint32_t, entity_id
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_string(topic_name, topic_name)
        ctf_integer(uint32_t, entity_id, entity_id)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_announcement_received,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        uint32_t, num_endpoints,
        int, disposed,
        uint64_t, sender_high,
        uint64_t, sender_low,
        uint64_t, endpoint_hash
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint32_t, num_endpoints, num_endpoints)
        ctf_integer(int, disposed, disposed)
        ctf_integer(uint64_t, sender_high, sender_high)
        ctf_integer(uint64_t, sender_low, sender_low)
        ctf_integer(uint64_t, endpoint_hash, endpoint_hash)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_pending_queued,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        int, kind,
        const char *, topic_name
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(int, kind, kind)
        ctf_string(topic_name, topic_name)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_pending_flushed,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        uint32_t, num_writers,
        uint32_t, num_readers
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint32_t, num_writers, num_writers)
        ctf_integer(uint32_t, num_readers, num_readers)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_response_deferred,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        uint64_t, requester_high,
        uint64_t, requester_low
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint64_t, requester_high, requester_high)
        ctf_integer(uint64_t, requester_low, requester_low)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_response_retried,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        uint64_t, requester_high,
        uint64_t, requester_low
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint64_t, requester_high, requester_high)
        ctf_integer(uint64_t, requester_low, requester_low)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_locator_resolve_failed,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        uint64_t, target_high,
        uint64_t, target_low
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint64_t, target_high, target_high)
        ctf_integer(uint64_t, target_low, target_low)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    gossip_message_dropped,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        int, reason
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(int, reason, reason)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    spdp_state_changed,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, local_high,
        uint64_t, local_low,
        int, old_state,
        int, new_state
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(int, old_state, old_state)
        ctf_integer(int, new_state, new_state)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    writer_new_change,
    TP_ARGS(
        uint64_t, eventId,
        const char *, writer_type,
        uint32_t, entity_id,
        int, change_kind,
        uint32_t, data_size,
        uint32_t, seq_num_high,
        uint32_t, seq_num_low
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_string(writer_type, writer_type)
        ctf_integer(uint32_t, entity_id, entity_id)
        ctf_integer(int, change_kind, change_kind)
        ctf_integer(uint32_t, data_size, data_size)
        ctf_integer(uint32_t, seq_num_high, seq_num_high)
        ctf_integer(uint32_t, seq_num_low, seq_num_low)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    writer_progress_start,
    TP_ARGS(
        const char *, writer_type,
        uint32_t, entity_id,
        uint32_t, num_proxies
    ),
    TP_FIELDS(
        ctf_string(writer_type, writer_type)
        ctf_integer(uint32_t, entity_id, entity_id)
        ctf_integer(uint32_t, num_proxies, num_proxies)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    writer_progress_end,
    TP_ARGS(
        const char *, writer_type,
        uint32_t, entity_id
    ),
    TP_FIELDS(
        ctf_string(writer_type, writer_type)
        ctf_integer(uint32_t, entity_id, entity_id)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    writer_send_to_proxy,
    TP_ARGS(
        uint64_t, eventId,
        uint32_t, writer_entity_id,
        uint32_t, reader_entity_id,
        uint64_t, reader_guid_prefix_high,
        uint64_t, reader_guid_prefix_low,
        uint32_t, seq_num_high,
        uint32_t, seq_num_low,
        int, use_multicast,
        int, is_retransmission
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint32_t, writer_entity_id, writer_entity_id)
        ctf_integer(uint32_t, reader_entity_id, reader_entity_id)
        ctf_integer(uint64_t, reader_guid_prefix_high, reader_guid_prefix_high)
        ctf_integer(uint64_t, reader_guid_prefix_low, reader_guid_prefix_low)
        ctf_integer(uint32_t, seq_num_high, seq_num_high)
        ctf_integer(uint32_t, seq_num_low, seq_num_low)
        ctf_integer(int, use_multicast, use_multicast)
        ctf_integer(int, is_retransmission, is_retransmission)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    writer_data_locator,
    TP_ARGS(
        uint64_t, local_high,
        uint64_t, local_low,
        uint32_t, writer_entity_id,
        uint64_t, reader_high,
        uint64_t, reader_low,
        uint32_t, dest_addr,
        uint16_t, dest_port,
        int, use_multicast
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint32_t, writer_entity_id, writer_entity_id)
        ctf_integer(uint64_t, reader_high, reader_high)
        ctf_integer(uint64_t, reader_low, reader_low)
        ctf_integer(uint32_t, dest_addr, dest_addr)
        ctf_integer(uint16_t, dest_port, dest_port)
        ctf_integer(int, use_multicast, use_multicast)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    reader_proxy_locator_stored,
    TP_ARGS(
        uint64_t, local_high,
        uint64_t, local_low,
        uint32_t, writer_entity_id,
        uint64_t, reader_high,
        uint64_t, reader_low,
        uint32_t, unicast_addr,
        uint16_t, unicast_port,
        int, refreshed
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint32_t, writer_entity_id, writer_entity_id)
        ctf_integer(uint64_t, reader_high, reader_high)
        ctf_integer(uint64_t, reader_low, reader_low)
        ctf_integer(uint32_t, unicast_addr, unicast_addr)
        ctf_integer(uint16_t, unicast_port, unicast_port)
        ctf_integer(int, refreshed, refreshed)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    threadpool_recv_packet,
    TP_ARGS(
        uint64_t, eventId,
        uint16_t, dest_port,
        uint16_t, src_port,
        uint32_t, packet_size
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint16_t, dest_port, dest_port)
        ctf_integer(uint16_t, src_port, src_port)
        ctf_integer(uint32_t, packet_size, packet_size)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    threadpool_queue_drop,
    TP_ARGS(
        int, queue_kind,
        uint64_t, event_id
    ),
    TP_FIELDS(
        ctf_integer(int, queue_kind, queue_kind)
        ctf_integer(uint64_t, event_id, event_id)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    domain_receive_callback,
    TP_ARGS(
        uint64_t, eventId,
        uint16_t, dest_port,
        uint32_t, payload_len,
        int, is_meta_multicast,
        int, is_user_multicast
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint16_t, dest_port, dest_port)
        ctf_integer(uint32_t, payload_len, payload_len)
        ctf_integer(int, is_meta_multicast, is_meta_multicast)
        ctf_integer(int, is_user_multicast, is_user_multicast)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    msg_receiver_process_start,
    TP_ARGS(
        uint64_t, eventId,
        uint32_t, message_size
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint32_t, message_size, message_size)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    msg_receiver_process_header,
    TP_ARGS(
        uint64_t, eventId,
        uint64_t, source_guid_prefix_high,
        uint64_t, source_guid_prefix_low,
        int, is_own_message,
        int, success
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint64_t, source_guid_prefix_high, source_guid_prefix_high)
        ctf_integer(uint64_t, source_guid_prefix_low, source_guid_prefix_low)
        ctf_integer(int, is_own_message, is_own_message)
        ctf_integer(int, success, success)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    msg_receiver_process_submsg,
    TP_ARGS(
        uint64_t, eventId,
        int, submsg_kind,
        uint32_t, submsg_size,
        int, success
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(int, submsg_kind, submsg_kind)
        ctf_integer(uint32_t, submsg_size, submsg_size)
        ctf_integer(int, success, success)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    msg_receiver_process_data,
    TP_ARGS(
        uint64_t, eventId,
        uint32_t, writer_entity_id,
        uint32_t, reader_entity_id,
        uint32_t, seq_num_high,
        uint32_t, seq_num_low,
        uint32_t, data_size,
        int, reader_found
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint32_t, writer_entity_id, writer_entity_id)
        ctf_integer(uint32_t, reader_entity_id, reader_entity_id)
        ctf_integer(uint32_t, seq_num_high, seq_num_high)
        ctf_integer(uint32_t, seq_num_low, seq_num_low)
        ctf_integer(uint32_t, data_size, data_size)
        ctf_integer(int, reader_found, reader_found)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    reader_new_change,
    TP_ARGS(
        uint64_t, eventId,
        const char *, reader_type,
        uint32_t, entity_id,
        uint32_t, writer_entity_id,
        uint32_t, seq_num_high,
        uint32_t, seq_num_low,
        uint32_t, data_size
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_string(reader_type, reader_type)
        ctf_integer(uint32_t, entity_id, entity_id)
        ctf_integer(uint32_t, writer_entity_id, writer_entity_id)
        ctf_integer(uint32_t, seq_num_high, seq_num_high)
        ctf_integer(uint32_t, seq_num_low, seq_num_low)
        ctf_integer(uint32_t, data_size, data_size)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    reader_callback_invoked,
    TP_ARGS(
        uint64_t, eventId,
        uint32_t, entity_id,
        int, has_callback
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(uint32_t, entity_id, entity_id)
        ctf_integer(int, has_callback, has_callback)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    stne_handshake_state,
    TP_ARGS(
        uint64_t, eventId,
        int, phase,
        uint32_t, writer_entity_id,
        uint32_t, reader_entity_id,
        uint32_t, streamId
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId, eventId)
        ctf_integer(int, phase, phase)
        ctf_integer(uint32_t, writer_entity_id, writer_entity_id)
        ctf_integer(uint32_t, reader_entity_id, reader_entity_id)
        ctf_integer(uint32_t, streamId, streamId)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    writer_stream_send,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, stream_id,
        int,      header_format,
        uint32_t, seq_num_low,
        uint32_t, data_size,
        int,      via_aggregator,
        uint32_t, aggregator_id
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,      entity_id)
        ctf_integer(uint32_t, stream_id,      stream_id)
        ctf_integer(int,      header_format,  header_format)
        ctf_integer(uint32_t, seq_num_low,    seq_num_low)
        ctf_integer(uint32_t, data_size,      data_size)
        ctf_integer(int,      via_aggregator, via_aggregator)
        ctf_integer(uint32_t, aggregator_id,  aggregator_id)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    aggregator_new_sample,
    TP_ARGS(
        uint32_t, aggregator_id,
        int,      aggregator_type,
        uint32_t, deadline_ms,
        uint32_t, sample_size,
        uint32_t, buffer_size
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, aggregator_id,   aggregator_id)
        ctf_integer(int,      aggregator_type, aggregator_type)
        ctf_integer(uint32_t, deadline_ms,     deadline_ms)
        ctf_integer(uint32_t, sample_size,     sample_size)
        ctf_integer(uint32_t, buffer_size,     buffer_size)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    aggregator_flush,
    TP_ARGS(
        uint32_t, aggregator_id,
        int,      aggregator_type,
        uint32_t, packet_size,
        uint32_t, message_count,
        uint32_t, dest_addr,
        uint16_t, dest_port
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, aggregator_id,   aggregator_id)
        ctf_integer(int,      aggregator_type, aggregator_type)
        ctf_integer(uint32_t, packet_size,     packet_size)
        ctf_integer(uint32_t, message_count,   message_count)
        ctf_integer(uint32_t, dest_addr,       dest_addr)
        ctf_integer(uint16_t, dest_port,       dest_port)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    aggregator_skip_flush,
    TP_ARGS(
        uint32_t, aggregator_id,
        int,      aggregator_type,
        int,      reason
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, aggregator_id,   aggregator_id)
        ctf_integer(int,      aggregator_type, aggregator_type)
        ctf_integer(int,      reason,          reason)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    aggregator_init,
    TP_ARGS(
        uint32_t, aggregator_id,
        int,      aggregator_type
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, aggregator_id,   aggregator_id)
        ctf_integer(int,      aggregator_type, aggregator_type)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    heartbeat_send,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, first_sn_low,
        uint32_t, last_sn_low,
        int,      via_aggregator
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,      entity_id)
        ctf_integer(uint32_t, first_sn_low,   first_sn_low)
        ctf_integer(uint32_t, last_sn_low,    last_sn_low)
        ctf_integer(int,      via_aggregator, via_aggregator)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    heartbeat_period_update,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, frequency_ms,
        uint32_t, hb_period_ms,
        int,      continuity_state
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,        entity_id)
        ctf_integer(uint32_t, frequency_ms,     frequency_ms)
        ctf_integer(uint32_t, hb_period_ms,     hb_period_ms)
        ctf_integer(int,      continuity_state, continuity_state)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    heartbeat_continuity_send,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, frequency_ms,
        int,      via_aggregator
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,      entity_id)
        ctf_integer(uint32_t, frequency_ms,   frequency_ms)
        ctf_integer(int,      via_aggregator, via_aggregator)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    heartbeat_continuity_state,
    TP_ARGS(
        uint32_t, entity_id,
        int,      old_state,
        int,      new_state
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,  entity_id)
        ctf_integer(int,      old_state,  old_state)
        ctf_integer(int,      new_state,  new_state)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    writer_recv_acknack,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, reader_entity_id,
        uint32_t, count,
        uint32_t, base_sn_low,
        uint32_t, num_bits
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,        entity_id)
        ctf_integer(uint32_t, reader_entity_id, reader_entity_id)
        ctf_integer(uint32_t, count,            count)
        ctf_integer(uint32_t, base_sn_low,      base_sn_low)
        ctf_integer(uint32_t, num_bits,         num_bits)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    writer_acknack_dropped,
    TP_ARGS(
        uint32_t, entity_id,
        int, reason,
        uint32_t, reader_entity_id
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id, entity_id)
        ctf_integer(int, reason, reason)
        ctf_integer(uint32_t, reader_entity_id, reader_entity_id)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    reader_recv_heartbeat,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, writer_entity_id,
        uint32_t, first_sn_low,
        uint32_t, last_sn_low,
        uint32_t, count
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,        entity_id)
        ctf_integer(uint32_t, writer_entity_id, writer_entity_id)
        ctf_integer(uint32_t, first_sn_low,     first_sn_low)
        ctf_integer(uint32_t, last_sn_low,      last_sn_low)
        ctf_integer(uint32_t, count,            count)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    reader_recv_data,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, writer_entity_id,
        uint32_t, seq_num_low,
        uint32_t, is_retransmission
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,          entity_id)
        ctf_integer(uint32_t, writer_entity_id,   writer_entity_id)
        ctf_integer(uint32_t, seq_num_low,        seq_num_low)
        ctf_integer(uint32_t, is_retransmission,  is_retransmission)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    reader_deliver,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, writer_entity_id,
        uint32_t, seq_num_low
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,        entity_id)
        ctf_integer(uint32_t, writer_entity_id, writer_entity_id)
        ctf_integer(uint32_t, seq_num_low,      seq_num_low)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    reader_send_acknack,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, writer_entity_id,
        uint32_t, base_sn_low,
        uint32_t, num_bits,
        uint32_t, trigger
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,        entity_id)
        ctf_integer(uint32_t, writer_entity_id, writer_entity_id)
        ctf_integer(uint32_t, base_sn_low,      base_sn_low)
        ctf_integer(uint32_t, num_bits,         num_bits)
        ctf_integer(uint32_t, trigger,          trigger)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    reader_acknack_deferred,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, writer_entity_id,
        uint32_t, num_missing,
        uint32_t, grace_ms
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,        entity_id)
        ctf_integer(uint32_t, writer_entity_id, writer_entity_id)
        ctf_integer(uint32_t, num_missing,      num_missing)
        ctf_integer(uint32_t, grace_ms,         grace_ms)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    reader_acknack_suppressed,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, writer_entity_id
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,        entity_id)
        ctf_integer(uint32_t, writer_entity_id, writer_entity_id)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    reader_acknack_grace_expired,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, writer_entity_id,
        uint32_t, num_still_missing
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,          entity_id)
        ctf_integer(uint32_t, writer_entity_id,   writer_entity_id)
        ctf_integer(uint32_t, num_still_missing,  num_still_missing)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    history_add_change,
    TP_ARGS(
        uint64_t, eventId,
        uint32_t, sn_low,
        uint32_t, path_kind,
        uint32_t, stored
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, eventId,   eventId)
        ctf_integer(uint32_t, sn_low,    sn_low)
        ctf_integer(uint32_t, path_kind, path_kind)
        ctf_integer(uint32_t, stored,    stored)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    history_register_gap,
    TP_ARGS(
        uint32_t, lastAvail_low,
        uint32_t, original_gap,
        uint32_t, capped_gap
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, lastAvail_low, lastAvail_low)
        ctf_integer(uint32_t, original_gap,   original_gap)
        ctf_integer(uint32_t, capped_gap,     capped_gap)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    history_get_missing,
    TP_ARGS(
        uint32_t, base_sn_low,
        uint32_t, num_bits
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, base_sn_low, base_sn_low)
        ctf_integer(uint32_t, num_bits,    num_bits)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    history_ensure_range,
    TP_ARGS(
        uint32_t, first_sn_low,
        uint32_t, last_sn_low,
        uint32_t, start_sn_low,
        uint32_t, total
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, first_sn_low, first_sn_low)
        ctf_integer(uint32_t, last_sn_low,  last_sn_low)
        ctf_integer(uint32_t, start_sn_low, start_sn_low)
        ctf_integer(uint32_t, total,        total)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    history_drop_oldest,
    TP_ARGS(
        uint32_t, lowest_sn_low
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, lowest_sn_low, lowest_sn_low)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    reader_recv_heartbeat_continuity,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, writer_entity_id,
        uint32_t, frequency_ms
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,        entity_id)
        ctf_integer(uint32_t, writer_entity_id, writer_entity_id)
        ctf_integer(uint32_t, frequency_ms,     frequency_ms)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    reader_continuity_violation,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, writer_entity_id
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,        entity_id)
        ctf_integer(uint32_t, writer_entity_id, writer_entity_id)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    heartbeat_loop_iteration,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, hb_period_ms,
        int,      continuity_state,
        uint32_t, frequency_ms,
        int,      inhibited
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,        entity_id)
        ctf_integer(uint32_t, hb_period_ms,     hb_period_ms)
        ctf_integer(int,      continuity_state, continuity_state)
        ctf_integer(uint32_t, frequency_ms,     frequency_ms)
        ctf_integer(int,      inhibited,        inhibited)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    writer_suppression_decision,
    TP_ARGS(
        uint32_t, entity_id,
        uint32_t, reader_entity_id,
        int,      suppress_unicast,
        int,      use_multicast,
        uint32_t, dest_addr
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, entity_id,        entity_id)
        ctf_integer(uint32_t, reader_entity_id, reader_entity_id)
        ctf_integer(int,      suppress_unicast, suppress_unicast)
        ctf_integer(int,      use_multicast,    use_multicast)
        ctf_integer(uint32_t, dest_addr,        dest_addr)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    log_message,
    TP_ARGS(
        const char *, subsystem,
        const char *, message
    ),
    TP_FIELDS(
        ctf_string(subsystem, subsystem)
        ctf_string(message, message)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    threadpool_queue_state,
    TP_ARGS(
        uint16_t, queue_level,
        uint16_t, queue_capacity,
        int, dropped,
        uint64_t, event_id
    ),
    TP_FIELDS(
        ctf_integer(uint16_t, queue_level, queue_level)
        ctf_integer(uint16_t, queue_capacity, queue_capacity)
        ctf_integer(int, dropped, dropped)
        ctf_integer(uint64_t, event_id, event_id)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    socket_buffer_state,
    TP_ARGS(
        uint16_t, port,
        uint32_t, packet_size,
        uint32_t, pending_bytes
    ),
    TP_FIELDS(
        ctf_integer(uint16_t, port, port)
        ctf_integer(uint32_t, packet_size, packet_size)
        ctf_integer(uint32_t, pending_bytes, pending_bytes)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    discovery_snapshot_begin,
    TP_ARGS(
        uint64_t, local_high,
        uint64_t, local_low,
        uint32_t, snapshot_id,
        int, is_final,
        int, state_packed,
        uint32_t, num_remote_parts,
        uint32_t, num_local_writers,
        uint32_t, num_local_readers,
        uint32_t, num_known_remote_writers,
        uint32_t, num_known_remote_readers
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint32_t, snapshot_id, snapshot_id)
        ctf_integer(int, is_final, is_final)
        ctf_integer(int, state_packed, state_packed)
        ctf_integer(uint32_t, num_remote_parts, num_remote_parts)
        ctf_integer(uint32_t, num_local_writers, num_local_writers)
        ctf_integer(uint32_t, num_local_readers, num_local_readers)
        ctf_integer(uint32_t, num_known_remote_writers, num_known_remote_writers)
        ctf_integer(uint32_t, num_known_remote_readers, num_known_remote_readers)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    discovery_snapshot_peer,
    TP_ARGS(
        uint64_t, local_high,
        uint64_t, local_low,
        uint32_t, snapshot_id,
        uint64_t, peer_high,
        uint64_t, peer_low,
        int, peer_spdp_state
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint32_t, snapshot_id, snapshot_id)
        ctf_integer(uint64_t, peer_high, peer_high)
        ctf_integer(uint64_t, peer_low, peer_low)
        ctf_integer(int, peer_spdp_state, peer_spdp_state)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    discovery_snapshot_local_endpoint,
    TP_ARGS(
        uint64_t, local_high,
        uint64_t, local_low,
        uint32_t, snapshot_id,
        int, kind,
        uint32_t, entity_id,
        const char *, topic_name
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint32_t, snapshot_id, snapshot_id)
        ctf_integer(int, kind, kind)
        ctf_integer(uint32_t, entity_id, entity_id)
        ctf_string(topic_name, topic_name)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    discovery_snapshot_remote_endpoint,
    TP_ARGS(
        uint64_t, local_high,
        uint64_t, local_low,
        uint32_t, snapshot_id,
        uint64_t, owner_high,
        uint64_t, owner_low,
        int, kind,
        uint32_t, entity_id,
        const char *, topic_name,
        int, matched
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint32_t, snapshot_id, snapshot_id)
        ctf_integer(uint64_t, owner_high, owner_high)
        ctf_integer(uint64_t, owner_low, owner_low)
        ctf_integer(int, kind, kind)
        ctf_integer(uint32_t, entity_id, entity_id)
        ctf_string(topic_name, topic_name)
        ctf_integer(int, matched, matched)
    )
)

TRACEPOINT_EVENT(
    streamrtps_trace,
    discovery_snapshot_end,
    TP_ARGS(
        uint64_t, local_high,
        uint64_t, local_low,
        uint32_t, snapshot_id
    ),
    TP_FIELDS(
        ctf_integer(uint64_t, local_high, local_high)
        ctf_integer(uint64_t, local_low, local_low)
        ctf_integer(uint32_t, snapshot_id, snapshot_id)
    )
)

#endif

#include <lttng/tracepoint-event.h>
