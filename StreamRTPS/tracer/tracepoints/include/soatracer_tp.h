#undef TRACEPOINT_PROVIDER
#define TRACEPOINT_PROVIDER soatracer_trace_provider

#undef LTTNG_UST_TRACEPOINT_INCLUDE
#define LTTNG_UST_TRACEPOINT_INCLUDE "./soatracer_tp.h"
#if !defined(_SOATRACER_TP_H) || defined(LTTNG_UST_TRACEPOINT_HEADER_MULTI_READ)
#define _SOATRACER_TP_H

#include <lttng/tracepoint.h>


TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    experiment_begin,
    TP_ARGS(
        uint32_t, experiment_id,
        uint64_t, unique_identifier
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, experiment_id, experiment_id)
        ctf_integer(uint32_t, unique_identifier, unique_identifier)
    )
)

TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    experiment_end,
    TP_ARGS(
        uint32_t, experiment_id,
        uint64_t, unique_identifier
    ),
    TP_FIELDS(
        ctf_integer(uint32_t, experiment_id, experiment_id)
        ctf_integer(uint32_t, unique_identifier, unique_identifier)
    )
)

LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    service_info,
    LTTNG_UST_TP_ARGS(
        int, test_id,
        const char *, service_type,
        int, service_id,
        int, pid
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_integer(int, test_id, test_id)
        lttng_ust_field_string(service_type, service_type)
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, pid, pid)
    )
)


LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    discovery_pre_publish,
    LTTNG_UST_TP_ARGS(
        const char *, topic_name,
        int, service_id,
        int, msg_id,
        int, test_id
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_string(topic_name, topic_name)
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, msg_id, msg_id)
        lttng_ust_field_integer(int, test_id, test_id)
    )
)

LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    discovery_after_publish,
    LTTNG_UST_TP_ARGS(
        const char *, topic_name,
        int, service_id,
        int, msg_id,
        int, test_id
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_string(topic_name, topic_name)
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, msg_id, msg_id)
        lttng_ust_field_integer(int, test_id, test_id)
    )
)

LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    discovery_received,
    LTTNG_UST_TP_ARGS(
        const char *, topic_name,
        int, service_id,
        int, msg_id,
        int, pub_id,
        int, test_id
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_string(topic_name, topic_name)
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, msg_id, msg_id)
        lttng_ust_field_integer(int, pub_id, pub_id)
        lttng_ust_field_integer(int, test_id, test_id)
    )
)


LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    pre_publish,
    LTTNG_UST_TP_ARGS(
        const char *, topic_name,
        int, service_id,
        int, msg_id,
        int, seq_num,
        int, test_id
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_string(topic_name, topic_name)
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, msg_id, msg_id)
        lttng_ust_field_integer(int, seq_num, seq_num)
        lttng_ust_field_integer(int, test_id, test_id)
    )
)

LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    after_publish,
    LTTNG_UST_TP_ARGS(
        const char *, topic_name,
        int, service_id,
        int, msg_id,
        int, seq_num,
        int, test_id
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_string(topic_name, topic_name)
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, msg_id, msg_id)
        lttng_ust_field_integer(int, seq_num, seq_num)
        lttng_ust_field_integer(int, test_id, test_id)
    )
)

LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    published_all,
    LTTNG_UST_TP_ARGS(
        const char *, topic_name,
        int, service_id,
        int, msg_id,
        int, seq_num,
        int, test_id
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_string(topic_name, topic_name)
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, msg_id, msg_id)
        lttng_ust_field_integer(int, seq_num, seq_num)
        lttng_ust_field_integer(int, test_id, test_id)
    )
)


LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    init_proc_timer,
    LTTNG_UST_TP_ARGS(
        const char *, topic_name,
        int, service_id,
        int, test_id
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_string(topic_name, topic_name)
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, test_id, test_id)
    )
)

LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    curr_msg_proc,
    LTTNG_UST_TP_ARGS(
        const char *, topic_name,
        int, msg_id,
        int, process_id,
        int, test_id
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_string(topic_name, topic_name)
        lttng_ust_field_integer(int, msg_id, msg_id)
        lttng_ust_field_integer(int, process_id, process_id)
        lttng_ust_field_integer(int, test_id, test_id)
    )
)


LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    after_receiving,
    LTTNG_UST_TP_ARGS(
        const char *, topic_name,
        int, service_id,
        int, msg_count,
        int, msg_id,
        int, pub_id,
        int, seq_num,
        int, test_id
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_string(topic_name, topic_name)
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, msg_count, msg_count)
        lttng_ust_field_integer(int, msg_id, msg_id)
        lttng_ust_field_integer(int, pub_id, pub_id)
        lttng_ust_field_integer(int, seq_num, seq_num)
        lttng_ust_field_integer(int, test_id, test_id)
    )
)

LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    received_all,
    LTTNG_UST_TP_ARGS(
        const char *, topic_name,
        int, service_id,
        int, msg_count,
        int, msg_id,
        int, seq_num,
        int, test_id
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_string(topic_name, topic_name)
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, msg_count, msg_count)
        lttng_ust_field_integer(int, msg_id, msg_id)
        lttng_ust_field_integer(int, seq_num, seq_num)
        lttng_ust_field_integer(int, test_id, test_id)
    )
)

LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    first_publish,
    LTTNG_UST_TP_ARGS(
        const char *, topic_name,
        int, service_id,
        int, test_id,
        uint64_t, elapsed_ns
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_string(topic_name, topic_name)
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, test_id, test_id)
        lttng_ust_field_integer(uint64_t, elapsed_ns, elapsed_ns)
    )
)

LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    first_receive,
    LTTNG_UST_TP_ARGS(
        const char *, topic_name,
        int, service_id,
        int, test_id,
        uint64_t, elapsed_ns,
        int, first_seq
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_string(topic_name, topic_name)
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, test_id, test_id)
        lttng_ust_field_integer(uint64_t, elapsed_ns, elapsed_ns)
        lttng_ust_field_integer(int, first_seq, first_seq)
    )
)

LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    participant_join,
    LTTNG_UST_TP_ARGS(
        int, service_id,
        int, test_id,
        const char *, role
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, test_id, test_id)
        lttng_ust_field_string(role, role)
    )
)

LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    participant_kill,
    LTTNG_UST_TP_ARGS(
        int, service_id,
        int, test_id,
        const char *, role,
        int, was_root
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, test_id, test_id)
        lttng_ust_field_string(role, role)
        lttng_ust_field_integer(int, was_root, was_root)
    )
)

LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    endpoint_delete,
    LTTNG_UST_TP_ARGS(
        const char *, topic_name,
        int, service_id,
        int, test_id
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_string(topic_name, topic_name)
        lttng_ust_field_integer(int, service_id, service_id)
        lttng_ust_field_integer(int, test_id, test_id)
    )
)

LTTNG_UST_TRACEPOINT_EVENT(
    TRACEPOINT_PROVIDER,
    machine_stats,
    LTTNG_UST_TP_ARGS(
        double, cpu_usage,
        double, memory_usage
    ),
    LTTNG_UST_TP_FIELDS(
        lttng_ust_field_float(double, cpu_usage, cpu_usage)
        lttng_ust_field_float(double, memory_usage, memory_usage)
    )
)

#endif

#include <lttng/tracepoint-event.h>
