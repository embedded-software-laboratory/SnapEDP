add_gossip_unit_test(test_gedp_messages test_gedp_messages.cpp streamrtps
    request_roundtrip response_roundtrip response_empty announcement_roundtrip
    peek_kind peek_kind_invalid frame_exact_fit frame_overflow max_length_names
    truncated_every_length count_exceeds_payload count_huge oversized_string
    total_endpoints_mismatch frame_writer_single_frame frame_writer_splits
    frame_writer_announcement)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/fuzz_gedp.cpp)
    add_executable(fuzz_gedp fuzz_gedp.cpp)
    target_compile_options(fuzz_gedp PRIVATE -fsanitize=fuzzer,address,undefined -g)
    target_link_options(fuzz_gedp PRIVATE -fsanitize=fuzzer,address,undefined)
    target_link_libraries(fuzz_gedp PRIVATE streamrtps)
endif()
