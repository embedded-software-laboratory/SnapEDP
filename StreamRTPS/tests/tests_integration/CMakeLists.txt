add_library(streamrtps_mock STATIC "")

target_include_directories(streamrtps_mock PUBLIC
    ${PROJECT_SOURCE_DIR}/include
    ${microcdr_streamrtps_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/tracepoint
)

target_sources(streamrtps_mock PRIVATE
    ${PROJECT_SOURCE_DIR}/src/rtps.cpp
    ${PROJECT_SOURCE_DIR}/src/ThreadPool.cpp
    ${PROJECT_SOURCE_DIR}/src/communication/UdpDriver.cpp
    ${PROJECT_SOURCE_DIR}/src/communication/MockNetworkDriver.cpp
    ${PROJECT_SOURCE_DIR}/src/communication/MockNetworkRouter.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/ParticipantProxyData.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/SPDPAgent.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/EDPAgentBase.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/SEDPAgent.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/SnapEDPAgent.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/SnapEDPAgentExchange.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/SnapEDPAgentEndpoints.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/SnapEDPEndpointStore.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/SnapEDPAgentWire.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/SnapEDPAgentHash.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/SnapEDPAgentDebug.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/SnapEDPMessages.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/SnapEDPPeerTable.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/SnapEDPResync.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/SnapEDPRoot.cpp
    ${PROJECT_SOURCE_DIR}/src/discovery/TopicData.cpp
    ${PROJECT_SOURCE_DIR}/src/entities/Domain.cpp
    ${PROJECT_SOURCE_DIR}/src/entities/Participant.cpp
    ${PROJECT_SOURCE_DIR}/src/entities/StatelessReader.cpp
    ${PROJECT_SOURCE_DIR}/src/entities/TransientReader.cpp
    ${PROJECT_SOURCE_DIR}/src/messages/MessageReceiver.cpp
    ${PROJECT_SOURCE_DIR}/src/messages/MessageTypes.cpp
    ${PROJECT_SOURCE_DIR}/src/storages/UnorderedHistoryCache.cpp
    ${PROJECT_SOURCE_DIR}/src/storages/OrderedHistoryCache.cpp
    ${PROJECT_SOURCE_DIR}/src/storages/SetCache.cpp
    ${PROJECT_SOURCE_DIR}/src/storages/PBufWrapper.cpp
    ${PROJECT_SOURCE_DIR}/src/entities/transient/HeartbeatPolicy.cpp
)

target_compile_definitions(streamrtps_mock PUBLIC
    EMBRTPS_USE_MOCK_DRIVER
)

target_link_libraries(streamrtps_mock PUBLIC microcdr_streamrtps)
if(EMBRTPS_ENABLE_TRACING)
    target_link_libraries(streamrtps_mock PUBLIC tracepoint_providers)
else()
    target_compile_definitions(streamrtps_mock PUBLIC EMBRTPS_DISABLE_TRACING)
endif()
target_compile_options(streamrtps_mock PUBLIC -fexceptions)

include(${PROJECT_SOURCE_DIR}/tests/tests_common/gossip_tests.cmake)

add_executable(test_mock_reliable test_mock_reliable.cpp)
target_link_libraries(test_mock_reliable PRIVATE streamrtps_mock)
configure_static_example(test_mock_reliable)

add_executable(test_mock_discovery test_mock_discovery.cpp)
target_include_directories(test_mock_discovery PRIVATE ${PROJECT_SOURCE_DIR}/tests/tests_common ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(test_mock_discovery PRIVATE streamrtps_mock pthread)
configure_static_example(test_mock_discovery)

add_executable(test_real_network_e2e test_real_network_e2e.cpp)
target_link_libraries(test_real_network_e2e PRIVATE streamrtps pthread)
configure_static_example(test_real_network_e2e)



add_test(NAME mock_basic_reliable_delivery
         COMMAND test_mock_reliable basic_reliable_delivery)
add_test(NAME mock_multiple_messages
         COMMAND test_mock_reliable multiple_messages)
add_test(NAME mock_reliable_with_packet_loss
         COMMAND test_mock_reliable reliable_with_packet_loss)
add_test(NAME mock_selective_drop_filter
         COMMAND test_mock_reliable selective_drop_filter)
add_test(NAME mock_bidirectional_exchange
         COMMAND test_mock_reliable bidirectional_exchange)

add_test(NAME discovery_two_standard_baseline
         COMMAND test_mock_discovery two_standard_baseline)
add_test(NAME discovery_spdp_advertises_gossip
         COMMAND test_mock_discovery spdp_advertises_gossip)
add_test(NAME discovery_two_gossip_cold_start
         COMMAND test_mock_discovery two_gossip_cold_start)
add_test(NAME discovery_three_gossip_cold_start
         COMMAND test_mock_discovery three_gossip_cold_start)
add_test(NAME discovery_bifurcation_recovery
         COMMAND test_mock_discovery bifurcation_recovery)
add_test(NAME discovery_forced_spdp_race_three
         COMMAND test_mock_discovery forced_spdp_race_three)
add_test(NAME discovery_late_joiner_mid_guid
         COMMAND test_mock_discovery late_joiner_mid_guid)
add_test(NAME discovery_low_guid_late_joiner_cascade
         COMMAND test_mock_discovery low_guid_late_joiner_cascade)
add_test(NAME discovery_cluster_merge_2plus2
         COMMAND test_mock_discovery cluster_merge_2plus2)
add_test(NAME discovery_endpoint_hash_converges
         COMMAND test_mock_discovery endpoint_hash_converges)
add_test(NAME discovery_endpoint_removal
         COMMAND test_mock_discovery endpoint_removal)
add_test(NAME discovery_steady_state_quiescence
         COMMAND test_mock_discovery steady_state_quiescence)
add_test(NAME discovery_resync_debounce_bounded
         COMMAND test_mock_discovery resync_debounce_bounded)
add_test(NAME discovery_concurrent_joiners_race
         COMMAND test_mock_discovery concurrent_joiners_race)
add_test(NAME discovery_root_death_no_readoption
         COMMAND test_mock_discovery root_death_no_readoption)

set_tests_properties(
    discovery_two_standard_baseline
    discovery_spdp_advertises_gossip
    discovery_two_gossip_cold_start
    discovery_three_gossip_cold_start
    discovery_bifurcation_recovery
    discovery_forced_spdp_race_three
    discovery_late_joiner_mid_guid
    discovery_low_guid_late_joiner_cascade
    discovery_cluster_merge_2plus2
    discovery_endpoint_hash_converges
    discovery_endpoint_removal
    discovery_steady_state_quiescence
    discovery_resync_debounce_bounded
    discovery_concurrent_joiners_race
    discovery_root_death_no_readoption
    PROPERTIES TIMEOUT 90)

set_tests_properties(
    mock_basic_reliable_delivery mock_multiple_messages
    mock_reliable_with_packet_loss mock_selective_drop_filter
    mock_bidirectional_exchange
    discovery_two_standard_baseline discovery_spdp_advertises_gossip
    discovery_two_gossip_cold_start discovery_three_gossip_cold_start
    discovery_bifurcation_recovery discovery_forced_spdp_race_three
    discovery_late_joiner_mid_guid discovery_low_guid_late_joiner_cascade
    discovery_cluster_merge_2plus2 discovery_endpoint_hash_converges
    discovery_endpoint_removal discovery_steady_state_quiescence
    discovery_resync_debounce_bounded discovery_concurrent_joiners_race
    discovery_root_death_no_readoption
    PROPERTIES LABELS integration)

add_test(NAME e2e_real_best_effort
         COMMAND test_real_network_e2e --best-effort --loss-pct 0 --count 20)
add_test(NAME e2e_real_reliable
         COMMAND test_real_network_e2e --reliable    --loss-pct 0 --count 20)
add_test(NAME e2e_real_reliable_loss_1
         COMMAND test_real_network_e2e --reliable    --loss-pct 1  --count 30)
add_test(NAME e2e_real_reliable_loss_5
         COMMAND test_real_network_e2e --reliable    --loss-pct 5  --count 30)
add_test(NAME e2e_real_reliable_loss_10
         COMMAND test_real_network_e2e --reliable    --loss-pct 10 --count 30)

set_tests_properties(
    e2e_real_reliable_loss_1
    e2e_real_reliable_loss_5
    e2e_real_reliable_loss_10
    PROPERTIES TIMEOUT 90)

file(GLOB _it_fragments ${CMAKE_CURRENT_SOURCE_DIR}/cmake/*.cmake)
foreach(f ${_it_fragments})
    include(${f})
endforeach()

set_tests_properties(e2e_real_best_effort e2e_real_reliable e2e_real_reliable_loss_1
    e2e_real_reliable_loss_5 e2e_real_reliable_loss_10 PROPERTIES LABELS realnet)
