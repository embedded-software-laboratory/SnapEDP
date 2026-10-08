cmake_minimum_required(VERSION 3.10)
project(streamRTPS VERSION 1.0.0)
set(CMAKE_CXX_STANDARD 17)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

option(EMBRTPS_STATIC_EXAMPLES "Link example executables as static binaries where supported" ON)
option(EMBRTPS_FULLY_STATIC_EXAMPLES "Force fully static example binaries (-static); requires static system/LTTng libraries" OFF)
option(EMBRTPS_ENABLE_TRACING "Enable LTTng-based tracing support" OFF)
option(RTPS_ENABLE_PACKET_LOSS  "Compile in UDP packet-loss injection hook" ON)
option(RTPS_DISABLE_PACKET_LOSS "Hard-disable packet-loss injection (overrides RTPS_ENABLE_PACKET_LOSS)" OFF)

option(EMBRTPS_BUILD_TESTS "Build unit and integration tests" ON)
set(EMBRTPS_SANITIZER "" CACHE STRING "Sanitizers: address, thread, undefined, address,undefined")
option(EMBRTPS_COVERAGE "Build with --coverage -O0" OFF)

if(EMBRTPS_SANITIZER)
    # sanitizer runtimes conflict with static libgcc/libstdc++
    set(EMBRTPS_STATIC_EXAMPLES OFF CACHE BOOL "" FORCE)
    add_compile_options(-fsanitize=${EMBRTPS_SANITIZER} -fno-omit-frame-pointer -O1 -g)
    add_link_options(-fsanitize=${EMBRTPS_SANITIZER})
endif()
if(EMBRTPS_COVERAGE)
    add_compile_options(--coverage -O0 -g)
    add_link_options(--coverage)
endif()

function(configure_static_example target_name)
    if(EMBRTPS_STATIC_EXAMPLES AND UNIX AND NOT APPLE)
        target_link_options(${target_name} PRIVATE -static-libgcc -static-libstdc++)
        if(EMBRTPS_FULLY_STATIC_EXAMPLES)
            target_link_options(${target_name} PRIVATE -static)
        endif()
    endif()
endfunction()

# Add micro-CDR
add_subdirectory(thirdparty/Micro-CDR)

# streamRTPS
add_library(streamrtps STATIC "")

include_directories(${microcdr_streamrtps_SOURCE_DIR}/include)

target_include_directories(streamrtps PUBLIC include ${microcdr_streamrtps_SOURCE_DIR}/include)

target_sources(streamrtps
        PRIVATE
        "src/rtps.cpp"
        "src/ThreadPool.cpp"
        "src/communication/UdpDriver.cpp"
        "src/discovery/ParticipantProxyData.cpp"
        "src/discovery/SPDPAgent.cpp"
        "src/discovery/EDPAgentBase.cpp"
        "src/discovery/SEDPAgent.cpp"
        "src/discovery/SnapEDPAgent.cpp"
        "src/discovery/SnapEDPAgentExchange.cpp"
        "src/discovery/SnapEDPAgentEndpoints.cpp"
        "src/discovery/SnapEDPEndpointStore.cpp"
        "src/discovery/SnapEDPAgentWire.cpp"
        "src/discovery/SnapEDPAgentHash.cpp"
        "src/discovery/SnapEDPAgentDebug.cpp"
        "src/discovery/SnapEDPMessages.cpp"
        "src/discovery/SnapEDPPeerTable.cpp"
        "src/discovery/SnapEDPResync.cpp"
        "src/discovery/SnapEDPRoot.cpp"
        "src/discovery/TopicData.cpp"
        "src/entities/Domain.cpp"
        "src/entities/Participant.cpp"
        "src/entities/StatelessReader.cpp"
        "src/entities/TransientReader.cpp"
        "src/messages/MessageReceiver.cpp"
        "src/messages/MessageTypes.cpp"
        "src/storages/UnorderedHistoryCache.cpp"
        "src/storages/OrderedHistoryCache.cpp"
        "src/storages/SetCache.cpp"
        "src/storages/PBufWrapper.cpp"
        "src/entities/transient/HeartbeatPolicy.cpp"
)

target_include_directories(streamrtps PUBLIC tracepoint)

if(EMBRTPS_ENABLE_TRACING)
    find_package(LTTngUST REQUIRED)

    add_library(tracepoint_providers STATIC
        "tracepoint/streamrtps_trace.cpp"
    )
    target_include_directories(tracepoint_providers PUBLIC tracepoint)
    target_link_libraries(tracepoint_providers PUBLIC LTTng::UST)

    target_link_libraries(streamrtps PUBLIC microcdr_streamrtps tracepoint_providers)
    # high-volume per-message perf tracepoints; opt-in only, they flood the
    # LTTng buffers at scale and cost real measurements (events get discarded)
    option(EMBRTPS_TRACE_PERF "Emit per-message perf tracepoints (heavy at scale)" OFF)
    if(EMBRTPS_TRACE_PERF)
        target_compile_definitions(streamrtps PUBLIC TRACE_PERF=1)
    endif()
else()
    target_compile_definitions(streamrtps PUBLIC EMBRTPS_DISABLE_TRACING)
    target_link_libraries(streamrtps PUBLIC microcdr_streamrtps)
endif()
target_compile_options(streamrtps PUBLIC -fexceptions)

if(RTPS_ENABLE_PACKET_LOSS AND NOT RTPS_DISABLE_PACKET_LOSS)
    target_compile_definitions(streamrtps PUBLIC RTPS_UDP_PACKET_LOSS_ENABLED)
endif()

# Examples
add_subdirectory(example/MessageChain)
add_subdirectory(example/ReqResp)
if(EMBRTPS_ENABLE_TRACING)
    add_subdirectory(tracer)
endif()
if(EMBRTPS_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests/tests_unit)
    add_subdirectory(tests/tests_integration)
    add_subdirectory(tests/tests_e2e)
endif()
