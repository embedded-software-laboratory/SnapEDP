add_executable(unordered_history_cache_tests
    test_unordered_history_cache.cpp
)

target_link_libraries(unordered_history_cache_tests PRIVATE streamrtps)

configure_static_example(unordered_history_cache_tests)

add_executable(ordered_history_cache_tests
    test_ordered_history_cache.cpp
)

target_link_libraries(ordered_history_cache_tests PRIVATE streamrtps)

configure_static_example(ordered_history_cache_tests)

add_executable(reliable_loss_tests
    test_reliable_loss.cpp
)

target_link_libraries(reliable_loss_tests PRIVATE streamrtps)

configure_static_example(reliable_loss_tests)

add_executable(heartbeat_continuity_tests
    test_heartbeat_continuity.cpp
)

target_link_libraries(heartbeat_continuity_tests PRIVATE streamrtps)

configure_static_example(heartbeat_continuity_tests)


add_test(NAME unordered_history_cache_tests COMMAND unordered_history_cache_tests)
add_test(NAME ordered_history_cache_tests COMMAND ordered_history_cache_tests)
add_test(NAME reliable_loss_tests COMMAND reliable_loss_tests)
add_test(NAME heartbeat_continuity_tests COMMAND heartbeat_continuity_tests)
set_tests_properties(unordered_history_cache_tests ordered_history_cache_tests
    reliable_loss_tests heartbeat_continuity_tests PROPERTIES LABELS unit)

function(add_gossip_unit_test binary source lib)
    if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${source})
        return()
    endif()
    add_executable(${binary} ${source})
    target_include_directories(${binary} PRIVATE ${PROJECT_SOURCE_DIR}/tests/tests_common ${PROJECT_SOURCE_DIR}/src)
    target_link_libraries(${binary} PRIVATE ${lib} pthread)
    configure_static_example(${binary})
    foreach(t ${ARGN})
        add_test(NAME unit_${t} COMMAND ${binary} ${t})
        set_tests_properties(unit_${t} PROPERTIES LABELS unit TIMEOUT 120)
    endforeach()
endfunction()
file(GLOB _unit_fragments ${CMAKE_CURRENT_SOURCE_DIR}/cmake/*.cmake)
foreach(f ${_unit_fragments})
    include(${f})
endforeach()
