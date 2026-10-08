# Chain Test Example - Three Node Communication

# Node A - Initiator and RTT Calculator
add_executable(NodeA NodeA.cpp)
target_include_directories(NodeA PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(NodeA streamrtps pthread)
configure_static_example(NodeA)

# Node B - Forwarder
add_executable(NodeB NodeB.cpp)
target_include_directories(NodeB PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(NodeB streamrtps pthread)
configure_static_example(NodeB)

# Node C - Responder
add_executable(NodeC NodeC.cpp)
target_include_directories(NodeC PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(NodeC streamrtps pthread)
configure_static_example(NodeC)


message(STATUS "Chain Test example configured")
