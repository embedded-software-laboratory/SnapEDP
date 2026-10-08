# ReqResp Example - Two Node Message Exchange

add_executable(Sender Sender.cpp)
target_include_directories(Sender PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(Sender streamrtps pthread)
configure_static_example(Sender)

add_executable(Responder Responder.cpp)
target_include_directories(Responder PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(Responder streamrtps pthread)
configure_static_example(Responder)

add_executable(StrictSender StrictSender.cpp)
target_include_directories(StrictSender PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(StrictSender streamrtps pthread)
configure_static_example(StrictSender)

add_executable(StrictResponder StrictResponder.cpp)
target_include_directories(StrictResponder PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(StrictResponder streamrtps pthread)
configure_static_example(StrictResponder)

message(STATUS "ReqResp message exchange example configured")
