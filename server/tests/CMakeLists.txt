cmake_minimum_required(VERSION 3.18)
project(message_delivery_tests LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
enable_testing()

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(spdlog REQUIRED)
find_package(GTest REQUIRED)
find_package(Python3 COMPONENTS Interpreter REQUIRED)
find_package(OpenSSL REQUIRED)
find_path(AMQPCPP_INCLUDE_DIR amqpcpp.h REQUIRED)
find_library(AMQPCPP_LIBRARY amqpcpp REQUIRED)
find_library(EV_LIBRARY ev REQUIRED)

set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../proto)
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/base.pb.cc
           ${CMAKE_CURRENT_BINARY_DIR}/base.pb.h
           ${CMAKE_CURRENT_BINARY_DIR}/base_pb2.py
    COMMAND protobuf::protoc --proto_path=${PROTO_DIR}
            --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
            --python_out=${CMAKE_CURRENT_BINARY_DIR}
            --experimental_allow_proto3_optional ${PROTO_DIR}/base.proto
    DEPENDS ${PROTO_DIR}/base.proto
    VERBATIM)
add_library(delivery_proto STATIC ${CMAKE_CURRENT_BINARY_DIR}/base.pb.cc)
target_link_libraries(delivery_proto PUBLIC protobuf::libprotobuf)
target_include_directories(delivery_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

add_executable(message_persistence_test message_persistence_test.cc)
target_include_directories(message_persistence_test PRIVATE ../common)
target_link_libraries(message_persistence_test PRIVATE delivery_proto spdlog::spdlog
                      GTest::gtest Threads::Threads)
add_test(NAME message_persistence COMMAND message_persistence_test)

add_executable(mq_consumer_probe mq_consumer_probe.cc)
target_include_directories(mq_consumer_probe PRIVATE ../common ${AMQPCPP_INCLUDE_DIR})
# Compile the same broker regressions against the pre-fix MQ wrapper if desired.
set(MQ_BASELINE_INCLUDE_DIR "" CACHE PATH "Directory containing the pre-fix rabbitmq.hpp")
if(MQ_BASELINE_INCLUDE_DIR)
    target_include_directories(mq_consumer_probe BEFORE PRIVATE ${MQ_BASELINE_INCLUDE_DIR})
endif()
target_link_libraries(mq_consumer_probe PRIVATE delivery_proto spdlog::spdlog
                      ${AMQPCPP_LIBRARY} ${EV_LIBRARY} OpenSSL::SSL OpenSSL::Crypto
                      Threads::Threads ${CMAKE_DL_LIBS})

option(MQ_BROKER_TESTS "Run broker integration tests against isolated localhost RabbitMQ" OFF)
if(MQ_BROKER_TESTS)
    add_test(NAME mq_delivery COMMAND ${Python3_EXECUTABLE}
             ${CMAKE_CURRENT_SOURCE_DIR}/test_mq_delivery.py
             --probe $<TARGET_FILE:mq_consumer_probe>
             --proto-dir ${CMAKE_CURRENT_BINARY_DIR})
    set_tests_properties(mq_delivery PROPERTIES TIMEOUT 90 RUN_SERIAL TRUE)
endif()
