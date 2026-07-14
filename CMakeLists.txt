cmake_minimum_required(VERSION 3.0)
project(crpc)

SET(CMAKE_INSTALL_PREFIX ${PROJECT_SOURCE_DIR})
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
enable_language(ASM)

option(CRPC_ENABLE_MYSQL "Build CRPC with the optional MySQL plugin" OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -std=c++11 -Wall -Wno-deprecated -Wno-unused-but-set-variable")

set(PATH_LIB lib)
set(PATH_BIN bin)
set(PATH_TESTCASES testcases)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/${PATH_LIB})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/${PATH_BIN})

include_directories(${PROJECT_SOURCE_DIR})

set(CRPC_BASE_SOURCES
    crpc/base/config.cc
    crpc/base/log.cc
    crpc/base/msg_req.cc
    crpc/base/mutex.cc
    crpc/base/string_util.cc
    crpc/base/thread_pool.cc
)

set(CRPC_COROUTINE_SOURCES
    crpc/coroutine/coroutine.cc
    crpc/coroutine/coroutine_hook.cc
    crpc/coroutine/coroutine_pool.cc
    crpc/coroutine/memory.cc
    crpc/coroutine/coctx_swap.S
)

set(CRPC_NET_EVENT_SOURCES
    crpc/net/event/fd_event.cc
    crpc/net/event/reactor.cc
    crpc/net/event/timer.cc
)

set(CRPC_NET_TRANSPORT_SOURCES
    crpc/net/transport/io_thread.cc
    crpc/net/transport/net_address.cc
    crpc/net/transport/tcp_buffer.cc
    crpc/net/transport/tcp_client.cc
    crpc/net/transport/tcp_connection.cc
    crpc/net/transport/tcp_connection_time_wheel.cc
    crpc/net/transport/tcp_server.cc
)

set(CRPC_RPC_SOURCES
    crpc/rpc/crpc_channel.cc
    crpc/rpc/rpc_codec.cc
    crpc/rpc/crpc_controller.cc
    crpc/rpc/rpc_dispatcher.cc
    crpc/rpc/rpc_server.cc
)

set(CRPC_PLUGIN_SOURCES)
if (CRPC_ENABLE_MYSQL)
    list(APPEND CRPC_PLUGIN_SOURCES
        crpc/plugins/mysql/mysql_instance.cc
    )
endif ()

add_library(crpc
    ${CRPC_BASE_SOURCES}
    ${CRPC_COROUTINE_SOURCES}
    ${CRPC_NET_EVENT_SOURCES}
    ${CRPC_NET_TRANSPORT_SOURCES}
    ${CRPC_RPC_SOURCES}
    ${CRPC_PLUGIN_SOURCES}
)

if (CRPC_ENABLE_MYSQL)
    target_compile_definitions(crpc PUBLIC DECLARE_MYSQL_PLUGIN)
    find_library(MYSQLCLIENT_LIBRARY mysqlclient)
    if (NOT MYSQLCLIENT_LIBRARY)
        message(FATAL_ERROR "CRPC_ENABLE_MYSQL=ON requires mysqlclient")
    endif ()
endif ()

install(TARGETS crpc DESTINATION ${PATH_LIB})


find_package(Protobuf REQUIRED)
if (${PROTOBUF_FOUND})
    message("protobuf found")
else ()
    message(FATAL_ERROR "Cannot find Protobuf")
endif ()

include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(TEST_RPC_PROTO ${PROJECT_SOURCE_DIR}/${PATH_TESTCASES}/test_rpc_server.proto)
set(TEST_RPC_PB_CC ${CMAKE_CURRENT_BINARY_DIR}/test_rpc_server.pb.cc)
set(TEST_RPC_PB_H ${CMAKE_CURRENT_BINARY_DIR}/test_rpc_server.pb.h)

add_custom_command(
    OUTPUT ${TEST_RPC_PB_CC} ${TEST_RPC_PB_H}
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
            --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
            -I ${PROJECT_SOURCE_DIR}/${PATH_TESTCASES}
            ${TEST_RPC_PROTO}
    DEPENDS ${TEST_RPC_PROTO}
)

set(tinyxml /usr/lib/libtinyxml.a)

set(LIBS
    crpc
    ${Protobuf_LIBRARY}
    ${tinyxml}
    dl
    pthread
)

if (CRPC_ENABLE_MYSQL)
    list(APPEND LIBS ${MYSQLCLIENT_LIBRARY})
endif ()

add_library(test_rpc_proto ${TEST_RPC_PB_CC})

# test_rpc_server
set(
    test_rpc_server
    ${PROJECT_SOURCE_DIR}/${PATH_TESTCASES}/test_rpc_server.cc
)
add_executable(test_rpc_server ${test_rpc_server})
target_link_libraries(test_rpc_server test_rpc_proto ${LIBS})
install(TARGETS test_rpc_server DESTINATION ${PATH_BIN})

# test_rpc_server_client
set(
    test_rpc_server_client
    ${PROJECT_SOURCE_DIR}/${PATH_TESTCASES}/test_rpc_server_client.cc
)
add_executable(test_rpc_server_client ${test_rpc_server_client})
target_link_libraries(test_rpc_server_client test_rpc_proto ${LIBS})
install(TARGETS test_rpc_server_client DESTINATION ${PATH_BIN})

# test_coroutine
set(
    test_coroutine
    ${PROJECT_SOURCE_DIR}/${PATH_TESTCASES}/test_coroutine.cc
)
add_executable(test_coroutine ${test_coroutine})
target_link_libraries(test_coroutine ${LIBS})
install(TARGETS test_coroutine DESTINATION ${PATH_BIN})

# install *.h
add_subdirectory(crpc/base)
add_subdirectory(crpc/coroutine)
add_subdirectory(crpc/net)
add_subdirectory(crpc/rpc)
add_subdirectory(crpc/plugins)
