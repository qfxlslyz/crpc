set(HEADERS
    timeout_slot.h
    io_thread.h
    net_address.h
    tcp_buffer.h
    tcp_client.h
    tcp_connection.h
    tcp_connection_time_wheel.h
    tcp_server.h
)

install(FILES ${HEADERS} DESTINATION include/crpc/net/transport)
