set(HEADERS
    crpc_async_channel.h
    crpc_channel.h
    rpc_closure.h
    crpc_controller.h
    rpc_dispatcher.h
    rpc_codec.h
    rpc_message.h
    rpc_server.h
)

install(FILES ${HEADERS} DESTINATION include/crpc/rpc)
