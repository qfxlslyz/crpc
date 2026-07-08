set(HEADERS
    codec.h
    protocol_message.h
    dispatcher.h
    byte.h
)

install(FILES ${HEADERS} DESTINATION include/crpc/net/protocol)
