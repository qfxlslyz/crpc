set(HEADERS
    fd_event.h
    reactor.h
    timer.h
)

install(FILES ${HEADERS} DESTINATION include/crpc/net/event)
