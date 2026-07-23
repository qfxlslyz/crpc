set(HEADERS
    config.h
    error_code.h
    log.h
    msg_req.h
    run_time.h
    string_util.h
    thread_pool.h
)

install(FILES ${HEADERS} DESTINATION include/crpc/base)
