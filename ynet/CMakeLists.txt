include_directories(
    ${PROJECT_SOURCE_DIR}/parser/include
    ${PROJECT_SOURCE_DIR}/ylib/include
    ${PROJECT_SOURCE_DIR}/ynet/include
    )
link_directories(${PROJECT_BINARY_DIR}/lib)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH    ${PROJECT_BINARY_DIR}/lib)

set(MOD_EXTRA_LIBS pthread aio parser ylib)

set(MOD_SRCS
    sock/sock_buffer.c
    sock/sock_passive.c
    sock/sock_tcp.c
    sock/sock_udp.c
    sock/sock_xnect.c
    sock/sock_xmit.c
    sock/sdevents.c
    net/net_crc.c
    net/net_events.c
    net/net_lib.c
    net/net_passive.c
    net/net_table.c
    net/xnect.c
    net/net_xnect.c
    net/net_xmit.c
    rpc/rpc_lib.c
    rpc/msgqueue.c
    rpc/rpc_proto.c
    rpc/main_loop.c
    rpc/rpc_passive.c
    rpc/rpc_reply.c
    rpc/rpc_xnect.c
    rpc/rpc_request.c
    rpc/rpc_xmit.c
    sunrpc/sunrpc_passive.c
    sunrpc/sunrpc_reply.c
    sunrpc/sunrpc_proto.c
    netbios/netbios_reply.c
    netbios/netbios_passive.c
    netbios/netbios_proto.c)

# add_library(ynet ${MOD_SRCS})
# target_link_libraries(ynet ${MOD_EXTRA_LIBS})

add_library(ynet ${MOD_SRCS})
target_link_libraries(ynet ${MOD_EXTRA_LIBS})
set_target_properties(ynet PROPERTIES OUTPUT_NAME "ynet")
set_target_properties(ynet PROPERTIES VERSION 1.0 SOVERSION 1)

install(TARGETS ynet
    ARCHIVE DESTINATION app/lib
    LIBRARY DESTINATION app/lib
    RUNTIME DESTINATION app/sbin
    )
