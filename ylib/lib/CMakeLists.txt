include_directories(
    ${PROJECT_SOURCE_DIR}/parser/include
    ${PROJECT_SOURCE_DIR}/ylib/include
    )
link_directories(${PROJECT_BINARY_DIR}/lib)

set(MOD_EXTRA_LIBS pthread aio)

set(MOD_SRCS job_dock.c async.c aiowp.c auth.c array_table.c
    buffer.c bitmap.c cmp.c crc32.c crcrs.c daemon.c
    dbg.c hash.c hash_table.c htable.c itab.c itab1.c cache.c
    job_tracker.c journal.c lock.c mem.c nls.c nls/nls_cp936.c
    path.c pipe_pool.c skiplist.c shm.c stat.c str.c
    sysutil.c timer.c xdr.c ylog.c ypool.c ytime.c bmap.c
    dynarray.c privilege.c proc.c md5.c squeue.c round_journal.c
    analysis.c heap.c)

# add_library(ylib ${MOD_SRCS})
# target_link_libraries(ylib ${MOD_EXTRA_LIBS})

add_library(ylib ${MOD_SRCS})
target_link_libraries(ylib ${MOD_EXTRA_LIBS})
# set_target_properties(ylib PROPERTIES OUTPUT_NAME "ylib")
set_target_properties(ylib PROPERTIES VERSION 1.0 SOVERSION 1)

install(TARGETS ylib
    ARCHIVE DESTINATION app/lib
    LIBRARY DESTINATION app/lib
    RUNTIME DESTINATION app/sbin
    )
