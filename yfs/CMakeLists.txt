include_directories(
    ${PROJECT_SOURCE_DIR}/parser/include
    ${PROJECT_SOURCE_DIR}/ylib/include
    ${PROJECT_SOURCE_DIR}/ynet/include
    ${PROJECT_SOURCE_DIR}/yfs/include
    )
link_directories(${PROJECT_BINARY_DIR}/lib)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH    ${PROJECT_BINARY_DIR}/lib)

set(MOD_EXTRA_LIBS pthread aio parser ylib ynet)

set(MOD_SRCS
    inode/inode_client.c
    inode/inode_event.c
    inode/inode_server.c
    inode/inode_proto.c
    inode/sequence.c
    mdc/mdc_inode.c
    mdc/mdc_dir.c
    mdc/mdc_node.c
    mdc/mdc_super.c
    mdc/mdc_chunk.c
    mdc/mdc_file.c
    mdc/mdc_lib.c
    mdc/namei_cache.c
    mdc/lease_timer.c
    cdc/cdc_super.c
    cdc/cdc_chunk.c
    cdc/cdc_replica.c
    cdc/cdc_lib.c
    cdc/cdc_journal.c
    libyfs/dir.c
    libyfs/node.c
    libyfs/super.c
    libyfs/chunk.c
    libyfs/raw.c
    libyfs/file_table.c
    libyfs/yfs_lib.c
    libyfs/write_crc.c
    libyfs/yfs_state_machine.c
    libyfs/djnl_master.c
    libyfs/djnl_slave.c
    libyfs/worm.c
    c60/c60_lib.c
    c60/c60_util.c)

add_library(yfs ${MOD_SRCS})
target_link_libraries(yfs ${MOD_EXTRA_LIBS})
set_target_properties(yfs PROPERTIES OUTPUT_NAME "yfs")
set_target_properties(yfs PROPERTIES VERSION 1.0 SOVERSION 1)

install(TARGETS yfs
    ARCHIVE DESTINATION app/lib
    LIBRARY DESTINATION app/lib
    RUNTIME DESTINATION app/sbin
    )
