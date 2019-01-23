include_directories(
    ${PROJECT_SOURCE_DIR}/parser/include
    ${PROJECT_SOURCE_DIR}/ylib/include
    ${PROJECT_SOURCE_DIR}/ynet/include
    ${PROJECT_SOURCE_DIR}/yfs/include
    )
link_directories(${PROJECT_BINARY_DIR}/lib)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/sbin)
set(LIBRARY_OUTPUT_PATH    ${PROJECT_BINARY_DIR}/lib)

set(MOD_EXTRA_LIBS pthread aio crypt uuid ssl parser ylib ynet yfs)

set(MOD_SRCS
	main.c
	cds.c
	cds_cache.c
	chk_jnl.c
	cds_robot.c
	cds_volume.c
	cds_proto.c
	cds_hb.c
	cds_lib.c
	cds_events.c
	cds_state_machine.c
        rangelock.c
	cds_jnl.c
	disk.c
	watchdog.c
	cds_sync.c
	cds_leveldb.c
	)

# cds_hotsum.c

add_executable(yfs_cds ${MOD_SRCS})
target_link_libraries(yfs_cds ${MOD_EXTRA_LIBS})

install(TARGETS yfs_cds
    RUNTIME DESTINATION app/sbin)

