include_directories(
    ${PROJECT_SOURCE_DIR}/parser/include
    ${PROJECT_SOURCE_DIR}/ylib/include
    ${PROJECT_SOURCE_DIR}/ynet/include
    ${PROJECT_SOURCE_DIR}/yfs/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    )
link_directories(${PROJECT_BINARY_DIR}/lib)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/sbin)
set(LIBRARY_OUTPUT_PATH    ${PROJECT_BINARY_DIR}/lib)

set(MOD_EXTRA_LIBS pthread aio crypt uuid ssl parser ylib ynet yfs)

set(MOD_SRCS
	mds_inode.c
	namei.c
	main.c
	mds.c
	mds_events.c
	mds_state_machine.c
	mirror.c
	dir.c
	disk.c
	super.c
	chunk.c
	img.c
	unlink.c
	diskpool.c
	nodepool.c
	lvs.c
	scan.c
        snapshot.c
	mds_newrep1.c)

add_executable(yfs_mds ${MOD_SRCS})
target_link_libraries(yfs_mds ${MOD_EXTRA_LIBS})

install(TARGETS yfs_mds
    RUNTIME DESTINATION app/sbin)
