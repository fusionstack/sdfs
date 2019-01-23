include_directories(
    ${PROJECT_SOURCE_DIR}/parser/include
    ${PROJECT_SOURCE_DIR}/ylib/include
    ${PROJECT_SOURCE_DIR}/ynet/include
    ${PROJECT_SOURCE_DIR}/yfs/include
    ${PROJECT_SOURCE_DIR}/ynfs/include
    )
link_directories(${PROJECT_BINARY_DIR}/lib /sysy/yfs/app/lib)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/sbin)
set(LIBRARY_OUTPUT_PATH    ${PROJECT_BINARY_DIR}/lib)

set(MOD_EXTRA_LIBS pthread aio crypt uuid ssl parser ylib ynet yfs)

set(MOD_SRCS
	main.c
    attr.c
    error.c
    readdir.c
    nfs_events.c
    mountlist.c
    nfs_state_machine.c
    xdr_nfs.c
    nfs_proc.c)

add_executable(ynfs_server ${MOD_SRCS})
target_link_libraries(ynfs_server ${MOD_EXTRA_LIBS})

install(TARGETS ynfs_server
    RUNTIME DESTINATION app/sbin)

