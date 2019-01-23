include_directories(
    ${PROJECT_SOURCE_DIR}/parser/include
    ${PROJECT_SOURCE_DIR}/ylib/include
    ${PROJECT_SOURCE_DIR}/ynet/include
    ${PROJECT_SOURCE_DIR}/yfs/include
    ${PROJECT_SOURCE_DIR}/yftp/include
    )
link_directories(${PROJECT_BINARY_DIR}/lib)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/sbin)
set(LIBRARY_OUTPUT_PATH    ${PROJECT_BINARY_DIR}/lib)

set(MOD_EXTRA_LIBS pthread aio crypt uuid ssl parser ylib ynet yfs)

set(MOD_SRCS
	main.c
    ftpfeatures.c
    prelogin.c
    postlogin.c
    cmdio.c
    dataio.c
    session.c)

add_executable(yftp_server ${MOD_SRCS})
target_link_libraries(yftp_server ${MOD_EXTRA_LIBS})

install(TARGETS yftp_server
    RUNTIME DESTINATION app/sbin)
