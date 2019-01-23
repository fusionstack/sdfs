include_directories(
    ${PROJECT_SOURCE_DIR}/parser/include
    ${PROJECT_SOURCE_DIR}/ylib/include
    ${PROJECT_SOURCE_DIR}/ynet/include
    ${PROJECT_SOURCE_DIR}/yfs/include
    ${PROJECT_SOURCE_DIR}/yweb/include
    )
link_directories(${PROJECT_BINARY_DIR}/lib /sysy/yfs/app/lib)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/sbin)
set(LIBRARY_OUTPUT_PATH    ${PROJECT_BINARY_DIR}/lib)

set(MOD_EXTRA_LIBS pthread aio crypt uuid ssl parser ylib ynet yfs)

set(MOD_SRCS
	mime.c
	str.c
	match.c
	tdate_parse.c
	request.c
	response.c
	http_ops.c
	http_proto.c
	http_readdir.c
	main.c
    http_state_machine.c
    )

# add_custom_command(mime.c DEPENDS mime_encodings.h mime_types.h)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/mime_encodings.h
    COMMAND rm -f ${CMAKE_CURRENT_SOURCE_DIR}/mime_encodings.h
    COMMAND touch ${CMAKE_CURRENT_SOURCE_DIR}/mime_encodings.h
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/mime_encodings.txt
    )

    # COMMAND sed < mime_encodings.txt > mime_encodings.h -e 's/#.*//' -e 's/[ 	]*$$//' -e '/^$$/d' -e 's/[ 	][ 	]*/", 0, "/' -e 's/^/{ "/' -e 's/$$/", 0 },/'
    # COMMAND sed < mime_types.txt > mime_types.h -e 's/#.*//' -e 's/[ 	]*$$//' -e '/^$$/d' -e 's/[ 	][ 	]*/", 0, "/' -e 's/^/{ "/' -e 's/$$/", 0 },/'
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/mime_types.h
    COMMAND rm -f ${CMAKE_CURRENT_SOURCE_DIR}/mime_types.h
    COMMAND touch ${CMAKE_CURRENT_SOURCE_DIR}/mime_types.h
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/mime_types.txt
    )

add_executable(yweb_server ${MOD_SRCS} mime_types.h mime_encodings.h)
target_link_libraries(yweb_server ${MOD_EXTRA_LIBS})

install(TARGETS yweb_server RUNTIME DESTINATION sbin)

