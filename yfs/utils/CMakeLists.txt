include_directories(
    ${PROJECT_SOURCE_DIR}/parser/include
    ${PROJECT_SOURCE_DIR}/ylib/include
    ${PROJECT_SOURCE_DIR}/ynet/include
    ${PROJECT_SOURCE_DIR}/yfs/include
    )
link_directories(${PROJECT_BINARY_DIR}/lib)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH    ${PROJECT_BINARY_DIR}/lib)

set(MOD_EXTRA_LIBS pthread aio crypt uuid ssl parser ylib ynet yfs)
set(UTILS
	ychkstat
	ychmod
	ylogin
	yls
	ymkdir
	yping
	ypasswd
	yrep
	yrm
	yrmdir
	ystat
	ystatvfs
	yshutdown
	jnlfind
	yrecycle
	ylvm
	ychunk_merge
	ysetopt
	ychunk_check
	repcheck
	rebuild_cdsjnl
	cmd
	ytruncate
	cdsop_read)

# ffc

foreach(x ${UTILS})
    message(STATUS "Util " ${x})
    add_executable(${x} ${x}.c)
    target_link_libraries(${x} ${MOD_EXTRA_LIBS})
endforeach()

install(TARGETS ${UTILS}
    RUNTIME DESTINATION app/bin)
