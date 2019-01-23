include_directories(
    ${PROJECT_SOURCE_DIR}/parser/include
    ${PROJECT_SOURCE_DIR}/ylib/include
    ${PROJECT_SOURCE_DIR}/ynet/include
    ${PROJECT_SOURCE_DIR}/yfs/include
    ${PROJECT_SOURCE_DIR}/proxy/include
    ${PROJECT_SOURCE_DIR}/itest/include
    )
link_directories(${PROJECT_BINARY_DIR}/lib)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/itest/bin)
set(LIBRARY_OUTPUT_PATH    ${PROJECT_BINARY_DIR}/lib)

set(MOD_EXTRA_LIBS pthread aio crypt uuid ssl readline
    yfsproxy parser ylib ynet yfs)
# set(UTILS
# 	ychkstat
# 	ychmod
# 	ylogin
# 	yls
# 	ymkdir
# 	yping
# 	ypasswd
# 	yrep
# 	yrm
# 	yrmdir
# 	ystat
# 	ystatvfs
# 	yuser
# 	yshutdown
# 	jnlfind
# 	yrecycle
# 	recycle
# 	ylvm
# 	ychunk_merge
# 	ysetopt
# 	ychunk_check
# 	repcheck
# 	rebuild_cdsjnl
# 	ffc
# 	cmd
# 	ytruncate
# 	cdsop_read)

foreach(x ${UTILS})
    message(STATUS "Util " ${x})
    add_executable(${x} ${x}.c)
    target_link_libraries(${x} gtest ${MOD_EXTRA_LIBS})
endforeach()

install(TARGETS ${UTILS}
    RUNTIME DESTINATION app/bin)

set(SHELLSRC yshell.cc)
add_executable(yshell ${SHELLSRC})
target_link_libraries(yshell ${MOD_EXTRA_LIBS})

install(TARGETS yshell
    RUNTIME DESTINATION app/bin)
