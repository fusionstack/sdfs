include_directories(
    ${PROJECT_SOURCE_DIR}/parser/include
    ${PROJECT_SOURCE_DIR}/ylib/include
    ${PROJECT_SOURCE_DIR}/ynet/include
    ${PROJECT_SOURCE_DIR}/yfs/include
    # ${PROJECT_SOURCE_DIR}/itest/gtest/include
    )
link_directories(${PROJECT_BINARY_DIR}/lib
    # ${PROJECT_SOURCE_DIR}/itest/gtest/build
    )

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/itest/bin)
set(LIBRARY_OUTPUT_PATH    ${PROJECT_BINARY_DIR}/lib)

set(EXTRA_LIBS uuid crypt pthread aio ssl parser ylib ynet yfs)

set(TESTS
    test_bitmap
    test_skiplist
    )

foreach(t ${TESTS})
    message(STATUS "Test " ${t})
    add_executable(${t} ${t}.c)
    target_link_libraries(${t} ${EXTRA_LIBS})
endforeach()

# gtest
set(GTESTS
    itest_ylib)

foreach(t ${GTESTS})
    message(STATUS "Test " ${t})
    add_executable(${t} ${t}.cc)
    target_link_libraries(${t} gtest ${EXTRA_LIBS})
endforeach()

install(TARGETS ${TESTS}
    RUNTIME DESTINATION app/itest)
