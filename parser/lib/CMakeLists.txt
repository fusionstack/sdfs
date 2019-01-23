find_package(FLEX)
find_package(BISON)

include_directories(
    ${PROJECT_SOURCE_DIR}/parser/include
    ${PROJECT_SOURCE_DIR}/ylib/include
    )

message(STATUS "CMAKE_CURRENT_SOURCE_DIR" ${CMAKE_CURRENT_SOURCE_DIR})
message(STATUS "CMAKE_CURRENT_BINARY_DIR" ${CMAKE_CURRENT_BINARY_DIR})

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/confl.c
    COMMAND flex -o${CMAKE_CURRENT_SOURCE_DIR}/confl.c ${CMAKE_CURRENT_SOURCE_DIR}/confl.l
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/confl.l
    )

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/confy.c ${CMAKE_CURRENT_SOURCE_DIR}/confy.h
    COMMAND bison -o ${CMAKE_CURRENT_SOURCE_DIR}/confy.c ${CMAKE_CURRENT_SOURCE_DIR}/confy.y
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/confy.y
    )

set(MOD_SRCS
    confl.c
    confy.c
    configure.c)

# add_library(parser ${MOD_SRCS})

add_library(parser ${MOD_SRCS})
set_target_properties(parser PROPERTIES OUTPUT_NAME "parser")
set_target_properties(parser PROPERTIES VERSION 1.0 SOVERSION 1)

install(TARGETS parser
    ARCHIVE DESTINATION app/lib
    LIBRARY DESTINATION app/lib
    RUNTIME DESTINATION app/sbin
    )
