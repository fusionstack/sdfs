macro(get_version vfile)
    message(STATUS "get version now ...")
    execute_process(
        COMMAND git show
        COMMAND grep -i ^commit
        COMMAND awk "{print $2}"
        OUTPUT_VARIABLE YFS_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(
        COMMAND git show
        COMMAND grep ^Date:
        OUTPUT_VARIABLE YFS_DATE OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(
        COMMAND git branch
        COMMAND grep ^\\*
        COMMAND awk "{print $2}"
        OUTPUT_VARIABLE YFS_BRANCH OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(
        COMMAND uname -ir
        OUTPUT_VARIABLE YFS_SYSTEM OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(
        COMMAND /lib/libc.so.6
        COMMAND head -n 1
        OUTPUT_VARIABLE YFS_GLIBC OUTPUT_STRIP_TRAILING_WHITESPACE)

    file(WRITE  ${vfile} "#ifndef __GET_VERSION_H__\n")
    file(APPEND ${vfile} "#define __GET_VERSION_H__\n")
    file(APPEND ${vfile} "\n")
    file(APPEND ${vfile} "#include <stdio.h>\n")
    file(APPEND ${vfile} "\n")
    file(APPEND ${vfile} "#define YVERSION \\\n")
    file(APPEND ${vfile} "\"Version: ${YFS_VERSION} \\\n")
    file(APPEND ${vfile} "\\n${YFS_DATE} \\\n")
    file(APPEND ${vfile} "\\nBranch: ${YFS_BRANCH} \\\n")
    file(APPEND ${vfile} "\\nSystem: ${YFS_SYSTEM} \\\n")
    file(APPEND ${vfile} "\\nGlibc: ${YFS_GLIBC} \"\n")
    file(APPEND ${vfile} "\n")
    file(APPEND ${vfile} "\n")
    file(APPEND ${vfile} "#define get_version() \\\n")
    file(APPEND ${vfile} "do { \\\n")
    file(APPEND ${vfile} "\tfprintf(stderr, \"%s\\n\", YVERSION); \\\n")
    file(APPEND ${vfile} "} while(0)\n")
    file(APPEND ${vfile} "\n")
    file(APPEND ${vfile} "#endif /* __GET_VERSION_H__ */\n")
endmacro(get_version)

# message(STATUS "CMAKE_SOURCE_DIR " ${CMAKE_SOURCE_DIR})
set(VERFILE get_version.h)
get_version(${VERFILE})
