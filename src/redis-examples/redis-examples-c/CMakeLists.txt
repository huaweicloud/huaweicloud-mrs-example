cmake_minimum_required(VERSION 3.11)

project(cKerberos)

set(CMAKE_CXX_STANDARD 11)

set(krb_source
        src/kerberos.c)

set(build_type "Release")
set(CMAKE_BUILD_TYPE ${build_type})

link_libraries("sasl2")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_DEBUG_POSTFIX d)

add_library(ckrb5 ${krb_source})

set(TEST_ENABLE true)

if (TEST_ENABLE STREQUAL "true")
    add_subdirectory(src/hiredis)
    set(SOURCE_FILES
            src/hiredis/hiredis.c
            src/hiredis/sds.c
            src/kerberos.c
            src/testMain.c)
    set(HIREDIS_SOURCE_FILES
            src/hiredis/alloc.c
            src/hiredis/async.c
            src/hiredis/dict.c
            src/hiredis/net.c
            src/hiredis/read.c
            src/hiredis/sds.c
            src/hiredis/hiredis.c
            src/hiredis/sockcompat.c)
    add_executable(kerberosTest ${HIREDIS_SOURCE_FILES} ${SOURCE_FILES})

endif ()

