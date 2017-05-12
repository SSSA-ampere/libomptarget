##===----------------------------------------------------------------------===##
#
#                     The LLVM Compiler Infrastructure
#
# This file is distributed under the University of Illinois Open Source
# License. See LICENSE.TXT for details.
#
##===----------------------------------------------------------------------===##
#
# Build an RTL for a x86_64 machine if available
#
##===----------------------------------------------------------------------===##



#find_package(HDFS QUIET)
if(NOT HDFS_FOUND)
    find_package(HDFS3 QUIET)
    set(HDFS_FOUND HDFS3_FOUND)
    set(HDFS_INCLUDE_DIR ${HDFS3_INCLUDE_DIR})
    set(HDFS_LIBRARIES ${HDFS3_LIBRARY})
endif()

find_package(LibSSH QUIET)
find_package(ZLIB REQUIRED)

if(HDFS_FOUND AND LIBSSH_FOUND AND ZLIB_FOUND)

    message("Building cloud target RTL")

    include_directories(
        src/
        src/providers
        src/util
        inih
        ${HDFS_INCLUDE_DIR}
        ${LIBELF_INCLUDE_DIRS}
        ${LIBSSH_INCLUDE_DIRS}
        ${ZLIB_INCLUDE_DIRS}
    )

    add_library(inih inih/ini.c inih/INIReader.cpp)

    add_library(
        omptarget.rtl.cloud
        src/rtl.cpp
        src/providers/amazon.cpp
        src/providers/azure.cpp
        src/providers/generic.cpp
        src/util/cloud_ssh.cpp
        src/util/cloud_compression.cpp
        src/util/cloud_util.cpp
    )

    target_link_libraries(omptarget.rtl.cloud
      inih
      ${HDFS_LIBRARIES}
      ${LIBELF_LIBRARIES}
      ${LIBSSH_LIBRARIES}
      ${ZLIB_LIBRARIES}
    )

else()

    if(NOT HDFS_FOUND)
        message("LibHDFS not found in the system")
    endif()
    if(NOT LIBSSH_FOUND)
        message("LibSSH not found in the system")
    endif()
    if(NOT ZLIB_FOUND)
        message("Zlib not found in the system")
    endif()
    message("not building cloud RTL")

endif()
