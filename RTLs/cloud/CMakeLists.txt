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

if((HDFS_FOUND) AND LIBSSH_FOUND)

    message("Building cloud target RTL")

    find_package(ZLIB REQUIRED)

    include_directories(
        inih  util
        gzip_cpp/include
        ${ZLIB_INCLUDE_DIRS}
        ${HDFS_INCLUDE_DIR}
        ${LIBELF_INCLUDE_DIRS}
        ${LIBSSH_INCLUDE_DIRS}
    )

    add_library(inih SHARED inih/ini.c inih/INIReader.cpp)

    add_library(gzip_cpp SHARED gzip_cpp/src/gzip_cpp.cc gzip_cpp/include/gzip_cpp.h)

    add_library(
        omptarget.rtl.cloud SHARED
        src/rtl.cpp
        src/providers/amazon.cpp
        src/providers/generic.cpp
        src/providers/google.cpp
        src/util/ssh.cpp
    )

    target_link_libraries(omptarget.rtl.cloud
      inih
      gzip_cpp
      ${HDFS_LIBRARIES}
      ${LIBELF_LIBRARIES}
      ${LIBSSH_LIBRARIES}
      ${ZLIB_LIBRARIES}
    )

else()

    if(NOT HDFS_FOUND)
        message("LibHDFS not found in system:")
    endif()
    if(NOT LIBSSH_FOUND)
        message("LibSSH not found in system:")
    endif()
    message("not building cloud RTL")

endif()
