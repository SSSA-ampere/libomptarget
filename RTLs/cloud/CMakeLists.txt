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



find_package(HDFS3 QUIET)
find_package(LibSSH QUIET)

if(HDFS3_FOUND AND LIBSSH_FOUND)

    message("Building cloud target RTL")

    include_directories(inih  util ${HDFS3_INCLUDE_DIR} ${LIBELF_INCLUDE_DIRS} ${LIBSSH_INCLUDE_DIRS})

    add_library(inih SHARED inih/ini.c inih/INIReader.cpp)

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
      ${HDFS3_LIBRARY}
      ${LIBELF_LIBRARIES}
      ${LIBSSH_LIBRARIES}
    )

else()

    if(NOT HDFS3_FOUND)
        message("LibHDFS3 not found in system:")
    endif()
    if(NOT LIBSSH_FOUND)
        message("LibSSH not found in system:")
    endif()
    message("not building cloud RTL")

endif()
