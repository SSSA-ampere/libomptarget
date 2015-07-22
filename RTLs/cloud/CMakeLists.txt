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

if(HDFS3_FOUND)

    message("Building cloud target RTL")

    include_directories(${HDFS3_INCLUDE_DIR})

    add_library(omptarget.rtl.cloud SHARED src/rtl.cpp)
    target_link_libraries(omptarget.rtl.cloud ${HDFS3_LIBRARY}
      ${LIBELF_LIBRARIES}
    )

else()

    message("LibHDFS3 not found in system: not building cloud RTL")

endif()
