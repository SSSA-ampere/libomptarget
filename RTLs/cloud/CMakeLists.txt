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

set(tmachine "x86_64")
set(tmachine_name "x86_64")
set(tmachine_libname "x86_64")


if(TRUE)

    message("Building cloud target RTL")

    find_package(HDFS3 REQUIRED)

    include_directories(${HDFS3_INCLUDE_DIR})

    add_library(omptarget.rtl.cloud src/rtl.cpp)
    target_link_libraries(omptarget.rtl.cloud)

else()

    message("Cloud infrastructure not found in system: not building cloud RTL")

endif()
