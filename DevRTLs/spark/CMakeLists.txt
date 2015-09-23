##===----------------------------------------------------------------------===##
# 
#                     The LLVM Compiler Infrastructure
#
# This file is distributed under the University of Illinois Open Source
# License. See LICENSE.TXT for details.
# 
##===----------------------------------------------------------------------===##
#
# Build the Cloud (Spark) Device RTL if the Spark tools are available
#
##===----------------------------------------------------------------------===##

find_package(SBT)

if(SBT_FOUND)

    message("Building Spark device RTL")

    add_custom_target(omptarget-spark ALL
        COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR} && ${SBT_EXECUTABLE} publish-local)

else()

    message("sbt not found in system: not building Spark device RTL")

endif()
