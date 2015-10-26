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
find_package(CURL QUIET)

if(CURL_FOUND AND HDFS3_FOUND)

    message("Building cloud target RTL")

    include_directories(inih rapidjson restclient ${CURL_INCLUDE_DIRS} ${HDFS3_INCLUDE_DIR})

    add_library(inih SHARED inih/ini.c inih/INIReader.cpp)
    add_library(restclient SHARED restclient/restclient.cpp)

    add_library(omptarget.rtl.cloud SHARED src/rtl.cpp)
    target_link_libraries(omptarget.rtl.cloud
      inih
      restclient
      ${HDFS3_LIBRARY}
      ${LIBELF_LIBRARIES}
      ${CURL_LIBRARIES}
    )

else()

    message("LibHDFS3 and/or Curl not found in system: not building cloud RTL")

endif()


