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
find_package(LibSSH QUIET)


if(CURL_FOUND AND HDFS3_FOUND AND LIBSSH_FOUND)

    message("Building cloud target RTL")

    include_directories(inih rapidjson restclient ${CURL_INCLUDE_DIR} ${HDFS3_INCLUDE_DIR})

    add_library(inih SHARED inih/ini.c inih/INIReader.cpp)

    if(APPLE)
        add_library(restclient restclient/restclient.cpp)
    else()
        add_library(restclient SHARED restclient/restclient.cpp)
    endif()

    add_library(omptarget.rtl.cloud SHARED src/rtl.cpp src/providers/amazon.cpp src/providers/generic.cpp src/providers/google.cpp)
    target_link_libraries(omptarget.rtl.cloud
      inih
      restclient
      ${HDFS3_LIBRARY}
      ${LIBELF_LIBRARIES}
      ${CURL_LIBRARIES}
      ${LIBSSH_LIBRARIES}
    )

else()

    if(NOT CURL_FOUND)
        message("Curl not found in system:")
    endif()
    if(NOT HDFS3_FOUND)
        message("LibHDFS3 not found in system:")
    endif()
    if(NOT LIBSSH_FOUND)
        message("LibSSH not found in system:")
    endif()
    message("not building cloud RTL")

endif()
