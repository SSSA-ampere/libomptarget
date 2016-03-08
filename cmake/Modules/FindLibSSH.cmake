# - Try to find LibSSH
# Once done this will define
#
#  LIBSSH_FOUND - system has LibSSH
#  LIBSSH_INCLUDE_DIRS - the LibSSH include directory
#  LIBSSH_LIBRARIES - Link these to use LibSSH
#  LIBSSH_DEFINITIONS - Compiler switches required for using LibSSH
#
#  Copyright (c) 2009 Andreas Schneider <mail@cynapses.org>
#
#  Redistribution and use is allowed according to the terms of the New
#  BSD license.
#  For details see the accompanying COPYING-CMAKE-SCRIPTS file.
#


SET(LIBSSH_SEARCH_PATHS
    ~/Library/Frameworks
    /Library/Frameworks
    /usr/local
    /usr
    /sw # Fink
    /opt/local # DarwinPorts
    /opt/csw # Blastwave
    /opt
    ${CMAKE_INCLUDE_PATH}
    ${CMAKE_INSTALL_PREFIX}
    ENV LIBRARY_PATH
    ENV LD_LIBRARY_PATH
)

find_path(LIBSSH_INCLUDE_DIR
    NAMES
        libssh.h
    PATHS
        ${LIBSSH_SEARCH_PATHS}
    PATH_SUFFIXES
        include/libssh include
)

find_library(SSH_LIBRARY
    NAMES
        ssh
    PATHS
        ${LIBSSH_SEARCH_PATHS}
    PATH_SUFFIXES
        lib lib64
)


set(LIBSSH_INCLUDE_DIRS
    ${LIBSSH_INCLUDE_DIR}
)

set(LIBSSH_LIBRARIES
    ${LIBSSH_LIBRARIES}
    ${SSH_LIBRARY}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LibSSH DEFAULT_MSG LIBSSH_LIBRARIES LIBSSH_INCLUDE_DIRS)

# show the LIBSSH_INCLUDE_DIRS and LIBSSH_LIBRARIES variables only in the advanced view
mark_as_advanced(LIBSSH_INCLUDE_DIRS LIBSSH_LIBRARIES)


