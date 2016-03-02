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

if (LIBSSH_LIBRARIES AND LIBSSH_INCLUDE_DIRS)
  # in cache already
  set(LIBSSH_FOUND TRUE)
else (LIBSSH_LIBRARIES AND LIBSSH_INCLUDE_DIRS)

  find_path(LIBSSH_INCLUDE_DIR
    NAMES
      libssh/libssh.h
    PATHS
      /usr/include
      /usr/local/include
      /opt/local/include
      /sw/include
      ${CMAKE_INCLUDE_PATH}
      ${CMAKE_INSTALL_PREFIX}/include
  )
  
  find_library(SSH_LIBRARY
    NAMES
      libssh
    PATHS
      /usr/lib
      /usr/local/lib
      /opt/local/lib
      /sw/lib
      ${CMAKE_LIBRARY_PATH}
      ${CMAKE_INSTALL_PREFIX}/lib
      ENV LIBRARY_PATH
      ENV LD_LIBRARY_PATH
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

endif (LIBSSH_LIBRARIES AND LIBSSH_INCLUDE_DIRS)

