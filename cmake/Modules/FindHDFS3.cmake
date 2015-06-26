# Locate libHDFS3 library
# This module defines
# HDFS3_LIBRARY, the name of the library to link against
# HDFS3_FOUND, if false, do not try to link to HDFS3
# HDFS3_INCLUDE_DIR, where to find header files.
#
# Additional Note: If you see an empty HDFS3_LIBRARY_TEMP in your configuration
# and no HDFS3_LIBRARY, it means CMake did not find your HDFS3 library
# (hdfs3.dll, libhdfs3.so, etc).
# Set HDFS3_LIBRARY_TEMP to point to your HDFS3 library, and configure again.
# These values are used to generate the final HDFS3_LIBRARY
# variable, but when these values are unset, HDFS3_LIBRARY does not get created.
#
# $HDFS3DIR is an environment variable that would correspond to a custom
# installation directory used in building HDFS3.
#
# On OSX, this will prefer the Framework version (if found) over others.
# People will have to manually change the cache values of
# HDFS3_LIBRARY to override this selection or set the CMake environment
# CMAKE_INCLUDE_PATH to modify the search paths.

SET(HDFS3_SEARCH_PATHS
    ~/Library/Frameworks
    /Library/Frameworks
    /usr/local
    /usr
    /sw # Fink
    /opt/local # DarwinPorts
    /opt/csw # Blastwave
    /opt
)

FIND_PATH(HDFS3_INCLUDE_DIR hdfs.h
    HINTS
    $ENV{HDFS3DIR}
    PATH_SUFFIXES include/hdfs include
    PATHS ${HDFS3_SEARCH_PATHS}
)

FIND_LIBRARY(HDFS3_LIBRARY_TEMP
    NAMES hdfs3
    HINTS
    $ENV{HDFS3DIR}
    PATH_SUFFIXES lib64 lib
    PATHS ${HDFS3_SEARCH_PATHS}
)

# MinGW needs an additional library mwindows
IF(MINGW)
        SET(MINGW32_LIBRARY mingw32 CACHE STRING "mwindows for MinGW")
ENDIF(MINGW)

IF(HDFS3_LIBRARY_TEMP)
    # For MinGW library
    IF(MINGW)
            SET(HDFS3_LIBRARY_TEMP ${MINGW32_LIBRARY} ${HDFS3_LIBRARY_TEMP})
    ENDIF(MINGW)

    # Set the final string here so the GUI reflects the final state.
    SET(HDFS3_LIBRARY ${HDFS3_LIBRARY_TEMP} CACHE STRING "Where the HDFS3 Library can be found")
    # Set the temp variable to INTERNAL so it is not seen in the CMake GUI
    SET(HDFS3_LIBRARY_TEMP "${HDFS3_LIBRARY_TEMP}" CACHE INTERNAL "")
ENDIF(HDFS3_LIBRARY_TEMP)

INCLUDE(FindPackageHandleStandardArgs)

FIND_PACKAGE_HANDLE_STANDARD_ARGS(HDFS3 REQUIRED_VARS HDFS3_LIBRARY HDFS3_INCLUDE_DIR)
