#.rst:
# FindSBT
# -----------
#
# this module looks for sbt
#
# Once done this will define
#
# ::
#
#   SBT_FOUND - system has sbt
#   SBT_EXECUTABLE - the sbt executable
#

set(SBT_SEARCH_PATHS
    ~/Library/Frameworks
    /Library/Frameworks
    /usr/local
    /usr
    /sw # Fink
    /opt/local # DarwinPorts
    /opt/csw # Blastwave
    /opt
)

find_program(SBT_EXECUTABLE
  NAMES
  sbt
  HINTS
  $ENV{SBTDIR}
  PATH_SUFFIXES bin
  PATHS ${SBT_SEARCH_PATHS}
)

# handle the QUIETLY and REQUIRED arguments and set SBT_FOUND to TRUE if
# all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(SBT REQUIRED_VARS SBT_EXECUTABLE)

mark_as_advanced( SBT_EXECUTABLE )

