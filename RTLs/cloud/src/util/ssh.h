//===-------- amazon.h----- - Information about Target RTLs ------ C++ -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// Util function
//
//===----------------------------------------------------------------------===//

#ifndef _INCLUDE_UTIL_H_
#define _INCLUDE_UTIL_H_

#include <libssh/libssh.h>

#include "omptarget.h"

int ssh_verify_knownhost(ssh_session session);

int ssh_copy(ssh_session session, const char *filename, const char *destpath,
             const char *destname);

int ssh_run(ssh_session session, const char *cmd);

#endif
