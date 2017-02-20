//===-------------------- Target RTLs Implementation -------------- C++ -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// RTL for Apache Spark cloud cluster
//
//===----------------------------------------------------------------------===//

#include <array>
#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include "cloud_util.h"


int32_t execute_command(const char *command, bool print_result) {
  FILE *fp = popen(command, "r");

  if (fp == NULL) {
    fprintf(stderr, "Failed to execute command.\n");
    return EXIT_FAILURE;
  }

  if (print_result) {
    char buf[512] = {0};
    uint read = 0;

    while ((read = fread(buf, sizeof(char), 511, fp)) == 512) {
      buf[511] = 0;
      fprintf(stdout, "%s", buf);
    }

    buf[read] = 0;
    fprintf(stdout, "%s", buf);
  }

  return pclose(fp);
}
