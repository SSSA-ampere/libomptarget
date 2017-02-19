//===-------------------- Target RTLs Implementation -------------- C++ -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
//  Compression function
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <cstdlib>
#include <string>
#include <iostream>

#include <zlib.h>

#include "cloud_compression.h"

size_t decompress_file(std::string comp_file, char *ptr_buff_out,
                       size_t buff_size) {
  int err;
  gzFile in = gzopen(comp_file.c_str(), "rb");
  int len = gzread(in, ptr_buff_out, buff_size);
  if (len < 0)
    fprintf(stderr, "Failed to decompress: %s", gzerror(in, &err));
  gzclose(in);
  return len;
}

size_t compress_to_file(std::string comp_file, char *ptr_buff_in,
                        size_t buff_size) {
  int err;
  gzFile out = gzopen(comp_file.c_str(), "wb");
  int len = gzwrite(out, ptr_buff_in, buff_size);
  if (len < 0)
    fprintf(stderr, "Failed to compress: %s", gzerror(out, &err));
  gzclose(out);
  return len;
}
