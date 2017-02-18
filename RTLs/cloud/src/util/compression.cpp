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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

#include <fstream>
#include <hdfs.h>
#include <iostream>

#include "compression.h"

#include "omptarget.h"

#include "gzip_cpp.h"

int decompress_file(std::string comp_file, char *ptr) {
  // Reading contents of temporary file
  FILE *ftmp = fopen(comp_file.c_str(), "rb");

  if (!ftmp) {
    fprintf(stderr, "Could not open temporary file.\n");
    return OFFLOAD_FAIL;
  }

  struct stat stat_buf;
  int rc = stat(comp_file.c_str(), &stat_buf);
  if (rc != 0)
    return OFFLOAD_FAIL;

  size_t comp_size = stat_buf.st_size;
  gzip::Data data = gzip::AllocateData(comp_size);

  if (fread(data->ptr, 1, data->size, ftmp) != comp_size) {
    fprintf(stderr, "Could not successfully read temporary file. => %ld\n",
            comp_size);
    fclose(ftmp);
    return OFFLOAD_FAIL;
  }

  gzip::Decomp decomp;
  if (!decomp.IsSucc())
    return OFFLOAD_FAIL;

  bool succ;
  gzip::DataList out_data_list;
  std::tie(succ, out_data_list) = decomp.Process(data);
  gzip::Data decomp_data = gzip::ExpandDataList(out_data_list);

  int decomp_size = 0;
  for (gzip::Data data : out_data_list) {
    memcpy(ptr + decomp_size, data->ptr, data->size);
    decomp_size += data->size;
  }

  return decomp_size;
}

int compress_to_file(std::string comp_file, char *ptr, int size) {
  std::ofstream tmpfile(comp_file);
  if (!tmpfile.is_open()) {
    fprintf(stderr, "Failed to open temporary file\n");
    exit(OFFLOAD_FAIL);
  }
  gzip::Comp comp(gzip::Comp::Level::Default, true);
  if (!comp.IsSucc()) {
    fprintf(stderr, "Failed to create compressor\n");
    exit(OFFLOAD_FAIL);
  }

  for (gzip::Data data : comp.Process(ptr, size, true))
    tmpfile.write(data->ptr, data->size);
  return OFFLOAD_SUCCESS;
}
