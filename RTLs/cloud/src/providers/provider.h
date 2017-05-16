//===------- generic.h----- - Information about Target RTLs ------ C++ -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// Interface for generic provider of Cloud RTL
//
//===----------------------------------------------------------------------===//

#ifndef _INCLUDE_PROVIDER_H_
#define _INCLUDE_PROVIDER_H_

#include "rtl.h"

const int BUFF_SIZE = 4096;

class CloudProvider {
protected:
  HdfsInfo hdfs;
  SparkInfo spark;
  char *currAddr;

public:
  CloudProvider(ResourceInfo &resources) {
    hdfs = resources.HDFSInfo;
    spark = resources.Spark;
    currAddr = (char *)1;
  }

  virtual int32_t parse_config(INIReader reader) = 0;
  virtual int32_t init_device() = 0;
  virtual int32_t send_file(std::string filename, std::string tgtfilename) = 0;
  virtual int32_t get_file(std::string host_filename, std::string filename) = 0;
  virtual void *data_alloc(int64_t size, int32_t type, int32_t id) = 0;
  virtual int32_t delete_file(std::string filename) = 0;
  virtual int32_t submit_job() = 0;
  virtual std::string get_job_args() = 0;
};

#endif
