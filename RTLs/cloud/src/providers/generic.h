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

#ifndef _INCLUDE_GENERIC_H_
#define _INCLUDE_GENERIC_H_

#include "rtl.h"

const int BUFF_SIZE = 4096;

GenericProvider *createGenericProvider(ResourceInfo &resources);

class GenericProvider {
protected:
  hdfsFS fs;
  HdfsInfo hdfs;
  SparkInfo spark;
  char *currAddr;

  int32_t submit_cluster();
  int32_t submit_local();
  int32_t submit_condor();

public:
  GenericProvider(ResourceInfo &resources) {
    hdfs = resources.HDFSInfo;
    spark = resources.Spark;
    currAddr = (char *)1;
  }

  virtual int32_t parse_config(INIReader reader);
  virtual int32_t init_device();
  virtual int32_t send_file(std::string filename, std::string tgtfilename);
  virtual int32_t get_file(std::string host_filename, std::string filename);
  virtual void *data_alloc(int64_t size, int32_t type, int32_t id);
  virtual int32_t delete_file(std::string filename);
  virtual int32_t submit_job();
  virtual std::string get_job_args();
};

#endif
