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

#include "../rtl.h"
#include "INIReader.h"


GenericProvider *createGenericProvider(ResourceInfo &resources);

class GenericProvider {
protected:
  hdfsFS fs;
  HdfsInfo hdfs;
  SparkInfo spark;
  int32_t currAddr;

  int32_t execute_command(const char *command, bool print_result);
  int32_t submit_cluster();
  int32_t submit_local();
  int32_t submit_condor();

public:
  GenericProvider(ResourceInfo &resources) {
    hdfs = resources.HDFSInfo;
    spark = resources.Spark;
    currAddr = 1;
  }

  virtual int32_t parse_config(INIReader reader);
  virtual int32_t init_device();
  virtual int32_t send_file(const char *filename, const char *tgtfilename);
  virtual void *data_alloc(int64_t size, int32_t type, int32_t id);
  virtual int32_t data_submit(void *data_ptr, int64_t size, std::string name);
  virtual int32_t data_retrieve(void *data_ptr, int64_t size, std::string name);
  virtual int32_t data_delete(void *tgt_ptr, int32_t id);
  virtual int32_t submit_job();
  virtual std::string get_job_args();
};

#endif
