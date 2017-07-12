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

#include "provider.h"

struct HdfsInfo {
  std::string ServAddress;
  int ServPort;
  std::string UserName;
};

const int DEFAULT_HDFS_PORT = 9000;
const int BUFF_SIZE = 4096;

CloudProvider *createGenericProvider(SparkInfo &sparkInfo);

class GenericProvider : public CloudProvider {
protected:
  void* fs;
  HdfsInfo hinfo;

  int32_t submit_cluster();
  int32_t submit_local();

public:
  GenericProvider(SparkInfo sparkInfo) : CloudProvider(sparkInfo) {}

  virtual int32_t parse_config(INIReader *reader);
  virtual int32_t init_device();
  virtual int32_t send_file(std::string filename, std::string tgtfilename);
  virtual int32_t get_file(std::string host_filename, std::string filename);
  virtual int32_t delete_file(std::string filename);
  virtual int32_t submit_job();
  virtual std::string get_job_args();
};

#endif
