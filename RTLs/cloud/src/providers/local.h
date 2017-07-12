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

#ifndef _INCLUDE_LOCAL_H_
#define _INCLUDE_LOCAL_H_

#include "provider.h"

CloudProvider *createLocalProvider(SparkInfo &sparkInfo);

class LocalProvider : public CloudProvider  {
public:
  LocalProvider(SparkInfo resources) : CloudProvider(resources) {}
  ~LocalProvider();

  std::string get_cloud_path(std::string filename);

  int32_t parse_config(INIReader *reader);
  int32_t init_device();
  int32_t send_file(std::string filename, std::string tgtfilename);
  int32_t get_file(std::string host_filename, std::string filename);
  void *data_alloc(int64_t size, int32_t type, int32_t id);
  int32_t delete_file(std::string filename);
  int32_t submit_job();
  std::string get_job_args();
};

#endif
