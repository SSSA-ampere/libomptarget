//===-------- amazon.h----- - Information about Target RTLs ------ C++ -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// Interface for amazon provider of Cloud RTL
//
//===----------------------------------------------------------------------===//

#ifndef _INCLUDE_AMAZON_H_
#define _INCLUDE_AMAZON_H_

#include "generic.h"
#include "rtl.h"

struct AmazonInfo {
  std::string Bucket;
  std::string Cluster;
  std::string AccessKey;
  std::string SecretKey;
  std::string KeyFile;
  std::string AdditionalArgs;
};

const std::string DEFAULT_AWS_BUCKET = "ompcloud-bucket";

CloudProvider *createAmazonProvider(ResourceInfo &resources);

class AmazonProvider : public GenericProvider {
private:
  AmazonInfo ainfo;

public:
  AmazonProvider(ResourceInfo resources) : GenericProvider(resources) {}

  std::string get_keys();
  std::string get_cloud_path(std::string filename);

  virtual int32_t parse_config(INIReader reader);
  virtual int32_t init_device();
  virtual int32_t send_file(std::string filename, std::string tgtfilename);
  virtual int32_t get_file(std::string host_filename, std::string filename);
  virtual int32_t delete_file(std::string filename);
  virtual int32_t submit_job();
  virtual std::string get_job_args();
};

#endif
