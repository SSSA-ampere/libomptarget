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

#include "../rtl.h"
#include "INIReader.h"

#include "../util/ssh.h"
#include "generic.h"

struct AmazonInfo {
  std::string Bucket;
  std::string Cluster;
  std::string AccessKey;
  std::string SecretKey;
  std::string KeyFile;
  std::string AdditionalArgs;
};

const std::string DEFAULT_AWS_BUCKET = "ompcloud-bucket";

GenericProvider *createAmazonProvider(ResourceInfo &resources);

class AmazonProvider : public GenericProvider {
private:
  AmazonInfo ainfo;

public:
  AmazonProvider(ResourceInfo resources) : GenericProvider(resources) {}

  std::string get_keys();
  std::string get_cloud_path(std::string filename);

  virtual int32_t parse_config(INIReader reader);
  virtual int32_t init_device();
  virtual int32_t send_file(const char *filename, const char *tgtfilename);
  virtual int32_t data_submit(void *data_ptr, int64_t size, std::string name);
  virtual int32_t data_retrieve(void *data_ptr, int64_t size, std::string name);
  virtual int32_t data_delete(void *tgt_ptr, int32_t id);
  virtual int32_t submit_job();
};

#endif
