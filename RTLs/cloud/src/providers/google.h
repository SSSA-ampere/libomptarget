#ifndef _INCLUDE_GOOGLE_H_
#define _INCLUDE_GOOGLE_H_

#include "../rtl.h"
#include "INIReader.h"
#include "generic.h"

struct GoogleInfo {
  std::string Bucket;
  std::string Cluster;
};

class GoogleProvider : public GenericProvider {
private:
  GoogleInfo ginfo;

public:
  GoogleProvider(ResourceInfo resources) : GenericProvider(resources) {}

  int32_t parse_config(INIReader reader);
  int32_t send_file(const char *filename, const char *tgtfilename);
  int32_t data_submit(void *tgt_ptr, void *hst_ptr, int64_t size, int32_t id);
  int32_t data_retrieve(void *hst_ptr, void *tgt_ptr, int64_t size, int32_t id);
  int32_t data_delete(void *tgt_ptr, int32_t id);
  int32_t submit_job();
};

#endif
