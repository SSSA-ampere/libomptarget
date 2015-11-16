#ifndef _INCLUDE_GENERIC_H_
#define _INCLUDE_GENERIC_H_

#include "../rtl.h"

class GenericProvider {
  private:
    hdfsFS fs;
    HdfsInfo hdfs;
    SparkInfo spark;
    int32_t currAddr;
  public:
    GenericProvider(ResourceInfo resources) {
      fs = resources.FS;
      hdfs = resources.HDFSInfo;
      spark = resources.Spark;
      currAddr = 1;
    }

    int32_t send_file(const char *filename, const char *tgtfilename);
    void *data_alloc(int64_t size, int32_t type, int32_t id);
    int32_t data_submit(void *tgt_ptr, void *hst_ptr, int64_t size, int32_t id);
    int32_t data_retrieve(void *hst_ptr, void *tgt_ptr, int64_t size, int32_t id);
    int32_t data_delete(void *tgt_ptr, int32_t id);
    int32_t submit_job();
};

#endif
