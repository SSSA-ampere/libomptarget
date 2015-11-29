#ifndef _INCLUDE_GENERIC_H_
#define _INCLUDE_GENERIC_H_

#include "INIReader.h"
#include "../rtl.h"

class GenericProvider {
  protected:
    hdfsFS fs;
    HdfsInfo hdfs;
    SparkInfo spark;
    int32_t currAddr;

    int32_t execute_command(const char *command, bool print_result);
    int32_t submit_cluster();
    int32_t submit_local();
  public:
    GenericProvider(ResourceInfo resources) {
      fs = resources.FS;
      hdfs = resources.HDFSInfo;
      spark = resources.Spark;
      currAddr = 1;
    }

    virtual int32_t parse_config(INIReader reader);
    virtual int32_t send_file(const char *filename, const char *tgtfilename);
    virtual void *data_alloc(int64_t size, int32_t type, int32_t id);
    virtual int32_t data_submit(void *tgt_ptr, void *hst_ptr, int64_t size, int32_t id);
    virtual int32_t data_retrieve(void *hst_ptr, void *tgt_ptr, int64_t size, int32_t id);
    virtual int32_t data_delete(void *tgt_ptr, int32_t id);
    virtual int32_t submit_job();
};

#endif
