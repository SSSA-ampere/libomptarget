class GenericProvider {
  private:
    hdfsFS fs;
    HdfsInfo hdfs;
    SparkInfo spark;
    int32_t currAddr;
  public:
    GenericProvider(ResourceInfo resources) {
      fs = resources.FS;
      info = resources.FSInfo;
      currAddr = 0;
    }

    int32_t send_file(const char *filename, const char *tgtfilename);
    void *data_alloc(int64_t size, int32_t type, int32_t id);
    int32_t data_submit(void *tgt_ptr, void *hst_ptr, int64_t size, int32_t id);
    int32_t data_retrieve(void *hst_ptr, void *tgt_ptr, int64_t size, int32_t id);
    int32_t data_delete(void *tgt_ptr, int32_t id);
    int32_t submit_job();
};
