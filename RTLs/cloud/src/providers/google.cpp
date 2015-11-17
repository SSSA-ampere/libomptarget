#include "generic.h"

class GoogleProvider : public GenericProvider {
  private:
    GoogleInfo ginfo;
  public:
    GoogleProvider(ResourceInfo resources) : GenericProvider(resources) {}

    int32_t send_file(const char *filename, const char *tgtfilename);
    int32_t data_submit(void *tgt_ptr, void *hst_ptr, int64_t size, int32_t id);
    int32_t data_retrieve(void *hst_ptr, void *tgt_ptr, int64_t size, int32_t id);
    int32_t data_delete(void *tgt_ptr, int32_t id);
    int32_t submit_job();
}

int32_t GoogleProvider::send_file(const char *filename, const char *tgtfilename) {
  std::string command = "gsutil cp ";

  command += std::string(filename);
  command += " ";
  command += ginfo.Bucket;
  command += hdfs.WorkingDir + std::string(tgtfilename);

  if (!execute_command(command, true)) {
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t GoogleProvider::data_submit(void *tgt_ptr, void *hst_ptr, int64_t size, int32_t id) {
  // Creating temporary file to hold data to submit
  char tmp_name[] = "/tmp/tmpfile_XXXXXX";
  int tmp_fd = mkstemp(tmp_name);

  if (tmp_fd == -1) {
    DP("Could not create temporary file.\n");
    return OFFLOAD_FAIL;
  }

  // Copying data to file
  FILE *ftmp = fdopen(tmp_fd, "wb");

  if (!ftmp) {
    DP("Could not open temporary file.\n");
    return OFFLOAD_FAIL;
  }

  if (fwrite(hst_ptr, 1, size, ftmp) != size) {
    DP("Could not successfully write to temporary file.\n");
    fclose(ftmp);
    return OFFLOAD_FAIL;
  }

  fclose(ftmp);

  // Submitting data to cloud
  std::string final_filename = std::to_string(id);
  return send_file(tmp_name, final_filename.c_str());
}

int32_t GoogleProvider::data_retrieve(void *hst_ptr, void *tgt_ptr, int64_t size, int32_t id) {
  // Creating temporary file to hold data retrieved
  char tmp_name[] = "/tmp/tmpfile_XXXXXX";
  int tmp_fd = mkstemp(tmp_name);

  if (tmp_fd == -1) {
    DP("Could not create temporary file.\n");
    return OFFLOAD_FAIL;
  }

  // Copying data from cloud
  std::string command = "gsutil cp ";

  command += ginfo.Bucket;
  command += hdfs.WorkingDir + std::string(id);
  command += " ";
  command += std::string(tmp_name);

  if (!execute_command(command, true)) {
    return OFFLOAD_FAIL;
  }

  // Reading contents of temporary file
  FILE *ftmp = fdopen(tmp_fd, "wb");

  if (!ftmp) {
    DP("Could not open temporary file.\n");
    return OFFLOAD_FAIL;
  }

  if (fread(hst_ptr, 1, size, ftmp) != size) {
    DP("Could not successfully read temporary file.\n");
    fclose(ftmp);
    return OFFLOAD_FAIL;
  }

  fclose(ftmp);
  return OFFLOAD_SUCCESS;
}

int32_t GoogleProvider::data_delete(void *tgt_ptr, int32_t id) {
  std::string command = "gsutil rm ";

  command += ginfo.Bucket + hdfs.WorkingDir + std::string(filename);

  if (!execute_command(command, true)) {
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t GoogleProvider::submit_job() {
  //gcloud beta dataproc jobs submit spark --cluster <my-dataproc-cluster> \
--class org.apache.spark.examples.SparkPi \
--jars file:///usr/lib/spark/lib/spark-examples.jar 1000
  send_file(spark.JarPath.c_str(), "test-assembly-0.1.0.jar");

  std::string command = "gcloud beta dataproc jobs submit spark ";

  command += "--cluster " + ginfo.Cluster;
  command += " ";
  command += "--class " + spark.Package;
  command += " ";
  command += "--jars " + ginfo.Bucket + hdfs.WorkingDir + "test-assembly-0.1.0.jar";

  // Execution arguments pass to the spark kernel
  if (hdfs.ServAddress.find("://") == std::string::npos) {
    command += " hdfs://";
  }
  command += hdfs.ServAddress;

  if (hdfs.ServAddress.back() == '/') {
    command.erase(command.end() - 1);
  }

  command += ":" + std::to_string(hdfs.ServPort);
  command += " " + hdfs.UserName;
  command += " " + hdfs.WorkingDir;

  if (!execute_command(command, true)) {
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}
