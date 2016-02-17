#include <hdfs.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "INIReader.h"
#include "omptarget.h"
#include "../rtl.h"
#include "generic.h"
#include "amazon.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define GETNAME2(name) #name
#define GETNAME(name) GETNAME2(name)
#define DP(...) DEBUGP("Target " GETNAME(TARGET_NAME) " RTL, Google Provider:", __VA_ARGS__)

// ./spark-ec2 -k <keypair> -i <key-file> -s <num-slaves> launch <cluster-name>

GenericProvider *createAmazonProvider(ResourceInfo &resources) {
  return new AmazonProvider(resources);
}

int32_t AmazonProvider::parse_config(INIReader reader) {
  AmazonInfo info {
    reader.Get("AmazonProvider", "Bucket", ""),
    reader.Get("AmazonProvider", "Cluster", ""),
  };

  ainfo = info;

  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::send_file(const char *filename, const char *tgtfilename) {
  std::string command = "s3cmd put ";

  command += std::string(filename);
  command += " ";
  command += ainfo.Bucket;
  command += hdfs.WorkingDir + std::string(tgtfilename);

  if (!execute_command(command.c_str(), true)) {
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::data_submit(void *tgt_ptr, void *hst_ptr, int64_t size, int32_t id) {
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

int32_t AmazonProvider::data_retrieve(void *hst_ptr, void *tgt_ptr, int64_t size, int32_t id) {
  // Creating temporary file to hold data retrieved
  char tmp_name[] = "/tmp/tmpfile_XXXXXX";
  int tmp_fd = mkstemp(tmp_name);

  if (tmp_fd == -1) {
    DP("Could not create temporary file.\n");
    return OFFLOAD_FAIL;
  }

  // Copying data from cloud
  std::string command = "s3cmd get ";

  command += ainfo.Bucket;
  command += hdfs.WorkingDir + std::to_string(id);
  command += " ";
  command += std::string(tmp_name);

  if (!execute_command(command.c_str(), true)) {
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

int32_t AmazonProvider::data_delete(void *tgt_ptr, int32_t id) {
  std::string command = "s3cmd rm ";

  command += ainfo.Bucket + hdfs.WorkingDir + std::to_string(id);

  if (!execute_command(command.c_str(), true)) {
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::submit_job() {
  //gcloud beta dataproc jobs submit spark --cluster <my-dataproc-cluster> \
--class org.apache.spark.examples.SparkPi \
--jars file:///usr/lib/spark/lib/spark-examples.jar 1000
  send_file(spark.JarPath.c_str(), "test-assembly-0.1.0.jar");

  std::string command = "gcloud beta dataproc jobs submit spark ";

  command += "--cluster " + ainfo.Cluster;
  command += " ";
  command += "--class " + spark.Package;
  command += " ";
  command += "--jars " + ainfo.Bucket + hdfs.WorkingDir + "test-assembly-0.1.0.jar";

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

  if (!execute_command(command.c_str(), true)) {
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}
