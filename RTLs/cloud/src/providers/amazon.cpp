//===-------------------- Target RTLs Implementation -------------- C++ -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// RTL for Apache Spark cloud cluster
//
//===----------------------------------------------------------------------===//

#include <cstdlib>
#include <hdfs.h>
#include <libssh/libssh.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../rtl.h"
#include "../util/ssh.h"
#include "INIReader.h"
#include "amazon.h"
#include "generic.h"
#include "omptarget.h"

#include "gzip_cpp.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define GETNAME2(name) #name
#define GETNAME(name) GETNAME2(name)
#define DP(...)                                                                \
  DEBUGP("Target " GETNAME(TARGET_NAME) " RTL, Amazon Provider:", __VA_ARGS__)

// ./spark-ec2 -k <keypair> -i <key-file> -s <num-slaves> launch <cluster-name>

GenericProvider *createAmazonProvider(ResourceInfo &resources) {
  return new AmazonProvider(resources);
}

int32_t AmazonProvider::parse_config(INIReader reader) {
  ainfo.Bucket = reader.Get("AmazonProvider", "Bucket", DEFAULT_AWS_BUCKET);
  if (ainfo.AccessKey.empty())
    DP("Did not find S3 bucket name, use default.\n");

  if (char *envAccessKey = std::getenv("AWS_ACCESS_KEY_ID")) {
    ainfo.AccessKey = std::string(envAccessKey);
  }
  if (ainfo.AccessKey.empty()) {
    ainfo.AccessKey = reader.Get("AmazonProvider", "AccessKey", "");
    if (ainfo.AccessKey.empty()) {
      DP("Did not find AWS Access Key.\n");
      exit(OFFLOAD_FAIL);
    }
  }

  if (char *envSecretKey = std::getenv("AWS_SECRET_ACCESS_KEY")) {
    ainfo.SecretKey = std::string(envSecretKey);
  }
  if (ainfo.SecretKey.empty()) {
    ainfo.SecretKey = reader.Get("AmazonProvider", "SecretKey", "");
    if (ainfo.SecretKey.empty()) {
      DP("Did not find AWS Secret Key.\n");
      exit(OFFLOAD_FAIL);
    }
  }

  // FIXME: not used anymore ?
  ainfo.Cluster = reader.Get("AmazonProvider", "Cluster", "");
  ainfo.KeyFile = reader.Get("AmazonProvider", "KeyFile", "");
  ainfo.AdditionalArgs = reader.Get("AmazonProvider", "AdditionalArgs", "");

  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::init_device() { return OFFLOAD_SUCCESS; }

std::string AmazonProvider::get_keys() {
  return "--access_key=" + ainfo.AccessKey + " --secret_key=" +
         ainfo.SecretKey + " " + ainfo.AdditionalArgs;
}

std::string AmazonProvider::get_cloud_path(std::string filename) {
  return "s3://" + ainfo.Bucket + hdfs.WorkingDir + filename;
}

int32_t AmazonProvider::send_file(const char *filename,
                                  const char *tgtfilename) {
  std::string command = "s3cmd put ";

  command += std::string(filename);
  command += " " + get_cloud_path(std::string(tgtfilename));
  command += " " + get_keys();

  if (execute_command(command.c_str(), true)) {
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::data_submit(void *data_ptr, int64_t size,
                                    std::string filename) {
  int ret;

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

  if (fwrite(data_ptr, 1, size, ftmp) != size) {
    DP("Could not successfully write to temporary file.\n");
    fclose(ftmp);
    return OFFLOAD_FAIL;
  }

  fclose(ftmp);

  ret = send_file(tmp_name, filename.c_str());

  remove(tmp_name);

  return ret;
}

int32_t AmazonProvider::data_retrieve(void *data_ptr, int64_t size,
                                      std::string filename) {
  if (hdfs.Compression && size >= MIN_SIZE_COMPRESSION) {
    filename += ".gz";
  }

  DP("File %s, size %d.\n", filename.c_str(), size);
  // Creating temporary file to hold data retrieved
  const char *tmp_name = "/tmp/tmpfile_da";

  // Copying data from cloud
  std::string command = "s3cmd get --force ";

  command += get_cloud_path(filename);
  command += " " + std::string(tmp_name) + " " + get_keys();

  if (execute_command(command.c_str(), true)) {
    return OFFLOAD_FAIL;
  }

  // Reading contents of temporary file
  FILE *ftmp = fopen(tmp_name, "rb");

  if (!ftmp) {
    DP("Could not open temporary file.\n");
    return OFFLOAD_FAIL;
  }

  if (hdfs.Compression && size >= MIN_SIZE_COMPRESSION) {
    struct stat stat_buf;
    int rc = stat(tmp_name, &stat_buf);
    if (rc != 0)
      return OFFLOAD_FAIL;

    size_t comp_size = stat_buf.st_size;
    gzip::Data data = gzip::AllocateData(comp_size);

    if (fread(data->ptr, 1, data->size, ftmp) != comp_size) {
      DP("Could not successfully read temporary file. => %d\n", comp_size);
      fclose(ftmp);
      remove(tmp_name);
      return OFFLOAD_FAIL;
    }

    gzip::Decomp decomp;
    if (!decomp.IsSucc())
      return OFFLOAD_FAIL;

    bool succ;
    gzip::DataList out_data_list;
    std::tie(succ, out_data_list) = decomp.Process(data);
    gzip::Data decomp_data = gzip::ExpandDataList(out_data_list);

    if (decomp_data->size != size) {
      DP("Decompressed data are not the right size. => %d\n",
         decomp_data->size);
      fclose(ftmp);
      remove(tmp_name);
      return OFFLOAD_FAIL;
    }
    memcpy(data_ptr, decomp_data->ptr, size);
  } else {

    if (fread(data_ptr, 1, size, ftmp) != size) {
      DP("Could not successfully read temporary file. => %d\n", size);
      fclose(ftmp);
      return OFFLOAD_FAIL;
    }

  }

  fclose(ftmp);
  remove(tmp_name);
  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::data_delete(void *tgt_ptr, int32_t id) {
  std::string command = "s3cmd rm ";

  std::string filename = std::to_string(id);
  if (hdfs.Compression) {
    filename += ".gz";
  }

  command += get_cloud_path(filename) + " " + get_keys();

  if (execute_command(command.c_str(), true)) {
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::submit_job() {
  int32_t rc;

  // init ssh session
  ssh_session aws_session = ssh_new();
  int verbosity = SSH_LOG_NOLOG;
  int port = 22;

  if (aws_session == NULL)
    exit(-1);

  ssh_options_set(aws_session, SSH_OPTIONS_HOST, spark.ServAddress.c_str());
  ssh_options_set(aws_session, SSH_OPTIONS_USER, spark.UserName.c_str());
  ssh_options_set(aws_session, SSH_OPTIONS_LOG_VERBOSITY, &verbosity);
  ssh_options_set(aws_session, SSH_OPTIONS_PORT, &port);

  rc = ssh_connect(aws_session);
  if (rc != SSH_OK) {
    fprintf(stderr, "Error connecting to server: %s\n",
            ssh_get_error(aws_session));
    return OFFLOAD_FAIL;
  }

  // Verify the server's identity
  if (ssh_verify_knownhost(aws_session) < 0) {
    ssh_disconnect(aws_session);
    ssh_free(aws_session);
    return OFFLOAD_FAIL;
  }

  rc = ssh_userauth_publickey_auto(aws_session, spark.UserName.c_str(), NULL);
  if (rc == SSH_AUTH_ERROR) {
    ssh_key pkey;

    ssh_pki_import_privkey_file(ainfo.KeyFile.c_str(), NULL, NULL, NULL, &pkey);
    rc = ssh_userauth_publickey(aws_session, spark.UserName.c_str(), pkey);

    if (rc == SSH_AUTH_ERROR) {
      fprintf(stderr, "Authentication failed: %s\n",
              ssh_get_error(aws_session));
      return (OFFLOAD_FAIL);
    }
  }

  // Copy jar file
  DP("Send Spark jar file\n");
  ssh_copy(aws_session, spark.JarPath.c_str(), "/tmp/", "spark_job.jar");

  // Run Spark
  std::string cmd = "export AWS_ACCESS_KEY_ID=" + ainfo.AccessKey +
                    " && export AWS_SECRET_ACCESS_KEY=" + ainfo.SecretKey +
                    " && " + spark.BinPath + "spark-submit --name " + "\"" +
                    __progname + "\"" + " --master spark://" +
                    spark.ServAddress + ":" + std::to_string(spark.ServPort) +
                    " " + spark.AdditionalArgs + " --class " + spark.Package +
                    " /tmp/spark_job.jar " + get_job_args() + " " +
                    ainfo.AccessKey + " " + ainfo.SecretKey;

  rc = ssh_run(aws_session, cmd.c_str());

  ssh_disconnect(aws_session);
  ssh_free(aws_session);

  return rc;
}
