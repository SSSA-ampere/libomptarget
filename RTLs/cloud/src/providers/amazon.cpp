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

#include <hdfs.h>
#include <libssh/libssh.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../rtl.h"
#include "INIReader.h"
#include "amazon.h"
#include "generic.h"
#include "omptarget.h"
#include "../util/ssh.h"

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

  ainfo.Bucket = reader.Get("AmazonProvider", "Bucket", "");
  ainfo.Cluster = reader.Get("AmazonProvider", "Cluster", "");
  ainfo.AccessKey = reader.Get("AmazonProvider", "AccessKey", std::getenv("AWS_ACCESS_KEY_ID"));
  ainfo.SecretKey = reader.Get("AmazonProvider", "SecretKey", std::getenv("AWS_SECRET_ACCESS_KEY"));
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

int32_t AmazonProvider::data_submit(void *tgt_ptr, void *hst_ptr, int64_t size,
                                    int32_t id) {
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

  if (fwrite(hst_ptr, 1, size, ftmp) != size) {
    DP("Could not successfully write to temporary file.\n");
    fclose(ftmp);
    return OFFLOAD_FAIL;
  }

  fclose(ftmp);

  // Submitting data to cloud
  std::string final_filename = std::to_string(id);
  ret = send_file(tmp_name, final_filename.c_str());

  remove(tmp_name);

  return ret;
}

int32_t AmazonProvider::data_retrieve(void *hst_ptr, void *tgt_ptr,
                                      int64_t size, int32_t id) {
  DP("File %d, size %d.\n", id, size);
  // Creating temporary file to hold data retrieved
  const char *tmp_name = "/tmp/tmpfile_da";

  // Copying data from cloud
  std::string command = "s3cmd get --force ";

  command += get_cloud_path(std::to_string(id));
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

  if (fread(hst_ptr, 1, size, ftmp) != size) {
    DP("Could not successfully read temporary file. => %d\n", size);
    fclose(ftmp);
    return OFFLOAD_FAIL;
  }

  fclose(ftmp);
  remove(tmp_name);
  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::data_delete(void *tgt_ptr, int32_t id) {
  std::string command = "s3cmd rm ";

  command += get_cloud_path(std::to_string(id)) + " " + get_keys();

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
  if (rc == SSH_AUTH_ERROR)
  {
    ssh_key pkey;

    ssh_pki_import_privkey_file(ainfo.KeyFile.c_str(), NULL, NULL, NULL, &pkey);
    rc = ssh_userauth_publickey(aws_session, spark.UserName.c_str(), pkey);

    if (rc == SSH_AUTH_ERROR)
    {
     fprintf(stderr, "Authentication failed: %s\n",
       ssh_get_error(aws_session));
     return(OFFLOAD_FAIL);
    }
  }



  // Copy jar file
  ssh_copy(aws_session, spark.JarPath.c_str(), "/tmp/", "spark_job.jar");

  // Run Spark
  std::string cmd = "export AWS_ACCESS_KEY_ID=" + ainfo.AccessKey +
                    " && export AWS_SECRET_ACCESS_KEY=" + ainfo.SecretKey +
                    " && " + spark.BinPath + "spark-submit --name " + "\"" + __progname + "\"" +
                    " --master spark://" +
                    spark.ServAddress + ":" + std::to_string(spark.ServPort) +
                    " " + spark.AdditionalArgs + " --class " + spark.Package +
                    " /tmp/spark_job.jar " + get_job_args() + " " + ainfo.AccessKey + " " + ainfo.SecretKey;

  DP("Executing SSH command: %s\n", cmd.c_str());

  rc = ssh_run(aws_session, cmd.c_str());

  ssh_disconnect(aws_session);
  ssh_free(aws_session);

  return rc;
}
