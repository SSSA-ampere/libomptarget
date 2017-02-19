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

#include <cstdio>
#include <cstdlib>
#include <string>

#include <hdfs.h>

#include "INIReader.h"
#include "omptarget.h"
#include "rtl.h"
#include "cloud_ssh.h"

#include "amazon.h"

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

int32_t AmazonProvider::send_file(std::string filename,
                                  std::string tgtfilename) {
  std::string command = "s3cmd put ";

  command += std::string(filename);
  command += " " + get_cloud_path(std::string(tgtfilename));
  command += " " + get_keys();

  if (execute_command(command.c_str(), true)) {
    DP("s3cmd failed: %s\n", command.c_str());
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::get_file(std::string host_filename,
                                 std::string filename) {
  // Copying data from cloud
  std::string command = "s3cmd get --force ";

  command += get_cloud_path(filename);
  command += " " + std::string(host_filename) + " " + get_keys();

  if (execute_command(command.c_str(), true)) {
    DP("s3cmd failed: %s\n", command.c_str());
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::delete_file(std::string filename) {
  std::string command = "s3cmd rm ";

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
  int verbosity = SSH_LOG_RARE;
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
      fprintf(stderr, "SSH authentication failed: %s\n",
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

std::string AmazonProvider::get_job_args() {
  std::string args = "";

  args += "S3";
  args += " s3a://" + ainfo.Bucket;
  args += " " + hdfs.UserName;
  args += " " + hdfs.WorkingDir;

  if (hdfs.Compression)
    args += " " + hdfs.CompressionFormat;
  else
    args += " false";

  return args;
}
