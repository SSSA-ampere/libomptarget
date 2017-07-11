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

#include "INIReader.h"
#include "cloud_ssh.h"
#include "cloud_util.h"
#include "omptarget.h"
#include "rtl.h"

#include "amazon.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define GETNAME2(name) #name
#define GETNAME(name) GETNAME2(name)
#define DP(...)                                                                \
  DEBUGP("Target " GETNAME(TARGET_NAME) " RTL, Amazon Provider:", __VA_ARGS__)

CloudProvider *createAmazonProvider(ResourceInfo &resources) {
  return new AmazonProvider(resources);
}

int32_t AmazonProvider::parse_config(INIReader *reader) {
  ainfo.Bucket = reader->Get("AmazonProvider", "Bucket", DEFAULT_AWS_BUCKET);
  if (ainfo.Bucket.empty())
    DP("WARNING: Did not find S3 bucket name, use default.\n");

  ainfo.AccessKey = reader->Get("AmazonProvider", "AccessKey", "");
  if (ainfo.AccessKey.empty()) {
    if (char *envAccessKey = std::getenv("AWS_ACCESS_KEY_ID")) {
      ainfo.AccessKey = std::string(envAccessKey);
    }
    if (ainfo.AccessKey.empty()) {
      DP("ERROR: Did not find AWS Access Key.\n");
      exit(OFFLOAD_FAIL);
    }
  }

  ainfo.SecretKey = reader->Get("AmazonProvider", "SecretKey", "");
  if (ainfo.SecretKey.empty()) {
    if (char *envSecretKey = std::getenv("AWS_SECRET_ACCESS_KEY")) {
      ainfo.SecretKey = std::string(envSecretKey);
    }
    if (ainfo.SecretKey.empty()) {
      DP("ERROR: Did not find AWS Secret Key.\n");
      exit(OFFLOAD_FAIL);
    }
  }

  // FIXME: not used anymore ?
  ainfo.Cluster = reader->Get("AmazonProvider", "Cluster", "");
  ainfo.KeyFile = reader->Get("AmazonProvider", "KeyFile", "");
  ainfo.AdditionalArgs = reader->Get("AmazonProvider", "AdditionalArgs", "");

  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::init_device() { return OFFLOAD_SUCCESS; }

std::string AmazonProvider::get_keys() {
  return "--access_key=" + ainfo.AccessKey +
         " --secret_key=" + ainfo.SecretKey + " " + ainfo.AdditionalArgs;
}

std::string AmazonProvider::get_cloud_path(std::string filename) {
  return "s3://" + ainfo.Bucket + spark.WorkingDir + filename;
}

int32_t AmazonProvider::send_file(std::string filename,
                                  std::string tgtfilename) {
  std::string command = "s3cmd put ";

  command += std::string(filename);
  command += " " + get_cloud_path(std::string(tgtfilename));
  command += " " + get_keys();

  if (execute_command(command.c_str(), true)) {
    DP("ERROR: s3cmd failed: %s\n", command.c_str());
    exit(OFFLOAD_FAIL);
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
    exit(OFFLOAD_FAIL);
  }

  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::delete_file(std::string filename) {
  std::string command = "s3cmd rm ";

  command += get_cloud_path(filename) + " " + get_keys();

  if (execute_command(command.c_str(), true)) {
    exit(OFFLOAD_FAIL);
  }

  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::submit_job() {
  int32_t rc;

  // init ssh session
  ssh_session aws_session = ssh_new();
  int verbosity = SSH_LOG_NOLOG;
  int port = 22;

  if (aws_session == NULL) {
    DP("ERROR: Cannot create ssh session\n")
    exit(OFFLOAD_FAIL);
  }

  ssh_options_set(aws_session, SSH_OPTIONS_HOST, spark.ServAddress.c_str());
  ssh_options_set(aws_session, SSH_OPTIONS_USER, spark.UserName.c_str());
  ssh_options_set(aws_session, SSH_OPTIONS_LOG_VERBOSITY, &verbosity);
  ssh_options_set(aws_session, SSH_OPTIONS_PORT, &port);

  rc = ssh_connect(aws_session);
  if (rc != SSH_OK) {
    fprintf(stderr, "ERROR: cannot connect to server: %s\n",
            ssh_get_error(aws_session));
    exit(OFFLOAD_FAIL);
  }

  // Verify the server's identity
  if (ssh_verify_knownhost(aws_session) != SSH_OK) {
    fprintf(stderr, "ERROR: the server identity is not known: %s\n",
            ssh_get_error(aws_session));
    ssh_disconnect(aws_session);
    ssh_free(aws_session);
    exit(OFFLOAD_FAIL);
  }

  rc = ssh_userauth_publickey_auto(aws_session, spark.UserName.c_str(), NULL);
  if (rc == SSH_AUTH_ERROR) {
    ssh_key pkey;

    ssh_pki_import_privkey_file(ainfo.KeyFile.c_str(), NULL, NULL, NULL, &pkey);
    rc = ssh_userauth_publickey(aws_session, spark.UserName.c_str(), pkey);

    if (rc == SSH_AUTH_ERROR) {
      fprintf(stderr, "ERROR: SSH authentication failed: %s\n",
              ssh_get_error(aws_session));
      exit(OFFLOAD_FAIL);
    }
  }

  // Copy jar file
  DP("Send Spark JAR file to the cluster driver\n");
  std::string JarFileName = "SparkJob-OmpCloud-" + random_string(8) + ".jar";

  rc = ssh_copy(aws_session, spark.JarPath.c_str(), "/tmp/",
                JarFileName.c_str());
  if (rc != SSH_OK) {
    fprintf(stderr, "ERROR: Copy of the JAR file failed: %s\n",
            ssh_get_error(aws_session));
    exit(OFFLOAD_FAIL);
  }

  // Run Spark
  DP("Submit Spark job to the cluster driver\n");
  std::string cmd = "export AWS_ACCESS_KEY_ID=" + ainfo.AccessKey +
                    " && export AWS_SECRET_ACCESS_KEY=" + ainfo.SecretKey +
                    " && " + spark.BinPath + "spark-submit --name " + "\"" +
                    __progname + "\"" + " --master spark://" +
                    spark.ServAddress + ":" + std::to_string(spark.ServPort) +
                    " " + spark.AdditionalArgs + " --class " + spark.Package +
                    " /tmp/" + JarFileName + " " + get_job_args() + " " +
                    ainfo.AccessKey + " " + ainfo.SecretKey;

  rc = ssh_run(aws_session, cmd.c_str(), spark.VerboseMode != Verbosity::quiet);
  if (rc != SSH_OK) {
    fprintf(stderr, "ERROR: Spark job execution through SSH failed %s\n",
            ssh_get_error(aws_session));
    exit(OFFLOAD_FAIL);
  }

  ssh_disconnect(aws_session);
  ssh_free(aws_session);

  return rc;
}

std::string AmazonProvider::get_job_args() {
  std::string args = "";

  args += "S3";
  args += " s3a://" + ainfo.Bucket;
  args += " " + spark.UserName;
  args += " " + spark.WorkingDir;

  if (spark.Compression)
    args += " " + spark.CompressionFormat;
  else
    args += " false";

  return args;
}
