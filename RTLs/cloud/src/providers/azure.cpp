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

#include "azure.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define GETNAME2(name) #name
#define GETNAME(name) GETNAME2(name)
#define DP(...)                                                                \
  DEBUGP("Target " GETNAME(TARGET_NAME) " RTL, Azure Provider:", __VA_ARGS__)

CloudProvider *createAzureProvider(SparkInfo &sparkInfo) {
  return new AzureProvider(sparkInfo);
}

int32_t AzureProvider::parse_config(INIReader *reader) {
  ainfo.Container = reader->Get("AzureProvider", "Container", "");

  if (ainfo.Container.empty() && spark.VerboseMode != Verbosity::quiet) {
    DP("Warning: Did not find Azure container name, use default.\n");
    ainfo.Container = DEFAULT_AZURE_CONTAINER;
  }

  ainfo.StorageAccount = reader->Get("AzureProvider", "StorageAccount", "");
  if (ainfo.StorageAccount.empty()) {
    if (char *envSecretKey = std::getenv("AZURE_STORAGE_ACCOUNT")) {
      ainfo.StorageAccount = std::string(envSecretKey);
    }
    if (ainfo.StorageAccount.empty()) {
      fprintf(stderr, "ERROR: Did not find Azure Storage Account.\n");
      exit(EXIT_FAILURE);
    }
  }

  ainfo.AccessKey = reader->Get("AzureProvider", "StorageAccessKey", "");
  if (ainfo.AccessKey.empty()) {
    if (char *envAccessKey = std::getenv("AZURE_STORAGE_ACCESS_KEY")) {
      ainfo.AccessKey = std::string(envAccessKey);
    }
    if (ainfo.AccessKey.empty()) {
      fprintf(stderr, "ERROR: Did not find Azure Storage Access Key.\n");
      exit(EXIT_FAILURE);
    }
  }

  ainfo.Cluster = reader->Get("AzureProvider", "Cluster", "");
  if (ainfo.Cluster.empty()) {
    fprintf(stderr, "ERROR: Did not find Azure cluster name.\n");
    exit(EXIT_FAILURE);
  }

  // FIXME: not used anymore ?
  ainfo.AdditionalArgs = reader->Get("AzureProvider", "AdditionalArgs", "");

  return OFFLOAD_SUCCESS;
}

int32_t AzureProvider::init_device() {
  std::string command = "az storage container create";
  command += " -n " + ainfo.Container;
  command += " --public-access blob";
  command += " " + get_keys();

  if (execute_command(command.c_str(), spark.VerboseMode != Verbosity::quiet,
                      spark.VerboseMode == Verbosity::debug)) {
    fprintf(stderr, "azure-cli failed: %s\n", command.c_str());
    exit(EXIT_FAILURE);
  }
  return OFFLOAD_SUCCESS;
}

std::string AzureProvider::get_keys() {
  return "--account-name=" + ainfo.StorageAccount +
         " --account-key=" + ainfo.AccessKey;
}

std::string AzureProvider::get_cloud_path(std::string filename) {
  std::string directory = spark.WorkingDir;
  // Azure blob path should not start with a slash
  if (directory.substr(0, 1) == "/")
    directory = directory.substr(1);
  return directory + filename;
}

int32_t AzureProvider::send_file(std::string filename,
                                 std::string tgtfilename) {
  std::string command = "az storage blob upload";
  command += " -f " + filename;
  command += " -c " + ainfo.Container;
  command += " -n " + get_cloud_path(tgtfilename);
  command += " " + get_keys();

  if (execute_command(command.c_str(), spark.VerboseMode != Verbosity::quiet,
                      spark.VerboseMode == Verbosity::debug)) {
    fprintf(stderr, "azure-cli failed: %s\n", command.c_str());
    exit(EXIT_FAILURE);
  }

  return OFFLOAD_SUCCESS;
}

int32_t AzureProvider::get_file(std::string host_filename,
                                std::string filename) {
  // Copying data from cloud
  std::string command = "az storage blob download";
  command += " -n " + get_cloud_path(filename);
  command += " -c " + ainfo.Container;
  command += " -f " + host_filename;
  command += " " + get_keys();

  if (execute_command(command.c_str(), spark.VerboseMode != Verbosity::quiet,
                      spark.VerboseMode == Verbosity::debug)) {
    fprintf(stderr, "azure-cli failed: %s\n", command.c_str());
    exit(EXIT_FAILURE);
  }

  return OFFLOAD_SUCCESS;
}

int32_t AzureProvider::delete_file(std::string filename) {
  std::string command = "az storage blob delete ";
  command += " -n " + get_cloud_path(filename);
  command += " -c " + ainfo.Container;
  command += " " + get_keys();

  if (execute_command(command.c_str(), spark.VerboseMode != Verbosity::quiet,
                      spark.VerboseMode == Verbosity::debug)) {
    fprintf(stderr, "azure-cli failed: %s\n", command.c_str());
    exit(EXIT_FAILURE);
  }

  return OFFLOAD_SUCCESS;
}

int32_t AzureProvider::submit_job() {
  int32_t rc;

  // init ssh session
  ssh_session aws_session = ssh_new();
  int verbosity = SSH_LOG_NOLOG;
  int port = 22;

  if (aws_session == NULL) {
    fprintf(stderr, "ERROR: Cannot create ssh session\n");
    exit(EXIT_FAILURE);
  }

  std::string ssh_server = ainfo.Cluster + "-ssh.azurehdinsight.net";

  ssh_options_set(aws_session, SSH_OPTIONS_HOST, ssh_server.c_str());
  ssh_options_set(aws_session, SSH_OPTIONS_USER, spark.UserName.c_str());
  ssh_options_set(aws_session, SSH_OPTIONS_LOG_VERBOSITY, &verbosity);
  ssh_options_set(aws_session, SSH_OPTIONS_PORT, &port);

  rc = ssh_connect(aws_session);
  if (rc != SSH_OK) {
    fprintf(stderr, "ERROR: cannot connect to server: %s\n",
            ssh_get_error(aws_session));
    exit(EXIT_FAILURE);
  }

  // Verify the server's identity
  if (ssh_verify_knownhost(aws_session) != SSH_OK) {
    fprintf(stderr, "ERROR: the server identity is not known: %s\n",
            ssh_get_error(aws_session));
    ssh_disconnect(aws_session);
    ssh_free(aws_session);
    exit(EXIT_FAILURE);
  }

  rc = ssh_userauth_publickey_auto(aws_session, spark.UserName.c_str(), NULL);
  if (rc == SSH_AUTH_ERROR) {
    ssh_key pkey;

    // ssh_pki_import_privkey_file(ainfo.KeyFile.c_str(), NULL, NULL, NULL,
    // &pkey);
    rc = ssh_userauth_publickey(aws_session, spark.UserName.c_str(), pkey);

    if (rc == SSH_AUTH_ERROR) {
      fprintf(stderr, "ERROR: SSH authentication failed: %s\n",
              ssh_get_error(aws_session));
      exit(EXIT_FAILURE);
    }
  }

  // Copy jar file
  if (spark.VerboseMode != Verbosity::quiet)
    DP("Send Spark JAR file to the cluster driver\n");
  std::string JarFileName = "SparkJob-OmpCloud-" + random_string(8) + ".jar";

  rc = ssh_copy(aws_session, spark.JarPath.c_str(), "/tmp/",
                JarFileName.c_str());
  if (rc != SSH_OK) {
    fprintf(stderr, "ERROR: Copy of the JAR file failed: %s\n",
            ssh_get_error(aws_session));
    exit(EXIT_FAILURE);
  }

  // Run Spark
  if (spark.VerboseMode != Verbosity::quiet)
    DP("Submit Spark job to the cluster driver\n");
  std::string cmd = spark.BinPath + "spark-submit --name " + "\"" + __progname +
                    "\"" + " --master yarn-cluster --deploy-mode cluster " +
                    spark.AdditionalArgs + " --class " + spark.Package +
                    " /tmp/" + JarFileName + " " + get_job_args();

  rc = ssh_run(aws_session, cmd.c_str(), spark.VerboseMode != Verbosity::quiet);
  if (rc != SSH_OK) {
    fprintf(stderr, "ERROR: Spark job execution through SSH failed %s\n",
            ssh_get_error(aws_session));
    exit(EXIT_FAILURE);
  }

  ssh_disconnect(aws_session);
  ssh_free(aws_session);

  return rc;
}

std::string AzureProvider::get_job_args() {
  std::string args = "";

  args += "HDFS";
  args += " wasb://" + ainfo.Container + "@" + ainfo.StorageAccount +
          ".blob.core.windows.net";
  args += " " + spark.UserName;
  args += " " + spark.WorkingDir;

  if (spark.Compression)
    args += " " + spark.CompressionFormat;
  else
    args += " false";

  args += " " + spark.SchedulingSize;
  args += " " + spark.SchedulingKind;

  return args;
}
