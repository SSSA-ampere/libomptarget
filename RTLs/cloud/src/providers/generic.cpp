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
#include "cloud_ssh.h"
#include "cloud_util.h"
#include "omptarget.h"

#include "generic.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define GETNAME2(name) #name
#define GETNAME(name) GETNAME2(name)
#define DP(...)                                                                \
  DEBUGP("Target " GETNAME(TARGET_NAME) " RTL, Generic Provider:", __VA_ARGS__)

CloudProvider *createGenericProvider(SparkInfo &sparkInfo) {
  return new GenericProvider(sparkInfo);
}

int32_t GenericProvider::parse_config(INIReader *reader) {

  hinfo = {
      reader->Get("HdfsProvider", "HostName", ""),
      (int)reader->GetInteger("HdfsProvider", "Port", DEFAULT_HDFS_PORT),
      reader->Get("HdfsProvider", "User", ""),
  };

  if (!hinfo.ServAddress.compare("") || !hinfo.UserName.compare("")) {
    fprintf(stderr, "Invalid values in configuration file for HDFS!");
    exit(EXIT_FAILURE);
  }

  if (spark.VerboseMode != Verbosity::quiet)
    DP("HDFS HostName: '%s' - Port: '%d' - User: '%s'\n",
       hinfo.ServAddress.c_str(), hinfo.ServPort, hinfo.UserName.c_str());

  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::init_device() {
  int retval;

  // Init connection to HDFS cluster
  struct hdfsBuilder *builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, hinfo.ServAddress.c_str());
  hdfsBuilderSetNameNodePort(builder, hinfo.ServPort);
  hdfsBuilderSetUserName(builder, hinfo.UserName.c_str());
  fs = hdfsBuilderConnect(builder);

  if (fs == NULL) {
    fprintf(stderr, "Connection problem with HDFS cluster. Check your "
                    "configuration file.\n");
    exit(EXIT_FAILURE);
  }

  hdfsFreeBuilder(builder);

  if (hdfsExists((hdfsFS)fs, spark.WorkingDir.c_str()) < 0) {
    retval = hdfsCreateDirectory((hdfsFS)fs, spark.WorkingDir.c_str());
    if (retval < 0) {
      fprintf(stderr, "ERROR: Cannot create HDFS working directory\n");
      exit(EXIT_FAILURE);
    }
  }

  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::send_file(std::string filename,
                                   std::string tgtfilename) {
  std::string final_name = spark.WorkingDir + std::string(tgtfilename);

  if (spark.VerboseMode != Verbosity::quiet)
    DP("submitting file %s as %s\n", filename.c_str(), final_name.c_str());

  hdfsFile file =
      hdfsOpenFile((hdfsFS)fs, final_name.c_str(), O_WRONLY, BUFF_SIZE, 0, 0);

  if (file == NULL) {
    fprintf(stderr, "ERROR: Opening file in HDFS failed.\n");
    exit(EXIT_FAILURE);
  }

  std::ifstream hstfile(filename, std::ios::in | std::ios::binary);

  if (!hstfile.is_open()) {
    fprintf(stderr, "ERROR: Opening host file %s failed.", filename.c_str());
    hdfsCloseFile((hdfsFS)fs, file);
    exit(EXIT_FAILURE);
  }

  char *buffer = new char[4096]();
  int retval;

  while (true) {
    hstfile.read(buffer, 4096);

    if (!hstfile.good()) {
      if (!hstfile.eof()) {
        break;
      }
    }

    retval = hdfsWrite((hdfsFS)fs, file, buffer, hstfile.gcount());

    if (retval < 0) {
      fprintf(stderr, "ERROR: Writing on HDFS failed.\n");
      hdfsCloseFile((hdfsFS)fs, file);
      exit(EXIT_FAILURE);
    }

    if (hstfile.eof()) {
      break;
    }
  }

  hstfile.close();

  retval = hdfsCloseFile((hdfsFS)fs, file);

  if (retval < 0) {
    fprintf(stderr, "ERROR: Closing on HDFS failed.\n");
    exit(EXIT_FAILURE);
  }

  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::get_file(std::string host_filename,
                                  std::string filename) {
  filename = spark.WorkingDir + filename;

  std::ofstream hostfile(host_filename);
  if (!hostfile.is_open()) {
    fprintf(stderr, "ERROR: Failed to open temporary file\n");
    exit(EXIT_FAILURE);
  }

  int retval = hdfsExists((hdfsFS)fs, filename.c_str());
  if (retval < 0) {
    fprintf(stderr, "ERROR: File does not exist on HDFS %s\n",
            filename.c_str());
    exit(EXIT_FAILURE);
  }

  hdfsFileInfo *fileInfo = hdfsGetPathInfo((hdfsFS)fs, filename.c_str());
  int size = fileInfo->mSize;

  hdfsFile file = hdfsOpenFile((hdfsFS)fs, filename.c_str(), O_RDONLY, 0, 0, 0);
  if (file == NULL) {
    fprintf(stderr, "ERROR: Opening failed on HDFS %s\n", filename.c_str());
    exit(EXIT_FAILURE);
  }

  // Retrieve data by packet
  const char *buffer = new char[BUFF_SIZE];
  int current_pos = 0;

  do {
    retval = hdfsRead((hdfsFS)fs, file, (void *)buffer, BUFF_SIZE);
    if (retval < 0) {
      fprintf(stderr, "Reading failed on HDFS %s.\n", filename.c_str());
      exit(EXIT_FAILURE);
    }
    current_pos += retval;
    // FIXME: Strange fix to avoid slow reading
    // sleep(0);
    // fprintf(stdout, "Reading %d bytes\n", retval);

    hostfile.write(buffer, BUFF_SIZE);
  } while (current_pos != size);

  retval = hdfsCloseFile((hdfsFS)fs, file);
  if (retval < 0) {
    fprintf(stderr, "Closing file on HDFS failed %s.\n", filename.c_str());
    exit(EXIT_FAILURE);
  }

  hostfile.close();

  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::delete_file(std::string filename) {
  DP("Deleting file '%s'\n", filename.c_str());

  int retval = hdfsDelete((hdfsFS)fs, filename.c_str(), 0);
  if (retval < 0) {
    fprintf(stderr, "ERROR: Deleting HDFS file failed.\n");
    exit(EXIT_FAILURE);
  }

  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::submit_job() {
  int32_t res;
  if (spark.Mode == SparkMode::cluster) {
    res = submit_cluster();
  } else {
    res = submit_local();
  }
  return res;
}

int32_t GenericProvider::submit_cluster() {
  int32_t rc;

  // init ssh session
  ssh_session session = ssh_new();
  if (session == NULL) {
    fprintf(stderr, "ERROR: Cannot create ssh channel.\n");
    exit(EXIT_FAILURE);
  }

  int verbosity = SSH_LOG_NOLOG;
  int port = 22;

  ssh_options_set(session, SSH_OPTIONS_HOST, spark.ServAddress.c_str());
  ssh_options_set(session, SSH_OPTIONS_USER, spark.UserName.c_str());
  ssh_options_set(session, SSH_OPTIONS_LOG_VERBOSITY, &verbosity);
  ssh_options_set(session, SSH_OPTIONS_PORT, &port);

  rc = ssh_connect(session);
  if (rc != SSH_OK) {
    fprintf(stderr, "ERROR: connection to ssh server failed: %s\n",
            ssh_get_error(session));
    exit(EXIT_FAILURE);
  }

  // Verify the server's identity
  if (ssh_verify_knownhost(session) < 0) {
    ssh_disconnect(session);
    ssh_free(session);
    exit(EXIT_FAILURE);
  }

  rc = ssh_userauth_publickey_auto(session, spark.UserName.c_str(), NULL);
  if (rc == SSH_AUTH_ERROR) {
    fprintf(stderr, "Authentication failed: %s\n", ssh_get_error(session));
    exit(EXIT_FAILURE);
  }

  // Copy jar file
  if (spark.VerboseMode != Verbosity::quiet)
    DP("Send Spark JAR file to the cluster driver\n");
  std::string JarFileName = "SparkJob-OmpCloud-" + random_string(8) + ".jar";

  rc = ssh_copy(session, spark.JarPath.c_str(), "/tmp/", JarFileName.c_str());

  if (rc != SSH_OK) {
    fprintf(stderr, "ERROR: Copy of the JAR file failed: %s\n",
            ssh_get_error(session));
    exit(EXIT_FAILURE);
  }

  // Run Spark
  if (spark.VerboseMode != Verbosity::quiet)
    DP("Submit Spark job to the cluster driver\n");
  std::string cmd = spark.BinPath + "spark-submit --master spark://" +
                    spark.ServAddress + ":" + std::to_string(spark.ServPort) +
                    " " + spark.AdditionalArgs + " --class " + spark.Package +
                    " --name \"" + __progname + "\" /tmp/" + JarFileName + " " +
                    get_job_args();

  if (spark.VerboseMode != Verbosity::quiet)
    DP("Executing SSH command: %s\n", cmd.c_str());

  rc = ssh_run(session, cmd.c_str(), spark.VerboseMode != Verbosity::quiet);
  if (rc != SSH_OK) {
    fprintf(stderr, "ERROR: Spark job execution through SSH failed %s\n",
            ssh_get_error(session));
    exit(EXIT_FAILURE);
  }

  ssh_disconnect(session);
  ssh_free(session);

  return rc;
}

int32_t GenericProvider::submit_local() {
  std::string cmd = "spark-submit";

  // Spark job entry point
  cmd += " " + spark.AdditionalArgs;
  cmd += " --name " + std::string("\"") + __progname + std::string("\"") +
         cmd += " --class " + spark.Package + " " + spark.JarPath;

  // Execution arguments pass to the spark kernel
  cmd += " " + get_job_args();

  if (execute_command(cmd.c_str(), spark.VerboseMode != Verbosity::quiet,
                      spark.VerboseMode == Verbosity::debug)) {
    fprintf(stderr, "ERROR: Spark job failed\n");
    exit(EXIT_FAILURE);
  }

  return OFFLOAD_SUCCESS;
}

std::string GenericProvider::get_job_args() {
  std::string args = "";

  if (hinfo.ServAddress.find("s3") != std::string::npos) {
    args += "S3";
  } else {
    args += "HDFS";
  }

  args += " ";

  if (hinfo.ServAddress.find("://") == std::string::npos) {
    args += "hdfs://";
  }
  args += hinfo.ServAddress;

  if (hinfo.ServAddress.back() == '/') {
    args.erase(args.end() - 1);
  }

  if (hinfo.ServAddress.find("hdfs") != std::string::npos) {
    args += ":" + std::to_string(hinfo.ServPort);
  }

  args += " " + hinfo.UserName;
  args += " " + spark.WorkingDir;

  if (spark.Compression)
    args += " " + spark.CompressionFormat;
  else
    args += " false";

  args += " " + spark.SchedulingSize;
  args += " " + spark.SchedulingKind;
  args += " " + std::to_string(spark.VerboseMode);

  return args;
}
