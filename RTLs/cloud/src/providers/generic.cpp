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
#include <thread>

#include <sys/stat.h>
#include <sys/types.h>

#include "../rtl.h"
#include "../util/ssh.h"
#include "INIReader.h"
#include "generic.h"
#include "omptarget.h"

#include "gzip_cpp.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define GETNAME2(name) #name
#define GETNAME(name) GETNAME2(name)
#define DP(...)                                                                \
  DEBUGP("Target " GETNAME(TARGET_NAME) " RTL, Generic Provider:", __VA_ARGS__)

GenericProvider *createGenericProvider(ResourceInfo &resources) {
  return new GenericProvider(resources);
}

int32_t GenericProvider::parse_config(INIReader reader) {
  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::init_device() {
  int retval;

  // Init connection to HDFS cluster
  struct hdfsBuilder *builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, hdfs.ServAddress.c_str());
  hdfsBuilderSetNameNodePort(builder, hdfs.ServPort);
  hdfsBuilderSetUserName(builder, hdfs.UserName.c_str());
  fs = hdfsBuilderConnect(builder);

  if (fs == NULL) {
    DP("Connection problem with HDFS cluster. Check your configuration in "
       "'cloud_rtl.ini'.\n");
    return OFFLOAD_FAIL;
  }

  hdfsFreeBuilder(builder);

  if (hdfsExists(fs, hdfs.WorkingDir.c_str()) < 0) {
    retval = hdfsCreateDirectory(fs, hdfs.WorkingDir.c_str());
    if (retval < 0) {
      DP("Cannot create directory\n");
      return OFFLOAD_FAIL;
    }
  }

  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::send_file(const char *filename,
                                   const char *tgtfilename) {
  std::string final_name = hdfs.WorkingDir + std::string(tgtfilename);

  DP("submitting file %s as %s\n", filename, final_name.c_str());

  hdfsFile file = hdfsOpenFile(fs, final_name.c_str(), O_WRONLY, 0, 0, 0);

  if (file == NULL) {
    DP("Opening file in HDFS failed.\n");
    return OFFLOAD_FAIL;
  }

  std::ifstream hstfile(filename, std::ios::in | std::ios::binary);

  if (!hstfile.is_open()) {
    DP("Opening host file %s failed.", filename);
    hdfsCloseFile(fs, file);
    return OFFLOAD_FAIL;
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

    retval = hdfsWrite(fs, file, buffer, hstfile.gcount());

    if (retval < 0) {
      DP("Writing on HDFS failed.\n");
      hdfsCloseFile(fs, file);
      return OFFLOAD_FAIL;
    }

    if (hstfile.eof()) {
      break;
    }
  }

  hstfile.close();

  retval = hdfsCloseFile(fs, file);

  if (retval < 0) {
    DP("Closing on HDFS failed.\n");
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

void *GenericProvider::data_alloc(int64_t size, int32_t type, int32_t id) {
  // NOTE: we do not create the HDFS file here because we do not want to
  // waste time creating stuff that we might not need before (there may be
  // unecessary allocations)

  // Return fake target address
  return (void *)currAddr++;
}

int32_t GenericProvider::data_retrieve(void *data_ptr, int64_t size,
                                       std::string filename) {
  int retval;
  filename = hdfs.WorkingDir + filename;

  if (hdfs.Compression && size >= MIN_SIZE_COMPRESSION) {
    filename += ".gz";
  }

  DP("Reading data from file '%s'\n", filename.c_str());

  retval = hdfsExists(fs, filename.c_str());
  if (retval < 0) {
    DP("File does not exist\n");
    return OFFLOAD_FAIL;
  }

  hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, filename.c_str());
  if (hdfs.Compression && size >= MIN_SIZE_COMPRESSION) {
    DP("File compressed to %d bytes\n", fileInfo->mSize, size);
  } else if (fileInfo->mSize != size) {
    DP("Wrong file size: %d instead of %d\n", fileInfo->mSize, size);
    return OFFLOAD_FAIL;
  }

  hdfsFile file = hdfsOpenFile(fs, filename.c_str(), O_RDONLY, 0, 0, 0);
  if (file == NULL) {
    DP("Opening failed.\n");
    return OFFLOAD_FAIL;
  }

  // Retrieve data by packet
  char *buffer;

  gzip::Data data_comp;
  if (hdfs.Compression && size >= MIN_SIZE_COMPRESSION) {
    data_comp = gzip::AllocateData(fileInfo->mSize);
    buffer = data_comp->ptr;
  } else
    buffer = reinterpret_cast<char *>(data_ptr);

  int current = 0;
  do {
    retval = hdfsRead(fs, file, &buffer[current], size - current);
    if (retval < 0) {
      DP("Reading failed.\n");
      return OFFLOAD_FAIL;
    }
    current = current + retval;
    // FIXME: Strange fix to avoid slow reading
    // sleep(0);
    printf("Reading %d bytes\n", retval);
  } while (current != fileInfo->mSize);

  if (retval < 0) {
    DP("Reading failed.\n");
    return OFFLOAD_FAIL;
  }

  retval = hdfsCloseFile(fs, file);
  if (retval < 0) {
    DP("Closing failed.\n");
    return OFFLOAD_FAIL;
  }

  if (hdfs.Compression && size >= MIN_SIZE_COMPRESSION) {
    gzip::Decomp decomp;
    if (!decomp.IsSucc())
      return OFFLOAD_FAIL;

    bool succ;
    gzip::DataList out_data_list;
    std::tie(succ, out_data_list) = decomp.Process(data_comp);
    gzip::Data decomp_data = gzip::ExpandDataList(out_data_list);

    if (decomp_data->size != size) {
      DP("Decompressed data are not the right size. => %d\n",
         decomp_data->size);
      return OFFLOAD_FAIL;
    }
    memcpy(data_ptr, decomp_data->ptr, size);
  }

  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::delete_file(std::string filename) {
  DP("Deleting file '%s'\n", filename.c_str());

  int retval = hdfsDelete(fs, filename.c_str(), 0);
  if (retval < 0) {
    DP("Deleting file failed.\n");
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::submit_job() {
  int32_t res;
  if (spark.Mode == SparkMode::cluster) {
    res = submit_cluster();
  } else if (spark.Mode == SparkMode::condor) {
    res = submit_condor();
  } else {
    res = submit_local();
  }
  return res;
}

int32_t GenericProvider::submit_cluster() {
  int32_t rc;

  // init ssh session
  ssh_session session = ssh_new();
  if (session == NULL)
    exit(-1);

  int verbosity = SSH_LOG_NOLOG;
  int port = 22;

  ssh_options_set(session, SSH_OPTIONS_HOST, spark.ServAddress.c_str());
  ssh_options_set(session, SSH_OPTIONS_USER, spark.UserName.c_str());
  ssh_options_set(session, SSH_OPTIONS_LOG_VERBOSITY, &verbosity);
  ssh_options_set(session, SSH_OPTIONS_PORT, &port);

  rc = ssh_connect(session);
  if (rc != SSH_OK) {
    fprintf(stderr, "Error connecting to server: %s\n", ssh_get_error(session));
    return (OFFLOAD_FAIL);
  }

  // Verify the server's identity
  if (ssh_verify_knownhost(session) < 0) {
    ssh_disconnect(session);
    ssh_free(session);
    return (OFFLOAD_FAIL);
  }

  rc = ssh_userauth_publickey_auto(session, spark.UserName.c_str(), NULL);
  if (rc == SSH_AUTH_ERROR) {
    fprintf(stderr, "Authentication failed: %s\n", ssh_get_error(session));
    return (OFFLOAD_FAIL);
  }

  // Copy jar file
  rc = ssh_copy(session, spark.JarPath.c_str(), "/tmp/", "spark_job.jar");
  if (rc != SSH_OK) {
    return (OFFLOAD_FAIL);
  }

  // Run Spark
  std::string cmd = spark.BinPath + "spark-submit --master spark://" +
                    spark.ServAddress + ":" + std::to_string(spark.ServPort) +
                    " " + spark.AdditionalArgs + " --class " + spark.Package +
                    " --name " + "\"" + __progname + "\"" +
                    " /tmp/spark_job.jar " + get_job_args();

  DP("Executing SSH command: %s\n", cmd.c_str());

  rc = ssh_run(session, cmd.c_str());
  if (rc != SSH_OK) {
    return (OFFLOAD_FAIL);
  }

  ssh_disconnect(session);
  ssh_free(session);

  return rc;
}

int32_t GenericProvider::submit_condor() {
  int32_t rc;

  // init ssh session
  ssh_session session = ssh_new();
  if (session == NULL)
    exit(-1);

  int verbosity = SSH_LOG_NOLOG;
  int port = 22;

  ssh_options_set(session, SSH_OPTIONS_HOST, spark.ServAddress.c_str());
  ssh_options_set(session, SSH_OPTIONS_USER, spark.UserName.c_str());
  ssh_options_set(session, SSH_OPTIONS_LOG_VERBOSITY, &verbosity);
  ssh_options_set(session, SSH_OPTIONS_PORT, &port);

  rc = ssh_connect(session);
  if (rc != SSH_OK) {
    fprintf(stderr, "Error connecting to server: %s\n", ssh_get_error(session));
    return (OFFLOAD_FAIL);
  }

  // Verify the server's identity
  if (ssh_verify_knownhost(session) < 0) {
    ssh_disconnect(session);
    ssh_free(session);
    return (OFFLOAD_FAIL);
  }

  rc = ssh_userauth_publickey_auto(session, spark.UserName.c_str(), NULL);
  if (rc == SSH_AUTH_ERROR) {
    fprintf(stderr, "Authentication failed: %s\n", ssh_get_error(session));
    return (OFFLOAD_FAIL);
  }

  std::string path = "/home/sparkcluster/";

  // Copy jar file
  rc = ssh_copy(session, spark.JarPath.c_str(), path.c_str(), "spark_job.jar");
  if (rc != SSH_OK) {
    return (OFFLOAD_FAIL);
  }

  // Run Spark
  std::string cmd =
      "CONDOR_REQUIREMENTS=\"Machine == \\\"n09.lsc.ic.unicamp.br\\\"\" "
      "condor_run \"" +
      spark.BinPath + "spark-submit --master spark://10.68.100.09:" +
      std::to_string(spark.ServPort) + " " + spark.AdditionalArgs +
      " --class " + spark.Package + " --name " + std::string("\"") +
      __progname + std::string("\"") + " /home/sparkcluster/spark_job.jar " +
      get_job_args() + "\"";

  DP("Executing SSH command: %s\n", cmd.c_str());

  rc = ssh_run(session, cmd.c_str());
  if (rc != SSH_OK) {
    return (OFFLOAD_FAIL);
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

  if (execute_command(cmd.c_str(), true)) {
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

std::string GenericProvider::get_job_args() {
  std::string args = "";

  if (hdfs.ServAddress.find("s3") != std::string::npos) {
    args += "S3";
  } else {
    args += "HDFS";
  }

  args += " ";

  if (hdfs.ServAddress.find("://") == std::string::npos) {
    args += "hdfs://";
  }
  args += hdfs.ServAddress;

  if (hdfs.ServAddress.back() == '/') {
    args.erase(args.end() - 1);
  }

  if (hdfs.ServAddress.find("hdfs") != std::string::npos) {
    args += ":" + std::to_string(hdfs.ServPort);
  }

  args += " " + hdfs.UserName;
  args += " " + hdfs.WorkingDir;

  if (hdfs.Compression)
    args += " true";
  else
    args += " false";

  return args;
}

int32_t GenericProvider::execute_command(const char *command,
                                         bool print_result) {
  FILE *fp = popen(command, "r");

  if (fp == NULL) {
    DP("Failed to execute command.\n");
    return EXIT_FAILURE;
  }

  if (print_result) {
    char buf[512] = {0};
    uint read = 0;

    while ((read = fread(buf, sizeof(char), 511, fp)) == 512) {
      buf[511] = 0;
      DP("%s", buf);
    }

    buf[read] = 0;
    DP("%s", buf);
  }

  return pclose(fp);
}
