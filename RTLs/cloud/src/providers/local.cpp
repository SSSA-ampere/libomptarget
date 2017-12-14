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

#include <unistd.h>

#include "INIReader.h"
#include "cloud_util.h"
#include "omptarget.h"

#include "local.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define GETNAME2(name) #name
#define GETNAME(name) GETNAME2(name)
#define DP(...)                                                                \
  DEBUGP("Target " GETNAME(TARGET_NAME) " RTL, Local Provider:", __VA_ARGS__)

static const std::string working_path = "/tmp/ompcloud.local." + random_string(8);

LocalProvider::~LocalProvider() {
  if (!spark.KeepTmpFiles)
    remove_directory(working_path.c_str());
}

CloudProvider *createLocalProvider(SparkInfo &sparkInfo) {
  return new LocalProvider(sparkInfo);
}

int32_t LocalProvider::parse_config(INIReader *reader) {
  return OFFLOAD_SUCCESS;
}

int32_t LocalProvider::init_device() {
  // Create the working folder
  std::string cmd("mkdir -p " + working_path + "/" + spark.WorkingDir);
  exec_cmd(cmd.c_str());
  return OFFLOAD_SUCCESS;
}

std::string LocalProvider::get_cloud_path(std::string filename) {
  return std::string(working_path) + "/" + spark.WorkingDir + filename;
}

int32_t LocalProvider::send_file(std::string filename,
                                 std::string tgtfilename) {
  remove(get_cloud_path(tgtfilename).c_str());
  int rc = rename(filename.c_str(), get_cloud_path(tgtfilename).c_str());
  if (rc < 0)
    perror("Error renaming file");
  return OFFLOAD_SUCCESS;
}

int32_t LocalProvider::get_file(std::string host_filename,
                                std::string filename) {
  remove(host_filename.c_str());
  int rc = rename(get_cloud_path(filename).c_str(), host_filename.c_str());
  if (rc < 0)
    perror("Error renaming file");
  return OFFLOAD_SUCCESS;
}

int32_t LocalProvider::delete_file(std::string filename) {
  if (spark.VerboseMode != Verbosity::quiet)
    DP("Deleting file '%s'\n", filename.c_str());
  if (!spark.KeepTmpFiles)
    remove(get_cloud_path(filename).c_str());
  return OFFLOAD_SUCCESS;
}

int32_t LocalProvider::submit_job() {
  if (spark.VerboseMode != Verbosity::quiet)
    DP("Submit Spark job\n");

  std::string cmd = "spark-submit";

  // Spark job entry point
  cmd += " " + spark.AdditionalArgs;
  cmd += " --name " + std::string("\"") + __progname + std::string("\"") +
         cmd += " --class " + spark.Package + " " + spark.JarPath;

  // Execution arguments pass to the spark kernel
  cmd += " " + get_job_args();

  if (execute_command(cmd.c_str(), spark.VerboseMode != Verbosity::quiet)) {
    fprintf(stderr, "ERROR: Spark job failed\n");
    exit(OFFLOAD_FAIL);
  }

  return OFFLOAD_SUCCESS;
}

std::string LocalProvider::get_job_args() {
  std::string args = "";

  args += "FILE";
  args += " file:///";
  args += " " + spark.UserName;
  args += " " + working_path + "/" + spark.WorkingDir;

  if (spark.Compression)
    args += " " + spark.CompressionFormat;
  else
    args += " false";

  args += " " + spark.SchedulingSize;
  args += " " + spark.SchedulingKind;

  return args;
}
