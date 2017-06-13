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

static std::string working_path;

LocalProvider::~LocalProvider() { remove_directory(working_path.c_str()); }

CloudProvider *createLocalProvider(ResourceInfo &resources) {
  return new LocalProvider(resources);
}

int32_t LocalProvider::parse_config(INIReader reader) {
  return OFFLOAD_SUCCESS;
}

int32_t LocalProvider::init_device() {
  int retval;
  // Create the working folder
  char tempdir_template[] = "/tmp/ompcloud.XXXXXX";
  char *tempdir = mkdtemp(tempdir_template);
  if (tempdir == NULL) {
    perror("ERROR: generating temp folder name\n");
    exit(OFFLOAD_FAIL);
  }
  working_path = tempdir;
  std::string cmd("mkdir -p " + working_path + "/" + spark.WorkingDir);
  exec_cmd(cmd.c_str());
  return OFFLOAD_SUCCESS;
}

std::string LocalProvider::get_cloud_path(std::string filename) {
  return std::string(working_path) + "/" + spark.WorkingDir + filename;
}

int32_t LocalProvider::send_file(std::string filename,
                                 std::string tgtfilename) {
  std::ifstream src(filename, std::ios::binary);
  std::ofstream dst(get_cloud_path(tgtfilename), std::ios::binary);
  dst << src.rdbuf();
  return OFFLOAD_SUCCESS;
}

int32_t LocalProvider::get_file(std::string host_filename,
                                std::string filename) {
  std::ifstream src(get_cloud_path(filename), std::ios::binary);
  std::ofstream dst(host_filename, std::ios::binary);
  dst << src.rdbuf();
  return OFFLOAD_SUCCESS;
}

int32_t LocalProvider::delete_file(std::string filename) {
  DP("Deleting file '%s'\n", filename.c_str());
  remove(get_cloud_path(filename).c_str());
  return OFFLOAD_SUCCESS;
}

int32_t LocalProvider::submit_job() {
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

  return args;
}
