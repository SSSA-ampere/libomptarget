#include <hdfs.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "INIReader.h"
#include "omptarget.h"
#include "../rtl.h"
#include "generic.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define GETNAME2(name) #name
#define GETNAME(name) GETNAME2(name)
#define DP(...) DEBUGP("Target " GETNAME(TARGET_NAME) " RTL, Generic Provider:", __VA_ARGS__)

int32_t GenericProvider::parse_config(INIReader reader) {
  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::send_file(const char *filename, const char *tgtfilename) {
  std::string final_name = hdfs.WorkingDir + std::string(tgtfilename);

  DP("submitting file %s as %s\n", filename, final_name.c_str());

  hdfsFile file = hdfsOpenFile(fs, final_name.c_str(), O_WRONLY, 0, 0, 0);

  if (file == NULL) {
    DP("Opening file in HDFS failed.\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  std::ifstream hstfile(filename, std::ios::in|std::ios::binary);

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
      DP("Writing on HDFS failed.\n%s", hdfsGetLastError());
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
    DP("Closing on HDFS failed.\n%s", hdfsGetLastError());
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

int32_t GenericProvider::data_submit(void *tgt_ptr, void *hst_ptr, int64_t size, int32_t id) {
  if (id < 0) {
    DP("No need to submit pointer\n");
    return OFFLOAD_SUCCESS;
  }

  // Since we now need the hdfs file, we create it here
  std::string filename = hdfs.WorkingDir + std::to_string(id);
  DP("Submitting data to file %s\n", filename.c_str());

  hdfsFile file = hdfsOpenFile(fs, filename.c_str(), O_WRONLY, 0, 0, 0);

  if (file == NULL) {
    DP("Opening failed.\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  int retval = hdfsWrite(fs, file, hst_ptr, size);
  if (retval < 0) {
    DP("Writing failed.\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  retval = hdfsCloseFile(fs, file);
  if (retval < 0) {
    DP("Closing failed.\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::data_retrieve(void *hst_ptr, void *tgt_ptr, int64_t size, int32_t id) {
  std::string filename = hdfs.WorkingDir + std::to_string(id);

  DP("Reading data from file '%s'\n", filename.c_str());

  hdfsFile file = hdfsOpenFile(fs, filename.c_str(), O_RDONLY, 0, 0, 0);
  if(file == NULL) {
    DP("Opening failed.\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  int retval = hdfsRead(fs, file, hst_ptr, size);
  if(retval < 0) {
    DP("Reading failed.\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  retval = hdfsCloseFile(fs, file);
  if(retval < 0) {
    DP("Closing failed.\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::data_delete(void *tgt_ptr, int32_t id) {
  if(id < 0) {
    DP("No file to delete\n");
    return OFFLOAD_SUCCESS;
  }

  std::string filename = hdfs.WorkingDir + std::to_string(id);

  DP("Deleting file '%s'\n", filename.c_str());

  int retval = hdfsDelete(fs, filename.c_str(), 0);
  if(retval < 0) {
    DP("Deleting file failed.\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::submit_job() {
  std::string cmd = "spark-submit";

  // Spark job entry point
  cmd += " --class " + spark.Package;

  if(spark.Mode == SparkMode::cluster) {
    // Run Spark in remote cluster
    cmd += " --master ";
    if (spark.ServAddress.find("://") == std::string::npos) {
      cmd += " spark://";
    }
    cmd += spark.ServAddress;
    if (spark.ServAddress.back() == '/') {
      cmd.erase(cmd.end() - 1);
    }
    cmd += ":" + std::to_string(spark.ServPort);

    cmd += " --deploy-mode cluster";

    if (hdfs.ServAddress.find("://") == std::string::npos) {
      cmd += " hdfs://";
    }
    cmd += hdfs.ServAddress;

    if (hdfs.ServAddress.back() == '/') {
      cmd.erase(cmd.end() - 1);
    }

    cmd += hdfs.WorkingDir;
    cmd += "test-assembly-0.1.0.jar";

    // Sending file to HDFS as the library to be loaded
    send_file(spark.JarPath.c_str(), "test-assembly-0.1.0.jar");
  }
  else {
    // Run Spark locally
    cmd += " " + spark.JarPath;
  }

  // Execution arguments pass to the spark kernel
  if (hdfs.ServAddress.find("://") == std::string::npos) {
    cmd += " hdfs://";
  }
  cmd += hdfs.ServAddress;

  if (hdfs.ServAddress.back() == '/') {
    cmd.erase(cmd.end() - 1);
  }

  cmd += ":" + std::to_string(hdfs.ServPort);
  cmd += " " + hdfs.UserName;
  cmd += " " + hdfs.WorkingDir;

  if (!execute_command(cmd.c_str(), true)) {
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t GenericProvider::execute_command(const char *command, bool print_result) {
  DP("Executing command: %s\n", command);

  FILE *fp = popen(command, "r");

  if (fp == NULL) {
    DP("Failed to execute command.\n");
    return 0;
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

  return 1;
}
