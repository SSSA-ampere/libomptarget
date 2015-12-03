#include <hdfs.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <thread>

#include "omptarget.h"
#include "../rtl.h"
#include "generic.h"

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "restclient-cpp/restclient.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define GETNAME2(name) #name
#define GETNAME(name) GETNAME2(name)
#define DP(...) DEBUGP("Target " GETNAME(TARGET_NAME) " RTL, Generic Provider:", __VA_ARGS__)

int32_t GenericProvider::send_file(const char *filename, const char *tgtfilename) {
  std::string final_name = hdfs.WorkingDir + std::string(tgtfilename);

  DP("submitting file %s as %s\n", filename, final_name.c_str());

  hdfsFile file = hdfsOpenFile(fs, final_name.c_str(), O_WRONLY, 0, 0, 0);

  if (file == NULL) {
    DP("Opening file in HDFS failed.\n%s", hdfsGetLastError());
    return -1;
  }

  std::ifstream hstfile(filename, std::ios::in|std::ios::binary);

  if (!hstfile.is_open()) {
    DP("Opening host file %s failed.", filename);
    hdfsCloseFile(fs, file);
    return -1;
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
      return -1;
    }

    if (hstfile.eof()) {
      break;
    }
  }

  hstfile.close();

  retval = hdfsCloseFile(fs, file);

  if (retval < 0) {
    DP("Closing on HDFS failed.\n%s", hdfsGetLastError());
    return -1;
  }

  return 0;
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
  if(spark.Mode == SparkMode::cluster) {
    submit_cluster();
  } else {
    submit_local();
  }
}

int32_t GenericProvider::submit_cluster() {

  // Checking if proxy option is set. If it is, set it already
  /*
  if (proxy.HostName != "") {
    std::string uri = proxy.HostName;

    if (uri.back() == '/') {
      uri.erase(uri.end() - 1);
    }

    uri += ":" + std::to_string(proxy.Port);

    RestClient::setProxy(uri, proxy.Type);
  }*/

  // Creating JSON request
  // Structure of a request to create a Spark Job:
  // {
  //   "action" : "CreateSubmissionRequest",
  //   "appArgs" : [ "hdfs://10.68.254.1:8020", "bernardo.stein", "/user/bernardo/cloud_test/" ],
  //   "appResource" : "hdfs://10.68.254.1/user/bernardo/cloud_test/test-assembly-0.1.0.jar",
  //   "clientSparkVersion" : "1.4.0",
  //   "environmentVariables" : {
  //     "SPARK_SCALA_VERSION" : "2.10",
  //     "SPARK_HOME" : "/home/bernardo/projects/spark-1.4.0-bin-hadoop2.6",
  //     "SPARK_ENV_LOADED" : "1"
  //   },
  //   "mainClass" : "org.llvm.openmp.OmpKernel",
  //   "sparkProperties" : {
  //     "spark.driver.supervise" : "false",
  //     "spark.master" : "spark://10.68.254.1:6066",
  //     "spark.app.name" : "org.llvm.openmp.OmpKernel",
  //     "spark.jars" : "hdfs://10.68.254.1/user/bernardo/cloud_test/test-assembly-0.1.0.jar"
  //   }
  // }
  // TODO: Maybe move those string constructions to the init functions
  std::string hdfsAddress = "";
  std::string hdfsResource = "";
  std::string sparkAddress = "";
  std::string sparkRESTAddress = "";

  if (hdfs.ServAddress.find("://") == std::string::npos) {
    hdfsAddress += "hdfs://";
  }

  hdfsAddress += hdfs.ServAddress;

  if (hdfs.ServAddress.back() == '/') {
    hdfsAddress.erase(hdfsAddress.end() - 1);
  }

  hdfsResource = hdfsAddress;

  hdfsAddress += ":";
  hdfsAddress += std::to_string(hdfs.ServPort);

  hdfsResource += hdfs.WorkingDir;
  if (hdfsResource.back() != '/') {
    hdfsResource += "/";
  }
  hdfsResource += "test-assembly-0.1.0.jar";

  sparkRESTAddress += "http://";

  if (spark.ServAddress.find("://") == std::string::npos) {
    sparkAddress += "spark://";
    sparkRESTAddress += spark.ServAddress;
  } else {
    sparkRESTAddress += spark.ServAddress.substr(8);
  }

  sparkAddress += spark.ServAddress;

  if (spark.ServAddress.back() == '/') {
    sparkAddress.erase(sparkAddress.end() - 1);
    sparkRESTAddress.erase(sparkRESTAddress.end() - 1);
  }
  sparkAddress += ":" + std::to_string(spark.ServPort);
  sparkRESTAddress += ":" + std::to_string(spark.ServPort);

  // Sending file to HDFS as the library to be loaded
  send_file(spark.JarPath.c_str(), "test-assembly-0.1.0.jar");

  DP("Creating JSON structure\n");
  rapidjson::Document d;
  d.SetObject();
  rapidjson::Document::AllocatorType& allocator = d.GetAllocator();

  d.AddMember("action", "CreateSubmissionRequest", allocator);

  rapidjson::Value appArgs;
  appArgs.SetArray();
  appArgs.PushBack(rapidjson::Value(hdfsAddress.c_str(), allocator), allocator);
  appArgs.PushBack(rapidjson::Value(hdfs.UserName.c_str(), allocator), allocator);
  appArgs.PushBack(rapidjson::Value(hdfs.WorkingDir.c_str(), allocator), allocator);
  d.AddMember("appArgs", appArgs, allocator);

  d.AddMember("appResource", rapidjson::Value(hdfsResource.c_str(), allocator), allocator);
  d.AddMember("clientSparkVersion", "1.5.0", allocator);
  d.AddMember("mainClass", rapidjson::Value(spark.Package.c_str(), allocator), allocator);

  rapidjson::Value environmentVariables;
  environmentVariables.SetObject();
  d.AddMember("environmentVariables", environmentVariables, allocator);

  rapidjson::Value sparkProperties;
  sparkProperties.SetObject();
  sparkProperties.AddMember("spark.driver.supervise", "false", allocator);
  sparkProperties.AddMember("spark.master", rapidjson::Value(sparkAddress.c_str(), allocator), allocator);
  sparkProperties.AddMember("spark.app.name", rapidjson::Value(spark.Package.c_str(), allocator), allocator);
  sparkProperties.AddMember("spark.jars", rapidjson::Value(hdfsResource.c_str(), allocator), allocator);
  d.AddMember("sparkProperties", sparkProperties, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  d.Accept(writer);

  RestClient::response r = RestClient::post(sparkRESTAddress + "/v1/submissions/create", "text/json", buffer.GetString());
  std::string driverid = "";

  if (r.code == 200) {
    rapidjson::Document answer;
    answer.Parse(r.body.c_str());

    assert(answer.IsObject());
    assert(answer.HasMember("success"));

    if (!answer["success"].GetBool()) {
      DP("Something bad happened when posting request.\n");
      return OFFLOAD_FAIL;
    }

    driverid = std::string(answer["submissionId"].GetString());
  } else {
    DP("Got response %d from REST server\n", r.code);
    DP("Answer: %s\n", r.body.c_str());

    return OFFLOAD_FAIL;
  }

  do {
    // Now polling the REST server until we get a good result
    DP("Requesting result from REST server\n");
    r = RestClient::get(sparkRESTAddress + "/v1/submissions/status/" + driverid);

    if (r.code == 200) {
      // Check if finished
      rapidjson::Document answer;
      answer.Parse(r.body.c_str());

      assert(answer.IsObject());
      assert(answer.HasMember("driverState"));
      assert(answer.HasMember("success"));

      if (!strcmp(answer["driverState"].GetString(), "RUNNING")) {
        std::this_thread::sleep_for(std::chrono::milliseconds(spark.PollInterval));
        continue;
      } else if (!strcmp(answer["driverState"].GetString(), "FINISHED") && answer["success"].GetBool()) {
        DP("Got response: SUCCEED!\n");
        return OFFLOAD_SUCCESS;
      } else {
        DP("Got response: FAILED!\n");
        return OFFLOAD_FAIL;
      }
    } else {
      DP("Got response %d from REST server when polling\n", r.code);
      DP("Answer: %s\n", r.body.c_str());

      return OFFLOAD_FAIL;
    }
  } while (true);

  return OFFLOAD_FAIL;

}

int32_t GenericProvider::submit_local(){
  std::string cmd = "spark-submit";

  // Spark job entry point
  cmd += " --class " + spark.Package + " " + spark.JarPath;

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

  DP("Executing command: %s\n", cmd.c_str());

  FILE *fp = popen(cmd.c_str(), "r");

  if (fp == NULL) {
    DP("Failed to start spark job.\n");
    return OFFLOAD_FAIL;
  }

  char buf[512] = {0};
  uint read = 0;

  while ((read = fread(buf, sizeof(char), 511, fp)) == 512) {
    buf[511] = 0;
    printf("    %s", buf);
  }

  buf[read] = 0;
  printf("    %s", buf);

  return OFFLOAD_SUCCESS;
}

