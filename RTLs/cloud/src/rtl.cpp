//===- RTLs/cloud/src/rtl.cpp - Target RTLs Implementation -------- C++ -*-===//
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

#include <assert.h>
#include <chrono>
#include <thread>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <hdfs.h>

#include <dlfcn.h>
#include <gelf.h>
#include <string.h>
#ifndef __APPLE__
#include <link.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#include "providers/generic.h"

#include "omptarget.h"
#include "INIReader.h"

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "restclient-cpp/restclient.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define NUMBER_OF_DEVICES 1

/// Array of Dynamic libraries loaded for this target
struct DynLibTy {
  char *FileName;
  void* Handle;
};

#define GETNAME2(name) #name
#define GETNAME(name) GETNAME2(name)
#define DP(...) DEBUGP("Target " GETNAME(TARGET_NAME) " RTL",__VA_ARGS__)


struct ResourceInfo {
  struct HdfsInfo hdfs;
  hdfsFS fs;
  struct SparkInfo spark;
  struct ProxyInfo proxy;
};


struct HdfsInfo{
  std::string ServAddress;
  int ServPort;
  std::string UserName;
  std::string WorkingDir;
  uintptr_t currAddr;
};

enum SparkMode { client, cluster, invalid };

struct SparkInfo{
  std::string ServAddress;
  int ServPort;
  SparkMode Mode;
  std::string UserName;
  std::string Package;
  std::string JarPath;
  int PollInterval;
};

struct ProxyInfo {
  std::string HostName;
  int Port;
  std::string Type;
};

#define DEFAULT_HDFS_PORT 9000
#define DEFAULT_SPARK_PORT 7077
#define DEFAULT_SPARK_MODE "client"
#define DEFAULT_SPARK_PACKAGE "org.llvm.openmp.OmpKernel"
#define DEFAULT_SPARK_JARPATH "target/scala-2.10/test-assembly-0.1.0.jar"
#define DEFAULT_SPARK_POLLINTERVAL 300
#define DEFAULT_PROXY_HOSTNAME ""

/// Keep entries table per device
struct FuncOrGblEntryTy{
  __tgt_target_table Table;
};

/// Class containing all the device information
class RTLDeviceInfoTy{
  std::vector<FuncOrGblEntryTy> FuncGblEntries;

public:
  int NumberOfDevices;

  std::vector<HdfsInfo> HdfsClusters;
  std::vector<SparkInfo> SparkClusters;
  ProxyInfo ProxyOptions;

  std::vector<hdfsFS> HdfsNodes;

  std::vector<GenericProvider*> Providers;

  // Record entry point associated with device
  void createOffloadTable(int32_t device_id, __tgt_offload_entry *begin, __tgt_offload_entry *end){
    assert( device_id < FuncGblEntries.size() && "Unexpected device id!");
    FuncOrGblEntryTy &E = FuncGblEntries[device_id];

    E.Table.EntriesBegin = begin;
    E.Table.EntriesEnd = end;
  }

  // Return true if the entry is associated with device
  bool findOffloadEntry(int32_t device_id, void *addr){
    assert( device_id < FuncGblEntries.size() && "Unexpected device id!");
    FuncOrGblEntryTy &E = FuncGblEntries[device_id];

    for(__tgt_offload_entry *i= E.Table.EntriesBegin,
                            *e= E.Table.EntriesEnd; i<e; ++i){
      if(i->addr == addr)
        return true;
    }

    return false;
  }

  // Return the pointer to the target entries table
  __tgt_target_table *getOffloadEntriesTable(int32_t device_id){
    assert( device_id < FuncGblEntries.size() && "Unexpected device id!");
    FuncOrGblEntryTy &E = FuncGblEntries[device_id];

    return &E.Table;
  }

  RTLDeviceInfoTy(){
    NumberOfDevices = 1;

    // TODO: Detect the number of clouds available

    FuncGblEntries.resize(NumberOfDevices);
    HdfsClusters.resize(NumberOfDevices);
    SparkClusters.resize(NumberOfDevices);
    HdfsNodes.resize(NumberOfDevices);
    Providers.resize(NumberOfDevices);
  }

  ~RTLDeviceInfoTy(){
    // Disconnecting clouds
    DP ("Disconnecting HDFS server(s)\n");
    for(int i=0; i<HdfsNodes.size(); i++) {
      int ret = hdfsDisconnect(HdfsNodes[i]);
      if (ret != 0) {
        DP ("Error with HDFS server %d\n", i);
      }
    }
  }

};

static RTLDeviceInfoTy DeviceInfo;


#ifdef __cplusplus
extern "C" {
#endif



int _cloud_spark_create_program() {
  // 1/ Generate scala code (with template)
  // 2/ Compile in jar -> system("sbt package");
  // 3/ Generate native kernel code (map function)
  // 4/ Compile
  // 5/ Upload to HDFS
  return 0;
}

int32_t send_file_to_hdfs(int32_t device_id, const char *filename, const char *tgtfilename) {
  DP("Submitting file for device %d\n", device_id);

  hdfsFS &fs = DeviceInfo.HdfsNodes[device_id];
  HdfsInfo hdfs = DeviceInfo.HdfsClusters[device_id];

  // TODO: Assuming the target runs Linux
  std::string stgtfilename(tgtfilename);
  std::string libraryFile = (hdfs.WorkingDir + stgtfilename);


  DP("Writing data in file '%s'\n", libraryFile.c_str());

  hdfsFile file = hdfsOpenFile(fs, libraryFile.c_str(), O_WRONLY, 0, 0, 0);
  if(file == NULL) {
    DP("Opening failed.\n%s", hdfsGetLastError());
    return -1;
  }

  std::ifstream hstfile(filename, std::ios::in|std::ios::binary);

  if (!hstfile.is_open()) {
    DP("Opening host file %s failed.", filename);
    hdfsCloseFile(fs, file);
    return -1;
  }

  DP("Reading...\n");

  // TODO: Malloc this buffer
  char buffer[4096] = {0};
  int retval;

  while (true) {
    hstfile.read(buffer, 4096);

    if (!hstfile.good()) {
      if (!hstfile.eof()) {
        break;
      }
    }

    retval = hdfsWrite(fs, file, buffer, hstfile.gcount());
    if(retval < 0) {
      DP("Writing failed.\n%s", hdfsGetLastError());
      hdfsCloseFile(fs, file);
      return -1;
    }

    if (hstfile.eof()) {
      break;
    }
  }

  hstfile.close();

  retval = hdfsCloseFile(fs, file);
  if(retval < 0) {
    DP("Closing failed.\n%s", hdfsGetLastError());
    return -1;
  }

  return 0;
}

int __tgt_rtl_device_type(int32_t device_id){

  return 0;
}

int __tgt_rtl_number_of_devices(){
  return DeviceInfo.NumberOfDevices;
}

int32_t __tgt_rtl_init_device(int32_t device_id){
  int retval;

  DP ("Getting device %d\n", device_id);

  // Parsing configuration
  INIReader reader("cloud_rtl.ini");

  if (reader.ParseError() < 0) {
    DP("Couldn't find 'cloud_rtl.ini'!");
    return OFFLOAD_FAIL;
  }

  HdfsInfo hdfs {
    reader.Get("HDFS", "HostName", ""),
    (int) reader.GetInteger("HDFS", "Port", DEFAULT_HDFS_PORT),
    reader.Get("HDFS", "User", ""),
    reader.Get("HDFS", "WorkingDir", ""),
    1,
  };

  if (!hdfs.ServAddress.compare("") ||
      !hdfs.UserName.compare("")) {
    DP("Invalid values in 'cloud_rtl.ini' for HDFS!");
    return OFFLOAD_FAIL;
  }

  DP("HDFS HostName: '%s' - Port: '%d' - User: '%s' - WorkingDir: '%s'\n",
     hdfs.ServAddress.c_str(), hdfs.ServPort, hdfs.UserName.c_str(),
     hdfs.WorkingDir.c_str());

  // Checking if given WorkingDir ends in a slash for path concatenation.
  // If it doesn't, add it
  if (hdfs.WorkingDir.back() != '/') {
    hdfs.WorkingDir += "/";
  }

  // Init connection to HDFS cluster
  struct hdfsBuilder *builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, hdfs.ServAddress.c_str());
  hdfsBuilderSetNameNodePort(builder, hdfs.ServPort);
  hdfsBuilderSetUserName(builder, hdfs.UserName.c_str());
  hdfsFS fs = hdfsBuilderConnect(builder);
  if(fs == NULL) {
    DP("Connection problem with HDFS cluster. Check your configuration in 'cloud_rtl.ini'.\n");
    return OFFLOAD_FAIL;
  }

  hdfsFreeBuilder(builder);
  DeviceInfo.HdfsNodes[device_id] = fs;

  if(hdfsExists(fs, hdfs.WorkingDir.c_str()) < 0) {
    retval = hdfsCreateDirectory(fs, hdfs.WorkingDir.c_str());
    if(retval < 0) {
      DP("%s", hdfsGetLastError());
      return OFFLOAD_FAIL;
    }
  }

  // TODO: Check connection to Apache Spark cluster

  SparkMode mode;
  std::string smode = reader.Get("Spark", "Mode", DEFAULT_SPARK_MODE);
  if(smode == "client") {
    mode = SparkMode::client;
  } else if (smode == "cluster") {
    mode = SparkMode::cluster;
  } else {
    mode = SparkMode::invalid;
  }

  SparkInfo spark {
    reader.Get("Spark", "HostName", ""),
    (int) reader.GetInteger("Spark", "Port", DEFAULT_SPARK_PORT),
    mode,
    reader.Get("Spark", "User", ""),
    reader.Get("Spark", "Package", DEFAULT_SPARK_PACKAGE),
    reader.Get("Spark", "JarPath", DEFAULT_SPARK_JARPATH),
    (int) reader.GetInteger("Spark", "PollInterval", DEFAULT_SPARK_POLLINTERVAL),
  };

  if (spark.Mode == SparkMode::invalid ||
      !spark.Package.compare("") ||
      !spark.JarPath.compare("")) {
    DP("Invalid values in 'cloud_rtl.ini' for Spark!");
    return OFFLOAD_FAIL;
  }

  DP("Spark HostName: '%s' - Port: '%d' - User: '%s' - Mode: %s\n",
     spark.ServAddress.c_str(), spark.ServPort, spark.UserName.c_str(), smode.c_str());
  DP("Jar: %s - Class: %s\n", spark.JarPath.c_str(), spark.Package.c_str());

  // Checking proxy options
  ProxyInfo proxy {
    reader.Get("Proxy", "HostName", DEFAULT_PROXY_HOSTNAME),
    (int) reader.GetInteger("Proxy", "Port", 0),
    reader.Get("Proxy", "Type", ""),
  };

  DeviceInfo.HdfsClusters[device_id] = hdfs;
  DeviceInfo.SparkClusters[device_id] = spark;
  DeviceInfo.ProxyOptions = proxy;

  return OFFLOAD_SUCCESS; // success
}

__tgt_target_table *__tgt_rtl_load_binary(int32_t device_id, __tgt_device_image *image){
  DP("Dev %d: load binary from 0x%llx image\n", device_id,
      (long long)image->ImageStart);

  assert(device_id>=0 && device_id<NUMBER_OF_DEVICES && "bad dev id");

  size_t ImageSize = (size_t)image->ImageEnd - (size_t)image->ImageStart;
  size_t NumEntries = (size_t) (image->EntriesEnd - image->EntriesBegin);
  DP("Expecting to have %ld entries defined.\n", (long)NumEntries);

  // We do not need to set the ELF version because the caller of this function
  // had to do that to decide the right runtime to use

  //Obtain elf handler
  Elf *e = elf_memory ((char*)image->ImageStart, ImageSize);
  if(!e){
    DP("Unable to get ELF handle: %s!\n", elf_errmsg(-1));
    return NULL;
  }

  if( elf_kind(e) !=  ELF_K_ELF){
    DP("Invalid Elf kind!\n");
    elf_end(e);
    return NULL;
  }

  //Find the entries section offset
  Elf_Scn *section = 0;
  Elf64_Off entries_offset = 0;

  size_t shstrndx;

  if (elf_getshdrstrndx (e , &shstrndx )) {
    DP("Unable to get ELF strings index!\n");
    elf_end(e);
    return NULL;
  }

  while ((section = elf_nextscn(e,section))) {
    GElf_Shdr hdr;
    gelf_getshdr(section, &hdr);

//    if (!strcmp(elf_strptr(e,shstrndx,hdr.sh_name),".openmptgt_host_entries")){
    if (!strcmp(elf_strptr(e,shstrndx,hdr.sh_name),".omptgt_hst_entr")){
      entries_offset = hdr.sh_addr;
      break;
    }
  }

  if (!entries_offset) {
    DP("Entries Section Offset Not Found\n");
    elf_end(e);
    return NULL;
  }

  DP("Offset of entries section is (%016lx).\n", entries_offset);

  // load dynamic library and get the entry points. We use the dl library
  // to do the loading of the library, but we could do it directly to avoid the
  // dump to the temporary file.
  //
  // 1) Create tmp file with the library contents
  // 2) Use dlopen to load the file and dlsym to retrieve the symbols
  char tmp_name[] = "/tmp/tmpfile_XXXXXX";
  int tmp_fd = mkstemp (tmp_name);

  if (tmp_fd == -1) {
    elf_end(e);
    return NULL;
  }

  FILE *ftmp = fdopen(tmp_fd, "wb");

  if(!ftmp) {
    elf_end(e);
    return NULL;
  }

  fwrite(image->ImageStart, ImageSize, 1, ftmp);
  fclose(ftmp);

  DP("Trying to send lib to HDFS\n");

  // Sending file to HDFS as the library to be loaded
  send_file_to_hdfs(device_id, tmp_name, "libmr.so");

  DP("Lib sent to HDFS!\n");

  DynLibTy Lib = { tmp_name, dlopen(tmp_name, RTLD_LAZY) };

  if (!Lib.Handle) {
    DP("target library loading error: %s\n",dlerror());
    elf_end(e);
    return NULL;
  }

#ifndef __APPLE__
  struct link_map *libInfo = (struct link_map *)Lib.Handle;

  // The place where the entries info is loaded is the library base address
  // plus the offset determined from the ELF file.
  Elf64_Addr entries_addr = libInfo->l_addr + entries_offset;

  DP("Pointer to first entry to be loaded is (%016lx).\n", entries_addr);

  // Table of pointers to all the entries in the target
  __tgt_offload_entry *entries_table = (__tgt_offload_entry*)entries_addr;

  __tgt_offload_entry *entries_begin = &entries_table[0];

  DP("Entry begin: (%016lx)\nEntry name: (%s)\nEntry size: (%016lx)\n",
     (uintptr_t)entries_begin->addr, entries_begin->name, entries_begin->size);
  //DP("Next entry: (%016lx)\nEntry name: (%s)\nEntry size: (%016lx)\n",
  //   (uintptr_t)entries_table[1].addr, entries_table[1].name, entries_table[1].size);

  __tgt_offload_entry *entries_end =  entries_begin + NumEntries;

  if(!entries_begin){
    DP("Can't obtain entries begin\n");
    elf_end(e);
    return NULL;
  }

  DP("Entries table range is (%016lx)->(%016lx)\n",(Elf64_Addr)entries_begin,(Elf64_Addr)entries_end)
  DeviceInfo.createOffloadTable(device_id,entries_begin,entries_end);

  elf_end(e);

#endif

  return DeviceInfo.getOffloadEntriesTable(device_id);
}

void *__tgt_rtl_data_alloc(int32_t device_id, int64_t size, int32_t type, int32_t id){
  // NOTE: we do not create the HDFS file here because we do not want to
  // waste time creating stuff that we might not need before (there may be
  // unecessary allocations)

  HdfsInfo hdfs = DeviceInfo.HdfsClusters[device_id];

  // Return fake target address
  return (void *)hdfs.currAddr++;
}

int32_t __tgt_rtl_data_submit(int32_t device_id, void *tgt_ptr, void *hst_ptr, int64_t size, int32_t id){
  if(id < 0) {
    DP("Not need to submit pointer for device %d\n", device_id);
    return OFFLOAD_SUCCESS;
  }

  // Since we now need the hdfs file, we create it here
  DP("Submitting data for device %d\n", device_id);

  HdfsInfo hdfs = DeviceInfo.HdfsClusters[device_id];
  hdfsFS &fs = DeviceInfo.HdfsNodes[device_id];

  std::string filename = hdfs.WorkingDir + std::to_string(id);

  DP("Writing data in file '%s'\n", filename.c_str());

  hdfsFile file = hdfsOpenFile(fs, filename.c_str(), O_WRONLY, 0, 0, 0);
  if(file == NULL) {
    DP("Opening failed.\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  int retval = hdfsWrite(fs, file, hst_ptr, size);
  if(retval < 0) {
    DP("Writing failed.\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  retval = hdfsCloseFile(fs, file);
  if(retval < 0) {
    DP("Closing failed.\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_data_retrieve(int32_t device_id, void *hst_ptr, void *tgt_ptr, int64_t size, int32_t id){

  HdfsInfo hdfs = DeviceInfo.HdfsClusters[device_id];
  hdfsFS &fs = DeviceInfo.HdfsNodes[device_id];

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

int32_t __tgt_rtl_data_delete(int32_t device_id, void* tgt_ptr, int32_t id){
  if(id < 0) {
    DP("No file to delete\n");
    return OFFLOAD_SUCCESS;
  }

  HdfsInfo hdfs = DeviceInfo.HdfsClusters[device_id];
  hdfsFS &fs = DeviceInfo.HdfsNodes[device_id];

  std::string filename = hdfs.WorkingDir + std::to_string(id);

  DP("Deleting file '%s'\n", filename.c_str());

  int retval = hdfsDelete(fs, filename.c_str(), 0);
  if(retval < 0) {
    DP("Deleting file failed.\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_run_target_team_region(int32_t device_id, void *tgt_entry_ptr,
   void **tgt_args, int32_t arg_num, int32_t team_num, int32_t thread_limit)
{
  HdfsInfo hdfs = DeviceInfo.HdfsClusters[device_id];
  SparkInfo spark = DeviceInfo.SparkClusters[device_id];
  ProxyInfo proxy = DeviceInfo.ProxyOptions;

  // Checking if proxy option is set. If it is, set it already
  if (proxy.HostName != "") {
    std::string uri = proxy.HostName;

    if (uri.back() == '/') {
      uri.erase(uri.end() - 1);
    }

    uri += ":" + std::to_string(proxy.Port);

    RestClient::setProxy(uri, proxy.Type);
  }

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
  send_file_to_hdfs(device_id, spark.JarPath.c_str(), "test-assembly-0.1.0.jar");

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
      } else {
        if (!strcmp(answer["driverState"].GetString(), "FINISHED") && answer["success"].GetBool()) {
          return OFFLOAD_SUCCESS;
        } else {
          return OFFLOAD_FAIL;
        }
      }
    } else {
      DP("Got response %d from REST server when polling\n", r.code);
      DP("Answer: %s\n", r.body.c_str());

      return OFFLOAD_FAIL;
    }
  } while (true);

  return OFFLOAD_FAIL;
}

int32_t __tgt_rtl_run_target_region(int32_t device_id, void *tgt_entry_ptr,
  void **tgt_args, int32_t arg_num)
{
  // use one team and one thread
  return __tgt_rtl_run_target_team_region(device_id, tgt_entry_ptr,
    tgt_args, arg_num, 1, 1);
}

#ifdef __cplusplus
}
#endif
