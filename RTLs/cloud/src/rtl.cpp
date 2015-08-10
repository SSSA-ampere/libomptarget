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
#include <vector>
#include <unordered_map>
#include <hdfs.h>

#include <dlfcn.h>
//#include <elf.h>
#include <gelf.h>
#include <string.h>
//#include <link.h>
#include <stdio.h>
#include <stdlib.h>

#include "omptarget.h"
#include "INIReader.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define NUMBER_OF_DEVICES 1

/// Array of Dynamic libraries loaded for this target
struct DynLibTy{
  char *FileName;
  void* Handle;
};

#define GETNAME2(name) #name
#define GETNAME(name) GETNAME2(name)
#define DP(...) DEBUGP("Target " GETNAME(TARGET_NAME) " RTL",__VA_ARGS__)

struct HdfsInfo{
  std::string ServAddress;
  int ServPort;
  std::string UserName;
  std::string WorkingDir;
};

struct SparkInfo{
  char *Name;
  char *Url;
  int Port;
};

struct JobInfo{
  std::string Package;
  std::string JarPath;
};


// Configuration variables
HdfsInfo testHdfs = {"", 0, "", ""};
JobInfo sparkJob = {"", ""};




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

  std::vector<hdfsFS> HdfsNodes;
  // TODO: I believe the best way would be for HdfsAddresses to be an unordered_map as
  // well. That way, we can map each hdfsFS to their corresponding addresses
  // table. However, that would require hdfsFS to be hasheable - see documentation
  // on unordered_map to know more
  std::vector<std::unordered_map<uintptr_t, std::string>> HdfsAddresses;

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
    HdfsAddresses.resize(NumberOfDevices);
  }

  ~RTLDeviceInfoTy(){
    // Disconnecting clouds
    for(int i=0; i<HdfsNodes.size(); i++) {
      if(HdfsNodes[i]) {
        int err = hdfsDisconnect(HdfsNodes[i]);
        if (err != 0) {
          DP ("Error when disconnecting HDFS server\n");
        }
      }
    }

    // TODO: clear map of addresses
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

int __tgt_rtl_device_type(int32_t device_id){

  return 0;
}

int __tgt_rtl_number_of_devices(){
  return DeviceInfo.NumberOfDevices;
}

int32_t __tgt_rtl_init_device(int32_t device_id){
  int retval;

  DP ("Getting device %d\n", device_id);

  //dfsInfo hdfs = DeviceInfo.HdfsClusters[device_id];
  //SparkInfo spark = DeviceInfo.SparkClusters[device_id];

  // Parsing configuration
  INIReader reader("cloud_rtl.ini");

  if (reader.ParseError() < 0) {
    DP("Couldn't find 'cloud_rtl.ini'!");
    return OFFLOAD_FAIL;
  }

  testHdfs.ServAddress = reader.Get("HDFS", "HostName", "");
  testHdfs.ServPort = reader.GetInteger("HDFS", "Port", 0);
  testHdfs.UserName = reader.Get("HDFS", "User", "");
  testHdfs.WorkingDir = reader.Get("HDFS", "WorkingDir", "");

  sparkJob.Package = reader.Get("Spark", "Package", "");
  sparkJob.JarPath = reader.Get("Spark", "JarPath", "");

  if (!testHdfs.ServAddress.compare("") ||
      (testHdfs.ServPort == 0) ||
      !testHdfs.UserName.compare("")) {
    DP("Invalid values in 'cloud_rtl.ini' for HDFS!");
    DP("Current values:\nHostName: '%s'\nPort: '%d'\nUser: '%s'\nWorkingDir: '%s'",
       testHdfs.ServAddress.c_str(), testHdfs.ServPort, testHdfs.UserName.c_str(),
       testHdfs.WorkingDir.c_str());
    return OFFLOAD_FAIL;
  }

  if (!sparkJob.Package.compare("") ||
      !sparkJob.JarPath.compare("")) {
    DP("Invalid values in 'cloud_rtl.ini' for Spark!");
    return OFFLOAD_FAIL;
  }

  // Checking if given WorkingDir ends in a slash for path concatenation.
  // If it doesn't, add it
  if (testHdfs.WorkingDir.back() != '/') {
    testHdfs.WorkingDir += "/";
  }

  // Init connection to HDFS cluster
  struct hdfsBuilder *builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, testHdfs.ServAddress.c_str());
  hdfsBuilderSetNameNodePort(builder, testHdfs.ServPort);
  hdfsBuilderSetUserName(builder, testHdfs.UserName.c_str());
  hdfsFS fs = hdfsBuilderConnect(builder);
  if(fs == NULL) {
    return OFFLOAD_FAIL;
  }

  hdfsFreeBuilder(builder);
  DeviceInfo.HdfsNodes[device_id] = fs;

  // TODO: Init connection to Apache Spark cluster

  if(hdfsExists(fs, testHdfs.WorkingDir.c_str()) < 0) {
    retval = hdfsCreateDirectory(fs, testHdfs.WorkingDir.c_str());
    if(retval < 0) {
      DP("%s", hdfsGetLastError());
      return OFFLOAD_FAIL;
    }
  }

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

return NULL;
}

void *__tgt_rtl_data_alloc(int32_t device_id, int64_t size){
  // NOTE: we do not create the HDFS file here because we do not want to
  // waste time creating stuff that we might not need before (there may be
  // unecessary allocations)
  std::unordered_map<uintptr_t, std::string> &currmapping = DeviceInfo.HdfsAddresses[device_id];

  // TODO: there's certainly a better way to do this
  uintptr_t highest = 0;
  for (auto &itr : currmapping) {
    if (itr.first > highest) {
      highest = itr.first;
    }
  }

  highest += 1;

  // TODO: something to do with the size too?

  // TODO: get basename from hdfs address or from local configuration from user
  std::string filename = testHdfs.WorkingDir + std::to_string(highest);
  currmapping.emplace(highest, filename);

  return (void *)highest;
}

int32_t __tgt_rtl_data_submit(int32_t device_id, void *tgt_ptr, void *hst_ptr, int64_t size){
  // Since we now need the hdfs file, we create it here
  DP("Submitting data for device %d\n", device_id);

  hdfsFS &fs = DeviceInfo.HdfsNodes[device_id];
  std::unordered_map<uintptr_t, std::string> &currmapping = DeviceInfo.HdfsAddresses[device_id];
  uintptr_t targetaddr = (uintptr_t)tgt_ptr;
  std::string filename = currmapping[targetaddr];

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

int32_t __tgt_rtl_data_retrieve(int32_t device_id, void *hst_ptr, void *tgt_ptr, int64_t size){
  hdfsFS &fs = DeviceInfo.HdfsNodes[device_id];
  std::unordered_map<uintptr_t, std::string> &currmapping = DeviceInfo.HdfsAddresses[device_id];
  uintptr_t targetaddr = (uintptr_t)tgt_ptr;
  std::string filename = currmapping[targetaddr];

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

int32_t __tgt_rtl_data_delete(int32_t device_id, void* tgt_ptr){
  // TODO: remove HDFS file
  /*hdfsFS &fs = DeviceInfo.HdfsNodes[device_id];
  hdfsFile file = hdfsOpenFile(fs, "", O_WRONLY, 0, 0, 0);
  hdfsDelete(fs, "", 0);*/
  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_run_target_team_region(int32_t device_id, void *tgt_entry_ptr,
   void **tgt_args, int32_t arg_num, int32_t team_num, int32_t thread_limit)
{
  int retval = 0;

  // run hardcoded spark kernel
  // Before launching, write address table in special file which will be read by
  // the scala kernel
  // TODO: create a function to create a file and write data to it
  hdfsFS &fs = DeviceInfo.HdfsNodes[device_id];

  std::string addressFile = (testHdfs.WorkingDir + "__address_table");

  hdfsFile file = hdfsOpenFile(fs, addressFile.c_str(), O_WRONLY, 0, 0, 0);
  if (file == NULL) {
    DP("Couldn't create address table file!\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  std::unordered_map<uintptr_t, std::string> &currmapping = DeviceInfo.HdfsAddresses[device_id];


  for (auto &itr : currmapping) {
    retval = hdfsWrite(fs, file, &(itr.first), sizeof(uintptr_t));
    if(retval < 0) {
      DP("Couldn't write address table!\n%s", hdfsGetLastError());
      return OFFLOAD_FAIL;
    }

    retval = hdfsWrite(fs, file, itr.second.c_str(), itr.second.length() + 1);
    if(retval < 0) {
      DP("Couldn't write address table!\n%s", hdfsGetLastError());
      return OFFLOAD_FAIL;
    }
  }

  retval = hdfsCloseFile(fs, file);
  if(retval < 0) {
    DP("Error when creating address table in HDFS.\n%s", hdfsGetLastError());
    return OFFLOAD_FAIL;
  }

  DP("Wrote address table in HDFS.\n");

  // FIXME: hardcoded execution
  std::string cmd = "spark-submit --class " + sparkJob.Package + " " + sparkJob.JarPath;
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
