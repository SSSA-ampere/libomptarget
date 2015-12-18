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
#include "rtl.h"

#include "omptarget.h"
#include "INIReader.h"

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "restclient-cpp/restclient.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define GETNAME(name) #name
#define DP(...) DEBUGP("Target " GETNAME(TARGET_NAME) " RTL",__VA_ARGS__)

static RTLDeviceInfoTy DeviceInfo;

struct ProviderListEntry ProviderList[] = {
  {"Generic", createGenericProvider, "GenericProvider"},
  {"Google", NULL, "GoogleProvider"}
};

#define NUMBER_OF_DEVICES 1

RTLDeviceInfoTy::RTLDeviceInfoTy() {
  NumberOfDevices = 1;

  // TODO: Detect the number of clouds available
  FuncGblEntries.resize(NumberOfDevices);
  HdfsClusters.resize(NumberOfDevices);
  SparkClusters.resize(NumberOfDevices);
  HdfsNodes.resize(NumberOfDevices);
  Providers.resize(NumberOfDevices);

  // Parsing proxy configuration, if exists
  INIReader reader(DEFAULT_CLOUD_RTL_CONF_FILE);

  if (reader.ParseError() < 0) {
    DP("Couldn't find '%s'!", DEFAULT_CLOUD_RTL_CONF_FILE);
  } else {
    ProxyInfo proxy {
      reader.Get("Proxy", "HostName", DEFAULT_PROXY_HOSTNAME),
      (int) reader.GetInteger("Proxy", "Port", DEFAULT_PROXY_PORT),
      reader.Get("Proxy", "Type", DEFAULT_PROXY_TYPE),
    };

    ProxyOptions = proxy;
  }
}

RTLDeviceInfoTy::~RTLDeviceInfoTy(){
  // Disconnecting clouds
  DP ("Disconnecting HDFS server(s)\n");
  for(int i=0; i<HdfsNodes.size(); i++) {
    int ret = hdfsDisconnect(HdfsNodes[i]);
    if (ret != 0) {
      DP ("Error with HDFS server %d\n", i);
    }
  }
}

void RTLDeviceInfoTy::createOffloadTable(int32_t device_id, __tgt_offload_entry *begin, __tgt_offload_entry *end){
  assert( device_id < FuncGblEntries.size() && "Unexpected device id!");
  FuncOrGblEntryTy &E = FuncGblEntries[device_id];

  E.Table.EntriesBegin = begin;
  E.Table.EntriesEnd = end;
}

bool RTLDeviceInfoTy::findOffloadEntry(int32_t device_id, void *addr){
  assert( device_id < FuncGblEntries.size() && "Unexpected device id!");
  FuncOrGblEntryTy &E = FuncGblEntries[device_id];

  for(__tgt_offload_entry *i= E.Table.EntriesBegin,
                          *e= E.Table.EntriesEnd; i<e; ++i){
    if(i->addr == addr)
      return true;
  }

  return false;
}

__tgt_target_table *RTLDeviceInfoTy::getOffloadEntriesTable(int32_t device_id){
  assert( device_id < FuncGblEntries.size() && "Unexpected device id!");
  FuncOrGblEntryTy &E = FuncGblEntries[device_id];

  return &E.Table;
}

#ifdef __cplusplus
extern "C" {
#endif

int __tgt_rtl_device_type(int32_t device_id){
  return 0;
}

int __tgt_rtl_number_of_devices(){
  return DeviceInfo.NumberOfDevices;
}

int32_t __tgt_rtl_init_device(int32_t device_id){
  int retval;

  DP("Initializing device %d\n", device_id);

  // Parsing configurations
  INIReader reader(DEFAULT_CLOUD_RTL_CONF_FILE);

  if (reader.ParseError() < 0) {
    DP("Couldn't find '%s'!", DEFAULT_CLOUD_RTL_CONF_FILE);
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

  if (fs == NULL) {
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

  DeviceInfo.HdfsClusters[device_id] = hdfs;
  DeviceInfo.SparkClusters[device_id] = spark;

  ResourceInfo resources {
    hdfs,
    fs,
    spark,
    DeviceInfo.ProxyOptions,
  };

  // Checking for listed provider. Each device id refers to a provider position
  // in the list
  DP("Creating provider %s", ProviderList[device_id].ProviderName.c_str());
  std::string providerSectionName = ProviderList[device_id].SectionName;
  DeviceInfo.Providers[device_id] = ProviderList[device_id].ProviderGenerator(resources);
  DeviceInfo.Providers[device_id]->parse_config(reader);

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
  int tmp_fd = mkstemp(tmp_name);

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
  DeviceInfo.Providers[device_id]->send_file(tmp_name, "libmr.so");

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
  return DeviceInfo.Providers[device_id]->data_alloc(size, type, id);
}

int32_t __tgt_rtl_data_submit(int32_t device_id, void *tgt_ptr, void *hst_ptr, int64_t size, int32_t id){
  return DeviceInfo.Providers[device_id]->data_submit(tgt_ptr, hst_ptr, size, id);
}

int32_t __tgt_rtl_data_retrieve(int32_t device_id, void *hst_ptr, void *tgt_ptr, int64_t size, int32_t id){
  return DeviceInfo.Providers[device_id]->data_retrieve(hst_ptr, tgt_ptr, size, id);
}

int32_t __tgt_rtl_data_delete(int32_t device_id, void* tgt_ptr, int32_t id){
  return DeviceInfo.Providers[device_id]->data_delete(tgt_ptr, id);
}

int32_t __tgt_rtl_run_target_team_region(int32_t device_id, void *tgt_entry_ptr,
    void **tgt_args, int32_t arg_num, int32_t team_num, int32_t thread_limit) {
  return DeviceInfo.Providers[device_id]->submit_job();
}

int32_t __tgt_rtl_run_target_region(int32_t device_id, void *tgt_entry_ptr,
    void **tgt_args, int32_t arg_num) {
  // use one team and one thread
  return __tgt_rtl_run_target_team_region(device_id, tgt_entry_ptr,
    tgt_args, arg_num, 1, 1);
}

#ifdef __cplusplus
}
#endif
