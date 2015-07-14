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
#include <hdfs.h>

#include <dlfcn.h>
#include <elf.h>
#include <ffi.h>
#include <gelf.h>
#include <string.h>
#include <link.h>
#include <stdio.h>
#include <stdlib.h>

#include "omptarget.h"

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
  char *ServAddress;
  int ServPort;
  char *UserName;
};

struct SparkInfo{
  char *Name;
  char *Url;
  int Port;
};


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

int _cloud_spark_launch() {
  // FIXME: hardcoded execution
  FILE *fp;

  fp = popen("/home/bernardo/projects/spark-1.4.0-bin-hadoop2.6/bin/spark-submit --class test.dummy.HdfsTest /home/bernardo/projects/cloud_test/test/target/scala-2.10/test_2.10-0.1.0.jar", "r");

  if (fp == NULL) {
      DP("Failed to start spark job.\n");
      return 1;
  }

  char buf[512] = {0};
  uint read = 0;

  while ((read = fread(buf, sizeof(char), 511, fp)) == 512) {
    buf[511] = 0;
    printf("    %s", buf);
  }

  buf[read] = 0;
  printf("    %s", buf);

  return 0;
}

int __tgt_rtl_device_type(int32_t device_id){

  return 0;
}

int __tgt_rtl_number_of_devices(){
  return DeviceInfo.NumberOfDevices;
}

int32_t __tgt_rtl_init_device(int32_t device_id){
  DP ("Getting device %d\n", device_id);

  //dfsInfo hdfs = DeviceInfo.HdfsClusters[device_id];
  //SparkInfo spark = DeviceInfo.SparkClusters[device_id];

  // Init connection to HDFS cluster
  struct hdfsBuilder *builder = hdfsNewBuilder();
  //hdfsBuilderSetNameNode(builder, hdfs.ServAddress);
  //hdfsBuilderSetNameNodePort(builder, hdfs.ServPort);
  //hdfsBuilderSetUserName(builder, hdfs.UserName);
  hdfsBuilderSetNameNode(builder, "localhost");
  hdfsBuilderSetNameNodePort(builder, 9000);
  hdfsBuilderSetUserName(builder, "bernardo");
  DeviceInfo.HdfsNodes[device_id] = hdfsBuilderConnect(builder);
  hdfsFreeBuilder(builder);

  // TODO: Init connection to Apache Spark cluster

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

  if( tmp_fd == -1 ){
    elf_end(e);
    return NULL;
  }

  FILE *ftmp = fdopen(tmp_fd, "wb");

  if( !ftmp ){
    elf_end(e);
    return NULL;
  }

  fwrite(image->ImageStart,ImageSize,1,ftmp);
  fclose(ftmp);

  DynLibTy Lib = { tmp_name, dlopen(tmp_name,RTLD_LAZY) };

  if(!Lib.Handle){
    DP("target library loading error: %s\n",dlerror());
    elf_end(e);
    return NULL;
  }

  struct link_map *libInfo = (struct link_map *)Lib.Handle;

  // The place where the entries info is loaded is the library base address
  // plus the offset determined from the ELF file.
  Elf64_Addr entries_addr = libInfo->l_addr + entries_offset;

  DP("Pointer to first entry to be loaded is (%016lx).\n", entries_addr);

  // Table of pointers to all the entries in the target
  __tgt_offload_entry *entries_table = (__tgt_offload_entry*)entries_addr;


  __tgt_offload_entry *entries_begin = &entries_table[0];
  __tgt_offload_entry *entries_end =  entries_begin + NumEntries;

  if(!entries_begin){
    DP("Can't obtain entries begin\n");
    elf_end(e);
    return NULL;
  }

  DP("Entries table range is (%016lx)->(%016lx)\n",(Elf64_Addr)entries_begin,(Elf64_Addr)entries_end)
  DeviceInfo.createOffloadTable(device_id,entries_begin,entries_end);

  elf_end(e);

  return DeviceInfo.getOffloadEntriesTable(device_id);
}

void *__tgt_rtl_data_alloc(int32_t device_id, int64_t size){
  // TODO: create HDFS file
  return (void *)0x30303030;
}

int32_t __tgt_rtl_data_submit(int32_t device_id, void *tgt_ptr, void *hst_ptr, int64_t size){
  if (size < 100) {
    return OFFLOAD_SUCCESS;
  }

  DP("Submitting data for device %d\n", device_id);

  hdfsFS &fs = DeviceInfo.HdfsNodes[device_id];
  hdfsFile file = hdfsOpenFile(fs, "/user/bernardo/cloud_test/A", O_WRONLY, 0, 0, 0);
  int retval = hdfsWrite(fs, file, hst_ptr, size);
  retval = hdfsCloseFile(fs, file);
  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_data_retrieve(int32_t device_id, void *hst_ptr, void *tgt_ptr, int64_t size){
  hdfsFS &fs = DeviceInfo.HdfsNodes[device_id];
  hdfsFile file = hdfsOpenFile(fs, "/user/bernardo/cloud_test/sum", O_RDONLY, 0, 0, 0);
  int retval = hdfsRead(fs, file, hst_ptr, size);
  retval = hdfsCloseFile(fs, file);
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
  // run hardcoded spark kernel
  _cloud_spark_launch();

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
