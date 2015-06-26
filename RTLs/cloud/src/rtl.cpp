//===-RTLs/generic-64bit/src/rtl.cpp - Target RTLs Implementation - C++ -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// RTL for generic 64-bit machine
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <assert.h>
#include <cstdio>
#include <dlfcn.h>
#include <gelf.h>
#include <list>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <hdfs.h>

#include "omptarget.h"

#ifndef TARGET_NAME
#define TARGET_NAME Generic-64bit
#endif

#define GETNAME2(name) #name
#define GETNAME(name) GETNAME2(name)
#define DP(...) DEBUGP("Target " GETNAME(TARGET_NAME) " RTL",__VA_ARGS__)

#define NUMBER_OF_DEVICES 4

typedef struct {
    char *servAddress;
    int servPort;
    char *userName;
    void *node;
} cloud_hdfs_t;

typedef struct {
    char *name;
    char *url;
    int port;
} cloud_spark_t;


/// Keep entries table per device
struct FuncOrGblEntryTy{
  __tgt_target_table Table;
};

/// Class containing all the device information
class RTLDeviceInfoTy{
  std::vector<FuncOrGblEntryTy> FuncGblEntries;

public:

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

  RTLDeviceInfoTy(int32_t num_devices){
    FuncGblEntries.resize(num_devices);
  }

  ~RTLDeviceInfoTy(){
    // Close dynamic libraries

  }
};

static RTLDeviceInfoTy DeviceInfo(NUMBER_OF_DEVICES);


#ifdef __cplusplus
extern "C" {
#endif

cloud_hdfs_t* _cloud_hdfs_init(char *servAddress, int servPort, char* userName) {
    cloud_hdfs_t *hdfs = (cloud_hdfs_t*) malloc(sizeof(cloud_hdfs_t));
    hdfs->servAddress = servAddress;
    hdfs->servPort = servPort;
    hdfs->userName = userName;
    return hdfs;
}

int _cloud_hdfs_connect(cloud_hdfs_t *cloud) {
    struct hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, cloud->servAddress);
    hdfsBuilderSetNameNodePort(builder, cloud->servPort);
    hdfsBuilderSetUserName(builder, cloud->userName);
    cloud->node = hdfsBuilderConnect(builder);
    hdfsFreeBuilder(builder);
    return 0;
}

int _cloud_hdfs_disconnect(cloud_hdfs_t *cloud) {
    hdfsFS fs = (hdfsFS) cloud->node;
    return hdfsDisconnect(fs);
}

int _cloud_hdfs_send(cloud_hdfs_t *cloud, char *id, void *buffer, int bufSize) {
    hdfsFS fs = (hdfsFS) cloud->node;
    hdfsFile file = hdfsOpenFile(fs, id, O_WRONLY, 0, 0, 0);

    int retval = hdfsWrite(fs, file, buffer, bufSize);
    retval = hdfsCloseFile(fs, file);
    return 0;
}

int _cloud_hdfs_send_int(cloud_hdfs_t *cloud, char *id, int *buffer, int bufSize) {
    hdfsFS fs = (hdfsFS) cloud->node;
    hdfsFile file = hdfsOpenFile(fs, id, O_WRONLY, 0, 0, 0);
    int i;
    int retval;
    for(i=0; i<bufSize; i++) {
        char str[10] = "";
        sprintf(str, "%*d ", 8, buffer[i]);
        retval = hdfsWrite(fs, file, str, 10*sizeof(char));

        //hdfsFlush(fs, file);
    }

    retval = hdfsCloseFile(fs, file);
    return 0;
}

int _cloud_hdfs_receive(cloud_hdfs_t *cloud, char *id, void *buffer, int bufSize) {
    hdfsFS fs = (hdfsFS) cloud->node;
    hdfsFile file = hdfsOpenFile(fs, id, O_RDONLY, 0, 0, 0);
    int retval = hdfsRead(fs, file, buffer, bufSize);
    retval = hdfsCloseFile(fs, file);
    return 0;
}

cloud_spark_t* _cloud_spark_init(char *adress, int port) {
    cloud_spark_t *spark = (cloud_spark_t*) malloc(sizeof(cloud_spark_t));
    return spark;
}

int _cloud_spark_connect(cloud_spark_t *cloud) {

    return 0;
}

int _cloud_spark_create_program(cloud_hdfs_t *cloud) {
    // 1/ Generate scala code (with template)
    // 2/ Compile in jar -> system("sbt package");
    // 3/ Generate native kernel code (map function)
    // 4/ Compile
    // 5/ Upload to HDFS
    return 0;
}

int _cloud_spark_launch(cloud_spark_t *cloud) {
    //system("spark-submit --class test.dummy.HdfsTest /Users/hyviquel/tmp/test/target/scala-2.11/test_2.11-0.1.0.jar");
    system("spark-submit --class test.dummy.FullTest /Users/hyviquel/tmp/test/target/scala-2.11/test_2.11-0.1.0.jar");
    return 0;
}

int __tgt_rtl_device_type(int32_t device_id){

  if( device_id < NUMBER_OF_DEVICES)
    return 21; // EM_PPC64

  return 0;
}

int __tgt_rtl_number_of_devices(){
  return NUMBER_OF_DEVICES;
}

int32_t __tgt_rtl_init_device(int32_t device_id){
  _cloud_hdfs_init(NULL, 0, NULL);
  _cloud_spark_init(NULL, 0);
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

    if (!strcmp(elf_strptr(e,shstrndx,hdr.sh_name),".openmptgt_host_entries")){
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


  elf_end(e);

  return DeviceInfo.getOffloadEntriesTable(device_id);
}

void *__tgt_rtl_data_alloc(int32_t device_id, int64_t size){
  void *ptr = malloc(size);
  return ptr;
}

int32_t __tgt_rtl_data_submit(int32_t device_id, void *tgt_ptr, void *hst_ptr, int64_t size){
  memcpy(tgt_ptr,hst_ptr,size);
  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_data_retrieve(int32_t device_id, void *hst_ptr, void *tgt_ptr, int64_t size){
  memcpy(hst_ptr,tgt_ptr,size);
  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_data_delete(int32_t device_id, void* tgt_ptr){
  free(tgt_ptr);
  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_run_target_team_region(int32_t device_id, void *tgt_entry_ptr,
   void **tgt_args, int32_t arg_num, int32_t team_num, int32_t thread_limit)
{
  // ignore team num and thread limit



  // All args are references

  std::vector<void*> args(arg_num);

  for(int32_t i=0; i<arg_num; ++i)
    args[i] = &tgt_args[i];

  void (*fptr)(void*) = (void (*)(void*))tgt_entry_ptr;

  DP("Running entry point at %016lx...\n",(Elf64_Addr)tgt_entry_ptr);

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
