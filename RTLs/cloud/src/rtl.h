#ifndef _INCLUDE_RTL_H
#define _INCLUDE_RTL_H

#include <assert.h>
#include <fstream>
#include <hdfs.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <vector>

#include "INIReader.h"
#include "omptarget.h"

class GenericProvider;

/// Array of Dynamic libraries loaded for this target
struct DynLibTy {
  char *FileName;
  void *Handle;
};

struct HdfsInfo {
  std::string ServAddress;
  int ServPort;
  std::string UserName;
  std::string WorkingDir;
  uintptr_t currAddr;
};

enum SparkMode { client, cluster, invalid };

struct SparkInfo {
  std::string ServAddress;
  int ServPort;
  SparkMode Mode;
  std::string UserName;
  std::string Package;
  std::string JarPath;
  int PollInterval;
  std::string AdditionalArgs;
};

struct ProxyInfo {
  std::string HostName;
  int Port;
  std::string Type;
};

struct ResourceInfo {
  struct HdfsInfo HDFSInfo;
  struct SparkInfo Spark;
  struct ProxyInfo Proxy;
  hdfsFS FS;
};

struct ProviderListEntry {
  std::string ProviderName;
  GenericProvider *(*ProviderGenerator)(ResourceInfo &);
  std::string SectionName;
};

#define DEFAULT_CLOUD_RTL_CONF_FILE "cloud_rtl.ini"
#define DEFAULT_HDFS_PORT 9000
#define DEFAULT_SPARK_PORT 7077
#define DEFAULT_SPARK_USER "anonymous"
#define DEFAULT_SPARK_MODE "client"
#define DEFAULT_SPARK_PACKAGE "org.llvm.openmp.OmpKernel"
#define DEFAULT_SPARK_JARPATH "target/scala-2.10/test-assembly-0.1.0.jar"
#define DEFAULT_SPARK_POLLINTERVAL 300
#define DEFAULT_PROXY_HOSTNAME ""
#define DEFAULT_PROXY_PORT 0
#define DEFAULT_PROXY_TYPE ""

/// Keep entries table per device
struct FuncOrGblEntryTy {
  __tgt_target_table Table;
};

/// Class containing all the device information
class RTLDeviceInfoTy {
  std::vector<FuncOrGblEntryTy> FuncGblEntries;

public:
  int NumberOfDevices;

  std::vector<HdfsInfo> HdfsClusters;
  std::vector<SparkInfo> SparkClusters;
  ProxyInfo ProxyOptions;
  std::vector<hdfsFS> HdfsNodes;
  std::vector<GenericProvider *> Providers;
  std::vector<std::string> AddressTables;

  // Record entry point associated with device
  void createOffloadTable(int32_t device_id, __tgt_offload_entry *begin,
                          __tgt_offload_entry *end);
  // Return true if the entry is associated with device
  bool findOffloadEntry(int32_t device_id, void *addr);
  // Return the pointer to the target entries table
  __tgt_target_table *getOffloadEntriesTable(int32_t device_id);

  RTLDeviceInfoTy();
  ~RTLDeviceInfoTy();
};

#endif
