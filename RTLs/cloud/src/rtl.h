#ifndef _INCLUDE_RTL_H
#define _INCLUDE_RTL_H

#include <assert.h>
#include <fstream>
#include <hdfs.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <thread>
#include <mutex>
#include <vector>

#include "INIReader.h"
#include "omptarget.h"

class CloudProvider;

/// Array of Dynamic libraries loaded for this target
struct DynLibTy {
  char *FileName;
  void *Handle;
};

extern const char *__progname; /* for job name */

struct HdfsInfo {
  std::string ServAddress;
  int ServPort;
  std::string UserName;
  std::string WorkingDir;
  bool Compression;
  std::string CompressionFormat;
  uintptr_t currAddr;
};

enum SparkMode { client, cluster, condor, invalid };

struct SparkInfo {
  std::string ServAddress;
  int ServPort;
  SparkMode Mode;
  std::string UserName;
  std::string BinPath;
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
  CloudProvider *(*ProviderGenerator)(ResourceInfo &);
  std::string SectionName;
};

struct ElapsedTime {
  int CompressionTime = 0;
  std::mutex CompressionTime_mutex;
  int DecompressionTime = 0;
  std::mutex DecompressionTime_mutex;
  int UploadTime = 0;
  std::mutex UploadTime_mutex;
  int DownloadTime = 0;
  std::mutex DownloadTime_mutex;
  int SparkExecutionTime = 0;
};

#define OMPCLOUD_CONF_ENV "OMPCLOUD_CONF_PATH"
#define DEFAULT_OMPCLOUD_CONF_FILE "ompcloud-conf.ini"
#define DEFAULT_HDFS_PORT 9000
#define DEFAULT_SPARK_PORT 7077
#define DEFAULT_SPARK_USER "anonymous"
#define DEFAULT_SPARK_MODE "client"
#define DEFAULT_SPARK_PACKAGE "org.llvm.openmp.OmpKernel"
#define DEFAULT_SPARK_JARPATH "target/scala-2.11/test-assembly-0.2.0.jar"
#define DEFAULT_SPARK_POLLINTERVAL 300
#define DEFAULT_PROXY_HOSTNAME ""
#define DEFAULT_PROXY_PORT 0
#define DEFAULT_PROXY_TYPE ""

// Only data larger than about 1MB are compressed
const int MIN_SIZE_COMPRESSION = 1000000;
const std::string DEFAULT_COMPRESSION_FORMAT = "gzip";

// Maximal size of offloaded data is about 2GB
// Size of JVM's ByteArrays are limited by MAX_JAVA_INT = 2^31-1
const long MAX_JAVA_INT = 2147483647;
const float MAX_SIZE_IN_MB = MAX_JAVA_INT / (1024 * 1024);

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
  std::vector<CloudProvider *> Providers;
  std::vector<std::string> AddressTables;
  std::vector<ElapsedTime> ElapsedTimes;

  std::vector<std::vector<std::thread>> submitting_threads;
  std::vector<std::vector<std::thread>> retrieving_threads;

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
