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

#include <fstream>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include <dlfcn.h>
#include <gelf.h>
#ifndef __APPLE__
#include <link.h>
#endif
#include <string.h>

#include "INIReader.h"
#include "amazon.h"
#include "azure.h"
#include "cloud_compression.h"
#include "generic.h"
#include "local.h"
#include "omptarget.h"

#include "provider.h"

#include "rtl.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define GETNAME(name) #name
#define GETNAME2(name) GETNAME(name)
#define DP(...) DEBUGP("Target " GETNAME2(TARGET_NAME) " RTL", __VA_ARGS__)

static std::vector<struct ProviderListEntry> ExistingProviderList = {
    {"Local", createLocalProvider, "LocalProvider"},
    {"Generic", createGenericProvider, "GenericProvider"},
    {"Azure", createAzureProvider, "AzureProvider"},
    {"AWS", createAmazonProvider, "AmazonProvider"}};

static std::vector<struct ProviderListEntry> ProviderList;

static RTLDeviceInfoTy DeviceInfo;

RTLDeviceInfoTy::RTLDeviceInfoTy() {

  const char *conf_filename = getenv(OMPCLOUD_CONF_ENV);
  if (conf_filename == NULL) {
    conf_filename = DEFAULT_OMPCLOUD_CONF_FILE;
  }

  INIReader reader(conf_filename);

  NumberOfDevices = 0;

  // Checking how many providers we have in the configuration file
  for (auto entry : ExistingProviderList) {
    if (reader.HasSection(entry.SectionName)) {
      DP("Provider '%s' detected in configuration file.\n",
         entry.ProviderName.c_str());
      ProviderList.push_back(entry);
      NumberOfDevices++;
    }
  }

  if (ProviderList.size() == 0) {
    DP("No specific provider detected in configuration file.\n");
    DP("Local provider will be used.\n");
    ProviderList.push_back(ExistingProviderList.front());
    NumberOfDevices++;
  }

  DP("Number of Devices: %d\n", NumberOfDevices);

  FuncGblEntries.resize(NumberOfDevices);
  SparkClusters.resize(NumberOfDevices);
  Providers.resize(NumberOfDevices);
  ElapsedTimes = std::vector<ElapsedTime>(NumberOfDevices);
  submitting_threads.resize(NumberOfDevices);
  retrieving_threads.resize(NumberOfDevices);

  for (int i = 0; i < NumberOfDevices; i++) {
    char *tmpname = strdup("/tmp/tmpfileXXXXXX");
    mkstemp(tmpname);
    AddressTables.push_back(std::string(tmpname));
  }
}

RTLDeviceInfoTy::~RTLDeviceInfoTy() {
  for (int i = 0; i < NumberOfDevices; i++) {
    ElapsedTime &timing = DeviceInfo.ElapsedTimes[i];
    DP("Uploading = %ds\n", timing.UploadTime);
    DP("Downloading = %ds\n", timing.DownloadTime);
    DP("Compression = %ds\n", timing.CompressionTime);
    DP("Decompression = %ds\n", timing.DecompressionTime);
    DP("Execution = %ds\n", timing.SparkExecutionTime);
  }
}

void RTLDeviceInfoTy::createOffloadTable(int32_t device_id,
                                         __tgt_offload_entry *begin,
                                         __tgt_offload_entry *end) {
  assert(device_id < FuncGblEntries.size() && "Unexpected device id!");
  FuncOrGblEntryTy &E = FuncGblEntries[device_id];

  E.Table.EntriesBegin = begin;
  E.Table.EntriesEnd = end;
}

bool RTLDeviceInfoTy::findOffloadEntry(int32_t device_id, void *addr) {
  assert(device_id < FuncGblEntries.size() && "Unexpected device id!");
  FuncOrGblEntryTy &E = FuncGblEntries[device_id];

  for (__tgt_offload_entry *i = E.Table.EntriesBegin, *e = E.Table.EntriesEnd;
       i < e; ++i) {
    if (i->addr == addr)
      return true;
  }

  return false;
}

__tgt_target_table *RTLDeviceInfoTy::getOffloadEntriesTable(int32_t device_id) {
  assert(device_id < FuncGblEntries.size() && "Unexpected device id!");
  FuncOrGblEntryTy &E = FuncGblEntries[device_id];

  return &E.Table;
}

#ifdef __cplusplus
extern "C" {
#endif

int __tgt_rtl_device_type(int32_t device_id) { return 0; }

int __tgt_rtl_number_of_devices() { return DeviceInfo.NumberOfDevices; }

int32_t __tgt_rtl_init_device(int32_t device_id) {
  int retval;

  DP("Initializing device %d\n", device_id);

  const char *conf_filename = getenv(OMPCLOUD_CONF_ENV);
  if (conf_filename == NULL) {
    conf_filename = DEFAULT_OMPCLOUD_CONF_FILE;
  }

  // Parsing configurations
  INIReader reader(conf_filename);

  if (reader.ParseError() < 0) {
    DP("Couldn't find '%s'!", conf_filename);
    return OFFLOAD_FAIL;
  }

  // TODO: Check connection to Apache Spark cluster

  SparkMode mode;
  std::string smode = reader.Get("Spark", "Mode", DEFAULT_SPARK_MODE);
  if (smode == "client") {
    mode = SparkMode::client;
  } else if (smode == "cluster") {
    mode = SparkMode::cluster;
  } else if (smode == "condor") {
    mode = SparkMode::condor;
  } else {
    mode = SparkMode::invalid;
  }

  SparkInfo spark{
      reader.Get("Spark", "HostName", ""),
      (int)reader.GetInteger("Spark", "Port", DEFAULT_SPARK_PORT),
      mode,
      reader.Get("Spark", "User", DEFAULT_SPARK_USER),
      reader.Get("Spark", "BinPath", ""),
      reader.Get("Spark", "Package", DEFAULT_SPARK_PACKAGE),
      reader.Get("Spark", "JarPath", DEFAULT_SPARK_JARPATH),
      (int)reader.GetInteger("Spark", "PollInterval",
                             DEFAULT_SPARK_POLLINTERVAL),
      reader.Get("Spark", "AdditionalArgs", ""),
      reader.Get("Spark", "WorkingDir", ""),
      reader.GetBoolean("Spark", "Compression", true),
      reader.Get("Spark", "CompressionFormat", DEFAULT_COMPRESSION_FORMAT),
      1,
  };

  // Checking if given WorkingDir ends in a slash for path concatenation.
  // If it doesn't, add it
  if (spark.WorkingDir.back() != '/') {
    spark.WorkingDir += "/";
  }

  if (spark.ServAddress.empty()) {
    // Look for env variable defining Spark hostname
    if (char *enServAddress = std::getenv("OMPCLOUD_SPARK_HOSTNAME")) {
      spark.ServAddress = std::string(enServAddress);
    }
  }

  if (spark.Mode == SparkMode::invalid || !spark.Package.compare("") ||
      !spark.JarPath.compare("")) {
    DP("Invalid values in 'cloud_rtl.ini' for Spark!");
    return OFFLOAD_FAIL;
  }

  DP("Spark HostName: '%s' - Port: '%d' - User: '%s' - Mode: %s\n",
     spark.ServAddress.c_str(), spark.ServPort, spark.UserName.c_str(),
     smode.c_str());
  DP("Jar: %s - Class: %s - WorkingDir: '%s'\n", spark.JarPath.c_str(),
     spark.Package.c_str(), spark.WorkingDir.c_str());

  DeviceInfo.SparkClusters[device_id] = spark;

  ResourceInfo resources{spark};

  // Checking for listed provider. Each device id refers to a provider position
  // in the list
  DP("Creating provider %s\n", ProviderList[device_id].ProviderName.c_str());

  std::string providerSectionName = ProviderList[device_id].SectionName;
  DeviceInfo.Providers[device_id] =
      ProviderList[device_id].ProviderGenerator(resources);
  DeviceInfo.Providers[device_id]->parse_config(reader);
  DeviceInfo.Providers[device_id]->init_device();

  return OFFLOAD_SUCCESS; // success
}

__tgt_target_table *__tgt_rtl_load_binary(int32_t device_id,
                                          __tgt_device_image *image) {
  DP("Dev %d: load binary from 0x%llx image\n", device_id,
     (long long)image->ImageStart);

  assert(device_id >= 0 && device_id < DeviceInfo.NumberOfDevices &&
         "bad dev id");

  size_t ImageSize = (size_t)image->ImageEnd - (size_t)image->ImageStart;
  size_t NumEntries = (size_t)(image->EntriesEnd - image->EntriesBegin);
  DP("Expecting to have %ld entries defined.\n", (long)NumEntries);

  // We do not need to set the ELF version because the caller of this function
  // had to do that to decide the right runtime to use

  // Obtain elf handler
  Elf *e = elf_memory((char *)image->ImageStart, ImageSize);
  if (!e) {
    DP("Unable to get ELF handle: %s!\n", elf_errmsg(-1));
    return NULL;
  }

  if (elf_kind(e) != ELF_K_ELF) {
    DP("Invalid Elf kind!\n");
    elf_end(e);
    return NULL;
  }

  // Find the entries section offset
  Elf_Scn *section = 0;
  Elf64_Off entries_offset = 0;

  size_t shstrndx;

  if (elf_getshdrstrndx(e, &shstrndx)) {
    DP("Unable to get ELF strings index!\n");
    elf_end(e);
    return NULL;
  }

  while ((section = elf_nextscn(e, section))) {
    GElf_Shdr hdr;
    gelf_getshdr(section, &hdr);

    //    if
    //    (!strcmp(elf_strptr(e,shstrndx,hdr.sh_name),".openmptgt_host_entries")){
    if (!strcmp(elf_strptr(e, shstrndx, hdr.sh_name), ".omptgt_hst_entr")) {
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

  if (!ftmp) {
    elf_end(e);
    return NULL;
  }

  fwrite(image->ImageStart, ImageSize, 1, ftmp);
  fclose(ftmp);

  DP("Trying to send lib to HDFS\n");

  // Sending file to HDFS as the library to be loaded
  DeviceInfo.Providers[device_id]->send_file(tmp_name, "libmr.so");

  DP("Lib sent to HDFS!\n");

  DynLibTy Lib = {tmp_name, dlopen(tmp_name, RTLD_LAZY)};

  if (!Lib.Handle) {
    DP("target library loading error: %s\n", dlerror());
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
  __tgt_offload_entry *entries_table = (__tgt_offload_entry *)entries_addr;

  __tgt_offload_entry *entries_begin = &entries_table[0];

  DP("Entry begin: (%016lx)\nEntry name: (%s)\nEntry size: (%016lx)\n",
     (uintptr_t)entries_begin->addr, entries_begin->name, entries_begin->size);
  // DP("Next entry: (%016lx)\nEntry name: (%s)\nEntry size: (%016lx)\n",
  //   (uintptr_t)entries_table[1].addr, entries_table[1].name,
  //   entries_table[1].size);

  __tgt_offload_entry *entries_end = entries_begin + NumEntries;

  if (!entries_begin) {
    DP("Can't obtain entries begin\n");
    elf_end(e);
    return NULL;
  }

  DP("Entries table range is (%016lx)->(%016lx)\n", (Elf64_Addr)entries_begin,
     (Elf64_Addr)entries_end)
  DeviceInfo.createOffloadTable(device_id, entries_begin, entries_end);

  elf_end(e);

#endif

  remove(tmp_name);

  return DeviceInfo.getOffloadEntriesTable(device_id);
}

void *__tgt_rtl_data_alloc(int32_t device_id, int64_t size, int32_t type,
                           int32_t id) {
  if (id >= 0) {
    // Write entry in the address table
    std::ofstream ofs(DeviceInfo.AddressTables[device_id], std::ios_base::app);
    ofs << id << ";" << size << ";" << std::endl;
    ofs.close();

    DP("Adding '%d' of size %ld to the address table\n", id, size);
  }

  return DeviceInfo.Providers[device_id]->data_alloc(size, type, id);
}

static int32_t data_submit(int32_t device_id, void *tgt_ptr, void *hst_ptr,
                           int64_t size, int32_t id) {
  float sizeInMB = size / (1024 * 1024);
  if (size > MAX_JAVA_INT) {
    DP("Not supported: size of %d is larger (%.2fMB) than the maximal size of "
       "JVM's bytearrays (%.2fMB).\n",
       id, sizeInMB, MAX_SIZE_IN_MB);
    exit(OFFLOAD_FAIL);
  }

  ElapsedTime &timing = DeviceInfo.ElapsedTimes[device_id];

  bool needCompression = DeviceInfo.SparkClusters[device_id].Compression &&
                         size >= MIN_SIZE_COMPRESSION;

  // Since we now need the hdfs file, we create it here
  std::string filename = std::to_string(id);
  std::string host_filepath = "/tmp/" + filename;

  int64_t sendingSize;
  if (needCompression) {
    auto t_start = std::chrono::high_resolution_clock::now();
    sendingSize = compress_to_file(host_filepath, (char *)hst_ptr, size);
    auto t_end = std::chrono::high_resolution_clock::now();
    auto t_delay =
        std::chrono::duration_cast<std::chrono::seconds>(t_end - t_start)
            .count();
    timing.CompressionTime_mutex.lock();
    timing.CompressionTime += t_delay;
    timing.CompressionTime_mutex.unlock();
    DP("Compressed %.1fMB in %lds\n", sizeInMB, t_delay);

  } else {
    std::ofstream tmpfile(host_filepath);
    if (!tmpfile.is_open()) {
      DP("Failed to open temporary file\n");
      exit(OFFLOAD_FAIL);
    }
    tmpfile.write((const char *)hst_ptr, size);
    tmpfile.close();
    sendingSize = size;
  }

  float sendingSizeInMB = sendingSize / (1024 * 1024);
  auto t_start = std::chrono::high_resolution_clock::now();
  int ret_val =
      DeviceInfo.Providers[device_id]->send_file(host_filepath, filename);
  auto t_end = std::chrono::high_resolution_clock::now();
  auto t_delay =
      std::chrono::duration_cast<std::chrono::seconds>(t_end - t_start).count();
  timing.UploadTime_mutex.lock();
  timing.UploadTime += t_delay;
  timing.UploadTime_mutex.unlock();
  DP("Uploaded %.1fMB in %lds\n", sendingSizeInMB, t_delay);

  remove(host_filepath.c_str());

  return ret_val;
}

static int32_t data_retrieve(int32_t device_id, void *hst_ptr, void *tgt_ptr,
                             int64_t size, int32_t id) {
  float sizeInMB = size / (1024 * 1024);
  if (size > MAX_JAVA_INT) {
    DP("Not supported: size of %d is larger (%.1fMB) than the maximal size of "
       "JVM's bytearrays (%.1fMB).\n",
       id, sizeInMB, MAX_SIZE_IN_MB);
    exit(OFFLOAD_FAIL);
  }

  ElapsedTime &timing = DeviceInfo.ElapsedTimes[device_id];

  bool needDecompression = DeviceInfo.SparkClusters[device_id].Compression &&
                           size >= MIN_SIZE_COMPRESSION;

  std::string filename = std::to_string(id);
  std::string host_filepath = "/tmp/" + filename;

  auto t_start = std::chrono::high_resolution_clock::now();
  DeviceInfo.Providers[device_id]->get_file(host_filepath, filename);
  auto t_end = std::chrono::high_resolution_clock::now();
  auto t_delay =
      std::chrono::duration_cast<std::chrono::seconds>(t_end - t_start).count();
  timing.DownloadTime_mutex.lock();
  timing.DownloadTime += t_delay;
  timing.DownloadTime_mutex.unlock();
  DP("Downloaded %.1fMB in %lds\n", sizeInMB, t_delay);

  if (needDecompression) {
    auto t_start = std::chrono::high_resolution_clock::now();
    // Decompress data directly to the host memory
    int decomp_size = decompress_file(host_filepath, (char *)hst_ptr, size);
    if (decomp_size != size) {
      DP("Decompressed data are not the right size. => %d\n", decomp_size);
      exit(OFFLOAD_FAIL);
    }
    auto t_end = std::chrono::high_resolution_clock::now();
    auto t_delay =
        std::chrono::duration_cast<std::chrono::seconds>(t_end - t_start)
            .count();

    timing.DecompressionTime_mutex.lock();
    timing.DecompressionTime += t_delay;
    timing.DecompressionTime_mutex.unlock();
    DP("Decompressed %.1fMB in %lds\n", sizeInMB, t_delay);

  } else {

    // Reading contents of temporary file
    FILE *ftmp = fopen(host_filepath.c_str(), "rb");

    if (!ftmp) {
      DP("Could not open temporary file.\n");
      return OFFLOAD_FAIL;
    }

    if (fread(hst_ptr, 1, size, ftmp) != size) {
      DP("Could not successfully read temporary file. => %ld\n", size);
      fclose(ftmp);
      remove(host_filepath.c_str());
      return OFFLOAD_FAIL;
    }

    fclose(ftmp);
  }

  remove(host_filepath.c_str());

  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_data_submit(int32_t device_id, void *tgt_ptr, void *hst_ptr,
                              int64_t size, int32_t id) {
  if (id < 0) {
    DP("No need to submit pointer\n");
    return OFFLOAD_SUCCESS;
  }

  DeviceInfo.submitting_threads[device_id].push_back(
      std::thread(data_submit, device_id, tgt_ptr, hst_ptr, size, id));
  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_data_retrieve(int32_t device_id, void *hst_ptr, void *tgt_ptr,
                                int64_t size, int32_t id) {
  DeviceInfo.retrieving_threads[device_id].push_back(
      std::thread(data_retrieve, device_id, hst_ptr, tgt_ptr, size, id));
  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_data_delete(int32_t device_id, void *tgt_ptr, int32_t id) {
  if (id < 0) {
    DP("No file to delete\n");
    return OFFLOAD_SUCCESS;
  }

  std::string filename = std::to_string(id);
  // FIXME: Check retrieving thread is over before deleting data
  // return DeviceInfo.Providers[device_id]->delete_file(filename);

  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_run_barrier_end(int32_t device_id) {
  for (auto it = DeviceInfo.retrieving_threads[device_id].begin();
       it != DeviceInfo.retrieving_threads[device_id].end(); it++) {
    (*it).join();
  }

  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_run_target_team_region(int32_t device_id, void *tgt_entry_ptr,
                                         void **tgt_args, int32_t arg_num,
                                         int32_t team_num,
                                         int32_t thread_limit) {
  ElapsedTime &timing = DeviceInfo.ElapsedTimes[device_id];
  const char *fileName = DeviceInfo.AddressTables[device_id].c_str();
  DeviceInfo.Providers[device_id]->send_file(fileName, "addressTable");
  remove(fileName);

  for (auto it = DeviceInfo.submitting_threads[device_id].begin();
       it != DeviceInfo.submitting_threads[device_id].end(); it++) {
    (*it).join();
  }

  auto t_start = std::chrono::high_resolution_clock::now();
  int32_t ret_val = DeviceInfo.Providers[device_id]->submit_job();
  auto t_end = std::chrono::high_resolution_clock::now();
  auto t_delay =
      std::chrono::duration_cast<std::chrono::seconds>(t_end - t_start).count();
  timing.SparkExecutionTime += t_delay;
  DP("Spark job executed in %lds\n", t_delay);

  return ret_val;
}

int32_t __tgt_rtl_run_target_region(int32_t device_id, void *tgt_entry_ptr,
                                    void **tgt_args, int32_t arg_num) {
  // use one team and one thread
  return __tgt_rtl_run_target_team_region(device_id, tgt_entry_ptr, tgt_args,
                                          arg_num, 1, 1);
}

#ifdef __cplusplus
}
#endif
