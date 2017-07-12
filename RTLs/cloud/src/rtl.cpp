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

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <thread>
#include <unistd.h>

#include <dlfcn.h>
#include <gelf.h>
#ifndef __APPLE__
#include <link.h>
#endif
#include <inttypes.h>
#include <string.h>

#include "INIReader.h"
#include "amazon.h"
#include "azure.h"
#include "cloud_compression.h"
#include "cloud_util.h"
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
    {"Generic", createGenericProvider, "HdfsProvider"},
    {"Azure", createAzureProvider, "AzureProvider"},
    {"AWS", createAmazonProvider, "AmazonProvider"}};

static std::vector<struct ProviderListEntry> ProviderList;

static RTLDeviceInfoTy DeviceInfo;

static std::string working_path;

static char *library_tmpfile = strdup("/tmp/libompcloudXXXXXX");

RTLDeviceInfoTy::RTLDeviceInfoTy() {
  std::string ConfigPath = std::string(getenv(OMPCLOUD_CONF_ENV.c_str()));
  reader = new INIReader(ConfigPath);

  if (reader->ParseError() < 0) {
    fprintf(stderr, "ERROR: Path to OmpCloud configuration seems wrong: %s\n",
            ConfigPath.c_str());
    exit(EXIT_FAILURE);
  }

  std::string vmode = DeviceInfo.reader->Get("Spark", "VerboseMode", "info");
  if (vmode == "debug") {
    verbose = Verbosity::debug;
  } else if (vmode == "info") {
    verbose = Verbosity::info;
  } else if (vmode == "quiet") {
    verbose = Verbosity::quiet;
  } else {
    fprintf(stderr, "Warning: invalid verbose mode\n");
    verbose = Verbosity::info;
  }

  char *tempdir = mkdtemp(strdup("/tmp/ompcloud.XXXXXX"));
  if (tempdir == NULL) {
    fprintf(stderr, "Error on mkdtemp\n");
    exit(EXIT_FAILURE);
  }
  working_path = tempdir;
  std::string cmd("mkdir -p " + working_path);

  DP("%s\n", exec_cmd(cmd.c_str()).c_str());

  NumberOfDevices = 0;

  // Checking how many providers we have in the configuration file
  for (auto entry : ExistingProviderList) {
    if (reader->HasSection(entry.SectionName)) {
      if (verbose != Verbosity::quiet)
        DP("Provider '%s' detected in configuration file.\n",
           entry.ProviderName.c_str());
      ProviderList.push_back(entry);
      NumberOfDevices++;
    }
  }

  if (ProviderList.size() == 0) {
    if (verbose != Verbosity::quiet) {
      DP("No specific provider detected in configuration file.\n");
      DP("Local provider will be used.\n");
    }
    ProviderList.push_back(ExistingProviderList.front());
    NumberOfDevices++;
  }

  assert(NumberOfDevices == 1 && "Do not support more than 1 device!");

  FuncGblEntries.resize(NumberOfDevices);
  SparkClusters.resize(NumberOfDevices);
  Providers.resize(NumberOfDevices);
  ElapsedTimes = std::vector<ElapsedTime>(NumberOfDevices);
  submitting_threads.resize(NumberOfDevices);
  retrieving_threads.resize(NumberOfDevices);

  for (int i = 0; i < NumberOfDevices; i++) {
    char *tmpname = strdup((working_path + "/addresstable_XXXXXX").c_str());
    int ret = mkstemp(tmpname);
    if (ret < 0) {
      perror("Cannot create address table file");
      exit(EXIT_FAILURE);
    }
    AddressTables.push_back(std::string(tmpname));
  }
}

RTLDeviceInfoTy::~RTLDeviceInfoTy() {
  if (verbose != Verbosity::quiet)
    for (int i = 0; i < NumberOfDevices; i++) {
      ElapsedTime &timing = DeviceInfo.ElapsedTimes[i];
      DP("Uploading = %ds\n", timing.UploadTime);
      DP("Downloading = %ds\n", timing.DownloadTime);
      DP("Compression = %ds\n", timing.CompressionTime);
      DP("Decompression = %ds\n", timing.DecompressionTime);
      DP("Execution = %ds\n", timing.SparkExecutionTime);
    }

  if (!DeviceInfo.SparkClusters[0].KeepTmpFiles)
    remove_directory(working_path.c_str());
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

  if (DeviceInfo.verbose != Verbosity::quiet)
    DP("Initializing device %d\n", device_id);

  // TODO: Check connection to Apache Spark cluster

  SparkMode mode;
  std::string smode =
      DeviceInfo.reader->Get("Spark", "Mode", DEFAULT_SPARK_MODE);
  if (smode == "client") {
    mode = SparkMode::client;
  } else if (smode == "cluster") {
    mode = SparkMode::cluster;
  } else {
    mode = SparkMode::invalid;
  }

  SparkInfo spark{
      DeviceInfo.reader->Get("Spark", "HostName", ""),
      (int)DeviceInfo.reader->GetInteger("Spark", "Port", DEFAULT_SPARK_PORT),
      mode,
      DeviceInfo.reader->Get("Spark", "User", DEFAULT_SPARK_USER),
      DeviceInfo.reader->Get("Spark", "BinPath", ""),
      DeviceInfo.reader->Get("Spark", "Package", DEFAULT_SPARK_PACKAGE),
      DeviceInfo.reader->Get("Spark", "JarPath", DEFAULT_SPARK_JARPATH),
      DeviceInfo.reader->Get("Spark", "AdditionalArgs", ""),
      DeviceInfo.reader->Get("Spark", "WorkingDir", ""),
      DeviceInfo.reader->GetBoolean("Spark", "Compression", true),
      DeviceInfo.reader->Get("Spark", "CompressionFormat",
                             DEFAULT_COMPRESSION_FORMAT),
      DeviceInfo.reader->GetBoolean("Spark", "UseThreads", true),
      DeviceInfo.verbose,
      DeviceInfo.reader->GetBoolean("Spark", "KeepTmpFiles", false),
      1,
  };

  // Checking if given WorkingDir ends in a slash for path concatenation.
  // If it doesn't, add it
  if (spark.WorkingDir.back() != '/') {
    spark.WorkingDir += "/";
  }

  if (spark.WorkingDir.empty()) {
    spark.WorkingDir = "ompcloud." + random_string(8);
  }

  if (spark.ServAddress.empty()) {
    // Look for env variable defining Spark hostname
    if (char *enServAddress = std::getenv("OMPCLOUD_SPARK_HOSTNAME")) {
      spark.ServAddress = std::string(enServAddress);
    }
  }

  if (spark.Mode == SparkMode::invalid || !spark.Package.compare("") ||
      !spark.JarPath.compare("")) {
    DP("ERROR: Invalid values in 'cloud_rtl.ini' for Spark!");
    exit(EXIT_FAILURE);
  }

  if (DeviceInfo.verbose != Verbosity::quiet) {
    DP("Spark HostName: '%s' - Port: '%d' - User: '%s' - Mode: %s\n",
       spark.ServAddress.c_str(), spark.ServPort, spark.UserName.c_str(),
       smode.c_str());
    DP("Jar: %s - Class: %s - WorkingDir: '%s'\n", spark.JarPath.c_str(),
       spark.Package.c_str(), spark.WorkingDir.c_str());
  }

  // Checking for listed provider. Each device id refers to a provider
  // position in the list
  if (spark.VerboseMode != Verbosity::quiet)
    DP("Creating provider %s\n", ProviderList[device_id].ProviderName.c_str());

  DeviceInfo.SparkClusters[device_id] = spark;
  std::string providerSectionName = ProviderList[device_id].SectionName;
  DeviceInfo.Providers[device_id] =
      ProviderList[device_id].ProviderGenerator(spark);
  DeviceInfo.Providers[device_id]->parse_config(DeviceInfo.reader);
  DeviceInfo.Providers[device_id]->init_device();

  return OFFLOAD_SUCCESS; // success
}

__tgt_target_table *__tgt_rtl_load_binary(int32_t device_id,
                                          __tgt_device_image *image) {

  char tempdir_template[] = "/tmp/ompcloud.XXXXXX";
  char *tempdir = mkdtemp(tempdir_template);
  if (tempdir == NULL) {
    DP("ERROR: Error on mkdtemp\n");
    exit(EXIT_FAILURE);
  }
  working_path = tempdir;
  std::string cmd("mkdir -p " + working_path);
  if (DeviceInfo.verbose == Verbosity::debug)
    DP("%s\n", exec_cmd(cmd.c_str()).c_str());

  if (DeviceInfo.verbose != Verbosity::quiet)
    DP("Dev %d: load binary from 0x%llx image\n", device_id,
       (long long)image->ImageStart);

  assert(device_id >= 0 && device_id < DeviceInfo.NumberOfDevices &&
         "bad dev id");

  size_t ImageSize = (size_t)image->ImageEnd - (size_t)image->ImageStart;
  size_t NumEntries = (size_t)(image->EntriesEnd - image->EntriesBegin);
  if (DeviceInfo.verbose != Verbosity::quiet)
    DP("Expecting to have %ld entries defined.\n", (long)NumEntries);

  // We do not need to set the ELF version because the caller of this function
  // had to do that to decide the right runtime to use

  // Obtain elf handler
  Elf *e = elf_memory((char *)image->ImageStart, ImageSize);
  if (!e) {
    DP("ERROR: Unable to get ELF handle: %s!\n", elf_errmsg(-1));
    return NULL;
  }

  if (elf_kind(e) != ELF_K_ELF) {
    DP("ERROR: Invalid Elf kind!\n");
    elf_end(e);
    return NULL;
  }

  // Find the entries section offset
  Elf_Scn *section = 0;
  Elf64_Off entries_offset = 0;

  size_t shstrndx;

  if (elf_getshdrstrndx(e, &shstrndx)) {
    DP("ERROR: Unable to get ELF strings index!\n");
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
    DP("ERROR: Entries Section Offset Not Found\n");
    elf_end(e);
    exit(EXIT_FAILURE);
  }

  if (DeviceInfo.verbose != Verbosity::quiet)
    DP("Offset of entries section is (%016lx).\n", entries_offset);

  // load dynamic library and get the entry points. We use the dl library
  // to do the loading of the library, but we could do it directly to avoid
  // the dump to the temporary file.
  //
  // 1) Create tmp file with the library contents
  // 2) Use dlopen to load the file and dlsym to retrieve the symbols
  int tmp_fd = mkstemp(library_tmpfile);
  if (tmp_fd == -1) {
    elf_end(e);
    perror("Error when creating temporary library file");
    exit(EXIT_FAILURE);
  }

  if (DeviceInfo.verbose != Verbosity::quiet)
    DP("Library will be written in %s\n", library_tmpfile);

  FILE *ftmp = fdopen(tmp_fd, "wb");
  if (!ftmp) {
    elf_end(e);
    perror("Error when opening temporary library file");
    exit(EXIT_FAILURE);
  }

  fwrite(image->ImageStart, ImageSize, 1, ftmp);
  fclose(ftmp);

  DynLibTy Lib = {library_tmpfile, dlopen(library_tmpfile, RTLD_LAZY)};

  if (!Lib.Handle) {
    fprintf(stderr, "Target library loading error: %s\n", dlerror());
    elf_end(e);
    exit(EXIT_FAILURE);
  }

#ifndef __APPLE__
  struct link_map *libInfo = (struct link_map *)Lib.Handle;

  // The place where the entries info is loaded is the library base address
  // plus the offset determined from the ELF file.
  Elf64_Addr entries_addr = libInfo->l_addr + entries_offset;

  if (DeviceInfo.SparkClusters[device_id].VerboseMode != Verbosity::quiet)
    DP("Pointer to first entry to be loaded is (%016lx).\n", entries_addr);

  // Table of pointers to all the entries in the target
  __tgt_offload_entry *entries_table = (__tgt_offload_entry *)entries_addr;

  __tgt_offload_entry *entries_begin = &entries_table[0];

  if (DeviceInfo.verbose != Verbosity::quiet) {
    DP("Entry begin: (%016lx)\nEntry name: (%s)\nEntry size: (%016lx)\n",
       (uintptr_t)entries_begin->addr, entries_begin->name,
       entries_begin->size);
  }
  // DP("Next entry: (%016lx)\nEntry name: (%s)\nEntry size: (%016lx)\n",
  //   (uintptr_t)entries_table[1].addr, entries_table[1].name,
  //   entries_table[1].size);

  __tgt_offload_entry *entries_end = entries_begin + NumEntries;

  if (!entries_begin) {
    fprintf(stderr, "ERROR: Can't obtain entries begin\n");
    elf_end(e);
    exit(EXIT_FAILURE);
  }

  if (DeviceInfo.verbose != Verbosity::quiet)
    DP("Entries table range is (%016lx)->(%016lx)\n", (Elf64_Addr)entries_begin,
       (Elf64_Addr)entries_end)
  DeviceInfo.createOffloadTable(device_id, entries_begin, entries_end);

  elf_end(e);

#endif

  return DeviceInfo.getOffloadEntriesTable(device_id);
}

void *__tgt_rtl_data_alloc(int32_t device_id, int64_t size, int32_t type,
                           int32_t id) {
  if (id >= 0) {
    // Write entry in the address table
    std::ofstream ofs(DeviceInfo.AddressTables[device_id], std::ios_base::app);
    ofs << id << ";" << size << ";" << std::endl;
    ofs.close();

    if (DeviceInfo.verbose != Verbosity::quiet)
      DP("Adding '%d' of size %ld to the address table\n", id, size);
  }

  return DeviceInfo.Providers[device_id]->data_alloc(size, type, id);
}

static int32_t data_submit(int32_t device_id, void *tgt_ptr, void *hst_ptr,
                           int64_t size, int32_t id) {
  float sizeInMB = size / (1024 * 1024);
  if (size > MAX_JAVA_INT) {
    fprintf(stderr,
            "ERROR: Not supported -- size of %d is larger (%.2fMB) than the "
            "maximal size of JVM's bytearrays (%.2fMB).\n",
            id, sizeInMB, MAX_SIZE_IN_MB);
    exit(EXIT_FAILURE);
  }

  ElapsedTime &timing = DeviceInfo.ElapsedTimes[device_id];

  bool needCompression = DeviceInfo.SparkClusters[device_id].Compression &&
                         size >= MIN_SIZE_COMPRESSION;

  // Since we now need the hdfs file, we create it here
  std::string filename = std::to_string(id);
  std::string host_filepath = working_path + "/" + filename;

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

    if (DeviceInfo.verbose != Verbosity::quiet)
      DP("Compressed %.1fMB in %lds\n", sizeInMB, t_delay);

  } else {
    std::ofstream tmpfile(host_filepath);
    if (!tmpfile.is_open()) {
      perror("ERROR: Failed to open temporary file\n");
      exit(EXIT_FAILURE);
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

  if (DeviceInfo.verbose != Verbosity::quiet)
    DP("Uploaded %.1fMB in %lds\n", sendingSizeInMB, t_delay);

  if (!DeviceInfo.SparkClusters[device_id].KeepTmpFiles)
    remove(host_filepath.c_str());

  return ret_val;
}

static int32_t data_retrieve(int32_t device_id, void *hst_ptr, void *tgt_ptr,
                             int64_t size, int32_t id) {
  float sizeInMB = size / (1024 * 1024);
  if (size > MAX_JAVA_INT) {
    fprintf(stderr,
            "ERROR: Not supported -- size of %d is larger (%.1fMB) than the "
            "maximal size of JVM's bytearrays (%.1fMB).\n",
            id, sizeInMB, MAX_SIZE_IN_MB);
    exit(EXIT_FAILURE);
  }

  ElapsedTime &timing = DeviceInfo.ElapsedTimes[device_id];

  bool needDecompression = DeviceInfo.SparkClusters[device_id].Compression &&
                           size >= MIN_SIZE_COMPRESSION;

  std::string filename = std::to_string(id);
  std::string host_filepath = working_path + "/" + filename;

  auto t_start = std::chrono::high_resolution_clock::now();
  DeviceInfo.Providers[device_id]->get_file(host_filepath, filename);
  auto t_end = std::chrono::high_resolution_clock::now();
  auto t_delay =
      std::chrono::duration_cast<std::chrono::seconds>(t_end - t_start).count();
  timing.DownloadTime_mutex.lock();
  timing.DownloadTime += t_delay;
  timing.DownloadTime_mutex.unlock();

  if (DeviceInfo.verbose != Verbosity::quiet)
    DP("Downloaded %.1fMB in %lds\n", sizeInMB, t_delay);

  if (needDecompression) {
    auto t_start = std::chrono::high_resolution_clock::now();
    // Decompress data directly to the host memory
    int decomp_size = decompress_file(host_filepath, (char *)hst_ptr, size);
    if (decomp_size != size) {
      fprintf(stderr, "Decompressed data are not the right size. => %d\n",
              decomp_size);
      exit(EXIT_FAILURE);
    }
    auto t_end = std::chrono::high_resolution_clock::now();
    auto t_delay =
        std::chrono::duration_cast<std::chrono::seconds>(t_end - t_start)
            .count();

    timing.DecompressionTime_mutex.lock();
    timing.DecompressionTime += t_delay;
    timing.DecompressionTime_mutex.unlock();
    if (DeviceInfo.verbose != Verbosity::quiet)
      DP("Decompressed %.1fMB in %lds\n", sizeInMB, t_delay);

  } else {

    // Reading contents of temporary file
    FILE *ftmp = fopen(host_filepath.c_str(), "rb");

    if (!ftmp) {
      perror("ERROR: Could not open temporary file.");
      exit(EXIT_FAILURE);
    }

    if (fread(hst_ptr, 1, size, ftmp) != size) {
      fprintf(stderr,
              "ERROR: Could not successfully read temporary file. => %" PRId64
              "\n",
              size);
      fclose(ftmp);

      if (!DeviceInfo.SparkClusters[device_id].KeepTmpFiles)
        remove(host_filepath.c_str());
      exit(EXIT_FAILURE);
    }

    fclose(ftmp);
  }

  if (!DeviceInfo.SparkClusters[device_id].KeepTmpFiles)
    remove(host_filepath.c_str());

  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_data_submit(int32_t device_id, void *tgt_ptr, void *hst_ptr,
                              int64_t size, int32_t id) {
  if (id < 0) {
    if (DeviceInfo.verbose != Verbosity::quiet)
      DP("No need to submit pointer\n");
    return OFFLOAD_SUCCESS;
  }
  if (DeviceInfo.SparkClusters[device_id].UseThreads) {
    DeviceInfo.submitting_threads[device_id].push_back(
        std::thread(data_submit, device_id, tgt_ptr, hst_ptr, size, id));
  } else {
    return data_submit(device_id, tgt_ptr, hst_ptr, size, id);
  }
  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_data_retrieve(int32_t device_id, void *hst_ptr, void *tgt_ptr,
                                int64_t size, int32_t id) {
  if (DeviceInfo.SparkClusters[device_id].UseThreads) {
    DeviceInfo.retrieving_threads[device_id].push_back(
        std::thread(data_retrieve, device_id, hst_ptr, tgt_ptr, size, id));
  } else {
    return data_retrieve(device_id, hst_ptr, tgt_ptr, size, id);
  }

  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_data_delete(int32_t device_id, void *tgt_ptr, int32_t id) {
  if (id < 0) {
    if (DeviceInfo.verbose != Verbosity::quiet)
      DP("No file to delete\n");
    return OFFLOAD_SUCCESS;
  }

  std::string filename = std::to_string(id);
  // FIXME: Check retrieving thread is over before deleting data
  // return DeviceInfo.Providers[device_id]->delete_file(filename);

  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_run_barrier_end(int32_t device_id) {
  if (DeviceInfo.SparkClusters[device_id].UseThreads) {
    for (auto it = DeviceInfo.retrieving_threads[device_id].begin();
         it != DeviceInfo.retrieving_threads[device_id].end(); it++) {
      (*it).join();
    }
    DeviceInfo.retrieving_threads[device_id].clear();
  }
  return OFFLOAD_SUCCESS;
}

int32_t __tgt_rtl_run_target_team_region(int32_t device_id, void *tgt_entry_ptr,
                                         void **tgt_args, int32_t arg_num,
                                         int32_t team_num,
                                         int32_t thread_limit) {
  ElapsedTime &timing = DeviceInfo.ElapsedTimes[device_id];

  if (DeviceInfo.verbose != Verbosity::quiet)
    DP("Send library and address table to the Spark driver\n");

  const char *fileName = DeviceInfo.AddressTables[device_id].c_str();
  DeviceInfo.Providers[device_id]->send_file(fileName, "addressTable");

  if (DeviceInfo.verbose != Verbosity::quiet)
    DP("Send Library: %s\n", library_tmpfile);
  DeviceInfo.Providers[device_id]->send_file(library_tmpfile, "libmr.so");
  if (DeviceInfo.verbose != Verbosity::quiet)
    DP("Done!\n");

  if (!DeviceInfo.SparkClusters[device_id].KeepTmpFiles)
    remove(fileName);

  if (DeviceInfo.SparkClusters[device_id].UseThreads) {
    for (auto it = DeviceInfo.submitting_threads[device_id].begin();
         it != DeviceInfo.submitting_threads[device_id].end(); it++) {
      (*it).join();
    }
    DeviceInfo.submitting_threads[device_id].clear();
  }

  auto t_start = std::chrono::high_resolution_clock::now();
  int32_t ret_val = DeviceInfo.Providers[device_id]->submit_job();
  auto t_end = std::chrono::high_resolution_clock::now();
  auto t_delay =
      std::chrono::duration_cast<std::chrono::seconds>(t_end - t_start).count();
  timing.SparkExecutionTime += t_delay;

  if (DeviceInfo.verbose != Verbosity::quiet)
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
