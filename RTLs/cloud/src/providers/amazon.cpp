#include <hdfs.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <libssh/libssh.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "INIReader.h"
#include "omptarget.h"
#include "../rtl.h"
#include "generic.h"
#include "amazon.h"

#ifndef TARGET_NAME
#define TARGET_NAME Cloud
#endif

#define GETNAME2(name) #name
#define GETNAME(name) GETNAME2(name)
#define DP(...) DEBUGP("Target " GETNAME(TARGET_NAME) " RTL, Amazon Provider:", __VA_ARGS__)

// ./spark-ec2 -k <keypair> -i <key-file> -s <num-slaves> launch <cluster-name>

GenericProvider *createAmazonProvider(ResourceInfo &resources) {
  return new AmazonProvider(resources);
}

int32_t AmazonProvider::parse_config(INIReader reader) {

  ainfo.Bucket = reader.Get("AmazonProvider", "Bucket", "");
  ainfo.Cluster = reader.Get("AmazonProvider", "Cluster", "");
  ainfo.AccessKey = reader.Get("AmazonProvider", "AccessKey", "");
  ainfo.SecretKey = reader.Get("AmazonProvider", "SecretKey", "");
  ainfo.KeyFile = reader.Get("AmazonProvider", "KeyFile", "");
  ainfo.AdditionalArgs = reader.Get("AmazonProvider", "AdditionalArgs", "");

  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::init_device() {
  return OFFLOAD_SUCCESS;
}

std::string AmazonProvider::get_keys() {
  return "--access_key=" + ainfo.AccessKey + " --secret_key=" + ainfo.SecretKey + " " + ainfo.AdditionalArgs;
}

std::string AmazonProvider::get_cloud_path(std::string filename) {
  return "s3://" + ainfo.Bucket + hdfs.WorkingDir + filename;
}

int32_t AmazonProvider::send_file(const char *filename, const char *tgtfilename) {
  std::string command = "s3cmd put ";

  command += std::string(filename);
  command += " " + get_cloud_path(std::string(tgtfilename));
  command += " " + get_keys();

  if (!execute_command(command.c_str(), true)) {
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::data_submit(void *tgt_ptr, void *hst_ptr, int64_t size, int32_t id) {
  // Creating temporary file to hold data to submit
  char tmp_name[] = "/tmp/tmpfile_XXXXXX";
  int tmp_fd = mkstemp(tmp_name);

  if (tmp_fd == -1) {
    DP("Could not create temporary file.\n");
    return OFFLOAD_FAIL;
  }

  // Copying data to file
  FILE *ftmp = fdopen(tmp_fd, "wb");

  if (!ftmp) {
    DP("Could not open temporary file.\n");
    return OFFLOAD_FAIL;
  }

  if (fwrite(hst_ptr, 1, size, ftmp) != size) {
    DP("Could not successfully write to temporary file.\n");
    fclose(ftmp);
    return OFFLOAD_FAIL;
  }

  fclose(ftmp);

  // Submitting data to cloud
  std::string final_filename = std::to_string(id);
  return send_file(tmp_name, final_filename.c_str());
}

int32_t AmazonProvider::data_retrieve(void *hst_ptr, void *tgt_ptr, int64_t size, int32_t id) {
  DP("File %d, size %d.\n", id, size);
  // Creating temporary file to hold data retrieved
  const char *tmp_name = "/tmp/tmpfile_da";

  // Copying data from cloud
  std::string command = "s3cmd get --force ";

  command += get_cloud_path(std::to_string(id));
  command += " " + std::string(tmp_name)  + " " + get_keys();

  if (!execute_command(command.c_str(), true)) {
    return OFFLOAD_FAIL;
  }

  // Reading contents of temporary file
  FILE *ftmp = fopen(tmp_name, "rb");

  if (!ftmp) {
    DP("Could not open temporary file.\n");
    return OFFLOAD_FAIL;
  }

  if (fread(hst_ptr, 1, size, ftmp) != size) {
    DP("Could not successfully read temporary file. => %d\n", size);
    fclose(ftmp);
    return OFFLOAD_FAIL;
  }

  fclose(ftmp);
  return OFFLOAD_SUCCESS;
}

int32_t AmazonProvider::data_delete(void *tgt_ptr, int32_t id) {
  std::string command = "s3cmd rm ";

  command += get_cloud_path(std::to_string(id)) + " " + get_keys();

  if (!execute_command(command.c_str(), true)) {
    return OFFLOAD_FAIL;
  }

  return OFFLOAD_SUCCESS;
}

int verify_knownhost(ssh_session session)
{
  int state;
  char buf[10];
  state = ssh_is_server_known(session);

  switch (state)
  {
    case SSH_SERVER_KNOWN_OK:
      break; /* ok */
    case SSH_SERVER_KNOWN_CHANGED:
      fprintf(stderr, "Host key for server changed: it is now:\n");
      fprintf(stderr, "For security reasons, connection will be stopped\n");
      return -1;
    case SSH_SERVER_FOUND_OTHER:
      fprintf(stderr, "The host key for this server was not found but an other"
                      "type of key exists.\n");
      fprintf(stderr, "An attacker might change the default server key to"
                      "confuse your client into thinking the key does not exist\n");
      return -1;
    case SSH_SERVER_FILE_NOT_FOUND:
      fprintf(stderr, "Could not find known host file.\n");
      fprintf(stderr, "If you accept the host key here, the file will be"
                      "automatically created.\n");
      /* fallback to SSH_SERVER_NOT_KNOWN behavior */
    case SSH_SERVER_NOT_KNOWN:
      fprintf(stderr,"The server is unknown. Do you trust the host key?\n");
      if (fgets(buf, sizeof(buf), stdin) == NULL)
      {
        return -1;
      }
      if (strncasecmp(buf, "yes", 3) != 0)
      {
        return -1;
      }
      if (ssh_write_knownhost(session) < 0)
      {
        fprintf(stderr, "Error %s\n", strerror(errno));
        return -1;
      }
      break;
    case SSH_SERVER_ERROR:
      fprintf(stderr, "Error %s", ssh_get_error(session));
      return -1;
  }
  return 0;
}

int32_t AmazonProvider::submit_job() {
  int32_t rc;

  ssh_session aws_session = ssh_new();
  int verbosity = SSH_LOG_NOLOG;
  int port = 22;

  ssh_channel channel;

  char buffer[256];
  int nbytes;

  if (aws_session == NULL)
    exit(-1);

  ssh_options_set(aws_session, SSH_OPTIONS_HOST, spark.ServAddress.c_str());
  ssh_options_set(aws_session, SSH_OPTIONS_USER, "root");
  ssh_options_set(aws_session, SSH_OPTIONS_LOG_VERBOSITY, &verbosity);
  ssh_options_set(aws_session, SSH_OPTIONS_PORT, &port);
  //ssh_options_set(aws_session, SSH_OPTIONS_ADD_IDENTITY, ainfo.KeyFile.c_str());


  rc = ssh_connect(aws_session);
  if (rc != SSH_OK)
  {
    fprintf(stderr, "Error connecting to server: %s\n",
            ssh_get_error(aws_session));
    exit(-1);
  }

  // Verify the server's identity
  if (verify_knownhost(aws_session) < 0)
  {
    ssh_disconnect(aws_session);
    ssh_free(aws_session);
    exit(-1);
  }

  ssh_key pkey ;

  ssh_pki_import_privkey_file(ainfo.KeyFile.c_str(), NULL, NULL, NULL, &pkey);
  ssh_userauth_publickey(aws_session, "root", pkey);

  // Copy jar file
  // Reading contents of the jar file

  FILE *fjar = fopen(spark.JarPath.c_str(), "rb");
  //fseek(fjar, 0, SEEK_END); // seek to end of file
  //size_t size = ftell(fjar); // get current file pointer
  //fseek(fjar, 0, SEEK_SET); // seek back to beginning of file

  int fd = fileno(fjar); //if you have a stream (e.g. from fopen), not a file descriptor.
  struct stat sjar;
  fstat(fd, &sjar);
  int size = sjar.st_size;

  DP("Size => %d.\n", size);

  void *fbuffer = malloc(size * sizeof(char));

  if (!fjar) {
    DP("Could not open temporary file.\n");
    return OFFLOAD_FAIL;
  }

  ssh_scp scp;
  scp = ssh_scp_new
    (aws_session, SSH_SCP_WRITE, "/root/");
  if (scp == NULL)
  {
    fprintf(stderr, "Error allocating scp session: %s\n",
            ssh_get_error(aws_session));
    return SSH_ERROR;
  }

  rc = ssh_scp_init(scp);
  if (rc != SSH_OK)
  {
    fprintf(stderr, "Error initializing scp session: %s\n",
            ssh_get_error(aws_session));
    ssh_scp_free(scp);
    return rc;
  }

  /*
  rc = ssh_scp_push_directory(scp, spark., S_IRWXU);
  if (rc != SSH_OK)
  {
    fprintf(stderr, "Can't create remote directory: %s\n",
            ssh_get_error(aws_session));
    return rc;
  }*/

  rc = ssh_scp_push_file
    (scp, "spark_job.jar", size, S_IRUSR |  S_IWUSR);
  if (rc != SSH_OK)
  {
    fprintf(stderr, "Can't open remote file: %s\n",
            ssh_get_error(aws_session));
    return rc;
  }

  if (fread(fbuffer, 1, size, fjar) != size) {
    DP("Could not successfully read temporary file.\n");
    fclose(fjar);
    return OFFLOAD_FAIL;
  }


  rc = ssh_scp_write(scp, fbuffer, size);
  if (rc != SSH_OK)
  {
    fprintf(stderr, "Can't write to remote file: %s\n",
            ssh_get_error(aws_session));
    return rc;
  }

  fclose(fjar);
  ssh_scp_close(scp);
  ssh_scp_free(scp);


  // Run Spark
  channel = ssh_channel_new(aws_session);
  if (channel == NULL)
    return SSH_ERROR;
  rc = ssh_channel_open_session(channel);
  if (rc != SSH_OK)
  {
    ssh_channel_free(channel);
    return rc;
  }

  std::string cmd = "export AWS_ACCESS_KEY_ID=" + ainfo.AccessKey +
                     " && export AWS_SECRET_ACCESS_KEY=" + ainfo.SecretKey +
                     " && ./spark/bin/spark-submit --master spark://" + spark.ServAddress + ":" + std::to_string(spark.ServPort) + " " + spark.AdditionalArgs +
                     " --class " + spark.Package + " spark_job.jar " +
                     get_job_args();

  DP("Executing SSH command: %s\n", cmd.c_str());

  rc = ssh_channel_request_exec(channel, cmd.c_str());

  if (rc != SSH_OK)
  {
    ssh_channel_close(channel);
    ssh_channel_free(channel);
    return rc;
  }

  nbytes = ssh_channel_read(channel, buffer, sizeof(buffer), 1);
  while (nbytes > 0)
  {
    if (fwrite(buffer, 1, nbytes, stdout) != (unsigned int) nbytes)
    {
      ssh_channel_close(channel);
      ssh_channel_free(channel);
      return SSH_ERROR;
    }
    nbytes = ssh_channel_read(channel, buffer, sizeof(buffer), 1);
  }

  if (nbytes < 0)
  {
    ssh_channel_close(channel);
    ssh_channel_free(channel);
    return SSH_ERROR;
  }

  ssh_channel_send_eof(channel);
  ssh_channel_close(channel);
  ssh_channel_free(channel);

  ssh_disconnect(aws_session);
  ssh_free(aws_session);

  return rc;
}
