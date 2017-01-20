//===-------------------- Target RTLs Implementation -------------- C++ -*-===//
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


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <thread>
#include <libssh/libssh.h>

#include "ssh.h"


int ssh_verify_knownhost(ssh_session session) {
  int state;
  char buf[10];
  state = ssh_is_server_known(session);

  switch (state) {
    case SSH_SERVER_KNOWN_OK:
      break; /* ok */
    case SSH_SERVER_KNOWN_CHANGED:
      fprintf(stderr, "Host key for server changed: it is now:\n");
      fprintf(stderr, "For security reasons, connection will be stopped\n");
      return -1;
    case SSH_SERVER_FOUND_OTHER:
      fprintf(stderr, "The host key for this server was not found but an other"
                      "type of key exists.\n");
      fprintf(stderr,
              "An attacker might change the default server key to"
              "confuse your client into thinking the key does not exist\n");
      return -1;
    case SSH_SERVER_FILE_NOT_FOUND:
      fprintf(stderr, "Could not find known host file.\n");
      fprintf(stderr, "If you accept the host key here, the file will be"
                      "automatically created.\n");
      /* fallback to SSH_SERVER_NOT_KNOWN behavior */
    case SSH_SERVER_NOT_KNOWN:
      // FIXME: Do we check the server ?
      /*
    fprintf(stderr, "The server is unknown. Do you trust the host key?\n");
    if (fgets(buf, sizeof(buf), stdin) == NULL) {
      return -1;
    }
    if (strncasecmp(buf, "yes", 3) != 0) {
      return -1;
    }*/
      if (ssh_write_knownhost(session) < 0) {
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


int ssh_copy(ssh_session session, const char* filename, const char* destpath, const char* destname) {
  int rc;

  // Copy jar file
  // Reading contents of the jar file

  FILE *file = fopen(filename, "rb");

   // if you have a stream (e.g. from fopen), not a file descriptor.
  int fd = fileno(file);
  struct stat sjar;
  fstat(fd, &sjar);
  int size = sjar.st_size;

  fprintf(stdout, "Size => %d.\n", size);

  void *fbuffer = malloc(size * sizeof(char));

  if (!file) {
    fprintf(stderr, "Could not open temporary file.\n");
    return OFFLOAD_FAIL;
  }

  ssh_scp scp;
  scp = ssh_scp_new(session, SSH_SCP_WRITE, destpath);
  if (scp == NULL) {
    fprintf(stderr, "Error allocating scp session: %s\n",
            ssh_get_error(session));
    return SSH_ERROR;
  }

  rc = ssh_scp_init(scp);
  if (rc != SSH_OK) {
    fprintf(stderr, "Error initializing scp session: %s\n",
            ssh_get_error(session));
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

  rc = ssh_scp_push_file(scp, destname, size, S_IRUSR | S_IWUSR);
  if (rc != SSH_OK) {
    fprintf(stderr, "Can't open remote file: %s\n", ssh_get_error(session));
    return rc;
  }

  if (fread(fbuffer, 1, size, file) != size) {
    fprintf(stderr, "Could not successfully read temporary file.\n");
    fclose(file);
    return OFFLOAD_FAIL;
  }

  rc = ssh_scp_write(scp, fbuffer, size);
  if (rc != SSH_OK) {
    fprintf(stderr, "Can't write to remote file: %s\n",
            ssh_get_error(session));
    return rc;
  }

  fclose(file);
  ssh_scp_close(scp);
  ssh_scp_free(scp);

  return OFFLOAD_SUCCESS;
}

int ssh_run(ssh_session session, const char* cmd) {
  int rc;
  char buffer[256];
  int nbytes;

  ssh_channel channel = ssh_channel_new(session);
  if (channel == NULL)
    return SSH_ERROR;

  rc = ssh_channel_open_session(channel);
  if (rc != SSH_OK) {
    ssh_channel_free(channel);
    return rc;
  }

  rc = ssh_channel_request_exec(channel, cmd);

  if (rc != SSH_OK) {
    ssh_channel_close(channel);
    ssh_channel_free(channel);
    return rc;
  }

  nbytes = ssh_channel_read(channel, buffer, sizeof(buffer), 1);
  while (nbytes > 0) {
    if (fwrite(buffer, 1, nbytes, stdout) != (unsigned int)nbytes) {
      ssh_channel_close(channel);
      ssh_channel_free(channel);
      return SSH_ERROR;
    }
    nbytes = ssh_channel_read(channel, buffer, sizeof(buffer), 1);
  }

  if (nbytes < 0) {
    ssh_channel_close(channel);
    ssh_channel_free(channel);
    return SSH_ERROR;
  }

  ssh_channel_send_eof(channel);
  ssh_channel_close(channel);
  ssh_channel_free(channel);

  return OFFLOAD_SUCCESS;
}
