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


int verify_knownhost(ssh_session session) {
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
