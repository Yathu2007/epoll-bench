/* State and helpers shared by the server's connection handling. */
#ifndef SERVER_H
#define SERVER_H

#include "common.h"

#define RBUF_INIT 1024u /* per-connection read buffer; grown only if a frame needs more */

void run_thread_per_conn(int lfd);

#endif
