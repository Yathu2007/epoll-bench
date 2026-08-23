/* State and helpers shared by the server's connection handling. */
#ifndef SERVER_H
#define SERVER_H

#include <stdatomic.h>

#include "common.h"

#define RBUF_INIT 1024u /* per-connection read buffer; grown only if a frame needs more */

/* Counters the connection handling maintains. report() prints them at shutdown. */
struct server_stats {
    atomic_ullong accepted, requests, bytes_in, bytes_out, errors;
    atomic_long active, peak_active;
    uint64_t t_start;
};

extern struct server_stats g_stats;

void bump_active(long delta);
void report(void);

void run_thread_per_conn(int lfd);

#endif
