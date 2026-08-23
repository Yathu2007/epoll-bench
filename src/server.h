/* State and helpers shared by the two server modes. */
#ifndef SERVER_H
#define SERVER_H

#include <signal.h>
#include <stdatomic.h>

#include "common.h"

#define RBUF_INIT 1024u /* per-connection read buffer; grown only if a frame needs more */

/* Counters both modes maintain. report() prints them at shutdown. */
struct server_stats {
    atomic_ullong accepted, requests, bytes_in, bytes_out, errors;
    atomic_long active, peak_active;
    uint64_t t_start;
};

extern struct server_stats g_stats;

/* Set by SIGINT/SIGTERM; both loops poll it. */
extern volatile sig_atomic_t g_stop;
/* eventfd the signal handler writes to, so the epoll loop's blocking wait
 * returns instead of sitting on an infinite timeout. -1 until run_epoll arms
 * it, which is why the handler checks before writing. */
extern int g_wake_fd;

void bump_active(long delta);
void report(const char *mode);

/* One per mode; see server_epoll.c and server_thread.c. */
void run_epoll(int lfd, const char *mode);
void run_thread_per_conn(int lfd, const char *mode, long stack_kb);

#endif
