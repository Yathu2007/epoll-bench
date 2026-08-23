/*
 * Framed-echo TCP server.
 *
 *   --mode=epoll   single thread, edge-triggered epoll, non-blocking sockets
 *   --mode=thread  one pthread per connection, blocking I/O
 *
 * This file holds everything the two modes have in common - argument parsing,
 * signal handling, the listening socket and the counters - so that the only
 * thing differing between them is the I/O strategy in server_epoll.c and
 * server_thread.c.
 */

#define _GNU_SOURCE
#include "server.h"

#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

struct server_stats g_stats;

volatile sig_atomic_t g_stop;
static int g_listen_fd = -1;

static void on_signal(int sig) {
    (void)sig;
    g_stop = 1;
    /* The signal may be delivered to a connection thread, which leaves the
     * accept loop blocked; shutting the listener down wakes it. */
    if (g_listen_fd >= 0) shutdown(g_listen_fd, SHUT_RDWR);
}

void bump_active(long delta) {
    long now = atomic_fetch_add(&g_stats.active, delta) + delta;
    long peak = atomic_load(&g_stats.peak_active);
    while (now > peak && !atomic_compare_exchange_weak(&g_stats.peak_active, &peak, now)) {
    }
}

void report(const char *mode) {
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    double wall = (double)(now_ns() - g_stats.t_start) / 1e9;
    double user = (double)ru.ru_utime.tv_sec + (double)ru.ru_utime.tv_usec / 1e6;
    double sys = (double)ru.ru_stime.tv_sec + (double)ru.ru_stime.tv_usec / 1e6;

    fprintf(stderr,
            "server mode=%s accepted=%llu requests=%llu bytes_in=%llu bytes_out=%llu "
            "errors=%llu peak_conns=%ld user_cpu_s=%.3f sys_cpu_s=%.3f wall_s=%.3f\n",
            mode, (unsigned long long)atomic_load(&g_stats.accepted),
            (unsigned long long)atomic_load(&g_stats.requests),
            (unsigned long long)atomic_load(&g_stats.bytes_in),
            (unsigned long long)atomic_load(&g_stats.bytes_out),
            (unsigned long long)atomic_load(&g_stats.errors),
            atomic_load(&g_stats.peak_active), user, sys, wall);
}

static int listen_socket(int port, int backlog) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) die("socket");

    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) die("bind port %d", port);
    if (listen(fd, backlog) < 0) die("listen");
    /* Non-blocking: the epoll loop drains the accept queue until EAGAIN.
     * (The thread mode re-blocks it.) */
    if (set_nonblocking(fd) < 0) die("set_nonblocking listen");
    return fd;
}

int main(int argc, char **argv) {
    long port = 9000, backlog = 4096, stack_kb = 512;
    const char *mode = "epoll";

    for (int i = 1; i < argc; i++) {
        if (arg_long(argv[i], "port", &port)) continue;
        if (arg_long(argv[i], "backlog", &backlog)) continue;
        if (arg_long(argv[i], "stack-kb", &stack_kb)) continue;
        if (arg_str(argv[i], "mode", &mode)) continue;
        fprintf(stderr,
                "usage: %s [--port=9000] [--mode=epoll|thread] [--backlog=4096] [--stack-kb=512]\n",
                argv[0]);
        return 2;
    }
    if (strcmp(mode, "epoll") != 0 && strcmp(mode, "thread") != 0)
        die("unknown mode '%s' (expected epoll or thread)", mode);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, NULL);
    sa.sa_handler = on_signal; /* no SA_RESTART: blocking accept must return */
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    g_stats.t_start = now_ns();

    int lfd = listen_socket((int)port, (int)backlog);
    g_listen_fd = lfd;
    fprintf(stderr, "listening mode=%s port=%ld backlog=%ld\n", mode, port, backlog);

    if (strcmp(mode, "epoll") == 0)
        run_epoll(lfd, mode);
    else
        run_thread_per_conn(lfd, mode, stack_kb);

    return 0;
}
