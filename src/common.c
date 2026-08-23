#define _GNU_SOURCE
#include "common.h"

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

void sleep_ms(unsigned ms) {
    struct timespec ts = {.tv_sec = ms / 1000, .tv_nsec = (long)(ms % 1000) * 1000000L};
    while (nanosleep(&ts, &ts) == -1 && errno == EINTR) {
    }
}

int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int set_nodelay(int fd) {
    int one = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

void die(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    if (errno) fprintf(stderr, ": %s", strerror(errno));
    fputc('\n', stderr);
    exit(1);
}

static const char *arg_match(const char *arg, const char *name) {
    size_t n = strlen(name);
    if (strncmp(arg, "--", 2) != 0) return NULL;
    if (strncmp(arg + 2, name, n) != 0) return NULL;
    if (arg[2 + n] != '=') return NULL;
    return arg + 2 + n + 1;
}

int arg_long(const char *arg, const char *name, long *out) {
    const char *v = arg_match(arg, name);
    if (!v) return 0;
    char *end;
    errno = 0;
    long x = strtol(v, &end, 10);
    if (errno || *end != '\0' || end == v) die("bad value for --%s: %s", name, v);
    *out = x;
    return 1;
}

int arg_str(const char *arg, const char *name, const char **out) {
    const char *v = arg_match(arg, name);
    if (!v) return 0;
    *out = v;
    return 1;
}
