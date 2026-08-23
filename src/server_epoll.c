/*
 * Single-threaded, edge-triggered epoll mode.
 *
 * One thread owns every connection, so nothing here needs locking; the shared
 * counters in g_stats are atomic only because the thread mode shares them.
 */
#define _GNU_SOURCE
#include "server.h"

#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#define MAX_EVENTS 512

typedef struct conn {
    int fd;
    unsigned char *rbuf;
    size_t rcap, rlen;
} conn;

static void conn_free(int epfd, conn *c) {
    epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    free(c->rbuf);
    free(c);
    bump_active(-1);
}

static int rbuf_reserve(conn *c, size_t need) {
    if (need <= c->rcap) return 0;
    size_t cap = c->rcap ? c->rcap : RBUF_INIT;
    while (cap < need) cap *= 2;
    unsigned char *p = realloc(c->rbuf, cap);
    if (!p) return -1;
    c->rbuf = p;
    c->rcap = cap;
    return 0;
}

/* Echo one frame. A socket that will not take all of it drops the connection
 * for now; buffering the remainder comes next. */
static int conn_write_all(conn *c, const unsigned char *data, size_t n) {
    size_t off = 0;
    while (off < n) {
        ssize_t w = write(c->fd, data + off, n - off);
        if (w > 0) {
            off += (size_t)w;
            continue;
        }
        if (w < 0 && errno == EINTR) continue;
        return -1;
    }
    atomic_fetch_add(&g_stats.bytes_out, off);
    return 0;
}

/* Echo every complete frame sitting in the read buffer. */
static int conn_process(conn *c) {
    size_t off = 0;

    for (;;) {
        if (c->rlen - off < PROTO_HDR_LEN) break;
        uint32_t len;
        memcpy(&len, c->rbuf + off, PROTO_HDR_LEN);
        len = ntohl(len);
        if (len > PROTO_MAX_PAYLOAD) return -1; /* protocol violation */
        size_t need = PROTO_HDR_LEN + len;
        if (c->rlen - off < need) {
            if (rbuf_reserve(c, need) < 0) return -1;
            break;
        }
        /* Frames are already in wire format, so the read buffer doubles as
         * the response buffer. */
        if (conn_write_all(c, c->rbuf + off, need) < 0) return -1;
        atomic_fetch_add(&g_stats.requests, 1);
        off += need;
    }

    if (off > 0) {
        c->rlen -= off;
        if (c->rlen) memmove(c->rbuf, c->rbuf + off, c->rlen);
    }
    return 0;
}

static int conn_read(conn *c) {
    for (;;) {
        if (c->rlen == c->rcap && rbuf_reserve(c, c->rcap * 2) < 0) return -1;

        ssize_t r = read(c->fd, c->rbuf + c->rlen, c->rcap - c->rlen);
        if (r > 0) {
            c->rlen += (size_t)r;
            atomic_fetch_add(&g_stats.bytes_in, (size_t)r);
            if (conn_process(c) < 0) return -1;
            continue;
        }
        if (r == 0) return -1; /* peer closed */
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        if (errno == EINTR) continue;
        return -1;
    }
}

void run_epoll(int lfd, const char *mode) {
    int epfd = epoll_create1(0);
    if (epfd < 0) die("epoll_create1");

    struct epoll_event ev = {.events = EPOLLIN | EPOLLET, .data.ptr = NULL};
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &ev) < 0) die("epoll_ctl listen");

    struct epoll_event *events = calloc(MAX_EVENTS, sizeof(*events));
    if (!events) die("calloc events");

    while (!g_stop) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, 100);
        if (n < 0) {
            if (errno == EINTR) continue;
            die("epoll_wait");
        }

        for (int i = 0; i < n; i++) {
            void *ptr = events[i].data.ptr;

            if (ptr == NULL) {
                /* Listening socket is edge-triggered: drain the accept queue. */
                for (;;) {
                    int cfd = accept4(lfd, NULL, NULL, SOCK_NONBLOCK);
                    if (cfd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        if (errno == EINTR) continue;
                        if (errno == EMFILE || errno == ENFILE) {
                            atomic_fetch_add(&g_stats.errors, 1);
                            break;
                        }
                        break;
                    }
                    set_nodelay(cfd);

                    conn *c = calloc(1, sizeof(*c));
                    if (!c || rbuf_reserve(c, RBUF_INIT) < 0) {
                        free(c);
                        close(cfd);
                        atomic_fetch_add(&g_stats.errors, 1);
                        continue;
                    }
                    c->fd = cfd;
                    struct epoll_event cev = {.events = EPOLLIN | EPOLLET, .data.ptr = c};
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev) < 0) {
                        free(c->rbuf);
                        free(c);
                        close(cfd);
                        atomic_fetch_add(&g_stats.errors, 1);
                        continue;
                    }
                    atomic_fetch_add(&g_stats.accepted, 1);
                    bump_active(1);
                }
                continue;
            }

            conn *c = ptr;
            uint32_t e = events[i].events;
            int dead = 0;

            if (e & (EPOLLERR | EPOLLHUP)) dead = 1;
            if (!dead && (e & EPOLLIN) && conn_read(c) < 0) dead = 1;

            if (dead) conn_free(epfd, c);
        }
    }

    free(events);
    close(epfd);
    close(lfd);
    report(mode);
}
