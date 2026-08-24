/*
 * Single-threaded, edge-triggered epoll mode.
 *
 * One thread owns every connection, so nothing here needs locking; the shared
 * counters in g_stats are atomic only because the thread mode and the RSS
 * monitor share them.
 */
#define _GNU_SOURCE
#include "server.h"

#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#define WBUF_HIGH (256u * 1024u) /* stop reading above this backlog */
#define WBUF_LOW (64u * 1024u)   /* resume reading below this       */
#define MAX_EVENTS 512

typedef struct conn {
    struct conn *prev, *next; /* live-connection list, for shutdown */
    int fd;
    unsigned char *rbuf;
    size_t rcap, rlen;
    unsigned char *wbuf;
    size_t wcap, wlen, woff; /* pending bytes are wbuf[woff .. wlen) */
    uint32_t events;         /* what is currently registered */
    int peer_closed;         /* saw EOF; close once the write buffer drains */
} conn;

/* Only the epoll mode keeps this list: it is single-threaded, so no locking. */
static conn *g_live;

static void live_link(conn *c) {
    c->prev = NULL;
    c->next = g_live;
    if (g_live) g_live->prev = c;
    g_live = c;
}

static void live_unlink(conn *c) {
    if (c->prev)
        c->prev->next = c->next;
    else
        g_live = c->next;
    if (c->next) c->next->prev = c->prev;
}

static int mod_events(int epfd, conn *c, uint32_t events) {
    if (events == c->events) return 0;
    struct epoll_event ev = {.events = events, .data.ptr = c};
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, c->fd, &ev) < 0) return -1;
    c->events = events;
    return 0;
}

static void conn_free(int epfd, conn *c) {
    live_unlink(c);
    epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    free(c->rbuf);
    free(c->wbuf);
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

/* Append to the write buffer, compacting consumed bytes first. */
static int wbuf_append(conn *c, const unsigned char *data, size_t n) {
    if (c->woff > 0) {
        memmove(c->wbuf, c->wbuf + c->woff, c->wlen - c->woff);
        c->wlen -= c->woff;
        c->woff = 0;
    }
    if (c->wlen + n > c->wcap) {
        size_t cap = c->wcap ? c->wcap : 4096;
        while (cap < c->wlen + n) cap *= 2;
        unsigned char *p = realloc(c->wbuf, cap);
        if (!p) return -1;
        c->wbuf = p;
        c->wcap = cap;
    }
    memcpy(c->wbuf + c->wlen, data, n);
    c->wlen += n;
    return 0;
}

static size_t wbuf_pending(const conn *c) { return c->wlen - c->woff; }

/* Write out `data`, buffering whatever the socket would not take. */
static int conn_send(conn *c, const unsigned char *data, size_t n) {
    size_t off = 0;
    if (wbuf_pending(c) == 0) {
        while (off < n) {
            ssize_t w = write(c->fd, data + off, n - off);
            if (w > 0) {
                off += (size_t)w;
                continue;
            }
            if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            if (w < 0 && errno == EINTR) continue;
            return -1;
        }
        atomic_fetch_add(&g_stats.bytes_out, off);
    }
    if (off < n) return wbuf_append(c, data + off, n - off);
    return 0;
}

/* Flush buffered output. Returns -1 on a fatal socket error. */
static int conn_flush(conn *c) {
    while (wbuf_pending(c) > 0) {
        ssize_t w = write(c->fd, c->wbuf + c->woff, wbuf_pending(c));
        if (w > 0) {
            c->woff += (size_t)w;
            atomic_fetch_add(&g_stats.bytes_out, (size_t)w);
            continue;
        }
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return 0;
        if (w < 0 && errno == EINTR) continue;
        return -1;
    }
    c->woff = c->wlen = 0;
    return 0;
}

/*
 * Echo every complete frame sitting in the read buffer. Frames are already in
 * wire format, so a run of them is echoed with a single write rather than one
 * write per frame.
 */
static int conn_process(conn *c) {
    size_t off = 0;
    unsigned long frames = 0;

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
        off += need;
        frames++;
    }

    if (off > 0) {
        if (conn_send(c, c->rbuf, off) < 0) return -1;
        atomic_fetch_add(&g_stats.requests, frames);
        c->rlen -= off;
        if (c->rlen) memmove(c->rbuf, c->rbuf + off, c->rlen);
    }
    return 0;
}

static int conn_read(conn *c) {
    for (;;) {
        if (wbuf_pending(c) >= WBUF_HIGH) return 0; /* back-pressure */
        if (c->rlen == c->rcap && rbuf_reserve(c, c->rcap * 2) < 0) return -1;

        ssize_t r = read(c->fd, c->rbuf + c->rlen, c->rcap - c->rlen);
        if (r > 0) {
            c->rlen += (size_t)r;
            atomic_fetch_add(&g_stats.bytes_in, (size_t)r);
            if (conn_process(c) < 0) return -1;
            continue;
        }
        if (r == 0) {
            c->peer_closed = 1;
            return 0;
        }
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

    g_wake_fd = eventfd(0, EFD_NONBLOCK);
    struct epoll_event wev = {.events = EPOLLIN, .data.ptr = &g_wake_fd};
    epoll_ctl(epfd, EPOLL_CTL_ADD, g_wake_fd, &wev);

    struct epoll_event *events = calloc(MAX_EVENTS, sizeof(*events));
    if (!events) die("calloc events");

    while (!g_stop) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            die("epoll_wait");
        }

        for (int i = 0; i < n; i++) {
            void *ptr = events[i].data.ptr;

            if (ptr == &g_wake_fd) continue; /* shutdown nudge */

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
                    c->events = EPOLLIN | EPOLLET | EPOLLRDHUP;
                    struct epoll_event cev = {.events = c->events, .data.ptr = c};
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev) < 0) {
                        free(c->rbuf);
                        free(c);
                        close(cfd);
                        atomic_fetch_add(&g_stats.errors, 1);
                        continue;
                    }
                    live_link(c);
                    atomic_fetch_add(&g_stats.accepted, 1);
                    bump_active(1);
                }
                continue;
            }

            conn *c = ptr;
            uint32_t e = events[i].events;
            int dead = 0;

            if (e & (EPOLLERR | EPOLLHUP)) dead = 1;
            if (!dead && (e & EPOLLOUT) && conn_flush(c) < 0) dead = 1;
            if (!dead && (e & (EPOLLIN | EPOLLRDHUP)) && conn_read(c) < 0) dead = 1;
            if (!dead && conn_flush(c) < 0) dead = 1;

            if (!dead) {
                uint32_t want = EPOLLET | EPOLLRDHUP;
                size_t pending = wbuf_pending(c);
                if (pending > 0) want |= EPOLLOUT;
                if (!c->peer_closed && pending < WBUF_LOW) want |= EPOLLIN;
                if (mod_events(epfd, c, want) < 0) dead = 1;
                /* Peer is gone and everything is flushed: nothing left to do. */
                if (c->peer_closed && pending == 0) dead = 1;
            }

            if (dead) conn_free(epfd, c);
        }
    }

    /* Graceful shutdown: flush what we can, then close every live connection. */
    long closed = 0;
    while (g_live) {
        conn *c = g_live;
        conn_flush(c);
        conn_free(epfd, c);
        closed++;
    }
    fprintf(stderr, "shutdown closed_connections=%ld\n", closed);

    free(events);
    close(epfd);
    close(lfd);
    report(mode);
}
