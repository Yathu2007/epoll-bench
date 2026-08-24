/*
 * Load generator for the framed-echo protocol.
 *
 * Closed loop: every connection keeps --depth requests in flight at all times
 * and sends a new one the moment a response lands. This measures saturation
 * throughput, not service latency.
 */
#define _GNU_SOURCE
#include "common.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#define PENDING_CAP 512u /* max outstanding requests tracked per connection */
#define MAX_EVENTS 512

typedef struct {
    uint64_t t; /* timestamp latency is measured from */
} pending;

typedef struct conn {
    int fd;
    int dead;

    unsigned char *rbuf;
    size_t rcap, rlen;
    unsigned char *wbuf;
    size_t wcap, wlen, woff;
    uint32_t events;

    pending pend[PENDING_CAP];
    size_t phead, pcount;
} conn;

typedef struct worker {
    pthread_t th;
    int id;
    int epfd;
    conn *conns;
    long nconns;
    long established;

    uint64_t *lat; /* latency samples, in ns */
    size_t lat_cap, lat_n;

    uint64_t sent, recvd, drops, errors, timeouts;
} worker;

/* -------------------------------------------------------------- run config */

static const char *g_host = "127.0.0.1";
static long g_port = 9000;
static long g_conns = 100;
static long g_duration = 10;
static long g_payload = 128;
static long g_threads = 4;
static long g_depth = 1;
static long g_budget = 8u << 20; /* total latency samples kept */

static uint64_t g_t0;  /* start of the measured run */
static uint64_t g_t_end; /* end of the run */
static pthread_barrier_t g_bar;

static size_t g_frame_len;             /* PROTO_HDR_LEN + payload */
static unsigned char *g_frame_template; /* header + filler */

/* ---------------------------------------------------------------- sampling */

static void record(worker *w, uint64_t lat_ns) {
    if (w->lat_n < w->lat_cap) w->lat[w->lat_n++] = lat_ns;
}

/* ---------------------------------------------------------------- plumbing */

static int mod_events(worker *w, conn *c, uint32_t events) {
    if (events == c->events) return 0;
    struct epoll_event ev = {.events = events, .data.ptr = c};
    if (epoll_ctl(w->epfd, EPOLL_CTL_MOD, c->fd, &ev) < 0) return -1;
    c->events = events;
    return 0;
}

static void conn_kill(worker *w, conn *c) {
    if (c->dead) return;
    epoll_ctl(w->epfd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    c->dead = 1;
    w->timeouts += c->pcount;
    c->pcount = 0;
}

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

static int conn_write(conn *c, const unsigned char *data, size_t n) {
    size_t off = 0;
    if (wbuf_pending(c) == 0) {
        while (off < n) {
            ssize_t r = write(c->fd, data + off, n - off);
            if (r > 0) {
                off += (size_t)r;
                continue;
            }
            if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            if (r < 0 && errno == EINTR) continue;
            return -1;
        }
    }
    if (off < n) return wbuf_append(c, data + off, n - off);
    return 0;
}

static int conn_flush(conn *c) {
    while (wbuf_pending(c) > 0) {
        ssize_t r = write(c->fd, c->wbuf + c->woff, wbuf_pending(c));
        if (r > 0) {
            c->woff += (size_t)r;
            continue;
        }
        if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return 0;
        if (r < 0 && errno == EINTR) continue;
        return -1;
    }
    c->woff = c->wlen = 0;
    return 0;
}

/* Send one request, remembering `stamp` as the instant latency starts from. */
static int send_request(worker *w, conn *c, uint64_t stamp) {
    if (c->pcount == PENDING_CAP) {
        w->drops++;
        return 0; /* server is too far behind to track more in flight */
    }
    size_t idx = (c->phead + c->pcount) % PENDING_CAP;
    c->pend[idx].t = stamp;
    c->pcount++;

    if (conn_write(c, g_frame_template, g_frame_len) < 0) return -1;

    w->sent++;
    return 0;
}

static int conn_read(worker *w, conn *c) {
    for (;;) {
        if (c->rlen == c->rcap) {
            size_t cap = c->rcap * 2;
            unsigned char *p = realloc(c->rbuf, cap);
            if (!p) return -1;
            c->rbuf = p;
            c->rcap = cap;
        }
        ssize_t r = read(c->fd, c->rbuf + c->rlen, c->rcap - c->rlen);
        if (r == 0) return -1;
        if (r < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
            if (errno == EINTR) continue;
            return -1;
        }
        c->rlen += (size_t)r;

        uint64_t now = now_ns();
        size_t off = 0;
        for (;;) {
            if (c->rlen - off < PROTO_HDR_LEN) break;
            uint32_t len;
            memcpy(&len, c->rbuf + off, PROTO_HDR_LEN);
            len = ntohl(len);
            if (len != (uint32_t)g_payload) return -1; /* echo must be identical */
            if (c->rlen - off < PROTO_HDR_LEN + len) break;

            if (c->pcount == 0) return -1; /* unsolicited response */
            pending p = c->pend[c->phead];
            c->phead = (c->phead + 1) % PENDING_CAP;
            c->pcount--;

            w->recvd++;
            record(w, now - p.t);
            off += PROTO_HDR_LEN + len;

            /* Closed loop: one response out, one request in. */
            if (now < g_t_end && send_request(w, c, now) < 0) return -1;
        }
        if (off) {
            c->rlen -= off;
            if (c->rlen) memmove(c->rbuf, c->rbuf + off, c->rlen);
        }
    }
}

/* ------------------------------------------------------------- connect all */

static int connect_all(worker *w) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)g_port);
    if (inet_pton(AF_INET, g_host, &addr.sin_addr) != 1) die("bad host %s", g_host);

    long inflight = 0;
    for (long i = 0; i < w->nconns; i++) {
        conn *c = &w->conns[i];
        c->fd = socket(AF_INET, SOCK_STREAM, 0);
        if (c->fd < 0) {
            c->dead = 1;
            w->errors++;
            continue;
        }
        set_nonblocking(c->fd);
        set_nodelay(c->fd);
        int r = connect(c->fd, (struct sockaddr *)&addr, sizeof(addr));
        if (r < 0 && errno != EINPROGRESS) {
            close(c->fd);
            c->dead = 1;
            w->errors++;
            continue;
        }
        c->events = EPOLLOUT;
        struct epoll_event ev = {.events = EPOLLOUT, .data.ptr = c};
        if (epoll_ctl(w->epfd, EPOLL_CTL_ADD, c->fd, &ev) < 0) {
            close(c->fd);
            c->dead = 1;
            w->errors++;
            continue;
        }
        inflight++;
    }

    struct epoll_event ev[MAX_EVENTS];
    while (inflight > 0) {
        int k = epoll_wait(w->epfd, ev, MAX_EVENTS, 200);
        if (k < 0) {
            if (errno == EINTR) continue;
            break;
        }
        for (int i = 0; i < k; i++) {
            conn *c = ev[i].data.ptr;
            int err = 0;
            socklen_t elen = sizeof(err);
            getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &elen);
            inflight--;
            if (err != 0 || (ev[i].events & (EPOLLERR | EPOLLHUP))) {
                epoll_ctl(w->epfd, EPOLL_CTL_DEL, c->fd, NULL);
                close(c->fd);
                c->dead = 1;
                w->errors++;
                continue;
            }
            mod_events(w, c, EPOLLIN | EPOLLET);
            w->established++;
        }
    }
    return 0;
}

/* --------------------------------------------------------------- run phase */

static void *worker_main(void *arg) {
    worker *w = arg;
    connect_all(w);

    pthread_barrier_wait(&g_bar); /* everyone connected */
    pthread_barrier_wait(&g_bar); /* g_t0 published */

    struct epoll_event ev[MAX_EVENTS];

    uint64_t now = now_ns();
    for (long i = 0; i < w->nconns; i++) {
        conn *c = &w->conns[i];
        if (c->dead) continue;
        for (long d = 0; d < g_depth; d++)
            if (send_request(w, c, now) < 0) {
                conn_kill(w, c);
                w->errors++;
                break;
            }
    }

    for (;;) {
        if (now_ns() >= g_t_end) break;

        int n = epoll_wait(w->epfd, ev, MAX_EVENTS, 10);
        if (n < 0 && errno != EINTR) break;
        for (int i = 0; i < n; i++) {
            conn *c = ev[i].data.ptr;
            if (c->dead) continue;
            int dead = 0;
            if (ev[i].events & (EPOLLERR | EPOLLHUP)) dead = 1;
            if (!dead && (ev[i].events & EPOLLOUT) && conn_flush(c) < 0) dead = 1;
            if (!dead && (ev[i].events & EPOLLIN) && conn_read(w, c) < 0) dead = 1;
            if (!dead && conn_flush(c) < 0) dead = 1;
            if (!dead)
                mod_events(w, c, wbuf_pending(c) ? (EPOLLIN | EPOLLOUT | EPOLLET) : (EPOLLIN | EPOLLET));
            if (dead) {
                conn_kill(w, c);
                w->errors++;
            }
        }
    }

    /* Drain: give in-flight responses a bounded window to arrive. */
    uint64_t drain_end = now_ns() + 500000000ull;
    for (;;) {
        long outstanding = 0;
        for (long i = 0; i < w->nconns; i++)
            if (!w->conns[i].dead) outstanding += (long)w->conns[i].pcount;
        if (outstanding == 0 || now_ns() >= drain_end) break;

        int n = epoll_wait(w->epfd, ev, MAX_EVENTS, 20);
        if (n < 0 && errno != EINTR) break;
        for (int i = 0; i < n; i++) {
            conn *c = ev[i].data.ptr;
            if (c->dead) continue;
            if (conn_read(w, c) < 0) {
                conn_kill(w, c);
                w->errors++;
            }
        }
    }

    for (long i = 0; i < w->nconns; i++) {
        conn *c = &w->conns[i];
        if (c->dead) continue;
        w->timeouts += c->pcount;
        epoll_ctl(w->epfd, EPOLL_CTL_DEL, c->fd, NULL);
        close(c->fd);
    }
    return NULL;
}

/* ------------------------------------------------------------------- main */

int main(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        if (arg_str(argv[i], "host", &g_host)) continue;
        if (arg_long(argv[i], "port", &g_port)) continue;
        if (arg_long(argv[i], "conns", &g_conns)) continue;
        if (arg_long(argv[i], "duration", &g_duration)) continue;
        if (arg_long(argv[i], "payload", &g_payload)) continue;
        if (arg_long(argv[i], "threads", &g_threads)) continue;
        if (arg_long(argv[i], "depth", &g_depth)) continue;
        if (arg_long(argv[i], "samples", &g_budget)) continue;
        fprintf(stderr,
                "usage: %s [--host=127.0.0.1] [--port=9000] [--conns=100] [--duration=10]\n"
                "          [--payload=128] [--threads=4] [--depth=1] [--samples=8388608]\n",
                argv[0]);
        return 2;
    }
    if (g_payload < 8 || g_payload > (long)PROTO_MAX_PAYLOAD)
        die("--payload must be between 8 and %u", PROTO_MAX_PAYLOAD);
    if (g_conns < 1) die("--conns must be >= 1");
    if (g_threads > g_conns) g_threads = g_conns;

    signal(SIGPIPE, SIG_IGN);

    g_frame_len = PROTO_HDR_LEN + (size_t)g_payload;
    g_frame_template = malloc(g_frame_len);
    if (!g_frame_template) die("malloc frame");
    uint32_t nlen = htonl((uint32_t)g_payload);
    memcpy(g_frame_template, &nlen, PROTO_HDR_LEN);
    for (size_t i = PROTO_HDR_LEN; i < g_frame_len; i++) g_frame_template[i] = (unsigned char)i;

    worker *ws = calloc((size_t)g_threads, sizeof(*ws));
    if (!ws) die("calloc workers");
    size_t per_worker_budget = (size_t)g_budget / (size_t)g_threads + 1;

    for (long t = 0; t < g_threads; t++) {
        worker *w = &ws[t];
        w->id = (int)t;
        w->epfd = epoll_create1(0);
        if (w->epfd < 0) die("epoll_create1");
        w->nconns = g_conns / g_threads + (t < g_conns % g_threads ? 1 : 0);
        w->conns = calloc((size_t)w->nconns, sizeof(conn));
        if (!w->conns) die("calloc conns");
        w->lat_cap = per_worker_budget;
        w->lat = malloc(w->lat_cap * sizeof(uint64_t));
        if (!w->lat) die("malloc latency buffer");
        for (long i = 0; i < w->nconns; i++) {
            conn *c = &w->conns[i];
            c->rcap = 4096;
            c->rbuf = malloc(c->rcap);
            if (!c->rbuf) die("malloc read buffer");
        }
    }

    pthread_barrier_init(&g_bar, NULL, (unsigned)g_threads + 1);

    uint64_t t_connect = now_ns();
    for (long t = 0; t < g_threads; t++) pthread_create(&ws[t].th, NULL, worker_main, &ws[t]);

    pthread_barrier_wait(&g_bar);
    double connect_s = (double)(now_ns() - t_connect) / 1e9;

    g_t0 = now_ns() + 50000000ull;
    g_t_end = g_t0 + (uint64_t)g_duration * 1000000000ull;
    pthread_barrier_wait(&g_bar);

    for (long t = 0; t < g_threads; t++) pthread_join(ws[t].th, NULL);

    /* -------------------------------------------------------- aggregate */
    uint64_t sent = 0, recvd = 0, drops = 0, errors = 0, timeouts = 0;
    long established = 0;
    size_t total = 0;
    for (long t = 0; t < g_threads; t++) total += ws[t].lat_n;

    uint64_t *lat = malloc((total ? total : 1) * sizeof(uint64_t));
    if (!lat) die("malloc merge buffer");
    size_t off = 0;
    for (long t = 0; t < g_threads; t++) {
        worker *w = &ws[t];
        memcpy(lat + off, w->lat, w->lat_n * sizeof(uint64_t));
        off += w->lat_n;
        sent += w->sent;
        recvd += w->recvd;
        drops += w->drops;
        errors += w->errors;
        timeouts += w->timeouts;
        established += w->established;
    }
    sort_u64(lat, total);

    double measured_s = (double)(g_t_end - g_t0) / 1e9;
    double achieved = measured_s > 0 ? (double)recvd / measured_s : 0.0;

    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    double user = (double)ru.ru_utime.tv_sec + (double)ru.ru_utime.tv_usec / 1e6;
    double sys = (double)ru.ru_stime.tv_sec + (double)ru.ru_stime.tv_usec / 1e6;

#define US(p) ((double)percentile(lat, total, (p)) / 1000.0)
    fprintf(stderr,
            "loadgen closed-loop conns=%ld/%ld established in %.2fs payload=%ldB threads=%ld\n"
            "  achieved=%.0f req/s over %.0fs\n"
            "  latency us: p50=%.1f p90=%.1f p99=%.1f p99.9=%.1f max=%.1f (n=%zu)\n"
            "  sent=%llu recvd=%llu errors=%llu drops=%llu timeouts=%llu\n"
            "  loadgen cpu: user=%.2fs sys=%.2fs\n",
            established, g_conns, connect_s, g_payload, g_threads, achieved, measured_s, US(0.50),
            US(0.90), US(0.99), US(0.999), total ? (double)lat[total - 1] / 1000.0 : 0.0, total,
            (unsigned long long)sent, (unsigned long long)recvd, (unsigned long long)errors,
            (unsigned long long)drops, (unsigned long long)timeouts, user, sys);
#undef US

    return 0;
}
