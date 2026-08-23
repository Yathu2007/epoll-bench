/*
 * Thread-per-connection mode: one detached pthread per connection, blocking
 * I/O, --stack-kb (default 512) per thread, same protocol handling as the
 * epoll mode.
 *
 * This is the comparison baseline. It shares the accept path and the counters
 * with the epoll mode so the I/O strategy is the only variable between them.
 */
#define _GNU_SOURCE
#include "server.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static void *conn_thread(void *arg) {
    int fd = (int)(intptr_t)arg;
    size_t cap = RBUF_INIT, len = 0;
    unsigned char *buf = malloc(cap);
    if (!buf) goto out;

    for (;;) {
        if (len == cap) {
            size_t ncap = cap * 2;
            unsigned char *p = realloc(buf, ncap);
            if (!p) break;
            buf = p;
            cap = ncap;
        }
        ssize_t r = read(fd, buf + len, cap - len);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) break;
        len += (size_t)r;
        atomic_fetch_add(&g_stats.bytes_in, (size_t)r);

        /* Echo every complete frame in one blocking write. */
        size_t off = 0;
        unsigned long frames = 0;
        int bad = 0;
        for (;;) {
            if (len - off < PROTO_HDR_LEN) break;
            uint32_t plen;
            memcpy(&plen, buf + off, PROTO_HDR_LEN);
            plen = ntohl(plen);
            if (plen > PROTO_MAX_PAYLOAD) {
                bad = 1;
                break;
            }
            size_t need = PROTO_HDR_LEN + plen;
            if (len - off < need) {
                if (need > cap) {
                    unsigned char *p = realloc(buf, need);
                    if (!p) {
                        bad = 1;
                        break;
                    }
                    buf = p;
                    cap = need;
                }
                break;
            }
            off += need;
            frames++;
        }
        if (bad) break;

        size_t sent = 0;
        while (sent < off) {
            ssize_t w = write(fd, buf + sent, off - sent);
            if (w < 0 && errno == EINTR) continue;
            if (w <= 0) {
                bad = 1;
                break;
            }
            sent += (size_t)w;
            atomic_fetch_add(&g_stats.bytes_out, (size_t)w);
        }
        if (bad) break;

        atomic_fetch_add(&g_stats.requests, frames);
        len -= off;
        if (len) memmove(buf, buf + off, len);
    }

    free(buf);
out:
    close(fd);
    bump_active(-1);
    return NULL;
}

void run_thread_per_conn(int lfd, const char *mode, long stack_kb) {
    int flags = fcntl(lfd, F_GETFL, 0);
    fcntl(lfd, F_SETFL, flags & ~O_NONBLOCK); /* blocking accept, one thread */

    int reported_limit = 0;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (stack_kb > 0) pthread_attr_setstacksize(&attr, (size_t)stack_kb * 1024);

    while (!g_stop) {
        int cfd = accept(lfd, NULL, NULL);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            if (errno == EMFILE || errno == ENFILE) {
                atomic_fetch_add(&g_stats.errors, 1);
                sleep_ms(1);
                continue;
            }
            break;
        }
        set_nodelay(cfd);

        pthread_t th;
        int rc = pthread_create(&th, &attr, conn_thread, (void *)(intptr_t)cfd);
        if (rc != 0) {
            /* thread-per-connection ceiling */
            if (!reported_limit) {
                reported_limit = 1;
                fprintf(stderr, "thread-limit conns=%ld reason=%s\n", atomic_load(&g_stats.active),
                        strerror(rc));
            }
            atomic_fetch_add(&g_stats.errors, 1);
            close(cfd);
            continue;
        }
        atomic_fetch_add(&g_stats.accepted, 1);
        bump_active(1);
    }

    pthread_attr_destroy(&attr);
    close(lfd);
    report(mode);
}
