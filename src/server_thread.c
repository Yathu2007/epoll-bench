/*
 * Thread-per-connection mode: one detached pthread per connection, blocking I/O.
 * SIGINT or SIGTERM breaks the accept loop.
 */
#define _GNU_SOURCE
#include "server.h"

#include <arpa/inet.h>
#include <errno.h>
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

        /* Echo every complete frame in one blocking write. */
        size_t off = 0;
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
        }
        if (bad) break;

        len -= off;
        if (len) memmove(buf, buf + off, len);
    }

    free(buf);
out:
    close(fd);
    return NULL;
}

void run_thread_per_conn(int lfd) {
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    while (!g_stop) {
        int cfd = accept(lfd, NULL, NULL);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            if (errno == EMFILE || errno == ENFILE) {
                sleep_ms(1);
                continue;
            }
            break;
        }
        set_nodelay(cfd);

        pthread_t th;
        if (pthread_create(&th, &attr, conn_thread, (void *)(intptr_t)cfd) != 0) {
            close(cfd);
            continue;
        }
    }

    pthread_attr_destroy(&attr);
    close(lfd);
}
