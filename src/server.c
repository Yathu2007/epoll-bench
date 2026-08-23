/*
 * Framed-echo TCP server: one pthread per connection, blocking I/O.
 *
 * This file holds the parts that are not about I/O strategy - ie. argument
 * parsing and the listening socket
 */

#define _GNU_SOURCE
#include "server.h"

#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

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
    return fd;
}

int main(int argc, char **argv) {
    long port = 9000, backlog = 4096;

    for (int i = 1; i < argc; i++) {
        if (arg_long(argv[i], "port", &port)) continue;
        if (arg_long(argv[i], "backlog", &backlog)) continue;
        fprintf(stderr, "usage: %s [--port=9000] [--backlog=4096]\n", argv[0]);
        return 2;
    }

    int lfd = listen_socket((int)port, (int)backlog);
    fprintf(stderr, "listening port=%ld backlog=%ld\n", port, backlog);

    run_thread_per_conn(lfd);
    return 0;
}
