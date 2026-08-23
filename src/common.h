/* Shared helpers for the server and the load generator. */
#ifndef COMMON_H
#define COMMON_H

#include <stddef.h>
#include <stdint.h>

/* Wire protocol: 4-byte big-endian payload length, then that many bytes.
 * The server echoes the payload back with identical framing. */
#define PROTO_HDR_LEN 4u
#define PROTO_MAX_PAYLOAD (1u << 16)

uint64_t now_ns(void);
void sleep_ms(unsigned ms);

int set_nonblocking(int fd);
int set_nodelay(int fd);

void die(const char *fmt, ...);

/* "--name=123" -> *out = 123. Returns 1 on match, 0 if the flag is not `name`,
 * and exits on a malformed value. */
int arg_long(const char *arg, const char *name, long *out);
int arg_str(const char *arg, const char *name, const char **out);

#endif
