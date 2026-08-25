# epoll-bench

Thread-per-connection vs. single-threaded `epoll`, measured on the same server.

Two C servers speak an identical framed-echo protocol and share the same accept path,
buffer handling and counters, so the **I/O strategy is the only variable** between them.
A purpose-built open-loop load generator drives them and reports latency percentiles that
are not corrupted by coordinated omission.

Everyone knows that `epoll` scales better. The point of this repo is to measure _where_ the crossover is, _what_ it costs, and where the event loop actually
loses.

```
make            # builds bin/server and bin/loadgen
make check      # correctness smoke test, both modes, 4 payload sizes
make bench      # full sweep -> results/{raw.csv,summary.md,env.txt}
```

---

## Headline result

**10,000 concurrent connections, fixed 100,000 req/s offered load, 128 B payload, server pinned to 2 cores.**

|                        |    `--mode=epoll` | `--mode=thread` |
| ---------------------- | ----------------: | --------------: |
| connections served     |        **10,000** |           9,060 |
| sustained throughput   | **100,000 req/s** |    90,600 req/s |
| p99.9 latency          |        **758 µs** |        1,387 µs |
| peak RSS               |       **12.6 MB** |         90.1 MB |
| memory per connection  |       **1.13 KB** |        10.01 KB |
| server CPU for the run |         **7.5 s** |          10.1 s |
| dropped connections    |             **0** |             940 |

Thread-per-connection did not merely get slower - it hit a **hard ceiling at 9,060
connections**, where `pthread_create` began returning `EAGAIN`:

```
thread-limit conns=9060 reason=Resource temporarily unavailable
```

- That ceiling is the cgroup `pids.max` of 9,291 on this box, not `RLIMIT_NPROC` (30,971).
- The 940 connections above it were accepted by the kernel and then closed by the server, which is why the thread run reports 940 errors and misses the offered rate by 9.4%.
- The `epoll` server carried the same load on one thread, with 8.9x less memory per connection and 25% less CPU.

### Where `epoll` loses

Under **closed-loop saturation** — every connection keeps a request in flight, no offered
rate — thread-per-connection wins on raw throughput:

| 10,000 conns, saturated |       `epoll` |          `thread` |
| ----------------------- | ------------: | ----------------: |
| throughput              | 130,517 req/s | **160,026 req/s** |
| p99 latency             |   **84.7 ms** |          124.7 ms |
| requests per CPU-second |   **125,000** |            78,000 |

The `epoll` server is **single-threaded**, so it uses one of the two pinned cores while the thread server uses both. Per unit of CPU the event loop does **1.6x the work**, and its tail is 32% better, but a one-thread design leaves absolute throughput on the table.

### Where it does not matter

Below ~1,000 connections the two designs are a wash on latency (p50 within 6 µs, p99 within 2 µs at 1,000 connections; both sustain the full offered rate). The `epoll` server still uses **36% less CPU** to deliver it. The scaling difference only becomes a correctness difference at 10k.

Full medians for every point are in [`results/summary.md`](results/summary.md); all 36 raw
runs are in [`results/raw.csv`](results/raw.csv).

---

## Methodology

Most "epoll is faster" benchmarks are wrong in one of a small number of ways. Each is
addressed explicitly:

**Coordinated omission.** A closed-loop client that waits for reply _n_ before sending
request _n+1_ silently lowers the offered load whenever the server stalls, so the stall
never appears in the latency numbers. The default mode here is **open loop**: each
connection has a fixed schedule derived from `--rate`, and every latency is measured from
the **intended** send time, not from when the client got around to sending. A server that
falls behind shows up as queueing delay. Closed loop still exists behind `--closed`, but it
is used only for the saturation table, never for latency claims.

**Warm-up.** `--warmup` seconds (3 in the sweep) run at full load and are excluded from
every percentile and from the achieved-rate figure.

**Run-to-run variance.** Every (mode, connections, loop) point is run `REPS` times (3) and
reported as the per-metric **median**, not a best-of.

**Sampling bias.** Latencies are kept in full up to `--samples`, then switch to **reservoir
sampling**, so a long run stays uniformly sampled instead of over-weighting whichever
window happened to fit in the buffer.

**Client-side timing skew.** The generator sleeps with `epoll_pwait2` and spins only the
last 60 µs before a scheduled send, so client scheduling jitter is not charged to the
server.

**Correctness under load.** Every request carries a sequence number inside its payload; the
generator checks each echo against FIFO order and counts mismatches. A run reporting any
`mismatch` is a failed run, not a fast one. `make check` additionally exercises both modes
at 8 B, 128 B, 4 KB and 64 KB payloads.

**Memory measured under load, not after.** A monitor thread samples `VmRSS` every 50 ms
while traffic is flowing; per-connection cost is `(peak RSS − baseline RSS) / peak
connections`. Note this is _resident_ memory — the thread mode's 512 KB stacks are mostly
untouched virtual address space, which is why the honest figure is 10 KB/conn and not
512 KB/conn.

**Interference.** Server and load generator are pinned to disjoint CPU sets with `taskset`
(`0-1` and `4-11`), so the generator cannot steal the server's cores.

**Fair comparison.** Both modes share `server.c` — same listening socket, same backlog, same
`TCP_NODELAY`, same framing, same counters. `server_epoll.c` and `server_thread.c` differ
only in how they wait for readable sockets.

---

## Protocol

4-byte big-endian payload length, then that many bytes (max 64 KiB). The server echoes the
payload back with identical framing. This framing choice is deliberate since it forces real partial-read and
partial-write handling.

## What the two modes actually do

**`--mode=epoll`** — one thread, edge-triggered `epoll`, non-blocking sockets, no locking on
the connection path.

- The listening socket is edge-triggered; the accept queue is drained with
  `accept4(..., SOCK_NONBLOCK)` until `EAGAIN`.
- Short writes are **buffered**, not treated as fatal; the connection re-arms `EPOLLOUT` and
  flushes on the next wakeup.
- **Watermark back-pressure**: a connection stops being read above 256 KB of pending output
  and resumes below 64 KB, so one slow reader cannot drive unbounded memory growth.
- A run of pipelined frames sitting in the read buffer is echoed with a **single** `write`
  rather than one syscall per frame.
- `EPOLLRDHUP` marks the peer closed; the connection is torn down only once its write buffer
  has drained.
- `SIGINT` writes to an `eventfd` registered with the loop, so a blocking `epoll_wait`
  returns and every live connection is flushed and closed — the shutdown path reports how
  many.

**`--mode=thread`** — the baseline: blocking `accept`, one detached pthread per connection,
blocking `read`/`write`, `--stack-kb` (default 512) of stack each, identical frame handling.

## Reproducing

```sh
git clone https://github.com/Yathu2007/epoll-bench && cd epoll-bench
ulimit -n 100000            # the 10k-connection points need this
make check                  # verify both modes are correct first
make bench                  # writes results/
```

The sweep is parameterised by environment variable:

```sh
CONNS="100 1000" REPS=1 DURATION=5 ./bench.sh
MODES=epoll RATE=200000 PAYLOAD=1024 ./bench.sh
```

`bench.sh` records the machine it ran on (kernel, CPU, core count, `ulimit -n`, `ulimit -u`,
`somaxconn`, cgroup `pids.max`, compiler) into `results/env.txt` alongside the numbers, so a
results table is never separated from the box that produced it.

Either binary can also be driven by hand:

```sh
./bin/server  --port=9000 --mode=epoll [--backlog=4096] [--stack-kb=512]
./bin/loadgen --host=127.0.0.1 --port=9000 --conns=10000 --rate=100000 \
              --payload=128 --duration=10 --warmup=3 --threads=4
./bin/loadgen --conns=1000 --closed --depth=1        # saturation instead of fixed rate
```

## Environment these numbers came from

```
kernel:  Linux 6.18.33.2-microsoft-standard-WSL2   (Ubuntu 24.04.4 LTS)
cpu:     13th Gen Intel Core i7-13620H, 16 logical cores, 7.6 GiB RAM
limits:  ulimit -n 1048576, ulimit -u 30971, somaxconn 4096, cgroup pids.max 9291
pinning: server on CPUs 0-1, load generator on CPUs 4-11
cc:      gcc 13.3.0, -O2 -std=c11 -Wall -Wextra -Wpedantic
```

## Limitations

- **Loopback only.** No NIC, no driver, no network stack under load. Absolute latencies are
  a floor, not a wire-realistic figure.
- **WSL2 kernel.** The 9,060-connection thread ceiling is a cgroup `pids.max` artifact of
  this environment; on a box with a higher pid limit the thread mode would degrade more
  gracefully, though the memory and CPU curves would not change.
- **One thread vs. many.** The saturation-throughput comparison is not apples-to-apples, as
  noted above. A sharded event loop (`SO_REUSEPORT`, one loop per core) is the fix, and is
  not implemented.
- **No `io_uring` mode**, no TLS, no HTTP — the protocol is deliberately trivial so that the
  I/O strategy is what is being measured.
- Single machine, single sweep; medians over 3 repetitions bound run-to-run noise, but not
  hardware-to-hardware variation.

## Layout

```
src/server.c          shared: args, listener, signals, counters, RSS monitor
src/server_epoll.c    edge-triggered event loop, buffering, back-pressure
src/server_thread.c   thread-per-connection baseline
src/loadgen.c         open/closed-loop generator, percentiles, echo verification
src/common.[ch]       framing constants, timing, percentile helpers
bench.sh              sweep harness -> results/raw.csv + summary.md + env.txt
tests/smoke.sh        correctness gate, both modes x 4 payload sizes
```

Built with `-Wall -Wextra -Wpedantic` and there are no dependencies beyond lib pthread.
