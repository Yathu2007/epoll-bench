#!/usr/bin/env bash
# Correctness smoke test: both modes, several payload sizes, checked echoes.
# Fails if the load generator reports any error, timeout, or sequence mismatch.
set -euo pipefail
cd "$(dirname "$0")/.."
make -s

PORT=${PORT:-9599}
fail=0

for mode in epoll thread; do
    for payload in 8 128 4096 65536; do
        ./bin/server --port="$PORT" --mode="$mode" >/dev/null 2>/tmp/epoll-bench-smoke.log &
        pid=$!
        for _ in $(seq 100); do grep -q '^listening' /tmp/epoll-bench-smoke.log && break; sleep 0.05; done

        out=$(./bin/loadgen --port="$PORT" --conns=32 --rate=5000 --duration=2 --warmup=1 \
                            --payload="$payload" --threads=2 2>/dev/null || true)
        kill -INT "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true

        bad=$(sed -n 's/.*errors=\([0-9]*\).*drops=\([0-9]*\).*timeouts=\([0-9]*\).*mismatch=\([0-9]*\).*/\1 \2 \3 \4/p' <<<"$out")
        recvd=$(sed -n 's/.* recvd=\([0-9]*\).*/\1/p' <<<"$out")
        if [ "$bad" = "0 0 0 0" ] && [ "${recvd:-0}" -gt 0 ]; then
            printf 'ok    %-6s payload=%-6s recvd=%s\n' "$mode" "$payload" "$recvd"
        else
            printf 'FAIL  %-6s payload=%-6s (errors drops timeouts mismatch = %s, recvd=%s)\n' \
                "$mode" "$payload" "${bad:-?}" "${recvd:-0}"
            fail=1
        fi
        sleep 0.5
    done
done

exit "$fail"