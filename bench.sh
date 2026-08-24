#!/usr/bin/env bash
# Reproduces the results table in README.md.
#
#   ./bench.sh                       # full sweep
#   CONNS="100 1000" REPS=1 ./bench.sh
#
# Each (mode, conns) pair is measured twice: an open-loop run at a fixed
# offered rate for latency, and a closed-loop run for saturation throughput.
# Every run is repeated REPS times and the per-metric median is reported.
set -euo pipefail

PORT=${PORT:-9000}
CONNS=${CONNS:-"100 1000 10000"}
MODES=${MODES:-"epoll thread"}
RATE=${RATE:-100000}          # offered req/s for the open-loop runs
PAYLOAD=${PAYLOAD:-128}
DURATION=${DURATION:-10}
WARMUP=${WARMUP:-3}
REPS=${REPS:-3}
DEPTH=${DEPTH:-1}             # in-flight requests per conn, closed loop
LG_THREADS=${LG_THREADS:-4}
SERVER_CPUS=${SERVER_CPUS:-0-1}
LOADGEN_CPUS=${LOADGEN_CPUS:-4-11}
STACK_KB=${STACK_KB:-512}
OUTDIR=${OUTDIR:-results}

cd "$(dirname "$0")"
make -s
mkdir -p "$OUTDIR/logs"
RAW="$OUTDIR/raw.csv"

TASKSET_S=(); TASKSET_L=()
if command -v taskset >/dev/null 2>&1; then
    TASKSET_S=(taskset -c "$SERVER_CPUS")
    TASKSET_L=(taskset -c "$LOADGEN_CPUS")
else
    echo "warning: taskset not found; server and load generator share all cores" >&2
fi

# ------------------------------------------------------------------- env
{
    echo "date: $(date -Is)"
    echo "kernel: $(uname -sr)"
    echo "distro: $(. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME")"
    echo "cpu: $(awk -F': ' '/model name/{print $2; exit}' /proc/cpuinfo)"
    echo "cores: $(nproc) logical"
    echo "memory: $(awk '/MemTotal/{printf "%.1f GiB", $2/1048576}' /proc/meminfo)"
    echo "ulimit -n: $(ulimit -n)"
    echo "ulimit -u: $(ulimit -u)"
    echo "somaxconn: $(cat /proc/sys/net/core/somaxconn)"
    echo "cgroup pids.max: $(cat /sys/fs/cgroup"$(awk -F: '{print $3}' /proc/self/cgroup)"/pids.max 2>/dev/null || echo n/a)"
    echo "server cpus: $SERVER_CPUS   loadgen cpus: $LOADGEN_CPUS"
    echo "cc: $(${CC:-cc} --version | head -1)"
} | tee "$OUTDIR/env.txt"
echo

if [ "$(ulimit -n)" -lt 20000 ]; then
    echo "warning: ulimit -n is $(ulimit -n); run 'ulimit -n 100000' before large sweeps" >&2
fi

echo "mode,loop,conns,rep,$(echo '')" >/dev/null
: >"$RAW"
echo "mode,loop,conns,rep,established,rate_achieved,p50_us,p90_us,p99_us,p999_us,max_us,errors,drops,timeouts,mismatch,peak_conns,rss_base_kb,rss_peak_kb,bytes_per_conn,srv_user_s,srv_sys_s,srv_requests" >>"$RAW"

field() { sed -n "s/.*[[:space:]]$2=\([^[:space:]]*\).*/\1/p" <<<"$1"; }

run_one() { # mode loop conns rep
    local mode=$1 loop=$2 conns=$3 rep=$4
    local tag="$mode-$loop-$conns-$rep"
    local slog="$OUTDIR/logs/server-$tag.log"
    local llog="$OUTDIR/logs/loadgen-$tag.log"

    "${TASKSET_S[@]}" ./bin/server --port="$PORT" --mode="$mode" --stack-kb="$STACK_KB" \
        >/dev/null 2>"$slog" &
    local spid=$!
    local ready=0
    for _ in $(seq 100); do grep -q '^listening' "$slog" && { ready=1; break; }; sleep 0.05; done
    if [ "$ready" != 1 ]; then
        echo "error: server failed to start for $tag:" >&2
        sed 's/^/  /' "$slog" >&2
        kill -9 "$spid" 2>/dev/null || true
        exit 1
    fi

    local args=(--host=127.0.0.1 --port="$PORT" --conns="$conns" --payload="$PAYLOAD"
                --duration="$DURATION" --warmup="$WARMUP" --threads="$LG_THREADS")
    if [ "$loop" = closed ]; then args+=(--closed --depth="$DEPTH"); else args+=(--rate="$RATE"); fi

    local result
    result=$("${TASKSET_L[@]}" ./bin/loadgen "${args[@]}" 2>"$llog" || true)

    kill -INT "$spid" 2>/dev/null || true
    for _ in $(seq 100); do grep -q '^server ' "$slog" && break; sleep 0.05; done
    kill -9 "$spid" 2>/dev/null || true
    wait "$spid" 2>/dev/null || true

    local srv
    srv=$(grep '^server ' "$slog" || echo "")
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$mode" "$loop" "$conns" "$rep" \
        "$(field "$result" established)" "$(field "$result" rate_achieved)" \
        "$(field "$result" p50_us)" "$(field "$result" p90_us)" "$(field "$result" p99_us)" \
        "$(field "$result" p999_us)" "$(field "$result" max_us)" "$(field "$result" errors)" \
        "$(field "$result" drops)" "$(field "$result" timeouts)" "$(field "$result" mismatch)" \
        "$(field "$srv" peak_conns)" "$(field "$srv" rss_base_kb)" "$(field "$srv" rss_peak_kb)" \
        "$(field "$srv" bytes_per_conn)" "$(field "$srv" user_cpu_s)" "$(field "$srv" sys_cpu_s)" \
        "$(field "$srv" requests)" >>"$RAW"

    printf '  %-6s %-6s conns=%-6s rep=%s  est=%-6s %8s req/s  p50=%-8s p99=%-9s rss=%s kB\n' \
        "$mode" "$loop" "$conns" "$rep" "$(field "$result" established)" \
        "$(field "$result" rate_achieved)" "$(field "$result" p50_us)" "$(field "$result" p99_us)" \
        "$(field "$srv" rss_peak_kb)"

    grep -h '^thread-limit' "$slog" 2>/dev/null | sed 's/^/  note: /' || true
    sleep 1  # let TIME_WAIT sockets drain before the next run
}

for mode in $MODES; do
    for conns in $CONNS; do
        for loop in open closed; do
            for rep in $(seq "$REPS"); do
                run_one "$mode" "$loop" "$conns" "$rep"
            done
        done
    done
done

# ------------------------------------------------------- median summary
awk -F, -v rate="$RATE" -v payload="$PAYLOAD" -v depth="$DEPTH" '
function median(arr, n,   i, tmp, j, k, t) {
    for (i = 1; i <= n; i++) tmp[i] = arr[i]
    for (i = 2; i <= n; i++) { t = tmp[i]; for (j = i-1; j >= 1 && tmp[j] > t; j--) tmp[j+1] = tmp[j]; tmp[j+1] = t }
    k = int((n+1)/2)
    return (n % 2) ? tmp[k] : (tmp[k] + tmp[k+1]) / 2
}
NR == 1 { next }
{
    key = $1 "," $2 "," $3
    if (!(key in seen)) { seen[key] = 1; order[++nkeys] = key }
    n[key]++
    est[key,n[key]] = $5; rate_a[key,n[key]] = $6; p50[key,n[key]] = $7; p99[key,n[key]] = $9
    p999[key,n[key]] = $10; err[key,n[key]] = $12 + $14; rss[key,n[key]] = $18
    perconn[key,n[key]] = $19; ucpu[key,n[key]] = $20; scpu[key,n[key]] = $21
}
END {
    printf "\n### Latency under fixed offered load (open loop, %d req/s, %d B payload)\n\n", rate, payload
    printf "| mode | conns | established | achieved req/s | p50 (us) | p99 (us) | p99.9 (us) | peak RSS (MB) | mem/conn (KB) | user CPU (s) | sys CPU (s) | errors |\n"
    printf "|---|---|---|---|---|---|---|---|---|---|---|---|\n"
    for (i = 1; i <= nkeys; i++) {
        split(order[i], f, ",")
        if (f[2] != "open") continue
        k = order[i]; m = n[k]
        for (j = 1; j <= m; j++) { a[j]=est[k,j]; b[j]=rate_a[k,j]; c[j]=p50[k,j]; d[j]=p99[k,j]; e[j]=p999[k,j]; g[j]=rss[k,j]; h[j]=perconn[k,j]; u[j]=ucpu[k,j]; s[j]=scpu[k,j]; x[j]=err[k,j] }
        printf "| %s | %s | %d | %.0f | %.1f | %.1f | %.1f | %.1f | %.2f | %.2f | %.2f | %d |\n",
            f[1], f[3], median(a,m), median(b,m), median(c,m), median(d,m), median(e,m),
            median(g,m)/1024, median(h,m)/1024, median(u,m), median(s,m), median(x,m)
    }
    printf "\n### Saturation throughput (closed loop, %d request(s) in flight per connection)\n\n", depth
    printf "| mode | conns | established | req/s | p50 (us) | p99 (us) | peak RSS (MB) | mem/conn (KB) | user CPU (s) | sys CPU (s) |\n"
    printf "|---|---|---|---|---|---|---|---|---|---|\n"
    for (i = 1; i <= nkeys; i++) {
        split(order[i], f, ",")
        if (f[2] != "closed") continue
        k = order[i]; m = n[k]
        for (j = 1; j <= m; j++) { a[j]=est[k,j]; b[j]=rate_a[k,j]; c[j]=p50[k,j]; d[j]=p99[k,j]; g[j]=rss[k,j]; h[j]=perconn[k,j]; u[j]=ucpu[k,j]; s[j]=scpu[k,j] }
        printf "| %s | %s | %d | %.0f | %.1f | %.1f | %.1f | %.2f | %.2f | %.2f |\n",
            f[1], f[3], median(a,m), median(b,m), median(c,m), median(d,m),
            median(g,m)/1024, median(h,m)/1024, median(u,m), median(s,m)
    }
}' "$RAW" | tee "$OUTDIR/summary.md"

echo
echo "raw runs: $RAW    summary: $OUTDIR/summary.md    env: $OUTDIR/env.txt"
