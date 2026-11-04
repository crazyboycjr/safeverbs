#!/usr/bin/env bash
# What one round of each public path costs, against code written by hand: instructions per
# round on the simulated NIC, with the NIC subtracted, and the ratio to the baseline each
# workload is compared with (crates/safeverbs/examples/measure.rs says what each one does).
#
#   scripts/measure.sh                the table
#   scripts/measure.sh -v eventloop   one workload's cost, function by function
#   scripts/measure.sh -s             the bytes of a slot and of the handles
#
# Needs valgrind (callgrind and callgrind_annotate). Each workload runs twice, with no rounds
# and with many, and the difference is what the rounds cost; so setup and teardown do not count.
# Instruction counts say nothing of atomics, caches or MMIO: the numbers that finally count
# come from a real NIC against perftest.
#
# Environment:
#   MEASURE_ROUNDS   rounds of a run (default 2000)
#   CARGO_TARGET_DIR where cargo builds (default: target)
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rounds="${MEASURE_ROUNDS:-2000}"
target="${CARGO_TARGET_DIR:-$root/target}"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

cargo build --release --quiet --manifest-path "$root/Cargo.toml" -p safeverbs --example measure
probe="$target/release/examples/measure"

# Every function's own instructions in a callgrind output: `<count> <name>`.
self_costs() {
    callgrind_annotate --threshold=100 --auto=no "$1" | awk '
        /PROGRAM TOTALS/ { next }
        match($0, /^ *[0-9,]+ \( *[0-9.]+%\) +/) {
            count = substr($0, 1, RLENGTH)
            name = substr($0, RLENGTH + 1)
            sub(/ *\(.*$/, "", count)
            gsub(/[ ,]/, "", count)
            sub(/^(\?\?\?|\/[^:]*|[^ :\/]*\.(rs|c|h|S|s)):/, "", name)
            sub(/ \[[^]]*\]$/, "", name)
            print count, name
        }'
}

# What the workload costs per round, function by function: `<per round> <name>`.
per_round() {
    local name="$1"
    for n in 0 "$rounds"; do
        if ! valgrind --tool=callgrind --callgrind-out-file="$scratch/$name.$n.out" \
            "$probe" "$name" "$n" >/dev/null 2>"$scratch/$name.$n.err"; then
            cat "$scratch/$name.$n.err" >&2
            echo "measure.sh: $name failed" >&2
            exit 1
        fi
        self_costs "$scratch/$name.$n.out" >"$scratch/$name.$n.costs"
    done
    awk -v rounds="$rounds" '
        FNR == NR { c = $1; $1 = ""; base[$0] += c; next }
        { c = $1; $1 = ""; cost[$0] += c }
        END {
            for (k in cost) { printf "%.1f%s\n", (cost[k] - base[k]) / rounds, k }
            for (k in base) { if (!(k in cost)) printf "%.1f%s\n", -base[k] / rounds, k }
        }' "$scratch/$name.0.costs" "$scratch/$name.$rounds.costs"
}

# The simulated NIC: its types, the collections and allocator it uses, memset.
is_nic='^ *(<?safeverbs_core::model::|<alloc::collections::(btree|vec_deque)|<alloc::raw_vec::RawVec|alloc::raw_vec::|alloc::alloc::|.*finish_grow|malloc$|free$|_int_malloc$|_int_free$|__rustc::__rdl_alloc|__rustc::__rust_alloc|__rustc::__rust_dealloc|__rustc::__rdl_dealloc|__rustc::__rust_no_alloc_shim_is_unstable_v2$|__memset)'

# The instructions per round the workload's own code takes.
library_cost() {
    per_round "$1" | awk -v nic="$is_nic" '
        { c = $1; $1 = ""; sub(/^ /, ""); if ($0 !~ nic) total += c }
        END { printf "%.0f\n", total }'
}

if [ "${1:-}" = "-s" ]; then
    "$probe" sizes
    exit 0
fi

if [ "${1:-}" = "-v" ]; then
    per_round "$2" | awk -v nic="$is_nic" '
        { c = $1; $1 = ""; sub(/^ /, ""); if ($0 !~ nic && (c >= 0.5 || c <= -0.5)) printf "%9.1f  %s\n", c, substr($0, 1, 150) }' |
        sort -n
    exit 0
fi

# Each workload, its baseline and how many of the baseline's rounds one round is.
workloads='raw - 0
careful - 0
eventloop careful 1
poll_one careful 1
engine careful 1
take careful 1
async careful 1
batch_careful - 0
batch batch_careful 1
async_batch batch_careful 1
sink batch_careful 1
train_careful - 0
train train_careful 1
async_train careful 16'

declare -A cost
printf '%-16s %12s   %s\n' workload instructions 'ratio to its baseline'
while read -r name baseline times; do
    cost[$name]="$(library_cost "$name")"
    if [ "$baseline" = - ]; then
        printf '%-16s %12s\n' "$name" "${cost[$name]}"
    else
        awk -v name="$name" -v c="${cost[$name]}" -v b="${cost[$baseline]}" -v base="$baseline" -v t="$times" '
            BEGIN { printf "%-16s %12d   %.2f x %s%s\n", name, c, c / (b * t), base, t == 1 ? "" : " x " t }'
    fi
done <<<"$workloads"
