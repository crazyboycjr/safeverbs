#!/usr/bin/env bash
# What one round of each public path costs, against code written by hand: instructions per
# round on the simulated NIC, with the NIC subtracted (all it runs, its allocations and copies
# too), and the ratio to the baseline each workload is compared with
# (crates/safeverbs/examples/measure.rs says what each one does).
#
#   scripts/measure.sh                the table
#   scripts/measure.sh -v eventloop   one workload's cost, function by function
#   scripts/measure.sh -s             the bytes of a slot and of the handles
#
# Needs valgrind (callgrind) and python3. Each workload runs twice, with no rounds
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

# What the workload costs per round outside the simulated NIC, function by function:
# `<per round> <name>`. The NIC is everything its own functions run, whatever that is called:
# what its `Vec`s ask of the allocator and its copies are the NIC's, on both sides of a comparison,
# and what the library or a baseline asks of the same allocator stays theirs. So a callgrind output
# is read for its call arcs: a function called from both has its instructions split by what each
# call took.
per_round() {
    local name="$1"
    for n in 0 "$rounds"; do
        if ! valgrind --tool=callgrind --callgrind-out-file="$scratch/$name.$n.out" \
            "$probe" "$name" "$n" >/dev/null 2>"$scratch/$name.$n.err"; then
            cat "$scratch/$name.$n.err" >&2
            echo "measure.sh: $name failed" >&2
            exit 1
        fi
    done
    python3 - "$scratch/$name.0.out" "$scratch/$name.$rounds.out" "$rounds" <<'PY'
import collections
import re
import sys

NIC = re.compile(r"^<?safeverbs_core::model::")


def outside_the_nic(path):
    """Each function's own instructions that did not run on behalf of the NIC."""
    names = {}

    def name_of(text):
        m = re.match(r"\((\d+)\)(?: (.*))?$", text)
        if m.group(2) is not None:
            names[m.group(1)] = m.group(2)
        return names[m.group(1)]

    own = collections.Counter()
    calls_into = collections.defaultdict(collections.Counter)
    caller = callee = None
    arc = False
    with open(path) as f:
        for line in f:
            if line.startswith("fn="):
                caller = name_of(line[3:].strip())
            elif line.startswith("cfn="):
                callee = name_of(line[4:].strip())
            elif line.startswith("calls="):
                arc = True
            elif line[0] in "0123456789+-*":
                cost = int(line.split()[1])
                if arc:
                    calls_into[callee][caller] += cost
                    arc = False
                else:
                    own[caller] += cost
    share = {f: 1.0 if NIC.search(f) else 0.0 for f in own}
    for _ in range(64):
        changed = False
        for f, callers in calls_into.items():
            if NIC.search(f):
                continue
            total = sum(callers.values())
            s = sum(c * share.get(g, 0.0) for g, c in callers.items()) / total if total else 0.0
            if abs(s - share.get(f, 0.0)) > 1e-9:
                share[f] = s
                changed = True
        if not changed:
            break
    return {f: c * (1.0 - share.get(f, 0.0)) for f, c in own.items()}


before = outside_the_nic(sys.argv[1])
after = outside_the_nic(sys.argv[2])
rounds = int(sys.argv[3])
for f in set(before) | set(after):
    print(f"{(after.get(f, 0.0) - before.get(f, 0.0)) / rounds:.3f} {f}")
PY
}

# The instructions per round the workload's own code takes.
library_cost() {
    per_round "$1" | awk '{ total += $1 } END { printf "%.0f\n", total }'
}

if [ "${1:-}" = "-s" ]; then
    "$probe" sizes
    exit 0
fi

if [ "${1:-}" = "-v" ]; then
    per_round "$2" | awk '{ c = $1; $1 = ""; sub(/^ /, ""); if (c >= 0.5 || c <= -0.5) printf "%9.1f  %s\n", c, substr($0, 1, 150) }' |
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
batch_owned batch_careful 1
async_batch batch_careful 1
sink batch_careful 1
batch_many_careful - 0
batch_many batch_many_careful 1
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
