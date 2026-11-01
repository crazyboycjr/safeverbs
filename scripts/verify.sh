#!/usr/bin/env bash
# Verify the formal model and the guarantees (crates/safeverbs-spec), then the verified parts
# of the core (the code of crates/safeverbs-core written inside `verus!`), with Verus.
#
# Needs `verus` and `cargo-verus` on PATH (a Verus release) and the Rust toolchain that
# Verus names in `verus --version` (rustup installs it). The core is checked in the main
# workspace, which needs the rdma-core submodule and what building safeverbs-sys needs
# (docs/dev-setup.md). Exits non-zero if any proof fails.
#
# Environment:
#   VERIFY_MEMORY_MAX  memory cap for the run when systemd-run is available (default 6G;
#                      "none" runs without a cap)
#   VERIFY_JOBS        cargo build jobs (default 4)
#   VERIFY_SPEC_ONLY   set to 1 to check only the specification crate (no submodule needed)
#   Extra arguments go to Verus, e.g. `scripts/verify.sh --rlimit 5`.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
spec="$root/crates/safeverbs-spec"
core="$root/crates/safeverbs-core"
allowlist="$root/scripts/trusted.txt"

if ! command -v verus >/dev/null || ! command -v cargo-verus >/dev/null; then
    echo "verify.sh: verus and cargo-verus must be on PATH (see docs/verification.md)" >&2
    exit 2
fi

# The specification proves everything it states: no escape hatches.
if grep -rnE '\b(assume|admit)\(|verifier::external|external_body|assume_specification' \
    "$spec/src" --include='*.rs' | grep -vE '^[^:]+:[0-9]+:\s*//'; then
    echo "verify.sh: the lines above skip verification; crates/safeverbs-spec must not" >&2
    exit 1
fi

# The core assumes nothing either, and trusts only the items listed in scripts/trusted.txt
# (each the line `<file> <item>`): every `external_body`, `external` or external
# specification must sit on one of them, and every listed item must still exist.
if grep -rnE '\b(assume|admit)\(' "$core/src" --include='*.rs' | grep -vE '^[^:]+:[0-9]+:\s*//'; then
    echo "verify.sh: the lines above assume what they should prove" >&2
    exit 1
fi
trusted="$(
    cd "$root"
    awk '
        /^[[:space:]]*\/\// { next }
        /verifier::(external|external_body|external_fn_specification|external_type_specification|external_trait_specification)\]|assume_specification/ {
            pending = 1
        }
        pending && match($0, /(fn|struct|enum|trait|type)[[:space:]]+[A-Za-z_][A-Za-z0-9_]*/) {
            item = substr($0, RSTART, RLENGTH)
            sub(/^(fn|struct|enum|trait|type)[[:space:]]+/, "", item)
            print FILENAME " " item
            pending = 0
        }
    ' $(cd "$root" && find crates/safeverbs-core/src -name '*.rs' | sort) | sort -u
)"
listed="$(grep -vE '^\s*(#|$)' "$allowlist" | awk '{ print $1 " " $2 }' | sort -u)"
unlisted="$(comm -23 <(printf '%s\n' "$trusted") <(printf '%s\n' "$listed"))"
stale="$(comm -13 <(printf '%s\n' "$trusted") <(printf '%s\n' "$listed"))"
if [ -n "$unlisted" ]; then
    printf 'verify.sh: trusted items missing from scripts/trusted.txt:\n%s\n' "$unlisted" >&2
    exit 1
fi
if [ -n "$stale" ]; then
    printf 'verify.sh: scripts/trusted.txt lists items that are no longer trusted:\n%s\n' "$stale" >&2
    exit 1
fi

# Verus runs cargo with its own toolchain, whatever rust-toolchain.toml pins.
toolchain="$(verus --version 2>/dev/null | sed -n 's/^ *Toolchain: \([^ ]*\).*/\1/p')"
if [ -z "$toolchain" ]; then
    echo "verify.sh: cannot read the toolchain from 'verus --version'" >&2
    exit 2
fi
export RUSTUP_TOOLCHAIN="$toolchain"

# Run a command at low priority, under a memory cap when systemd-run is available.
memory="${VERIFY_MEMORY_MAX:-6G}"
run() {
    if [ "$memory" != "none" ] && command -v systemd-run >/dev/null; then
        systemd-run --user --scope --quiet -p "MemoryMax=$memory" -p MemorySwapMax=0 nice -n 19 "$@"
    else
        nice -n 19 "$@"
    fi
}

log="$(mktemp)"
trap 'rm -f "$log"' EXIT
jobs="${VERIFY_JOBS:-4}"
summaries=()

# verify <name> <manifest>: build the crate without Verus (users do; its ghost code must
# erase), then verify it. Cargo skips crates it considers fresh, so the crate is always
# re-verified (vstd stays cached).
verify() {
    local name="$1" manifest="$2"
    cargo clean --quiet --manifest-path "$manifest" -p "$name"
    if ! run cargo build --quiet --manifest-path "$manifest" -p "$name" -j "$jobs"; then
        echo "verify.sh: FAILED ($name does not build without Verus)" >&2
        exit 1
    fi
    local cmd=(run cargo verus verify --manifest-path "$manifest" -p "$name" -j "$jobs")
    if [ "$#" -gt 2 ]; then
        cmd+=(-- "${@:3}")
    fi
    local status=0
    "${cmd[@]}" 2>&1 | tee "$log" || status=$?
    # cargo verus prints one summary per verified crate: vstd's, then ours.
    local summary
    summary="$(grep 'verification results::' "$log" | tail -n 1 || true)"
    if [ "$status" -ne 0 ] || [ -z "$summary" ] || ! grep -q ' 0 errors' <<<"$summary"; then
        echo "verify.sh: FAILED${summary:+ ($name: $summary)}" >&2
        exit 1
    fi
    summaries+=("$name: $summary")
}

verify safeverbs-spec "$spec/Cargo.toml" "$@"
if [ "${VERIFY_SPEC_ONLY:-0}" != 1 ]; then
    verify safeverbs-core "$root/Cargo.toml" "$@"
fi
printf '%s\n' "${summaries[@]}"
