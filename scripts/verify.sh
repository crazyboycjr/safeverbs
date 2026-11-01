#!/usr/bin/env bash
# Verify the formal model and the guarantees (crates/safeverbs-spec) with Verus.
#
# Needs `verus` and `cargo-verus` on PATH (a Verus release) and the Rust toolchain that
# Verus names in `verus --version` (rustup installs it). Exits non-zero if any proof fails.
#
# Environment:
#   VERIFY_MEMORY_MAX  memory cap for the run when systemd-run is available (default 6G;
#                      "none" runs without a cap)
#   VERIFY_JOBS        cargo build jobs (default 4)
#   Extra arguments go to Verus, e.g. `scripts/verify.sh --rlimit 5`.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
crate="$root/crates/safeverbs-spec"

if ! command -v verus >/dev/null || ! command -v cargo-verus >/dev/null; then
    echo "verify.sh: verus and cargo-verus must be on PATH (see docs/verification.md)" >&2
    exit 2
fi

# The repository pins an older toolchain; Verus runs cargo with its own.
toolchain="$(verus --version 2>/dev/null | sed -n 's/^ *Toolchain: \([^ ]*\).*/\1/p')"
if [ -z "$toolchain" ]; then
    echo "verify.sh: cannot read the toolchain from 'verus --version'" >&2
    exit 2
fi
export RUSTUP_TOOLCHAIN="$toolchain"

# Cargo skips crates it considers fresh; always re-verify ours (vstd stays cached).
cargo clean --quiet --manifest-path "$crate/Cargo.toml" -p safeverbs-spec

cmd=(nice -n 19 cargo verus verify --manifest-path "$crate/Cargo.toml" -j "${VERIFY_JOBS:-4}")
if [ "$#" -gt 0 ]; then
    cmd+=(-- "$@")
fi

memory="${VERIFY_MEMORY_MAX:-6G}"
if [ "$memory" != "none" ] && command -v systemd-run >/dev/null; then
    cmd=(systemd-run --user --scope --quiet -p "MemoryMax=$memory" -p MemorySwapMax=0 "${cmd[@]}")
fi

log="$(mktemp)"
trap 'rm -f "$log"' EXIT
status=0
"${cmd[@]}" 2>&1 | tee "$log" || status=$?

# cargo verus prints one summary per verified crate: vstd's, then ours.
summary="$(grep 'verification results::' "$log" | tail -n 1 || true)"
if [ "$status" -ne 0 ] || [ -z "$summary" ] || ! grep -q ' 0 errors' <<<"$summary"; then
    echo "verify.sh: FAILED${summary:+ ($summary)}" >&2
    exit 1
fi
echo "safeverbs-spec: $summary"
