# Working on SafeVerbs

Rules for anyone changing this repository, agents included.

- **Keep the public documents honest about the verified boundary.** README.md,
  `docs/design.md` and `docs/verification.md` say what is proved, what is only stated
  and what is assumed. A verified component, such as the completion ring or the
  worker's hardware side, does not make Theorem 1 hold for the whole library: say
  which part a claim covers.
- **Never strengthen M, the NIC model, only to make a proof pass.** M is what users
  trust. Change the implementation or the proof instead. No `assume` or `admit`
  either: the core trusts only the items in `scripts/trusted.txt`, which
  `scripts/verify.sh` checks.
- Work in an isolated git worktree. Keep history linear (rebase, never merge) and
  commits unsigned.
- Write code, comments and documents in English.
- Cap build jobs and memory: Verus and the rdma-core build are heavy
  (`CARGO_BUILD_JOBS`, and `VERIFY_JOBS` and `VERIFY_MEMORY_MAX` for
  `scripts/verify.sh`).
- Run `cargo fmt` after changing code, in the main workspace and in
  `crates/safeverbs-spec`, and keep `cargo clippy --workspace --all-targets` clean.
  Before committing, run `scripts/verify.sh` and `cargo test --workspace`.
