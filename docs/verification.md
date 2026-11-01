# Verification

SafeVerbs' guarantee is a theorem checked by [Verus](https://github.com/verus-lang/verus).
This document tells users what is proved, what is only stated so far, and what is assumed;
it tells contributors how to run the proofs and how code written inside `verus!` is built
with plain `cargo`. The design behind it is in [`design.md`](design.md).

## What is proved, what is stated, what is assumed

### Proved (checked by `scripts/verify.sh`)

The model, the abstract machine and the theorem's composition are in `crates/safeverbs-spec`
and verified with no `assume`, `admit`, `external_body` or assumed specification anywhere in
the crate (the script refuses them). The core's verified parts follow them, under
"The core".

**A, the abstract machine** (`abs.rs`). Its invariants are inductive: they hold in its start
state and every transition preserves them (`lemma_step_keeps_invariant`). The guarantees are
lemmas about every state A can reach (`guarantees.rs`):

| Property | Lemma | Meaning |
| --- | --- | --- |
| S1 | `lemma_s1_no_conflict` | what the program may write, the NIC may neither read nor write; what the program may read, the NIC may not write |
| S1 | `lemma_s1_nic_writes_any_bytes` | the NIC writes only uniquely held memory whose element type accepts every bit pattern |
| F1 | `lemma_f1_outcome_fixed` | an outcome, once produced, never changes |
| F1 | `lemma_f1_take` | taking a request's outcome returns that outcome and exactly the bytes it was posted with |
| F1 | `lemma_f1_taken_once` | a request no longer held never comes back (ids are not reused) |
| R1 | `lemma_r1_kept_while_in_flight` | an in-flight request keeps its lease, unchanged, until its outcome exists |
| R1 | `lemma_r1_requests_on_live_qps` | no request outlives its queue pair |
| R1 | `lemma_r1_released_once` | a released lease never comes back |
| L1 | `lemma_l1_no_waker_left` | a complete request holds no waker |
| L1 | `lemma_l1_completion_wakes` | completing a request invokes the waker it holds |

**M, the NIC model** (`nic.rs`, proofs in `nic_proofs.rs`). Its invariants are inductive
(`lemma_step_keeps_invariant`): each queue is a prefix of executed WRs followed by
unexecuted ones, within its depth; every CQE names an executed, unretired WR with that WR's
`wr_id` and outcome, and a queue's CQEs wait in posting order; the buffers of every posted WR
stay registered. On top of them:

- `lemma_cq_never_overflows`: given the reservation `create_qp` requires, no CQ ever holds
  more entries than its capacity (also checked as a safety condition inside the NIC steps).
- `lemma_poll`: a polled CQE reports its own WR's `wr_id` and outcome, and retires that WR
  and every earlier one of its queue, which were unsignaled sends that succeeded; M records
  every retired WR's outcome.
- `lemma_nic_steps_keep_verbs_enabled`: nothing the NIC does on its own invalidates a verb's
  precondition, so the library may check preconditions against the state it last saw.
- `lemma_nic_touches_registered`: the NIC only touches memory that is registered right now.

**The refinement theorem's composition** (`refinement.rs`). `theorem_1` proves that for any
implementation meeting the obligations of the `Refines` trait, along any execution, A's and
M's invariants hold, every verb met its precondition (S2), and A and M stay coupled.
`corollary_s1` and `corollary_f1` restate S1 and F1 against M: the NIC touches only memory A
gives it, and the outcome a client takes is the one M recorded for that request's own WR.

**Worked examples** (`examples.rs`) are concrete executions checked step by step; they show
that the transitions can happen, so no lemma above is vacuous.

#### The core

The code of `crates/safeverbs-core` written inside `verus!` is verified with no `assume` or
`admit`; it trusts only the items listed in `scripts/trusted.txt` (see "Trusted computing
base" below), and the script refuses any other.

**The completion ring** (`ring.rs`). `Ring::new` establishes the eight invariants of the
module documentation (`Ring::inv`) and every operation requires and keeps them; Verus also
checks that no operation panics (indexing, the `assert!`s left for callers outside the
proofs) or overflows. The only run-time check added for the proofs is one comparison in
`has_free_slot`, which refuses a new request once the WQE numbers could not hold `max_wqes`
more (after 2^64 WQEs). Every operation is specified by what A sees of the ring: its requests
(`Ring::req`: posted and not freed, in flight or complete, with waker, owner and leases), the
requests written but not posted (`Ring::staged`) and the wakers woken (`Ring::woken`, a ghost
log). `take` returns exactly the outcome and the very leases of a complete request; the
leases a request holds are the value `end` moved in, until it retires, is taken, is aborted
or the queue pair is destroyed.

**The ring refines A** (`refine.rs`). In every state of A where the ring's requests sit
(`placed`: each request A sees in the ring is a request lease of A, on the ring's queue
pair, with the same phase, waker and owner; each staged request's leases a program lease of
A; no two the same), each operation's change is zero or more steps of A to a state where they
sit again: none for `begin`, `push_wqe`, `end` and `take_detached_error`; `post_request` (then
`register` or `detach`) for each request `commit` posts; `drop` for each staged lease an
abort releases; A's `complete` for each request `complete` retires, in order, A's wakes
growing by exactly the wakers the ring woke (L1 for the ring); A's `take`, `detach` and
`register` for `take`, `detach` and `set_waker`; and for `release_all` a flush, `detach` and
`drop` that leave nothing of the queue pair in A. The async layer's side list of results
moved out of a clogged ring is covered as well (`with_side`): moving a result there is no
step, taking it is A's `take` with the outcome and leases the ring gave. This is the ring's
share of the obligations of `Refines`.

**The trusted verbs** (`trusted.rs`). The data path's calls into the NIC (`start`, `post`,
`doorbell`, `abort`, `post_recv`, `poll`) have contracts taken from M: each requires M's
precondition and ensures the NIC's own steps followed by the verb's (`doorbell` posts the WRs
written since `start` as one `post_send`; `poll` reports the CQE each `poll_cq` step pops).
The NIC's state is a ghost token (`NicToken`) nothing else can make. These are trusted, not
proved; they are the interface the queue pair is to be verified against.

### Stated, not yet proved

- **Theorem 1 for the real library.** The obligations of `Refines` (start states; every API
  call is zero or more steps of A and of M; the invariant couples A and M) are what
  `safeverbs-core` must prove. The ring discharges its share against A (above); what remains
  is the queue pair as a whole (`qp.rs`, still outside `verus!`): calling the trusted verbs
  with their preconditions (S2), keeping the map from requests to WQEs (`ids` and the WQE
  numbers the ring hands out) that couples A and M, and the buffers (`buf.rs`), whose
  allocation, splitting, freezing and exposure are A's buffer transitions. Until then Theorem
  1 holds for any implementation that discharges the obligations, and for the ring's part of
  SafeVerbs.
- **Theorem 2**, two SafeVerbs endpoints under assume-guarantee (no receiver-not-ready or
  remote access errors; every error completion due to a fault). It is stated in
  `refinement.rs` and `design.md` only.

### Assumed

The trusted base is listed in `design.md`, section 4: rustc and std, Verus and Z3,
rdma-core with the provider and kernel drivers, the NIC behaving as M says, and the bodies
of the trusted verbs wrappers. M is the assumption users should read: every rule the
library relies on about hardware is a transition or precondition there, not prose. Evidence
that real NICs conform will come from differential testing on hardware and soft-RoCE.

### Trusted computing base

Beyond the toolchain and M, the verified core trusts exactly the items of
`scripts/trusted.txt`; `scripts/verify.sh` fails if an `external_body` or external
specification appears anywhere else in `crates/safeverbs-core`, or if a listed item no longer
exists. Each of them, and why it is trusted:

| Item (`crates/safeverbs-core/src/`) | What Verus takes on trust |
| --- | --- |
| `ring.rs` `ExWaker` | `core::task::Waker` is an opaque type |
| `ring.rs` `wake` | `Waker::wake` wakes the task (no contract: the ring logs the waker's name itself) |
| `ring.rs` `will_wake` | `Waker::will_wake`: when it says two wakers wake the same task, they share a name in A (`waker_id`) |
| `ring.rs` `clone_waker` | `Waker::clone` gives a waker of the same task, with the same name |
| `trusted.rs` `ExSge`, `ExRemoteBuf`, `ExWrOp` | the definitions of these plain data types, as written in `nic.rs` |
| `trusted.rs` `ExError` | `Error` is an opaque type |
| `trusted.rs` `ExNicQp`, `ExNicCq` | the NIC traits may bound the wrappers' generics (declared without specifications) |
| `trusted.rs` `start`, `post`, `abort` | `ibv_wr_start`, `ibv_wr_*`, `ibv_wr_abort`: writing WRs changes nothing in M until the doorbell; an abort discards them |
| `trusted.rs` `doorbell` | `ibv_wr_complete` is M's `post_send` of the WRs written since the start (after the NIC's own steps), or nothing on error |
| `trusted.rs` `post_recv` | `ibv_post_recv` of one WR is M's `post_recv`, or nothing on error |
| `trusted.rs` `poll` | each CQE `ibv_poll_cq` returns is the one M's `poll_cq` pops, with its `wr_id` and outcome |

Two modelling choices sit in these contracts: M has no atomics, and the contracts treat a
fetch-and-add or compare-and-swap as an RDMA READ (locally both have the NIC write the local
buffer); and a `Wc`'s status maps to M's six statuses as `Status::model` says.

`Waker::wake` and the verbs' bodies are the only code the proofs call without checking it.
The rest of `safeverbs-core` (`buf.rs`, `qp.rs`, `verbs.rs`, `model.rs`, `pd.rs`) is outside
`verus!`: not trusted by the proofs, but not verified either (see above).

Liveness is outside the claim: that every request eventually completes needs the NIC to
make progress, the executor to poll woken tasks and someone to poll the CQ.

## Running the proofs

Install a Verus release (the binary release from GitHub, or on Arch Linux the AUR package
`verus-bin`) so that `verus` and `cargo-verus` are on `PATH`, and install the Rust toolchain
it names. The core is verified in the main workspace, which needs the rdma-core submodule
and what building `safeverbs-sys` needs (`docs/dev-setup.md`); `VERIFY_SPEC_ONLY=1` checks
only the specification crate, which needs neither.

```console
$ verus --version
Verus
  Version: 0.2026.10.26.d17fbd0
  ...
  Toolchain: 1.99.1-x86_64-unknown-linux-gnu
$ rustup toolchain install 1.99.1
$ scripts/verify.sh
...
safeverbs-spec: verification results:: 115 verified, 0 errors
safeverbs-core: verification results:: 120 verified, 0 errors
```

The script first checks that neither crate assumes anything and that the core trusts only
the listed items, then, for each crate, builds it with plain `cargo` (it must compile with
its ghost code erased, as users build it) and runs `cargo verus verify` on it with the
toolchain Verus names (whatever `rust-toolchain.toml` pins). It always re-verifies the crates
while vstd stays cached, runs under a memory cap when `systemd-run` is available
(`VERIFY_MEMORY_MAX`, default `6G`), and exits non-zero on any failure. The first run also verifies vstd itself (about
1900 items); after that a run takes a few seconds. Extra arguments go to Verus, for example
`scripts/verify.sh --rlimit 2`, which still passes: the proofs are not at the edge of the
solver's resource limit.

For quick iteration, the crate can also be checked without cargo:

```console
$ cd crates/safeverbs-spec && verus --crate-type=lib src/lib.rs
```

## Building `verus!` code with plain cargo

Parts of `safeverbs-core` are written inside `verus!` and must still build with plain `cargo`
for users who never install Verus. We tried this with a crate shaped like the core (exec code
whose contracts use this crate's ghost types, a trusted `external_body` wrapper, a tracked
token, `#![no_std]` and `#![forbid(unsafe_code)]`), verified with `cargo verus verify` and
built and tested with plain `cargo test` on stable Rust. What we found:

- **Dependencies.** A crate that uses `verus!` depends on `vstd` from crates.io. vstd is
  published for every Verus build with a date version; pin the one that belongs to your
  Verus release (it is the version in that release's `source/vstd/Cargo.toml`), here
  `vstd = { version = "=0.0.0-2026-10-25-0158", default-features = false }`
  (`default-features = false` gives a `no_std` vstd; a crate whose verified code uses `Vec`
  adds `features = ["alloc"]`, as the core does). `state_machine!` is not re-exported by
  vstd: a crate that defines state machines also depends on `verus_state_machines_macros` of
  the same version and puts `state_machine!` outside `verus!`.
- **Erasure.** Without Verus, the `verus!` macro erases ghost code: `spec` and `proof`
  functions disappear, `requires` and `ensures` are dropped, and `Ghost<T>` and `Tracked<T>`
  values and ghost fields become zero-sized. So a `use` of a spec or proof function fails to
  compile without Verus; call it by its path inside the proof instead, import it with a glob
  (globs tolerate missing items), or guard the `use` with `#[cfg(verus_only)]`. An external
  type or trait specification is a real item after erasure; the core puts it under
  `#[cfg(verus_keep_ghost)]` so that a plain build has none. A plain build produces the code it would produce
  without Verus. It also checks nothing: a function with a false `ensures` compiles. Proofs
  are checked only by `cargo verus verify` (or `scripts/verify.sh`), which CI must run.
- **Ghost parameters stay in signatures.** A `Tracked<T>` or `Ghost<T>` parameter is still a
  parameter after erasure (of a zero-sized type), so code outside Verus has to pass one
  (`Tracked::assume_new()`). The core's public API should therefore keep its tokens inside
  its structs and take no ghost parameters.
- **Toolchain.** vstd's macro dependencies use edition 2024 and need Cargo 1.85 or newer
  (Rust 1.82, which the repository pinned before, cannot build them). We built with stable
  1.98.1 and with 1.99.1, which Verus uses and the repository now pins.
- **Workspace.** `safeverbs-spec` is its own workspace, so that it verifies on a plain clone:
  the main workspace needs the rdma-core submodule even for
  `cargo metadata`. `safeverbs-core` can be a member of the main workspace and depend on it
  by path.
- **Verification through cargo.** `cargo verus verify` builds with `RUSTC_WRAPPER=verus`
  and verifies every crate whose `Cargo.toml` has `[package.metadata.verus] verify = true`,
  importing what each verified dependency exports. It runs `cargo metadata` with the
  toolchain of the current directory, so in this repository it needs
  `RUSTUP_TOOLCHAIN` set to Verus's toolchain (the script does this). Verus-relevant cargo
  options such as `--manifest-path` and `-p` must come before generic ones such as `-j`.
- **Across crates, state machines need wrappers.** The copy of a crate that its dependents
  compile against is built without `cfg(verus_keep_ghost_body)`, and `state_machine!` emits
  its transition relations, `invariant()` and `*_enabled` predicates only under that cfg. A
  dependent crate therefore cannot name `Nic::State::poll_cq` or `s.invariant()`. This crate
  exports plain spec functions instead, which do cross: `nic_inv`, `nic_init`, `nic_next`,
  `nic_step(pre, post, step)` and `nic_enabled(pre, step)` for M, `abs_inv`, `abs_init`,
  `abs_next` and `abs_step` for A, with lemmas that a step is a step of its machine. A
  negative check (a false assertion about M in the dependent crate) fails as it should, so
  their meaning crosses too.
- **Lints.** Verus sets `cfg(verus_only)` and `cfg(verus_keep_ghost)`; declare them in
  `[lints.rust] unexpected_cfgs` to silence warnings, and allow `non_snake_case` for the
  modules `state_machine!` generates.

The `Cargo.toml` of `safeverbs-core` has, besides its other dependencies:

```toml
[dependencies]
safeverbs-spec = { version = "0.1.0", path = "../safeverbs-spec" }
vstd = { version = "=0.0.0-2026-10-25-0158", default-features = false, features = ["alloc"] }

[package.metadata.verus]
verify = true

[lints.rust]
unexpected_cfgs = { level = "warn", check-cfg = ['cfg(verus_only)', 'cfg(verus_keep_ghost)', 'cfg(verus_keep_ghost_body)'] }
```

## How the core connects to the model

The core's state carries ghost state that the abstraction function reads: a tracked token
whose view is M's state, the map from requests to WRs, and the leases of A. Three pieces
connect it to the specification crate. The first two exist (`trusted.rs`, `refine.rs`); the
third waits for the queue pair to move into `verus!`.

**Trusted verbs wrappers** (`trusted.rs`). Each verb is one small function with an
`external_body` and a contract taken from M. The precondition is checked by Verus at every
call site, which is S2; the postcondition says the verb happened after whatever the NIC did
meanwhile, which `lemma_nic_env_keeps_enabled` shows is consistent. The sketch below was the
plan; the module follows it, with the WRs written since `start` kept in the token and
posted together by the doorbell:

```rust
verus! {
#[verifier::external_body]
fn post_send(qp: &RawQp, wrs: &WrList, Tracked(nic): Tracked<&mut NicToken>)
    requires
        nic_inv(nic.view()),
        nic_enabled(nic.view(), Nic::Step::post_send(qp.id(), wrs.view())),
    ensures
        exists|mid: Nic::State| nic_env_reaches(old(nic).view(), mid)
            && nic_step(mid, final(nic).view(), Nic::Step::post_send(qp.id(), wrs.view())),
{
    // ibv_post_send(qp.raw, wrs.head, &mut bad)
}
}
```

Polling is the same with `Nic::Step::poll_cq`, and returns the CQE that M's `poll_cq` pops.

**Leases.** The ring tracks each request's leases as the value itself: the `L` A sees a
request hold is the one `end` moved into its slot, and `take` or `abort_keeping` hands back
that value (`refine.rs`); the ghost maps from sequence numbers to A's lease ids and
directions belong to the queue pair. The plan for the buffers is linear ghost permissions,
below. A ghost token cannot cross unverified code (a caller outside `verus!` cannot pass a
`Tracked` argument without assuming it), so the permissions come with the queue pair's move
into `verus!`; and since M is one state for the whole device while each `LocalQp` moves
between threads alone, the queue pair will need its own share of M (a tokenized M, or one
token per queue pair over M's per-QP state).

**Memory as linear ghost permissions.** The core holds vstd's `PointsToRaw` permission for
every byte of every buffer. A `BufMut` or `Buf` carries the permissions for its bytes (a
`Buf` shares them read-only). Posting moves the permissions of the WR's bytes into the NIC
token, and the trusted poll wrapper hands back the permissions of the WRs a CQE retires; the
token's contract keeps the permissions it holds in step with what M says the NIC may access
(`nic_may_read`, `nic_may_write`). Ordinary Verus ownership checking then gives S1 for the
implementation, and turning bytes into a `T` goes through `FromBytes` or `TryFromBytes`.

**The `Refines` implementation.** The core implements `refinement::Refines` for its state,
relates each public function's postcondition to `step`, and proves the three obligations;
`theorem_1` does the rest. The ring's lemmas in `refine.rs` (`lemma_ring_*`) are the steps of
A for its part of each call.
