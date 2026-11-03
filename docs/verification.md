# Verification

SafeVerbs is verified with [Verus](https://github.com/verus-lang/verus). This page says
what is proved, what is only tested, what is assumed and what remains, then how to run
the proofs. [`design.md`](design.md) explains the design behind them.

## Where things stand

The guarantee is one refinement theorem: under a model **M** of the NIC
(`crates/safeverbs-spec/src/nic.rs`), the library behaves like a small abstract machine
**A** (`abs.rs`), and the properties S1, F1, R1 and L1 are lemmas about A. The theorem
(`theorem_1` in `refinement.rs`) is proved for every implementation that discharges the
obligations of the `Refines` trait. **The library does not discharge them yet.** What is
proved of it are components: the completion ring refines A, and the worker's hardware
side keeps the NIC within M and its rings coupled to M's queues. The step that would tie
them together, mapping the worker's requests onto A's groups, is the main open item
(see "Remaining").

`scripts/verify.sh` checks everything below that is called proved. The specification
crate uses no `assume`, `admit`, `external_body` or assumed specification; the code of
`crates/safeverbs-core` written inside `verus!` uses no `assume` or `admit` and trusts
only the items of `scripts/trusted.txt` (see "Trusted computing base"). The rest of the
core (`buf.rs`, `worker.rs`, `pd.rs`, `verbs.rs`, `model.rs`) and the `safeverbs` crate
are ordinary Rust: not trusted by the proofs, and not verified either.

## The properties

For each property: what the specification proves (`crates/safeverbs-spec`), what the
ring proves (`ring.rs`, with the steps of A in `refine.rs`) and what the worker proves
(`hw.rs` and its submodules, `buf/`).

**S1, no undefined behaviour.** The program and the NIC never touch a byte in
conflicting ways.
- *Spec.* In every state A reaches, nothing the program may write is read or written by
  the NIC or the peer, and nothing the program may read is written by them
  (`lemma_s1_no_conflict`); whatever the NIC may write is held uniquely by a type that
  accepts any bytes (`lemma_s1_nic_writes_any_bytes`). `corollary_s1` restates this
  against M for a coupled state.
- *Ring.* A request's leases stay in its slot, unreachable, from `end` until A says the
  NIC is done with them (completion, or never posted), and leave it exactly once.
- *Worker.* Every byte a WQE in M may access locally lies in a buffer its live request
  holds, with the access its operation needs.
  Buffers carry the allocator's linear byte permissions through splitting, freezing,
  slicing and thawing (`buf/memory.rs`, `buf/view.rs`), each SGE is covered by the
  permission of the buffer its request retains (`buf/lease.rs`), the inputs of one request
  are disjoint where one is unique, and a buffer posted as a receive is disjoint from
  every buffer the worker holds.
- *Not proved:* that the worker's requests are groups of A, so the coupling of A and M for
  the whole library. S1 of the library still rests on Rust's ownership of `BufMut`, `Buf`
  and `Part` together with these component proofs.

**S2, no local misuse.** Every verb the library calls meets M's precondition.
- *Spec.* The preconditions are M's `require`s. The NIC's own steps never invalidate one
  (`lemma_nic_steps_keep_verbs_enabled`), and neither do other workers' verbs on their own
  queues (`share.rs`: `lemma_rely_keeps_enabled`, `lemma_mine_is_foreign`).
- *Worker.* Proved: every call into the NIC goes through a trusted wrapper whose
  precondition is M's, and Verus checks each call site: queue pair creation within the
  CQ's reservation, connection from `Reset`, doorbells on a queue pair in `Rts` or `Err`
  with room in its queue and every WR one M accepts (scatter/gather count, operation,
  length, registered regions), receives, polls, and destruction of a queue pair only with
  empty queues and of a CQ only with no queue pair. Between two calls the worker's view
  stays valid whatever the NIC and the other workers do (`lemma_rely`).
- *Not proved:* the control path that allocates protection domains and registers and
  deregisters regions (`buf.rs`, `pd.rs`). The worker's rely assumes it deregisters no
  region a written WR names, which leases ensure.

**F1, outcome correspondence.** Each request gets at most one outcome, the one M produced
for its own WQEs, with its own buffers.
- *Spec.* An outcome never changes once produced, taking it returns it with exactly the
  members the request was posted with, and a request taken never comes back
  (`lemma_f1_outcome_fixed`, `lemma_f1_take`, `lemma_f1_taken_once`). Against M, a request
  owns a nonempty interval of WQEs on one queue, and no other request owns any of them;
  its outcome exists only once all of them retired, and is the first error in posting
  order or the last WQE's success (`corollary_f1`, `lemma_aggregate`); a request whose
  prefix retired is still in flight (`corollary_inflight`). A successful SEND or WRITE has
  one outcome, reported or implied (`lemma_ack_outcome_unique`).
- *Ring.* `take` returns exactly the outcome and the leases of a complete request, or of
  one whose doorbell was refused with a flush error. Sequence numbers are never reused.
- *Worker.* Each completion reaches the queue pair, queue and request of its own WQE
  (`wr_id.rs` and the coupling of rings to M's queues). A receive request's result is the
  outcome M retired for its WQE (`hw/outcomes.rs`). A send request's implied success is
  the success M retired for its last WQE, the length of a READ included
  (`hw/send_outcomes.rs`).
- *Not proved:* the first error of a send batch, which the worker keeps natively, against
  M's aggregate.

**R1, no leak by the library.** Every buffer handed to the library is returned or released
once, never before the NIC is done with it.
- *Spec.* A request in flight keeps its members unchanged until its outcome exists
  (`lemma_r1_kept_while_in_flight`); every request lives on an existing queue pair
  (`lemma_r1_requests_on_live_qps`); a released group handle or lease never comes back
  (`lemma_r1_released_once`, `lemma_r1_member_released_once`), and each lease belongs to
  one group (`lemma_member_owned_once`).
- *Ring.* Leases stay in their slot until the request completes, is taken, refused,
  detached or released with its queue pair, and leave it once.
- *Worker.* A queue pair is destroyed only when neither ring has a WQE in flight, which M
  turns into empty queues: its buffers are released after the NIC retired their WQEs.
- *Conditional.* Release is eventual only if the device flushes. When it does not, the
  worker keeps the queue pair and its buffers and reports `Error::NotFlushed`; this
  retention is tested, not proved.

**L1, no lost wakeup.** Once a request is complete, any waker its future registered has
been invoked.
- *Spec.* A complete request holds no waker, and completing one invokes the waker it holds
  (`lemma_l1_no_waker_left`, `lemma_l1_completion_wakes`).
- *Ring.* `complete` wakes exactly the wakers of the requests it retires.
- *Worker.* A waker that panics is caught; the worker finishes the poll's bookkeeping, then
  resumes the panic. Verus checks the normal path; the panicking one is tested.

Liveness (that a request eventually completes) is outside the claim: it needs the NIC to
make progress, someone to poll the CQ and the executor to poll woken tasks.

## Tested, not proved

- Failed memory-region revocation: a refused close keeps its handle,
  `Exposed::try_reclaim` returns the still exposed owner for a retry, and a failed final
  cleanup keeps the registration, the memory, the protection domain and the device
  (`buf.rs` unit tests).
- Wake panics: a caught panic keeps every polled completion and its leases, the poll's
  bookkeeping finishes before it resumes, across queue pairs on one CQ and across refused
  doorbells (`crates/safeverbs-core/tests/waker_unwind.rs`). Unwinding through other
  callbacks and destructors is not covered.
- Memory: a refused registration frees its memory; split, frozen, exposed and streamed
  buffers free theirs once (`crates/safeverbs/tests/allocation.rs`).
- Batches: an error stays within its own request's interval (`tests/outcome.rs`), and a
  batch of READs reports the last READ's length (`tests/read_batch.rs`). A WRITE with
  immediate puts no data in the receive's buffers (`tests/wc.rs`, on the model and rxe).
- `RingBuf`'s placement and the reuse of its chunks.
- The simulated NIC (`model.rs`) checks some of M's rules at run time: no queue pair
  destroyed with WQEs not retired, no CQ destroyed with a queue pair attached, no work
  posted in `Reset`. That it behaves within M is neither tested systematically nor proved.
- The asynchronous API of the `safeverbs` crate, on the model and on soft-RoCE, and random
  operation sequences with injected errors and flushes on the ring, the worker and the
  public API (`docs/dev-setup.md` lists the tests).

## What is assumed

The trusted computing base is rustc and the standard library, Verus and Z3, rdma-core
with its provider and kernel drivers, the items of `scripts/trusted.txt`, and above all
that the NIC behaves as M says. M is the assumption to read: every rule the library
relies on about hardware is a transition or a precondition there, not prose.
`docs/design.md` (section 4) lists the in-order completion rules. Four premises about
completions and registration deserve a note:

- a successful RDMA READ reports exactly the length it asked for;
- a successful SEND or WRITE reports its status only: no length, no immediate;
- a successful receive is either a payload within its own SGEs or the notification of a
  peer's WRITE with immediate, whose length describes the remote write;
- `reg_mr` asks for local write whenever it asks for remote write (a precondition of M
  that the library's unverified control path meets).

Errors report their status only: their opcode, length and immediate are undefined. Evidence
that real NICs conform to M will come from differential testing on hardware and soft-RoCE.

Where unverified code calls a verified function, that function's precondition is assumed,
not proved:

- `BufMut::freeze` relies on its buffer fitting `T` (`Buf::from_frozen`);
- `BufMut::into_part` on at most `u32::MAX` bytes (`Part::unique`; also checked at run
  time), and `BufMut::split_at` on an index in bounds (also checked);
- `RingBuf` builds its chunks with byte permissions it does not prove
  (`OwnedRange::ring_chunk`) and relies on their base when placing them;
- the failure path of `Region::new` hands `release_failed_allocation` what `allocate`
  returned;
- every caller of `Leases::len` relies on the count fitting `usize`.

### Trusted computing base

Beyond the toolchain and M, the verified core trusts exactly the items of
`scripts/trusted.txt`. `scripts/verify.sh` fails if an `external_body` or external
specification appears anywhere else in `crates/safeverbs-core`, or if a listed item no
longer exists.

| Item (`crates/safeverbs-core/src/`) | What Verus takes on trust |
| --- | --- |
| `ring.rs` `ExWaker` | `core::task::Waker` is an opaque type |
| `ring/wake.rs` `ExWakePanic` | std's boxed panic payload is opaque |
| `ring/wake.rs` `try_wake` | `catch_unwind` catches an unwind from `Waker::wake` and does not unwind itself |
| `ring/wake.rs` `resume` | `resume_unwind` never returns |
| `ring.rs` `will_wake` | when `Waker::will_wake` says two wakers wake the same task, they have one name in A |
| `ring.rs` `clone_waker` | `Waker::clone` gives a waker of the same task, with the same name |
| `trusted.rs` `ExSge`, `ExRemoteBuf`, `ExWrOp`, `ExQpCaps`, `ExDeviceCaps`, `ExQpType`, `ExError`, `ExBufError` | the definitions of these plain data types, as written |
| `trusted.rs` `ExConnectAttr`, `ExEndpoint`, `ExPd` | these types are opaque (only the device or `pd.rs` reads them) |
| `buf/memory.rs` `ExRegion` | a registered region is opaque: registration and its final cleanup are not verified |
| `buf/memory.rs` `allocate_zeroed_raw` | the fallible zeroed allocator returns the address, layout, provenance and linear permissions vstd expects, or nothing; no claim about initialized contents |
| `buf/memory.rs` `arc_try_unwrap` | `Arc::try_unwrap` returns the exact contents, or an `Arc` to the same contents |
| `buf/lease.rs` `same_frozen_arc` | `Arc::ptr_eq` means both hold the same frozen allocation |
| `trusted.rs` `ExNic`, `ExNicQp`, `ExNicCq` | the NIC traits may bound the wrappers' generics (declared without specifications) |
| `trusted.rs` `device_caps` | the device's limits as the library reads them are M's |
| `trusted.rs` `create_cq` | `ibv_create_cq_ex` is M's `create_cq` of a fresh name, in a state where M's invariants hold; the new worker's share is that CQ and queue pair names nobody uses (this is where tokens come from) |
| `trusted.rs` `alloc_domain` | thread domains change which locks the driver takes, not what the NIC does (no contract) |
| `trusted.rs` `create_qp`, `connect`, `to_error`, `destroy_qp`, `destroy_cq` | these are M's `create_qp`, three `modify_qp` steps, `modify_qp` to `Err`, `destroy_qp` and `destroy_cq`, or nothing on error, after rely steps |
| `trusted.rs` `qp_num_of`, `qp_lock_free` | facts that mean nothing in M (no contract) |
| `trusted.rs` `no_qp` | a key that names no queue pair panics and never returns |
| `trusted.rs` `pd_id_of` | the protection domain's run-time identity is its identity in the model |
| `trusted.rs` `registered`, `registered_all` | a part's region is registered in its domain (writable if the part is unique) and stays so while the part lives |
| `trusted.rs` `start`, `post`, `abort` | `ibv_wr_start`, `ibv_wr_*`, `ibv_wr_abort`: writing WRs changes nothing in M until the doorbell, and an abort discards them |
| `trusted.rs` `doorbell` | `ibv_wr_complete` is M's `post_send` of the WRs written since the start, after rely steps, or nothing on error |
| `trusted.rs` `post_recv` | `ibv_post_recv` of one WR is M's `post_recv`, or nothing on error |
| `trusted.rs` `poll` | each CQE `ibv_poll_cq` returns is the one M's `poll_cq` pops, with its `wr_id` and outcome |

Each wrapper requires M's precondition on the worker's token and ensures that M moved by
rely steps (the NIC's own, and other workers' on their own queues) and then by the verb.
The rely pins the regions the worker's written WRs name, whose leases the worker holds.
Some modelling choices sit in these contracts: M has no atomics, and the contracts treat a
fetch-and-add or compare-and-swap like an RDMA READ (the NIC writes the local buffer); a
`Wc`'s status maps onto M's six as `Status::model` says; M has one scatter/gather limit per
queue pair, the smaller of its send and receive limits; and the library posts no inline
data, so M sees a queue pair's inline limit as zero.

## Remaining

- **The worker's `impl Refines`.** Map each ring request onto a group of A, with fresh
  names, each member's direction and the check that groups stay compatible, and couple
  them to M (`coupled`): the WQE interval of every request, and the first error of a send
  batch against M's aggregate.
- **Buffers.** The typed wrappers of `buf.rs` are outside `verus!`; initialized contents
  and the zerocopy contracts, `RingBuf`, exposure and revocation and the final cleanup of a
  region are not verified, and the registration lifetime is trusted (`registered`,
  `registered_all`). The preconditions listed above, which unverified callers rely on,
  should become proved.
- **Several workers.** Each worker's view of M is proved under rely, and each worker's
  verbs are foreign steps for the others, but one device history for all workers is not
  built: `create_cq` makes each token rather than splitting one. Worker creation and
  destruction, and moving buffers between workers, are part of this. This will be
  designed with the tokens.
- **The public API.** Connect the futures, sinks and streams of `safeverbs` to the
  verified worker, and state exactly which public operations the theorem covers, including
  reuse after a caught panic. Keep conditional release distinct from leak freedom.
- **Theorem 2**, for two endpoints that both run SafeVerbs, is stated in `refinement.rs`
  and `design.md` only.
- **M.** Gather evidence that real NICs conform (differential tests on hardware and
  soft-RoCE), and give out-of-order transports and shared receive queues their own rules.
- **Performance.** Compare the CPU paths with native ibverbs on real NICs; soft-RoCE is
  functional evidence only.

## Running the proofs

Install a Verus release (the binary release from GitHub, or the AUR package `verus-bin`
on Arch Linux) so that `verus` and `cargo-verus` are on `PATH`, and the Rust toolchain it
names. The core is verified in the main workspace, which needs the rdma-core submodule and
what building `safeverbs-sys` needs (`docs/dev-setup.md`); `VERIFY_SPEC_ONLY=1` checks
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
safeverbs-spec: verification results:: 154 verified, 0 errors
safeverbs-core: verification results:: 389 verified, 0 errors
```

The script first checks that neither crate assumes anything and that the core trusts only
the listed items. Then, for each crate, it builds it with plain `cargo` (its ghost code
must erase, as users build it) and runs `cargo verus verify` with the toolchain Verus
names. It re-verifies the crates every time while vstd stays cached, runs under a memory
cap when `systemd-run` is available (`VERIFY_MEMORY_MAX`, default `6G`; `VERIFY_JOBS` sets
the build jobs) and exits non-zero on any failure. The first run also verifies vstd
itself; after that a run takes a few minutes. Extra arguments go to Verus, for example
`scripts/verify.sh --rlimit 2`. For quick iteration on the specification alone:
`cd crates/safeverbs-spec && verus --crate-type=lib src/lib.rs`.

## Building `verus!` code with plain cargo

Parts of `safeverbs-core` are written inside `verus!` and still build with plain `cargo`
for users who never install Verus.

- **Dependencies.** A crate that uses `verus!` depends on the `vstd` of its Verus release,
  pinned (`vstd = { version = "=0.0.0-2026-10-25-0158", default-features = false }`, with
  `features = ["alloc"]` for verified code that uses `Vec`). `state_machine!` comes from
  `verus_state_machines_macros` of the same version, outside `verus!`.
- **Erasure.** Without Verus, `verus!` drops `spec` and `proof` functions, `requires` and
  `ensures`, and `Ghost<T>` and `Tracked<T>` values become zero-sized. So a `use` of a spec
  or proof function fails to compile without Verus: call it by its path inside the proof,
  or import it with a glob. Verus sets `cfg(verus_keep_ghost)` when it compiles a crate;
  the core puts external type specifications under it, and the exec code that exists only
  to give the proofs their facts (the traversals that prove buffers disjoint), so a plain
  build has none of it. A plain build checks nothing: proofs are checked only by
  `scripts/verify.sh`.
- **Ghost parameters stay in signatures**, as zero-sized values, so code outside Verus has
  to pass one (`Tracked::assume_new()`). The public API therefore keeps its tokens inside
  its structs and takes no ghost parameters.
- **Workspaces.** `safeverbs-spec` is a workspace of its own, so that it verifies on a plain
  clone; `cargo verus verify` runs `cargo metadata` with the current directory's toolchain,
  so the script sets `RUSTUP_TOOLCHAIN` to Verus's. Options such as `--manifest-path` and
  `-p` must come before generic ones such as `-j`.
- **State machines across crates.** The copy of a crate its dependents compile against is
  built without `cfg(verus_keep_ghost_body)`, under which `state_machine!` emits its
  transition relations and invariants. So the specification exports plain spec functions
  that do cross: `nic_inv`, `nic_init`, `nic_next`, `nic_step` and `nic_enabled` for M,
  `abs_inv`, `abs_init`, `abs_next` and `abs_step` for A.
- **Lints.** Declare `cfg(verus_only)`, `cfg(verus_keep_ghost)` and
  `cfg(verus_keep_ghost_body)` in `[lints.rust] unexpected_cfgs`, and allow
  `non_snake_case` for the modules `state_machine!` generates.
