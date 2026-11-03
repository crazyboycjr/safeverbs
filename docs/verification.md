# Verification

SafeVerbs is being verified with [Verus](https://github.com/verus-lang/verus).
The complete library refinement theorem is still an obligation; the checked components
and trusted boundaries below describe the current result.
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

A separates **member resources** (`Lease`: identity, byte range, uniqueness and byte
validity) from **groups** (`Group`: an ordered member sequence and one holder). A request
owns one group, with one access direction per member and one outcome and waker for the
whole group. Ghost `join` and `separate` transitions preserve member identities and
permissions while replacing the group handles. Shared sources may overlap other shared
clones; each writable destination must remain exclusive. These are specification steps,
with no runtime allocation or interval lookup.

| Property | Lemma | Meaning |
| --- | --- | --- |
| S1 | `lemma_s1_no_conflict` | what the program may write, the NIC may neither read nor write; what the program may read, the NIC may not write |
| S1 | `lemma_s1_nic_writes_any_bytes` | the NIC writes only uniquely held memory whose element type accepts every bit pattern |
| F1 | `lemma_f1_outcome_fixed` | an outcome, once produced, never changes |
| F1 | `lemma_f1_take` | taking a request's outcome returns that outcome and its exact ordered member identities, ranges and permissions |
| F1 | `lemma_f1_taken_once` | a request no longer held never comes back (ids are not reused) |
| R1 | `lemma_member_owned_once` | each live member identity occupies exactly one position in one group |
| R1 | `lemma_r1_member_released_once` | once an allocated member identity disappears from all groups, no later step resurrects it |
| R1 | `lemma_r1_kept_while_in_flight` | an in-flight request keeps its members, unchanged, until its outcome exists |
| R1 | `lemma_r1_requests_on_live_qps` | no request outlives its queue pair |
| R1 | `lemma_r1_released_once` | a retired group handle never comes back; its members may survive under fresh handles |
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

**Each worker's share of M** (`share.rs`). M is one state for the whole device, while the
library drives it from several threads: each worker owns one CQ and the queue pairs it creates
there, and the protection domain with its regions is shared through the control path. M is
unchanged; `share.rs` states, from M's definition, what the threads rely on and guarantee each
other. A worker's share (`Share`) is its CQ and a set of QP names no other worker uses;
`share_wf` says a state respects it (its QPs attached to its CQ only, no other QP attached
there). Between two calls of a worker, M moves by the NIC's own steps and by verbs of the
others on their own CQs and QPs, which deregister no region the worker's next verb names
(`rely_reaches` with the `pins`). Proved: such steps keep the share well formed and leave it as
the NIC alone would (`framed`, `lemma_rely_frame`); every verb of the worker's own
(`mine`) that was enabled in the state it last saw is still enabled
(`lemma_rely_keeps_enabled`, which generalizes `lemma_nic_steps_keep_verbs_enabled`); every
rely run is a run of M (`lemma_rely_is_nic_run`); and a worker's own verbs are foreign steps
for every disjoint share (`lemma_mine_is_foreign`, the guarantee) and keep its own share well
formed (`lemma_mine_keeps_share_wf`).

**Each worker's share of A** (`abs_share.rs`). A share owns QP names and historical
group handles, not disjoint byte ranges: readonly clones may overlap across workers.
`foreign` covers every real transition of A; `lemma_foreign_step` and `lemma_rely_frame`
prove that foreign steps preserve local groups (including wakers and phases), posted
members, outcomes and QPs. Fresh allocations may advance the allocator and foreign
completions may append wakes. `lemma_mine_is_foreign` supplies the disjoint-share
compatibility rule. `lemma_transfer` proves explicit ownership transfer for program
leases without changing A. Concrete two-worker examples cover local in-flight state,
foreign allocation/completion and overlapping readonly clones with transferred IDs.
These are ghost ownership/frame proofs; they do not construct concrete buffer permission
tokens or the real worker's simulation. A local thaw's enabledness is not preserved by
foreign readonly cloning, so the implementation must still test uniqueness.

**Finite interleavings** (`composition.rs`). Given one coherent global history of
legal events, the checked trace lemmas lift disjoint workers' guarantees into M and A
runs and each worker's rely/frame relation. M also preserves enabled local verbs. A
does not promise this for thaw, which a foreign readonly clone can invalidate. These
lemmas do not create a shared device authority from independent `NicToken`s.

**The refinement theorem's composition** (`refinement.rs`). `theorem_1` proves that for any
implementation meeting the obligations of the `Refines` trait, along any execution, A's and
M's invariants hold, every verb met its precondition (S2), and A and M stay coupled.
`corollary_s1` and `corollary_f1` restate S1 and F1 against M: the NIC touches only memory A
gives it, and the outcome a client takes aggregates only its own fully retired WQE
interval. `WqeRange` enforces nonempty, disjoint intervals. `lemma_aggregate` proves that
aggregation keeps the earliest error or the final success, `lemma_singleton` preserves
the single-WQE case, and `lemma_poll_prefix_pending` proves that retiring a proper prefix
keeps the batch in flight. Concrete traces include a remote error followed by a flush;
the batch returns the remote error only after both WQEs retire. These are specification
results; the real worker still needs to discharge `Refines`.

**Worked examples** (`examples.rs`) are concrete executions checked step by step; they show
that the transitions can happen, so no lemma above is vacuous. A mixed group contains a
READ destination and a frozen SEND/WRITE source whose clone remains readable by the
program. Both ordinary completion/take/separation and cancellation preserve the individual
permissions and aggregate the WQE interval before releasing the group.

#### The core

The code of `crates/safeverbs-core` written inside `verus!` is verified with no `assume` or
`admit`; it trusts only the items listed in `scripts/trusted.txt` (see "Trusted computing
base" below), and the script refuses any other.

**Executable buffer ranges** (`buf/range.rs`, `buf/memory.rs`). The real `RawMut` holds
an `OwnedRange`: a pointer, a byte length and erased `ByteRange` geometry. Frozen `Buf` views use `ElementRange`,
which keeps the original offset/element-count representation and erases its byte
geometry and element width; `Buf::len()` still needs only a field read. Verus proves
checked construction, non-overflowing end calculation, exact adjacent splits, and
bounded element slicing (including multiplication by the element size). Checked SGE
length conversion either preserves every byte or rejects a length above `u32::MAX`.
The ordinary buffer methods call these executable operations, rather than a separate
copy of their arithmetic. Compile-time layout assertions keep both runtime ranges at
two machine words. Checked lemmas identify its finite byte domain with A/M's `Span`,
prove that slices stay within the retained range, and split an existing vstd
`PointsToRaw` token along the exact range partition without changing provenance.

**Actual allocation permissions** (`buf/memory.rs`). The fallible zeroed allocator
returns a linear raw-byte permission and deallocation capability on success; it
returns no permission on failure. Checked executable code carries that permission in
the actual `RawMut` and `Frozen` contents, partitions it when splitting, and transfers
the identical contents through freeze and successful thaw. Shared views retain the
same frozen permission. An erased map of per-byte vstd tokens supports borrowed
nonaliasing proofs while preserving exact token identities; no map is built at runtime.
`Part`'s invariant ties its actual SGE to that permission's
byte domain; `Leases` preserves this relation through guard deduplication. Alignment
checks use the absolute pointer, including when recovering over-aligned elements.
Failed registration returns the whole permission for checked deallocation. An erased
pool supports streaming withdrawal and return, but the native `RingBuf` callers still
need to prove the pool preconditions. Final Arc-driven region cleanup, registration
lifetime, initialized typed contents and the public wrappers remain unverified. These
tokens describe allocation ownership, not validity of arbitrary incoming bytes as `T`.

**Actual frozen views** (`buf/view.rs`). `Buf<T, N>` itself is verified: its native
Arc and element range retain the same frozen permission through cloning and bounded
slicing. The view has whole-element offsets and stays within the frozen range. Checked
thaw returns that whole range on success, including when the handle was a subview, or
retains the same view on failure. SGE construction uses the actual view geometry.
The public range-syntax parser and zerocopy/slice-reference wrappers remain outside
Verus; these geometry facts alone do not establish initialized `T` contents. The actual
view-pointer computation proves absolute typed alignment, provenance and byte bounds
within `isize::MAX`. Fallible `Buf::try_into_part` returns an oversize view unchanged;
the checked successful SGE has exactly that view's bytes and readonly access.

**Actual request resources** (`buf/lease.rs`). `Part` and `Leases` are no longer
opaque to the verifier. Their actual field observations, empty construction, native
retention/deduplication algorithm, and first-unique-resource extraction are checked.
`Leases::view` describes physical keepers, while its erased `loans` ledger preserves
every logical input's actual SGE, domain and operation access. An existential backing
relation proves each logical input has a retained keeper; several overlapping shared
inputs may use the same keeper. Incoming local writes require unique resources. `new`
establishes the invariant and the native `push` algorithm preserves it even when dropping
a redundant Arc. Extraction returns the first unique resource from the original sequence.
The container's length accessor assumes its mathematical resource count fits `usize`.

The erased `wqes` ledger records each WQE's actual `wr_id`, opcode, SGEs and disjoint
interval of logical inputs. The actual collection loop in `Hw` proves the correspondence,
including its reverse SGE order. Receive and drain paths record their footprints too.
`fully_recorded` says every logical input belongs to a WQE. `Hw::inv` establishes both
properties for open resources and all staged, posted, completed or refused ring payloads;
`take` and an abort returning a kept owner export them. The ring preserves the entire
resource value, including both ledgers, through completion, refusal, detach and waker
changes. Layout tests show the ledgers add no native size or alignment.

**Actual member projection** (`buf/projection.rs`). A member's byte set is derived from
the corresponding actual input SGE, and exclusivity from its actual `Part`. Each projected
member has a retained guard. With complete provenance, the projected request's local
read/write footprint is exactly the union of its recorded WQE SGEs with that role.
SEND/WRITE sources remain distinct from READ destinations and receives. The adapter
constructs `Permissions<Leases<N>>` for the ring using request/member-indexed names.
Names and arbitrary-byte validity are supplied separately; it does not establish fresh
allocation, `Group::wf`, A placement or coupling to M's current request intervals.
The actual payload types `RawMut` and `Frozen` now carry byte permissions, but the
projection still needs their disjointness, fresh A identities and typed validity to
construct a well-formed A group. Registration lifetime is a separate obligation.

**Failed MR revocation (runtime behavior, tested).** `Nic::close_mr` is fallible and
idempotent. A failed close retains its handle. `Exposed::try_reclaim` returns the still
exposed owner for retry; successful reclaim alone returns typed access. On failed final
cleanup, the wrapper retains the registration, backing allocation, PD and device rather
than freeing memory the NIC can still access. Backend unwinding also retains backing
resources. The model tests cover refusal/retry, successful release and retained dependency
chains. These control-path routines remain outside Verus; no unconditional leak-freedom
claim follows.

**The completion ring** (`ring.rs`). `Ring::new` establishes the eight invariants of the
module documentation (`Ring::inv`) and every operation requires and keeps them; Verus also
checks that no operation panics (indexing, the `assert!`s left for callers outside the
proofs) or overflows. The only run-time check added for the proofs is one comparison in
`has_free_slot`, which refuses a new request once the WQE or request numbers could not hold
`max_wqes` more (after 2^64 of them; one comparison of their bitwise or covers both). Every
operation is specified by what A sees of the ring: its requests (`Ring::req`: posted and not
freed, in flight or complete, with waker, owner and leases), the requests written but not
posted (`Ring::staged`), the requests whose doorbell the device refused (`Ring::refused`:
never posted, their leases kept for their owners) and the wakers woken (`Ring::woken`, a ghost
log). `take` returns exactly the outcome and the very leases of a complete request, or of a
refused one with a flush error; the leases a request holds are the value `end` moved in,
until it retires, is taken, is refused without an owner, is handed back by `abort_keeping`
or the queue pair is destroyed.

A refused doorbell (`abort`, `abort_keeping`) never makes a sequence number void: each staged
request with an owner becomes a refused one in the window, with no WQEs (the invariants allow
such void slots among the requests in flight, `Ring::void_ok`), which its owner takes or
drops at any time; `retired` moves past them without a completion, and the WQE numbers the
NIC never saw leave the accounting and go to the next requests. Wakers registered on the
staged requests are woken (the ring's log grows by them; A, which never saw those requests,
wakes nothing).

**The ring refines A** (`refine.rs`). In every state of A where the ring's requests sit
(`placed`: each request A sees in the ring is a request group of A, on the ring's queue
pair, with the same phase, waker and owner; each staged or refused request's resources a
program group of A; no two the same), each operation's change is zero or more steps of A to a state
where they sit again: none for `begin`, `push_wqe`, `end` and `take_detached_error`;
`post_request` (then `register` or `detach`) for each request `commit` posts; `drop` for each
staged lease without an owner an abort releases, none for the others (their leases stay the
program's, kept in the ring or handed back); A's `complete` for each request `complete`
retires, in order, A's wakes growing by exactly the wakers the ring woke (L1 for the ring);
A's `take`, `detach` and `register` for `take`, `detach` and `set_waker`, none for taking a
refused request and `drop` for detaching one; and for `release_all` a flush, `detach` and
`drop` that leave nothing of the queue pair in A. The async layer's side list of results
moved out of a clogged ring is covered as well (`with_side`): moving a result there is no
step, taking it is A's `take` with the outcome and leases the ring gave (no step for a
refused one). This is the ring's share of the obligations of `Refines`.

`Permissions<L>` supplies a ghost interpretation indexed by request sequence and concrete
resource value, plus each request's member directions. Equal frozen values in distinct
requests can therefore have distinct member names without a native ID counter. Placement requires
that correspondence, and every ring/side-list adapter preserves it. Concrete ghost views
exercise these contracts with mixed permissions and repeated equal source values. The
actual loan projection supplies spans and exclusivity; originating byte permissions and
fresh identities remains a buffer-token obligation below.

The ring also states its WQE accounting (`spec_wqe_retired`, `spec_wqe_committed`,
`spec_wqe_posted`, `lemma_wqes`): `push_wqe` hands out the next number, `commit` commits every
number handed out, an abort takes the uncommitted ones back, `complete` retires no WQE past the
one completed, and the other operations leave the numbers alone. The worker needs this to
relate a ring to its queue in M.

**The `wr_id`** (`wr_id.rs`). `wr_id` packs a QP's index, the queue and the low 47 bits of a
WQE number; `index_of`, `is_recv` and `wqe_of` are proved to recover them, `wqe_of` for every
WQE at most 2^47 after the oldest in flight (`lemma_wr_id_fields`, `lemma_low_bits_determine`,
bit-vector proofs).

**The worker's hardware side** (`hw.rs`). Everything of a worker that M sees lives in `Hw`: its
CQ, each QP's hardware handle with the rings of its two queues, the WQE a request holds back
with its parts, and the worker's token (`NicToken`: M's state as the worker last saw it and its
share of M). The unverified `Worker` (`worker.rs`) drives it through its public methods only,
each of which requires and keeps the invariant `Hw::inv`:

- every call into the NIC goes through a trusted verb of `trusted.rs` whose precondition is
  M's, so Verus checks S2 at each call: `create_qp` (M's reservation: the CQ's reserved count
  in M is exactly `Hw::reserved`, and a QP is created only if its depths fit, so no CQ can
  overrun), `connect` (from `Reset` only), `to_error`, the doorbell (only on a QP M sees in
  `Rts` or `Err`, with room in the queue from the ring's accounting and every WR M accepts:
  its scatter/gather count, operation, length and registered regions), `post_recv`, `poll`,
  `destroy_qp` and `destroy_cq`;
- the coupling: QP `i` of the table is M's QP `id`, M's queue holds the ring's WQEs from at or
  after the oldest in flight to the last committed, and the `k`-th has the `wr_id` of index
  `i`, its queue and number `head + k`; the phase the worker knows is M's state;
- routing (`progress`, `dispatch`): each completion `ibv_poll_cq` reports is the CQE M's
  `poll_cq` pops (`lemma_poll_steps` takes a poll apart), which names a live QP of the
  worker's share; its `wr_id` decodes to exactly that QP's index, queue and WQE, which the ring
  retires while M retires it too: the request a completion reaches is that WQE's;
- teardown: `start_drain` moves a QP with work in flight to `Err`, `post_drain` writes the
  drain WQE (a signaled SEND of nothing, which M accepts in `Err`), and `destroy` destroys a QP
  only when neither ring has a WQE in flight, which the coupling turns into M's empty queues,
  `destroy_qp`'s precondition; `finish` destroys the CQ only with no QP left;
- under rely, the invariant survives whatever the NIC and the other workers do between two
  calls (`lemma_rely`).

**The trusted verbs** (`trusted.rs`). The core's calls into the NIC have contracts taken from
M: each requires M's precondition, on the token's own share, and ensures that M moved by rely
steps and then by the verb's step (`doorbell` posts the WRs written since `start` as one
`post_send`; `poll` reports the CQE each `poll_cq` step pops; `connect` is three `modify_qp`
steps). The token is one worker's and only these wrappers make or change it: `create_cq` makes
a token over a fresh share. These are trusted, not proved.

### Stated, not yet proved

- **Theorem 1 for the real library.** The obligations of `Refines` (start states; every API
  call is zero or more steps of A and of M; the invariant couples A and M) are what
  `safeverbs-core` must prove. Today:
  - **M's side is proved for the worker** (`hw.rs`): every verb the worker calls meets M's
    precondition (S2) in the state the worker last saw, which under rely means it does when it
    happens; M stays in states its invariants hold in; and the rings stay coupled to M's
    queues, so the completion routed to a request is its own WQE's (F1's routing half) and the
    reservation keeps the CQ from overrunning. This holds for every operation of the
    synchronous worker, the event loop's and the futures' alike, since both go through `Hw`.
  - **A's side is proved for the ring** (`refine.rs`), not yet for the worker: the worker has
    no ghost map from its rings' requests to A's leases (`ids`, `dirs` of `placed`), and so no
    `impl Refines`. What that needs: the ghost state of A (which leases the program holds) in
    the worker, which only the buffers can supply (next point); and integration with the
    checked A-side frame/transfer rules of `abs_share.rs`.
  - **Buffers.** Actual mutable/frozen ranges and frozen views now carry the allocator's
    byte permissions through checked geometry, splitting, freeze/thaw and SGE construction.
    The typed wrappers in `buf.rs` remain outside `verus!`. Initialized contents,
    zerocopy contracts, native streaming-pool callers, exposure/revocation and final
    cleanup remain unfinished; registration lifetime is still trusted through
    `registered` and `registered_all`.
  - **Concrete grouped permissions.** The checked actual loan ledger now accounts for
    shared deduplication, operation access and exact SGE/member projection. `Hw` retains
    that invariant through its rings and returns it with owned results. Actual keepers
    now prove nonaliasing within each request, so projected groups satisfy `Group::wf`
    given distinct names. Fresh global member names, arbitrary-byte validity,
    cross-request compatibility and A placement remain unfinished.
  - **Composition.** Each worker's view is coupled to M under rely, and every worker verb is a
    foreign step for the others (`lemma_mine_is_foreign`). `composition.rs` now proves
    finite interleavings given one coherent legal global history. Constructing that
    authority from real workers remains open: tokens are not split from one global token;
    `create_cq` makes each. The control path (allocating
    PDs, registering and deregistering regions in `buf.rs` and `pd.rs`) is not verified; rely
    assumes it deregisters no region a worker's written WR names, which leases ensure.
  - **Operation-specific outcomes.** The earlier SEND/WRITE `byte_len` mismatch is
    resolved by projecting undefined fields away: `CompletionKind::StatusOnly` carries
    only status. `lemma_ack_outcome_unique` proves the observed and implied successes
    agree. READ outcomes carry read length; receive payloads are bounded by receive
    SGEs; WRITE_WITH_IMM notifications are not. This resolves that particular obstruction,
    but does not construct the missing worker-to-A map or discharge `impl Refines`.

  Until then Theorem 1 holds for any implementation that discharges the obligations; the core
  discharges M's side for the worker and A's side for the ring.
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
| `trusted.rs` `ExSge`, `ExRemoteBuf`, `ExWrOp`, `ExQpCaps`, `ExDeviceCaps`, `ExQpType`, `ExError`, `ExBufError` | the definitions of these plain data types, as written |
| `trusted.rs` `ExConnectAttr`, `ExEndpoint`, `ExPd` | these types are opaque (only the device or `pd.rs` reads them) |
| `buf/memory.rs` `ExRegion` | registration and final Arc-driven cleanup remain opaque; actual buffer ranges and permissions are verified |
| `buf/memory.rs` `allocate_zeroed_raw` | fallible `alloc_zeroed`, with vstd-compatible address, layout, provenance and linear allocation/deallocation permissions on success; no initialized-content claim |
| `buf/memory.rs` `arc_try_unwrap` | std Arc returns its exact contents on success, or an Arc to the same contents on failure; no promised success condition |
| `buf/lease.rs` `same_frozen_arc` | std Arc pointer equality implies that both references retain the same frozen allocation |
| `trusted.rs` `ExNic`, `ExNicQp`, `ExNicCq` | the NIC traits may bound the wrappers' generics (declared without specifications) |
| `trusted.rs` `device_caps` | the device's limits as the library reads them are M's (`caps_model`) |
| `trusted.rs` `create_cq` | `ibv_create_cq_ex` is M's `create_cq` of a name never used, after whatever happened before, in a state where M's invariants hold; the new worker's share is that CQ and QP names nobody uses (this is where tokens come from) |
| `trusted.rs` `alloc_domain` | thread domains change which locks the driver takes, not what the NIC does (no contract) |
| `trusted.rs` `create_qp`, `connect`, `to_error`, `destroy_qp`, `destroy_cq` | these verbs are M's `create_qp` (with the attributes `qp_attr` says), three `modify_qp` steps, `modify_qp` to `Err`, `destroy_qp` and `destroy_cq`, or (on error) nothing, after rely steps |
| `trusted.rs` `qp_num_of`, `qp_lock_free` | facts of no meaning in M (no contract) |
| `trusted.rs` `no_qp` | panics: a key that names no QP never returns |
| `trusted.rs` `pd_id_of` | the PD's runtime identity matches its observation in the model |
| `trusted.rs` `registered`, `registered_all` | a part's lease promises its region is registered in its domain (writable if the part is unique) and stays so while the lease lives (`buf.rs`'s `Region`); learning it moves the view to the present |
| `trusted.rs` `start`, `post`, `abort` | `ibv_wr_start`, `ibv_wr_*`, `ibv_wr_abort`: writing WRs changes nothing in M until the doorbell; an abort discards them |
| `trusted.rs` `doorbell` | `ibv_wr_complete` is M's `post_send` of the WRs written since the start (after rely steps), or nothing on error |
| `trusted.rs` `post_recv` | `ibv_post_recv` of one WR is M's `post_recv`, or nothing on error |
| `trusted.rs` `poll` | each CQE `ibv_poll_cq` returns is the one M's `poll_cq` pops, with its `wr_id` and outcome |

Every wrapper's rely pins the regions the worker's written WRs name (`pinned`): their leases
are held (by the hardware side, for the WQE a request holds back and its parts), so no other
thread deregisters them meanwhile.

Modelling choices sit in these contracts: M has no atomics, and the contracts treat a
fetch-and-add or compare-and-swap as an RDMA READ (locally both have the NIC write the local
buffer); a `Wc`'s status maps to M's six statuses as `Status::model` says; M has one limit of
scatter/gather elements per QP, the smaller of the QP's send and receive limits (the worker
holds every WQE to it); and safeverbs posts no inline data, so M sees a QP's inline limit as
zero.

`Waker::wake` and the wrappers' bodies are the only code the proofs call without checking it.
The rest of `safeverbs-core` (`buf.rs` apart from its verified range/resource helpers, `worker.rs`,
`verbs.rs`, `model.rs`, `pd.rs`) is
outside `verus!`: not trusted by the proofs, but not verified either. `worker.rs` holds no
state M sees (keys, the event loop's cursors, the drain's clock): it cannot break the coupling,
only misuse the API, which `Hw` handles. The futures layer (`safeverbs`) is safe Rust on top
of the worker, outside Verus.

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
safeverbs-spec: verification results:: 173 verified, 0 errors
safeverbs-core: verification results:: 290 verified, 0 errors
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

The completed refinement needs a tracked token whose view is M's state, a map from
requests to WQE intervals, and A's lease permissions. The token and separate component
proofs exist (`trusted.rs`, `hw.rs`, `refine.rs`); the worker-level request map, permission
integration and `impl Refines` remain to be constructed. Proof-only hardware entry points
are crate-private, so ordinary Rust callers cannot bypass their verified preconditions.

**Trusted verbs wrappers** (`trusted.rs`). Each verb is one small function with an
`external_body` and a contract taken from M. The precondition is checked by Verus at every
call site, which is S2; the postcondition says the verb happened after rely steps (the NIC's
own, and the other workers' on their own shares), which `share::lemma_rely_keeps_enabled`
shows is consistent. The WRs written since `start` are kept in the token and posted together
by the doorbell:

```rust
pub fn doorbell<Q: NicQp>(qp: &mut Q, Ghost(q): Ghost<QpId>, Tracked(token): Tracked<&mut NicToken>)
    -> (r: Result<()>)
    requires
        qp_of(*old(qp)) == q,
        old(token).started(q),
        nic_inv(old(token).nic()),
        share_wf(old(token).nic(), old(token).share()),
        old(token).share().owns.contains(q),
        nic_enabled(old(token).nic(), Nic::Step::post_send(q, old(token).written(q))),
    ensures
        r is Ok ==> exists|mid: Nic::State|
            rely_reaches(old(token).share(), pinned(*old(token)), old(token).nic(), mid)
                && nic_step(mid, final(token).nic(), Nic::Step::post_send(q, old(token).written(q))),
        // ...
```

**The hardware side** (`hw.rs`). A ghost token cannot cross unverified code (a caller
outside `verus!` cannot pass a `Tracked` argument without assuming it), so the token lives
inside `Hw`, a verified struct with private fields, next to everything the token describes:
the CQ, the QPs' handles and rings, the held WQE and its parts. Its public methods require and
keep `Hw::inv`; nothing outside the module can touch its fields, so the invariant holds
whenever the unverified worker calls in.

**Leases.** The ring tracks each request's leases as the value itself: the `L` A sees a
request hold is the one `end` moved into its slot, and `take` or `abort_keeping` hands back
that value (`refine.rs`). Its ghost `Permissions<L>` interpretation preserves the exact
ordered member resources and their access directions through the ring and side list.
`buf/projection.rs` derives spans and exclusivity from the actual logical inputs, and
the worker retains their complete WQE provenance. The maps from sequence numbers to A's
group/member names, typed validity and global byte compatibility still need the buffer
and worker integration before those projected groups can be placed in A.

**Memory as linear ghost permissions (partly integrated).** The real buffer payloads
carry the fallible allocator's byte permissions through `OwnedRange`, `RawMut`, frozen
Arc contents and bounded `Buf` views. Actual requests retain those payloads. Native
WQE provenance links each local SGE to its retained loan and keeper. Global nonaliasing,
A placement, initialized typed access and final release are still needed for S1 of the
whole implementation. `registered` continues to supply the registration-lifetime
assumption used by the verbs contracts. These remaining obligations are not discharged
by having allocation tokens in the structs.

**The `Refines` implementation (not done).** The core would implement `refinement::Refines`
for a worker's state (its `Hw`, with A's state as the worker last saw it), relate each public
function's postcondition to `step`, and prove the three obligations; `theorem_1` does the
rest. The M obligations are what `Hw`'s methods prove; the ring's lemmas in `refine.rs`
(`lemma_ring_*`) are the steps of A for each call's ring part.
