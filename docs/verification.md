# Verification

SafeVerbs is verified with [Verus](https://github.com/verus-lang/verus). This page says
what is proved, what is only tested, what is assumed and what remains, then how to run
the proofs. [`design.md`](design.md) explains the design behind them.

## Where things stand

The guarantee is one refinement theorem: under a model **M** of the NIC
(`crates/safeverbs-spec/src/nic.rs`), the library behaves like a small abstract machine
**A** (`abs.rs`), and the properties S1, F1, R1 and L1 are lemmas about A. The theorem
(`theorem_1` in `refinement.rs`) is proved for every implementation that discharges the
obligations of the `Refines` trait.

**One engine and its buffers discharge them** (`EngineState`, `theorem_1_engine` in
`crates/safeverbs-core/src/hw/refines.rs`). Its state is the `Hw` inside an `Engine`: its CQ, its
queue pairs, their rings, and the requests in them. Each method of `Engine` on the data path keeps
the invariant, is zero or more steps of A, and moves the NIC only by legal steps of M, and A and M
are coupled at every point (`coupled`). `example_run` there drives `Engine`'s public methods, on
typed buffers, through the theorem.

**A buffer needs no lease until it meets an engine.** Until then its linear byte permission is all
there is to it, so what the program does with its buffers (allocating, splitting, freezing, sharing,
thawing, dropping) goes through no engine and owes A nothing. When a request is written, what keeps
its inputs alive is shown apart from everything in the rings, by a traversal a plain build does not
have (`Hw::separate_lease`), and A's own steps make it a lease of the program's: `drop` of the free
leases over those bytes, which no live buffer can stand for, then `alloc`, then `freeze` for a frozen
allocation (`lemma_adopt_loans`). For any buffer the program holds, at any moment, the same steps
reach a state of A the engine stands for too (`Hw::adopt`), so A's S1 is said of the buffer itself:
the NIC touches none of its bytes (`BufMut::apart_from_nic`, `Buf::apart_from_nic`).

So the buffers carry no ghost name and take no step: a shared authority over A's leases would have
to be opened by every buffer's `Drop`, which Verus does not allow, and tied to `Arc`'s counts on
trust. And this composes: each engine has its A, what one adopts is apart from what the others hold
by the same permissions, and what they owe each other is the one premise left.

**The premise left: what the NIC may touch beyond the engine's queues, A gives it**
(`outside_covered` in `share.rs`): the WQEs of other engines' queue pairs and the regions open to a
peer are leases of A in the NIC's hands. For an engine alone on its device with no region open to a
peer, there is nothing to say. It is stated of each state, in `Refines::inv` and `Refines::step`.

**So what the theorem does not cover yet:** the handles of the `safeverbs` crate, one device history
shared by several engines, exposure, `RingBuf`'s placement and liveness (see "Remaining").

`scripts/verify.sh` checks everything below that is called proved. The specification
crate uses no `assume`, `admit`, `external_body` or assumed specification; the code of
`crates/safeverbs-core` written inside `verus!` uses no `assume` or `admit` and trusts
only the items of `scripts/trusted.txt` (see "Trusted computing base"). The rest is ordinary Rust,
not trusted by the proofs and not verified either: `pd.rs`, `verbs.rs`, `model.rs`, the `safeverbs`
crate, and the shells of `buf.rs` and `engine.rs`. A shell is what Verus cannot read: the methods
with zerocopy's bounds, `Region::new`'s layout, `Exposed` and `RingBuf`; an engine's callbacks
(`poll`), clocks (`drain`), parent domains (`create_qp`), and `Request`, whose `Drop` writes. They
have no proof obligations to get wrong: they reach verified code only by calling it, and every
method they call needs no more than the invariants below ("What is assumed").

## The properties

For each property: what the specification proves (`crates/safeverbs-spec`), what the
ring proves (`ring.rs`, with the steps of A in `refine.rs`) and what the engine proves
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
- *Engine.* Every byte a WQE in M may access locally lies in a buffer its live request
  holds, with the access its operation needs.
  Buffers carry the allocator's linear byte permissions through splitting, freezing,
  slicing and thawing (`buf/memory.rs`, `buf/view.rs`), each SGE is covered by the
  permission of the buffer its request retains (`buf/lease.rs`), the inputs of one request
  are disjoint where one is unique, and a buffer posted as a receive is disjoint from
  every buffer the engine holds. A request being written is a value of its own
  (`OpenRequest`), the program's until it is closed: nothing of it is in M or in the rings, the
  inputs of one such request are disjoint where one is unique (`Kept`), and what it holds is
  apart from a receive by the ownership and linear permissions of its parts, not by the engine,
  which does not hold it.
  Those buffers are leases of A: each input of a request is in a lease the request holds, in its
  direction (`holds_cover`), so every byte M lets the NIC touch is one A gives it (`coupled`,
  `lemma_wqe_byte`), and `corollary_s1` applies to the engine's states.
- *Real buffers.* A buffer the program holds is apart from whatever the NIC may touch
  (`Hw::keeper_apart`, `BufMut::apart_from_nic`, `Buf::apart_from_nic`): `corollary_s1` in a state
  of A where it is a lease of the program's. A `BufMut` and a `Part` keep what they say as type
  invariants, `Peek` too, so no caller has to promise anything. This rests on Rust's ownership of
  the buffers, which is what their linear permissions model, and on `any_bytes`, below.
- *The one exception:* `Exposed::copy_to_slice` reads memory a peer may be writing. No
  reference to exposed memory is ever formed; it is read only through this copy, with volatile
  reads into a `T` that accepts any bytes, which is a trusted assumption (`volatile_copy`
  below). The copy may be torn: a protocol learns that the data is complete from a
  write-with-immediate notification or a SEND, never by polling the bytes (on MRC, data lands
  out of order).

**S2, no local misuse.** Every verb the library calls meets M's precondition.
- *Spec.* The preconditions are M's `require`s. The NIC's own steps never invalidate one
  (`lemma_nic_steps_keep_verbs_enabled`), and neither do other engines' verbs on their own
  queues (`share.rs`: `lemma_rely_keeps_enabled`, `lemma_mine_is_foreign`).
- *Engine.* Proved: every call into the NIC goes through a trusted wrapper whose
  precondition is M's, and Verus checks each call site: queue pair creation within the
  CQ's reservation, connection from `Reset`, doorbells on a queue pair in `Rts` or `Err`
  with room in its queue and every WR one M accepts (scatter/gather count, operation,
  length, registered regions), receives, polls, and destruction of a queue pair only with
  empty queues and of a CQ only with no queue pair. Between two calls the engine's view
  stays valid whatever the NIC and the other engines do (`lemma_rely`). A request being written
  goes out only on a queue pair that takes what it was written for: its protection domain, its
  message length, its number of WQEs and, if a WQE of it needs them, a reliable connection and
  its scatter/gather elements are compared when it closes (`HwQp::takes`). A queue pair's
  protection domain is its own, and of the engine's device (`same_device`).
- *Not proved:* the control path that allocates protection domains and registers and
  deregisters regions (`register_region`, the region's `Drop`, `pd.rs`). The engine's rely assumes
  it deregisters no region a written WR names, which leases ensure.

**F1, what a request's outcome means.** Each request gets at most one outcome, with its own
buffers. Success means every WQE succeeded; an error names the first that failed, those before
it having succeeded and it and those after it not, their remote effects unknown; a refusal
means the NIC never saw the request.
- *Spec.* An outcome never changes once produced, and a request taken never comes back
  (`lemma_f1_one_outcome`); taking it returns it, and each lease under its own name
  (`lemma_f1_take`). Against M (`corollary_f1`), a request owns a nonempty interval of WQEs on
  one queue, no other request owns any of them, and its outcome exists only once all of them
  retired. Success: all of them succeeded, READs with the full length they asked for, and the
  outcome is the last WQE's. An error: it is the first in posting order (`lemma_aggregate`),
  those before it succeeded, and it and those after it did not. M gives this last part: a queue
  pair stays in `Err` while its queue holds a WQE, and in `Err` every WQE fails
  (`lemma_request_step`, `WqeRange::no_success_after_error`). A refusal: M's queues hold no new
  WQE and the program kept the leases, of which the NIC writes none and reads no unique one
  (`corollary_f1_refused`). A successful SEND or WRITE has one outcome, reported or implied
  (`lemma_ack_outcome_unique`).
- *Ring.* `take` returns exactly the outcome and the payload of a complete request, or of
  one whose doorbell was refused, and says which; a refused request never becomes a request of
  A, and its leases stay the program's throughout (`lemma_abort`,
  `lemma_take_refused`). Sequence numbers are never reused. An engine's
  payload is the request's buffers and its context (`ring/payload.rs`), so each context comes
  back exactly once with its own buffers by the same proof, with no new trust.
- *Engine.* Each completion reaches the queue pair, queue and request of its own WQE
  (`wr_id.rs` and the coupling of rings to M's queues). M pops a CQE when it is read and the
  ring hears of it when the event loop routes it, the program posting and polling in between, so
  M is ahead of the rings: the invariant keeps the CQEs read and not routed (`Hw::pending`,
  `pending_ok`), each naming a queue pair of the table and a WQE in flight there, and every WQE M
  popped that a ring has not heard of succeeded without a completion or is named by a CQE to
  come. So routing changes nothing in M, and a queue pair with a CQE to come cannot be destroyed.
  A receive request's result is the outcome M retired for its WQE (`hw/outcomes.rs`). A send
  request, one WQE or a batch, agrees with M's retired WQEs of its interval through every engine
  operation (`send_results` in `hw/send_outcomes.rs`, part of the engine's invariant), refused
  doorbells, flushes after an error and the drain before a destroy included. While it is in
  flight, no completion the ring heard of named its last WQE or one after it, and the error it
  caches natively is the first error among the WQEs of its interval the ring heard of, none if
  they all succeeded. Once
  it is complete, all its WQEs retired and its outcome is M's aggregate: the first error in
  posting order, else its last WQE's success, which is what that WR's operation defines (the
  length a READ asked for, so a batch of READs reports its last READ's length; the status alone
  for a SEND or WRITE). `Hw::take` returns that outcome, as a typed result equal to it
  (`Completed::model`), with the request's own context and buffers (`send_taken_matches`,
  `recv_taken_matches`), and so does the event loop, one request at a time as it routes the CQEs
  read (`Hw::next_completion`, `hands_out`), with the buffers that may leave a request of several
  (the frozen ones are released there). An error's `position` is proved to be that of M's first
  failed WQE in the request's interval: it is computed from the WQE number of the completion
  that ended the walk or lay inside the request, with one subtraction, on the error path only. A
  request whose doorbell was refused, which M never saw, comes back as
  `CompletionError::Refused`, a case of its own. A request with an owner that is complete or
  refused is always handed back (`takeable`), and one without an owner holds no context, so a
  poll runs no destructor of the user's. The event loop's walk of one CQE ends: it retires a
  request at each step, or the CQE is spent.
- *Detached.* The first error of a queue's requests without an owner, which
  `Engine::take_send_error` reports (a sink's error), is tied to M the same way
  (`detached_result`, `detached_taken_matches`): the request it came from retired all its WQEs,
  and its status and position are M's first error among them; or it says a refused request,
  which M never saw. Which of those requests fails first across the queue is not claimed
  beyond what the ring keeps: the first one to retire with an error since the last take.
- *Against A.* A request completes in A when M pops its last WQE, with M's aggregate of its WQEs,
  which is what its ring then reports. The ring hears of it later, when the CQE is routed, which
  is no step of A (`seen`). What the event loop hands out of a request the NIC saw is what A says
  of it: A has it complete, and the result reports that outcome (`lemma_hands_out_in_a`). Under
  Theorem 1, `corollary_f1` and `corollary_f1_refused` apply to the engine's states: distinct
  requests own disjoint WQE intervals for good, those complete or taken too (`ranges_ok`), and no
  WQE of a request succeeds after one that failed.

**R1, the library never loses track of a buffer.** Every lease is in exactly one place at every
moment, moves only along the allowed transitions, and is released once, after the NIC is done.
- *Spec.* `Abs::State::place` says where a lease is: with the program, with the peer, in flight
  in a request, in a complete request not taken yet, or released (`inv_places` makes that
  meaningful, and every request lives on an existing queue pair). Every step keeps it there or
  moves it along `allowed_move`, and a lease leaves a request in flight only once the request
  has its outcome (`lemma_r1`). Against M, a lease in flight has its request's last WQE still
  posted, even if a prefix retired with an error, and a lease in a complete request has all its
  WQEs retired (`corollary_r1`).
- *Ring.* Leases stay in their slot until the request completes, is taken, refused,
  detached or released with its queue pair, and leave it once.
- *Engine.* A queue pair is destroyed only when neither ring has a WQE in flight, which M
  turns into empty queues: its buffers are released after the NIC retired their WQEs. Each
  operation on a request is a move of its leases in A, and destroying a queue pair is A's `detach`
  of what its rings still hold and `drop_qp`, with no outcome made up (`lemma_linked_destroyed`).
- *A device that does not flush.* The engine keeps the queue pair and its buffers and reports
  `Error::NotFlushed`: their leases stay in flight, and nothing the NIC may still use is freed.
  This retention is tested, not proved.

**L1, no lost wakeup.** Once a request is complete, any waker its future registered has
been invoked.
- *Spec.* A complete request holds no waker, and completing one invokes the waker it holds
  (`lemma_l1_no_waker_left`, `lemma_l1_completion_wakes`).
- *Ring.* `complete` wakes exactly the wakers of the requests it retires.
- *Engine.* A waker that panics is caught; the engine finishes the poll's bookkeeping, then
  resumes the panic. Verus checks the normal path; the panicking one is tested. In A a request
  completes when M pops its last WQE, so A's waker runs ahead of the ring's: L1 of the real wakers
  stays the ring's statement above, and a waker registered after M's pop is no step of A.

**Liveness is stated, not proved.** F1-live (every request gets exactly one outcome) and R1-live
(nothing stays in flight after its queue pair is dropped, and every buffer is returned or
released) share one premise, that the device retires every posted WQE: `design.md`, section 3.4.
Waking a future also needs the executor to poll woken tasks. One step of it is proved:
`make_send_room` waits only for a signaled WQE out (`Ring::lemma_room_or_signal`), whose completion
makes room, so that premise is all its end rests on; and it fails rather than wait when no completion
could give it a slot.

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
- Batches on the model and on rxe, through `Engine` and evidence that rxe
  conforms to M: an error in a batch is its outcome, at its position, the WQEs after it flush
  and its context and buffers come back once; on rxe a batch of READs, signaled or retired by
  a later CQE, reports its last READ's length (`tests/batch.rs`). On the model, an error stays
  within its own request's interval (`tests/outcome.rs`) and READ batches step through both
  ways of completing (`tests/read_batch.rs`).
- Contexts: each comes back with its own request across several QPs and exactly once, counted
  by its destructor, whatever becomes of the request, including removed QPs and dropped
  engines (`tests/engine.rs`, and on rxe `event_loop_on_four_qps_of_one_cq`). Refused
  requests are told from flushed ones on both.
- A WRITE with immediate puts no data in the receive's buffers, and every kind of receive
  gets its case of `Received`, zero-SGE and zero-byte notifications included (`tests/wc.rs`, on
  the model and rxe). A remote WRITE with immediate is read with the safe copy, the same rkey
  still valid after it (`tests/rxe.rs`).
- A sink's item number, in unverified code (`sink.rs`), tested on the model and rxe.
- `RingBuf`'s placement and the reuse of its chunks.
- The simulated NIC (`model.rs`) checks some of M's rules at run time: no queue pair
  destroyed with WQEs not retired, no CQ destroyed with a queue pair attached, no work
  posted in `Reset`. That it behaves within M is neither tested systematically nor proved.
- The asynchronous API of the `safeverbs` crate, on the model and on soft-RoCE, and random
  operation sequences with injected errors and flushes on the ring, the engine and the
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

Where a shell calls a verified function, that function's precondition is assumed, not proved. Few
are left, and each is an invariant only verified code can change:

- `Engine::inv` and `Hw::inv`: the fields are private, only `Hw::new` makes a `Hw`, which ensures it,
  and each method keeps it;
- `OpenRequest::inv` and `Leases::inv`, of what `Engine`'s methods take: the fields are private, each
  maker ensures it and each method keeps it. They are no type invariants because a request's fields
  change one at a time on the data path;
- `RingBuf`'s chunks: their byte permissions are trusted (`ring_chunk`), and placing one relies on
  its base.

A `BufMut`, a `Buf`, a `Part` and a `Peek` need no such word: what they say is a type invariant, and
a post takes them as they are.

### Trusted computing base

Beyond the toolchain and M, the verified core trusts exactly the items of
`scripts/trusted.txt`. `scripts/verify.sh` fails if an `external_body` or external
specification appears anywhere else in `crates/safeverbs-core`, if a listed item no longer
exists, or if ghost state is conjured (`Tracked::assume_new()`, `Ghost::assume_new()` and
their kin) anywhere in the core's sources, inside `verus!` or not, outside a listed item.

| Item (`crates/safeverbs-core/src/`) | What Verus takes on trust |
| --- | --- |
| `ring.rs` `ExWaker` | `core::task::Waker` is an opaque type |
| `ring/wake.rs` `ExWakePanic` | std's boxed panic payload is opaque |
| `ring/wake.rs` `try_wake` | `catch_unwind` catches an unwind from `Waker::wake` and does not unwind itself |
| `ring/wake.rs` `resume` | `resume_unwind` never returns |
| `ring.rs` `will_wake` | when `Waker::will_wake` says two wakers wake the same task, they have one name in A |
| `ring.rs` `clone_waker` | `Waker::clone` gives a waker of the same task, with the same name |
| `buf/lease.rs` `forget_free` | forgetting a value the caller proved holds nothing releases nothing: it writes a free slot with no drop glue, which would be a landing pad on the data path |
| `buf/lease.rs` `replace_value` | `core::mem::replace` leaves the new value and returns the old one |
| `buf/lease.rs` `capacity_of` | `Vec::capacity` is at least its length |
| `buf/lease.rs` `swap_in` | `slice::swap` swaps the two elements, in bounds |
| `hw/open.rs` `one_of` | `core::slice::from_ref` is a list of that one element |
| `trusted.rs` `ExSge`, `ExRemoteBuf`, `ExWrOp`, `ExQpCaps`, `ExDeviceCaps`, `ExQpType`, `ExError`, `ExBufError` | the definitions of these plain data types, as written |
| `trusted.rs` `ExConnectAttr`, `ExEndpoint`, `ExContext`, `ExPd` | these types are opaque (only the device or `pd.rs` reads them) |
| `buf.rs` `bytes_back` | when a region goes, the permissions split from it are its own again: each lives in a `RawMut`, which holds an `Arc` of the region, so none is left when the last `Arc` goes. The rest of freeing it is verified (`Region::free_memory`, vstd's `deallocate` with the allocation's `Dealloc`) |
| `buf.rs` `sge_too_long` | a panic never returns |
| `engine.rs` `ExTransport` | the transport trait may bound a key's generics (declared without specifications) |
| `engine.rs` `hw_mut` | `ManuallyDrop::deref_mut` is a reference to its contents |
| `buf/memory.rs` `allocate_zeroed_raw` | the fallible zeroed allocator returns the address, layout, provenance and linear permissions vstd expects, or nothing; no claim about initialized contents |
| `buf/memory.rs` `arc_try_unwrap` | `Arc::try_unwrap` returns the exact contents, or an `Arc` to the same contents |
| `buf/memory.rs` `ring_chunk` | a `RingBuf` chunk comes with the byte permission for exactly its bytes: the ring's placement, which is not verified, keeps its chunks apart |
| `buf/memory.rs` `volatile_copy` | volatile reads of memory a device may write concurrently are defined and yield arbitrary bytes; its one caller, `Exposed::copy_to_slice`, passes a range it owns and copies into a type that accepts any bytes (S1's one exception) |
| `buf/lease.rs` `same_frozen_arc` | `Arc::ptr_eq` means both hold the same frozen allocation |
| `buf/lease.rs` `frozen_apart` | a frozen allocation and another real buffer are that one allocation or own different bytes. Linear permissions make it so, but vstd shows two permissions disjoint only with one of them borrowed exclusively, and a frozen allocation's is behind its `Arc`. An exclusive one needs no trust (`Lease::separate_input`) |
| `trusted.rs` `ExNic`, `ExNicQp`, `ExNicCq` | the NIC traits may bound the wrappers' generics (declared without specifications) |
| `trusted.rs` `device_caps` | the device's limits as the library reads them are M's |
| `trusted.rs` `create_cq` | `ibv_create_cq_ex` is M's `create_cq` of a fresh name, in a state M reaches from a start state (the device just opened), of which its invariants follow; the new engine's share is that CQ and queue pair names nobody uses (this is where tokens come from) |
| `trusted.rs` `create_qp`, `connect`, `to_error`, `destroy_qp`, `destroy_cq` | these are M's `create_qp`, three `modify_qp` steps, `modify_qp` to `Err`, `destroy_qp` and `destroy_cq`, or nothing on error, after rely steps; a queue pair's thread domain changes which locks the driver takes, not what the NIC does |
| `trusted.rs` `qp_num_of`, `qp_lock_free` | facts that mean nothing in M (no contract) |
| `trusted.rs` `no_qp` | a key that names no queue pair panics and never returns |
| `trusted.rs` `pd_id_of` | the protection domain's run-time identity is its identity in the model |
| `trusted.rs` `same_device` | `Context::same_device` says whether a protection domain is of the device an engine's M models |
| `trusted.rs` `clone_pd` | a clone of a handle is a handle of the same protection domain |
| `trusted.rs` `register_region` | `ibv_reg_mr` of the bytes given, with no contract of M's: what an engine learns of it is `registered` |
| `trusted.rs` `pd_alive` | a protection domain of the device exists in M as long as a handle to it does |
| `trusted.rs` `registered`, `registered_all` | a part's region is registered in its domain (writable if the part is unique) and stays so while the part lives; `registered_all` says so of the inputs of a request being written, each kept alive by what the request holds (one that a `Peek` added is of the allocation of an input before it, which the request compared by its `Arc`, so of that input's domain) |
| `trusted.rs` `start`, `post`, `abort` | `ibv_wr_start`, `ibv_wr_*`, `ibv_wr_abort`: writing WRs changes nothing in M until the doorbell, and an abort discards them |
| `trusted.rs` `doorbell` | `ibv_wr_complete` is M's `post_send` of the WRs written since the start, after rely steps, or nothing on error |
| `trusted.rs` `post_recv` | `ibv_post_recv` of one WR is M's `post_recv`, or nothing on error |
| `trusted.rs` `poll` | each CQE `ibv_poll_cq` returns is the one M's `poll_cq` pops, with its `wr_id` and outcome |

**Why `FromBytes`, `IntoBytes` and `Immutable`, and what a reference to a buffer is, stay out of the
proofs.** The proofs read bytes and permissions, never a value of `T`: M and A say which bytes the NIC
may touch, and no more. So what those traits give is trusted, as rustc and zerocopy's own contracts:

- *Initialized contents.* Memory comes zeroed from the allocator, which nothing here states
  (`allocate_zeroed_raw` claims no contents), and `T: FromBytes` makes zeroes and any bytes a valid
  `T`, which is what `Pd::alloc` and `BufMut`'s other makers ask of `T`. A says of a lease that it
  accepts any bytes (`any_bytes`), true of every lease the engine adopts for that reason: a `BufMut<T>`
  exists only for such a `T`, a bound on the shell's methods that the proofs take as given.
- *Sending.* `T: IntoBytes` says that a `T` has no padding, so that sending one reads only
  initialized bytes: a bound on `into_part` and the `Lend` impls.
- *Sharing.* `T: Immutable` says that no `T` has interior mutability, so that a frozen buffer's bytes
  do not change while the NIC reads them: a bound on `freeze`.
- *References.* `Deref` and `DerefMut` of a `BufMut` and `Deref` of a `Buf` make a `&[T]` or a `&mut
  [T]` with `from_raw_parts`, which their linear permissions say nothing to Verus about: they stand
  on the permission's being unique or frozen, on the type invariant's fit of `T` (`fits_type`), and on
  the two facts above.

Each wrapper requires M's precondition on the engine's token and ensures that M moved by
rely steps (the NIC's own, and other engines' on their own queues) and then by the verb.
The rely pins the regions the engine's written WRs name, whose leases the engine holds.
Some modelling choices sit in these contracts: M has no atomics, and the contracts treat a
fetch-and-add or compare-and-swap like an RDMA READ (the NIC writes the local buffer); a
`Wc`'s status maps onto M's six as `Status::model` says; M has one scatter/gather limit per
queue pair, the smaller of its send and receive limits; and the library posts no inline
data, so M sees a queue pair's inline limit as zero.

## Remaining

- **F1-live and R1-live**, stated in `design.md` (section 3.4), with the device's retiring every
  posted WQE as their premise.
- **Buffers.** A region's final cleanup trusts that the permissions split from it are back
  (`bytes_back`). A deposit in a `RawMut`'s `Drop` would not remove that: Verus's `Drop` opens no
  invariant, and the last step would still be to trust that no `Arc` is left. `RingBuf`'s placement
  and the permissions of its chunks (`ring_chunk`) are not verified, nor exposure and revocation.
  The registration lifetime is trusted (`registered`, `registered_all`): saying more needs the
  control path in one device history, as below.
- **Several engines.** Each engine's view of M is proved under rely, and each engine's
  verbs are foreign steps for the others, but one device history for all engines is not
  built: `create_cq` makes each token rather than splitting one, and each engine's A has its own
  `next_id`. `outside_covered`, a premise for one engine, is what they would owe each other.
  Engine creation and destruction, and moving buffers between engines, are part of this. This will
  be designed with the tokens.
- **The WR of a request's last WQE.** `Refines::wrs` keeps every send WQE's WR from M's history
  (`wr_log`); the rings still keep each request's last one as well, from when it is written
  (`send_tails`), for what a result reports. One record could serve both.
- **The public API.** Connect the futures, sinks and streams of `safeverbs` to the
  verified engine, and state exactly which public operations the theorem covers, including
  reuse after a caught panic. `Engine`'s shell, `Request` above all, could come inside `verus!` when
  Verus reads its callbacks. Keep conditional release distinct from leak freedom.
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
safeverbs-spec: verification results:: 163 verified, 0 errors
safeverbs-core: verification results:: 751 verified, 0 errors
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
