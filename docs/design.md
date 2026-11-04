# SafeVerbs: design of the verified rewrite

This document describes the design of SafeVerbs after its rewrite: what the library
guarantees, what that guarantee rests on, and the API, completion model and buffer types
that make it possible without giving up speed. It is written for someone who knows RDMA
verbs and Rust but has not seen this project before. The companion document
[`verification.md`](verification.md) says exactly what is proved today, what is only
stated, and how to check it; the formal model itself lives in
[`crates/safeverbs-spec`](../crates/safeverbs-spec).

## 1. Why a rewrite

RDMA verbs give a program direct access to the network card. A program registers memory,
posts work requests (WRs) that name that memory, and later polls a completion queue (CQ)
to learn that the NIC is done. Between posting and completion the NIC reads or writes the
memory by DMA, behind the compiler's back. A peer that holds a region's rkey can write into
it at any time. None of this is visible to Rust's type system, so every C or Rust binding of
libibverbs either marks the data path `unsafe` or protects it with runtime bookkeeping.

The first version of SafeVerbs chose bookkeeping. Queue pairs used the typestate pattern for
connection setup. A `MemoryRegion<T>` handed out `MemorySegment<T, RO>` and
`MemorySegment<T, RW>` views whose ranges were tracked in an interval tree behind a mutex, so
that a writable segment could never overlap another segment. Every posted request became a
future backed by an entry in a `sharded_slab`, which the CQ poller filled in and woke.

That design made the common path safe, but it left gaps and costs:

- **Dropping a future freed memory the NIC was still using.** A request future owned the
  segments it was posted with. Dropping it before completion released them, and with the
  last segment the region itself was deregistered and freed while the WR was still in
  flight. Its slab entry was never removed.
- **Unsignaled and batched posting were `unsafe`.** Performance on small messages depends on
  posting many WRs per doorbell and asking for a completion only for the last one. The old
  API offered this only as `unsafe` functions whose safety contract asked the user to keep
  everything alive "until the operation finishes", which the user cannot observe for an
  unsignaled WR.
- **Flushes were not accounted for.** When a queue pair enters the error state, the NIC
  completes every outstanding WR, unsignaled ones included, with the `wr_id` the program
  gave it. The old poller looked every `wr_id` up in the slab as a slab key, including the
  user-chosen ids of unsignaled WRs.
- **Remote access raced with the program.** `MemorySegment<T, RW>::remote_memory()` handed
  the peer the address and rkey of the whole region while the program kept a writable view
  of it, and the rkey covered every other segment of the region too. A peer's RDMA write
  could land in memory the program was reading or writing.
- **Capacity was unchecked.** Nothing related the size of a CQ to the depths of the queue
  pairs attached to it; the bandwidth example creates a 128-entry CQ for a queue pair with
  up to 128 send and 128 receive WRs, and in the error state flushes can produce more CQEs
  than fit.
- **Bookkeeping on the data path.** Creating or dropping a segment took a lock and an
  interval-tree operation; every request took a slab insertion and removal; the CQ, the
  slab and the queue pairs were `Sync` and paid for it on every call.
- **No statement of the guarantee.** "Memory safe" was a goal, not a theorem.

The rewrite keeps the goal (safe, fast, async RDMA in Rust) and changes the method: first
write down precisely what is guaranteed and under which assumptions, then build the
library so that a verifier can check the guarantee.

## 2. Goals

1. **Safety by construction, stated as one theorem.** Under an explicit model of the NIC,
   no sequence of safe calls, and nothing the peer does, can cause undefined behaviour,
   misuse a verb, mix up completions, lose track of or prematurely free a buffer, or lose a
   wakeup.
2. **Zero cost on the data path.** Posting checks only what the hardware needs checked
   (ring space). A thread's worker (its CQ and queue pairs) takes no lock. Unsignaled WRs, batches with one
   doorbell and one completion, and repeated sends of the same buffer are all safe API.
3. **One verifier.** The whole claim is checked by Verus. No part of the guarantee depends
   on Kani, loom, Miri or testing; tests and differential runs on hardware gather evidence
   for the model, which is an assumption and says so.
4. **A small trusted core.** All `unsafe` code lives in one small crate, written inside
   Verus, with the proofs next to the code. Everything else is `#![forbid(unsafe_code)]`.

## 3. The guarantee

### 3.1 One refinement theorem

The guarantee is a single refinement theorem between two state machines, both written in
Verus in `crates/safeverbs-spec`:

- **M** (`nic.rs`) is the model of the RDMA NIC, its driver and the network, as one endpoint
  sees them. It is trusted: it is what we assume the hardware does.
- **A** (`abs.rs`) is the abstract SafeVerbs machine: what the library promises its users.
  It is small enough to read in a few minutes and knows nothing about queues, doorbells or
  CQEs.

> **Theorem 1.** Under M, for any behaviour of the peer and the network, every execution of
> SafeVerbs corresponds to an execution of A, every verb the library issues satisfies its
> precondition in M, and A and M stay coupled: each request owns a disjoint interval of
> WQEs, its outcome aggregates that interval's retired outcomes, and the NIC touches only
> memory A gives it.

Users need to read only M, A and this theorem. The properties below are invariants and
lemmas of A and of its coupling to M, proved once and for all; they hold for the library
because it refines A.

### 3.2 The properties

**S1, no undefined behaviour.** At every moment, no byte is touched by the program and by
the NIC (or the peer) in conflicting ways: memory the program may write is neither read nor
written by the NIC, and memory the program may read is not written by the NIC. The NIC's
reach is everything named by an in-flight WR (its scatter/gather list) and every region
exposed to the peer by rkey; the program's reach is every `BufMut` (read and write) and every
`Buf` view (read). Bytes the NIC or the peer may write are always held uniquely and have an
element type that accepts every bit pattern, so received bytes become a `T` only through
`zerocopy::FromBytes`, or through `TryFromBytes` with a validity check. In A this is the
invariant that a unique lease overlaps no other lease (`inv_unique`) together with
`lemma_s1_no_conflict` and `lemma_s1_nic_writes_any_bytes`.

S1 has one exception, and it is deliberate: `Exposed::copy_to_slice` reads memory the peer may
be writing. No reference to exposed memory is ever formed; it is read only through this copy,
with volatile reads into a `T` that accepts any bytes. That such reads are defined and yield
arbitrary bytes is a trusted assumption (`docs/verification.md`, "Trusted computing base"). The
copy may be torn, so a protocol learns that data is complete from a write-with-immediate
notification or a SEND, never by polling the bytes: on MRC, data lands out of order.

**S2, no local misuse.** Every verbs call the library makes satisfies its precondition in
M: there is space in the send or receive queue; the CQ has capacity reserved for every
completion it may receive; the queue pair is in a state that allows the call; the number of
scatter/gather elements, the message length and the inline size are within the device's
and the queue pair's capabilities; every lkey names a registered region of the queue pair's
protection domain that covers the element, and allows local write when the NIC writes it.
The preconditions are written against what the library can know: the NIC may move a
connected queue pair to the error state at any moment without telling anyone, so a post is
allowed in `Rts` *or* `Err`. M also proves that no step the NIC takes on its own invalidates
a precondition the library checked earlier (`lemma_nic_steps_keep_verbs_enabled`).

**F1, what a request's outcome means.** A request owns an interval of WQEs on one queue: one
WQE, or several for a batch. It gets an outcome only once all of them retired, and the outcome
says this:

- **Success** means every WQE of the request succeeded: for READs, every destination got its
  full requested length. This follows from M: a polled completion retires every earlier WQE of
  its queue, and those were unsignaled successes (`lemma_poll`). The typed result is the last
  WQE's: a batch of READs reports the length of its last READ, and SEND and WRITE successes
  report their status only.
- **An error** names the failing WQE's position in the request. The WQEs before that position
  completed successfully. That WQE and those after it did not complete successfully, and their
  remote effects are unknown: a later WRITE may land at the peer before an earlier READ fails,
  and a lost acknowledgement can hide a write that landed. This follows from M too: a queue
  pair stays in the error state while its queue holds a WQE, and in that state every WQE fails
  (`lemma_request_step`).
- **A refusal** says the NIC never saw the request: M holds no WQE of it, and the program kept
  its leases throughout.
- **At most one outcome** per request, handed back with its own leases and context.

A proper prefix of a batch may retire, even with an error, while the request stays in flight
with its leases. In A: an outcome, once produced, never changes, and a request taken never comes
back (`lemma_f1_one_outcome`); taking it returns exactly that outcome, and each lease under its
own name (`lemma_f1_take`). Against M, in a state coupled to A, the outcome means what is said
above of the request's own interval, of which no other request owns a WQE (`corollary_f1`), and
a refusal is no WQE and no step of A (`corollary_f1_refused`).

**R1, the library never loses track of a buffer.** At every moment each lease is in exactly one
place: with the program, with the peer by rkey, in flight in a request (which includes being kept
for a device that has not flushed: `NotFlushed` frees nothing), in a completed request not yet
taken, or released. It moves only along the allowed transitions: from the program into a request
or to the peer, or released by the program; from the peer back to the program; from in flight to
completed, or released at once if the request has no owner left; from completed back to the
program, or released. It is released once, and only after the NIC is done: a lease leaves a
request in flight only when the NIC produced that request's outcome, for which all its WQEs must
have retired. In A this is the invariant `inv_places` with `lemma_r1`; against M, `corollary_r1`.
Leaks the user causes with `mem::forget` are out of scope, as they are for the standard library.

**L1, no lost wakeup.** Once a request is complete, any waker its future registered has
been invoked. This is a safety invariant: a complete request holds no waker
(`lemma_l1_no_waker_left`), and the completion step itself invokes the waker it finds
(`lemma_l1_completion_wakes`). Turning it into "every future eventually wakes" needs two
liveness assumptions that are not part of the claim: the executor polls tasks that were
woken, and someone polls the CQ.

**Cancellation is not a separate property.** Dropping a future is just another client
action in A (`detach`). It never cancels the WR: the request stays in its ring slot with its
buffers, and its leases are released when the NIC completes it. A has a worked example
(`example_dropped_receive_in_a`).

### 3.3 What the peer may do

Theorem 1 is single-ended. The peer and the network are fully nondeterministic in M: the
peer may send any bytes, write any exposed memory within the bounds of an rkey it holds at
any time, never post a receive, or crash; packets may be lost. Receiver-not-ready, remote
access errors and retry exhaustion are allowed outcomes; the library reports them as typed
errors and none of them affects S1 to L1 or S2.

### 3.4 Liveness, under one premise

F1 and R1 above are safety: they say nothing wrong happens, not that anything happens. Two
statements complete them, with one premise between them: *the device retires every posted WQE*.
That is, it executes or flushes each one, and the completions that retire them reach a poll.

- **F1-live.** If the device retires every posted WQE, every request gets exactly one outcome.
- **R1-live.** If the device retires every posted WQE, nothing stays in flight after its queue
  pair is dropped, and every buffer is returned or released.

Both are stated here and **not proved yet**. Without the premise neither is claimed: a device
that never flushes keeps its requests in flight for good, and the worker keeps their queue pair
and buffers (`Error::NotFlushed`). That is R1 at work: it frees nothing the NIC may still use.

## 4. What is assumed

The trusted computing base is:

- **rustc and the standard library**, as for any Rust program, including the specifications
  Verus assumes for the parts of `core`, `alloc` and `std` the core uses.
- **Verus and Z3**: if the verifier is unsound, the proofs mean nothing.
- **rdma-core and the provider driver**, and the kernel driver below them: the trusted verbs
  wrappers call them, and their behaviour is what M describes.
- **The NIC conforms to M.** This is the main assumption, and it is explicit: M is a
  readable state machine, not a comment. In particular we assume that send queues of RC,
  UC, UD (and MRC) and all receive queues complete in posting order; that an unsignaled WR
  that succeeds produces no CQE; that a failed WR produces a CQE even if unsignaled and moves
  the queue pair to the error state, after which every remaining WR completes with an error
  (normally a flush error), in order; that a CQE carries the `wr_id` of its WR; that once a
  CQE is polled the NIC no longer touches the buffers of the WRs it retires; that inline data
  is copied when it is posted; that the NIC never accesses memory outside the scatter/gather
  lists of posted WRs and the regions exposed for remote access; that after `ibv_dereg_mr`
  returns, the region's rkey stops working and the NIC no longer accesses it; and that a CQ
  with enough reserved capacity never overflows. About what completions report, we assume
  that a successful READ reports exactly the length it asked for, a successful SEND or
  WRITE its status only, and a successful receive either a payload within its own
  scatter/gather list or the notification of a peer's WRITE with immediate.
- **The trusted wrappers' bodies.** Each verb is called through a small wrapper whose
  contract is M (it requires the verb's precondition and ensures the matching M step); its
  body is the FFI call and is not verified.

Evidence for M will come from differential testing on real NICs and on soft-RoCE: random
sequences of verbs, faults (link down, peer crash, forced error state) and peer behaviour,
with every observed completion checked against the transitions M allows.

## 5. The two theorems

**Theorem 1 (now)** is the single-ended theorem above. `refinement.rs` states it as the
interface the core must prove: a trait `Refines` with the core's invariant, its abstraction
function to A, its ghost view of the NIC in M, the map from requests to disjoint WQE intervals and the WR of each send WQE, and three
obligations (start states, every API call is zero or more steps of A and of M, the invariant
couples A and M). `theorem_1` proves, for every implementation meeting the obligations, that
along any execution A's and M's invariants hold, every verb met its precondition, and A and
M stay coupled.

**Theorem 2 (later; stated only)** covers two endpoints that both run SafeVerbs' protocol
layer: credit-based flow control for sends and rkey leases for one-sided access. Each side
refines A under M with only the network left nondeterministic, and by assume-guarantee
reasoning (each side assumes the other honours the credits and leases it granted, and
guarantees the same) no completion is receiver-not-ready or a remote access error; every
error completion is attributable to a fault step of M (link down, peer crash, timeout).

## 6. The API

The public API is asynchronous, built on a synchronous core that does the posting and
polling. A sketch:

```rust
let mut buf = pd.alloc::<u8>(1 << 20)?;   // BufMut: unique and writable
buf.fill(42);
let buf = buf.freeze();                    // Buf: shared and read-only
qp.send(buf.slice(..)).await?;
qp.send(buf.slice(4096..8192)).await?;
qp.batch(|b| {                             // one doorbell, one CQE
    for i in 0..128 {
        b.write(buf.slice(..64), dst.offset(i * 64));
    }
})
.await?;
let mut tx = qp.sink();                    // impl Sink<WriteOp>
tx.send_all(&mut ops).await?;
let mut rx = qp.receiver(bufs);            // a stream of (Result<Received, _>, BufMut)
while let Some((received, buf)) = rx.next().await {
    rx.repost(buf)?;
}
let buf = buf.try_thaw()?;                 // BufMut again once nothing else holds it
```

`send` and `write` take a `Buf` (or a slice of one); posting moves a lease on the buffer
into the request's ring slot. Since the buffer is shared and read-only, sending the same
buffer twice, or any sub-ranges of it, needs no check at all. `batch` posts many WRs with
one doorbell and signals only the last; its future completes when that CQE arrives, which
retires the whole batch. `sink` turns a queue pair into a `futures::Sink<WriteOp>`:
`poll_ready` waits for ring space, `start_send` writes a WQE, `poll_flush` rings the
doorbell and `poll_close` waits until everything completed. `receiver` turns a set of
buffers into a stream of received messages, each with its buffer, which the user may repost.

Results have only the fields their operation defines. Sends, writes, batches and the sink
resolve to `Ok(())`; a READ gives its buffer back; a receive says `Received::Message { len, imm }` (a SEND, whose `len` bytes are in the buffer) or
`Received::WriteNotification { imm, len }` (a peer's WRITE with immediate, whose `len` bytes went
to exposed memory, the buffer untouched). What is undefined in C's work completion, such as the
byte count of a SEND or the opcode of a failed completion, does not exist in these types.

An error keeps handing buffers back and says what happened. `CompletionError::Failed` has the
`status`, the `vendor_err` and the `position` of the failing WQE within its request. The WQEs
before that position completed successfully. That WQE and those after it did not complete
successfully, and their remote effects are unknown: a later WRITE may land at the peer before
an earlier READ fails, and a lost acknowledgement can hide a WRITE that landed.
`CompletionError::Refused` is another case: the doorbell was refused, the NIC never saw the
request, and nothing of it happened anywhere. A sink's error also says which item failed, its
position among the items sent through that sink (`Error::SinkItem`).

The event loop of the synchronous core has no future to stand for a request, so each request
carries a value of the user's own type, which replaces C's `wr_id`:

```rust
let mut w: Worker<Job> = Worker::new(&pd, 4096)?;
w.send(qp, buf.slice(..), Job::Reply { client: 7 })?;
w.poll(|w, c| match (c.ctx, c.result) {
    (Job::Reply { client }, Ok(_)) => done(client),
    (job, Err(e)) => retry(job, e),
});
```

`Worker<C = (), N = Verbs>` keeps the context in the request's ring slot, as part of the slot's
generic payload, so that "returned exactly once, with its own buffers" is what the ring already
proves of every payload. C-style users pick `u64`. `C = ()` costs no work, only the word an
`Option<()>` takes in the slot. A batch is one request with one context (`Request::submit(signal, ctx)`, the context
given last, as in `Worker::send`). `c.result` is a `Result<Completed, CompletionError>`, with
`Completed::Sent`, `Completed::Read { len }` and `Completed::Received(_)`. The hardware `wr_id`
stays internal: it routes completions (section 10). The asynchronous layer needs no context,
since each future is its own request's identity: `LocalWorker` is `C = ()`.

A future holds only `(qp, seq)`: the queue pair and the sequence number of its slot. The
waker lives in the slot. Futures poll their worker's CQ themselves when they are polled (section 10), so there is no
background task and no thread; an interrupt-driven mode through the completion channel can
come later. Dropping a future detaches it (see 3.2): the slot keeps the buffers until the
NIC completes the WR, and the completion is then discarded.

## 7. The core boundary

All `unsafe` code lives in one small crate, `safeverbs-core`: the FFI to rdma-core, the ring
of slots, buffer ownership and memory registration. Every other crate, the async API and the
protocol layer included, is `#![forbid(unsafe_code)]`.

The NIC sits behind a trait with two implementations: the real one, calling ibverbs, and a
model one, which executes M and serves for testing. The core is generic over it and
monomorphized, so the abstraction costs nothing at run time.

The ring and the ownership logic are written inside `verus!`, so the proofs live with the
code they are about. Ghost state is erased by a normal build: a plain `cargo build` produces
the same machine code it would without Verus. The model came first: `safeverbs-spec` (this
holds M, A and the theorem's interface; the core is written against it.

## 8. Completion model and transports

Each queue pair has a ring of slots, one per outstanding WR, indexed by sequence number
modulo the ring size. A slot holds the request's buffer leases, its waker and, once known,
its outcome. This replaces the global slab.

Send queues of RC, UC, XRC (the send side) and MRC complete in posting order, and so do
receive queues without an SRQ. MRC places data out of order at the responder, but its
completion semantics are unchanged; so arrival must be signalled by a write with immediate
or a send, never by polling memory. On an in-order queue, a signaled completion for slot
`j` retires every earlier slot: those WRs executed without a CQE, which M shows means they
were unsignaled and succeeded. Unsignaled WRs and batches are therefore safe: their buffers
stay in their slots until a later CQE retires them. When the queue pair enters the error
state, the remaining WRs complete with an error (normally `FLUSH_ERR`), in order, signaled
or not, and each releases its slot.

A transport trait carries `IN_ORDER_COMPLETION`. Out-of-order transports (SRD on EFA, UEC's
RUD) use a bitmap ring, in which a slot is freed when its own completion arrives and the
ring's head advances past completed slots; they get no unsignaled API, since nothing would
ever retire an unsignaled WR. A shared receive queue needs a free list instead of a ring.
M today models the in-order transports (RC, UC, UD); out-of-order completion is a planned
extension of M. A does not change: it never mentions order.

### Operation-specific contracts

The runtime and M distinguish SEND, SEND_WITH_IMM, WRITE, WRITE_WITH_IMM and READ.
They share queue accounting but differ in memory access and completion semantics:

| Operation | Local NIC access | Peer action | Successful local completion |
| --- | --- | --- | --- |
| SEND | Reads source | Consumes receive WQE; payload goes to its SGEs | Status; no reported length or immediate |
| SEND_WITH_IMM | Reads source | As SEND; peer receives immediate | Status; immediate belongs to the peer |
| WRITE | Reads source | Writes rkey-authorized region; no receive WQE consumed | Status; no reported length |
| WRITE_WITH_IMM | Reads source | Writes rkey region and consumes receive WQE for notification | Status; peer gets write notification and immediate |
| READ | Writes destination | Reads rkey-authorized region; no receive WQE consumed | Read transfer length; no immediate |

A batch is one request of several WQEs. It reports the outcome of the first of them that
failed or, if none did, the success of its last WQE: a batch of READs reports the length
of its last READ, not a sum. An error completion's opcode, length and immediate are
undefined, so an error reports its status, with the vendor's code and the position of that
WQE in the request, which the library knows from the WQE's number.

A WRITE_WITH_IMM notification length describes the remote write. It is independent of
receive SGE capacity and never a length to use when slicing the returned receive buffer,
which is why it is another case of the typed result: `Received::WriteNotification { imm, len }`
next to `Received::Message { len, imm }`. A zero-length message is `Message { len: 0, .. }`.
The raw `Wc` is the only place undefined fields exist; it stays inside the core, and what
leaves is derived from its verified `outcome()`.

M's `Outcome` contains a `CompletionKind`. Status-only outcomes canonically carry zero
length and no immediate; zero there is bookkeeping, not a hardware-reported byte count.
`lemma_ack_outcome_unique` proves that a successful SEND/WRITE inferred from a later CQE
is exactly the same observable outcome as its own successful CQE. READ retains its
transfer semantics. So F1 never compares a send length the hardware leaves undefined, and
assumes nothing about those fields.

Local completion permits local source reuse; it does not promise that a remote
application has processed the data. UC success also does not promise remote delivery.
Atomics remain outside M's functional semantics; their current local-buffer abstraction
as READ must not be presented as a proof of atomicity or remote ordering.

Sources: rdma-core's `ibv_poll_cq(3)` documents error-field validity; the
[NVIDIA RDMA programming guide](https://networking-docs.nvidia.com/doca/archive/3-5-0/rdma-aware-networks-programming-guide)
describes the operations and WRITE_WITH_IMM notifications. Provider CQE values are
projected onto their defined observable fields before entering M or A.

## 9. Buffers

There is no interval tree. Buffers are owned values whose types say who may do what:

- `BufMut<T>` is unique and writable. It can be split with `split_at` and `chunks` into
  disjoint parts that are again `BufMut`s.
- `freeze()` turns it into a `Buf<T>`: shared, read-only, cheaply cloned and sliced
  (`buf.slice(a..b)`), like `bytes::Bytes`. Any number of views may be sent at once.
- `try_thaw()` turns a `Buf<T>` back into a `BufMut<T>` when no other view and no in-flight
  request holds it (like `Arc::try_unwrap`), and otherwise returns `Err(buf)`.
- `RingBuf` streams variable-size messages through one registered area.

Posting moves a lease clone into the ring slot, one clone per batch when the same buffer is
posted repeatedly. A buffer is registered when it is allocated (`pd.alloc`) and belongs to
one protection domain; posting it to a queue pair of another domain is ruled out by a type
brand or a cheap runtime id check. The NIC may write only into a `BufMut<T>` with
`T: FromBytes`; a buffer exposed for remote write is moved into the exposure (it is no longer
the program's until the rkey is revoked) and is registered on its own, so that the rkey
reaches exactly that buffer and nothing next to it. These are exactly the rules of A's
`post_request` and `expose`. While it is exposed, `Exposed::copy_to_slice` reads it with
volatile reads into a `[T]` with `T: FromBytes + IntoBytes`, with the rkey still valid after: S1's
one exception (section 3.2). A protocol that lets a peer write there learns that the data is
complete from a write-with-immediate notification or a SEND, then copies.

In A, a request holds several leases, each under its own name, with its own byte range,
permissions and direction: a batch may hold exclusive READ destinations and shared SEND/WRITE
sources without making those sources exclusive. The request has one outcome and one waker.
Posting takes the leases from the program and taking the outcome gives each one back. Mapping
the core's requests onto A must account for the leases it retains, which deduplicate
shared references, rather than assume one lease per WQE.

The executable range operations in `buf/range.rs` are verified separately from the
ownership permissions: they bound the unique ranges and frozen views and preserve the
exact byte partition when splitting. Capacity is ghost state, so each range still takes
two machine words; frozen views still store their element count directly, so `len()`
does not add a division. A single SGE has a native 32-bit byte length; `into_part` panics for
a larger range instead of truncating it. Large registered buffers remain usable through
splits and slices.

## 10. Workers, queue pairs and completion queues

The efficient way to use RDMA is one CQ per thread, shared by all of that thread's queue
pairs, and a loop that polls it, dispatches each completion and posts new work. SafeVerbs
offers that directly: a thread drives its queue pairs through a **worker**, one completion
queue and the queue pairs attached to it, the unit of thread affinity.

In the synchronous core (`safeverbs-core::worker::Worker`) a worker is one value, `Send` but
not `Sync`: it moves between threads as a whole, with its queue pairs, and is used by one
thread at a time. Its CQ is created with `IBV_CREATE_CQ_ATTR_SINGLE_THREADED` and polled with
the `ibv_start_poll` family. Where the device has thread domains, the worker allocates one
(`ibv_alloc_td`) with a parent domain over its protection domain
(`ibv_alloc_parent_domain`) and creates every queue pair there: mlx5 then takes no queue pair
locks and gives the worker's queue pairs a doorbell page of their own, which they share.
Where that fails (rxe) they are plain queue pairs. A worker belongs to one protection domain.

**Verified hardware side.** Everything of a worker that M sees (the CQ, the queue pairs'
hardware handles and rings, the WQE a request holds back with its buffers, the worker's
share of M) lives in `hw::Hw`, inside `verus!`; the `Worker` is an unverified shell around it
(keys, the event loop's cursors, the drain's clock). `verification.md` says what is proved.

**Capacity.** Each queue pair reserves a CQ entry for every completion it may produce, its
send and receive depths, when it is created, and one that does not fit in what is left is
refused (`Error::CqFull`). The rings never let more WQEs be in flight than the queues hold,
and each WQE produces at most one completion, so the CQ cannot overrun. This is the rule of
M's `create_qp`, under which M proves that no CQ overflows.

**Removal.** M's `destroy_qp` allows destroying a queue pair only with empty queues: until a
WQE is retired the NIC may still use its buffers. Removing a queue pair (`remove_qp`, and
dropping the worker, for all of them at once) therefore drains it first: the WQEs written
since the last doorbell are discarded, every request is detached, and if anything is in
flight the queue pair moves to the error state, where the NIC completes every WQE left with
a flush error. That does not reach an unsignaled WQE that succeeded before the error: it has
no completion of its own and only a later one of its queue retires it. So the worker also
posts a *drain WQE*, a signaled SEND of nothing, which the error state flushes after all the
others (the kernel's `ib_drain_sq` does the same); it goes through the ring as a request
without an owner, so its number follows the others. The CQ is polled, the other queue pairs'
completions going to their rings, until neither ring has anything in flight. Only then is the
queue pair destroyed and its buffers released; no completion of it is left in the CQ, so
another queue pair may take its index and its reservation at once. A device that never
flushes is a fatal error that never frees memory the NIC may use: after `DRAIN_TIMEOUT` (one
second) without a completion, `remove_qp` fails with `Error::NotFlushed` and keeps the queue
pair, its buffers, its index and its reservation; dropping the worker tries once more and
otherwise leaks them, with the CQ and the thread domain (destroying those needs the queue pair
gone). The cost is waiting for the NIC's flush when a queue pair or a worker is dropped. The
contexts of requests not delivered yet are dropped when the removal gives those requests up,
before any poll; their buffers follow once the NIC is done. A slot without an owner holds no
context, so a poll never runs a destructor of the user's and none can unwind through it with
completions still to record.

**State.** M accepts work only on a queue pair in RTS or ERR, and a connection starts from
RESET. The worker records what it knows: a queue pair takes work once `connect` succeeded or
`to_error` moved it to the error state, and connects once, from RESET. A doorbell on a queue
pair that takes no work is refused as a device would refuse it (`Error::NotConnected`: its
requests retire with a flush error), and so is a receive.

**Routing.** One CQ serves all the queue pairs, so each completion must find its queue pair
and queue. The `wr_id` the library gives a WQE holds the queue pair's index in the worker (16
bits), the queue (1 bit: a failed completion's opcode is undefined, so the queue must be in
the `wr_id` in any case) and the low 47 bits of the WQE's number in its ring; the full number
is recovered from the oldest WQE in flight, since the WQEs in flight span at most the
queue's depth. Routing is a shift, a mask and an array index, the same at any number of queue
pairs. Finding the queue pair by its `qp_num` instead costs as much with a few queue pairs
but grows with them when scanning (about 2 ns at 16 queue pairs, 11 ns at 64 and 35 ns at
256, against 1.2 ns at any number for the `wr_id`, on the development machine), or takes a
hash lookup (about 6 ns). The test `worker::tests::demux_cost` measures routing alone (`cargo
test --release -p safeverbs-core -- --ignored --nocapture demux_cost`); next to what the
model NIC or the driver spends per completion it is small either way, and the `wr_id` keeps
it constant.

**Two ways to drive a worker.** The event loop (`Worker::poll`) polls the CQ and hands every
request that retired with an owner to a callback, with its context, its result and its buffers,
and the queue pair's index, the queue and the sequence number, in each queue's order; the
callback may post new work. Nothing is allocated. Owners that take their results by sequence number
(`Worker::progress`, then `take_send` or `take_recv`) are how the futures work: polling
retires requests into their rings and wakes their wakers. One worker is driven one way or
the other, since `poll` hands out the results owners would wait for. Giving up a request
(`detach_send`, `detach_recv`) hands its context back and leaves its buffers until it retires.

**Futures.** The asynchronous layer puts a worker behind an `Rc<RefCell<..>>`
(`LocalWorker`): one borrow flag, no atomics. Each `LocalQp` holds a handle to its worker;
every pending future polls the worker's CQ once, which retires the requests of all its queue
pairs and wakes their futures, and then takes its own result if it retired. A `LocalQp` is
therefore neither `Send` nor `Sync`: it lives on its worker's thread, as thread-per-core
runtimes want (tokio's `current_thread` runtime or `LocalSet`, or a plain `block_on`).
`QueuePairBuilder::build_on` creates a queue pair on a given worker; `build` gives it a worker
of its own, sized to its queues, which is the standalone queue pair of the first version.
`SharedQp` is such a worker of one queue pair behind a `Mutex`: one lock covers the ring, the
hardware queue and the CQ, so they can never disagree. The trade-off is that every thread
using it takes the lock, and its CQ serves that one queue pair: threads that each drive queue
pairs of their own do better with a worker each.

**Refused doorbells.** When the device refuses a doorbell (`ibv_wr_complete` fails), none of
the requests written since the last doorbell was posted. A request with an owner retires at
once as `CompletionError::Refused`, never a made-up flush error, and its owner takes that with
its context and buffers (the event loop gets it at once too); one without an owner releases its
buffers and is reported as its queue's detached error. Sequence numbers are never reused, so no
owner can take another request's result. The
ring keeps the refused requests in its window with no WQEs, and their WQE numbers, which the
NIC never saw, go to the next requests. A refused doorbell may take with it the signaled
request that would have retired the unsignaled ones before it; those retire with the next
signaled request of their queue.

Device capabilities (queue depths, scatter/gather elements, inline size, CQ size) are checked
when objects are created, not on every post. What a WQE asks of its queue pair is checked
against the hardware queue pair as well (READ and atomics only on RC, atomics only where the
device has them): the synchronous API is safe, and a queue pair key could name another queue
pair than the one its type was made for.

Connection setup keeps the typestate and builder pattern of the first version. Once a queue
pair is ready to send, its run-time state is "ready, or in error and not yet noticed", as M
says.

## 11. What changes from the old code

| Old | New |
| --- | --- |
| `MemoryRegion<T>` with `MemorySegment<T, RO \| RW>`, checked by an interval tree behind a mutex | `BufMut<T>` and `Buf<T>`: ownership and sharing in the types, no runtime tree |
| one `sharded_slab` entry per request, filled by the CQ poller | a ring of slots per queue pair; futures hold `(qp, seq)` |
| dropping a future frees its buffers while in flight | dropping detaches; buffers stay in the slot until completion |
| unsignaled and batched posting `unsafe` | safe, through in-order retirement |
| flushed completions looked up as slab keys | flushes complete their slots in order |
| `remote_memory()` exposes a whole region while the program writes it | exposure takes the buffer away from the program and registers exactly it |
| CQ size unrelated to queue depths | attachment reserves CQ capacity |
| everything `Send + Sync`, a CQ per queue pair | a worker per thread: one CQ shared by its queue pairs, no locks; `SharedQp` when sharing is needed |
| `unsafe` spread through the crate | all `unsafe` in `safeverbs-core`, the rest `forbid(unsafe_code)` |
| no statement of the guarantee | a conditional Theorem 1 over M and A; the real implementation's refinement is still being completed |
| a user-chosen `wr_id` looked up in a slab | a typed context in each request's slot, handed back once; the `wr_id` is the library's own |
| `Wc` fields read by hand, some undefined | typed results with only what each operation defines; an error names the failing WQE |

The typestate builder for connection setup, the reference-counted resource handles and the
async style of the API carry over.

## 12. Open questions

- Which thread owns a CQ shared by several queue pairs is answered by the worker (section
  10): a group that owns the CQ and the queue pairs attached to it, reserves each queue pair's
  depths on attach, routes completions by `wr_id` and moves between threads as one unit.
- Theorem 1 for the worker: each worker's share of M is settled by rely and guarantee
  (`safeverbs-spec/src/share.rs`), and M's side of the refinement is proved for the worker's
  hardware side (`safeverbs-core/src/hw.rs`). Open: A's side for the worker (the map from
  its requests and buffers to A's requests and leases), the rest of the buffers (the typed wrappers, `RingBuf`,
  exposure) and how several workers compose. Undefined SEND/WRITE lengths are already projected
  out of outcomes; READ and receive metadata have operation-specific contracts (section 8).
- Whether a request should say who takes its result (the event loop or its owner), so that
  futures and the event loop can share a worker.
- Out-of-order transports and SRQs need their own completion rules in M before the core
  supports them.
- The simulated NIC runs the integration scenarios and injected-error cases, and checks some
  of M's rules at run time; that it behaves within M is neither tested systematically nor
  proved.
