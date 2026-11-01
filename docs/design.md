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
   misuse a verb, mix up completions, leak or prematurely free a buffer, or lose a wakeup.
2. **Zero cost on the data path.** Posting checks only what the hardware needs checked
   (ring space). A thread-local queue pair takes no lock. Unsignaled WRs, batches with one
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
> precondition in M, and A and M stay coupled: each request's outcome is the one M produced
> for its WR, and the NIC touches only memory A gives it.

Users need to read only M, A and this theorem. The properties below are invariants and
lemmas of A, proved once and for all; they hold for the library because it refines A.

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

**F1, completion correspondence.** Each request gets exactly one outcome, the one M produced
for its WR (status, byte length, immediate data), and it is returned together with the
request's own buffers. For an unsignaled WR the outcome is "success": M's in-order rule
implies that a WR retired by a later CQE executed without error, and M proves it
(`lemma_poll`). In A: an outcome, once produced, never changes (`lemma_f1_outcome_fixed`);
taking it returns exactly that outcome and the bytes the request was posted with
(`lemma_f1_take`); ids are never reused, so nothing is taken twice (`lemma_f1_taken_once`).
Against M: the outcome is the one M recorded when it retired the request's own WR, and no
other request shares that WR (`corollary_f1`).

**R1, no leak by the library.** Every buffer handed to the library is returned or dropped
exactly once, never before the NIC is done with it, and at the latest when its queue pair is
dropped. Queue pairs are destroyed only after their WRs are retired, which happens before
their buffers are freed. In A: an in-flight request keeps its lease, unchanged, until its
outcome exists (`lemma_r1_kept_while_in_flight`); every request lives on a queue pair that
exists (`lemma_r1_requests_on_live_qps`); a released lease never comes back
(`lemma_r1_released_once`). Leaks the user causes with `mem::forget` are out of scope, as
they are for the standard library.

**L1, no lost wakeup.** Once a request is complete, any waker its future registered has
been invoked. This is a safety invariant: a complete request holds no waker
(`lemma_l1_no_waker_left`), and the completion step itself invokes the waker it finds
(`lemma_l1_completion_wakes`). Turning it into "every future eventually wakes" needs two
liveness assumptions that are not part of the claim: the executor polls tasks that were
woken, and someone polls the CQ.

**Cancellation is not a separate property.** Dropping a future is just another client
action in A (`detach`). It never cancels the WR: the request stays in its ring slot with its
buffers, and its lease is released when the NIC completes it. A has a worked example
(`example_dropped_receive_in_a`).

### 3.3 What the peer may do

Theorem 1 is single-ended. The peer and the network are fully nondeterministic in M: the
peer may send any bytes, write any exposed memory within the bounds of an rkey it holds at
any time, never post a receive, or crash; packets may be lost. Receiver-not-ready, remote
access errors and retry exhaustion are allowed outcomes; the library reports them as typed
errors and none of them affects S1 to L1 or S2.

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
  the queue pair to the error state, after which every remaining WR completes with a flush
  error in order; that a CQE carries the `wr_id` of its WR; that once a CQE is polled the NIC
  no longer touches the buffers of the WRs it retires; that inline data is copied when it is
  posted; that the NIC never accesses memory outside the scatter/gather lists of posted WRs
  and the regions exposed for remote access; that after `ibv_dereg_mr` returns, the region's
  rkey stops working and the NIC no longer accesses it; and that a CQ with enough reserved
  capacity never overflows.
- **The trusted wrappers' bodies.** Each verb is called through a small wrapper whose
  contract is M (it requires the verb's precondition and ensures the matching M step); its
  body is the FFI call and is not verified.

Evidence for M will come from differential testing on real NICs and on soft-RoCE: random
sequences of verbs, faults (link down, peer crash, forced error state) and peer behaviour,
with every observed completion checked against the transitions M allows.

## 5. The two theorems

**Theorem 1 (now)** is the single-ended theorem above. `refinement.rs` states it as the
interface the core must prove: a trait `Refines` with the core's invariant, its abstraction
function to A, its ghost view of the NIC in M and the map from requests to WRs, and three
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
let mut rx = qp.receiver(bufs);            // impl Stream<Item = (Wc, BufMut)>
while let Some((wc, buf)) = rx.next().await {
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

A future holds only `(qp, seq)`: the queue pair and the sequence number of its slot. The
waker lives in the slot. Futures poll the CQ themselves when they are polled, so there is no
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
step) holds M, A and the theorem's interface; the core is written against it next.

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
state, the remaining WRs complete with `FLUSH_ERR`, in order, signaled or not, and each
releases its slot.

A transport trait carries `IN_ORDER_COMPLETION`. Out-of-order transports (SRD on EFA, UEC's
RUD) use a bitmap ring, in which a slot is freed when its own completion arrives and the
ring's head advances past completed slots; they get no unsignaled API, since nothing would
ever retire an unsignaled WR. A shared receive queue needs a free list instead of a ring.
M today models the in-order transports (RC, UC, UD); out-of-order completion is a planned
extension of M. A does not change: it never mentions order.

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
`post_request` and `expose`.

## 10. Queue pairs and completion queues

`LocalQp` is `Send + !Sync`. It is created on a thread domain (`ibv_alloc_td`) through a
parent domain (`ibv_alloc_parent_domain`), on which mlx5 skips the queue pair's spinlocks
and gives the queue pair its own doorbell page (UAR); where that is not supported, it falls
back to a normal queue pair. `SharedQp` is a `Mutex<LocalQp>`: one lock covers both our ring
and the hardware queue, so the two can never disagree.

CQs are per thread, created with `IBV_CREATE_CQ_ATTR_SINGLE_THREADED` and polled with the
`ibv_start_poll` family. Attaching a queue pair to a CQ reserves capacity for every
completion it may produce (its queue depths), and creation fails when the CQ would be
over-committed; M's `create_qp` precondition is exactly this rule, and M proves that no CQ
can then overflow. Device capabilities (queue depths, scatter/gather elements, inline size,
CQ size) are checked when objects are created, not on every post.

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
| everything `Send + Sync` | `LocalQp` and per-thread CQs, `SharedQp` when sharing is needed |
| `unsafe` spread through the crate | all `unsafe` in `safeverbs-core`, the rest `forbid(unsafe_code)` |
| no statement of the guarantee | Theorem 1 over M and A, checked by Verus |

The typestate builder for connection setup, the reference-counted resource handles and the
async style of the API carry over.

## 12. Open questions

- Which thread owns a CQ when a `LocalQp` moves between threads: a `LocalQp` is `Send`, but
  its CQ is single-threaded. One answer is a group that owns a CQ and the queue pairs
  attached to it and moves as a unit.
- Out-of-order transports and SRQs need their own completion rules in M before the core
  supports them.
- How much of the ring's state the trusted wrappers expose: the less they know, the smaller
  the trusted base.
- What the model implementation of the NIC trait looks like, so that the same tests run
  against it and against hardware.
