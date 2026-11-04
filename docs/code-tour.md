# Reading the code

Start with an application, follow one request through the engine, then read the handles
over it and the proofs. The large verified modules contain both executable Rust and ghost
code; on a first pass, follow the executable methods and return to their contracts
afterward.

## 1. Start at a shared-CQ application

Read [`examples/event_loop/src/main.rs`](../examples/event_loop/src/main.rs): each of two
threads creates a `LocalCq`, attaches several QPs to it, connects them, and posts receives.
`serve` and `ping_pong` show the steady-state loop: poll the CQ, dispatch each result by its
`wr_id` (a small `Job` enum, the example's own), repost buffers, stage work, and ring each
active QP's doorbell once per turn.

For a smaller asynchronous application, read the
[greeter client](../examples/greeter/src/client.rs) and
[server](../examples/greeter/src/server.rs). Return to the async internals in step 4.

Keep three units distinct:

- A **WQE** is one hardware work item.
- A **request** may own several WQEs, but has one sequence number, `wr_id` and result, and
  holds several leases. A batch completes only after its entire WQE interval completes.
- An **engine** owns one shared CQ and several QPs; `LocalCq` and `AsyncCq` are handles to
  one. Each QP still has its own send and receive rings; sharing the CQ does not merge their
  completion accounting.

Inside the core, a request that has completed is said to *retire* from its ring: that is the
ring's word for the step, and what M calls it too. Public docs say it completes.

## 2. Follow one request through the engine

Read these files in order:

| File | What to follow |
| --- | --- |
| [`local.rs`](../crates/safeverbs/src/local.rs) | `LocalQp::post_send`, `LocalQp::post_recv` and `LocalCq::poll`: thin handles over the engine, which keep no state of their own |
| [`engine.rs`](../crates/safeverbs-core/src/engine.rs) | `Engine::try_post`, `Engine::poll`, `Engine::read_completions`, `Engine::next_completion`, then the general path, `Engine::request`, `Request::push`, `Request::finish`, `Engine::commit` |
| [`hw.rs`](../crates/safeverbs-core/src/hw.rs) | `Hw::post_one`, `Hw::post_recv`, `read_completions`, `next_completion`, `progress`, then `open_request`, `push_other`, `close`, `commit`: per-QP state, hardware calls, CQ routing and ring updates |
| [`hw/open.rs`](../crates/safeverbs-core/src/hw/open.rs) | `OpenRequest`: a request being written, a value of its own, in storage that comes from its QP |
| [`ring.rs`](../crates/safeverbs-core/src/ring.rs) | `fill`, `stage_filled`, `commit`, `retire_oldest`, `retire_next`, `complete_with_wakes`, `take`, `detach`: the request lifecycle independently of FFI |
| [`ring/payload.rs`](../crates/safeverbs-core/src/ring/payload.rs) | what a slot of an engine's rings holds: the buffers and the `wr_id`, one generic value to the ring |
| [`wr_id.rs`](../crates/safeverbs-core/src/wr_id.rs) | how a CQE identifies its QP, queue and WQE number (internal: users have their own `wr_id`s, kept in the slots) |
| [`nic.rs`](../crates/safeverbs-core/src/nic.rs) | the unsafe backend contract the core relies on |
| [`verbs.rs`](../crates/safeverbs-core/src/verbs.rs) | the concrete libibverbs implementation; read after understanding the core |

There are three useful traces:

```text
post_send -> checks, one comparison each -> slot -> WQE -> doorbell -> slot staged and committed
poll -> read the CQEs, update the consumer index -> for each: decode wr_id, retire in the ring, hand out
batch -> open_request -> its WQEs collected, nothing held -> close (signaling now known) -> WQEs -> slot -> doorbell
```

A CQE can retire earlier unsignaled WQEs too, and then hands out those requests first.
`Engine::poll` delivers owned results to its callback, while `progress` leaves them in the rings
for their owners to take: that is what futures do. What `progress` leaves is found by a cold scan
if a poll comes next, so a CQ driven both ways loses nothing, but the handles do not allow it.

## 3. Follow the memory separately

Read [`buf.rs`](../crates/safeverbs-core/src/buf.rs), especially `Region`,
`BufMut` and `Exposed`, then
[`buf/memory.rs`](../crates/safeverbs-core/src/buf/memory.rs) for `RawMut`/`Frozen`
ownership and [`buf/view.rs`](../crates/safeverbs-core/src/buf/view.rs) for frozen `Buf`
views. Continue with
[`buf/lease.rs`](../crates/safeverbs-core/src/buf/lease.rs) for `Part` and `Leases`:

- `Region` owns the allocation and its registration; references keep it alive.
- `BufMut` owns an exclusive byte range. Splitting produces disjoint ranges.
- `Buf` freezes a range and supports overlapping shared slices and repeated sends.
- `Part` supplies one SGE and the resource that keeps its bytes valid.
- `Leases` retains a request's resources until they can be returned or released. It
  can deduplicate consecutive references to the same frozen allocation, so the number
  of retained resources need not equal the number of WQEs or SGEs.

`Leases::view` describes physical keepers. Its ghost `loans` and `wqes` describe every
logical SGE and its operation, even when multiple inputs share one physical Arc. Follow
`post_one` and `close` in `hw.rs`, then `ring_resources_ok` and `take` to see how a request's
metadata travels through the rings with it.

For remote access, `Exposed::try_reclaim` revokes the remote registration before returning
typed access. A refused close returns its owner for retry; a failed final cleanup retains
the allocation and dependency chain. While the access lasts, `Exposed::copy_to_slice` reads the
bytes with volatile reads and forms no reference to them (`buf/memory.rs`, `volatile_copy`).

[`buf/range.rs`](../crates/safeverbs-core/src/buf/range.rs) is the arithmetic these
buffers use for splits and frozen views. `memory.rs` ties that geometry to the allocator's
permissions; `permissions.rs` keeps the per-byte linear tokens, which a plain build erases.

Then read [`wc.rs`](../crates/safeverbs-core/src/wc.rs): `Completed`, `Received` and
`CompletionError` have only operation-defined metadata, derived from the raw `Wc`, which stays
in the core. SEND/WRITE report no byte count; READ and receive cases have different metadata. A
WRITE_WITH_IMM notification is another case than a receive payload written into the posted
receive buffer, and an error names the WQE that failed.

## 4. Add the asynchronous layer

Read [`cq.rs`](../crates/safeverbs/src/cq.rs) in the application crate first: `AsyncCq` shares
one engine through `Rc<RefCell<_>>`, as `LocalCq` does in `local.rs`. Then read
[`qp.rs`](../crates/safeverbs/src/qp.rs),
[`op.rs`](../crates/safeverbs/src/op.rs) and
[`backlog.rs`](../crates/safeverbs/src/backlog.rs).

`Op` tracks a queued ticket or posted sequence number. Polling it polls the shared CQ and takes
its own result, which the operation's `Outcome` turns into its output: `()` for sends and writes,
the buffer for a READ, a `Received` and the buffer for a receive. Dropping queued work releases
it; dropping a posted future detaches its owner, retaining the buffers until the NIC is done. The
future is the request's identity, so the requests carry no `wr_id` here. Finally read `batch.rs`,
`sink.rs` (which maps a failed request back to its item) and `receiver.rs` for amortized operation
APIs.

## 5. Read the proof claim before its mechanics

Start with [verification.md](verification.md), then:

1. [`guarantees.rs`](../crates/safeverbs-spec/src/guarantees.rs): the precise S1, F1,
   R1 and L1 statements; S2 belongs to the implementation's legal calls into M.
2. [`abs.rs`](../crates/safeverbs-spec/src/abs.rs): A, the abstract ownership and
   request machine, including requests that mix unique and shared leases, and
   cancellation.
3. [`nic.rs`](../crates/safeverbs-spec/src/nic.rs): M, the hardware behavior and verb
   preconditions being assumed.
4. [`refinement.rs`](../crates/safeverbs-spec/src/refinement.rs): the `Refines`
   obligations, the coupling of requests to WQE intervals and outcomes, and Theorem 1.
5. [`examples.rs`](../crates/safeverbs-spec/src/examples.rs): concrete checked traces.
6. [`refine.rs`](../crates/safeverbs-core/src/refine.rs): the steps of A behind each change
   of a request, one request at a time, over any view of requests. Then
   [`hw/refines.rs`](../crates/safeverbs-core/src/hw/refines.rs): the view of an engine's rings,
   its ghost state, the invariant that ties them to A (`linked`), how a buffer comes to A
   (`lemma_adopt_loans`, `Hw::adopt`), and at the end `impl Refines`, the theorem, and a run of
   `Engine`'s methods on typed buffers under it (`example_run`).
   Return to `Hw::inv` and [`trusted.rs`](../crates/safeverbs-core/src/trusted.rs)
   for the hardware-facing contracts. `share.rs` provides the hardware side's framing
   rules. [`hw/send_outcomes.rs`](../crates/safeverbs-core/src/hw/send_outcomes.rs) and
   [`hw/outcomes.rs`](../crates/safeverbs-core/src/hw/outcomes.rs) tie requests' results
   to the WQEs M retired.

The executable test backend [`model.rs`](../crates/safeverbs-core/src/model.rs) is not
the formal model M: tests use it to move real bytes and inject failures, and
[`scripts/measure.sh`](../scripts/measure.sh) to count what each public path costs against
code written by hand ([`measure.rs`](../crates/safeverbs/examples/measure.rs)).
