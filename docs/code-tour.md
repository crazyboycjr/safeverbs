# Reading the code

Start with an application, follow one request through the synchronous core, then read
the asynchronous wrapper and the proofs. The large verified modules contain both
executable Rust and ghost code; on a first pass, follow the executable methods and
return to their contracts afterward.

## 1. Start at a shared-CQ application

Read [`examples/event_loop/src/main.rs`](../examples/event_loop/src/main.rs): `main`
creates two workers, attaches several QPs to each, connects them, and posts receives.
`serve` and `ping_pong` show the steady-state loop: poll one CQ, dispatch each result by its
context (a small `Job` enum, the example's own), repost buffers, stage work, and commit each
active QP once per turn.

For a smaller asynchronous application, read the
[greeter client](../examples/greeter/src/client.rs) and
[server](../examples/greeter/src/server.rs). Return to the async internals in step 4.

Keep three units distinct:

- A **WQE** is one hardware work item.
- A **request** may own several WQEs, but has one sequence number, context, result and group
  of leases. A batch completes only after its entire WQE interval retires.
- A **worker** owns one shared CQ and several QPs. Each QP still has its own send and
  receive rings; sharing the CQ does not merge their completion accounting.

## 2. Follow one synchronous request

Read these files in order:

| File | What to follow |
| --- | --- |
| [`worker.rs`](../crates/safeverbs-core/src/worker.rs) | `Worker::request`, `Request::push`, `Request::finish`, `Worker::commit`, then `Worker::poll` or `Worker::progress` |
| [`hw.rs`](../crates/safeverbs-core/src/hw.rs) | `Hw::gather`, `push`, `close`, `commit`, `progress`: per-QP state, hardware calls, CQ routing and ring updates |
| [`ring.rs`](../crates/safeverbs-core/src/ring.rs) | `begin`, `push_wqe`, `end`, `commit`, `complete`, `take`, `detach`: the request lifecycle independently of FFI |
| [`ring/payload.rs`](../crates/safeverbs-core/src/ring/payload.rs) | what a slot of a worker's rings holds: the buffers and the context, one generic value to the ring |
| [`wr_id.rs`](../crates/safeverbs-core/src/wr_id.rs) | how a CQE identifies its QP, queue and WQE number (internal: users have contexts) |
| [`nic.rs`](../crates/safeverbs-core/src/nic.rs) | the unsafe backend contract the core relies on |
| [`verbs.rs`](../crates/safeverbs-core/src/verbs.rs) | the concrete libibverbs implementation; read after understanding the core |

There are two useful traces:

```text
request -> gather/push -> close (with its context) -> staged in ring -> commit -> doorbell
CQ poll -> decode wr_id -> ring.complete -> take context, typed result and leases
```

`finish` stages a request; `commit` posts it. Neither means it has completed. A CQE
can retire earlier unsignaled WQEs too. `Worker::poll` delivers owned results to its
callback, while `progress` leaves them for their owners to take. Mixing those result
consumers on one worker can consume results a future is waiting for.

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
`keep_parts` in `hw.rs`, then `ring_resources_ok` and `take` to see how a request's
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

Read [`worker.rs`](../crates/safeverbs/src/worker.rs) in the application crate first:
`LocalWorker` shares one synchronous worker through `Rc<RefCell<_>>`. Then read
[`qp.rs`](../crates/safeverbs/src/qp.rs),
[`op.rs`](../crates/safeverbs/src/op.rs) and
[`backlog.rs`](../crates/safeverbs/src/backlog.rs).

`Op` tracks a queued ticket or posted sequence number. Polling it progresses the
worker's shared CQ and takes its own result, which the operation's `Outcome` turns into its
output: `()` for sends and writes, the buffer for a READ, a `Received` and the buffer for a
receive. Dropping queued work releases it; dropping a posted future detaches its owner,
retaining the buffers until the NIC is done. The future is the request's identity, so the
worker's context is `()` here. Finally read `batch.rs`, `sink.rs` (which maps a failed request
back to its item) and `receiver.rs` for amortized operation APIs.

## 5. Read the proof claim before its mechanics

Start with [verification.md](verification.md), then:

1. [`guarantees.rs`](../crates/safeverbs-spec/src/guarantees.rs): the precise S1, F1,
   R1 and L1 statements; S2 belongs to the implementation's legal calls into M.
2. [`abs.rs`](../crates/safeverbs-spec/src/abs.rs): A, the abstract ownership and
   request machine, including groups that mix unique and shared leases, and
   cancellation.
3. [`nic.rs`](../crates/safeverbs-spec/src/nic.rs): M, the hardware behavior and verb
   preconditions being assumed.
4. [`refinement.rs`](../crates/safeverbs-spec/src/refinement.rs): the `Refines`
   obligations, the coupling of requests to WQE intervals and outcomes, and Theorem 1.
5. [`examples.rs`](../crates/safeverbs-spec/src/examples.rs): concrete checked traces.
6. [`refine.rs`](../crates/safeverbs-core/src/refine.rs): the ring-to-A adapters;
   return to `Hw::inv` and [`trusted.rs`](../crates/safeverbs-core/src/trusted.rs)
   for the hardware-facing contracts. `share.rs` provides the hardware worker framing
   rules. [`hw/send_outcomes.rs`](../crates/safeverbs-core/src/hw/send_outcomes.rs) and
   [`hw/outcomes.rs`](../crates/safeverbs-core/src/hw/outcomes.rs) tie requests' results
   to the WQEs M retired.

The executable test backend [`model.rs`](../crates/safeverbs-core/src/model.rs) is not
the formal model M: tests use it to move real bytes and inject failures.
