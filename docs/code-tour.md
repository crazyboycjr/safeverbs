# Reading the code

Start with an application, follow one request through the synchronous core, then read
the asynchronous wrapper and the proofs. The large verified modules contain both
executable Rust and ghost code; on a first pass, follow the executable methods and
return to their contracts afterward.

## 1. Start at a shared-CQ application

Read [`examples/event_loop/src/main.rs`](../examples/event_loop/src/main.rs): `main`
creates two workers, attaches several QPs to each, connects them, and posts receives.
`serve` and `ping_pong` show the steady-state loop: poll one CQ, dispatch results,
repost buffers, stage work, and commit each active QP once per turn.

For a smaller asynchronous application, read the
[greeter client](../examples/greeter/src/client.rs) and
[server](../examples/greeter/src/server.rs). Return to the async internals in step 4.

Keep three units distinct:

- A **WQE** is one hardware work item.
- A **request** may own several WQEs, but has one sequence number, result and resource
  group. A batch completes only after its entire WQE interval retires.
- A **worker** owns one shared CQ and several QPs. Each QP still has its own send and
  receive rings; sharing the CQ does not merge their completion accounting.

## 2. Follow one synchronous request

Read these files in order:

| File | What to follow |
| --- | --- |
| [`worker.rs`](../crates/safeverbs-core/src/worker.rs) | `Worker::request`, `Request::push`, `Request::finish`, `Worker::commit`, then `Worker::poll` or `Worker::progress` |
| [`hw.rs`](../crates/safeverbs-core/src/hw.rs) | `Hw::gather`, `push`, `close`, `commit`, `progress`: per-QP state, hardware calls, CQ routing and ring updates |
| [`ring.rs`](../crates/safeverbs-core/src/ring.rs) | `begin`, `push_wqe`, `end`, `commit`, `complete`, `take`, `detach`: the request lifecycle independently of FFI |
| [`wr_id.rs`](../crates/safeverbs-core/src/wr_id.rs) | how a CQE identifies its QP, queue and WQE number |
| [`nic.rs`](../crates/safeverbs-core/src/nic.rs) | the unsafe backend contract the core relies on |
| [`verbs.rs`](../crates/safeverbs-core/src/verbs.rs) | the concrete libibverbs implementation; read after understanding the core |

There are two useful traces:

```text
request -> gather/push -> close -> staged in ring -> commit -> doorbell
CQ poll -> decode wr_id -> ring.complete -> take result and leases
```

`finish` stages a request; `commit` posts it. Neither means it has completed. A CQE
can retire earlier unsignaled WQEs too. `Worker::poll` delivers owned results to its
callback, while `progress` leaves them for their owners to take. Mixing those result
consumers on one worker can consume results a future is waiting for.

## 3. Follow the memory separately

Read [`buf.rs`](../crates/safeverbs-core/src/buf.rs), especially `Region`, `RawMut`,
`BufMut`, `Buf` and `Exposed`, then
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
`Hw::keep_parts`, `ring_resources_ok`, `take`, then
[`buf/projection.rs`](../crates/safeverbs-core/src/buf/projection.rs) to see how the
actual request metadata becomes A's member ranges and access directions. Fresh names
and byte permissions are still separate obligations.

For remote access, `Exposed::try_reclaim` revokes the remote registration before returning
typed access. A refused close returns its owner for retry; a failed final cleanup retains
the allocation and dependency chain.

[`buf/range.rs`](../crates/safeverbs-core/src/buf/range.rs) is the verified executable
arithmetic these buffers use for splits and frozen views. Read it before the eventual
allocation-permission integration: bounded ranges alone do not establish ownership.

Then read [`wc.rs`](../crates/safeverbs-core/src/wc.rs): its accessors expose only
operation-defined metadata. SEND/WRITE do not report a byte count; READ and receive
cases have different metadata. A WRITE_WITH_IMM notification is distinct from a receive
payload written into the posted receive buffer.

## 4. Add the asynchronous layer

Read [`worker.rs`](../crates/safeverbs/src/worker.rs) in the application crate first:
`LocalWorker` shares one synchronous worker through `Rc<RefCell<_>>`. Then read
[`qp.rs`](../crates/safeverbs/src/qp.rs),
[`op.rs`](../crates/safeverbs/src/op.rs) and
[`backlog.rs`](../crates/safeverbs/src/backlog.rs).

`Op` tracks a queued ticket or posted sequence number. Polling it progresses the
worker's shared CQ and takes its own result. Dropping queued work releases it; dropping
a posted future detaches its owner, retaining the buffers until the NIC is done.
Finally read `batch.rs`, `sink.rs` and `receiver.rs` for amortized operation APIs.

## 5. Read the proof claim before its mechanics

Start with [verification.md](verification.md), then:

1. [`guarantees.rs`](../crates/safeverbs-spec/src/guarantees.rs): the precise S1, F1,
   R1 and L1 statements; S2 belongs to the implementation's legal calls into M.
2. [`abs.rs`](../crates/safeverbs-spec/src/abs.rs): A, the abstract ownership and
   request machine, including mixed resource groups and cancellation.
3. [`nic.rs`](../crates/safeverbs-spec/src/nic.rs): M, the hardware behavior and verb
   preconditions being assumed.
4. [`refinement.rs`](../crates/safeverbs-spec/src/refinement.rs): the `Refines`
   obligations, WQE interval/outcome coupling and conditional Theorem 1.
5. [`examples.rs`](../crates/safeverbs-spec/src/examples.rs): concrete checked traces.
6. [`refine.rs`](../crates/safeverbs-core/src/refine.rs): the ring-to-A adapters;
   return to `Hw::inv` and [`trusted.rs`](../crates/safeverbs-core/src/trusted.rs)
   for the hardware-facing contracts. `abs_share.rs` and `share.rs` provide the
   abstract and hardware worker framing rules. `composition.rs` lifts coherent finite
   interleavings; constructing that global authority from actual workers remains open.

The executable test backend
[`model.rs`](../crates/safeverbs-core/src/model.rs) is different from the formal model
M. Tests use it to move real bytes and inject failures; agreement with M is not itself
proved. The real worker's complete `impl Refines`, concrete byte-permission integration
and global thread composition remain unfinished. Component proof counts do not imply
the entire library has been verified.
