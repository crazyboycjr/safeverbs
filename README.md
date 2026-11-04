# SafeVerbs
__SafeVerbs__ implements a memory-safe RDMA API in Rust. It leverages Rust’s type system to ensure safety properties at compile time, simplifying the development of high-performance RDMA applications.

- 🛡️ __Memory-Safe__: Memory the NIC uses belongs to the work request until it completes, no reference to memory a peer may write ever exists (it is read through a volatile copy), and queues never overflow. No `unsafe` in your code, and none in the public crate either.
- ✨ __User-Friendly__: Typestate connection setup with a builder, registered buffers that behave like `Box<[T]>` and `bytes::Bytes`, and every operation an `async` future, a `Sink` or a `Stream`.
- 🚀 __High Performance__: Unsignaled work requests, batches with one doorbell and one completion, and repeated sends of the same buffer are safe API without interval-tree checks. Each thread has one completion queue shared by all of its queue pairs; supported providers can omit their locks. Device limits and posting arguments are checked before submission.

Formal verification with Verus is in progress; [docs/verification.md](docs/verification.md) says exactly what is proved, tested and assumed.

```rust
let mut buf = pd.alloc::<u8>(1 << 20)?;   // BufMut: unique, writable
buf.fill(42);
let buf = buf.freeze();                    // Buf: shared, read only, cheap to clone and slice
qp.send(buf.slice(..)).await?;
qp.send(buf.slice(4096..8192)).await?;
qp.batch(|b| {                             // one doorbell, one completion, nothing allocated
    for i in 0..128 {
        b.write(buf.view(..64), dst.offset(i * 64));
    }
})
.await?;
let mut tx = qp.sink();                    // impl Sink<WriteOp>
tx.send_all(&mut ops).await?;
let mut rx = qp.receiver(bufs);            // a stream of (Result<Received, _>, BufMut)
while let Some((received, buf)) = rx.next().await {
    match received? {
        Received::Message { len, imm } => handle(&buf[..len as usize], imm),
        Received::WriteNotification { imm, len } => wrote(imm, len), // buf is untouched
    }
    rx.repost(buf)?;
}
let buf = buf.try_thaw()?;                 // BufMut again once nothing holds it
```

That is the futures' side, on an `AsyncQp`. The event loop's side looks like verbs: the type of `wr_id` belongs to the CQ, a QP refers to its CQ, and a loop polls the CQ and decides the dispatch. A `wr_id` may be any value of your own type, and comes back exactly once, with its request's typed result and its buffers:

```rust
let cq: LocalCq<Req> = ctx.create_local_cq(4096)?;     // one CQ for this thread's QPs
let qp: LocalQp<RC, Req> = pd.create_qp(&cq, caps)?.handshake(remote)?;
qp.post_send(buf.slice(..), Req::Reply { client: 7 })?; // also post_write, post_read, post_recv

let n = cq.poll(32, |wc| match (wc.wr_id, wc.result) {  // reads at most 32 CQEs
    (Req::Reply { client }, Ok(_)) => done(client),     // may post on any QP from here
    (req, Err(e)) => retry(req, e),
    _ => {}
});
```

`cq.poll` reads the CQEs and updates the CQ's consumer index before it hands any of them out, so the closure may post new work on any QP. `cq.poll_one()` is there for loops that want `break`, `?` or `.await`. A CQ is driven one way, which its type says: `AsyncCq` and `AsyncQp` for futures, `LocalCq` and `LocalQp` for an event loop, and a thread may have one of each. All of them stay on the thread that created them; `SharedQp` shares a QP between threads behind a lock.

An error tells which WQE of the request failed: those before it completed successfully, and it and those after it did not, with their remote effects unknown (a later WRITE may land at the peer before an earlier READ fails, and a lost acknowledgement can hide a WRITE that landed). A request whose doorbell the device refused says so: the NIC never saw it.

## Getting Started

1. Clone the repo and its submodule (rdma-core, which is built with SafeVerbs).
```
git clone git@github.com:crazyboycjr/safeverbs.git --recursive
```
2. Install the build requirements and the Rust toolchain (see [docs/dev-setup.md](docs/dev-setup.md) for other distributions).
```
sudo apt update
sudo apt install build-essential cmake pkg-config python3 libclang-dev libnl-3-dev libnl-route-3-dev
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
3. Build the codebase.
```
cargo build --release
```
4. No RDMA NIC? Create a Soft-RoCE device (needs Docker) and run the tests.
```
scripts/rxe.sh up
cargo test --workspace
```

To use SafeVerbs in your project, depend on the `safeverbs` crate. On hosts with MLNX_OFED or DOCA-OFED, build against the installed rdma-core instead of the bundled one; [docs/dev-setup.md](docs/dev-setup.md) explains how.

## Devcontainer
You can alternatively set up the environment using the provided devcontainer for an isolated and pre-configured development setup.

## Run Examples
SafeVerbs includes example applications to demonstrate its usage. They run on any RDMA NIC, and on a Soft-RoCE device with client and server on the same machine. They use the first RDMA device unless told otherwise.

### Greeter
A simple "Hello, World" to verify that your setup works.
```
cargo run --release -p greeter --bin greeter_server
cargo run --release -p greeter --bin greeter_client
```
The client connects to the server on the local machine and sends it "Hello, SafeVerbs", which the server prints.

### Write bandwidth
RDMA WRITE bandwidth over RC or UC, comparable to [perftest](https://github.com/linux-rdma/perftest)'s `ib_write_bw`, on one or several QPs of one CQ. By default the client runs an event loop on a `LocalCq`: it polls the CQ, counts each completion, refills every send queue and rings each QP's doorbell once per turn, asking for a completion every `--cq-mod` WRITEs. With `--async` it streams WRITEs through a `Sink` per QP instead, all on one `AsyncCq`.
```
$ cargo run --release -p perftest --bin safeverbs_write_bw -- --help
RDMA WRITE bandwidth test for SafeVerbs.

Usage: safeverbs_write_bw [OPTIONS] [HOST]

Arguments:
  [HOST]  The host to connect to, an IP address or a name. Without it the binary runs as the server and listens on 0.0.0.0

Options:
  -p, --port <PORT>                    The TCP port to use [default: 5000]
  -d, --device <DEVICE>                The RDMA device to use (default: the first)
  -t, --tx-depth <TX_DEPTH>            Send queue depth (max_send_wr) [default: 128]
  -n, --num-iters <NUM_ITERS>          Number of measured iterations [default: 5000]
  -w, --warmup <WARMUP>                Number of warmup iterations [default: 100]
  -s, --size <SIZE>                    Message size in bytes [default: 65536]
  -c, --connection <CONNECTION>        Connection type, RC or UC [default: RC]
  -x, --gid-index <GID_INDEX>          The GID index (perftest's -x); default: the first RoCE v2 GID with an IPv4 address
      --traffic-class <TRAFFIC_CLASS>  Traffic class with a GRH [default: 0]
  -q, --qps <QPS>                      Number of QPs, all on one CQ; the iterations are spread over them [default: 1]
      --cq-mod <CQ_MOD>                Ask for a completion every this many WRITEs of a QP (perftest's --cq-mod) [default: 16]
      --async                          Drive the QPs with futures (a sink per QP) instead of an event loop
  -h, --help                           Print help
```

Start the server on one machine, then the client on another.
```
cargo run --release -p perftest --bin safeverbs_write_bw -- --qps 4
cargo run --release -p perftest --bin safeverbs_write_bw -- <server_addr> --qps 4 -s 65536 -n 1000000
```

### Event loop
The idiomatic high-performance pattern: one `LocalCq` per thread, a loop that polls it, dispatches every completion by its `wr_id` and posts new work. Two threads on the same device, each with a CQ of QPs connected to the other's, ping-pong messages; the example prints the round trips per second.
```
cargo run --release -p event_loop -- [device] [qps] [pings]
```

### UChan
An unreliable channel: a stream of RDMA WRITEs on UC (unreliable connection), every thousandth carrying its sequence number as immediate data, which the receiver prints. On a lossy network some numbers never arrive and the sender never learns about it.
```
cargo run --release -p uchan
cargo run --release -p uchan -- -c <server_addr> -s 65536 --num-iters 1000000
```

## Code Structure
For a source-level reading order and a request's path through the implementation, see
[the code tour](docs/code-tour.md).

SafeVerbs was inspired by [rust-ibverbs](https://github.com/jonhoo/rust-ibverbs), which provides simple abstractions for RDMA but leaves data path operations marked as unsafe. SafeVerbs makes the data path safe too, and keeps every `unsafe` line in one small crate.

- `crates/safeverbs`: the crate applications use, `#![forbid(unsafe_code)]`: completion queues and queue pairs per thread, for an event loop or for futures, sinks and streams.
- `crates/safeverbs-core`: everything `unsafe` behind a small API: the verbs resources, registered memory, the engine under the handles (a CQ and its queue pairs by index), and the per-queue completion ring, which is pure bookkeeping, formally verified. The NIC sits behind a trait with two implementations: libibverbs, and a simulated NIC that runs the whole stack in tests without hardware.
- `crates/safeverbs-sys`: raw FFI bindings for libibverbs and mlx5dv, with rdma-core bundled.
- `crates/safeverbs-spec`: the formal specification (a model of the NIC and the abstract machine SafeVerbs refines), checked by Verus; see [docs/verification.md](docs/verification.md).
- `examples/`: sample applications.
- `demo/`: short side-by-side comparisons with libibverbs in C.

## Design Highlights
RDMA programming with C libraries like `libibverbs` is powerful but prone to errors due to implicit contracts and manual memory management. SafeVerbs addresses these challenges; [docs/design.md](docs/design.md) has the full design.

__Data Path__
- __Owned buffers instead of bookkeeping__: `BufMut<T>` is unique and writable, `Buf<T>` shared and read only. Posting moves a lease of the buffer into the request, which keeps it until the NIC is done, so a buffer can never be freed or written while in flight, and sending the same buffer or any part of it many times at once needs no check.
- __Typed memory__: the NIC only writes into types that accept any bit pattern (`zerocopy::FromBytes`); what goes on the wire has no padding (`IntoBytes`); `TryFromBytes` views check types with invalid bit patterns.
- __Remote access under control__: a buffer exposed to a peer is registered on its own, so its remote key reaches exactly that buffer. No reference to it exists until the access is revoked; while it lasts, a safe volatile copy reads its bytes, and a write-with-immediate notification or a SEND says when they are complete.
- __Completion ring__: each queue pair has a ring of slots indexed by sequence number. Send queues complete in order, so one completion completes every request before it: unsignaled requests and batches are safe, and flushes after an error hand every buffer back with the error. A doorbell the device refuses hands each of its requests back as refused; sequence numbers are never reused.
- __Typed `wr_id`s and results__: a request carries a value of your own type, kept in its slot and handed back once with its buffers. Its result has only the fields its operation defines: a SEND has no length, a receive is a message or the notification of a peer's WRITE, and an error names the WQE that failed.
- __One completion queue per thread__: a `LocalCq` or `AsyncCq` is one completion queue and the queue pairs attached to it, on a thread domain where the device has them, with an `Rc` and a `RefCell` flag on the way and no atomics. Each queue pair reserves room on the CQ for every completion it may produce, and completions are routed to their queue pair by the hardware `wr_id` with no lookup.
- __An event loop at the cost of a hand-written one__: a single operation has its own path (check, slot, WQE, doorbell), polling handles each CQE in one pass, and nothing is allocated. A batch collects its WQEs first, in storage that stays with its QP, with nothing held while its closure runs. A full queue is an error that hands the `wr_id` and the buffers back. `scripts/measure.sh` prints what each path costs against code written by hand.
- __Asynchronous operations__: every operation is a `Future` (dropping it never cancels the request), WRITEs stream through a `futures::Sink`, which gathers them into requests and looks at the CQ only when it has no room, on a flush and on the close, and receives through a `futures::Stream`. A full queue means waiting, not an error.
- __An engine under the handles__, for those who want to remove the `Rc` too: an `Engine` is a CQ with its QPs by index, one value that moves between threads, reachable with `as_raw()`.

__Control Path__
- __Typestate Pattern__: connection setup uses the typestate pattern, so a queue pair posts only once connected, and RC-only settings and operations exist only for RC.
- __Builder Pattern__: sensible defaults for reliable connections (RC) and unreliable connections (UC), with setters for the rest.
- __Resource Dependency Management__: resources are reference counted and released in the right order, without lifetime annotations; a queue pair is drained (moved to the error state, its flushed work requests polled) and destroyed before any buffer it held is released.
- __Checked capacities__: queue depths, scatter/gather counts and CQ sizes are checked against the device when objects are created, not on every post; a queue pair that does not fit in its CQ is refused.

## License
SafeVerbs is released under the BSD 3 License. See the `LICENSE` file for details.
