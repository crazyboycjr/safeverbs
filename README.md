# SafeVerbs
__SafeVerbs__ implements a memory-safe RDMA API in Rust. It leverages Rust’s type system to ensure safety properties at compile time, simplifying the development of high-performance RDMA applications.

- 🛡️ __Memory-Safe__: Memory the NIC uses belongs to the work request until it completes, memory a peer may write is never readable as Rust values, and queues never overflow. No `unsafe` in your code, and none in the public crate either.
- ✨ __User-Friendly__: Typestate connection setup with a builder, registered buffers that behave like `Box<[T]>` and `bytes::Bytes`, and every operation an `async` future, a `Sink` or a `Stream`.
- 🚀 __High Performance__: Unsignaled work requests, batches with one doorbell and one completion, and repeated sends of the same buffer are safe API with no runtime checks on the data path; each thread runs a worker, one completion queue shared by all of its queue pairs, with no locks.

```rust
let mut buf = pd.alloc::<u8>(1 << 20)?;   // BufMut: unique, writable
buf.fill(42);
let buf = buf.freeze();                    // Buf: shared, read only, cheap to clone and slice
qp.send(buf.slice(..)).await?;
qp.send(buf.slice(4096..8192)).await?;
qp.batch(|b| {                             // one doorbell, one completion
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
let buf = buf.try_thaw()?;                 // BufMut again once nothing holds it
```

The QPs of a thread share one completion queue: build them on one worker, which every future of the thread polls, or drive the worker yourself in an event loop.

```rust
let worker = LocalWorker::new(&pd, 4096)?;    // one CQ for this thread's QPs
let qp = QueuePairBuilder::<RC>::new(&pd).build_on(&worker)?.handshake(remote)?;

let mut w = Worker::new(&pd, 4096)?;          // or the synchronous core
loop {
    w.poll(|w, c| {                            // each request that retired:
        handle(c.qp, c.wc, c.leases);          // its QP, outcome and buffers
        // ... post new work with w.request(qp)
    });
}
```

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
RDMA WRITE bandwidth over RC or UC, comparable to [perftest](https://github.com/linux-rdma/perftest)'s `ib_write_bw`, on one or several QPs of one worker. By default the client runs the worker's event loop: it polls the CQ, counts each completion, refills every send queue and rings each QP's doorbell once per turn, asking for a completion every `--cq-mod` WRITEs. With `--async` it streams WRITEs through a `Sink` per QP instead, all on one `LocalWorker`.
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
  -q, --qps <QPS>                      Number of QPs, all on one worker (one CQ); the iterations are spread over them [default: 1]
      --cq-mod <CQ_MOD>                Ask for a completion every this many WRITEs of a QP (perftest's --cq-mod) [default: 16]
      --async                          Drive the QPs with futures (a sink per QP) instead of the worker's event loop
  -h, --help                           Print help
```

Start the server on one machine, then the client on another.
```
cargo run --release -p perftest --bin safeverbs_write_bw -- --qps 4
cargo run --release -p perftest --bin safeverbs_write_bw -- <server_addr> --qps 4 -s 65536 -n 1000000
```

### Event loop
The idiomatic high-performance pattern: one worker per thread, a loop that polls its CQ, dispatches every completion to its QP and posts new work. Two threads on the same device, each owning a worker of QPs connected to the other's, ping-pong messages; the example prints the round trips per second.
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
SafeVerbs was inspired by [rust-ibverbs](https://github.com/jonhoo/rust-ibverbs), which provides simple abstractions for RDMA but leaves data path operations marked as unsafe. SafeVerbs makes the data path safe too, and keeps every `unsafe` line in one small crate.

- `crates/safeverbs`: the crate applications use, `#![forbid(unsafe_code)]`: queue pairs on per-thread workers, futures, sinks and streams.
- `crates/safeverbs-core`: everything `unsafe` behind a small API: the verbs resources, registered memory, workers (a CQ and its queue pairs), and the per-queue completion ring, which is pure bookkeeping, formally verified. The NIC sits behind a trait with two implementations: libibverbs, and a simulated NIC that runs the whole stack in tests without hardware.
- `crates/safeverbs-sys`: raw FFI bindings for libibverbs and mlx5dv, with rdma-core bundled.
- `crates/safeverbs-spec`: the formal specification (a model of the NIC and the abstract machine SafeVerbs refines), checked by Verus; see [docs/verification.md](docs/verification.md).
- `examples/`: sample applications.
- `demo/`: short side-by-side comparisons with libibverbs in C.

## Design Highlights
RDMA programming with C libraries like `libibverbs` is powerful but prone to errors due to implicit contracts and manual memory management. SafeVerbs addresses these challenges; [docs/design.md](docs/design.md) has the full design.

__Data Path__
- __Owned buffers instead of bookkeeping__: `BufMut<T>` is unique and writable, `Buf<T>` shared and read only. Posting moves a lease of the buffer into the request, which keeps it until the NIC is done, so a buffer can never be freed or written while in flight, and sending the same buffer or any part of it many times at once needs no check.
- __Typed memory__: the NIC only writes into types that accept any bit pattern (`zerocopy::FromBytes`); what goes on the wire has no padding (`IntoBytes`); `TryFromBytes` views check types with invalid bit patterns.
- __Remote access under control__: a buffer exposed to a peer is registered on its own, so its remote key reaches exactly that buffer, and it is not readable as Rust values until the access is revoked.
- __Completion ring__: each queue pair has a ring of slots indexed by sequence number. Send queues complete in order, so one completion retires every request before it: unsignaled requests and batches are safe, and flushes after an error hand every buffer back with the error. A doorbell the device refuses hands each of its requests back with a flush error; sequence numbers are never reused.
- __Workers__: a worker is one completion queue and the queue pairs attached to it, the unit of thread affinity, on a thread domain where the device has them. Each queue pair reserves room on the CQ for every completion it may produce, and completions are routed to their queue pair by `wr_id` with no lookup.
- __Asynchronous operations__: every operation is a `Future` (dropping it never cancels the request), WRITEs stream through a `futures::Sink` and receives through a `futures::Stream`. A full queue means waiting, not an error.
- __A synchronous layer__ under the futures, for applications that drive their own polling loop: a worker moves between threads as one value, and its poll hands every completed request to the loop with its queue pair and buffers, without allocating.

__Control Path__
- __Typestate Pattern__: connection setup uses the typestate pattern, so a queue pair posts only once connected, and RC-only settings and operations exist only for RC.
- __Builder Pattern__: sensible defaults for reliable connections (RC) and unreliable connections (UC), with setters for the rest.
- __Resource Dependency Management__: resources are reference counted and released in the right order, without lifetime annotations; a queue pair is destroyed before any buffer it still holds is released.
- __Checked capacities__: queue depths, scatter/gather counts and CQ sizes are checked against the device when objects are created, not on every post; a queue pair that does not fit in its worker's CQ is refused.

## License
SafeVerbs is released under the BSD 3 License. See the `LICENSE` file for details.
