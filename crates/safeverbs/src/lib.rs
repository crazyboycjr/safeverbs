//! Memory-safe, zero-cost RDMA (ibverbs) for Rust.
//!
//! safeverbs makes the contracts of libibverbs part of Rust's type system:
//! memory the NIC uses is owned by the work request until it completes,
//! no reference to memory a peer may write ever exists, queues never
//! overflow, and only connected QPs post. This crate has no `unsafe` code;
//! everything unsafe lives in `safeverbs-core` behind a small API.
//!
//! # Overview
//!
//! - Open a device with [`Context::open`] and allocate a protection domain
//!   ([`Context::alloc_pd`]).
//! - Allocate registered memory with [`Pd::alloc`]: a [`BufMut<T>`] is
//!   unique and writable (receive into it, READ into it); freeze it into a
//!   [`Buf<T>`], shared and read only, cheap to clone and slice, to send it
//!   or any part of it as often as wanted. [`Buf::try_thaw`] gives the
//!   `BufMut` back once nothing holds it. [`BufMut::expose`] lets a peer
//!   WRITE, READ and run atomics on a buffer through its
//!   [`RemoteBuf`]; [`Exposed::copy_to_slice`] reads it meanwhile, and
//!   [`Exposed::reclaim`] revokes that access and gives the buffer back.
//! - Connect a QP: [`QueuePairBuilder`] → [`PreparedQp`] (exchange
//!   [`Endpoint`]s with the peer) → [`PreparedQp::handshake`] →
//!   [`LocalQp`] (or [`SharedQp`] for several threads). Build the QPs of a
//!   thread on one [`LocalWorker`] ([`QueuePairBuilder::build_on`]): they
//!   share its completion queue, which every future of the thread polls.
//! - Post: every operation ([`LocalQp::send`], [`LocalQp::write`],
//!   [`LocalQp::read`], [`LocalQp::recv`], atomics, [`LocalQp::batch`]) is
//!   posted when called and returns a future of its outcome: `()` for what
//!   the NIC reports nothing else of, the buffer for a READ, and what it got
//!   ([`Received`]) with the buffer for a receive. A
//!   [`LocalQp::sink`] streams WRITEs with as few completions as the queue
//!   allows; a [`LocalQp::receiver`] is a stream of receives. Errors say what
//!   happened ([`CompletionError`]) and hand buffers back.
//! - Or drive the [`sync`] layer yourself: a [`Worker`](sync::Worker) is a
//!   completion queue with its QPs, one value that moves between threads;
//!   post into the rings with a context of your own type, commit, and poll
//!   the CQ for each request's context, result and buffers, in a loop of your
//!   own.
//!
//! Awaiting a future polls its worker's completion queue: there is no
//! background task, and a pending future asks to be polled again (busy
//! polling, as RDMA applications do).
//!
//! # Example
//!
//! Two RC QPs connected to each other on the simulated NIC ([`Model`]),
//! which needs no hardware; with a device, open it with
//! [`Context::open`] instead and exchange the endpoints over the network.
//!
//! ```
//! use futures::{StreamExt, executor::block_on};
//! use safeverbs::{Context, Model, QueuePairBuilder, RC, Received};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let ctx = Context::new(Model::new(1));
//! let pd = ctx.alloc_pd()?;
//! let a = QueuePairBuilder::<RC, _>::new(&pd).build()?;
//! let b = QueuePairBuilder::<RC, _>::new(&pd).build()?;
//! let (ea, eb) = (a.endpoint(), b.endpoint());
//! let (qp, peer) = (a.handshake(eb)?, b.handshake(ea)?);
//!
//! block_on(async {
//!     // The peer exposes a buffer to WRITE into and posts receives.
//!     let dst = pd.alloc::<u8>(4096)?.expose()?;
//!     let mut rx = peer.receiver((0..4).map(|_| pd.alloc::<u8>(64).unwrap()));
//!
//!     let mut buf = pd.alloc::<u8>(4096)?; // BufMut: unique, writable
//!     buf.fill(42);
//!     let buf = buf.freeze(); // Buf: shared, read only
//!     qp.write(buf.slice(..), dst.remote()).await?;
//!     qp.send(buf.slice(..64)).await?;
//!
//!     let (received, got) = rx.next().await.expect("a receive");
//!     assert_eq!(received, Ok(Received::Message { len: 64, imm: None }));
//!     assert!(got.iter().all(|&b| b == 42));
//!     rx.repost(got)?;
//!     // The SEND came after the WRITE, so what was written is complete.
//!     let mut copy = [0u8; 4096];
//!     dst.copy_to_slice(&mut copy); // a volatile copy; the rkey stays valid
//!     assert!(copy.iter().all(|&b| b == 42));
//!
//!     // Nothing holds the frozen buffer any more: write into it again.
//!     let mut buf = buf.try_thaw().expect("no other handle, nothing in flight");
//!     buf.fill(7);
//!     Ok::<(), Box<dyn std::error::Error>>(())
//! })?;
//! # Ok(())
//! # }
//! ```
//!
//! The shapes of the other operations:
//!
//! ```no_run
//! # use futures::{SinkExt, StreamExt};
//! # use safeverbs::{BufMut, LocalQp, Pd, RC, Received, RemoteBuf, WriteOp};
//! # async fn f(pd: Pd, qp: LocalQp<RC>, dst: RemoteBuf, bufs: Vec<BufMut<u8>>) -> Result<(), Box<dyn std::error::Error>> {
//! let mut buf = pd.alloc::<u8>(1 << 20)?; // BufMut
//! buf.fill(42);
//! let buf = buf.freeze(); // Buf
//! qp.send(buf.slice(..)).await?;
//! qp.send(buf.slice(4096..8192)).await?;
//! // One doorbell, one completion.
//! qp.batch(|b| {
//!     for i in 0..128 {
//!         b.write(buf.slice(..64), dst.offset(i * 64));
//!     }
//! })
//! .await?;
//! let mut tx = qp.sink();
//! let mut ops = futures::stream::iter((0..1000u64).map(|i| Ok(WriteOp::new(buf.slice(..64), dst.offset(i % 128 * 64)))));
//! tx.send_all(&mut ops).await?;
//! let mut rx = qp.receiver(bufs);
//! while let Some((received, buf)) = rx.next().await {
//!     match received {
//!         Ok(Received::Message { len, .. }) => println!("received {:?}", &buf[..len as usize]),
//!         Ok(Received::WriteNotification { imm, len }) => println!("{len} bytes written, {imm}"),
//!         Err(error) => println!("a receive failed: {error}"),
//!     }
//!     rx.repost(buf)?;
//! }
//! # Ok(()) }
//! ```

//! # What does not compile
//!
//! The rules above are checked by the compiler. A frozen buffer is read
//! only:
//!
//! ```compile_fail,E0594
//! # fn f(pd: safeverbs::Pd) -> safeverbs::Result<()> {
//! let buf = pd.alloc::<u8>(64)?.freeze();
//! buf[0] = 1;
//! # Ok(()) }
//! ```
//!
//! A buffer posted for a receive belongs to the request until it comes
//! back:
//!
//! ```compile_fail,E0382
//! # fn f(pd: safeverbs::Pd, qp: safeverbs::LocalQp<safeverbs::RC>) -> safeverbs::Result<()> {
//! let mut buf = pd.alloc::<u8>(64)?;
//! let recv = qp.recv(buf);
//! buf[0] = 1;
//! # Ok(()) }
//! ```
//!
//! Memory a peer may write cannot be read as Rust values:
//!
//! ```compile_fail,E0608
//! # fn f(pd: safeverbs::Pd) -> Result<(), Box<dyn std::error::Error>> {
//! let exposed = pd.alloc::<u8>(64)?.expose()?;
//! let first = exposed[0];
//! # Ok(()) }
//! ```
//!
//! The NIC only writes types that accept every bit pattern:
//!
//! ```compile_fail,E0277
//! # fn f(pd: safeverbs::Pd) -> safeverbs::Result<()> {
//! let flags = pd.alloc::<bool>(64)?;
//! # Ok(()) }
//! ```
//!
//! A QP posts only once connected, and UC has no RDMA READ:
//!
//! ```compile_fail,E0599
//! # use safeverbs::{Pd, QueuePairBuilder, RC};
//! # fn f(pd: Pd) -> safeverbs::Result<()> {
//! let prepared = QueuePairBuilder::<RC>::new(&pd).build()?;
//! let send = prepared.send(pd.alloc::<u8>(64)?.freeze());
//! # Ok(()) }
//! ```
//!
//! ```compile_fail,E0599
//! # use safeverbs::{LocalQp, Pd, RemoteBuf, UC};
//! # fn f(pd: Pd, qp: LocalQp<UC>, src: RemoteBuf) -> safeverbs::Result<()> {
//! let read = qp.read(pd.alloc::<u8>(64)?, src);
//! # Ok(()) }
//! ```
//!
//! A `LocalQp` belongs to the thread of its worker (use a [`SharedQp`] to
//! share one):
//!
//! ```compile_fail,E0277
//! # fn f(qp: safeverbs::LocalQp<safeverbs::RC>) {
//! std::thread::scope(|s| {
//!     s.spawn(|| qp.qp_num());
//! });
//! # }
//! ```

//!
//! A synchronous worker moves between threads as one value, but is used by
//! one thread at a time:
//!
//! ```
//! fn moves<T: Send>(_: T) {}
//! # fn f(w: safeverbs::sync::Worker) {
//! moves(w);
//! # }
//! ```
//!
//! ```compile_fail,E0277
//! # fn f(w: safeverbs::sync::Worker) {
//! std::thread::scope(|s| {
//!     s.spawn(|| w.cq_size());
//! });
//! # }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod backlog;
mod batch;
mod builder;
mod op;
mod qp;
mod receiver;
mod sink;
mod worker;

pub use batch::Batch;
pub use builder::{PreparedQp, QueuePairBuilder, RcQueuePairBuilder, UcQueuePairBuilder};
pub use op::{Flush, Op, Outcome, Read, Recv, Sent};
pub use qp::{LocalQp, QpState, QueueAccess, SharedQp};
pub use receiver::Receiver;
pub use safeverbs_core::model::Model;
pub use safeverbs_core::verbs::{Verbs, devices};
pub use safeverbs_core::{
    Access, Buf, BufError, BufMut, Chunks, Completed, CompletionError, Context, Endpoint, Error,
    Exposed, Gid, GidEntry, Nic, Pd, PortInfo, RC, Received, Reliable, RemoteBuf, Result, RingBuf,
    Status, Transport, UC,
};
pub use sink::{WriteOp, WriteSink};
pub use worker::LocalWorker;

/// The synchronous layer under the futures: a [`Worker`](sync::Worker) is a
/// completion queue and the QPs attached to it, one value that is `Send`
/// but not `Sync`; its QPs' requests are written into completion rings and
/// the hardware queues together, committed with one doorbell, and come back
/// (context, result and buffers) when the CQ is polled.
///
/// This is the zero-cost core: a thread's loop polls its worker, dispatches
/// each completion by its context and posts new work. Lend buffers with
/// [`Buf::into_part`] and [`BufMut::into_part`], or let [`Worker::send`] and its
/// kin do it; get them back with
/// [`Leases::into_buf_mut`](sync::Leases::into_buf_mut).
///
/// ```no_run
/// # use safeverbs::{Buf, Completed, Pd, RC, RemoteBuf};
/// # use safeverbs::sync::{QpCaps, Worker, WrOp};
/// /// What each request is for: the loop's own type.
/// enum Job {
///     Write { client: u32 },
///     Stats,
/// }
///
/// # fn f(pd: Pd, buf: Buf<u8>, dst: RemoteBuf) -> safeverbs::Result<()> {
/// let caps = QpCaps { max_send_wr: 128, max_recv_wr: 128, max_send_sge: 1, max_recv_sge: 1, max_inline_data: 0 };
/// let mut w: Worker<Job> = Worker::new(&pd, 4096)?;
/// let qps: Vec<_> = (0..8).map(|_| w.create_qp::<RC>(caps, 32)).collect::<Result<_, _>>()?;
/// // ... exchange endpoints (`w.endpoint(qp, ..)`) and `w.connect(qp, ..)` each ...
/// for (client, &qp) in qps.iter().enumerate() {
///     w.write(qp, buf.slice(..64), dst, Job::Write { client: client as u32 })?;
/// }
/// // One context per request, whatever the number of WQEs.
/// let mut req = w.request(qps[0]);
/// req.push(WrOp::Write(dst), buf.slice(..64).into_part()).map_err(|e| e.error)?;
/// req.push(WrOp::Write(dst), buf.slice(64..128).into_part()).map_err(|e| e.error)?;
/// req.submit(true, Job::Stats)?;
/// loop {
///     w.poll(|w, c| match (c.ctx, c.result) {
///         (Job::Write { client }, Ok(Completed::Sent)) => {
///             let _ = w.write(qps[c.qp], buf.slice(..64), dst, Job::Write { client });
///         }
///         (Job::Stats, Ok(_)) => {}
///         // Before the error's position every WQE completed; from it on, none did,
///         // and what those did at the peer is unknown.
///         (_, Err(error)) => eprintln!("request failed: {error}"),
///         (_, Ok(other)) => unreachable!("{other:?}"),
///     });
/// }
/// # }
/// ```
pub mod sync {
    pub use safeverbs_core::nic::QpCaps;
    pub use safeverbs_core::ring::{Ring, State};
    pub use safeverbs_core::worker::{
        Completion, DetachedError, MAX_QPS, MAX_SGES, PostError, QpKey, Queue, Request, Worker,
    };
    pub use safeverbs_core::{ConnectAttr, Endpoint, Leases, Part, Seq, WrOp};
}
