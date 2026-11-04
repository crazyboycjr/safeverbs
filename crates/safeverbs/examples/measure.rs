//! What one round of each public path costs, against code written by hand on the same NIC.
//!
//! `scripts/measure.sh` runs this under callgrind, with no rounds and with many, subtracts the
//! simulated NIC and prints the instructions per round and the ratio to the hand-written
//! baseline. Every workload runs on [`Model`], whose work happens inside its own functions, so
//! that what is left is the library's (or the baseline's).
//!
//! The baselines:
//!
//! - `raw`: perftest style. No per-request state at all, a constant `wr_id`, status checked.
//! - `careful`: what a careful hand-written program keeps, which is what the library keeps too:
//!   per-QP rings of requests, each holding its buffer and context until its completion, `wr_id`
//!   routing, a check of the queue's room before each post.
//! - `batch_careful`: the same, for 16 WRITEs of one buffer as one request of one doorbell and
//!   one completion.
//! - `train_careful`: the same, for 16 SENDs of one buffer as 16 requests of one doorbell, only
//!   the last signaled, and 16 receives.
//!
//! ```text
//! measure <workload> <rounds>
//! ```

use std::future::Future;
use std::hint::black_box;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use futures::SinkExt;
use futures::task::noop_waker;
use safeverbs::{
    AsyncQp, Buf, Context, LocalCq, LocalQp, Model, Pd, QueuePairBuilder, RC, Received, RemoteBuf,
    WriteOp,
};
use safeverbs_core::nic::{Nic, NicCq, NicQp, QpType, Sge};
use safeverbs_core::{Access, ConnectAttr, Endpoint, Engine, QpCaps, QpKey, Seq, Wc, WrOp};

/// The depth of every queue.
const DEPTH: u64 = 256;
/// Bytes of a message.
const SIZE: usize = 64;
/// WQEs of a batch, and requests of a train.
const TRAIN: usize = 16;

fn caps() -> QpCaps {
    QpCaps {
        max_send_wr: DEPTH as u32,
        max_recv_wr: DEPTH as u32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
    }
}

fn attr() -> ConnectAttr {
    ConnectAttr {
        port: 1,
        access: Access::LOCAL_WRITE | Access::REMOTE_WRITE | Access::REMOTE_READ,
        path_mtu: 5,
        rq_psn: 0,
        sq_psn: 0,
        gid_index: None,
        traffic_class: 0,
        timeout: 14,
        retry_count: 7,
        rnr_retry: 7,
        min_rnr_timer: 12,
        max_rd_atomic: 16,
        max_dest_rd_atomic: 16,
    }
}

// -----------------------------------------------------------------------------
// By hand, on the NIC's trait

type RawQp = <Model as Nic>::Qp;
type RawCq = <Model as Nic>::Cq;
type RawMr = <Model as Nic>::Mr;

/// Dropped in this order: the QPs before their CQ.
struct RawSetup {
    a: RawQp,
    b: RawQp,
    cq: RawCq,
    pd: <Model as Nic>::Pd,
    model: Model,
}

fn raw_setup() -> RawSetup {
    let model = Model::new(7);
    let pd = model.alloc_pd().unwrap();
    let cq = model.create_cq(4 * DEPTH as u32, true).unwrap();
    let mut a = model
        .create_qp(&pd, None, &cq, &cq, QpType::Rc, &caps())
        .unwrap();
    let mut b = model
        .create_qp(&pd, None, &cq, &cq, QpType::Rc, &caps())
        .unwrap();
    let (an, bn) = (a.qp_num(), b.qp_num());
    let end = |qp_num| Endpoint {
        qp_num,
        lid: 1,
        gid: None,
    };
    a.connect(&attr(), &end(bn)).unwrap();
    b.connect(&attr(), &end(an)).unwrap();
    RawSetup {
        model,
        pd,
        cq,
        a,
        b,
    }
}

/// Registers `mem` for local writes and remote access.
fn register(s: &RawSetup, mem: &mut [u8]) -> (RawMr, Sge, RemoteBuf) {
    let access = Access::LOCAL_WRITE | Access::REMOTE_WRITE | Access::REMOTE_READ;
    // SAFETY: the callers keep `mem` alive and in place until the QPs are gone.
    let mr = unsafe {
        s.model
            .reg_mr(&s.pd, mem.as_mut_ptr(), mem.len(), access)
            .unwrap()
    };
    let (lkey, rkey) = Model::mr_keys(&mr);
    let addr = mem.as_ptr() as u64;
    let sge = Sge {
        addr,
        length: mem.len() as u32,
        lkey,
    };
    let remote = RemoteBuf {
        addr,
        len: mem.len() as u64,
        rkey,
    };
    (mr, sge, remote)
}

/// perftest style: no per-request state at all, a constant `wr_id`, status checked.
fn raw(rounds: usize) {
    let mut s = raw_setup();
    let mut src = vec![1u8; SIZE];
    let mut dst = vec![0u8; SIZE];
    let (_m1, ssge, _) = register(&s, &mut src);
    let (_m2, rsge, _) = register(&s, &mut dst);
    let mut wcs = [Wc::default(); 4];
    let mut ok = 0usize;
    for _ in 0..rounds {
        // SAFETY: the buffers are registered, stay in place and are not read while in flight.
        unsafe {
            s.b.post_recv(1, &[rsge]).unwrap();
            s.a.start();
            s.a.post(2, true, &WrOp::Send, &[ssge]);
            s.a.complete().unwrap();
        }
        s.model.run();
        let mut seen = 0;
        while seen < 2 {
            let n = s.cq.poll(&mut wcs);
            for wc in &wcs[..n] {
                ok += wc.result().is_ok() as usize;
            }
            seen += n;
        }
    }
    black_box(ok);
}

/// A request of a careful hand-written program: what keeps its memory alive and its context.
#[derive(Default)]
struct Slot {
    shared: Option<Arc<Vec<u8>>>,
    unique: Option<Box<[u8]>>,
    sge: Sge,
    ctx: u64,
}

/// A ring of requests, by number modulo its depth.
struct Q {
    slots: Vec<Slot>,
    head: u64,
    tail: u64,
}

impl Q {
    fn new() -> Q {
        Q {
            slots: (0..DEPTH).map(|_| Slot::default()).collect(),
            head: 0,
            tail: 0,
        }
    }
}

struct Conn {
    qp: RawQp,
    send: Q,
    recv: Q,
}

impl Conn {
    fn new(qp: RawQp) -> Conn {
        Conn {
            qp,
            send: Q::new(),
            recv: Q::new(),
        }
    }
}

const QUEUE_BIT: u64 = 1 << 47;
const SEQ_MASK: u64 = QUEUE_BIT - 1;

#[inline(never)]
fn post_recv(c: &mut Conn, idx: u64, buf: Box<[u8]>, sge: Sge, ctx: u64) {
    assert!(c.recv.tail - c.recv.head < DEPTH);
    let seq = c.recv.tail;
    let slot = &mut c.recv.slots[(seq % DEPTH) as usize];
    slot.unique = Some(buf);
    slot.sge = sge;
    slot.ctx = ctx;
    // SAFETY: the slot keeps the buffer until the receive completes.
    unsafe {
        c.qp.post_recv((idx << 48) | QUEUE_BIT | seq, &[sge])
            .unwrap();
    }
    c.recv.tail = seq + 1;
}

#[inline(never)]
fn post_send(c: &mut Conn, idx: u64, buf: Arc<Vec<u8>>, sge: Sge, ctx: u64) {
    assert!(c.send.tail - c.send.head < DEPTH);
    let seq = c.send.tail;
    let slot = &mut c.send.slots[(seq % DEPTH) as usize];
    slot.shared = Some(buf);
    slot.sge = sge;
    slot.ctx = ctx;
    // SAFETY: the slot keeps the buffer until the send completes.
    unsafe {
        c.qp.start();
        c.qp.post((idx << 48) | seq, true, &WrOp::Send, &[sge]);
        c.qp.complete().unwrap();
    }
    c.send.tail = seq + 1;
}

/// Polls the CQ, retires every request up to each completion and hands each one's context and
/// receive buffer to `f`. Every request has `WQES` WQEs.
#[inline(never)]
fn progress<const WQES: u64>(
    cq: &mut RawCq,
    conns: &mut [Conn],
    wcs: &mut [Wc; 16],
    mut f: impl FnMut(u64, bool, Option<Box<[u8]>>, Sge),
) -> usize {
    let n = cq.poll(wcs);
    for wc in &wcs[..n] {
        let id = wc.probe_wr_id();
        let conn = &mut conns[(id >> 48) as usize];
        let is_recv = id & QUEUE_BIT != 0;
        let q = if is_recv {
            &mut conn.recv
        } else {
            &mut conn.send
        };
        let seq = (id & SEQ_MASK) / WQES;
        let ok = wc.result().is_ok();
        while q.head <= seq {
            let slot = &mut q.slots[(q.head % DEPTH) as usize];
            slot.shared = None;
            let unique = slot.unique.take();
            f(slot.ctx, ok, unique, slot.sge);
            q.head += 1;
        }
    }
    n
}

fn careful(rounds: usize) {
    let RawSetup {
        model,
        pd,
        mut cq,
        a,
        b,
    } = raw_setup();
    let mut src = vec![1u8; SIZE];
    let mut dst: Box<[u8]> = vec![0u8; SIZE].into_boxed_slice();
    let access = Access::LOCAL_WRITE;
    // SAFETY: both stay in place until the QPs are gone.
    let (m1, m2) = unsafe {
        (
            model.reg_mr(&pd, src.as_mut_ptr(), SIZE, access).unwrap(),
            model.reg_mr(&pd, dst.as_mut_ptr(), SIZE, access).unwrap(),
        )
    };
    let ssge = Sge {
        addr: src.as_ptr() as u64,
        length: SIZE as u32,
        lkey: Model::mr_keys(&m1).0,
    };
    let rsge = Sge {
        addr: dst.as_ptr() as u64,
        length: SIZE as u32,
        lkey: Model::mr_keys(&m2).0,
    };
    let src = Arc::new(src);
    let mut conns = vec![Conn::new(a), Conn::new(b)];
    let mut wcs = [Wc::default(); 16];
    let mut spare = Some((dst, rsge));
    let mut ok = 0usize;
    for i in 0..rounds {
        let (buf, sge) = spare.take().unwrap();
        post_recv(&mut conns[1], 1, buf, sge, i as u64);
        post_send(&mut conns[0], 0, src.clone(), ssge, i as u64);
        model.run();
        let mut seen = 0;
        while seen < 2 {
            seen += progress::<1>(&mut cq, &mut conns, &mut wcs, |ctx, good, unique, sge| {
                black_box(ctx);
                ok += good as usize;
                if let Some(b) = unique {
                    spare = Some((b, sge));
                }
            });
        }
    }
    black_box(ok);
    black_box((m1, m2));
}

/// 16 WRITEs of one buffer as one request, of one doorbell and one completion. Every request has
/// 16 WQEs: a WQE's number tells its request, and the queue holds a known number of them.
#[inline(never)]
fn post_batch(c: &mut Conn, idx: u64, buf: Arc<Vec<u8>>, sge: Sge, to: RemoteBuf, ctx: u64) {
    assert!((c.send.tail - c.send.head + 1) * TRAIN as u64 <= DEPTH);
    let seq = c.send.tail;
    let slot = &mut c.send.slots[(seq % DEPTH) as usize];
    slot.shared = Some(buf);
    slot.sge = sge;
    slot.ctx = ctx;
    // SAFETY: the slot keeps the buffer until the request completes.
    unsafe {
        c.qp.start();
        let mut i = 0;
        while i < TRAIN as u64 {
            let wr_id = (idx << 48) | (seq * TRAIN as u64 + i);
            c.qp.post(wr_id, i + 1 == TRAIN as u64, &WrOp::Write(to), &[sge]);
            i += 1;
        }
        c.qp.complete().unwrap();
    }
    c.send.tail = seq + 1;
}

fn batch_careful(rounds: usize) {
    let s = raw_setup();
    let mut src = vec![1u8; SIZE];
    let mut dst = vec![0u8; SIZE];
    let (m1, ssge, _) = register(&s, &mut src);
    let (m2, _, to) = register(&s, &mut dst);
    let RawSetup {
        model,
        mut cq,
        a,
        b,
        ..
    } = s;
    let src = Arc::new(src);
    let mut conns = vec![Conn::new(a), Conn::new(b)];
    let mut wcs = [Wc::default(); 16];
    let mut ok = 0usize;
    for i in 0..rounds {
        post_batch(&mut conns[0], 0, src.clone(), ssge, to, i as u64);
        model.run();
        let mut seen = 0;
        while seen < 1 {
            seen +=
                progress::<{ TRAIN as u64 }>(&mut cq, &mut conns, &mut wcs, |ctx, good, _, _| {
                    black_box(ctx);
                    ok += good as usize;
                });
        }
    }
    black_box(ok);
    black_box((m1, m2));
}

/// 16 SENDs of one buffer as 16 requests of one doorbell, only the last signaled.
#[inline(never)]
fn post_train(c: &mut Conn, idx: u64, buf: &Arc<Vec<u8>>, sge: Sge, ctx: u64) {
    // SAFETY: the slots keep the buffer until the requests complete.
    unsafe { c.qp.start() };
    let mut i = 0;
    while i < TRAIN {
        assert!(c.send.tail - c.send.head < DEPTH);
        let seq = c.send.tail;
        let slot = &mut c.send.slots[(seq % DEPTH) as usize];
        slot.shared = Some(buf.clone());
        slot.sge = sge;
        slot.ctx = ctx;
        // SAFETY: as above.
        unsafe {
            c.qp.post((idx << 48) | seq, i + 1 == TRAIN, &WrOp::Send, &[sge])
        };
        c.send.tail = seq + 1;
        i += 1;
    }
    // SAFETY: as above.
    unsafe { c.qp.complete().unwrap() };
}

fn train_careful(rounds: usize) {
    let RawSetup {
        model,
        pd,
        mut cq,
        a,
        b,
    } = raw_setup();
    let mut src = vec![1u8; SIZE];
    // SAFETY: both stay in place until the QPs are gone.
    let m1 = unsafe {
        model
            .reg_mr(&pd, src.as_mut_ptr(), SIZE, Access::LOCAL_WRITE)
            .unwrap()
    };
    let ssge = Sge {
        addr: src.as_ptr() as u64,
        length: SIZE as u32,
        lkey: Model::mr_keys(&m1).0,
    };
    let mut bufs: Vec<(Box<[u8]>, Sge)> = Vec::new();
    let mut regions = Vec::new();
    for _ in 0..TRAIN {
        let mut dst: Box<[u8]> = vec![0u8; SIZE].into_boxed_slice();
        // SAFETY: as above.
        let mr = unsafe {
            model
                .reg_mr(&pd, dst.as_mut_ptr(), SIZE, Access::LOCAL_WRITE)
                .unwrap()
        };
        let sge = Sge {
            addr: dst.as_ptr() as u64,
            length: SIZE as u32,
            lkey: Model::mr_keys(&mr).0,
        };
        regions.push(mr);
        bufs.push((dst, sge));
    }
    let src = Arc::new(src);
    let mut conns = vec![Conn::new(a), Conn::new(b)];
    let mut wcs = [Wc::default(); 16];
    let mut ok = 0usize;
    for i in 0..rounds {
        while let Some((buf, sge)) = bufs.pop() {
            post_recv(&mut conns[1], 1, buf, sge, i as u64);
        }
        post_train(&mut conns[0], 0, &src, ssge, i as u64);
        model.run();
        let mut seen = 0;
        while seen < TRAIN + 1 {
            seen += progress::<1>(&mut cq, &mut conns, &mut wcs, |ctx, good, unique, sge| {
                black_box(ctx);
                ok += good as usize;
                if let Some(b) = unique {
                    bufs.push((b, sge));
                }
            });
        }
    }
    black_box(ok);
    black_box((m1, regions));
}

// -----------------------------------------------------------------------------
// The library, through the event loop's handles

struct Setup {
    model: Model,
    pd: Pd<Model>,
    cq: LocalCq<u64, Model>,
    local: LocalQp<RC, u64, Model>,
    peer: LocalQp<RC, u64, Model>,
}

fn setup() -> Setup {
    let model = Model::new(7);
    let ctx = Context::new(model.clone());
    let pd = ctx.alloc_pd().unwrap();
    let cq: LocalCq<u64, Model> = ctx.create_local_cq(4 * DEPTH as u32).unwrap();
    let build = || {
        QueuePairBuilder::<RC, Model>::new(&pd)
            .set_caps(caps())
            .set_max_batch(TRAIN as u32)
            .build_on(&cq)
            .unwrap()
    };
    let (a, b) = (build(), build());
    let (ea, eb) = (a.endpoint(), b.endpoint());
    let local = a.handshake(eb).unwrap();
    let peer = b.handshake(ea).unwrap();
    Setup {
        model,
        pd,
        cq,
        local,
        peer,
    }
}

fn frozen(pd: &Pd<Model>) -> Buf<u8, Model> {
    let mut src = pd.alloc::<u8>(SIZE).unwrap();
    src.fill(1);
    src.freeze()
}

/// The event loop: `poll` hands every completion to a closure.
fn eventloop(rounds: usize) {
    let Setup {
        model,
        pd,
        cq,
        local,
        peer,
    } = setup();
    let src = frozen(&pd);
    let mut rbuf = Some(pd.alloc::<u8>(SIZE).unwrap());
    let mut ok = 0usize;
    for i in 0..rounds {
        peer.post_recv(rbuf.take().unwrap(), i as u64).unwrap();
        local.post_send(src.slice(..), i as u64).unwrap();
        model.run();
        cq.poll(16, |mut wc| {
            ok += wc.result.is_ok() as usize;
            black_box(wc.wr_id);
            if let Some(buf) = wc.buf_mut() {
                rbuf = Some(buf);
            }
        });
    }
    black_box(ok);
}

/// The event loop, one completion at a time.
fn poll_one(rounds: usize) {
    let Setup {
        model,
        pd,
        cq,
        local,
        peer,
    } = setup();
    let src = frozen(&pd);
    let mut rbuf = Some(pd.alloc::<u8>(SIZE).unwrap());
    let mut ok = 0usize;
    for i in 0..rounds {
        peer.post_recv(rbuf.take().unwrap(), i as u64).unwrap();
        local.post_send(src.slice(..), i as u64).unwrap();
        model.run();
        while let Some(mut wc) = cq.poll_one() {
            ok += wc.result.is_ok() as usize;
            black_box(wc.wr_id);
            if let Some(buf) = wc.buf_mut() {
                rbuf = Some(buf);
            }
        }
    }
    black_box(ok);
}

/// 16 WRITEs of one buffer as one request, on the event loop.
fn batch(rounds: usize) {
    let Setup {
        model,
        pd,
        cq,
        local,
        peer: _peer,
    } = setup();
    let src = frozen(&pd);
    let dst = pd.alloc::<u8>(SIZE).unwrap().expose().unwrap();
    let to = dst.remote();
    let mut ok = 0usize;
    for i in 0..rounds {
        local
            .post_batch(i as u64, |b| {
                for _ in 0..TRAIN {
                    b.write(src.slice(..), to);
                }
            })
            .unwrap();
        model.run();
        cq.poll(16, |wc| {
            ok += wc.result.is_ok() as usize;
            black_box(wc.wr_id);
        });
    }
    black_box(ok);
}

/// 16 SENDs of one buffer as 16 requests of one doorbell, only the last signaled, and 16
/// receives, on the event loop.
fn train(rounds: usize) {
    let Setup {
        model,
        pd,
        cq,
        local,
        peer,
    } = setup();
    let src = frozen(&pd);
    let mut bufs: Vec<_> = (0..TRAIN).map(|_| pd.alloc::<u8>(SIZE).unwrap()).collect();
    let mut ok = 0usize;
    for i in 0..rounds {
        while let Some(buf) = bufs.pop() {
            peer.post_recv(buf, i as u64).unwrap();
        }
        for j in 0..TRAIN {
            local
                .stage(
                    WrOp::Send,
                    src.slice(..).into_part(),
                    i as u64,
                    j + 1 == TRAIN,
                )
                .unwrap();
        }
        local.ring().unwrap();
        model.run();
        let mut seen = 0;
        while seen < 2 * TRAIN {
            seen += cq.poll(16, |mut wc| {
                ok += wc.result.is_ok() as usize;
                black_box(wc.wr_id);
                if let Some(buf) = wc.buf_mut() {
                    bufs.push(buf);
                }
            });
        }
    }
    black_box(ok);
}

// -----------------------------------------------------------------------------
// The library, by index

struct RawLibSetup {
    model: Model,
    pd: safeverbs_core::Pd<Model>,
    engine: Engine<u64, Model>,
    local: QpKey<RC>,
    peer: QpKey<RC>,
}

fn raw_lib_setup() -> RawLibSetup {
    let model = Model::new(7);
    let ctx = safeverbs_core::Context::new(model.clone());
    let pd = ctx.alloc_pd().unwrap();
    let mut engine: Engine<u64, Model> = Engine::new(&ctx, 4 * DEPTH as u32).unwrap();
    let local = engine.create_qp::<RC>(&pd, caps(), TRAIN as u32).unwrap();
    let peer = engine.create_qp::<RC>(&pd, caps(), TRAIN as u32).unwrap();
    let e = engine.endpoint(peer, 1, None).unwrap();
    engine.connect(local, &attr(), &e).unwrap();
    let e = engine.endpoint(local, 1, None).unwrap();
    engine.connect(peer, &attr(), &e).unwrap();
    RawLibSetup {
        model,
        pd,
        engine,
        local,
        peer,
    }
}

/// The engine's event loop, with no reference count or `RefCell` on the way.
fn engine_loop(rounds: usize) {
    let RawLibSetup {
        model,
        pd,
        mut engine,
        local,
        peer,
    } = raw_lib_setup();
    let mut src = pd.alloc::<u8>(SIZE).unwrap();
    src.fill(1);
    let src = src.freeze();
    let mut rbuf = Some(pd.alloc::<u8>(SIZE).unwrap());
    let mut ok = 0usize;
    for i in 0..rounds {
        engine.recv(peer, rbuf.take().unwrap(), i as u64).unwrap();
        engine.send(local, src.slice(..), i as u64).unwrap();
        model.run();
        engine.poll(16, |_, mut c| {
            ok += c.result.is_ok() as usize;
            black_box(c.wr_id);
            if let Some(buf) = c.leases.take_buf_mut() {
                rbuf = Some(buf);
            }
        });
    }
    black_box(ok);
}

/// The owners' path, what the futures use: progress, then take by number.
fn take(rounds: usize) {
    let RawLibSetup {
        model,
        pd,
        mut engine,
        local,
        peer,
    } = raw_lib_setup();
    let mut src = pd.alloc::<u8>(SIZE).unwrap();
    src.fill(1);
    let src = src.freeze();
    let mut rbuf = pd.alloc::<u8>(SIZE).unwrap();
    let mut ok = 0usize;
    for i in 0..rounds {
        let r: Seq = engine.recv(peer, rbuf, i as u64).unwrap();
        let s: Seq = engine.send(local, src.slice(..), i as u64).unwrap();
        model.run();
        engine.progress();
        let c = engine.take_send(local, s).unwrap();
        ok += c.result.is_ok() as usize;
        let c = engine.take_recv(peer, r).unwrap();
        ok += c.result.is_ok() as usize;
        rbuf = c.leases.into_buf_mut::<u8>().unwrap();
    }
    black_box(ok);
}

// -----------------------------------------------------------------------------
// The library, with futures

/// Runs `f` to completion, polling it in a loop with a waker that does nothing, so that what is
/// counted is the library's and not an executor's.
fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    let waker = noop_waker();
    let mut cx = TaskContext::from_waker(&waker);
    loop {
        if let Poll::Ready(out) = Pin::as_mut(&mut f).poll(&mut cx) {
            return out;
        }
    }
}

struct AsyncSetup {
    model: Model,
    pd: Pd<Model>,
    local: AsyncQp<RC, Model>,
    peer: AsyncQp<RC, Model>,
}

fn async_setup() -> AsyncSetup {
    let model = Model::new(7);
    let ctx = Context::new(model.clone());
    let pd = ctx.alloc_pd().unwrap();
    let cq = ctx.create_async_cq(4 * DEPTH as u32).unwrap();
    let build = || {
        QueuePairBuilder::<RC, Model>::new(&pd)
            .set_caps(caps())
            .set_max_batch(TRAIN as u32)
            .build_on(&cq)
            .unwrap()
    };
    let (a, b) = (build(), build());
    let (ea, eb) = (a.endpoint(), b.endpoint());
    let local = a.handshake(eb).unwrap();
    let peer = b.handshake(ea).unwrap();
    AsyncSetup {
        model,
        pd,
        local,
        peer,
    }
}

/// A SEND and a receive as futures: each is ready at its first poll.
fn async_round(rounds: usize) {
    let AsyncSetup {
        model,
        pd,
        local,
        peer,
    } = async_setup();
    let src = frozen(&pd);
    let mut rbuf = Some(pd.alloc::<u8>(SIZE).unwrap());
    let mut ok = 0usize;
    block_on(async {
        for _ in 0..rounds {
            let r = peer.recv(rbuf.take().unwrap());
            let s = local.send(src.slice(..));
            model.run();
            s.await.unwrap();
            let (received, buf) = r.await.unwrap();
            ok += matches!(received, Received::Message { .. }) as usize;
            rbuf = Some(buf);
        }
    });
    black_box(ok);
}

/// 16 WRITEs of one buffer as one request, as a future.
fn async_batch(rounds: usize) {
    let AsyncSetup {
        model,
        pd,
        local,
        peer: _peer,
    } = async_setup();
    let src = frozen(&pd);
    let dst = pd.alloc::<u8>(SIZE).unwrap().expose().unwrap();
    let to = dst.remote();
    let mut ok = 0usize;
    block_on(async {
        for _ in 0..rounds {
            let f = local.batch(|b| {
                for _ in 0..TRAIN {
                    b.write(src.slice(..), to);
                }
            });
            model.run();
            f.await.unwrap();
            ok += 1;
        }
    });
    black_box(ok);
}

/// 16 SENDs of one buffer and 16 receives, as futures.
fn async_train(rounds: usize) {
    let AsyncSetup {
        model,
        pd,
        local,
        peer,
    } = async_setup();
    let src = frozen(&pd);
    let mut bufs: Vec<_> = (0..TRAIN).map(|_| pd.alloc::<u8>(SIZE).unwrap()).collect();
    let mut recvs = Vec::with_capacity(TRAIN);
    let mut sends = Vec::with_capacity(TRAIN);
    let mut ok = 0usize;
    block_on(async {
        for _ in 0..rounds {
            while let Some(buf) = bufs.pop() {
                recvs.push(peer.recv(buf));
            }
            for _ in 0..TRAIN {
                sends.push(local.send(src.slice(..)));
            }
            model.run();
            for s in sends.drain(..) {
                s.await.unwrap();
            }
            for r in recvs.drain(..) {
                let (received, buf) = r.await.unwrap();
                ok += matches!(received, Received::Message { .. }) as usize;
                bufs.push(buf);
            }
        }
    });
    black_box(ok);
}

/// 16 WRITEs of one buffer through a sink, and a flush.
fn sink(rounds: usize) {
    let AsyncSetup {
        model,
        pd,
        local,
        peer: _peer,
    } = async_setup();
    let src = frozen(&pd);
    let dst = pd.alloc::<u8>(SIZE).unwrap().expose().unwrap();
    let to = dst.remote();
    let mut tx = local.sink();
    let mut ok = 0usize;
    block_on(async {
        for _ in 0..rounds {
            for _ in 0..TRAIN {
                tx.feed(WriteOp::new(src.slice(..), to)).await.unwrap();
            }
            tx.flush().await.unwrap();
            model.run();
            local.poll();
            ok += 1;
        }
    });
    black_box(ok);
}

/// The bytes of what the data path moves, and of the handles.
fn sizes() {
    use core::mem::size_of;
    use safeverbs_core::ring::Ring;
    type Payload<C> = safeverbs_core::ring::Payload<C, safeverbs::Verbs>;
    type Fut<'q> = safeverbs::Op<'q, AsyncQp<RC>, safeverbs::Sent>;
    println!("slot, wr_id u64  {}", Ring::<Payload<u64>>::SLOT_BYTES);
    println!("slot, futures    {}", Ring::<Payload<()>>::SLOT_BYTES);
    println!("QP in the table  {}", Engine::<u64>::QP_BYTES);
    println!("Wc<u64>          {}", size_of::<safeverbs::Wc<u64>>());
    println!("Leases           {}", size_of::<safeverbs::Leases>());
    println!("LocalCq          {}", size_of::<LocalCq>());
    println!("LocalQp          {}", size_of::<LocalQp<RC>>());
    println!("AsyncQp          {}", size_of::<AsyncQp<RC>>());
    println!("a future         {}", size_of::<Fut<'static>>());
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let which = args.get(1).map(String::as_str).unwrap_or("none");
    let rounds: usize = args.get(2).map_or(0, |s| s.parse().unwrap());
    match which {
        "raw" => raw(rounds),
        "careful" => careful(rounds),
        "batch_careful" => batch_careful(rounds),
        "train_careful" => train_careful(rounds),
        "eventloop" => eventloop(rounds),
        "poll_one" => poll_one(rounds),
        "engine" => engine_loop(rounds),
        "take" => take(rounds),
        "batch" => batch(rounds),
        "train" => train(rounds),
        "async" => async_round(rounds),
        "async_batch" => async_batch(rounds),
        "async_train" => async_train(rounds),
        "sink" => sink(rounds),
        "sizes" => sizes(),
        other => panic!("no workload {other}"),
    }
}
