//! Engines on the simulated NIC ([`Model`]): several QPs on one CQ, driven
//! by the event loop ([`Engine::poll`]) through random operations, dropped
//! (detached) requests, injected errors, flushes and refused doorbells.
//!
//! Throughout, every ring's invariants hold and the model's CQ is never
//! overrun (the model panics if it is: the CQ is exactly as large as the
//! QPs reserve); every completion reaches the QP and request it belongs to,
//! in each queue's order, exactly once, with that request's own context and
//! buffers; at the end every context was dropped once, every buffer is back
//! or released and nothing stays registered.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use safeverbs_core::model::Model;
use safeverbs_core::{
    Access, Buf, BufMut, Completed, CompletionError, ConnectAttr, Context, Engine, Pd, QpCaps,
    QpKey, Queue, RC, Received, Status, WrOp,
};

/// A context of an owned type: it counts its drops, to check that every one
/// made ends exactly once, whatever becomes of its request.
struct Job {
    /// Which one it is.
    id: u64,
    /// Drops so far, of all of them.
    drops: Rc<Cell<u64>>,
}

impl Drop for Job {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

/// Makes numbered [`Job`]s.
#[derive(Default)]
struct Jobs {
    made: u64,
    drops: Rc<Cell<u64>>,
}

impl Jobs {
    fn next(&mut self) -> Job {
        self.made += 1;
        Job {
            id: self.made,
            drops: self.drops.clone(),
        }
    }
}

/// A deterministic generator.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn caps(depth: u32) -> QpCaps {
    QpCaps {
        max_send_wr: depth,
        max_recv_wr: depth,
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

/// `pairs` pairs of RC QPs on one worker, each connected to the other QP
/// of its pair; the worker's CQ holds exactly their queues.
fn worker(pd: &Pd<Model>, pairs: usize, depth: u32) -> (Engine<Job, Model>, Vec<QpKey<RC>>) {
    let mut w = Engine::new(pd.context(), (4 * pairs) as u32 * depth).unwrap();
    let qps: Vec<QpKey<RC>> = (0..2 * pairs)
        .map(|_| {
            w.create_qp::<RC>(pd, caps(depth), depth.div_ceil(2))
                .unwrap()
        })
        .collect();
    for pair in qps.chunks(2) {
        let (ea, eb) = (
            w.endpoint(pair[0], 1, None).unwrap(),
            w.endpoint(pair[1], 1, None).unwrap(),
        );
        w.connect(pair[0], &attr(), &eb).unwrap();
        w.connect(pair[1], &attr(), &ea).unwrap();
    }
    assert_eq!(w.reserved(), w.cq_size());
    (w, qps)
}

/// What a request was posted to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A SEND or WRITE, or a batch of them, of this many WQEs.
    Sent(u32),
    /// A READ of this length.
    Read(u32),
    /// A receive.
    Recv,
}

/// What a request was posted to do, and what must come back with it.
#[derive(Debug)]
struct Expect {
    /// Its context.
    job: u64,
    kind: Kind,
    /// The address of the unique buffer it holds, which must come back.
    buf: Option<usize>,
}

#[test]
fn event_loop_on_several_qps() {
    // What the runs covered, checked at the end.
    let (mut delivered, mut failed, mut refused) = (0, 0, 0);
    for seed in 1..80u64 {
        let model = Model::new(seed);
        model.set_pace((seed * 37 % 400) as u32, 1 + (seed % 6) as u32);
        let pd = Context::new(model.clone()).alloc_pd().unwrap();
        let pairs = 1 + (seed % 3) as usize;
        let depth = 2 + (seed % 7) as u32;
        let (mut w, qps) = worker(&pd, pairs, depth);
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let dst = pd.alloc::<u8>(4096).unwrap().expose().unwrap();
        let frozen: Vec<Buf<u8, Model>> = (0..4)
            .map(|_| pd.alloc::<u8>(512).unwrap().freeze())
            .collect();
        let mut free: Vec<BufMut<u8, Model>> =
            (0..24).map(|_| pd.alloc::<u8>(512).unwrap()).collect();
        let mut expected: HashMap<(usize, Queue, u64), Expect> = HashMap::new();
        let mut last: HashMap<(usize, Queue), u64> = HashMap::new();
        let mut counter = 0u32;
        let mut jobs = Jobs::default();
        let mut seen = HashSet::new();

        for step in 0..400 {
            let q = qps[rng.below(qps.len() as u64) as usize];
            let i = q.index();
            match rng.below(12) {
                // A SEND or WRITE of a frozen buffer, owned or not, signaled
                // or not, committed now or later.
                0..=2 => {
                    if !w.send_ring(q).can_post() {
                        continue;
                    }
                    counter += 1;
                    let len = 1 + (counter * 7919) % 250;
                    let f = &frozen[rng.below(4) as usize];
                    let op = if rng.below(2) == 0 {
                        WrOp::Send
                    } else {
                        WrOp::Write(dst.remote().offset(rng.below(3000)))
                    };
                    let detached = rng.below(4) == 0;
                    let signal = rng.below(3) != 0;
                    let mut req = w.request(q);
                    req.push(op, f.slice(..len as usize).into_part()).unwrap();
                    if detached {
                        req.finish_detached(signal).unwrap();
                    } else {
                        let job = jobs.next();
                        let id = job.id;
                        let seq = req.finish(signal, job).ok().unwrap();
                        expected.insert(
                            (i, Queue::Send, seq),
                            Expect {
                                job: id,
                                kind: Kind::Sent(1),
                                buf: None,
                            },
                        );
                    }
                    if rng.below(2) == 0 {
                        let _ = w.commit(q);
                    }
                }
                // A batch of WRITEs, as one request.
                3 => {
                    let ring = w.send_ring(q);
                    let n = 1 + rng.below(ring.max_wqes() as u64) as u32;
                    if !ring.can_post() || ring.depth() - ring.in_flight_wqes() < n {
                        continue;
                    }
                    let mut req = w.request(q);
                    for _ in 0..n {
                        let l = 1 + rng.below(64) as u32;
                        req.push(
                            WrOp::Write(dst.remote()),
                            frozen[0].slice(..l as usize).into_part(),
                        )
                        .unwrap();
                    }
                    let job = jobs.next();
                    let id = job.id;
                    match req.submit(true, job) {
                        Ok(seq) => {
                            expected.insert(
                                (i, Queue::Send, seq),
                                Expect {
                                    job: id,
                                    kind: Kind::Sent(n),
                                    buf: None,
                                },
                            );
                        }
                        // Refused: its context and buffers come back at once.
                        Err(e) => {
                            refused += 1;
                            assert_eq!(e.wr_id.id, id);
                            assert!(!e.leases.is_empty());
                        }
                    }
                }
                // A READ into a unique buffer.
                4 => {
                    if !w.send_ring(q).can_post() {
                        continue;
                    }
                    let Some(buf) = free.pop() else { continue };
                    let ptr = buf.as_ptr() as usize;
                    let len = 1 + rng.below(512) as u32;
                    // The rest of the buffer is not used, and released.
                    let (part, _rest) = buf.split_at(len as usize);
                    let mut req = w.request(q);
                    req.push(
                        WrOp::Read(dst.remote().slice(..len as u64)),
                        part.into_part(),
                    )
                    .unwrap();
                    let job = jobs.next();
                    let id = job.id;
                    match req.submit(true, job) {
                        Ok(seq) => {
                            expected.insert(
                                (i, Queue::Send, seq),
                                Expect {
                                    job: id,
                                    kind: Kind::Read(len),
                                    buf: Some(ptr),
                                },
                            );
                        }
                        Err(e) => {
                            refused += 1;
                            assert_eq!(e.wr_id.id, id);
                            let back = e.leases.into_buf_mut::<u8>().unwrap();
                            assert_eq!(back.as_ptr() as usize, ptr);
                        }
                    }
                }
                // A receive.
                5 | 6 => {
                    let Some(buf) = free.pop() else { continue };
                    let ptr = buf.as_ptr() as usize;
                    let job = jobs.next();
                    let id = job.id;
                    match w.post_recv(q, buf.into_part(), job) {
                        Ok(seq) => {
                            expected.insert(
                                (i, Queue::Recv, seq),
                                Expect {
                                    job: id,
                                    kind: Kind::Recv,
                                    buf: Some(ptr),
                                },
                            );
                        }
                        Err(e) => {
                            assert_eq!(e.wr_id.id, id);
                            free.push(e.leases.into_buf_mut().unwrap());
                        }
                    }
                }
                // Drop a request: it goes on, its buffers are released when
                // it retires, and its context comes back now.
                7 => {
                    let mine: Vec<_> = expected.keys().filter(|k| k.0 == i).copied().collect();
                    if let Some(&(_, queue, seq)) =
                        mine.get(rng.below(mine.len() as u64 + 1) as usize)
                    {
                        let e = expected.remove(&(i, queue, seq)).unwrap();
                        let ctx = match queue {
                            Queue::Send => w.detach_send(q, seq),
                            Queue::Recv => w.detach_recv(q, seq),
                        };
                        assert_eq!(
                            ctx.map(|c| c.id),
                            Some(e.job),
                            "seed {seed}: its own context"
                        );
                    }
                }
                // Faults: an error, a flush, refused doorbells.
                8 => match rng.below(40) {
                    0 => model.inject_error(w.qp_num(q), Status::REM_OP_ERR),
                    1 => model.force_error(w.qp_num(q)),
                    2..=7 => model.refuse_doorbells(w.qp_num(q), 1),
                    _ => {}
                },
                9 => {
                    let _ = w.commit(q);
                }
                // The event loop.
                _ => {
                    w.poll(32, |w, c| {
                        let e = expected.remove(&(c.qp, c.queue, c.seq)).unwrap_or_else(|| {
                            panic!(
                                "seed {seed} step {step}: an unknown or repeated completion {:?}",
                                (c.qp, c.queue, c.seq, c.result)
                            )
                        });
                        assert_eq!(c.wr_id.id, e.job, "seed {seed}: another request's context");
                        assert!(seen.insert(e.job), "seed {seed}: a context came back twice");
                        // In each queue's order, but for refused requests, which come at once.
                        if c.result != Err(CompletionError::Refused) {
                            let prev = last.insert((c.qp, c.queue), c.seq);
                            assert!(prev.is_none_or(|p| p < c.seq), "seed {seed}: out of order");
                        }
                        delivered += 1;
                        match c.result {
                            Ok(done) => match (e.kind, done) {
                                (Kind::Sent(_), Completed::Sent) => {}
                                (Kind::Read(len), Completed::Read { len: got }) => {
                                    assert_eq!(got, len, "seed {seed} step {step}: {e:?}");
                                }
                                (
                                    Kind::Recv,
                                    Completed::Received(Received::Message { imm: None, .. }),
                                ) => {}
                                other => panic!("seed {seed} step {step}: {e:?} gave {other:?}"),
                            },
                            Err(CompletionError::Failed { position, .. }) => {
                                failed += 1;
                                let wqes = match e.kind {
                                    Kind::Sent(wqes) => wqes,
                                    Kind::Read(_) | Kind::Recv => 1,
                                };
                                assert!(position < wqes, "seed {seed}: {e:?} at WQE {position}");
                            }
                            Err(CompletionError::Refused) => {
                                assert_eq!(c.queue, Queue::Send, "seed {seed}");
                            }
                        }
                        match e.buf {
                            Some(ptr) => {
                                let buf = c.leases.into_buf_mut::<u8>().expect("a unique buffer");
                                assert_eq!(
                                    buf.as_ptr() as usize,
                                    ptr,
                                    "seed {seed}: another buffer"
                                );
                                // A receive's buffer is whole: used again.
                                if e.kind == Kind::Recv {
                                    let _ = w.post_recv_detached(qps[c.qp], buf.into_part());
                                }
                            }
                            None => assert!(!c.leases.is_empty()),
                        }
                    });
                }
            }
            for &k in &qps {
                assert!(
                    w.send_ring(k).invariant() && w.recv_ring(k).invariant(),
                    "seed {seed}"
                );
            }
            if rng.below(50) == 0 {
                let _ = w.take_send_error(q);
            }
        }

        // Settle: every QP in error flushes what it holds, then the loop
        // gets every owned request back.
        for &k in &qps {
            let _ = w.commit(k);
            w.to_error(k).unwrap();
        }
        // A signaled request on each send queue retires the unsignaled ones
        // before it (refused doorbells may have taken theirs).
        for &k in &qps {
            loop {
                let mut req = w.request(k);
                req.push(WrOp::Write(dst.remote()), frozen[0].slice(..1).into_part())
                    .unwrap();
                if req.submit_detached().is_ok() {
                    break;
                }
            }
        }
        model.set_pace(0, 8);
        let mut rounds = 0;
        while !expected.is_empty() {
            model.run();
            w.poll(32, |_, c| {
                let e = expected
                    .remove(&(c.qp, c.queue, c.seq))
                    .expect("a known request");
                assert_eq!(c.wr_id.id, e.job, "seed {seed}: another request's context");
                assert!(seen.insert(e.job), "seed {seed}: a context came back twice");
            });
            rounds += 1;
            assert!(
                rounds < 10_000,
                "seed {seed}: requests never came back: {expected:?}"
            );
        }
        // The requests without an owner retire too.
        for _ in 0..1000 {
            model.run();
            if model.pending() == 0 && w.progress() == 0 {
                break;
            }
        }
        for &k in &qps {
            let ring = w.send_ring(k);
            assert_eq!(
                ring.retired(),
                ring.committed(),
                "seed {seed}: a request still in flight"
            );
        }
        drop(w);
        assert_eq!(
            jobs.drops.get(),
            jobs.made,
            "seed {seed}: every context ends exactly once"
        );
        for f in frozen {
            assert!(
                f.try_thaw().is_ok(),
                "seed {seed}: a frozen buffer still held"
            );
        }
        drop((free, dst));
        assert_eq!(model.registered(), 0, "seed {seed}: a region leaked");
    }
    eprintln!(
        "{delivered} completions handed out, {failed} with errors, {refused} refused doorbells handed back"
    );
    assert!(delivered > 2000 && failed > 100 && refused > 20);
}

/// Engines and QPs removed or dropped at random moments, with work in
/// flight: sends signaled or not, batches, READs, receives, dropped
/// requests, errors, flushes and refused doorbells. The model panics if a
/// QP is destroyed with a WQE not retired by a polled completion (M's
/// `destroy_qp`) or a CQ with a QP attached (M's `destroy_cq`); afterwards
/// every buffer is back and nothing stays registered.
#[test]
fn removing_busy_qps_and_dropping_busy_workers() {
    let mut removed = 0;
    for seed in 1..120u64 {
        let model = Model::new(seed);
        model.set_pace((seed * 53 % 900) as u32, 1 + (seed % 5) as u32);
        let pd = Context::new(model.clone()).alloc_pd().unwrap();
        let pairs = 1 + (seed % 3) as usize;
        let depth = 2 + (seed % 7) as u32;
        let (mut w, qps) = worker(&pd, pairs, depth);
        let mut live: Vec<QpKey<RC>> = qps.clone();
        let mut jobs = Jobs::default();
        let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1);
        let dst = pd.alloc::<u8>(4096).unwrap().expose().unwrap();
        let frozen = pd.alloc::<u8>(512).unwrap().freeze();
        let steps = rng.below(300);
        for _ in 0..steps {
            if live.is_empty() {
                break;
            }
            let q = live[rng.below(live.len() as u64) as usize];
            match rng.below(14) {
                0..=3 => {
                    // Only what fits now: waiting for room could wait for
                    // a receive the peer never posts.
                    let ring = w.send_ring(q);
                    let n = 1 + rng.below(ring.max_wqes() as u64) as u32;
                    if !ring.can_post() || ring.depth() - ring.in_flight_wqes() < n {
                        continue;
                    }
                    let mut req = w.request(q);
                    for _ in 0..n {
                        let len = 1 + rng.below(200) as usize;
                        let op = if rng.below(2) == 0 {
                            WrOp::Send
                        } else {
                            WrOp::Write(dst.remote().offset(rng.below(3000)))
                        };
                        if req.push(op, frozen.slice(..len).into_part()).is_err() {
                            break;
                        }
                    }
                    let signal = rng.below(3) == 0;
                    if rng.below(2) == 0 {
                        let _ = req.finish_detached(signal);
                    } else {
                        let _ = req.finish(signal, jobs.next());
                    }
                    if rng.below(2) == 0 {
                        let _ = w.commit(q);
                    }
                }
                4 => {
                    if !w.send_ring(q).can_post() {
                        continue;
                    }
                    let buf = pd.alloc::<u8>(64).unwrap();
                    let mut req = w.request(q);
                    req.push(WrOp::Read(dst.remote().slice(..64)), buf.into_part())
                        .unwrap();
                    let signal = rng.below(2) == 0;
                    if rng.below(2) == 0 {
                        let _ = req.finish_detached(signal);
                    } else {
                        let _ = req.finish(signal, jobs.next());
                    }
                }
                5..=7 => {
                    let buf = pd.alloc::<u8>(256).unwrap();
                    if rng.below(2) == 0 {
                        let _ = w.post_recv_detached(q, buf.into_part());
                    } else {
                        let _ = w.post_recv(q, buf.into_part(), jobs.next());
                    }
                }
                8 => match rng.below(10) {
                    0 => model.inject_error(w.qp_num(q), Status::REM_OP_ERR),
                    1 => model.force_error(w.qp_num(q)),
                    2..=4 => model.refuse_doorbells(w.qp_num(q), 1),
                    _ => {}
                },
                9 => {
                    let _ = w.commit(q);
                }
                10 => {
                    if rng.below(8) == 0 {
                        w.remove_qp(q).unwrap();
                        live.retain(|&k| k != q);
                        removed += 1;
                    }
                }
                _ => {
                    w.poll(32, |_, c| drop(c));
                }
            }
        }
        // Drop the worker as it is, with whatever is in flight or written.
        drop(w);
        assert_eq!(
            jobs.drops.get(),
            jobs.made,
            "seed {seed}: every context ends exactly once"
        );
        assert!(
            frozen.try_thaw().is_ok(),
            "seed {seed}: a frozen buffer still held"
        );
        drop((dst, pd));
        assert_eq!(model.registered(), 0, "seed {seed}: a region leaked");
    }
    assert!(removed > 20, "{removed} QPs removed");
}

/// Several QPs with requests of each kind, on the event loop: each completion comes with the
/// context of its own request, on its own QP, with its own buffers.
#[test]
fn contexts_come_back_with_their_own_requests() {
    let model = Model::new(201);
    let pd = Context::new(model.clone()).alloc_pd().unwrap();
    let (mut w, qps) = worker(&pd, 2, 16);
    let dst = pd.alloc::<u8>(4096).unwrap().expose().unwrap();
    let mut jobs = Jobs::default();
    let mut want = HashMap::new();
    for &qp in &qps {
        for i in 0..4usize {
            let frozen = pd.alloc::<u8>(64).unwrap().freeze();
            let job = jobs.next();
            want.insert(job.id, (qp.index(), Queue::Recv, None));
            let buf = pd.alloc::<u8>(64).unwrap();
            let seq = w.recv(qp, buf, job).unwrap();
            assert_eq!(seq as usize, i);
            let job = jobs.next();
            let keeper = frozen.clone();
            want.insert(job.id, (qp.index(), Queue::Send, Some(keeper)));
            w.write(qp, frozen, dst.remote(), job).unwrap();
        }
        let job = jobs.next();
        want.insert(job.id, (qp.index(), Queue::Send, None));
        let mut req = w.request(qp);
        for _ in 0..3 {
            let f = pd.alloc::<u8>(8).unwrap().freeze();
            req.push(WrOp::Write(dst.remote()), f.into_part()).unwrap();
        }
        req.submit(true, job).unwrap();
    }
    // The receives wait for messages: each QP SENDs its peer four.
    for &qp in &qps {
        for _ in 0..4 {
            let job = jobs.next();
            want.insert(job.id, (qp.index(), Queue::Send, None));
            w.send(qp, pd.alloc::<u8>(8).unwrap().freeze(), job)
                .unwrap();
        }
    }
    let made = jobs.made;
    let drops = jobs.drops.clone();
    let delivered = RefCell::new(0u64);
    let mut rounds = 0;
    while !want.is_empty() {
        model.run();
        w.poll(32, |_, c| {
            let (qp, queue, keeper) = want
                .remove(&c.wr_id.id)
                .unwrap_or_else(|| panic!("job {} came back twice", c.wr_id.id));
            assert_eq!((c.qp, c.queue), (qp, queue), "job {}", c.wr_id.id);
            assert!(c.result.is_ok(), "{:?}", c.result);
            *delivered.borrow_mut() += 1;
            if let Some(keeper) = keeper {
                // Its lease is the only other handle: gone with it, it thaws.
                drop(c);
                assert!(keeper.try_thaw().is_ok(), "another request's buffer");
            }
        });
        rounds += 1;
        assert!(rounds < 1000, "requests never came back: {}", want.len());
    }
    assert_eq!(*delivered.borrow(), made);
    assert_eq!(drops.get(), made, "dropped by the loop, once each");
}

/// Refused requests are told apart from flushed ones: the NIC never saw the former, and did
/// fail the latter.
#[test]
fn a_refused_request_is_not_a_flush() {
    let model = Model::new(202);
    let pd = Context::new(model.clone()).alloc_pd().unwrap();
    let (mut w, qps) = worker(&pd, 1, 8);
    let (a, _b) = (qps[0], qps[1]);
    let frozen = pd.alloc::<u8>(8).unwrap().freeze();
    let mut jobs = Jobs::default();

    // Two requests staged, the doorbell refused.
    let mut req = w.request(a);
    req.push(WrOp::Send, frozen.slice(..).into_part()).unwrap();
    let first = jobs.next();
    let first_id = first.id;
    let refused = req.finish(true, first).ok().unwrap();
    let mut req = w.request(a);
    req.push(WrOp::Send, frozen.slice(..).into_part()).unwrap();
    req.finish_detached(true).unwrap();
    model.refuse_doorbells(w.qp_num(a), 1);
    assert!(w.commit(a).is_err());
    let c = w.take_send(a, refused).unwrap();
    assert_eq!(c.result, Err(CompletionError::Refused));
    assert_eq!(c.result.unwrap_err().status(), None);
    assert_eq!(c.wr_id.id, first_id);
    drop(c);
    // The one without an owner reports the same, by its error.
    let detached = w.take_send_error(a).unwrap();
    assert_eq!(detached.seq, refused + 1);
    assert_eq!(detached.error, CompletionError::Refused);

    // In the error state, a request reaches the NIC and is flushed.
    w.to_error(a).unwrap();
    let flushed = w.send(a, frozen.slice(..), jobs.next()).unwrap();
    model.run();
    w.progress();
    let c = w.take_send(a, flushed).unwrap();
    assert_eq!(
        c.result,
        Err(CompletionError::Failed {
            status: Status::WR_FLUSH_ERR,
            vendor_err: 0,
            position: 0
        })
    );
    drop(c);
    drop(w);
    assert_eq!(jobs.drops.get(), jobs.made);
}

/// The detached error names its request and the position of its failed WQE, as an owned
/// request's error does.
#[test]
fn a_detached_batch_error_has_its_position() {
    let model = Model::new(203);
    model.set_pace(0, 1);
    let pd = Context::new(model.clone()).alloc_pd().unwrap();
    let (mut w, qps) = worker(&pd, 1, 8);
    let a = qps[0];
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let frozen = pd.alloc::<u8>(8).unwrap().freeze();
    let mut jobs = Jobs::default();
    let first = w
        .write(a, frozen.slice(..), dst.remote(), jobs.next())
        .unwrap();
    let mut req = w.request(a);
    for _ in 0..3 {
        req.push(WrOp::Write(dst.remote()), frozen.slice(..).into_part())
            .unwrap();
    }
    assert!(req.submit_detached().is_ok());
    // The owned WRITE runs and completes, then one WQE of the batch; then an error.
    assert_eq!(w.progress(), 1);
    assert_eq!(w.progress(), 0);
    model.inject_error(w.qp_num(a), Status::REM_OP_ERR);
    while w.send_ring(a).in_flight_wqes() > 0 {
        w.progress();
    }
    assert!(w.take_send(a, first).unwrap().result.is_ok());
    let failed = w.take_send_error(a).unwrap();
    assert_eq!(failed.seq, first + 1);
    assert_eq!(
        failed.error,
        CompletionError::Failed {
            status: Status::REM_OP_ERR,
            vendor_err: 0,
            position: 1
        }
    );
    assert!(w.take_send_error(a).is_none(), "once");
}

/// Giving up a request hands its context back at once, while it is in flight, and retiring it
/// later runs no destructor; removing a QP or dropping the worker drops the contexts of the
/// requests not delivered.
#[test]
fn undelivered_contexts_are_dropped() {
    let model = Model::new(204);
    model.set_pace(1000, 1);
    let pd = Context::new(model.clone()).alloc_pd().unwrap();
    let (mut w, qps) = worker(&pd, 1, 8);
    let (a, b) = (qps[0], qps[1]);
    let frozen = pd.alloc::<u8>(8).unwrap().freeze();
    let mut jobs = Jobs::default();

    let given_up = w.send(a, frozen.slice(..), jobs.next()).unwrap();
    let ctx = w.detach_send(a, given_up).expect("its context");
    assert_eq!(ctx.id, 1);
    assert_eq!(jobs.drops.get(), 0);
    assert!(w.detach_send(a, given_up).is_none(), "once");
    drop(ctx);
    assert_eq!(jobs.drops.get(), 1);

    w.send(a, frozen.slice(..), jobs.next()).unwrap();
    let buf = pd.alloc::<u8>(8).unwrap();
    w.recv(a, buf, jobs.next()).unwrap();
    assert_eq!(jobs.drops.get(), 1, "still in flight");
    w.remove_qp(a).unwrap();
    assert_eq!(jobs.drops.get(), 3, "gone with their QP");

    w.send(b, frozen.slice(..), jobs.next()).unwrap();
    let buf = pd.alloc::<u8>(8).unwrap();
    w.recv(b, buf, jobs.next()).unwrap();
    assert_eq!(jobs.drops.get(), 3);
    drop(w);
    assert_eq!(jobs.drops.get(), jobs.made, "gone with the worker");
    assert!(frozen.try_thaw().is_ok(), "and the buffers");
}

/// `C = ()` adds nothing to a request's slot but what a context needs anyway.
#[test]
fn a_context_is_generic() {
    fn sizes<C>() -> usize {
        size_of::<safeverbs_core::ring::Payload<C, Model>>()
    }
    assert_eq!(sizes::<()>(), sizes::<bool>());
    assert_eq!(sizes::<u64>(), sizes::<()>() + 8);
}
