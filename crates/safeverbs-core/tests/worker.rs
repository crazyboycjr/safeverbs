//! Workers on the simulated NIC ([`Model`]): several QPs on one CQ, driven
//! by the event loop ([`Worker::poll`]) through random operations, dropped
//! (detached) requests, injected errors, flushes and refused doorbells.
//!
//! Throughout, every ring's invariants hold and the model's CQ is never
//! overrun (the model panics if it is: the CQ is exactly as large as the
//! QPs reserve); every completion reaches the QP and request it belongs to,
//! in each queue's order, exactly once, with that request's own buffers;
//! at the end every buffer is back or released and nothing stays
//! registered.

use std::collections::HashMap;

use safeverbs_core::model::Model;
use safeverbs_core::{
    Access, Buf, BufMut, ConnectAttr, Context, Opcode, Pd, QpCaps, QpKey, Queue, RC, Status,
    Worker, WrOp,
};

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
fn worker(pd: &Pd<Model>, pairs: usize, depth: u32) -> (Worker<Model>, Vec<QpKey<RC>>) {
    let mut w = Worker::new(pd, (4 * pairs) as u32 * depth).unwrap();
    let qps: Vec<QpKey<RC>> = (0..2 * pairs)
        .map(|_| w.create_qp::<RC>(caps(depth), depth.div_ceil(2)).unwrap())
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
#[derive(Debug)]
struct Expect {
    opcode: Opcode,
    /// Its defined byte count, when known (SEND and WRITE have none).
    len: Option<u32>,
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
                    let (op, opcode) = if rng.below(2) == 0 {
                        (WrOp::Send, Opcode::SEND)
                    } else {
                        (
                            WrOp::Write(dst.remote().offset(rng.below(3000))),
                            Opcode::RDMA_WRITE,
                        )
                    };
                    let detached = rng.below(4) == 0;
                    let mut req = w.request(q);
                    req.push(op, f.slice(..len as usize).into_part()).unwrap();
                    let seq = req.finish(rng.below(3) != 0, detached).unwrap();
                    if !detached {
                        expected.insert(
                            (i, Queue::Send, seq),
                            Expect {
                                opcode,
                                len: None,
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
                    match req.submit(true).unwrap() {
                        Ok(seq) => {
                            expected.insert(
                                (i, Queue::Send, seq),
                                Expect {
                                    opcode: Opcode::RDMA_WRITE,
                                    // WRITE completions have no defined byte count.
                                    len: None,
                                    buf: None,
                                },
                            );
                        }
                        // Refused: the buffers come back at once.
                        Err((_, leases)) => {
                            refused += 1;
                            assert!(!leases.is_empty());
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
                    match req.submit(true).unwrap() {
                        Ok(seq) => {
                            expected.insert(
                                (i, Queue::Send, seq),
                                Expect {
                                    opcode: Opcode::RDMA_READ,
                                    len: Some(len),
                                    buf: Some(ptr),
                                },
                            );
                        }
                        Err((_, leases)) => {
                            refused += 1;
                            let back = leases.into_buf_mut::<u8>().unwrap();
                            assert_eq!(back.as_ptr() as usize, ptr);
                        }
                    }
                }
                // A receive.
                5 | 6 => {
                    let Some(buf) = free.pop() else { continue };
                    let ptr = buf.as_ptr() as usize;
                    match w.post_recv(q, buf.into_part(), false) {
                        Ok(seq) => {
                            expected.insert(
                                (i, Queue::Recv, seq),
                                Expect {
                                    opcode: Opcode::RECV,
                                    len: None,
                                    buf: Some(ptr),
                                },
                            );
                        }
                        Err(e) => free.push(e.buf.into_buf_mut().unwrap()),
                    }
                }
                // Drop a request: it goes on, its buffers are released when
                // it retires.
                7 => {
                    let mine: Vec<_> = expected.keys().filter(|k| k.0 == i).copied().collect();
                    if let Some(&(_, queue, seq)) =
                        mine.get(rng.below(mine.len() as u64 + 1) as usize)
                    {
                        expected.remove(&(i, queue, seq));
                        match queue {
                            Queue::Send => w.detach_send(q, seq),
                            Queue::Recv => w.detach_recv(q, seq),
                        }
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
                    w.poll(|w, c| {
                        let e = expected.remove(&(c.qp, c.queue, c.seq)).unwrap_or_else(|| {
                            panic!(
                                "seed {seed} step {step}: an unknown or repeated completion {:?}",
                                (c.qp, c.queue, c.seq, c.wc)
                            )
                        });
                        // In each queue's order, but for refused requests,
                        // which come at once (with flush errors).
                        if c.wc.status != Status::WR_FLUSH_ERR {
                            let prev = last.insert((c.qp, c.queue), c.seq);
                            assert!(prev.is_none_or(|p| p < c.seq), "seed {seed}: out of order");
                        }
                        delivered += 1;
                        if !c.wc.is_ok() {
                            failed += 1;
                            assert_eq!(c.wc.opcode(), None);
                            assert_eq!(c.wc.byte_len(), None);
                            assert_eq!(c.wc.received_len(), None);
                            assert_eq!(c.wc.imm(), None);
                        }
                        if c.wc.is_ok() {
                            assert_eq!(c.wc.opcode(), Some(e.opcode), "seed {seed}: {e:?}");
                            if matches!(e.opcode, Opcode::SEND | Opcode::RDMA_WRITE) {
                                assert_eq!(c.wc.byte_len(), None);
                            }
                            if let Some(len) = e.len {
                                assert_eq!(
                                    c.wc.byte_len(),
                                    Some(len),
                                    "seed {seed} step {step}: {e:?} {:?} qp {} seq {}",
                                    c.wc,
                                    c.qp,
                                    c.seq
                                );
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
                                if e.opcode == Opcode::RECV {
                                    let _ = w.post_recv(qps[c.qp], buf.into_part(), true);
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
            w.poll(|_, c| {
                expected
                    .remove(&(c.qp, c.queue, c.seq))
                    .expect("a known request");
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

/// Workers and QPs removed or dropped at random moments, with work in
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
                    let _ = req.finish(rng.below(3) == 0, rng.below(2) == 0);
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
                    let _ = req.finish(rng.below(2) == 0, rng.below(2) == 0);
                }
                5..=7 => {
                    let buf = pd.alloc::<u8>(256).unwrap();
                    let _ = w.post_recv(q, buf.into_part(), rng.below(2) == 0);
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
                    w.poll(|_, c| drop(c));
                }
            }
        }
        // Drop the worker as it is, with whatever is in flight or written.
        drop(w);
        assert!(
            frozen.try_thaw().is_ok(),
            "seed {seed}: a frozen buffer still held"
        );
        drop((dst, pd));
        assert_eq!(model.registered(), 0, "seed {seed}: a region leaked");
    }
    assert!(removed > 20, "{removed} QPs removed");
}
