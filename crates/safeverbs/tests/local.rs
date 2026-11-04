//! The event loop's handles over the simulated NIC: `wr_id`s come back exactly once with their
//! buffers, polling reads the CQEs before handing any out, so the closure may post, poll and
//! drop QPs, a full queue is an error, and what is refused or flushed still comes back. Random
//! sequences of these end with every `wr_id` back or dropped once and nothing registered.

use std::cell::{Cell, RefCell};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

use safeverbs::{
    Buf, Completed, CompletionError, Context, Error, LocalCq, LocalQp, Model, Pd, QpCaps,
    QueuePairBuilder, RC, Received, Status, Transport, UC, WrOp,
};

type Cq<W = u64> = LocalCq<W, Model>;
type Qp<T = RC, W = u64> = LocalQp<T, W, Model>;

fn caps(depth: u32) -> QpCaps {
    QpCaps {
        max_send_wr: depth,
        max_recv_wr: depth,
        max_send_sge: 2,
        max_recv_sge: 2,
        max_inline_data: 0,
    }
}

/// Two connected QPs of one model on one CQ of exactly their queues.
#[allow(clippy::type_complexity)]
fn pair<T: Transport, W>(seed: u64, depth: u32) -> (Model, Pd<Model>, Cq<W>, Qp<T, W>, Qp<T, W>) {
    let model = Model::new(seed);
    let ctx = Context::new(model.clone());
    let pd = ctx.alloc_pd().unwrap();
    let cq = ctx.create_local_cq(4 * depth).unwrap();
    let a = pd.create_qp::<T, _>(&cq, caps(depth)).unwrap();
    let b = pd.create_qp::<T, _>(&cq, caps(depth)).unwrap();
    let (ea, eb) = (a.endpoint(), b.endpoint());
    let (a, b) = (a.handshake(eb).unwrap(), b.handshake(ea).unwrap());
    (model, pd, cq, a, b)
}

fn filled(pd: &Pd<Model>, len: usize, byte: u8) -> Buf<u8, Model> {
    let mut buf = pd.alloc::<u8>(len).unwrap();
    buf.fill(byte);
    buf.freeze()
}

/// A `wr_id` that counts how many times it was dropped.
struct Counted(Rc<Cell<u64>>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[test]
fn a_send_and_its_receive_come_back_with_their_wr_ids_and_buffers() {
    let (_m, pd, cq, a, b) = pair::<RC, u64>(1, 8);
    b.post_recv(pd.alloc::<u8>(64).unwrap(), 2).unwrap();
    let src = filled(&pd, 32, 7);
    a.post_send(src.clone(), 1).unwrap();
    assert!(src.clone().try_thaw().is_err(), "the request holds it");

    let mut got = Vec::new();
    while got.len() < 2 {
        cq.poll(32, |mut wc| {
            let qp_num = wc.qp_num();
            let buf = wc.buf_mut::<u8>();
            got.push((wc.wr_id, wc.result, qp_num, buf));
        });
    }
    got.sort_by_key(|(wr_id, ..)| *wr_id);
    let (wr_id, result, qp_num, buf) = got.remove(0);
    assert_eq!((wr_id, qp_num), (1, a.qp_num()));
    assert_eq!(result, Ok(Completed::Sent));
    assert!(buf.is_none(), "a SEND's buffer is shared");
    let (wr_id, result, qp_num, buf) = got.remove(0);
    assert_eq!((wr_id, qp_num), (2, b.qp_num()));
    assert_eq!(
        result,
        Ok(Completed::Received(Received::Message {
            len: 32,
            imm: None
        }))
    );
    assert_eq!(&buf.expect("a receive's buffer")[..32], &[7; 32]);
    assert!(src.try_thaw().is_ok(), "the completion released it");
}

#[test]
fn every_operation_of_the_loop() {
    let (_m, pd, cq, a, b) = pair::<RC, &str>(2, 8);
    let drain = |n: usize| {
        let mut got = Vec::new();
        while got.len() < n {
            cq.poll(32, |wc| got.push(wc));
        }
        got
    };

    let mut word = pd.alloc::<u64>(1).unwrap();
    word[0] = 40;
    let word = word.expose().unwrap();
    let src = filled(&pd, 64, 3);
    let dst = pd.alloc::<u8>(64).unwrap().expose().unwrap();

    a.post_write(src.clone(), dst.remote(), "write").unwrap();
    let mut got = drain(1);
    assert_eq!(got.remove(0).wr_id, "write");
    let mut copy = [0u8; 64];
    dst.copy_to_slice(&mut copy);
    assert_eq!(copy, [3; 64]);

    a.post_read(pd.alloc::<u8>(64).unwrap(), dst.remote(), "read")
        .unwrap();
    let mut got = drain(1);
    let mut read = got.remove(0);
    assert_eq!(read.wr_id, "read");
    assert_eq!(&read.buf_mut::<u8>().unwrap()[..], &[3; 64]);

    a.post_fetch_add(pd.alloc::<u64>(1).unwrap(), word.remote(), 2, "add")
        .unwrap();
    let mut got = drain(1);
    let mut old = got.remove(0);
    assert_eq!(old.wr_id, "add");
    let old = old.buf_mut::<u64>().unwrap();
    assert_eq!(old[0], 40);
    a.post_compare_swap(old, word.remote(), 42, 7, "swap")
        .unwrap();
    let mut got = drain(1);
    let mut old = got.remove(0);
    assert_eq!(old.wr_id, "swap");
    assert_eq!(old.buf_mut::<u64>().unwrap()[0], 42);
    assert_eq!(word.reclaim()[0], 7);

    b.post_recv(pd.alloc::<u8>(8).unwrap(), "recv 1").unwrap();
    b.post_recv(pd.alloc::<u8>(8).unwrap(), "recv 2").unwrap();
    a.post_send_imm(src.slice(..8), 9, "send imm").unwrap();
    a.post_write_imm(src.slice(..16), dst.remote(), 11, "write imm")
        .unwrap();
    let got = drain(4);
    let of = |name: &str| {
        got.iter()
            .find(|wc| wc.wr_id == name)
            .map(|wc| wc.result)
            .unwrap()
    };
    assert_eq!(
        of("recv 1"),
        Ok(Completed::Received(Received::Message {
            len: 8,
            imm: Some(9)
        }))
    );
    assert_eq!(
        of("recv 2"),
        Ok(Completed::Received(Received::WriteNotification {
            imm: 11,
            len: 16
        }))
    );

    b.post_recv(pd.alloc::<u8>(64).unwrap(), "gathered")
        .unwrap();
    a.post_send_gather([src.slice(..4), src.slice(4..8)], "gather")
        .unwrap();
    let got = drain(2);
    let gathered = got.iter().find(|wc| wc.wr_id == "gathered").unwrap();
    assert_eq!(
        gathered.result,
        Ok(Completed::Received(Received::Message { len: 8, imm: None }))
    );

    assert!(a.pd().id() == pd.id());
    assert_eq!(a.endpoint().qp_num, a.qp_num());
    assert!(a.is_lock_free() == cq.is_lock_free());
    assert_eq!(cq.size(), 4 * 8);
    assert_eq!(cq.reserved(), 4 * 8);
    drop(dst);
}

#[test]
fn poll_reads_at_most_n_cqes() {
    let (_m, pd, cq, a, b) = pair::<RC, u64>(3, 16);
    let src = filled(&pd, 8, 1);
    for i in 0..4 {
        b.post_recv(pd.alloc::<u8>(8).unwrap(), i).unwrap();
        a.post_send(src.clone(), 100 + i).unwrap();
    }
    let mut total = 0;
    let mut in_turn = cq.poll(1, |_| {});
    assert!(in_turn <= 1, "one CQE, one signaled request");
    total += in_turn;
    while total < 8 {
        in_turn = cq.poll(3, |_| {});
        assert!(in_turn <= 3);
        total += in_turn;
    }
    assert_eq!(cq.poll(32, |_| panic!("nothing left")), 0);
}

#[test]
fn an_unsignaled_request_completes_with_the_next_signaled_one() {
    let (m, pd, cq, a, _b) = pair::<RC, u64>(4, 16);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    for i in 0..3 {
        a.stage(WrOp::Write(dst.remote()), src.clone().into_part(), i, false)
            .unwrap();
    }
    a.stage(WrOp::Write(dst.remote()), src.clone().into_part(), 3, true)
        .unwrap();
    assert_eq!(a.in_flight(), 4);
    a.ring().unwrap();
    m.run();

    // One CQE, for the signaled one, hands out all four, in order.
    let mut got = Vec::new();
    assert_eq!(cq.poll(1, |wc| got.push(wc.wr_id)), 4);
    assert_eq!(got, [0, 1, 2, 3]);
    assert_eq!(a.in_flight(), 0);
    assert!(src.try_thaw().is_ok());
}

#[test]
fn poll_one_hands_out_one_at_a_time() {
    let (m, pd, cq, a, _b) = pair::<RC, u64>(5, 32);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    for i in 0..20 {
        a.post_write(src.clone(), dst.remote(), i).unwrap();
    }
    m.run();
    let mut expected = 0;
    while expected < 20 {
        if let Some(wc) = cq.poll_one() {
            assert_eq!(wc.wr_id, expected);
            expected += 1;
        }
    }
    assert!(cq.poll_one().is_none());
}

#[test]
fn poll_and_poll_one_share_what_was_read() {
    let (m, pd, cq, a, _b) = pair::<RC, u64>(6, 32);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    for i in 0..20 {
        a.post_write(src.clone(), dst.remote(), i).unwrap();
    }
    m.run();
    let mut got = Vec::new();
    // Reads several CQEs and hands out one.
    got.push(cq.poll_one().unwrap().wr_id);
    // Hands out the others it read, and reads no more until they are gone.
    while got.len() < 20 {
        cq.poll(4, |wc| got.push(wc.wr_id));
    }
    assert_eq!(got, (0..20).collect::<Vec<_>>());
}

/// CQ capacity holds exactly: the closure posts a new request for every one it gets, with every
/// queue full, for many rounds. The model panics if a CQ ever overruns.
#[test]
fn the_closure_may_post_with_every_queue_full() {
    let depth = 8;
    let (m, pd, cq, a, b) = pair::<RC, (u8, u32)>(7, depth);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    for i in 0..depth {
        a.post_write(src.clone(), dst.remote(), (0, i)).unwrap();
        b.post_write(src.clone(), dst.remote(), (1, i)).unwrap();
    }
    assert!(!a.can_post() && !b.can_post());
    let mut seen = [0u32; 2];
    let mut handed = 0;
    while handed < 5000 {
        m.run();
        handed += cq.poll(32, |wc| {
            let (side, i) = wc.wr_id;
            wc.result.unwrap();
            assert_eq!(i, seen[side as usize], "each QP's order");
            seen[side as usize] += 1;
            let qp = if side == 0 { &a } else { &b };
            qp.post_write(src.clone(), dst.remote(), (side, i + depth))
                .unwrap();
        });
    }
    drop((a, b));
    assert!(src.try_thaw().is_ok());
}

/// A request handed out on one QP makes the closure post on the other, back and forth.
#[test]
fn the_closure_may_post_on_another_qp() {
    let (m, pd, cq, a, b) = pair::<RC, usize>(8, 4);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    let qps = [&a, &b];
    a.post_write(src.clone(), dst.remote(), 0).unwrap();
    let mut hops = 0;
    while hops < 100 {
        m.run();
        hops += cq.poll(32, |wc| {
            assert_eq!(wc.qp_num(), qps[wc.wr_id].qp_num());
            let next = 1 - wc.wr_id;
            qps[next]
                .post_write(src.clone(), dst.remote(), next)
                .unwrap();
        });
    }
}

#[test]
fn the_closure_may_poll_again() {
    let (m, pd, cq, a, _b) = pair::<RC, u32>(9, 32);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    for i in 0..10 {
        a.post_write(src.clone(), dst.remote(), i).unwrap();
    }
    m.run();
    let got = RefCell::new(Vec::new());
    fn poll_nested(cq: &Cq<u32>, got: &RefCell<Vec<u32>>) {
        cq.poll(32, |wc| {
            got.borrow_mut().push(wc.wr_id);
            if wc.wr_id % 3 == 0 {
                poll_nested(cq, got);
            }
        });
    }
    poll_nested(&cq, &got);
    assert_eq!(*got.borrow(), (0..10).collect::<Vec<_>>());
}

#[test]
fn the_closure_may_drop_qps_and_other_qps_keep_their_results() {
    let (m, pd, cq, a, b) = pair::<RC, (u8, u32)>(10, 32);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    for i in 0..8 {
        a.post_write(src.clone(), dst.remote(), (0, i)).unwrap();
        b.post_write(src.clone(), dst.remote(), (1, i)).unwrap();
    }
    m.run();
    let mut a = Some(a);
    let mut got = [0u32; 2];
    while got[1] < 8 {
        m.run();
        cq.poll(32, |wc| {
            let (side, _) = wc.wr_id;
            got[side as usize] += 1;
            // The first completion of `a` drops it, with the requests it still has.
            if side == 0 {
                a = None;
            }
        });
    }
    assert_eq!(
        got,
        [1, 8],
        "the dropped QP's other requests do not come back"
    );
    assert!(a.is_none());
    assert_eq!(cq.reserved(), 2 * 32, "the dropped QP's share is back");

    // A QP taking its index and its share gets nothing of the old one's.
    let p = pd.create_qp::<RC, _>(&cq, caps(32)).unwrap();
    let e = p.endpoint();
    let c = p.handshake(e).unwrap();
    c.post_write(src.clone(), dst.remote(), (2, 0)).unwrap();
    let mut handed = Vec::new();
    while handed.is_empty() {
        m.run();
        cq.poll(32, |wc| handed.push(wc.wr_id));
    }
    assert_eq!(handed, [(2, 0)]);
    drop((b, c));
    assert!(src.try_thaw().is_ok());
}

#[test]
fn wr_ids_come_back_exactly_once_or_are_dropped_with_their_qp() {
    let drops = Rc::new(Cell::new(0));
    let w = || Counted(drops.clone());
    let (m, pd, cq, a, b) = pair::<RC, Counted>(11, 16);
    let src = filled(&pd, 8, 1);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();

    // Handed out: dropped by the closure, once.
    a.post_write(src.clone(), dst.remote(), w()).unwrap();
    m.run();
    let mut handed = 0;
    while handed == 0 {
        handed = cq.poll(32, drop);
    }
    assert_eq!(drops.get(), 1);

    // Not handed out: dropped with the QP.
    for _ in 0..5 {
        a.post_write(src.clone(), dst.remote(), w()).unwrap();
    }
    b.post_recv(pd.alloc::<u8>(8).unwrap(), w()).unwrap();
    drop(a);
    assert_eq!(drops.get(), 1 + 5);

    // Refused by a full queue: comes back with the error.
    let mut posted = 0;
    let e = loop {
        match b.post_write(src.clone(), dst.remote(), w()) {
            Ok(()) => posted += 1,
            Err(e) => break e,
        }
    };
    assert_eq!(e.error, Error::QueueFull);
    assert_eq!(posted, 16);
    assert_eq!(e.leases.len(), 1);
    drop(e);
    assert_eq!(drops.get(), 1 + 5 + 1);

    // With the CQ gone: everything left goes with the last handle.
    drop(b);
    drop(cq);
    assert_eq!(drops.get(), 1 + 5 + 1 + 1 + 16);
    assert!(src.try_thaw().is_ok());
}

#[test]
fn a_full_queue_is_an_error_and_never_waits() {
    let (m, pd, cq, a, b) = pair::<RC, u32>(12, 4);
    m.halt(true);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    for i in 0..4 {
        a.post_write(src.clone(), dst.remote(), i).unwrap();
    }
    assert!(!a.can_post());
    let e = a.post_write(src.clone(), dst.remote(), 77).unwrap_err();
    assert_eq!(
        (e.error, e.wr_id, e.leases.len()),
        (Error::QueueFull, 77, 1)
    );
    drop(e);
    let e = a
        .stage(WrOp::Write(dst.remote()), src.clone().into_part(), 78, true)
        .unwrap_err();
    assert_eq!((e.error, e.wr_id), (Error::QueueFull, 78));
    drop(e);

    for i in 0..4 {
        b.post_recv(pd.alloc::<u8>(8).unwrap(), i).unwrap();
    }
    assert!(!b.can_post_recv());
    let mut e = b.post_recv(pd.alloc::<u8>(8).unwrap(), 79).unwrap_err();
    assert_eq!(e.wr_id, 79);
    assert_eq!(e.buf_mut::<u8>().expect("its buffer").len(), 8);

    m.halt(false);
    let mut handed = 0;
    while handed < 4 {
        handed += cq.poll(32, |wc| assert!(wc.result.is_ok()));
    }
    assert!(a.can_post());
    a.post_write(src.clone(), dst.remote(), 5).unwrap();
}

#[test]
fn a_loop_that_polls_always_finds_room() {
    let (m, pd, cq, a, _b) = pair::<RC, u32>(13, 4);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    for i in 0..1000 {
        while !a.can_post() {
            m.run();
            cq.poll(32, |wc| assert!(wc.result.is_ok()));
        }
        a.post_write(src.clone(), dst.remote(), i).unwrap();
    }
}

#[test]
fn invalid_requests_come_back_with_their_error() {
    let (_m, pd, _cq, a, b) = pair::<RC, u32>(14, 4);
    let other = Context::new(Model::new(99)).alloc_pd().unwrap();
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 64, 1);

    // A buffer of another protection domain.
    let e = a.post_send(filled(&other, 8, 1), 1).unwrap_err();
    assert!(matches!(e.error, Error::PdMismatch), "{:?}", e.error);
    assert_eq!((e.wr_id, e.leases.len()), (1, 1));
    let e = b.post_recv(other.alloc::<u8>(8).unwrap(), 2).unwrap_err();
    assert!(matches!(e.error, Error::PdMismatch), "{:?}", e.error);
    assert_eq!(e.wr_id, 2);

    // A WRITE longer than its destination.
    let e = a.post_write(src.clone(), dst.remote(), 3).unwrap_err();
    assert!(matches!(e.error, Error::TooLong { .. }), "{:?}", e.error);
    assert_eq!((e.wr_id, e.leases.len()), (3, 1));

    // A receive into a shared buffer cannot be written as a call; a READ into one is refused.
    let e = a
        .stage(
            WrOp::Read(dst.remote()),
            src.slice(..8).into_part(),
            4,
            true,
        )
        .unwrap_err();
    assert_eq!(e.wr_id, 4);

    // Nothing of these is in flight.
    assert_eq!(a.in_flight(), 0);
}

#[test]
fn refused_doorbells_come_back_from_poll_with_their_buffers() {
    let (m, pd, cq, a, _b) = pair::<RC, u32>(15, 8);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);

    // A request of its own, refused: the call says so and gives everything back.
    m.refuse_doorbells(a.qp_num(), 1);
    let e = a.post_write(src.clone(), dst.remote(), 1).unwrap_err();
    assert!(matches!(e.error, Error::Os(_)), "{:?}", e.error);
    assert_eq!((e.wr_id, e.leases.len()), (1, 1));
    drop(e);

    // Written and refused together: each comes back as refused, in order, with no CQE.
    m.refuse_doorbells(a.qp_num(), 1);
    for i in 2..5 {
        a.stage(WrOp::Write(dst.remote()), src.clone().into_part(), i, true)
            .unwrap();
    }
    assert!(a.ring().is_err());
    let mut got = Vec::new();
    cq.poll(32, |wc| got.push((wc.wr_id, wc.result, wc.leases.len())));
    let refused = Err(CompletionError::Refused);
    assert_eq!(got, [(2, refused, 1), (3, refused, 1), (4, refused, 1)]);

    // The QP goes on.
    a.post_write(src.clone(), dst.remote(), 5).unwrap();
    m.run();
    let mut handed = 0;
    while handed == 0 {
        handed = cq.poll(32, |wc| {
            assert_eq!((wc.wr_id, wc.result.is_ok()), (5, true))
        });
    }
}

#[test]
fn errors_and_flushes_report_positions_and_hand_buffers_back() {
    let (m, pd, cq, a, b) = pair::<RC, u32>(16, 8);
    m.halt(true);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    for i in 0..3 {
        a.post_write(src.clone(), dst.remote(), i).unwrap();
    }
    b.post_recv(pd.alloc::<u8>(8).unwrap(), 10).unwrap();
    m.inject_error(a.qp_num(), Status::REM_ACCESS_ERR);
    m.halt(false);
    let mut got = Vec::new();
    while got.len() < 3 {
        cq.poll(32, |wc| {
            if wc.wr_id < 10 {
                got.push((wc.wr_id, wc.result));
            }
        });
    }
    assert!(got.iter().any(|(_, r)| r.is_err()), "{got:?}");
    a.force_error().unwrap();

    // Moved to the error state, everything posted is flushed and comes back.
    b.force_error().unwrap();
    let mut handed = None;
    while handed.is_none() {
        cq.poll(32, |mut wc| {
            assert_eq!(wc.wr_id, 10);
            handed = Some((wc.result, wc.buf_mut::<u8>()));
        });
    }
    let (result, buf) = handed.unwrap();
    assert!(matches!(
        result,
        Err(CompletionError::Failed {
            status: Status::WR_FLUSH_ERR,
            ..
        })
    ));
    assert_eq!(buf.expect("the receive's buffer").len(), 8);
}

#[test]
fn a_batch_is_one_request_with_one_wr_id() {
    let (m, pd, cq, a, b) = pair::<RC, u32>(17, 16);
    let dst = pd.alloc::<u8>(64).unwrap().expose().unwrap();
    let src = filled(&pd, 64, 5);
    b.post_recv(pd.alloc::<u8>(8).unwrap(), 100).unwrap();
    let other = filled(&pd, 8, 6);
    a.post_batch(1, |batch| {
        batch.write(src.slice(..8), dst.remote());
        batch.send(src.slice(..8));
        batch.write(other.clone(), dst.remote().offset(8));
        assert_eq!(batch.len(), 3);
    })
    .unwrap();
    assert_eq!(a.in_flight(), 3);
    m.run();
    let mut got = Vec::new();
    while got.len() < 2 {
        cq.poll(32, |wc| got.push((wc.wr_id, wc.leases.len())));
    }
    // Frozen buffers have no way back: those of a request of several are released as it comes back.
    assert!(got.contains(&(1, 0)), "one wr_id: {got:?}");
    assert!(other.is_unique());
    assert!(got.contains(&(100, 1)));
    assert_eq!(a.in_flight(), 0);

    // An empty batch posts nothing, and an invalid operation is the batch's error.
    let e = a.post_batch(2, |_| {}).unwrap_err();
    assert_eq!(e.wr_id, 2);
    assert!(matches!(e.error, Error::Invalid(_)));
    let e = a
        .post_batch(3, |batch| batch.write(src.clone(), dst.remote().slice(..8)))
        .unwrap_err();
    assert_eq!(e.wr_id, 3);
}

#[test]
fn a_batch_longer_than_its_limit_goes_out_in_parts() {
    let model = Model::new(22);
    let ctx = Context::new(model.clone());
    let pd = ctx.alloc_pd().unwrap();
    let dst = pd.alloc::<u8>(64).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 5);
    let cq: Cq<u32> = ctx.create_local_cq(16).unwrap();
    let mut builder = QueuePairBuilder::<RC, Model>::new(&pd);
    builder.set_send_depth(4).set_recv_depth(4).set_max_batch(2);
    let a = builder.build_on(&cq).unwrap();
    let b = builder.build_on(&cq).unwrap();
    let (ea, eb) = (a.endpoint(), b.endpoint());
    let a = a.handshake(eb).unwrap();
    let _b = b.handshake(ea).unwrap();
    a.post_batch(9, |batch| {
        for i in 0..7 {
            batch.write(src.clone(), dst.remote().offset(i));
        }
    })
    .unwrap();
    // Only the last part has the `wr_id`; the parts before it had no owner.
    let mut got = Vec::new();
    while got.is_empty() {
        model.run();
        cq.poll(32, |wc| got.push((wc.wr_id, wc.result.is_ok())));
    }
    assert_eq!(got, [(9, true)]);
    while a.in_flight() > 0 {
        model.run();
        cq.poll(32, |_| {});
    }
    drop(a);
    assert!(src.try_thaw().is_ok());
}

#[test]
fn exact_capacity_is_all_a_cq_takes() {
    let model = Model::new(18);
    let ctx = Context::new(model);
    let pd = ctx.alloc_pd().unwrap();
    let cq: Cq<u32> = ctx.create_local_cq(8).unwrap();
    let a = pd.create_qp::<RC, _>(&cq, caps(2)).unwrap();
    let b = pd.create_qp::<RC, _>(&cq, caps(2)).unwrap();
    assert_eq!(cq.reserved(), 8);
    let full = pd.create_qp::<RC, _>(&cq, caps(1)).err();
    assert_eq!(full, Some(Error::CqFull { need: 2, free: 0 }));
    // A QP that never connected gives its share back.
    drop(b);
    assert_eq!(cq.reserved(), 4);
    let _c = pd.create_qp::<RC, _>(&cq, caps(2)).unwrap();
    assert_eq!(cq.reserved(), 8);
    drop(a);
    assert_eq!(cq.reserved(), 4);
}

#[test]
fn a_panicking_closure_loses_nothing() {
    let (m, pd, cq, a, _b) = pair::<RC, u32>(19, 16);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    for i in 0..6 {
        a.post_write(src.clone(), dst.remote(), i).unwrap();
    }
    m.run();
    let mut got = Vec::new();
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        cq.poll(32, |wc| {
            got.push(wc.wr_id);
            if wc.wr_id == 2 {
                panic!("the program's bug");
            }
        })
    }));
    assert!(panicked.is_err());
    // The handle still works, and the others of that read come next.
    cq.poll(32, |wc| got.push(wc.wr_id));
    assert_eq!(got, (0..6).collect::<Vec<_>>());
}

#[test]
fn the_raw_engine_is_reachable_from_the_handles() {
    let (m, pd, cq, a, _b) = pair::<RC, u32>(20, 4);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    let key = a.as_raw();
    {
        let mut engine = cq.as_raw();
        engine.write(key, src.clone(), dst.remote(), 7).unwrap();
    }
    m.run();
    let mut got = None;
    cq.as_raw().poll(32, |_, c| got = Some(c.wr_id));
    assert_eq!(got, Some(7));
}

#[test]
fn uc_has_the_loop_too() {
    let (m, pd, cq, a, b) = pair::<UC, u32>(21, 8);
    let src = filled(&pd, 8, 2);
    b.post_recv(pd.alloc::<u8>(8).unwrap(), 1).unwrap();
    a.post_send(src.clone(), 2).unwrap();
    m.run();
    let mut got = Vec::new();
    while got.len() < 2 {
        cq.poll(32, |wc| got.push((wc.wr_id, wc.result.is_ok())));
    }
    got.sort();
    assert_eq!(got, [(1, true), (2, true)]);
}

/// Random operations on several QPs of one CQ, with the closure posting, errors, flushes, refused
/// doorbells and QPs dropped at random: every `wr_id` comes back or is dropped exactly once, each
/// queue's in order, and at the end every buffer is back and nothing stays registered.
#[test]
fn random_operations_on_a_cq() {
    struct Step {
        qp: usize,
        queue: usize,
        n: u64,
        _drops: Counted,
    }
    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }
    let mut handed_out = 0;
    for seed in 1..40u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let model = Model::new(seed);
        let ctx = Context::new(model.clone());
        let pd = ctx.alloc_pd().unwrap();
        let depth = 2 + seed as u32 % 7;
        let pairs = 1 + seed as usize % 3;
        let cq: Cq<Step> = ctx.create_local_cq(4 * pairs as u32 * depth).unwrap();
        let mut qps: Vec<Option<Qp<RC, Step>>> = Vec::new();
        for _ in 0..pairs {
            let a = pd.create_qp::<RC, _>(&cq, caps(depth)).unwrap();
            let b = pd.create_qp::<RC, _>(&cq, caps(depth)).unwrap();
            let (ea, eb) = (a.endpoint(), b.endpoint());
            qps.push(Some(a.handshake(eb).unwrap()));
            qps.push(Some(b.handshake(ea).unwrap()));
        }
        let qps = RefCell::new(qps);
        let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
        let src = filled(&pd, 8, 1);
        let drops = Rc::new(Cell::new(0u64));
        let created = Cell::new(0u64);
        // Per QP and queue: the next number and the last one handed out.
        let next = RefCell::new(vec![[0u64; 2]; 2 * pairs]);
        let seen = RefCell::new(vec![[None::<u64>; 2]; 2 * pairs]);
        let step = |qp: usize, queue: usize| {
            let mut next = next.borrow_mut();
            let n = next[qp][queue];
            next[qp][queue] += 1;
            created.set(created.get() + 1);
            Step {
                qp,
                queue,
                n,
                _drops: Counted(drops.clone()),
            }
        };
        let handled = Cell::new(0u64);
        let handle = |mut wc: safeverbs::Wc<Step, Model>| {
            handled.set(handled.get() + 1);
            let (qp, queue, n) = (wc.wr_id.qp, wc.wr_id.queue, wc.wr_id.n);
            // A refused request comes back at once, before those still in flight.
            if wc.result != Err(CompletionError::Refused) {
                let mut seen = seen.borrow_mut();
                let last = &mut seen[qp][queue];
                assert!(last.is_none_or(|last| last < n), "each queue's order");
                *last = Some(n);
            }
            // A receive's buffer goes straight back in, when it did not fail.
            if queue == 1 && wc.result.is_ok() {
                let qps = qps.borrow();
                if let Some(q) = &qps[qp] {
                    let buf = wc.buf_mut::<u8>().expect("a receive's buffer");
                    let _ = q.post_recv(buf, step(qp, 1));
                }
            }
        };
        for _ in 0..1500 {
            let i = rng.below(2 * pairs as u64) as usize;
            let chosen = rng.below(100);
            match chosen {
                0..=39 => {
                    if let Some(qp) = &qps.borrow()[i] {
                        let _ = qp.post_write(src.clone(), dst.remote(), step(i, 0));
                    }
                }
                40..=59 => {
                    if let Some(qp) = &qps.borrow()[i] {
                        let _ = qp.post_recv(pd.alloc::<u8>(8).unwrap(), step(i, 1));
                    }
                }
                60..=64 => {
                    if let Some(qp) = &qps.borrow()[i] {
                        let op = WrOp::Write(dst.remote());
                        let signaled = rng.below(2) == 0;
                        let _ = qp.stage(op, src.clone().into_part(), step(i, 0), signaled);
                        let _ = qp.ring();
                    }
                }
                65..=69 => {
                    if let Some(qp) = &qps.borrow()[i] {
                        model.refuse_doorbells(qp.qp_num(), 1);
                    }
                }
                70..=71 => {
                    if let Some(qp) = &qps.borrow()[i] {
                        model.inject_error(qp.qp_num(), Status::REM_OP_ERR);
                    }
                }
                72 => {
                    if let Some(qp) = &qps.borrow()[i] {
                        let _ = qp.force_error();
                    }
                }
                73 => {
                    let dropped = qps.borrow_mut()[i].take();
                    drop(dropped);
                }
                74..=89 => {
                    model.run();
                }
                _ => {
                    let n = 1 + rng.below(32) as usize;
                    cq.poll(n, &handle);
                }
            }
            if let Some(qp) = &qps.borrow()[i] {
                let engine = cq.as_raw();
                let (send, recv) = (engine.send_ring(qp.as_raw()), engine.recv_ring(qp.as_raw()));
                assert!(send.invariant() && recv.invariant());
            }
        }
        handed_out += handled.get();
        // Everything left goes with the handles.
        let left = qps.take();
        drop(left);
        drop(cq);
        assert_eq!(
            drops.get(),
            created.get(),
            "every wr_id dropped exactly once"
        );
        assert!(src.try_thaw().is_ok(), "every request released its buffer");
        drop(dst);
        drop(pd);
        drop(ctx);
        assert_eq!(model.registered(), 0, "nothing stays registered");
    }
    assert!(handed_out > 1000, "{handed_out} handed out");
}

/// A `wr_id` that does something when it goes.
struct OnDrop(Option<Box<dyn FnOnce()>>);

impl Drop for OnDrop {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

fn on_drop(f: impl FnOnce() + 'static) -> OnDrop {
    OnDrop(Some(Box::new(f)))
}

/// A model with its protection domain, one CQ and connected QPs of that CQ.
type Connected = (Model, Pd<Model>, Cq<OnDrop>, Vec<Qp<RC, OnDrop>>);

/// `2 * pairs` connected QPs of one model on one CQ of exactly their queues.
fn qps(seed: u64, pairs: usize, depth: u32) -> Connected {
    let model = Model::new(seed);
    let ctx = Context::new(model.clone());
    let pd = ctx.alloc_pd().unwrap();
    let cq = ctx.create_local_cq(4 * pairs as u32 * depth).unwrap();
    let mut qps = Vec::new();
    for _ in 0..pairs {
        let a = pd.create_qp::<RC, _>(&cq, caps(depth)).unwrap();
        let b = pd.create_qp::<RC, _>(&cq, caps(depth)).unwrap();
        let (ea, eb) = (a.endpoint(), b.endpoint());
        qps.push(a.handshake(eb).unwrap());
        qps.push(b.handshake(ea).unwrap());
    }
    (model, pd, cq, qps)
}

/// A `wr_id` not delivered goes when its QP goes, and its destructor may poll the CQ, post on any
/// QP and drop the last handle of another QP, whose own undelivered `wr_id` may do the same.
#[test]
fn a_wr_id_destructor_may_use_the_cq_when_its_qp_goes() {
    let (m, pd, cq, mut all) = qps(30, 3, 8);
    m.halt(true);
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let to = dst.remote();
    let src = filled(&pd, 8, 1);
    let log = Rc::new(RefCell::new(Vec::new()));

    // The peers are never dropped by this test's chain.
    let f = all.pop().unwrap();
    let e = Rc::new(all.pop().unwrap());
    let d = all.pop().unwrap();
    let c = all.pop().unwrap();
    let _b = all.pop().unwrap();
    let a = all.pop().unwrap();
    let _f = f;

    // The last in the chain: posts on `e` and polls.
    let (last_log, last_cq, last_e, last_src) = (log.clone(), cq.clone(), e.clone(), src.clone());
    let last = on_drop(move || {
        last_cq.poll(32, drop);
        let posted_log = last_log.clone();
        let posted = on_drop(move || posted_log.borrow_mut().push("what the last posted"));
        last_e
            .post_write(last_src, to, posted)
            .expect("room on another QP");
        last_log.borrow_mut().push("the last");
    });
    d.post_write(src.clone(), to, last).unwrap();

    // Holds the last handles of `c` and `d`: dropping `d` runs the wr_id above.
    let (first_log, first_cq) = (log.clone(), cq.clone());
    let held = RefCell::new(Some((c, d)));
    let first = on_drop(move || {
        drop(held.borrow_mut().take());
        first_cq.poll(32, drop);
        first_log.borrow_mut().push("the first");
    });
    a.post_write(src.clone(), to, first).unwrap();

    let reserved = cq.reserved();
    // The NIC flushes what the removals give up.
    m.halt(false);
    drop(a);
    assert_eq!(
        *log.borrow(),
        ["the last", "what the last posted", "the first"]
    );
    assert_eq!(
        cq.reserved(),
        reserved - 3 * 16,
        "`a`, `c` and `d` are gone"
    );
}

/// A `wr_id` may hold the last handle of its own QP: that keeps the QP until the request comes back,
/// and then the closure's drop of it removes the QP.
#[test]
fn a_wr_id_may_hold_the_last_handle_of_its_own_qp() {
    struct Conn {
        qp: Qp<RC, Rc<Conn>>,
        gone: Rc<Cell<bool>>,
    }
    impl Drop for Conn {
        fn drop(&mut self) {
            self.gone.set(true);
        }
    }
    let model = Model::new(31);
    let ctx = Context::new(model.clone());
    let pd = ctx.alloc_pd().unwrap();
    let cq: Cq<Rc<Conn>> = ctx.create_local_cq(64).unwrap();
    let a = pd.create_qp::<RC, _>(&cq, caps(8)).unwrap();
    let b = pd.create_qp::<RC, _>(&cq, caps(8)).unwrap();
    let (ea, eb) = (a.endpoint(), b.endpoint());
    let gone = Rc::new(Cell::new(false));
    let conn = Rc::new(Conn {
        qp: a.handshake(eb).unwrap(),
        gone: gone.clone(),
    });
    let _b = b.handshake(ea).unwrap();
    let dst = pd.alloc::<u8>(8).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    for _ in 0..3 {
        conn.qp
            .post_write(src.clone(), dst.remote(), conn.clone())
            .unwrap();
    }
    let reserved = cq.reserved();
    drop(conn);
    assert!(!gone.get(), "its requests hold it");
    let mut handed = 0;
    while !gone.get() {
        model.run();
        handed += cq.poll(1, drop);
    }
    assert_eq!(handed, 3);
    assert_eq!(cq.reserved(), reserved - 16);
    assert!(src.try_thaw().is_ok());
}

/// A batch's closure runs with nothing held: it may post on its own QP and others, and poll.
#[test]
fn a_batch_closure_may_use_the_qp_and_the_cq() {
    let (m, pd, cq, a, b) = pair::<RC, u32>(32, 16);
    let dst = pd.alloc::<u8>(64).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    a.post_write(src.clone(), dst.remote(), 1).unwrap();
    m.run();
    let mut got = Vec::new();
    a.post_batch(4, |batch| {
        batch.write(&src, dst.remote());
        cq.poll(32, |wc| got.push(wc.wr_id));
        a.post_write(src.clone(), dst.remote(), 2).unwrap();
        b.post_write(src.clone(), dst.remote(), 3).unwrap();
        batch.write(src.view(..4), dst.remote());
    })
    .unwrap();
    assert_eq!(got, [1]);
    m.run();
    while got.len() < 4 {
        cq.poll(32, |wc| got.push(wc.wr_id));
    }
    // On its QP, what the closure posted went first.
    assert_eq!(got[1..], [3, 2, 4]);
    assert!(src.try_thaw().is_ok());
}

/// Many WQEs of one buffer by reference keep one reference to it, and the request holds it until
/// it completes.
#[test]
fn a_batch_by_reference_keeps_one_lease_of_one_buffer() {
    let (m, pd, cq, a, _b) = pair::<RC, u32>(33, 32);
    let dst = pd.alloc::<u8>(64).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    a.post_batch(1, |batch| {
        for i in 0..8 {
            batch.write(&src, dst.remote().offset(i));
        }
        for i in 0..8 {
            batch.write(src.view(i..i + 1), dst.remote().offset(i as u64));
        }
        assert_eq!(batch.len(), 16);
    })
    .unwrap();
    assert!(!src.is_unique(), "the request holds it");
    m.run();
    let mut got = Vec::new();
    while got.is_empty() {
        cq.poll(32, |wc| got.push((wc.wr_id, wc.leases.len())));
    }
    assert_eq!(got, [(1, 1)]);
    assert!(src.is_unique(), "released");
}

/// Several buffers in one batch, unique ones among them: what can come back comes back, on either
/// way of driving a CQ, and a list of them is used again.
#[test]
fn a_batch_of_several_buffers_comes_back_or_is_released() {
    let (m, pd, cq, a, _b) = pair::<RC, u32>(34, 32);
    let dst = pd.alloc::<u8>(64).unwrap().expose().unwrap();
    let srcs: Vec<_> = (0..4).map(|i| filled(&pd, 8, i)).collect();
    for round in 0..10 {
        a.post_batch(round, |batch| {
            for src in &srcs {
                batch.write(src, dst.remote());
            }
        })
        .unwrap();
        assert!(
            srcs.iter().all(|src| !src.is_unique()),
            "the request holds them"
        );
        m.run();
        let mut got = Vec::new();
        while got.is_empty() {
            cq.poll(32, |wc| got.push((wc.wr_id, wc.leases.len())));
        }
        assert_eq!(got, [(round, 0)]);
        assert!(srcs.iter().all(|src| src.is_unique()), "released");
    }
}
