//! The public API on a real RDMA device: two QPs of one device connected
//! to each other (on Soft-RoCE, `scripts/rxe.sh up`), one test per feature.
//!
//! Every test passes without doing anything when the host has no RDMA
//! device. `SAFEVERBS_TEST_DEVICE` picks a device by name (else an rxe
//! device, else the first) and `SAFEVERBS_TEST_GID_INDEX` a GID (else the
//! builder's default: the first RoCE v2 GID holding an IPv4 address). Every
//! wait has a deadline, so a test fails instead of hanging.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context as TaskContext, Poll};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::task::noop_waker;
use futures::{SinkExt, StreamExt};
use safeverbs::raw::{ConnectAttr, Engine, QpCaps, QpKey, WrOp};
use safeverbs::{
    Access, AsyncQp, Buf, Completed, CompletionError, Context, Error, LocalCq, LocalQp, Nic, Pd,
    QueuePairBuilder, RC, Received, Status, Transport, UC, WriteOp,
};

/// How long any one wait may take.
const DEADLINE: Duration = Duration::from_secs(10);

/// Runs `f` to completion, polling it in a loop; panics after `DEADLINE`.
fn wait<F: Future>(f: F) -> F::Output {
    let mut f = Box::pin(f);
    let waker = noop_waker();
    let mut cx = TaskContext::from_waker(&waker);
    let start = Instant::now();
    loop {
        if let Poll::Ready(out) = Pin::as_mut(&mut f).poll(&mut cx) {
            return out;
        }
        assert!(start.elapsed() < DEADLINE, "timed out");
    }
}

/// The test device, or `None` (and the test passes) without one.
fn device() -> Option<Context> {
    let names = safeverbs::devices().unwrap_or_default();
    let name = match std::env::var("SAFEVERBS_TEST_DEVICE") {
        Ok(n) => n,
        Err(_) => match names.iter().find(|n| n.contains("rxe")).or(names.first()) {
            Some(n) => n.clone(),
            None => {
                eprintln!("no RDMA device: skipped (scripts/rxe.sh up creates one)");
                return None;
            }
        },
    };
    Some(Context::open(Some(&name)).expect("open the test device"))
}

/// Two QPs of the test device connected to each other, and their PD.
fn pair<T: Transport>(ctx: &Context, depth: u32) -> (Pd, AsyncQp<T>, AsyncQp<T>) {
    let pd = ctx.alloc_pd().unwrap();
    let build = || {
        let mut b = QueuePairBuilder::<T>::new(&pd);
        b.set_send_depth(depth).set_recv_depth(depth);
        if let Ok(i) = std::env::var("SAFEVERBS_TEST_GID_INDEX") {
            b.set_gid_index(i.parse().expect("SAFEVERBS_TEST_GID_INDEX"));
        }
        b.build().unwrap()
    };
    let (a, b) = (build(), build());
    let (ea, eb) = (a.endpoint(), b.endpoint());
    let (a, b) = (a.handshake(eb).unwrap(), b.handshake(ea).unwrap());
    (pd, a, b)
}

fn filled(pd: &Pd, len: usize, seed: u8) -> Buf<u8> {
    let mut buf = pd.alloc::<u8>(len).unwrap();
    for (i, b) in buf.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(31).wrapping_add(seed);
    }
    buf.freeze()
}

fn send_recv<T: Transport>() {
    let Some(ctx) = device() else { return };
    let (pd, a, b) = pair::<T>(&ctx, 64);
    let src = filled(&pd, 4096, 1);
    let r1 = b.recv(pd.alloc::<u8>(4096).unwrap());
    let r2 = b.recv(pd.alloc::<u8>(4096).unwrap());
    wait(a.send(src.clone())).unwrap();
    wait(a.send_imm(src.slice(..100), 0x1234_5678)).unwrap();
    let (received, got) = wait(r1).unwrap();
    assert_eq!(
        received,
        Received::Message {
            len: 4096,
            imm: None
        }
    );
    assert_eq!(&got[..], &src[..]);
    let (received, got) = wait(r2).unwrap();
    assert_eq!(
        received,
        Received::Message {
            len: 100,
            imm: Some(0x1234_5678)
        }
    );
    assert_eq!(&got[..100], &src[..100]);
}

#[test]
fn rc_send_recv_and_send_with_imm() {
    send_recv::<RC>();
}

#[test]
fn uc_send_recv_and_send_with_imm() {
    send_recv::<UC>();
}

fn write_and_write_imm<T: Transport>(ctx: &Context) -> (Pd, AsyncQp<T>, AsyncQp<T>) {
    let (pd, a, b) = pair::<T>(ctx, 64);
    let dst = pd.alloc::<u8>(8192).unwrap().expose().unwrap();
    let src = filled(&pd, 8192, 2);
    wait(a.write(src.slice(..), dst.remote())).unwrap();
    let recv = b.recv(pd.alloc::<u8>(8).unwrap());
    wait(a.write_imm(src.slice(..64), dst.remote().offset(4096), 99)).unwrap();
    let (received, _) = wait(recv).unwrap();
    assert_eq!(received, Received::WriteNotification { imm: 99, len: 64 });
    let back = dst.reclaim();
    assert_eq!(&back[..4096], &src[..4096]);
    assert_eq!(&back[4096..4160], &src[..64]);
    assert_eq!(&back[4160..], &src[4160..]);
    (pd, a, b)
}

#[test]
fn rc_write_write_imm_and_read() {
    let Some(ctx) = device() else { return };
    let (pd, a, _b) = write_and_write_imm::<RC>(&ctx);
    let src = filled(&pd, 4096, 3);
    let mut exposed = pd.alloc::<u8>(4096).unwrap();
    exposed.copy_from_slice(&src);
    let exposed = exposed.expose().unwrap();
    let got = wait(a.read(
        pd.alloc::<u8>(1000).unwrap(),
        exposed.remote().slice(500..1500),
    ))
    .unwrap();
    assert_eq!(&got[..], &src[500..1500]);
}

#[test]
fn uc_write_and_write_imm() {
    let Some(ctx) = device() else { return };
    write_and_write_imm::<UC>(&ctx);
}

#[test]
fn write_notification_does_not_fill_receive_buffer() {
    let Some(ctx) = device() else { return };
    let (pd, a, b) = pair::<RC>(&ctx, 8);
    let src = filled(&pd, 64 * 1024, 17);
    let dst = pd.alloc::<u8>(src.len()).unwrap().expose().unwrap();
    let mut rq = pd.alloc::<u8>(8).unwrap();
    rq.fill(0x5a);
    let recv = b.recv(rq);
    let () = wait(a.write_imm(src.clone(), dst.remote(), 0xfedc_ba98)).unwrap();
    let (received, rq) = wait(recv).unwrap();
    assert_eq!(
        received,
        Received::WriteNotification {
            imm: 0xfedc_ba98,
            len: src.len() as u32
        }
    );
    assert_eq!(&rq[..], &[0x5a; 8]);
    assert_eq!(&dst.reclaim()[..], &src[..]);
}

/// A peer's WRITE with immediate into exposed memory: its notification says the data is
/// complete, the safe copy reads it, and the same rkey still works for the next one.
#[test]
fn write_with_immediate_read_with_the_safe_copy() {
    let Some(ctx) = device() else { return };
    let (pd, a, b) = pair::<RC>(&ctx, 16);
    let dst = pd.alloc::<u8>(8192).unwrap().expose().unwrap();
    let remote = dst.remote();
    let mut rx = b.receiver((0..4).map(|_| pd.alloc::<u8>(8).unwrap()));
    for round in 0..4u8 {
        let src = filled(&pd, 8192, round);
        wait(a.write_imm(src.slice(..), remote, u32::from(round))).unwrap();
        let (received, buf) = wait(rx.next()).unwrap();
        assert_eq!(
            received,
            Ok(Received::WriteNotification {
                imm: u32::from(round),
                len: 8192
            })
        );
        let mut copy = vec![0; 8192];
        dst.copy_to_slice(&mut copy);
        assert_eq!(&copy[..], &src[..], "round {round}");
        assert_eq!(dst.remote(), remote, "exposed still, with its key");
        rx.repost(buf).unwrap();
    }
    assert_eq!(&dst.reclaim()[..], &filled(&pd, 8192, 3)[..]);
}

#[test]
fn rc_atomics() {
    let Some(ctx) = device() else { return };
    if !ctx.nic().caps().atomics {
        eprintln!("no atomics on {}: skipped", ctx.name());
        return;
    }
    let (pd, a, _b) = pair::<RC>(&ctx, 16);
    let mut word = pd.alloc::<u64>(1).unwrap();
    word[0] = 40;
    let word = word.expose().unwrap();
    let old = wait(a.fetch_add(pd.alloc::<u64>(1).unwrap(), word.remote(), 2)).unwrap();
    assert_eq!(old[0], 40);
    let old = wait(a.compare_swap(old, word.remote(), 42, 7)).unwrap();
    assert_eq!(old[0], 42);
    assert_eq!(word.reclaim()[0], 7);
}

#[test]
fn batch_of_writes_is_one_request() {
    let Some(ctx) = device() else { return };
    let (pd, a, _b) = pair::<RC>(&ctx, 256);
    let dst = pd.alloc::<u8>(128 * 64).unwrap().expose().unwrap();
    let src = filled(&pd, 64, 4);
    let before = a.with(|w, qp| w.send_ring(qp).posted());
    let () = wait(a.batch(|b| {
        for i in 0..128 {
            b.write(src.slice(..), dst.remote().offset(i * 64));
        }
    }))
    .unwrap();
    assert_eq!(a.with(|w, qp| w.send_ring(qp).posted()) - before, 1);
    let back = dst.reclaim();
    assert!(back[..].chunks(64).all(|c| c == &src[..]));
    assert!(src.try_thaw().is_ok(), "the batch released its lease");
}

#[test]
fn sink_of_writes_and_stream_of_receives() {
    let Some(ctx) = device() else { return };
    let (pd, a, b) = pair::<RC>(&ctx, 128);
    let dst = pd.alloc::<u8>(64 * 64).unwrap().expose().unwrap();
    let src = filled(&pd, 64, 5);
    let mut rx = b.receiver((0..16).map(|_| pd.alloc::<u8>(16).unwrap()));
    let mut tx = a.sink();
    let mut ops = futures::stream::iter((0..2000u64).map(|i| {
        let op = WriteOp::new(src.clone(), dst.remote().offset(i % 64 * 64));
        Ok(if i % 200 == 199 {
            op.with_imm(i as u32)
        } else {
            op
        })
    }));
    wait(tx.send_all(&mut ops)).unwrap();
    wait(tx.close()).unwrap();
    drop(tx);
    let mut imms = Vec::new();
    while imms.len() < 10 {
        let (received, buf) = wait(rx.next()).unwrap();
        let Ok(Received::WriteNotification { imm, len: 64 }) = received else {
            panic!("{received:?}");
        };
        imms.push(imm);
        rx.repost(buf).unwrap();
    }
    assert_eq!(imms, (0..10).map(|i| i * 200 + 199).collect::<Vec<_>>());
    assert!(src.try_thaw().is_ok());
}

/// A sink's error names the item that failed.
#[test]
fn sink_error_names_its_item() {
    let Some(ctx) = device() else { return };
    let (pd, a, _b) = pair::<RC>(&ctx, 32);
    let dst = pd.alloc::<u8>(64).unwrap().expose().unwrap();
    let src = filled(&pd, 8, 1);
    let mut bad = dst.remote();
    bad.rkey ^= 0x5a5a_0000;
    let mut tx = a.sink();
    for item in 0..6 {
        let to = if item == 4 { bad } else { dst.remote() };
        wait(tx.feed(WriteOp::new(src.clone(), to))).unwrap();
    }
    let error = wait(tx.close()).unwrap_err();
    assert!(
        matches!(
            error,
            Error::SinkItem {
                item: 4,
                error: CompletionError::Failed {
                    status: Status::REM_ACCESS_ERR,
                    position: 0,
                    ..
                }
            }
        ),
        "{error:?}"
    );
}

#[test]
fn repeated_sends_of_a_frozen_buffer_and_try_thaw() {
    let Some(ctx) = device() else { return };
    let (pd, a, b) = pair::<RC>(&ctx, 64);
    let src = filled(&pd, 8192, 6);
    let recvs: Vec<_> = (0..30)
        .map(|_| b.recv(pd.alloc::<u8>(8192).unwrap()))
        .collect();
    let sends: Vec<_> = (0..30)
        .map(|i| match i % 3 {
            0 => a.send(src.slice(..)),
            1 => a.send(src.slice(4096..8192)),
            _ => a.send(src.slice(i..i + 10)),
        })
        .collect();
    let src = src.try_thaw().expect_err("in flight: no thaw");
    for s in sends {
        wait(s).unwrap();
    }
    for (i, r) in recvs.into_iter().enumerate() {
        let (received, got) = wait(r).unwrap();
        let want: &[u8] = match i % 3 {
            0 => &src[..],
            1 => &src[4096..],
            _ => &src[i..i + 10],
        };
        assert_eq!(
            received,
            Received::Message {
                len: want.len() as u32,
                imm: None
            }
        );
        assert_eq!(&got[..want.len()], want);
    }
    let copy = src.clone();
    let src = src.try_thaw().expect_err("another view: no thaw");
    drop(copy);
    let mut buf = src.try_thaw().expect("nothing holds it");
    buf.fill(0);
}

#[test]
fn dropped_futures_hold_their_buffers_until_completion() {
    let Some(ctx) = device() else { return };
    let (pd, a, b) = pair::<RC>(&ctx, 16);
    let src = filled(&pd, 4096, 7);
    let mut rx = b.receiver((0..4).map(|_| pd.alloc::<u8>(4096).unwrap()));
    // Without polling, nothing retires: the buffer stays held.
    drop(a.send(src.clone()));
    drop(a.write(
        src.clone(),
        pd.alloc::<u8>(4096).unwrap().expose().unwrap().remote(),
    ));
    let src = src.try_thaw().expect_err("held by the dropped requests");
    let (received, got) = wait(rx.next()).unwrap();
    assert!(received.is_ok(), "{received:?}");
    assert_eq!(&got[..], &src[..]);
    wait(a.flush()).unwrap();
    assert!(src.try_thaw().is_ok(), "released at retirement");
}

#[test]
fn bad_rkey_hands_the_buffer_back() {
    let Some(ctx) = device() else { return };
    let (pd, a, _b) = pair::<RC>(&ctx, 16);
    let mut bogus = pd.alloc::<u8>(64).unwrap().expose().unwrap().remote();
    bogus.rkey ^= 0x5a5a_0000;
    let e = wait(a.read(pd.alloc::<u8>(64).unwrap(), bogus)).unwrap_err();
    assert_eq!(
        e.error.status(),
        Some(Status::REM_ACCESS_ERR),
        "{}",
        e.error
    );
    assert_eq!(e.buf.len(), 64);
    // The QP is in the error state: later requests are flushed.
    let src = filled(&pd, 64, 0);
    let e = wait(a.send(src.clone())).unwrap_err();
    assert_eq!(e.status(), Some(Status::WR_FLUSH_ERR));
    assert!(src.try_thaw().is_ok());
}

#[test]
fn forced_error_flushes_with_the_buffers() {
    let Some(ctx) = device() else { return };
    let (pd, _a, b) = pair::<RC>(&ctx, 16);
    let mut rx = b.receiver((0..8).map(|_| pd.alloc::<u8>(64).unwrap()));
    b.force_error().unwrap();
    let mut n = 0;
    while let Some((result, buf)) = wait(rx.next()) {
        assert_eq!(result.unwrap_err().status(), Some(Status::WR_FLUSH_ERR));
        assert_eq!(rx.repost(buf).unwrap_err().error, Error::QpFailed);
        n += 1;
    }
    assert_eq!(n, 8);
}

#[test]
fn uc_forced_error_flushes_sends() {
    let Some(ctx) = device() else { return };
    let (pd, a, _b) = pair::<UC>(&ctx, 16);
    a.force_error().unwrap();
    let src = filled(&pd, 64, 0);
    let e = wait(a.send(src.clone())).unwrap_err();
    assert_eq!(e.status(), Some(Status::WR_FLUSH_ERR));
    assert!(src.try_thaw().is_ok());
}

#[test]
fn shared_qp_from_threads() {
    let Some(ctx) = device() else { return };
    let (pd, a, _b) = pair::<RC>(&ctx, 64);
    let a = a.into_shared().ok().expect("alone on its CQ");
    let dst = pd.alloc::<u8>(4 * 64).unwrap().expose().unwrap();
    let remote = dst.remote();
    std::thread::scope(|s| {
        for t in 0..4u64 {
            let (a, pd) = (&a, pd.clone());
            s.spawn(move || {
                let src = filled(&pd, 64, t as u8);
                for _ in 0..100 {
                    wait(a.write(src.clone(), remote.offset(t * 64))).unwrap();
                }
            });
        }
    });
    let back = dst.reclaim();
    for t in 0..4 {
        assert_eq!(&back[t * 64..t * 64 + 64], &filled(&pd, 64, t as u8)[..]);
    }
}

#[test]
fn backpressure_waits_for_room() {
    let Some(ctx) = device() else { return };
    let (pd, a, b) = pair::<RC>(&ctx, 8);
    let src = filled(&pd, 32, 9);
    // More sends than the queue holds, before any receive is posted: the
    // rest wait in order instead of blocking the thread.
    let sends: Vec<_> = (0..40).map(|i| a.send_imm(src.clone(), i)).collect();
    let mut rx = b.receiver((0..8).map(|_| pd.alloc::<u8>(32).unwrap()));
    let mut sends = sends.into_iter();
    let mut first = sends.next();
    for i in 0..40 {
        let (received, buf) = wait(async {
            loop {
                // Poll the sender too, so waiting sends get posted.
                if let Some(f) = first.as_mut()
                    && let Poll::Ready(r) = futures::poll!(f)
                {
                    r.unwrap();
                    first = sends.next();
                }
                if let Poll::Ready(item) = futures::poll!(rx.next()) {
                    break item.unwrap();
                }
            }
        });
        let Ok(Received::Message { imm, .. }) = received else {
            panic!("{received:?}");
        };
        assert_eq!(imm, Some(i), "in order");
        rx.repost(buf).unwrap();
    }
}

#[test]
fn ring_buf_streams_messages() {
    let Some(ctx) = device() else { return };
    let (pd, a, b) = pair::<RC>(&ctx, 16);
    let mut ring = pd.ring_buf(512).unwrap();
    let mut rx = b.receiver((0..4).map(|_| pd.alloc::<u8>(256).unwrap()));
    for i in 0..60u8 {
        let len = 1 + (i as usize * 37) % 200;
        let msg = ring
            .write_with(len, |b| b.fill(i))
            .expect("room: earlier messages retired");
        wait(a.send(msg)).unwrap();
        let (received, buf) = wait(rx.next()).unwrap();
        assert_eq!(
            received,
            Ok(Received::Message {
                len: len as u32,
                imm: None
            })
        );
        assert!(buf[..len].iter().all(|&x| x == i));
        rx.repost(buf).unwrap();
    }
    assert_eq!(ring.in_use(), 0);
}

/// The attributes that connect a QP of the test device, as the builder's
/// defaults have them.
fn connect_attr(ctx: &Context) -> ConnectAttr {
    let rd = ctx.nic().caps().max_qp_rd_atom.clamp(1, 16) as u8;
    ConnectAttr {
        port: 1,
        access: Access::LOCAL_WRITE | Access::REMOTE_WRITE | Access::REMOTE_READ,
        path_mtu: ctx.port(1).unwrap().active_mtu,
        rq_psn: 0,
        sq_psn: 0,
        gid_index: gid_index(ctx),
        traffic_class: 0,
        timeout: 14,
        retry_count: 7,
        rnr_retry: 7,
        min_rnr_timer: 12,
        max_rd_atomic: rd,
        max_dest_rd_atomic: rd,
    }
}

fn gid_index(ctx: &Context) -> Option<u8> {
    match std::env::var("SAFEVERBS_TEST_GID_INDEX") {
        Ok(i) => Some(i.parse().expect("SAFEVERBS_TEST_GID_INDEX")),
        Err(_) => ctx.default_gid_index(1).unwrap(),
    }
}

/// A QP not connected takes no work: a request's own doorbell hands its context and buffer back
/// with the error, and one staged before it comes back from the event loop as refused, not as
/// a flush: the NIC never saw it.
#[test]
fn refused_requests_are_not_flushed() {
    let Some(ctx) = device() else { return };
    let pd = ctx.alloc_pd().unwrap();
    let caps = QpCaps {
        max_send_wr: 8,
        max_recv_wr: 8,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
    };
    let mut w: Engine<&str> = Engine::new(&ctx, 32).unwrap();
    let qp: QpKey<RC> = w.create_qp(pd.as_raw(), caps, 2).unwrap();
    let src = filled(&pd, 8, 1);
    let e = w.send(qp, src.slice(..), "own doorbell").unwrap_err();
    assert_eq!(e.error, Error::NotConnected);
    assert_eq!((e.wr_id, e.leases.len()), ("own doorbell", 1));
    drop(e);
    let mut req = w.request(qp);
    req.push(WrOp::Send, src.slice(..).into_part()).unwrap();
    let staged = req.finish(true, "staged").unwrap();
    assert_eq!(w.commit(qp), Err(Error::NotConnected));
    let mut got = Vec::new();
    w.poll(32, |_, c| got.push((c.seq, c.wr_id, c.result)));
    assert_eq!(got, [(staged, "staged", Err(CompletionError::Refused))]);
    drop(w);
    assert!(src.try_thaw().is_ok());
}

/// What a request of [`event_loop_on_four_qps_of_one_cq`] was for: an owned value, which
/// counts its drops, so that the test sees each one come back exactly once.
struct Job {
    kind: Kind,
    /// The QP it was posted on.
    qp: usize,
    drops: std::rc::Rc<core::cell::Cell<usize>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Inbox,
    Send,
    Write,
    Read { len: u32 },
}

impl Drop for Job {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

/// Four QPs on one CQ, connected pairwise, driven by the event loop:
/// SENDs into posted receives, WRITEs and READs on every QP at once, each
/// completion handed to the loop with the context of its own request, on its
/// own QP, and its buffers, which repost and post more.
#[test]
fn event_loop_on_four_qps_of_one_cq() {
    let Some(ctx) = device() else { return };
    let pd = ctx.alloc_pd().unwrap();
    let depth = 32;
    let caps = QpCaps {
        max_send_wr: depth,
        max_recv_wr: depth,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
    };
    let mut w: Engine<Job> = Engine::new(&ctx, 4 * 2 * depth).unwrap();
    let qps: Vec<QpKey<RC>> = (0..4)
        .map(|_| w.create_qp(pd.as_raw(), caps, 8).unwrap())
        .collect();
    let attr = connect_attr(&ctx);
    let gid = gid_index(&ctx);
    for pair in qps.chunks(2) {
        let ea = w.endpoint(pair[0], 1, gid).unwrap();
        let eb = w.endpoint(pair[1], 1, gid).unwrap();
        w.connect(pair[0], &attr, &eb).unwrap();
        w.connect(pair[1], &attr, &ea).unwrap();
    }
    // What each QP sends, what its peer exposes for it to read, and where
    // its peer lets it write.
    let srcs: Vec<Buf<u8>> = (0..4).map(|i| filled(&pd, 256, i as u8)).collect();
    let readable: Vec<_> = (0..4)
        .map(|i| {
            let mut b = pd.alloc::<u8>(256).unwrap();
            b.copy_from_slice(&filled(&pd, 256, 100 + i as u8));
            b.expose().unwrap()
        })
        .collect();
    let writable: Vec<_> = (0..4)
        .map(|_| pd.alloc::<u8>(256).unwrap().expose().unwrap())
        .collect();
    let drops = std::rc::Rc::new(core::cell::Cell::new(0));
    let job = |kind, qp| Job {
        kind,
        qp,
        drops: drops.clone(),
    };
    let made = core::cell::Cell::new(0usize);
    for (i, &qp) in qps.iter().enumerate() {
        for _ in 0..depth / 2 {
            made.set(made.get() + 1);
            w.recv(qp, pd.alloc::<u8>(256).unwrap(), job(Kind::Inbox, i))
                .unwrap();
        }
    }
    const OPS: usize = 300;
    let mut posted = [0usize; 4];
    let mut done = [0usize; 4];
    let mut received = [0usize; 4];
    let post = |w: &mut Engine<Job>, i: usize, posted: &mut [usize; 4]| {
        let qp = qps[i];
        let peer = i ^ 1;
        let len = 1 + posted[i] % 256;
        made.set(made.get() + 1);
        match posted[i] % 3 {
            0 => w.send(qp, srcs[i].slice(..len), job(Kind::Send, i)),
            1 => w.write(
                qp,
                srcs[i].slice(..),
                writable[peer].remote(),
                job(Kind::Write, i),
            ),
            _ => w.read(
                qp,
                pd.alloc::<u8>(len).unwrap(),
                readable[peer].remote().slice(..len as u64),
                job(Kind::Read { len: len as u32 }, i),
            ),
        }
        .unwrap();
        posted[i] += 1;
    };
    // A few requests in flight on every QP to start.
    for i in 0..4 {
        for _ in 0..4 {
            post(&mut w, i, &mut posted);
        }
    }
    let start = Instant::now();
    while done.iter().any(|&d| d < OPS) || received.iter().any(|&r| r < OPS.div_ceil(3)) {
        w.poll(32, |w, c| {
            let i = qps.iter().position(|q| q.index() == c.qp).unwrap();
            assert_eq!(c.wr_id.qp, i, "another QP's context");
            let kind = c.wr_id.kind;
            let result = c.result.unwrap_or_else(|e| panic!("{kind:?} failed: {e}"));
            match (kind, result) {
                (Kind::Inbox, Completed::Received(Received::Message { len, imm: None })) => {
                    let buf = c.leases.into_buf_mut::<u8>().unwrap();
                    let len = len as usize;
                    assert_eq!(&buf[..len], &srcs[i ^ 1][..len], "from its peer");
                    received[i] += 1;
                    made.set(made.get() + 1);
                    w.recv(qps[i], buf, job(Kind::Inbox, i)).unwrap();
                }
                (Kind::Send | Kind::Write, Completed::Sent) => {
                    done[i] += 1;
                    if posted[i] < OPS {
                        post(w, i, &mut posted);
                    }
                }
                (Kind::Read { len }, Completed::Read { len: got }) => {
                    assert_eq!(got, len);
                    let buf = c.leases.into_buf_mut::<u8>().unwrap();
                    assert_eq!(buf.len(), len as usize);
                    assert_eq!(
                        &buf[..],
                        &filled(&pd, 256, 100 + (i ^ 1) as u8)[..len as usize]
                    );
                    done[i] += 1;
                    if posted[i] < OPS {
                        post(w, i, &mut posted);
                    }
                }
                other => panic!("{other:?}"),
            }
        });
        assert!(
            start.elapsed() < DEADLINE,
            "timed out: {done:?} {received:?}"
        );
    }
    assert_eq!(done, [OPS; 4]);
    drop(w);
    assert_eq!(drops.get(), made.get(), "every context ended exactly once");
    for (i, buf) in writable.into_iter().enumerate() {
        let back = buf.reclaim();
        assert_eq!(&back[..], &srcs[i ^ 1][..], "written by its peer");
    }
}

/// Four QPs on one CQ, connected pairwise, with futures: every
/// future's poll drives the shared CQ for all of them.
#[test]
fn futures_on_four_qps_of_one_cq() {
    let Some(ctx) = device() else { return };
    let pd = ctx.alloc_pd().unwrap();
    let cq = ctx.create_async_cq(4 * 2 * 64).unwrap();
    let build = || {
        let mut b = QueuePairBuilder::<RC>::new(&pd);
        b.set_send_depth(64).set_recv_depth(64);
        if let Some(i) = gid_index(&ctx) {
            b.set_gid_index(i);
        }
        b.build_on(&cq).unwrap()
    };
    let mut qps = Vec::new();
    for _ in 0..2 {
        let (a, b) = (build(), build());
        let (ea, eb) = (a.endpoint(), b.endpoint());
        qps.push(a.handshake(eb).unwrap());
        qps.push(b.handshake(ea).unwrap());
    }
    let readable: Vec<_> = (0..4)
        .map(|i| {
            let mut b = pd.alloc::<u8>(4096).unwrap();
            b.copy_from_slice(&filled(&pd, 4096, i as u8));
            b.expose().unwrap()
        })
        .collect();
    let dst = pd.alloc::<u8>(4 * 4096).unwrap().expose().unwrap();
    let mut rxs: Vec<_> = qps
        .iter()
        .map(|q| q.receiver((0..8).map(|_| pd.alloc::<u8>(4096).unwrap())))
        .collect();
    wait(async {
        for round in 0..20u8 {
            let src = filled(&pd, 4096, round);
            // On every QP at once: a SEND, a WRITE and a READ.
            let sends: Vec<_> = qps.iter().map(|q| q.send(src.slice(..1000))).collect();
            let writes: Vec<_> = qps
                .iter()
                .enumerate()
                .map(|(i, q)| {
                    q.write(
                        src.clone(),
                        dst.remote().slice(i as u64 * 4096..(i as u64 + 1) * 4096),
                    )
                })
                .collect();
            let reads: Vec<_> = qps
                .iter()
                .enumerate()
                .map(|(i, q)| {
                    q.read(
                        pd.alloc::<u8>(512).unwrap(),
                        readable[i ^ 1].remote().slice(..512),
                    )
                })
                .collect();
            for (i, r) in reads.into_iter().enumerate() {
                let got = r.await.unwrap();
                assert_eq!(&got[..], &filled(&pd, 4096, (i ^ 1) as u8)[..512]);
            }
            for f in sends.into_iter().chain(writes) {
                f.await.unwrap();
            }
            for rx in &mut rxs {
                let (received, buf) = rx.next().await.unwrap();
                assert_eq!(
                    received,
                    Ok(Received::Message {
                        len: 1000,
                        imm: None
                    })
                );
                assert_eq!(&buf[..1000], &src[..1000]);
                rx.repost(buf).unwrap();
            }
        }
    });
    let back = dst.reclaim();
    for i in 0..4 {
        assert_eq!(&back[i * 4096..(i + 1) * 4096], &filled(&pd, 4096, 19)[..]);
    }
}

/// Removing QPs and dropping an engine with work in flight drains each QP
/// before destroying it: receives that never match, WRITEs whose last WQE
/// is unsignaled (it succeeds with no completion of its own, so only the
/// drain WQE retires it), and READs the peer answers. Every removal
/// succeeds (the device flushed within the bound) and every buffer comes
/// back.
#[test]
fn removing_and_dropping_busy_qps_drains_them() {
    let Some(ctx) = device() else { return };
    let pd = ctx.alloc_pd().unwrap();
    let depth = 16;
    let caps = QpCaps {
        max_send_wr: depth,
        max_recv_wr: depth,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
    };
    let src = filled(&pd, 4096, 3);
    let dst = pd.alloc::<u8>(4096).unwrap().expose().unwrap();
    for drop_engine in [false, true] {
        let mut w: Engine<()> = Engine::new(&ctx, 4 * 2 * depth).unwrap();
        // Requests of one WQE, so that up to `depth - 1` may go unsignaled.
        let qps: Vec<QpKey<RC>> = (0..4)
            .map(|_| w.create_qp(pd.as_raw(), caps, 1).unwrap())
            .collect();
        let attr = connect_attr(&ctx);
        let gid = gid_index(&ctx);
        for pair in qps.chunks(2) {
            let ea = w.endpoint(pair[0], 1, gid).unwrap();
            let eb = w.endpoint(pair[1], 1, gid).unwrap();
            w.connect(pair[0], &attr, &eb).unwrap();
            w.connect(pair[1], &attr, &ea).unwrap();
        }
        for &qp in &qps {
            for _ in 0..4 {
                w.post_recv(qp, pd.alloc::<u8>(256).unwrap().into_part(), ())
                    .unwrap();
            }
            // A signaled WRITE, then unsignaled ones.
            for i in 0..4 {
                let mut req = w.request(qp);
                req.push(WrOp::Write(dst.remote()), src.slice(..512).into_part())
                    .unwrap();
                req.finish(i == 0, ()).unwrap();
            }
            let mut req = w.request(qp);
            req.push(
                WrOp::Read(dst.remote().slice(..256)),
                pd.alloc::<u8>(256).unwrap().into_part(),
            )
            .unwrap();
            req.finish_detached(false).unwrap();
            w.commit(qp).unwrap();
        }
        // Let the WRITEs and READs run, so that the unsignaled ones succeed
        // without completions.
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(200) {
            w.progress();
        }
        assert!(
            qps.iter().all(|&qp| w.send_ring(qp).in_flight_wqes() > 0),
            "unsignaled WQEs wait for a later completion"
        );
        if drop_engine {
            drop(w);
        } else {
            for &qp in &qps {
                w.remove_qp(qp).expect("the device flushes");
            }
            assert_eq!(w.reserved(), 0);
            drop(w);
        }
    }
    assert!(src.try_thaw().is_ok(), "every request released the buffer");
}

/// `2 * pairs` connected QPs of one device on one [`LocalCq`] of exactly their queues.
fn local_qps<W>(ctx: &Context, pairs: usize, depth: u32) -> (Pd, LocalCq<W>, Vec<LocalQp<RC, W>>) {
    let pd = ctx.alloc_pd().unwrap();
    let cq = ctx.create_local_cq(pairs as u32 * 4 * depth).unwrap();
    let build = || {
        let mut b = QueuePairBuilder::<RC>::new(&pd);
        b.set_send_depth(depth).set_recv_depth(depth);
        if let Some(i) = gid_index(ctx) {
            b.set_gid_index(i);
        }
        b.build_on(&cq).unwrap()
    };
    let mut qps = Vec::new();
    for _ in 0..pairs {
        let (a, b) = (build(), build());
        let (ea, eb) = (a.endpoint(), b.endpoint());
        qps.push(a.handshake(eb).unwrap());
        qps.push(b.handshake(ea).unwrap());
    }
    (pd, cq, qps)
}

/// Polls until `n` requests were handed to `f`.
fn poll_until<W>(cq: &LocalCq<W>, n: usize, mut f: impl FnMut(safeverbs::Wc<W>)) {
    let start = Instant::now();
    let mut handed = 0;
    while handed < n {
        handed += cq.poll(32, &mut f);
        assert!(start.elapsed() < DEADLINE, "timed out");
    }
}

/// Every operation of the event loop's handles, each `wr_id` coming back with its result and
/// buffer.
#[test]
fn local_cq_every_operation() {
    let Some(ctx) = device() else { return };
    let (pd, cq, qps) = local_qps::<&str>(&ctx, 1, 16);
    let (a, b) = (&qps[0], &qps[1]);
    let dst = pd.alloc::<u8>(256).unwrap().expose().unwrap();
    let src = filled(&pd, 256, 5);

    a.post_write(src.clone(), dst.remote(), "write").unwrap();
    poll_until(&cq, 1, |wc| {
        assert_eq!((wc.wr_id, wc.qp_num()), ("write", a.qp_num()));
        assert_eq!(wc.result, Ok(Completed::Sent));
    });
    let mut read = None;
    a.post_read(pd.alloc::<u8>(256).unwrap(), dst.remote(), "read")
        .unwrap();
    poll_until(&cq, 1, |mut wc| {
        assert_eq!(wc.wr_id, "read");
        wc.result.unwrap();
        read = wc.buf_mut::<u8>();
    });
    assert_eq!(&read.unwrap()[..], &src[..]);

    let mut got = Vec::new();
    b.post_recv(pd.alloc::<u8>(256).unwrap(), "recv").unwrap();
    a.post_send_imm(src.clone(), 9, "send").unwrap();
    poll_until(&cq, 2, |wc| got.push(wc));
    got.sort_by_key(|wc| wc.wr_id);
    assert_eq!(got[0].result.map(|_| got[0].qp_num()), Ok(b.qp_num()));
    let mut recv = got.remove(0);
    assert_eq!(
        recv.result,
        Ok(Completed::Received(Received::Message {
            len: 256,
            imm: Some(9)
        }))
    );
    assert_eq!(&recv.buf_mut::<u8>().unwrap()[..], &src[..]);
    assert_eq!(got[0].wr_id, "send");
    drop(got);

    let mut word = pd.alloc::<u64>(1).unwrap();
    word[0] = 40;
    let word = word.expose().unwrap();
    a.post_fetch_add(pd.alloc::<u64>(1).unwrap(), word.remote(), 2, "add")
        .unwrap();
    let mut old = None;
    poll_until(&cq, 1, |mut wc| {
        wc.result.unwrap();
        old = wc.buf_mut::<u64>();
    });
    assert_eq!(old.unwrap()[0], 40);
    assert_eq!(word.reclaim()[0], 42);
}

/// Pings and pongs on four QPs of one CQ of exactly their queues, the closure posting on the QP
/// the completion came from, in `poll` and in `poll_one`.
#[test]
fn local_cq_event_loop_on_four_qps() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Job {
        Inbox(usize),
        Message,
    }
    const ROUNDS: u32 = 2000;
    let Some(ctx) = device() else { return };
    let depth = 16;
    let (pd, cq, qps) = local_qps::<Job>(&ctx, 2, depth);
    for (i, qp) in qps.iter().enumerate() {
        for _ in 0..depth {
            qp.post_recv(pd.alloc::<u8>(64).unwrap(), Job::Inbox(i))
                .unwrap();
        }
    }
    let msg = filled(&pd, 64, 3);
    // The first of each pair starts; each message received is answered, until the rounds are up.
    let mut owed = [0u32; 4];
    owed[0] = 8;
    owed[2] = 8;
    let mut received = 0;
    let start = Instant::now();
    while received < ROUNDS {
        let by_one = received % 3 == 0;
        let mut handle = |mut wc: safeverbs::Wc<Job>| {
            wc.result.expect("no failures");
            if let Job::Inbox(i) = wc.wr_id {
                received += 1;
                assert_eq!(wc.qp_num(), qps[i].qp_num(), "another QP's wr_id");
                owed[i ^ 1] += 1;
                let buf = wc.buf_mut::<u8>().expect("a receive's buffer");
                assert_eq!(&buf[..], &msg[..]);
                // A receive's slot just came free.
                qps[i].post_recv(buf, Job::Inbox(i)).unwrap();
            }
        };
        if by_one {
            while let Some(wc) = cq.poll_one() {
                handle(wc);
            }
        } else {
            cq.poll(4, &mut handle);
        }
        for (i, qp) in qps.iter().enumerate() {
            while owed[i] > 0 && qp.can_post() {
                qp.post_send(msg.clone(), Job::Message).unwrap();
                owed[i] -= 1;
            }
        }
        assert!(start.elapsed() < DEADLINE, "timed out");
    }
    drop(qps);
    assert!(msg.try_thaw().is_ok(), "released, drained");
}

/// Unsignaled requests come back with the next signaled one, one doorbell for all; a full queue
/// is an error; a batch is one request.
#[test]
fn local_cq_stage_ring_and_batch() {
    let Some(ctx) = device() else { return };
    let depth = 8;
    let (pd, cq, qps) = local_qps::<u32>(&ctx, 1, depth);
    let a = &qps[0];
    let dst = pd.alloc::<u8>(64).unwrap().expose().unwrap();
    let src = filled(&pd, 64, 1);
    for i in 0..3 {
        a.stage(WrOp::Write(dst.remote()), src.clone().into_part(), i, false)
            .unwrap();
    }
    a.stage(WrOp::Write(dst.remote()), src.clone().into_part(), 3, true)
        .unwrap();
    a.ring().unwrap();
    let mut got = Vec::new();
    poll_until(&cq, 4, |wc| {
        wc.result.unwrap();
        got.push(wc.wr_id);
    });
    assert_eq!(got, [0, 1, 2, 3]);

    let mut posted = 0;
    let e = loop {
        match a.post_write(src.clone(), dst.remote(), 100 + posted) {
            Ok(()) => posted += 1,
            Err(e) => break e,
        }
    };
    assert_eq!(e.error, Error::QueueFull);
    assert_eq!((e.wr_id, e.leases.len()), (100 + posted, 1));
    drop(e);
    poll_until(&cq, posted as usize, |wc| {
        wc.result.unwrap();
    });

    a.post_batch(7, |batch| {
        for i in 0..5 {
            batch.write(src.slice(..8), dst.remote().offset(8 * i));
        }
    })
    .unwrap();
    poll_until(&cq, 1, |wc| {
        assert_eq!((wc.wr_id, wc.result.is_ok()), (7, true))
    });
    assert_eq!(a.in_flight(), 0);
}

/// Dropping a QP with work in flight drains it first, from the closure too, and the other QPs'
/// completions are still handed out.
#[test]
fn local_cq_dropping_qps_with_work_in_flight() {
    let Some(ctx) = device() else { return };
    let drops = Rc::new(Cell::new(0));
    #[allow(dead_code)]
    struct Counted(Rc<Cell<u32>>);
    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }
    let (pd, cq, mut qps) = local_qps::<Counted>(&ctx, 2, 16);
    let dst = pd.alloc::<u8>(64).unwrap().expose().unwrap();
    let src = filled(&pd, 64, 1);
    for qp in &qps {
        for _ in 0..8 {
            qp.post_write(src.clone(), dst.remote(), Counted(drops.clone()))
                .unwrap();
        }
        qp.post_recv(pd.alloc::<u8>(8).unwrap(), Counted(drops.clone()))
            .unwrap();
    }
    let first = RefCell::new(Some(qps.remove(0)));
    let mut others = 0;
    let start = Instant::now();
    while others < 8 || first.borrow().is_some() {
        cq.poll(32, |wc| {
            if first
                .borrow()
                .as_ref()
                .is_some_and(|q| q.qp_num() == wc.qp_num())
            {
                // A request of the first QP: it goes, with the others it has.
                drop(first.borrow_mut().take());
            } else {
                others += 1;
            }
        });
        assert!(start.elapsed() < DEADLINE, "timed out");
    }
    drop(qps);
    drop(cq);
    assert_eq!(drops.get(), 4 * 9, "every wr_id exactly once");
    assert!(src.try_thaw().is_ok(), "drained, then released");
}
