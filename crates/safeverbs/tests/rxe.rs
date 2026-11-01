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
use std::time::{Duration, Instant};

use futures::task::noop_waker;
use futures::{SinkExt, StreamExt};
use safeverbs::{
    Buf, Context, Error, LocalQp, Nic, Opcode, Pd, QueuePairBuilder, RC, Status, Transport, UC,
    WriteOp,
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
fn pair<T: Transport>(ctx: &Context, depth: u32) -> (Pd, LocalQp<T>, LocalQp<T>) {
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
    let (wc, got) = wait(r1).unwrap();
    assert_eq!((wc.opcode, wc.byte_len, wc.imm), (Opcode::RECV, 4096, None));
    assert_eq!(&got[..], &src[..]);
    let (wc, got) = wait(r2).unwrap();
    assert_eq!((wc.byte_len, wc.imm), (100, Some(0x1234_5678)));
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

fn write_and_write_imm<T: Transport>(ctx: &Context) -> (Pd, LocalQp<T>, LocalQp<T>) {
    let (pd, a, b) = pair::<T>(ctx, 64);
    let dst = pd.alloc::<u8>(8192).unwrap().expose().unwrap();
    let src = filled(&pd, 8192, 2);
    wait(a.write(src.slice(..), dst.remote())).unwrap();
    let recv = b.recv(pd.alloc::<u8>(8).unwrap());
    wait(a.write_imm(src.slice(..64), dst.remote().offset(4096), 99)).unwrap();
    let (wc, _) = wait(recv).unwrap();
    assert_eq!(
        (wc.opcode, wc.byte_len, wc.imm),
        (Opcode::RECV_RDMA_WITH_IMM, 64, Some(99))
    );
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
    let before = a.with(|qp| qp.send_ring().posted());
    let wc = wait(a.batch(|b| {
        for i in 0..128 {
            b.write(src.slice(..), dst.remote().offset(i * 64));
        }
    }))
    .unwrap();
    assert_eq!(wc.opcode, Opcode::RDMA_WRITE);
    assert_eq!(a.with(|qp| qp.send_ring().posted()) - before, 1);
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
        let (wc, buf) = wait(rx.next()).unwrap();
        assert!(wc.is_ok(), "{wc:?}");
        imms.push(wc.imm.unwrap());
        rx.repost(buf).unwrap();
    }
    assert_eq!(imms, (0..10).map(|i| i * 200 + 199).collect::<Vec<_>>());
    assert!(src.try_thaw().is_ok());
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
        let (wc, got) = wait(r).unwrap();
        let want: &[u8] = match i % 3 {
            0 => &src[..],
            1 => &src[4096..],
            _ => &src[i..i + 10],
        };
        assert_eq!(&got[..wc.byte_len as usize], want);
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
    let (wc, got) = wait(rx.next()).unwrap();
    assert!(wc.is_ok());
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
    while let Some((wc, buf)) = wait(rx.next()) {
        assert_eq!(wc.status, Status::WR_FLUSH_ERR);
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
    let a = a.into_shared();
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
        let (wc, buf) = wait(async {
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
        assert_eq!(wc.imm, Some(i), "in order");
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
        let (wc, buf) = wait(rx.next()).unwrap();
        assert_eq!(wc.byte_len as usize, len);
        assert!(buf[..len].iter().all(|&x| x == i));
        rx.repost(buf).unwrap();
    }
    assert_eq!(ring.in_use(), 0);
}
