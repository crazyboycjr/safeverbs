//! The public API over the simulated NIC ([`Model`]): every operation, the
//! sink and the stream, buffers held while in flight, errors and flushes
//! handing buffers back, and random sequences of operations (with dropped
//! futures, injected errors and flushes) after which every buffer is back
//! or released exactly once and nothing stays registered.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context as TaskContext, Poll};
use std::collections::HashSet;

use futures::executor::block_on;
use futures::task::noop_waker;
use futures::{SinkExt, StreamExt};
use safeverbs::{
    Buf, BufMut, Context, Error, LocalQp, Model, Opcode, Pd, QueuePairBuilder, RC, Status,
    Transport, UC, WriteOp,
};

type Qp<T> = LocalQp<T, Model>;

/// Two QPs of one model connected to each other.
fn pair<T: Transport>(seed: u64, depth: u32) -> (Model, Pd<Model>, Qp<T>, Qp<T>) {
    let model = Model::new(seed);
    let pd = Context::new(model.clone()).alloc_pd().unwrap();
    let build = || {
        QueuePairBuilder::<T, Model>::new(&pd)
            .set_send_depth(depth)
            .set_recv_depth(depth)
            .build()
            .unwrap()
    };
    let (a, b) = (build(), build());
    let (ea, eb) = (a.endpoint(), b.endpoint());
    (
        model,
        pd.clone(),
        a.handshake(eb).unwrap(),
        b.handshake(ea).unwrap(),
    )
}

fn filled(pd: &Pd<Model>, len: usize, seed: u8) -> Buf<u8, Model> {
    let mut buf = pd.alloc::<u8>(len).unwrap();
    for (i, b) in buf.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(31).wrapping_add(seed);
    }
    buf.freeze()
}

/// Polls a future once with a waker that does nothing.
fn poll_once<F: Future + Unpin>(f: &mut F) -> Poll<F::Output> {
    let waker = noop_waker();
    let mut cx = TaskContext::from_waker(&waker);
    Pin::new(f).poll(&mut cx)
}

#[test]
fn send_recv_and_send_with_imm() {
    let (_m, pd, a, b) = pair::<RC>(1, 16);
    block_on(async {
        let src = filled(&pd, 256, 3);
        let r1 = b.recv(pd.alloc::<u8>(256).unwrap());
        let r2 = b.recv(pd.alloc::<u8>(256).unwrap());
        a.send(src.clone()).await.unwrap();
        a.send_imm(src.slice(..100), 0xdead_beef).await.unwrap();
        let (wc, got) = r1.await.unwrap();
        assert_eq!((wc.opcode, wc.byte_len, wc.imm), (Opcode::RECV, 256, None));
        assert_eq!(&got[..], &src[..]);
        let (wc, got) = r2.await.unwrap();
        assert_eq!((wc.byte_len, wc.imm), (100, Some(0xdead_beef)));
        assert_eq!(&got[..100], &src[..100]);
    });
}

#[test]
fn write_write_imm_and_read() {
    let (_m, pd, a, b) = pair::<RC>(2, 16);
    block_on(async {
        let dst = pd.alloc::<u8>(4096).unwrap().expose().unwrap();
        let src = filled(&pd, 4096, 9);
        a.write(src.slice(..), dst.remote()).await.unwrap();
        let mut copy = vec![0u8; 4096];
        dst.copy_to_slice(&mut copy);
        assert_eq!(&copy[..], &src[..]);

        let recv = b.recv(pd.alloc::<u8>(8).unwrap());
        a.write_imm(src.slice(..16), dst.remote().offset(1000), 7)
            .await
            .unwrap();
        let (wc, _) = recv.await.unwrap();
        assert_eq!(
            (wc.opcode, wc.byte_len, wc.imm),
            (Opcode::RECV_RDMA_WITH_IMM, 16, Some(7))
        );

        let into = a
            .read(pd.alloc::<u8>(512).unwrap(), dst.remote().slice(100..612))
            .await
            .unwrap();
        assert_eq!(&into[..], &src[100..612]);
        let back = dst.reclaim();
        assert_eq!(&back[..1000], &src[..1000]);
        assert_eq!(&back[1000..1016], &src[..16]);
    });
}

#[test]
fn atomics() {
    let (_m, pd, a, _b) = pair::<RC>(3, 8);
    block_on(async {
        let mut word = pd.alloc::<u64>(1).unwrap();
        word[0] = 40;
        let word = word.expose().unwrap();
        let old = a
            .fetch_add(pd.alloc::<u64>(1).unwrap(), word.remote(), 2)
            .await
            .unwrap();
        assert_eq!(old[0], 40);
        let old = a.compare_swap(old, word.remote(), 42, 7).await.unwrap();
        assert_eq!(old[0], 42);
        let old = a.compare_swap(old, word.remote(), 42, 9).await.unwrap();
        assert_eq!(old[0], 7);
        assert_eq!(word.reclaim()[0], 7);
        // A result buffer that is not 8 bytes comes back with the error.
        let e = a
            .fetch_add(
                pd.alloc::<u64>(2).unwrap(),
                pd.alloc::<u64>(1).unwrap().expose().unwrap().remote(),
                1,
            )
            .await
            .unwrap_err();
        assert!(matches!(e.error, Error::Invalid(_)));
        assert_eq!(e.buf.len(), 2);
    });
}

#[test]
fn batch_is_one_request() {
    let (_m, pd, a, _b) = pair::<RC>(4, 256);
    block_on(async {
        let dst = pd.alloc::<u8>(128 * 64).unwrap().expose().unwrap();
        let src = filled(&pd, 64, 5);
        let posted = a.with(|qp| qp.send_ring().posted());
        let wc = a
            .batch(|b| {
                for i in 0..128 {
                    b.write(src.slice(..), dst.remote().offset(i * 64));
                }
            })
            .await
            .unwrap();
        assert_eq!(wc.opcode, Opcode::RDMA_WRITE);
        assert_eq!(
            a.with(|qp| qp.send_ring().posted()) - posted,
            1,
            "one request"
        );
        let mut copy = vec![0u8; 128 * 64];
        dst.copy_to_slice(&mut copy);
        for chunk in copy.chunks(64) {
            assert_eq!(chunk, &src[..]);
        }
        // The batch kept one lease of the buffer: it is free again.
        assert!(src.try_thaw().is_ok());
    });
}

#[test]
fn batch_longer_than_max_batch_is_split() {
    let (_m, pd, a, _b) = pair::<RC>(5, 16);
    block_on(async {
        let dst = pd.alloc::<u8>(100 * 8).unwrap().expose().unwrap();
        let src = filled(&pd, 8, 1);
        a.batch(|b| {
            for i in 0..100 {
                b.write(src.clone(), dst.remote().offset(i * 8));
            }
        })
        .await
        .unwrap();
        a.flush().await.unwrap();
        let mut copy = vec![0u8; 800];
        dst.copy_to_slice(&mut copy);
        assert!(copy.chunks(8).all(|c| c == &src[..]));
        assert!(src.try_thaw().is_ok());
    });
}

#[test]
fn invalid_operations_are_refused() {
    let (m, pd, a, _b) = pair::<RC>(6, 8);
    let other = Context::new(m.clone()).alloc_pd().unwrap();
    block_on(async {
        let dst = pd.alloc::<u8>(16).unwrap().expose().unwrap();
        let e = a.write(filled(&pd, 32, 0), dst.remote()).await.unwrap_err();
        assert_eq!(e, Error::TooLong { len: 32, max: 16 });
        let e = a.send(filled(&other, 8, 0)).await.unwrap_err();
        assert_eq!(e, Error::PdMismatch);
        let e = a.recv(other.alloc::<u8>(8).unwrap()).await.unwrap_err();
        assert_eq!(e.error, Error::PdMismatch);
        let e = a.batch(|_| {}).await.unwrap_err();
        assert!(matches!(e, Error::Invalid(_)));
    });
}

#[test]
fn repeated_sends_of_one_frozen_buffer_and_its_parts() {
    let (_m, pd, a, b) = pair::<RC>(7, 64);
    block_on(async {
        let src = filled(&pd, 8192, 2);
        let recvs: Vec<_> = (0..20)
            .map(|_| b.recv(pd.alloc::<u8>(8192).unwrap()))
            .collect();
        let sends: Vec<_> = (0..20)
            .map(|i| match i % 3 {
                0 => a.send(src.slice(..)),
                1 => a.send(src.slice(4096..8192)),
                _ => a.send(src.slice(i..i + 10)),
            })
            .collect();
        // While in flight, nobody can thaw it.
        let src = src.try_thaw().unwrap_err();
        for s in sends {
            s.await.unwrap();
        }
        for (i, r) in recvs.into_iter().enumerate() {
            let (wc, got) = r.await.unwrap();
            let want: &[u8] = match i % 3 {
                0 => &src[..],
                1 => &src[4096..],
                _ => &src[i..i + 10],
            };
            assert_eq!(&got[..wc.byte_len as usize], want);
        }
        assert!(src.try_thaw().is_ok(), "every lease released");
    });
}

#[test]
fn dropped_futures_keep_their_buffers_until_completion() {
    let (m, pd, a, b) = pair::<RC>(8, 16);
    m.set_pace(1000, 1); // nothing progresses on its own
    let src = filled(&pd, 64, 4);
    drop(b.recv(pd.alloc::<u8>(64).unwrap()));
    drop(a.send(src.clone()));
    a.poll();
    b.poll();
    let src = src
        .try_thaw()
        .expect_err("the dropped send still holds the buffer");
    assert_eq!(m.registered(), 2, "both regions still registered");
    m.set_pace(0, 4);
    m.run();
    a.poll();
    b.poll();
    let src = src.try_thaw().expect("released at retirement");
    drop(src);
    assert_eq!(
        m.registered(),
        0,
        "the dropped receive's buffer was released too"
    );
}

#[test]
fn dropping_the_qp_releases_buffers_in_flight() {
    let (m, pd, a, b) = pair::<RC>(9, 16);
    m.set_pace(1000, 1);
    let src = filled(&pd, 64, 4);
    drop(a.send(src.clone()));
    drop(b.recv(pd.alloc::<u8>(64).unwrap()));
    assert_eq!(m.registered(), 2);
    drop(a);
    drop(b);
    assert!(src.try_thaw().is_ok());
    assert_eq!(m.registered(), 0);
    assert_eq!(m.pending(), 0, "the QPs are gone with their WQEs");
}

#[test]
fn errors_hand_buffers_back() {
    let (m, pd, a, b) = pair::<RC>(10, 16);
    block_on(async {
        // A bad remote key.
        let mut bogus = pd.alloc::<u8>(64).unwrap().expose().unwrap().remote();
        bogus.rkey ^= 0x55aa;
        let e = a
            .read(pd.alloc::<u8>(64).unwrap(), bogus)
            .await
            .unwrap_err();
        assert_eq!(e.error.status(), Some(Status::REM_ACCESS_ERR));
        assert_eq!(e.buf.len(), 64);
        // The QP is in error now: later requests are flushed.
        let src = filled(&pd, 64, 1);
        let e = a.send(src.clone()).await.unwrap_err();
        assert_eq!(e.status(), Some(Status::WR_FLUSH_ERR));
        assert!(src.try_thaw().is_ok());
    });
    // A QP forced into error flushes its receives with their buffers.
    let mut rx = b.receiver((0..4).map(|_| pd.alloc::<u8>(32).unwrap()));
    b.force_error().unwrap();
    let got: Vec<_> = block_on(rx.by_ref().collect());
    assert_eq!(got.len(), 4);
    for (wc, buf) in got {
        assert_eq!(wc.status, Status::WR_FLUSH_ERR);
        let e = rx.repost(buf).unwrap_err();
        assert_eq!(e.error, Error::QpFailed);
    }
    drop(rx);
    // An injected error on an unsignaled WQE inside a batch.
    let (m2, pd2, c, _d) = pair::<RC>(11, 32);
    let dst = pd2.alloc::<u8>(64).unwrap().expose().unwrap();
    m2.inject_error(c.qp_num(), Status::REM_OP_ERR);
    let src = filled(&pd2, 8, 0);
    let e = block_on(c.batch(|b| {
        for _ in 0..4 {
            b.write(src.clone(), dst.remote());
        }
    }))
    .unwrap_err();
    assert_eq!(e.status(), Some(Status::REM_OP_ERR));
    assert!(src.try_thaw().is_ok());
    drop(m);
}

#[test]
fn sink_and_stream() {
    let (_m, pd, a, b) = pair::<RC>(12, 32);
    block_on(async {
        let dst = pd.alloc::<u8>(64 * 64).unwrap().expose().unwrap();
        let src = filled(&pd, 64, 8);
        // A receive for each WRITE with immediate data: RC waits for one.
        let mut rx = b.receiver((0..10).map(|_| pd.alloc::<u8>(16).unwrap()));
        let mut tx = a.sink();
        let mut ops = futures::stream::iter((0..1000u64).map(|i| {
            let op = WriteOp::new(src.clone(), dst.remote().offset(i % 64 * 64));
            Ok(if i % 100 == 99 {
                op.with_imm(i as u32)
            } else {
                op
            })
        }));
        tx.send_all(&mut ops).await.unwrap();
        tx.close().await.unwrap();
        drop(tx);
        let mut imms = Vec::new();
        while imms.len() < 10 {
            let (wc, buf) = rx.next().await.unwrap();
            assert!(wc.is_ok());
            imms.push(wc.imm.unwrap());
            rx.repost(buf).unwrap();
        }
        assert_eq!(imms, (0..10).map(|i| i * 100 + 99).collect::<Vec<_>>());
        // Far fewer completions than writes.
        assert!(src.try_thaw().is_ok());
    });
}

#[test]
fn uc_drops_messages_without_a_receive() {
    let (_m, pd, a, b) = pair::<UC>(13, 16);
    block_on(async {
        let src = filled(&pd, 32, 0);
        a.send(src.clone()).await.unwrap();
        let recv = b.recv(pd.alloc::<u8>(32).unwrap());
        a.send(src.clone()).await.unwrap();
        let (wc, got) = recv.await.unwrap();
        assert_eq!(wc.byte_len, 32);
        assert_eq!(&got[..], &src[..]);
        let dst = pd.alloc::<u8>(32).unwrap().expose().unwrap();
        a.write(src.clone(), dst.remote()).await.unwrap();
    });
}

#[test]
fn shared_qp_across_threads() {
    let (_m, pd, a, b) = pair::<RC>(14, 64);
    let a = a.into_shared();
    let dst = pd.alloc::<u8>(4 * 64).unwrap().expose().unwrap();
    let remote = dst.remote();
    std::thread::scope(|s| {
        for t in 0..4u64 {
            let (a, pd) = (&a, pd.clone());
            s.spawn(move || {
                let src = filled(&pd, 64, t as u8);
                block_on(async {
                    for _ in 0..50 {
                        a.write(src.clone(), remote.offset(t * 64)).await.unwrap();
                    }
                });
            });
        }
    });
    let mut copy = vec![0u8; 256];
    dst.copy_to_slice(&mut copy);
    for t in 0..4 {
        assert_eq!(&copy[t * 64..t * 64 + 64], &filled(&pd, 64, t as u8)[..]);
    }
    drop(b);
}

#[test]
fn ring_buf_streams_messages() {
    let (_m, pd, a, b) = pair::<RC>(15, 16);
    let mut ring = pd.ring_buf(256).unwrap();
    block_on(async {
        let mut rx = b.receiver((0..4).map(|_| pd.alloc::<u8>(128).unwrap()));
        for i in 0..40u8 {
            let len = 1 + (i as usize * 37) % 100;
            let msg = loop {
                if let Some(m) = ring.write_with(len, |b| b.fill(i)) {
                    break m;
                }
                a.poll();
            };
            a.send(msg).await.unwrap();
            let (wc, buf) = rx.next().await.unwrap();
            assert_eq!(wc.byte_len as usize, len);
            assert!(buf[..len].iter().all(|&x| x == i));
            rx.repost(buf).unwrap();
        }
    });
    assert_eq!(ring.in_use(), 0);
}

#[test]
fn try_ref_views_types_with_invalid_bit_patterns() {
    #[derive(
        zerocopy::TryFromBytes, zerocopy::KnownLayout, zerocopy::Immutable, Debug, PartialEq,
    )]
    #[repr(u8)]
    enum Kind {
        A = 1,
        B = 2,
    }
    let (_m, pd, _a, _b) = pair::<RC>(16, 4);
    let mut buf = pd.alloc::<u8>(1).unwrap();
    assert_eq!(buf.try_ref::<Kind>(), None);
    buf[0] = 1;
    assert_eq!(buf.try_ref::<Kind>(), Some(&Kind::A));
    buf[0] = 2;
    assert_eq!(buf.freeze().try_ref::<Kind>(), Some(&Kind::B));
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

/// Random operations, drops and polls on both QPs with random delays,
/// injected errors and forced flushes. Throughout, the rings' invariants
/// hold (they are checked on every operation in debug builds); a buffer
/// handed back is never one already in hand; at the end every frozen buffer
/// thaws and nothing stays registered.
#[test]
fn random_operations_return_every_buffer_once() {
    for seed in 1..60u64 {
        let (m, pd, a, b) = pair::<RC>(seed, 1 + seed as u32 % 16);
        m.set_pace((seed * 37 % 500) as u32, 1 + (seed % 5) as u32);
        let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1);
        let dst = pd.alloc::<u8>(1024).unwrap().expose().unwrap();
        let frozen: Vec<Buf<u8, Model>> = (0..3).map(|i| filled(&pd, 256, i)).collect();
        let mut free: Vec<BufMut<u8, Model>> =
            (0..12).map(|_| pd.alloc::<u8>(256).unwrap()).collect();
        let mut in_hand: HashSet<usize> = free.iter().map(|b| b.as_ptr() as usize).collect();
        let total = free.len();
        let mut lost = 0; // buffers whose futures were dropped (released, not returned)
        let mut sends = Vec::new();
        let mut reads = Vec::new();
        let mut recvs = Vec::new();
        let give_back =
            |buf: BufMut<u8, Model>, free: &mut Vec<_>, in_hand: &mut HashSet<usize>| {
                assert!(
                    in_hand.insert(buf.as_ptr() as usize),
                    "seed {seed}: a buffer came back twice"
                );
                free.push(buf);
            };
        for _ in 0..200 {
            match rng.below(10) {
                0 | 1 => {
                    let f = &frozen[rng.below(3) as usize];
                    let s = rng.below(200) as usize;
                    sends.push(a.send(f.slice(s..s + 1 + rng.below(56) as usize)));
                }
                2 => {
                    let f = &frozen[rng.below(3) as usize];
                    sends.push(a.write(f.slice(..64), dst.remote().offset(rng.below(900))));
                }
                3 => {
                    if let Some(buf) = free.pop() {
                        in_hand.remove(&(buf.as_ptr() as usize));
                        reads.push(a.read(buf, dst.remote().slice(..256)));
                    }
                }
                4 => {
                    if let Some(buf) = free.pop() {
                        in_hand.remove(&(buf.as_ptr() as usize));
                        recvs.push(b.recv(buf));
                    }
                }
                5 => {
                    // Drop a random future: its work request goes on.
                    let k = rng.below(3);
                    if k == 0 && !sends.is_empty() {
                        let i = rng.below(sends.len() as u64) as usize;
                        drop(sends.swap_remove(i));
                    } else if k == 1 && !reads.is_empty() {
                        let i = rng.below(reads.len() as u64) as usize;
                        drop(reads.swap_remove(i));
                        lost += 1;
                    } else if k == 2 && !recvs.is_empty() {
                        let i = rng.below(recvs.len() as u64) as usize;
                        drop(recvs.swap_remove(i));
                        lost += 1;
                    }
                }
                6 => {
                    if rng.below(30) == 0 {
                        m.inject_error(a.qp_num(), Status::REM_OP_ERR);
                    } else if rng.below(40) == 0 {
                        m.force_error(b.qp_num());
                    }
                }
                _ => {
                    // Poll everything once.
                    sends.retain_mut(|s| poll_once(s).is_pending());
                    let mut i = 0;
                    while i < reads.len() {
                        match poll_once(&mut reads[i]) {
                            Poll::Ready(Ok(buf)) => give_back(buf, &mut free, &mut in_hand),
                            Poll::Ready(Err(e)) => give_back(e.buf, &mut free, &mut in_hand),
                            Poll::Pending => {
                                i += 1;
                                continue;
                            }
                        }
                        drop(reads.swap_remove(i));
                    }
                    let mut i = 0;
                    while i < recvs.len() {
                        match poll_once(&mut recvs[i]) {
                            Poll::Ready(Ok((_, buf))) => give_back(buf, &mut free, &mut in_hand),
                            Poll::Ready(Err(e)) => give_back(e.buf, &mut free, &mut in_hand),
                            Poll::Pending => {
                                i += 1;
                                continue;
                            }
                        }
                        drop(recvs.swap_remove(i));
                    }
                }
            }
            assert!(a.with(|q| q.send_ring().invariant() && q.recv_ring().invariant()));
            assert!(b.with(|q| q.send_ring().invariant() && q.recv_ring().invariant()));
        }
        // Settle: force both QPs into error so everything retires (RC
        // SENDs without a receive would wait forever otherwise).
        a.force_error().unwrap();
        b.force_error().unwrap();
        m.set_pace(0, 8);
        for s in sends {
            let _ = block_on(s);
        }
        for r in reads {
            match block_on(r) {
                Ok(buf) => give_back(buf, &mut free, &mut in_hand),
                Err(e) => give_back(e.buf, &mut free, &mut in_hand),
            }
        }
        for r in recvs {
            match block_on(r) {
                Ok((_, buf)) => give_back(buf, &mut free, &mut in_hand),
                Err(e) => give_back(e.buf, &mut free, &mut in_hand),
            }
        }
        m.run();
        a.poll();
        b.poll();
        assert_eq!(free.len() + lost, total, "seed {seed}");
        for f in frozen {
            assert!(
                f.try_thaw().is_ok(),
                "seed {seed}: a frozen buffer still held"
            );
        }
        drop((free, dst, a, b));
        assert_eq!(m.registered(), 0, "seed {seed}: a region leaked");
    }
}
