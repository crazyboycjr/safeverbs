//! M's rules for error completions, against what the devices do: each case runs once on the
//! simulated NIC ([`Model`]) and once on a real device (Soft-RoCE, `scripts/rxe.sh up`), and
//! every status a send gets is checked against what M allows it (`Qp::allows_send_status` in
//! `crates/safeverbs-spec/src/nic.rs`):
//!
//! - **M2**: a remote status or a retry counter's only on RC;
//! - **M3**: a remote access error only on an RDMA WRITE, WRITE with immediate, READ or atomic;
//! - **M4**: the RNR retry counter only when the QP was connected with fewer than 7 retries.
//!
//! They also show what the responder does (IB's Table 58), which M, single-ended, does not say:
//! the joint rules of theorem 2 will. A device test passes without doing anything when the host
//! has no RDMA device (`SAFEVERBS_TEST_DEVICE`, `SAFEVERBS_TEST_GID_INDEX` as in `rxe.rs`). Every
//! wait has a deadline.
//!
//! Not here: a link going down, which needs `CAP_NET_ADMIN` on the device's interface (root);
//! and READs beyond the responder's depth on Soft-RoCE, which does not enforce IB C9-86.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};

use futures::task::noop_waker;
use safeverbs::{
    AsyncQp, Buf, BufError, Context, Error, Model, Nic, Pd, QueuePairBuilder, RC, RemoteBuf,
    Status, Transport, UC,
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

/// Polls `f` for `time`: `None` if it is still pending then.
fn poll_for<F: Future + Unpin>(f: &mut F, time: Duration) -> Option<F::Output> {
    let waker = noop_waker();
    let mut cx = TaskContext::from_waker(&waker);
    let start = Instant::now();
    while start.elapsed() < time {
        if let Poll::Ready(out) = Pin::new(&mut *f).poll(&mut cx) {
            return Some(out);
        }
    }
    None
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

/// A device on a model of its own.
fn model(seed: u64) -> Context<Model> {
    Context::new(Model::new(seed))
}

/// What a send WQE does, as M's rules tell operations apart.
#[derive(Clone, Copy, Debug)]
enum Op {
    Send,
    Write,
    Read,
}

/// Whether M lets a send WQE doing `op` on a QP (`rc`: of a reliable connection, connected
/// with `rnr_retry`) complete with `status`: M2 to M4 on `Status::model`'s map, as
/// `Qp::allows_send_status` says them.
fn m_allows(rc: bool, op: Op, rnr_retry: u8, status: Status) -> bool {
    let remote = [
        Status::BAD_RESP_ERR,
        Status::REM_INV_REQ_ERR,
        Status::REM_ACCESS_ERR,
        Status::REM_OP_ERR,
        Status::REM_ABORT_ERR,
    ]
    .contains(&status);
    let retries = [
        Status::RETRY_EXC_ERR,
        Status::RNR_RETRY_EXC_ERR,
        Status::RESP_TIMEOUT_ERR,
    ]
    .contains(&status);
    let m2 = !(remote || retries) || rc;
    let m3 = status != Status::REM_ACCESS_ERR || matches!(op, Op::Write | Op::Read);
    let m4 = status != Status::RNR_RETRY_EXC_ERR || rnr_retry < 7;
    m2 && m3 && m4
}

/// The status a send got, `SUCCESS` for none, checked against M.
fn checked(rc: bool, op: Op, rnr_retry: u8, result: Result<(), Error>) -> Status {
    let status = match result {
        Ok(()) => Status::SUCCESS,
        Err(e) => e.status().unwrap_or_else(|| panic!("a send refused: {e}")),
    };
    assert!(
        m_allows(rc, op, rnr_retry, status),
        "{op:?} on {} with RNR retry {rnr_retry} completed with {status:?}, which M does not allow",
        if rc { "RC" } else { "UC" },
    );
    status
}

/// Two connected QPs of `ctx`'s device, `a` set up by `set_a` and `b` by `set_b`.
fn pair<T: Transport, N: Nic>(
    ctx: &Context<N>,
    set_a: impl FnOnce(&mut QueuePairBuilder<T, N>),
    set_b: impl FnOnce(&mut QueuePairBuilder<T, N>),
) -> (Pd<N>, AsyncQp<T, N>, AsyncQp<T, N>) {
    let pd = ctx.alloc_pd().unwrap();
    let builder = || {
        let mut b = QueuePairBuilder::<T, N>::new(&pd);
        b.set_send_depth(16).set_recv_depth(16);
        if ctx.nic().name() != "model"
            && let Ok(i) = std::env::var("SAFEVERBS_TEST_GID_INDEX")
        {
            b.set_gid_index(i.parse().expect("SAFEVERBS_TEST_GID_INDEX"));
        }
        b
    };
    let (mut ba, mut bb) = (builder(), builder());
    set_a(&mut ba);
    set_b(&mut bb);
    let (a, b) = (ba.build().unwrap(), bb.build().unwrap());
    let (ea, eb) = (a.endpoint(), b.endpoint());
    (pd, a.handshake(eb).unwrap(), b.handshake(ea).unwrap())
}

fn filled<N: Nic>(pd: &Pd<N>, len: usize, seed: u8) -> Buf<u8, N> {
    let mut buf = pd.alloc::<u8>(len).unwrap();
    for (i, b) in buf.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(31).wrapping_add(seed);
    }
    buf.freeze()
}

/// The status of a receive that failed.
fn failed_recv<T, B>(result: Result<T, BufError<B>>) -> Status {
    match result {
        Ok(_) => panic!("the receive succeeded"),
        Err(e) => e.error.status().expect("a completion's error"),
    }
}

// =========================================================================================
// M4: the RNR retry counter

/// A SEND with no receive posted, from a QP connected with an RNR retry count of 0: the first
/// RNR NAK ends it with `RNR_RETRY_EXC_ERR` (IB C9-132), which M allows below 7.
fn rnr_retry_zero_gives_up<N: Nic>(ctx: &Context<N>) {
    let (pd, a, _b) = pair::<RC, N>(
        ctx,
        |a| {
            a.set_rnr_retry(0);
        },
        |b| {
            b.set_min_rnr_timer(1);
        },
    );
    let status = checked(true, Op::Send, 0, wait(a.send(filled(&pd, 64, 1))));
    assert_eq!(status, Status::RNR_RETRY_EXC_ERR);
}

#[test]
fn model_rnr_retry_zero_gives_up() {
    rnr_retry_zero_gives_up(&model(1));
}

#[test]
fn device_rnr_retry_zero_gives_up() {
    let Some(ctx) = device() else { return };
    rnr_retry_zero_gives_up(&ctx);
}

/// The same with an RNR retry count of 7, which is infinite (IB 9.9.2.1.1): nothing fails while
/// no receive is posted, and the SEND completes once one is.
fn rnr_retry_seven_waits<N: Nic>(ctx: &Context<N>) {
    let (pd, a, b) = pair::<RC, N>(
        ctx,
        |a| {
            a.set_rnr_retry(7);
        },
        |b| {
            b.set_min_rnr_timer(1);
        },
    );
    let src = filled(&pd, 64, 2);
    let mut send = Box::pin(a.send(src.clone()));
    assert!(
        poll_for(&mut send, Duration::from_millis(500)).is_none(),
        "a SEND with no receive completed with an RNR retry count of 7"
    );
    let recv = b.recv(pd.alloc::<u8>(64).unwrap());
    let status = checked(true, Op::Send, 7, wait(send));
    assert_eq!(status, Status::SUCCESS);
    let (_, got) = wait(recv).unwrap();
    assert_eq!(&got[..], &src[..]);
}

#[test]
fn model_rnr_retry_seven_waits() {
    rnr_retry_seven_waits(&model(2));
}

#[test]
fn device_rnr_retry_seven_waits() {
    let Some(ctx) = device() else { return };
    rnr_retry_seven_waits(&ctx);
}

// =========================================================================================
// M3: a remote access error, on a WRITE only

/// A WRITE to `to`, which the responder refuses: `REM_ACCESS_ERR` at the requester (IB C9-89),
/// and the responder's QP goes to the error state (Table 58, Class C: an R_Key violation not
/// tied to a receive is an affiliated asynchronous error, which the library does not read), so
/// the receive it had posted is flushed.
fn refused_write<N: Nic>(ctx: &Context<N>, to: impl FnOnce(RemoteBuf) -> RemoteBuf) {
    let (pd, a, b) = pair::<RC, N>(ctx, |_| {}, |_| {});
    let dst = pd.alloc::<u8>(64).unwrap().expose().unwrap();
    let recv = b.recv(pd.alloc::<u8>(64).unwrap());
    let status = checked(
        true,
        Op::Write,
        7,
        wait(a.write(filled(&pd, 64, 3), to(dst.remote()))),
    );
    assert_eq!(status, Status::REM_ACCESS_ERR);
    assert_eq!(failed_recv(wait(recv)), Status::WR_FLUSH_ERR);
    let e = wait(b.send(filled(&pd, 8, 4))).unwrap_err();
    assert_eq!(
        e.status(),
        Some(Status::WR_FLUSH_ERR),
        "the responder is in error"
    );
}

/// A key the region does not have.
fn bad_rkey(r: RemoteBuf) -> RemoteBuf {
    RemoteBuf {
        rkey: r.rkey ^ 0x5a5a_0000,
        ..r
    }
}

/// The region's key, past its end (a zero-length WRITE would not be checked: IB C9-88).
fn out_of_bounds(r: RemoteBuf) -> RemoteBuf {
    RemoteBuf {
        addr: r.addr + r.len / 2,
        ..r
    }
}

#[test]
fn model_write_with_a_bad_rkey() {
    refused_write(&model(3), bad_rkey);
}

#[test]
fn device_write_with_a_bad_rkey() {
    let Some(ctx) = device() else { return };
    refused_write(&ctx, bad_rkey);
}

#[test]
fn model_write_out_of_bounds() {
    refused_write(&model(4), out_of_bounds);
}

#[test]
fn device_write_out_of_bounds() {
    let Some(ctx) = device() else { return };
    refused_write(&ctx, out_of_bounds);
}

/// A SEND longer than the receive it takes: an error at the requester that M allows a SEND, and
/// the receive fails; then the responder is in the error state, and the receive posted after it
/// is flushed. IB says which errors (C9-91, Table 58: a length error, Class C, a NAK of an
/// invalid request; 11.6.2: a local length error on the receive), but M, single-ended, does not,
/// and says nothing of receives: `send_status` and `recv_status` are what the device reports.
///
/// A receive posted once the responder is in the error state: `flushed_alone` says whether the
/// device flushes it by itself; if not, moving the QP to the error state again does, as the
/// drain of a removal does.
fn long_send<N: Nic>(
    ctx: &Context<N>,
    send_status: Status,
    recv_status: Status,
    flushed_alone: bool,
) {
    let (pd, a, b) = pair::<RC, N>(ctx, |_| {}, |_| {});
    let recv = b.recv(pd.alloc::<u8>(64).unwrap());
    let next = b.recv(pd.alloc::<u8>(64).unwrap());
    let status = checked(true, Op::Send, 7, wait(a.send(filled(&pd, 128, 5))));
    assert_eq!(status, send_status);
    assert_eq!(failed_recv(wait(recv)), recv_status);
    assert_eq!(
        failed_recv(wait(next)),
        Status::WR_FLUSH_ERR,
        "the responder is in error"
    );
    let mut late = Box::pin(b.recv(pd.alloc::<u8>(64).unwrap()));
    let alone = poll_for(&mut late, Duration::from_millis(300));
    assert_eq!(alone.is_some(), flushed_alone);
    let late = match alone {
        Some(late) => late,
        None => {
            b.force_error().unwrap();
            wait(late)
        }
    };
    assert_eq!(failed_recv(late), Status::WR_FLUSH_ERR);
}

/// The model does what IB says: an invalid request at the requester, a local length error on
/// the receive.
#[test]
fn model_send_longer_than_its_receive() {
    long_send(
        &model(5),
        Status::REM_INV_REQ_ERR,
        Status::LOC_LEN_ERR,
        true,
    );
}

/// Soft-RoCE takes the receive for a malformed WQE (IB's Responder Class A): a remote operation
/// error at the requester and a local QP operation error on the receive (`send_data_in` in
/// `rxe_resp.c`: a copy that does not fit is not `-ENOSPC`, so `RESPST_ERR_MALFORMED_WQE`). A
/// receive posted after that waits until the QP is moved to the error state again: its
/// provider posts receives without a system call, so the kernel's responder hears of it only
/// then.
#[test]
fn device_send_longer_than_its_receive() {
    let Some(ctx) = device() else { return };
    long_send(&ctx, Status::REM_OP_ERR, Status::LOC_QP_OP_ERR, false);
}

// =========================================================================================
// Retries and depth

/// A WRITE to a peer QP that was destroyed: nothing answers, and after the transport's retries
/// the WQE completes with `RETRY_EXC_ERR` (IB 9.9.2.1.1). A short ACK timeout (4.096 µs ×
/// 2^10, about 4 ms) and one retry keep it to a few milliseconds.
fn write_to_a_destroyed_peer<N: Nic>(ctx: &Context<N>) {
    let (pd, a, b) = pair::<RC, N>(
        ctx,
        |a| {
            a.set_timeout(10).set_retry_count(1);
        },
        |_| {},
    );
    let dst = pd.alloc::<u8>(64).unwrap().expose().unwrap();
    drop(b);
    let start = Instant::now();
    let status = checked(
        true,
        Op::Write,
        7,
        wait(a.write(filled(&pd, 64, 6), dst.remote())),
    );
    assert_eq!(status, Status::RETRY_EXC_ERR);
    eprintln!("RETRY_EXC_ERR after {:?}", start.elapsed());
}

#[test]
fn model_write_to_a_destroyed_peer() {
    write_to_a_destroyed_peer(&model(6));
}

#[test]
fn device_write_to_a_destroyed_peer() {
    let Some(ctx) = device() else { return };
    write_to_a_destroyed_peer(&ctx);
}

/// READs past the responder's depth (IB C9-86): four go out together from a QP that may have
/// four outstanding, to a peer that takes one; the second is refused with `REM_INV_REQ_ERR` and
/// the rest flush. On the model only: Soft-RoCE recycles its responder resources instead
/// (`check_resource` in `rxe_resp.c`), and refuses only at a depth of 0 (below).
#[test]
fn model_reads_beyond_the_responder_depth() {
    let ctx = model(7);
    let (pd, a, _b) = pair::<RC, Model>(
        &ctx,
        |a| {
            a.set_max_rd_atomic(4);
        },
        |b| {
            b.set_max_dest_rd_atomic(1);
        },
    );
    let src = pd.alloc::<u8>(64).unwrap().expose().unwrap();
    let reads: Vec<_> = (0..4)
        .map(|_| a.read(pd.alloc::<u8>(64).unwrap(), src.remote()))
        .collect();
    let statuses: Vec<Status> = reads
        .into_iter()
        .map(|r| checked(true, Op::Read, 7, wait(r).map(drop).map_err(|e| e.error)))
        .collect();
    assert_eq!(
        statuses,
        [
            Status::SUCCESS,
            Status::REM_INV_REQ_ERR,
            Status::WR_FLUSH_ERR,
            Status::WR_FLUSH_ERR
        ]
    );
}

/// A READ to a peer that takes none (a responder depth of 0): `REM_INV_REQ_ERR` (IB C9-86),
/// on both.
fn read_to_a_depth_of_zero<N: Nic>(ctx: &Context<N>) {
    let (pd, a, _b) = pair::<RC, N>(
        ctx,
        |_| {},
        |b| {
            b.set_max_dest_rd_atomic(0);
        },
    );
    let src = pd.alloc::<u8>(64).unwrap().expose().unwrap();
    let result = wait(a.read(pd.alloc::<u8>(64).unwrap(), src.remote()));
    let status = checked(true, Op::Read, 7, result.map(drop).map_err(|e| e.error));
    assert_eq!(status, Status::REM_INV_REQ_ERR);
}

#[test]
fn model_read_to_a_depth_of_zero() {
    read_to_a_depth_of_zero(&model(8));
}

#[test]
fn device_read_to_a_depth_of_zero() {
    let Some(ctx) = device() else { return };
    read_to_a_depth_of_zero(&ctx);
}

// =========================================================================================
// M2: nothing remote on UC

/// On UC the requester hears nothing from the other end (IB 9.9.2.2: remotely detected errors
/// are unique to reliable services): a SEND with no receive posted and a WRITE with a bad key
/// both succeed, the responder dropping them (Table 58, Classes D and E), and it still takes a
/// SEND once a receive is posted.
fn uc_hears_nothing<N: Nic>(ctx: &Context<N>) {
    let (pd, a, b) = pair::<UC, N>(ctx, |_| {}, |_| {});
    let dst = pd.alloc::<u8>(64).unwrap().expose().unwrap();
    let status = checked(false, Op::Send, 7, wait(a.send(filled(&pd, 64, 7))));
    assert_eq!(status, Status::SUCCESS);
    let write = a.write(filled(&pd, 64, 8), bad_rkey(dst.remote()));
    assert_eq!(checked(false, Op::Write, 7, wait(write)), Status::SUCCESS);
    let recv = b.recv(pd.alloc::<u8>(64).unwrap());
    let src = filled(&pd, 64, 9);
    assert_eq!(
        checked(false, Op::Send, 7, wait(a.send(src.clone()))),
        Status::SUCCESS
    );
    let (_, got) = wait(recv).unwrap();
    assert_eq!(&got[..], &src[..]);
}

#[test]
fn model_uc_hears_nothing() {
    uc_hears_nothing(&model(9));
}

#[test]
fn device_uc_hears_nothing() {
    let Some(ctx) = device() else { return };
    uc_hears_nothing(&ctx);
}
