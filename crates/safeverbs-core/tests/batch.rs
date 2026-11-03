//! Batches on the model NIC and on a real device (Soft-RoCE, `scripts/rxe.sh up`): an error
//! in the middle of a batch is its outcome, the WQEs after it flush, and its buffers come
//! back once; a batch of READs reports the length of its last READ.
//!
//! The rxe tests pass without doing anything when the host has no rxe device;
//! `SAFEVERBS_TEST_DEVICE` picks another device and `SAFEVERBS_TEST_GID_INDEX` a GID.

use std::time::{Duration, Instant};

use safeverbs_core::model::Model;
use safeverbs_core::verbs::Verbs;
use safeverbs_core::{
    Access, Buf, ConnectAttr, Context, Leases, Nic, Opcode, Pd, QpCaps, QpKey, RC, Seq, Status, Wc,
    Worker, WrOp,
};

/// How long a request may take to complete on a real device.
const DEADLINE: Duration = Duration::from_secs(10);

/// The test device, or `None` (and the test passes) without one.
fn rxe() -> Option<Context<Verbs>> {
    let names = safeverbs_core::verbs::devices().unwrap();
    let name = std::env::var("SAFEVERBS_TEST_DEVICE")
        .ok()
        .or_else(|| names.into_iter().find(|n| n.contains("rxe")));
    let Some(name) = name else {
        eprintln!("no rxe device: skipped (scripts/rxe.sh up creates one)");
        return None;
    };
    Some(Context::open(Some(&name)).unwrap())
}

/// A worker on `ctx` with two RC QPs connected to each other; the first.
fn connected<N: Nic>(ctx: &Context<N>) -> (Pd<N>, Worker<N>, QpKey<RC>) {
    let pd = ctx.alloc_pd().unwrap();
    let caps = QpCaps {
        max_send_wr: 16,
        max_recv_wr: 16,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
    };
    let mut worker = Worker::new(&pd, 64).unwrap();
    let local = worker.create_qp::<RC>(caps, 4).unwrap();
    let peer = worker.create_qp::<RC>(caps, 4).unwrap();
    let port = ctx.port(1).unwrap();
    let gid_index = match std::env::var("SAFEVERBS_TEST_GID_INDEX") {
        Ok(i) if port.ethernet => Some(i.parse().expect("SAFEVERBS_TEST_GID_INDEX")),
        _ => ctx.default_gid_index(1).unwrap(),
    };
    let attr = ConnectAttr {
        port: 1,
        access: Access::LOCAL_WRITE | Access::REMOTE_WRITE | Access::REMOTE_READ,
        path_mtu: port.active_mtu,
        rq_psn: 0,
        sq_psn: 0,
        gid_index,
        traffic_class: 0,
        timeout: 14,
        retry_count: 7,
        rnr_retry: 7,
        min_rnr_timer: 12,
        max_rd_atomic: 1,
        max_dest_rd_atomic: 1,
    };
    let endpoint = worker.endpoint(peer, 1, gid_index).unwrap();
    worker.connect(local, &attr, &endpoint).unwrap();
    let endpoint = worker.endpoint(local, 1, gid_index).unwrap();
    worker.connect(peer, &attr, &endpoint).unwrap();
    (pd, worker, local)
}

/// Polls until request `seq` of `qp`'s send queue completes; its outcome and buffers.
fn wait_send<N: Nic>(worker: &mut Worker<N>, qp: QpKey<RC>, seq: Seq) -> (Wc, Leases<N>) {
    let start = Instant::now();
    loop {
        worker.progress();
        if let Some(done) = worker.take_send(qp, seq) {
            return done;
        }
        assert!(
            start.elapsed() < DEADLINE,
            "timed out waiting for request {seq}"
        );
    }
}

/// A batch of four WRITEs whose third has a bad rkey, then one more WRITE: the first two
/// execute, the third fails, and the rest flush. The batch reports the third's error with
/// its four buffers, once; the WRITE after it reports a flush.
fn error_in_the_middle<N: Nic>(ctx: Context<N>) {
    const LEN: usize = 8;
    let (pd, mut worker, local) = connected(&ctx);
    let target = pd.alloc::<u8>(4 * LEN).unwrap().expose().unwrap();
    let slot = |i: usize| {
        target
            .remote()
            .slice((i * LEN) as u64..((i + 1) * LEN) as u64)
    };
    // Four allocations, so that each buffer comes back as a lease of its own.
    let sources: Vec<Buf<u8, N>> = (0..4u8)
        .map(|i| {
            let mut b = pd.alloc::<u8>(LEN).unwrap();
            b.fill(i + 1);
            b.freeze()
        })
        .collect();
    let mut request = worker.request(local);
    for (i, src) in sources.iter().enumerate() {
        let mut dst = slot(i);
        if i == 2 {
            dst.rkey ^= 0x5a5a_0000;
        }
        request
            .push(WrOp::Write(dst), src.slice(..).into_part())
            .unwrap();
    }
    let batch = request.finish(true, false).unwrap();
    let mut request = worker.request(local);
    request
        .push(WrOp::Write(slot(3)), sources[1].slice(..).into_part())
        .unwrap();
    let after = request.finish(true, false).unwrap();
    worker.commit(local).unwrap();

    let (wc, leases) = wait_send(&mut worker, local, batch);
    assert_eq!(
        wc.status,
        Status::REM_ACCESS_ERR,
        "the first error, not the flush after it"
    );
    assert_eq!(wc.opcode(), None);
    assert_eq!(wc.byte_len(), None);
    assert_eq!(leases.len(), 4, "every buffer of the batch");
    assert!(worker.take_send(local, batch).is_none(), "taken once");
    drop(leases);
    let (wc, leases) = wait_send(&mut worker, local, after);
    assert_eq!(wc.status, Status::WR_FLUSH_ERR);
    drop(leases);
    assert_eq!(worker.send_ring(local).in_flight_wqes(), 0);
    for src in sources {
        assert!(src.try_thaw().is_ok(), "nothing holds the buffer any more");
    }
    drop(worker);
    let target = target.try_reclaim().unwrap();
    assert_eq!(&target[..LEN], &[1; LEN]);
    assert_eq!(&target[LEN..2 * LEN], &[2; LEN]);
    assert_eq!(
        &target[2 * LEN..],
        &[0; 2 * LEN],
        "the failed and flushed WRITEs did not execute"
    );
}

#[test]
fn error_in_the_middle_of_a_batch_model() {
    let model = Model::new(91);
    error_in_the_middle(Context::new(model.clone()));
    assert_eq!(model.registered(), 0);
}

#[test]
fn error_in_the_middle_of_a_batch_rxe() {
    let Some(ctx) = rxe() else { return };
    error_in_the_middle(ctx);
}

/// A batch of two READs of different lengths. Signaled, its own CQE reports the last READ's
/// length. Unsignaled, the CQE of a later READ retires it, and it reports the length of its
/// own last READ, not the later one's. (`read_batch.rs` steps through both on the model.)
fn read_batch<N: Nic>(ctx: Context<N>, signal: bool) {
    const FIRST: usize = 17;
    const LAST: usize = 31;
    const TAIL: usize = 7;
    let (pd, mut worker, local) = connected(&ctx);
    let payload: Vec<u8> = (0..128).map(|i| (i * 29 + 7) as u8).collect();
    let mut source = pd.alloc::<u8>(payload.len()).unwrap();
    source.copy_from_slice(&payload);
    let source = source.expose().unwrap();
    let (first, last) = pd.alloc::<u8>(FIRST + LAST).unwrap().split_at(FIRST);
    let mut request = worker.request(local);
    request
        .push(
            WrOp::Read(source.remote().slice(..FIRST as u64)),
            first.into_part(),
        )
        .unwrap();
    request
        .push(
            WrOp::Read(source.remote().offset(43).slice(..LAST as u64)),
            last.into_part(),
        )
        .unwrap();
    let batch = request.submit(signal).unwrap().unwrap();
    if !signal {
        let mut request = worker.request(local);
        request
            .push(
                WrOp::Read(source.remote().offset(89).slice(..TAIL as u64)),
                pd.alloc::<u8>(TAIL).unwrap().into_part(),
            )
            .unwrap();
        let tail = request.submit(true).unwrap().unwrap();
        let (wc, leases) = wait_send(&mut worker, local, tail);
        assert!(wc.is_ok(), "{wc:?}");
        assert_eq!(wc.byte_len(), Some(TAIL as u32));
        assert_eq!(
            &*leases.into_buf_mut::<u8>().unwrap(),
            &payload[89..89 + TAIL]
        );
    }
    let (wc, leases) = wait_send(&mut worker, local, batch);
    assert!(wc.is_ok(), "{wc:?}");
    assert_eq!(wc.opcode(), Some(Opcode::RDMA_READ));
    assert_eq!(wc.byte_len(), Some(LAST as u32), "the last READ's length");
    assert_eq!(leases.len(), 2, "both destinations, together");
    assert!(worker.take_send(local, batch).is_none(), "taken once");
    let first = leases.into_buf_mut::<u8>().unwrap();
    assert_eq!(&*first, &payload[..FIRST]);
    drop(first);
    drop(worker);
    assert_eq!(&*source.try_reclaim().unwrap(), payload.as_slice());
}

#[test]
fn read_batch_reports_the_last_length_rxe() {
    for signal in [true, false] {
        let Some(ctx) = rxe() else { return };
        read_batch(ctx, signal);
    }
}
