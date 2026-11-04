//! Batches on the model NIC and on a real device (Soft-RoCE, `scripts/rxe.sh up`): an error
//! in a batch is its outcome, with its position, the WQEs after it flush, and its context and
//! buffers come back once; a batch of READs reports the length of its last READ.
//!
//! The rxe tests pass without doing anything when the host has no rxe device;
//! `SAFEVERBS_TEST_DEVICE` picks another device and `SAFEVERBS_TEST_GID_INDEX` a GID.

use std::time::{Duration, Instant};

use safeverbs_core::model::Model;
use safeverbs_core::verbs::Verbs;
use safeverbs_core::{
    Access, Buf, Completed, Completion, CompletionError, ConnectAttr, Context, Engine, Nic, Pd,
    QpCaps, QpKey, RC, Seq, Status, WrOp,
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

/// An engine on `ctx` with two RC QPs connected to each other; the first.
fn connected<C, N: Nic>(ctx: &Context<N>) -> (Pd<N>, Engine<C, N>, QpKey<RC>) {
    let pd = ctx.alloc_pd().unwrap();
    let caps = QpCaps {
        max_send_wr: 16,
        max_recv_wr: 16,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
    };
    let mut engine = Engine::new(ctx, 64).unwrap();
    let local = engine.create_qp::<RC>(&pd, caps, 4).unwrap();
    let peer = engine.create_qp::<RC>(&pd, caps, 4).unwrap();
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
    let endpoint = engine.endpoint(peer, 1, gid_index).unwrap();
    engine.connect(local, &attr, &endpoint).unwrap();
    let endpoint = engine.endpoint(local, 1, gid_index).unwrap();
    engine.connect(peer, &attr, &endpoint).unwrap();
    (pd, engine, local)
}

/// Polls until request `seq` of `qp`'s send queue completes.
fn wait_send<C, N: Nic>(engine: &mut Engine<C, N>, qp: QpKey<RC>, seq: Seq) -> Completion<C, N> {
    let start = Instant::now();
    loop {
        engine.progress();
        if let Some(done) = engine.take_send(qp, seq) {
            return done;
        }
        assert!(
            start.elapsed() < DEADLINE,
            "timed out waiting for request {seq}"
        );
    }
}

/// A batch of four WRITEs, the one at `bad` with a bad rkey, then one more WRITE: those before
/// it execute, it fails, and the rest flush. The batch reports that error at its position, with
/// its context and its four buffers, once; the WRITE after it reports a flush at position 0,
/// its own first WQE.
fn error_in_a_batch<N: Nic>(ctx: Context<N>, bad: usize) {
    const LEN: usize = 8;
    let (pd, mut engine, local) = connected::<&'static str, N>(&ctx);
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
    let mut request = engine.request(local);
    for (i, src) in sources.iter().enumerate() {
        let mut dst = slot(i);
        if i == bad {
            dst.rkey ^= 0x5a5a_0000;
        }
        request
            .push(WrOp::Write(dst), src.slice(..).into_part())
            .unwrap();
    }
    let batch = request.finish(true, "the batch").unwrap();
    let mut request = engine.request(local);
    request
        .push(WrOp::Write(slot(3)), sources[1].slice(..).into_part())
        .unwrap();
    let after = request.finish(true, "the WRITE after it").unwrap();
    engine.commit(local).unwrap();

    let done = wait_send(&mut engine, local, batch);
    assert_eq!(done.wr_id, "the batch");
    let Err(CompletionError::Failed {
        status, position, ..
    }) = done.result
    else {
        panic!("the batch completed with {:?}", done.result);
    };
    assert_eq!(
        status,
        Status::REM_ACCESS_ERR,
        "the first error, not the flush after it"
    );
    assert_eq!(position as usize, bad, "the WQE that failed");
    assert_eq!(done.leases.len(), 4, "every buffer of the batch");
    assert!(engine.take_send(local, batch).is_none(), "taken once");
    drop(done);
    let done = wait_send(&mut engine, local, after);
    assert_eq!(done.wr_id, "the WRITE after it");
    assert_eq!(
        done.result.unwrap_err().status(),
        Some(Status::WR_FLUSH_ERR)
    );
    assert!(
        matches!(
            done.result,
            Err(CompletionError::Failed { position: 0, .. })
        ),
        "the earlier request's error did not spill into it"
    );
    drop(done);
    assert_eq!(engine.send_ring(local).in_flight_wqes(), 0);
    for src in sources {
        assert!(src.try_thaw().is_ok(), "nothing holds the buffer any more");
    }
    drop(engine);
    let target = target.try_reclaim().unwrap();
    for i in 0..4 {
        // Only what these devices do here: an error says nothing of the remote effects
        // of the WQEs from it on, which M leaves unknown.
        let want = if i < bad { i as u8 + 1 } else { 0 };
        assert_eq!(&target[i * LEN..(i + 1) * LEN], &[want; LEN], "slot {i}");
    }
}

#[test]
fn error_in_a_batch_model() {
    for bad in [0, 2, 3] {
        let model = Model::new(91);
        error_in_a_batch(Context::new(model.clone()), bad);
        assert_eq!(model.registered(), 0);
    }
}

#[test]
fn error_in_a_batch_rxe() {
    for bad in [0, 2, 3] {
        let Some(ctx) = rxe() else { return };
        error_in_a_batch(ctx, bad);
    }
}

/// An error injected after the first WQEs of a batch ran: the position counts the ones that
/// completed, unsignaled and with no completion of their own.
#[test]
fn position_counts_the_wqes_that_completed() {
    for done in 0..4 {
        let model = Model::new(92);
        model.set_pace(0, 1);
        let ctx = Context::new(model.clone());
        let (pd, mut engine, local) = connected::<(), Model>(&ctx);
        let target = pd.alloc::<u8>(8).unwrap().expose().unwrap();
        let source = pd.alloc::<u8>(8).unwrap().freeze();
        let mut request = engine.request(local);
        for _ in 0..4 {
            request
                .push(WrOp::Write(target.remote()), source.slice(..).into_part())
                .unwrap();
        }
        let batch = request.submit(true, ()).unwrap();
        // Each poll executes one WQE, leaving a CQE only for an error or the signaled last.
        for _ in 0..done {
            assert_eq!(engine.progress(), 0);
        }
        model.inject_error(engine.qp_num(local), Status::REM_OP_ERR);
        let finished = wait_send(&mut engine, local, batch);
        assert!(
            matches!(
                finished.result,
                Err(CompletionError::Failed { status, position, .. })
                    if status == Status::REM_OP_ERR && position == done
            ),
            "{:?} after {done} WQEs",
            finished.result
        );
    }
}

/// A batch of two READs of different lengths. Signaled, its own CQE reports the last READ's
/// length. Unsignaled, the CQE of a later READ retires it, and it reports the length of its
/// own last READ, not the later one's. (`read_batch.rs` steps through both on the model.)
fn read_batch<N: Nic>(ctx: Context<N>, signal: bool) {
    const FIRST: usize = 17;
    const LAST: usize = 31;
    const TAIL: usize = 7;
    let (pd, mut engine, local) = connected::<(), N>(&ctx);
    let payload: Vec<u8> = (0..128).map(|i| (i * 29 + 7) as u8).collect();
    let mut source = pd.alloc::<u8>(payload.len()).unwrap();
    source.copy_from_slice(&payload);
    let source = source.expose().unwrap();
    let (first, last) = pd.alloc::<u8>(FIRST + LAST).unwrap().split_at(FIRST);
    let mut request = engine.request(local);
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
    let batch = request.submit(signal, ()).unwrap();
    if !signal {
        let mut request = engine.request(local);
        request
            .push(
                WrOp::Read(source.remote().offset(89).slice(..TAIL as u64)),
                pd.alloc::<u8>(TAIL).unwrap().into_part(),
            )
            .unwrap();
        let tail = request.submit(true, ()).unwrap();
        let done = wait_send(&mut engine, local, tail);
        assert_eq!(done.result, Ok(Completed::Read { len: TAIL as u32 }));
        assert_eq!(
            &*done.leases.into_buf_mut::<u8>().unwrap(),
            &payload[89..89 + TAIL]
        );
    }
    let done = wait_send(&mut engine, local, batch);
    assert_eq!(
        done.result,
        Ok(Completed::Read { len: LAST as u32 }),
        "the last READ's length"
    );
    assert_eq!(done.leases.len(), 2, "both destinations, together");
    assert!(engine.take_send(local, batch).is_none(), "taken once");
    let first = done.leases.into_buf_mut::<u8>().unwrap();
    assert_eq!(&*first, &payload[..FIRST]);
    drop(first);
    drop(engine);
    assert_eq!(&*source.try_reclaim().unwrap(), payload.as_slice());
}

#[test]
fn read_batch_reports_the_last_length_rxe() {
    for signal in [true, false] {
        let Some(ctx) = rxe() else { return };
        read_batch(ctx, signal);
    }
}
