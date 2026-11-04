//! Caught user wake panics preserve every already-polled completion and its leases.

#![forbid(unsafe_code)]
use safeverbs_core::model::Model;
use safeverbs_core::{
    Access, CompletionError, ConnectAttr, Context, Engine, Pd, QpCaps, QpKey, RC, Received, Status,
    WrOp,
};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
struct Notify {
    calls: Arc<AtomicUsize>,
    panic: Option<&'static str>,
}

impl Wake for Notify {
    fn wake(self: Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(message) = self.panic {
            std::panic::panic_any(message);
        }
    }
}

fn waker(calls: &Arc<AtomicUsize>, panic: Option<&'static str>) -> Waker {
    Waker::from(Arc::new(Notify {
        calls: calls.clone(),
        panic,
    }))
}

/// A model, its protection domain, and a worker with two QPs connected to each other.
type Pair = (Model, Pd<Model>, Engine<(), Model>, QpKey<RC>, QpKey<RC>);

fn pair(seed: u64) -> Pair {
    let model = Model::new(seed);
    model.set_pace(0, 64);
    let pd = Context::new(model.clone()).alloc_pd().unwrap();
    let mut worker = Engine::new(pd.context(), 32).unwrap();
    let caps = QpCaps {
        max_send_wr: 8,
        max_recv_wr: 8,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
    };
    let local = worker.create_qp::<RC>(&pd, caps, 2).unwrap();
    let peer = worker.create_qp::<RC>(&pd, caps, 2).unwrap();
    let attr = ConnectAttr {
        port: 1,
        access: Access::LOCAL_WRITE | Access::REMOTE_WRITE,
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
    };
    let endpoint = worker.endpoint(peer, 1, None).unwrap();
    worker.connect(local, &attr, &endpoint).unwrap();
    let endpoint = worker.endpoint(local, 1, None).unwrap();
    worker.connect(peer, &attr, &endpoint).unwrap();
    (model, pd, worker, local, peer)
}

#[test]
fn caught_waker_panic_preserves_other_hardware_outcomes() {
    let (model, pd, mut worker, local, _) = pair(111);
    let target = pd.alloc::<u8>(1).unwrap().expose().unwrap();
    let mut source = pd.alloc::<u8>(1).unwrap();
    source.fill(0xa5);
    let source = source.freeze();

    let mut request = worker.request(local);
    request
        .push(WrOp::Write(target.remote()), source.clone().into_part())
        .unwrap();
    let first = request.finish(true, ()).unwrap();
    let mut invalid = target.remote();
    invalid.rkey ^= 0x40000000;
    let mut request = worker.request(local);
    request
        .push(WrOp::Write(invalid), source.clone().into_part())
        .unwrap();
    let second = request.finish(true, ()).unwrap();
    worker.commit(local).unwrap();
    assert_eq!(model.run(), 2);
    let calls = Arc::new(AtomicUsize::new(0));
    worker.set_send_waker(local, first, &waker(&calls, Some("user waker panic")));
    worker.set_send_waker(local, second, &waker(&calls, None));
    assert!(catch_unwind(AssertUnwindSafe(|| worker.progress())).is_err());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "later notifications still run"
    );
    assert!(worker.take_send(local, first).unwrap().result.is_ok());
    let mut request = worker.request(local);
    request
        .push(WrOp::Write(target.remote()), source.into_part())
        .unwrap();
    let third = request.submit(true, ()).unwrap();
    assert_eq!(model.run(), 1);
    worker.progress();
    let second_result = worker.take_send(local, second).unwrap().result;
    let third_result = worker.take_send(local, third).unwrap().result;
    assert_eq!(
        third_result.unwrap_err().status(),
        Some(Status::WR_FLUSH_ERR)
    );
    assert_eq!(
        second_result.unwrap_err().status(),
        Some(Status::REM_ACCESS_ERR),
        "caught user panic must not turn a consumed error CQE into inferred success"
    );
}

#[test]
fn one_cqe_finishes_every_inferred_request_before_resuming_panic() {
    let (model, pd, mut worker, local, _) = pair(112);
    let target = pd.alloc::<u8>(1).unwrap().expose().unwrap();
    let mut source = pd.alloc::<u8>(1).unwrap();
    source.fill(0xa5);
    let source = source.freeze();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut seqs = Vec::new();
    for i in 0..3 {
        let mut request = worker.request(local);
        request
            .push(WrOp::Write(target.remote()), source.clone().into_part())
            .unwrap();
        let seq = request.finish(i == 2, ()).unwrap();
        seqs.push(seq);
        worker.set_send_waker(
            local,
            seq,
            &waker(
                &calls,
                match i {
                    0 => Some("first wake panic"),
                    1 => Some("second wake panic"),
                    _ => None,
                },
            ),
        );
    }
    worker.commit(local).unwrap();
    assert_eq!(model.run(), 3);
    let panic = catch_unwind(AssertUnwindSafe(|| worker.progress())).unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"first wake panic"));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    for seq in seqs {
        assert!(worker.take_send(local, seq).unwrap().result.is_ok());
        assert!(worker.take_send(local, seq).is_none());
    }
    assert_eq!(worker.send_ring(local).in_flight_wqes(), 0);
    assert!(source.try_thaw().is_ok());
    drop(worker);
    assert_eq!(&*target.try_reclaim().unwrap(), &[0xa5]);
    assert_eq!(model.registered(), 0);
}

#[test]
fn refused_doorbell_notifies_all_owners_and_keeps_sequence_numbers() {
    let (model, pd, mut worker, local, _) = pair(113);
    let target = pd.alloc::<u8>(1).unwrap().expose().unwrap();
    let source = pd.alloc::<u8>(1).unwrap().freeze();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut seqs = Vec::new();
    for i in 0..2 {
        let mut request = worker.request(local);
        request
            .push(WrOp::Write(target.remote()), source.clone().into_part())
            .unwrap();
        let seq = request.finish(true, ()).unwrap();
        seqs.push(seq);
        worker.set_send_waker(
            local,
            seq,
            &waker(
                &calls,
                if i == 0 {
                    Some("refusal wake panic")
                } else {
                    None
                },
            ),
        );
    }
    model.refuse_doorbells(worker.qp_num(local), 1);
    assert!(catch_unwind(AssertUnwindSafe(|| worker.commit(local))).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    for &seq in &seqs {
        let done = worker.take_send(local, seq).unwrap();
        assert_eq!(done.result, Err(CompletionError::Refused));
        drop(done);
        assert!(worker.take_send(local, seq).is_none());
    }
    let mut request = worker.request(local);
    request
        .push(WrOp::Write(target.remote()), source.clone().into_part())
        .unwrap();
    let next = request.submit(true, ()).unwrap();
    assert!(next > seqs[1]);
    assert_eq!(model.run(), 1);
    worker.progress();
    assert!(worker.take_send(local, next).unwrap().result.is_ok());
    assert!(source.try_thaw().is_ok());
    drop(worker);
    drop(target.try_reclaim().unwrap());
    assert_eq!(model.registered(), 0);
}

#[test]
fn receive_wake_panic_preserves_other_qps_on_the_shared_cq() {
    let (model, pd, mut worker, local, peer) = pair(114);
    let recv_local = worker
        .post_recv(local, pd.alloc::<u8>(1).unwrap().into_part(), ())
        .unwrap();
    let recv_peer = worker
        .post_recv(peer, pd.alloc::<u8>(1).unwrap().into_part(), ())
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    worker.set_recv_waker(
        local,
        recv_local,
        &waker(&calls, Some("receive wake panic")),
    );
    worker.set_recv_waker(peer, recv_peer, &waker(&calls, None));
    let mut seqs = Vec::new();
    for (qp, byte) in [(local, 0xa5), (peer, 0x5a)] {
        let mut source = pd.alloc::<u8>(1).unwrap();
        source.fill(byte);
        let mut request = worker.request(qp);
        request
            .push(WrOp::Send, source.freeze().into_part())
            .unwrap();
        seqs.push((qp, request.submit(true, ()).unwrap()));
    }
    model.run();
    assert!(catch_unwind(AssertUnwindSafe(|| worker.progress())).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    for (qp, seq) in seqs {
        assert!(worker.take_send(qp, seq).unwrap().result.is_ok());
    }
    for (qp, seq, byte) in [(local, recv_local, 0x5a), (peer, recv_peer, 0xa5)] {
        let done = worker.take_recv(qp, seq).unwrap();
        assert_eq!(
            done.result,
            Ok(safeverbs_core::Completed::Received(Received::Message {
                len: 1,
                imm: None
            }))
        );
        let buf = done.leases.into_buf_mut::<u8>().unwrap();
        assert_eq!(&*buf, &[byte]);
        assert!(worker.take_recv(qp, seq).is_none());
    }
    drop(worker);
    assert_eq!(model.registered(), 0);
}
