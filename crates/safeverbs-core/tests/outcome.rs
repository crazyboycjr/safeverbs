//! Error aggregation stays within each native request's own WQE interval.

use safeverbs_core::model::Model;
use safeverbs_core::ring::State;
use safeverbs_core::{
    Access, CompletionError, ConnectAttr, Context, QpCaps, RC, Status, Worker, WrOp,
};

fn adjacent_errors(first_wqes: usize, signal_first: bool) {
    let model = Model::new(97);
    model.set_pace(0, 1);
    let pd = Context::new(model.clone()).alloc_pd().unwrap();
    let mut worker: Worker<(), _> = Worker::new(&pd, 32).unwrap();
    let caps = QpCaps {
        max_send_wr: 8,
        max_recv_wr: 8,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
    };
    let local = worker.create_qp::<RC>(caps, 2).unwrap();
    let peer = worker.create_qp::<RC>(caps, 2).unwrap();
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
    let target = pd.alloc::<u8>(1).unwrap().expose().unwrap();
    let mut source = pd.alloc::<u8>(1).unwrap();
    source.fill(0xa5);
    let source = source.freeze();

    let mut request = worker.request(local);
    for _ in 0..first_wqes {
        request
            .push(WrOp::Write(target.remote()), source.slice(..).into_part())
            .unwrap();
    }
    let first = request.finish(signal_first, ()).unwrap();
    let mut request = worker.request(local);
    for _ in 0..2 {
        request
            .push(WrOp::Write(target.remote()), source.slice(..).into_part())
            .unwrap();
    }
    let next = request.finish(true, ()).unwrap();
    worker.commit(local).unwrap();
    model.inject_error(worker.qp_num(local), Status::REM_ACCESS_ERR);

    // Each poll executes exactly one WQE, making the partial prefix observable.
    assert_eq!(worker.progress(), 1);
    assert_eq!(worker.send_ring(local).state(next), State::InFlight);
    if first_wqes > 1 {
        assert!(worker.take_send(local, first).is_none());
        assert_eq!(worker.send_ring(local).state(first), State::InFlight);
        assert_eq!(worker.progress(), 1);
    }
    let done = worker.take_send(local, first).unwrap();
    assert_eq!(
        done.result,
        Err(CompletionError::Failed {
            status: Status::REM_ACCESS_ERR,
            vendor_err: 0,
            position: 0
        }),
        "first error survives its flush"
    );
    drop(done);
    assert_eq!(worker.send_ring(local).state(next), State::InFlight);
    assert!(worker.take_send(local, next).is_none());

    assert_eq!(worker.progress(), 1);
    assert!(
        worker.take_send(local, next).is_none(),
        "a proper prefix is not complete"
    );
    assert_eq!(worker.progress(), 1);
    let done = worker.take_send(local, next).unwrap();
    assert_eq!(
        done.result,
        Err(CompletionError::Failed {
            status: Status::WR_FLUSH_ERR,
            vendor_err: 0,
            position: 0
        }),
        "the preceding request's error must not spill"
    );
    drop(done);
    assert!(worker.take_send(local, first).is_none());
    assert!(worker.take_send(local, next).is_none());
    assert_eq!(worker.send_ring(local).in_flight_wqes(), 0);
    drop(worker);
    assert!(source.try_thaw().is_ok());
    let target = target.try_reclaim().unwrap();
    assert_eq!(
        &*target,
        &[0],
        "the model wrote nothing for the WQEs it failed"
    );
    drop(target);
    assert_eq!(model.registered(), 0);
}

#[test]
fn first_error_is_retained_only_by_its_own_request() {
    for first_wqes in [1, 2] {
        for signal_first in [false, true] {
            adjacent_errors(first_wqes, signal_first);
        }
    }
}
