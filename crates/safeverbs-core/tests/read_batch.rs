//! A request's inferred READ result describes its final WQE, as a CQE does.

use safeverbs_core::model::Model;
use safeverbs_core::{Access, ConnectAttr, Context, Opcode, QpCaps, RC, Wc, Worker, WrOp};

fn two_read_result(signal: bool) -> Wc {
    const FIRST: usize = 17;
    const LAST: usize = 31;
    const TAIL: usize = 7;
    let model = Model::new(83);
    let pd = Context::new(model.clone()).alloc_pd().unwrap();
    let mut worker = Worker::new(&pd, 32).unwrap();
    let caps = QpCaps {
        max_send_wr: 8,
        max_recv_wr: 8,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
    };
    // Two WQEs fit below depth - max_batch, so finish(false) is unsignaled.
    let local = worker.create_qp::<RC>(caps, 2).unwrap();
    let peer = worker.create_qp::<RC>(caps, 2).unwrap();
    let attr = ConnectAttr {
        port: 1,
        access: Access::LOCAL_WRITE | Access::REMOTE_READ,
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

    let payload: Vec<u8> = (0..128).map(|i| (i * 29 + 7) as u8).collect();
    let mut source = pd.alloc::<u8>(payload.len()).unwrap();
    source.copy_from_slice(&payload);
    let source = source.expose().unwrap();
    // Disjoint unique destinations, not repeated READs into one allocation view.
    let (first, last) = pd.alloc::<u8>(FIRST + LAST).unwrap().split_at(FIRST);
    let first_ptr = first.as_ptr();
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
    assert_eq!(model.run(), 2);

    if signal {
        assert_eq!(worker.progress(), 1);
    } else {
        assert_eq!(worker.progress(), 0, "the two READs produced no CQE");
        assert!(worker.take_send(local, batch).is_none());
        assert_eq!(worker.send_ring(local).in_flight_wqes(), 2);
        // A different length makes an accidental reuse of this later CQE fail too.
        let mut request = worker.request(local);
        request
            .push(
                WrOp::Read(source.remote().offset(89).slice(..TAIL as u64)),
                pd.alloc::<u8>(TAIL).unwrap().into_part(),
            )
            .unwrap();
        let tail = request.submit(true).unwrap().unwrap();
        assert_eq!(model.run(), 1);
        assert_eq!(worker.progress(), 1);
        let (wc, leases) = worker.take_send(local, tail).unwrap();
        assert!(wc.is_ok(), "{wc:?}");
        assert_eq!(wc.opcode(), Some(Opcode::RDMA_READ));
        assert_eq!(wc.byte_len(), Some(TAIL as u32));
        assert_eq!(
            &*leases.into_buf_mut::<u8>().unwrap(),
            &payload[89..89 + TAIL]
        );
    }

    let (wc, leases) = worker.take_send(local, batch).unwrap();
    assert!(wc.is_ok(), "{wc:?}");
    assert_eq!(wc.opcode(), Some(Opcode::RDMA_READ));
    assert_eq!(wc.byte_len(), Some(LAST as u32));
    assert_eq!(wc.received_len(), None);
    assert_eq!(wc.imm(), None);
    assert_eq!(
        leases.len(),
        2,
        "both disjoint destinations returned together"
    );
    let first = leases.into_buf_mut::<u8>().unwrap();
    assert_eq!(first.as_ptr(), first_ptr);
    assert_eq!(&*first, &payload[..FIRST]);
    assert!(worker.take_send(local, batch).is_none());
    assert_eq!(worker.send_ring(local).in_flight_wqes(), 0);
    drop(first);
    drop(worker);
    let source = source.try_reclaim().unwrap();
    assert_eq!(&*source, payload.as_slice());
    drop(source);
    assert_eq!(model.registered(), 0);
    wc
}

#[test]
fn inferred_and_signaled_read_batches_report_the_final_wqe_length() {
    let signaled = two_read_result(true);
    let inferred = two_read_result(false);
    assert_eq!(inferred.status, signaled.status);
    assert_eq!(inferred.opcode(), signaled.opcode());
    assert_eq!(inferred.byte_len(), signaled.byte_len());
    assert_eq!(inferred.imm(), signaled.imm());
}
