//! WRITE_WITH_IMM consumes an RQ credit without putting data in its SGEs.

use std::time::{Duration, Instant};

use safeverbs_core::model::Model;
use safeverbs_core::nic::{NicCq, NicQp, QpType, Sge};
use safeverbs_core::{
    Access, ConnectAttr, Context, Endpoint, Nic, Opcode, QpCaps, RemoteBuf, Wc, WrOp,
};

/// Uses a genuine zero-SGE receive; the public buffer API posts one SGE.
fn zero_sge_notification<N: Nic>(ctx: Context<N>) {
    const LEN: usize = 64 * 1024;
    let mut src = vec![0xa5u8; LEN];
    let mut dst = vec![0x3cu8; LEN];
    let nic = ctx.nic();
    let pd = nic.alloc_pd().unwrap();
    // SAFETY: these allocations stay registered until both completions
    // have been polled. Rust does not touch dst while the NIC may write it.
    let src_mr = unsafe { nic.reg_mr(&pd, src.as_mut_ptr(), LEN, Access::LOCAL_WRITE) }.unwrap();
    // SAFETY: as above; the only remote writer is the request posted below.
    let dst_mr = unsafe {
        nic.reg_mr(
            &pd,
            dst.as_mut_ptr(),
            LEN,
            Access::LOCAL_WRITE | Access::REMOTE_WRITE,
        )
    }
    .unwrap();
    let mut cq = nic.create_cq(4, true).unwrap();
    let caps = QpCaps {
        max_send_wr: 1,
        max_recv_wr: 1,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
    };
    let mut a = nic
        .create_qp(&pd, None, &cq, &cq, QpType::Rc, &caps)
        .unwrap();
    let mut b = nic
        .create_qp(&pd, None, &cq, &cq, QpType::Rc, &caps)
        .unwrap();
    let port = ctx.port(1).unwrap();
    let gid_index = if port.ethernet {
        std::env::var("SAFEVERBS_TEST_GID_INDEX")
            .ok()
            .map(|i| i.parse().expect("SAFEVERBS_TEST_GID_INDEX"))
            .or(ctx.default_gid_index(1).unwrap())
    } else {
        None
    };
    let gid = gid_index.map(|i| ctx.gids(1).into_iter().find(|g| g.index == i).unwrap().gid);
    let attr = ConnectAttr {
        port: 1,
        access: Access::LOCAL_WRITE | Access::REMOTE_WRITE,
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
    a.connect(
        &attr,
        &Endpoint {
            qp_num: b.qp_num(),
            lid: port.lid,
            gid,
        },
    )
    .unwrap();
    b.connect(
        &attr,
        &Endpoint {
            qp_num: a.qp_num(),
            lid: port.lid,
            gid,
        },
    )
    .unwrap();
    let remote = RemoteBuf {
        addr: dst.as_mut_ptr() as u64,
        len: LEN as u64,
        rkey: N::mr_keys(&dst_mr).1,
    };
    let sge = Sge {
        addr: src.as_ptr() as u64,
        length: LEN as u32,
        lkey: N::mr_keys(&src_mr).0,
    };
    // SAFETY: both queues are empty and ready; no receive buffer is named.
    // The registered source and remote destination stay alive and untouched
    // until both WQEs retire, with a signaled send and enough CQ space.
    unsafe {
        b.post_recv(2, &[]).unwrap();
        a.start();
        a.post(1, true, &WrOp::WriteImm(remote, 0xfedc_ba98), &[sge]);
        a.complete().unwrap();
    }
    let mut completions = Vec::new();
    let start = Instant::now();
    while completions.len() < 2 {
        let mut out = [Wc::success(Opcode::SEND, 0); 2];
        let n = cq.poll(&mut out);
        completions.extend_from_slice(&out[..n]);
        if start.elapsed() >= Duration::from_secs(10) && completions.len() < 2 {
            // Retain every dependency if the device did not quiesce, as
            // the production worker does, rather than freeing live DMA memory.
            core::mem::forget((a, b, cq, src_mr, dst_mr, pd, src, dst, ctx));
            panic!("timed out waiting for the zero-SGE WRITE_WITH_IMM");
        }
    }
    let sent = completions.iter().find(|wc| wc.wr_id == 1).unwrap();
    let recv = completions.iter().find(|wc| wc.wr_id == 2).unwrap();
    assert!(sent.is_ok(), "{sent:?}");
    assert!(recv.is_ok(), "{recv:?}");
    assert_eq!(sent.opcode(), Some(Opcode::RDMA_WRITE));
    assert_eq!(sent.byte_len(), None);
    assert_eq!(sent.imm(), None);
    assert_eq!(recv.opcode(), Some(Opcode::RECV_RDMA_WITH_IMM));
    assert_eq!(recv.byte_len(), Some(LEN as u32));
    assert_eq!(recv.received_len(), None);
    assert_eq!(recv.imm(), Some(0xfedc_ba98));
    assert_eq!(src, dst);
}

#[test]
fn zero_sge_write_notification_model() {
    zero_sge_notification(Context::new(Model::new(42)));
}

#[test]
fn zero_sge_write_notification_rxe() {
    let names = safeverbs_core::verbs::devices().unwrap();
    let name = std::env::var("SAFEVERBS_TEST_DEVICE")
        .ok()
        .or_else(|| names.into_iter().find(|n| n.contains("rxe")));
    let Some(name) = name else {
        eprintln!("no rxe device: skipped zero-SGE notification test");
        return;
    };
    zero_sge_notification(Context::open(Some(&name)).unwrap());
}
