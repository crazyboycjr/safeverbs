//! WRITE_WITH_IMM consumes an RQ credit without putting data in its SGEs: the receive
//! reports a notification with the WRITE's length, whatever its own buffer holds.

use std::time::{Duration, Instant};

use safeverbs_core::model::Model;
use safeverbs_core::nic::{NicCq, NicQp, QpType, Sge};
use safeverbs_core::{
    Access, Completed, ConnectAttr, Context, Endpoint, Engine, Nic, Pd, QpCaps, QpKey, RC,
    Received, RemoteBuf, Wc, WrOp,
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
        let mut out = [Wc::default(); 2];
        let n = cq.poll(&mut out);
        completions.extend_from_slice(&out[..n]);
        if start.elapsed() >= Duration::from_secs(10) && completions.len() < 2 {
            // Retain every dependency if the device did not quiesce, as
            // the production engine does, rather than freeing live DMA memory.
            core::mem::forget((a, b, cq, src_mr, dst_mr, pd, src, dst, ctx));
            panic!("timed out waiting for the zero-SGE WRITE_WITH_IMM");
        }
    }
    // Which is which is in the typed results alone: their ids stay inside the core.
    let results: Vec<_> = completions.iter().map(Wc::result).collect();
    assert!(results.contains(&Ok(Completed::Sent)), "{results:?}");
    assert!(
        results.contains(&Ok(Completed::Received(Received::WriteNotification {
            imm: 0xfedc_ba98,
            len: LEN as u32,
        }))),
        "{results:?}"
    );
    assert_eq!(src, dst);
}

/// The attributes that connect a QP of `ctx`.
fn attr<N: Nic>(ctx: &Context<N>) -> ConnectAttr {
    let port = ctx.port(1).unwrap();
    ConnectAttr {
        port: 1,
        access: Access::LOCAL_WRITE | Access::REMOTE_WRITE,
        path_mtu: port.active_mtu,
        rq_psn: 0,
        sq_psn: 0,
        gid_index: if port.ethernet {
            std::env::var("SAFEVERBS_TEST_GID_INDEX")
                .ok()
                .map(|i| i.parse().expect("SAFEVERBS_TEST_GID_INDEX"))
                .or(ctx.default_gid_index(1).unwrap())
        } else {
            None
        },
        traffic_class: 0,
        timeout: 14,
        retry_count: 7,
        rnr_retry: 7,
        min_rnr_timer: 12,
        max_rd_atomic: 1,
        max_dest_rd_atomic: 1,
    }
}

/// An engine on `ctx` with two RC QPs connected to each other.
fn pair<N: Nic>(ctx: &Context<N>) -> (Pd<N>, Engine<u32, N>, QpKey<RC>, QpKey<RC>) {
    let pd = ctx.alloc_pd().unwrap();
    let caps = QpCaps {
        max_send_wr: 8,
        max_recv_wr: 8,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
    };
    let mut engine = Engine::new(ctx, 32).unwrap();
    let a = engine.create_qp::<RC>(&pd, caps, 1).unwrap();
    let b = engine.create_qp::<RC>(&pd, caps, 1).unwrap();
    let attr = attr(ctx);
    let (ea, eb) = (
        engine.endpoint(a, 1, attr.gid_index).unwrap(),
        engine.endpoint(b, 1, attr.gid_index).unwrap(),
    );
    engine.connect(a, &attr, &eb).unwrap();
    engine.connect(b, &attr, &ea).unwrap();
    (pd, engine, a, b)
}

/// Every kind of receive, through the engine: a message, one with immediate data, an empty
/// one (a message of no bytes, still), and notifications, into a buffer too small for the
/// WRITE and into a buffer of no bytes, which is all a notification needs.
fn every_kind_of_receive<N: Nic>(ctx: Context<N>) {
    let (pd, mut engine, a, b) = pair(&ctx);
    let mut small = pd.alloc::<u8>(8).unwrap();
    small.fill(0x5a);
    let (no_bytes, _) = pd.alloc::<u8>(8).unwrap().split_at(0);
    let dst = pd.alloc::<u8>(4096).unwrap().expose().unwrap();
    let mut src = pd.alloc::<u8>(4096).unwrap();
    src.fill(0xa5);
    let src = src.freeze();
    engine.recv(b, pd.alloc::<u8>(64).unwrap(), 0).unwrap();
    engine.recv(b, pd.alloc::<u8>(64).unwrap(), 1).unwrap();
    engine.recv(b, pd.alloc::<u8>(64).unwrap(), 2).unwrap();
    engine.recv(b, small, 3).unwrap();
    engine.recv(b, no_bytes, 4).unwrap();
    engine.send(a, src.slice(..16), 10).unwrap();
    engine.send_imm(a, src.slice(..32), 77, 11).unwrap();
    engine.send(a, src.slice(..0), 12).unwrap();
    engine
        .write_imm(a, src.slice(..), dst.remote(), 88, 13)
        .unwrap();
    engine
        .write_imm(a, src.slice(..100), dst.remote(), 99, 14)
        .unwrap();
    let mut got = Vec::new();
    let start = Instant::now();
    while got.len() < 10 {
        engine.poll(32, |_, c| got.push((c.wr_id, c.result, c.leases)));
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "timed out with {} completions",
            got.len()
        );
    }
    got.sort_by_key(|g| g.0);
    let message = |len, imm| Ok(Completed::Received(Received::Message { len, imm }));
    let notification = |imm, len| {
        Ok(Completed::Received(Received::WriteNotification {
            imm,
            len,
        }))
    };
    let results: Vec<_> = got.iter().map(|g| (g.0, g.1)).collect();
    assert_eq!(
        results,
        [
            (0, message(16, None)),
            (1, message(32, Some(77))),
            (2, message(0, None)),
            (3, notification(88, 4096)),
            (4, notification(99, 100)),
            (10, Ok(Completed::Sent)),
            (11, Ok(Completed::Sent)),
            (12, Ok(Completed::Sent)),
            (13, Ok(Completed::Sent)),
            (14, Ok(Completed::Sent)),
        ]
    );
    let mut got = got.into_iter();
    let buf = got.next().unwrap().2.into_buf_mut::<u8>().unwrap();
    assert_eq!(&buf[..16], &src[..16]);
    let buf = got.nth(2).unwrap().2.into_buf_mut::<u8>().unwrap();
    assert_eq!(
        &buf[..],
        &[0x5a; 8],
        "a notification leaves its buffer alone"
    );
    assert_eq!(got.next().unwrap().2.into_buf_mut::<u8>().unwrap().len(), 0);
    drop(engine);
    assert_eq!(&dst.reclaim()[..], &src[..]);
}

#[test]
fn every_kind_of_receive_model() {
    every_kind_of_receive(Context::new(Model::new(43)));
}

#[test]
fn every_kind_of_receive_rxe() {
    let names = safeverbs_core::verbs::devices().unwrap();
    let name = std::env::var("SAFEVERBS_TEST_DEVICE")
        .ok()
        .or_else(|| names.into_iter().find(|n| n.contains("rxe")));
    let Some(name) = name else {
        eprintln!("no rxe device: skipped");
        return;
    };
    every_kind_of_receive(Context::open(Some(&name)).unwrap());
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
