//! This code snippet shows that safeverbs provides full flexiblity in the process of modifying QP
//! while disallowing user passing in wrong arguments at wrong phase.
use safeverbs::{Context, QueuePair, ProtectionDomain, QueuePairSetting};
use std::net::TcpStream;

pub const fn qp_capability() -> QpCapability {
    QpCapability(safeverbs::ffi::ibv_qp_cap {
        max_send_wr: 128,
        max_recv_wr: 128,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 32,
    })
}

pub fn exchange_endpoint(
    stream: &mut TcpStream,
    local: QueuePairEndpoint,
) -> Result<QueuePairEndpoint, Box<dyn std::error::Error>> {
    let buf = bincode::serialize(&local)?;
    stream.write_all(&(buf.len() as u32).to_be_bytes())?;
    stream.write_all(&buf)?;

    let mut len_buf = [0u8; 4];
    stream.read(len_buf.as_mut())?;
    let len = u32::from_be_bytes(len_buf);
    let mut remote_buf = vec![0u8; len as usize];
    stream.read_exact(&mut remote_buf)?;
    let remote = bincode::deserialize(&remote_buf)?;
    Ok(remote)
}

fn qp_setting(pd: &ProtectionDomain) -> QueuePairSetting {
    QueuePairSetting {
        access: ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE,
        path_mtu: pd.context().port_attr().unwrap().active_mtu,
        rq_psn: 0,
        sq_psn: 0,
        min_rnr_timer: 12,
        retry_count: 7,
        rnr_retry: 7,
        timeout: 14,
        max_rd_atomic: 1,
        max_dest_rd_atomic: 1,
        traffic_class: 0,
    },
}

pub fn demo_typestate_pattern() -> Box<QueuePair<RC, RTS>, Box<dyn std::error::Error>> {
    // List all RDMA devices and find the proper device.
    let devlist = safeverbs::devices()?;
    let device = devlist
        .iter()
        .find(|dev| dev.name().unwrap().to_bytes() == b"mlx5_0")
        .unwrap_or_else(|| panic!("{mlx5_0} not found"));
    let context = Context::with_device(&device)?;
    let pd = context.alloc_pd()?;
    let cq = context.create_cq(128, 0)?;
    let qp_init_attr = QpInitAttr {
        qp_context: 0,
        send_cq: cq.clone(),
        recv_cq: cq.clone(),
        cap: qp_capability(),
        qp_type: RC,
        sq_sig_all: false,
    };

    let mut stream = TcpStream::connect(("127.0.0.1", config::SERVER_PORT))?;

    // Create QP
    let access = safeverbs::ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE;
    let qp: QueuePair<RC, RESET> = pd.create_qp(&qp_init_attr)?;

    let endpoint = qp.endpoint();
    let remote: QueuePairEndpoint = exchange_endpoint(&mut stream, endpoint);

    let gid_index = 0;
    let setting = qp_setting(Some(gid_index));
    let init_qp = self.qp.modify_to_init(0, 1, setting.access)?;
    let mut ah_attr = ffi::ibv_ah_attr {
        dlid: remote.lid,
        sl: 0,
        src_path_bits: 0,
        port_num: 1,
        grh: Default::default(),
        ..Default::default()
    };
    if let Some(gid) = remote.gid {
        ah_attr.is_global = 1;
        ah_attr.grh = ffi::ibv_global_route {
            dgid: gid.into(),
            flow_label: 0,
            sgid_index: gid_index,
            hop_limit: 0xff,
            traffic_class: setting.traffic_class,
        };
    }
    let rtr_qp = init_qp.modify_to_rtr(
        ah_attr,
        setting.path_mtu,
        remote.num,
        setting.rq_psn,
        setting.max_dest_rd_atomic.expect("RC"),
        setting.min_rnr_timer.expect("RC"),
        None, // pkey_index already set
        None, // access_flags already set
        None,
    )?;
    let rts_qp = rtr_qp.modify_to_rts(
        setting.sq_psn,
        setting.timeout.expect("RC"),
        setting.retry_count.expect("RC"),
        setting.rnr_retry.expect("RC"),
        setting.max_rd_atomic.expect("RC"),
        None, // cur_qp_state
        None, // access_flags already set
        None, // min_rnr_timer already set
        None,
        None,
    )?;

    // Here we got rts_qp
    // All the resources created before were reference-counted and are safe to drop.
    // In C, if user forgets to destory resources or destorys while the it is being referenced,
    // it may cause unexpected behavior.
    Ok(rts_qp)
}