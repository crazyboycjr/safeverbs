//! This code snippet shows that safeverbs is easy to use with simplified API.
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

    // Create QP Builder
    let prepared_qp =
        QueuePairBuilder::new(pd.clone(), qp_init_attr)
            .set_min_rnr_timer(12)
            .set_timeout(14)
            .set_retry_count(7)
            .set_rnr_retry(7)
            .set_max_rd_atomic(1)
            .build()?;
    let endpoint = prepared_qp.endpoint();
    println!("Client endpoint: {:?}", endpoint);

    let remote = exchange_endpoint(&mut stream, endpoint)?;
    println!("Server endpoint: {:?}", remote);

    let qp = prepared_qp.handshake(remote)?;
    println!("QP handshake finished.");

    // Here we got an RC QP that is fully established.
    // All the resources created before were reference-counted and are safe to drop.
    // In C, if user forgets to destory resources or destorys while the it is being referenced,
    // it may cause unexpected behavior.
    Ok(qp)
}