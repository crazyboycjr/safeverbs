use std::net::TcpStream;
use std::io::{Write, Read};

use safeverbs::{Context, QpCapability, QueuePairEndpoint};

pub const SERVER_PORT: u16 = 5000;
pub const IB_NAME: &'static str = "mlx5_0";

pub fn find_and_open_device() -> Result<Context, Box<dyn std::error::Error>> {
    let devlist = safeverbs::devices()?;
    let device = devlist
        .iter()
        .find(|dev| dev.name().unwrap().to_bytes() == IB_NAME.as_bytes())
        .unwrap_or_else(|| panic!("{IB_NAME} not found"));
    Ok(Context::with_device(&device)?)
}

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
