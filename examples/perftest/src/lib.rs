pub mod cli;
use cli::get_args;

use safeverbs::{Context, QpCapability, QueuePairEndpoint};
use std::net::TcpStream;
use std::io::{Read, Write};

pub fn find_and_open_device() -> anyhow::Result<Context> {
    let args = get_args();
    let devlist = safeverbs::devices()?;
    let device = devlist
        .iter()
        .find(|dev| dev.name().unwrap().to_bytes() == args.device.as_bytes())
        .unwrap_or_else(|| panic!("{} not found", args.device));
    Ok(Context::with_device(&device)?)
}

pub fn qp_capability() -> QpCapability {
    QpCapability(safeverbs::ffi::ibv_qp_cap {
        max_send_wr: get_args().tx_depth,
        max_recv_wr: 128,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 32,
    })
}

pub fn exchange_endpoint(
    stream: &mut TcpStream,
    local: QueuePairEndpoint,
) -> anyhow::Result<QueuePairEndpoint> {
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