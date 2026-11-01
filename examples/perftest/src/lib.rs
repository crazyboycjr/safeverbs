//! What the perftest binaries share: arguments, the device and the TCP
//! exchanges.

pub mod cli;

use std::io::{Read, Write};
use std::net::TcpStream;

use cli::get_args;
use safeverbs::{Context, Nic, QueuePairBuilder, Transport};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Opens the device the arguments name, or the first.
pub fn open_device() -> anyhow::Result<Context> {
    Ok(Context::open(get_args().device.as_deref())?)
}

/// A QP builder configured from the arguments.
pub fn builder<T: Transport, N: Nic>(pd: &safeverbs::Pd<N>) -> QueuePairBuilder<T, N> {
    let args = get_args();
    let mut b = QueuePairBuilder::new(pd);
    b.set_send_depth(args.tx_depth)
        .set_traffic_class(args.traffic_class);
    if let Some(i) = args.gid_index {
        b.set_gid_index(i);
    }
    b
}

/// Sends `value` and receives the peer's (length-prefixed bincode).
pub fn exchange<T: Serialize + DeserializeOwned>(
    stream: &mut TcpStream,
    value: &T,
) -> anyhow::Result<T> {
    send(stream, value)?;
    recv(stream)
}

/// Sends `value` (length-prefixed bincode).
pub fn send<T: Serialize>(stream: &mut TcpStream, value: &T) -> anyhow::Result<()> {
    let buf = bincode::serialize(value)?;
    stream.write_all(&(buf.len() as u32).to_be_bytes())?;
    stream.write_all(&buf)?;
    Ok(())
}

/// Receives a value (length-prefixed bincode).
pub fn recv<T: DeserializeOwned>(stream: &mut TcpStream) -> anyhow::Result<T> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
    stream.read_exact(&mut buf)?;
    Ok(bincode::deserialize(&buf)?)
}
