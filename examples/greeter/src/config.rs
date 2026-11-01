//! What the client and the server share: the device, the port and how they
//! exchange endpoints over TCP.

use std::io::{Read, Write};
use std::net::TcpStream;

use safeverbs::{Context, Endpoint};

/// The TCP port the server listens on.
pub const SERVER_PORT: u16 = 5000;

/// Opens the RDMA device named by `SAFEVERBS_DEVICE`, else the first one.
pub fn open_device() -> safeverbs::Result<Context> {
    let name = std::env::var("SAFEVERBS_DEVICE").ok();
    Context::open(name.as_deref())
}

/// Sends `local` and receives the peer's endpoint (length-prefixed bincode).
pub fn exchange_endpoint(
    stream: &mut TcpStream,
    local: Endpoint,
) -> Result<Endpoint, Box<dyn std::error::Error>> {
    let buf = bincode::serialize(&local)?;
    stream.write_all(&(buf.len() as u32).to_be_bytes())?;
    stream.write_all(&buf)?;
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let mut remote = vec![0u8; u32::from_be_bytes(len) as usize];
    stream.read_exact(&mut remote)?;
    Ok(bincode::deserialize(&remote)?)
}

/// Waits until both sides reach this point.
pub fn sync(stream: &mut TcpStream) -> std::io::Result<()> {
    let mut byte = [1u8];
    stream.write_all(&byte)?;
    stream.read_exact(&mut byte)
}
