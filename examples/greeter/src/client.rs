//! The greeter's client: connects a UC QP to the server's and SENDs it a
//! message.

use std::net::TcpStream;

use futures::executor::block_on;
use safeverbs::UcQueuePairBuilder;

mod config;

/// The message.
const MSG: &str = "Hello, SafeVerbs";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = config::open_device()?;
    let pd = ctx.alloc_pd()?;
    let mut stream = TcpStream::connect(("127.0.0.1", config::SERVER_PORT))?;

    let prepared = UcQueuePairBuilder::new(&pd).build()?;
    println!("Client endpoint: {:?}", prepared.endpoint());
    let remote = config::exchange_endpoint(&mut stream, prepared.endpoint())?;
    println!("Server endpoint: {remote:?}");
    let qp = prepared.handshake(remote)?;
    println!("QP handshake finished.");

    // The server has posted its receive.
    config::sync(&mut stream)?;

    let mut buf = pd.alloc::<u8>(MSG.len())?;
    buf.copy_from_slice(MSG.as_bytes());
    block_on(qp.send(buf.freeze()))?;
    println!("Sent.");
    Ok(())
}
