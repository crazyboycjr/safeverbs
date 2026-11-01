//! The greeter's server: accepts a client, posts a receive on a UC QP
//! connected to the client's, and prints the message that arrives.

use std::net::TcpListener;
use std::time::Instant;

use futures::executor::block_on;
use safeverbs::UcQueuePairBuilder;

mod config;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = config::open_device()?;
    let pd = ctx.alloc_pd()?;
    let listener = TcpListener::bind(("0.0.0.0", config::SERVER_PORT))?;
    let (mut stream, _) = listener.accept()?;

    let prepared = UcQueuePairBuilder::new(&pd).build()?;
    println!("Server endpoint: {:?}", prepared.endpoint());
    let remote = config::exchange_endpoint(&mut stream, prepared.endpoint())?;
    println!("Client endpoint: {remote:?}");
    let qp = prepared.handshake(remote)?;
    println!("QP handshake finished.");

    // UC drops a message that finds no receive: post it before the client
    // sends.
    let recv = qp.recv(pd.alloc::<u8>(1024)?);
    config::sync(&mut stream)?;

    let start = Instant::now();
    let (wc, buf) = block_on(recv)?;
    println!("{wc:?}");
    println!("{}", std::str::from_utf8(&buf[..wc.byte_len as usize])?);
    println!("Duration: {:?}", start.elapsed());
    Ok(())
}
