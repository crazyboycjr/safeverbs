//! RDMA WRITE bandwidth, like perftest's `ib_write_bw`: the client WRITEs
//! one frozen buffer into the server's exposed buffer as fast as the send
//! queue allows, through a sink that asks for a completion only as often
//! as the queue needs; the server only exposes the buffer and waits.

use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use futures::SinkExt;
use futures::executor::block_on;
use perftest::cli::get_args;
use perftest::{builder, exchange, open_device, recv, send};
use safeverbs::{RC, RemoteBuf, Transport, UC, WriteOp};

fn run_sender<T: Transport>(host: &str) -> anyhow::Result<()> {
    let args = get_args();
    let ctx = open_device()?;
    let pd = ctx.alloc_pd()?;
    let prepared = builder::<T, _>(&pd).build()?;
    println!("Client endpoint: {:?}", prepared.endpoint());
    let mut stream = TcpStream::connect((host, args.port))?;
    let remote = exchange(&mut stream, &prepared.endpoint())?;
    println!("Server endpoint: {remote:?}");
    let qp = prepared.handshake(remote)?;
    println!("QP handshake finished.");
    let dst: RemoteBuf = recv(&mut stream)?;

    let mut buf = pd.alloc::<u8>(args.size)?;
    buf.fill(42);
    let buf = buf.freeze();

    let total = args.warmup + args.num_iters;
    let mut tx = qp.sink();
    let (mut start, mut last) = (Instant::now(), Instant::now());
    let (mut sent, mut last_sent) = (0usize, 0usize);
    block_on(async {
        while sent < total {
            tx.feed(WriteOp::new(buf.clone(), dst)).await?;
            sent += 1;
            if sent == args.warmup {
                start = Instant::now();
            }
            let elapsed = last.elapsed();
            if elapsed > Duration::from_secs(1) && sent > args.warmup {
                let n = sent - last_sent;
                println!(
                    "{:.2} Mops/s, {:.2} Gb/s",
                    n as f64 / elapsed.as_secs_f64() / 1e6,
                    8e-9 * (n * args.size) as f64 / elapsed.as_secs_f64()
                );
                last = Instant::now();
                last_sent = sent;
            }
        }
        // Wait until every WRITE has completed.
        tx.close().await?;
        Ok::<(), safeverbs::Error>(())
    })?;
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "{} WRITEs of {} bytes: {:.2} Mops/s, {:.2} Gb/s",
        args.num_iters,
        args.size,
        args.num_iters as f64 / elapsed / 1e6,
        8e-9 * (args.num_iters * args.size) as f64 / elapsed
    );
    // Tell the server it may go.
    send(&mut stream, &0u8)?;
    Ok(())
}

fn run_receiver<T: Transport>() -> anyhow::Result<()> {
    let args = get_args();
    let ctx = open_device()?;
    let pd = ctx.alloc_pd()?;
    let listener = TcpListener::bind(("0.0.0.0", args.port))?;
    let (mut stream, _) = listener.accept()?;
    let prepared = builder::<T, _>(&pd).build()?;
    println!("Server endpoint: {:?}", prepared.endpoint());
    let remote = exchange(&mut stream, &prepared.endpoint())?;
    println!("Client endpoint: {remote:?}");
    let _qp = prepared.handshake(remote)?;
    println!("QP handshake finished.");

    // The buffer the client writes into, exposed to it alone.
    let dst = pd.alloc::<u8>(args.size)?.expose()?;
    send(&mut stream, &dst.remote())?;
    let _done: u8 = recv(&mut stream)?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = get_args();
    match (args.host.as_deref(), args.connection.as_str()) {
        (Some(host), "RC") => run_sender::<RC>(host),
        (Some(host), "UC") => run_sender::<UC>(host),
        (None, "RC") => run_receiver::<RC>(),
        (None, "UC") => run_receiver::<UC>(),
        (_, other) => anyhow::bail!("unknown connection type: {other}"),
    }
}
