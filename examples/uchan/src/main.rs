//! An unreliable channel: the sender streams RDMA WRITEs on a UC QP into
//! the receiver's buffer and, every `--imm-every` WRITEs, one with its
//! sequence number as immediate data; the receiver prints the sequence
//! numbers that arrive. UC does not retransmit, so on a lossy network some
//! numbers never show up, and the sender never learns about it.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};

use clap::Parser;
use futures::executor::block_on;
use futures::{SinkExt, StreamExt};
use safeverbs::{Context, Endpoint, Received, RemoteBuf, UcQueuePairBuilder, WriteOp};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Arguments.
#[derive(Parser, Debug)]
#[command(about = "An unreliable channel: UC WRITEs with sequence numbers.")]
struct Args {
    /// The address to connect to. Without it the binary runs as the
    /// receiver and listens on 0.0.0.0.
    #[arg(short, long)]
    connect: Option<String>,

    /// The TCP port to use.
    #[arg(short, long, default_value = "5000")]
    port: u16,

    /// The RDMA device to use (default: the first).
    #[arg(short, long)]
    device: Option<String>,

    /// The GID index; default: the first RoCE v2 GID with an IPv4 address.
    #[arg(short = 'x', long)]
    gid_index: Option<u8>,

    /// Send queue depth (max_send_wr).
    #[arg(short, long, default_value = "128")]
    tx_depth: u32,

    /// Number of measured iterations.
    #[arg(short, long, default_value = "5000")]
    num_iters: usize,

    /// Number of warmup iterations.
    #[arg(short, long, default_value = "100")]
    warmup: usize,

    /// Message size in bytes.
    #[arg(short, long, default_value = "65536")]
    size: usize,

    /// Send a sequence number with every this many WRITEs.
    #[arg(long, default_value = "1000")]
    imm_every: usize,
}

/// Sends `value` (length-prefixed bincode).
fn send<T: Serialize>(stream: &mut TcpStream, value: &T) -> anyhow::Result<()> {
    let buf = bincode::serialize(value)?;
    stream.write_all(&(buf.len() as u32).to_be_bytes())?;
    stream.write_all(&buf)?;
    Ok(())
}

/// Receives a value (length-prefixed bincode).
fn recv<T: DeserializeOwned>(stream: &mut TcpStream) -> anyhow::Result<T> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
    stream.read_exact(&mut buf)?;
    Ok(bincode::deserialize(&buf)?)
}

/// Opens the device and connects a UC QP through `stream`.
fn connect(
    args: &Args,
    stream: &mut TcpStream,
) -> anyhow::Result<(safeverbs::Pd, safeverbs::LocalQp<safeverbs::UC>)> {
    let ctx = Context::open(args.device.as_deref())?;
    let pd = ctx.alloc_pd()?;
    let mut b = UcQueuePairBuilder::new(&pd);
    b.set_send_depth(args.tx_depth);
    if let Some(i) = args.gid_index {
        b.set_gid_index(i);
    }
    let prepared = b.build()?;
    println!("Local endpoint: {:?}", prepared.endpoint());
    send(stream, &prepared.endpoint())?;
    let remote: Endpoint = recv(stream)?;
    println!("Remote endpoint: {remote:?}");
    let qp = prepared.handshake(remote)?;
    println!("QP handshake finished.");
    Ok((pd, qp))
}

fn run_sender(args: &Args, host: &str) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect((host, args.port))?;
    let (pd, qp) = connect(args, &mut stream)?;
    let dst: RemoteBuf = recv(&mut stream)?;

    let mut buf = pd.alloc::<u8>(args.size)?;
    buf.fill(42);
    let buf = buf.freeze();
    let total = args.warmup + args.num_iters;
    let mut tx = qp.sink();
    let mut start = Instant::now();
    block_on(async {
        for seq in 0..total {
            if seq == args.warmup {
                start = Instant::now();
            }
            let op = WriteOp::new(buf.clone(), dst);
            let op = if (seq + 1) % args.imm_every == 0 {
                op.with_imm(seq as u32)
            } else {
                op
            };
            tx.feed(op).await?;
        }
        tx.close().await?;
        Ok::<(), safeverbs::Error>(())
    })?;
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "{:.2} Mops/s, {:.2} Gb/s",
        args.num_iters as f64 / elapsed / 1e6,
        8e-9 * (args.num_iters * args.size) as f64 / elapsed
    );
    // Closing the connection tells the receiver to stop.
    Ok(())
}

fn run_receiver(args: &Args) -> anyhow::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", args.port))?;
    let (mut stream, _) = listener.accept()?;
    let (pd, qp) = connect(args, &mut stream)?;

    // Where the WRITEs land, exposed to the sender alone.
    let dst = pd.alloc::<u8>(args.size)?.expose()?;
    // Each WRITE with immediate data consumes a receive (its buffer stays
    // empty): post them before the sender starts.
    let bufs = (0..128)
        .map(|_| pd.alloc::<u8>(64))
        .collect::<Result<Vec<_>, _>>()?;
    let mut rx = qp.receiver(bufs);
    send(&mut stream, &dst.remote())?;

    // Stop when the sender closes the connection.
    let done = Arc::new(AtomicBool::new(false));
    let flag = done.clone();
    let watcher = std::thread::spawn(move || {
        let _ = stream.read(&mut [0u8; 1]);
        flag.store(true, Ordering::Release);
    });

    let mut received = 0usize;
    block_on(async {
        loop {
            match futures::poll!(rx.next()) {
                Poll::Ready(Some((got, buf))) => {
                    match got.map_err(safeverbs::Error::from)? {
                        Received::WriteNotification { imm, .. } => {
                            println!("{imm}");
                            received += 1;
                        }
                        Received::Message { .. } => anyhow::bail!("a message on a WRITE channel"),
                    }
                    rx.repost(buf)?;
                }
                Poll::Ready(None) => break,
                Poll::Pending if done.load(Ordering::Acquire) => break,
                Poll::Pending => std::thread::sleep(Duration::from_micros(10)),
            }
        }
        Ok(())
    })?;
    println!("{received} sequence numbers received");
    watcher.join().expect("the watcher thread");
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match &args.connect {
        Some(host) => run_sender(&args, host),
        None => run_receiver(&args),
    }
}
