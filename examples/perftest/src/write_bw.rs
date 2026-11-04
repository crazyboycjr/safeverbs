//! RDMA WRITE bandwidth, like perftest's `ib_write_bw`: the client WRITEs
//! one frozen buffer into the server's exposed buffer as fast as the send
//! queues allow, on `--qps` QPs of one CQ, asking for a completion
//! every `--cq-mod` WRITEs of a QP; the server only exposes the buffer and
//! waits.
//!
//! By default the client runs an event loop on a [`LocalCq`]: poll the CQ, count
//! each completion, refill every QP's send queue and ring its doorbell once.
//! With `--async` it drives a sink of WRITEs per QP instead, all on one
//! [`AsyncCq`], whose futures share the CQ.

use std::net::{TcpListener, TcpStream};
use std::time::Instant;

use futures::SinkExt;
use futures::executor::block_on;
use perftest::cli::get_args;
use perftest::{builder, exchange, open_device, recv, send};
use safeverbs::{Buf, Context, Endpoint, LocalCq, Pd, RC, RemoteBuf, Transport, UC, WrOp, WriteOp};

/// The CQ's entries: room for every QP's queues.
fn cq_size(qps: usize, tx_depth: u32) -> u32 {
    qps as u32 * (tx_depth + 1)
}

/// How many WRITEs QP `i` of `n` does, of `total`.
fn share(total: usize, n: usize, i: usize) -> usize {
    total / n + usize::from(i < total % n)
}

/// The event loop: refill, ring, poll, count.
fn run_loop<T: Transport>(
    ctx: &Context,
    pd: &Pd,
    stream: &mut TcpStream,
    buf: &Buf<u8>,
    dst: &mut Option<RemoteBuf>,
) -> anyhow::Result<f64> {
    let args = get_args();
    let cq: LocalCq = ctx.create_local_cq(cq_size(args.qps, args.tx_depth))?;
    let prepared = (0..args.qps)
        .map(|_| builder::<T, _>(pd).build_on(&cq))
        .collect::<Result<Vec<_>, _>>()?;
    println!("CQ on a thread domain: {}", cq.is_lock_free());
    let mine: Vec<Endpoint> = prepared.iter().map(|p| p.endpoint()).collect();
    let theirs: Vec<Endpoint> = exchange(stream, &mine)?;
    let qps = prepared
        .into_iter()
        .zip(theirs)
        .map(|(p, e)| p.handshake(e))
        .collect::<Result<Vec<_>, _>>()?;
    let dst = dst.insert(recv(stream)?);
    println!("QP handshakes finished.");

    let total = args.warmup + args.num_iters;
    let cq_mod = args.cq_mod.max(1);
    let mut posted = vec![0usize; qps.len()];
    let mut completed = 0usize;
    let mut start = Instant::now();
    while completed < total {
        // Dispatch: every WRITE that completed comes back with its buffer.
        cq.poll(32, |wc| {
            assert!(wc.result.is_ok(), "a WRITE failed: {:?}", wc.result);
            completed += 1;
            if completed == args.warmup {
                start = Instant::now();
            }
        });
        // Post: fill every send queue, one doorbell per QP.
        for (i, qp) in qps.iter().enumerate() {
            let mine = share(total, qps.len(), i);
            let mut n = 0;
            while posted[i] < mine && qp.can_post() {
                posted[i] += 1;
                let signal = posted[i] % cq_mod == 0 || posted[i] == mine;
                qp.stage(WrOp::Write(*dst), buf.slice(..).into_part(), 0, signal)
                    .map_err(|e| e.error)?;
                n += 1;
            }
            if n > 0 {
                qp.ring()?;
            }
        }
    }
    Ok(start.elapsed().as_secs_f64())
}

/// Futures: a sink of WRITEs per QP, all on one CQ.
fn run_async<T: Transport>(
    ctx: &Context,
    pd: &Pd,
    stream: &mut TcpStream,
    buf: &Buf<u8>,
    dst: &mut Option<RemoteBuf>,
) -> anyhow::Result<f64> {
    let args = get_args();
    let cq = ctx.create_async_cq(cq_size(args.qps, args.tx_depth))?;
    let prepared = (0..args.qps)
        .map(|_| builder::<T, _>(pd).build_on(&cq))
        .collect::<Result<Vec<_>, _>>()?;
    println!("CQ on a thread domain: {}", cq.is_lock_free());
    let mine: Vec<Endpoint> = prepared.iter().map(|p| p.endpoint()).collect();
    let theirs: Vec<Endpoint> = exchange(stream, &mine)?;
    let qps = prepared
        .into_iter()
        .zip(theirs)
        .map(|(p, e)| p.handshake(e))
        .collect::<Result<Vec<_>, _>>()?;
    let dst = *dst.insert(recv(stream)?);
    println!("QP handshakes finished.");

    let total = args.warmup + args.num_iters;
    // The warmup on the first QP, then every QP at once.
    block_on(async {
        let mut tx = qps[0].sink();
        for _ in 0..args.warmup {
            tx.feed(WriteOp::new(buf.clone(), dst)).await?;
        }
        tx.close().await
    })?;
    let start = Instant::now();
    let n = qps.len();
    let sends = qps.iter().enumerate().map(|(i, qp)| async move {
        let mut tx = qp.sink();
        for _ in 0..share(total - args.warmup, n, i) {
            tx.feed(WriteOp::new(buf.clone(), dst)).await?;
        }
        tx.close().await
    });
    for r in block_on(futures::future::join_all(sends)) {
        r?;
    }
    Ok(start.elapsed().as_secs_f64())
}

fn run_sender<T: Transport>(host: &str) -> anyhow::Result<()> {
    let args = get_args();
    let ctx = open_device()?;
    let pd = ctx.alloc_pd()?;
    let mut stream = TcpStream::connect((host, args.port))?;
    let mut buf = pd.alloc::<u8>(args.size)?;
    buf.fill(42);
    let buf = buf.freeze();
    let mut dst = None;
    let elapsed = if args.use_async {
        run_async::<T>(&ctx, &pd, &mut stream, &buf, &mut dst)?
    } else {
        run_loop::<T>(&ctx, &pd, &mut stream, &buf, &mut dst)?
    };
    println!(
        "{} WRITEs of {} bytes on {} QPs: {:.2} Mops/s, {:.2} Gb/s",
        args.num_iters,
        args.size,
        args.qps,
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
    let cq = ctx.create_async_cq(cq_size(args.qps, args.tx_depth))?;
    let prepared = (0..args.qps)
        .map(|_| builder::<T, _>(&pd).build_on(&cq))
        .collect::<Result<Vec<_>, _>>()?;
    let mine: Vec<Endpoint> = prepared.iter().map(|p| p.endpoint()).collect();
    let theirs: Vec<Endpoint> = exchange(&mut stream, &mine)?;
    let _qps = prepared
        .into_iter()
        .zip(theirs)
        .map(|(p, e)| p.handshake(e))
        .collect::<Result<Vec<_>, _>>()?;
    println!("QP handshakes finished.");

    // The buffer the client writes into, exposed to it alone.
    let dst = pd.alloc::<u8>(args.size)?.expose()?;
    send(&mut stream, &dst.remote())?;
    let _done: u8 = recv(&mut stream)?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = get_args();
    anyhow::ensure!(args.qps >= 1, "at least one QP");
    match (args.host.as_deref(), args.connection.as_str()) {
        (Some(host), "RC") => run_sender::<RC>(host),
        (Some(host), "UC") => run_sender::<UC>(host),
        (None, "RC") => run_receiver::<RC>(),
        (None, "UC") => run_receiver::<UC>(),
        (_, other) => anyhow::bail!("unknown connection type: {other}"),
    }
}
