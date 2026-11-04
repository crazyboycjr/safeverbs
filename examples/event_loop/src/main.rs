//! The idiomatic high-performance pattern: one completion queue per thread.
//!
//! A [`LocalCq`] and the QPs attached to it are created on their thread and stay there. The
//! thread's loop polls the CQ, dispatches every completion by its `wr_id` (a [`Job`] here, the
//! loop's own type, handed back with the request's result and buffers) and posts new work, ringing
//! each QP's doorbell once per turn. Nothing is locked or allocated on the way.
//!
//! Here a client thread and a server thread each have a CQ with `qps` QPs of the same device, QP i
//! of one connected to QP i of the other, the endpoints travelling over a channel. The client keeps
//! `DEPTH` pings in flight on every QP; the server's loop answers each ping with a pong and reposts
//! the receive; the client counts the round trips.
//!
//! ```text
//! cargo run --release -p event_loop [device] [qps] [pings]
//! ```

use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::Instant;

use safeverbs::{Buf, Context, Endpoint, LocalCq, LocalQp, Pd, QpCaps, RC, WrOp};

/// Pings in flight on each QP (and receives posted on each side).
const DEPTH: u32 = 32;
/// Bytes of a ping or a pong.
const SIZE: usize = 64;
/// CQEs a turn of a loop reads at most.
const POLL: usize = 32;

type Result<T, E = Box<dyn std::error::Error + Send + Sync>> = std::result::Result<T, E>;

/// What a request of the loops is for: the `wr_id` of a `LocalCq<Job>`. It comes back with the
/// request's completion, and that is how the loops dispatch.
#[derive(Debug)]
enum Job {
    /// A receive waiting for a message from the peer of `qps[qp]`.
    Inbox { qp: usize },
    /// A ping on its way.
    Ping,
    /// A pong on its way.
    Pong,
}

/// What a side's thread sends the other: how to reach its QPs.
type Endpoints = (Sender<Vec<Endpoint>>, Receiver<Vec<Endpoint>>);
/// A thread's CQ and the QPs on it.
type Side = (LocalCq<Job>, Vec<LocalQp<RC, Job>>);

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let device = args.next();
    let n: usize = args.next().map_or(Ok(4), |s| s.parse())?;
    let pings: usize = args.next().map_or(Ok(200_000), |s| s.parse())?;
    let ctx = Context::open(device.as_deref())?;
    let pd = ctx.alloc_pd()?;
    println!("{} on {n} QP pairs", ctx.name());
    let ping = filled(&pd, 1)?;
    let pong = filled(&pd, 2)?;

    let (to_server, from_client) = channel();
    let (to_client, from_server) = channel();
    let elapsed = std::thread::scope(|s| {
        let server = s.spawn(|| {
            let (cq, qps) = connect(&ctx, &pd, n, (to_client, from_client))?;
            serve(&cq, &qps, &pong, pings)
        });
        let (cq, qps) = connect(&ctx, &pd, n, (to_server, from_server))?;
        let elapsed = ping_pong(&cq, &qps, &ping, pings);
        server.join().expect("the server thread")?;
        elapsed
    })?;
    println!(
        "{pings} round trips of {SIZE} bytes: {:.2} M/s, {:.2} µs each with {} in flight",
        pings as f64 / elapsed / 1e6,
        elapsed * 1e6 * (n as f64 * DEPTH as f64) / pings as f64,
        n as u32 * DEPTH,
    );
    Ok(())
}

fn filled(pd: &Pd, byte: u8) -> Result<Buf<u8>> {
    let mut buf = pd.alloc::<u8>(SIZE)?;
    buf.fill(byte);
    Ok(buf.freeze())
}

/// This thread's CQ with `n` QPs on it, each connected to its peer on the other thread and with
/// every receive a ping or a pong may need posted, before anything is sent.
fn connect(ctx: &Context, pd: &Pd, n: usize, (to_peer, from_peer): Endpoints) -> Result<Side> {
    // Room for all of its QPs' queues.
    let cq: LocalCq<Job> = ctx.create_local_cq(n as u32 * 2 * DEPTH)?;
    let caps = QpCaps {
        max_send_wr: DEPTH,
        max_recv_wr: DEPTH,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
    };
    let mut prepared = Vec::new();
    for _ in 0..n {
        prepared.push(pd.create_qp::<RC, _>(&cq, caps)?);
    }
    to_peer.send(prepared.iter().map(|qp| qp.endpoint()).collect())?;
    let mut qps = Vec::new();
    for (qp, remote) in prepared.into_iter().zip(from_peer.recv()?) {
        qps.push(qp.handshake(remote)?);
    }
    for (i, qp) in qps.iter().enumerate() {
        for _ in 0..DEPTH {
            qp.post_recv(pd.alloc::<u8>(SIZE)?, Job::Inbox { qp: i })?;
        }
    }
    Ok((cq, qps))
}

/// The server's loop: poll, repost each ping's receive, then answer every ping that has room in
/// its send queue, with one doorbell per QP. The rest wait for the next turn.
fn serve(cq: &LocalCq<Job>, qps: &[LocalQp<RC, Job>], pong: &Buf<u8>, total: usize) -> Result<()> {
    let mut answered = 0;
    let mut owed = vec![0u32; qps.len()];
    let mut error = None;
    while answered < total {
        cq.poll(POLL, |mut wc| {
            if wc.result.is_err() {
                error = Some(format!("{:?}: {:?}", wc.wr_id, wc.result));
                return;
            }
            if let Job::Inbox { qp } = wc.wr_id {
                owed[qp] += 1;
                let buf = wc.buf_mut::<u8>().expect("a receive's buffer");
                qps[qp]
                    .post_recv(buf, Job::Inbox { qp })
                    .expect("room for the receive it freed");
            }
        });
        if let Some(error) = error.take() {
            return Err(error.into());
        }
        for (qp, owed) in qps.iter().zip(&mut owed) {
            let mut posted = false;
            while *owed > 0 && qp.can_post() {
                qp.stage(WrOp::Send, pong.slice(..).into_part(), Job::Pong, true)?;
                *owed -= 1;
                answered += 1;
                posted = true;
            }
            if posted {
                qp.ring()?;
            }
        }
    }
    // The last pongs must leave before the QPs go.
    while qps.iter().any(|qp| qp.in_flight() > 0) {
        cq.poll(POLL, |_| {});
    }
    Ok(())
}

/// The client's loop: poll, count each pong and repost its receive, fill every QP with pings up to
/// `DEPTH`, ring each QP's doorbell once.
fn ping_pong(
    cq: &LocalCq<Job>,
    qps: &[LocalQp<RC, Job>],
    ping: &Buf<u8>,
    total: usize,
) -> Result<f64> {
    let mut sent = 0;
    let mut pongs = 0;
    let mut in_flight = vec![0; qps.len()];
    let mut error = None;
    let start = Instant::now();
    while pongs < total {
        cq.poll(POLL, |mut wc| {
            if wc.result.is_err() {
                error = Some(format!("{:?}: {:?}", wc.wr_id, wc.result));
                return;
            }
            if let Job::Inbox { qp } = wc.wr_id {
                pongs += 1;
                in_flight[qp] -= 1;
                let buf = wc.buf_mut::<u8>().expect("a receive's buffer");
                qps[qp]
                    .post_recv(buf, Job::Inbox { qp })
                    .expect("room for the receive it freed");
            }
        });
        if let Some(error) = error.take() {
            return Err(error.into());
        }
        for (i, qp) in qps.iter().enumerate() {
            let mut posted = false;
            while in_flight[i] < DEPTH && sent < total && qp.can_post() {
                // Unsignaled: the queue asks for a completion only as often as it needs, and the
                // buffers of the pings before it come back with it.
                qp.stage(WrOp::Send, ping.slice(..).into_part(), Job::Ping, false)?;
                in_flight[i] += 1;
                sent += 1;
                posted = true;
            }
            if posted {
                qp.ring()?;
            }
        }
    }
    Ok(start.elapsed().as_secs_f64())
}
