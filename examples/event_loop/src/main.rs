//! The idiomatic high-performance pattern: one worker per thread.
//!
//! A [`Worker`] is one CQ and the QPs attached to it, one value that moves
//! into its thread. The thread's loop polls the CQ, dispatches every
//! completion by its context (a [`Job`] here, the loop's own type, kept with
//! its request and handed back with its result and buffers) and posts new
//! work, ringing each QP's doorbell once per turn. Nothing is locked or
//! allocated on the way.
//!
//! Here a client thread and a server thread each own a worker of `qps` QPs
//! on the same device, QP i of one connected to QP i of the other. The
//! client keeps `DEPTH` pings in flight on every QP; the server's loop
//! answers each ping with a pong and reposts the receive; the client counts
//! the round trips.
//!
//! ```text
//! cargo run --release -p event_loop [device] [qps] [pings]
//! ```

use std::time::Instant;

use safeverbs::sync::{QpKey, Worker, WrOp};
use safeverbs::{Buf, Completed, Context, Pd, QueuePairBuilder, RC, Received};

/// Pings in flight on each QP (and receives posted on each side).
const DEPTH: u32 = 32;
/// Bytes of a ping or a pong.
const SIZE: usize = 64;

type Result<T, E = Box<dyn std::error::Error + Send + Sync>> = std::result::Result<T, E>;

/// What a request of the loops is for: the context of a `Worker<Job>`. It comes back
/// with the request's completion, and that is how the loops dispatch.
#[derive(Debug)]
enum Job {
    /// A receive waiting for a message from the peer of `qps[qp]`.
    Inbox { qp: usize },
    /// A pong on its way.
    Pong,
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let device = args.next();
    let n: usize = args.next().map_or(Ok(4), |s| s.parse())?;
    let pings: usize = args.next().map_or(Ok(200_000), |s| s.parse())?;
    let ctx = Context::open(device.as_deref())?;
    let pd = ctx.alloc_pd()?;

    // Two workers, each with a CQ for all of its QPs' queues.
    let cq_size = n as u32 * 2 * DEPTH;
    let mut client: Worker<Job> = Worker::new(&pd, cq_size)?;
    let mut server: Worker<Job> = Worker::new(&pd, cq_size)?;
    let mut b = QueuePairBuilder::<RC>::new(&pd);
    b.set_send_depth(DEPTH).set_recv_depth(DEPTH);
    let (mut cqps, mut sqps) = (Vec::new(), Vec::new());
    for _ in 0..n {
        let (c, c_end, c_attr) = b.build_in(&mut client)?;
        let (s, s_end, s_attr) = b.build_in(&mut server)?;
        client.connect(c, &c_attr, &s_end)?;
        server.connect(s, &s_attr, &c_end)?;
        cqps.push(c);
        sqps.push(s);
    }
    println!(
        "{} on {n} QP pairs, workers on a thread domain: {}",
        ctx.name(),
        client.is_lock_free()
    );
    // Every receive a ping or a pong may need, before anything is sent.
    for (w, qps) in [(&mut client, &cqps), (&mut server, &sqps)] {
        for (i, &qp) in qps.iter().enumerate() {
            for _ in 0..DEPTH {
                w.recv(qp, pd.alloc::<u8>(SIZE)?, Job::Inbox { qp: i })
                    .map_err(|e| e.error)?;
            }
        }
    }
    let ping = filled(&pd, 1)?;
    let pong = filled(&pd, 2)?;

    // Each worker moves into its thread.
    let elapsed = std::thread::scope(|s| {
        let server = s.spawn(move || serve(server, &sqps, &pong, pings));
        let elapsed = ping_pong(client, &cqps, &ping, pings);
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

/// The server's loop: poll, answer each ping, repost its receive, ring each
/// QP's doorbell once.
fn serve(mut w: Worker<Job>, qps: &[QpKey<RC>], pong: &Buf<u8>, total: usize) -> Result<()> {
    let mut answered = 0;
    let mut rung = vec![true; qps.len()];
    let mut error = None;
    while answered < total {
        w.poll(|w, c| match (c.ctx, c.result) {
            (Job::Inbox { qp }, Ok(Completed::Received(Received::Message { .. }))) => {
                // The pong goes out with this turn's doorbell.
                let mut req = w.request(qps[qp]);
                req.push(WrOp::Send, pong.slice(..).into_part())
                    .expect("a valid SEND");
                req.finish(true, Job::Pong).expect("a WQE was added");
                let buf = c.leases.into_buf_mut::<u8>().expect("a receive buffer");
                w.recv(qps[qp], buf, Job::Inbox { qp })
                    .expect("room for the receive it freed");
                rung[qp] = false;
                answered += 1;
            }
            // A pong's buffer back.
            (Job::Pong, Ok(_)) => {}
            (job, result) => error = Some(format!("{job:?}: {result:?}")),
        });
        if let Some(error) = error.take() {
            return Err(error.into());
        }
        for (i, &qp) in qps.iter().enumerate() {
            if !rung[i] {
                w.commit(qp)?;
                rung[i] = true;
            }
        }
    }
    // The last pongs must leave before the QPs go.
    while qps.iter().any(|&qp| w.send_ring(qp).in_flight_wqes() > 0) {
        w.progress();
    }
    Ok(())
}

/// The client's loop: poll, count each pong and repost its receive, fill
/// every QP with pings up to `DEPTH`, ring each QP's doorbell once.
fn ping_pong(mut w: Worker<Job>, qps: &[QpKey<RC>], ping: &Buf<u8>, total: usize) -> Result<f64> {
    let mut sent = 0;
    let mut pongs = 0;
    let mut in_flight = vec![0; qps.len()];
    let mut error = None;
    let start = Instant::now();
    while pongs < total {
        w.poll(|w, c| match (c.ctx, c.result) {
            (Job::Inbox { qp }, Ok(Completed::Received(Received::Message { .. }))) => {
                pongs += 1;
                in_flight[qp] -= 1;
                let buf = c.leases.into_buf_mut::<u8>().expect("a receive buffer");
                w.recv(qps[qp], buf, Job::Inbox { qp })
                    .expect("room for the receive it freed");
            }
            (job, result) => error = Some(format!("{job:?}: {result:?}")),
        });
        if let Some(error) = error.take() {
            return Err(error.into());
        }
        for (i, &qp) in qps.iter().enumerate() {
            let mut posted = 0;
            while in_flight[i] < DEPTH && sent < total && w.send_ring(qp).can_post() {
                // Without an owner and unsignaled: the ring asks for a
                // completion only as often as the send queue needs.
                let mut req = w.request(qp);
                req.push(WrOp::Send, ping.slice(..).into_part())
                    .expect("a valid SEND");
                req.finish_detached(false);
                in_flight[i] += 1;
                sent += 1;
                posted += 1;
            }
            if posted > 0 {
                w.commit(qp)?;
            }
        }
    }
    Ok(start.elapsed().as_secs_f64())
}
