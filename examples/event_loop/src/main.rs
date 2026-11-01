//! The idiomatic high-performance pattern: one worker per thread.
//!
//! A [`Worker`] is one CQ and the QPs attached to it, one value that moves
//! into its thread. The thread's loop polls the CQ, dispatches every
//! completion (its QP, outcome and buffers) and posts new work, ringing each
//! QP's doorbell once per turn. Nothing is locked or allocated on the way.
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

use safeverbs::sync::{QpKey, Queue, Worker, WrOp};
use safeverbs::{Buf, Context, Pd, QueuePairBuilder, RC};

/// Pings in flight on each QP (and receives posted on each side).
const DEPTH: u32 = 32;
/// Bytes of a ping or a pong.
const SIZE: usize = 64;

type Result<T, E = Box<dyn std::error::Error + Send + Sync>> = std::result::Result<T, E>;

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let device = args.next();
    let n: usize = args.next().map_or(Ok(4), |s| s.parse())?;
    let pings: usize = args.next().map_or(Ok(200_000), |s| s.parse())?;
    let ctx = Context::open(device.as_deref())?;
    let pd = ctx.alloc_pd()?;

    // Two workers, each with a CQ for all of its QPs' queues.
    let cq_size = n as u32 * 2 * DEPTH;
    let mut client = Worker::new(&pd, cq_size)?;
    let mut server = Worker::new(&pd, cq_size)?;
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
        for &qp in qps {
            for _ in 0..DEPTH {
                w.post_recv(qp, pd.alloc::<u8>(SIZE)?.into_part(), false)
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
fn serve(mut w: Worker, qps: &[QpKey<RC>], pong: &Buf<u8>, total: usize) -> Result<()> {
    let mut answered = 0;
    let mut rung = vec![true; qps.len()];
    let mut error = None;
    while answered < total {
        w.poll(|w, c| {
            if c.queue != Queue::Recv {
                // A pong's buffer back.
                return;
            }
            if !c.wc.is_ok() {
                error = Some(c.wc);
                return;
            }
            let qp = qps[c.qp];
            // The pong goes out with this turn's doorbell; the loop does not
            // wait for it (detached).
            let mut req = w.request(qp);
            req.push(WrOp::Send, pong.slice(..).into_part())
                .expect("a valid SEND");
            req.finish(true, true);
            let buf = c.leases.into_buf_mut::<u8>().expect("a receive buffer");
            w.post_recv(qp, buf.into_part(), false)
                .expect("room for the receive it freed");
            rung[c.qp] = false;
            answered += 1;
        });
        if let Some(wc) = error {
            return Err(format!("a ping failed: {wc:?}").into());
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
fn ping_pong(mut w: Worker, qps: &[QpKey<RC>], ping: &Buf<u8>, total: usize) -> Result<f64> {
    let mut sent = 0;
    let mut pongs = 0;
    let mut in_flight = vec![0; qps.len()];
    let mut error = None;
    let start = Instant::now();
    while pongs < total {
        w.poll(|w, c| match c.queue {
            Queue::Recv if c.wc.is_ok() => {
                pongs += 1;
                in_flight[c.qp] -= 1;
                let buf = c.leases.into_buf_mut::<u8>().expect("a receive buffer");
                w.post_recv(qps[c.qp], buf.into_part(), false)
                    .expect("room for the receive it freed");
            }
            Queue::Recv => error = Some(c.wc),
            Queue::Send => {}
        });
        if let Some(wc) = error {
            return Err(format!("a pong failed: {wc:?}").into());
        }
        for (i, &qp) in qps.iter().enumerate() {
            let mut posted = 0;
            while in_flight[i] < DEPTH && sent < total && w.send_ring(qp).can_post() {
                // Detached and unsignaled: the ring asks for a completion
                // only as often as the send queue needs.
                let mut req = w.request(qp);
                req.push(WrOp::Send, ping.slice(..).into_part())
                    .expect("a valid SEND");
                req.finish(false, true);
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
