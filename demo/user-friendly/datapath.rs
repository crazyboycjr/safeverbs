//! The data path of `datapath.c` (rc_pingpong's) with SafeVerbs: two sides
//! ping-pong a message `ITERS` times, each posting a receive and a SEND per
//! round and waiting for both.
//!
//! In C there is no value that stands for an operation in progress: the
//! program keeps counters of what it posted, decodes `wr_id`s out of the
//! CQ, and must itself keep the buffers alive and untouched until then. In
//! Rust each operation is a future that owns its buffer until it completes
//! and gives it back; both sides here are plain `async` code, run together
//! on one thread.

#[path = "../common.rs"]
mod common;

use futures::executor::block_on;
use futures::future::try_join;
use safeverbs::{BufMut, Context, LocalQp, Nic, Pd, RC};

const ITERS: u32 = 1000;
const SIZE: usize = 4096;

struct Datapath;

/// One side: post a receive, SEND the message (stamped with the round),
/// wait for both, check what arrived, and go again with the same buffers.
async fn side<N: Nic>(
    qp: &LocalQp<RC, N>,
    pd: &Pd<N>,
    me: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut msg: BufMut<u8, N> = pd.alloc(SIZE)?;
    let mut inbox: BufMut<u8, N> = pd.alloc(SIZE)?;
    for round in 0..ITERS {
        msg.fill(me);
        msg[..4].copy_from_slice(&round.to_le_bytes());
        let frozen = msg.freeze();
        let recv = qp.recv(inbox);
        qp.send(frozen.clone()).await?;
        let (wc, got) = recv.await?;
        assert_eq!(
            wc.received_len().expect("a successful SEND receive") as usize,
            SIZE
        );
        assert_eq!(&got[..4], &round.to_le_bytes());
        assert!(got[4..].iter().all(|&b| b != me), "the peer's message");
        inbox = got;
        // The SEND completed and nothing else holds the buffer.
        msg = frozen.try_thaw().expect("nothing holds it");
    }
    Ok(())
}

impl common::Demo for Datapath {
    fn run<N: Nic>(self, ctx: Context<N>) -> Result<(), Box<dyn std::error::Error>> {
        let pd = ctx.alloc_pd()?;
        let (a, b) = common::pair::<RC, N>(&pd)?;
        let start = std::time::Instant::now();
        block_on(try_join(side(&a, &pd, 1), side(&b, &pd, 2)))?;
        println!(
            "{ITERS} round trips of {SIZE} bytes in {:?}",
            start.elapsed()
        );
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::run(Datapath)
}
