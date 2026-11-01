//! SafeVerbs is easy to set up: a builder with sensible defaults (queue
//! sizes, path MTU from the port, the RoCE v2 GID, RC timeouts) and setters
//! for what you want to change. Compare with the dozens of lines of
//! `ibv_modify_qp` attributes in `rc_pingpong.c`.
//!
//! Every resource is reference counted and released in the right order,
//! whatever order the program drops them in.

#[path = "../common.rs"]
mod common;

use safeverbs::{Context, Nic, QueuePairBuilder, RC};

struct Builder;

impl common::Demo for Builder {
    fn run<N: Nic>(self, ctx: Context<N>) -> Result<(), Box<dyn std::error::Error>> {
        let pd = ctx.alloc_pd()?;

        // Configure, then create: the QP exists only once `build` succeeds.
        let mut builder = QueuePairBuilder::<RC, N>::new(&pd);
        builder
            .set_send_depth(128)
            .set_recv_depth(128)
            .set_min_rnr_timer(12)
            .set_timeout(14)
            .set_retry_count(7)
            .set_rnr_retry(7)
            .set_max_rd_atomic(1);
        let prepared = builder.build()?;
        let peer = QueuePairBuilder::<RC, N>::new(&pd).build()?;

        // Exchange endpoints (over TCP between machines), then connect.
        let (local, remote) = (prepared.endpoint(), peer.endpoint());
        println!("local {local:?}, remote {remote:?}");
        let qp = prepared.handshake(remote)?;
        let _peer = peer.handshake(local)?;

        // A fully connected RC QP. Dropping `ctx` and `pd` now is fine: the
        // QP keeps what it depends on alive.
        drop(pd);
        drop(ctx);
        println!(
            "connected QP {} (lock free: {})",
            qp.qp_num(),
            qp.is_lock_free()
        );
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::run(Builder)
}
