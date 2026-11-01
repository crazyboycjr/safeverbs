//! The typestate pattern: each step of connection setup is its own type,
//! so a QP can only be used in the order the verbs require, and settings
//! that make no sense for a transport do not exist for it.
//!
//! - `QueuePairBuilder<T>`: configuration only.
//! - `PreparedQp<T>`: the QP exists (RESET); it has an endpoint to send to
//!   the peer, and the only way forward is `handshake` with the peer's,
//!   which moves it through INIT and RTR to RTS.
//! - `LocalQp<T>`: connected; the data path is here and nowhere else.
//!
//! The lines commented out below do not compile; each says why.

#[path = "../common.rs"]
mod common;

use futures::executor::block_on;
use safeverbs::{Context, Nic, QueuePairBuilder, RC, UC};

struct Typestate;

impl common::Demo for Typestate {
    fn run<N: Nic>(self, ctx: Context<N>) -> Result<(), Box<dyn std::error::Error>> {
        let pd = ctx.alloc_pd()?;
        let buf = pd.alloc::<u8>(64)?.freeze();

        let mut rc = QueuePairBuilder::<RC, N>::new(&pd);
        rc.set_retry_count(7); // RC only
        let mut uc = QueuePairBuilder::<UC, N>::new(&pd);
        uc.set_send_depth(64);
        // uc.set_retry_count(7); // no such method: UC does not retransmit

        let a = rc.build()?;
        let b = rc.build()?;
        // a.send(buf.clone()); // no such method: a PreparedQp cannot post
        let (ea, eb) = (a.endpoint(), b.endpoint());
        let a = a.handshake(eb)?;
        let b = b.handshake(ea)?;

        let dst = pd.alloc::<u8>(64)?.expose()?;
        block_on(a.write(buf.clone(), dst.remote()))?;
        let back = block_on(b.read(pd.alloc::<u8>(64)?, dst.remote()))?;
        println!("RC: wrote and read back {} bytes", back.len());

        let u1 = uc.build()?;
        let u2 = uc.build()?;
        let (e1, e2) = (u1.endpoint(), u2.endpoint());
        let u1 = u1.handshake(e2)?;
        let _u2 = u2.handshake(e1)?;
        block_on(u1.write(buf.clone(), dst.remote()))?;
        // u1.read(pd.alloc::<u8>(64)?, dst.remote()); // no such method: UC has no READ
        println!("UC: wrote {} bytes", buf.len());
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::run(Typestate)
}
