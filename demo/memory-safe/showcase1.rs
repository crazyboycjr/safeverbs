//! The Rust side of `showcase1.c`: in C, nothing stops a program from
//! freeing (or reusing) a buffer while a posted SEND still reads it; the
//! bytes that go out are then whatever the memory holds by the time the
//! NIC gets to them.
//!
//! With SafeVerbs the request owns a lease on the buffer from the moment it
//! is posted until it completes. Dropping every handle the program has, and
//! even the future of the SEND, frees nothing early: the memory stays
//! registered and unchanged until the NIC is done, then it is released.
//! Writing into the buffer while it is in flight does not compile: a sent
//! buffer is a frozen `Buf`, which has no `&mut` access, and getting the
//! writable `BufMut` back (`try_thaw`) fails while any request holds it.

#[path = "../common.rs"]
mod common;

use futures::executor::block_on;
use safeverbs::{Context, Nic, RC};

const MSG: &[u8] = b"Hello, SafeVerbs";

struct Showcase;

impl common::Demo for Showcase {
    fn run<N: Nic>(self, ctx: Context<N>) -> Result<(), Box<dyn std::error::Error>> {
        let pd = ctx.alloc_pd()?;
        let (qp, peer) = common::pair::<RC, N>(&pd)?;
        let recv = peer.recv(pd.alloc::<u8>(64)?);

        let mut buf = pd.alloc::<u8>(MSG.len())?;
        buf.copy_from_slice(MSG);
        let buf = buf.freeze();
        let send = qp.send(buf.clone());

        // In C: free(addr) here. In Rust the request still holds the buffer:
        let buf = buf
            .try_thaw()
            .expect_err("in flight: cannot get it back to write into");
        drop(buf); // the last handle of the program
        // buf[0] = b'X'; // does not compile: `Buf` is read only, and `buf` is gone
        drop(send); // even the future: the SEND goes on, the lease stays in its slot

        let (wc, got) = block_on(recv)?;
        println!(
            "received {:?}",
            std::str::from_utf8(&got[..wc.byte_len as usize])?
        );
        assert_eq!(&got[..MSG.len()], MSG);
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::run(Showcase)
}
