//! What the demos share: a device and two QPs of it connected to each
//! other, so that each demo runs in one process.
//!
//! With `SAFEVERBS_DEVICE` set (for example to `svrxe0` after
//! `scripts/rxe.sh up`) the demos use that RDMA device; otherwise they run
//! on SafeVerbs' simulated NIC, which needs no hardware.

use safeverbs::{AsyncQp, Context, Model, Nic, Pd, QueuePairBuilder, Transport, Verbs};

/// Runs `demo` on the device `SAFEVERBS_DEVICE` names, else on the model.
pub fn run(demo: impl Demo) -> Result<(), Box<dyn std::error::Error>> {
    match std::env::var("SAFEVERBS_DEVICE") {
        Ok(name) => demo.run(Context::<Verbs>::open(Some(&name))?),
        Err(_) => demo.run(Context::new(Model::new(1))),
    }
}

/// A demo, generic over the NIC.
pub trait Demo {
    /// Runs it on `ctx`.
    fn run<N: Nic>(self, ctx: Context<N>) -> Result<(), Box<dyn std::error::Error>>;
}

/// Two QPs of `pd` connected to each other. Across machines, each side
/// builds one and the endpoints travel over the network (see the examples).
#[allow(dead_code)]
pub fn pair<T: Transport, N: Nic>(pd: &Pd<N>) -> safeverbs::Result<(AsyncQp<T, N>, AsyncQp<T, N>)> {
    let a = QueuePairBuilder::<T, N>::new(pd).build()?;
    let b = QueuePairBuilder::<T, N>::new(pd).build()?;
    let (ea, eb) = (a.endpoint(), b.endpoint());
    Ok((a.handshake(eb)?, b.handshake(ea)?))
}
