//! Batches: several SENDs and WRITEs posted as one request.

use safeverbs_core::{Buf, Error, Leases, Nic, Part, PostError, RemoteBuf, Request, Seq};
use safeverbs_core::{Transport, WrOp};
use zerocopy::{Immutable, IntoBytes};

use crate::Verbs;

/// The SENDs and WRITEs of one [`AsyncQp::batch`](crate::AsyncQp::batch) or
/// [`LocalQp::post_batch`](crate::LocalQp::post_batch), rung with one doorbell and completed with
/// one completion. `C` is what the request carries: the `wr_id` of a `LocalQp`.
///
/// When the send queue has room they go into the request as they are
/// added; otherwise they are collected and posted, in order, once there is
/// room. The methods return nothing: the first invalid operation (another
/// protection domain, a WRITE longer than its destination) is skipped and
/// later ones are ignored; what was added before it still goes out, and the
/// batch yields the error.
pub struct Batch<'a, T: Transport, N: Nic = Verbs, C = ()> {
    target: Target<'a, T, N, C>,
    error: Option<Error>,
    ops: usize,
}

/// Where a batch's operations go.
enum Target<'a, T: Transport, N: Nic, C> {
    /// Into the request as added.
    Direct(Request<'a, T, C, N>),
    /// Collected for later.
    Collect(Vec<(WrOp, Part<N>)>),
}

impl<'a, T: Transport, N: Nic, C> Batch<'a, T, N, C> {
    pub(crate) fn direct(req: Request<'a, T, C, N>) -> Self {
        Batch {
            target: Target::Direct(req),
            error: None,
            ops: 0,
        }
    }

    pub(crate) fn collect() -> Self {
        Batch {
            target: Target::Collect(Vec::new()),
            error: None,
            ops: 0,
        }
    }

    /// Finishes a direct batch, with `ctx` to come back with its completion, and rings the
    /// doorbell.
    pub(crate) fn post(self, ctx: C) -> Result<Seq, PostError<C, N>> {
        let Batch { target, error, .. } = self;
        let Target::Direct(req) = target else {
            unreachable!("a collected batch is queued, not posted")
        };
        let error = match error {
            Some(error) => {
                // What was added before the error goes out, owned by nobody.
                let _ = req.submit_detached();
                error
            }
            None if req.is_empty() => Error::Invalid("an empty batch"),
            None => return req.submit(true, ctx),
        };
        Err(PostError {
            error,
            wr_id: ctx,
            leases: Leases::default(),
        })
    }

    /// The operations of a collected batch.
    pub(crate) fn into_ops(self) -> Result<Vec<(WrOp, Part<N>)>, Error> {
        match (self.target, self.error) {
            (_, Some(error)) => Err(error),
            (Target::Collect(ops), None) if !ops.is_empty() => Ok(ops),
            _ => Err(Error::Invalid("an empty batch")),
        }
    }

    fn add(&mut self, op: WrOp, part: Part<N>) {
        if self.error.is_some() {
            return;
        }
        match &mut self.target {
            Target::Direct(req) => match req.push(op, part) {
                Ok(()) => self.ops += 1,
                Err(e) => self.error = Some(e.error),
            },
            Target::Collect(ops) => {
                ops.push((op, part));
                self.ops += 1;
            }
        }
    }

    /// The operations added so far.
    pub fn len(&self) -> usize {
        self.ops
    }

    /// Whether nothing was added.
    pub fn is_empty(&self) -> bool {
        self.ops == 0
    }

    /// Adds a SEND of `buf`.
    pub fn send<U: IntoBytes + Immutable>(&mut self, buf: Buf<U, N>) {
        self.add(WrOp::Send, buf.into_part());
    }

    /// Adds a SEND of `buf` with immediate data.
    pub fn send_imm<U: IntoBytes + Immutable>(&mut self, buf: Buf<U, N>, imm: u32) {
        self.add(WrOp::SendImm(imm), buf.into_part());
    }

    /// Adds an RDMA WRITE of `buf` to `dst`.
    pub fn write<U: IntoBytes + Immutable>(&mut self, buf: Buf<U, N>, dst: RemoteBuf) {
        self.add(WrOp::Write(dst), buf.into_part());
    }

    /// Adds an RDMA WRITE of `buf` to `dst` with immediate data.
    pub fn write_imm<U: IntoBytes + Immutable>(
        &mut self,
        buf: Buf<U, N>,
        dst: RemoteBuf,
        imm: u32,
    ) {
        self.add(WrOp::WriteImm(dst, imm), buf.into_part());
    }
}
