//! Batches: several SENDs and WRITEs posted as one request.

use safeverbs_core::engine::OpenRequest;
use safeverbs_core::{
    BufError, Engine, Error, Leases, Lend, Nic, Part, PostError, QpKey, RemoteBuf, Seq, Transport,
    WrOp,
};

use crate::Verbs;

/// Where a batch goes for what its request being written cannot tell by itself.
pub(crate) trait BatchQp<N: Nic> {
    /// Adds a WQE, whatever it takes: a request that is full goes out and waits for room.
    fn push_general(
        &self,
        open: &mut OpenRequest<N>,
        op: WrOp,
        part: Part<N>,
    ) -> Result<(), BufError<Part<N>>>;
}

/// The SENDs and WRITEs of one [`AsyncQp::batch`](crate::AsyncQp::batch) or
/// [`LocalQp::post_batch`](crate::LocalQp::post_batch), rung with one doorbell and completed with
/// one completion.
///
/// They are collected in storage that comes from the QP and goes back to it, so nothing is
/// allocated per batch, and written to the hardware when the closure returns, with nothing held
/// meanwhile: the closure may use the QP, its CQ and other QPs. Consecutive operations from one
/// frozen buffer keep one reference to it. By reference, `b.write(&buf, dst)` or
/// `b.write(buf.view(..64), dst)`, they touch no reference count after the first; by value they
/// take a clone each.
///
/// The methods return nothing: the first invalid operation (another
/// protection domain, a WRITE longer than its destination) is skipped and
/// later ones are ignored; what was added before it still goes out if nothing waits before it, and
/// the batch yields the error.
pub struct Batch<'a, N: Nic = Verbs> {
    open: OpenRequest<N>,
    qp: &'a dyn BatchQp<N>,
    error: Option<Error>,
    /// The operations that went out already, in requests of their own.
    out: usize,
}

impl<'a, N: Nic> Batch<'a, N> {
    #[inline(always)]
    pub(crate) fn new(open: OpenRequest<N>, qp: &'a dyn BatchQp<N>) -> Self {
        Batch {
            open,
            qp,
            error: None,
            out: 0,
        }
    }

    /// What was collected, as a request of its own with `ctx`, and the doorbell. It waits for room
    /// if it must, polling the CQ.
    #[inline(always)]
    pub(crate) fn post<T: Transport, C>(
        &mut self,
        w: &mut Engine<C, N>,
        qp: QpKey<T>,
        ctx: C,
    ) -> Result<Seq, PostError<C, N>> {
        if self.error.is_none() && !self.open.is_empty() {
            w.submit_open(qp, &mut self.open, true, ctx)
        } else {
            Err(self.not_posted(w, qp, ctx))
        }
    }

    /// Why [`Batch::post`] has nothing to post.
    #[cold]
    #[inline(never)]
    fn not_posted<T: Transport, C>(
        &mut self,
        w: &mut Engine<C, N>,
        qp: QpKey<T>,
        ctx: C,
    ) -> PostError<C, N> {
        // What was added before the error goes out, owned by nobody.
        let _ = w.submit_open_detached(qp, &mut self.open);
        PostError {
            error: self
                .error
                .take()
                .unwrap_or(Error::Invalid("an empty batch")),
            wr_id: ctx,
            leases: Leases::default(),
        }
    }

    /// The first error of its operations, if any.
    pub(crate) fn take_error(&mut self) -> Option<Error> {
        self.error.take()
    }

    /// The request being written.
    #[inline(always)]
    pub(crate) fn open(&mut self) -> &mut OpenRequest<N> {
        &mut self.open
    }

    /// The request being written, for later.
    pub(crate) fn take_open(&mut self) -> OpenRequest<N> {
        core::mem::replace(&mut self.open, OpenRequest::closed())
    }

    /// An operation the request being written does not take from a buffer it already keeps.
    #[inline(never)]
    fn add_lent<B: Lend<N>>(&mut self, op: WrOp, buf: B) {
        if self.error.is_some() {
            return;
        }
        match self.open.try_push(op, buf.lend()) {
            Ok(()) => {}
            Err(part) => self.add_other(op, part),
        }
    }

    /// An operation out of the common: an error before it, or one of its own, or a request that
    /// is full.
    #[inline(never)]
    fn add_other(&mut self, op: WrOp, part: Part<N>) {
        if self.error.is_some() {
            return;
        }
        let before = self.open.len();
        match self.qp.push_general(&mut self.open, op, part) {
            Ok(()) => {
                if self.open.len() <= before {
                    self.out += before;
                }
            }
            Err(e) => {
                self.error = Some(e.error);
                self.open.stop();
            }
        }
    }

    /// The operations added so far.
    pub fn len(&self) -> usize {
        self.out + self.open.len()
    }

    /// Whether nothing was added.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Adds a SEND of `buf`: a [`Buf`](crate::Buf), by value or by reference, or a
    /// [`BufView`](crate::BufView).
    #[inline(always)]
    pub fn send<B: Lend<N>>(&mut self, buf: B) {
        if !self.open.try_push_lent(WrOp::Send, &buf) {
            self.add_lent(WrOp::Send, buf);
        }
    }

    /// Adds a SEND of `buf` with immediate data.
    #[inline(always)]
    pub fn send_imm<B: Lend<N>>(&mut self, buf: B, imm: u32) {
        if !self.open.try_push_lent(WrOp::SendImm(imm), &buf) {
            self.add_lent(WrOp::SendImm(imm), buf);
        }
    }

    /// Adds an RDMA WRITE of `buf` to `dst`: a [`Buf`](crate::Buf), by value or by reference, or a
    /// [`BufView`](crate::BufView).
    #[inline(always)]
    pub fn write<B: Lend<N>>(&mut self, buf: B, dst: RemoteBuf) {
        if !self.open.try_push_write(dst, &buf) {
            self.add_lent(WrOp::Write(dst), buf);
        }
    }

    /// Adds an RDMA WRITE of `buf` to `dst` with immediate data.
    #[inline(always)]
    pub fn write_imm<B: Lend<N>>(&mut self, buf: B, dst: RemoteBuf, imm: u32) {
        if !self.open.try_push_lent(WrOp::WriteImm(dst, imm), &buf) {
            self.add_lent(WrOp::WriteImm(dst, imm), buf);
        }
    }
}
