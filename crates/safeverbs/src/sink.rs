//! A sink of RDMA WRITEs.

use core::pin::Pin;
use core::task::{Context, Poll};

use futures_sink::Sink;
use safeverbs_core::{Buf, Error, Nic, RemoteBuf, Seq, WrOp};
use zerocopy::{Immutable, IntoBytes};

use crate::Verbs;
use crate::qp::QueueAccess;

/// One RDMA WRITE for a [`WriteSink`]: a frozen buffer, where it goes in
/// the peer's memory, and optionally immediate data.
pub struct WriteOp<U = u8, N: Nic = Verbs> {
    /// What to write.
    pub buf: Buf<U, N>,
    /// Where in the peer's memory.
    pub dst: RemoteBuf,
    /// Immediate data, which also consumes one of the peer's receives.
    pub imm: Option<u32>,
}

impl<U, N: Nic> WriteOp<U, N> {
    /// A WRITE of `buf` to `dst`.
    pub fn new(buf: Buf<U, N>, dst: RemoteBuf) -> Self {
        WriteOp {
            buf,
            dst,
            imm: None,
        }
    }

    /// The same WRITE with immediate data.
    pub fn with_imm(mut self, imm: u32) -> Self {
        self.imm = Some(imm);
        self
    }
}

/// A [`Sink`] of RDMA WRITEs on a QP, from
/// [`LocalQp::sink`](crate::LocalQp::sink).
///
/// - `poll_ready` waits for room in the send queue;
/// - `start_send` writes the WQE (unsignaled unless the ring needs a
///   signaled one), holding the latest back so that a flush can signal it;
/// - `poll_flush` writes the held WQE signaled and rings the doorbell;
/// - `poll_close` flushes and waits until everything sent has completed.
///
/// Errors: the first error completion among the QP's requests without an
/// owner (this sink's among them) is reported by the next call. Dropping
/// the sink flushes it.
pub struct WriteSink<'q, Q: QueueAccess, U> {
    q: &'q Q,
    held: Option<WriteOp<U, Q::N>>,
    last: Option<Seq>,
}

impl<'q, Q: QueueAccess, U> WriteSink<'q, Q, U> {
    pub(crate) fn new(q: &'q Q) -> Self {
        WriteSink {
            q,
            held: None,
            last: None,
        }
    }

    /// Writes `op`'s WQE as a request without an owner.
    fn post(&mut self, op: WriteOp<U, Q::N>, signal: bool) -> Result<(), Error> {
        let wr = match op.imm {
            Some(imm) => WrOp::WriteImm(op.dst, imm),
            None => WrOp::Write(op.dst),
        };
        let seq = self.q.with_qp(|qp| {
            let mut req = qp.request();
            req.push(wr, op.buf.into_part()).map_err(|e| e.error)?;
            Ok::<_, Error>(req.finish(signal, true))
        })?;
        if seq.is_some() {
            self.last = seq;
        }
        Ok(())
    }

    /// Writes the held WQE signaled and rings the doorbell.
    fn flush_now(&mut self) -> Result<(), Error> {
        if let Some(op) = self.held.take() {
            self.post(op, true)?;
        }
        self.q.with_qp(|qp| qp.commit())
    }

    fn take_error(&self) -> Result<(), Error> {
        match self.q.with_qp(|qp| qp.take_send_error()) {
            Some(wc) => Err(Error::Completion(wc)),
            None => Ok(()),
        }
    }
}

impl<Q: QueueAccess, U> Unpin for WriteSink<'_, Q, U> {}

impl<Q: QueueAccess, U: IntoBytes + Immutable> Sink<WriteOp<U, Q::N>> for WriteSink<'_, Q, U> {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.take_error()?;
        let ready = self.q.with_state(|s| {
            s.drain();
            let idle = s.sends_idle();
            let qp = s.qp();
            qp.poll_send();
            if idle && qp.send_ring().can_post() {
                return Ok::<bool, Error>(true);
            }
            // What is written needs the doorbell to ever complete.
            qp.commit()?;
            Ok(false)
        })?;
        if ready {
            Poll::Ready(Ok(()))
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    fn start_send(self: Pin<&mut Self>, item: WriteOp<U, Q::N>) -> Result<(), Error> {
        let this = self.get_mut();
        if let Some(op) = this.held.replace(item) {
            this.post(op, false)?;
        }
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let this = self.get_mut();
        this.flush_now()?;
        Poll::Ready(this.take_error())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let this = self.get_mut();
        this.flush_now()?;
        let done = match this.last {
            Some(last) => this.q.with_qp(|qp| {
                qp.poll_send();
                qp.send_ring().retired() > last
            }),
            None => true,
        };
        if done {
            Poll::Ready(this.take_error())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl<Q: QueueAccess, U> Drop for WriteSink<'_, Q, U> {
    fn drop(&mut self) {
        let _ = self.flush_now();
    }
}
