//! A sink of RDMA WRITEs.

use core::pin::Pin;
use core::task::{Context, Poll};

use futures_sink::Sink;
use safeverbs_core::ring::State;
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
/// [`AsyncQp::sink`](crate::AsyncQp::sink).
///
/// - `poll_ready` waits for room in the send queue;
/// - `start_send` writes the WQE (unsignaled unless the ring needs a
///   signaled one), holding the latest back so that a flush can signal it;
/// - `poll_flush` writes the held WQE signaled and rings the doorbell;
/// - `poll_close` flushes and waits until everything sent has completed.
///
/// Errors: the first error among the QP's requests without an owner (this
/// sink's among them) is reported by the next call: as
/// [`Error::SinkItem`](safeverbs_core::Error::SinkItem) with the failed item's
/// position among the items sent through this sink, or
/// [`Error::Completion`](safeverbs_core::Error::Completion) if it was another
/// request's. Items before it completed successfully; it and those after it did
/// not, and their remote effects are unknown (see
/// [`CompletionError`](safeverbs_core::CompletionError)). Dropping the sink
/// flushes it.
pub struct WriteSink<'q, Q: QueueAccess, U: IntoBytes + Immutable> {
    q: &'q Q,
    held: Option<WriteOp<U, Q::N>>,
    last: Option<Seq>,
    /// How many items were posted.
    items: u64,
    /// The runs of consecutive sequence numbers its items took, oldest first,
    /// each from a request and its item: another request of the QP in between
    /// starts a new run. One allocation, unless other requests come between.
    runs: Vec<(Seq, u64)>,
}

impl<'q, Q: QueueAccess, U: IntoBytes + Immutable> WriteSink<'q, Q, U> {
    pub(crate) fn new(q: &'q Q) -> Self {
        WriteSink {
            q,
            held: None,
            last: None,
            items: 0,
            runs: Vec::new(),
        }
    }

    /// Writes `op`'s WQE as a request without an owner.
    fn post(&mut self, op: WriteOp<U, Q::N>, signal: bool) -> Result<(), Error> {
        let wr = match op.imm {
            Some(imm) => WrOp::WriteImm(op.dst, imm),
            None => WrOp::Write(op.dst),
        };
        let item = self.items;
        self.items += 1;
        let seq = self
            .q
            .with_qp(|w, qp| w.stage_detached(qp, wr, op.buf.into_part(), signal))
            .map_err(|e| e.error)?;
        self.last = Some(seq);
        let follows = self
            .runs
            .last()
            .is_some_and(|&(first, of)| seq - first == item - of);
        if !follows {
            self.runs.push((seq, item));
        }
        Ok(())
    }

    /// The item that took request `seq`, if one did.
    fn item_of(&self, seq: Seq) -> Option<u64> {
        let i = self.runs.partition_point(|&(first, _)| first <= seq);
        let (first, of) = *self.runs.get(i.checked_sub(1)?)?;
        let item = of + (seq - first);
        // The run ends where the next one starts, or at the items posted.
        let end = self.runs.get(i).map_or(self.items, |&(_, next)| next);
        (item < end).then_some(item)
    }

    /// Writes the held WQE signaled and rings the doorbell.
    fn flush_now(&mut self) -> Result<(), Error> {
        if let Some(op) = self.held.take() {
            self.post(op, true)?;
        }
        self.q.with_qp(|w, qp| w.commit(qp))
    }

    fn take_error(&self) -> Result<(), Error> {
        match self.q.with_qp(|w, qp| w.take_send_error(qp)) {
            Some(failed) => Err(match self.item_of(failed.seq) {
                Some(item) => Error::SinkItem {
                    item,
                    error: failed.error,
                },
                None => Error::Completion(failed.error),
            }),
            None => Ok(()),
        }
    }
}

impl<Q: QueueAccess, U: IntoBytes + Immutable> Unpin for WriteSink<'_, Q, U> {}

impl<Q: QueueAccess, U: IntoBytes + Immutable> Sink<WriteOp<U, Q::N>> for WriteSink<'_, Q, U> {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.take_error()?;
        let ready = self.q.with_state(|s| {
            s.drain();
            let idle = s.sends_idle();
            let qp = s.key();
            let w = s.engine();
            w.progress();
            if idle && w.send_ring(qp).can_post() {
                return Ok::<bool, Error>(true);
            }
            // What is written needs the doorbell to ever complete.
            w.commit(qp)?;
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
            Some(last) => this.q.with_qp(|w, qp| {
                w.progress();
                // Completed (and freed, it has no owner); or refused by the device with its
                // doorbell (the ring then records it as refused).
                matches!(w.send_ring(qp).state(last), State::Free)
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

impl<Q: QueueAccess, U: IntoBytes + Immutable> Drop for WriteSink<'_, Q, U> {
    fn drop(&mut self) {
        let _ = self.flush_now();
    }
}
