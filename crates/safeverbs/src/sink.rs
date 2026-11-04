//! A sink of RDMA WRITEs.

use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::VecDeque;

use futures_sink::Sink;
use safeverbs_core::engine::OpenRequest;
use safeverbs_core::ring::State;
use safeverbs_core::{Buf, CompletionError, DetachedError, Error, Nic, Part, RemoteBuf, Seq, WrOp};
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

    /// What it asks of the NIC.
    #[inline(always)]
    fn wr(&self) -> WrOp {
        match self.imm {
            Some(imm) => WrOp::WriteImm(self.dst, imm),
            None => WrOp::Write(self.dst),
        }
    }
}

/// A [`Sink`] of RDMA WRITEs on a QP, from
/// [`AsyncQp::sink`](crate::AsyncQp::sink).
///
/// Its items are written like a batch's: they gather in a request of its own, with nothing held
/// and no look at the CQ, and go out together.
///
/// - `poll_ready` is ready while that request has room; once it is full it goes out as a signaled
///   request, waiting for room in the send queue if it must, and only then is the CQ polled;
/// - `start_send` adds the WQE;
/// - `poll_flush` sends the request, signaled, rings the doorbell and polls the CQ;
/// - `poll_close` flushes and waits until everything sent has completed.
///
/// So until a flush, items have no order against other requests of the QP: flush first if one
/// must come after them.
///
/// Errors: the first error among the QP's requests without an owner (this
/// sink's among them) is seen when the CQ is polled, and reported by the next `poll_ready` that
/// starts or sends a request, the next flush or the close: as
/// [`Error::SinkItem`](safeverbs_core::Error::SinkItem) with the failed item's
/// position among the items sent through this sink, or
/// [`Error::Completion`](safeverbs_core::Error::Completion) if it was another
/// request's. Items before it completed successfully; it and those after it did
/// not, and their remote effects are unknown (see
/// [`CompletionError`](safeverbs_core::CompletionError)). An item refused at once, for another
/// protection domain or a WRITE longer than its destination, is `start_send`'s error, and no item.
/// Dropping the sink flushes it.
pub struct WriteSink<'q, Q: QueueAccess, U: IntoBytes + Immutable> {
    q: &'q Q,
    /// The request its items gather in.
    open: OpenRequest<Q::N>,
    last: Option<Seq>,
    /// How many items were accepted.
    items: u64,
    /// The requests of its items that may still report an error, oldest first, each with its first
    /// item.
    runs: VecDeque<(Seq, u64)>,
    _u: PhantomData<fn(U)>,
}

impl<'q, Q: QueueAccess, U: IntoBytes + Immutable> WriteSink<'q, Q, U> {
    pub(crate) fn new(q: &'q Q) -> Self {
        WriteSink {
            q,
            open: q.with_state(|s| s.open_request()),
            last: None,
            items: 0,
            runs: VecDeque::new(),
            _u: PhantomData,
        }
    }

    /// Sends what gathered, as a signaled request, if nothing waits before it and it fits; else
    /// rings the doorbell for what is written and polls the CQ, and says it did not.
    fn send_open(&mut self) -> Result<bool, Error> {
        if self.open.is_empty() {
            return Ok(true);
        }
        let first = self.items - self.open.len() as u64;
        let open = &mut self.open;
        let sent = self.q.with_state(|s| {
            s.drain();
            let idle = s.sends_idle();
            let qp = s.key();
            let w = s.engine();
            if idle && w.open_fits(qp, open) {
                return Ok::<_, Error>(w.close_open(qp, open, true, None).ok());
            }
            // What is written needs the doorbell to ever complete.
            w.commit(qp)?;
            w.progress();
            Ok(None)
        })?;
        match sent {
            Some(seq) => {
                self.last = Some(seq);
                self.runs.push_back((seq, first));
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Sends what gathered if it can, then rings the doorbell and polls the CQ; whether it could.
    fn flush_now(&mut self) -> Result<bool, Error> {
        if !self.send_open()? {
            return Ok(false);
        }
        self.q.with_qp(|w, qp| {
            w.commit(qp)?;
            w.progress();
            Ok(true)
        })
    }

    /// The error of the request `failed`, as an item's if it is a request of this sink's.
    fn error_of(&self, failed: DetachedError) -> Error {
        let Ok(i) = self.runs.binary_search_by_key(&failed.seq, |&(seq, _)| seq) else {
            return Error::Completion(failed.error);
        };
        let first = self.runs[i].1;
        match failed.error {
            CompletionError::Failed {
                status,
                vendor_err,
                position,
            } => Error::SinkItem {
                item: first + position as u64,
                error: CompletionError::Failed {
                    status,
                    vendor_err,
                    position: 0,
                },
            },
            error => Error::SinkItem { item: first, error },
        }
    }

    /// Reports the first error seen, if any; else forgets the requests that completed.
    fn take_error(&mut self) -> Result<(), Error> {
        let (failed, completed) = self
            .q
            .with_qp(|w, qp| (w.take_send_error(qp), w.send_ring(qp).retired()));
        if let Some(failed) = failed {
            return Err(self.error_of(failed));
        }
        while self.runs.front().is_some_and(|&(seq, _)| seq < completed) {
            self.runs.pop_front();
        }
        Ok(())
    }

    /// [`Sink::poll_ready`] at the start of a request, or with one that is full.
    #[inline(never)]
    fn ready_other(&mut self, cx: &Context<'_>) -> Poll<Result<(), Error>> {
        self.take_error()?;
        if self.open.has_room() || self.send_open()? {
            return Poll::Ready(Ok(()));
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    /// [`Sink::start_send`] of a buffer the request does not keep yet.
    #[inline(never)]
    fn push_lent(&mut self, item: WriteOp<U, Q::N>) -> Result<(), Error> {
        let wr = item.wr();
        match self.open.try_push(wr, item.buf.into_part()) {
            Ok(()) => Ok(()),
            Err(part) => self.push_other(wr, part),
        }
    }

    /// [`Sink::start_send`] out of the common, which says why if it has to refuse.
    #[inline(never)]
    fn push_other(&mut self, wr: WrOp, part: Part<Q::N>) -> Result<(), Error> {
        // Full, for a caller that did not ask `poll_ready`: it waits here.
        while !self.open.has_room() && !self.open.is_empty() {
            self.send_open()?;
        }
        let open = &mut self.open;
        self.q
            .with_qp(|w, qp| w.push_open(qp, open, wr, part))
            .map_err(|e| e.error)
    }
}

impl<Q: QueueAccess, U: IntoBytes + Immutable> Unpin for WriteSink<'_, Q, U> {}

impl<Q: QueueAccess, U: IntoBytes + Immutable> Sink<WriteOp<U, Q::N>> for WriteSink<'_, Q, U> {
    type Error = Error;

    #[inline]
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let this = self.get_mut();
        if this.open.has_room() && !this.open.is_empty() {
            return Poll::Ready(Ok(()));
        }
        this.ready_other(cx)
    }

    #[inline]
    fn start_send(self: Pin<&mut Self>, item: WriteOp<U, Q::N>) -> Result<(), Error> {
        let this = self.get_mut();
        let pushed = match item.imm {
            None => this.open.try_push_write(item.dst, &item.buf),
            Some(imm) => this
                .open
                .try_push_lent(WrOp::WriteImm(item.dst, imm), &item.buf),
        };
        if !pushed {
            this.push_lent(item)?;
        }
        this.items += 1;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let this = self.get_mut();
        if !this.flush_now()? {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready(this.take_error())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let this = self.get_mut();
        if !this.flush_now()? {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let done = match this.last {
            Some(last) => this.q.with_qp(|w, qp| {
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
        let open = &mut self.open;
        let _ = self.q.with_qp(|w, qp| w.submit_open_detached(qp, open));
    }
}
