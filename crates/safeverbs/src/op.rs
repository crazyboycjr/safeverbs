//! The futures of single operations: each holds its queue and the
//! request's sequence number (or, while the queue is full, its place in
//! the backlog); the request's buffers and waker live in the QP's ring.

use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll};

use safeverbs_core::{
    BufError, BufMut, Completed, CompletionError, CompletionResult, Error, Leases, Nic, Part,
    Queue, Received, Seq, WrOp,
};
use zerocopy::FromBytes;

use crate::backlog::{Posted, Work};
use crate::qp::QueueAccess;

/// What a request that completed reports: what it did, or why it did not.
pub(crate) type TakenResult = CompletionResult;

/// What a finished operation yields, made from its result and the buffers it
/// gets back.
pub trait Outcome<N: Nic> {
    /// The future's output.
    type Output;
    /// The request completed with `result`.
    fn done(result: Result<Completed, CompletionError>, leases: Leases<N>) -> Self::Output;
    /// The request could not be posted; `back` holds its buffers.
    fn failed(error: Error, back: Leases<N>) -> Self::Output;
}

/// SEND and RDMA WRITE (and batches of them): the buffers were frozen, the
/// caller keeps its own copies; yields nothing, since the NIC reports nothing
/// but the status of these operations.
pub struct Sent;

impl<N: Nic> Outcome<N> for Sent {
    type Output = Result<(), Error>;
    #[inline(always)]
    fn done(result: TakenResult, _leases: Leases<N>) -> Self::Output {
        match result {
            Ok(_) => Ok(()),
            Err(error) => Err(Error::Completion(error)),
        }
    }
    fn failed(error: Error, _back: Leases<N>) -> Self::Output {
        Err(error)
    }
}

/// RDMA READ and atomics into a `BufMut<T>`: yields the buffer, or the
/// error with the buffer.
pub struct Read<T>(PhantomData<T>);

impl<T: FromBytes, N: Nic> Outcome<N> for Read<T> {
    type Output = Result<BufMut<T, N>, BufError<BufMut<T, N>>>;
    #[inline(always)]
    fn done(result: TakenResult, leases: Leases<N>) -> Self::Output {
        let buf = leases.into_buf_mut().expect("a READ holds its buffer");
        match result {
            Ok(_) => Ok(buf),
            Err(error) => Err(BufError {
                error: Error::Completion(error),
                buf,
            }),
        }
    }
    fn failed(error: Error, back: Leases<N>) -> Self::Output {
        let buf = back
            .into_buf_mut()
            .expect("a refused READ hands its buffer back");
        Err(BufError { error, buf })
    }
}

/// What a successful receive reports (M allows it nothing else).
#[inline(always)]
pub(crate) fn received(done: Completed) -> Received {
    match done {
        Completed::Received(received) => received,
        other => unreachable!("a receive completed as {other:?}"),
    }
}

/// A receive into a `BufMut<T>`: yields what it got (a message or the
/// notification of a peer's WRITE) and the buffer, or the error with the
/// buffer.
pub struct Recv<T>(PhantomData<T>);

impl<T: FromBytes, N: Nic> Outcome<N> for Recv<T> {
    type Output = Result<(Received, BufMut<T, N>), BufError<BufMut<T, N>>>;
    #[inline(always)]
    fn done(result: TakenResult, leases: Leases<N>) -> Self::Output {
        let buf = leases.into_buf_mut().expect("a receive holds its buffer");
        match result {
            Ok(done) => Ok((received(done), buf)),
            Err(error) => Err(BufError {
                error: Error::Completion(error),
                buf,
            }),
        }
    }
    fn failed(error: Error, back: Leases<N>) -> Self::Output {
        let buf = back
            .into_buf_mut()
            .expect("a refused receive hands its buffer back");
        Err(BufError { error, buf })
    }
}

/// The future of one request.
///
/// The request is posted when the operation is called, or, if the queue is
/// full (or earlier work waits), as soon as there is room, in order, by
/// whichever future of the QP is polled. Polling takes its result if it completed and otherwise
/// polls the QP's CQ (no background task is needed), completing the requests of every
/// QP on it, and, until its own request completes, registers the
/// task's waker in the request's slot and asks to be polled again (the CQ
/// is polled, not interrupt driven).
///
/// Dropping it does not cancel a posted request: its buffers stay in the
/// slot until it completes. Work still waiting for room is dropped with it.
#[must_use = "an operation is posted when created (or when there is room); await it for its outcome"]
pub struct Op<'q, Q: QueueAccess, K: Outcome<Q::N>> {
    q: &'q Q,
    side: Queue,
    state: OpState<Q::N>,
    _k: PhantomData<fn() -> K>,
}

/// Where a request is.
pub(crate) enum OpState<N: Nic> {
    /// Posted with this sequence number.
    Posted(Seq),
    /// Waiting for room with this ticket.
    Queued(u64),
    /// Not posted.
    Failed(Box<(Error, Leases<N>)>),
    /// Output taken.
    Done,
}

impl<N: Nic> OpState<N> {
    /// Not posted: why, and its buffers.
    #[cold]
    #[inline(never)]
    pub(crate) fn failed(error: Error, back: Leases<N>) -> Self {
        OpState::Failed(Box::new((error, back)))
    }

    /// What posting work gave.
    pub(crate) fn from_posted(posted: Posted<N>) -> Self {
        match posted {
            Ok(seq) => OpState::Posted(seq),
            Err((error, back)) => OpState::failed(error, back),
        }
    }
}

impl<'q, Q: QueueAccess, K: Outcome<Q::N>> Op<'q, Q, K> {
    #[inline(always)]
    pub(crate) fn new(q: &'q Q, side: Queue, state: OpState<Q::N>) -> Self {
        Op {
            q,
            side,
            state,
            _k: PhantomData,
        }
    }

    /// Posts a send request of one WQE now if it fits and nothing waits before it, else queues it.
    #[inline(always)]
    pub(crate) fn post(q: &'q Q, op: WrOp, part: Part<Q::N>) -> Self {
        Self::new(q, Queue::Send, q.with_state(|s| s.post_send(op, part)))
    }

    /// Posts a receive now if it fits and nothing waits before it, else queues it.
    #[inline(always)]
    pub(crate) fn post_recv(q: &'q Q, part: Part<Q::N>) -> Self {
        Self::new(q, Queue::Recv, q.with_state(|s| s.post_recv(part)))
    }

    /// Posts send work of any kind now if it fits and nothing waits before it, else queues it.
    pub(crate) fn post_work(q: &'q Q, work: Work<Q::N>) -> Self {
        Self::new(q, Queue::Send, q.with_state(|s| s.post_work(work)))
    }

    /// The request's sequence number in its queue, once posted.
    pub fn seq(&self) -> Option<Seq> {
        match self.state {
            OpState::Posted(seq) => Some(seq),
            _ => None,
        }
    }

    /// A poll of a future that is not simply posted.
    #[cold]
    #[inline(never)]
    fn poll_other(&mut self, cx: &Context<'_>) -> Poll<K::Output> {
        let side = self.side;
        match core::mem::replace(&mut self.state, OpState::Done) {
            OpState::Failed(failed) => {
                let (error, back) = *failed;
                Poll::Ready(K::failed(error, back))
            }
            OpState::Queued(ticket) => {
                let posted = self.q.with_state(|s| {
                    s.drain();
                    s.take_ticket(side, ticket)
                });
                match posted {
                    Some(Ok(seq)) => {
                        self.state = OpState::Posted(seq);
                        self.poll_posted(seq, cx)
                    }
                    Some(Err((error, back))) => Poll::Ready(K::failed(error, back)),
                    None => {
                        self.state = OpState::Queued(ticket);
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
            OpState::Posted(_) | OpState::Done => panic!("operation polled after it finished"),
        }
    }

    /// A poll of a posted request: its result if it is there, else after a poll of the CQ; else
    /// the waker is registered and the future asks to be polled again.
    #[inline(always)]
    fn poll_posted(&mut self, seq: Seq, cx: &Context<'_>) -> Poll<K::Output> {
        let side = self.side;
        let taken = self.q.with_state(|s| {
            s.drain();
            if let Some(done) = s.take_request(side, seq) {
                return Some(done);
            }
            // One poll of the CQ retires the requests of all its QPs.
            s.engine().progress();
            let done = s.take_request(side, seq);
            if done.is_none() {
                s.set_waker(side, seq, cx.waker());
            }
            done
        });
        match taken {
            Some((result, leases)) => {
                self.state = OpState::Done;
                Poll::Ready(K::done(result, leases))
            }
            None => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl<Q: QueueAccess, K: Outcome<Q::N>> Unpin for Op<'_, Q, K> {}

impl<Q: QueueAccess, K: Outcome<Q::N>> Future for Op<'_, Q, K> {
    type Output = K::Output;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<K::Output> {
        let this = self.get_mut();
        match this.state {
            OpState::Posted(seq) => this.poll_posted(seq, cx),
            _ => this.poll_other(cx),
        }
    }
}

impl<Q: QueueAccess, K: Outcome<Q::N>> Drop for Op<'_, Q, K> {
    #[inline]
    fn drop(&mut self) {
        match self.state {
            OpState::Done => {}
            _ => self.give_up(),
        }
    }
}

impl<Q: QueueAccess, K: Outcome<Q::N>> Op<'_, Q, K> {
    /// The future is dropped before its request finished.
    #[cold]
    #[inline(never)]
    fn give_up(&mut self) {
        let side = self.side;
        match self.state {
            OpState::Posted(seq) => self.q.with_state(|s| s.detach(side, seq)),
            OpState::Queued(ticket) => self.q.with_state(|s| {
                if let Some(seq) = s.cancel(side, ticket) {
                    s.detach(side, seq);
                }
            }),
            OpState::Failed(_) | OpState::Done => {}
        }
    }
}

/// The future of [`AsyncQp::flush`](crate::AsyncQp::flush): the send queue
/// is empty (nothing waits for room, every request posted has completed).
#[must_use = "futures do nothing unless awaited"]
pub struct Flush<'q, Q: QueueAccess> {
    pub(crate) q: &'q Q,
}

impl<Q: QueueAccess> Future for Flush<'_, Q> {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let done = self.q.with_state(|s| {
            s.drain();
            let idle = s.sends_idle();
            let qp = s.key();
            let w = s.engine();
            w.commit(qp)?;
            w.progress();
            let ring = w.send_ring(qp);
            Ok::<bool, Error>(idle && ring.retired() >= ring.posted())
        })?;
        if done {
            Poll::Ready(Ok(()))
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
