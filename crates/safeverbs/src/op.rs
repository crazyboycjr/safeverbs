//! The futures of single operations: each holds its queue and the
//! request's sequence number (or, while the queue is full, its place in
//! the backlog); the request's buffers and waker live in the QP's ring.

use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll};

use safeverbs_core::{BufError, BufMut, Error, Leases, Nic, Seq, Wc};
use zerocopy::FromBytes;

use crate::backlog::{Posted, Work};
use crate::qp::{QpState, QueueAccess};

/// Which queue of the QP a request is on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Side {
    Send,
    Recv,
}

/// What a finished operation yields, made from its completion and the
/// buffers it gets back.
pub trait Outcome<N: Nic> {
    /// The future's output.
    type Output;
    /// The request retired with `wc` (success or error).
    fn done(wc: Wc, leases: Leases<N>) -> Self::Output;
    /// The request could not be posted; `back` holds its buffers.
    fn failed(error: Error, back: Leases<N>) -> Self::Output;
}

/// SEND and RDMA WRITE (and batches of them): the buffers were frozen, the
/// caller keeps its own copies; yields the completion.
pub struct Sent;

impl<N: Nic> Outcome<N> for Sent {
    type Output = Result<Wc, Error>;
    fn done(wc: Wc, _leases: Leases<N>) -> Self::Output {
        if wc.is_ok() {
            Ok(wc)
        } else {
            Err(Error::Completion(wc))
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
    fn done(wc: Wc, leases: Leases<N>) -> Self::Output {
        let buf = leases.into_buf_mut().expect("a READ holds its buffer");
        if wc.is_ok() {
            Ok(buf)
        } else {
            Err(BufError {
                error: Error::Completion(wc),
                buf,
            })
        }
    }
    fn failed(error: Error, back: Leases<N>) -> Self::Output {
        let buf = back
            .into_buf_mut()
            .expect("a refused READ hands its buffer back");
        Err(BufError { error, buf })
    }
}

/// A receive into a `BufMut<T>`: yields the completion and the buffer, or
/// the error with the buffer.
pub struct Recv<T>(PhantomData<T>);

impl<T: FromBytes, N: Nic> Outcome<N> for Recv<T> {
    type Output = Result<(Wc, BufMut<T, N>), BufError<BufMut<T, N>>>;
    fn done(wc: Wc, leases: Leases<N>) -> Self::Output {
        let buf = leases.into_buf_mut().expect("a receive holds its buffer");
        if wc.is_ok() {
            Ok((wc, buf))
        } else {
            Err(BufError {
                error: Error::Completion(wc),
                buf,
            })
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
/// whichever future of the QP is polled. Polling polls the QP's CQ (no
/// background task is needed) and, until the request retires, registers the
/// task's waker in the request's slot and asks to be polled again (the CQ
/// is polled, not interrupt driven).
///
/// Dropping it does not cancel a posted request: its buffers stay in the
/// slot until it retires. Work still waiting for room is dropped with it.
#[must_use = "an operation is posted when created (or when there is room); await it for its outcome"]
pub struct Op<'q, Q: QueueAccess, K: Outcome<Q::N>> {
    q: &'q Q,
    side: Side,
    state: State<Q::N>,
    _k: PhantomData<fn() -> K>,
}

enum State<N: Nic> {
    /// Waiting for room with this ticket.
    Queued(u64),
    /// Posted with this sequence number.
    Posted(Seq),
    /// Not posted.
    Failed(Error, Leases<N>),
    /// Output taken.
    Done,
}

impl<'q, Q: QueueAccess, K: Outcome<Q::N>> Op<'q, Q, K> {
    /// Posts `work` now if it fits and nothing waits before it, else queues
    /// it.
    pub(crate) fn submit(q: &'q Q, side: Side, work: Work<Q::N>) -> Self {
        let state = q.with_state(|s| {
            s.drain();
            match s.submit(side, work) {
                Ok(Ok(seq)) => State::Posted(seq),
                Ok(Err((error, back))) => State::Failed(error, back),
                Err(ticket) => State::Queued(ticket),
            }
        });
        Op {
            q,
            side,
            state,
            _k: PhantomData,
        }
    }

    pub(crate) fn from_result(q: &'q Q, side: Side, result: Posted<Q::N>) -> Self {
        let state = match result {
            Ok(seq) => State::Posted(seq),
            Err((error, back)) => State::Failed(error, back),
        };
        Op {
            q,
            side,
            state,
            _k: PhantomData,
        }
    }

    pub(crate) fn queued(q: &'q Q, side: Side, ticket: u64) -> Self {
        Op {
            q,
            side,
            state: State::Queued(ticket),
            _k: PhantomData,
        }
    }

    /// The request's sequence number in its queue, once posted.
    pub fn seq(&self) -> Option<Seq> {
        match self.state {
            State::Posted(seq) => Some(seq),
            _ => None,
        }
    }
}

impl<Q: QueueAccess, K: Outcome<Q::N>> Unpin for Op<'_, Q, K> {}

/// One poll of a request: its outcome if it retired, else `None` with the
/// waker registered.
fn poll_request<T: safeverbs_core::Transport, N: Nic>(
    s: &mut QpState<T, N>,
    side: Side,
    state: &mut State<N>,
    cx: &Context<'_>,
) -> Option<Result<(Wc, Leases<N>), (Error, Leases<N>)>> {
    s.drain();
    if let State::Queued(ticket) = *state {
        match s.take(side, ticket) {
            Some(Ok(seq)) => *state = State::Posted(seq),
            Some(Err(failed)) => return Some(Err(failed)),
            None => return None,
        }
    }
    let State::Posted(seq) = *state else {
        unreachable!("polled in a final state")
    };
    let qp = s.qp();
    match side {
        Side::Send => {
            qp.poll_send();
            let taken = qp.take_send(seq);
            if taken.is_none() {
                qp.set_send_waker(seq, cx.waker());
            }
            taken.map(Ok)
        }
        Side::Recv => {
            qp.poll_recv();
            let taken = qp.take_recv(seq);
            if taken.is_none() {
                qp.set_recv_waker(seq, cx.waker());
            }
            taken.map(Ok)
        }
    }
}

impl<Q: QueueAccess, K: Outcome<Q::N>> Future for Op<'_, Q, K> {
    type Output = K::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<K::Output> {
        let this = self.get_mut();
        let mut state = core::mem::replace(&mut this.state, State::Done);
        match state {
            State::Failed(error, back) => return Poll::Ready(K::failed(error, back)),
            State::Done => panic!("operation polled after it finished"),
            State::Queued(_) | State::Posted(_) => {}
        }
        let side = this.side;
        let outcome = this.q.with_state(|s| poll_request(s, side, &mut state, cx));
        match outcome {
            Some(Ok((wc, leases))) => Poll::Ready(K::done(wc, leases)),
            Some(Err((error, back))) => Poll::Ready(K::failed(error, back)),
            None => {
                this.state = state;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl<Q: QueueAccess, K: Outcome<Q::N>> Drop for Op<'_, Q, K> {
    fn drop(&mut self) {
        let side = self.side;
        match self.state {
            State::Posted(seq) => self.q.with_state(|s| s.detach(side, seq)),
            State::Queued(ticket) => self.q.with_state(|s| {
                if let Some(seq) = s.cancel(side, ticket) {
                    s.detach(side, seq);
                }
            }),
            State::Failed(..) | State::Done => {}
        }
    }
}

/// The future of [`LocalQp::flush`](crate::LocalQp::flush): the send queue
/// is empty (nothing waits for room, every request posted has retired).
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
            let qp = s.qp();
            qp.commit()?;
            qp.poll_send();
            Ok::<bool, Error>(idle && qp.send_ring().retired() >= qp.send_ring().posted())
        })?;
        if done {
            Poll::Ready(Ok(()))
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
