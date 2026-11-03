//! A stream of receives.

use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::VecDeque;

use futures_core::Stream;
use safeverbs_core::{BufError, BufMut, CompletionError, Error, Received, Seq};
use zerocopy::{FromBytes, IntoBytes};

use crate::op::{Side, received};
use crate::qp::QueueAccess;

/// A [`Stream`] of receives on a QP, from
/// [`LocalQp::receiver`](crate::LocalQp::receiver): each item is what a
/// receive got, or why not, and its buffer, in completion order.
///
/// Items with an error (flush errors after the QP failed) hand their buffers
/// back too; once the QP has failed, [`Receiver::repost`] refuses and the
/// stream ends when every receive is back. Dropping the receiver
/// leaves its receives posted: their buffers come back to the QP and are
/// released when they complete (at the latest when the QP is removed,
/// which flushes them first).
pub struct Receiver<'q, Q: QueueAccess, U> {
    q: &'q Q,
    /// Receives posted by this receiver, oldest first.
    posted: VecDeque<Seq>,
    /// Buffers waiting for room in the receive queue.
    spare: Vec<BufMut<U, Q::N>>,
}

impl<'q, Q: QueueAccess, U: FromBytes + IntoBytes> Receiver<'q, Q, U> {
    pub(crate) fn new(q: &'q Q, bufs: impl IntoIterator<Item = BufMut<U, Q::N>>) -> Self {
        let mut rx = Receiver {
            q,
            posted: VecDeque::new(),
            spare: Vec::new(),
        };
        for buf in bufs {
            if let Err(e) = rx.repost(buf) {
                panic!("a buffer for the receiver: {}", e.error);
            }
        }
        rx
    }

    /// Receives posted and not handed out yet.
    pub fn posted(&self) -> usize {
        self.posted.len()
    }

    /// Posts `buf` again (or keeps it until the receive queue has room).
    /// Refused, with the buffer, once the QP has failed or for a buffer of
    /// another protection domain.
    pub fn repost(&mut self, buf: BufMut<U, Q::N>) -> Result<(), BufError<BufMut<U, Q::N>>> {
        let q = self.q;
        q.with_state(|s| {
            s.unclog();
            let qp = s.key();
            let w = s.worker();
            if w.recv_ring(qp).failed() {
                return Err(BufError {
                    error: Error::QpFailed,
                    buf,
                });
            }
            match w.post_recv(qp, buf.into_part(), ()) {
                Ok(seq) => {
                    self.posted.push_back(seq);
                    Ok(())
                }
                Err(e) => {
                    let buf = e
                        .leases
                        .into_buf_mut()
                        .expect("a receive buffer comes back");
                    if e.error == Error::QueueFull {
                        self.spare.push(buf);
                        Ok(())
                    } else {
                        Err(BufError {
                            error: e.error,
                            buf,
                        })
                    }
                }
            }
        })
    }
}

impl<Q: QueueAccess, U> Unpin for Receiver<'_, Q, U> {}

impl<Q: QueueAccess, U: FromBytes + IntoBytes> Stream for Receiver<'_, Q, U> {
    type Item = (Result<Received, CompletionError>, BufMut<U, Q::N>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let q = this.q;
        let next = q.with_state(|s| {
            s.unclog();
            let qp = s.key();
            let w = s.worker();
            while !this.spare.is_empty() && w.recv_ring(qp).can_post() && !w.recv_ring(qp).failed()
            {
                let buf = this.spare.pop().expect("a spare buffer");
                match w.post_recv(qp, buf.into_part(), ()) {
                    Ok(seq) => this.posted.push_back(seq),
                    Err(e) => {
                        this.spare.push(
                            e.leases
                                .into_buf_mut()
                                .expect("a receive buffer comes back"),
                        );
                        break;
                    }
                }
            }
            w.progress();
            let failed = w.recv_ring(qp).failed();
            match this.posted.front() {
                Some(&seq) => match s.take_request(Side::Recv, seq) {
                    Some((result, leases)) => {
                        this.posted.pop_front();
                        let buf = leases.into_buf_mut().expect("a receive holds its buffer");
                        Poll::Ready(Some((result.map(received), buf)))
                    }
                    None => {
                        s.set_waker(Side::Recv, seq, cx.waker());
                        Poll::Pending
                    }
                },
                None if this.spare.is_empty() || failed => Poll::Ready(None),
                None => Poll::Pending,
            }
        });
        if next.is_pending() {
            cx.waker().wake_by_ref();
        }
        next
    }
}

impl<Q: QueueAccess, U> Drop for Receiver<'_, Q, U> {
    fn drop(&mut self) {
        let posted = &self.posted;
        self.q.with_state(|s| {
            for &seq in posted {
                s.detach(Side::Recv, seq);
            }
        });
    }
}
