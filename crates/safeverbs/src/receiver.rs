//! A stream of receives.

use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::VecDeque;

use futures_core::Stream;
use safeverbs_core::{BufError, BufMut, Error, Seq, Wc};
use zerocopy::{FromBytes, IntoBytes};

use crate::qp::QueueAccess;

/// A [`Stream`] of receives on a QP, from
/// [`LocalQp::receiver`](crate::LocalQp::receiver): each item is a
/// completion and the buffer it filled, in completion order.
///
/// Items with an error status (flush errors after the QP failed) hand their
/// buffers back too; once the QP has failed, [`Receiver::repost`] refuses
/// and the stream ends when every receive is back. Dropping the receiver
/// leaves its receives posted: their buffers come back to the QP and are
/// released when they complete or the QP is destroyed.
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
        q.with_qp(|qp| {
            if qp.recv_ring().failed() {
                return Err(BufError {
                    error: Error::QpFailed,
                    buf,
                });
            }
            match qp.post_recv(buf.into_part(), false) {
                Ok(seq) => {
                    self.posted.push_back(seq);
                    Ok(())
                }
                Err(e) => {
                    let buf = e.buf.into_buf_mut().expect("a receive buffer comes back");
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
    type Item = (Wc, BufMut<U, Q::N>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let q = this.q;
        let next = q.with_qp(|qp| {
            while !this.spare.is_empty() && qp.recv_ring().can_post() && !qp.recv_ring().failed() {
                let buf = this.spare.pop().expect("a spare buffer");
                match qp.post_recv(buf.into_part(), false) {
                    Ok(seq) => this.posted.push_back(seq),
                    Err(e) => {
                        this.spare
                            .push(e.buf.into_buf_mut().expect("a receive buffer comes back"));
                        break;
                    }
                }
            }
            qp.poll_recv();
            match this.posted.front() {
                Some(&seq) => match qp.take_recv(seq) {
                    Some((wc, leases)) => {
                        this.posted.pop_front();
                        let buf = leases.into_buf_mut().expect("a receive holds its buffer");
                        Poll::Ready(Some((wc, buf)))
                    }
                    None => {
                        qp.set_recv_waker(seq, cx.waker());
                        Poll::Pending
                    }
                },
                None if this.spare.is_empty() || qp.recv_ring().failed() => Poll::Ready(None),
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
        self.q.with_qp(|qp| {
            for &seq in posted {
                qp.detach_recv(seq);
            }
        });
    }
}
