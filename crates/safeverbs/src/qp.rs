//! Connected queue pairs with the asynchronous API: [`LocalQp`] for one
//! thread, [`SharedQp`] behind a lock.

use core::cell::RefCell;
use std::sync::Mutex;

use safeverbs_core::Wc;
use safeverbs_core::qp::{MAX_SGES, QueuePair};
use safeverbs_core::{
    Buf, BufMut, Endpoint, Error, Leases, Nic, Part, Pd, Reliable, RemoteBuf, Result, Seq,
    Transport, WrOp,
};
use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::Verbs;
use crate::backlog::{self, Backlog, Posted, Work};
use crate::batch::Batch;
use crate::op::{Flush, Op, Read, Recv, Sent, Side};
use crate::receiver::Receiver;
use crate::sink::WriteSink;

mod sealed {
    pub trait Sealed {}
}

/// What is behind a QP: the synchronous queue pair and the work waiting
/// for room in each of its queues.
pub struct QpState<T: Transport, N: Nic> {
    qp: QueuePair<T, N>,
    sends: Backlog<N>,
    recvs: Backlog<N>,
    /// Results moved out of the send ring to free their slots (see
    /// [`QpState::unclog`]).
    sends_done: Vec<(Seq, Wc, Leases<N>)>,
    /// Results moved out of the receive ring.
    recvs_done: Vec<(Seq, Wc, Leases<N>)>,
}

impl<T: Transport, N: Nic> QpState<T, N> {
    fn new(qp: QueuePair<T, N>) -> Self {
        QpState {
            qp,
            sends: Backlog::new(),
            recvs: Backlog::new(),
            sends_done: Vec::new(),
            recvs_done: Vec::new(),
        }
    }

    /// The synchronous queue pair. Work posted on it directly goes ahead
    /// of work the futures queued while the queue was full.
    pub fn qp(&mut self) -> &mut QueuePair<T, N> {
        &mut self.qp
    }

    /// Posts waiting work while it fits (polling the CQs first, and ringing
    /// the doorbell for what is written, so that room can appear).
    pub(crate) fn drain(&mut self) {
        self.unclog();
        if !self.sends.is_idle() {
            let _ = self.qp.commit();
            self.qp.poll_send();
            self.sends.drain(&mut self.qp);
        }
        if !self.recvs.is_idle() {
            self.qp.poll_recv();
            self.recvs.drain(&mut self.qp);
        }
    }

    /// Frees slots held by results nobody has taken yet when no slot is
    /// free otherwise: the oldest such results move to a side list, where
    /// their futures find them. So a ring never clogs, whatever order the
    /// futures are awaited in.
    pub(crate) fn unclog(&mut self) {
        while self.qp.send_ring().is_clogged() {
            let seq = self.qp.send_ring().head();
            match self.qp.take_send(seq) {
                Some((wc, leases)) => self.sends_done.push((seq, wc, leases)),
                None => break,
            }
        }
        while self.qp.recv_ring().is_clogged() {
            let seq = self.qp.recv_ring().head();
            match self.qp.take_recv(seq) {
                Some((wc, leases)) => self.recvs_done.push((seq, wc, leases)),
                None => break,
            }
        }
    }

    /// Takes the outcome and buffers of retired request `seq`, from its
    /// slot or the side list.
    pub(crate) fn take_request(&mut self, side: Side, seq: Seq) -> Option<(Wc, Leases<N>)> {
        let (taken, done) = match side {
            Side::Send => (self.qp.take_send(seq), &mut self.sends_done),
            Side::Recv => (self.qp.take_recv(seq), &mut self.recvs_done),
        };
        if taken.is_some() {
            return taken;
        }
        let i = done.iter().position(|d| d.0 == seq)?;
        let (_, wc, leases) = done.swap_remove(i);
        Some((wc, leases))
    }

    /// Whether no send-queue work waits for room.
    pub(crate) fn sends_idle(&self) -> bool {
        self.sends.is_idle()
    }

    /// Posts `work` now if nothing waits before it and it fits; else queues
    /// it and returns its ticket.
    pub(crate) fn submit(&mut self, side: Side, work: Work<N>) -> Result<Posted<N>, u64> {
        self.unclog();
        let backlog = match side {
            Side::Send => &mut self.sends,
            Side::Recv => &mut self.recvs,
        };
        if backlog.is_idle() && backlog::fits(&self.qp, &work) {
            Ok(backlog::post(&mut self.qp, work))
        } else {
            Err(backlog.push(work))
        }
    }

    /// The posting result of queued work, once posted.
    pub(crate) fn take_ticket(&mut self, side: Side, ticket: u64) -> Option<Posted<N>> {
        match side {
            Side::Send => self.sends.take(ticket),
            Side::Recv => self.recvs.take(ticket),
        }
    }

    /// Drops queued work whose future is gone; a request already posted
    /// for it is returned.
    pub(crate) fn cancel(&mut self, side: Side, ticket: u64) -> Option<Seq> {
        match side {
            Side::Send => self.sends.cancel(ticket),
            Side::Recv => self.recvs.cancel(ticket),
        }
    }

    /// Gives up a posted request (its buffers are released when it
    /// retires).
    pub(crate) fn detach(&mut self, side: Side, seq: Seq) {
        let done = match side {
            Side::Send => &mut self.sends_done,
            Side::Recv => &mut self.recvs_done,
        };
        if let Some(i) = done.iter().position(|d| d.0 == seq) {
            done.swap_remove(i);
            return;
        }
        match side {
            Side::Send => self.qp.detach_send(seq),
            Side::Recv => self.qp.detach_recv(seq),
        }
    }
}

/// Access to what is behind a QP: a `RefCell` borrow for [`LocalQp`], a
/// lock for [`SharedQp`]. The futures, sinks and streams of this crate are
/// generic over it.
pub trait QueueAccess: sealed::Sealed {
    /// The transport.
    type T: Transport;
    /// The NIC.
    type N: Nic;

    /// Runs `f` on the QP's state.
    ///
    /// # Panics
    ///
    /// For a [`LocalQp`], if called again from inside `f` (the `RefCell`
    /// is borrowed).
    fn with_state<R>(&self, f: impl FnOnce(&mut QpState<Self::T, Self::N>) -> R) -> R;

    /// Runs `f` on the synchronous queue pair.
    fn with_qp<R>(&self, f: impl FnOnce(&mut QueuePair<Self::T, Self::N>) -> R) -> R {
        self.with_state(|s| f(s.qp()))
    }
}

/// A connected QP for one thread at a time: `Send` but not `Sync`, its
/// ring a plain array behind a `RefCell` (no atomics, no locks), created on
/// a thread domain where the device has them (so the driver takes no locks
/// either).
///
/// Every operation posts at once (or, while the queue is full, as soon as
/// there is room, in order) and returns a future of its outcome; awaiting
/// it polls the QP's CQ. Dropping a future never cancels a posted work
/// request: the buffers stay with the QP until it completes. Dropping the
/// QP destroys the hardware QP before releasing any buffer still in flight.
pub struct LocalQp<T: Transport, N: Nic = Verbs> {
    state: RefCell<QpState<T, N>>,
    endpoint: Endpoint,
}

/// A connected QP shared between threads: a [`LocalQp`]'s state behind a
/// `Mutex`, which is the only lock taken (the QP is still created on a
/// thread domain where possible, so the driver takes none).
pub struct SharedQp<T: Transport, N: Nic = Verbs> {
    state: Mutex<QpState<T, N>>,
    endpoint: Endpoint,
}

impl<T: Transport, N: Nic> sealed::Sealed for LocalQp<T, N> {}
impl<T: Transport, N: Nic> sealed::Sealed for SharedQp<T, N> {}

impl<T: Transport, N: Nic> QueueAccess for LocalQp<T, N> {
    type T = T;
    type N = N;
    fn with_state<R>(&self, f: impl FnOnce(&mut QpState<T, N>) -> R) -> R {
        f(&mut self.state.borrow_mut())
    }
}

impl<T: Transport, N: Nic> QueueAccess for SharedQp<T, N> {
    type T = T;
    type N = N;
    fn with_state<R>(&self, f: impl FnOnce(&mut QpState<T, N>) -> R) -> R {
        f(&mut self.state.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

impl<T: Transport, N: Nic> LocalQp<T, N> {
    pub(crate) fn new(qp: QueuePair<T, N>, endpoint: Endpoint) -> Self {
        LocalQp {
            state: RefCell::new(QpState::new(qp)),
            endpoint,
        }
    }

    /// Makes it shareable between threads (behind a lock).
    pub fn into_shared(self) -> SharedQp<T, N> {
        SharedQp {
            state: Mutex::new(self.state.into_inner()),
            endpoint: self.endpoint,
        }
    }

    /// The synchronous queue pair under it.
    pub fn into_inner(self) -> QueuePair<T, N> {
        self.state.into_inner().qp
    }
}

impl<T: Transport, N: Nic> SharedQp<T, N> {
    /// The synchronous queue pair under it.
    pub fn into_inner(self) -> QueuePair<T, N> {
        self.state
            .into_inner()
            .unwrap_or_else(|e| e.into_inner())
            .qp
    }
}

/// Runs a batch closure: straight into the send queue when it has room for
/// a whole batch and nothing waits, else collected and queued.
fn post_batch<'q, Q: QueueAccess>(
    q: &'q Q,
    f: impl FnOnce(&mut Batch<'_, Q::T, Q::N>),
) -> Op<'q, Q, Sent> {
    enum Outcome<N: Nic> {
        Now(Posted<N>),
        Queued(u64),
    }
    let outcome = q.with_state(|s| {
        s.drain();
        let ring = s.qp.send_ring();
        let room = ring.depth() - ring.in_flight_wqes();
        if s.sends.is_idle() && ring.can_post() && room >= ring.max_wqes() {
            let mut batch = Batch::direct(s.qp.request());
            f(&mut batch);
            Outcome::Now(batch.post())
        } else {
            let mut batch = Batch::collect();
            f(&mut batch);
            match batch.into_ops() {
                Ok(ops) => Outcome::Queued(s.sends.push(Work::Batch(ops))),
                Err(error) => Outcome::Now(Err((error, Leases::default()))),
            }
        }
    });
    match outcome {
        Outcome::Now(result) => Op::from_result(q, Side::Send, result),
        Outcome::Queued(ticket) => Op::queued(q, Side::Send, ticket),
    }
}

fn one<N: Nic>(op: WrOp, part: Part<N>) -> Work<N> {
    Work::One(op, part)
}

macro_rules! qp_api {
    ($ty:ident) => {
        impl<T: Transport, N: Nic> $ty<T, N> {
            /// How the peer reaches this QP (what the handshake exchanged).
            pub fn endpoint(&self) -> Endpoint {
                self.endpoint
            }

            /// The QP number.
            pub fn qp_num(&self) -> u32 {
                self.endpoint.qp_num
            }

            /// The protection domain.
            pub fn pd(&self) -> Pd<N> {
                self.with(|qp| qp.pd().clone())
            }

            /// Whether the QP was created without the driver's locks (on a
            /// thread domain; not every device has them).
            pub fn is_lock_free(&self) -> bool {
                self.with(|qp| qp.is_lock_free())
            }

            /// Runs `f` on the synchronous queue pair under this QP, for
            /// callers who drive their own loop. Work posted there goes
            /// ahead of work the futures queued while the queue was full.
            pub fn with<R>(&self, f: impl FnOnce(&mut QueuePair<T, N>) -> R) -> R {
                self.with_state(|s| f(s.qp()))
            }

            /// SENDs `buf` into the peer's next receive.
            pub fn send<U: IntoBytes + Immutable>(&self, buf: Buf<U, N>) -> Op<'_, Self, Sent> {
                Op::submit(self, Side::Send, one(WrOp::Send, buf.into_part()))
            }

            /// SENDs `buf` with immediate data `imm`.
            pub fn send_imm<U: IntoBytes + Immutable>(
                &self,
                buf: Buf<U, N>,
                imm: u32,
            ) -> Op<'_, Self, Sent> {
                Op::submit(self, Side::Send, one(WrOp::SendImm(imm), buf.into_part()))
            }

            /// SENDs the concatenation of `bufs` as one message (a gather
            /// list of at most the QP's `max_send_sge` buffers).
            pub fn send_gather<U: IntoBytes + Immutable>(
                &self,
                bufs: impl IntoIterator<Item = Buf<U, N>>,
            ) -> Op<'_, Self, Sent> {
                let mut parts = [const { None }; MAX_SGES];
                for (i, buf) in bufs.into_iter().enumerate() {
                    if i == MAX_SGES {
                        let error = Error::TooManySges {
                            got: i + 1,
                            max: MAX_SGES,
                        };
                        return Op::from_result(self, Side::Send, Err((error, Leases::default())));
                    }
                    parts[i] = Some(buf.into_part());
                }
                Op::submit(self, Side::Send, Work::Gather(WrOp::Send, parts))
            }

            /// RDMA WRITEs `buf` to `dst` in the peer's memory.
            pub fn write<U: IntoBytes + Immutable>(
                &self,
                buf: Buf<U, N>,
                dst: RemoteBuf,
            ) -> Op<'_, Self, Sent> {
                Op::submit(self, Side::Send, one(WrOp::Write(dst), buf.into_part()))
            }

            /// RDMA WRITEs `buf` to `dst` with immediate data `imm`, which
            /// also consumes one of the peer's receives.
            pub fn write_imm<U: IntoBytes + Immutable>(
                &self,
                buf: Buf<U, N>,
                dst: RemoteBuf,
                imm: u32,
            ) -> Op<'_, Self, Sent> {
                Op::submit(
                    self,
                    Side::Send,
                    one(WrOp::WriteImm(dst, imm), buf.into_part()),
                )
            }

            /// Posts a receive into `buf`; yields the completion and the
            /// buffer.
            pub fn recv<U: FromBytes + IntoBytes>(
                &self,
                buf: BufMut<U, N>,
            ) -> Op<'_, Self, Recv<U>> {
                Op::submit(self, Side::Recv, Work::Recv(buf.into_part()))
            }

            /// Posts the SENDs and WRITEs `f` adds as one request: one
            /// doorbell, one completion. A batch longer than the QP's
            /// `max_batch` is split into requests of that size (which may
            /// wait for room by polling).
            pub fn batch(&self, f: impl FnOnce(&mut Batch<'_, T, N>)) -> Op<'_, Self, Sent> {
                post_batch(self, f)
            }

            /// A [`futures_sink::Sink`] of RDMA WRITEs: `poll_ready` waits
            /// for room in the ring, `start_send` writes the WQE,
            /// `poll_flush` rings the doorbell and `poll_close` waits until
            /// everything sent has completed. WQEs are signaled only as
            /// often as the ring needs (and the last one at each flush).
            pub fn sink<U: IntoBytes + Immutable>(&self) -> WriteSink<'_, Self, U> {
                WriteSink::new(self)
            }

            /// A [`futures_core::Stream`] of receives into `bufs`: each item
            /// is a completion (successful or not) and its buffer, in the
            /// order the receives complete; [`Receiver::repost`] gives a
            /// buffer back. Buffers that do not fit in the receive queue
            /// wait for room.
            ///
            /// # Panics
            ///
            /// If a buffer is registered in another protection domain.
            pub fn receiver<U: FromBytes + IntoBytes>(
                &self,
                bufs: impl IntoIterator<Item = BufMut<U, N>>,
            ) -> Receiver<'_, Self, U> {
                Receiver::new(self, bufs)
            }

            /// Waits until the send queue is empty: nothing waits for room
            /// and every request posted has completed (the last one must be
            /// signaled, as every future's and every flushed sink's is).
            pub fn flush(&self) -> Flush<'_, Self> {
                Flush { q: self }
            }

            /// Polls both CQs once, retiring what completed (and waking the
            /// futures waiting on it), and posts waiting work that now
            /// fits; returns the number of completions.
            pub fn poll(&self) -> usize {
                self.with_state(|s| {
                    let n = s.qp().poll_send() + s.qp().poll_recv();
                    s.drain();
                    n
                })
            }

            /// Moves the QP to the error state: every request in flight
            /// completes with a flush error, handing its buffers back.
            pub fn force_error(&self) -> Result<()> {
                self.with(|qp| qp.to_error())
            }
        }

        impl<T: Reliable, N: Nic> $ty<T, N> {
            /// RDMA READs `src` from the peer's memory into `buf`; yields
            /// the buffer.
            pub fn read<U: FromBytes + IntoBytes>(
                &self,
                buf: BufMut<U, N>,
                src: RemoteBuf,
            ) -> Op<'_, Self, Read<U>> {
                Op::submit(self, Side::Send, one(WrOp::Read(src), buf.into_part()))
            }

            /// Atomically adds `add` to the 8 bytes at `dst` in the peer's
            /// memory; their old value lands in `old` (one `u64`), which is
            /// yielded.
            pub fn fetch_add(
                &self,
                old: BufMut<u64, N>,
                dst: RemoteBuf,
                add: u64,
            ) -> Op<'_, Self, Read<u64>> {
                Op::submit(
                    self,
                    Side::Send,
                    one(WrOp::FetchAdd { remote: dst, add }, old.into_part()),
                )
            }

            /// Atomically replaces the 8 bytes at `dst` in the peer's memory
            /// with `swap` if they equal `compare`; their old value lands in
            /// `old` (one `u64`), which is yielded.
            pub fn compare_swap(
                &self,
                old: BufMut<u64, N>,
                dst: RemoteBuf,
                compare: u64,
                swap: u64,
            ) -> Op<'_, Self, Read<u64>> {
                let op = WrOp::CompareSwap {
                    remote: dst,
                    compare,
                    swap,
                };
                Op::submit(self, Side::Send, one(op, old.into_part()))
            }
        }
    };
}

qp_api!(LocalQp);
qp_api!(SharedQp);
