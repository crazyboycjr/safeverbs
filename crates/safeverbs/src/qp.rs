//! Connected queue pairs with the asynchronous API: [`LocalQp`] on a
//! worker of one thread ([`LocalWorker`]), [`SharedQp`] behind a lock.

use core::cell::RefCell;
use std::rc::Rc;
use std::sync::Mutex;

use safeverbs_core::worker::MAX_SGES;
use safeverbs_core::{
    Buf, BufMut, Endpoint, Error, Leases, Nic, Part, Pd, QpKey, Reliable, RemoteBuf, Result, Seq,
    Transport, Worker, WrOp,
};
use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::Verbs;
use crate::backlog::{self, Backlog, Posted, Work};
use crate::batch::Batch;
use crate::op::{Flush, Op, Read, Recv, Sent, Side, TakenResult};
use crate::receiver::Receiver;
use crate::sink::WriteSink;
use crate::worker::LocalWorker;

mod sealed {
    pub trait Sealed {}
}

/// The futures' share of a QP: the work waiting for room in each of its
/// queues and the results moved out of its rings.
pub(crate) struct Queues<N: Nic> {
    sends: Backlog<N>,
    recvs: Backlog<N>,
    /// Results moved out of the send ring to free their slots (see
    /// [`QpState::unclog`]).
    sends_done: Vec<(Seq, TakenResult, Leases<N>)>,
    /// Results moved out of the receive ring.
    recvs_done: Vec<(Seq, TakenResult, Leases<N>)>,
}

/// What a worker of the asynchronous layer holds: the synchronous worker
/// and, by the index of each of its QPs, the futures' share of it.
pub(crate) struct State<N: Nic> {
    pub(crate) worker: Worker<(), N>,
    queues: Vec<Option<Queues<N>>>,
}

impl<N: Nic> State<N> {
    pub(crate) fn new(worker: Worker<(), N>) -> Self {
        State {
            worker,
            queues: Vec::new(),
        }
    }

    /// Gives the futures their share of QP `key`, just created on the
    /// worker.
    pub(crate) fn adopt<T: Transport>(&mut self, key: QpKey<T>) {
        let i = key.index();
        if self.queues.len() <= i {
            self.queues.resize_with(i + 1, || None);
        }
        self.queues[i] = Some(Queues {
            sends: Backlog::new(),
            recvs: Backlog::new(),
            sends_done: Vec::new(),
            recvs_done: Vec::new(),
        });
    }

    /// Destroys a QP of the worker, then drops the work and results the
    /// futures left with it.
    pub(crate) fn remove_qp<T: Transport>(&mut self, key: QpKey<T>) {
        // A QP the device does not flush stays in the worker with its
        // buffers (`Error::NotFlushed`): nothing more to do here.
        let _ = self.worker.remove_qp(key);
        if let Some(q) = self.queues.get_mut(key.index()) {
            *q = None;
        }
    }

    /// The state of QP `key`.
    pub(crate) fn qp<T: Transport>(&mut self, key: QpKey<T>) -> QpState<'_, T, N> {
        let q = match self.queues.get_mut(key.index()) {
            Some(Some(q)) => q,
            _ => panic!("no QP {} in this worker", key.index()),
        };
        QpState {
            w: &mut self.worker,
            q,
            key,
        }
    }
}

/// What is behind a QP: the worker it is on (the CQ it shares with the
/// worker's other QPs, and its rings) and the work waiting for room in
/// each of its queues.
pub struct QpState<'a, T: Transport, N: Nic> {
    w: &'a mut Worker<(), N>,
    q: &'a mut Queues<N>,
    key: QpKey<T>,
}

impl<T: Transport, N: Nic> QpState<'_, T, N> {
    /// The synchronous worker the QP is on. Work posted there on the QP
    /// goes ahead of work the futures queued while the queue was full.
    pub fn worker(&mut self) -> &mut Worker<(), N> {
        self.w
    }

    /// The QP's key in its worker.
    pub fn key(&self) -> QpKey<T> {
        self.key
    }

    /// Posts waiting work while it fits (polling the CQ first, and ringing
    /// the doorbell for what is written, so that room can appear).
    pub(crate) fn drain(&mut self) {
        self.unclog();
        if !self.q.sends.is_idle() {
            let _ = self.w.commit(self.key);
            self.w.progress();
            self.q.sends.drain(self.w, self.key);
        }
        if !self.q.recvs.is_idle() {
            self.w.progress();
            self.q.recvs.drain(self.w, self.key);
        }
    }

    /// Frees slots held by results nobody has taken yet when no slot is
    /// free otherwise: the oldest such results move to a side list, where
    /// their futures find them. So a ring never clogs, whatever order the
    /// futures are awaited in.
    pub(crate) fn unclog(&mut self) {
        let k = self.key;
        while self.w.send_ring(k).is_clogged() {
            let seq = self.w.send_ring(k).head();
            match self.w.take_send(k, seq) {
                Some(c) => self.q.sends_done.push((seq, c.result, c.leases)),
                None => break,
            }
        }
        while self.w.recv_ring(k).is_clogged() {
            let seq = self.w.recv_ring(k).head();
            match self.w.take_recv(k, seq) {
                Some(c) => self.q.recvs_done.push((seq, c.result, c.leases)),
                None => break,
            }
        }
    }

    /// Takes the result and buffers of retired request `seq`, from its slot
    /// or the side list.
    pub(crate) fn take_request(
        &mut self,
        side: Side,
        seq: Seq,
    ) -> Option<(TakenResult, Leases<N>)> {
        let (taken, done) = match side {
            Side::Send => (self.w.take_send(self.key, seq), &mut self.q.sends_done),
            Side::Recv => (self.w.take_recv(self.key, seq), &mut self.q.recvs_done),
        };
        if let Some(c) = taken {
            return Some((c.result, c.leases));
        }
        let i = done.iter().position(|d| d.0 == seq)?;
        let (_, result, leases) = done.swap_remove(i);
        Some((result, leases))
    }

    /// Wakes `waker` when request `seq` retires.
    pub(crate) fn set_waker(&mut self, side: Side, seq: Seq, waker: &core::task::Waker) {
        match side {
            Side::Send => self.w.set_send_waker(self.key, seq, waker),
            Side::Recv => self.w.set_recv_waker(self.key, seq, waker),
        }
    }

    /// Whether no send-queue work waits for room.
    pub(crate) fn sends_idle(&self) -> bool {
        self.q.sends.is_idle()
    }

    /// Posts `work` now if nothing waits before it and it fits; else queues
    /// it and returns its ticket.
    pub(crate) fn submit(&mut self, side: Side, work: Work<N>) -> Result<Posted<N>, u64> {
        self.unclog();
        let backlog = match side {
            Side::Send => &mut self.q.sends,
            Side::Recv => &mut self.q.recvs,
        };
        if backlog.is_idle() && backlog::fits(self.w, self.key, &work) {
            Ok(backlog::post(self.w, self.key, work))
        } else {
            Err(backlog.push(work))
        }
    }

    /// The posting result of queued work, once posted.
    pub(crate) fn take_ticket(&mut self, side: Side, ticket: u64) -> Option<Posted<N>> {
        match side {
            Side::Send => self.q.sends.take(ticket),
            Side::Recv => self.q.recvs.take(ticket),
        }
    }

    /// Drops queued work whose future is gone; a request already posted
    /// for it is returned.
    pub(crate) fn cancel(&mut self, side: Side, ticket: u64) -> Option<Seq> {
        match side {
            Side::Send => self.q.sends.cancel(ticket),
            Side::Recv => self.q.recvs.cancel(ticket),
        }
    }

    /// Gives up a posted request (its buffers are released when it
    /// retires).
    pub(crate) fn detach(&mut self, side: Side, seq: Seq) {
        let done = match side {
            Side::Send => &mut self.q.sends_done,
            Side::Recv => &mut self.q.recvs_done,
        };
        if let Some(i) = done.iter().position(|d| d.0 == seq) {
            let _ = done.swap_remove(i);
            return;
        }
        let _ = match side {
            Side::Send => self.w.detach_send(self.key, seq),
            Side::Recv => self.w.detach_recv(self.key, seq),
        };
    }
}

/// Access to what is behind a QP: a `RefCell` borrow of its worker for
/// [`LocalQp`], a lock for [`SharedQp`]. The futures, sinks and streams of
/// this crate are generic over it.
pub trait QueueAccess: sealed::Sealed {
    /// The transport.
    type T: Transport;
    /// The NIC.
    type N: Nic;

    /// Runs `f` on the QP's state.
    ///
    /// # Panics
    ///
    /// For a [`LocalQp`], if called from inside `f` for a QP of the same
    /// worker (the worker's `RefCell` is borrowed).
    fn with_state<R>(&self, f: impl FnOnce(&mut QpState<'_, Self::T, Self::N>) -> R) -> R;

    /// Runs `f` on the synchronous worker and the QP's key.
    fn with_qp<R>(&self, f: impl FnOnce(&mut Worker<(), Self::N>, QpKey<Self::T>) -> R) -> R {
        self.with_state(|s| {
            let key = s.key;
            f(s.w, key)
        })
    }
}

/// A QP of a worker, removed from it when dropped (drained, destroyed,
/// then its buffers released).
pub(crate) struct QpGuard<T: Transport, N: Nic> {
    /// The worker; `None` once handed on.
    state: Option<Rc<RefCell<State<N>>>>,
    pub(crate) key: QpKey<T>,
}

impl<T: Transport, N: Nic> QpGuard<T, N> {
    pub(crate) fn new(state: Rc<RefCell<State<N>>>, key: QpKey<T>) -> Self {
        QpGuard {
            state: Some(state),
            key,
        }
    }

    pub(crate) fn state(&self) -> &Rc<RefCell<State<N>>> {
        self.state.as_ref().expect("a QP's worker")
    }
}

impl<T: Transport, N: Nic> Drop for QpGuard<T, N> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            state.borrow_mut().remove_qp(self.key);
        }
    }
}

/// A connected QP on a worker of one thread ([`LocalWorker`]): neither
/// `Send` nor `Sync`. The QPs of a worker share its CQ, and its ring and
/// the CQ are plain data behind one `RefCell` (no atomics, no locks); the
/// QP is created on the worker's thread domain where the device has them
/// (so the driver takes no locks either).
///
/// Every operation posts at once (or, while the queue is full, as soon as
/// there is room, in order) and returns a future of its outcome; awaiting
/// it polls the worker's CQ, which retires the requests of every QP of the
/// worker and wakes their futures. Dropping a future never cancels a posted
/// work request: the buffers stay with the QP until it completes. Dropping
/// the QP drains it (moves it to the error state and waits until the NIC
/// has flushed every work request left) before destroying the hardware QP
/// and releasing the buffers; see
/// [`Worker::remove_qp`](safeverbs_core::Worker::remove_qp).
pub struct LocalQp<T: Transport, N: Nic = Verbs> {
    guard: QpGuard<T, N>,
    endpoint: Endpoint,
}

/// A connected QP shared between threads: a worker of its own, with this
/// one QP, behind a `Mutex`, which is the only lock taken (the QP is still
/// created on a thread domain where possible, so the driver takes none).
///
/// The trade-off: every thread that uses it takes the lock, and its CQ
/// serves this QP alone. Threads that each drive QPs of their own do better
/// with a [`LocalWorker`] each.
pub struct SharedQp<T: Transport, N: Nic = Verbs> {
    state: Mutex<State<N>>,
    key: QpKey<T>,
    endpoint: Endpoint,
}

impl<T: Transport, N: Nic> sealed::Sealed for LocalQp<T, N> {}
impl<T: Transport, N: Nic> sealed::Sealed for SharedQp<T, N> {}

impl<T: Transport, N: Nic> QueueAccess for LocalQp<T, N> {
    type T = T;
    type N = N;
    fn with_state<R>(&self, f: impl FnOnce(&mut QpState<'_, T, N>) -> R) -> R {
        let mut state = self.guard.state().borrow_mut();
        f(&mut state.qp(self.guard.key))
    }
}

impl<T: Transport, N: Nic> QueueAccess for SharedQp<T, N> {
    type T = T;
    type N = N;
    fn with_state<R>(&self, f: impl FnOnce(&mut QpState<'_, T, N>) -> R) -> R {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut state.qp(self.key))
    }
}

impl<T: Transport, N: Nic> LocalQp<T, N> {
    pub(crate) fn new(guard: QpGuard<T, N>, endpoint: Endpoint) -> Self {
        LocalQp { guard, endpoint }
    }

    /// The worker the QP is on: build more QPs on it with
    /// [`QueuePairBuilder::build_on`](crate::QueuePairBuilder::build_on).
    pub fn worker(&self) -> LocalWorker<N> {
        LocalWorker::from_state(self.guard.state().clone())
    }

    /// The QP's key in its worker.
    pub fn key(&self) -> QpKey<T> {
        self.guard.key
    }

    /// Makes it shareable between threads (behind a lock). Only a QP that
    /// is alone on its worker, with no other handle to the worker, can be
    /// (as one built with [`QueuePairBuilder::build`](crate::QueuePairBuilder::build));
    /// else it is handed back.
    pub fn into_shared(mut self) -> Result<SharedQp<T, N>, LocalQp<T, N>> {
        let key = self.guard.key;
        let state = self.guard.state.take().expect("a QP's worker");
        match Rc::try_unwrap(state) {
            Ok(state) => Ok(SharedQp {
                state: Mutex::new(state.into_inner()),
                key,
                endpoint: self.endpoint,
            }),
            Err(state) => {
                self.guard.state = Some(state);
                Err(self)
            }
        }
    }
}

impl<T: Transport, N: Nic> SharedQp<T, N> {
    /// The QP's key in its worker.
    pub fn key(&self) -> QpKey<T> {
        self.key
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
        let ring = s.w.send_ring(s.key);
        let room = ring.depth() - ring.in_flight_wqes();
        if s.q.sends.is_idle() && ring.can_post() && room >= ring.max_wqes() {
            let mut batch = Batch::direct(s.w.request(s.key));
            f(&mut batch);
            Outcome::Now(batch.post())
        } else {
            let mut batch = Batch::collect();
            f(&mut batch);
            match batch.into_ops() {
                Ok(ops) => Outcome::Queued(s.q.sends.push(Work::Batch(ops))),
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
                self.with(|w, _| w.pd().clone())
            }

            /// Whether the QP was created without the driver's locks (on a
            /// thread domain; not every device has them).
            pub fn is_lock_free(&self) -> bool {
                self.with(|w, qp| w.qp_is_lock_free(qp))
            }

            /// Runs `f` on the synchronous worker under this QP, with the
            /// QP's key, for callers who drive their own loop. Work posted
            /// there goes ahead of work the futures queued while the queue
            /// was full. On a worker the futures drive, take results by
            /// sequence number ([`Worker::progress`],
            /// [`Worker::take_send`]), not with [`Worker::poll`], which would
            /// hand out the futures' results too.
            pub fn with<R>(&self, f: impl FnOnce(&mut Worker<(), N>, QpKey<T>) -> R) -> R {
                self.with_qp(f)
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

            /// Posts a receive into `buf`; yields what it got (a message or the
            /// notification of a peer's WRITE) and the buffer.
            pub fn recv<U: FromBytes + IntoBytes>(
                &self,
                buf: BufMut<U, N>,
            ) -> Op<'_, Self, Recv<U>> {
                Op::submit(self, Side::Recv, Work::Recv(buf.into_part()))
            }

            /// Posts the SENDs and WRITEs `f` adds as one request: one
            /// doorbell, one completion. A batch longer than the QP's
            /// `max_batch` is split into requests of that size (which may
            /// wait for room by polling). `f` runs with the QP's worker
            /// borrowed: it must not use another QP of the same worker.
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
            /// is what the receive got, or why not, and its buffer, in the
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

            /// Polls the worker's CQ once, retiring what completed on any
            /// of its QPs (and waking the futures waiting on it), and posts
            /// this QP's waiting work that now fits; returns the number of
            /// completions.
            pub fn poll(&self) -> usize {
                self.with_state(|s| {
                    let n = s.w.progress();
                    s.drain();
                    n
                })
            }

            /// Moves the QP to the error state: every request in flight
            /// completes with a flush error, handing its buffers back.
            pub fn force_error(&self) -> Result<()> {
                self.with(|w, qp| w.to_error(qp))
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
