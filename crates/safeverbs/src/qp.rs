//! Connected queue pairs with the asynchronous API: [`AsyncQp`] on a
//! completion queue of one thread ([`AsyncCq`]), [`SharedQp`] behind a lock.

use core::cell::RefCell;
use std::rc::Rc;
use std::sync::Mutex;

use safeverbs_core::engine::{MAX_SGES, Unposted};
use safeverbs_core::{
    Buf, BufMut, Endpoint, Engine, Error, Leases, Nic, Part, QpKey, Queue, Reliable, RemoteBuf,
    Result, Seq, Transport, WrOp,
};
use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::backlog::{self, Backlog, Posted, Work};
use crate::batch::Batch;
use crate::cq::AsyncCq;
use crate::op::{Flush, Op, OpState, Read, Recv, Sent, TakenResult};
use crate::receiver::Receiver;
use crate::sink::WriteSink;
use crate::{Pd, Verbs};

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

impl<N: Nic> Queues<N> {
    /// What it holds, for [`State::pending`].
    fn count(&self) -> usize {
        self.sends.len() + self.recvs.len() + self.sends_done.len() + self.recvs_done.len()
    }
}

/// What a completion queue of the asynchronous layer holds: the engine and, by the index of each
/// of its QPs, the futures' share of it.
pub(crate) struct State<N: Nic> {
    pub(crate) engine: Engine<(), N>,
    queues: Vec<Option<Queues<N>>>,
    /// What the shares of all its QPs hold. While this is zero, as it mostly is, the data path
    /// need not look at any.
    pending: usize,
}

impl<N: Nic> State<N> {
    pub(crate) fn new(engine: Engine<(), N>) -> Self {
        State {
            engine,
            queues: Vec::new(),
            pending: 0,
        }
    }

    /// Gives the futures their share of QP `key`, just created on the engine.
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

    /// Destroys a QP of the engine, then drops the work and results the
    /// futures left with it.
    pub(crate) fn remove_qp<T: Transport>(&mut self, key: QpKey<T>) {
        // A QP the device does not flush stays in the engine with its
        // buffers (`Error::NotFlushed`): nothing more to do here.
        let _ = self.engine.remove_qp(key);
        if let Some(q) = self.queues.get_mut(key.index())
            && let Some(gone) = q.take()
        {
            self.pending -= gone.count();
        }
    }

    /// The state of QP `key`.
    #[inline(always)]
    pub(crate) fn qp<T: Transport>(&mut self, key: QpKey<T>) -> QpState<'_, T, N> {
        QpState { state: self, key }
    }
}

/// What is behind a QP: the engine it is on (the CQ it shares with the
/// other QPs, and its rings) and the work waiting for room in
/// each of its queues.
pub struct QpState<'a, T: Transport, N: Nic> {
    state: &'a mut State<N>,
    key: QpKey<T>,
}

impl<T: Transport, N: Nic> QpState<'_, T, N> {
    /// The engine the QP is on. Work posted there on the QP goes ahead of work the futures queued
    /// while the queue was full.
    #[inline(always)]
    pub fn engine(&mut self) -> &mut Engine<(), N> {
        &mut self.state.engine
    }

    /// The QP's key in its engine.
    #[inline(always)]
    pub fn key(&self) -> QpKey<T> {
        self.key
    }

    /// Runs `f` on the QP's share of the futures and the engine, and keeps the count of what the
    /// shares hold.
    fn with_queues<R>(&mut self, f: impl FnOnce(&mut Queues<N>, &mut Engine<(), N>) -> R) -> R {
        let state = &mut *self.state;
        let q = match state.queues.get_mut(self.key.index()) {
            Some(Some(q)) => q,
            _ => panic!("no QP {} on this CQ", self.key.index()),
        };
        let before = q.count();
        let r = f(q, &mut state.engine);
        state.pending = state.pending - before + q.count();
        r
    }

    /// Posts waiting work while it fits (polling the CQ first, and ringing
    /// the doorbell for what is written, so that room can appear).
    #[inline(always)]
    pub(crate) fn drain(&mut self) {
        if self.state.pending != 0 {
            self.drain_waiting();
        }
    }

    #[cold]
    #[inline(never)]
    fn drain_waiting(&mut self) {
        self.unclog();
        let key = self.key;
        self.with_queues(|q, w| {
            if !q.sends.is_idle() {
                let _ = w.commit(key);
                w.progress();
                q.sends.drain(w, key);
            }
            if !q.recvs.is_idle() {
                w.progress();
                q.recvs.drain(w, key);
            }
        });
    }

    /// Frees slots held by results nobody has taken yet when no slot is
    /// free otherwise: the oldest such results move to a side list, where
    /// their futures find them. So a ring never clogs, whatever order the
    /// futures are awaited in.
    pub(crate) fn unclog(&mut self) {
        let k = self.key;
        self.with_queues(|q, w| {
            while w.send_ring(k).is_clogged() {
                let seq = w.send_ring(k).head();
                match w.take_send(k, seq) {
                    Some(c) => q.sends_done.push((seq, c.result, c.leases)),
                    None => break,
                }
            }
            while w.recv_ring(k).is_clogged() {
                let seq = w.recv_ring(k).head();
                match w.take_recv(k, seq) {
                    Some(c) => q.recvs_done.push((seq, c.result, c.leases)),
                    None => break,
                }
            }
        });
    }

    /// Takes the result and buffers of retired request `seq`, from its slot
    /// or the side list.
    #[inline(always)]
    pub(crate) fn take_request(
        &mut self,
        side: Queue,
        seq: Seq,
    ) -> Option<(TakenResult, Leases<N>)> {
        if let Some(c) = self.state.engine.take(self.key, side, seq) {
            return Some((c.result, c.leases));
        }
        if self.state.pending != 0 {
            return self.take_moved(side, seq);
        }
        None
    }

    /// Takes the result and buffers of request `seq` from the side list.
    #[cold]
    #[inline(never)]
    fn take_moved(&mut self, side: Queue, seq: Seq) -> Option<(TakenResult, Leases<N>)> {
        self.with_queues(|q, _| {
            let done = match side {
                Queue::Send => &mut q.sends_done,
                Queue::Recv => &mut q.recvs_done,
            };
            let i = done.iter().position(|d| d.0 == seq)?;
            let (_, result, leases) = done.swap_remove(i);
            Some((result, leases))
        })
    }

    /// Whether request `seq`, posted, has completed; a refused one may not say so yet.
    #[inline(always)]
    pub(crate) fn completed(&self, side: Queue, seq: Seq) -> bool {
        let engine = &self.state.engine;
        let ring = match side {
            Queue::Send => engine.send_ring(self.key),
            Queue::Recv => engine.recv_ring(self.key),
        };
        seq < ring.retired()
    }

    /// Wakes `waker` when request `seq` retires.
    pub(crate) fn set_waker(&mut self, side: Queue, seq: Seq, waker: &core::task::Waker) {
        let w = &mut self.state.engine;
        match side {
            Queue::Send => w.set_send_waker(self.key, seq, waker),
            Queue::Recv => w.set_recv_waker(self.key, seq, waker),
        }
    }

    /// Whether no send-queue work waits for room.
    #[inline]
    pub(crate) fn sends_idle(&mut self) -> bool {
        self.state.pending == 0 || self.with_queues(|q, _| q.sends.is_idle())
    }

    /// A send request of one WQE: posted now if nothing waits before it and it fits; else it waits
    /// for room, in order.
    #[inline(always)]
    pub(crate) fn post_send(&mut self, op: WrOp, part: Part<N>) -> OpState<N> {
        if self.state.pending == 0 {
            match self.state.engine.try_post(self.key, op, part, ()) {
                Ok(Ok(seq)) => OpState::Posted(seq),
                Ok(Err(e)) => OpState::failed(e.error, e.leases),
                Err(unposted) => self.send_slow(unposted),
            }
        } else {
            self.send_slow(Unposted {
                op,
                part,
                ctx: Some(()),
            })
        }
    }

    /// [`QpState::post_send`] out of the common: something waits, there is no room, or a check
    /// fails, which the engine's general path says.
    #[cold]
    #[inline(never)]
    fn send_slow(&mut self, unposted: Unposted<(), N>) -> OpState<N> {
        self.drain();
        self.unclog();
        let key = self.key;
        let Unposted { op, part, .. } = unposted;
        let work = Work::One(op, part);
        self.submit(Queue::Send, work, key)
    }

    /// A receive: posted now if nothing waits before it and it fits; else it waits for room, in
    /// order.
    #[inline(always)]
    pub(crate) fn post_recv(&mut self, part: Part<N>) -> OpState<N> {
        if self.state.pending == 0 {
            match self.state.engine.try_post_recv(self.key, part, ()) {
                Ok(Ok(seq)) => OpState::Posted(seq),
                Ok(Err(e)) => OpState::failed(e.error, e.leases),
                Err((part, ())) => self.recv_slow(part),
            }
        } else {
            self.recv_slow(part)
        }
    }

    /// [`QpState::post_recv`] out of the common: something waits or there is no room.
    #[cold]
    #[inline(never)]
    fn recv_slow(&mut self, part: Part<N>) -> OpState<N> {
        self.drain();
        self.unclog();
        let key = self.key;
        self.submit(Queue::Recv, Work::Recv(part), key)
    }

    /// A send request of work of any kind, in its order.
    pub(crate) fn post_work(&mut self, work: Work<N>) -> OpState<N> {
        self.drain();
        self.unclog();
        let key = self.key;
        self.submit(Queue::Send, work, key)
    }

    /// Posts `work` now if nothing waits before it and it fits; else queues it.
    fn submit(&mut self, side: Queue, work: Work<N>, key: QpKey<T>) -> OpState<N> {
        self.with_queues(|q, w| {
            let waiting = match side {
                Queue::Send => &mut q.sends,
                Queue::Recv => &mut q.recvs,
            };
            if waiting.is_idle() && backlog::fits(w, key, &work) {
                OpState::from_posted(backlog::post(w, key, work))
            } else {
                OpState::Queued(waiting.push(work))
            }
        })
    }

    /// The posting result of queued work, once posted.
    pub(crate) fn take_ticket(&mut self, side: Queue, ticket: u64) -> Option<Posted<N>> {
        self.with_queues(|q, _| match side {
            Queue::Send => q.sends.take(ticket),
            Queue::Recv => q.recvs.take(ticket),
        })
    }

    /// Drops queued work whose future is gone; a request already posted
    /// for it is returned.
    pub(crate) fn cancel(&mut self, side: Queue, ticket: u64) -> Option<Seq> {
        self.with_queues(|q, _| match side {
            Queue::Send => q.sends.cancel(ticket),
            Queue::Recv => q.recvs.cancel(ticket),
        })
    }

    /// Gives up a posted request (its buffers are released when it
    /// retires).
    pub(crate) fn detach(&mut self, side: Queue, seq: Seq) {
        let key = self.key;
        if self.state.pending != 0 {
            let moved = self.with_queues(|q, _| {
                let done = match side {
                    Queue::Send => &mut q.sends_done,
                    Queue::Recv => &mut q.recvs_done,
                };
                match done.iter().position(|d| d.0 == seq) {
                    Some(i) => {
                        let _ = done.swap_remove(i);
                        true
                    }
                    None => false,
                }
            });
            if moved {
                return;
            }
        }
        let w = &mut self.state.engine;
        let _ = match side {
            Queue::Send => w.detach_send(key, seq),
            Queue::Recv => w.detach_recv(key, seq),
        };
    }

    /// A batch's closure runs here: straight into the send queue when it has room for a whole
    /// batch and nothing waits, else collected and queued.
    fn post_batch(&mut self, f: impl FnOnce(&mut Batch<'_, T, N>)) -> OpState<N> {
        self.drain();
        let idle = self.sends_idle();
        let key = self.key;
        if idle {
            let w = self.engine();
            let ring = w.send_ring(key);
            let room = ring.depth() - ring.in_flight_wqes();
            if ring.can_post() && room >= ring.max_wqes() {
                let mut batch = Batch::direct(w.request(key));
                f(&mut batch);
                return match batch.post(()) {
                    Ok(seq) => OpState::Posted(seq),
                    Err(e) => OpState::failed(e.error, e.leases),
                };
            }
        }
        let mut batch = Batch::collect();
        f(&mut batch);
        match batch.into_ops() {
            Ok(ops) => self.with_queues(|q, _| OpState::Queued(q.sends.push(Work::Batch(ops)))),
            Err(error) => OpState::failed(error, Leases::default()),
        }
    }
}

/// Access to what is behind a QP: a `RefCell` borrow of its completion queue for
/// [`AsyncQp`], a lock for [`SharedQp`]. The futures, sinks and streams of
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
    /// For an [`AsyncQp`], if called from inside `f` for a QP of the same
    /// CQ (its `RefCell` is borrowed).
    fn with_state<R>(&self, f: impl FnOnce(&mut QpState<'_, Self::T, Self::N>) -> R) -> R;

    /// Runs `f` on the engine and the QP's key.
    #[inline]
    fn with_qp<R>(&self, f: impl FnOnce(&mut Engine<(), Self::N>, QpKey<Self::T>) -> R) -> R {
        self.with_state(|s| {
            let key = s.key;
            f(s.engine(), key)
        })
    }
}

/// A QP of an engine, removed from it when dropped (drained, destroyed,
/// then its buffers released).
pub(crate) struct QpGuard<T: Transport, N: Nic> {
    /// The CQ's state; `None` once handed on.
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

    #[inline(always)]
    pub(crate) fn state(&self) -> &Rc<RefCell<State<N>>> {
        self.state.as_ref().expect("a QP's completion queue")
    }
}

impl<T: Transport, N: Nic> Drop for QpGuard<T, N> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            state.borrow_mut().remove_qp(self.key);
        }
    }
}

/// A connected QP on a completion queue of one thread ([`AsyncCq`]): neither
/// `Send` nor `Sync`. The QPs of a CQ share it, and its rings and
/// the CQ are plain data behind one `RefCell` (no atomics, no locks); the
/// QP is created on the CQ's thread domain where the device has them
/// (so the driver takes no locks either).
///
/// Every operation posts at once (or, while the queue is full, as soon as
/// there is room, in order) and returns a future of its outcome; awaiting
/// it polls the CQ, which completes the requests of every QP on it and wakes
/// their futures. Dropping a future never cancels a posted
/// work request: the buffers stay with the QP until it completes. Dropping
/// the QP drains it (moves it to the error state and waits until the NIC
/// has flushed every work request left) before destroying the hardware QP
/// and releasing the buffers; see
/// [`Engine::remove_qp`](safeverbs_core::Engine::remove_qp).
pub struct AsyncQp<T: Transport, N: Nic = Verbs> {
    guard: QpGuard<T, N>,
    endpoint: Endpoint,
}

/// A connected QP shared between threads: an engine of its own, with this
/// one QP, behind a `Mutex`, which is the only lock taken (the QP is still
/// created on a thread domain where possible, so the driver takes none).
///
/// The trade-off: every thread that uses it takes the lock, and its CQ
/// serves this QP alone. Threads that each drive QPs of their own do better
/// with an [`AsyncCq`] or a [`LocalCq`](crate::LocalCq) each.
pub struct SharedQp<T: Transport, N: Nic = Verbs> {
    state: Mutex<State<N>>,
    key: QpKey<T>,
    endpoint: Endpoint,
}

impl<T: Transport, N: Nic> sealed::Sealed for AsyncQp<T, N> {}
impl<T: Transport, N: Nic> sealed::Sealed for SharedQp<T, N> {}

impl<T: Transport, N: Nic> QueueAccess for AsyncQp<T, N> {
    type T = T;
    type N = N;
    #[inline(always)]
    fn with_state<R>(&self, f: impl FnOnce(&mut QpState<'_, T, N>) -> R) -> R {
        let mut state = self.guard.state().borrow_mut();
        f(&mut state.qp(self.guard.key))
    }
}

impl<T: Transport, N: Nic> QueueAccess for SharedQp<T, N> {
    type T = T;
    type N = N;
    #[inline]
    fn with_state<R>(&self, f: impl FnOnce(&mut QpState<'_, T, N>) -> R) -> R {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut state.qp(self.key))
    }
}

impl<T: Transport, N: Nic> AsyncQp<T, N> {
    pub(crate) fn new(guard: QpGuard<T, N>, endpoint: Endpoint) -> Self {
        AsyncQp { guard, endpoint }
    }

    /// The CQ the QP is on: build more QPs on it with
    /// [`QueuePairBuilder::build_on`](crate::QueuePairBuilder::build_on).
    pub fn cq(&self) -> AsyncCq<N> {
        AsyncCq::from_state(self.guard.state().clone())
    }

    /// The QP's key in the engine under its CQ ([`AsyncCq::as_raw`]).
    pub fn as_raw(&self) -> QpKey<T> {
        self.guard.key
    }

    /// Makes it shareable between threads (behind a lock). Only a QP that
    /// is alone on its CQ, with no other handle to the CQ, can be
    /// (as one built with [`QueuePairBuilder::build`](crate::QueuePairBuilder::build));
    /// else it is handed back.
    pub fn into_shared(mut self) -> Result<SharedQp<T, N>, AsyncQp<T, N>> {
        let key = self.guard.key;
        let state = self.guard.state.take().expect("a QP's completion queue");
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
    /// The QP's key in its engine.
    pub fn key(&self) -> QpKey<T> {
        self.key
    }
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
                self.with(|w, qp| Pd::from_raw(w.pd(qp).clone()))
            }

            /// Whether the QP was created without the driver's locks (on a
            /// thread domain; not every device has them).
            pub fn is_lock_free(&self) -> bool {
                self.with(|w, qp| w.qp_is_lock_free(qp))
            }

            /// Runs `f` on the engine under this QP, with the QP's key, for what the handle does
            /// not offer. Work posted there goes ahead of work the futures queued while the queue
            /// was full. On a CQ the futures drive, take results by sequence number
            /// ([`Engine::progress`], [`Engine::take_send`]), not with [`Engine::poll`], which
            /// would hand out the futures' results too.
            pub fn with<R>(&self, f: impl FnOnce(&mut Engine<(), N>, QpKey<T>) -> R) -> R {
                self.with_qp(f)
            }

            /// SENDs `buf` into the peer's next receive.
            #[inline(always)]
            pub fn send<U: IntoBytes + Immutable>(&self, buf: Buf<U, N>) -> Op<'_, Self, Sent> {
                Op::post(self, WrOp::Send, || buf.into_part())
            }

            /// SENDs `buf` with immediate data `imm`.
            #[inline]
            pub fn send_imm<U: IntoBytes + Immutable>(
                &self,
                buf: Buf<U, N>,
                imm: u32,
            ) -> Op<'_, Self, Sent> {
                Op::post(self, WrOp::SendImm(imm), || buf.into_part())
            }

            /// SENDs the concatenation of `bufs` as one message (a gather
            /// list of at most the QP's `max_send_sge` buffers).
            pub fn send_gather<U: IntoBytes + Immutable>(
                &self,
                bufs: impl IntoIterator<Item = Buf<U, N>>,
            ) -> Op<'_, Self, Sent> {
                let mut parts = Vec::with_capacity(MAX_SGES);
                for (i, buf) in bufs.into_iter().enumerate() {
                    if i == MAX_SGES {
                        let error = Error::TooManySges {
                            got: i + 1,
                            max: MAX_SGES,
                        };
                        return Op::new(self, OpState::failed(error, Leases::default()));
                    }
                    parts.push(buf.into_part());
                }
                Op::post_work(self, Work::Gather(WrOp::Send, parts))
            }

            /// RDMA WRITEs `buf` to `dst` in the peer's memory.
            #[inline]
            pub fn write<U: IntoBytes + Immutable>(
                &self,
                buf: Buf<U, N>,
                dst: RemoteBuf,
            ) -> Op<'_, Self, Sent> {
                Op::post(self, WrOp::Write(dst), || buf.into_part())
            }

            /// RDMA WRITEs `buf` to `dst` with immediate data `imm`, which
            /// also consumes one of the peer's receives.
            pub fn write_imm<U: IntoBytes + Immutable>(
                &self,
                buf: Buf<U, N>,
                dst: RemoteBuf,
                imm: u32,
            ) -> Op<'_, Self, Sent> {
                Op::post(self, WrOp::WriteImm(dst, imm), || buf.into_part())
            }

            /// Posts a receive into `buf`; yields what it got (a message or the
            /// notification of a peer's WRITE) and the buffer.
            #[inline(always)]
            pub fn recv<U: FromBytes + IntoBytes>(
                &self,
                buf: BufMut<U, N>,
            ) -> Op<'_, Self, Recv<U>> {
                Op::post_recv(self, || buf.into_part())
            }

            /// Posts the SENDs and WRITEs `f` adds as one request: one
            /// doorbell, one completion. A batch longer than the QP's
            /// `max_batch` is split into requests of that size (which may
            /// wait for room by polling). `f` runs with the QP's CQ
            /// borrowed: it must not use another QP of the same CQ.
            pub fn batch(&self, f: impl FnOnce(&mut Batch<'_, T, N>)) -> Op<'_, Self, Sent> {
                let state = self.with_state(|s| s.post_batch(f));
                Op::new(self, state)
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

            /// Polls the CQ once, completing what finished on any of its QPs (and waking the
            /// futures waiting on it), and posts this QP's waiting work that now fits; returns the
            /// number of completions.
            pub fn poll(&self) -> usize {
                self.with_state(|s| {
                    let n = s.engine().progress();
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
                Op::post(self, WrOp::Read(src), || buf.into_part())
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
                Op::post(self, WrOp::FetchAdd { remote: dst, add }, || {
                    old.into_part()
                })
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
                Op::post(self, op, || old.into_part())
            }
        }
    };
}

qp_api!(AsyncQp);
qp_api!(SharedQp);
