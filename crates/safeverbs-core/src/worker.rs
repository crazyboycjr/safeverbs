//! A worker: one completion queue and the queue pairs attached to it, the
//! unit of thread affinity, driven synchronously.
//!
//! Everything of a worker that the NIC model M sees (its CQ, its QPs'
//! hardware queues and rings, its share of M) lives in the internal `Hw`,
//! which is verified: every call into the NIC meets M's precondition, the
//! rings stay coupled to M's queues, completions are routed to the WQEs they
//! name, and QPs are destroyed only drained. This module is the unverified
//! shell around it: keys, the event loop's bookkeeping and the drain's
//! timing.
//!
//! The efficient way to use RDMA is one CQ per thread, shared by all of that
//! thread's QPs, and a loop that polls it, dispatches each completion and
//! posts new work. A [`Worker`] is that CQ with its QPs, as one value: `Send`
//! but not `Sync`, so it moves between threads as a whole and is used by one
//! thread at a time. Its CQ is created `IBV_CREATE_CQ_ATTR_SINGLE_THREADED`,
//! and, where the device has thread domains, its QPs are created on one
//! thread domain of the worker's: mlx5 then takes no QP locks and gives the
//! worker's QPs a doorbell of their own, which they share. Elsewhere (rxe)
//! they are plain QPs.
//!
//! # QPs and their keys
//!
//! [`Worker::create_qp`] creates a QP attached to the worker's CQ and returns
//! its [`QpKey`], a typed index: every per-QP method takes one. A QP reserves
//! CQ capacity for every completion it may produce, its send and receive
//! depths, and creating one that does not fit is an error ([`Error::CqFull`]),
//! never a possible overrun: each WQE in flight produces at most one
//! completion, and each QP's rings never let more WQEs be in flight than its
//! queues hold. This is the rule of the NIC model M (`create_qp` reserves
//! the depths on the CQ).
//!
//! # Completions
//!
//! Every completion is routed to its QP and queue by its `wr_id`, which
//! holds the QP's index in the worker (16 bits), the queue (1 bit) and the
//! low 47 bits of the WQE's number in that queue's ring. The ring's number
//! is recovered from the WQEs in flight, which span at most the queue's
//! depth, so the 47 bits never wrap ambiguously. Routing therefore costs a
//! shift and an array index per completion, with no lookup; the queue's bit
//! is needed in any case, since a failed completion's opcode is undefined.
//! The QP's ring then retires its requests in order, as before.
//!
//! There are two ways to drive a worker:
//!
//! - **The event loop** ([`Worker::poll`]): poll the CQ, and get every
//!   request that retired (with an owner) as a [`Completion`]: its QP, queue,
//!   sequence number, outcome and buffers, handed to a callback that may post
//!   new work. Nothing is allocated.
//! - **Owners that take their results** ([`Worker::progress`] and
//!   [`Worker::take_send`]): polling retires requests into their rings and
//!   wakes their wakers; each owner takes its request's result by sequence
//!   number. The futures of `safeverbs` work this way.
//!
//! A worker driven one way should not be driven the other: `poll` hands out
//! the results owners would wait for.
//!
//! # Removing QPs
//!
//! The NIC may read and write the buffers of every WQE posted and not yet
//! retired, and M (`destroy_qp`) allows destroying a QP only when its queues
//! are empty. So a QP is never destroyed with work in flight: removing it
//! ([`Worker::remove_qp`], and dropping the worker, for each of its QPs)
//! first moves it to the error state, where the NIC completes every WQE
//! left with a flush error, then posts a signaled *drain* WQE on its send
//! queue (a SEND of nothing, which the error state flushes too: it retires
//! the unsignaled WQEs that succeeded before the error and would never get
//! a completion of their own) and polls the CQ until both rings have
//! nothing in flight. Only then is the QP destroyed, and only after that
//! are its buffers released. The other QPs' completions polled meanwhile
//! go to their rings as [`Worker::progress`] sends them.
//!
//! A device that never flushes is a fatal error, and no buffer is ever
//! freed while the NIC may access it: if no completion arrives for
//! [`DRAIN_TIMEOUT`], the QP is kept as it is, with its buffers, its index
//! and its share of the CQ, and [`Worker::remove_qp`] fails with
//! [`Error::NotFlushed`]. Dropping the worker tries once more; a QP still
//! not drained then is leaked with its buffers, and so are the CQ and the
//! thread domain it is attached to.

use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::task::Waker;
use std::time::{Duration, Instant};

use crate::buf::{BufError, Leases, Part};
use crate::error::{Error, Result};
use crate::hw::{Hw, Side};
use crate::nic::{ConnectAttr, Endpoint, Nic, QpCaps, WrOp};
use crate::pd::Pd;
use crate::ring::{Ring, Seq};
use crate::transport::Transport;
use crate::verbs::Verbs;
use crate::wc::Wc;

pub use crate::hw::{MAX_QPS, MAX_SGES};

/// How long removing a QP waits for a completion while the device flushes
/// its work, before it gives up: then the QP and its buffers are kept, never
/// freed (see the module documentation).
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

/// Empty polls of the CQ between two looks at the clock while draining.
const DRAIN_SPINS: u32 = 1024;

/// A QP of a worker: its index there, typed by its transport. Cheap to copy.
///
/// A key names a QP of the worker that created it, until the QP is removed;
/// using it with another worker, or after the removal, names another QP or
/// none (and the methods panic). That is a logic error, not a memory-safety
/// one: the worker checks what each WQE may do against the QP it is posted
/// on.
pub struct QpKey<T: Transport> {
    index: u16,
    _t: PhantomData<fn() -> T>,
}

impl<T: Transport> QpKey<T> {
    /// The QP's index in its worker, as [`Completion::qp`] reports it.
    pub fn index(self) -> usize {
        self.index as usize
    }
}

impl<T: Transport> Clone for QpKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Transport> Copy for QpKey<T> {}

impl<T: Transport> PartialEq for QpKey<T> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl<T: Transport> Eq for QpKey<T> {}

impl<T: Transport> core::fmt::Debug for QpKey<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "QpKey({})", self.index)
    }
}

/// One of a QP's two work queues.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Queue {
    /// The send queue: SENDs, WRITEs, READs, atomics.
    Send,
    /// The receive queue.
    Recv,
}

/// A request that retired, as [`Worker::poll`] hands it out.
pub struct Completion<N: Nic = Verbs> {
    /// The QP's index in the worker ([`QpKey::index`]).
    pub qp: usize,
    /// The queue it was posted on.
    pub queue: Queue,
    /// Its sequence number in that queue.
    pub seq: Seq,
    /// Its outcome: status, byte length, immediate data. A request whose
    /// doorbell the device refused has a flush error.
    pub wc: Wc,
    /// Its buffers.
    pub leases: Leases<N>,
}

/// A completion queue and the QPs attached to it (the synchronous layer).
///
/// Dropping it drains its QPs (as [`Worker::remove_qp`] does, all at once),
/// destroys them, releases the buffers their requests held, then destroys
/// the CQ and the thread domain. A QP the device never flushes is leaked
/// with its buffers, and so are the CQ and the domain.
pub struct Worker<N: Nic = Verbs> {
    /// The verified hardware side; dropped by hand, in `drop`.
    hw: ManuallyDrop<Hw<N>>,
    /// The next request of each queue of each QP [`Worker::poll`] hands
    /// out, by index.
    cursors: Vec<(Seq, Seq)>,
}

impl<N: Nic> Worker<N> {
    /// A worker of `pd` with a CQ of `cq_size` entries, on a thread domain
    /// of its own where the device has them.
    pub fn new(pd: &Pd<N>, cq_size: u32) -> Result<Worker<N>> {
        Ok(Worker {
            hw: ManuallyDrop::new(Hw::new(pd.clone(), cq_size)?),
            cursors: Vec::new(),
        })
    }

    /// The protection domain of its QPs.
    pub fn pd(&self) -> &Pd<N> {
        self.hw.pd()
    }

    /// Whether its QPs are created on a thread domain (so the driver takes
    /// no locks for them); not every device has thread domains.
    pub fn is_lock_free(&self) -> bool {
        self.hw.is_lock_free()
    }

    /// The entries of its CQ.
    pub fn cq_size(&self) -> u32 {
        self.hw.cq_size()
    }

    /// The CQ entries its QPs reserve (the sum of their queue depths).
    pub fn reserved(&self) -> u32 {
        self.hw.reserved()
    }

    /// Panics unless `index` names a QP of the worker.
    fn check(&self, index: usize) {
        assert!(self.hw.is_qp(index), "no QP {index} in this worker");
    }

    /// Creates a QP (in the RESET state) with queues of `caps` and requests
    /// of at most `max_batch` WQEs, attached to the worker's CQ.
    ///
    /// The sizes are checked against what the device supports, and the
    /// QP's depths must fit in what the CQ has not reserved yet. M has one
    /// limit of scatter/gather elements per QP: every WQE is held to the
    /// smaller of `max_send_sge` and `max_recv_sge`.
    pub fn create_qp<T: Transport>(&mut self, caps: QpCaps, max_batch: u32) -> Result<QpKey<T>> {
        const {
            assert!(
                T::IN_ORDER_COMPLETION,
                "only in-order transports are supported"
            );
        }
        let index = self.hw.create_qp(T::QP_TYPE, caps, max_batch)?;
        if self.cursors.len() <= index {
            self.cursors.resize(index + 1, (0, 0));
        }
        self.cursors[index] = (0, 0);
        Ok(QpKey {
            index: index as u16,
            _t: PhantomData,
        })
    }

    /// Removes a QP: drains it, destroys it, then releases the buffers its
    /// requests still hold (none is handed out) and its share of the CQ.
    ///
    /// The NIC may still use the buffers of the WQEs in flight, so the QP is
    /// drained first (see the module documentation): moved to the error
    /// state, given a signaled drain WQE, and the CQ polled until nothing of
    /// it is in flight, the other QPs' completions going to their rings as
    /// [`Worker::progress`] sends them. No completion of the QP is left in
    /// the CQ then, so another QP may take its index and its share at once.
    ///
    /// If the device does not flush (no completion for [`DRAIN_TIMEOUT`]),
    /// the QP is kept with its buffers, index and share, never destroyed or
    /// freed, and this fails with [`Error::NotFlushed`]; dropping the
    /// worker tries again. Either way the key names no QP afterwards.
    pub fn remove_qp<T: Transport>(&mut self, qp: QpKey<T>) -> Result<()> {
        let index = qp.index();
        if !self.hw.is_qp(index) {
            return Ok(());
        }
        self.hw.start_drain(index);
        if self.drain(Some(index)) && self.hw.destroy(index) {
            Ok(())
        } else {
            Err(Error::NotFlushed)
        }
    }

    /// Polls the CQ until the QPs being removed (`only` that one, or all)
    /// have nothing in flight; `false` if no completion came for
    /// [`DRAIN_TIMEOUT`] while some still had.
    fn drain(&mut self, only: Option<usize>) -> bool {
        let (from, to) = match only {
            Some(index) => (index, index + 1),
            None => (0, self.cursors.len()),
        };
        let mut deadline = Instant::now() + DRAIN_TIMEOUT;
        let mut idle = 0u32;
        loop {
            let mut busy = false;
            for index in from..to {
                self.hw.post_drain(index);
                busy |= self.hw.draining(index);
            }
            if !busy {
                return true;
            }
            if self.hw.progress() > 0 {
                deadline = Instant::now() + DRAIN_TIMEOUT;
                idle = 0;
                continue;
            }
            idle += 1;
            if idle.is_multiple_of(DRAIN_SPINS) {
                if Instant::now() > deadline {
                    return false;
                }
                std::thread::yield_now();
            } else {
                core::hint::spin_loop();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Polling

    /// Polls the CQ once, retiring what completed in the rings of its QPs
    /// (and waking the wakers registered there); returns the number of
    /// completions. Owners take their results by sequence number
    /// ([`Worker::take_send`], [`Worker::take_recv`]).
    pub fn progress(&mut self) -> usize {
        self.hw.progress()
    }

    /// The event loop's poll: polls the CQ once and hands every request
    /// that retired with an owner (and was not taken otherwise) to `f`, with
    /// the worker, in each queue's order, and every request refused with its
    /// doorbell at once; returns the number of completions polled. `f` may
    /// post new work, take or detach requests, even remove QPs. Nothing is
    /// allocated.
    ///
    /// Requests without an owner (detached) release their buffers when they
    /// retire; their first error is reported by [`Worker::take_send_error`].
    /// An unsignaled request retires with the next signaled one of its
    /// queue: a loop that posts unsignaled requests also posts signaled
    /// ones.
    pub fn poll<F: FnMut(&mut Worker<N>, Completion<N>)>(&mut self, mut f: F) -> usize {
        let n = self.progress();
        while let Some(next) = self.hw.pop_dirty() {
            let Some((index, refused)) = next else {
                continue;
            };
            self.deliver(index, Queue::Send, &mut f);
            self.deliver(index, Queue::Recv, &mut f);
            if refused {
                self.deliver_refused(index, &mut f);
            }
        }
        n
    }

    /// Hands the refused requests among the ones in flight of QP `index`'s
    /// send queue to `f` (refusals are rare: the scan is too).
    fn deliver_refused<F: FnMut(&mut Worker<N>, Completion<N>)>(
        &mut self,
        index: usize,
        f: &mut F,
    ) {
        let Some(ring) = self.hw.ring(index, Side::Send) else {
            return;
        };
        let (from, to) = (ring.retired(), ring.committed());
        let mut seq = from;
        while seq < to {
            let refused = match self.hw.ring(index, Side::Send) {
                Some(ring) => ring.state(seq) == crate::ring::State::Refused,
                None => return,
            };
            if refused && let Some((wc, leases)) = self.hw.take(index, Side::Send, seq) {
                f(
                    self,
                    Completion {
                        qp: index,
                        queue: Queue::Send,
                        seq,
                        wc,
                        leases,
                    },
                );
            }
            seq += 1;
        }
    }

    /// Hands the retired requests of one queue to `f`, in order.
    fn deliver<F: FnMut(&mut Worker<N>, Completion<N>)>(
        &mut self,
        index: usize,
        queue: Queue,
        f: &mut F,
    ) {
        let side = match queue {
            Queue::Send => Side::Send,
            Queue::Recv => Side::Recv,
        };
        loop {
            let Some(ring) = self.hw.ring(index, side) else {
                return;
            };
            let (head, retired) = (ring.head(), ring.retired());
            let Some(cursors) = self.cursors.get_mut(index) else {
                return;
            };
            let cursor = match queue {
                Queue::Send => &mut cursors.0,
                Queue::Recv => &mut cursors.1,
            };
            // Requests before the head were taken or freed already.
            if *cursor < head {
                *cursor = head;
            }
            if *cursor >= retired {
                return;
            }
            let seq = *cursor;
            *cursor += 1;
            if let Some((wc, leases)) = self.hw.take(index, side, seq) {
                f(
                    self,
                    Completion {
                        qp: index,
                        queue,
                        seq,
                        wc,
                        leases,
                    },
                );
            }
        }
    }

    // -------------------------------------------------------------------------
    // One QP

    /// The QP number.
    pub fn qp_num<T: Transport>(&self, qp: QpKey<T>) -> u32 {
        self.check(qp.index());
        self.hw.qp_num(qp.index()).map_or(0, |(num, _)| num)
    }

    /// The queues' sizes.
    pub fn caps<T: Transport>(&self, qp: QpKey<T>) -> QpCaps {
        self.check(qp.index());
        self.hw.qp_caps(qp.index()).expect("a QP of this worker")
    }

    /// Whether the QP was created without the driver's locks (on the
    /// worker's thread domain).
    pub fn qp_is_lock_free<T: Transport>(&self, qp: QpKey<T>) -> bool {
        self.check(qp.index());
        self.hw.qp_num(qp.index()).is_some_and(|(_, free)| free)
    }

    /// The send queue's ring.
    pub fn send_ring<T: Transport>(&self, qp: QpKey<T>) -> &Ring<Leases<N>> {
        match self.hw.ring(qp.index(), Side::Send) {
            Some(ring) => ring,
            None => panic!("no QP {} in this worker", qp.index()),
        }
    }

    /// The receive queue's ring.
    pub fn recv_ring<T: Transport>(&self, qp: QpKey<T>) -> &Ring<Leases<N>> {
        match self.hw.ring(qp.index(), Side::Recv) {
            Some(ring) => ring,
            None => panic!("no QP {} in this worker", qp.index()),
        }
    }

    /// How peers reach the QP through `port`, by GID `gid_index` if given.
    pub fn endpoint<T: Transport>(
        &self,
        qp: QpKey<T>,
        port: u8,
        gid_index: Option<u8>,
    ) -> Result<Endpoint> {
        let ctx = self.pd().context();
        let info = ctx.port(port)?;
        let gid = match gid_index {
            Some(index) => {
                let mut found = None;
                for entry in ctx.gids(port) {
                    if entry.index == index {
                        found = Some(entry.gid);
                    }
                }
                Some(found.ok_or(Error::Invalid("no GID at that index"))?)
            }
            None => None,
        };
        Ok(Endpoint {
            qp_num: self.qp_num(qp),
            lid: info.lid,
            gid,
        })
    }

    /// Connects the QP to `remote` (RESET to INIT, RTR and RTS). A QP
    /// connects once: from RESET, as created. Until it is connected (or
    /// moved to the error state) it takes no work: doorbells are refused
    /// and receives are not posted. If connecting fails part way, the QP
    /// takes no work until it is moved to the error state.
    pub fn connect<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        attr: &ConnectAttr,
        remote: &Endpoint,
    ) -> Result<()> {
        self.check(qp.index());
        self.hw.connect(qp.index(), attr, remote)
    }

    /// Moves the QP to the error state: every request in flight completes
    /// with a flush error and hands its buffers back. Work posted from then
    /// on is flushed too.
    pub fn to_error<T: Transport>(&mut self, qp: QpKey<T>) -> Result<()> {
        self.check(qp.index());
        self.hw.to_error(qp.index())
    }

    /// Starts a request on the QP's send queue. Its WQEs go out at the next
    /// [`Worker::commit`] of the QP.
    pub fn request<T: Transport>(&mut self, qp: QpKey<T>) -> Request<'_, T, N> {
        Request {
            w: self,
            index: qp.index(),
            _t: PhantomData,
        }
    }

    /// Rings the QP's doorbell for every request written since the last
    /// one. If the device refuses, none of them is posted: each one with an
    /// owner retires at once with a flush error, which its owner takes with
    /// its buffers ([`Worker::take_send`], or [`Worker::poll`]); each one
    /// without an owner releases its buffers and is reported by
    /// [`Worker::take_send_error`] as a flush error. Sequence numbers are
    /// never reused. [`Request::submit`] and [`Worker::commit_or_take`] hand
    /// the buffers of their own request back with the error instead.
    pub fn commit<T: Transport>(&mut self, qp: QpKey<T>) -> Result<()> {
        self.hw.commit(qp.index(), None).map_err(|(e, _)| e)
    }

    /// Like [`Worker::commit`], but if the device refuses, hands back the
    /// buffers of request `seq` (staged, with an owner) with the error
    /// instead of keeping them for its owner.
    pub fn commit_or_take<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        seq: Seq,
    ) -> Result<(), (Error, Leases<N>)> {
        self.hw
            .commit(qp.index(), Some(seq))
            .map_err(|(e, kept)| (e, kept.unwrap_or_default()))
    }

    /// Takes the completion and buffers of retired (or refused) send
    /// request `seq`.
    pub fn take_send<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) -> Option<(Wc, Leases<N>)> {
        self.hw.take(qp.index(), Side::Send, seq)
    }

    /// Takes the completion and buffer of retired receive `seq`.
    pub fn take_recv<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) -> Option<(Wc, Leases<N>)> {
        self.hw.take(qp.index(), Side::Recv, seq)
    }

    /// Gives up send request `seq`: its buffers are released when it
    /// retires (now, if it has).
    pub fn detach_send<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) {
        self.hw.detach(qp.index(), Side::Send, seq);
    }

    /// Gives up receive `seq`: its buffer is released when it retires (now,
    /// if it has).
    pub fn detach_recv<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) {
        self.hw.detach(qp.index(), Side::Recv, seq);
    }

    /// The first error completion of a send request without an owner
    /// (a dropped future's, a sink's, a split batch's part), once.
    pub fn take_send_error<T: Transport>(&mut self, qp: QpKey<T>) -> Option<Wc> {
        self.hw.take_send_error(qp.index())
    }

    /// Wakes `waker` when send request `seq` retires.
    pub fn set_send_waker<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq, waker: &Waker) {
        self.hw.set_waker(qp.index(), Side::Send, seq, waker);
    }

    /// Wakes `waker` when receive `seq` retires.
    pub fn set_recv_waker<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq, waker: &Waker) {
        self.hw.set_waker(qp.index(), Side::Recv, seq, waker);
    }

    /// Waits until a new send request of at least one WQE fits on the QP:
    /// rings its doorbell for what is written and polls the CQ until a slot
    /// and a WQE are free. Fails if every slot holds a result nobody took.
    pub fn make_send_room<T: Transport>(&mut self, qp: QpKey<T>) -> Result<()> {
        self.hw.make_room(qp.index())
    }

    /// Posts a receive into `part`, a unique buffer, on the QP; `detached`
    /// if nobody will take it back by its sequence number. Refused when the
    /// receive queue is full or the QP takes no work.
    pub fn post_recv<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        part: Part<N>,
        detached: bool,
    ) -> Result<Seq, BufError<Part<N>>> {
        self.hw.post_recv(qp.index(), part, detached)
    }
}

impl<N: Nic> Drop for Worker<N> {
    /// Drains every QP (all at once, as [`Worker::remove_qp`] does one),
    /// destroys the drained ones and releases their buffers, then destroys
    /// the CQ and the thread domain. If the device does not flush some QP,
    /// that QP is leaked with its buffers, and so are the CQ and the domain
    /// (destroying them would need the QP gone).
    fn drop(&mut self) {
        for index in 0..self.cursors.len() {
            self.hw.start_drain(index);
        }
        self.drain(None);
        for index in 0..self.cursors.len() {
            if !self.hw.draining(index) {
                self.hw.destroy(index);
            }
        }
        // SAFETY: taken once, here; the field is not used again.
        let hw = unsafe { ManuallyDrop::take(&mut self.hw) };
        if let Err(hw) = hw.finish() {
            // A QP the NIC may still use: nothing of it is ever freed.
            core::mem::forget(hw);
        }
    }
}

/// A request being written on a QP of a worker: one or more WQEs that
/// retire together.
///
/// Each WQE is held back until the next one arrives (or the request is
/// finished), so that the last can be written signaled. A request longer
/// than the QP's `max_batch` WQEs, or than the room in the send queue, is
/// split: the part so far goes out as a request of its own (signaled,
/// without an owner) and the rest continues once there is room. Dropping
/// an unfinished request finishes it without an owner. The WQEs and their
/// buffers are kept by the worker's verified hardware side.
pub struct Request<'w, T: Transport, N: Nic = Verbs> {
    w: &'w mut Worker<N>,
    index: usize,
    _t: PhantomData<fn() -> T>,
}

impl<T: Transport, N: Nic> Request<'_, T, N> {
    /// Adds a WQE doing `op` on `part`; on error nothing is added and the
    /// part is handed back.
    pub fn push(&mut self, op: WrOp, part: Part<N>) -> Result<(), BufError<Part<N>>> {
        self.w.hw.gather(self.index, part)?;
        self.w
            .hw
            .push(self.index, op)
            .map_err(|(error, back)| BufError {
                error,
                buf: back.expect("the part of a WQE of one part"),
            })
    }

    /// Adds a WQE doing `op` on the concatenation of `parts` (a gather
    /// list for sends and WRITEs, a scatter list for READs); on error
    /// nothing is added and the parts are dropped.
    pub fn push_gather<I>(&mut self, op: WrOp, parts: I) -> Result<()>
    where
        I: IntoIterator<Item = Part<N>>,
    {
        for (i, part) in parts.into_iter().enumerate() {
            let n = i + 1;
            if let Err(e) = self.w.hw.gather(self.index, part) {
                self.w.hw.drop_gathered(self.index);
                let max = match e.error {
                    Error::TooManySges { max, .. } => max,
                    _ => MAX_SGES,
                };
                return Err(Error::TooManySges { got: n, max });
            }
        }
        self.w.hw.push(self.index, op).map_err(|(error, _)| error)
    }

    /// Whether no WQE was added.
    pub fn is_empty(&self) -> bool {
        !self.w.hw.is_open(self.index)
    }

    /// Finishes the request: signaled if `signal` (else only if the ring
    /// needs it), `detached` if nobody will take it back. Returns its
    /// sequence number, `None` if it has no WQE. It goes out at the next
    /// commit; if the device refuses that doorbell, it retires with a flush
    /// error (see [`Worker::commit`]).
    pub fn finish(self, signal: bool, detached: bool) -> Option<Seq> {
        self.w.hw.close(self.index, signal, detached)
    }

    /// Finishes the request with an owner and rings the doorbell. Returns
    /// its sequence number (`None` if it has no WQE); if the device
    /// refuses, its buffers come back with the error.
    pub fn submit(self, signal: bool) -> Option<Result<Seq, (Error, Leases<N>)>> {
        let seq = self.w.hw.close(self.index, signal, false)?;
        let key = QpKey::<T> {
            index: self.index as u16,
            _t: PhantomData,
        };
        Some(self.w.commit_or_take(key, seq).map(|()| seq))
    }

    /// Finishes the request without an owner (signaled) and rings the
    /// doorbell.
    pub fn submit_detached(self) -> Result<()> {
        self.w.hw.close(self.index, true, true);
        self.w.hw.commit(self.index, None).map_err(|(e, _)| e)
    }
}

impl<T: Transport, N: Nic> Drop for Request<'_, T, N> {
    fn drop(&mut self) {
        // An unfinished request goes out without an owner (a no-op once
        // finished).
        self.w.hw.close(self.index, true, true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Model;
    use crate::nic::{ConnectAttr, QpCaps};
    use crate::pd::Context;
    use crate::transport::RC;
    use crate::wc::Status;
    use crate::wr_id::{WQE_MASK, index_of, is_recv, wqe_of, wr_id};

    fn caps(depth: u32) -> QpCaps {
        QpCaps {
            max_send_wr: depth,
            max_recv_wr: depth,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 0,
        }
    }

    #[test]
    fn wr_ids_round_trip() {
        for &(index, recv, wqe) in &[
            (0u16, false, 0u64),
            (65535, true, WQE_MASK),
            (7, false, (1 << 50) + 3),
        ] {
            let id = wr_id(index, recv, wqe);
            assert_eq!(index_of(id), index as usize);
            assert_eq!(is_recv(id), recv);
            // Any WQE at most 2^47 after the oldest in flight comes back.
            for retired in [wqe, wqe.saturating_sub(1), wqe.saturating_sub(4096)] {
                assert_eq!(wqe_of(id, retired), wqe);
            }
        }
        // A WQE before the oldest in flight maps far ahead.
        assert!(wqe_of(wr_id(0, false, 5), 6) > 6 + (1 << 46));
    }

    /// What routing a completion costs: the `wr_id` decoding the worker
    /// does, against finding the QP by its number (a linear scan, as a
    /// worker of a few QPs would, and a hash map). Run with `cargo test
    /// --release -p safeverbs-core -- --ignored --nocapture demux_cost`.
    #[test]
    #[ignore = "a measurement"]
    fn demux_cost() {
        use core::hint::black_box;
        use std::collections::HashMap;
        use std::time::Instant;
        const N: usize = 1 << 24;
        for qps in [1usize, 4, 16, 64, 256] {
            // Completions of random QPs and queues, with their QP numbers.
            let mut x = 0x9E37_79B9_7F4A_7C15u64;
            let wcs: Vec<(u64, u32)> = (0..4096)
                .map(|i| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let q = (x as usize) % qps;
                    (wr_id(q as u16, x & 1 == 0, i), 0x100 + 7 * q as u32)
                })
                .collect();
            let nums: Vec<u32> = (0..qps).map(|q| 0x100 + 7 * q as u32).collect();
            let map: HashMap<u32, usize> = nums.iter().enumerate().map(|(i, &n)| (n, i)).collect();
            let time = |f: &dyn Fn(u64, u32) -> u64| {
                let start = Instant::now();
                let mut sum = 0u64;
                for i in 0..N {
                    let (id, num) = wcs[i % wcs.len()];
                    sum = sum.wrapping_add(f(black_box(id), black_box(num)));
                }
                black_box(sum);
                start.elapsed().as_nanos() as f64 / N as f64
            };
            let by_wr_id = time(&|id, _| {
                let index = index_of(id) as u64;
                let recv = is_recv(id);
                index * 2 + recv as u64 + wqe_of(id, 0)
            });
            let by_scan = time(&|id, num| {
                let index = nums.iter().position(|&n| n == num).unwrap_or(0) as u64;
                let recv = is_recv(id);
                index * 2 + recv as u64 + wqe_of(id, 0)
            });
            let by_map = time(&|id, num| {
                let index = map.get(&num).copied().unwrap_or(0) as u64;
                let recv = is_recv(id);
                index * 2 + recv as u64 + wqe_of(id, 0)
            });
            println!(
                "{qps:4} QPs: wr_id {by_wr_id:.2} ns, qp_num scan {by_scan:.2} ns, qp_num hash map {by_map:.2} ns per completion"
            );
        }
    }

    /// The CQ's capacity is reserved per QP; a QP that does not fit is
    /// refused, and removing one frees its share.
    #[test]
    fn qps_reserve_the_cq() {
        let pd = Context::new(Model::new(3)).alloc_pd().unwrap();
        let mut w = Worker::new(&pd, 64).unwrap();
        let a = w.create_qp::<RC>(caps(16), 4).unwrap();
        let b = w.create_qp::<RC>(caps(16), 4).unwrap();
        assert_eq!(w.reserved(), 64);
        assert_eq!(
            w.create_qp::<RC>(caps(1), 1).err(),
            Some(Error::CqFull { need: 2, free: 0 })
        );
        w.remove_qp(a).unwrap();
        assert_eq!(w.reserved(), 32);
        let c = w.create_qp::<RC>(caps(16), 4).unwrap();
        assert_eq!(c.index(), a.index(), "the index is reused");
        assert_ne!(w.qp_num(c), w.qp_num(b));
    }

    /// Two RC QPs of one worker connected to each other.
    fn connected_pair(w: &mut Worker<Model>, depth: u32, batch: u32) -> (QpKey<RC>, QpKey<RC>) {
        let a = w.create_qp::<RC>(caps(depth), batch).unwrap();
        let b = w.create_qp::<RC>(caps(depth), batch).unwrap();
        let attr = ConnectAttr {
            port: 1,
            access: crate::nic::Access::LOCAL_WRITE,
            path_mtu: 5,
            rq_psn: 0,
            sq_psn: 0,
            gid_index: None,
            traffic_class: 0,
            timeout: 14,
            retry_count: 7,
            rnr_retry: 7,
            min_rnr_timer: 12,
            max_rd_atomic: 1,
            max_dest_rd_atomic: 1,
        };
        let (ea, eb) = (
            w.endpoint(a, 1, None).unwrap(),
            w.endpoint(b, 1, None).unwrap(),
        );
        w.connect(a, &attr, &eb).unwrap();
        w.connect(b, &attr, &ea).unwrap();
        (a, b)
    }

    /// Removing QPs with work in flight drains them first: receives that
    /// will never match, and unsignaled sends that succeeded but have no
    /// completion of their own (the drain WQE retires them). The model
    /// panics if a QP is destroyed with a WQE not retired (M's
    /// `destroy_qp`); every buffer comes back.
    #[test]
    fn remove_qp_drains_work_in_flight() {
        let model = Model::new(11);
        let pd = Context::new(model.clone()).alloc_pd().unwrap();
        let mut w = Worker::new(&pd, 64).unwrap();
        let (a, b) = connected_pair(&mut w, 8, 1);
        let frozen = pd.alloc::<u8>(64).unwrap().freeze();
        // Receives on both sides; the sends of `a` take two of `b`'s.
        for qp in [a, a, b, b, b] {
            let buf = pd.alloc::<u8>(64).unwrap();
            w.post_recv(qp, buf.into_part(), false).unwrap();
        }
        // A signaled send, then an unsignaled one: once both ran, the last
        // has no completion and only a later one could retire it.
        let mut req = w.request(a);
        req.push(WrOp::Send, frozen.slice(..8).into_part()).unwrap();
        req.submit(true).unwrap().unwrap();
        let mut req = w.request(a);
        req.push(WrOp::Send, frozen.slice(..8).into_part()).unwrap();
        req.submit(false).unwrap().unwrap();
        model.run();
        w.progress();
        assert_eq!(model.pending(), 3, "the sends ran, three receives wait");
        assert_eq!(w.send_ring(a).in_flight_wqes(), 1, "the unsignaled send");
        w.remove_qp(a).unwrap();
        assert_eq!(w.reserved(), 16);
        w.remove_qp(b).unwrap();
        assert_eq!(w.reserved(), 0);
        assert!(frozen.try_thaw().is_ok(), "the sends' buffer is back");
        drop(w);
        drop(pd);
        assert_eq!(model.registered(), 0);
    }

    /// Dropping a worker drains every QP before destroying it.
    #[test]
    fn dropping_a_worker_drains_its_qps() {
        let model = Model::new(13);
        model.set_pace(900, 1);
        let pd = Context::new(model.clone()).alloc_pd().unwrap();
        let mut w = Worker::new(&pd, 64).unwrap();
        let (a, b) = connected_pair(&mut w, 8, 6);
        let frozen = pd.alloc::<u8>(64).unwrap().freeze();
        for _ in 0..4 {
            let buf = pd.alloc::<u8>(64).unwrap();
            w.post_recv(b, buf.into_part(), true).unwrap();
        }
        let mut req = w.request(a);
        for _ in 0..6 {
            req.push(WrOp::Send, frozen.slice(..8).into_part()).unwrap();
        }
        req.submit(false).unwrap().unwrap();
        drop(w);
        assert!(frozen.try_thaw().is_ok(), "the sends' buffer is back");
        drop(pd);
        assert_eq!(model.registered(), 0);
    }

    /// A device that never flushes: removing the QP fails, and its buffers
    /// are kept, never freed, with its index and share of the CQ; once the
    /// device moves again, dropping the worker drains and releases them.
    /// If it never does, the worker leaks the QP, its buffers and its CQ.
    #[test]
    fn a_device_that_never_flushes_keeps_the_buffers() {
        for recovers in [true, false] {
            let model = Model::new(17);
            let pd = Context::new(model.clone()).alloc_pd().unwrap();
            let mut w = Worker::new(&pd, 64).unwrap();
            let (a, b) = connected_pair(&mut w, 8, 1);
            let buf = pd.alloc::<u8>(64).unwrap();
            let ptr = buf.as_ptr() as usize;
            w.post_recv(a, buf.into_part(), false).unwrap();
            model.halt(true);
            assert_eq!(w.remove_qp(a), Err(Error::NotFlushed));
            assert_eq!(w.reserved(), 32, "its share of the CQ stays taken");
            let c = w.create_qp::<RC>(caps(8), 8).unwrap();
            assert_ne!(c.index(), a.index(), "its index stays taken");
            assert_eq!(model.registered(), 1, "its buffer stays registered");
            let _ = (b, ptr);
            if recovers {
                model.halt(false);
            }
            drop(w);
            drop(pd);
            assert_eq!(model.registered(), if recovers { 0 } else { 1 });
        }
    }

    /// A refused doorbell drops the requests written before it: one
    /// without an owner is reported as a flush error, one with an owner
    /// retires with it (and `poll` hands it out), and numbers are not
    /// reused.
    #[test]
    fn refused_doorbell_retires_staged_requests() {
        let pd = Context::new(Model::new(7)).alloc_pd().unwrap();
        let mut w = Worker::new(&pd, 64).unwrap();
        // Not connected: the worker refuses the doorbell, as a device would.
        let qp = w.create_qp::<RC>(caps(8), 4).unwrap();
        let buf = pd.alloc::<u8>(64).unwrap().freeze();
        let mut req = w.request(qp);
        req.push(WrOp::Send, buf.clone().into_part()).unwrap();
        assert_eq!(req.finish(false, true), Some(0));
        let mut req = w.request(qp);
        req.push(WrOp::Send, buf.clone().into_part()).unwrap();
        let owned = req.finish(true, false).unwrap();
        let mut req = w.request(qp);
        req.push(WrOp::Send, buf.into_part()).unwrap();
        let (error, leases) = req.submit(true).unwrap().unwrap_err();
        assert_eq!(error, Error::NotConnected);
        assert_eq!(leases.len(), 1);
        assert_eq!(w.send_ring(qp).posted(), 3);
        assert_eq!(w.send_ring(qp).in_flight_wqes(), 0);
        let wc = w
            .take_send_error(qp)
            .expect("the dropped request is reported");
        assert_eq!(wc.status, Status::WR_FLUSH_ERR);
        assert!(w.take_send_error(qp).is_none());
        let mut got = Vec::new();
        w.poll(|_, c| got.push((c.qp, c.queue, c.seq, c.wc.status, c.leases.len())));
        assert_eq!(
            got,
            [(qp.index(), Queue::Send, owned, Status::WR_FLUSH_ERR, 1)]
        );
    }
}
