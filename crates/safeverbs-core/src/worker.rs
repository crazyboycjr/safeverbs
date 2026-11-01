//! A worker: one completion queue and the queue pairs attached to it, the
//! unit of thread affinity, driven synchronously.
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
use crate::nic::{ConnectAttr, Endpoint, Nic, NicCq, NicQp, QpCaps, Sge, WrOp};
use crate::pd::Pd;
use crate::ring::{Ring, Seq};
use crate::transport::Transport;
use crate::verbs::Verbs;
use crate::wc::{Opcode, Wc};

/// The most scatter/gather elements of one WQE (they are kept on the
/// stack).
pub const MAX_SGES: usize = 16;

/// The most QPs of one worker: their index takes the top 16 bits of a
/// `wr_id`.
pub const MAX_QPS: usize = 1 << 16;

/// Completions taken from the CQ per poll.
const POLL_BATCH: usize = 16;

/// Bits of a `wr_id` that hold the WQE's number.
const WQE_BITS: u32 = 47;
const WQE_MASK: u64 = (1 << WQE_BITS) - 1;
/// The bit of a `wr_id` that marks a receive.
const RECV_BIT: u64 = 1 << WQE_BITS;
/// Where the QP's index starts in a `wr_id`.
const INDEX_SHIFT: u32 = WQE_BITS + 1;

const EMPTY_WC: Wc = Wc::success(Opcode(0), 0);

/// How long removing a QP waits for a completion while the device flushes
/// its work, before it gives up: then the QP and its buffers are kept, never
/// freed (see the module documentation).
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

/// Empty polls of the CQ between two looks at the clock while draining.
const DRAIN_SPINS: u32 = 1024;

/// The `wr_id` of WQE `wqe` of QP `index`'s send or receive queue.
#[inline]
fn wr_id(index: usize, recv: bool, wqe: u64) -> u64 {
    ((index as u64) << INDEX_SHIFT) | if recv { RECV_BIT } else { 0 } | (wqe & WQE_MASK)
}

/// The ring's number of the WQE a completion names, from its low bits and
/// the oldest WQE in flight (`retired`): the one at or after `retired` with
/// those bits. A WQE before `retired` maps far ahead, where the ring ignores
/// it.
#[inline]
fn wqe_of(wr_id: u64, retired: u64) -> u64 {
    retired.wrapping_add((wr_id & WQE_MASK).wrapping_sub(retired) & WQE_MASK)
}

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

/// What the worker knows of a hardware QP's state: it decides which verbs
/// the worker may call (M's preconditions: work is posted only in RTS or
/// ERR, and a QP connects from RESET).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    /// Created (RESET): it may connect; it takes no work.
    Reset,
    /// Connected (RTS, or moved to ERR by the NIC since), or moved to ERR
    /// by the worker: it takes work (in ERR, to flush it).
    Ready,
    /// A connection failed part way (INIT or RTR): it neither connects
    /// again nor takes work, until it is moved to the error state.
    Broken,
}

/// A QP of a worker with its rings.
///
/// The worker destroys the hardware QP only once nothing of it is in
/// flight (see [`Worker::remove_qp`]); fields drop in order, the hardware
/// QP first, then the rings, which release the buffers.
struct Entry<N: Nic> {
    hw: N::Qp,
    send: Ring<Leases<N>>,
    recv: Ring<Leases<N>>,
    caps: QpCaps,
    /// RDMA READ is allowed (a reliable connection).
    reads: bool,
    /// Atomics are allowed (a reliable connection on a device with them).
    atomics: bool,
    /// Whether WQEs were written since the last doorbell (`start` called).
    started: bool,
    /// The next request of each queue [`Worker::poll`] hands out.
    send_cursor: Seq,
    recv_cursor: Seq,
    /// Whether the QP is in the worker's list of QPs with requests to hand
    /// out.
    dirty: bool,
    /// Whether a doorbell was refused since [`Worker::poll`] last looked:
    /// refused requests may wait among the ones in flight.
    refused: bool,
    /// What the worker knows of the hardware QP's state.
    phase: Phase,
    /// Being removed: its owners are gone and it waits for the device to
    /// flush its work. Its key names no QP any more, but its index and its
    /// share of the CQ stay taken until it is destroyed.
    removed: bool,
    /// Removal waits to post the drain WQE on its send queue (for room).
    drain_pending: bool,
}

impl<N: Nic> Drop for Entry<N> {
    fn drop(&mut self) {
        if self.started {
            // SAFETY: `start` was called and the WQEs since are not wanted.
            unsafe { self.hw.abort() };
        }
    }
}

/// A completion queue and the QPs attached to it (the synchronous layer).
///
/// Dropping it drains its QPs (as [`Worker::remove_qp`] does, all at once),
/// destroys them, releases the buffers their requests held, then destroys
/// the CQ and the thread domain. A QP the device never flushes is leaked
/// with its buffers, and so are the CQ and the domain.
pub struct Worker<N: Nic = Verbs> {
    // Dropped by hand, in `drop`: the QPs (each before the buffers it
    // holds), then the CQ, then the domain they were created on; the PD
    // last, as a field.
    qps: Vec<Option<Entry<N>>>,
    cq: ManuallyDrop<N::Cq>,
    domain: ManuallyDrop<Option<N::Domain>>,
    pd: Pd<N>,
    /// Entries of the CQ, as asked for.
    cq_size: u32,
    /// CQ entries reserved by the QPs attached.
    reserved: u32,
    /// The QPs with retired requests [`Worker::poll`] has not handed out.
    dirty: Vec<u16>,
}

/// The WQE a request holds back until it knows whether it is its last.
struct Held {
    wr_id: u64,
    op: WrOp,
    sges: [Sge; MAX_SGES],
    n: usize,
}

impl<N: Nic> Worker<N> {
    /// A worker of `pd` with a CQ of `cq_size` entries, on a thread domain
    /// of its own where the device has them.
    pub fn new(pd: &Pd<N>, cq_size: u32) -> Result<Worker<N>> {
        let nic = pd.context().nic();
        if cq_size == 0 || cq_size > nic.caps().max_cqe {
            return Err(Error::Invalid("a CQ needs 1..=max_cqe entries"));
        }
        let cq = nic.create_cq(cq_size, true)?;
        let domain = nic.alloc_domain(pd.raw());
        Ok(Worker {
            qps: Vec::new(),
            cq: ManuallyDrop::new(cq),
            domain: ManuallyDrop::new(domain),
            pd: pd.clone(),
            cq_size,
            reserved: 0,
            dirty: Vec::new(),
        })
    }

    /// The protection domain of its QPs.
    pub fn pd(&self) -> &Pd<N> {
        &self.pd
    }

    /// Whether its QPs are created on a thread domain (so the driver takes
    /// no locks for them); not every device has thread domains.
    pub fn is_lock_free(&self) -> bool {
        self.domain.is_some()
    }

    /// The entries of its CQ.
    pub fn cq_size(&self) -> u32 {
        self.cq_size
    }

    /// The CQ entries its QPs reserve (the sum of their queue depths).
    pub fn reserved(&self) -> u32 {
        self.reserved
    }

    /// Creates a QP (in the RESET state) with queues of `caps` and requests
    /// of at most `max_batch` WQEs, attached to the worker's CQ.
    ///
    /// The sizes are checked against what the device supports, and the
    /// QP's depths must fit in what the CQ has not reserved yet.
    pub fn create_qp<T: Transport>(&mut self, caps: QpCaps, max_batch: u32) -> Result<QpKey<T>> {
        const {
            assert!(
                T::IN_ORDER_COMPLETION,
                "only in-order transports are supported"
            );
        }
        let nic = self.pd.context().nic();
        let dev = nic.caps();
        if caps.max_send_wr == 0 || caps.max_recv_wr == 0 {
            return Err(Error::Invalid("queues need at least one WQE"));
        }
        if caps.max_send_wr > dev.max_qp_wr || caps.max_recv_wr > dev.max_qp_wr {
            return Err(Error::Invalid(
                "queue deeper than the device allows (max_qp_wr)",
            ));
        }
        let max_sge = (dev.max_sge as usize).min(MAX_SGES) as u32;
        if caps.max_send_sge == 0 || caps.max_recv_sge == 0 {
            return Err(Error::Invalid(
                "WQEs need at least one scatter/gather element",
            ));
        }
        if caps.max_send_sge > max_sge || caps.max_recv_sge > max_sge {
            return Err(Error::Invalid(
                "more scatter/gather elements than the device (max_sge) or safeverbs allows",
            ));
        }
        if max_batch == 0 || max_batch > caps.max_send_wr {
            return Err(Error::Invalid("a batch needs 1..=max_send_wr WQEs"));
        }
        let need = caps.max_send_wr as u64 + caps.max_recv_wr as u64;
        let free = (self.cq_size - self.reserved) as u64;
        if need > free {
            return Err(Error::CqFull { need, free });
        }
        let index = match self.qps.iter().position(Option::is_none) {
            Some(i) => i,
            None if self.qps.len() < MAX_QPS => self.qps.len(),
            None => return Err(Error::Invalid("a worker holds at most 65536 QPs")),
        };
        let hw = nic.create_qp(
            self.pd.raw(),
            (*self.domain).as_ref(),
            &self.cq,
            &self.cq,
            T::QP_TYPE,
            &caps,
        )?;
        let reliable = T::QP_TYPE == crate::nic::QpType::Rc;
        // Twice the depth: requests in flight never take more than the depth
        // (each has a WQE at least); the rest is room for results not taken
        // yet.
        let entry = Entry {
            hw,
            send: Ring::new(2 * caps.max_send_wr as usize, caps.max_send_wr, max_batch),
            recv: Ring::new(2 * caps.max_recv_wr as usize, caps.max_recv_wr, 1),
            caps,
            reads: reliable,
            atomics: reliable && dev.atomics,
            started: false,
            send_cursor: 0,
            recv_cursor: 0,
            dirty: false,
            refused: false,
            phase: Phase::Reset,
            removed: false,
            drain_pending: false,
        };
        if index == self.qps.len() {
            self.qps.push(Some(entry));
        } else {
            self.qps[index] = Some(entry);
        }
        self.reserved += need as u32;
        // Room to list every QP as dirty, so that polling never allocates.
        self.dirty
            .reserve(self.qps.len().saturating_sub(self.dirty.len()));
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
        match self.qps.get(index) {
            Some(Some(entry)) if !entry.removed => {}
            _ => return Ok(()),
        }
        self.dirty.retain(|&d| d as usize != index);
        self.start_drain(index);
        if self.drain(Some(index)) {
            self.destroy(index);
            Ok(())
        } else {
            Err(Error::NotFlushed)
        }
    }

    /// Starts removing QP `index`: discards the WQEs written since the last
    /// doorbell, gives up every request (each releases its buffers once the
    /// NIC is done with it) and, if anything is in flight, moves the QP to
    /// the error state, so that the NIC flushes it, and asks for a drain
    /// WQE.
    fn start_drain(&mut self, index: usize) {
        let Some(Some(qp)) = self.qps.get_mut(index) else {
            return;
        };
        qp.removed = true;
        qp.dirty = false;
        qp.refused = false;
        if qp.started {
            qp.started = false;
            // SAFETY: `start` was called (`started`); the WQEs since were
            // never posted.
            unsafe { qp.hw.abort() };
            qp.send.abort();
        }
        let mut seq = qp.send.head();
        while seq < qp.send.posted() {
            qp.send.detach(seq);
            seq += 1;
        }
        let mut seq = qp.recv.head();
        while seq < qp.recv.posted() {
            qp.recv.detach(seq);
            seq += 1;
        }
        if qp.send.in_flight_wqes() == 0 && qp.recv.in_flight_wqes() == 0 {
            return;
        }
        // ERR is reachable from every state (M's `modify_qp`). A device
        // that refuses it is left to the timeout: the QP is then kept.
        if qp.hw.to_error().is_ok() {
            qp.phase = Phase::Ready;
            qp.drain_pending = qp.send.in_flight_wqes() > 0;
        }
    }

    /// Whether QP `index`, being removed, still has work in flight (or its
    /// drain WQE to post).
    fn draining(&self, index: usize) -> bool {
        match self.qps.get(index) {
            Some(Some(qp)) if qp.removed => {
                qp.drain_pending || qp.send.in_flight_wqes() > 0 || qp.recv.in_flight_wqes() > 0
            }
            _ => false,
        }
    }

    /// Posts the drain WQE of QP `index` if its send queue has room: a
    /// signaled SEND of nothing, without an owner. In the error state it
    /// completes with a flush error, after every WQE before it, and polling
    /// that completion retires them all, including unsignaled ones that
    /// succeeded before the error (which have no completion of their own).
    fn post_drain(&mut self, index: usize) {
        let Some(Some(qp)) = self.qps.get_mut(index) else {
            return;
        };
        if !qp.drain_pending || !qp.send.can_post() {
            return;
        }
        if qp.send.begin().is_none() {
            return;
        }
        let Some(wqe) = qp.send.push_wqe() else {
            return;
        };
        let implied = Wc::success(Opcode::SEND, 0);
        let (_, signaled) = qp.send.end(true, true, implied, Leases::default());
        // SAFETY: not started (`start_drain` discarded what was written);
        // the WQE names no memory; the ring handed out its number, so the
        // queue has room; the QP is in the error state, where M accepts
        // work (to flush it).
        let posted = unsafe {
            qp.hw.start();
            qp.hw
                .post(wr_id(index, false, wqe), signaled, &WrOp::Send, &[]);
            qp.hw.complete()
        };
        match posted {
            Ok(()) => {
                qp.send.commit();
                qp.drain_pending = false;
            }
            // Refused: tried again on the next round.
            Err(_) => {
                qp.send.abort();
            }
        }
    }

    /// Polls the CQ until the QPs being removed (`only` that one, or all)
    /// have nothing in flight; `false` if no completion came for
    /// [`DRAIN_TIMEOUT`] while some still had.
    fn drain(&mut self, only: Option<usize>) -> bool {
        let (from, to) = match only {
            Some(index) => (index, index + 1),
            None => (0, self.qps.len()),
        };
        let mut deadline = Instant::now() + DRAIN_TIMEOUT;
        let mut idle = 0u32;
        loop {
            let mut busy = false;
            let mut index = from;
            while index < to {
                self.post_drain(index);
                busy |= self.draining(index);
                index += 1;
            }
            if !busy {
                return true;
            }
            if self.progress() > 0 {
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

    /// Destroys QP `index`, drained, then releases its buffers and its
    /// share of the CQ.
    fn destroy(&mut self, index: usize) {
        let Some(entry) = self.qps.get_mut(index).and_then(Option::take) else {
            return;
        };
        let need = entry.caps.max_send_wr + entry.caps.max_recv_wr;
        // The hardware QP first (fields drop in order), then the rings.
        drop(entry);
        self.reserved -= need;
    }

    fn entry(&self, index: usize) -> &Entry<N> {
        match self.qps.get(index) {
            Some(Some(entry)) if !entry.removed => entry,
            _ => panic!("no QP {index} in this worker"),
        }
    }

    fn entry_mut(&mut self, index: usize) -> &mut Entry<N> {
        match self.qps.get_mut(index) {
            Some(Some(entry)) if !entry.removed => entry,
            _ => panic!("no QP {index} in this worker"),
        }
    }

    // -------------------------------------------------------------------------
    // Polling

    /// Polls the CQ once, retiring what completed in the rings of its QPs
    /// (and waking the wakers registered there); returns the number of
    /// completions. Owners take their results by sequence number
    /// ([`Worker::take_send`], [`Worker::take_recv`]).
    pub fn progress(&mut self) -> usize {
        let mut wcs = [EMPTY_WC; POLL_BATCH];
        let n = self.cq.poll(&mut wcs);
        let mut i = 0;
        while i < n {
            self.dispatch(&wcs[i]);
            i += 1;
        }
        n
    }

    /// Routes one completion to its QP's ring.
    #[inline]
    fn dispatch(&mut self, wc: &Wc) {
        let index = (wc.wr_id >> INDEX_SHIFT) as usize;
        // A completion of a QP removed since: dropped.
        let Some(Some(qp)) = self.qps.get_mut(index) else {
            return;
        };
        let ring = if wc.wr_id & RECV_BIT != 0 {
            &mut qp.recv
        } else {
            &mut qp.send
        };
        let retired = ring.retired();
        let known = ring.complete(wqe_of(wc.wr_id, ring.wqe_retired()), *wc);
        debug_assert!(known, "a completion for an unknown WQE: {wc:?}");
        if ring.retired() != retired && !qp.dirty && !qp.removed {
            qp.dirty = true;
            self.dirty.push(index as u16);
        }
    }

    /// Lists QP `index` as having refused requests to hand out.
    fn mark_refused(&mut self, index: usize) {
        let qp = self.entry_mut(index);
        qp.refused = true;
        if !qp.dirty {
            qp.dirty = true;
            self.dirty.push(index as u16);
        }
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
        while let Some(index) = self.dirty.pop() {
            let index = index as usize;
            let refused = match self.qps.get_mut(index) {
                Some(Some(qp)) => {
                    qp.dirty = false;
                    core::mem::replace(&mut qp.refused, false)
                }
                _ => continue,
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
        let (from, to) = {
            let ring = &self.entry(index).send;
            (ring.retired(), ring.committed())
        };
        let mut seq = from;
        while seq < to {
            let Some(Some(qp)) = self.qps.get_mut(index) else {
                return;
            };
            if qp.send.state(seq) == crate::ring::State::Refused
                && let Some((wc, leases)) = qp.send.take(seq)
            {
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
        loop {
            let Some(Some(qp)) = self.qps.get_mut(index) else {
                return;
            };
            let (ring, cursor) = match queue {
                Queue::Send => (&mut qp.send, &mut qp.send_cursor),
                Queue::Recv => (&mut qp.recv, &mut qp.recv_cursor),
            };
            // Requests before the head were taken or freed already.
            if *cursor < ring.head() {
                *cursor = ring.head();
            }
            if *cursor >= ring.retired() {
                return;
            }
            let seq = *cursor;
            *cursor += 1;
            if let Some((wc, leases)) = ring.take(seq) {
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
        self.entry(qp.index()).hw.qp_num()
    }

    /// The queues' sizes.
    pub fn caps<T: Transport>(&self, qp: QpKey<T>) -> QpCaps {
        self.entry(qp.index()).caps
    }

    /// Whether the QP was created without the driver's locks (on the
    /// worker's thread domain).
    pub fn qp_is_lock_free<T: Transport>(&self, qp: QpKey<T>) -> bool {
        self.entry(qp.index()).hw.is_lock_free()
    }

    /// The send queue's ring.
    pub fn send_ring<T: Transport>(&self, qp: QpKey<T>) -> &Ring<Leases<N>> {
        &self.entry(qp.index()).send
    }

    /// The receive queue's ring.
    pub fn recv_ring<T: Transport>(&self, qp: QpKey<T>) -> &Ring<Leases<N>> {
        &self.entry(qp.index()).recv
    }

    /// How peers reach the QP through `port`, by GID `gid_index` if given.
    pub fn endpoint<T: Transport>(
        &self,
        qp: QpKey<T>,
        port: u8,
        gid_index: Option<u8>,
    ) -> Result<Endpoint> {
        let ctx = self.pd.context();
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
        let entry = self.entry_mut(qp.index());
        if entry.phase != Phase::Reset {
            return Err(Error::Invalid("a QP connects once, from RESET"));
        }
        match entry.hw.connect(attr, remote) {
            Ok(()) => {
                entry.phase = Phase::Ready;
                Ok(())
            }
            Err(e) => {
                entry.phase = Phase::Broken;
                Err(e)
            }
        }
    }

    /// Moves the QP to the error state: every request in flight completes
    /// with a flush error and hands its buffers back. Work posted from then
    /// on is flushed too.
    pub fn to_error<T: Transport>(&mut self, qp: QpKey<T>) -> Result<()> {
        let entry = self.entry_mut(qp.index());
        entry.hw.to_error()?;
        entry.phase = Phase::Ready;
        Ok(())
    }

    /// Starts a request on the QP's send queue. Its WQEs go out at the next
    /// [`Worker::commit`] of the QP.
    pub fn request<T: Transport>(&mut self, qp: QpKey<T>) -> Request<'_, T, N> {
        Request {
            w: self,
            index: qp.index(),
            open: false,
            leases: Leases::default(),
            held: None,
            bytes: 0,
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
        self.commit_at(qp.index())
    }

    fn commit_at(&mut self, index: usize) -> Result<()> {
        let qp = self.entry_mut(index);
        if !qp.started {
            return Ok(());
        }
        qp.started = false;
        // SAFETY: `start` was called (`started`); the QP takes work (M's
        // `post_send` needs RTS or ERR), else the WQEs are discarded.
        match unsafe { ring_doorbell(qp) } {
            Ok(()) => {
                qp.send.commit();
                Ok(())
            }
            Err(e) => {
                qp.send.abort();
                self.mark_refused(index);
                Err(e)
            }
        }
    }

    /// Like [`Worker::commit`], but if the device refuses, hands back the
    /// buffers of request `seq` (staged, with an owner) with the error
    /// instead of keeping them for its owner.
    pub fn commit_or_take<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        seq: Seq,
    ) -> Result<(), (Error, Leases<N>)> {
        let index = qp.index();
        let entry = self.entry_mut(index);
        if !entry.started {
            return Ok(());
        }
        entry.started = false;
        // SAFETY: as in `commit_at`.
        match unsafe { ring_doorbell(entry) } {
            Ok(()) => {
                entry.send.commit();
                Ok(())
            }
            Err(e) => {
                let back = entry.send.abort_keeping(seq).unwrap_or_default();
                self.mark_refused(index);
                Err((e, back))
            }
        }
    }

    /// Takes the completion and buffers of retired (or refused) send
    /// request `seq`.
    pub fn take_send<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) -> Option<(Wc, Leases<N>)> {
        self.entry_mut(qp.index()).send.take(seq)
    }

    /// Takes the completion and buffer of retired receive `seq`.
    pub fn take_recv<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) -> Option<(Wc, Leases<N>)> {
        self.entry_mut(qp.index()).recv.take(seq)
    }

    /// Gives up send request `seq`: its buffers are released when it
    /// retires (now, if it has).
    pub fn detach_send<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) {
        self.entry_mut(qp.index()).send.detach(seq);
    }

    /// Gives up receive `seq`: its buffer is released when it retires (now,
    /// if it has).
    pub fn detach_recv<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) {
        self.entry_mut(qp.index()).recv.detach(seq);
    }

    /// The first error completion of a send request without an owner
    /// (a dropped future's, a sink's, a split batch's part), once.
    pub fn take_send_error<T: Transport>(&mut self, qp: QpKey<T>) -> Option<Wc> {
        self.entry_mut(qp.index()).send.take_detached_error()
    }

    /// Wakes `waker` when send request `seq` retires.
    pub fn set_send_waker<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq, waker: &Waker) {
        self.entry_mut(qp.index()).send.set_waker(seq, waker);
    }

    /// Wakes `waker` when receive `seq` retires.
    pub fn set_recv_waker<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq, waker: &Waker) {
        self.entry_mut(qp.index()).recv.set_waker(seq, waker);
    }

    /// Waits until a new send request of at least one WQE fits on the QP:
    /// rings its doorbell for what is written and polls the CQ until a slot
    /// and a WQE are free. Fails if every slot holds a result nobody took.
    pub fn make_send_room<T: Transport>(&mut self, qp: QpKey<T>) -> Result<()> {
        self.make_send_room_at(qp.index())
    }

    fn make_send_room_at(&mut self, index: usize) -> Result<()> {
        loop {
            let ring = &self.entry(index).send;
            if ring.can_post() {
                return Ok(());
            }
            if ring.is_clogged() {
                return Err(Error::Clogged);
            }
            self.commit_at(index)?;
            if self.progress() == 0 {
                core::hint::spin_loop();
            }
        }
    }

    /// Posts a receive into `part`, a unique buffer, on the QP; `detached`
    /// if nobody will take it back by its sequence number. Refused when the
    /// receive queue is full.
    pub fn post_recv<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        part: Part<N>,
        detached: bool,
    ) -> Result<Seq, BufError<Part<N>>> {
        let index = qp.index();
        if part.pd != self.pd.id() {
            return Err(BufError {
                error: Error::PdMismatch,
                buf: part,
            });
        }
        if !part.is_writable() {
            return Err(BufError {
                error: Error::Invalid("receive into a frozen buffer"),
                buf: part,
            });
        }
        let qp = self.entry_mut(index);
        if qp.phase != Phase::Ready {
            return Err(BufError {
                error: Error::NotConnected,
                buf: part,
            });
        }
        if !qp.recv.can_post() {
            let error = if qp.recv.is_clogged() {
                Error::Clogged
            } else {
                Error::QueueFull
            };
            return Err(BufError { error, buf: part });
        }
        let seq = qp.recv.begin().expect("a free slot");
        let wqe = qp.recv.push_wqe().expect("room for a WQE");
        // SAFETY: the buffer is unique (checked) and moves into the ring
        // below, which keeps it until the receive retires (the QP is
        // drained before it is destroyed); `can_post` checked the queue's
        // room; the QP takes work (connected, or in the error state).
        match unsafe { qp.hw.post_recv(wr_id(index, true, wqe), &[part.sge]) } {
            Ok(()) => {
                let implied = Wc::success(Opcode::RECV, part.sge.length);
                let mut leases = Leases::default();
                leases.push(part.lease);
                qp.recv.end(true, detached, implied, leases);
                qp.recv.commit();
                Ok(seq)
            }
            Err(error) => {
                qp.recv.abort();
                Err(BufError { error, buf: part })
            }
        }
    }

    /// Writes `held` into QP `index`'s send queue.
    fn hw_post(&mut self, index: usize, held: &Held, signaled: bool) {
        let qp = self.entry_mut(index);
        // SAFETY: the request's leases keep the memory valid until the WQE
        // retires (they move into the ring with it); the ring handed out
        // its number, so the queue has room; `start` is called once per
        // doorbell; the WQE was checked against what the QP may do.
        unsafe {
            if !qp.started {
                qp.hw.start();
                qp.started = true;
            }
            qp.hw
                .post(held.wr_id, signaled, &held.op, &held.sges[..held.n]);
        }
    }
}

impl<N: Nic> Drop for Worker<N> {
    /// Drains every QP (all at once, as [`Worker::remove_qp`] does one),
    /// destroys the drained ones and releases their buffers, then destroys
    /// the CQ and the thread domain. If the device does not flush some QP,
    /// that QP is leaked with its buffers, and so are the CQ and the domain
    /// (destroying them would need the QP gone).
    fn drop(&mut self) {
        let mut index = 0;
        while index < self.qps.len() {
            let removed = matches!(&self.qps[index], Some(entry) if entry.removed);
            if !removed {
                self.start_drain(index);
            }
            index += 1;
        }
        let drained = self.drain(None);
        let mut index = 0;
        while index < self.qps.len() {
            if self.draining(index) {
                if let Some(entry) = self.qps[index].take() {
                    // The NIC may still use its buffers: never freed.
                    core::mem::forget(entry);
                }
            } else {
                self.destroy(index);
            }
            index += 1;
        }
        if drained {
            // SAFETY: dropped once, here; every QP attached to the CQ and
            // created on the domain is destroyed.
            unsafe {
                ManuallyDrop::drop(&mut self.cq);
                ManuallyDrop::drop(&mut self.domain);
            }
        }
    }
}

/// Rings the doorbell of `qp` for the WQEs written since its start, if it
/// takes work; otherwise discards them and fails with
/// [`Error::NotConnected`], as a device refusing the doorbell would.
///
/// # Safety
///
/// After `start` (as [`NicQp::complete`]).
unsafe fn ring_doorbell<N: Nic>(qp: &mut Entry<N>) -> Result<()> {
    if qp.phase == Phase::Ready {
        // SAFETY: after `start` (the caller's promise).
        unsafe { qp.hw.complete() }
    } else {
        // SAFETY: after `start` (the caller's promise).
        unsafe { qp.hw.abort() };
        Err(Error::NotConnected)
    }
}

/// The opcode a successful completion of `op` reports.
fn opcode_of(op: &WrOp) -> Opcode {
    match op {
        WrOp::Send | WrOp::SendImm(_) => Opcode::SEND,
        WrOp::Write(_) | WrOp::WriteImm(..) => Opcode::RDMA_WRITE,
        WrOp::Read(_) => Opcode::RDMA_READ,
        WrOp::FetchAdd { .. } => Opcode::FETCH_ADD,
        WrOp::CompareSwap { .. } => Opcode::COMP_SWAP,
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
/// an unfinished request finishes it without an owner.
pub struct Request<'w, T: Transport, N: Nic = Verbs> {
    w: &'w mut Worker<N>,
    index: usize,
    open: bool,
    leases: Leases<N>,
    held: Option<Held>,
    bytes: u64,
    _t: PhantomData<fn() -> T>,
}

impl<T: Transport, N: Nic> Request<'_, T, N> {
    /// Adds a WQE doing `op` on `part`; on error nothing is added and the
    /// part is handed back.
    pub fn push(&mut self, op: WrOp, part: Part<N>) -> Result<(), BufError<Part<N>>> {
        let sges = [part.sge];
        if let Err(error) = self.check(&op, &sges, &[(part.pd, part.is_writable())]) {
            return Err(BufError { error, buf: part });
        }
        if let Err(error) = self.add(op, &sges) {
            return Err(BufError { error, buf: part });
        }
        self.leases.push(part.lease);
        Ok(())
    }

    /// Adds a WQE doing `op` on the concatenation of `parts` (a gather
    /// list for sends and WRITEs, a scatter list for READs); on error
    /// nothing is added and the parts are dropped.
    pub fn push_gather<I>(&mut self, op: WrOp, parts: I) -> Result<()>
    where
        I: IntoIterator<Item = Part<N>>,
    {
        let mut keep: [Option<Part<N>>; MAX_SGES] = [const { None }; MAX_SGES];
        let mut sges = [Sge::default(); MAX_SGES];
        let mut owners = [(0u64, false); MAX_SGES];
        let mut n = 0;
        for part in parts {
            if n == MAX_SGES {
                return Err(Error::TooManySges {
                    got: n + 1,
                    max: self.w.entry(self.index).caps.max_send_sge as usize,
                });
            }
            sges[n] = part.sge;
            owners[n] = (part.pd, part.is_writable());
            keep[n] = Some(part);
            n += 1;
        }
        self.check(&op, &sges[..n], &owners[..n])?;
        self.add(op, &sges[..n])?;
        let mut i = 0;
        while i < n {
            if let Some(part) = keep[i].take() {
                self.leases.push(part.lease);
            }
            i += 1;
        }
        Ok(())
    }

    /// Checks a WQE before it is written.
    fn check(&self, op: &WrOp, sges: &[Sge], owners: &[(u64, bool)]) -> Result<()> {
        let qp = self.w.entry(self.index);
        let max = qp.caps.max_send_sge as usize;
        if sges.is_empty() || sges.len() > max {
            return Err(Error::TooManySges {
                got: sges.len(),
                max,
            });
        }
        // What the hardware QP was created to do: the types allow READs and
        // atomics on reliable connections only, but a key may name another
        // QP than the one it was made for.
        match op {
            WrOp::Read(_) if !qp.reads => {
                return Err(Error::Unsupported("RDMA READ on this QP"));
            }
            WrOp::FetchAdd { .. } | WrOp::CompareSwap { .. } if !qp.atomics => {
                return Err(Error::Unsupported("atomics on this QP"));
            }
            _ => {}
        }
        let writes_local = matches!(
            op,
            WrOp::Read(_) | WrOp::FetchAdd { .. } | WrOp::CompareSwap { .. }
        );
        let pd = self.w.pd.id();
        let mut total = 0u64;
        let mut i = 0;
        while i < sges.len() {
            if owners[i].0 != pd {
                return Err(Error::PdMismatch);
            }
            if writes_local && !owners[i].1 {
                return Err(Error::Invalid("the NIC would write a frozen buffer"));
            }
            total += sges[i].length as u64;
            i += 1;
        }
        if total > i32::MAX as u64 {
            return Err(Error::TooLong {
                len: total,
                max: i32::MAX as u64,
            });
        }
        match op {
            WrOp::Write(r) | WrOp::WriteImm(r, _) | WrOp::Read(r) => {
                if total > r.len {
                    return Err(Error::TooLong {
                        len: total,
                        max: r.len,
                    });
                }
            }
            WrOp::FetchAdd { remote, .. } | WrOp::CompareSwap { remote, .. } => {
                if total != 8 || sges.len() != 1 {
                    return Err(Error::Invalid(
                        "an atomic needs one local buffer of 8 bytes",
                    ));
                }
                if remote.len < 8 || remote.addr % 8 != 0 {
                    return Err(Error::Invalid("an atomic needs 8 aligned remote bytes"));
                }
            }
            WrOp::Send | WrOp::SendImm(_) => {}
        }
        Ok(())
    }

    /// Writes a checked WQE: opens the request (or splits it) as needed,
    /// writes the WQE held so far and holds this one.
    fn add(&mut self, op: WrOp, sges: &[Sge]) -> Result<()> {
        if !self.open {
            self.w.make_send_room_at(self.index)?;
            self.w
                .entry_mut(self.index)
                .send
                .begin()
                .expect("room was made");
            self.open = true;
        } else if self.w.entry(self.index).send.wqe_room() == 0 {
            self.close(true, true);
            self.w.make_send_room_at(self.index)?;
            self.w
                .entry_mut(self.index)
                .send
                .begin()
                .expect("room was made");
            self.open = true;
        }
        let wqe = self
            .w
            .entry_mut(self.index)
            .send
            .push_wqe()
            .expect("room for a WQE");
        let mut held = Held {
            wr_id: wr_id(self.index, false, wqe),
            op,
            sges: [Sge::default(); MAX_SGES],
            n: sges.len(),
        };
        held.sges[..sges.len()].copy_from_slice(sges);
        if let Some(prev) = self.held.replace(held) {
            self.w.hw_post(self.index, &prev, false);
        }
        let mut i = 0;
        while i < sges.len() {
            self.bytes += sges[i].length as u64;
            i += 1;
        }
        Ok(())
    }

    /// Closes the open request: stages it in the ring and writes its last
    /// WQE, signaled if asked or if the ring says so.
    fn close(&mut self, signal: bool, detached: bool) -> Seq {
        let held = self.held.take().expect("an open request has a WQE");
        let implied = Wc::success(opcode_of(&held.op), self.bytes.min(u32::MAX as u64) as u32);
        let leases = core::mem::take(&mut self.leases);
        let (seq, signaled) = self
            .w
            .entry_mut(self.index)
            .send
            .end(signal, detached, implied, leases);
        self.w.hw_post(self.index, &held, signaled);
        self.open = false;
        self.bytes = 0;
        seq
    }

    /// Whether no WQE was added.
    pub fn is_empty(&self) -> bool {
        !self.open
    }

    /// Finishes the request: signaled if `signal` (else only if the ring
    /// needs it), `detached` if nobody will take it back. Returns its
    /// sequence number, `None` if it has no WQE. It goes out at the next
    /// commit; if the device refuses that doorbell, it retires with a flush
    /// error (see [`Worker::commit`]).
    pub fn finish(mut self, signal: bool, detached: bool) -> Option<Seq> {
        if self.open {
            Some(self.close(signal, detached))
        } else {
            None
        }
    }

    /// Finishes the request with an owner and rings the doorbell. Returns
    /// its sequence number (`None` if it has no WQE); if the device
    /// refuses, its buffers come back with the error.
    pub fn submit(mut self, signal: bool) -> Option<Result<Seq, (Error, Leases<N>)>> {
        if !self.open {
            return None;
        }
        let seq = self.close(signal, false);
        let key = QpKey::<T> {
            index: self.index as u16,
            _t: PhantomData,
        };
        Some(self.w.commit_or_take(key, seq).map(|()| seq))
    }

    /// Finishes the request without an owner (signaled) and rings the
    /// doorbell.
    pub fn submit_detached(mut self) -> Result<()> {
        if self.open {
            self.close(true, true);
        }
        self.w.commit_at(self.index)
    }
}

impl<T: Transport, N: Nic> Drop for Request<'_, T, N> {
    fn drop(&mut self) {
        if self.open {
            self.close(true, true);
        }
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
            (0usize, false, 0u64),
            (65535, true, WQE_MASK),
            (7, false, (1 << 50) + 3),
        ] {
            let id = wr_id(index, recv, wqe);
            assert_eq!((id >> INDEX_SHIFT) as usize, index);
            assert_eq!(id & RECV_BIT != 0, recv);
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
                    (wr_id(q, x & 1 == 0, i), 0x100 + 7 * q as u32)
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
                let index = id >> INDEX_SHIFT;
                let recv = id & RECV_BIT != 0;
                index * 2 + recv as u64 + wqe_of(id, 0)
            });
            let by_scan = time(&|id, num| {
                let index = nums.iter().position(|&n| n == num).unwrap_or(0) as u64;
                let recv = id & RECV_BIT != 0;
                index * 2 + recv as u64 + wqe_of(id, 0)
            });
            let by_map = time(&|id, num| {
                let index = map.get(&num).copied().unwrap_or(0) as u64;
                let recv = id & RECV_BIT != 0;
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
