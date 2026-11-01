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

use core::marker::PhantomData;
use core::task::Waker;

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
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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

/// A QP of a worker with its rings.
///
/// Fields drop in order: the hardware QP first, so the NIC stops, then the
/// rings, which release the buffers still held by requests in flight.
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
/// Dropping it destroys its QPs first, so the NIC stops, then releases the
/// buffers still held by requests in flight, then destroys the CQ and the
/// thread domain.
pub struct Worker<N: Nic = Verbs> {
    // Fields drop in order: the QPs (each before the buffers it holds), then
    // the CQ, then the domain they were created on, then the PD.
    qps: Vec<Option<Entry<N>>>,
    cq: N::Cq,
    domain: Option<N::Domain>,
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
            cq,
            domain,
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
            self.domain.as_ref(),
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

    /// Destroys a QP and releases the buffers its requests still hold (none
    /// is handed out), and its reservation on the CQ.
    ///
    /// Completions of the QP may still wait in the CQ, so the CQ is polled
    /// until it has none left, the other QPs' completions going to their
    /// rings as [`Worker::progress`] sends them: only then may another QP
    /// take the index or the reservation.
    pub fn remove_qp<T: Transport>(&mut self, qp: QpKey<T>) {
        let index = qp.index();
        let Some(entry) = self.qps.get_mut(index).and_then(Option::take) else {
            return;
        };
        let need = entry.caps.max_send_wr + entry.caps.max_recv_wr;
        drop(entry);
        self.dirty.retain(|&d| d as usize != index);
        // The CQ holds at most `cq_size` entries, the QP's among them.
        let mut polled = 0;
        while polled < self.cq_size as usize {
            let n = self.progress();
            if n == 0 {
                break;
            }
            polled += n;
        }
        self.reserved -= need;
    }

    fn entry(&self, index: usize) -> &Entry<N> {
        match self.qps.get(index) {
            Some(Some(entry)) => entry,
            _ => panic!("no QP {index} in this worker"),
        }
    }

    fn entry_mut(&mut self, index: usize) -> &mut Entry<N> {
        match self.qps.get_mut(index) {
            Some(Some(entry)) => entry,
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
        if ring.retired() != retired && !qp.dirty {
            qp.dirty = true;
            self.dirty.push(index as u16);
        }
    }

    /// Lists QP `index` as having requests to hand out.
    fn mark_dirty(&mut self, index: usize) {
        let qp = self.entry_mut(index);
        if !qp.dirty {
            qp.dirty = true;
            self.dirty.push(index as u16);
        }
    }

    /// The event loop's poll: polls the CQ once and hands every request
    /// that retired with an owner (and was not taken otherwise) to `f`, with
    /// the worker, in each queue's order; returns the number of completions
    /// polled. `f` may post new work, take or detach requests, even remove
    /// QPs. Nothing is allocated.
    ///
    /// Requests without an owner (detached) release their buffers when they
    /// retire; their first error is reported by [`Worker::take_send_error`].
    pub fn poll<F: FnMut(&mut Worker<N>, Completion<N>)>(&mut self, mut f: F) -> usize {
        let n = self.progress();
        while let Some(index) = self.dirty.pop() {
            let index = index as usize;
            match self.qps.get_mut(index) {
                Some(Some(qp)) => qp.dirty = false,
                _ => continue,
            }
            self.deliver(index, Queue::Send, &mut f);
            self.deliver(index, Queue::Recv, &mut f);
        }
        n
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

    /// Connects the QP to `remote` (RESET to INIT, RTR and RTS).
    pub fn connect<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        attr: &ConnectAttr,
        remote: &Endpoint,
    ) -> Result<()> {
        self.entry_mut(qp.index()).hw.connect(attr, remote)
    }

    /// Moves the QP to the error state: every request in flight completes
    /// with a flush error and hands its buffers back.
    pub fn to_error<T: Transport>(&mut self, qp: QpKey<T>) -> Result<()> {
        self.entry_mut(qp.index()).hw.to_error()
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
        // SAFETY: `start` was called (`started`).
        match unsafe { qp.hw.complete() } {
            Ok(()) => {
                qp.send.commit();
                Ok(())
            }
            Err(e) => {
                qp.send.abort();
                self.mark_dirty(index);
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
        // SAFETY: `start` was called (`started`).
        match unsafe { entry.hw.complete() } {
            Ok(()) => {
                entry.send.commit();
                Ok(())
            }
            Err(e) => {
                let back = entry.send.abort_keeping(seq).unwrap_or_default();
                self.mark_dirty(index);
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
        // below, which keeps it until the receive retires or the QP is
        // destroyed; `can_post` checked the queue's room.
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
    use crate::nic::QpCaps;
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
        w.remove_qp(a);
        assert_eq!(w.reserved(), 32);
        let c = w.create_qp::<RC>(caps(16), 4).unwrap();
        assert_eq!(c.index(), a.index(), "the index is reused");
        assert_ne!(w.qp_num(c), w.qp_num(b));
    }

    /// A refused doorbell drops the requests written before it: one
    /// without an owner is reported as a flush error, one with an owner
    /// retires with it (and `poll` hands it out), and numbers are not
    /// reused.
    #[test]
    fn refused_doorbell_retires_staged_requests() {
        let pd = Context::new(Model::new(7)).alloc_pd().unwrap();
        let mut w = Worker::new(&pd, 64).unwrap();
        // Not connected: the model refuses the doorbell.
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
        assert!(matches!(error, Error::Os(_)));
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
