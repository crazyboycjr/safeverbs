//! A connected queue pair driven synchronously: requests are written into
//! the completion ring and the hardware queue together, committed with one
//! doorbell, and retired by polling the CQs; the caller takes each
//! request's completion and buffers back by its sequence number.
//!
//! This is the layer the futures of `safeverbs` are built on, and the one
//! for callers who drive their own loop.
//!
//! Each QP has a send CQ and a receive CQ of its own, sized to its queues,
//! so a CQ can never overrun: every WQE in flight produces at most one
//! completion and the rings never let more WQEs be in flight than the
//! queues hold. (CQs shared by several QPs would reserve each QP's depth
//! when it attaches; that, and the demultiplexing it needs, is future work.)

use core::marker::PhantomData;
use core::mem::ManuallyDrop;
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

/// Completions taken from a CQ per poll.
const POLL_BATCH: usize = 16;

const EMPTY_WC: Wc = Wc::success(Opcode(0), 0);

/// A connected queue pair with its completion rings (the synchronous
/// layer).
///
/// Dropping it destroys the hardware QP first, so the NIC stops, and only
/// then releases the buffers still held by requests in flight.
pub struct QueuePair<T: Transport, N: Nic = Verbs> {
    hw: ManuallyDrop<N::Qp>,
    send_cq: ManuallyDrop<N::Cq>,
    recv_cq: ManuallyDrop<N::Cq>,
    send: Ring<Leases<N>>,
    recv: Ring<Leases<N>>,
    pd: Pd<N>,
    caps: QpCaps,
    /// Whether WQEs were written since the last doorbell (`start` called).
    started: bool,
    _t: PhantomData<T>,
}

/// The WQE a request holds back until it knows whether it is its last.
struct Held {
    wr_id: u64,
    op: WrOp,
    sges: [Sge; MAX_SGES],
    n: usize,
}

impl<T: Transport, N: Nic> QueuePair<T, N> {
    /// Creates a QP in `pd` (in the RESET state) with queues of `caps` and
    /// requests of at most `max_batch` WQEs, and a CQ for each queue. With
    /// `local` the QP is meant for one user at a time and created without
    /// the driver's locks where the device allows.
    ///
    /// The sizes are checked against what the device supports.
    pub fn new(pd: &Pd<N>, caps: QpCaps, max_batch: u32, local: bool) -> Result<QueuePair<T, N>> {
        const {
            assert!(
                T::IN_ORDER_COMPLETION,
                "only in-order transports are supported"
            );
        }
        let ctx = pd.context();
        let dev = ctx.nic().caps();
        if caps.max_send_wr == 0 || caps.max_recv_wr == 0 {
            return Err(Error::Invalid("queues need at least one WQE"));
        }
        if caps.max_send_wr > dev.max_qp_wr || caps.max_recv_wr > dev.max_qp_wr {
            return Err(Error::Invalid(
                "queue deeper than the device allows (max_qp_wr)",
            ));
        }
        if caps.max_send_wr > dev.max_cqe || caps.max_recv_wr > dev.max_cqe {
            return Err(Error::Invalid(
                "queue deeper than a CQ of the device (max_cqe)",
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
        let send_cq = ctx.nic().create_cq(caps.max_send_wr, true)?;
        let recv_cq = ctx.nic().create_cq(caps.max_recv_wr, true)?;
        let hw = ctx
            .nic()
            .create_qp(pd.raw(), &send_cq, &recv_cq, T::QP_TYPE, &caps, local)?;
        // Twice the depth: requests in flight never take more than the depth
        // (each has a WQE at least); the rest is room for results not taken
        // yet.
        let send_slots = 2 * caps.max_send_wr as usize;
        let recv_slots = 2 * caps.max_recv_wr as usize;
        Ok(QueuePair {
            hw: ManuallyDrop::new(hw),
            send_cq: ManuallyDrop::new(send_cq),
            recv_cq: ManuallyDrop::new(recv_cq),
            send: Ring::new(send_slots, caps.max_send_wr, max_batch),
            recv: Ring::new(recv_slots, caps.max_recv_wr, 1),
            pd: pd.clone(),
            caps,
            started: false,
            _t: PhantomData,
        })
    }

    /// The QP number.
    pub fn qp_num(&self) -> u32 {
        self.hw.qp_num()
    }

    /// The protection domain.
    pub fn pd(&self) -> &Pd<N> {
        &self.pd
    }

    /// The queues' sizes.
    pub fn caps(&self) -> QpCaps {
        self.caps
    }

    /// Whether the QP was created without the driver's locks (on a thread
    /// domain).
    pub fn is_lock_free(&self) -> bool {
        self.hw.is_lock_free()
    }

    /// The send queue's ring.
    pub fn send_ring(&self) -> &Ring<Leases<N>> {
        &self.send
    }

    /// The receive queue's ring.
    pub fn recv_ring(&self) -> &Ring<Leases<N>> {
        &self.recv
    }

    /// How peers reach this QP through `port`, by GID `gid_index` if given.
    pub fn endpoint(&self, port: u8, gid_index: Option<u8>) -> Result<Endpoint> {
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
            qp_num: self.qp_num(),
            lid: info.lid,
            gid,
        })
    }

    /// Connects the QP to `remote` (RESET to INIT, RTR and RTS).
    pub fn connect(&mut self, attr: &ConnectAttr, remote: &Endpoint) -> Result<()> {
        self.hw.connect(attr, remote)
    }

    /// Moves the QP to the error state: every request in flight completes
    /// with a flush error and hands its buffers back.
    pub fn to_error(&mut self) -> Result<()> {
        self.hw.to_error()
    }

    /// Starts a request on the send queue. Its WQEs go out at the next
    /// [`QueuePair::commit`].
    pub fn request(&mut self) -> Request<'_, T, N> {
        Request {
            qp: self,
            open: false,
            leases: Leases::default(),
            held: None,
            bytes: 0,
        }
    }

    /// Rings the doorbell for every request written since the last one. If
    /// the device refuses, those requests are dropped and their buffers
    /// released.
    pub fn commit(&mut self) -> Result<()> {
        if !self.started {
            return Ok(());
        }
        self.started = false;
        // SAFETY: `start` was called (`started`).
        match unsafe { self.hw.complete() } {
            Ok(()) => {
                self.send.commit();
                Ok(())
            }
            Err(e) => {
                self.send.abort();
                Err(e)
            }
        }
    }

    /// Like [`QueuePair::commit`], but if the device refuses, hands back
    /// the buffers of request `seq` (staged, with an owner) with the error
    /// instead of releasing them.
    pub fn commit_or_take(&mut self, seq: Seq) -> Result<(), (Error, Leases<N>)> {
        if !self.started {
            return Ok(());
        }
        self.started = false;
        // SAFETY: `start` was called (`started`).
        match unsafe { self.hw.complete() } {
            Ok(()) => {
                self.send.commit();
                Ok(())
            }
            Err(e) => Err((e, self.send.abort_keeping(seq).unwrap_or_default())),
        }
    }

    /// Polls the send CQ once, retiring what completed; returns the number
    /// of completions.
    pub fn poll_send(&mut self) -> usize {
        let mut wcs = [EMPTY_WC; POLL_BATCH];
        let n = self.send_cq.poll(&mut wcs);
        let mut i = 0;
        while i < n {
            let known = self.send.complete(wcs[i].wr_id, wcs[i]);
            debug_assert!(known, "a completion for an unknown WQE: {:?}", wcs[i]);
            i += 1;
        }
        n
    }

    /// Polls the receive CQ once, retiring what completed; returns the
    /// number of completions.
    pub fn poll_recv(&mut self) -> usize {
        let mut wcs = [EMPTY_WC; POLL_BATCH];
        let n = self.recv_cq.poll(&mut wcs);
        let mut i = 0;
        while i < n {
            let known = self.recv.complete(wcs[i].wr_id, wcs[i]);
            debug_assert!(known, "a completion for an unknown receive: {:?}", wcs[i]);
            i += 1;
        }
        n
    }

    /// Takes the completion and buffers of retired send request `seq`.
    pub fn take_send(&mut self, seq: Seq) -> Option<(Wc, Leases<N>)> {
        self.send.take(seq)
    }

    /// Takes the completion and buffer of retired receive `seq`.
    pub fn take_recv(&mut self, seq: Seq) -> Option<(Wc, Leases<N>)> {
        self.recv.take(seq)
    }

    /// Gives up send request `seq`: its buffers are released when it
    /// retires (now, if it has).
    pub fn detach_send(&mut self, seq: Seq) {
        self.send.detach(seq);
    }

    /// Gives up receive `seq`: its buffer is released when it retires (now,
    /// if it has).
    pub fn detach_recv(&mut self, seq: Seq) {
        self.recv.detach(seq);
    }

    /// The first error completion of a send request without an owner
    /// (a dropped future's, a sink's, a split batch's part), once.
    pub fn take_send_error(&mut self) -> Option<Wc> {
        self.send.take_detached_error()
    }

    /// Wakes `waker` when send request `seq` retires.
    pub fn set_send_waker(&mut self, seq: Seq, waker: &Waker) {
        self.send.set_waker(seq, waker);
    }

    /// Wakes `waker` when receive `seq` retires.
    pub fn set_recv_waker(&mut self, seq: Seq, waker: &Waker) {
        self.recv.set_waker(seq, waker);
    }

    /// Waits until a new send request of at least one WQE fits: rings the
    /// doorbell for what is written and polls the send CQ until a slot and
    /// a WQE are free. Fails if every slot holds a result nobody took.
    pub fn make_send_room(&mut self) -> Result<()> {
        loop {
            if self.send.can_post() {
                return Ok(());
            }
            if self.send.is_clogged() {
                return Err(Error::Clogged);
            }
            self.commit()?;
            if self.poll_send() == 0 {
                core::hint::spin_loop();
            }
        }
    }

    /// Posts a receive into `part`, a unique buffer; `detached` if nobody
    /// will take it back by its sequence number. Refused when the receive
    /// queue is full.
    pub fn post_recv(&mut self, part: Part<N>, detached: bool) -> Result<Seq, BufError<Part<N>>> {
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
        if !self.recv.can_post() {
            let error = if self.recv.is_clogged() {
                Error::Clogged
            } else {
                Error::QueueFull
            };
            return Err(BufError { error, buf: part });
        }
        let seq = self.recv.begin().expect("a free slot");
        let wr_id = self.recv.push_wqe().expect("room for a WQE");
        // SAFETY: the buffer is unique (checked) and moves into the ring
        // below, which keeps it until the receive retires or the QP is
        // destroyed; `can_post` checked the queue's room.
        match unsafe { self.hw.post_recv(wr_id, &[part.sge]) } {
            Ok(()) => {
                let implied = Wc::success(Opcode::RECV, part.sge.length);
                let mut leases = Leases::default();
                leases.push(part.lease);
                self.recv.end(true, detached, implied, leases);
                self.recv.commit();
                Ok(seq)
            }
            Err(error) => {
                self.recv.abort();
                Err(BufError { error, buf: part })
            }
        }
    }

    /// Writes `held` into the hardware queue.
    fn hw_post(&mut self, held: &Held, signaled: bool) {
        // SAFETY: the request's leases keep the memory valid until the WQE
        // retires (they move into the ring with it); the ring handed out
        // its id, so the queue has room; `start` is called once per
        // doorbell.
        unsafe {
            if !self.started {
                self.hw.start();
                self.started = true;
            }
            self.hw
                .post(held.wr_id, signaled, &held.op, &held.sges[..held.n]);
        }
    }
}

impl<T: Transport, N: Nic> Drop for QueuePair<T, N> {
    fn drop(&mut self) {
        // SAFETY: each handle is dropped once, the QP before its CQs; the
        // fields are not used again.
        unsafe {
            if self.started {
                self.hw.abort();
            }
            ManuallyDrop::drop(&mut self.hw);
            ManuallyDrop::drop(&mut self.send_cq);
            ManuallyDrop::drop(&mut self.recv_cq);
        }
        // The NIC is done with every buffer now.
        self.send.release_all();
        self.recv.release_all();
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

/// A request being written: one or more WQEs that retire together.
///
/// Each WQE is held back until the next one arrives (or the request is
/// finished), so that the last can be written signaled. A request longer
/// than the QP's `max_batch` WQEs, or than the room in the send queue, is
/// split: the part so far goes out as a request of its own (signaled,
/// without an owner) and the rest continues once there is room. Dropping
/// an unfinished request finishes it without an owner.
pub struct Request<'q, T: Transport, N: Nic = Verbs> {
    qp: &'q mut QueuePair<T, N>,
    open: bool,
    leases: Leases<N>,
    held: Option<Held>,
    bytes: u64,
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
                    max: self.qp.caps.max_send_sge as usize,
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
        let max = self.qp.caps.max_send_sge as usize;
        if sges.is_empty() || sges.len() > max {
            return Err(Error::TooManySges {
                got: sges.len(),
                max,
            });
        }
        let writes_local = matches!(
            op,
            WrOp::Read(_) | WrOp::FetchAdd { .. } | WrOp::CompareSwap { .. }
        );
        let mut total = 0u64;
        let mut i = 0;
        while i < sges.len() {
            if owners[i].0 != self.qp.pd.id() {
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
            self.qp.make_send_room()?;
            self.qp.send.begin().expect("room was made");
            self.open = true;
        } else if self.qp.send.wqe_room() == 0 {
            self.close(true, true);
            self.qp.make_send_room()?;
            self.qp.send.begin().expect("room was made");
            self.open = true;
        }
        let wr_id = self.qp.send.push_wqe().expect("room for a WQE");
        let mut held = Held {
            wr_id,
            op,
            sges: [Sge::default(); MAX_SGES],
            n: sges.len(),
        };
        held.sges[..sges.len()].copy_from_slice(sges);
        if let Some(prev) = self.held.replace(held) {
            self.qp.hw_post(&prev, false);
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
        let (seq, signaled) = self.qp.send.end(signal, detached, implied, leases);
        self.qp.hw_post(&held, signaled);
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
    /// commit.
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
        Some(self.qp.commit_or_take(seq).map(|()| seq))
    }

    /// Finishes the request without an owner (signaled) and rings the
    /// doorbell.
    pub fn submit_detached(mut self) -> Result<()> {
        if self.open {
            self.close(true, true);
        }
        self.qp.commit()
    }
}

impl<T: Transport, N: Nic> Drop for Request<'_, T, N> {
    fn drop(&mut self) {
        if self.open {
            self.close(true, true);
        }
    }
}
