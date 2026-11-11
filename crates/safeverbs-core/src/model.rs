//! A simulated NIC in pure Rust: [`Nic`] without hardware, for tests.
//!
//! Every device opened from one [`Model`] (and its clones) shares one fabric
//! in which QPs connect by number. Work requests are carried out when a CQ
//! is polled: each poll runs a random number of steps, each step taking the
//! oldest WQE of a random QP that has work, so work completes in any order
//! the transports allow (each QP's queues in order, QPs interleaved at
//! random) and with random delays. Data really moves between the registered
//! buffers, and the NIC's checks are modeled: keys, protection domains,
//! bounds and access rights, receive buffers too small, receivers not ready
//! (RC retries as its RNR retry count says, 7 for ever; UC drops), more RDMA
//! READs and atomics than the responder takes, a responder's QP going to the
//! error state on the errors IB's Table 58 puts in its Class C, the error
//! state flushing every outstanding WQE (signaled or not) with its own
//! completion. Errors, flushes and refused doorbells can be injected
//! ([`Model::inject_error`], [`Model::force_error`],
//! [`Model::refuse_doorbells`]).
//! MR close refusals can be injected with [`Model::refuse_mr_closes`].
//! The next registration can be refused with [`Model::refuse_next_mr_registration`].
//!
//! The model panics where hardware would silently misbehave and safeverbs
//! promises it never happens: a send or receive queue overflow, a CQ
//! overrun, a doorbell rung for nothing started. It also checks the
//! preconditions of the NIC model M (`safeverbs_spec::nic`) that the
//! library must meet: work is posted only on a QP that left RESET, a QP is
//! destroyed only when every WQE posted on it was retired by a polled
//! completion (its own or a later one of its queue, as M's `poll_cq`
//! retires them), and a CQ only when no QP is attached to it. Every status a
//! send WQE fails with, an injected one included, is one M allows for it
//! (`Qp::allows_send_status`: by its transport, its operation and its RNR
//! retry count); the model panics otherwise.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::nic::{
    Access, ConnectAttr, DeviceCaps, Endpoint, GidEntry, Nic, NicCq, NicQp, PortInfo, QpCaps,
    QpType, Sge, WrOp,
};
use crate::wc::{Opcode, Status, Wc};

/// Linux's `EINVAL`.
const EINVAL: i32 = 22;
/// Linux's `EAGAIN`, for an injected refusal.
const EAGAIN: i32 = 11;

/// A simulated RDMA device on a simulated fabric.
#[derive(Clone)]
pub struct Model {
    fabric: Arc<Mutex<Fabric>>,
}

/// The QP's state as the model tracks it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum QpState {
    Reset,
    Ready,
    Error,
}

struct SendWqe {
    wr_id: u64,
    signaled: bool,
    op: WrOp,
    sges: Vec<Sge>,
}

struct RecvWqe {
    wr_id: u64,
    sges: Vec<Sge>,
}

struct QpRec {
    qp_type: QpType,
    pd: u32,
    send_cq: u32,
    recv_cq: u32,
    caps: QpCaps,
    state: QpState,
    remote: u32,
    access: Access,
    sq: VecDeque<SendWqe>,
    rq: VecDeque<RecvWqe>,
    /// The RNR retry count it was connected with (7: for ever).
    rnr_retry: u8,
    /// RNR NAKs the WQE at the head of the send queue may still take.
    rnr_left: u8,
    /// RDMA READs and atomics it sends back to back (its initiator depth).
    max_rd_atomic: u8,
    /// RDMA READs and atomics of its peer it takes at once (its responder depth).
    max_dest_rd_atomic: u8,
    /// The READs and atomics sent together that the head of the send queue
    /// belongs to, and how many of them are still to execute.
    reads_sent: u32,
    reads_left: u32,
    /// Errors to inject into the next WQEs this QP processes.
    inject: VecDeque<Status>,
    /// The `wr_id`s of the send WQEs posted and not retired by a polled
    /// completion, oldest first: what M's send queue holds.
    unretired_send: VecDeque<u64>,
    /// The same for the receive queue.
    unretired_recv: VecDeque<u64>,
    /// Doorbells to refuse next.
    refuse: u32,
}

struct MrRec {
    addr: usize,
    len: usize,
    access: Access,
    pd: u32,
    /// Close attempts to refuse before allowing deregistration.
    refuse_close: u32,
}

struct CqRec {
    capacity: usize,
    entries: VecDeque<Cqe>,
}

/// A completion waiting in a CQ, with the QP and the queue it completes.
struct Cqe {
    wc: Wc,
    qp: u32,
    recv: bool,
}

struct Fabric {
    rng: u64,
    next_id: u32,
    mrs: BTreeMap<u32, MrRec>,
    /// Refuse the next registration without creating an MR.
    refuse_registration: bool,
    cqs: BTreeMap<u32, CqRec>,
    qps: BTreeMap<u32, QpRec>,
    /// Chance in 1000 that a poll carries out nothing.
    delay_permille: u32,
    /// The most steps one poll carries out.
    burst: u32,
    /// The device carries out nothing, not even flushes.
    halted: bool,
}

impl Model {
    /// A device on a new fabric; `seed` makes its choices reproducible.
    pub fn new(seed: u64) -> Model {
        Model {
            fabric: Arc::new(Mutex::new(Fabric {
                rng: seed | 1,
                next_id: 1,
                mrs: BTreeMap::new(),
                refuse_registration: false,
                cqs: BTreeMap::new(),
                qps: BTreeMap::new(),
                delay_permille: 0,
                burst: 4,
                halted: false,
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Fabric> {
        self.fabric.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Sets the chance in 1000 that a poll carries out nothing, and the most
    /// steps (WQEs) one poll carries out.
    pub fn set_pace(&self, delay_permille: u32, burst: u32) {
        let mut f = self.lock();
        f.delay_permille = delay_permille.min(999);
        f.burst = burst.max(1);
    }

    /// Halts the device (or lets it go on): halted, it carries out no work
    /// request and flushes nothing, as a device that hung would.
    pub fn halt(&self, halted: bool) {
        self.lock().halted = halted;
    }

    /// Makes the next WQE QP `qp_num` processes fail with `status`, which
    /// puts the QP into the error state.
    pub fn inject_error(&self, qp_num: u32, status: Status) {
        if let Some(qp) = self.lock().qps.get_mut(&qp_num) {
            qp.inject.push_back(status);
        }
    }

    /// Makes the device refuse the next `n` doorbells of QP `qp_num`
    /// (`ibv_wr_complete` failing): the WQEs written since the start are
    /// not posted.
    pub fn refuse_doorbells(&self, qp_num: u32, n: u32) {
        if let Some(qp) = self.lock().qps.get_mut(&qp_num) {
            qp.refuse += n;
        }
    }

    /// Refuses the next MR registration on this fabric with `EAGAIN`.
    /// Clones share this one-shot refusal; existing MRs are unaffected.
    pub fn refuse_next_mr_registration(&self) {
        self.lock().refuse_registration = true;
    }

    /// Refuses the next `n` close attempts for the MR with `lkey`, in addition
    /// to any refusals already pending. A refusal returns `EAGAIN` and leaves
    /// the registration and its keys usable. Unknown keys are ignored.
    pub fn refuse_mr_closes(&self, lkey: u32, n: u32) {
        if let Some(mr) = self.lock().mrs.get_mut(&lkey) {
            mr.refuse_close = mr.refuse_close.saturating_add(n);
        }
    }

    /// Puts QP `qp_num` into the error state, as an asynchronous error
    /// would: its outstanding WQEs are flushed.
    pub fn force_error(&self, qp_num: u32) {
        if let Some(qp) = self.lock().qps.get_mut(&qp_num) {
            qp.state = QpState::Error;
        }
    }

    /// Carries out work until nothing more can progress; returns the steps.
    pub fn run(&self) -> usize {
        let mut f = self.lock();
        let mut steps = 0;
        while f.step() {
            steps += 1;
        }
        steps
    }

    /// WQEs posted and not carried out, on every QP.
    pub fn pending(&self) -> usize {
        let f = self.lock();
        let mut n = 0;
        for qp in f.qps.values() {
            n += qp.sq.len() + qp.rq.len();
        }
        n
    }

    /// Memory regions registered and not deregistered.
    pub fn registered(&self) -> usize {
        self.lock().mrs.len()
    }
}

impl Fabric {
    fn random(&mut self) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng
    }

    fn id(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Queues `wc`, a completion of QP `qp`'s receive queue if `recv`, else
    /// of its send queue.
    fn push_cqe(&mut self, cq: u32, wc: Wc, qp: u32, recv: bool) {
        if let Some(cq) = self.cqs.get_mut(&cq) {
            assert!(cq.entries.len() < cq.capacity, "model: CQ overrun");
            cq.entries.push_back(Cqe { wc, qp, recv });
        }
    }

    /// Polling `cqe` retires its WQE and, on a send queue, every earlier
    /// one (unsignaled WQEs that succeeded), as M's `poll_cq` does.
    fn retire(&mut self, cqe: &Cqe) {
        let num = cqe.qp;
        let qp = self
            .qps
            .get_mut(&num)
            .unwrap_or_else(|| panic!("model: a completion of QP {num}, destroyed"));
        if cqe.recv {
            let oldest = qp.unretired_recv.pop_front();
            assert_eq!(
                oldest,
                Some(cqe.wc.wr_id),
                "model: a receive completes out of order"
            );
        } else {
            loop {
                match qp.unretired_send.pop_front() {
                    Some(id) if id == cqe.wc.wr_id => break,
                    Some(_) => {}
                    None => panic!("model: a completion of no posted send WQE"),
                }
            }
        }
    }

    /// Whether QP `num` has a WQE it could process now.
    fn ready(&self, num: u32) -> bool {
        let Some(qp) = self.qps.get(&num) else {
            return false;
        };
        match qp.state {
            QpState::Reset => false,
            QpState::Error => !qp.sq.is_empty() || !qp.rq.is_empty(),
            QpState::Ready => {
                let Some(wqe) = qp.sq.front() else {
                    return false;
                };
                if !qp.inject.is_empty() || qp.qp_type == QpType::Uc {
                    return true;
                }
                // An RC SEND or WRITE with immediate waits for a receive, for
                // ever with an RNR retry count of 7, else through its RNR NAKs.
                match wqe.op {
                    WrOp::Send | WrOp::SendImm(_) | WrOp::WriteImm(..) => {
                        match self.qps.get(&qp.remote) {
                            Some(peer) if peer.state == QpState::Ready => {
                                !peer.rq.is_empty() || qp.rnr_retry < 7
                            }
                            _ => true,
                        }
                    }
                    _ => true,
                }
            }
        }
    }

    /// Carries out one WQE of a random QP that can progress.
    fn step(&mut self) -> bool {
        if self.halted {
            return false;
        }
        let mut ready = Vec::new();
        for &num in self.qps.keys() {
            if self.ready(num) {
                ready.push(num);
            }
        }
        if ready.is_empty() {
            return false;
        }
        let pick = ready[(self.random() % ready.len() as u64) as usize];
        self.process(pick);
        true
    }

    /// Validates `sges` as local memory of `pd` (writable if `write`);
    /// returns the total length.
    fn check_local(&self, pd: u32, sges: &[Sge], write: bool) -> Option<usize> {
        let mut total: usize = 0;
        for sge in sges {
            let mr = self.mrs.get(&sge.lkey)?;
            if mr.pd != pd || (write && !mr.access.contains(Access::LOCAL_WRITE)) {
                return None;
            }
            let start = usize::try_from(sge.addr).ok()?;
            let end = start.checked_add(sge.length as usize)?;
            let region_end = mr.addr.checked_add(mr.len)?;
            if start < mr.addr || end > region_end {
                return None;
            }
            total = total.checked_add(sge.length as usize)?;
        }
        Some(total)
    }

    /// Validates `len` bytes at `addr` under `rkey` for a peer QP of `pd`
    /// asking for `right`.
    fn check_remote(&self, pd: u32, rkey: u32, addr: u64, len: usize, right: Access) -> bool {
        match self.mrs.get(&rkey) {
            Some(mr) => {
                let Ok(start) = usize::try_from(addr) else {
                    return false;
                };
                let Some(end) = start.checked_add(len) else {
                    return false;
                };
                let Some(region_end) = mr.addr.checked_add(mr.len) else {
                    return false;
                };
                mr.pd == pd && mr.access.contains(right) && start >= mr.addr && end <= region_end
            }
            None => false,
        }
    }

    fn fail(&mut self, num: u32, cq: u32, wr_id: u64, status: Status, recv: bool) {
        let qp = self.qps.get_mut(&num).expect("model: QP");
        qp.state = QpState::Error;
        self.push_cqe(cq, error_wc(wr_id, status), num, recv);
    }

    /// Send WQE `wqe` of QP `num` fails with `status`, which M must allow it.
    fn fail_send(&mut self, num: u32, cq: u32, wqe: &SendWqe, status: Status) {
        let qp = self.qps.get(&num).expect("model: QP");
        assert!(
            allows_send_status(qp, &wqe.op, status),
            "model: a {:?} WQE of a {:?} QP with RNR retry {} cannot fail with {status:?} (M)",
            wqe.op,
            qp.qp_type,
            qp.rnr_retry
        );
        self.fail(num, cq, wqe.wr_id, status, false);
    }

    /// The responder QP `num` meets an error of IB's Responder Class C
    /// (Table 58: an R_Key violation, a SEND longer than its receive, more
    /// RDMA READs or atomics than it takes): it goes to the error state.
    fn responder_error(&mut self, num: u32) {
        if let Some(qp) = self.qps.get_mut(&num) {
            qp.state = QpState::Error;
        }
    }

    fn process(&mut self, num: u32) {
        let qp = self.qps.get_mut(&num).expect("model: QP");
        let (send_cq, recv_cq, pd, qp_type, remote) =
            (qp.send_cq, qp.recv_cq, qp.pd, qp.qp_type, qp.remote);
        if qp.state == QpState::Error {
            if let Some(wqe) = qp.sq.pop_front() {
                self.push_cqe(
                    send_cq,
                    error_wc(wqe.wr_id, Status::WR_FLUSH_ERR),
                    num,
                    false,
                );
            } else if let Some(wqe) = qp.rq.pop_front() {
                self.push_cqe(
                    recv_cq,
                    error_wc(wqe.wr_id, Status::WR_FLUSH_ERR),
                    num,
                    true,
                );
            }
            return;
        }
        let injected = qp.inject.pop_front();
        let wqe = qp.sq.pop_front().expect("model: a ready QP has a WQE");
        if let Some(status) = injected {
            self.fail_send(num, send_cq, &wqe, status);
            return;
        }
        let local_write = matches!(
            wqe.op,
            WrOp::Read(_) | WrOp::FetchAdd { .. } | WrOp::CompareSwap { .. }
        );
        let Some(len) = self.check_local(pd, &wqe.sges, local_write) else {
            self.fail_send(num, send_cq, &wqe, Status::LOC_PROT_ERR);
            return;
        };
        let peer = match self.qps.get(&remote) {
            Some(p) if p.state == QpState::Ready && p.remote == num => {
                Some((p.pd, p.recv_cq, p.access, p.max_dest_rd_atomic))
            }
            _ => None,
        };
        let Some((peer_pd, peer_cq, peer_access, peer_depth)) = peer else {
            // Nobody answers: RC runs out of retries, UC loses the message.
            if qp_type == QpType::Rc {
                self.fail_send(num, send_cq, &wqe, Status::RETRY_EXC_ERR);
            } else {
                self.done(num, send_cq, &wqe, len);
            }
            return;
        };
        let reliable = qp_type == QpType::Rc;
        if reliable && matches!(wqe.op, WrOp::Send | WrOp::SendImm(_) | WrOp::WriteImm(..)) {
            let peer = self.qps.get(&remote).expect("model: peer");
            if peer.rq.is_empty() {
                // Not ready: an RNR NAK, retried while the count allows
                // (`ready` waits instead when it is 7).
                let qp = self.qps.get_mut(&num).expect("model: QP");
                if qp.rnr_left == 0 {
                    self.fail_send(num, send_cq, &wqe, Status::RNR_RETRY_EXC_ERR);
                } else {
                    qp.rnr_left -= 1;
                    qp.sq.push_front(wqe);
                }
                return;
            }
        }
        if reliable
            && matches!(
                wqe.op,
                WrOp::Read(_) | WrOp::FetchAdd { .. } | WrOp::CompareSwap { .. }
            )
        {
            // The READs and atomics at the head go out together, up to the
            // initiator depth; the responder takes as many as its own depth
            // and answers the next with an invalid request (IB C9-86).
            let qp = self.qps.get_mut(&num).expect("model: QP");
            if qp.reads_left == 0 {
                let more = qp
                    .sq
                    .iter()
                    .take_while(|w| {
                        matches!(
                            w.op,
                            WrOp::Read(_) | WrOp::FetchAdd { .. } | WrOp::CompareSwap { .. }
                        )
                    })
                    .count() as u32;
                qp.reads_sent = (1 + more).min(u32::from(qp.max_rd_atomic.max(1)));
                qp.reads_left = qp.reads_sent;
            }
            let at = qp.reads_sent - qp.reads_left;
            qp.reads_left -= 1;
            if at >= u32::from(peer_depth) {
                qp.reads_left = 0;
                self.responder_error(remote);
                self.fail_send(num, send_cq, &wqe, Status::REM_INV_REQ_ERR);
                return;
            }
        }
        match wqe.op {
            WrOp::Send | WrOp::SendImm(_) => {
                let imm = match wqe.op {
                    WrOp::SendImm(imm) => Some(imm),
                    _ => None,
                };
                let Some(recv) = self
                    .qps
                    .get_mut(&remote)
                    .expect("model: peer")
                    .rq
                    .pop_front()
                else {
                    // UC drops a message without a receive (RC waits: not ready).
                    self.done(num, send_cq, &wqe, len);
                    return;
                };
                match self.check_local(peer_pd, &recv.sges, true) {
                    Some(room) if room >= len => {
                        let data = self.gather(&wqe.sges, len);
                        self.scatter(&recv.sges, &data);
                        self.push_cqe(
                            peer_cq,
                            recv_wc(recv.wr_id, Opcode::RECV, len, imm),
                            remote,
                            true,
                        );
                        self.done(num, send_cq, &wqe, len);
                    }
                    Some(_) => {
                        self.fail(remote, peer_cq, recv.wr_id, Status::LOC_LEN_ERR, true);
                        if reliable {
                            self.fail_send(num, send_cq, &wqe, Status::REM_INV_REQ_ERR);
                        } else {
                            self.done(num, send_cq, &wqe, len);
                        }
                    }
                    None => {
                        self.fail(remote, peer_cq, recv.wr_id, Status::LOC_PROT_ERR, true);
                        if reliable {
                            self.fail_send(num, send_cq, &wqe, Status::REM_OP_ERR);
                        } else {
                            self.done(num, send_cq, &wqe, len);
                        }
                    }
                }
            }
            WrOp::Write(r) | WrOp::WriteImm(r, _) => {
                let ok = peer_access.contains(Access::REMOTE_WRITE)
                    && self.check_remote(peer_pd, r.rkey, r.addr, len, Access::REMOTE_WRITE);
                if !ok {
                    if reliable {
                        self.responder_error(remote);
                        self.fail_send(num, send_cq, &wqe, Status::REM_ACCESS_ERR);
                    } else {
                        self.done(num, send_cq, &wqe, len);
                    }
                    return;
                }
                let recv = if let WrOp::WriteImm(_, imm) = wqe.op {
                    match self
                        .qps
                        .get_mut(&remote)
                        .expect("model: peer")
                        .rq
                        .pop_front()
                    {
                        Some(recv) => Some((recv, imm)),
                        None => {
                            // UC drops it (RC waits: not ready).
                            self.done(num, send_cq, &wqe, len);
                            return;
                        }
                    }
                } else {
                    None
                };
                let data = self.gather(&wqe.sges, len);
                // SAFETY: inside a registered region (checked), which stays
                // allocated while registered.
                unsafe { core::ptr::copy(data.as_ptr(), r.addr as usize as *mut u8, len) };
                if let Some((recv, imm)) = recv {
                    // The RQ WQE is only a notification credit: even a
                    // zero-SGE receive can report this remote write's length.
                    self.push_cqe(
                        peer_cq,
                        recv_wc(recv.wr_id, Opcode::RECV_RDMA_WITH_IMM, len, Some(imm)),
                        remote,
                        true,
                    );
                }
                self.done(num, send_cq, &wqe, len);
            }
            WrOp::Read(r) => {
                let ok = peer_access.contains(Access::REMOTE_READ)
                    && self.check_remote(peer_pd, r.rkey, r.addr, len, Access::REMOTE_READ);
                if !ok {
                    self.responder_error(remote);
                    self.fail_send(num, send_cq, &wqe, Status::REM_ACCESS_ERR);
                    return;
                }
                let mut data = vec![0u8; len];
                // SAFETY: inside a registered region (checked).
                unsafe { core::ptr::copy(r.addr as usize as *const u8, data.as_mut_ptr(), len) };
                self.scatter(&wqe.sges, &data);
                self.done(num, send_cq, &wqe, len);
            }
            WrOp::FetchAdd { remote: r, .. } | WrOp::CompareSwap { remote: r, .. } => {
                let ok = len == 8
                    && r.addr % 8 == 0
                    && peer_access.contains(Access::REMOTE_ATOMIC)
                    && self.check_remote(peer_pd, r.rkey, r.addr, 8, Access::REMOTE_ATOMIC);
                if !ok {
                    self.responder_error(remote);
                    self.fail_send(num, send_cq, &wqe, Status::REM_ACCESS_ERR);
                    return;
                }
                let target = r.addr as usize as *mut u64;
                // SAFETY: 8 aligned bytes inside a registered region (checked).
                let old = unsafe { core::ptr::read_volatile(target) };
                let new = match wqe.op {
                    WrOp::FetchAdd { add, .. } => old.wrapping_add(add),
                    WrOp::CompareSwap { compare, swap, .. } => {
                        if old == compare {
                            swap
                        } else {
                            old
                        }
                    }
                    _ => old,
                };
                // SAFETY: as above.
                unsafe { core::ptr::write_volatile(target, new) };
                self.scatter(&wqe.sges, &old.to_ne_bytes());
                self.done(num, send_cq, &wqe, len);
            }
        }
    }

    /// A WQE succeeded: its completion, if signaled. An acknowledgement
    /// other than an RNR NAK reloads the RNR retry counter (IB 9.9.2.1.1).
    fn done(&mut self, num: u32, cq: u32, wqe: &SendWqe, len: usize) {
        if let Some(qp) = self.qps.get_mut(&num) {
            qp.rnr_left = qp.rnr_retry;
        }
        if wqe.signaled {
            let opcode = match wqe.op {
                WrOp::Send | WrOp::SendImm(_) => Opcode::SEND,
                WrOp::Write(_) | WrOp::WriteImm(..) => Opcode::RDMA_WRITE,
                WrOp::Read(_) => Opcode::RDMA_READ,
                WrOp::FetchAdd { .. } => Opcode::FETCH_ADD,
                WrOp::CompareSwap { .. } => Opcode::COMP_SWAP,
            };
            let mut wc = Wc::success(opcode, len as u32);
            wc.wr_id = wqe.wr_id;
            self.push_cqe(cq, wc, num, false);
        }
    }

    fn gather(&self, sges: &[Sge], len: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(len);
        for sge in sges {
            // SAFETY: validated against a registered region by the caller.
            let part = unsafe {
                core::slice::from_raw_parts(sge.addr as usize as *const u8, sge.length as usize)
            };
            data.extend_from_slice(part);
        }
        data
    }

    fn scatter(&self, sges: &[Sge], data: &[u8]) {
        let mut at = 0;
        for sge in sges {
            if at >= data.len() {
                break;
            }
            let n = (sge.length as usize).min(data.len() - at);
            // SAFETY: validated against a registered, writable region by the
            // caller; nothing reads it as Rust values meanwhile (a receive or
            // READ buffer is owned by its slot).
            unsafe { core::ptr::copy(data[at..].as_ptr(), sge.addr as usize as *mut u8, n) };
            at += n;
        }
    }
}

/// Whether M lets a send WQE doing `op` on `qp` fail with `status`: a remote
/// status or a retry counter's only on RC (M2), a remote access error only on
/// an RDMA operation or an atomic (M3), the RNR retry counter only below 7
/// (M4). What `Qp::allows_send_status` says, on `Status::model`'s map.
fn allows_send_status(qp: &QpRec, op: &WrOp, status: Status) -> bool {
    let remote = matches!(status.0, 7 | 9 | 10 | 11 | 16);
    let retries = matches!(status.0, 12 | 13);
    let rdma = matches!(
        op,
        WrOp::Write(_)
            | WrOp::WriteImm(..)
            | WrOp::Read(_)
            | WrOp::FetchAdd { .. }
            | WrOp::CompareSwap { .. }
    );
    (!(remote || retries) || qp.qp_type == QpType::Rc)
        && (status != Status::REM_ACCESS_ERR || rdma)
        && (status != Status::RNR_RETRY_EXC_ERR || qp.rnr_retry < 7)
}

fn error_wc(wr_id: u64, status: Status) -> Wc {
    Wc {
        wr_id,
        status,
        opcode: Opcode(0),
        byte_len: 0,
        imm: None,
        vendor_err: 0,
    }
}

fn recv_wc(wr_id: u64, opcode: Opcode, len: usize, imm: Option<u32>) -> Wc {
    Wc {
        wr_id,
        imm,
        ..Wc::success(opcode, len as u32)
    }
}

/// A protection domain of the model.
pub struct ModelPd {
    id: u32,
}

/// A registered region of the model; closed explicitly or on drop.
/// Like the verbs backend, drop aborts if close is refused, since this handle
/// cannot keep the caller's backing memory alive by itself.
pub struct ModelMr {
    fabric: Arc<Mutex<Fabric>>,
    /// None only after successful deregistration.
    key: Option<u32>,
}

impl Drop for ModelMr {
    fn drop(&mut self) {
        if Model::close_mr(self).is_err() {
            // Do not unwind into the caller's backing-memory destructor.
            std::process::abort();
        }
    }
}

/// A thread domain of the model (QPs of the model take no locks anyway).
pub struct ModelTd;

/// A parent domain of the model.
pub struct ModelDomain;

/// A completion queue of the model; polling it carries out work.
pub struct ModelCq {
    fabric: Arc<Mutex<Fabric>>,
    id: u32,
}

impl Drop for ModelCq {
    fn drop(&mut self) {
        let mut f = self.fabric.lock().unwrap_or_else(|e| e.into_inner());
        let id = self.id;
        let attached = f
            .qps
            .values()
            .any(|qp| qp.send_cq == id || qp.recv_cq == id);
        f.cqs.remove(&id);
        drop(f);
        // M's `destroy_cq`.
        assert!(!attached, "model: a CQ destroyed with QPs attached");
    }
}

impl NicCq for ModelCq {
    fn poll(&mut self, out: &mut [Wc]) -> usize {
        let mut f = self.fabric.lock().unwrap_or_else(|e| e.into_inner());
        if f.random() % 1000 >= f.delay_permille as u64 {
            let steps = 1 + f.random() % f.burst as u64;
            let mut i = 0;
            while i < steps && f.step() {
                i += 1;
            }
        }
        let mut n = 0;
        while n < out.len() {
            let cq = f.cqs.get_mut(&self.id).expect("model: CQ");
            match cq.entries.pop_front() {
                Some(cqe) => {
                    f.retire(&cqe);
                    out[n] = cqe.wc;
                    n += 1;
                }
                None => break,
            }
        }
        n
    }
}

/// A queue pair of the model.
pub struct ModelQp {
    fabric: Arc<Mutex<Fabric>>,
    num: u32,
    /// WQEs written since `start`, `None` outside a start.
    staged: Option<Vec<SendWqe>>,
}

impl ModelQp {
    fn lock(&self) -> MutexGuard<'_, Fabric> {
        self.fabric.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Drop for ModelQp {
    fn drop(&mut self) {
        let num = self.num;
        let qp = self.lock().qps.remove(&num);
        // M's `destroy_qp`: the NIC holds no buffer of the QP, and no
        // completion of it is left.
        if let Some(qp) = qp {
            assert!(
                qp.unretired_send.is_empty() && qp.unretired_recv.is_empty(),
                "model: QP {num} destroyed with {} send and {} receive WQEs not retired",
                qp.unretired_send.len(),
                qp.unretired_recv.len()
            );
        }
    }
}

impl NicQp for ModelQp {
    fn qp_num(&self) -> u32 {
        self.num
    }

    fn is_lock_free(&self) -> bool {
        true
    }

    fn connect(&mut self, attr: &ConnectAttr, remote: &Endpoint) -> Result<()> {
        let num = self.num;
        let mut f = self.lock();
        let qp = f.qps.get_mut(&num).expect("model: QP");
        if qp.state != QpState::Reset {
            return Err(Error::Os(EINVAL));
        }
        qp.remote = remote.qp_num;
        qp.access = attr.access;
        qp.rnr_retry = attr.rnr_retry;
        qp.rnr_left = attr.rnr_retry;
        qp.max_rd_atomic = attr.max_rd_atomic;
        qp.max_dest_rd_atomic = attr.max_dest_rd_atomic;
        qp.state = QpState::Ready;
        Ok(())
    }

    fn to_error(&mut self) -> Result<()> {
        let num = self.num;
        let mut f = self.lock();
        f.qps.get_mut(&num).expect("model: QP").state = QpState::Error;
        Ok(())
    }

    unsafe fn start(&mut self) {
        assert!(self.staged.is_none(), "model: start twice");
        self.staged = Some(Vec::new());
    }

    unsafe fn post(&mut self, wr_id: u64, signaled: bool, op: &WrOp, sges: &[Sge]) {
        let staged = self.staged.as_mut().expect("model: post outside a start");
        staged.push(SendWqe {
            wr_id,
            signaled,
            op: *op,
            sges: sges.to_vec(),
        });
    }

    unsafe fn complete(&mut self) -> Result<()> {
        let staged = self.staged.take().expect("model: complete outside a start");
        let num = self.num;
        let mut f = self.lock();
        let qp = f.qps.get_mut(&num).expect("model: QP");
        assert!(
            qp.unretired_send.len() + staged.len() <= qp.caps.max_send_wr as usize,
            "model: send queue overflow"
        );
        // M's `post_send` needs RTS or ERR.
        assert!(qp.state != QpState::Reset, "model: a doorbell in RESET");
        if qp.refuse > 0 {
            qp.refuse -= 1;
            return Err(Error::Os(EAGAIN));
        }
        qp.unretired_send.extend(staged.iter().map(|wqe| wqe.wr_id));
        qp.sq.extend(staged);
        Ok(())
    }

    unsafe fn abort(&mut self) {
        self.staged = None;
    }

    unsafe fn post_recv(&mut self, wr_id: u64, sges: &[Sge]) -> Result<()> {
        let num = self.num;
        let mut f = self.lock();
        let qp = f.qps.get_mut(&num).expect("model: QP");
        // M's `post_recv` needs a QP out of RESET, and room.
        assert!(
            qp.state != QpState::Reset,
            "model: a receive posted in RESET"
        );
        assert!(
            qp.unretired_recv.len() < qp.caps.max_recv_wr as usize,
            "model: receive queue overflow"
        );
        qp.unretired_recv.push_back(wr_id);
        qp.rq.push_back(RecvWqe {
            wr_id,
            sges: sges.to_vec(),
        });
        Ok(())
    }
}

// SAFETY: the fabric serializes all DMA and queue transitions under one
// mutex, validates memory before access, and reports each retired WQE's
// outcome. A QP is destroyed only after every outstanding WQE is retired.
unsafe impl Nic for Model {
    type Pd = ModelPd;
    type Mr = ModelMr;
    type Cq = ModelCq;
    type Qp = ModelQp;
    type Td = ModelTd;
    type Domain = ModelDomain;

    fn name(&self) -> &str {
        "model"
    }

    fn caps(&self) -> DeviceCaps {
        DeviceCaps {
            max_qp_wr: 4096,
            max_sge: 16,
            max_cqe: 1 << 16,
            max_qp_rd_atom: 16,
            atomics: true,
            max_msg: 1 << 31,
        }
    }

    fn port(&self, _port: u8) -> Result<PortInfo> {
        Ok(PortInfo {
            lid: 1,
            active_mtu: 5,
            ethernet: false,
            active: true,
        })
    }

    fn gids(&self, _port: u8) -> Vec<GidEntry> {
        Vec::new()
    }

    unsafe fn alloc_pd(&self) -> Result<ModelPd> {
        Ok(ModelPd {
            id: self.lock().id(),
        })
    }

    unsafe fn reg_mr(
        &self,
        pd: &ModelPd,
        addr: *mut u8,
        len: usize,
        access: Access,
    ) -> Result<ModelMr> {
        let mut f = self.lock();
        if core::mem::take(&mut f.refuse_registration) {
            return Err(Error::Os(EAGAIN));
        }
        let key = f.id();
        f.mrs.insert(
            key,
            MrRec {
                addr: addr as usize,
                len,
                access,
                pd: pd.id,
                refuse_close: 0,
            },
        );
        Ok(ModelMr {
            fabric: self.fabric.clone(),
            key: Some(key),
        })
    }

    fn mr_keys(mr: &ModelMr) -> (u32, u32) {
        let key = mr.key.expect("model: keys of a closed MR");
        (key, key)
    }

    fn close_mr(mr: &mut ModelMr) -> Result<()> {
        let Some(key) = mr.key else {
            return Ok(());
        };
        // Every simulated DMA access holds this mutex. Removing the record
        // under it revokes both keys before any later access can proceed.
        let mut f = mr.fabric.lock().unwrap_or_else(|e| e.into_inner());
        let registration = f
            .mrs
            .get_mut(&key)
            .ok_or(Error::Invalid("model: unregistered MR"))?;
        if registration.refuse_close > 0 {
            registration.refuse_close -= 1;
            return Err(Error::Os(EAGAIN));
        }
        f.mrs.remove(&key);
        mr.key = None;
        Ok(())
    }

    unsafe fn create_cq(&self, entries: u32, _single_threaded: bool) -> Result<ModelCq> {
        let mut f = self.lock();
        let id = f.id();
        f.cqs.insert(
            id,
            CqRec {
                capacity: entries as usize,
                entries: VecDeque::new(),
            },
        );
        Ok(ModelCq {
            fabric: self.fabric.clone(),
            id,
        })
    }

    unsafe fn alloc_td(&self) -> Option<ModelTd> {
        Some(ModelTd)
    }

    unsafe fn alloc_domain(&self, _pd: &ModelPd, _td: &ModelTd) -> Option<ModelDomain> {
        Some(ModelDomain)
    }

    unsafe fn create_qp(
        &self,
        pd: &ModelPd,
        _domain: Option<&ModelDomain>,
        send_cq: &ModelCq,
        recv_cq: &ModelCq,
        qp_type: QpType,
        caps: &QpCaps,
    ) -> Result<ModelQp> {
        let mut f = self.lock();
        let num = f.id();
        f.qps.insert(
            num,
            QpRec {
                qp_type,
                pd: pd.id,
                send_cq: send_cq.id,
                recv_cq: recv_cq.id,
                caps: *caps,
                state: QpState::Reset,
                remote: 0,
                access: Access::default(),
                sq: VecDeque::new(),
                rq: VecDeque::new(),
                rnr_retry: 7,
                rnr_left: 7,
                max_rd_atomic: 0,
                max_dest_rd_atomic: 0,
                reads_sent: 0,
                reads_left: 0,
                inject: VecDeque::new(),
                unretired_send: VecDeque::new(),
                unretired_recv: VecDeque::new(),
                refuse: 0,
            },
        );
        Ok(ModelQp {
            fabric: self.fabric.clone(),
            num,
            staged: None,
        })
    }
}

#[cfg(test)]
mod mr_tests {
    use super::*;

    #[test]
    fn refused_close_retains_registration_for_retry() {
        let model = Model::new(7);
        let pd = unsafe { model.alloc_pd() }.unwrap();
        let mut bytes = [0u8; 16];
        let addr = bytes.as_mut_ptr();
        // SAFETY: the memory, PD and model outlive the MR; no work is posted.
        let mut mr = unsafe {
            model.reg_mr(
                &pd,
                addr,
                bytes.len(),
                Access::LOCAL_WRITE | Access::REMOTE_WRITE,
            )
        }
        .unwrap();
        let (lkey, rkey) = Model::mr_keys(&mr);
        let sge = Sge {
            addr: addr as u64,
            length: bytes.len() as u32,
            lkey,
        };
        model.refuse_mr_closes(lkey, 2);
        for _ in 0..2 {
            assert_eq!(Model::close_mr(&mut mr), Err(Error::Os(EAGAIN)));
            assert_eq!(model.registered(), 1);
            assert_eq!(Model::mr_keys(&mr), (lkey, rkey));
            let fabric = model.lock();
            assert_eq!(fabric.check_local(pd.id, &[sge], true), Some(bytes.len()));
            assert!(fabric.check_remote(
                pd.id,
                rkey,
                addr as u64,
                bytes.len(),
                Access::REMOTE_WRITE,
            ));
        }
        assert_eq!(Model::close_mr(&mut mr), Ok(()));
        assert!(mr.key.is_none());
        assert_eq!(model.registered(), 0);
        {
            let fabric = model.lock();
            assert_eq!(fabric.check_local(pd.id, &[sge], true), None);
            assert!(!fabric.check_remote(
                pd.id,
                rkey,
                addr as u64,
                bytes.len(),
                Access::REMOTE_WRITE,
            ));
        }
        assert_eq!(Model::close_mr(&mut mr), Ok(()));
        drop(mr);
        assert_eq!(model.registered(), 0);
    }

    #[test]
    fn drop_retries_a_previously_refused_close() {
        let model = Model::new(9);
        let pd = unsafe { model.alloc_pd() }.unwrap();
        let mut bytes = [0u8; 16];
        // SAFETY: the memory, PD and model outlive the MR; no work is posted.
        let mut mr =
            unsafe { model.reg_mr(&pd, bytes.as_mut_ptr(), bytes.len(), Access::LOCAL_WRITE) }
                .unwrap();
        model.refuse_mr_closes(Model::mr_keys(&mr).0, 1);
        assert_eq!(Model::close_mr(&mut mr), Err(Error::Os(EAGAIN)));
        assert_eq!(model.registered(), 1);
        drop(mr);
        assert_eq!(model.registered(), 0);
    }
}

#[cfg(test)]
mod range_tests {
    use super::*;

    #[test]
    fn forged_ranges_never_wrap_into_registered_memory() {
        let model = Model::new(1);
        let mut fabric = model.lock();
        fabric.mrs.insert(
            7,
            MrRec {
                addr: 0x1000,
                len: 4096,
                access: Access::LOCAL_WRITE | Access::REMOTE_WRITE | Access::REMOTE_READ,
                pd: 3,
                refuse_close: 0,
            },
        );
        assert!(fabric.check_remote(3, 7, 0x1000, 4096, Access::REMOTE_WRITE));
        assert!(!fabric.check_remote(3, 7, (usize::MAX - 3) as u64, 8, Access::REMOTE_WRITE));
        assert!(!fabric.check_remote(3, 7, 0x1fff, 2, Access::REMOTE_READ));
        assert!(!fabric.check_remote(3, 7, 0x1000, usize::MAX, Access::REMOTE_READ));
        let valid = Sge {
            addr: 0x1000,
            length: 4096,
            lkey: 7,
        };
        assert_eq!(fabric.check_local(3, &[valid], true), Some(4096));
        let forged = Sge {
            addr: (usize::MAX - 3) as u64,
            length: 8,
            ..valid
        };
        assert_eq!(fabric.check_local(3, &[forged], true), None);
    }
}
