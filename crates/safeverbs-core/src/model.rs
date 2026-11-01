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
//! (RC waits, UC drops), the error state flushing every outstanding WQE
//! (signaled or not) with its own completion. Errors and flushes can be
//! injected ([`Model::inject_error`], [`Model::force_error`]).
//!
//! The model panics where hardware would silently misbehave and safeverbs
//! promises it never happens: a send or receive queue overflow, a CQ
//! overrun, a doorbell rung for nothing started.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::nic::{
    Access, ConnectAttr, DeviceCaps, Endpoint, GidEntry, Nic, NicCq, NicQp, PortInfo, QpCaps,
    QpType, Sge, WrOp,
};
use crate::wc::{Opcode, Status, Wc};

/// Linux's `ENOMEM`, for a queue posted past its depth.
const ENOMEM: i32 = 12;
/// Linux's `EINVAL`.
const EINVAL: i32 = 22;

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
    /// Errors to inject into the next WQEs this QP processes.
    inject: VecDeque<Status>,
}

struct MrRec {
    addr: usize,
    len: usize,
    access: Access,
    pd: u32,
}

struct CqRec {
    capacity: usize,
    entries: VecDeque<Wc>,
}

struct Fabric {
    rng: u64,
    next_id: u32,
    mrs: BTreeMap<u32, MrRec>,
    cqs: BTreeMap<u32, CqRec>,
    qps: BTreeMap<u32, QpRec>,
    /// Chance in 1000 that a poll carries out nothing.
    delay_permille: u32,
    /// The most steps one poll carries out.
    burst: u32,
}

impl Model {
    /// A device on a new fabric; `seed` makes its choices reproducible.
    pub fn new(seed: u64) -> Model {
        Model {
            fabric: Arc::new(Mutex::new(Fabric {
                rng: seed | 1,
                next_id: 1,
                mrs: BTreeMap::new(),
                cqs: BTreeMap::new(),
                qps: BTreeMap::new(),
                delay_permille: 0,
                burst: 4,
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

    /// Makes the next WQE QP `qp_num` processes fail with `status`, which
    /// puts the QP into the error state.
    pub fn inject_error(&self, qp_num: u32, status: Status) {
        if let Some(qp) = self.lock().qps.get_mut(&qp_num) {
            qp.inject.push_back(status);
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

    fn push_cqe(&mut self, cq: u32, wc: Wc) {
        if let Some(cq) = self.cqs.get_mut(&cq) {
            assert!(cq.entries.len() < cq.capacity, "model: CQ overrun");
            cq.entries.push_back(wc);
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
                // An RC SEND or WRITE with immediate waits for a receive.
                match wqe.op {
                    WrOp::Send | WrOp::SendImm(_) | WrOp::WriteImm(..) => {
                        match self.qps.get(&qp.remote) {
                            Some(peer) if peer.state == QpState::Ready => !peer.rq.is_empty(),
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
        let mut total = 0;
        for sge in sges {
            let mr = self.mrs.get(&sge.lkey)?;
            if mr.pd != pd || (write && !mr.access.contains(Access::LOCAL_WRITE)) {
                return None;
            }
            let start = sge.addr as usize;
            if start < mr.addr || start + sge.length as usize > mr.addr + mr.len {
                return None;
            }
            total += sge.length as usize;
        }
        Some(total)
    }

    /// Validates `len` bytes at `addr` under `rkey` for a peer QP of `pd`
    /// asking for `right`.
    fn check_remote(&self, pd: u32, rkey: u32, addr: u64, len: usize, right: Access) -> bool {
        match self.mrs.get(&rkey) {
            Some(mr) => {
                let start = addr as usize;
                mr.pd == pd
                    && mr.access.contains(right)
                    && start >= mr.addr
                    && start + len <= mr.addr + mr.len
            }
            None => false,
        }
    }

    fn fail(&mut self, num: u32, cq: u32, wr_id: u64, status: Status) {
        let qp = self.qps.get_mut(&num).expect("model: QP");
        qp.state = QpState::Error;
        self.push_cqe(cq, error_wc(wr_id, status, num));
    }

    fn process(&mut self, num: u32) {
        let qp = self.qps.get_mut(&num).expect("model: QP");
        let (send_cq, recv_cq, pd, qp_type, remote) =
            (qp.send_cq, qp.recv_cq, qp.pd, qp.qp_type, qp.remote);
        if qp.state == QpState::Error {
            if let Some(wqe) = qp.sq.pop_front() {
                self.push_cqe(send_cq, error_wc(wqe.wr_id, Status::WR_FLUSH_ERR, num));
            } else if let Some(wqe) = qp.rq.pop_front() {
                self.push_cqe(recv_cq, error_wc(wqe.wr_id, Status::WR_FLUSH_ERR, num));
            }
            return;
        }
        let injected = qp.inject.pop_front();
        let wqe = qp.sq.pop_front().expect("model: a ready QP has a WQE");
        if let Some(status) = injected {
            self.fail(num, send_cq, wqe.wr_id, status);
            return;
        }
        let local_write = matches!(
            wqe.op,
            WrOp::Read(_) | WrOp::FetchAdd { .. } | WrOp::CompareSwap { .. }
        );
        let Some(len) = self.check_local(pd, &wqe.sges, local_write) else {
            self.fail(num, send_cq, wqe.wr_id, Status::LOC_PROT_ERR);
            return;
        };
        let peer = match self.qps.get(&remote) {
            Some(p) if p.state == QpState::Ready && p.remote == num => {
                Some((p.pd, p.recv_cq, p.access))
            }
            _ => None,
        };
        let Some((peer_pd, peer_cq, peer_access)) = peer else {
            // Nobody answers: RC runs out of retries, UC loses the message.
            if qp_type == QpType::Rc {
                self.fail(num, send_cq, wqe.wr_id, Status::RETRY_EXC_ERR);
            } else {
                self.done(num, send_cq, &wqe, len);
            }
            return;
        };
        let reliable = qp_type == QpType::Rc;
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
                        self.push_cqe(peer_cq, recv_wc(recv.wr_id, Opcode::RECV, len, imm, remote));
                        self.done(num, send_cq, &wqe, len);
                    }
                    Some(_) => {
                        self.fail(remote, peer_cq, recv.wr_id, Status::LOC_LEN_ERR);
                        if reliable {
                            self.fail(num, send_cq, wqe.wr_id, Status::REM_INV_REQ_ERR);
                        } else {
                            self.done(num, send_cq, &wqe, len);
                        }
                    }
                    None => {
                        self.fail(remote, peer_cq, recv.wr_id, Status::LOC_PROT_ERR);
                        if reliable {
                            self.fail(num, send_cq, wqe.wr_id, Status::REM_OP_ERR);
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
                        self.fail(num, send_cq, wqe.wr_id, Status::REM_ACCESS_ERR);
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
                    self.push_cqe(
                        peer_cq,
                        recv_wc(
                            recv.wr_id,
                            Opcode::RECV_RDMA_WITH_IMM,
                            len,
                            Some(imm),
                            remote,
                        ),
                    );
                }
                self.done(num, send_cq, &wqe, len);
            }
            WrOp::Read(r) => {
                let ok = peer_access.contains(Access::REMOTE_READ)
                    && self.check_remote(peer_pd, r.rkey, r.addr, len, Access::REMOTE_READ);
                if !ok {
                    self.fail(num, send_cq, wqe.wr_id, Status::REM_ACCESS_ERR);
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
                    self.fail(num, send_cq, wqe.wr_id, Status::REM_ACCESS_ERR);
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

    /// A WQE succeeded: its completion, if signaled.
    fn done(&mut self, num: u32, cq: u32, wqe: &SendWqe, len: usize) {
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
            wc.qp_num = num;
            self.push_cqe(cq, wc);
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

fn error_wc(wr_id: u64, status: Status, qp_num: u32) -> Wc {
    Wc {
        wr_id,
        status,
        opcode: Opcode(0),
        byte_len: 0,
        imm: None,
        vendor_err: 0,
        qp_num,
    }
}

fn recv_wc(wr_id: u64, opcode: Opcode, len: usize, imm: Option<u32>, qp_num: u32) -> Wc {
    Wc {
        wr_id,
        status: Status::SUCCESS,
        opcode,
        byte_len: len as u32,
        imm,
        vendor_err: 0,
        qp_num,
    }
}

/// A protection domain of the model.
pub struct ModelPd {
    id: u32,
}

/// A registered region of the model; deregistered when dropped.
pub struct ModelMr {
    fabric: Arc<Mutex<Fabric>>,
    key: u32,
}

impl Drop for ModelMr {
    fn drop(&mut self) {
        let mut f = self.fabric.lock().unwrap_or_else(|e| e.into_inner());
        f.mrs.remove(&self.key);
    }
}

/// A thread domain of the model (QPs of the model take no locks anyway).
pub struct ModelDomain;

/// A completion queue of the model; polling it carries out work.
pub struct ModelCq {
    fabric: Arc<Mutex<Fabric>>,
    id: u32,
}

impl Drop for ModelCq {
    fn drop(&mut self) {
        let mut f = self.fabric.lock().unwrap_or_else(|e| e.into_inner());
        f.cqs.remove(&self.id);
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
        let cq = f.cqs.get_mut(&self.id).expect("model: CQ");
        let mut n = 0;
        while n < out.len() {
            match cq.entries.pop_front() {
                Some(wc) => {
                    out[n] = wc;
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
        self.lock().qps.remove(&num);
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
            qp.sq.len() + staged.len() <= qp.caps.max_send_wr as usize,
            "model: send queue overflow"
        );
        if qp.state == QpState::Reset {
            return Err(Error::Os(EINVAL));
        }
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
        if qp.rq.len() >= qp.caps.max_recv_wr as usize {
            return Err(Error::Os(ENOMEM));
        }
        qp.rq.push_back(RecvWqe {
            wr_id,
            sges: sges.to_vec(),
        });
        Ok(())
    }
}

impl Nic for Model {
    type Pd = ModelPd;
    type Mr = ModelMr;
    type Cq = ModelCq;
    type Qp = ModelQp;
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

    fn alloc_pd(&self) -> Result<ModelPd> {
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
        let key = f.id();
        f.mrs.insert(
            key,
            MrRec {
                addr: addr as usize,
                len,
                access,
                pd: pd.id,
            },
        );
        Ok(ModelMr {
            fabric: self.fabric.clone(),
            key,
        })
    }

    fn mr_keys(mr: &ModelMr) -> (u32, u32) {
        (mr.key, mr.key)
    }

    fn create_cq(&self, entries: u32, _single_threaded: bool) -> Result<ModelCq> {
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

    fn alloc_domain(&self, _pd: &ModelPd) -> Option<ModelDomain> {
        Some(ModelDomain)
    }

    fn create_qp(
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
                inject: VecDeque::new(),
            },
        );
        Ok(ModelQp {
            fabric: self.fabric.clone(),
            num,
            staged: None,
        })
    }
}
