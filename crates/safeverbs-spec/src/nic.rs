//! **M**: the RDMA NIC as one endpoint sees it. This is the trusted model.
//!
//! M is a state machine with two kinds of transitions:
//!
//! - **Verbs**, the calls the library makes (`reg_mr`, `create_qp`, `post_send`, `poll_cq`,
//!   ...). Each has an explicit precondition, its `require`s. M says nothing about a call
//!   outside its precondition, and **S2** is the promise that the library never makes one.
//!   The preconditions are what the library can know: a verb on a connected queue pair is
//!   allowed in `Rts` *or* `Err`, since the NIC may move a queue pair to `Err` at any moment
//!   without telling anyone until a completion says so.
//! - **NIC steps**, which happen on their own at any time: executing the next work request
//!   (with any outcome the transport allows, chosen by the peer and the network), and faults.
//!   The peer is not modelled beyond this: it may send anything, write or read any region
//!   it holds an rkey for (see [`Nic::State::nic_may_write`]), never post a receive, or
//!   vanish. Its effects show up only as outcomes and faults.
//!
//! What the NIC may access is derived from the state: the buffers of every work request
//! posted and not yet retired, and the regions registered for remote access. The library
//! hands a buffer to the NIC by posting it and gets it back when a polled completion
//! retires the request; in the implementation these are linear ghost permissions moving
//! between the program and the NIC (see `docs/verification.md`).
//!
//! Completions follow the in-order rule of RC, UC and UD send queues (MRC's are the same)
//! and of receive queues: work requests execute in posting order, an unsignaled one that
//! succeeds produces no completion, and a polled completion retires its work request and
//! every earlier one. An error moves the queue pair to `Err`; from then on every work
//! request completes with `FlushErr`, in order, signaled or not.
#![allow(unused_imports)]

use crate::types::*;
use verus_state_machines_macros::state_machine;
use vstd::multiset::Multiset;
use vstd::prelude::*;

verus! {

/// Device capabilities (`ibv_query_device`), fixed when the device is opened.
pub struct Caps {
    pub max_qp_wr: nat,
    pub max_sge: nat,
    pub max_inline: nat,
    pub max_cqe: nat,
    pub max_msg: nat,
}

/// A registered memory region (`ibv_reg_mr`): the bytes `span`, in protection domain `pd`.
pub struct Mr {
    pub pd: PdId,
    pub span: Span,
    pub local_write: bool,
    pub remote_read: bool,
    pub remote_write: bool,
}

/// A scatter/gather element (`ibv_sge`).
pub struct Sge {
    pub addr: int,
    pub len: nat,
    pub lkey: Key,
}

impl Sge {
    pub open spec fn span(self) -> Span {
        Span { start: self.addr, len: self.len }
    }
}

/// The total length of a scatter/gather list.
pub open spec fn total_len(sges: Seq<Sge>) -> nat
    decreases sges.len(),
{
    if sges.len() == 0 {
        0
    } else {
        sges.last().len + total_len(sges.drop_last())
    }
}

/// Some element of `sges` covers byte `a`.
pub open spec fn covers(sges: Seq<Sge>, a: int) -> bool {
    exists|i: int| 0 <= i < sges.len() && (#[trigger] sges[i]).span().contains(a)
}

/// Some element of `sges` names region `key`.
pub open spec fn names_key(sges: Seq<Sge>, key: Key) -> bool {
    exists|i: int| 0 <= i < sges.len() && (#[trigger] sges[i]).lkey == key
}

/// The transport service of a queue pair. All three complete their queues in posting order.
pub enum Transport {
    Rc,
    Uc,
    Ud,
}

pub enum Opcode {
    Send,
    SendImm(u32),
    Write,
    WriteImm(u32),
    Read,
}

impl Opcode {
    /// The NIC writes the local buffers (RDMA read); otherwise it reads them.
    pub open spec fn writes_local(self) -> bool {
        self is Read
    }

    pub open spec fn allowed_on(self, t: Transport) -> bool {
        match t {
            Transport::Rc => true,
            Transport::Uc => !(self is Read),
            Transport::Ud => self is Send || self is SendImm,
        }
    }
}

/// A send work request (`ibv_send_wr`, one element of its list).
pub struct SendWr {
    pub wr_id: u64,
    pub opcode: Opcode,
    pub sges: Seq<Sge>,
    pub signaled: bool,
    /// The data is copied into the WQE when posted; the NIC never touches the buffers later.
    pub inline: bool,
}

/// A receive work request (`ibv_recv_wr`).
pub struct RecvWr {
    pub wr_id: u64,
    pub sges: Seq<Sge>,
}

pub enum QpState {
    Reset,
    Init,
    Rtr,
    Rts,
    Err,
}

/// The state changes `ibv_modify_qp` accepts (to `Reset` only with empty queues, see
/// `modify_qp`).
pub open spec fn legal(from: QpState, to: QpState) -> bool {
    match (from, to) {
        (_, QpState::Err) => true,
        (_, QpState::Reset) => true,
        (QpState::Reset, QpState::Init) => true,
        (QpState::Init, QpState::Init) => true,
        (QpState::Init, QpState::Rtr) => true,
        (QpState::Rtr, QpState::Rts) => true,
        (QpState::Rts, QpState::Rts) => true,
        _ => false,
    }
}

/// What a queue pair is created with (`ibv_qp_init_attr`).
pub struct QpAttr {
    pub pd: PdId,
    pub transport: Transport,
    pub send_cq: CqId,
    pub recv_cq: CqId,
    pub sq_depth: nat,
    pub rq_depth: nat,
    pub max_sge: nat,
    pub max_inline: nat,
}

/// A posted send WQE and, once the NIC executed it, its outcome.
pub struct SendSlot {
    pub wr: SendWr,
    pub outcome: Option<Outcome>,
}

impl SendSlot {
    /// Executing it produced (or produces) a CQE: it is signaled, or it failed.
    pub open spec fn generates(self) -> bool {
        self.wr.signaled || (self.outcome is Some && self.outcome->0.status.is_err())
    }
}

/// `SendSlot::generates`, as a value (so every count uses the same predicate).
pub open spec fn gen_pred() -> spec_fn(SendSlot) -> bool {
    |s: SendSlot| s.generates()
}

/// How many slots of `s` produce a CQE.
pub open spec fn count_generating(s: Seq<SendSlot>) -> nat {
    s.filter(gen_pred()).len()
}

/// A posted receive WQE and, once the NIC executed it, its outcome. Receives always
/// produce a CQE.
pub struct RecvSlot {
    pub wr: RecvWr,
    pub outcome: Option<Outcome>,
}

/// A queue pair. Each queue holds the WQEs posted and not yet retired, oldest first; the WQE
/// at index `k` has sequence number `head + k`, and the first `done` have been executed.
pub struct Qp {
    pub attr: QpAttr,
    pub state: QpState,
    pub sq: Seq<SendSlot>,
    pub sq_head: nat,
    pub sq_done: nat,
    pub rq: Seq<RecvSlot>,
    pub rq_head: nat,
    pub rq_done: nat,
}

impl Qp {
    pub open spec fn with_state(self, state: QpState) -> Qp {
        Qp { state, ..self }
    }

    /// How many executed, unretired send WQEs have a CQE waiting.
    pub open spec fn sq_cqes(self) -> nat {
        count_generating(self.sq.take(self.sq_done as int))
    }
}

pub enum Queue {
    Send,
    Recv,
}

/// Names one WQE for good: its queue pair, queue and sequence number.
pub struct WqeRef {
    pub qp: QpId,
    pub queue: Queue,
    pub seq: nat,
}

/// A completion queue entry (`ibv_wc`). `seq` names the WQE it completes; the hardware
/// reports only `wr_id`, which the library sets so that it determines `seq`.
pub struct Cqe {
    pub qp: QpId,
    pub queue: Queue,
    pub wr_id: u64,
    pub outcome: Outcome,
    pub seq: nat,
}

/// The WQEs that polling `e` retires from `qp` (its own and every earlier one of its queue),
/// with their outcomes.
#[verifier::opaque]
pub open spec fn retires(qp: Qp, e: Cqe) -> IMap<WqeRef, Outcome> {
    match e.queue {
        Queue::Send => IMap::new(
            |w: WqeRef| w.qp == e.qp && w.queue is Send && qp.sq_head <= w.seq <= e.seq,
            |w: WqeRef| qp.sq[w.seq - qp.sq_head].outcome->0,
        ),
        Queue::Recv => IMap::new(
            |w: WqeRef| w.qp == e.qp && w.queue is Recv && qp.rq_head <= w.seq <= e.seq,
            |w: WqeRef| qp.rq[w.seq - qp.rq_head].outcome->0,
        ),
    }
}

/// A completion queue. `reserved` holds each attached queue pair once per WQE it may have
/// outstanding (its queue depths); `used` holds the queue pair of each entry. Both are
/// bookkeeping for the capacity argument.
pub struct Cq {
    pub cap: nat,
    pub entries: Seq<Cqe>,
    pub reserved: Multiset<QpId>,
    pub used: Multiset<QpId>,
}

/// `n` copies of `q`.
pub open spec fn copies(q: QpId, n: nat) -> Multiset<QpId>
    decreases n,
{
    if n == 0 {
        Multiset::empty()
    } else {
        copies(q, (n - 1) as nat).insert(q)
    }
}

impl Cq {
    pub open spec fn reserve(self, q: QpId, n: nat) -> Cq {
        Cq { reserved: self.reserved.add(copies(q, n)), ..self }
    }

    pub open spec fn unreserve(self, q: QpId, n: nat) -> Cq {
        Cq { reserved: self.reserved.sub(copies(q, n)), ..self }
    }

    pub open spec fn push(self, e: Cqe) -> Cq {
        Cq { entries: self.entries.push(e), used: self.used.insert(e.qp), ..self }
    }

    pub open spec fn pop(self) -> Cq {
        Cq { entries: self.entries.drop_first(), used: self.used.remove(self.entries[0].qp), ..self }
    }
}

} // verus!

state_machine! {
    Nic {
        fields {
            pub caps: Caps,
            pub pds: Set<PdId>,
            pub mrs: Map<Key, Mr>,
            pub cqs: Map<CqId, Cq>,
            pub qps: Map<QpId, Qp>,
            /// The outcome of every WQE retired so far, as the NIC reported it. Bookkeeping
            /// (the hardware keeps no such log): F1 is stated against it.
            pub retired: IMap<WqeRef, Outcome>,
        }

        // =================================================================================
        // Helpers

        /// Every byte of `sge` lies in a region of protection domain `pd`, writable locally
        /// if `write`.
        pub open spec fn registered(self, pd: PdId, sge: Sge, write: bool) -> bool {
            &&& self.mrs.contains_key(sge.lkey)
            &&& self.mrs[sge.lkey].pd == pd
            &&& sge.span().within(self.mrs[sge.lkey].span)
            &&& write ==> self.mrs[sge.lkey].local_write
        }

        /// A send WR the device accepts on a queue pair created with `attr`.
        pub open spec fn send_wr_ok(self, attr: QpAttr, wr: SendWr) -> bool {
            &&& wr.opcode.allowed_on(attr.transport)
            &&& wr.sges.len() <= attr.max_sge
            &&& total_len(wr.sges) <= self.caps.max_msg
            &&& wr.inline ==> total_len(wr.sges) <= attr.max_inline && !wr.opcode.writes_local()
            &&& !wr.inline ==> forall|i: int| 0 <= i < wr.sges.len()
                ==> self.registered(attr.pd, #[trigger] wr.sges[i], wr.opcode.writes_local())
        }

        /// A receive WR the device accepts.
        pub open spec fn recv_wr_ok(self, attr: QpAttr, wr: RecvWr) -> bool {
            &&& wr.sges.len() <= attr.max_sge
            &&& forall|i: int| 0 <= i < wr.sges.len() ==> self.registered(attr.pd, #[trigger] wr.sges[i], true)
        }

        /// Some posted, unretired WQE names region `key`.
        pub open spec fn mr_in_use(self, key: Key) -> bool {
            ||| exists|q: QpId, k: int| #![trigger self.qps[q].sq[k]] self.qps.contains_key(q)
                    && 0 <= k < self.qps[q].sq.len() && !self.qps[q].sq[k].wr.inline
                    && names_key(self.qps[q].sq[k].wr.sges, key)
            ||| exists|q: QpId, k: int| #![trigger self.qps[q].rq[k]] self.qps.contains_key(q)
                    && 0 <= k < self.qps[q].rq.len() && names_key(self.qps[q].rq[k].wr.sges, key)
        }

        /// The CQ capacity queue pair `q` reserves on CQ `c`: its depth on each queue
        /// attached there.
        pub open spec fn reserved_by(self, c: CqId, q: QpId) -> nat {
            if !self.qps.contains_key(q) {
                0
            } else {
                let qp = self.qps[q];
                (if qp.attr.send_cq == c { qp.attr.sq_depth } else { 0 })
                    + (if qp.attr.recv_cq == c { qp.attr.rq_depth } else { 0 })
            }
        }

        /// The CQEs of queue pair `q` waiting in CQ `c`.
        pub open spec fn pending(self, c: CqId, q: QpId) -> nat {
            if !self.qps.contains_key(q) {
                0
            } else {
                let qp = self.qps[q];
                (if qp.attr.send_cq == c { qp.sq_cqes() } else { 0 })
                    + (if qp.attr.recv_cq == c { qp.rq_done } else { 0 })
            }
        }

        // What the NIC may access (S1 is stated against these).

        /// The NIC may read byte `a`: it lies in a buffer of a posted, unretired send, RDMA
        /// write or (as the source) anything but an RDMA read, unless inline; or the peer
        /// holds the rkey of a region allowing remote read.
        pub open spec fn nic_may_read(self, a: int) -> bool {
            ||| exists|q: QpId, k: int| #![trigger self.qps[q].sq[k]] self.qps.contains_key(q)
                    && 0 <= k < self.qps[q].sq.len() && {
                    let wr = self.qps[q].sq[k].wr;
                    !wr.inline && !wr.opcode.writes_local() && covers(wr.sges, a)
                }
            ||| exists|key: Key| #[trigger] self.mrs.contains_key(key) && self.mrs[key].remote_read
                    && self.mrs[key].span.contains(a)
        }

        /// The NIC may write byte `a`: it lies in a buffer of a posted, unretired receive or
        /// RDMA read; or the peer holds the rkey of a region allowing remote write, which it
        /// may use at any time, with any bytes.
        pub open spec fn nic_may_write(self, a: int) -> bool {
            ||| exists|q: QpId, k: int| #![trigger self.qps[q].sq[k]] self.qps.contains_key(q)
                    && 0 <= k < self.qps[q].sq.len() && {
                    let wr = self.qps[q].sq[k].wr;
                    wr.opcode.writes_local() && covers(wr.sges, a)
                }
            ||| exists|q: QpId, k: int| #![trigger self.qps[q].rq[k]] self.qps.contains_key(q)
                    && 0 <= k < self.qps[q].rq.len() && covers(self.qps[q].rq[k].wr.sges, a)
            ||| exists|key: Key| #[trigger] self.mrs.contains_key(key) && self.mrs[key].remote_write
                    && self.mrs[key].span.contains(a)
        }

        // =================================================================================
        // Opening the device

        init! {
            open(caps: Caps) {
                init caps = caps;
                init pds = Set::empty();
                init mrs = Map::empty();
                init cqs = Map::empty();
                init qps = Map::empty();
                init retired = IMap::empty();
            }
        }

        // =================================================================================
        // Verbs: the library's calls. Each `require` is part of S2.

        transition! {
            alloc_pd(pd: PdId) {
                require !pre.pds.contains(pd);
                update pds = pre.pds.insert(pd);
            }
        }

        transition! {
            reg_mr(key: Key, mr: Mr) {
                require pre.pds.contains(mr.pd);
                require !pre.mrs.contains_key(key);
                update mrs = pre.mrs.insert(key, mr);
            }
        }

        /// Deregistering a region no posted WQE uses. (After this returns, the peer's rkey
        /// no longer works.)
        transition! {
            dereg_mr(key: Key) {
                require pre.mrs.contains_key(key);
                require !pre.mr_in_use(key);
                update mrs = pre.mrs.remove(key);
            }
        }

        transition! {
            create_cq(c: CqId, cap: nat) {
                require !pre.cqs.contains_key(c);
                require 1 <= cap <= pre.caps.max_cqe;
                update cqs = pre.cqs.insert(c,
                    Cq { cap, entries: Seq::empty(), reserved: Multiset::empty(), used: Multiset::empty() });
            }
        }

        transition! {
            destroy_cq(c: CqId) {
                require pre.cqs.contains_key(c);
                require forall|q: QpId| #[trigger] pre.qps.contains_key(q)
                    ==> pre.qps[q].attr.send_cq != c && pre.qps[q].attr.recv_cq != c;
                update cqs = pre.cqs.remove(c);
            }
        }

        /// Creating a queue pair within the device's limits. It reserves its queue depths on
        /// its CQs, and no CQ may end up reserving more than its capacity: so no CQ can
        /// overflow (see the `assert`s in the NIC steps).
        transition! {
            create_qp(q: QpId, attr: QpAttr) {
                require !pre.qps.contains_key(q);
                // `q` is the model's name for the queue pair, never reused (hardware QP
                // numbers are; the library maps them).
                require forall|w: WqeRef| #[trigger] pre.retired.contains_key(w) ==> w.qp != q;
                require pre.pds.contains(attr.pd);
                require pre.cqs.contains_key(attr.send_cq) && pre.cqs.contains_key(attr.recv_cq);
                require attr.sq_depth <= pre.caps.max_qp_wr && attr.rq_depth <= pre.caps.max_qp_wr;
                require attr.max_sge <= pre.caps.max_sge && attr.max_inline <= pre.caps.max_inline;
                let cqs1 = pre.cqs.insert(attr.send_cq, pre.cqs[attr.send_cq].reserve(q, attr.sq_depth));
                let cqs2 = cqs1.insert(attr.recv_cq, cqs1[attr.recv_cq].reserve(q, attr.rq_depth));
                require cqs2[attr.send_cq].reserved.len() <= cqs2[attr.send_cq].cap;
                require cqs2[attr.recv_cq].reserved.len() <= cqs2[attr.recv_cq].cap;
                update cqs = cqs2;
                update qps = pre.qps.insert(q, Qp {
                    attr, state: QpState::Reset,
                    sq: Seq::empty(), sq_head: 0, sq_done: 0,
                    rq: Seq::empty(), rq_head: 0, rq_done: 0,
                });
            }
        }

        /// Changing a queue pair's state. The library must ask for a legal change from the
        /// state it knows; the NIC may have moved to `Err` meanwhile, and then a change other
        /// than to `Err` or `Reset` fails and nothing happens. Going to `Reset` (which would
        /// discard WQEs) needs empty queues.
        transition! {
            modify_qp(q: QpId, to: QpState) {
                require pre.qps.contains_key(q);
                let qp = pre.qps[q];
                require legal(qp.state, to) || qp.state is Err;
                require to is Reset ==> qp.sq.len() == 0 && qp.rq.len() == 0;
                update qps = pre.qps.insert(q, qp.with_state(if legal(qp.state, to) { to } else { qp.state }));
            }
        }

        /// Destroying a queue pair whose queues are empty: every WQE was retired, so the
        /// NIC holds none of its buffers and no CQE of it is left.
        transition! {
            destroy_qp(q: QpId) {
                require pre.qps.contains_key(q);
                let qp = pre.qps[q];
                require qp.sq.len() == 0 && qp.rq.len() == 0;
                let cqs1 = pre.cqs.insert(qp.attr.send_cq, pre.cqs[qp.attr.send_cq].unreserve(q, qp.attr.sq_depth));
                let cqs2 = cqs1.insert(qp.attr.recv_cq, cqs1[qp.attr.recv_cq].unreserve(q, qp.attr.rq_depth));
                update cqs = cqs2;
                update qps = pre.qps.remove(q);
            }
        }

        /// Posting send WRs with one doorbell (`ibv_post_send` with a list).
        transition! {
            post_send(q: QpId, wrs: Seq<SendWr>) {
                require pre.qps.contains_key(q);
                let qp = pre.qps[q];
                require qp.state is Rts || qp.state is Err;
                require qp.sq.len() + wrs.len() <= qp.attr.sq_depth;
                require forall|i: int| 0 <= i < wrs.len() ==> pre.send_wr_ok(qp.attr, #[trigger] wrs[i]);
                let slots = wrs.map_values(|wr: SendWr| SendSlot { wr, outcome: None });
                update qps = pre.qps.insert(q, Qp { sq: qp.sq + slots, ..qp });
            }
        }

        /// Posting receive WRs (`ibv_post_recv` with a list).
        transition! {
            post_recv(q: QpId, wrs: Seq<RecvWr>) {
                require pre.qps.contains_key(q);
                let qp = pre.qps[q];
                require !(qp.state is Reset);
                require qp.rq.len() + wrs.len() <= qp.attr.rq_depth;
                require forall|i: int| 0 <= i < wrs.len() ==> pre.recv_wr_ok(qp.attr, #[trigger] wrs[i]);
                let slots = wrs.map_values(|wr: RecvWr| RecvSlot { wr, outcome: None });
                update qps = pre.qps.insert(q, Qp { rq: qp.rq + slots, ..qp });
            }
        }

        /// Polling one CQE (`ibv_poll_cq` returning it). It retires its WQE and every earlier
        /// one of the same queue: those executed without a CQE, which means they were
        /// unsignaled and succeeded (`lemma_retired_unsignaled_succeeded`).
        transition! {
            poll_cq(c: CqId) {
                require pre.cqs.contains_key(c);
                require pre.cqs[c].entries.len() > 0;
                let e = pre.cqs[c].entries[0];
                let qp = pre.qps[e.qp];
                update cqs = pre.cqs.insert(c, pre.cqs[c].pop());
                update retired = pre.retired.union_prefer_right(retires(qp, e));
                update qps = pre.qps.insert(e.qp, match e.queue {
                    Queue::Send => {
                        let n = (e.seq + 1 - qp.sq_head) as nat;
                        Qp { sq: qp.sq.skip(n as int), sq_head: e.seq + 1, sq_done: (qp.sq_done - n) as nat, ..qp }
                    }
                    Queue::Recv => {
                        let n = (e.seq + 1 - qp.rq_head) as nat;
                        Qp { rq: qp.rq.skip(n as int), rq_head: e.seq + 1, rq_done: (qp.rq_done - n) as nat, ..qp }
                    }
                });
            }
        }

        // =================================================================================
        // NIC steps: they happen by themselves, with outcomes chosen by the peer and network.

        /// The NIC executes the oldest unexecuted send WQE. In `Rts` any outcome but
        /// `FlushErr` may happen (the peer may refuse, vanish, never post a receive); an
        /// error moves the queue pair to `Err`. In `Err` it completes with `FlushErr`. A CQE
        /// appears if the WQE was signaled or failed; the CQ always has room.
        transition! {
            execute_send(q: QpId, o: Outcome) {
                require pre.qps.contains_key(q);
                let qp = pre.qps[q];
                require qp.sq_done < qp.sq.len();
                require (qp.state is Err) == (o.status is FlushErr);
                let slot = qp.sq[qp.sq_done as int];
                let done = SendSlot { wr: slot.wr, outcome: Some(o) };
                update qps = pre.qps.insert(q, Qp {
                    sq: qp.sq.update(qp.sq_done as int, done),
                    sq_done: qp.sq_done + 1,
                    state: if o.status.is_err() { QpState::Err } else { qp.state },
                    ..qp
                });
                if done.generates() {
                    let c = qp.attr.send_cq;
                    assert(pre.cqs[c].entries.len() < pre.cqs[c].cap) by {
                        crate::nic_proofs::lemma_room_for_send_cqe(pre, q);
                    };
                    update cqs = pre.cqs.insert(c, pre.cqs[c].push(Cqe {
                        qp: q, queue: Queue::Send, wr_id: slot.wr.wr_id, outcome: o, seq: qp.sq_head + qp.sq_done,
                    }));
                }
            }
        }

        /// The NIC executes the oldest unexecuted receive WQE: a message arrived (its length
        /// fits the buffers if it succeeded) or, in `Err`, it is flushed. Always a CQE.
        transition! {
            execute_recv(q: QpId, o: Outcome) {
                require pre.qps.contains_key(q);
                let qp = pre.qps[q];
                require qp.rq_done < qp.rq.len();
                require qp.state is Rtr || qp.state is Rts || qp.state is Err;
                require (qp.state is Err) == (o.status is FlushErr);
                let slot = qp.rq[qp.rq_done as int];
                require o.status is Success ==> o.byte_len <= total_len(slot.wr.sges);
                let c = qp.attr.recv_cq;
                assert(pre.cqs[c].entries.len() < pre.cqs[c].cap) by {
                    crate::nic_proofs::lemma_room_for_recv_cqe(pre, q);
                };
                update qps = pre.qps.insert(q, Qp {
                    rq: qp.rq.update(qp.rq_done as int, RecvSlot { wr: slot.wr, outcome: Some(o) }),
                    rq_done: qp.rq_done + 1,
                    state: if o.status.is_err() { QpState::Err } else { qp.state },
                    ..qp
                });
                update cqs = pre.cqs.insert(c, pre.cqs[c].push(Cqe {
                    qp: q, queue: Queue::Recv, wr_id: slot.wr.wr_id, outcome: o, seq: qp.rq_head + qp.rq_done,
                }));
            }
        }

        /// A fault: the link went down, the peer crashed or sent something invalid. A
        /// connected queue pair moves to `Err` at any time.
        transition! {
            fault(q: QpId) {
                require pre.qps.contains_key(q);
                require pre.qps[q].state is Rtr || pre.qps[q].state is Rts;
                update qps = pre.qps.insert(q, pre.qps[q].with_state(QpState::Err));
            }
        }

        // =================================================================================
        // Invariants (M's own sanity; proofs in `nic_proofs`).

        /// Queue pairs: their objects exist, their queues are a prefix of executed WQEs
        /// followed by unexecuted ones, within their depths, and only a ready queue pair has
        /// send WQEs.
        #[invariant]
        pub fn inv_qps(&self) -> bool {
            forall|q: QpId| #[trigger] self.qps.contains_key(q) ==> self.qp_wf(self.qps[q])
        }

        /// The buffers of every posted WQE are (still) registered.
        #[invariant]
        pub fn inv_registered(&self) -> bool {
            forall|q: QpId| #[trigger] self.qps.contains_key(q) ==> {
                let qp = self.qps[q];
                &&& forall|k: int| 0 <= k < qp.sq.len() ==> self.send_wr_ok(qp.attr, (#[trigger] qp.sq[k]).wr)
                &&& forall|k: int| 0 <= k < qp.rq.len() ==> self.recv_wr_ok(qp.attr, (#[trigger] qp.rq[k]).wr)
            }
        }

        /// Completion queues: never fuller than reserved, never reserving more than their
        /// capacity, and each entry is the CQE of an executed, unretired WQE of a queue pair
        /// attached there, carrying that WQE's id and outcome.
        #[invariant]
        pub fn inv_cqs(&self) -> bool {
            forall|c: CqId| #[trigger] self.cqs.contains_key(c) ==> self.cq_wf(c)
        }

        /// Every executed WQE that produced a CQE still has it waiting, and a queue's CQEs
        /// wait in posting order.
        #[invariant]
        pub fn inv_cqes_present(&self) -> bool {
            forall|q: QpId| #[trigger] self.qps.contains_key(q) ==> self.cqes_present(q)
        }

        pub open spec fn qp_wf(self, qp: Qp) -> bool {
            &&& self.pds.contains(qp.attr.pd)
            &&& self.cqs.contains_key(qp.attr.send_cq) && self.cqs.contains_key(qp.attr.recv_cq)
            &&& qp.sq_done <= qp.sq.len() <= qp.attr.sq_depth
            &&& qp.rq_done <= qp.rq.len() <= qp.attr.rq_depth
            &&& forall|k: int| 0 <= k < qp.sq.len() ==> ((#[trigger] qp.sq[k]).outcome is Some <==> k < qp.sq_done)
            &&& forall|k: int| 0 <= k < qp.rq.len() ==> ((#[trigger] qp.rq[k]).outcome is Some <==> k < qp.rq_done)
            &&& !(qp.state is Rts || qp.state is Err) ==> qp.sq.len() == 0
            &&& qp.state is Reset ==> qp.rq.len() == 0
        }

        /// Entry `e` of CQ `c` is the CQE of the executed, unretired WQE it names.
        pub open spec fn cqe_wf(self, c: CqId, e: Cqe) -> bool {
            &&& self.qps.contains_key(e.qp)
            &&& match e.queue {
                Queue::Send => {
                    let qp = self.qps[e.qp];
                    let k = e.seq - qp.sq_head;
                    &&& qp.attr.send_cq == c
                    &&& 0 <= k < qp.sq_done
                    &&& qp.sq[k].generates()
                    &&& e.wr_id == qp.sq[k].wr.wr_id && qp.sq[k].outcome == Some(e.outcome)
                }
                Queue::Recv => {
                    let qp = self.qps[e.qp];
                    let k = e.seq - qp.rq_head;
                    &&& qp.attr.recv_cq == c
                    &&& 0 <= k < qp.rq_done
                    &&& e.wr_id == qp.rq[k].wr.wr_id && qp.rq[k].outcome == Some(e.outcome)
                }
            }
        }

        pub open spec fn cq_wf(self, c: CqId) -> bool {
            let cq = self.cqs[c];
            &&& cq.entries.len() == cq.used.len()
            &&& cq.reserved.len() <= cq.cap
            &&& forall|q: QpId| #[trigger] cq.used.count(q) == self.pending(c, q)
            &&& forall|q: QpId| #[trigger] cq.reserved.count(q) == self.reserved_by(c, q)
            &&& forall|m: int| 0 <= m < cq.entries.len() ==> self.cqe_wf(c, #[trigger] cq.entries[m])
            &&& forall|m1: int, m2: int| #![trigger cq.entries[m1], cq.entries[m2]]
                0 <= m1 < m2 < cq.entries.len() && cq.entries[m1].qp == cq.entries[m2].qp
                    && cq.entries[m1].queue == cq.entries[m2].queue
                ==> cq.entries[m1].seq < cq.entries[m2].seq
        }

        pub open spec fn cqes_present(self, q: QpId) -> bool {
            let qp = self.qps[q];
            &&& forall|k: int| 0 <= k < qp.sq_done && (#[trigger] qp.sq[k]).generates() ==>
                exists|m: int| 0 <= m < self.cqs[qp.attr.send_cq].entries.len() && {
                    let e = #[trigger] self.cqs[qp.attr.send_cq].entries[m];
                    e.qp == q && e.queue is Send && e.seq == qp.sq_head + k
                }
            &&& forall|k: int| 0 <= k < qp.rq_done ==> #[trigger] qp.rq[k].outcome is Some &&
                exists|m: int| 0 <= m < self.cqs[qp.attr.recv_cq].entries.len() && {
                    let e = #[trigger] self.cqs[qp.attr.recv_cq].entries[m];
                    e.qp == q && e.queue is Recv && e.seq == qp.rq_head + k
                }
        }

        // =================================================================================
        // Inductiveness

        #[inductive(open)]
        pub fn open_inductive(post: Self, caps: Caps) {}

        #[inductive(alloc_pd)]
        pub fn alloc_pd_inductive(pre: Self, post: Self, pd: PdId) {
            crate::nic_proofs::lemma_alloc_pd(pre, post, pd);
        }

        #[inductive(reg_mr)]
        pub fn reg_mr_inductive(pre: Self, post: Self, key: Key, mr: Mr) {
            crate::nic_proofs::lemma_reg_mr(pre, post, key, mr);
        }

        #[inductive(dereg_mr)]
        pub fn dereg_mr_inductive(pre: Self, post: Self, key: Key) {
            crate::nic_proofs::lemma_dereg_mr(pre, post, key);
        }

        #[inductive(create_cq)]
        pub fn create_cq_inductive(pre: Self, post: Self, c: CqId, cap: nat) {
            crate::nic_proofs::lemma_create_cq(pre, post, c, cap);
        }

        #[inductive(destroy_cq)]
        pub fn destroy_cq_inductive(pre: Self, post: Self, c: CqId) {
            crate::nic_proofs::lemma_destroy_cq(pre, post, c);
        }

        #[inductive(create_qp)]
        pub fn create_qp_inductive(pre: Self, post: Self, q: QpId, attr: QpAttr) {
            crate::nic_proofs::lemma_create_qp(pre, post, q, attr);
        }

        #[inductive(modify_qp)]
        pub fn modify_qp_inductive(pre: Self, post: Self, q: QpId, to: QpState) {
            crate::nic_proofs::lemma_modify_qp(pre, post, q, to);
        }

        #[inductive(destroy_qp)]
        pub fn destroy_qp_inductive(pre: Self, post: Self, q: QpId) {
            crate::nic_proofs::lemma_destroy_qp(pre, post, q);
        }

        #[inductive(post_send)]
        pub fn post_send_inductive(pre: Self, post: Self, q: QpId, wrs: Seq<SendWr>) {
            crate::nic_proofs::lemma_post_send(pre, post, q, wrs);
        }

        #[inductive(post_recv)]
        pub fn post_recv_inductive(pre: Self, post: Self, q: QpId, wrs: Seq<RecvWr>) {
            crate::nic_proofs::lemma_post_recv(pre, post, q, wrs);
        }

        #[inductive(poll_cq)]
        pub fn poll_cq_inductive(pre: Self, post: Self, c: CqId) {
            crate::nic_proofs::lemma_poll_cq(pre, post, c);
        }

        #[inductive(execute_send)]
        pub fn execute_send_inductive(pre: Self, post: Self, q: QpId, o: Outcome) {
            crate::nic_proofs::lemma_execute_send(pre, post, q, o);
        }

        #[inductive(execute_recv)]
        pub fn execute_recv_inductive(pre: Self, post: Self, q: QpId, o: Outcome) {
            crate::nic_proofs::lemma_execute_recv(pre, post, q, o);
        }

        #[inductive(fault)]
        pub fn fault_inductive(pre: Self, post: Self, q: QpId) {
            crate::nic_proofs::lemma_fault(pre, post, q);
        }
    }
}

verus! {

// =========================================================================================
// Names for other crates
//
// `state_machine!` emits its relations, `invariant()` and the `_enabled` predicates only when
// Verus keeps ghost bodies, which it does not for the copy of this crate that dependent crates
// compile against. These wrappers carry them across (their meaning comes along in Verus's
// exported definitions).

/// `s` satisfies the invariants of Nic (`Nic::State::invariant`).
pub open spec fn nic_inv(s: Nic::State) -> bool {
    s.invariant()
}

/// `s` is a start state of Nic.
pub open spec fn nic_init(s: Nic::State) -> bool {
    Nic::State::init(s)
}

/// `post` follows from `pre` by one step of Nic, any step.
pub open spec fn nic_next(pre: Nic::State, post: Nic::State) -> bool {
    Nic::State::next(pre, post)
}

/// `post` follows from `pre` by `step`.
pub open spec fn nic_step(pre: Nic::State, post: Nic::State, step: Nic::Step) -> bool {
    match step {
        Nic::Step::alloc_pd(pd) => Nic::State::alloc_pd(pre, post, pd),
        Nic::Step::reg_mr(key, mr) => Nic::State::reg_mr(pre, post, key, mr),
        Nic::Step::dereg_mr(key) => Nic::State::dereg_mr(pre, post, key),
        Nic::Step::create_cq(c, cap) => Nic::State::create_cq(pre, post, c, cap),
        Nic::Step::destroy_cq(c) => Nic::State::destroy_cq(pre, post, c),
        Nic::Step::create_qp(q, attr) => Nic::State::create_qp(pre, post, q, attr),
        Nic::Step::modify_qp(q, to) => Nic::State::modify_qp(pre, post, q, to),
        Nic::Step::destroy_qp(q) => Nic::State::destroy_qp(pre, post, q),
        Nic::Step::post_send(q, wrs) => Nic::State::post_send(pre, post, q, wrs),
        Nic::Step::post_recv(q, wrs) => Nic::State::post_recv(pre, post, q, wrs),
        Nic::Step::poll_cq(c) => Nic::State::poll_cq(pre, post, c),
        Nic::Step::execute_send(q, o) => Nic::State::execute_send(pre, post, q, o),
        Nic::Step::execute_recv(q, o) => Nic::State::execute_recv(pre, post, q, o),
        Nic::Step::fault(q) => Nic::State::fault(pre, post, q),
        Nic::Step::dummy_to_use_type_params(_) => false,
    }
}

/// `step` may happen in `pre`: for a verb, its precondition (what S2 promises).
pub open spec fn nic_enabled(pre: Nic::State, step: Nic::Step) -> bool {
    match step {
        Nic::Step::alloc_pd(pd) => Nic::State::alloc_pd_enabled(pre, pd),
        Nic::Step::reg_mr(key, mr) => Nic::State::reg_mr_enabled(pre, key, mr),
        Nic::Step::dereg_mr(key) => Nic::State::dereg_mr_enabled(pre, key),
        Nic::Step::create_cq(c, cap) => Nic::State::create_cq_enabled(pre, c, cap),
        Nic::Step::destroy_cq(c) => Nic::State::destroy_cq_enabled(pre, c),
        Nic::Step::create_qp(q, attr) => Nic::State::create_qp_enabled(pre, q, attr),
        Nic::Step::modify_qp(q, to) => Nic::State::modify_qp_enabled(pre, q, to),
        Nic::Step::destroy_qp(q) => Nic::State::destroy_qp_enabled(pre, q),
        Nic::Step::post_send(q, wrs) => Nic::State::post_send_enabled(pre, q, wrs),
        Nic::Step::post_recv(q, wrs) => Nic::State::post_recv_enabled(pre, q, wrs),
        Nic::Step::poll_cq(c) => Nic::State::poll_cq_enabled(pre, c),
        Nic::Step::execute_send(q, o) => Nic::State::execute_send_enabled(pre, q, o),
        Nic::Step::execute_recv(q, o) => Nic::State::execute_recv_enabled(pre, q, o),
        Nic::Step::fault(q) => Nic::State::fault_enabled(pre, q),
        Nic::Step::dummy_to_use_type_params(_) => false,
    }
}

/// The NIC's own steps (they happen by themselves); every other step is a verb.
pub open spec fn is_nic_step(step: Nic::Step) -> bool {
    step is execute_send || step is execute_recv || step is fault
}

/// A verb: a call the library makes.
pub open spec fn is_verb(step: Nic::Step) -> bool {
    !is_nic_step(step) && !(step is dummy_to_use_type_params)
}

/// A step by `step` is a step of Nic.
pub proof fn lemma_nic_step_is_next(pre: Nic::State, post: Nic::State, step: Nic::Step)
    requires
        nic_step(pre, post, step),
    ensures
        nic_next(pre, post),
{
    reveal(Nic::State::next);
    reveal(Nic::State::next_by);
    assert(Nic::State::next_by(pre, post, step));
}

// =========================================================================================
// What M promises the library (proofs in `nic_proofs`)

/// M's invariants hold in every state it can reach: every step, verb or NIC step, keeps them.
pub proof fn lemma_step_keeps_invariant(pre: Nic::State, post: Nic::State)
    requires
        pre.invariant(),
        Nic::State::next(pre, post),
    ensures
        post.invariant(),
{
    crate::nic_proofs::lemma_next(pre, post);
}

/// No CQ ever holds more entries than its capacity, given that every `create_qp` met its
/// precondition (the reservation).
pub proof fn lemma_cq_never_overflows(s: Nic::State, c: CqId)
    requires
        s.invariant(),
        s.cqs.contains_key(c),
    ensures
        s.cqs[c].entries.len() <= s.cqs[c].cap,
{
    crate::nic_proofs::lemma_cq_bound(s, c);
}

/// Polling a CQE: it reports the id and outcome of the WQE it names, and retires that WQE and
/// every earlier one of its queue. The earlier ones were unsignaled send WQEs that succeeded
/// (a receive retires only itself). All of them land in `retired` with their outcomes.
pub proof fn lemma_poll(pre: Nic::State, post: Nic::State, c: CqId)
    requires
        pre.invariant(),
        Nic::State::poll_cq(pre, post, c),
    ensures
        ({
            let e = pre.cqs[c].entries[0];
            let qp = pre.qps[e.qp];
            let me = WqeRef { qp: e.qp, queue: e.queue, seq: e.seq };
            &&& pre.qps.contains_key(e.qp)
            &&& e.queue is Send ==> e.wr_id == qp.sq[e.seq - qp.sq_head].wr.wr_id
            &&& e.queue is Recv ==> e.wr_id == qp.rq[e.seq - qp.rq_head].wr.wr_id && e.seq == qp.rq_head
            &&& post.retired.contains_key(me) && post.retired[me] == e.outcome
            &&& e.queue is Send ==> forall|j: int| 0 <= j < e.seq - qp.sq_head ==> {
                let w = WqeRef { qp: e.qp, queue: Queue::Send, seq: (qp.sq_head + j) as nat };
                &&& !(#[trigger] qp.sq[j]).wr.signaled
                &&& post.retired.contains_key(w) && post.retired[w].status is Success
            }
        }),
{
    crate::nic_proofs::lemma_poll_reports(pre, post, c);
}

/// The NIC's own steps never invalidate a verb's precondition. So the library may check a
/// precondition against the state it last observed: whatever the NIC did since (executing
/// work requests, moving a queue pair to `Err`), the call is still within M.
pub proof fn lemma_nic_steps_keep_verbs_enabled(pre: Nic::State, post: Nic::State, step: Nic::Step, verb: Nic::Step)
    requires
        nic_inv(pre),
        is_nic_step(step),
        nic_step(pre, post, step),
        is_verb(verb),
        nic_enabled(pre, verb),
    ensures
        nic_enabled(post, verb),
{
    crate::nic_proofs::lemma_keeps_enabled(pre, post, step, verb);
}

/// The NIC only touches registered memory: whatever it may read or write lies in a region
/// that is registered right now.
pub proof fn lemma_nic_touches_registered(s: Nic::State, a: int)
    requires
        s.invariant(),
        s.nic_may_read(a) || s.nic_may_write(a),
    ensures
        exists|key: Key| #[trigger] s.mrs.contains_key(key) && s.mrs[key].span.contains(a),
{
    crate::nic_proofs::lemma_touches_registered(s, a);
}

} // verus!
