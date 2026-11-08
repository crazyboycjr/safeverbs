//! A worker's share of M: rely and guarantee between the threads of one device.
//!
//! M is one state for the whole device, but the library drives it from several threads: each
//! worker owns one CQ and the QPs it creates there and moves between threads alone, while the
//! protection domain and its memory regions are shared through the control path. This module
//! says what each worker may assume about the others and what it promises them, against M as
//! it stands (M is unchanged; everything here is proved from its definition):
//!
//! - **A share** ([`Share`]) is a worker's part of M: its CQ, and the QP names it owns, a set of
//!   names no other worker uses (so a new QP's name is fresh without a global counter).
//!   [`share_wf`] says a state respects it: the worker's CQ exists, its QPs are attached to
//!   that CQ only, and no other QP is attached there.
//! - **Rely** ([`rely_reaches`]): between two calls of a worker, M moves by the NIC's own
//!   steps, which touch the worker's queues only as M says (executions, faults), and by verbs
//!   of the others, which touch none of its CQ and QPs ([`foreign`]). The others may register
//!   and deregister memory regions, but not one the worker's next verb names (`pins`): a
//!   worker names a region only through a buffer whose lease keeps it registered.
//! - **Guarantee** ([`lemma_mine_is_foreign`]): every verb a worker calls on its own share
//!   ([`mine`]) is a foreign step for every other share.
//!
//! Under rely, a worker may check a verb's precondition in the state it last saw:
//! [`lemma_rely_keeps_enabled`] shows that whatever the NIC and the others did since, the verb
//! is still enabled, and [`lemma_rely_frame`] what of its share stayed as it was ([`framed`]).
//! Both generalize M's `lemma_nic_steps_keep_verbs_enabled`, which is the case of no foreign
//! verb ([`lemma_env_is_rely`]), and every rely run is a run of M ([`lemma_rely_is_nic_run`]),
//! so the per-worker views compose into executions of M.
#![allow(unused_imports)]

use crate::abs::*;
use crate::nic::*;
use crate::refinement::*;
use crate::types::*;
use vstd::prelude::*;

verus! {

/// A worker's part of M: its CQ, and the names of the QPs it owns.
pub struct Share {
    pub cq: CqId,
    /// The QP names of this worker: no other worker creates a QP with one of them.
    pub owns: ISet<QpId>,
}

/// `m` respects share `sh`: its CQ exists, its QPs are attached to that CQ only, and every QP
/// attached to that CQ is its own.
pub open spec fn share_wf(m: Nic::State, sh: Share) -> bool {
    &&& m.cqs.contains_key(sh.cq)
    &&& forall|q: QpId| #[trigger] m.qps.contains_key(q) && sh.owns.contains(q)
        ==> m.qps[q].attr.send_cq == sh.cq && m.qps[q].attr.recv_cq == sh.cq
    &&& forall|q: QpId| #[trigger] m.qps.contains_key(q) && (m.qps[q].attr.send_cq == sh.cq
        || m.qps[q].attr.recv_cq == sh.cq) ==> sh.owns.contains(q)
}

/// The NIC may read byte `a` beyond the queues of share `sh`: for a WQE of another queue pair, or
/// for a peer holding the rkey of a region.
pub open spec fn outside_reads(m: Nic::State, sh: Share, a: int) -> bool {
    ||| exists|q: QpId, k: int| #![trigger m.qps[q].sq[k]] !sh.owns.contains(q) && m.qps.contains_key(q)
            && 0 <= k < m.qps[q].sq.len() && {
            let wr = m.qps[q].sq[k].wr;
            !wr.inline && !wr.opcode.writes_local() && covers(wr.sges, a)
        }
    ||| exists|key: Key| #[trigger] m.mrs.contains_key(key) && m.mrs[key].remote_read
            && m.mrs[key].span.contains(a)
}

/// The NIC may write byte `a` beyond the queues of share `sh`: for a WQE of another queue pair, or
/// for a peer holding the rkey of a region.
pub open spec fn outside_writes(m: Nic::State, sh: Share, a: int) -> bool {
    ||| exists|q: QpId, k: int| #![trigger m.qps[q].sq[k]] !sh.owns.contains(q) && m.qps.contains_key(q)
            && 0 <= k < m.qps[q].sq.len() && {
            let wr = m.qps[q].sq[k].wr;
            wr.opcode.writes_local() && covers(wr.sges, a)
        }
    ||| exists|q: QpId, k: int| #![trigger m.qps[q].rq[k]] !sh.owns.contains(q) && m.qps.contains_key(q)
            && 0 <= k < m.qps[q].rq.len() && covers(m.qps[q].rq[k].wr.sges, a)
    ||| exists|key: Key| #[trigger] m.mrs.contains_key(key) && m.mrs[key].remote_write
            && m.mrs[key].span.contains(a)
}

/// The NIC may read byte `a` for a WQE of one of the queue pairs of share `sh`.
pub open spec fn inside_reads(m: Nic::State, sh: Share, a: int) -> bool {
    exists|q: QpId, k: int| #![trigger m.qps[q].sq[k]] sh.owns.contains(q) && m.qps.contains_key(q)
        && 0 <= k < m.qps[q].sq.len() && {
        let wr = m.qps[q].sq[k].wr;
        !wr.inline && !wr.opcode.writes_local() && covers(wr.sges, a)
    }
}

/// The NIC may write byte `a` for a WQE of one of the queue pairs of share `sh`.
pub open spec fn inside_writes(m: Nic::State, sh: Share, a: int) -> bool {
    ||| exists|q: QpId, k: int| #![trigger m.qps[q].sq[k]] sh.owns.contains(q) && m.qps.contains_key(q)
            && 0 <= k < m.qps[q].sq.len() && {
            let wr = m.qps[q].sq[k].wr;
            wr.opcode.writes_local() && covers(wr.sges, a)
        }
    ||| exists|q: QpId, k: int| #![trigger m.qps[q].rq[k]] sh.owns.contains(q) && m.qps.contains_key(q)
            && 0 <= k < m.qps[q].rq.len() && covers(m.qps[q].rq[k].wr.sges, a)
}

/// [`coupled`] on the queue pairs of share `sh`: what one engine's state of A says of M. Its
/// requests own disjoint WQE intervals of those queue pairs, those in flight still posted, the
/// complete ones with M's aggregate of what retired; and what the NIC may touch for a WQE of those
/// queue pairs, A gives it. What it may touch beyond them is [`outside_covered`].
pub open spec fn coupled_on(a: Abs::State, m: Nic::State, sh: Share, ranges: Map<ReqId, WqeRange>) -> bool {
    &&& forall|r: ReqId| #[trigger] a.requests.contains_key(r) ==> {
        &&& ranges.contains_key(r)
        &&& ranges[r].qp == a.requests[r].qp
        &&& sh.owns.contains(ranges[r].qp)
    }
    &&& forall|r: ReqId| #[trigger] a.outcomes.contains_key(r) ==> ranges.contains_key(r)
    &&& forall|r: ReqId| #[trigger] ranges.contains_key(r) ==> ranges[r].wf()
    &&& forall|r1: ReqId, r2: ReqId| #![trigger ranges[r1], ranges[r2]]
        ranges.contains_key(r1) && ranges.contains_key(r2) && r1 != r2
            ==> ranges[r1].disjoint(ranges[r2])
    &&& forall|r: ReqId| #[trigger] a.in_flight(r) ==> ranges[r].pending(m)
    &&& forall|r: ReqId| #[trigger] a.outcomes.contains_key(r) ==> {
        &&& ranges[r].all_retired(m.retired)
        &&& ranges[r].aggregate(m.retired) == a.outcomes[r]
        &&& ranges[r].no_success_after_error(m.retired)
    }
    &&& forall|x: int| #[trigger] inside_reads(m, sh, x) ==> a.nic_reads(x)
    &&& forall|x: int| #[trigger] inside_writes(m, sh, x) ==> a.nic_writes(x)
}

/// Coupled on its share, with what lies beyond it A's to give and every send WQE's WR remembered,
/// an engine's state of A is [`coupled`] to M.
pub proof fn lemma_coupled_on(a: Abs::State, m: Nic::State, sh: Share, ranges: Map<ReqId, WqeRange>, wrs: IMap<WqeRef, SendWr>)
    requires
        coupled_on(a, m, sh, ranges),
        outside_covered(a, m, sh),
        records_wrs(m, wrs),
    ensures
        coupled(a, m, ranges, wrs),
{
    assert forall|x: int| #[trigger] m.nic_may_read(x) implies a.nic_reads(x) by {
        if !outside_reads(m, sh, x) {
            let (q, k) = choose|q: QpId, k: int| #![trigger m.qps[q].sq[k]] m.qps.contains_key(q)
                && 0 <= k < m.qps[q].sq.len() && {
                let wr = m.qps[q].sq[k].wr;
                !wr.inline && !wr.opcode.writes_local() && covers(wr.sges, x)
            };
            assert(sh.owns.contains(q));
            assert(inside_reads(m, sh, x));
        }
    }
    assert forall|x: int| #[trigger] m.nic_may_write(x) implies a.nic_writes(x) by {
        if !outside_writes(m, sh, x) {
            if exists|q: QpId, k: int| #![trigger m.qps[q].sq[k]] m.qps.contains_key(q)
                && 0 <= k < m.qps[q].sq.len() && {
                let wr = m.qps[q].sq[k].wr;
                wr.opcode.writes_local() && covers(wr.sges, x)
            } {
                let (q, k) = choose|q: QpId, k: int| #![trigger m.qps[q].sq[k]] m.qps.contains_key(q)
                    && 0 <= k < m.qps[q].sq.len() && {
                    let wr = m.qps[q].sq[k].wr;
                    wr.opcode.writes_local() && covers(wr.sges, x)
                };
                assert(sh.owns.contains(q));
            } else {
                let (q, k) = choose|q: QpId, k: int| #![trigger m.qps[q].rq[k]] m.qps.contains_key(q)
                    && 0 <= k < m.qps[q].rq.len() && covers(m.qps[q].rq[k].wr.sges, x);
                assert(sh.owns.contains(q));
            }
            assert(inside_writes(m, sh, x));
        }
    }
}

/// What the NIC may touch beyond the queues of share `sh`, A gives it: the regions open to a peer
/// are leases exposed in A, and other workers' WQEs are too. Theorem 1 for one worker takes this
/// as a premise of each state: nothing in that worker can say it, since those belong to the control
/// path and to the others. For a worker alone with no region open to a peer, there is nothing to
/// say.
pub open spec fn outside_covered(a: Abs::State, m: Nic::State, sh: Share) -> bool {
    &&& forall|x: int| #[trigger] outside_reads(m, sh, x) ==> a.nic_reads(x)
    &&& forall|x: int| #[trigger] outside_writes(m, sh, x) ==> a.nic_writes(x)
}

/// The memory regions a list of send WRs names (inline data names none).
pub open spec fn send_keys(wrs: Seq<SendWr>) -> ISet<Key> {
    ISet::new(|k: Key| exists|i: int| 0 <= i < wrs.len() && !(#[trigger] wrs[i]).inline
        && names_key(wrs[i].sges, k))
}

/// The memory regions a list of receive WRs names.
pub open spec fn recv_keys(wrs: Seq<RecvWr>) -> ISet<Key> {
    ISet::new(|k: Key| exists|i: int| 0 <= i < wrs.len() && names_key((#[trigger] wrs[i]).sges, k))
}

/// A step that does not touch share `sh`: a NIC step (on any queue pair: the NIC executes the
/// worker's WQEs too), or a verb of the others, on none of its CQ and QPs, and deregistering
/// no region in `pins`.
pub open spec fn foreign(sh: Share, pins: ISet<Key>, step: Nic::Step) -> bool {
    match step {
        Nic::Step::alloc_pd(_) => true,
        Nic::Step::reg_mr(_, _) => true,
        Nic::Step::dereg_mr(key) => !pins.contains(key),
        Nic::Step::create_cq(c, _) => c != sh.cq,
        Nic::Step::destroy_cq(c) => c != sh.cq,
        Nic::Step::create_qp(q, attr) => !sh.owns.contains(q) && attr.send_cq != sh.cq
            && attr.recv_cq != sh.cq,
        Nic::Step::modify_qp(q, _) => !sh.owns.contains(q),
        Nic::Step::destroy_qp(q) => !sh.owns.contains(q),
        Nic::Step::post_send(q, _) => !sh.owns.contains(q),
        Nic::Step::post_recv(q, _) => !sh.owns.contains(q),
        Nic::Step::poll_cq(c) => c != sh.cq,
        Nic::Step::execute_send(_, _) => true,
        Nic::Step::execute_recv(_, _) => true,
        Nic::Step::fault(_) => true,
        Nic::Step::dummy_to_use_type_params(_) => false,
    }
}

/// A verb of the worker of share `sh`, on its own CQ and QPs, naming only regions in `pins`.
pub open spec fn mine(sh: Share, pins: ISet<Key>, verb: Nic::Step) -> bool {
    match verb {
        Nic::Step::create_qp(q, attr) => sh.owns.contains(q) && attr.send_cq == sh.cq
            && attr.recv_cq == sh.cq,
        Nic::Step::modify_qp(q, _) => sh.owns.contains(q),
        Nic::Step::destroy_qp(q) => sh.owns.contains(q),
        Nic::Step::post_send(q, wrs) => sh.owns.contains(q) && send_keys(wrs).subset_of(pins),
        Nic::Step::post_recv(q, wrs) => sh.owns.contains(q) && recv_keys(wrs).subset_of(pins),
        Nic::Step::poll_cq(c) => c == sh.cq,
        Nic::Step::destroy_cq(c) => c == sh.cq,
        _ => false,
    }
}

/// `run` is an execution of M by steps that do not touch share `sh` (with `pins`).
pub open spec fn rely_run(sh: Share, pins: ISet<Key>, run: Seq<Nic::State>, steps: Seq<Nic::Step>) -> bool {
    &&& run.len() > 0
    &&& steps.len() == run.len() - 1
    &&& forall|i: int| 0 <= i < steps.len()
        ==> foreign(sh, pins, #[trigger] steps[i]) && nic_step(run[i], run[i + 1], steps[i])
}

/// `m` reaches `n` by steps that do not touch share `sh` (with `pins`): what may happen
/// between two calls of its worker.
pub open spec fn rely_reaches(sh: Share, pins: ISet<Key>, m: Nic::State, n: Nic::State) -> bool {
    exists|run: Seq<Nic::State>, steps: Seq<Nic::Step>| rely_run(sh, pins, run, steps) && run[0] == m
        && run.last() == n
}

/// The state of a queue pair may only have moved to `Err`, as the NIC moves it.
pub open spec fn state_kept(a: QpState, b: QpState) -> bool {
    b == a || (b is Err && (a is Rtr || a is Rts || a is Err))
}

/// Queue pair `b` is queue pair `a` after steps of the NIC only: the same attributes, the
/// same WQEs in its queues, the executed ones with the same outcomes, maybe more executed.
pub open spec fn qp_kept(a: Qp, b: Qp) -> bool {
    &&& b.attr == a.attr
    &&& state_kept(a.state, b.state)
    &&& b.sq_head == a.sq_head && b.sq.len() == a.sq.len() && a.sq_done <= b.sq_done
    &&& b.rq_head == a.rq_head && b.rq.len() == a.rq.len() && a.rq_done <= b.rq_done
    &&& forall|k: int| 0 <= k < a.sq.len() ==> (#[trigger] b.sq[k]).wr == a.sq[k].wr
    &&& forall|k: int| 0 <= k < a.rq.len() ==> (#[trigger] b.rq[k]).wr == a.rq[k].wr
    &&& forall|k: int| 0 <= k < a.sq_done ==> (#[trigger] b.sq[k]).outcome == a.sq[k].outcome
    &&& forall|k: int| 0 <= k < a.rq_done ==> (#[trigger] b.rq[k]).outcome == a.rq[k].outcome
}

/// What of share `sh` stays as it was from `m` to `n` under rely (with `pins`): the device's
/// capabilities and protection domains; the regions in `pins`; its queue pairs, each changed
/// by the NIC only ([`qp_kept`]); its CQ, whose entries only grow; and the outcomes recorded
/// for its WQEs.
pub open spec fn framed(sh: Share, pins: ISet<Key>, m: Nic::State, n: Nic::State) -> bool {
    &&& n.caps == m.caps
    &&& m.pds.subset_of(n.pds)
    &&& forall|k: Key| #[trigger] pins.contains(k) && m.mrs.contains_key(k)
        ==> n.mrs.contains_key(k) && n.mrs[k] == m.mrs[k]
    &&& forall|q: QpId| #[trigger] sh.owns.contains(q)
        ==> (n.qps.contains_key(q) <==> m.qps.contains_key(q))
    &&& forall|q: QpId| #[trigger] sh.owns.contains(q) && m.qps.contains_key(q)
        ==> qp_kept(m.qps[q], n.qps[q])
    &&& n.cqs.contains_key(sh.cq)
    &&& n.cqs[sh.cq].cap == m.cqs[sh.cq].cap
    &&& n.cqs[sh.cq].reserved == m.cqs[sh.cq].reserved
    &&& m.cqs[sh.cq].entries.len() <= n.cqs[sh.cq].entries.len()
    &&& forall|i: int| 0 <= i < m.cqs[sh.cq].entries.len()
        ==> #[trigger] n.cqs[sh.cq].entries[i] == m.cqs[sh.cq].entries[i]
    &&& forall|w: WqeRef| #[trigger] sh.owns.contains(w.qp) ==> (n.retired.contains_key(w)
        <==> m.retired.contains_key(w))
    &&& forall|w: WqeRef| #[trigger] sh.owns.contains(w.qp) && m.retired.contains_key(w)
        ==> n.retired[w] == m.retired[w]
}

// =========================================================================================
// One step

/// The CQ of a queue pair attached to CQ `c`, with room when its WQE generates a CQE: the step
/// relations of the NIC's executions are then fully known.
proof fn lemma_room(pre: Nic::State, step: Nic::Step)
    requires
        nic_inv(pre),
    ensures
        match step {
            Nic::Step::execute_send(q, o) => pre.qps.contains_key(q) && pre.qps[q].sq_done
                < pre.qps[q].sq.len() ==> pre.cqs[pre.qps[q].attr.send_cq].entries.len()
                < pre.cqs[pre.qps[q].attr.send_cq].cap,
            Nic::Step::execute_recv(q, o) => pre.qps.contains_key(q) && pre.qps[q].rq_done
                < pre.qps[q].rq.len() ==> pre.cqs[pre.qps[q].attr.recv_cq].entries.len()
                < pre.cqs[pre.qps[q].attr.recv_cq].cap,
            _ => true,
        },
{
    match step {
        Nic::Step::execute_send(q, o) => {
            if pre.qps.contains_key(q) && pre.qps[q].sq_done < pre.qps[q].sq.len() {
                crate::nic_proofs::lemma_room_for_send_cqe(pre, q);
            }
        },
        Nic::Step::execute_recv(q, o) => {
            if pre.qps.contains_key(q) && pre.qps[q].rq_done < pre.qps[q].rq.len() {
                crate::nic_proofs::lemma_room_for_recv_cqe(pre, q);
            }
        },
        _ => {},
    }
}

/// A queue pair attached to another CQ than `sh`'s is not `sh`'s, and the head entry of
/// another CQ names such a queue pair.
proof fn lemma_poll_foreign(pre: Nic::State, sh: Share, c: CqId)
    requires
        nic_inv(pre),
        share_wf(pre, sh),
        c != sh.cq,
        pre.cqs.contains_key(c),
        pre.cqs[c].entries.len() > 0,
    ensures
        pre.qps.contains_key(pre.cqs[c].entries[0].qp),
        !sh.owns.contains(pre.cqs[c].entries[0].qp),
{
    assert(pre.cq_wf(c));
    let e = pre.cqs[c].entries[0];
    assert(pre.cqe_wf(c, e));
}

/// One step that does not touch share `sh` keeps its well-formedness and frames it.
pub proof fn lemma_foreign_step(sh: Share, pins: ISet<Key>, pre: Nic::State, post: Nic::State, step: Nic::Step)
    requires
        nic_inv(pre),
        share_wf(pre, sh),
        foreign(sh, pins, step),
        nic_step(pre, post, step),
    ensures
        share_wf(post, sh),
        framed(sh, pins, pre, post),
{
    lemma_room(pre, step);
    match step {
        Nic::Step::poll_cq(c) => {
            lemma_poll_foreign(pre, sh, c);
            let e = pre.cqs[c].entries[0];
            reveal(retires);
            assert forall|w: WqeRef| #[trigger] sh.owns.contains(w.qp) implies (post.retired.contains_key(w)
                <==> pre.retired.contains_key(w)) by {
                assert(w.qp != e.qp);
            }
            assert forall|w: WqeRef| #[trigger] sh.owns.contains(w.qp) && pre.retired.contains_key(w)
                implies post.retired[w] == pre.retired[w] by {
                assert(w.qp != e.qp);
            }
        },
        Nic::Step::create_qp(q, attr) => {
            assert(post.cqs[sh.cq] == pre.cqs[sh.cq]);
        },
        Nic::Step::destroy_qp(q) => {
            let qp = pre.qps[q];
            assert(qp.attr.send_cq != sh.cq && qp.attr.recv_cq != sh.cq);
            assert(post.cqs[sh.cq] == pre.cqs[sh.cq]);
        },
        Nic::Step::destroy_cq(c) => {},
        _ => {},
    }
}

/// One step that does not touch share `sh` keeps every verb of its worker enabled.
pub proof fn lemma_foreign_step_keeps_enabled(
    sh: Share,
    pins: ISet<Key>,
    pre: Nic::State,
    post: Nic::State,
    step: Nic::Step,
    verb: Nic::Step,
)
    requires
        nic_inv(pre),
        share_wf(pre, sh),
        foreign(sh, pins, step),
        nic_step(pre, post, step),
        mine(sh, pins, verb),
        nic_enabled(pre, verb),
    ensures
        nic_enabled(post, verb),
{
    lemma_foreign_step(sh, pins, pre, post, step);
    match verb {
        Nic::Step::post_send(q, wrs) => {
            let attr = pre.qps[q].attr;
            assert forall|i: int| 0 <= i < wrs.len() implies post.send_wr_ok(post.qps[q].attr, #[trigger] wrs[i]) by {
                assert(pre.send_wr_ok(attr, wrs[i]));
                if !wrs[i].inline {
                    assert forall|j: int| 0 <= j < wrs[i].sges.len()
                        implies post.registered(attr.pd, #[trigger] wrs[i].sges[j], wrs[i].opcode.writes_local()) by {
                        let k = wrs[i].sges[j].lkey;
                        assert(names_key(wrs[i].sges, k));
                        assert(send_keys(wrs).contains(k));
                        assert(pre.registered(attr.pd, wrs[i].sges[j], wrs[i].opcode.writes_local()));
                    }
                }
            }
        },
        Nic::Step::post_recv(q, wrs) => {
            let attr = pre.qps[q].attr;
            assert forall|i: int| 0 <= i < wrs.len() implies post.recv_wr_ok(post.qps[q].attr, #[trigger] wrs[i]) by {
                assert(pre.recv_wr_ok(attr, wrs[i]));
                assert forall|j: int| 0 <= j < wrs[i].sges.len()
                    implies post.registered(attr.pd, #[trigger] wrs[i].sges[j], true) by {
                    let k = wrs[i].sges[j].lkey;
                    assert(names_key(wrs[i].sges, k));
                    assert(recv_keys(wrs).contains(k));
                    assert(pre.registered(attr.pd, wrs[i].sges[j], true));
                }
            }
        },
        Nic::Step::create_qp(q, attr) => {
            assert forall|w: WqeRef| #[trigger] post.retired.contains_key(w) implies w.qp != q by {
                if w.qp == q {
                    assert(sh.owns.contains(w.qp));
                    assert(pre.retired.contains_key(w));
                }
            }
            let cqs1 = pre.cqs.insert(attr.send_cq, pre.cqs[attr.send_cq].reserve(q, attr.sq_depth));
            let cqs2 = cqs1.insert(attr.recv_cq, cqs1[attr.recv_cq].reserve(q, attr.rq_depth));
            let pcqs1 = post.cqs.insert(attr.send_cq, post.cqs[attr.send_cq].reserve(q, attr.sq_depth));
            let pcqs2 = pcqs1.insert(attr.recv_cq, pcqs1[attr.recv_cq].reserve(q, attr.rq_depth));
            assert(pcqs2[sh.cq].reserved == cqs2[sh.cq].reserved);
        },
        Nic::Step::destroy_cq(c) => {
            assert forall|q: QpId| #[trigger] post.qps.contains_key(q)
                implies post.qps[q].attr.send_cq != c && post.qps[q].attr.recv_cq != c by {
                if post.qps[q].attr.send_cq == c || post.qps[q].attr.recv_cq == c {
                    assert(sh.owns.contains(q));
                    assert(pre.qps.contains_key(q));
                }
            }
        },
        Nic::Step::poll_cq(c) => {
            assert(post.cqs[c].entries[0] == pre.cqs[c].entries[0]);
        },
        Nic::Step::modify_qp(q, to) => {},
        Nic::Step::destroy_qp(q) => {},
        _ => {},
    }
}

// =========================================================================================
// Runs

/// `framed` is reflexive.
pub proof fn lemma_framed_refl(sh: Share, pins: ISet<Key>, m: Nic::State)
    requires
        m.cqs.contains_key(sh.cq),
    ensures
        framed(sh, pins, m, m),
{
}

/// `framed` is transitive.
pub proof fn lemma_framed_trans(sh: Share, pins: ISet<Key>, a: Nic::State, b: Nic::State, c: Nic::State)
    requires
        framed(sh, pins, a, b),
        framed(sh, pins, b, c),
    ensures
        framed(sh, pins, a, c),
{
    assert forall|q: QpId| #[trigger] sh.owns.contains(q) && a.qps.contains_key(q)
        implies qp_kept(a.qps[q], c.qps[q]) by {
        let (x, y, z) = (a.qps[q], b.qps[q], c.qps[q]);
        assert(qp_kept(x, y) && qp_kept(y, z));
        assert forall|k: int| 0 <= k < x.sq.len() implies (#[trigger] z.sq[k]).wr == x.sq[k].wr by {
            assert(y.sq[k].wr == x.sq[k].wr);
        }
        assert forall|k: int| 0 <= k < x.rq.len() implies (#[trigger] z.rq[k]).wr == x.rq[k].wr by {
            assert(y.rq[k].wr == x.rq[k].wr);
        }
        assert forall|k: int| 0 <= k < x.sq_done implies (#[trigger] z.sq[k]).outcome == x.sq[k].outcome by {
            assert(y.sq[k].outcome == x.sq[k].outcome);
        }
        assert forall|k: int| 0 <= k < x.rq_done implies (#[trigger] z.rq[k]).outcome == x.rq[k].outcome by {
            assert(y.rq[k].outcome == x.rq[k].outcome);
        }
    }
    assert forall|i: int| 0 <= i < a.cqs[sh.cq].entries.len()
        implies #[trigger] c.cqs[sh.cq].entries[i] == a.cqs[sh.cq].entries[i] by {
        assert(b.cqs[sh.cq].entries[i] == a.cqs[sh.cq].entries[i]);
    }
    assert forall|w: WqeRef| #[trigger] sh.owns.contains(w.qp) && a.retired.contains_key(w)
        implies c.retired[w] == a.retired[w] by {
        assert(b.retired.contains_key(w));
    }
}

proof fn lemma_rely_run(sh: Share, pins: ISet<Key>, run: Seq<Nic::State>, steps: Seq<Nic::Step>, verb: Option<Nic::Step>)
    requires
        rely_run(sh, pins, run, steps),
        nic_inv(run[0]),
        share_wf(run[0], sh),
        verb matches Some(v) ==> mine(sh, pins, v) && nic_enabled(run[0], v),
    ensures
        nic_inv(run.last()),
        share_wf(run.last(), sh),
        framed(sh, pins, run[0], run.last()),
        verb matches Some(v) ==> nic_enabled(run.last(), v),
        nic_run(run),
    decreases run.len(),
{
    if run.len() == 1 {
        lemma_framed_refl(sh, pins, run[0]);
    } else {
        let (pr, ps) = (run.drop_last(), steps.drop_last());
        assert forall|i: int| 0 <= i < ps.len() implies foreign(sh, pins, #[trigger] ps[i]) && nic_step(pr[i], pr[i + 1], ps[i]) by {
            assert(ps[i] == steps[i] && pr[i] == run[i] && pr[i + 1] == run[i + 1]);
        }
        lemma_rely_run(sh, pins, pr, ps, verb);
        let k = steps.len() - 1;
        assert(pr.last() == run[k]);
        assert(foreign(sh, pins, steps[k]) && nic_step(run[k], run[k + 1], steps[k]));
        lemma_foreign_step(sh, pins, run[k], run[k + 1], steps[k]);
        lemma_framed_trans(sh, pins, run[0], run[k], run[k + 1]);
        if let Some(v) = verb {
            lemma_foreign_step_keeps_enabled(sh, pins, run[k], run[k + 1], steps[k], v);
        }
        crate::nic::lemma_nic_step_is_next(run[k], run[k + 1], steps[k]);
        crate::nic::lemma_step_keeps_invariant(run[k], run[k + 1]);
        assert forall|i: int| 0 <= i < run.len() - 1 implies Nic::State::next(#[trigger] run[i], run[i + 1]) by {
            if i < k {
                assert(pr[i] == run[i] && pr[i + 1] == run[i + 1]);
            }
        }
    }
}

/// What a worker may assume between two of its calls: M's invariants and the share's
/// well-formedness still hold, its share is framed, and every one of its verbs that was
/// enabled still is.
pub proof fn lemma_rely_frame(sh: Share, pins: ISet<Key>, m: Nic::State, n: Nic::State)
    requires
        nic_inv(m),
        share_wf(m, sh),
        rely_reaches(sh, pins, m, n),
    ensures
        nic_inv(n),
        share_wf(n, sh),
        framed(sh, pins, m, n),
        nic_reaches(m, n),
{
    let (run, steps) = choose|run: Seq<Nic::State>, steps: Seq<Nic::Step>|
        rely_run(sh, pins, run, steps) && run[0] == m && run.last() == n;
    lemma_rely_run(sh, pins, run, steps, None);
}

/// A verb of a worker's own, enabled in the state it last saw, is enabled after whatever the
/// NIC and the others did since.
pub proof fn lemma_rely_keeps_enabled(sh: Share, pins: ISet<Key>, m: Nic::State, n: Nic::State, verb: Nic::Step)
    requires
        nic_inv(m),
        share_wf(m, sh),
        rely_reaches(sh, pins, m, n),
        mine(sh, pins, verb),
        nic_enabled(m, verb),
    ensures
        nic_enabled(n, verb),
{
    let (run, steps) = choose|run: Seq<Nic::State>, steps: Seq<Nic::Step>|
        rely_run(sh, pins, run, steps) && run[0] == m && run.last() == n;
    lemma_rely_run(sh, pins, run, steps, Some(verb));
}

/// A run of the NIC's own steps is a rely run for every share.
pub proof fn lemma_env_is_rely(sh: Share, pins: ISet<Key>, m: Nic::State, n: Nic::State)
    requires
        nic_env_reaches(m, n),
    ensures
        rely_reaches(sh, pins, m, n),
{
    let (run, steps) = choose|run: Seq<Nic::State>, steps: Seq<Nic::Step>|
        nic_env_run(run, steps) && run[0] == m && run.last() == n;
    assert(rely_run(sh, pins, run, steps));
}

/// Every rely run is a run of M.
pub proof fn lemma_rely_is_nic_run(sh: Share, pins: ISet<Key>, m: Nic::State, n: Nic::State)
    requires
        nic_inv(m),
        share_wf(m, sh),
        rely_reaches(sh, pins, m, n),
    ensures
        nic_reaches(m, n),
{
    lemma_rely_frame(sh, pins, m, n);
}

/// Rely runs compose.
pub proof fn lemma_rely_trans(sh: Share, pins: ISet<Key>, a: Nic::State, b: Nic::State, c: Nic::State)
    requires
        rely_reaches(sh, pins, a, b),
        rely_reaches(sh, pins, b, c),
    ensures
        rely_reaches(sh, pins, a, c),
{
    let (r1, s1) = choose|run: Seq<Nic::State>, steps: Seq<Nic::Step>|
        rely_run(sh, pins, run, steps) && run[0] == a && run.last() == b;
    let (r2, s2) = choose|run: Seq<Nic::State>, steps: Seq<Nic::Step>|
        rely_run(sh, pins, run, steps) && run[0] == b && run.last() == c;
    let run = r1 + r2.drop_first();
    let steps = s1 + s2;
    assert forall|i: int| 0 <= i < steps.len() implies foreign(sh, pins, #[trigger] steps[i]) && nic_step(run[i], run[i + 1], steps[i]) by {
        if i < s1.len() {
            assert(steps[i] == s1[i] && run[i] == r1[i] && run[i + 1] == r1[i + 1]);
        } else {
            let j = i - s1.len();
            assert(steps[i] == s2[j] && run[i] == r2[j] && run[i + 1] == r2[j + 1]);
        }
    }
    assert(run.last() == c);
    assert(rely_run(sh, pins, run, steps));
}

/// Rely is reflexive.
pub proof fn lemma_rely_refl(sh: Share, pins: ISet<Key>, m: Nic::State)
    ensures
        rely_reaches(sh, pins, m, m),
{
    let run = seq![m];
    let steps = Seq::<Nic::Step>::empty();
    assert(run.last() == m);
    assert(rely_run(sh, pins, run, steps));
}

/// Rely with more pins is rely with fewer.
pub proof fn lemma_rely_fewer_pins(sh: Share, pins: ISet<Key>, fewer: ISet<Key>, m: Nic::State, n: Nic::State)
    requires
        rely_reaches(sh, pins, m, n),
        fewer.subset_of(pins),
    ensures
        rely_reaches(sh, fewer, m, n),
{
    let (run, steps) = choose|run: Seq<Nic::State>, steps: Seq<Nic::Step>|
        rely_run(sh, pins, run, steps) && run[0] == m && run.last() == n;
    assert forall|i: int| 0 <= i < steps.len() implies foreign(sh, fewer, #[trigger] steps[i]) && nic_step(run[i], run[i + 1], steps[i]) by {
        assert(foreign(sh, pins, steps[i]));
    }
    assert(rely_run(sh, fewer, run, steps));
}

// =========================================================================================
// One device

/// A step of the device the engine of share `sh` did not take: a verb of another engine on its own
/// share, apart from `sh`, or a step of the NIC itself or of the control path (protection domains
/// and memory regions).
pub open spec fn elsewhere(sh: Share, step: Nic::Step) -> bool {
    ||| exists|other: Share| #[trigger] disjoint(other, sh) && mine(other, ISet::full(), step)
    ||| is_nic_step(step)
    ||| step is alloc_pd || step is reg_mr || step is dereg_mr
}

/// The device went from `m` to `n` by steps the engine of share `sh` did not take: the device's
/// history between two moments, as that engine sees it.
pub open spec fn interleaved(sh: Share, m: Nic::State, n: Nic::State) -> bool {
    exists|run: Seq<Nic::State>, steps: Seq<Nic::Step>| {
        &&& run.len() > 0
        &&& run[0] == m
        &&& run.last() == n
        &&& steps.len() == run.len() - 1
        &&& forall|i: int| 0 <= i < steps.len()
            ==> elsewhere(sh, #[trigger] steps[i]) && nic_step(run[i], run[i + 1], steps[i])
    }
}

/// **One device, one run of M.** The interleaving of other engines' verbs, each on its own share,
/// with the steps of the NIC and of the control path, is a run of M that touches none of the share
/// of the engine that saw it: its rely, with no region it must keep.
pub proof fn lemma_interleaved(sh: Share, m: Nic::State, n: Nic::State)
    requires
        interleaved(sh, m, n),
    ensures
        rely_reaches(sh, ISet::empty(), m, n),
{
    let (run, steps) = choose|run: Seq<Nic::State>, steps: Seq<Nic::Step>| {
        &&& run.len() > 0
        &&& run[0] == m
        &&& run.last() == n
        &&& steps.len() == run.len() - 1
        &&& forall|i: int| 0 <= i < steps.len()
            ==> elsewhere(sh, #[trigger] steps[i]) && nic_step(run[i], run[i + 1], steps[i])
    };
    assert forall|i: int| 0 <= i < steps.len()
        implies foreign(sh, ISet::empty(), #[trigger] steps[i]) && nic_step(run[i], run[i + 1], steps[i]) by {
        if exists|other: Share| #[trigger] disjoint(other, sh) && mine(other, ISet::full(), steps[i]) {
            let other = choose|other: Share| #[trigger] disjoint(other, sh) && mine(other, ISet::full(), steps[i]);
            lemma_mine_is_foreign(other, ISet::full(), sh, ISet::empty(), steps[i]);
        }
    }
    assert(rely_run(sh, ISet::empty(), run, steps));
}

// =========================================================================================
// Guarantee

/// Two shares are disjoint: different CQs, and no QP name in common.
pub open spec fn disjoint(a: Share, b: Share) -> bool {
    &&& a.cq != b.cq
    &&& forall|q: QpId| #[trigger] a.owns.contains(q) ==> !b.owns.contains(q)
}

/// **Guarantee.** A verb a worker calls on its own share is a foreign step for every disjoint
/// share, whatever regions that one pins: so each worker's verbs are within what the others
/// rely on.
pub proof fn lemma_mine_is_foreign(sh: Share, pins: ISet<Key>, other: Share, other_pins: ISet<Key>, verb: Nic::Step)
    requires
        disjoint(sh, other),
        mine(sh, pins, verb),
    ensures
        foreign(other, other_pins, verb),
{
}

/// A verb of a worker's own keeps its share well formed (the guarantee's other half: what it
/// does to its own share leaves it a share).
pub proof fn lemma_mine_keeps_share_wf(sh: Share, pins: ISet<Key>, pre: Nic::State, post: Nic::State, verb: Nic::Step)
    requires
        nic_inv(pre),
        share_wf(pre, sh),
        mine(sh, pins, verb),
        !(verb is destroy_cq),
        nic_step(pre, post, verb),
    ensures
        share_wf(post, sh),
{
    match verb {
        Nic::Step::poll_cq(c) => {
            assert(pre.cq_wf(c));
            assert(pre.cqe_wf(c, pre.cqs[c].entries[0]));
        },
        _ => {},
    }
}

} // verus!
