//! **Liveness**: that outcomes come, under a premise about the device.
//!
//! F1 and R1 are safety: an outcome is right if there is one, and a buffer is released only once
//! the NIC is done with it. Neither says that anything happens, and M does not either: its NIC may
//! execute a WQE at any time, never included. Liveness needs a premise about the run, which is not a
//! rule of M and is stated here as a hypothesis of the theorems that use it:
//!
//! - **The device executes the WQEs it holds** ([`executes`]): a WQE posted and not executed is
//!   executed later. A queue pair's removal needs less, that it flushes those of a queue pair in the
//!   error state ([`flushes`]).
//! - **The CQEs are polled** ([`polls`]): a WQE with a CQE waiting retires later. That is the
//!   program polling the CQ, and a poll returning a CQE when the CQ holds one.
//!
//! Then a request gets its outcome once a WQE at or after its last one, on its queue, completes with
//! a CQE ([`theorem_f1_live`]): a receive, or a signaled send request, gets it once the device
//! executes its last WQE ([`corollary_f1_live`]). An unsignaled send request is retired by a later
//! CQE of its queue, so nothing comes of it while nothing is posted after it: the ring signals a
//! request after at most `depth - max_wqes` unsignaled WQEs, and a queue pair's removal posts a
//! signaled WQE of its own.
//!
//! The library's own waits, for room on a send queue and for a queue pair's drain, end under the same
//! premise: that is proved in the core, with [`lemma_settles`].
#![allow(unused_imports)]

use crate::abs::*;
use crate::nic::*;
#[cfg(verus_keep_ghost)]
use crate::nic_proofs::{history_ok, known, lemma_history_init, lemma_history_reaches};
use crate::refinement::*;
use crate::share::*;
use crate::types::*;
use vstd::prelude::*;

verus! {

// =========================================================================================
// The premise

/// How many WQEs of queue `queue` the NIC has executed, since its queue pair was created.
pub open spec fn executed_of(qp: Qp, queue: Queue) -> nat {
    match queue {
        Queue::Send => qp.sq_head + qp.sq_done,
        Queue::Recv => qp.rq_head + qp.rq_done,
    }
}

/// How many WQEs queue `queue` was given, since its queue pair was created.
pub open spec fn posted_of(qp: Qp, queue: Queue) -> nat {
    match queue {
        Queue::Send => qp.sq_head + qp.sq.len(),
        Queue::Recv => qp.rq_head + qp.rq.len(),
    }
}

/// WQE `w` is posted in `m` and the NIC has not executed it.
pub open spec fn unexecuted(m: Nic::State, w: WqeRef) -> bool {
    &&& posted(m, w)
    &&& w.seq >= executed_of(m.qps[w.qp], w.queue)
}

/// WQE `w` is posted in `m`, executed, and its CQE waits in its CQ: a receive, or a send that was
/// signaled or failed.
pub open spec fn completed(m: Nic::State, w: WqeRef) -> bool {
    &&& posted(m, w)
    &&& !unexecuted(m, w)
    &&& w.queue is Send ==> m.qps[w.qp].sq[w.seq - m.qps[w.qp].sq_head].generates()
}

/// **The device executes every WQE it holds**, along the run of M `nic`: a WQE posted and not
/// executed is executed later.
pub open spec fn executes(nic: spec_fn(nat) -> Nic::State) -> bool {
    forall|k: nat, w: WqeRef| #[trigger] unexecuted(nic(k), w)
        ==> exists|l: nat| l >= k && !unexecuted(#[trigger] nic(l), w)
}

/// **The device executes the send WQEs it holds**: [`executes`] for send queues, which a wait for
/// room needs.
pub open spec fn executes_sends(nic: spec_fn(nat) -> Nic::State) -> bool {
    forall|k: nat, w: WqeRef| #[trigger] unexecuted(nic(k), w) && w.queue is Send
        ==> exists|l: nat| l >= k && !unexecuted(#[trigger] nic(l), w)
}

/// **The device flushes**: [`executes`] for the WQEs of a queue pair in the error state, where
/// each fails.
pub open spec fn flushes(nic: spec_fn(nat) -> Nic::State) -> bool {
    forall|k: nat, w: WqeRef| #[trigger] unexecuted(nic(k), w) && nic(k).qps[w.qp].state is Err
        ==> exists|l: nat| l >= k && !unexecuted(#[trigger] nic(l), w)
}

/// **Every CQE is polled**, along the run of M `nic`: a WQE with a CQE waiting retires later.
pub open spec fn polls(nic: spec_fn(nat) -> Nic::State) -> bool {
    forall|k: nat, w: WqeRef| #[trigger] completed(nic(k), w)
        ==> exists|l: nat| l >= k && (#[trigger] nic(l)).retired.contains_key(w)
}

// =========================================================================================
// Runs of the core

/// `run` is a run of the core without end: a start state, then calls of its API, the NIC and the
/// peer doing whatever M allows in between.
pub open spec fn refines_run<C: Refines>(run: spec_fn(nat) -> C) -> bool {
    &&& run(0).init()
    &&& forall|k: nat| #[trigger] run(k).step(&run(k + 1))
}

/// The run of M a run of the core stands for.
pub open spec fn nic_of<C: Refines>(run: spec_fn(nat) -> C) -> spec_fn(nat) -> Nic::State {
    |k: nat| run(k).nic()
}

/// What Theorem 1 says at each point of a run, with M's history kept.
proof fn lemma_run_at<C: Refines>(run: spec_fn(nat) -> C, l: nat)
    requires
        refines_run(run),
    ensures
        run(l).inv(),
        abs_inv(run(l).abs()),
        nic_inv(run(l).nic()),
        history_ok(run(l).nic()),
        coupled(run(l).abs(), run(l).nic(), run(l).ranges(), run(l).wrs()),
        nic_init(run(0).nic()),
        nic_reaches(run(0).nic(), run(l).nic()),
    decreases l,
{
    if l == 0 {
        C::lemma_init(run(0));
        lemma_abs_init_invariant(run(0).abs());
        lemma_nic_init_invariant(run(0).nic());
        lemma_nic_reaches_refl(run(0).nic());
    } else {
        let k = (l - 1) as nat;
        lemma_run_at(run, k);
        assert(run(k).step(&run(k + 1)));
        C::lemma_step(run(k), run(l));
        lemma_abs_reaches_invariant(run(k).abs(), run(l).abs());
        lemma_nic_reaches_invariant(run(k).nic(), run(l).nic());
        lemma_nic_reaches_trans(run(0).nic(), run(k).nic(), run(l).nic());
    }
    C::lemma_coupled(run(l));
    let m0 = run(0).nic();
    lemma_nic_init_invariant(m0);
    lemma_history_init(m0);
    lemma_history_reaches(m0, run(l).nic(), IMap::empty());
}

/// From one point of a run to a later one: A and M by their own steps, a request keeping its
/// WQEs and a WQE its WR.
proof fn lemma_run_between<C: Refines>(run: spec_fn(nat) -> C, i: nat, l: nat)
    requires
        refines_run(run),
        i <= l,
    ensures
        abs_reaches(run(i).abs(), run(l).abs()),
        nic_reaches(run(i).nic(), run(l).nic()),
        run(i).ranges().submap_of(run(l).ranges()),
        run(i).wrs().submap_of(run(l).wrs()),
    decreases l - i,
{
    if l == i {
        lemma_abs_reaches_refl(run(i).abs());
        lemma_nic_reaches_refl(run(i).nic());
    } else {
        let k = (l - 1) as nat;
        lemma_run_between(run, i, k);
        lemma_run_at(run, k);
        assert(run(k).step(&run(k + 1)));
        C::lemma_step(run(k), run(l));
        lemma_abs_reaches_trans(run(i).abs(), run(k).abs(), run(l).abs());
        lemma_nic_reaches_trans(run(i).nic(), run(k).nic(), run(l).nic());
        assert forall|r: ReqId| #[trigger] run(i).ranges().dom().contains(r)
            implies run(l).ranges().dom().contains(r) && run(i).ranges()[r] == run(l).ranges()[r] by {
            assert(run(k).ranges().dom().contains(r));
        }
        assert forall|w: WqeRef| #[trigger] run(i).wrs().dom().contains(w)
            implies run(l).wrs().dom().contains(w) && run(i).wrs()[w] == run(l).wrs()[w] by {
            assert(run(k).wrs().dom().contains(w));
        }
    }
}

/// A request in flight stays in flight or gets its outcome: whatever A does, it leaves flight only
/// by `complete`.
proof fn lemma_in_flight_kept(a: Abs::State, b: Abs::State, r: ReqId)
    requires
        abs_inv(a),
        abs_reaches(a, b),
        a.in_flight(r),
    ensures
        b.in_flight(r) || b.outcomes.contains_key(r),
{
    let run = choose|run: Seq<Abs::State>| run.len() > 0 && run[0] == a && run.last() == b && abs_run(run);
    lemma_in_flight_run(run, r);
}

proof fn lemma_in_flight_run(run: Seq<Abs::State>, r: ReqId)
    requires
        run.len() > 0,
        abs_run(run),
        abs_inv(run[0]),
        run[0].in_flight(r),
    ensures
        run.last().in_flight(r) || run.last().outcomes.contains_key(r),
    decreases run.len(),
{
    if run.len() > 1 {
        let prefix = run.drop_last();
        assert forall|i: int| 0 <= i < prefix.len() - 1 implies Abs::State::next(#[trigger] prefix[i], prefix[i + 1]) by {
            assert(prefix[i] == run[i] && prefix[i + 1] == run[i + 1]);
        }
        assert(prefix[0] == run[0]);
        lemma_in_flight_run(prefix, r);
        lemma_abs_run_invariant(prefix);
        let (pre, post) = (prefix.last(), run.last());
        assert(Abs::State::next(run[run.len() - 2], run[run.len() - 1]));
        crate::guarantees::lemma_f1_one_outcome(pre, post, r);
        reveal(Abs::State::next);
        reveal(Abs::State::next_by);
    }
}

/// A request in flight at point `i` has its outcome at point `l` if M retired by then a WQE at or
/// after its last one, on its queue: M retires a queue in posting order.
proof fn lemma_outcome_once_retired<C: Refines>(run: spec_fn(nat) -> C, i: nat, r: ReqId, l: nat, w: WqeRef)
    requires
        refines_run(run),
        i <= l,
        run(i).abs().in_flight(r),
        w.qp == run(i).ranges()[r].qp,
        w.queue == run(i).ranges()[r].queue,
        run(i).ranges()[r].end <= w.seq + 1,
        run(l).nic().retired.contains_key(w),
    ensures
        run(l).abs().outcomes.contains_key(r),
{
    lemma_run_at(run, i);
    lemma_run_at(run, l);
    lemma_run_between(run, i, l);
    lemma_in_flight_kept(run(i).abs(), run(l).abs(), r);
    let range = run(i).ranges()[r];
    if run(l).abs().in_flight(r) {
        assert(run(l).ranges()[r] == range);
        lemma_pending_last(range, run(l).nic());
        let m = run(l).nic();
        assert(w.seq < head_of(m.qps[w.qp], w.queue));
        assert(m.retired.contains_key(range.last()));
    }
}

// =========================================================================================
// F1-live

/// **F1-live.** Along a run of the core in which every CQE is polled, a request in flight gets its
/// outcome once a WQE at or after its last one, on its queue, has completed with a CQE: its own
/// last WQE if that is a receive or signaled, a later signaled one of its queue, or one that failed.
/// Exactly one: no step changes an outcome (`lemma_f1_one_outcome`).
pub proof fn theorem_f1_live<C: Refines>(run: spec_fn(nat) -> C, i: nat, r: ReqId, j: nat, w: WqeRef)
    requires
        refines_run(run),
        polls(nic_of(run)),
        run(i).abs().in_flight(r),
        i <= j,
        w.qp == run(i).ranges()[r].qp,
        w.queue == run(i).ranges()[r].queue,
        run(i).ranges()[r].end <= w.seq + 1,
        completed(run(j).nic(), w),
    ensures
        exists|l: nat| l >= i && #[trigger] run(l).abs().outcomes.contains_key(r),
{
    assert(completed(nic_of(run)(j), w));
    let l = choose|l: nat| l >= j && (#[trigger] nic_of(run)(l)).retired.contains_key(w);
    lemma_outcome_once_retired(run, i, r, l, w);
}

/// **F1-live for a receive and a signaled send request.** Along a run of the core in which the
/// device executes every WQE it holds and every CQE is polled, a request in flight whose last WQE is
/// a receive, or a signaled send, gets its outcome.
pub proof fn corollary_f1_live<C: Refines>(run: spec_fn(nat) -> C, i: nat, r: ReqId)
    requires
        refines_run(run),
        executes(nic_of(run)),
        polls(nic_of(run)),
        run(i).abs().in_flight(r),
        run(i).ranges()[r].queue is Send ==> run(i).wrs()[run(i).ranges()[r].last()].signaled,
    ensures
        exists|l: nat| l >= i && #[trigger] run(l).abs().outcomes.contains_key(r),
{
    lemma_run_at(run, i);
    let range = run(i).ranges()[r];
    let w = range.last();
    lemma_pending_last(range, run(i).nic());
    // When the device has executed it.
    let j = if unexecuted(nic_of(run)(i), w) {
        choose|l: nat| l >= i && !unexecuted(#[trigger] nic_of(run)(l), w)
    } else {
        i
    };
    assert(!unexecuted(run(j).nic(), w));
    lemma_run_at(run, j);
    lemma_run_between(run, i, j);
    // M knows it at `j` too: posted still, or retired.
    lemma_nic_init_invariant(run(0).nic());
    lemma_history_init(run(0).nic());
    let log = lemma_history_reaches(run(0).nic(), run(i).nic(), IMap::empty());
    lemma_history_reaches(run(i).nic(), run(j).nic(), log);
    assert(known(run(i).nic(), w));
    if run(j).nic().retired.contains_key(w) {
        lemma_outcome_once_retired(run, i, r, j, w);
    } else {
        let m = run(j).nic();
        if w.queue is Send {
            // Its WR is the one it was posted with, signaled.
            assert(run(j).wrs()[w] == run(i).wrs()[w]);
            assert(m.qps[w.qp].sq[w.seq - m.qps[w.qp].sq_head].wr == run(j).wrs()[w]);
        }
        assert(completed(m, w));
        theorem_f1_live(run, i, r, j, w);
    }
}

// =========================================================================================
// For the library's waits

/// How many WQEs QP `q`'s queues have been given, since it was created.
pub open spec fn wqes_posted(m: Nic::State, q: QpId) -> nat {
    posted_of(m.qps[q], Queue::Send) + posted_of(m.qps[q], Queue::Recv)
}

/// How many WQEs of QP `q`'s queues the NIC has executed, since it was created.
pub open spec fn wqes_executed(m: Nic::State, q: QpId) -> nat {
    executed_of(m.qps[q], Queue::Send) + executed_of(m.qps[q], Queue::Recv)
}

/// Queue pair `b` is queue pair `a` given more WQEs, perhaps, and having executed more.
pub open spec fn counts_grow(a: Qp, b: Qp) -> bool {
    &&& executed_of(a, Queue::Send) <= executed_of(b, Queue::Send)
    &&& executed_of(a, Queue::Recv) <= executed_of(b, Queue::Recv)
    &&& posted_of(a, Queue::Send) <= posted_of(b, Queue::Send)
    &&& posted_of(a, Queue::Recv) <= posted_of(b, Queue::Recv)
}

/// Queue pair `b` is queue pair `a` having executed more, perhaps, and given no WQE.
pub open spec fn executes_only(a: Qp, b: Qp) -> bool {
    &&& counts_grow(a, b)
    &&& posted_of(a, Queue::Send) == posted_of(b, Queue::Send)
    &&& posted_of(a, Queue::Recv) == posted_of(b, Queue::Recv)
}

/// A WQE the NIC executed is counted: its queue pair's count of executed WQEs grows.
pub proof fn lemma_executed_count(m: Nic::State, n: Nic::State, w: WqeRef)
    requires
        unexecuted(m, w),
        !unexecuted(n, w),
        n.qps.contains_key(w.qp),
        counts_grow(m.qps[w.qp], n.qps[w.qp]),
    ensures
        wqes_executed(m, w.qp) < wqes_executed(n, w.qp),
{
}

/// Polling a CQE retires executed WQEs: no queue pair is given or executes a WQE.
pub proof fn lemma_poll_counts(pre: Nic::State, post: Nic::State, c: CqId)
    requires
        nic_inv(pre),
        nic_step(pre, post, Nic::Step::poll_cq(c)),
    ensures
        post.qps.dom() == pre.qps.dom(),
        forall|q: QpId| #[trigger] pre.qps.contains_key(q) ==> {
            &&& executes_only(pre.qps[q], post.qps[q])
            &&& wqes_executed(post, q) == wqes_executed(pre, q)
            &&& post.qps[q].state == pre.qps[q].state
        },
{
    let e = pre.cqs[c].entries[0];
    crate::nic_proofs::lemma_head_is_first(pre, c);
    assert(pre.qp_wf(pre.qps[e.qp]));
    assert(post.qps.dom() =~= pre.qps.dom());
}

/// Posting send WRs on queue pair `q` gives it that many WQEs, none executed; no other queue pair
/// changes.
pub proof fn lemma_post_counts(pre: Nic::State, post: Nic::State, q: QpId, wrs: Seq<SendWr>)
    requires
        nic_step(pre, post, Nic::Step::post_send(q, wrs)),
    ensures
        post.qps.dom() == pre.qps.dom(),
        counts_grow(pre.qps[q], post.qps[q]),
        posted_of(post.qps[q], Queue::Send) == posted_of(pre.qps[q], Queue::Send) + wrs.len(),
        posted_of(post.qps[q], Queue::Recv) == posted_of(pre.qps[q], Queue::Recv),
        wqes_executed(post, q) == wqes_executed(pre, q),
        post.qps[q].state == pre.qps[q].state,
        forall|p: QpId| p != q && #[trigger] pre.qps.contains_key(p) ==> post.qps[p] == pre.qps[p],
{
    assert(post.qps.dom() =~= pre.qps.dom());
}

/// What the NIC and the other engines do between two calls of an engine leaves each of its queue
/// pairs with the WQEs it was given, executes some, and keeps the error state.
pub proof fn lemma_rely_counts(sh: Share, pins: ISet<Key>, m: Nic::State, n: Nic::State, q: QpId)
    requires
        nic_inv(m),
        share_wf(m, sh),
        rely_reaches(sh, pins, m, n),
        sh.owns.contains(q),
        m.qps.contains_key(q),
    ensures
        n.qps.contains_key(q),
        executes_only(m.qps[q], n.qps[q]),
        m.qps[q].state is Err ==> n.qps[q].state is Err,
{
    lemma_rely_frame(sh, pins, m, n);
}

/// A measure that never grows along a run stops falling: from some point on it stays as it is. So
/// a wait whose every step that the device owes is followed by a fall, under the premise, ends.
pub proof fn lemma_settles(rank: spec_fn(nat) -> nat) -> (k: nat)
    requires
        forall|i: nat| #[trigger] rank(i + 1) <= rank(i),
    ensures
        forall|j: nat| j >= k ==> #[trigger] rank(j) == rank(k),
{
    lemma_settles_from(rank, 0)
}

proof fn lemma_settles_from(rank: spec_fn(nat) -> nat, start: nat) -> (k: nat)
    requires
        forall|i: nat| #[trigger] rank(i + 1) <= rank(i),
    ensures
        k >= start,
        forall|j: nat| j >= k ==> #[trigger] rank(j) == rank(k),
    decreases rank(start),
{
    if forall|j: nat| j >= start ==> #[trigger] rank(j) == rank(start) {
        start
    } else {
        assert(exists|j: nat| j >= start && #[trigger] rank(j) != rank(start));
        let j = choose|j: nat| j >= start && #[trigger] rank(j) != rank(start);
        lemma_falls(rank, start, j);
        lemma_settles_from(rank, j)
    }
}

/// What holds at `a` and not at `b`, later, stops holding at some step between.
pub proof fn lemma_stops(p: spec_fn(nat) -> bool, a: nat, b: nat) -> (k: nat)
    requires
        a <= b,
        p(a),
        !p(b),
    ensures
        a <= k < b,
        p(k),
        !p(k + 1),
    decreases b - a,
{
    if p(a + 1) {
        lemma_stops(p, a + 1, b)
    } else {
        a
    }
}

/// A measure that never grows is no larger later.
proof fn lemma_falls(rank: spec_fn(nat) -> nat, i: nat, j: nat)
    requires
        forall|k: nat| #[trigger] rank(k + 1) <= rank(k),
        i <= j,
    ensures
        rank(j) <= rank(i),
    decreases j - i,
{
    if i < j {
        lemma_falls(rank, i, (j - 1) as nat);
        assert(rank(((j - 1) as nat) + 1) <= rank((j - 1) as nat));
    }
}

} // verus!
