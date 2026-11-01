//! A worker's share of A, framed across the other workers' abstract transitions.
//!
//! Shares own QP names and stable lease IDs, not disjoint sets of bytes. Frozen clones
//! may overlap while their IDs have different owners. A share reserves no future IDs:
//! unrelated allocations may advance A's global allocator and append to its wake log.
//! Existing IDs move between owners only through an explicit ghost ownership transfer.
//!
//! Every real variant of `Abs::Step` is covered by `foreign` and `lemma_foreign_step`.
//! A foreign completion acts on a foreign request, never on a local request on behalf
//! of the NIC. The local worker must account for its own completions as its own A steps.
//! This is a frame foundation, not a worker `Refines` implementation, a memory permission
//! construction, or a theorem composing all workers' concrete states.
#![allow(unused_imports)]

use crate::abs::*;
use crate::refinement::*;
use crate::types::*;
use vstd::prelude::*;

verus! {

/// QP names and lease IDs owned by one worker. IDs remain owned after a lease is taken
/// or dropped, so its historical outcome and posted bytes are still protected.
pub struct Share {
    pub qps: ISet<QpId>,
    pub ids: ISet<Id>,
}

impl Share {
    pub open spec fn disjoint(self, other: Share) -> bool {
        self.qps.disjoint(other.qps) && self.ids.disjoint(other.ids)
    }

    pub open spec fn with_ids(self, ids: ISet<Id>) -> Share {
        Share { qps: self.qps, ids }
    }
}

/// No future lease ID is reserved. A request belongs to this share exactly when its QP
/// does; program and exposed leases are owned separately by ID. QP names may be reserved
/// before creation. This adds no condition about the byte ranges of different shares.
pub open spec fn share_wf(a: Abs::State, sh: Share) -> bool {
    &&& forall|i: Id| #[trigger] sh.ids.contains(i) ==> i < a.next_id
    &&& forall|r: Id| #[trigger] a.leases.contains_key(r) && a.leases[r].is_request()
        ==> (sh.ids.contains(r) <==> sh.qps.contains(a.req(r).qp))
}

/// Permitted interference with a fixed share. A shared clone only reads its source and
/// allocates a distinct ID, so its source may be local. All mutations of existing leases
/// require a foreign ID. Posting also requires a foreign QP. A's own preconditions still
/// govern every step, including uniqueness, allocation and exposure rules.
pub open spec fn foreign(sh: Share, step: Abs::Step) -> bool {
    match step {
        Abs::Step::create_qp(q) => !sh.qps.contains(q),
        Abs::Step::drop_qp(q) => !sh.qps.contains(q),
        Abs::Step::alloc(_, _) => true,
        Abs::Step::split(i, _) => !sh.ids.contains(i),
        Abs::Step::freeze(i) => !sh.ids.contains(i),
        Abs::Step::share(_, _) => true,
        Abs::Step::thaw(i) => !sh.ids.contains(i),
        Abs::Step::drop(i) => !sh.ids.contains(i),
        Abs::Step::post_request(i, q, _) => !sh.ids.contains(i) && !sh.qps.contains(q),
        Abs::Step::complete(r, _) => !sh.ids.contains(r),
        Abs::Step::register(r, _) => !sh.ids.contains(r),
        Abs::Step::take(r, _) => !sh.ids.contains(r),
        Abs::Step::detach(r) => !sh.ids.contains(r),
        Abs::Step::expose(i, _) => !sh.ids.contains(i),
        Abs::Step::unexpose(i) => !sh.ids.contains(i),
        Abs::Step::dummy_to_use_type_params(_) => false,
    }
}

/// A worker acts on its own existing IDs and QP names. Steps allocating new IDs must
/// explicitly acquire those IDs in the worker's next share. This predicate alone does
/// not establish that share update or a concrete API call's simulation.
pub open spec fn mine(sh: Share, step: Abs::Step) -> bool {
    match step {
        Abs::Step::create_qp(q) => sh.qps.contains(q),
        Abs::Step::drop_qp(q) => sh.qps.contains(q),
        Abs::Step::alloc(_, _) => true,
        Abs::Step::split(i, _) => sh.ids.contains(i),
        Abs::Step::freeze(i) => sh.ids.contains(i),
        Abs::Step::share(i, _) => sh.ids.contains(i),
        Abs::Step::thaw(i) => sh.ids.contains(i),
        Abs::Step::drop(i) => sh.ids.contains(i),
        Abs::Step::post_request(i, q, _) => sh.ids.contains(i) && sh.qps.contains(q),
        Abs::Step::complete(r, _) => sh.ids.contains(r),
        Abs::Step::register(r, _) => sh.ids.contains(r),
        Abs::Step::take(r, _) => sh.ids.contains(r),
        Abs::Step::detach(r) => sh.ids.contains(r),
        Abs::Step::expose(i, _) => sh.ids.contains(i),
        Abs::Step::unexpose(i) => sh.ids.contains(i),
        Abs::Step::dummy_to_use_type_params(_) => false,
    }
}

/// Preserve both presence and absence, and the entire lease (including its phase,
/// registered waker and detached flag), outcome and posted-byte record at one ID.
pub open spec fn id_framed(pre: Abs::State, post: Abs::State, i: Id) -> bool {
    &&& post.leases.contains_key(i) <==> pre.leases.contains_key(i)
    &&& pre.leases.contains_key(i) ==> post.leases[i] == pre.leases[i]
    &&& post.outcomes.contains_key(i) <==> pre.outcomes.contains_key(i)
    &&& pre.outcomes.contains_key(i) ==> post.outcomes[i] == pre.outcomes[i]
    &&& post.posted.contains_key(i) <==> pre.posted.contains_key(i)
    &&& pre.posted.contains_key(i) ==> post.posted[i] == pre.posted[i]
}

/// Local records are unchanged; the allocator may advance and foreign wakes may append.
/// Waker names can alias across workers, so no disjointness of waker IDs is assumed.
pub open spec fn framed(sh: Share, pre: Abs::State, post: Abs::State) -> bool {
    &&& forall|q: QpId| #[trigger] sh.qps.contains(q)
        ==> (post.qps.contains(q) <==> pre.qps.contains(q))
    &&& forall|i: Id| #[trigger] sh.ids.contains(i) ==> id_framed(pre, post, i)
    &&& pre.next_id <= post.next_id
    &&& pre.wakes.len() <= post.wakes.len()
    &&& forall|i: int| 0 <= i < pre.wakes.len()
        ==> #[trigger] post.wakes[i] == pre.wakes[i]
}

// =========================================================================================
// One step: exhaustive coverage of A

/// A foreign step cannot allocate over, remove or mutate an owned ID.
proof fn lemma_foreign_id(sh: Share, pre: Abs::State, post: Abs::State, step: Abs::Step, i: Id)
    requires
        share_wf(pre, sh),
        sh.ids.contains(i),
        foreign(sh, step),
        abs_step(pre, post, step),
    ensures
        id_framed(pre, post, i),
{
    assert(i < pre.next_id);
    match step {
        Abs::Step::create_qp(_) => {},
        Abs::Step::drop_qp(_) => {},
        Abs::Step::alloc(_, _) => {},
        Abs::Step::split(j, _) => { assert(i != j && i != pre.next_id + 1); },
        Abs::Step::freeze(j) => { assert(i != j); },
        Abs::Step::share(_, _) => {},
        Abs::Step::thaw(j) => { assert(i != j); },
        Abs::Step::drop(j) => { assert(i != j); },
        Abs::Step::post_request(j, _, _) => { assert(i != j); },
        Abs::Step::complete(r, _) => { assert(i != r); },
        Abs::Step::register(r, _) => { assert(i != r); },
        Abs::Step::take(r, _) => { assert(i != r); },
        Abs::Step::detach(r) => { assert(i != r); },
        Abs::Step::expose(j, _) => { assert(i != j); },
        Abs::Step::unexpose(j) => { assert(i != j); },
        Abs::Step::dummy_to_use_type_params(_) => {},
    }
}

/// Every request left by a foreign step still has matching lease-ID and QP ownership.
proof fn lemma_foreign_request(
    sh: Share,
    pre: Abs::State,
    post: Abs::State,
    step: Abs::Step,
    r: Id,
)
    requires
        share_wf(pre, sh),
        foreign(sh, step),
        abs_step(pre, post, step),
        post.is_request(r),
    ensures
        sh.ids.contains(r) <==> sh.qps.contains(post.req(r).qp),
{
    match step {
        Abs::Step::create_qp(_) => {},
        Abs::Step::drop_qp(_) => {},
        Abs::Step::alloc(_, _) => { assert(r != pre.next_id); },
        Abs::Step::split(_, _) => { assert(r != pre.next_id && r != pre.next_id + 1); },
        Abs::Step::freeze(_) => {},
        Abs::Step::share(_, _) => { assert(r != pre.next_id); },
        Abs::Step::thaw(_) => {},
        Abs::Step::drop(_) => {},
        Abs::Step::post_request(_, q, _) => {
            if r == pre.next_id {
                assert(!sh.ids.contains(r));
                assert(post.req(r).qp == q);
            }
        },
        Abs::Step::complete(_, _) => {},
        Abs::Step::register(_, _) => {},
        Abs::Step::take(_, _) => { assert(r != pre.next_id); },
        Abs::Step::detach(_) => {},
        Abs::Step::expose(_, _) => {},
        Abs::Step::unexpose(_) => {},
        Abs::Step::dummy_to_use_type_params(_) => {},
    }
}

/// All fifteen real transition variants preserve A's invariant, the share, and its frame.
/// The restrictions classify interference; no transition or invariant of A is changed.
pub proof fn lemma_foreign_step(sh: Share, pre: Abs::State, post: Abs::State, step: Abs::Step)
    requires
        abs_inv(pre),
        share_wf(pre, sh),
        foreign(sh, step),
        abs_step(pre, post, step),
    ensures
        abs_inv(post),
        share_wf(post, sh),
        framed(sh, pre, post),
{
    crate::abs::lemma_abs_step_is_next(pre, post, step);
    crate::abs::lemma_step_keeps_invariant(pre, post);
    assert forall|i: Id| #[trigger] sh.ids.contains(i) implies id_framed(pre, post, i) by {
        lemma_foreign_id(sh, pre, post, step, i);
    }
    assert forall|r: Id| #[trigger] post.leases.contains_key(r) && post.leases[r].is_request()
        implies (sh.ids.contains(r) <==> sh.qps.contains(post.req(r).qp)) by {
        lemma_foreign_request(sh, pre, post, step, r);
    }
    match step {
        Abs::Step::create_qp(q) => { assert(!sh.qps.contains(q)); },
        Abs::Step::drop_qp(q) => { assert(!sh.qps.contains(q)); },
        Abs::Step::alloc(_, _) => {},
        Abs::Step::split(_, _) => {},
        Abs::Step::freeze(_) => {},
        Abs::Step::share(_, _) => {},
        Abs::Step::thaw(_) => {},
        Abs::Step::drop(_) => {},
        Abs::Step::post_request(_, _, _) => {},
        Abs::Step::complete(_, _) => {},
        Abs::Step::register(_, _) => {},
        Abs::Step::take(_, _) => {},
        Abs::Step::detach(_) => {},
        Abs::Step::expose(_, _) => {},
        Abs::Step::unexpose(_) => {},
        Abs::Step::dummy_to_use_type_params(_) => {},
    }
}

/// A worker's own step is foreign interference for every disjoint share. Acquiring fresh
/// IDs and transferring existing IDs remain separate ownership obligations.
pub proof fn lemma_mine_is_foreign(left: Share, right: Share, step: Abs::Step)
    requires
        left.disjoint(right),
        mine(left, step),
    ensures
        foreign(right, step),
{
    match step {
        Abs::Step::create_qp(q) => { assert(!right.qps.contains(q)); },
        Abs::Step::drop_qp(q) => { assert(!right.qps.contains(q)); },
        Abs::Step::alloc(_, _) => {},
        Abs::Step::split(i, _) => { assert(!right.ids.contains(i)); },
        Abs::Step::freeze(i) => { assert(!right.ids.contains(i)); },
        Abs::Step::share(_, _) => {},
        Abs::Step::thaw(i) => { assert(!right.ids.contains(i)); },
        Abs::Step::drop(i) => { assert(!right.ids.contains(i)); },
        Abs::Step::post_request(i, q, _) => {
            assert(!right.ids.contains(i) && !right.qps.contains(q));
        },
        Abs::Step::complete(r, _) => { assert(!right.ids.contains(r)); },
        Abs::Step::register(r, _) => { assert(!right.ids.contains(r)); },
        Abs::Step::take(r, _) => { assert(!right.ids.contains(r)); },
        Abs::Step::detach(r) => { assert(!right.ids.contains(r)); },
        Abs::Step::expose(i, _) => { assert(!right.ids.contains(i)); },
        Abs::Step::unexpose(i) => { assert(!right.ids.contains(i)); },
        Abs::Step::dummy_to_use_type_params(_) => {},
    }
}

// =========================================================================================
// Rely executions

pub open spec fn rely_run(sh: Share, run: Seq<Abs::State>, steps: Seq<Abs::Step>) -> bool {
    &&& run.len() > 0
    &&& steps.len() == run.len() - 1
    &&& forall|i: int| 0 <= i < steps.len()
        ==> foreign(sh, #[trigger] steps[i]) && abs_step(run[i], run[i + 1], steps[i])
}

pub open spec fn rely_reaches(sh: Share, pre: Abs::State, post: Abs::State) -> bool {
    exists|run: Seq<Abs::State>, steps: Seq<Abs::Step>| rely_run(sh, run, steps)
        && run[0] == pre && run.last() == post
}

pub proof fn lemma_framed_refl(sh: Share, a: Abs::State)
    ensures
        framed(sh, a, a),
{
}

pub proof fn lemma_framed_trans(sh: Share, a: Abs::State, b: Abs::State, c: Abs::State)
    requires
        framed(sh, a, b),
        framed(sh, b, c),
    ensures
        framed(sh, a, c),
{
    assert forall|i: Id| #[trigger] sh.ids.contains(i) implies id_framed(a, c, i) by {
        assert(id_framed(a, b, i) && id_framed(b, c, i));
    }
    assert forall|i: int| 0 <= i < a.wakes.len() implies #[trigger] c.wakes[i] == a.wakes[i] by {
        assert(b.wakes[i] == a.wakes[i]);
    }
}

/// Rely runs really are executions of the existing A, not a replacement abstract machine.
pub proof fn lemma_rely_is_abs_run(sh: Share, run: Seq<Abs::State>, steps: Seq<Abs::Step>)
    requires
        rely_run(sh, run, steps),
    ensures
        abs_run(run),
        abs_reaches(run[0], run.last()),
{
    assert forall|i: int| 0 <= i < run.len() - 1 implies
        Abs::State::next(#[trigger] run[i], run[i + 1]) by {
        crate::abs::lemma_abs_step_is_next(run[i], run[i + 1], steps[i]);
    }
}

/// Induction over arbitrary finite foreign interleavings, including fresh allocations
/// and completions that append foreign wakes.
pub proof fn lemma_rely_run(sh: Share, run: Seq<Abs::State>, steps: Seq<Abs::Step>)
    requires
        rely_run(sh, run, steps),
        abs_inv(run[0]),
        share_wf(run[0], sh),
    ensures
        abs_inv(run.last()),
        share_wf(run.last(), sh),
        framed(sh, run[0], run.last()),
    decreases run.len(),
{
    if run.len() == 1 {
        lemma_framed_refl(sh, run[0]);
    } else {
        let prefix = run.drop_last();
        let prior_steps = steps.drop_last();
        assert forall|i: int| 0 <= i < prior_steps.len() implies
            foreign(sh, #[trigger] prior_steps[i]) && abs_step(prefix[i], prefix[i + 1], prior_steps[i]) by {
            assert(prior_steps[i] == steps[i] && prefix[i] == run[i] && prefix[i + 1] == run[i + 1]);
        }
        lemma_rely_run(sh, prefix, prior_steps);
        let k = steps.len() - 1;
        assert(prefix.last() == run[k]);
        lemma_foreign_step(sh, run[k], run[k + 1], steps[k]);
        lemma_framed_trans(sh, run[0], run[k], run[k + 1]);
    }
}

/// A fixed worker share and its records survive every permitted rely execution.
pub proof fn lemma_rely_frame(sh: Share, pre: Abs::State, post: Abs::State)
    requires
        abs_inv(pre),
        share_wf(pre, sh),
        rely_reaches(sh, pre, post),
    ensures
        abs_inv(post),
        share_wf(post, sh),
        framed(sh, pre, post),
        abs_reaches(pre, post),
{
    let (run, steps) = choose|run: Seq<Abs::State>, steps: Seq<Abs::Step>|
        rely_run(sh, run, steps) && run[0] == pre && run.last() == post;
    lemma_rely_run(sh, run, steps);
    lemma_rely_is_abs_run(sh, run, steps);
}

/// In particular, foreign work preserves a local in-flight lease, its registered waker,
/// and the absence of an outcome. Global wake-log growth does not consume that waker.
pub proof fn lemma_rely_inflight(sh: Share, pre: Abs::State, post: Abs::State, r: Id)
    requires
        abs_inv(pre),
        share_wf(pre, sh),
        sh.ids.contains(r),
        pre.in_flight(r),
        rely_reaches(sh, pre, post),
    ensures
        post.in_flight(r),
        post.leases[r] == pre.leases[r],
        post.req(r) == pre.req(r),
        !post.outcomes.contains_key(r),
        post.posted[r] == pre.posted[r],
{
    lemma_rely_frame(sh, pre, post);
}

// =========================================================================================
// Explicit ownership transfer, with no change to A

/// Transfer live program leases between two disjoint owners. QP names do not move here,
/// and request/exposed leases cannot be transferred by this operation. A separate protocol
/// is needed to transfer a QP or its requests. The abstract state itself does not change.
pub open spec fn transfer(
    a: Abs::State,
    from: Share,
    to: Share,
    moved: ISet<Id>,
    from_after: Share,
    to_after: Share,
) -> bool {
    &&& from.disjoint(to)
    &&& moved.subset_of(from.ids)
    &&& forall|i: Id| #[trigger] moved.contains(i) ==> a.is_program(i)
    &&& from_after == from.with_ids(from.ids.difference(moved))
    &&& to_after == to.with_ids(to.ids.union(moved))
}

/// Moving program-lease IDs preserves both well-formed shares and disjoint ownership,
/// without requiring their read-only bytes to be disjoint.
pub proof fn lemma_transfer(
    a: Abs::State,
    from: Share,
    to: Share,
    moved: ISet<Id>,
    from_after: Share,
    to_after: Share,
)
    requires
        share_wf(a, from),
        share_wf(a, to),
        transfer(a, from, to, moved, from_after, to_after),
    ensures
        share_wf(a, from_after),
        share_wf(a, to_after),
        from_after.disjoint(to_after),
        forall|i: Id| #[trigger] moved.contains(i)
            ==> !from_after.ids.contains(i) && to_after.ids.contains(i),
{
    assert forall|r: Id| #[trigger] a.leases.contains_key(r) && a.leases[r].is_request()
        implies !moved.contains(r) by {
        if moved.contains(r) {
            assert(a.is_program(r));
        }
    }
    assert forall|r: Id| #[trigger] a.leases.contains_key(r) && a.leases[r].is_request()
        implies (from_after.ids.contains(r) <==> from_after.qps.contains(a.req(r).qp)) by {
        assert(!moved.contains(r));
    }
    assert forall|r: Id| #[trigger] a.leases.contains_key(r) && a.leases[r].is_request()
        implies (to_after.ids.contains(r) <==> to_after.qps.contains(a.req(r).qp)) by {
        assert(!moved.contains(r));
    }
}

} // verus!
