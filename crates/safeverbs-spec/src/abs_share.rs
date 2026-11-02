//! A worker's share of A, framed across the other workers' abstract transitions.
//!
//! Shares own QP names and stable group handles, not disjoint sets of bytes. Frozen clones
//! may overlap while their IDs have different owners. A share reserves no future IDs:
//! unrelated allocations may advance A's global allocator and append to its wake log.
//! Moving an existing group between owners is an explicit ghost ownership transfer;
//! member identities and permissions remain unchanged.
//!
//! Every real variant of `Abs::Step` is covered by `foreign` and `lemma_foreign_step`.
//! A foreign completion acts on a foreign request, never on a local request on behalf
//! of the NIC. The local worker must account for its own completions as its own A steps.
//! This is a frame foundation, not a worker `Refines` implementation, a memory permission
//! construction, or a theorem composing all workers' concrete states.
//!
//! Frame equality does not preserve operation enabledness. A foreign read-only clone can
//! leave a local lease unchanged while invalidating its `thaw` precondition. Each worker
//! must establish A's current preconditions when taking its own next step.
//!
//! A request now owns an ordered group of separately named leases with per-member access
//! directions. Framing preserves that whole group without imposing uniform permissions.
//! Connecting those ghost resources to concrete buffer permissions is still an obligation.
#![allow(unused_imports)]

use crate::abs::*;
use crate::refinement::*;
use crate::types::*;
use vstd::prelude::*;

verus! {

/// QP names and group handles owned by one worker. Handles remain owned after a group is taken
/// or dropped, so its historical outcome and original resource sequence remain protected.
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
    &&& forall|r: Id| #[trigger] a.groups.contains_key(r) && a.groups[r].is_request()
        ==> (sh.ids.contains(r) <==> sh.qps.contains(a.req(r).qp))
}

/// Permitted interference with a fixed share. A shared clone only reads its source and
/// allocates a distinct ID, so its source may be local. All mutations of existing leases
/// require a foreign ID. Posting also requires a foreign QP. A's own preconditions still
/// govern every step, including uniqueness, allocation and exposure rules.
pub open spec fn foreign(sh: Share, step: Abs::Step) -> bool {
    match step {
        Abs::Step::empty() => true,
        Abs::Step::join(i, j) => !sh.ids.contains(i) && !sh.ids.contains(j),
        Abs::Step::separate(i, _) => !sh.ids.contains(i),
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
        Abs::Step::empty() => true,
        Abs::Step::join(i, j) => sh.ids.contains(i) && sh.ids.contains(j),
        Abs::Step::separate(i, _) => sh.ids.contains(i),
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
    &&& post.groups.contains_key(i) <==> pre.groups.contains_key(i)
    &&& pre.groups.contains_key(i) ==> post.groups[i] == pre.groups[i]
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
        Abs::Step::empty() => {},
        Abs::Step::join(j, k) => { assert(i != j && i != k); },
        Abs::Step::separate(j, _) => { assert(i != j && i != pre.next_id + 1); },
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
        Abs::Step::empty() => { assert(r != pre.next_id); },
        Abs::Step::join(_, _) => { assert(r != pre.next_id); },
        Abs::Step::separate(_, _) => { assert(r != pre.next_id && r != pre.next_id + 1); },
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

/// All eighteen real transition variants preserve A's invariant, the share, and its frame.
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
    assert forall|r: Id| #[trigger] post.groups.contains_key(r) && post.groups[r].is_request()
        implies (sh.ids.contains(r) <==> sh.qps.contains(post.req(r).qp)) by {
        lemma_foreign_request(sh, pre, post, step, r);
    }
    match step {
        Abs::Step::empty() => {},
        Abs::Step::join(_, _) => {},
        Abs::Step::separate(_, _) => {},
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
        Abs::Step::empty() => {},
        Abs::Step::join(i, j) => { assert(!right.ids.contains(i) && !right.ids.contains(j)); },
        Abs::Step::separate(i, _) => { assert(!right.ids.contains(i)); },
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
        post.groups[r] == pre.groups[r],
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
    assert forall|r: Id| #[trigger] a.groups.contains_key(r) && a.groups[r].is_request()
        implies !moved.contains(r) by {
        if moved.contains(r) {
            assert(a.is_program(r));
        }
    }
    assert forall|r: Id| #[trigger] a.groups.contains_key(r) && a.groups[r].is_request()
        implies (from_after.ids.contains(r) <==> from_after.qps.contains(a.req(r).qp)) by {
        assert(!moved.contains(r));
    }
    assert forall|r: Id| #[trigger] a.groups.contains_key(r) && a.groups[r].is_request()
        implies (to_after.ids.contains(r) <==> to_after.qps.contains(a.req(r).qp)) by {
        assert(!moved.contains(r));
    }
}

// =========================================================================================
// Concrete interleavings, without premises

/// Start A, clone one frozen buffer, and post the two IDs on different workers' QPs.
/// Foreign allocation, waker registration, completion and take preserve the local request.
/// After a local completion, more foreign work preserves its outcome and appends wakes.
#[verifier::spinoff_prover]
#[verifier::rlimit(20)]
pub proof fn example_two_workers() {
    let mem = Span { start: 0, len: 64 }.bytes();
    let fresh = Span { start: 128, len: 64 }.bytes();
    let ok = Outcome { status: Status::Success, kind: CompletionKind::StatusOnly, byte_len: 0, imm: None };
    let a0 = Abs::State {
        qps: Set::empty(), groups: Map::empty(), next_id: 0,
        outcomes: Map::empty(), posted: Map::empty(), wakes: Seq::empty(),
    };
    assert(Abs::State::start(a0));
    Abs::State::start_inductive(a0);
    let a1 = Abs::State { qps: a0.qps.insert(7), ..a0 };
    assert(Abs::State::create_qp(a0, a1, 7));
    Abs::State::create_qp_inductive(a0, a1, 7);
    let a2 = Abs::State { qps: a1.qps.insert(8), ..a1 };
    assert(Abs::State::create_qp(a1, a2, 8));
    Abs::State::create_qp_inductive(a1, a2, 8);
    let unique = Group::singleton(0, mem, true, true);
    let a3 = Abs::State { groups: a2.groups.insert(0, unique), next_id: 1, ..a2 };
    assert(Abs::State::alloc(a2, a3, mem, true));
    Abs::State::alloc_inductive(a2, a3, mem, true);
    let frozen = Group::singleton(0, mem, false, true);
    let a4 = Abs::State { groups: a3.groups.insert(0, frozen), ..a3 };
    assert(Abs::State::freeze(a3, a4, 0));
    Abs::State::freeze_inductive(a3, a4, 0);
    let a5 = Abs::State { groups: a4.groups.insert(1, Group::singleton(1, mem, false, true)), next_id: 2, ..a4 };
    assert(Abs::State::share(a4, a5, 0, mem));
    Abs::State::share_inductive(a4, a5, 0, mem);
    let local_req = Req { qp: 7, dirs: seq![Dir::Out], phase: Phase::InFlight, waker: None, detached: false };
    let other_req = Req { qp: 8, ..local_req };
    let clone = Group::singleton(1, mem, false, true);
    let a6 = Abs::State {
        groups: a5.groups.remove(0).insert(2, frozen.with_req(local_req)),
        posted: a5.posted.insert(2, frozen.members), next_id: 3, ..a5
    };
    assert(Abs::State::post_request(a5, a6, 0, 7, seq![Dir::Out]));
    Abs::State::post_request_inductive(a5, a6, 0, 7, seq![Dir::Out]);
    let a7 = Abs::State {
        groups: a6.groups.remove(1).insert(3, clone.with_req(other_req)),
        posted: a6.posted.insert(3, clone.members), next_id: 4, ..a6
    };
    assert(Abs::State::post_request(a6, a7, 1, 8, seq![Dir::Out]));
    Abs::State::post_request_inductive(a6, a7, 1, 8, seq![Dir::Out]);
    let a8 = Abs::State { groups: a7.groups.insert(2, frozen.with_req(local_req.with_waker(9))), ..a7 };
    assert(Abs::State::register(a7, a8, 2, 9));
    Abs::State::register_inductive(a7, a8, 2, 9);
    let a9 = Abs::State { groups: a8.groups.insert(3, clone.with_req(other_req.with_waker(11))), ..a8 };
    assert(Abs::State::register(a8, a9, 3, 11));
    Abs::State::register_inductive(a8, a9, 3, 11);
    let local = Share { qps: ISet::empty().insert(7), ids: ISet::empty().insert(2) };
    let other = Share { qps: ISet::empty().insert(8), ids: ISet::empty().insert(3) };
    assert(local.disjoint(other));
    assert(share_wf(a9, local) && share_wf(a9, other));
    assert(a9.groups[2].members[0].mem.contains(0) && a9.groups[3].members[0].mem.contains(0));

    let b1 = Abs::State {
        groups: a9.groups.insert(4, Group::singleton(4, fresh, true, true)), next_id: 5, ..a9
    };
    assert(abs_step(a9, b1, Abs::Step::alloc(fresh, true)));
    lemma_foreign_step(local, a9, b1, Abs::Step::alloc(fresh, true));
    // Acquisition of the freshly allocated ID is explicit in the other worker's share.
    let other1 = other.with_ids(other.ids.insert(4));
    assert(share_wf(b1, other1) && local.disjoint(other1));
    let b2 = Abs::State { groups: b1.groups.insert(3, clone.with_req(other_req.with_waker(12))), ..b1 };
    assert(abs_step(b1, b2, Abs::Step::register(3, 12)));
    lemma_mine_is_foreign(other1, local, Abs::Step::register(3, 12));
    lemma_foreign_step(local, b1, b2, Abs::Step::register(3, 12));
    let b3 = Abs::State {
        groups: b2.groups.insert(3, clone.with_req(other_req.completed(ok))),
        outcomes: b2.outcomes.insert(3, ok), wakes: b2.wakes.push(12), ..b2
    };
    assert(abs_step(b2, b3, Abs::Step::complete(3, ok)));
    lemma_foreign_step(local, b2, b3, Abs::Step::complete(3, ok));
    let b4 = Abs::State { groups: b3.groups.remove(3).insert(5, clone), next_id: 6, ..b3 };
    assert(abs_step(b3, b4, Abs::Step::take(3, ok)));
    lemma_foreign_step(local, b3, b4, Abs::Step::take(3, ok));
    let other2 = other1.with_ids(other1.ids.insert(5));
    assert(share_wf(b4, other2) && local.disjoint(other2));

    let run = seq![a9, b1, b2, b3, b4];
    let steps = seq![Abs::Step::alloc(fresh, true), Abs::Step::register(3, 12),
        Abs::Step::complete(3, ok), Abs::Step::take(3, ok)];
    assert forall|i: int| 0 <= i < steps.len() implies
        foreign(local, #[trigger] steps[i]) && abs_step(run[i], run[i + 1], steps[i]) by {
        if i == 0 {} else if i == 1 {} else if i == 2 {} else { assert(i == 3); }
    }
    assert(rely_run(local, run, steps));
    assert(rely_reaches(local, a9, b4));
    lemma_rely_inflight(local, a9, b4, 2);
    assert(b4.req(2).waker == Some(9));
    assert(!b4.outcomes.contains_key(2));
    assert(b4.groups[2].members[0].mem == mem);
    assert(b4.next_id == 6 && b4.wakes == seq![12nat]);

    // The local worker completes its own request. Foreign work may append more wakes,
    // but cannot replace the completed local lease or its historical outcome.
    let c0 = Abs::State {
        groups: b4.groups.insert(2, frozen.with_req(local_req.completed(ok))),
        outcomes: b4.outcomes.insert(2, ok), wakes: b4.wakes.push(9), ..b4
    };
    assert(Abs::State::complete(b4, c0, 2, ok));
    Abs::State::complete_inductive(b4, c0, 2, ok);
    assert(share_wf(c0, local));
    let c1 = Abs::State {
        groups: c0.groups.remove(5).insert(6, clone.with_req(other_req)),
        posted: c0.posted.insert(6, clone.members), next_id: 7, ..c0
    };
    assert(abs_step(c0, c1, Abs::Step::post_request(5, 8, seq![Dir::Out])));
    lemma_foreign_step(local, c0, c1, Abs::Step::post_request(5, 8, seq![Dir::Out]));
    let other3 = other2.with_ids(other2.ids.insert(6));
    assert(share_wf(c1, other3) && local.disjoint(other3));
    let c2 = Abs::State { groups: c1.groups.insert(6, clone.with_req(other_req.with_waker(13))), ..c1 };
    assert(abs_step(c1, c2, Abs::Step::register(6, 13)));
    lemma_foreign_step(local, c1, c2, Abs::Step::register(6, 13));
    let c3 = Abs::State {
        groups: c2.groups.insert(6, clone.with_req(other_req.completed(ok))),
        outcomes: c2.outcomes.insert(6, ok), wakes: c2.wakes.push(13), ..c2
    };
    assert(abs_step(c2, c3, Abs::Step::complete(6, ok)));
    lemma_foreign_step(local, c2, c3, Abs::Step::complete(6, ok));
    lemma_framed_trans(local, c0, c1, c2);
    lemma_framed_trans(local, c0, c2, c3);
    assert(c3.groups[2] == c0.groups[2]);
    assert(c3.outcomes[2] == ok && c3.req(2).waker is None);
    assert(c3.wakes == seq![12nat, 9nat, 13nat]);
}

/// A frozen clone gets a separate fresh ID, then that ID is explicitly transferred to
/// another worker. Its bytes overlap the source; the source's ID and lease remain local.
#[verifier::spinoff_prover]
pub proof fn example_clone_transfer() {
    let mem = Span { start: 0, len: 64 }.bytes();
    let frozen = Group::singleton(0, mem, false, true);
    let a0 = Abs::State {
        qps: Set::empty().insert(7).insert(8), groups: Map::empty().insert(0, frozen),
        next_id: 1, outcomes: Map::empty(), posted: Map::empty(), wakes: Seq::empty(),
    };
    assert(abs_inv(a0));
    let left = Share { qps: ISet::empty().insert(7), ids: ISet::empty().insert(0) };
    let right = Share { qps: ISet::empty().insert(8), ids: ISet::empty() };
    assert(share_wf(a0, left) && share_wf(a0, right) && left.disjoint(right));
    let a1 = Abs::State { groups: a0.groups.insert(1, Group::singleton(1, mem, false, true)), next_id: 2, ..a0 };
    assert(abs_step(a0, a1, Abs::Step::share(0, mem)));
    // Cloning is nonmutating interference even when the source ID is locally owned.
    lemma_foreign_step(left, a0, a1, Abs::Step::share(0, mem));
    lemma_foreign_step(right, a0, a1, Abs::Step::share(0, mem));
    let acquired = left.with_ids(left.ids.insert(1));
    assert(share_wf(a1, acquired) && acquired.disjoint(right));
    let moved = ISet::empty().insert(1);
    let left_after = acquired.with_ids(acquired.ids.difference(moved));
    let right_after = right.with_ids(right.ids.union(moved));
    assert(transfer(a1, acquired, right, moved, left_after, right_after));
    lemma_transfer(a1, acquired, right, moved, left_after, right_after);
    assert(left_after.ids.contains(0) && !left_after.ids.contains(1));
    assert(right_after.ids.contains(1) && !right_after.ids.contains(0));
    assert(a1.groups[0].members[0].mem.contains(0) && a1.groups[1].members[0].mem.contains(0));
    assert(!a1.groups[0].members[0].mem.disjoint(a1.groups[1].members[0].mem));
    assert(a1.groups[0] == a0.groups[0]);
    let clone = Group::singleton(1, mem, false, true);
    let req = Req { qp: 8, dirs: seq![Dir::Out], phase: Phase::InFlight, waker: None, detached: false };
    let a2 = Abs::State {
        groups: a1.groups.remove(1).insert(2, clone.with_req(req)),
        posted: a1.posted.insert(2, clone.members), next_id: 3, ..a1
    };
    let step = Abs::Step::post_request(1, 8, seq![Dir::Out]);
    assert(mine(right_after, step));
    assert(abs_step(a1, a2, step));
    lemma_mine_is_foreign(right_after, left_after, step);
    lemma_foreign_step(left_after, a1, a2, step);
    // The request's fresh ID is acquired separately from the transferred clone's ID.
    let right_posted = right_after.with_ids(right_after.ids.insert(2));
    assert(share_wf(a2, right_posted) && left_after.disjoint(right_posted));
    assert(a2.groups[0] == a0.groups[0] && a2.in_flight(2));
    assert(a2.groups[0].members[0].mem.contains(0) && a2.groups[2].members[0].mem.contains(0));
    assert(!foreign(left_after, Abs::Step::drop(0)));
}

} // verus!
