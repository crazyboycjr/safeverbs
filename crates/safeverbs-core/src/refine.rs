//! The ring refines the abstract machine A (`safeverbs_spec::abs`).
//!
//! Every operation of [`Ring`] is specified by what A sees of the ring: its
//! requests (`Ring::req`: posted and not freed), the requests written but
//! not posted (`Ring::staged`) and the wakers woken (`Ring::woken`).
//! This module proves that each of those changes is zero or more steps of
//! A, from any state of A in which the ring's requests sit (`placed`) to
//! one in which they sit again. That is the ring's share of the obligations
//! of `safeverbs_spec::refinement::Refines`:
//!
//! | Ring operation | Steps of A |
//! | --- | --- |
//! | `begin`, `push_wqe`, `take_detached_error` | none (`lemma_ring_stutter`) |
//! | `end` | none: a staged request's leases are still the program's (`lemma_ring_end`) |
//! | `commit` | `post_request` for each staged request, then its `register` or `detach` (`lemma_ring_commit`) |
//! | `abort`, `abort_keeping` | `drop` for each staged request without an owner; none for the others, whose leases the ring keeps for their owners (refused) or hands back (`lemma_ring_abort`) |
//! | `complete` | `complete` for each request retired, in order, waking the same wakers (`lemma_ring_complete`) |
//! | `take` | `take` (`lemma_ring_take`); none for a refused request (`lemma_ring_take_refused`) |
//! | `detach` | `detach`, none for a staged request, `drop` for a refused one (`lemma_ring_detach`) |
//! | `set_waker` | `register`, none for a staged request (`lemma_ring_register`) |
//! | `release_all` | `complete` for each request in flight, then `detach`, and `drop` for each staged or refused one (`lemma_ring_release_all`) |
//!
//! A request whose doorbell was refused never reached A: its leases stay
//! the program's (A's lease is unchanged), and the ring only keeps them
//! for the owner (`View::refused`), who takes them back with a flush error
//! or drops them.
//!
//! The async layer's side list of results moved out of a clogged ring is
//! covered too (`with_side`, `lemma_side_move`, `lemma_side_take`,
//! `lemma_side_detach` and their `_refused` forms): moving a result there
//! is no step, taking it is A's `take` with the outcome and leases the ring
//! gave (none for a refused one), dropping its future A's `detach` (`drop`
//! for a refused one).
//!
//! Leases are tracked as values: the `L` that A sees a request hold is the
//! one `end` moved into the ring, and `take` and `abort_keeping` hand back
//! that value. With the linearity of `L` (Rust moves it; the ring never
//! duplicates it), this proves retention of the ring's resource values: its leases stay
//! in its slot, unreachable, from `end` until A says the NIC is done with
//! them (complete, or never posted), and leave it exactly once.
//!
//! Which group of A a request's `L` stands for (`ids`) and its members' directions
//! (`dirs`) are ghost maps the queue pair keeps; the ring needs neither.
//!
//! Everything here is ghost code: a plain build erases it.
// The imports are used by ghost code only.
#![allow(unused_imports)]

use safeverbs_spec::abs::*;
use safeverbs_spec::types::{Outcome, QpId, Status};
use vstd::prelude::*;

use crate::ring::{ReqView, Ring};
use crate::wc::Wc;

verus! {

/// A pure ghost interpretation of each request's concrete resource value, with one direction
/// per member. A future buffer-token proof must construct this interpretation honestly;
/// these adapters prove that the ring preserves it rather than assuming a whole worker
/// refinement. No runtime field, allocation or check is introduced.
#[verifier::reject_recursive_types(L)]
pub ghost struct Permissions<L> {
    /// The operation's required access for each retained resource member.
    pub access: Map<int, Seq<Dir>>,
    /// The exact ordered members at a request sequence and concrete lease value. Equal
    /// shared values in different requests can have distinct abstract member identities.
    /// The sequence stays fixed through posting, refusal, side storage and taking; it is
    /// not A's group handle, which can change without changing the members.
    pub resources: spec_fn(int, L) -> Seq<Lease>,
}

impl<L> Permissions<L> {
    /// Whether this request has an assigned per-member access sequence.
    pub open spec fn contains_key(self, s: int) -> bool { self.access.contains_key(s) }
    /// Assigns member directions without changing the resource interpretation.
    pub open spec fn insert(self, s: int, dirs: Seq<Dir>) -> Self {
        Permissions { access: self.access.insert(s, dirs), resources: self.resources }
    }
}

/// What A sees of a ring: its requests and its staged requests, by sequence
/// number.
pub ghost struct View<L> {
    /// The requests A sees: posted and not freed.
    pub req: spec_fn(int) -> Option<ReqView<L>>,
    /// The requests written and not posted, whose leases A still gives the
    /// program.
    pub staged: spec_fn(int) -> Option<ReqView<L>>,
    /// The leases of the requests whose doorbell was refused, kept for
    /// their owners: never posted, so A still gives them to the program.
    pub refused: spec_fn(int) -> Option<L>,
}

/// A request is well formed: a detached one has no waker, a complete one
/// has an owner and no waker.
pub open spec fn req_wf<L>(r: ReqView<L>) -> bool {
    &&& r.detached ==> r.waker is None
    &&& r.phase is Complete ==> r.waker is None && !r.detached
}

/// A staged request is well formed: not complete, and without a waker if
/// detached.
pub open spec fn staged_wf<L>(r: ReqView<L>) -> bool {
    &&& r.phase is InFlight
    &&& r.detached ==> r.waker is None
}

/// Every request and staged request of `v` is well formed, and a request
/// is at most one of posted, staged and refused.
pub open spec fn wf<L>(v: View<L>) -> bool {
    &&& forall|s: int| #[trigger] (v.req)(s) is Some ==> req_wf((v.req)(s)->0)
    &&& forall|s: int| #[trigger] (v.staged)(s) is Some ==> staged_wf((v.staged)(s)->0)
    &&& forall|s: int| #[trigger] (v.refused)(s) is Some ==> (v.req)(s) is None && (v.staged)(
        s,
    ) is None
    &&& forall|s: int| #[trigger] (v.staged)(s) is Some ==> (v.req)(s) is None
}

/// Request `s` holds a lease (posted, staged or refused).
pub open spec fn holds<L>(v: View<L>, s: int) -> bool {
    (v.req)(s) is Some || (v.staged)(s) is Some || (v.refused)(s) is Some
}

/// The request of A that request view `r` stands for, on queue pair `q`.
pub open spec fn as_req<L>(r: ReqView<L>, q: QpId, dir: Seq<Dir>) -> Req {
    Req { qp: q, dirs: dir, phase: r.phase, waker: r.waker, detached: r.detached }
}

/// The abstract group is exactly the resource sequence represented by request `s`'s `L`.
pub open spec fn resources_match<L>(v: View<L>, a: Abs::State, ids: Map<int, Id>, dirs: Permissions<L>, s: int) -> bool {
    &&& (v.req)(s) is Some ==> a.groups[ids[s]].members == (dirs.resources)(s, (v.req)(s)->0.leases)
    &&& (v.staged)(s) is Some ==> a.groups[ids[s]].members == (dirs.resources)(s, (v.staged)(s)->0.leases)
    &&& (v.refused)(s) is Some ==> a.groups[ids[s]].members == (dirs.resources)(s, (v.refused)(s)->0)
}

/// The ring's requests sit in `a`: every request A sees in the ring is the
/// request group `ids[s]` of `a`, on queue pair `q`, with member directions
/// `dirs.access[s]`, with the same phase, waker and owner; every staged request's
/// leases are the program group `ids[s]`, which may be posted in direction
/// `dirs.access[s]`; every refused request's leases are the program lease
/// `ids[s]`; no two hold the same lease.
pub open spec fn placed<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
) -> bool {
    &&& abs_inv(a)
    &&& wf(v)
    &&& a.qps.contains(q)
    &&& forall|s: int| #[trigger] holds(v, s) ==> resources_match(v, a, ids, dirs, s)
    &&& forall|s: int| #[trigger] holds(v, s) ==> ids.contains_key(s) && dirs.contains_key(s)
    &&& forall|s: int|
        #[trigger] (v.req)(s) is Some ==> a.groups.contains_key(ids[s]) && a.groups[ids[s]].holder
            == (Holder::Request { req: as_req((v.req)(s)->0, q, dirs.access[s]) })
    &&& forall|s: int|
        #[trigger] (v.staged)(s) is Some ==> a.is_program(ids[s]) && a.groups[ids[s]].allows(dirs.access[s])
    &&& forall|s: int| #[trigger] (v.refused)(s) is Some ==> a.is_program(ids[s])
    &&& forall|s: int, t: int|
        #![trigger ids[s], ids[t]]
        holds(v, s) && holds(v, t) && s != t ==> ids[s] != ids[t]
}

/// The names of the wakers held by requests `[from, to)` of `v`, in order.
pub open spec fn wakers<L>(v: View<L>, from: int, to: int) -> Seq<WakerId>
    decreases to - from,
{
    if to <= from {
        Seq::empty()
    } else {
        let before = wakers(v, from, to - 1);
        match (v.req)(to - 1) {
            Some(r) => match r.waker {
                Some(w) => before.push(w),
                None => before,
            },
            None => before,
        }
    }
}

/// `v` and `w` agree.
pub open spec fn same<L>(v: View<L>, w: View<L>) -> bool {
    &&& forall|s: int| #[trigger] (v.req)(s) == (w.req)(s)
    &&& forall|s: int| #[trigger] (v.staged)(s) == (w.staged)(s)
    &&& forall|s: int| #[trigger] (v.refused)(s) == (w.refused)(s)
}

/// The outcome A gives a request in flight when its queue pair is destroyed.
pub open spec fn flushed() -> Outcome {
    Outcome { status: Status::FlushErr, kind: safeverbs_spec::types::CompletionKind::StatusOnly, byte_len: 0, imm: None }
}

// =============================================================================
// Single steps

/// Placement carries over to a view that agrees.
pub proof fn lemma_same<L>(v: View<L>, w: View<L>, a: Abs::State, q: QpId, ids: Map<int, Id>, dirs: Permissions<L>)
    requires
        placed(v, a, q, ids, dirs),
        same(v, w),
    ensures
        placed(w, a, q, ids, dirs),
{
    assert forall|s: int| #[trigger] holds(w, s) implies resources_match(w, a, ids, dirs, s) by {
        assert(holds(v, s));
        assert(resources_match(v, a, ids, dirs, s));
        assert((v.req)(s) == (w.req)(s) && (v.staged)(s) == (w.staged)(s) && (v.refused)(s) == (w.refused)(s));
    }
    assert forall|s: int| #[trigger] (w.req)(s) is Some implies req_wf((w.req)(s)->0) by {
        assert((v.req)(s) == (w.req)(s));
    }
    assert forall|s: int| #[trigger] (w.staged)(s) is Some implies staged_wf((w.staged)(s)->0) by {
        assert((v.staged)(s) == (w.staged)(s));
    }
    assert forall|s: int| #[trigger] (w.refused)(s) is Some implies (w.req)(s) is None && (w.staged)(
        s,
    ) is None by {
        assert((v.refused)(s) == (w.refused)(s));
        assert((v.req)(s) == (w.req)(s));
        assert((v.staged)(s) == (w.staged)(s));
    }
    assert forall|s: int| #[trigger] (w.staged)(s) is Some implies (w.req)(s) is None by {
        assert((v.req)(s) == (w.req)(s));
        assert((v.staged)(s) == (w.staged)(s));
    }
    assert forall|s: int| #[trigger] holds(w, s) implies ids.contains_key(s) && dirs.contains_key(s) by {
        assert((v.refused)(s) == (w.refused)(s));
        assert(holds(v, s));
    }
    assert forall|s: int| #[trigger] (w.refused)(s) is Some implies a.is_program(ids[s]) by {
        assert((v.refused)(s) == (w.refused)(s));
    }
    assert forall|s: int| #[trigger] (w.req)(s) is Some implies a.groups.contains_key(ids[s])
        && a.groups[ids[s]].holder == (Holder::Request { req: as_req((w.req)(s)->0, q, dirs.access[s]) }) by {
        assert((v.req)(s) == (w.req)(s));
    }
    assert forall|s: int| #[trigger] (w.staged)(s) is Some implies a.is_program(ids[s]) && a.groups[ids[s]].allows(dirs.access[s]) by {
        assert((v.staged)(s) == (w.staged)(s));
    }
    assert forall|s: int, t: int|
        #![trigger ids[s], ids[t]]
        holds(w, s) && holds(w, t) && s != t implies ids[s] != ids[t] by {
        assert((v.refused)(s) == (w.refused)(s));
        assert((v.refused)(t) == (w.refused)(t));
        assert(holds(v, s) && holds(v, t));
    }
}

/// A's `complete` of request `s`, in flight, with `o`: the view after it.
pub open spec fn completed<L>(v: View<L>, s: int, o: Wc) -> View<L> {
    View {
        req: |t: int|
            if t == s {
                if (v.req)(s)->0.detached {
                    None
                } else {
                    Some((v.req)(s)->0.completed(o))
                }
            } else {
                (v.req)(t)
            },
        staged: v.staged,
        refused: v.refused,
    }
}

/// A's state after `complete` of request lease `r` with outcome `o`.
pub open spec fn complete_state(a: Abs::State, r: Id, o: Outcome) -> Abs::State {
    let l = a.groups[r];
    let req = a.req(r);
    Abs::State {
        outcomes: a.outcomes.insert(r, o),
        wakes: if req.waker is Some {
            a.wakes.push(req.waker->0)
        } else {
            a.wakes
        },
        groups: if req.detached {
            a.groups.remove(r)
        } else {
            a.groups.insert(r, l.with_req(req.completed(o)))
        },
        ..a
    }
}

/// Completing request `s` in flight is A's `complete` of its lease.
pub proof fn lemma_complete_one<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    s: int,
    o: Wc,
)
    requires
        placed(v, a, q, ids, dirs),
        (v.req)(s) is Some,
        (v.req)(s)->0.phase is InFlight,
    ensures
        abs_step(a, complete_state(a, ids[s], o.outcome()), Abs::Step::complete(ids[s], o.outcome())),
        placed(completed(v, s, o), complete_state(a, ids[s], o.outcome()), q, ids, dirs),
        complete_state(a, ids[s], o.outcome()).wakes == match (v.req)(s)->0.waker {
            Some(w) => a.wakes.push(w),
            None => a.wakes,
        },
{
    let r = ids[s];
    let a2 = complete_state(a, r, o.outcome());
    let w = completed(v, s, o);
    assert(holds(v, s));
    assert(abs_step(a, a2, Abs::Step::complete(r, o.outcome())));
    safeverbs_spec::abs::lemma_abs_step_is_next(a, a2, Abs::Step::complete(r, o.outcome()));
    safeverbs_spec::refinement::lemma_abs_reaches_next(a, a2);
    safeverbs_spec::refinement::lemma_abs_reaches_invariant(a, a2);
    assert forall|t: int| #[trigger] holds(w, t) implies resources_match(w, a2, ids, dirs, t) by {
        assert(holds(v, t));
        assert(resources_match(v, a, ids, dirs, t));
        if t != s { assert(ids[t] != ids[s]); }
    }
    assert forall|t: int| #[trigger] holds(w, t) implies ids.contains_key(t) && dirs.contains_key(t) by {
        assert(holds(v, t));
    }
    assert forall|t: int| #[trigger] (w.req)(t) is Some implies a2.groups.contains_key(ids[t])
        && a2.groups[ids[t]].holder == (Holder::Request { req: as_req((w.req)(t)->0, q, dirs.access[t]) }) by {
        if t != s {
            assert(holds(v, t));
        }
    }
    assert forall|t: int| #[trigger] (w.staged)(t) is Some implies a2.is_program(ids[t]) && a2.groups[ids[t]].allows(dirs.access[t]) by {
        assert(holds(v, t));
    }
    assert forall|t: int| #[trigger] (w.refused)(t) is Some implies a2.is_program(ids[t]) by {
        assert(holds(v, t));
    }
    assert forall|t: int, u: int|
        #![trigger ids[t], ids[u]]
        holds(w, t) && holds(w, u) && t != u implies ids[t] != ids[u] by {
        assert(holds(v, t) && holds(v, u));
    }
}

/// Requests `[from, to)` of `v` retired with outcomes `o`: each in flight
/// completes (and is gone if detached); refused ones A never saw.
pub open spec fn retired_range<L>(v: View<L>, from: int, to: int, o: spec_fn(int) -> Wc) -> View<L> {
    View {
        req: |t: int|
            if from <= t < to && (v.req)(t) is Some {
                if (v.req)(t)->0.detached {
                    None
                } else {
                    Some((v.req)(t)->0.completed(o(t)))
                }
            } else {
                (v.req)(t)
            },
        staged: v.staged,
        refused: v.refused,
    }
}

/// Retiring requests `[from, to)` in order is A's `complete` of each, and
/// wakes their wakers in that order.
pub proof fn lemma_complete<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    from: int,
    to: int,
    o: spec_fn(int) -> Wc,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, ids, dirs),
        from <= to,
        forall|s: int| from <= s < to && #[trigger] (v.req)(s) is Some ==> (v.req)(s)->0.phase is InFlight,
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(retired_range(v, from, to, o), a2, q, ids, dirs),
        a2.wakes == a.wakes + wakers(v, from, to),
    decreases to - from,
{
    if to == from {
        safeverbs_spec::refinement::lemma_abs_reaches_refl(a);
        lemma_same(v, retired_range(v, from, to, o), a, q, ids, dirs);
        assert(a.wakes =~= a.wakes + wakers(v, from, to));
        a
    } else if (v.req)(to - 1) is None {
        let s = to - 1;
        let a1 = lemma_complete(v, a, q, ids, dirs, from, s, o);
        lemma_same(retired_range(v, from, s, o), retired_range(v, from, to, o), a1, q, ids, dirs);
        a1
    } else {
        let s = to - 1;
        let a1 = lemma_complete(v, a, q, ids, dirs, from, s, o);
        let v1 = retired_range(v, from, s, o);
        assert((v1.req)(s) == (v.req)(s));
        lemma_complete_one(v1, a1, q, ids, dirs, s, o(s));
        let a2 = complete_state(a1, ids[s], o(s).outcome());
        lemma_same(completed(v1, s, o(s)), retired_range(v, from, to, o), a2, q, ids, dirs);
        safeverbs_spec::abs::lemma_abs_step_is_next(a1, a2, Abs::Step::complete(ids[s], o(s).outcome()));
        safeverbs_spec::refinement::lemma_abs_reaches_next(a1, a2);
        safeverbs_spec::refinement::lemma_abs_reaches_trans(a, a1, a2);
        assert(a2.wakes =~= a.wakes + wakers(v, from, to));
        a2
    }
}

/// A's state after posting program group `i` on `q` with per-member `dir`.
pub open spec fn post_state(a: Abs::State, i: Id, q: QpId, dir: Seq<Dir>) -> Abs::State {
    let l = a.groups[i];
    let r = a.next_id;
    let req = Req { qp: q, dirs: dir, phase: Phase::InFlight, waker: None, detached: false };
    Abs::State {
        groups: a.groups.remove(i).insert(r, l.with_req(req)),
        posted: a.posted.insert(r, l.members),
        next_id: r + 1,
        ..a
    }
}

/// A's state after `register` of waker `w` on request lease `r`.
pub open spec fn register_state(a: Abs::State, r: Id, w: WakerId) -> Abs::State {
    Abs::State { groups: a.groups.insert(r, a.groups[r].with_req(a.req(r).with_waker(w))), ..a }
}

/// A's state after `detach` of request lease `r`.
pub open spec fn detach_state(a: Abs::State, r: Id) -> Abs::State {
    Abs::State {
        groups: if a.req(r).phase is Complete {
            a.groups.remove(r)
        } else {
            a.groups.insert(r, a.groups[r].with_req(a.req(r).detach()))
        },
        ..a
    }
}

/// A's state after `drop` of program lease `i`.
pub open spec fn drop_state(a: Abs::State, i: Id) -> Abs::State {
    Abs::State { groups: a.groups.remove(i), ..a }
}

/// A's state after `take` of request lease `r`.
pub open spec fn take_state(a: Abs::State, r: Id) -> Abs::State {
    Abs::State {
        groups: a.groups.remove(r).insert(a.next_id, a.groups[r].with_holder(Holder::Program)),
        next_id: a.next_id + 1,
        ..a
    }
}

/// One step of A, from a state where `v` is placed, to a state where the
/// view `w` is placed: `w` agrees with `v` but at request `s`, which still
/// holds the same lease, or none.
proof fn lemma_step_one<L>(
    v: View<L>,
    w: View<L>,
    a: Abs::State,
    a2: Abs::State,
    step: Abs::Step,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    s: int,
)
    requires
        placed(v, a, q, ids, dirs),
        abs_step(a, a2, step),
        wf(w),
        holds(v, s),
        resources_match(w, a2, ids, dirs, s),
        forall|t: int| t != s ==> #[trigger] (w.req)(t) == (v.req)(t),
        forall|t: int| t != s ==> #[trigger] (w.staged)(t) == (v.staged)(t),
        forall|t: int| t != s ==> #[trigger] (w.refused)(t) == (v.refused)(t),
        // Leases other than `s`'s are untouched.
        forall|i: Id| i != ids[s] && #[trigger] a.groups.contains_key(i) ==> a2.groups.contains_key(i)
            && a2.groups[i] == a.groups[i],
        a2.qps == a.qps,
        (w.req)(s) is Some ==> a2.groups.contains_key(ids[s]) && a2.groups[ids[s]].holder == (Holder::Request {
            req: as_req((w.req)(s)->0, q, dirs.access[s]),
        }),
        (w.staged)(s) is Some ==> a2.is_program(ids[s]) && a2.groups[ids[s]].allows(dirs.access[s]),
        (w.refused)(s) is Some ==> a2.is_program(ids[s]),
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(w, a2, q, ids, dirs),
{
    safeverbs_spec::abs::lemma_abs_step_is_next(a, a2, step);
    safeverbs_spec::refinement::lemma_abs_reaches_next(a, a2);
    safeverbs_spec::refinement::lemma_abs_reaches_invariant(a, a2);
    lemma_placed_but(v, w, a, a2, q, ids, dirs, s);
}

/// What [`lemma_step_one`] and [`lemma_reshape`] share: `w` is placed in
/// `a2` when it agrees with `v`, placed in `a`, but at request `s`, and `a2`
/// keeps every lease of `a` but `s`'s and holds `s`'s as `w` says.
proof fn lemma_placed_but<L>(
    v: View<L>,
    w: View<L>,
    a: Abs::State,
    a2: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    s: int,
)
    requires
        placed(v, a, q, ids, dirs),
        abs_inv(a2),
        wf(w),
        holds(v, s),
        resources_match(w, a2, ids, dirs, s),
        forall|t: int| t != s ==> #[trigger] (w.req)(t) == (v.req)(t),
        forall|t: int| t != s ==> #[trigger] (w.staged)(t) == (v.staged)(t),
        forall|t: int| t != s ==> #[trigger] (w.refused)(t) == (v.refused)(t),
        forall|i: Id| i != ids[s] && #[trigger] a.groups.contains_key(i) ==> a2.groups.contains_key(i)
            && a2.groups[i] == a.groups[i],
        a2.qps == a.qps,
        (w.req)(s) is Some ==> a2.groups.contains_key(ids[s]) && a2.groups[ids[s]].holder == (Holder::Request {
            req: as_req((w.req)(s)->0, q, dirs.access[s]),
        }),
        (w.staged)(s) is Some ==> a2.is_program(ids[s]) && a2.groups[ids[s]].allows(dirs.access[s]),
        (w.refused)(s) is Some ==> a2.is_program(ids[s]),
    ensures
        placed(w, a2, q, ids, dirs),
{
    assert forall|t: int| #[trigger] holds(w, t) implies resources_match(w, a2, ids, dirs, t) by {
        if t != s {
            assert(holds(v, t) && ids[t] != ids[s]);
            assert(resources_match(v, a, ids, dirs, t));
            assert(a.groups.contains_key(ids[t]) && a2.groups[ids[t]] == a.groups[ids[t]]);
        }
    }
    assert forall|t: int| #[trigger] holds(w, t) implies ids.contains_key(t) && dirs.contains_key(t) by {
        if t != s {
            assert(holds(v, t));
        }
    }
    assert forall|t: int| #[trigger] (w.req)(t) is Some implies a2.groups.contains_key(ids[t])
        && a2.groups[ids[t]].holder == (Holder::Request { req: as_req((w.req)(t)->0, q, dirs.access[t]) }) by {
        if t != s {
            assert(holds(v, t));
        }
    }
    assert forall|t: int| #[trigger] (w.staged)(t) is Some implies a2.is_program(ids[t]) && a2.groups[ids[t]].allows(dirs.access[t]) by {
        if t != s {
            assert(holds(v, t));
            assert(ids[t] != ids[s]);
            assert(a.groups.contains_key(ids[t]));
        }
    }
    assert forall|t: int| #[trigger] (w.refused)(t) is Some implies a2.is_program(ids[t]) by {
        if t != s {
            assert(holds(v, t));
            assert(ids[t] != ids[s]);
            assert(a.groups.contains_key(ids[t]));
        }
    }
    assert forall|t: int, u: int|
        #![trigger ids[t], ids[u]]
        holds(w, t) && holds(w, u) && t != u implies ids[t] != ids[u] by {
        assert(holds(v, t) || t == s);
        assert(holds(v, u) || u == s);
    }
}

/// No step of A: `w` agrees with `v` but at request `s`, which holds the
/// same program lease of A (staged or refused), or none.
proof fn lemma_reshape<L>(
    v: View<L>,
    w: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    s: int,
)
    requires
        placed(v, a, q, ids, dirs),
        wf(w),
        holds(v, s),
        resources_match(w, a, ids, dirs, s),
        forall|t: int| t != s ==> #[trigger] (w.req)(t) == (v.req)(t),
        forall|t: int| t != s ==> #[trigger] (w.staged)(t) == (v.staged)(t),
        forall|t: int| t != s ==> #[trigger] (w.refused)(t) == (v.refused)(t),
        (w.req)(s) is None,
        (w.staged)(s) is Some ==> a.is_program(ids[s]) && a.groups[ids[s]].allows(dirs.access[s]),
        (w.refused)(s) is Some ==> a.is_program(ids[s]),
    ensures
        placed(w, a, q, ids, dirs),
{
    lemma_placed_but(v, w, a, a, q, ids, dirs, s);
}

/// Posting staged request `s` is A's `post_request` of its lease, then
/// `register` of its waker or `detach` if it has no owner. Its lease gets a
/// new id.
pub proof fn lemma_post_one<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    s: int,
) -> (r: (Abs::State, Map<int, Id>))
    requires
        placed(v, a, q, ids, dirs),
        (v.staged)(s) is Some,
    ensures
        safeverbs_spec::refinement::abs_reaches(a, r.0),
        placed(posted(v, s), r.0, q, r.1, dirs),
        r.0.wakes == a.wakes,
        forall|t: int| t != s ==> #[trigger] r.1[t] == ids[t],
        forall|t: int| #[trigger] ids.contains_key(t) ==> r.1.contains_key(t),
{
    let st = (v.staged)(s)->0;
    let i = ids[s];
    let dir = dirs.access[s];
    let id = a.next_id;
    assert(holds(v, s));
    // post_request
    let a1 = post_state(a, i, q, dir);
    assert(abs_step(a, a1, Abs::Step::post_request(i, q, dir)));
    safeverbs_spec::abs::lemma_abs_step_is_next(a, a1, Abs::Step::post_request(i, q, dir));
    safeverbs_spec::refinement::lemma_abs_reaches_next(a, a1);
    // register or detach
    let a2 = if st.waker is Some {
        register_state(a1, id, st.waker->0)
    } else if st.detached {
        detach_state(a1, id)
    } else {
        a1
    };
    if st.waker is Some {
        assert(abs_step(a1, a2, Abs::Step::register(id, st.waker->0)));
        safeverbs_spec::abs::lemma_abs_step_is_next(a1, a2, Abs::Step::register(id, st.waker->0));
        safeverbs_spec::refinement::lemma_abs_reaches_next(a1, a2);
        safeverbs_spec::refinement::lemma_abs_reaches_trans(a, a1, a2);
    } else if st.detached {
        assert(abs_step(a1, a2, Abs::Step::detach(id)));
        safeverbs_spec::abs::lemma_abs_step_is_next(a1, a2, Abs::Step::detach(id));
        safeverbs_spec::refinement::lemma_abs_reaches_next(a1, a2);
        safeverbs_spec::refinement::lemma_abs_reaches_trans(a, a1, a2);
    }
    safeverbs_spec::refinement::lemma_abs_reaches_invariant(a, a2);
    let ids2 = ids.insert(s, id);
    let w = posted(v, s);
    assert forall|t: int| #[trigger] holds(w, t) implies resources_match(w, a2, ids2, dirs, t) by {
        assert(holds(v, t));
        assert(resources_match(v, a, ids, dirs, t));
        if t != s { assert(ids[t] != ids[s] && ids[t] < id); }
    }
    // Every lease the ring holds is older than the fresh id.
    assert forall|t: int| #[trigger] holds(v, t) implies ids[t] < id by {
        assert(a.groups.contains_key(ids[t]));
    }
    assert forall|t: int| #[trigger] (w.req)(t) is Some implies req_wf((w.req)(t)->0) by {}
    assert forall|t: int| #[trigger] holds(w, t) implies ids2.contains_key(t) && dirs.contains_key(t) by {
        assert(holds(v, t));
    }
    assert forall|t: int| #[trigger] (w.req)(t) is Some implies a2.groups.contains_key(ids2[t])
        && a2.groups[ids2[t]].holder == (Holder::Request { req: as_req((w.req)(t)->0, q, dirs.access[t]) }) by {
        assert(holds(v, t));
    }
    assert forall|t: int| #[trigger] (w.staged)(t) is Some implies a2.is_program(ids2[t]) && a2.groups[ids2[t]].allows(dirs.access[t]) by {
        assert(holds(v, t));
    }
    assert forall|t: int| #[trigger] (w.refused)(t) is Some implies a2.is_program(ids2[t]) by {
        assert(holds(v, t));
    }
    assert forall|t: int, u: int|
        #![trigger ids2[t], ids2[u]]
        holds(w, t) && holds(w, u) && t != u implies ids2[t] != ids2[u] by {
        assert(holds(v, t) && holds(v, u));
    }
    (a2, ids2)
}

/// Staged request `s` of `v` posted.
pub open spec fn posted<L>(v: View<L>, s: int) -> View<L> {
    View {
        req: |t: int|
            if t == s {
                (v.staged)(s)
            } else {
                (v.req)(t)
            },
        staged: |t: int|
            if t == s {
                None
            } else {
                (v.staged)(t)
            },
        refused: v.refused,
    }
}

/// Staged requests `[from, to)` of `v` posted.
pub open spec fn posted_range<L>(v: View<L>, from: int, to: int) -> View<L> {
    View {
        req: |t: int|
            if from <= t < to {
                (v.staged)(t)
            } else {
                (v.req)(t)
            },
        staged: |t: int|
            if from <= t < to {
                None
            } else {
                (v.staged)(t)
            },
        refused: v.refused,
    }
}

/// Posting staged requests `[from, to)` is A's `post_request` of each (with
/// its `register` or `detach`).
pub proof fn lemma_commit<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    from: int,
    to: int,
) -> (r: (Abs::State, Map<int, Id>))
    requires
        placed(v, a, q, ids, dirs),
        from <= to,
        forall|s: int| from <= s < to ==> #[trigger] (v.staged)(s) is Some,
    ensures
        safeverbs_spec::refinement::abs_reaches(a, r.0),
        placed(posted_range(v, from, to), r.0, q, r.1, dirs),
        r.0.wakes == a.wakes,
    decreases to - from,
{
    if to == from {
        safeverbs_spec::refinement::lemma_abs_reaches_refl(a);
        lemma_same(v, posted_range(v, from, to), a, q, ids, dirs);
        (a, ids)
    } else {
        let s = to - 1;
        let (a1, ids1) = lemma_commit(v, a, q, ids, dirs, from, s);
        let v1 = posted_range(v, from, s);
        assert((v1.staged)(s) == (v.staged)(s));
        let (a2, ids2) = lemma_post_one(v1, a1, q, ids1, dirs, s);
        lemma_same(posted(v1, s), posted_range(v, from, to), a2, q, ids2, dirs);
        safeverbs_spec::refinement::lemma_abs_reaches_trans(a, a1, a2);
        (a2, ids2)
    }
}

/// Staged requests `[from, to)` of `v` refused by the device: `keep`'s goes
/// back to the program, a detached one's leases are released, the others'
/// leases are kept for their owners.
pub open spec fn refused_range<L>(v: View<L>, from: int, to: int, keep: int) -> View<L> {
    View {
        req: v.req,
        staged: |t: int|
            if from <= t < to {
                None
            } else {
                (v.staged)(t)
            },
        refused: |t: int|
            if from <= t < to && t != keep && !(v.staged)(t)->0.detached {
                Some((v.staged)(t)->0.leases)
            } else {
                (v.refused)(t)
            },
    }
}

/// Refusing staged requests `[from, to)` is A's `drop` of each detached
/// one's lease; the others' leases stay the program's: `keep`'s goes back to
/// it, the rest wait in the ring for their owners.
pub proof fn lemma_abort<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    from: int,
    to: int,
    keep: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, ids, dirs),
        from <= to,
        forall|s: int| from <= s < to ==> #[trigger] (v.staged)(s) is Some,
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(refused_range(v, from, to, keep), a2, q, ids, dirs),
        a2.wakes == a.wakes,
        (v.staged)(keep) is Some ==> a2.is_program(ids[keep]) && a2.groups[ids[keep]]
            == a.groups[ids[keep]],
    decreases to - from,
{
    if to == from {
        safeverbs_spec::refinement::lemma_abs_reaches_refl(a);
        lemma_same(v, refused_range(v, from, to, keep), a, q, ids, dirs);
        a
    } else {
        let s = to - 1;
        let a1 = lemma_abort(v, a, q, ids, dirs, from, s, keep);
        let v1 = refused_range(v, from, s, keep);
        let w = refused_range(v, from, to, keep);
        assert((v1.staged)(s) == (v.staged)(s));
        assert((v1.refused)(s) == (v.refused)(s));
        assert(holds(v1, s));
        assert(a1.is_program(ids[s]));
        if (v.staged)(keep) is Some && keep != s {
            assert(holds(v, keep) && holds(v, s));
            assert(ids[keep] != ids[s]);
        }
        if s == keep || !(v.staged)(s)->0.detached {
            // The program keeps the lease (back in its hands, or held in the
            // ring for the owner): no step.
            safeverbs_spec::refinement::lemma_abs_reaches_refl(a1);
            lemma_reshape(v1, w, a1, q, ids, dirs, s);
            a1
        } else {
            let a2 = drop_state(a1, ids[s]);
            assert(abs_step(a1, a2, Abs::Step::drop(ids[s])));
            lemma_step_one(v1, w, a1, a2, Abs::Step::drop(ids[s]), q, ids, dirs, s);
            safeverbs_spec::refinement::lemma_abs_reaches_trans(a, a1, a2);
            a2
        }
    }
}

/// Staged requests `[from, to)` of `v` dropped: every lease released.
pub open spec fn dropped_range<L>(v: View<L>, from: int, to: int) -> View<L> {
    View {
        req: v.req,
        staged: |t: int|
            if from <= t < to {
                None
            } else {
                (v.staged)(t)
            },
        refused: v.refused,
    }
}

/// Dropping staged requests `[from, to)` is A's `drop` of each lease.
pub proof fn lemma_drop_staged<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    from: int,
    to: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, ids, dirs),
        from <= to,
        forall|s: int| from <= s < to ==> #[trigger] (v.staged)(s) is Some,
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(dropped_range(v, from, to), a2, q, ids, dirs),
    decreases to - from,
{
    if to == from {
        safeverbs_spec::refinement::lemma_abs_reaches_refl(a);
        lemma_same(v, dropped_range(v, from, to), a, q, ids, dirs);
        a
    } else {
        let s = to - 1;
        let a1 = lemma_drop_staged(v, a, q, ids, dirs, from, s);
        let v1 = dropped_range(v, from, s);
        let w = dropped_range(v, from, to);
        assert((v1.staged)(s) == (v.staged)(s));
        assert(holds(v1, s));
        let a2 = drop_state(a1, ids[s]);
        assert(abs_step(a1, a2, Abs::Step::drop(ids[s])));
        lemma_step_one(v1, w, a1, a2, Abs::Step::drop(ids[s]), q, ids, dirs, s);
        safeverbs_spec::refinement::lemma_abs_reaches_trans(a, a1, a2);
        a2
    }
}

/// `v` with request `s` gone.
pub open spec fn without<L>(v: View<L>, s: int) -> View<L> {
    View {
        req: |t: int|
            if t == s {
                None
            } else {
                (v.req)(t)
            },
        staged: v.staged,
        refused: v.refused,
    }
}

/// `v` with refused request `s` gone.
pub open spec fn without_refused<L>(v: View<L>, s: int) -> View<L> {
    View {
        req: v.req,
        staged: v.staged,
        refused: |t: int|
            if t == s {
                None
            } else {
                (v.refused)(t)
            },
    }
}

/// Taking refused request `s` is no step of A: its lease was the
/// program's all along.
pub proof fn lemma_take_refused<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    s: int,
)
    requires
        placed(v, a, q, ids, dirs),
        (v.refused)(s) is Some,
    ensures
        placed(without_refused(v, s), a, q, ids, dirs),
        a.is_program(ids[s]),
{
    assert(holds(v, s));
    lemma_reshape(v, without_refused(v, s), a, q, ids, dirs, s);
}

/// Dropping refused request `s` is A's `drop` of its lease.
pub proof fn lemma_drop_refused<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    s: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, ids, dirs),
        (v.refused)(s) is Some,
    ensures
        abs_step(a, a2, Abs::Step::drop(ids[s])),
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(without_refused(v, s), a2, q, ids, dirs),
        a2.wakes == a.wakes,
{
    assert(holds(v, s));
    let a2 = drop_state(a, ids[s]);
    assert(abs_step(a, a2, Abs::Step::drop(ids[s])));
    lemma_step_one(v, without_refused(v, s), a, a2, Abs::Step::drop(ids[s]), q, ids, dirs, s);
    a2
}

/// Taking complete request `s`, with outcome `o` and leases `l`, is A's
/// `take`: the program holds the same bytes again, under a new id.
pub proof fn lemma_take<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    s: int,
    o: Wc,
    l: L,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, ids, dirs),
        (v.req)(s) == Some(ReqView { phase: Phase::Complete(o.outcome()), waker: None, detached: false, leases: l }),
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        abs_step(a, a2, Abs::Step::take(ids[s], o.outcome())),
        placed(without(v, s), a2, q, ids, dirs),
        a2.wakes == a.wakes,
        a2.is_program(a.next_id) && a2.groups[a.next_id].members == a.groups[ids[s]].members,
        a2.groups[a.next_id].members == (dirs.resources)(s, l),
{
    let r = ids[s];
    let a2 = take_state(a, r);
    assert(holds(v, s));
    assert(abs_step(a, a2, Abs::Step::take(r, o.outcome())));
    assert forall|i: Id| i != r && #[trigger] a.groups.contains_key(i) implies a2.groups.contains_key(i)
        && a2.groups[i] == a.groups[i] by {
        assert(i < a.next_id);
    }
    lemma_step_one(v, without(v, s), a, a2, Abs::Step::take(r, o.outcome()), q, ids, dirs, s);
    a2
}

/// `v` with the owner of request `s` gone, as the ring's `detach` says.
pub open spec fn detached<L>(v: View<L>, s: int) -> View<L> {
    View {
        req: |t: int|
            if t == s && (v.req)(s) is Some {
                if (v.req)(s)->0.phase is Complete {
                    None
                } else {
                    Some((v.req)(s)->0.detach())
                }
            } else {
                (v.req)(t)
            },
        staged: |t: int|
            if t == s && (v.staged)(s) is Some {
                Some((v.staged)(s)->0.detach())
            } else {
                (v.staged)(t)
            },
        refused: |t: int|
            if t == s {
                None
            } else {
                (v.refused)(t)
            },
    }
}

/// Detaching request `s` is A's `detach` if A sees it with an owner, A's
/// `drop` if it was refused, else no step (a staged request's leases are
/// still the program's).
pub proof fn lemma_detach<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    s: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, ids, dirs),
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(detached(v, s), a2, q, ids, dirs),
        a2.wakes == a.wakes,
{
    let w = detached(v, s);
    if (v.req)(s) is Some && !(v.req)(s)->0.detached {
        let r = ids[s];
        let a2 = detach_state(a, r);
        assert(holds(v, s));
        assert(abs_step(a, a2, Abs::Step::detach(r)));
        lemma_step_one(v, w, a, a2, Abs::Step::detach(r), q, ids, dirs, s);
        a2
    } else if (v.refused)(s) is Some {
        let a2 = lemma_drop_refused(v, a, q, ids, dirs, s);
        lemma_same(without_refused(v, s), w, a2, q, ids, dirs);
        a2
    } else if holds(v, s) {
        if (v.req)(s) is Some {
            assert((v.req)(s)->0.detach() == (v.req)(s)->0);
        }
        safeverbs_spec::refinement::lemma_abs_reaches_refl(a);
        assert forall|t: int| #[trigger] (w.req)(t) is Some implies req_wf((w.req)(t)->0) by {}
        assert forall|t: int| #[trigger] (w.staged)(t) is Some implies staged_wf((w.staged)(t)->0) by {}
        assert forall|t: int| #[trigger] holds(w, t) implies resources_match(w, a, ids, dirs, t) by {
            assert(holds(v, t));
            assert(resources_match(v, a, ids, dirs, t));
        }
        assert forall|t: int| #[trigger] holds(w, t) implies ids.contains_key(t) && dirs.contains_key(t) by {
            assert(holds(v, t));
        }
        assert forall|t: int| #[trigger] (w.req)(t) is Some implies a.groups.contains_key(ids[t])
            && a.groups[ids[t]].holder == (Holder::Request { req: as_req((w.req)(t)->0, q, dirs.access[t]) }) by {
            assert(holds(v, t));
        }
        assert forall|t: int| #[trigger] (w.staged)(t) is Some implies a.is_program(ids[t]) && a.groups[ids[t]].allows(dirs.access[t]) by {
            assert(holds(v, t));
        }
        assert forall|t: int| #[trigger] (w.refused)(t) is Some implies a.is_program(ids[t]) by {
            assert(holds(v, t));
        }
        assert forall|t: int, u: int|
            #![trigger ids[t], ids[u]]
            holds(w, t) && holds(w, u) && t != u implies ids[t] != ids[u] by {
            assert(holds(v, t) && holds(v, u));
        }
        a
    } else {
        safeverbs_spec::refinement::lemma_abs_reaches_refl(a);
        lemma_same(v, w, a, q, ids, dirs);
        a
    }
}

/// `v` with waker `wk` registered on request `s`, as the ring's
/// `set_waker` says.
pub open spec fn registered<L>(v: View<L>, s: int, wk: WakerId) -> View<L> {
    View {
        req: |t: int|
            if t == s && (v.req)(s) is Some && (v.req)(s)->0.phase is InFlight && !(v.req)(
                s,
            )->0.detached {
                Some((v.req)(s)->0.with_waker(wk))
            } else {
                (v.req)(t)
            },
        staged: |t: int|
            if t == s && (v.staged)(s) is Some && !(v.staged)(s)->0.detached {
                Some((v.staged)(s)->0.with_waker(wk))
            } else {
                (v.staged)(t)
            },
        refused: v.refused,
    }
}

/// Registering a waker on request `s` is A's `register` if A sees it in
/// flight with an owner, else no step.
pub proof fn lemma_register<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    s: int,
    wk: WakerId,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, ids, dirs),
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(registered(v, s, wk), a2, q, ids, dirs),
        a2.wakes == a.wakes,
{
    let w = registered(v, s, wk);
    if (v.req)(s) is Some && (v.req)(s)->0.phase is InFlight && !(v.req)(s)->0.detached {
        let r = ids[s];
        let a2 = register_state(a, r, wk);
        assert(holds(v, s));
        assert(abs_step(a, a2, Abs::Step::register(r, wk)));
        lemma_step_one(v, w, a, a2, Abs::Step::register(r, wk), q, ids, dirs, s);
        a2
    } else {
        safeverbs_spec::refinement::lemma_abs_reaches_refl(a);
        assert forall|t: int| #[trigger] (w.req)(t) is Some implies req_wf((w.req)(t)->0) by {}
        assert forall|t: int| #[trigger] (w.staged)(t) is Some implies staged_wf((w.staged)(t)->0) by {}
        assert forall|t: int| #[trigger] holds(w, t) implies resources_match(w, a, ids, dirs, t) by {
            assert(holds(v, t));
            assert(resources_match(v, a, ids, dirs, t));
        }
        assert forall|t: int| #[trigger] holds(w, t) implies ids.contains_key(t) && dirs.contains_key(t) by {
            assert(holds(v, t));
        }
        assert forall|t: int| #[trigger] (w.req)(t) is Some implies a.groups.contains_key(ids[t])
            && a.groups[ids[t]].holder == (Holder::Request { req: as_req((w.req)(t)->0, q, dirs.access[t]) }) by {
            assert(holds(v, t));
        }
        assert forall|t: int| #[trigger] (w.staged)(t) is Some implies a.is_program(ids[t]) && a.groups[ids[t]].allows(dirs.access[t]) by {
            assert(holds(v, t));
        }
        assert forall|t: int| #[trigger] (w.refused)(t) is Some implies a.is_program(ids[t]) by {
            assert(holds(v, t));
        }
        assert forall|t: int, u: int|
            #![trigger ids[t], ids[u]]
            holds(w, t) && holds(w, u) && t != u implies ids[t] != ids[u] by {
            assert(holds(v, t) && holds(v, u));
        }
        a
    }
}

/// `v` with request `s` staged, holding `l`.
pub open spec fn staged_with<L>(v: View<L>, s: int, detached: bool, l: L) -> View<L> {
    View {
        req: v.req,
        staged: |t: int|
            if t == s {
                Some(ReqView::new(detached, l))
            } else {
                (v.staged)(t)
            },
        refused: v.refused,
    }
}

/// Staging a request is no step of A: its leases, program lease `i` of A,
/// stay the program's until it is posted. They may be posted in `dir`.
pub proof fn lemma_end<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    s: int,
    i: Id,
    dir: Seq<Dir>,
    detached: bool,
    l: L,
)
    requires
        placed(v, a, q, ids, dirs),
        !holds(v, s),
        a.is_program(i),
        a.groups[i].members == (dirs.resources)(s, l),
        a.groups[i].allows(dir),
        forall|t: int| #[trigger] holds(v, t) ==> ids[t] != i,
    ensures
        placed(staged_with(v, s, detached, l), a, q, ids.insert(s, i), dirs.insert(s, dir)),
{
    let w = staged_with(v, s, detached, l);
    let ids2 = ids.insert(s, i);
    let dirs2 = dirs.insert(s, dir);
    assert forall|t: int| #[trigger] holds(w, t) implies resources_match(w, a, ids2, dirs2, t) by {
        if t != s {
            assert(holds(v, t));
            assert(resources_match(v, a, ids, dirs, t));
        }
    }
    assert forall|t: int| #[trigger] (w.staged)(t) is Some implies staged_wf((w.staged)(t)->0) by {}
    assert forall|t: int| #[trigger] holds(w, t) implies ids2.contains_key(t) && dirs2.contains_key(t) by {
        if t != s {
            assert(holds(v, t));
        }
    }
    assert forall|t: int| #[trigger] (w.req)(t) is Some implies a.groups.contains_key(ids2[t])
        && a.groups[ids2[t]].holder == (Holder::Request { req: as_req((w.req)(t)->0, q, dirs2.access[t]) }) by {
        assert(holds(v, t));
    }
    assert forall|t: int| #[trigger] (w.staged)(t) is Some implies a.is_program(ids2[t]) && a.groups[ids2[t]].allows(dirs2.access[t]) by {
        if t != s {
            assert(holds(v, t));
        }
    }
    assert forall|t: int| #[trigger] (w.refused)(t) is Some implies a.is_program(ids2[t]) by {
        assert(holds(v, t));
    }
    assert forall|t: int, u: int|
        #![trigger ids2[t], ids2[u]]
        holds(w, t) && holds(w, u) && t != u implies ids2[t] != ids2[u] by {
        if t != s {
            assert(holds(v, t));
        }
        if u != s {
            assert(holds(v, u));
        }
    }
}

/// Releasing request `s` when its queue pair is destroyed is A's
/// `complete` (with a flush, if it is in flight) and then its `detach` (if
/// it has an owner).
pub proof fn lemma_release_one<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    s: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, ids, dirs),
        (v.req)(s) is Some,
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(without(v, s), a2, q, ids, dirs),
{
    let rv = (v.req)(s)->0;
    let r = ids[s];
    assert(holds(v, s));
    let a1 = if rv.phase is InFlight {
        complete_state(a, r, flushed())
    } else {
        a
    };
    let rv1 = if rv.phase is InFlight {
        ReqView { phase: Phase::Complete(flushed()), waker: None, ..rv }
    } else {
        rv
    };
    let v1 = View {
        req: |t: int|
            if t == s {
                if rv.detached {
                    None
                } else {
                    Some(rv1)
                }
            } else {
                (v.req)(t)
            },
        staged: v.staged,
        refused: v.refused,
    };
    if rv.phase is InFlight {
        assert(abs_step(a, a1, Abs::Step::complete(r, flushed())));
        lemma_step_one(v, v1, a, a1, Abs::Step::complete(r, flushed()), q, ids, dirs, s);
    } else {
        safeverbs_spec::refinement::lemma_abs_reaches_refl(a);
        lemma_same(v, v1, a, q, ids, dirs);
    }
    if rv.detached {
        lemma_same(v1, without(v, s), a1, q, ids, dirs);
        a1
    } else {
        let a2 = detach_state(a1, r);
        assert(holds(v1, s));
        assert(abs_step(a1, a2, Abs::Step::detach(r)));
        lemma_step_one(v1, without(v, s), a1, a2, Abs::Step::detach(r), q, ids, dirs, s);
        safeverbs_spec::refinement::lemma_abs_reaches_trans(a, a1, a2);
        a2
    }
}

/// `v` with requests `[from, to)` gone, refused ones too.
pub open spec fn without_range<L>(v: View<L>, from: int, to: int) -> View<L> {
    View {
        req: |t: int|
            if from <= t < to {
                None
            } else {
                (v.req)(t)
            },
        staged: v.staged,
        refused: |t: int|
            if from <= t < to {
                None
            } else {
                (v.refused)(t)
            },
    }
}

/// Releasing requests `[from, to)` is A's `complete` and `detach` of each,
/// and `drop` of each refused one.
pub proof fn lemma_release<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    from: int,
    to: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, ids, dirs),
        from <= to,
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(without_range(v, from, to), a2, q, ids, dirs),
    decreases to - from,
{
    if to == from {
        safeverbs_spec::refinement::lemma_abs_reaches_refl(a);
        lemma_same(v, without_range(v, from, to), a, q, ids, dirs);
        a
    } else {
        let s = to - 1;
        let a1 = lemma_release(v, a, q, ids, dirs, from, s);
        let v1 = without_range(v, from, s);
        assert((v1.req)(s) == (v.req)(s));
        assert((v1.refused)(s) == (v.refused)(s));
        if (v.req)(s) is Some {
            let a2 = lemma_release_one(v1, a1, q, ids, dirs, s);
            lemma_same(without(v1, s), without_range(v, from, to), a2, q, ids, dirs);
            safeverbs_spec::refinement::lemma_abs_reaches_trans(a, a1, a2);
            a2
        } else if (v.refused)(s) is Some {
            let a2 = lemma_drop_refused(v1, a1, q, ids, dirs, s);
            lemma_same(without_refused(v1, s), without_range(v, from, to), a2, q, ids, dirs);
            safeverbs_spec::refinement::lemma_abs_reaches_trans(a, a1, a2);
            a2
        } else {
            lemma_same(v1, without_range(v, from, to), a1, q, ids, dirs);
            a1
        }
    }
}

// =============================================================================
// The ring's operations
//
// Each lemma takes the state of the ring before and after an operation,
// related by the operation's postcondition, and a state of A in which the
// ring's requests sit before; it gives the state of A after the steps the
// table at the top names, in which they sit after.

/// `begin`, `push_wqe` and `take_detached_error`: no step.
pub proof fn lemma_ring_stutter<L>(
    pre: Ring<L>,
    post: Ring<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
)
    requires
        placed(pre.view(), a, q, ids, dirs),
        post.same_view(&pre),
    ensures
        placed(post.view(), a, q, ids, dirs),
{
    post.lemma_same_view(&pre);
    lemma_same(pre.view(), post.view(), a, q, ids, dirs);
}

/// `end`: no step; the staged request holds program lease `i`, which may
/// be posted in `dir`.
pub proof fn lemma_ring_end<L>(
    pre: Ring<L>,
    post: Ring<L>,
    detached: bool,
    leases: L,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
    i: Id,
    dir: Seq<Dir>,
)
    requires
        pre.inv(),
        placed(pre.view(), a, q, ids, dirs),
        a.is_program(i),
        a.groups[i].members == (dirs.resources)(pre.spec_posted() as int, leases),
        a.groups[i].allows(dir),
        forall|t: int| #[trigger] holds(pre.view(), t) ==> ids[t] != i,
        forall|s: int| #[trigger] post.req(s) == pre.req(s),
        forall|s: int|
            #[trigger] post.staged(s) == if s == pre.spec_posted() {
                Some(ReqView::new(detached, leases))
            } else {
                pre.staged(s)
            },
        forall|s: int| #[trigger] post.refused(s) == pre.refused(s),
    ensures
        placed(
            post.view(),
            a,
            q,
            ids.insert(pre.spec_posted() as int, i),
            dirs.insert(pre.spec_posted() as int, dir),
        ),
{
    let p = pre.spec_posted() as int;
    pre.lemma_view();
    lemma_end(pre.view(), a, q, ids, dirs, p, i, dir, detached, leases);
    lemma_same(
        staged_with(pre.view(), p, detached, leases),
        post.view(),
        a,
        q,
        ids.insert(p, i),
        dirs.insert(p, dir),
    );
}

/// `commit`: `post_request` of every staged request (with its `register`
/// or `detach`). Their leases get new ids.
pub proof fn lemma_ring_commit<L>(
    pre: Ring<L>,
    post: Ring<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
) -> (r: (Abs::State, Map<int, Id>))
    requires
        pre.inv(),
        placed(pre.view(), a, q, ids, dirs),
        forall|s: int|
            #[trigger] post.req(s) == if pre.staged(s) is Some {
                pre.staged(s)
            } else {
                pre.req(s)
            },
        forall|s: int| #[trigger] post.staged(s) is None,
        forall|s: int| #[trigger] post.refused(s) == pre.refused(s),
    ensures
        safeverbs_spec::refinement::abs_reaches(a, r.0),
        placed(post.view(), r.0, q, r.1, dirs),
        r.0.wakes == a.wakes,
{
    pre.lemma_view();
    let (c, p) = (pre.spec_committed() as int, pre.spec_posted() as int);
    let (a2, ids2) = lemma_commit(pre.view(), a, q, ids, dirs, c, p);
    lemma_same(posted_range(pre.view(), c, p), post.view(), a2, q, ids2, dirs);
    (a2, ids2)
}

/// `abort_keeping(keep)` (and `abort`): `drop` of every detached staged
/// lease; `keep`'s is the program's again, unchanged, and the other staged
/// requests' leases, still the program's, wait in the ring for their owners.
pub proof fn lemma_ring_abort<L>(
    pre: Ring<L>,
    post: Ring<L>,
    keep: int,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
) -> (a2: Abs::State)
    requires
        pre.inv(),
        placed(pre.view(), a, q, ids, dirs),
        forall|s: int| #[trigger] post.req(s) == pre.req(s),
        forall|s: int| #[trigger] post.staged(s) is None,
        forall|s: int|
            #[trigger] post.refused(s) == if pre.staged(s) is Some && s != keep && !pre.staged(
                s,
            )->0.detached {
                Some(pre.staged(s)->0.leases)
            } else {
                pre.refused(s)
            },
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(post.view(), a2, q, ids, dirs),
        a2.wakes == a.wakes,
        pre.staged(keep) is Some ==> a2.is_program(ids[keep]) && a2.groups[ids[keep]]
            == a.groups[ids[keep]],
{
    pre.lemma_view();
    let (c, p) = (pre.spec_committed() as int, pre.spec_posted() as int);
    let a2 = lemma_abort(pre.view(), a, q, ids, dirs, c, p, keep);
    lemma_same(refused_range(pre.view(), c, p, keep), post.view(), a2, q, ids, dirs);
    a2
}

/// `complete`: A's `complete` of each request retired, in order, with the
/// outcome the ring gives it (refused ones A never saw); A's wakes grow by
/// exactly the wakers the ring woke (L1 for the ring).
pub proof fn lemma_ring_complete<L>(
    pre: Ring<L>,
    post: Ring<L>,
    wqe: u64,
    wc: Wc,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
) -> (a2: Abs::State)
    requires
        pre.inv(),
        placed(pre.view(), a, q, ids, dirs),
        pre.spec_retired() <= post.spec_retired() <= pre.spec_committed(),
        forall|s: int|
            #[trigger] post.req(s) == if pre.spec_retired() <= s < post.spec_retired() {
                pre.retired_view(s, pre.retire_outcome(s, wqe, wc))
            } else {
                pre.req(s)
            },
        forall|s: int| #[trigger] post.staged(s) == pre.staged(s),
        forall|s: int| #[trigger] post.refused(s) == pre.refused(s),
        post.woken() == pre.woken() + pre.wakers(
            pre.spec_retired() as int,
            post.spec_retired() as int,
        ),
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(post.view(), a2, q, ids, dirs),
        ({
            let woke = wakers(pre.view(), pre.spec_retired() as int, post.spec_retired() as int);
            &&& post.woken() == pre.woken() + woke
            &&& a2.wakes == a.wakes + woke
        }),
{
    pre.lemma_view();
    let (r0, r1) = (pre.spec_retired() as int, post.spec_retired() as int);
    let o = |s: int| pre.retire_outcome(s, wqe, wc);
    let a2 = lemma_complete(pre.view(), a, q, ids, dirs, r0, r1, o);
    assert forall|s: int| #[trigger] (retired_range(pre.view(), r0, r1, o).req)(s) == (post.view().req)(
        s,
    ) by {
        if r0 <= s < r1 {
            pre.lemma_retired_view(s, o(s));
        }
    }
    lemma_same(retired_range(pre.view(), r0, r1, o), post.view(), a2, q, ids, dirs);
    pre.lemma_wakers(r0, r1);
    a2
}

/// `take(seq)` returning `Some((outcome, leases))` for a request A sees:
/// A's `take`; the program holds the request's bytes again.
pub proof fn lemma_ring_take<L>(
    pre: Ring<L>,
    post: Ring<L>,
    seq: int,
    outcome: Wc,
    leases: L,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
) -> (a2: Abs::State)
    requires
        placed(pre.view(), a, q, ids, dirs),
        pre.req(seq) == Some(
            ReqView { phase: Phase::Complete(outcome.outcome()), waker: None, detached: false, leases },
        ),
        forall|s: int|
            #[trigger] post.req(s) == if s == seq {
                None
            } else {
                pre.req(s)
            },
        forall|s: int| #[trigger] post.staged(s) == pre.staged(s),
        forall|s: int|
            #[trigger] post.refused(s) == if s == seq {
                None
            } else {
                pre.refused(s)
            },
    ensures
        abs_step(a, a2, Abs::Step::take(ids[seq], outcome.outcome())),
        placed(post.view(), a2, q, ids, dirs),
        a2.wakes == a.wakes,
        a2.is_program(a.next_id) && a2.groups[a.next_id].members == a.groups[ids[seq]].members,
        a2.groups[a.next_id].members == (dirs.resources)(seq, leases),
{
    let a2 = lemma_take(pre.view(), a, q, ids, dirs, seq, outcome, leases);
    lemma_same(without(pre.view(), seq), post.view(), a2, q, ids, dirs);
    a2
}

/// `take(seq)` returning `Some((outcome, leases))` for a refused request:
/// no step; the program, which A says held the lease all along, has it in
/// its hands again.
pub proof fn lemma_ring_take_refused<L>(
    pre: Ring<L>,
    post: Ring<L>,
    seq: int,
    leases: L,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
)
    requires
        placed(pre.view(), a, q, ids, dirs),
        pre.refused(seq) == Some(leases),
        forall|s: int| #[trigger] post.req(s) == pre.req(s),
        forall|s: int| #[trigger] post.staged(s) == pre.staged(s),
        forall|s: int|
            #[trigger] post.refused(s) == if s == seq {
                None
            } else {
                pre.refused(s)
            },
    ensures
        placed(post.view(), a, q, ids, dirs),
        a.is_program(ids[seq]),
{
    lemma_take_refused(pre.view(), a, q, ids, dirs, seq);
    lemma_same(without_refused(pre.view(), seq), post.view(), a, q, ids, dirs);
}

/// `detach(seq)`: A's `detach`, `drop` for a refused request, or no step.
pub proof fn lemma_ring_detach<L>(
    pre: Ring<L>,
    post: Ring<L>,
    seq: int,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
) -> (a2: Abs::State)
    requires
        placed(pre.view(), a, q, ids, dirs),
        forall|s: int|
            #[trigger] post.req(s) == if s == seq && pre.req(s) is Some {
                if pre.req(s)->0.phase is Complete {
                    None
                } else {
                    Some(pre.req(s)->0.detach())
                }
            } else {
                pre.req(s)
            },
        forall|s: int|
            #[trigger] post.staged(s) == if s == seq && pre.staged(s) is Some {
                Some(pre.staged(s)->0.detach())
            } else {
                pre.staged(s)
            },
        forall|s: int|
            #[trigger] post.refused(s) == if s == seq {
                None
            } else {
                pre.refused(s)
            },
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(post.view(), a2, q, ids, dirs),
        a2.wakes == a.wakes,
{
    let a2 = lemma_detach(pre.view(), a, q, ids, dirs, seq);
    lemma_same(detached(pre.view(), seq), post.view(), a2, q, ids, dirs);
    a2
}

/// `set_waker(seq, waker)`: A's `register`, or no step.
pub proof fn lemma_ring_register<L>(
    pre: Ring<L>,
    post: Ring<L>,
    seq: int,
    wk: WakerId,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
) -> (a2: Abs::State)
    requires
        placed(pre.view(), a, q, ids, dirs),
        forall|s: int|
            #[trigger] post.req(s) == if s == seq && pre.req(s) is Some && pre.req(s)->0.phase is InFlight
                && !pre.req(s)->0.detached {
                Some(pre.req(s)->0.with_waker(wk))
            } else {
                pre.req(s)
            },
        forall|s: int|
            #[trigger] post.staged(s) == if s == seq && pre.staged(s) is Some && !pre.staged(
                s,
            )->0.detached {
                Some(pre.staged(s)->0.with_waker(wk))
            } else {
                pre.staged(s)
            },
        forall|s: int| #[trigger] post.refused(s) == pre.refused(s),
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(post.view(), a2, q, ids, dirs),
        a2.wakes == a.wakes,
{
    let a2 = lemma_register(pre.view(), a, q, ids, dirs, seq, wk);
    lemma_same(registered(pre.view(), seq, wk), post.view(), a2, q, ids, dirs);
    a2
}

/// `release_all`: A's `complete` (a flush) of every request in flight and
/// `detach` of every owned one, `drop` of every staged or refused lease.
/// Nothing of the ring's is left in A, so its queue pair may go
/// (`drop_qp`).
pub proof fn lemma_ring_release_all<L>(
    pre: Ring<L>,
    post: Ring<L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
) -> (a2: Abs::State)
    requires
        pre.inv(),
        placed(pre.view(), a, q, ids, dirs),
        forall|s: int| #[trigger] post.req(s) is None,
        forall|s: int| #[trigger] post.staged(s) is None,
        forall|s: int| #[trigger] post.refused(s) is None,
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(post.view(), a2, q, ids, dirs),
{
    pre.lemma_view();
    let (h, c, p) = (pre.spec_head() as int, pre.spec_committed() as int, pre.spec_posted() as int);
    let a1 = lemma_release(pre.view(), a, q, ids, dirs, h, c);
    let v1 = without_range(pre.view(), h, c);
    assert forall|s: int| c <= s < p implies #[trigger] (v1.staged)(s) is Some by {}
    let a2 = lemma_drop_staged(v1, a1, q, ids, dirs, c, p);
    lemma_same(dropped_range(v1, c, p), post.view(), a2, q, ids, dirs);
    safeverbs_spec::refinement::lemma_abs_reaches_trans(a, a1, a2);
    a2
}

// =============================================================================
// Results moved out of the ring
//
// `safeverbs` moves results nobody has taken yet out of a clogged ring into a
// side list, where their futures find them. A still sees each such result
// as a complete request with an owner (`side`), or, for a refused request,
// as a program lease (`refused`): moving it is no step, and taking it from
// the side list (or dropping its future) is A's `take` (or `detach`), with
// exactly the outcome and leases the ring gave, or no step (or `drop`) for a
// refused one.

/// `v` with the side list: by sequence number, the outcome and leases of
/// complete requests moved out of the ring (`side`), each a complete request
/// with an owner in A, and the leases of refused requests moved out
/// (`refused`), each a program lease of A.
pub open spec fn with_side<L>(v: View<L>, side: Map<int, (Wc, L)>, refused: Map<int, L>) -> View<L> {
    View {
        req: |s: int|
            if side.contains_key(s) {
                Some(
                    ReqView {
                        phase: Phase::Complete(side[s].0.outcome()),
                        waker: None,
                        detached: false,
                        leases: side[s].1,
                    },
                )
            } else {
                (v.req)(s)
            },
        staged: v.staged,
        refused: |s: int|
            if refused.contains_key(s) {
                Some(refused[s])
            } else {
                (v.refused)(s)
            },
    }
}

/// Moving a complete request's outcome and leases, as `take` returned them,
/// from the ring to the side list is no step of A.
pub proof fn lemma_side_move<L>(
    pre: Ring<L>,
    post: Ring<L>,
    seq: int,
    outcome: Wc,
    leases: L,
    side: Map<int, (Wc, L)>,
    refused: Map<int, L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
)
    requires
        placed(with_side(pre.view(), side, refused), a, q, ids, dirs),
        !side.contains_key(seq),
        !refused.contains_key(seq),
        pre.req(seq) == Some(
            ReqView { phase: Phase::Complete(outcome.outcome()), waker: None, detached: false, leases },
        ),
        forall|s: int|
            #[trigger] post.req(s) == if s == seq {
                None
            } else {
                pre.req(s)
            },
        forall|s: int| #[trigger] post.staged(s) == pre.staged(s),
        forall|s: int| #[trigger] post.refused(s) == pre.refused(s),
    ensures
        placed(with_side(post.view(), side.insert(seq, (outcome, leases)), refused), a, q, ids, dirs),
{
    lemma_same(
        with_side(pre.view(), side, refused),
        with_side(post.view(), side.insert(seq, (outcome, leases)), refused),
        a,
        q,
        ids,
        dirs,
    );
}

/// Moving a refused request's leases, as `take` returned them, from the
/// ring to the side list is no step of A.
pub proof fn lemma_side_move_refused<L>(
    pre: Ring<L>,
    post: Ring<L>,
    seq: int,
    leases: L,
    side: Map<int, (Wc, L)>,
    refused: Map<int, L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
)
    requires
        placed(with_side(pre.view(), side, refused), a, q, ids, dirs),
        !side.contains_key(seq),
        !refused.contains_key(seq),
        pre.refused(seq) == Some(leases),
        forall|s: int| #[trigger] post.req(s) == pre.req(s),
        forall|s: int| #[trigger] post.staged(s) == pre.staged(s),
        forall|s: int|
            #[trigger] post.refused(s) == if s == seq {
                None
            } else {
                pre.refused(s)
            },
    ensures
        placed(with_side(post.view(), side, refused.insert(seq, leases)), a, q, ids, dirs),
{
    lemma_same(
        with_side(pre.view(), side, refused),
        with_side(post.view(), side, refused.insert(seq, leases)),
        a,
        q,
        ids,
        dirs,
    );
}

/// Taking a result from the side list is A's `take`: the outcome and the
/// leases the ring gave, the program holding the same bytes again.
pub proof fn lemma_side_take<L>(
    v: View<L>,
    seq: int,
    side: Map<int, (Wc, L)>,
    refused: Map<int, L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
) -> (a2: Abs::State)
    requires
        placed(with_side(v, side, refused), a, q, ids, dirs),
        side.contains_key(seq),
        (v.req)(seq) is None,
    ensures
        abs_step(a, a2, Abs::Step::take(ids[seq], side[seq].0.outcome())),
        placed(with_side(v, side.remove(seq), refused), a2, q, ids, dirs),
        a2.is_program(a.next_id) && a2.groups[a.next_id].members == a.groups[ids[seq]].members,
        a2.groups[a.next_id].members == (dirs.resources)(seq, side[seq].1),
{
    let w = with_side(v, side, refused);
    let a2 = lemma_take(w, a, q, ids, dirs, seq, side[seq].0, side[seq].1);
    lemma_same(without(w, seq), with_side(v, side.remove(seq), refused), a2, q, ids, dirs);
    a2
}

/// Taking a refused request's leases from the side list is no step of A.
pub proof fn lemma_side_take_refused<L>(
    v: View<L>,
    seq: int,
    side: Map<int, (Wc, L)>,
    refused: Map<int, L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
)
    requires
        placed(with_side(v, side, refused), a, q, ids, dirs),
        refused.contains_key(seq),
        (v.refused)(seq) is None,
    ensures
        placed(with_side(v, side, refused.remove(seq)), a, q, ids, dirs),
        a.is_program(ids[seq]),
{
    let w = with_side(v, side, refused);
    lemma_take_refused(w, a, q, ids, dirs, seq);
    lemma_same(without_refused(w, seq), with_side(v, side, refused.remove(seq)), a, q, ids, dirs);
}

/// Dropping the future of a result in the side list is A's `detach`, which
/// releases its leases.
pub proof fn lemma_side_detach<L>(
    v: View<L>,
    seq: int,
    side: Map<int, (Wc, L)>,
    refused: Map<int, L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
) -> (a2: Abs::State)
    requires
        placed(with_side(v, side, refused), a, q, ids, dirs),
        side.contains_key(seq),
        (v.req)(seq) is None,
    ensures
        safeverbs_spec::refinement::abs_reaches(a, a2),
        placed(with_side(v, side.remove(seq), refused), a2, q, ids, dirs),
{
    let w = with_side(v, side, refused);
    let a2 = lemma_detach(w, a, q, ids, dirs, seq);
    lemma_same(detached(w, seq), with_side(v, side.remove(seq), refused), a2, q, ids, dirs);
    a2
}

/// Dropping the owner of a refused result in the side list is A's `drop`
/// of its lease.
pub proof fn lemma_side_detach_refused<L>(
    v: View<L>,
    seq: int,
    side: Map<int, (Wc, L)>,
    refused: Map<int, L>,
    a: Abs::State,
    q: QpId,
    ids: Map<int, Id>,
    dirs: Permissions<L>,
) -> (a2: Abs::State)
    requires
        placed(with_side(v, side, refused), a, q, ids, dirs),
        refused.contains_key(seq),
        (v.refused)(seq) is None,
    ensures
        abs_step(a, a2, Abs::Step::drop(ids[seq])),
        placed(with_side(v, side, refused.remove(seq)), a2, q, ids, dirs),
{
    let w = with_side(v, side, refused);
    let a2 = lemma_drop_refused(w, a, q, ids, dirs, seq);
    lemma_same(without_refused(w, seq), with_side(v, side, refused.remove(seq)), a2, q, ids, dirs);
    a2
}

/// A concrete mixed-resource view exercises the same adapters used by the ring
/// postconditions. Here `L` is the ordered resource sequence itself, so its interpretation
/// is identity. This is not a construction of the concrete buffer-token interpretation.
#[verifier::spinoff_prover]
#[verifier::rlimit(20)]
pub proof fn example_mixed_resources() {
    let src = safeverbs_spec::types::Span { start: 0, len: 64 }.bytes();
    let dst = safeverbs_spec::types::Span { start: 128, len: 64 }.bytes();
    let read = Group::singleton(0, dst, true, true);
    let send = Group::singleton(1, src, false, true);
    let clone = Group::singleton(2, src, false, true);
    let mixed = Group { members: read.members + send.members, holder: Holder::Program };
    let a0 = Abs::State {
        qps: Set::empty().insert(7), groups: Map::empty().insert(3, mixed).insert(2, clone),
        next_id: 4, outcomes: Map::empty(), posted: Map::empty(), wakes: Seq::empty(),
    };
    assert(abs_inv(a0));
    let v0 = View::<Seq<Lease>> { req: |s: int| None, staged: |s: int| None, refused: |s: int| None };
    let ids0 = Map::empty();
    let dirs0 = Permissions { access: Map::empty(), resources: |_s: int, members: Seq<Lease>| members };
    assert(placed(v0, a0, 7, ids0, dirs0));
    let access = seq![Dir::In, Dir::Out];
    lemma_end(v0, a0, 7, ids0, dirs0, 0, 3, access, false, mixed.members);
    let staged = staged_with(v0, 0, false, mixed.members);
    let ids = ids0.insert(0, 3);
    let dirs = dirs0.insert(0, access);
    let (a1, posted_ids) = lemma_post_one(staged, a0, 7, ids, dirs, 0);
    let posted = posted(staged, 0);
    assert((staged.staged)(0) is Some);
    assert((posted.req)(0) is Some);
    assert(holds(posted, 0));
    assert(posted_ids.contains_key(0));
    let a2 = lemma_register(posted, a1, 7, posted_ids, dirs, 0, 9);
    let waiting = registered(posted, 0, 9);
    assert((waiting.req)(0) is Some);
    assert((waiting.req)(0)->0.phase is InFlight);
    assert((waiting.req)(0)->0.waker == Some(9));
    assert(holds(waiting, 0));
    assert(resources_match(waiting, a2, posted_ids, dirs, 0));
    let r = posted_ids[0];
    assert(a2.in_flight(r) && a2.groups[r].members == mixed.members);
    assert(a2.req(r).waker == Some(9));
    assert(a2.nic_writes(128));
    let wc = Wc {
        wr_id: 1, status: crate::wc::Status::WR_FLUSH_ERR, opcode: crate::wc::Opcode::SEND,
        byte_len: 0, imm: None, vendor_err: 0, qp_num: 7,
    };
    lemma_complete_one(waiting, a2, 7, posted_ids, dirs, 0, wc);
    let done = complete_state(a2, r, wc.outcome());
    let completed = completed(waiting, 0, wc);
    let taken = lemma_take(completed, done, 7, posted_ids, dirs, 0, wc, mixed.members);
    assert(taken.groups[done.next_id].members == mixed.members);
    assert(taken.groups[done.next_id].members[0].unique);
    assert(!taken.groups[done.next_id].members[1].unique);
    assert(taken.wakes == seq![9nat]);

    let detached_state = lemma_detach(waiting, a2, 7, posted_ids, dirs, 0);
    let detached_view = detached(waiting, 0);
    assert((detached_view.req)(0) is Some);
    assert((detached_view.req)(0)->0.phase is InFlight);
    assert(holds(detached_view, 0));
    assert(resources_match(detached_view, detached_state, posted_ids, dirs, 0));
    assert(detached_state.in_flight(r) && detached_state.groups[r].members == mixed.members);
    lemma_complete_one(detached_view, detached_state, 7, posted_ids, dirs, 0, wc);
    let released = complete_state(detached_state, r, wc.outcome());
    assert(!released.groups.contains_key(r));
    assert(released.wakes.len() == 0);
}

/// Equal shared descriptors in different request positions have different A member IDs.
/// Their bytes overlap compatibly, and the interpretation keeps the request sequence
/// through posting, completion, side storage and take. `Span` is only an example resource
/// value; constructing the physical buffer-token interpretation remains a separate proof.
#[verifier::spinoff_prover]
#[verifier::rlimit(20)]
pub proof fn example_indexed_shared_resources() {
    let shared = safeverbs_spec::types::Span { start: 0, len: 64 };
    let first = Group::singleton(0, shared.bytes(), false, true);
    let second = Group::singleton(1, shared.bytes(), false, true);
    assert(first.compatible(second));
    assert(first.members[0].mem == second.members[0].mem);
    assert(first.members[0].mem.contains(0) && second.members[0].mem.contains(0));
    assert(first.members[0].id != second.members[0].id);
    let a0 = Abs::State {
        qps: Set::empty().insert(7), groups: Map::empty().insert(0, first).insert(1, second),
        next_id: 2, outcomes: Map::empty(), posted: Map::empty(), wakes: Seq::empty(),
    };
    assert(abs_inv(a0));
    let empty = View::<safeverbs_spec::types::Span> {
        req: |s: int| None, staged: |s: int| None, refused: |s: int| None,
    };
    let ids0 = Map::empty();
    let dirs0 = Permissions {
        access: Map::empty(),
        resources: |s: int, span: safeverbs_spec::types::Span|
            Group::singleton(if s == 0 { 0nat } else { 1nat }, span.bytes(), false, true).members,
    };
    assert((dirs0.resources)(0, shared) == first.members);
    assert((dirs0.resources)(1, shared) == second.members);
    assert(placed(empty, a0, 7, ids0, dirs0));
    let out = seq![Dir::Out];
    lemma_end(empty, a0, 7, ids0, dirs0, 0, 0, out, false, shared);
    let one = staged_with(empty, 0, false, shared);
    let ids1 = ids0.insert(0, 0);
    let dirs1 = dirs0.insert(0, out);
    lemma_end(one, a0, 7, ids1, dirs1, 1, 1, out, false, shared);
    let staged = staged_with(one, 1, false, shared);
    let staged_ids = ids1.insert(1, 1);
    let dirs = dirs1.insert(1, out);
    assert((staged.staged)(0) is Some && (staged.staged)(1) is Some);
    assert((staged.staged)(0)->0.leases == (staged.staged)(1)->0.leases);
    assert((dirs.resources)(0, shared) != (dirs.resources)(1, shared));

    let (a1, ids) = lemma_commit(staged, a0, 7, staged_ids, dirs, 0, 2);
    let in_flight = posted_range(staged, 0, 2);
    assert((in_flight.req)(0) is Some && (in_flight.req)(1) is Some);
    assert((in_flight.req)(0)->0.leases == shared && (in_flight.req)(1)->0.leases == shared);
    assert(holds(in_flight, 0) && holds(in_flight, 1));
    assert(resources_match(in_flight, a1, ids, dirs, 0));
    assert(resources_match(in_flight, a1, ids, dirs, 1));
    assert(ids[0] != ids[1]);
    assert(a1.in_flight(ids[0]) && a1.in_flight(ids[1]));
    assert(a1.groups[ids[0]].members == first.members);
    assert(a1.groups[ids[1]].members == second.members);
    assert(a1.groups[ids[0]].compatible(a1.groups[ids[1]]));
    assert(a1.groups[ids[0]].nic_may_read(0) && a1.groups[ids[1]].nic_may_read(0));
    assert(!a1.groups[ids[0]].nic_may_write(0) && !a1.groups[ids[1]].nic_may_write(0));

    let wc = Wc {
        wr_id: 1, status: crate::wc::Status::SUCCESS, opcode: crate::wc::Opcode::SEND,
        byte_len: 0, imm: None, vendor_err: 0, qp_num: 7,
    };
    lemma_complete_one(in_flight, a1, 7, ids, dirs, 0, wc);
    let a2 = complete_state(a1, ids[0], wc.outcome());
    let one_done = completed(in_flight, 0, wc);
    assert((one_done.req)(1) is Some && (one_done.req)(1)->0.phase is InFlight);
    lemma_complete_one(one_done, a2, 7, ids, dirs, 1, wc);
    let done = complete_state(a2, ids[1], wc.outcome());
    let done_view = completed(one_done, 1, wc);
    let side = Map::empty().insert(0, (wc, shared)).insert(1, (wc, shared));
    let refused = Map::empty();
    let side_view = with_side(empty, side, refused);
    assert(same(done_view, side_view));
    lemma_same(done_view, side_view, done, 7, ids, dirs);
    assert(side[0].1 == side[1].1);
    assert(resources_match(side_view, done, ids, dirs, 0));
    assert(resources_match(side_view, done, ids, dirs, 1));

    let taken = lemma_side_take(empty, 0, side, refused, done, 7, ids, dirs);
    assert(taken.groups[done.next_id].members == (dirs.resources)(0, shared));
    let remaining = with_side(empty, side.remove(0), refused);
    assert((remaining.req)(1) is Some && holds(remaining, 1));
    assert(resources_match(remaining, taken, ids, dirs, 1));
    assert(taken.groups[done.next_id].members[0].id == 0);
    assert(taken.groups[ids[1]].members[0].id == 1);
    assert(taken.groups[done.next_id].members[0].mem == taken.groups[ids[1]].members[0].mem);
    assert(!taken.groups[done.next_id].members[0].unique && !taken.groups[ids[1]].members[0].unique);
}

} // verus!
