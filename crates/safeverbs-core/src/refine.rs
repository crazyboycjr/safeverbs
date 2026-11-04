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
//! | `fill`, `unfill`, `take_general_path`, `take_detached_error` | none (`lemma_ring_stutter`) |
//! | `stage`, `stage_filled` | none: a staged request's leases are still the program's (`lemma_ring_end`) |
//! | `commit` | `post_request` for each staged request, then its `register` or `detach` (`lemma_ring_commit`) |
//! | `abort`, `abort_keeping` | `drop` of each lease of each staged request without an owner; none for the others, whose leases the ring keeps for their owners (refused) or hands back (`lemma_ring_abort`) |
//! | `complete`, `retire_next` | `complete` for each request retired, in order, waking the same wakers (`lemma_ring_complete`) |
//! | `retire_oldest`, its slot emptied | `complete`, then `take`, of the oldest request in flight (`lemma_ring_oldest`) |
//! | `take`, `take_retired` | `take` (`lemma_ring_take`); none for a refused request (`lemma_ring_take_refused`) |
//! | `detach` | `detach`, none for a staged request, `drop` of each lease for a refused one (`lemma_ring_detach`) |
//! | `set_waker` | `register`, none for a staged request (`lemma_ring_register`) |
//! | `release_all` | `complete` for each request in flight, then `detach`, and `drop` of each lease of each staged or refused one (`lemma_ring_release_all`) |
//!
//! A request whose doorbell was refused never reached A: its leases stay
//! the program's, and the ring only keeps them for the owner (`View::refused`),
//! who takes them back with a refusal or drops them.
//!
//! The async layer's side list of results moved out of a clogged ring is
//! covered too (`with_side`, `lemma_side_move`, `lemma_side_take`,
//! `lemma_side_detach` and their `_refused` forms): moving a result there
//! is no step, taking it is A's `take` with the outcome and leases the ring
//! gave (none for a refused one), dropping its future A's `detach` (`drop`
//! of each lease for a refused one).
//!
//! Leases are tracked as values: the `L` that A sees a request hold is the
//! one `stage` moved into the ring, and `take` and `abort_keeping` hand back
//! that value. With the linearity of `L` (Rust moves it; the ring never
//! duplicates it), this proves retention of the ring's resource values: its leases stay
//! in its slot, unreachable, from `stage` until A says the NIC is done with
//! them (complete, or never posted), and leave it exactly once.
//!
//! Which request of A a request of the ring is (`reqs`) and which leases of A
//! its value `L` stands for (`Held`) are ghost; the ring needs neither.
//!
//! Everything here is ghost code: a plain build erases it.
// The imports are used by ghost code only.
#![allow(unused_imports)]

use safeverbs_spec::abs::*;
use safeverbs_spec::refinement::*;
use safeverbs_spec::types::{Outcome, QpId, Status};
use vstd::prelude::*;

use crate::ring::{ReqView, Ring};
use crate::wc::Wc;

verus! {

/// Which leases of A a request's value `L` stands for, each with how the NIC
/// touches it, given the request's sequence number: equal shared values in
/// different requests stand for different leases. Building it from the engine's
/// buffers is the engine's proof; these adapters prove that the ring preserves it.
pub type Held<L> = spec_fn(int, L) -> Map<LeaseId, Dir>;

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

/// Request `s` of `v` is well formed, and at most one of posted, staged and
/// refused.
pub open spec fn wf_at<L>(v: View<L>, s: int) -> bool {
    &&& (v.req)(s) is Some ==> req_wf((v.req)(s)->0)
    &&& (v.staged)(s) is Some ==> staged_wf((v.staged)(s)->0) && (v.req)(s) is None
    &&& (v.refused)(s) is Some ==> (v.req)(s) is None && (v.staged)(s) is None
}

/// Every request of `v` is well formed.
pub open spec fn wf<L>(v: View<L>) -> bool {
    forall|s: int| #[trigger] wf_at(v, s)
}

/// Request `s` holds a value (posted, staged or refused).
pub open spec fn holds<L>(v: View<L>, s: int) -> bool {
    (v.req)(s) is Some || (v.staged)(s) is Some || (v.refused)(s) is Some
}

/// The leases of A that request `s` of `v` holds.
pub open spec fn leases_of<L>(v: View<L>, held: Held<L>, s: int) -> Map<LeaseId, Dir> {
    if (v.req)(s) is Some {
        held(s, (v.req)(s)->0.leases)
    } else if (v.staged)(s) is Some {
        held(s, (v.staged)(s)->0.leases)
    } else if (v.refused)(s) is Some {
        held(s, (v.refused)(s)->0)
    } else {
        Map::empty()
    }
}

/// The request of A that request view `r` stands for, on queue pair `q`.
pub open spec fn as_req<L>(r: ReqView<L>, q: QpId) -> Req {
    Req { qp: q, phase: r.phase, waker: r.waker, detached: r.detached }
}

/// Request `r` of `a` holds exactly the leases `dirs`, each in its direction.
pub open spec fn holds_exactly(a: Abs::State, r: ReqId, dirs: Map<LeaseId, Dir>) -> bool {
    &&& forall|l: LeaseId| #[trigger] dirs.contains_key(l) ==> a.leases.contains_key(l)
        && a.leases[l].holder == (Holder::Request { req: r, dir: dirs[l] })
    &&& forall|l: LeaseId| #[trigger] a.leases.contains_key(l) && a.leases[l].held_by(r)
        ==> dirs.contains_key(l)
}

/// Request `s` of `v` sits in `a`: a posted one is request `reqs[s]` of `a`, on
/// queue pair `q`, with the same phase, waker and owner, and holds exactly its
/// leases; the leases of a staged or refused one are the program's, and may
/// be posted in their directions.
pub open spec fn sits<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
) -> bool {
    &&& (v.req)(s) is Some ==> {
        &&& reqs.contains_key(s)
        &&& a.requests.contains_key(reqs[s])
        &&& a.requests[reqs[s]] == as_req((v.req)(s)->0, q)
        &&& holds_exactly(a, reqs[s], held(s, (v.req)(s)->0.leases))
    }
    &&& (v.staged)(s) is Some ==> a.may_post(held(s, (v.staged)(s)->0.leases))
    &&& (v.refused)(s) is Some ==> a.may_post(held(s, (v.refused)(s)->0))
}

/// The ring's requests sit in `a`, and no two hold the same lease or are the
/// same request of `a`.
pub open spec fn placed<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
) -> bool {
    &&& abs_inv(a)
    &&& wf(v)
    &&& a.qps.contains(q)
    &&& forall|s: int| #[trigger] sits(v, a, q, reqs, held, s)
    &&& forall|s: int, t: int| #![trigger leases_of(v, held, s), leases_of(v, held, t)]
        s != t ==> leases_of(v, held, s).dom().disjoint(leases_of(v, held, t).dom())
    &&& forall|s: int, t: int| #![trigger reqs[s], reqs[t]]
        (v.req)(s) is Some && (v.req)(t) is Some && s != t ==> reqs[s] != reqs[t]
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
// A's states after one step

/// A's state after `complete` of request `r` with outcome `o`.
pub open spec fn complete_state(a: Abs::State, r: ReqId, o: Outcome) -> Abs::State {
    let req = a.requests[r];
    Abs::State {
        outcomes: a.outcomes.insert(r, o),
        wakes: if req.waker is Some {
            a.wakes.push(req.waker->0)
        } else {
            a.wakes
        },
        requests: if req.detached {
            a.requests.remove(r)
        } else {
            a.requests.insert(r, req.completed(o))
        },
        leases: if req.detached {
            a.leases_but(r)
        } else {
            a.leases
        },
        ..a
    }
}

/// A's state after posting the program's leases `dirs` on `q`.
pub open spec fn post_state(a: Abs::State, dirs: Map<LeaseId, Dir>, q: QpId) -> Abs::State {
    let r = a.next_id;
    Abs::State {
        requests: a.requests.insert(r, Req { qp: q, phase: Phase::InFlight, waker: None, detached: false }),
        leases: a.leases_in_request(dirs, r),
        next_id: r + 1,
        ..a
    }
}

/// A's state after `register` of waker `w` on request `r`.
pub open spec fn register_state(a: Abs::State, r: ReqId, w: WakerId) -> Abs::State {
    Abs::State { requests: a.requests.insert(r, a.requests[r].with_waker(w)), ..a }
}

/// A's state after `detach` of request `r`.
pub open spec fn detach_state(a: Abs::State, r: ReqId) -> Abs::State {
    let done = a.requests[r].phase is Complete;
    Abs::State {
        requests: if done {
            a.requests.remove(r)
        } else {
            a.requests.insert(r, a.requests[r].detach())
        },
        leases: if done {
            a.leases_but(r)
        } else {
            a.leases
        },
        ..a
    }
}

/// A's state after `drop` of the program's lease `l`.
pub open spec fn drop_state(a: Abs::State, l: LeaseId) -> Abs::State {
    Abs::State { leases: a.leases.remove(l), ..a }
}

/// A's state after `take` of request `r`.
pub open spec fn take_state(a: Abs::State, r: ReqId) -> Abs::State {
    Abs::State { requests: a.requests.remove(r), leases: a.leases_returned(r), ..a }
}

/// One step of A keeps its invariants and is an execution.
proof fn lemma_step_one(a: Abs::State, a2: Abs::State, step: Abs::Step)
    requires
        abs_inv(a),
        abs_step(a, a2, step),
    ensures
        abs_inv(a2),
        abs_reaches(a, a2),
{
    lemma_abs_step_is_next(a, a2, step);
    lemma_abs_reaches_next(a, a2);
    lemma_abs_reaches_invariant(a, a2);
}

/// Dropping a set of the program's leases is A's `drop` of each.
proof fn lemma_drop_leases(a: Abs::State, ls: Set<LeaseId>) -> (a2: Abs::State)
    requires
        abs_inv(a),
        forall|l: LeaseId| #[trigger] ls.contains(l) ==> a.is_program(l),
    ensures
        abs_inv(a2),
        abs_reaches(a, a2),
        a2 == (Abs::State { leases: a.leases.remove_keys(ls), ..a }),
    decreases ls.len(),
{
    if ls =~= Set::empty() {
        lemma_abs_reaches_refl(a);
        assert(a.leases.remove_keys(ls) =~= a.leases);
        a
    } else {
        let l = ls.choose();
        assert(ls.contains(l));
        let a1 = drop_state(a, l);
        assert(abs_step(a, a1, Abs::Step::drop(l)));
        lemma_step_one(a, a1, Abs::Step::drop(l));
        let a2 = lemma_drop_leases(a1, ls.remove(l));
        lemma_abs_reaches_trans(a, a1, a2);
        assert(a1.leases.remove_keys(ls.remove(l)) =~= a.leases.remove_keys(ls));
        a2
    }
}

// =============================================================================
// Single steps

/// Placement carries over to a view that agrees.
pub proof fn lemma_same<L>(
    v: View<L>,
    w: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
)
    requires
        placed(v, a, q, reqs, held),
        same(v, w),
    ensures
        placed(w, a, q, reqs, held),
{
    assert forall|s: int| #[trigger] wf_at(w, s) by {
        assert(wf_at(v, s));
        assert((v.req)(s) == (w.req)(s) && (v.staged)(s) == (w.staged)(s) && (v.refused)(s) == (w.refused)(s));
    }
    assert forall|s: int| #[trigger] sits(w, a, q, reqs, held, s) by {
        assert(sits(v, a, q, reqs, held, s));
        assert((v.req)(s) == (w.req)(s) && (v.staged)(s) == (w.staged)(s) && (v.refused)(s) == (w.refused)(s));
    }
    assert forall|s: int, t: int| #![trigger leases_of(w, held, s), leases_of(w, held, t)]
        s != t implies leases_of(w, held, s).dom().disjoint(leases_of(w, held, t).dom()) by {
        assert(leases_of(v, held, s).dom().disjoint(leases_of(v, held, t).dom()));
        assert((v.req)(s) == (w.req)(s) && (v.staged)(s) == (w.staged)(s) && (v.refused)(s) == (w.refused)(s));
        assert((v.req)(t) == (w.req)(t) && (v.staged)(t) == (w.staged)(t) && (v.refused)(t) == (w.refused)(t));
    }
    assert forall|s: int, t: int| #![trigger reqs[s], reqs[t]]
        (w.req)(s) is Some && (w.req)(t) is Some && s != t implies reqs[s] != reqs[t] by {
        assert((v.req)(s) == (w.req)(s) && (v.req)(t) == (w.req)(t));
    }
}

/// `w` is placed in `a2` when it agrees with `v`, placed in `a`, but at request
/// `s`, which sits in `a2`, and `a2` keeps what `a` has but request `r` and the
/// leases of `s`, which request `r` or the program holds if they are left.
proof fn lemma_placed_but<L>(
    v: View<L>,
    w: View<L>,
    a: Abs::State,
    a2: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    reqs2: Map<int, ReqId>,
    held: Held<L>,
    s: int,
    r: ReqId,
)
    requires
        placed(v, a, q, reqs, held),
        abs_inv(a2),
        a2.qps == a.qps,
        forall|t: int| t != s ==> #[trigger] (w.req)(t) == (v.req)(t),
        forall|t: int| t != s ==> #[trigger] (w.staged)(t) == (v.staged)(t),
        forall|t: int| t != s ==> #[trigger] (w.refused)(t) == (v.refused)(t),
        forall|t: int| t != s && #[trigger] reqs.contains_key(t) ==> reqs2.contains_key(t) && reqs2[t] == reqs[t],
        forall|t: ReqId| #[trigger] a.requests.contains_key(t) && t != r
            ==> a2.requests.contains_key(t) && a2.requests[t] == a.requests[t],
        r == a.next_id || ((v.req)(s) is Some && r == reqs[s]),
        forall|l: LeaseId| #[trigger] a.leases.contains_key(l) && !leases_of(v, held, s).contains_key(l)
            ==> a2.leases.contains_key(l) && a2.leases[l] == a.leases[l],
        forall|l: LeaseId| #[trigger] a2.leases.contains_key(l) ==> a.leases.contains_key(l)
            && (leases_of(v, held, s).contains_key(l)
                ==> a2.leases[l].holder is Program || a2.leases[l].held_by(r)),
        wf_at(w, s),
        sits(w, a2, q, reqs2, held, s),
        leases_of(w, held, s).dom().subset_of(leases_of(v, held, s).dom()),
        (w.req)(s) is Some ==> reqs2[s] == r,
    ensures
        placed(w, a2, q, reqs2, held),
{
    assert forall|t: int| t != s && #[trigger] (v.req)(t) is Some implies reqs[t] != r by {
        assert(sits(v, a, q, reqs, held, t));
    }
    assert forall|t: int| #[trigger] wf_at(w, t) by {
        if t != s {
            assert(wf_at(v, t));
        }
    }
    assert forall|t: int| #[trigger] sits(w, a2, q, reqs2, held, t) by {
        if t != s {
            assert(sits(v, a, q, reqs, held, t));
            assert(wf_at(v, t));
            assert(leases_of(v, held, t).dom().disjoint(leases_of(v, held, s).dom()));
            assert((w.req)(t) == (v.req)(t) && (w.staged)(t) == (v.staged)(t) && (w.refused)(t) == (v.refused)(t));
            if (v.req)(t) is Some {
                let dirs = held(t, (v.req)(t)->0.leases);
                assert(dirs == leases_of(v, held, t));
                assert(reqs2.contains_key(t) && reqs2[t] == reqs[t]);
                assert forall|l: LeaseId| #[trigger] dirs.contains_key(l) implies a2.leases.contains_key(l)
                    && a2.leases[l].holder == (Holder::Request { req: reqs[t], dir: dirs[l] }) by {
                    assert(!leases_of(v, held, s).dom().contains(l));
                }
                assert forall|l: LeaseId| #[trigger] a2.leases.contains_key(l) && a2.leases[l].held_by(reqs[t])
                    implies dirs.contains_key(l) by {
                    assert(a.leases.contains_key(l));
                    assert(reqs[t] != r);
                }
            }
            if (v.staged)(t) is Some {
                let dirs = held(t, (v.staged)(t)->0.leases);
                assert(dirs == leases_of(v, held, t));
                assert forall|l: LeaseId| #[trigger] dirs.contains_key(l) implies a2.is_program(l)
                    && (dirs[l] is In ==> a2.leases[l].writable()) by {
                    assert(!leases_of(v, held, s).dom().contains(l));
                }
            }
            if (v.refused)(t) is Some {
                let dirs = held(t, (v.refused)(t)->0);
                assert(dirs == leases_of(v, held, t));
                assert forall|l: LeaseId| #[trigger] dirs.contains_key(l) implies a2.is_program(l)
                    && (dirs[l] is In ==> a2.leases[l].writable()) by {
                    assert(!leases_of(v, held, s).dom().contains(l));
                }
            }
        }
    }
    assert forall|t: int, u: int| #![trigger leases_of(w, held, t), leases_of(w, held, u)]
        t != u implies leases_of(w, held, t).dom().disjoint(leases_of(w, held, u).dom()) by {
        assert(leases_of(v, held, t).dom().disjoint(leases_of(v, held, u).dom()));
        if t != s {
            assert((w.req)(t) == (v.req)(t) && (w.staged)(t) == (v.staged)(t) && (w.refused)(t) == (v.refused)(t));
            assert(leases_of(w, held, t) == leases_of(v, held, t));
        }
        if u != s {
            assert((w.req)(u) == (v.req)(u) && (w.staged)(u) == (v.staged)(u) && (w.refused)(u) == (v.refused)(u));
            assert(leases_of(w, held, u) == leases_of(v, held, u));
        }
    }
    assert forall|t: int, u: int| #![trigger reqs2[t], reqs2[u]]
        (w.req)(t) is Some && (w.req)(u) is Some && t != u implies reqs2[t] != reqs2[u] by {
        if t != s {
            assert(sits(v, a, q, reqs, held, t));
            assert((w.req)(t) == (v.req)(t) && reqs2[t] == reqs[t]);
        }
        if u != s {
            assert(sits(v, a, q, reqs, held, u));
            assert((w.req)(u) == (v.req)(u) && reqs2[u] == reqs[u]);
        }
    }
}

/// `w` is placed in `a`: it agrees with `v` but at request `s`, whose leases
/// (if any are left to it) are the program's: no step of A.
proof fn lemma_reshape<L>(
    v: View<L>,
    w: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
)
    requires
        placed(v, a, q, reqs, held),
        forall|t: int| t != s ==> #[trigger] (w.req)(t) == (v.req)(t),
        forall|t: int| t != s ==> #[trigger] (w.staged)(t) == (v.staged)(t),
        forall|t: int| t != s ==> #[trigger] (w.refused)(t) == (v.refused)(t),
        (v.req)(s) is None,
        (w.req)(s) is None,
        wf_at(w, s),
        sits(w, a, q, reqs, held, s),
        leases_of(w, held, s).dom().subset_of(leases_of(v, held, s).dom()),
    ensures
        placed(w, a, q, reqs, held),
{
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    lemma_placed_but(v, w, a, a, q, reqs, reqs, held, s, a.next_id);
}

/// A's `complete` of request `s`, in flight, with `o`: the view after it.
pub open spec fn completed<L>(v: View<L>, s: int, o: Outcome) -> View<L> {
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

/// Completing request `s` in flight is A's `complete` of it.
pub proof fn lemma_complete_one<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
    o: Outcome,
)
    requires
        placed(v, a, q, reqs, held),
        (v.req)(s) is Some,
        (v.req)(s)->0.phase is InFlight,
    ensures
        abs_step(a, complete_state(a, reqs[s], o), Abs::Step::complete(reqs[s], o)),
        placed(completed(v, s, o), complete_state(a, reqs[s], o), q, reqs, held),
        complete_state(a, reqs[s], o).wakes == match (v.req)(s)->0.waker {
            Some(w) => a.wakes.push(w),
            None => a.wakes,
        },
{
    let r = reqs[s];
    let a2 = complete_state(a, r, o);
    let w = completed(v, s, o);
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    assert(abs_step(a, a2, Abs::Step::complete(r, o)));
    lemma_step_one(a, a2, Abs::Step::complete(r, o));
    lemma_placed_but(v, w, a, a2, q, reqs, reqs, held, s, r);
}

/// Requests `[from, to)` of `v` retired with outcomes `o`: each in flight
/// completes (and is gone if detached); refused ones A never saw.
pub open spec fn retired_range<L>(v: View<L>, from: int, to: int, o: spec_fn(int) -> Outcome) -> View<L> {
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
    reqs: Map<int, ReqId>,
    held: Held<L>,
    from: int,
    to: int,
    o: spec_fn(int) -> Outcome,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
        from <= to,
        forall|s: int| from <= s < to && #[trigger] (v.req)(s) is Some ==> (v.req)(s)->0.phase is InFlight,
    ensures
        abs_reaches(a, a2),
        placed(retired_range(v, from, to, o), a2, q, reqs, held),
        a2.wakes == a.wakes + wakers(v, from, to),
    decreases to - from,
{
    if to == from {
        lemma_abs_reaches_refl(a);
        lemma_same(v, retired_range(v, from, to, o), a, q, reqs, held);
        assert(a.wakes =~= a.wakes + wakers(v, from, to));
        a
    } else if (v.req)(to - 1) is None {
        let s = to - 1;
        let a1 = lemma_complete(v, a, q, reqs, held, from, s, o);
        lemma_same(retired_range(v, from, s, o), retired_range(v, from, to, o), a1, q, reqs, held);
        a1
    } else {
        let s = to - 1;
        let a1 = lemma_complete(v, a, q, reqs, held, from, s, o);
        let v1 = retired_range(v, from, s, o);
        assert((v1.req)(s) == (v.req)(s));
        lemma_complete_one(v1, a1, q, reqs, held, s, o(s));
        let a2 = complete_state(a1, reqs[s], o(s));
        lemma_same(completed(v1, s, o(s)), retired_range(v, from, to, o), a2, q, reqs, held);
        lemma_abs_step_is_next(a1, a2, Abs::Step::complete(reqs[s], o(s)));
        lemma_abs_reaches_next(a1, a2);
        lemma_abs_reaches_trans(a, a1, a2);
        assert(a2.wakes =~= a.wakes + wakers(v, from, to));
        a2
    }
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

/// Posting staged request `s` is A's `post_request` of its leases, then
/// `register` of its waker or `detach` if it has no owner. It becomes a new request of A.
pub proof fn lemma_post_one<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
        (v.staged)(s) is Some,
    ensures
        abs_reaches(a, a2),
        placed(posted(v, s), a2, q, reqs.insert(s, a.next_id), held),
        a2.wakes == a.wakes,
{
    let st = (v.staged)(s)->0;
    let dirs = held(s, st.leases);
    let r = a.next_id;
    let reqs2 = reqs.insert(s, r);
    let w = posted(v, s);
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    assert(leases_of(v, held, s) == dirs);
    // post_request
    let a1 = post_state(a, dirs, q);
    assert(abs_step(a, a1, Abs::Step::post_request(dirs, q)));
    lemma_step_one(a, a1, Abs::Step::post_request(dirs, q));
    // register or detach
    let a2 = if st.waker is Some {
        register_state(a1, r, st.waker->0)
    } else if st.detached {
        detach_state(a1, r)
    } else {
        a1
    };
    if st.waker is Some {
        assert(abs_step(a1, a2, Abs::Step::register(r, st.waker->0)));
        lemma_step_one(a1, a2, Abs::Step::register(r, st.waker->0));
        lemma_abs_reaches_trans(a, a1, a2);
    } else if st.detached {
        assert(abs_step(a1, a2, Abs::Step::detach(r)));
        lemma_step_one(a1, a2, Abs::Step::detach(r));
        lemma_abs_reaches_trans(a, a1, a2);
    }
    assert(a2.leases == a1.leases);
    assert(holds_exactly(a2, r, dirs));
    lemma_placed_but(v, w, a, a2, q, reqs, reqs2, held, s, r);
    a2
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
    reqs: Map<int, ReqId>,
    held: Held<L>,
    from: int,
    to: int,
) -> (r: (Abs::State, Map<int, ReqId>))
    requires
        placed(v, a, q, reqs, held),
        from <= to,
        forall|s: int| from <= s < to ==> #[trigger] (v.staged)(s) is Some,
    ensures
        abs_reaches(a, r.0),
        placed(posted_range(v, from, to), r.0, q, r.1, held),
        r.0.wakes == a.wakes,
    decreases to - from,
{
    if to == from {
        lemma_abs_reaches_refl(a);
        lemma_same(v, posted_range(v, from, to), a, q, reqs, held);
        (a, reqs)
    } else {
        let s = to - 1;
        let (a1, reqs1) = lemma_commit(v, a, q, reqs, held, from, s);
        let v1 = posted_range(v, from, s);
        assert((v1.staged)(s) == (v.staged)(s));
        let a2 = lemma_post_one(v1, a1, q, reqs1, held, s);
        let reqs2 = reqs1.insert(s, a1.next_id);
        lemma_same(posted(v1, s), posted_range(v, from, to), a2, q, reqs2, held);
        lemma_abs_reaches_trans(a, a1, a2);
        (a2, reqs2)
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
/// one's leases; the others' leases stay the program's: `keep`'s go back to
/// it, the rest wait in the ring for their owners.
pub proof fn lemma_abort<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    from: int,
    to: int,
    keep: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
        from <= to,
        forall|s: int| from <= s < to ==> #[trigger] (v.staged)(s) is Some,
    ensures
        abs_reaches(a, a2),
        placed(refused_range(v, from, to, keep), a2, q, reqs, held),
        a2.wakes == a.wakes,
        a2.requests == a.requests,
        a2.outcomes == a.outcomes,
        (v.staged)(keep) is Some ==> forall|l: LeaseId| #[trigger] held(keep, (v.staged)(keep)->0.leases).contains_key(l)
            ==> a2.is_program(l) && a2.leases[l] == a.leases[l],
    decreases to - from,
{
    if to == from {
        lemma_abs_reaches_refl(a);
        lemma_same(v, refused_range(v, from, to, keep), a, q, reqs, held);
        if (v.staged)(keep) is Some {
            assert(sits(v, a, q, reqs, held, keep));
        }
        a
    } else {
        let s = to - 1;
        let a1 = lemma_abort(v, a, q, reqs, held, from, s, keep);
        let v1 = refused_range(v, from, s, keep);
        let w = refused_range(v, from, to, keep);
        assert((v1.staged)(s) == (v.staged)(s));
        assert((v1.refused)(s) == (v.refused)(s));
        assert(wf_at(v1, s));
        assert(sits(v1, a1, q, reqs, held, s));
        if s == keep || !(v.staged)(s)->0.detached {
            // The program keeps the leases (back in its hands, or held in the
            // ring for the owner): no step.
            lemma_abs_reaches_refl(a1);
            lemma_reshape(v1, w, a1, q, reqs, held, s);
            a1
        } else {
            let dirs = held(s, (v.staged)(s)->0.leases);
            assert(leases_of(v1, held, s) == dirs);
            let a2 = lemma_drop_leases(a1, dirs.dom());
            lemma_placed_but(v1, w, a1, a2, q, reqs, reqs, held, s, a1.next_id);
            lemma_abs_reaches_trans(a, a1, a2);
            if (v.staged)(keep) is Some {
                // `keep`'s leases are not `s`'s.
                assert(keep != s);
                let kept = held(keep, (v.staged)(keep)->0.leases);
                assert forall|l: LeaseId| #[trigger] kept.contains_key(l)
                    implies a2.is_program(l) && a2.leases[l] == a.leases[l] by {
                    assert(leases_of(v, held, keep) == kept) by {
                        assert(wf_at(v, keep));
                    }
                    assert(leases_of(v, held, s) == dirs) by {
                        assert(wf_at(v, s));
                    }
                    assert(leases_of(v, held, keep).dom().disjoint(leases_of(v, held, s).dom()));
                }
            }
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
    reqs: Map<int, ReqId>,
    held: Held<L>,
    from: int,
    to: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
        from <= to,
        forall|s: int| from <= s < to ==> #[trigger] (v.staged)(s) is Some,
    ensures
        abs_reaches(a, a2),
        placed(dropped_range(v, from, to), a2, q, reqs, held),
    decreases to - from,
{
    if to == from {
        lemma_abs_reaches_refl(a);
        lemma_same(v, dropped_range(v, from, to), a, q, reqs, held);
        a
    } else {
        let s = to - 1;
        let a1 = lemma_drop_staged(v, a, q, reqs, held, from, s);
        let v1 = dropped_range(v, from, s);
        let w = dropped_range(v, from, to);
        assert((v1.staged)(s) == (v.staged)(s));
        assert(wf_at(v1, s));
        assert(sits(v1, a1, q, reqs, held, s));
        let dirs = held(s, (v.staged)(s)->0.leases);
        assert(leases_of(v1, held, s) == dirs);
        let a2 = lemma_drop_leases(a1, dirs.dom());
        lemma_placed_but(v1, w, a1, a2, q, reqs, reqs, held, s, a1.next_id);
        lemma_abs_reaches_trans(a, a1, a2);
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

/// Taking refused request `s` is no step of A: its leases were the
/// program's all along.
pub proof fn lemma_take_refused<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
)
    requires
        placed(v, a, q, reqs, held),
        (v.refused)(s) is Some,
    ensures
        placed(without_refused(v, s), a, q, reqs, held),
        a.may_post(held(s, (v.refused)(s)->0)),
{
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    lemma_reshape(v, without_refused(v, s), a, q, reqs, held, s);
}

/// Dropping refused request `s` is A's `drop` of each of its leases.
pub proof fn lemma_drop_refused<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
        (v.refused)(s) is Some,
    ensures
        abs_reaches(a, a2),
        placed(without_refused(v, s), a2, q, reqs, held),
        a2.wakes == a.wakes,
{
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    let dirs = held(s, (v.refused)(s)->0);
    assert(leases_of(v, held, s) == dirs);
    let a2 = lemma_drop_leases(a, dirs.dom());
    lemma_placed_but(v, without_refused(v, s), a, a2, q, reqs, reqs, held, s, a.next_id);
    a2
}

/// Taking complete request `s`, with outcome `o` and value `l`, is A's
/// `take`: the program holds each of its leases again, under its own name.
pub proof fn lemma_take<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
    o: Outcome,
    l: L,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
        (v.req)(s) == Some(ReqView { phase: Phase::Complete(o), waker: None, detached: false, leases: l }),
    ensures
        abs_reaches(a, a2),
        abs_step(a, a2, Abs::Step::take(reqs[s], o)),
        placed(without(v, s), a2, q, reqs, held),
        a2.wakes == a.wakes,
        forall|k: LeaseId| #[trigger] held(s, l).contains_key(k)
            ==> a2.is_program(k) && a2.leases[k].same_as(a.leases[k]),
{
    let r = reqs[s];
    let a2 = take_state(a, r);
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    assert(abs_step(a, a2, Abs::Step::take(r, o)));
    lemma_step_one(a, a2, Abs::Step::take(r, o));
    assert forall|k: LeaseId| #[trigger] a.leases.contains_key(k) && !leases_of(v, held, s).contains_key(k)
        implies a2.leases.contains_key(k) && a2.leases[k] == a.leases[k] by {
        assert(!a.leases[k].held_by(r));
    }
    assert forall|k: LeaseId| #[trigger] held(s, l).contains_key(k)
        implies a2.is_program(k) && a2.leases[k].same_as(a.leases[k]) by {
        assert(a.leases[k].held_by(r));
    }
    lemma_placed_but(v, without(v, s), a, a2, q, reqs, reqs, held, s, r);
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
/// `drop` of its leases if it was refused, else no step (a staged request's
/// leases are still the program's).
pub proof fn lemma_detach<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
    ensures
        abs_reaches(a, a2),
        placed(detached(v, s), a2, q, reqs, held),
        a2.wakes == a.wakes,
{
    let w = detached(v, s);
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    if (v.req)(s) is Some && !(v.req)(s)->0.detached {
        let r = reqs[s];
        let a2 = detach_state(a, r);
        assert(abs_step(a, a2, Abs::Step::detach(r)));
        lemma_step_one(a, a2, Abs::Step::detach(r));
        lemma_placed_but(v, w, a, a2, q, reqs, reqs, held, s, r);
        a2
    } else if (v.refused)(s) is Some {
        let a2 = lemma_drop_refused(v, a, q, reqs, held, s);
        lemma_same(without_refused(v, s), w, a2, q, reqs, held);
        a2
    } else {
        // Nothing A sees changes.
        lemma_abs_reaches_refl(a);
        if (v.req)(s) is Some {
            assert((v.req)(s)->0.detach() == (v.req)(s)->0);
            lemma_same(v, w, a, q, reqs, held);
        } else {
            lemma_reshape(v, w, a, q, reqs, held, s);
        }
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
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
    wk: WakerId,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
    ensures
        abs_reaches(a, a2),
        placed(registered(v, s, wk), a2, q, reqs, held),
        a2.wakes == a.wakes,
{
    let w = registered(v, s, wk);
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    if (v.req)(s) is Some && (v.req)(s)->0.phase is InFlight && !(v.req)(s)->0.detached {
        let r = reqs[s];
        let a2 = register_state(a, r, wk);
        assert(abs_step(a, a2, Abs::Step::register(r, wk)));
        lemma_step_one(a, a2, Abs::Step::register(r, wk));
        lemma_placed_but(v, w, a, a2, q, reqs, reqs, held, s, r);
        a2
    } else {
        lemma_abs_reaches_refl(a);
        if (v.req)(s) is Some {
            lemma_same(v, w, a, q, reqs, held);
        } else {
            lemma_reshape(v, w, a, q, reqs, held, s);
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

/// Staging a request is no step of A: its leases stay the program's until it
/// is posted. They may be posted in their directions, and no other request of
/// the ring holds them.
pub proof fn lemma_end<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
    detached: bool,
    l: L,
)
    requires
        placed(v, a, q, reqs, held),
        !holds(v, s),
        a.may_post(held(s, l)),
        forall|t: int| #[trigger] leases_of(v, held, t).dom().disjoint(held(s, l).dom()),
    ensures
        placed(staged_with(v, s, detached, l), a, q, reqs, held),
{
    let w = staged_with(v, s, detached, l);
    assert forall|t: int| #[trigger] wf_at(w, t) by {
        assert(wf_at(v, t));
    }
    assert forall|t: int| #[trigger] sits(w, a, q, reqs, held, t) by {
        assert(sits(v, a, q, reqs, held, t));
    }
    assert forall|t: int| t != s implies #[trigger] leases_of(w, held, t) == leases_of(v, held, t) by {}
    assert(leases_of(w, held, s) == held(s, l));
    assert forall|t: int, u: int| #![trigger leases_of(w, held, t), leases_of(w, held, u)]
        t != u implies leases_of(w, held, t).dom().disjoint(leases_of(w, held, u).dom()) by {
        assert(leases_of(v, held, t).dom().disjoint(leases_of(v, held, u).dom()));
        assert(leases_of(v, held, t).dom().disjoint(held(s, l).dom()));
        assert(leases_of(v, held, u).dom().disjoint(held(s, l).dom()));
    }
}

/// Releasing request `s` when its queue pair is destroyed is A's
/// `complete` (with a flush, if it is in flight) and then its `detach` (if
/// it has an owner).
pub proof fn lemma_release_one<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
        (v.req)(s) is Some,
    ensures
        abs_reaches(a, a2),
        placed(without(v, s), a2, q, reqs, held),
{
    let rv = (v.req)(s)->0;
    let r = reqs[s];
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
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
        lemma_step_one(a, a1, Abs::Step::complete(r, flushed()));
        lemma_placed_but(v, v1, a, a1, q, reqs, reqs, held, s, r);
    } else {
        lemma_abs_reaches_refl(a);
        lemma_same(v, v1, a, q, reqs, held);
    }
    if rv.detached {
        lemma_same(v1, without(v, s), a1, q, reqs, held);
        a1
    } else {
        let a2 = detach_state(a1, r);
        assert(sits(v1, a1, q, reqs, held, s));
        assert(abs_step(a1, a2, Abs::Step::detach(r)));
        lemma_step_one(a1, a2, Abs::Step::detach(r));
        lemma_placed_but(v1, without(v, s), a1, a2, q, reqs, reqs, held, s, r);
        lemma_abs_reaches_trans(a, a1, a2);
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
/// and `drop` of each refused one's leases.
pub proof fn lemma_release<L>(
    v: View<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    from: int,
    to: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
        from <= to,
    ensures
        abs_reaches(a, a2),
        placed(without_range(v, from, to), a2, q, reqs, held),
    decreases to - from,
{
    if to == from {
        lemma_abs_reaches_refl(a);
        lemma_same(v, without_range(v, from, to), a, q, reqs, held);
        a
    } else {
        let s = to - 1;
        let a1 = lemma_release(v, a, q, reqs, held, from, s);
        let v1 = without_range(v, from, s);
        assert((v1.req)(s) == (v.req)(s));
        assert((v1.refused)(s) == (v.refused)(s));
        assert(wf_at(v, s));
        if (v.req)(s) is Some {
            let a2 = lemma_release_one(v1, a1, q, reqs, held, s);
            lemma_same(without(v1, s), without_range(v, from, to), a2, q, reqs, held);
            lemma_abs_reaches_trans(a, a1, a2);
            a2
        } else if (v.refused)(s) is Some {
            let a2 = lemma_drop_refused(v1, a1, q, reqs, held, s);
            lemma_same(without_refused(v1, s), without_range(v, from, to), a2, q, reqs, held);
            lemma_abs_reaches_trans(a, a1, a2);
            a2
        } else {
            lemma_same(v1, without_range(v, from, to), a1, q, reqs, held);
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

/// Filling a slot or emptying it again, marking the request's path and `take_detached_error`: no
/// step.
pub proof fn lemma_ring_stutter<L>(
    pre: Ring<L>,
    post: Ring<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
)
    requires
        placed(pre.view(), a, q, reqs, held),
        post.same_view(&pre),
    ensures
        placed(post.view(), a, q, reqs, held),
{
    post.lemma_same_view(&pre);
    lemma_same(pre.view(), post.view(), a, q, reqs, held);
}

/// `stage`: no step; the staged request holds leases of the program's, which
/// may be posted in their directions.
pub proof fn lemma_ring_end<L>(
    pre: Ring<L>,
    post: Ring<L>,
    detached: bool,
    leases: L,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
)
    requires
        pre.inv(),
        placed(pre.view(), a, q, reqs, held),
        a.may_post(held(pre.spec_posted() as int, leases)),
        forall|t: int| #[trigger] leases_of(pre.view(), held, t).dom().disjoint(
            held(pre.spec_posted() as int, leases).dom(),
        ),
        forall|s: int| #[trigger] post.req(s) == pre.req(s),
        forall|s: int|
            #[trigger] post.staged(s) == if s == pre.spec_posted() {
                Some(ReqView::new(detached, leases))
            } else {
                pre.staged(s)
            },
        forall|s: int| #[trigger] post.refused(s) == pre.refused(s),
    ensures
        placed(post.view(), a, q, reqs, held),
{
    let p = pre.spec_posted() as int;
    pre.lemma_view();
    lemma_end(pre.view(), a, q, reqs, held, p, detached, leases);
    lemma_same(staged_with(pre.view(), p, detached, leases), post.view(), a, q, reqs, held);
}

/// `commit`: `post_request` of every staged request (with its `register`
/// or `detach`). Each becomes a new request of A.
pub proof fn lemma_ring_commit<L>(
    pre: Ring<L>,
    post: Ring<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
) -> (r: (Abs::State, Map<int, ReqId>))
    requires
        pre.inv(),
        placed(pre.view(), a, q, reqs, held),
        forall|s: int|
            #[trigger] post.req(s) == if pre.staged(s) is Some {
                pre.staged(s)
            } else {
                pre.req(s)
            },
        forall|s: int| #[trigger] post.staged(s) is None,
        forall|s: int| #[trigger] post.refused(s) == pre.refused(s),
    ensures
        abs_reaches(a, r.0),
        placed(post.view(), r.0, q, r.1, held),
        r.0.wakes == a.wakes,
{
    pre.lemma_view();
    let (c, p) = (pre.spec_committed() as int, pre.spec_posted() as int);
    let (a2, reqs2) = lemma_commit(pre.view(), a, q, reqs, held, c, p);
    lemma_same(posted_range(pre.view(), c, p), post.view(), a2, q, reqs2, held);
    (a2, reqs2)
}

/// `abort_keeping(keep)` (and `abort`): `drop` of the leases of every detached
/// staged request; `keep`'s are the program's again, unchanged, and the other
/// staged requests' leases, still the program's, wait in the ring for their owners.
pub proof fn lemma_ring_abort<L>(
    pre: Ring<L>,
    post: Ring<L>,
    keep: int,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
) -> (a2: Abs::State)
    requires
        pre.inv(),
        placed(pre.view(), a, q, reqs, held),
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
        abs_reaches(a, a2),
        placed(post.view(), a2, q, reqs, held),
        a2.wakes == a.wakes,
        a2.requests == a.requests,
        a2.outcomes == a.outcomes,
        pre.staged(keep) is Some ==> forall|l: LeaseId| #[trigger] held(keep, pre.staged(keep)->0.leases).contains_key(l)
            ==> a2.is_program(l) && a2.leases[l] == a.leases[l],
{
    pre.lemma_view();
    let (c, p) = (pre.spec_committed() as int, pre.spec_posted() as int);
    let a2 = lemma_abort(pre.view(), a, q, reqs, held, c, p, keep);
    lemma_same(refused_range(pre.view(), c, p, keep), post.view(), a2, q, reqs, held);
    a2
}

/// `complete`: A's `complete` of each request retired, in order, with the
/// outcome the ring gives it (refused ones A never saw); A's wakes grow by
/// exactly the wakers the ring woke (L1 for the ring).
pub proof fn lemma_ring_complete<L>(
    pre: Ring<L>,
    post: Ring<L>,
    recv: bool,
    wqe: u64,
    wc: Wc,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
) -> (a2: Abs::State)
    requires
        pre.inv(),
        placed(pre.view(), a, q, reqs, held),
        pre.spec_retired() <= post.spec_retired() <= pre.spec_committed(),
        forall|s: int|
            #[trigger] post.req(s) == if pre.spec_retired() <= s < post.spec_retired() {
                pre.retired_view(s, pre.retire_outcome(s, recv, wqe, wc))
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
        abs_reaches(a, a2),
        placed(post.view(), a2, q, reqs, held),
        ({
            let woke = wakers(pre.view(), pre.spec_retired() as int, post.spec_retired() as int);
            &&& post.woken() == pre.woken() + woke
            &&& a2.wakes == a.wakes + woke
        }),
{
    pre.lemma_view();
    let (r0, r1) = (pre.spec_retired() as int, post.spec_retired() as int);
    let o = |s: int| crate::wc::result_model(pre.retire_outcome(s, recv, wqe, wc));
    let a2 = lemma_complete(pre.view(), a, q, reqs, held, r0, r1, o);
    assert forall|s: int| #[trigger] (retired_range(pre.view(), r0, r1, o).req)(s) == (post.view().req)(
        s,
    ) by {
        if r0 <= s < r1 {
            pre.lemma_retired_view(s, pre.retire_outcome(s, recv, wqe, wc));
        }
    }
    lemma_same(retired_range(pre.view(), r0, r1, o), post.view(), a2, q, reqs, held);
    pre.lemma_wakers(r0, r1);
    a2
}

/// `retire_oldest`, its caller emptying the slot it returns: A's `complete` of the oldest request in
/// flight, which holds no waker, then A's `take`.
pub proof fn lemma_ring_oldest<L>(
    pre: Ring<L>,
    post: Ring<L>,
    recv: bool,
    wqe: u64,
    wc: Wc,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
) -> (a2: Abs::State)
    requires
        pre.inv(),
        placed(pre.view(), a, q, reqs, held),
        post.retired_and_taken(&pre),
        pre.req(pre.spec_retired() as int) is Some,
        !pre.req(pre.spec_retired() as int)->0.detached,
        pre.req(pre.spec_retired() as int)->0.waker is None,
    ensures
        abs_reaches(a, a2),
        placed(post.view(), a2, q, reqs, held),
        post.woken() == pre.woken(),
        a2.wakes == a.wakes,
{
    let r = pre.spec_retired() as int;
    post.lemma_retired_and_taken(&pre);
    pre.lemma_view();
    let o = |s: int| crate::wc::result_model(pre.retire_outcome(s, recv, wqe, wc));
    let a1 = lemma_complete(pre.view(), a, q, reqs, held, r, r + 1, o);
    let v1 = retired_range(pre.view(), r, r + 1, o);
    let req = pre.req(r)->0;
    let a2 = lemma_take(v1, a1, q, reqs, held, r, o(r), req.leases);
    lemma_abs_reaches_trans(a, a1, a2);
    lemma_same(without(v1, r), post.view(), a2, q, reqs, held);
    assert(wakers(pre.view(), r, r) =~= Seq::<WakerId>::empty());
    assert(a.wakes + wakers(pre.view(), r, r + 1) =~= a.wakes);
    a2
}

/// `take(seq)` returning `Some((outcome, leases))` for a request A sees:
/// A's `take`; the program holds each of the request's leases again.
pub proof fn lemma_ring_take<L>(
    pre: Ring<L>,
    post: Ring<L>,
    seq: int,
    outcome: Outcome,
    leases: L,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
) -> (a2: Abs::State)
    requires
        placed(pre.view(), a, q, reqs, held),
        pre.req(seq) == Some(
            ReqView { phase: Phase::Complete(outcome), waker: None, detached: false, leases },
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
        abs_step(a, a2, Abs::Step::take(reqs[seq], outcome)),
        placed(post.view(), a2, q, reqs, held),
        a2.wakes == a.wakes,
        forall|k: LeaseId| #[trigger] held(seq, leases).contains_key(k)
            ==> a2.is_program(k) && a2.leases[k].same_as(a.leases[k]),
{
    let a2 = lemma_take(pre.view(), a, q, reqs, held, seq, outcome, leases);
    assert(wf_at(pre.view(), seq));
    lemma_same(without(pre.view(), seq), post.view(), a2, q, reqs, held);
    a2
}

/// `take(seq)` returning `Some((outcome, leases))` for a refused request:
/// no step; the program, which A says held the leases all along, has them in
/// its hands again.
pub proof fn lemma_ring_take_refused<L>(
    pre: Ring<L>,
    post: Ring<L>,
    seq: int,
    leases: L,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
)
    requires
        placed(pre.view(), a, q, reqs, held),
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
        placed(post.view(), a, q, reqs, held),
        a.may_post(held(seq, leases)),
{
    lemma_take_refused(pre.view(), a, q, reqs, held, seq);
    lemma_same(without_refused(pre.view(), seq), post.view(), a, q, reqs, held);
}

/// `detach(seq)`: A's `detach`, `drop` of the leases of a refused request, or no step.
pub proof fn lemma_ring_detach<L>(
    pre: Ring<L>,
    post: Ring<L>,
    seq: int,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
) -> (a2: Abs::State)
    requires
        placed(pre.view(), a, q, reqs, held),
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
        abs_reaches(a, a2),
        placed(post.view(), a2, q, reqs, held),
        a2.wakes == a.wakes,
{
    let a2 = lemma_detach(pre.view(), a, q, reqs, held, seq);
    lemma_same(detached(pre.view(), seq), post.view(), a2, q, reqs, held);
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
    reqs: Map<int, ReqId>,
    held: Held<L>,
) -> (a2: Abs::State)
    requires
        placed(pre.view(), a, q, reqs, held),
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
        abs_reaches(a, a2),
        placed(post.view(), a2, q, reqs, held),
        a2.wakes == a.wakes,
{
    let a2 = lemma_register(pre.view(), a, q, reqs, held, seq, wk);
    lemma_same(registered(pre.view(), seq, wk), post.view(), a2, q, reqs, held);
    a2
}

/// `release_all`: A's `complete` (a flush) of every request in flight and
/// `detach` of every owned one, `drop` of every lease of each staged or refused
/// one. Nothing of the ring's is left in A, so its queue pair may go (`drop_qp`).
pub proof fn lemma_ring_release_all<L>(
    pre: Ring<L>,
    post: Ring<L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
) -> (a2: Abs::State)
    requires
        pre.inv(),
        placed(pre.view(), a, q, reqs, held),
        forall|s: int| #[trigger] post.req(s) is None,
        forall|s: int| #[trigger] post.staged(s) is None,
        forall|s: int| #[trigger] post.refused(s) is None,
    ensures
        abs_reaches(a, a2),
        placed(post.view(), a2, q, reqs, held),
{
    pre.lemma_view();
    let (h, c, p) = (pre.spec_head() as int, pre.spec_committed() as int, pre.spec_posted() as int);
    let a1 = lemma_release(pre.view(), a, q, reqs, held, h, c);
    let v1 = without_range(pre.view(), h, c);
    assert forall|s: int| c <= s < p implies #[trigger] (v1.staged)(s) is Some by {}
    let a2 = lemma_drop_staged(v1, a1, q, reqs, held, c, p);
    lemma_same(dropped_range(v1, c, p), post.view(), a2, q, reqs, held);
    lemma_abs_reaches_trans(a, a1, a2);
    a2
}

// =============================================================================
// Results moved out of the ring
//
// `safeverbs` moves results nobody has taken yet out of a clogged ring into a
// side list, where their futures find them. A still sees each such result
// as a complete request with an owner (`side`), or, for a refused request,
// as leases of the program (`refused`): moving it is no step, and taking it from
// the side list (or dropping its future) is A's `take` (or `detach`), with
// exactly the outcome and leases the ring gave, or no step (or `drop`) for a
// refused one.

/// `v` with the side list: by sequence number, the outcome and leases of
/// complete requests moved out of the ring (`side`), each a complete request
/// with an owner in A, and the leases of refused requests moved out
/// (`refused`), each leases of the program in A.
pub open spec fn with_side<L>(v: View<L>, side: Map<int, (Outcome, L)>, refused: Map<int, L>) -> View<L> {
    View {
        req: |s: int|
            if side.contains_key(s) {
                Some(
                    ReqView {
                        phase: Phase::Complete(side[s].0),
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
    outcome: Outcome,
    leases: L,
    side: Map<int, (Outcome, L)>,
    refused: Map<int, L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
)
    requires
        placed(with_side(pre.view(), side, refused), a, q, reqs, held),
        !side.contains_key(seq),
        !refused.contains_key(seq),
        pre.req(seq) == Some(
            ReqView { phase: Phase::Complete(outcome), waker: None, detached: false, leases },
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
        placed(with_side(post.view(), side.insert(seq, (outcome, leases)), refused), a, q, reqs, held),
{
    lemma_same(
        with_side(pre.view(), side, refused),
        with_side(post.view(), side.insert(seq, (outcome, leases)), refused),
        a,
        q,
        reqs,
        held,
    );
}

/// Moving a refused request's leases, as `take` returned them, from the
/// ring to the side list is no step of A.
pub proof fn lemma_side_move_refused<L>(
    pre: Ring<L>,
    post: Ring<L>,
    seq: int,
    leases: L,
    side: Map<int, (Outcome, L)>,
    refused: Map<int, L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
)
    requires
        placed(with_side(pre.view(), side, refused), a, q, reqs, held),
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
        placed(with_side(post.view(), side, refused.insert(seq, leases)), a, q, reqs, held),
{
    lemma_same(
        with_side(pre.view(), side, refused),
        with_side(post.view(), side, refused.insert(seq, leases)),
        a,
        q,
        reqs,
        held,
    );
}

/// Taking a result from the side list is A's `take`: the outcome and the
/// leases the ring gave, the program holding each of them again.
pub proof fn lemma_side_take<L>(
    v: View<L>,
    seq: int,
    side: Map<int, (Outcome, L)>,
    refused: Map<int, L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
) -> (a2: Abs::State)
    requires
        placed(with_side(v, side, refused), a, q, reqs, held),
        side.contains_key(seq),
        (v.req)(seq) is None,
    ensures
        abs_step(a, a2, Abs::Step::take(reqs[seq], side[seq].0)),
        placed(with_side(v, side.remove(seq), refused), a2, q, reqs, held),
        forall|k: LeaseId| #[trigger] held(seq, side[seq].1).contains_key(k)
            ==> a2.is_program(k) && a2.leases[k].same_as(a.leases[k]),
{
    let w = with_side(v, side, refused);
    let a2 = lemma_take(w, a, q, reqs, held, seq, side[seq].0, side[seq].1);
    lemma_same(without(w, seq), with_side(v, side.remove(seq), refused), a2, q, reqs, held);
    a2
}

/// Taking a refused request's leases from the side list is no step of A.
pub proof fn lemma_side_take_refused<L>(
    v: View<L>,
    seq: int,
    side: Map<int, (Outcome, L)>,
    refused: Map<int, L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
)
    requires
        placed(with_side(v, side, refused), a, q, reqs, held),
        refused.contains_key(seq),
        (v.refused)(seq) is None,
    ensures
        placed(with_side(v, side, refused.remove(seq)), a, q, reqs, held),
        a.may_post(held(seq, refused[seq])),
{
    let w = with_side(v, side, refused);
    lemma_take_refused(w, a, q, reqs, held, seq);
    lemma_same(without_refused(w, seq), with_side(v, side, refused.remove(seq)), a, q, reqs, held);
}

/// Dropping the future of a result in the side list is A's `detach`, which
/// releases its leases.
pub proof fn lemma_side_detach<L>(
    v: View<L>,
    seq: int,
    side: Map<int, (Outcome, L)>,
    refused: Map<int, L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
) -> (a2: Abs::State)
    requires
        placed(with_side(v, side, refused), a, q, reqs, held),
        side.contains_key(seq),
        (v.req)(seq) is None,
    ensures
        abs_reaches(a, a2),
        placed(with_side(v, side.remove(seq), refused), a2, q, reqs, held),
{
    let w = with_side(v, side, refused);
    let a2 = lemma_detach(w, a, q, reqs, held, seq);
    assert(wf_at(w, seq));
    lemma_same(detached(w, seq), with_side(v, side.remove(seq), refused), a2, q, reqs, held);
    a2
}

/// Dropping the owner of a refused result in the side list is A's `drop` of
/// each of its leases.
pub proof fn lemma_side_detach_refused<L>(
    v: View<L>,
    seq: int,
    side: Map<int, (Outcome, L)>,
    refused: Map<int, L>,
    a: Abs::State,
    q: QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
) -> (a2: Abs::State)
    requires
        placed(with_side(v, side, refused), a, q, reqs, held),
        refused.contains_key(seq),
        (v.refused)(seq) is None,
    ensures
        abs_reaches(a, a2),
        placed(with_side(v, side, refused.remove(seq)), a2, q, reqs, held),
{
    let w = with_side(v, side, refused);
    let a2 = lemma_drop_refused(w, a, q, reqs, held, seq);
    lemma_same(without_refused(w, seq), with_side(v, side, refused.remove(seq)), a2, q, reqs, held);
    a2
}

// =============================================================================
// Examples: the adapters' preconditions can be met.

/// A view with no request.
spec fn no_requests<L>() -> View<L> {
    View { req: |s: int| None, staged: |s: int| None, refused: |s: int| None }
}

/// A ring with no request sits in any state of A, whatever its values stand for.
proof fn lemma_no_requests_placed<L>(a: Abs::State, q: QpId, held: Held<L>)
    requires
        abs_inv(a),
        a.qps.contains(q),
    ensures
        placed(no_requests::<L>(), a, q, Map::empty(), held),
{
    let (v, reqs) = (no_requests::<L>(), Map::<int, ReqId>::empty());
    assert forall|s: int| #[trigger] wf_at(v, s) by {}
    assert forall|s: int| #[trigger] sits(v, a, q, reqs, held, s) by {}
}

/// A with the program holding a unique READ destination, a shared source and a
/// clone of that source, on queue pair 7.
spec fn source_and_destination() -> Abs::State {
    let dst = safeverbs_spec::types::Span { start: 128, len: 64 }.bytes();
    let src = safeverbs_spec::types::Span { start: 0, len: 64 }.bytes();
    let source = Lease { mem: src, unique: false, any_bytes: true, holder: Holder::Program };
    Abs::State {
        qps: Set::empty().insert(7),
        leases: Map::empty()
            .insert(0, Lease { mem: dst, unique: true, any_bytes: true, holder: Holder::Program })
            .insert(1, source)
            .insert(2, source),
        requests: Map::empty(),
        next_id: 3,
        outcomes: Map::empty(),
        wakes: Seq::empty(),
    }
}

/// A request with a READ destination and a shared source, of which the program
/// keeps a clone. `L` is the leases themselves. Posted and given a waker: the NIC writes the
/// one and reads the other. Completed and taken: each comes back. Or detached: it
/// stays in flight, and completing releases it.
#[verifier::spinoff_prover]
#[verifier::rlimit(20)]
pub proof fn example_request_of_two_leases() {
    let a0 = source_and_destination();
    assert(abs_inv(a0));
    let held = |_s: int, dirs: Map<LeaseId, Dir>| dirs;
    let dirs = Map::empty().insert(0nat, Dir::In).insert(1nat, Dir::Out);
    let reqs0 = Map::<int, ReqId>::empty();
    let v0 = no_requests::<Map<LeaseId, Dir>>();
    lemma_no_requests_placed(a0, 7, held);
    assert forall|t: int| #[trigger] leases_of(v0, held, t).dom().disjoint(dirs.dom()) by {}
    lemma_end(v0, a0, 7, reqs0, held, 0, false, dirs);
    let staged = staged_with(v0, 0, false, dirs);
    let a1 = lemma_post_one(staged, a0, 7, reqs0, held, 0);
    let in_flight = posted(staged, 0);
    let reqs = reqs0.insert(0, a0.next_id);
    let r = a0.next_id;
    let a2 = lemma_register(in_flight, a1, 7, reqs, held, 0, 9);
    let waiting = registered(in_flight, 0, 9);
    assert(sits(waiting, a2, 7, reqs, held, 0));
    assert(a2.in_flight(r) && a2.requests[r].waker == Some(9));
    assert(dirs.contains_key(0) && a2.place(0) == Place::InFlight(r));
    assert(dirs.contains_key(1) && a2.place(1) == Place::InFlight(r));

    // Taken: each lease comes back, under its own name, the waker having run.
    let flush = flushed();
    lemma_complete_one(waiting, a2, 7, reqs, held, 0, flush);
    let done = complete_state(a2, r, flush);
    let completed = completed(waiting, 0, flush);
    let taken = lemma_take(completed, done, 7, reqs, held, 0, flush, dirs);
    assert(taken.is_program(0) && taken.is_program(1));
    assert(taken.wakes == seq![9nat]);

    // Detached: still in flight with its leases, until it completes.
    let detached_state = lemma_detach(waiting, a2, 7, reqs, held, 0);
    let detached_view = detached(waiting, 0);
    assert(sits(detached_view, detached_state, 7, reqs, held, 0));
    assert(detached_state.place(0) == Place::InFlight(r));
    lemma_complete_one(detached_view, detached_state, 7, reqs, held, 0, flush);
    let released = complete_state(detached_state, r, flush);
    assert(released.place(0) is Released && released.place(1) is Released);
}

/// A doorbell refused: no request of A ever appears, and each lease stays the program's
/// from `end` to the owner's `take`, or until a detached request's leases are dropped.
#[verifier::spinoff_prover]
#[verifier::rlimit(20)]
pub proof fn example_refused() {
    let a0 = source_and_destination();
    assert(abs_inv(a0));
    let held = |_s: int, dirs: Map<LeaseId, Dir>| dirs;
    let owned = Map::empty().insert(0nat, Dir::In);
    let unowned = Map::empty().insert(1nat, Dir::Out);
    let reqs = Map::<int, ReqId>::empty();
    let v0 = no_requests::<Map<LeaseId, Dir>>();
    lemma_no_requests_placed(a0, 7, held);
    assert forall|t: int| #[trigger] leases_of(v0, held, t).dom().disjoint(owned.dom()) by {}
    lemma_end(v0, a0, 7, reqs, held, 0, false, owned);
    let v1 = staged_with(v0, 0, false, owned);
    assert forall|t: int| #[trigger] leases_of(v1, held, t).dom().disjoint(unowned.dom()) by {
        assert(t != 0 ==> leases_of(v1, held, t) == Map::<LeaseId, Dir>::empty());
    }
    lemma_end(v1, a0, 7, reqs, held, 1, true, unowned);
    let v2 = staged_with(v1, 1, true, unowned);

    // Refused: the one with an owner is kept for it, the other's lease is dropped.
    let a1 = lemma_abort(v2, a0, 7, reqs, held, 0, 2, -1);
    let v3 = refused_range(v2, 0, 2, -1);
    assert((v3.refused)(0) == Some(owned) && (v3.staged)(0) is None);
    assert((v3.refused)(1) is None && (v3.staged)(1) is None);

    // Its owner takes it: no step, and the program never gave the lease up.
    lemma_take_refused(v3, a1, 7, reqs, held, 0);
    assert(owned.contains_key(0) && a1.is_program(0));
    assert(a1.requests =~= a0.requests && a1.outcomes =~= a0.outcomes);
}

/// Equal shared values in different requests stand for different leases: the
/// interpretation depends on the request's sequence number.
#[verifier::spinoff_prover]
pub proof fn example_equal_values_different_leases() {
    let a0 = source_and_destination();
    assert(abs_inv(a0));
    let shared = 64nat;
    let held = |s: int, _len: nat| Map::empty().insert(if s == 0 { 1nat } else { 2nat }, Dir::Out);
    let reqs = Map::<int, ReqId>::empty();
    let v0 = no_requests::<nat>();
    lemma_no_requests_placed(a0, 7, held);
    assert forall|t: int| #[trigger] leases_of(v0, held, t).dom().disjoint(held(0, shared).dom()) by {}
    lemma_end(v0, a0, 7, reqs, held, 0, false, shared);
    let v1 = staged_with(v0, 0, false, shared);
    assert forall|t: int| #[trigger] leases_of(v1, held, t).dom().disjoint(held(1, shared).dom()) by {
        if t != 0 {
            assert(leases_of(v1, held, t) == Map::<LeaseId, Dir>::empty());
        }
    }
    lemma_end(v1, a0, 7, reqs, held, 1, false, shared);
    let v2 = staged_with(v1, 1, false, shared);
    assert((v2.staged)(0)->0.leases == (v2.staged)(1)->0.leases);
    let (a1, reqs1) = lemma_commit(v2, a0, 7, reqs, held, 0, 2);
    let v3 = posted_range(v2, 0, 2);
    assert(sits(v3, a1, 7, reqs1, held, 0) && sits(v3, a1, 7, reqs1, held, 1));
    assert(held(0, shared).contains_key(1) && held(1, shared).contains_key(2));
    assert(a1.leases[1].held_by(reqs1[0]) && a1.leases[2].held_by(reqs1[1]));
    assert(reqs1[0] != reqs1[1]);
}

} // verus!
