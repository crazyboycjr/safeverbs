//! The steps of the abstract machine A (`safeverbs_spec::abs`) behind an engine's requests.
//!
//! What A sees of an engine is a [`View`]: its requests by key (`req`: posted to the NIC and not
//! freed), those written and not posted (`staged`), whose leases are still the program's, and those
//! whose doorbell was refused (`refused`), which never reached A. This module proves, one request at
//! a time, that each change of what A sees is zero or more steps of A, from any state of A in which
//! the view sits (`placed`) to one in which it sits again. The engine (`crate::hw::refines`) makes
//! one view of all its rings, so these lemmas leave the other rings alone by construction.
//!
//! | What changes of a request | Steps of A |
//! | --- | --- |
//! | it is staged | none: a staged request's leases are still the program's (`lemma_end`) |
//! | it is posted | `post_request`, then its `register` or `detach` (`lemma_post_one`) |
//! | its doorbell is refused | `drop` of each lease of one without an owner; none for the others, whose leases wait for their owners or go back (`lemma_abort`) |
//! | M retired it | `complete` (`lemma_complete_one`) |
//! | its owner takes it | `take`, and `drop` of its shared leases (`lemma_take`); of a refused one, those `drop`s alone (`lemma_forget`) |
//! | its owner gives it up | `detach`, none for a staged one, `drop` of each lease of a refused one (`lemma_detach`) |
//! | a future registers a waker | `register`, none for a staged one (`lemma_register`) |
//! | its queue pair goes, nothing in flight | `detach` (`lemma_release_one`), and `drop` of each lease of a refused one |
//!
//! A request whose doorbell was refused never reached A: its leases stay the program's, and the
//! ring only keeps them for the owner, who takes them back with a refusal or drops them. A shared
//! lease is the request's alone (`share` makes it when the request is written): it has no way back
//! to the program, so it goes with the request. A unique one comes back under its own name.
//!
//! Everything here is ghost code: a plain build erases it.
// The imports are used by ghost code only.
#![allow(unused_imports)]

use safeverbs_spec::abs::*;
use safeverbs_spec::refinement::*;
use safeverbs_spec::types::{Outcome, QpId, Status};
use vstd::prelude::*;

use crate::ring::{ReqView, Ring};

verus! {

/// A lease a request holds, as the request knows it: how the NIC touches it, and its bytes.
pub ghost struct Hold {
    /// How the NIC touches it.
    pub dir: Dir,
    /// Its bytes.
    pub mem: ISet<int>,
}

/// The leases of A a request holds, by name.
pub type Holds = Map<LeaseId, Hold>;

/// Which leases of A a request's value `L` stands for, given its key.
pub type Held<L> = spec_fn(int, L) -> Holds;

/// The directions of `holds`, as A's `post_request` takes them.
pub open spec fn dirs_of(holds: Holds) -> Map<LeaseId, Dir> {
    holds.map_values(|h: Hold| h.dir)
}

/// What A sees of a set of requests, by key.
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
pub open spec fn leases_of<L>(v: View<L>, held: Held<L>, s: int) -> Holds {
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

/// Request `r` of `a` holds exactly the leases `h`, each in its direction, with its bytes.
pub open spec fn holds_exactly(a: Abs::State, r: ReqId, h: Holds) -> bool {
    &&& forall|l: LeaseId| #[trigger] h.contains_key(l) ==> a.leases.contains_key(l)
        && a.leases[l].holder == (Holder::Request { req: r, dir: h[l].dir })
        && a.leases[l].mem == h[l].mem
    &&& forall|l: LeaseId| #[trigger] a.leases.contains_key(l) && a.leases[l].held_by(r)
        ==> h.contains_key(l)
}

/// The program holds the leases `h`, with their bytes, and may post them in their directions.
pub open spec fn may_post_holds(a: Abs::State, h: Holds) -> bool {
    forall|l: LeaseId| #[trigger] h.contains_key(l) ==> {
        &&& a.is_program(l)
        &&& h[l].dir is In ==> a.leases[l].writable()
        &&& a.leases[l].mem == h[l].mem
    }
}

/// Request `s` of `v` sits in `a`: a posted one is request `reqs[s]` of `a`, on its queue pair, with
/// the same phase, waker and owner, and holds exactly its leases; the leases of a staged or
/// refused one are the program's, and may be posted in their directions, a staged one on a queue
/// pair that exists.
pub open spec fn sits<L>(
    v: View<L>,
    a: Abs::State,
    q: spec_fn(L) -> QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
) -> bool {
    &&& (v.req)(s) is Some ==> {
        &&& reqs.contains_key(s)
        &&& a.requests.contains_key(reqs[s])
        &&& a.requests[reqs[s]] == as_req((v.req)(s)->0, q((v.req)(s)->0.leases))
        &&& holds_exactly(a, reqs[s], held(s, (v.req)(s)->0.leases))
    }
    &&& (v.staged)(s) is Some ==> {
        &&& may_post_holds(a, held(s, (v.staged)(s)->0.leases))
        &&& a.qps.contains(q((v.staged)(s)->0.leases))
    }
    &&& (v.refused)(s) is Some ==> may_post_holds(a, held(s, (v.refused)(s)->0))
}

/// Request `r` of A is one of `v`.
pub open spec fn names_request<L>(v: View<L>, reqs: Map<int, ReqId>, r: ReqId) -> bool {
    exists|s: int| #[trigger] (v.req)(s) is Some && reqs[s] == r
}

/// The requests of `v` sit in `a` and are all it has, and no two hold the same lease or are the
/// same request of `a`.
pub open spec fn placed<L>(
    v: View<L>,
    a: Abs::State,
    q: spec_fn(L) -> QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
) -> bool {
    &&& abs_inv(a)
    &&& wf(v)
    &&& forall|s: int| #[trigger] sits(v, a, q, reqs, held, s)
    &&& forall|s: int, t: int| #![trigger leases_of(v, held, s), leases_of(v, held, t)]
        s != t ==> leases_of(v, held, s).dom().disjoint(leases_of(v, held, t).dom())
    &&& forall|s: int, t: int| #![trigger reqs[s], reqs[t]]
        (v.req)(s) is Some && (v.req)(t) is Some && s != t ==> reqs[s] != reqs[t]
    &&& forall|r: ReqId| #[trigger] a.requests.contains_key(r) ==> names_request(v, reqs, r)
}

/// `v` and `w` agree.
pub open spec fn same<L>(v: View<L>, w: View<L>) -> bool {
    &&& forall|s: int| #[trigger] (v.req)(s) == (w.req)(s)
    &&& forall|s: int| #[trigger] (v.staged)(s) == (w.staged)(s)
    &&& forall|s: int| #[trigger] (v.refused)(s) == (w.refused)(s)
}

/// The program holds lease `l` of `a`, and no request of `v` keeps it for its owner.
pub open spec fn free_lease<L>(v: View<L>, a: Abs::State, held: Held<L>, l: LeaseId) -> bool {
    &&& a.is_program(l)
    &&& forall|s: int| !(#[trigger] leases_of(v, held, s).contains_key(l))
}

/// What the program holds free in `a` it still holds free in `a2`, as it was.
pub open spec fn kept_free<L>(v: View<L>, a: Abs::State, w: View<L>, a2: Abs::State, held: Held<L>) -> bool {
    forall|l: LeaseId| #[trigger] free_lease(v, a, held, l)
        ==> free_lease(w, a2, held, l) && a2.leases[l] == a.leases[l]
}

/// `a2` has the queue pairs and outcomes of `a`, and no request it did not have, each as it was as
/// far as its queue pair and its being in flight go.
pub open spec fn kept_requests(a: Abs::State, a2: Abs::State) -> bool {
    &&& a2.qps == a.qps
    &&& a2.outcomes == a.outcomes
    &&& a2.next_id == a.next_id
    &&& forall|r: ReqId| #[trigger] a2.requests.contains_key(r) ==> {
        &&& a.requests.contains_key(r)
        &&& a2.requests[r].qp == a.requests[r].qp
        &&& a2.in_flight(r) == a.in_flight(r)
    }
}

/// The shared ones among the leases `h`, in `a`.
pub open spec fn shared_in(a: Abs::State, h: Holds) -> Set<LeaseId> {
    h.dom().filter(|l: LeaseId| !a.leases[l].unique)
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

/// A's state after `share` of the shared lease `l`, for the bytes `sub`.
pub open spec fn share_state(a: Abs::State, l: LeaseId, sub: ISet<int>) -> Abs::State {
    Abs::State {
        leases: a.leases.insert(a.next_id, Lease { mem: sub, ..a.leases[l] }),
        next_id: a.next_id + 1,
        ..a
    }
}

/// One step of A keeps its invariants and is an execution.
pub proof fn lemma_step_one(a: Abs::State, a2: Abs::State, step: Abs::Step)
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
pub proof fn lemma_drop_leases(a: Abs::State, ls: Set<LeaseId>) -> (a2: Abs::State)
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
// Placement

/// Placement carries over to a view that agrees.
pub proof fn lemma_same<L>(
    v: View<L>,
    w: View<L>,
    a: Abs::State,
    q: spec_fn(L) -> QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
)
    requires
        placed(v, a, q, reqs, held),
        same(v, w),
    ensures
        placed(w, a, q, reqs, held),
        kept_free(v, a, w, a, held),
{
    assert forall|s: int| #[trigger] wf_at(w, s) by {
        assert(wf_at(v, s));
        assert((v.req)(s) == (w.req)(s) && (v.staged)(s) == (w.staged)(s) && (v.refused)(s) == (w.refused)(s));
    }
    assert forall|s: int| #[trigger] sits(w, a, q, reqs, held, s) by {
        assert(sits(v, a, q, reqs, held, s));
        assert((v.req)(s) == (w.req)(s) && (v.staged)(s) == (w.staged)(s) && (v.refused)(s) == (w.refused)(s));
    }
    assert forall|s: int| #[trigger] leases_of(w, held, s) == leases_of(v, held, s) by {
        assert((v.req)(s) == (w.req)(s) && (v.staged)(s) == (w.staged)(s) && (v.refused)(s) == (w.refused)(s));
    }
    assert forall|s: int, t: int| #![trigger leases_of(w, held, s), leases_of(w, held, t)]
        s != t implies leases_of(w, held, s).dom().disjoint(leases_of(w, held, t).dom()) by {
        assert(leases_of(w, held, s) == leases_of(v, held, s));
        assert(leases_of(w, held, t) == leases_of(v, held, t));
        assert(leases_of(v, held, s).dom().disjoint(leases_of(v, held, t).dom()));
    }
    assert forall|s: int, t: int| #![trigger reqs[s], reqs[t]]
        (w.req)(s) is Some && (w.req)(t) is Some && s != t implies reqs[s] != reqs[t] by {
        assert((v.req)(s) == (w.req)(s) && (v.req)(t) == (w.req)(t));
    }
    assert forall|r: ReqId| #[trigger] a.requests.contains_key(r) implies names_request(w, reqs, r) by {
        assert(names_request(v, reqs, r));
        let s = choose|s: int| #[trigger] (v.req)(s) is Some && reqs[s] == r;
        assert((v.req)(s) == (w.req)(s));
    }
    assert forall|l: LeaseId| #[trigger] free_lease(v, a, held, l)
        implies free_lease(w, a, held, l) && a.leases[l] == a.leases[l] by {
        assert forall|s: int| !(#[trigger] leases_of(w, held, s).contains_key(l)) by {
            assert(leases_of(w, held, s) == leases_of(v, held, s));
            assert(!leases_of(v, held, s).contains_key(l));
        }
    }
}

/// `w` is placed in `a2` when it agrees with `v`, placed in `a`, but at request
/// `s`, which sits in `a2`, and `a2` keeps what `a` has but request `r` and the
/// leases of `s`, which request `r` or the program holds if they are left.
pub proof fn lemma_placed_but<L>(
    v: View<L>,
    w: View<L>,
    a: Abs::State,
    a2: Abs::State,
    q: spec_fn(L) -> QpId,
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
        forall|t: ReqId| #[trigger] a2.requests.contains_key(t) && t != r ==> a.requests.contains_key(t),
        a2.requests.contains_key(r) ==> (w.req)(s) is Some && reqs2[s] == r,
        (r == a.next_id && (v.req)(s) is None) || ((v.req)(s) is Some && r == reqs[s]),
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
        kept_free(v, a, w, a2, held),
{
    assert forall|t: int| t != s && #[trigger] (v.req)(t) is Some implies reqs[t] != r by {
        assert(sits(v, a, q, reqs, held, t));
        assert(a.requests.contains_key(reqs[t]));
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
                let h = held(t, (v.req)(t)->0.leases);
                assert(h == leases_of(v, held, t));
                assert(reqs2.contains_key(t) && reqs2[t] == reqs[t]);
                assert forall|l: LeaseId| #[trigger] h.contains_key(l) implies a2.leases.contains_key(l)
                    && a2.leases[l].holder == (Holder::Request { req: reqs[t], dir: h[l].dir })
                    && a2.leases[l].mem == h[l].mem by {
                    assert(!leases_of(v, held, s).dom().contains(l));
                }
                assert forall|l: LeaseId| #[trigger] a2.leases.contains_key(l) && a2.leases[l].held_by(reqs[t])
                    implies h.contains_key(l) by {
                    assert(a.leases.contains_key(l));
                    assert(reqs[t] != r);
                }
            }
            if (v.staged)(t) is Some {
                let h = held(t, (v.staged)(t)->0.leases);
                assert(h == leases_of(v, held, t));
                assert forall|l: LeaseId| #[trigger] h.contains_key(l) implies {
                    &&& a2.is_program(l)
                    &&& h[l].dir is In ==> a2.leases[l].writable()
                    &&& a2.leases[l].mem == h[l].mem
                } by {
                    assert(!leases_of(v, held, s).dom().contains(l));
                }
            }
            if (v.refused)(t) is Some {
                let h = held(t, (v.refused)(t)->0);
                assert(h == leases_of(v, held, t));
                assert forall|l: LeaseId| #[trigger] h.contains_key(l) implies {
                    &&& a2.is_program(l)
                    &&& h[l].dir is In ==> a2.leases[l].writable()
                    &&& a2.leases[l].mem == h[l].mem
                } by {
                    assert(!leases_of(v, held, s).dom().contains(l));
                }
            }
        }
    }
    assert forall|t: int| t != s implies #[trigger] leases_of(w, held, t) == leases_of(v, held, t) by {
        assert((w.req)(t) == (v.req)(t) && (w.staged)(t) == (v.staged)(t) && (w.refused)(t) == (v.refused)(t));
    }
    assert forall|t: int, u: int| #![trigger leases_of(w, held, t), leases_of(w, held, u)]
        t != u implies leases_of(w, held, t).dom().disjoint(leases_of(w, held, u).dom()) by {
        assert(leases_of(v, held, t).dom().disjoint(leases_of(v, held, u).dom()));
        if t != s {
            assert(leases_of(w, held, t) == leases_of(v, held, t));
        }
        if u != s {
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
    assert forall|t: ReqId| #[trigger] a2.requests.contains_key(t) implies names_request(w, reqs2, t) by {
        if t == r {
            assert((w.req)(s) is Some && reqs2[s] == r);
        } else {
            assert(a.requests.contains_key(t));
            assert(names_request(v, reqs, t));
            let u = choose|u: int| #[trigger] (v.req)(u) is Some && reqs[u] == t;
            assert(sits(v, a, q, reqs, held, u));
            // `s`, if it was posted, is request `r`.
            assert(u != s);
            assert((w.req)(u) == (v.req)(u) && reqs2[u] == reqs[u]);
        }
    }
    assert forall|l: LeaseId| #[trigger] free_lease(v, a, held, l)
        implies free_lease(w, a2, held, l) && a2.leases[l] == a.leases[l] by {
        assert(!leases_of(v, held, s).contains_key(l));
        assert forall|t: int| !(#[trigger] leases_of(w, held, t).contains_key(l)) by {
            assert(!leases_of(v, held, t).contains_key(l));
            if t != s {
                assert(leases_of(w, held, t) == leases_of(v, held, t));
            } else {
                assert(!leases_of(v, held, s).dom().contains(l));
            }
        }
    }
}

/// A lease a request of `v` holds or keeps is one `a` has.
pub proof fn lemma_held_exists<L>(
    v: View<L>,
    a: Abs::State,
    q: spec_fn(L) -> QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
    l: LeaseId,
)
    requires
        placed(v, a, q, reqs, held),
        leases_of(v, held, s).contains_key(l),
    ensures
        a.leases.contains_key(l),
{
    assert(sits(v, a, q, reqs, held, s));
}

/// `v` stays placed through a change of A that leaves its requests, and every lease of its own, as
/// they were: a lease of the program changes, or a queue pair no request is on.
pub proof fn lemma_placed_apart<L>(
    v: View<L>,
    a: Abs::State,
    a2: Abs::State,
    q: spec_fn(L) -> QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
)
    requires
        placed(v, a, q, reqs, held),
        abs_inv(a2),
        a2.requests == a.requests,
        forall|s: int, l: LeaseId| #[trigger] leases_of(v, held, s).contains_key(l)
            ==> a2.leases.contains_key(l) && a2.leases[l] == a.leases[l],
        forall|l: LeaseId| #[trigger] a2.leases.contains_key(l) && a2.leases[l].holder is Request
            ==> a.leases.contains_key(l) && a2.leases[l] == a.leases[l],
        forall|s: int| #[trigger] (v.staged)(s) is Some ==> a2.qps.contains(q((v.staged)(s)->0.leases)),
    ensures
        placed(v, a2, q, reqs, held),
{
    assert forall|s: int| #[trigger] sits(v, a2, q, reqs, held, s) by {
        assert(sits(v, a, q, reqs, held, s));
        assert(wf_at(v, s));
        if (v.req)(s) is Some {
            let h = held(s, (v.req)(s)->0.leases);
            assert(h == leases_of(v, held, s));
            assert forall|l: LeaseId| #[trigger] h.contains_key(l) implies a2.leases.contains_key(l)
                && a2.leases[l].holder == (Holder::Request { req: reqs[s], dir: h[l].dir })
                && a2.leases[l].mem == h[l].mem by {
                assert(leases_of(v, held, s).contains_key(l));
            }
        }
        if (v.staged)(s) is Some {
            let h = held(s, (v.staged)(s)->0.leases);
            assert(h == leases_of(v, held, s));
            assert forall|l: LeaseId| #[trigger] h.contains_key(l) implies {
                &&& a2.is_program(l)
                &&& h[l].dir is In ==> a2.leases[l].writable()
                &&& a2.leases[l].mem == h[l].mem
            } by {
                assert(leases_of(v, held, s).contains_key(l));
            }
        }
        if (v.refused)(s) is Some {
            let h = held(s, (v.refused)(s)->0);
            assert(h == leases_of(v, held, s));
            assert forall|l: LeaseId| #[trigger] h.contains_key(l) implies {
                &&& a2.is_program(l)
                &&& h[l].dir is In ==> a2.leases[l].writable()
                &&& a2.leases[l].mem == h[l].mem
            } by {
                assert(leases_of(v, held, s).contains_key(l));
            }
        }
    }
    assert forall|r: ReqId| #[trigger] a2.requests.contains_key(r) implies names_request(v, reqs, r) by {
        assert(a.requests.contains_key(r));
    }
}

/// `w` is placed in `a2`: it agrees with `v` but at request `s`, of which it has no posted request,
/// nor `v` had, and the leases `gone` of the program, which only `s` kept, are dropped; what is left
/// to `s` of its leases is the program's.
pub proof fn lemma_forget<L>(
    v: View<L>,
    w: View<L>,
    a: Abs::State,
    q: spec_fn(L) -> QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
    gone: Set<LeaseId>,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
        forall|t: int| t != s ==> #[trigger] (w.req)(t) == (v.req)(t),
        forall|t: int| t != s ==> #[trigger] (w.staged)(t) == (v.staged)(t),
        forall|t: int| t != s ==> #[trigger] (w.refused)(t) == (v.refused)(t),
        (v.req)(s) is None,
        (w.req)(s) is None,
        wf_at(w, s),
        (w.staged)(s) is Some ==> a.qps.contains(q((w.staged)(s)->0.leases)),
        (w.staged)(s) is Some ==> leases_of(w, held, s) == leases_of(v, held, s),
        (w.refused)(s) is Some ==> leases_of(w, held, s) == leases_of(v, held, s),
        gone.subset_of(leases_of(v, held, s).dom()),
        (w.staged)(s) is Some || (w.refused)(s) is Some ==> gone == Set::<LeaseId>::empty(),
    ensures
        abs_reaches(a, a2),
        placed(w, a2, q, reqs, held),
        kept_free(v, a, w, a2, held),
        kept_requests(a, a2),
        a2.requests == a.requests,
        forall|l: LeaseId| #[trigger] a.leases.contains_key(l) && !gone.contains(l)
            ==> a2.leases.contains_key(l) && a2.leases[l] == a.leases[l],
{
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    let h = leases_of(v, held, s);
    assert forall|l: LeaseId| #[trigger] gone.contains(l) implies a.is_program(l) by {
        assert(h.dom().contains(l));
        if (v.staged)(s) is Some {
            assert(h == held(s, (v.staged)(s)->0.leases));
        } else {
            assert(h == held(s, (v.refused)(s)->0));
        }
    }
    let a2 = lemma_drop_leases(a, gone);
    assert(sits(w, a2, q, reqs, held, s)) by {
        if (w.staged)(s) is Some {
            let g = held(s, (w.staged)(s)->0.leases);
            assert(g == h);
            assert forall|l: LeaseId| #[trigger] g.contains_key(l) implies {
                &&& a2.is_program(l)
                &&& g[l].dir is In ==> a2.leases[l].writable()
                &&& a2.leases[l].mem == g[l].mem
            } by {
                assert(!gone.contains(l));
                if (v.staged)(s) is Some {
                    assert(h == held(s, (v.staged)(s)->0.leases));
                } else {
                    assert(h == held(s, (v.refused)(s)->0));
                }
            }
        }
        if (w.refused)(s) is Some {
            let g = held(s, (w.refused)(s)->0);
            assert(g == h);
            assert forall|l: LeaseId| #[trigger] g.contains_key(l) implies {
                &&& a2.is_program(l)
                &&& g[l].dir is In ==> a2.leases[l].writable()
                &&& a2.leases[l].mem == g[l].mem
            } by {
                assert(!gone.contains(l));
                if (v.staged)(s) is Some {
                    assert(h == held(s, (v.staged)(s)->0.leases));
                } else {
                    assert(h == held(s, (v.refused)(s)->0));
                }
            }
        }
    }
    assert(leases_of(w, held, s).dom().subset_of(h.dom()));
    assert(!a.requests.contains_key(a.next_id));
    assert forall|l: LeaseId| #[trigger] a2.leases.contains_key(l) implies a.leases.contains_key(l)
        && (leases_of(v, held, s).contains_key(l) ==> a2.leases[l].holder is Program) by {
        if leases_of(v, held, s).contains_key(l) {
            if (v.staged)(s) is Some {
                assert(h == held(s, (v.staged)(s)->0.leases));
            } else {
                assert(h == held(s, (v.refused)(s)->0));
            }
        }
    }
    lemma_placed_but(v, w, a, a2, q, reqs, reqs, held, s, a.next_id);
    a2
}

/// `share` of a lease of the program: a new lease of the program, no request's.
pub proof fn lemma_share<L>(
    v: View<L>,
    a: Abs::State,
    q: spec_fn(L) -> QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    l: LeaseId,
    sub: ISet<int>,
)
    requires
        placed(v, a, q, reqs, held),
        a.is_program(l),
        !a.leases[l].unique,
        sub.subset_of(a.leases[l].mem),
    ensures
        abs_step(a, share_state(a, l, sub), Abs::Step::share(l, sub)),
        abs_reaches(a, share_state(a, l, sub)),
        placed(v, share_state(a, l, sub), q, reqs, held),
        free_lease(v, share_state(a, l, sub), held, a.next_id),
        forall|k: LeaseId| #[trigger] free_lease(v, a, held, k)
            ==> free_lease(v, share_state(a, l, sub), held, k) && share_state(a, l, sub).leases[k] == a.leases[k],
{
    let a2 = share_state(a, l, sub);
    assert(abs_step(a, a2, Abs::Step::share(l, sub)));
    lemma_step_one(a, a2, Abs::Step::share(l, sub));
    assert forall|s: int, k: LeaseId| #[trigger] leases_of(v, held, s).contains_key(k)
        implies a.leases.contains_key(k) by {
        assert(sits(v, a, q, reqs, held, s));
        assert(wf_at(v, s));
        if (v.req)(s) is Some {
            assert(held(s, (v.req)(s)->0.leases) == leases_of(v, held, s));
        } else if (v.staged)(s) is Some {
            assert(held(s, (v.staged)(s)->0.leases) == leases_of(v, held, s));
        } else {
            assert(held(s, (v.refused)(s)->0) == leases_of(v, held, s));
        }
    }
    assert forall|s: int| !(#[trigger] leases_of(v, held, s).contains_key(a.next_id)) by {
        if leases_of(v, held, s).contains_key(a.next_id) {
            assert(a.leases.contains_key(a.next_id));
        }
    }
    assert forall|s: int, k: LeaseId| #[trigger] leases_of(v, held, s).contains_key(k)
        implies a2.leases.contains_key(k) && #[trigger] a2.leases[k] == a.leases[k] by {
        assert(a.leases.contains_key(k));
        assert(k < a.next_id);
    }
    assert forall|s: int| #[trigger] (v.staged)(s) is Some implies a2.qps.contains(q((v.staged)(s)->0.leases)) by {
        assert(sits(v, a, q, reqs, held, s));
    }
    lemma_placed_apart(v, a, a2, q, reqs, held);
}

/// What is free stays free through two changes.
pub proof fn lemma_kept_free_trans<L>(
    v1: View<L>,
    a1: Abs::State,
    v2: View<L>,
    a2: Abs::State,
    v3: View<L>,
    a3: Abs::State,
    held: Held<L>,
)
    requires
        kept_free(v1, a1, v2, a2, held),
        kept_free(v2, a2, v3, a3, held),
    ensures
        kept_free(v1, a1, v3, a3, held),
{
    assert forall|l: LeaseId| #[trigger] free_lease(v1, a1, held, l)
        implies free_lease(v3, a3, held, l) && a3.leases[l] == a1.leases[l] by {
        assert(free_lease(v2, a2, held, l));
    }
}

/// `kept_requests` is transitive.
pub proof fn lemma_kept_requests_trans(a1: Abs::State, a2: Abs::State, a3: Abs::State)
    requires
        kept_requests(a1, a2),
        kept_requests(a2, a3),
    ensures
        kept_requests(a1, a3),
{
    assert forall|r: ReqId| #[trigger] a3.requests.contains_key(r) implies {
        &&& a1.requests.contains_key(r)
        &&& a3.requests[r].qp == a1.requests[r].qp
        &&& a3.in_flight(r) == a1.in_flight(r)
    } by {
        assert(a2.requests.contains_key(r));
    }
}

// =============================================================================
// Single steps

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
    q: spec_fn(L) -> QpId,
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
        abs_reaches(a, complete_state(a, reqs[s], o)),
        placed(completed(v, s, o), complete_state(a, reqs[s], o), q, reqs, held),
        kept_free(v, a, completed(v, s, o), complete_state(a, reqs[s], o), held),
{
    let r = reqs[s];
    let a2 = complete_state(a, r, o);
    let w = completed(v, s, o);
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    assert(abs_step(a, a2, Abs::Step::complete(r, o)));
    lemma_step_one(a, a2, Abs::Step::complete(r, o));
    let h = held(s, (v.req)(s)->0.leases);
    assert(h == leases_of(v, held, s));
    assert forall|l: LeaseId| #[trigger] a.leases.contains_key(l) && !leases_of(v, held, s).contains_key(l)
        implies a2.leases.contains_key(l) && a2.leases[l] == a.leases[l] by {
        assert(!a.leases[l].held_by(r));
    }
    assert(sits(w, a2, q, reqs, held, s)) by {
        if !(v.req)(s)->0.detached {
            assert forall|l: LeaseId| #[trigger] a2.leases.contains_key(l) && a2.leases[l].held_by(r)
                implies h.contains_key(l) by {
                assert(a.leases[l].held_by(r));
            }
        }
    }
    lemma_placed_but(v, w, a, a2, q, reqs, reqs, held, s, r);
}

/// The requests of `[from, to)` that `done` says in flight, each completed with its `o`.
pub open spec fn completed_where<L>(v: View<L>, from: int, to: int, done: spec_fn(int) -> bool, o: spec_fn(int) -> Outcome) -> View<L> {
    View {
        req: |t: int|
            if from <= t < to && done(t) && (v.req)(t) is Some {
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

/// Completing those requests is A's `complete` of each.
pub proof fn lemma_complete_where<L>(
    v: View<L>,
    a: Abs::State,
    q: spec_fn(L) -> QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    from: int,
    to: int,
    done: spec_fn(int) -> bool,
    o: spec_fn(int) -> Outcome,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
        from <= to,
        forall|s: int| from <= s < to && done(s) && #[trigger] (v.req)(s) is Some ==> (v.req)(s)->0.phase is InFlight,
    ensures
        abs_reaches(a, a2),
        placed(completed_where(v, from, to, done, o), a2, q, reqs, held),
        kept_free(v, a, completed_where(v, from, to, done, o), a2, held),
        a2.qps == a.qps,
        a2.next_id == a.next_id,
        forall|t: ReqId| #[trigger] a2.requests.contains_key(t) ==> a.requests.contains_key(t)
            && a2.requests[t].qp == a.requests[t].qp,
        forall|t: ReqId| #[trigger] a2.in_flight(t) ==> a.in_flight(t),
        forall|t: ReqId| #[trigger] a.outcomes.contains_key(t) ==> a2.outcomes.contains_key(t)
            && a2.outcomes[t] == a.outcomes[t],
        forall|t: ReqId| #[trigger] a2.outcomes.contains_key(t) && !a.outcomes.contains_key(t)
            ==> exists|s: int| from <= s < to && done(s) && #[trigger] (v.req)(s) is Some
                && reqs[s] == t && a2.outcomes[t] == o(s),
    decreases to - from,
{
    if to == from {
        lemma_abs_reaches_refl(a);
        lemma_same(v, completed_where(v, from, to, done, o), a, q, reqs, held);
        a
    } else {
        let s = to - 1;
        let a1 = lemma_complete_where(v, a, q, reqs, held, from, s, done, o);
        let v1 = completed_where(v, from, s, done, o);
        let w = completed_where(v, from, to, done, o);
        if done(s) && (v.req)(s) is Some {
            assert((v1.req)(s) == (v.req)(s));
            let r = reqs[s];
            let a2 = complete_state(a1, r, o(s));
            lemma_complete_one(v1, a1, q, reqs, held, s, o(s));
            lemma_abs_reaches_trans(a, a1, a2);
            assert(sits(v1, a1, q, reqs, held, s));
            assert(a1.in_flight(r));
            assert(!a1.outcomes.contains_key(r));
            lemma_same(completed(v1, s, o(s)), w, a2, q, reqs, held);
            lemma_kept_free_trans(v, a, v1, a1, completed(v1, s, o(s)), a2, held);
            lemma_kept_free_trans(v, a, completed(v1, s, o(s)), a2, w, a2, held);
            assert forall|t: ReqId| #[trigger] a2.outcomes.contains_key(t) && !a.outcomes.contains_key(t)
                implies exists|u: int| from <= u < to && done(u) && #[trigger] (v.req)(u) is Some
                    && reqs[u] == t && a2.outcomes[t] == o(u) by {
                if t != r {
                    assert(a1.outcomes.contains_key(t));
                    let u = choose|u: int| from <= u < s && done(u) && #[trigger] (v.req)(u) is Some
                        && reqs[u] == t && a1.outcomes[t] == o(u);
                } else {
                    assert(from <= s < to && done(s) && (v.req)(s) is Some && reqs[s] == t && a2.outcomes[t] == o(s));
                }
            }
            a2
        } else {
            lemma_same(v1, w, a1, q, reqs, held);
            lemma_kept_free_trans(v, a, v1, a1, w, a1, held);
            a1
        }
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
    q: spec_fn(L) -> QpId,
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
        kept_free(v, a, posted(v, s), a2, held),
        a2.qps == a.qps,
        a2.outcomes == a.outcomes,
        a2.next_id == a.next_id + 1,
        a2.in_flight(a.next_id),
        a2.requests[a.next_id].qp == q((v.staged)(s)->0.leases),
        forall|t: ReqId| t != a.next_id ==> (#[trigger] a2.requests.contains_key(t)
            <==> a.requests.contains_key(t)),
        forall|t: ReqId| t != a.next_id && #[trigger] a.requests.contains_key(t)
            ==> a2.requests[t] == a.requests[t],
{
    let st = (v.staged)(s)->0;
    let h = held(s, st.leases);
    let dirs = dirs_of(h);
    let r = a.next_id;
    let reqs2 = reqs.insert(s, r);
    let w = posted(v, s);
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    assert(leases_of(v, held, s) == h);
    assert(dirs.dom() =~= h.dom());
    // post_request
    let a1 = post_state(a, dirs, q(st.leases));
    assert(a.may_post(dirs));
    assert(abs_step(a, a1, Abs::Step::post_request(dirs, q(st.leases))));
    lemma_step_one(a, a1, Abs::Step::post_request(dirs, q(st.leases)));
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
    assert(!a.leases.contains_key(r) || a.leases[r].holder is Program || true);
    assert forall|l: LeaseId| #[trigger] h.contains_key(l) implies a2.leases.contains_key(l)
        && a2.leases[l].holder == (Holder::Request { req: r, dir: h[l].dir })
        && a2.leases[l].mem == h[l].mem by {
        assert(dirs.contains_key(l));
    }
    assert forall|l: LeaseId| #[trigger] a2.leases.contains_key(l) && a2.leases[l].held_by(r)
        implies h.contains_key(l) by {
        if !h.contains_key(l) {
            // No lease is held by a name not given yet.
            assert(a.leases[l].held_by(r));
            assert(a.requests.contains_key(r));
        }
    }
    assert(holds_exactly(a2, r, h));
    assert forall|l: LeaseId| #[trigger] a.leases.contains_key(l) && !leases_of(v, held, s).contains_key(l)
        implies a2.leases.contains_key(l) && a2.leases[l] == a.leases[l] by {
        assert(!dirs.contains_key(l));
    }
    assert((w.req)(s) == Some(st));
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

/// Refusing staged requests `[from, to)` is A's `drop` of each detached one's leases, and of
/// the shared ones of `keep`, whose unique ones are the program's again; the others' leases stay
/// the program's and wait in the ring for their owners.
pub proof fn lemma_abort<L>(
    v: View<L>,
    a: Abs::State,
    q: spec_fn(L) -> QpId,
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
        kept_free(v, a, refused_range(v, from, to, keep), a2, held),
        kept_requests(a, a2),
    decreases to - from,
{
    if to == from {
        lemma_abs_reaches_refl(a);
        lemma_same(v, refused_range(v, from, to, keep), a, q, reqs, held);
        a
    } else {
        let s = to - 1;
        let a1 = lemma_abort(v, a, q, reqs, held, from, s, keep);
        let v1 = refused_range(v, from, s, keep);
        let w = refused_range(v, from, to, keep);
        assert((v1.staged)(s) == (v.staged)(s));
        assert((v1.refused)(s) == (v.refused)(s));
        assert(wf_at(v, s));
        assert(wf_at(v1, s));
        assert(sits(v1, a1, q, reqs, held, s));
        let h = held(s, (v.staged)(s)->0.leases);
        assert(leases_of(v1, held, s) == h);
        let gone = if s == keep {
            shared_in(a1, h)
        } else if (v.staged)(s)->0.detached {
            h.dom()
        } else {
            Set::empty()
        };
        assert(gone.subset_of(h.dom()));
        assert((w.refused)(s) is Some ==> gone == Set::<LeaseId>::empty());
        if (w.refused)(s) is Some {
            assert(leases_of(w, held, s) == h);
        }
        let a2 = lemma_forget(v1, w, a1, q, reqs, held, s, gone);
        lemma_abs_reaches_trans(a, a1, a2);
        lemma_kept_free_trans(v, a, v1, a1, w, a2, held);
        lemma_kept_requests_trans(a, a1, a2);
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

/// Taking refused request `s` is A's `drop` of its shared leases: its unique ones were the
/// program's all along, and are in its hands again.
pub proof fn lemma_take_refused<L>(
    v: View<L>,
    a: Abs::State,
    q: spec_fn(L) -> QpId,
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
        kept_free(v, a, without_refused(v, s), a2, held),
        kept_requests(a, a2),
{
    assert(wf_at(v, s));
    let h = held(s, (v.refused)(s)->0);
    assert(leases_of(v, held, s) == h);
    let w = without_refused(v, s);
    assert(wf_at(w, s));
    lemma_forget(v, w, a, q, reqs, held, s, shared_in(a, h))
}

/// Dropping refused request `s` is A's `drop` of each of its leases.
pub proof fn lemma_drop_refused<L>(
    v: View<L>,
    a: Abs::State,
    q: spec_fn(L) -> QpId,
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
        kept_free(v, a, without_refused(v, s), a2, held),
        kept_requests(a, a2),
{
    assert(wf_at(v, s));
    let h = held(s, (v.refused)(s)->0);
    assert(leases_of(v, held, s) == h);
    let w = without_refused(v, s);
    assert(wf_at(w, s));
    lemma_forget(v, w, a, q, reqs, held, s, h.dom())
}

/// Taking complete request `s`, with outcome `o` and value `l`, is A's `take`: the program holds
/// each of its unique leases again, under its own name; its shared ones, which have no way back,
/// are dropped.
pub proof fn lemma_take<L>(
    v: View<L>,
    a: Abs::State,
    q: spec_fn(L) -> QpId,
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
        placed(without(v, s), a2, q, reqs, held),
        kept_free(v, a, without(v, s), a2, held),
        kept_requests(a, a2),
        forall|k: LeaseId| #[trigger] held(s, l).contains_key(k) && a.leases[k].unique
            ==> a2.is_program(k) && a2.leases[k].same_as(a.leases[k]),
{
    let r = reqs[s];
    let a1 = take_state(a, r);
    let h = held(s, l);
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    assert(leases_of(v, held, s) == h);
    assert(abs_step(a, a1, Abs::Step::take(r, o)));
    lemma_step_one(a, a1, Abs::Step::take(r, o));
    let gone = shared_in(a1, h);
    assert forall|k: LeaseId| #[trigger] gone.contains(k) implies a1.is_program(k) by {
        assert(a.leases[k].held_by(r));
    }
    let a2 = lemma_drop_leases(a1, gone);
    lemma_abs_reaches_trans(a, a1, a2);
    assert forall|k: LeaseId| #[trigger] a.leases.contains_key(k) && !leases_of(v, held, s).contains_key(k)
        implies a2.leases.contains_key(k) && a2.leases[k] == a.leases[k] by {
        assert(!a.leases[k].held_by(r));
        assert(!gone.contains(k));
    }
    assert forall|k: LeaseId| #[trigger] held(s, l).contains_key(k) && a.leases[k].unique
        implies a2.is_program(k) && a2.leases[k].same_as(a.leases[k]) by {
        assert(a.leases[k].held_by(r));
        assert(!gone.contains(k));
    }
    assert forall|k: LeaseId| #[trigger] a2.leases.contains_key(k) implies a.leases.contains_key(k)
        && (leases_of(v, held, s).contains_key(k) ==> a2.leases[k].holder is Program || a2.leases[k].held_by(r)) by {
        if leases_of(v, held, s).contains_key(k) {
            assert(a.leases[k].held_by(r));
        }
    }
    let w = without(v, s);
    assert(wf_at(w, s));
    lemma_placed_but(v, w, a, a2, q, reqs, reqs, held, s, r);
    a2
}

/// `v` with the owner of request `s` gone.
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
    q: spec_fn(L) -> QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
    ensures
        abs_reaches(a, a2),
        placed(detached(v, s), a2, q, reqs, held),
        kept_free(v, a, detached(v, s), a2, held),
        kept_requests(a, a2),
{
    let w = detached(v, s);
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    if (v.req)(s) is Some && !(v.req)(s)->0.detached {
        let r = reqs[s];
        let a2 = detach_state(a, r);
        assert(abs_step(a, a2, Abs::Step::detach(r)));
        lemma_step_one(a, a2, Abs::Step::detach(r));
        let h = held(s, (v.req)(s)->0.leases);
        assert(h == leases_of(v, held, s));
        assert forall|l: LeaseId| #[trigger] a.leases.contains_key(l) && !leases_of(v, held, s).contains_key(l)
            implies a2.leases.contains_key(l) && a2.leases[l] == a.leases[l] by {
            assert(!a.leases[l].held_by(r));
        }
        assert(sits(w, a2, q, reqs, held, s)) by {
            if (v.req)(s)->0.phase is InFlight {
                assert forall|l: LeaseId| #[trigger] a2.leases.contains_key(l) && a2.leases[l].held_by(r)
                    implies h.contains_key(l) by {
                    assert(a.leases[l].held_by(r));
                }
            }
        }
        lemma_placed_but(v, w, a, a2, q, reqs, reqs, held, s, r);
        a2
    } else if (v.refused)(s) is Some {
        let a2 = lemma_drop_refused(v, a, q, reqs, held, s);
        assert forall|t: int| #[trigger] (without_refused(v, s).staged)(t) == (w.staged)(t) by {}
        lemma_same(without_refused(v, s), w, a2, q, reqs, held);
        lemma_kept_free_trans(v, a, without_refused(v, s), a2, w, a2, held);
        a2
    } else {
        // Nothing A sees changes.
        lemma_abs_reaches_refl(a);
        if (v.req)(s) is Some {
            assert((v.req)(s)->0.detach() == (v.req)(s)->0);
            lemma_same(v, w, a, q, reqs, held);
            a
        } else {
            if (v.staged)(s) is Some {
                assert(leases_of(w, held, s) == leases_of(v, held, s));
            }
            assert(wf_at(w, s));
            lemma_forget(v, w, a, q, reqs, held, s, Set::empty())
        }
    }
}

/// `v` with waker `wk` registered on request `s`.
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
    q: spec_fn(L) -> QpId,
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
        kept_requests(a, a2),
{
    let w = registered(v, s, wk);
    assert(sits(v, a, q, reqs, held, s));
    assert(wf_at(v, s));
    if (v.req)(s) is Some && (v.req)(s)->0.phase is InFlight && !(v.req)(s)->0.detached {
        let r = reqs[s];
        let a2 = register_state(a, r, wk);
        assert(abs_step(a, a2, Abs::Step::register(r, wk)));
        lemma_step_one(a, a2, Abs::Step::register(r, wk));
        let h = held(s, (v.req)(s)->0.leases);
        assert(h == leases_of(v, held, s));
        lemma_placed_but(v, w, a, a2, q, reqs, reqs, held, s, r);
        a2
    } else {
        lemma_abs_reaches_refl(a);
        if (v.req)(s) is Some {
            lemma_same(v, w, a, q, reqs, held);
            a
        } else {
            if (v.staged)(s) is Some {
                assert(leases_of(w, held, s) == leases_of(v, held, s));
            }
            if (v.refused)(s) is Some {
                assert(leases_of(w, held, s) == leases_of(v, held, s));
            }
            assert(wf_at(w, s));
            lemma_forget(v, w, a, q, reqs, held, s, Set::empty())
        }
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
/// is posted. They may be posted in their directions, and no other request
/// holds them.
pub proof fn lemma_end<L>(
    v: View<L>,
    a: Abs::State,
    q: spec_fn(L) -> QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    s: int,
    detached: bool,
    l: L,
)
    requires
        placed(v, a, q, reqs, held),
        !holds(v, s),
        may_post_holds(a, held(s, l)),
        a.qps.contains(q(l)),
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
    assert forall|r: ReqId| #[trigger] a.requests.contains_key(r) implies names_request(w, reqs, r) by {
        assert(names_request(v, reqs, r));
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

/// Releasing requests `[from, to)`, none of them in flight or staged, is A's `detach` of each that
/// has an owner, and `drop` of each lease of each refused one. Nothing made up: A has no outcome to
/// give a request in flight here, and none is.
pub proof fn lemma_release<L>(
    v: View<L>,
    a: Abs::State,
    q: spec_fn(L) -> QpId,
    reqs: Map<int, ReqId>,
    held: Held<L>,
    from: int,
    to: int,
) -> (a2: Abs::State)
    requires
        placed(v, a, q, reqs, held),
        from <= to,
        forall|s: int| from <= s < to && #[trigger] (v.req)(s) is Some ==> (v.req)(s)->0.phase is Complete,
        forall|s: int| from <= s < to ==> #[trigger] (v.staged)(s) is None,
    ensures
        abs_reaches(a, a2),
        placed(without_range(v, from, to), a2, q, reqs, held),
        kept_requests(a, a2),
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
        assert((v1.staged)(s) == (v.staged)(s));
        let a2 = lemma_detach(v1, a1, q, reqs, held, s);
        lemma_abs_reaches_trans(a, a1, a2);
        lemma_kept_requests_trans(a, a1, a2);
        lemma_same(detached(v1, s), without_range(v, from, to), a2, q, reqs, held);
        a2
    }
}

// =============================================================================
// Examples: the lemmas' preconditions can be met.

/// A view with no request.
spec fn no_requests<L>() -> View<L> {
    View { req: |s: int| None, staged: |s: int| None, refused: |s: int| None }
}

/// A request with no name is no problem to a state of A with no request.
proof fn lemma_no_requests_placed<L>(a: Abs::State, q: spec_fn(L) -> QpId, held: Held<L>)
    requires
        abs_inv(a),
        a.requests == Map::<ReqId, Req>::empty(),
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

/// What the requests of the examples hold: the destination, to write, and the source, to read.
spec fn two_leases() -> Holds {
    Map::empty()
        .insert(0nat, Hold { dir: Dir::In, mem: source_and_destination().leases[0].mem })
        .insert(1nat, Hold { dir: Dir::Out, mem: source_and_destination().leases[1].mem })
}

/// A request with a READ destination and a shared source, of which the program
/// keeps a clone. `L` is the leases themselves. Posted and given a waker: the NIC writes the
/// one and reads the other. Completed and taken: the destination comes back, the source is gone.
/// Or detached: it stays in flight, and completing releases it.
#[verifier::spinoff_prover]
#[verifier::rlimit(20)]
pub proof fn example_request_of_two_leases() {
    let a0 = source_and_destination();
    assert(abs_inv(a0));
    let held = |_s: int, h: Holds| h;
    let q = |_h: Holds| 7nat;
    let h = two_leases();
    let reqs0 = Map::<int, ReqId>::empty();
    let v0 = no_requests::<Holds>();
    lemma_no_requests_placed(a0, q, held);
    assert forall|t: int| #[trigger] leases_of(v0, held, t).dom().disjoint(h.dom()) by {}
    lemma_end(v0, a0, q, reqs0, held, 0, false, h);
    let staged = staged_with(v0, 0, false, h);
    let a1 = lemma_post_one(staged, a0, q, reqs0, held, 0);
    let in_flight = posted(staged, 0);
    let reqs = reqs0.insert(0, a0.next_id);
    let r = a0.next_id;
    let a2 = lemma_register(in_flight, a1, q, reqs, held, 0, 9);
    let waiting = registered(in_flight, 0, 9);
    assert(sits(waiting, a2, q, reqs, held, 0));
    assert(a2.in_flight(r));
    assert(h.contains_key(0) && a2.place(0) == Place::InFlight(r));
    assert(h.contains_key(1) && a2.place(1) == Place::InFlight(r));

    // Taken: the destination comes back, under its own name.
    let flush = Outcome { status: Status::FlushErr, kind: safeverbs_spec::types::CompletionKind::StatusOnly, byte_len: 0, imm: None };
    lemma_complete_one(waiting, a2, q, reqs, held, 0, flush);
    let done = complete_state(a2, r, flush);
    let completed = completed(waiting, 0, flush);
    let taken = lemma_take(completed, done, q, reqs, held, 0, flush, h);
    assert(h.contains_key(0) && done.leases[0].unique);
    assert(taken.is_program(0));

    // Detached: still in flight with its leases, until it completes.
    let detached_state = lemma_detach(waiting, a2, q, reqs, held, 0);
    let detached_view = detached(waiting, 0);
    assert(sits(detached_view, detached_state, q, reqs, held, 0));
    assert(detached_state.place(0) == Place::InFlight(r));
    lemma_complete_one(detached_view, detached_state, q, reqs, held, 0, flush);
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
    let held = |_s: int, h: Holds| h;
    let q = |_h: Holds| 7nat;
    let owned = Map::empty().insert(0nat, Hold { dir: Dir::In, mem: a0.leases[0].mem });
    let unowned = Map::empty().insert(1nat, Hold { dir: Dir::Out, mem: a0.leases[1].mem });
    let reqs = Map::<int, ReqId>::empty();
    let v0 = no_requests::<Holds>();
    lemma_no_requests_placed(a0, q, held);
    assert forall|t: int| #[trigger] leases_of(v0, held, t).dom().disjoint(owned.dom()) by {}
    lemma_end(v0, a0, q, reqs, held, 0, false, owned);
    let v1 = staged_with(v0, 0, false, owned);
    assert forall|t: int| #[trigger] leases_of(v1, held, t).dom().disjoint(unowned.dom()) by {
        assert(t != 0 ==> leases_of(v1, held, t) == Map::<LeaseId, Hold>::empty());
    }
    lemma_end(v1, a0, q, reqs, held, 1, true, unowned);
    let v2 = staged_with(v1, 1, true, unowned);

    // Refused: the one with an owner is kept for it, the other's lease is dropped.
    let a1 = lemma_abort(v2, a0, q, reqs, held, 0, 2, -1);
    let v3 = refused_range(v2, 0, 2, -1);
    assert((v3.refused)(0) == Some(owned) && (v3.staged)(0) is None);
    assert((v3.refused)(1) is None && (v3.staged)(1) is None);

    // Its owner takes it: the program never gave the lease up.
    assert(sits(v3, a1, q, reqs, held, 0));
    assert(owned.contains_key(0) && a1.is_program(0));
    lemma_take_refused(v3, a1, q, reqs, held, 0);
}

} // verus!
