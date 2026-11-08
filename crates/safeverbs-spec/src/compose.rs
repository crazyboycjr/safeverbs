//! Several engines on one device: their states of A compose into one, coupled to M.
//!
//! Theorem 1 is proved of one engine, whose state of A has its own requests and nothing else. A
//! program may hold several engines of one device, a CQ per thread with its QPs, and buffers exposed
//! to a peer, and M is one state for all of them. Each engine's state of A is coupled to M on its
//! own share ([`crate::share::coupled_on`]); this module puts them together:
//!
//! - **A state built as a composition builds it is one A reaches** ([`assembled`],
//!   [`lemma_assembled`]): the requests of every engine, each with its leases, and the exposed
//!   buffers, cut from one shared root when they are shared.
//! - **The composition** ([`theorem_1_device`]): engines coupled to M on disjoint shares, whose
//!   requests' buffers are apart where one is unique (what their linear permissions say), and
//!   exposed buffers apart from them, stand for one state of A, reached from its start, in which
//!   every engine's requests are as its own A has them, coupled to M for the whole device: so
//!   [`crate::refinement::corollary_s1`], [`crate::refinement::corollary_f1`] and
//!   [`crate::refinement::corollary_r1`] apply to the device as a whole.
#![allow(unused_imports)]

use crate::abs::*;
use crate::nic::*;
use crate::refinement::*;
use crate::share::*;
use crate::types::*;
use vstd::prelude::*;

verus! {

// =========================================================================================
// A state of A, assembled

/// The shared lease a composition cuts every shared lease from: name 0, over `bytes`.
pub open spec fn root_lease(bytes: ISet<int>) -> Lease {
    Lease { mem: bytes, unique: false, any_bytes: true, holder: Holder::Program }
}

/// `s` with its root.
pub open spec fn with_root(s: Abs::State, bytes: ISet<int>) -> Abs::State {
    Abs::State { leases: s.leases.insert(0, root_lease(bytes)), ..s }
}

/// `s` is as a composition builds it, its shared leases cut from a root over `bytes` it no longer
/// has: no name is both a lease and a request, name 0 neither; each request's leases are older than
/// it; every lease accepts any bytes, a shared one lies in `bytes` and a unique one outside; no waker
/// was woken; the outcomes are those of its complete requests, none of which was given up.
pub open spec fn assembled(s: Abs::State, bytes: ISet<int>) -> bool {
    &&& abs_inv(s)
    &&& !s.leases.contains_key(0) && !s.requests.contains_key(0)
    &&& forall|x: nat| #[trigger] s.leases.contains_key(x) ==> !s.requests.contains_key(x)
    &&& s.wakes == Seq::<WakerId>::empty()
    &&& forall|r: ReqId| #[trigger] s.outcomes.contains_key(r) ==> s.requests.contains_key(r)
    &&& forall|r: ReqId| #[trigger] s.requests.contains_key(r) && s.requests[r].phase is Complete
        ==> !s.requests[r].detached
    &&& forall|l: LeaseId| #[trigger] s.leases.contains_key(l) && s.leases[l].holder is Request
        ==> l < s.leases[l].holder->req
    &&& forall|l: LeaseId| #[trigger] s.leases.contains_key(l) ==> s.leases[l].any_bytes
    &&& forall|l: LeaseId| #[trigger] s.leases.contains_key(l) && !s.leases[l].unique
        ==> s.leases[l].mem.subset_of(bytes)
    &&& forall|l: LeaseId| #[trigger] s.leases.contains_key(l) && s.leases[l].unique
        ==> s.leases[l].mem.disjoint(bytes)
}

/// One more step of A.
proof fn lemma_then(a: Abs::State, b: Abs::State, c: Abs::State, step: Abs::Step)
    requires
        abs_reaches(a, b),
        abs_step(b, c, step),
    ensures
        abs_reaches(a, c),
{
    lemma_abs_step_is_next(b, c, step);
    lemma_abs_reaches_next(b, c);
    lemma_abs_reaches_trans(a, b, c);
}

/// A's start, with queue pairs `qs`.
proof fn lemma_qps(qs: Set<QpId>)
    ensures
        abs_reaches(abs_start(), Abs::State { qps: qs, ..abs_start() }),
    decreases qs.len(),
{
    if qs.len() == 0 {
        assert(qs =~= Set::empty());
        assert(Abs::State { qps: qs, ..abs_start() } == abs_start());
        lemma_abs_reaches_refl(abs_start());
    } else {
        let q = qs.choose();
        let rest = qs.remove(q);
        lemma_qps(rest);
        let pre = Abs::State { qps: rest, ..abs_start() };
        assert(rest.insert(q) =~= qs);
        assert(abs_step(pre, Abs::State { qps: qs, ..abs_start() }, Abs::Step::create_qp(q)));
        lemma_then(abs_start(), pre, Abs::State { qps: qs, ..abs_start() }, Abs::Step::create_qp(q));
    }
}

/// `s` with name `x`, its newest, gone: a lease or a request, its leases back with the program.
spec fn without_newest(s: Abs::State, x: nat) -> Abs::State {
    if s.requests.contains_key(x) {
        Abs::State {
            requests: s.requests.remove(x),
            leases: s.leases_returned(x),
            outcomes: s.outcomes.remove(x),
            next_id: x,
            ..s
        }
    } else {
        Abs::State { leases: s.leases.remove(x), next_id: x, ..s }
    }
}

/// Without its newest name, an assembled state is assembled.
proof fn lemma_without_newest(s: Abs::State, bytes: ISet<int>)
    requires
        assembled(s, bytes),
        s.next_id >= 1,
    ensures
        assembled(without_newest(s, (s.next_id - 1) as nat), bytes),
{
    let x = (s.next_id - 1) as nat;
    let t = without_newest(s, x);
    if s.requests.contains_key(x) {
        assert forall|l: LeaseId| #[trigger] t.leases.contains_key(l) implies l < t.next_id by {
            assert(s.leases.contains_key(l));
            assert(l != x);
        }
        assert forall|r: ReqId| #[trigger] t.requests.contains_key(r) implies r < t.next_id by {}
        assert forall|r: ReqId| #[trigger] t.outcomes.contains_key(r) implies r < t.next_id by {
            assert(s.requests.contains_key(r));
        }
        assert(t.inv_ids());
        assert forall|k: LeaseId, l: LeaseId| #![trigger t.leases[k], t.leases[l]]
            t.leases.contains_key(k) && t.leases.contains_key(l) && k != l
                && (t.leases[k].unique || t.leases[l].unique)
            implies t.leases[k].mem.disjoint(t.leases[l].mem) by {
            assert(s.leases[k].mem == t.leases[k].mem && s.leases[k].unique == t.leases[k].unique);
            assert(s.leases[l].mem == t.leases[l].mem && s.leases[l].unique == t.leases[l].unique);
        }
        assert(t.inv_unique());
        assert(t.inv_nic_writes());
        assert forall|l: LeaseId| #[trigger] t.leases.contains_key(l) && t.leases[l].holder is Request
            implies t.requests.contains_key(t.leases[l].holder->req) by {
            assert(s.leases[l].holder is Request && !s.leases[l].held_by(x));
        }
        assert(t.inv_places());
        assert(t.inv_requests());
        assert(t.inv_wakers());
        assert forall|l: LeaseId| #[trigger] t.leases.contains_key(l) && t.leases[l].holder is Request
            implies l < t.leases[l].holder->req by {
            assert(s.leases[l].holder is Request && !s.leases[l].held_by(x));
        }
    } else {
        assert forall|l: LeaseId| #[trigger] t.leases.contains_key(l) implies l < t.next_id by {
            assert(s.leases.contains_key(l));
        }
        assert forall|r: ReqId| #[trigger] t.requests.contains_key(r) implies r < t.next_id by {
            assert(s.requests.contains_key(r));
            assert(r != x);
        }
        assert forall|r: ReqId| #[trigger] t.outcomes.contains_key(r) implies r < t.next_id by {
            assert(s.requests.contains_key(r));
            assert(r != x);
        }
        assert(t.inv_ids());
        assert(t.inv_unique());
        assert(t.inv_nic_writes());
        assert forall|l: LeaseId| #[trigger] t.leases.contains_key(l) && t.leases[l].holder is Request
            implies t.requests.contains_key(t.leases[l].holder->req) by {
            assert(s.leases.contains_key(l));
        }
        assert(t.inv_places());
        assert(t.inv_requests());
        assert(t.inv_wakers());
    }
}

/// An assembled state with its root is one A reaches, newest name last.
proof fn lemma_rooted(s: Abs::State, bytes: ISet<int>)
    requires
        assembled(s, bytes),
        s.next_id >= 1,
    ensures
        abs_reaches(abs_start(), with_root(s, bytes)),
    decreases s.next_id,
{
    let x = (s.next_id - 1) as nat;
    let root = root_lease(bytes);
    if x == 0 {
        // Nothing but the queue pairs and the root.
        assert(s.leases =~= Map::empty()) by {
            assert forall|l: LeaseId| !#[trigger] s.leases.contains_key(l) by {
                if s.leases.contains_key(l) {
                    assert(l < s.next_id);
                }
            }
        }
        assert(s.requests =~= Map::empty()) by {
            assert forall|r: ReqId| !#[trigger] s.requests.contains_key(r) by {
                if s.requests.contains_key(r) {
                    assert(r < s.next_id);
                }
            }
        }
        assert(s.outcomes =~= Map::empty()) by {
            assert forall|r: ReqId| !#[trigger] s.outcomes.contains_key(r) by {
                if s.outcomes.contains_key(r) {
                    assert(s.requests.contains_key(r));
                }
            }
        }
        let s0 = Abs::State { qps: s.qps, ..abs_start() };
        lemma_qps(s.qps);
        let fresh = Lease { mem: bytes, unique: true, any_bytes: true, holder: Holder::Program };
        let s1 = Abs::State { leases: s0.leases.insert(0, fresh), next_id: 1, ..s0 };
        assert(abs_step(s0, s1, Abs::Step::alloc(bytes, true)));
        lemma_then(abs_start(), s0, s1, Abs::Step::alloc(bytes, true));
        let s2 = Abs::State { leases: s1.leases.insert(0, Lease { unique: false, ..s1.leases[0] }), ..s1 };
        assert(abs_step(s1, s2, Abs::Step::freeze(0)));
        lemma_then(abs_start(), s1, s2, Abs::Step::freeze(0));
        assert(s2.leases =~= with_root(s, bytes).leases);
        assert(s2 == with_root(s, bytes));
    } else {
        let t = without_newest(s, x);
        lemma_without_newest(s, bytes);
        lemma_rooted(t, bytes);
        let u = with_root(t, bytes);
        let goal = with_root(s, bytes);
        if s.leases.contains_key(x) {
            // A lease the program or a peer holds: cut from the root if shared, else allocated.
            let lease = s.leases[x];
            assert(!s.requests.contains_key(x));
            assert(!(lease.holder is Request)) by {
                if lease.holder is Request {
                    assert(s.requests.contains_key(lease.holder->req));
                    assert(lease.holder->req < s.next_id);
                }
            }
            let made = Lease { holder: Holder::Program, ..lease };
            let u1 = Abs::State { leases: u.leases.insert(x, made), next_id: x + 1, ..u };
            if lease.unique {
                assert forall|l: LeaseId| #[trigger] u.leases.contains_key(l)
                    implies u.leases[l].mem.disjoint(lease.mem) by {
                    if l != 0 {
                        assert(s.leases.contains_key(l) && l != x);
                        assert(s.leases[l].mem.disjoint(lease.mem));
                    }
                }
                assert(abs_step(u, u1, Abs::Step::alloc(lease.mem, true)));
                lemma_then(abs_start(), u, u1, Abs::Step::alloc(lease.mem, true));
            } else {
                assert(u.leases[0] == root);
                assert(made == Lease { mem: lease.mem, ..u.leases[0] });
                assert(abs_step(u, u1, Abs::Step::share(0, lease.mem)));
                lemma_then(abs_start(), u, u1, Abs::Step::share(0, lease.mem));
            }
            if lease.holder is Exposed {
                let rw = lease.holder->remote_write;
                let u2 = Abs::State { leases: u1.leases.insert(x, u1.leases[x].with_holder(Holder::Exposed { remote_write: rw })), ..u1 };
                assert(abs_step(u1, u2, Abs::Step::expose(x, rw)));
                lemma_then(abs_start(), u1, u2, Abs::Step::expose(x, rw));
                assert(u2.leases =~= goal.leases);
                assert(u2 == goal);
            } else {
                assert(u1.leases =~= goal.leases);
                assert(u1 == goal);
            }
        } else if s.requests.contains_key(x) {
            // A request: its leases go into it, then it comes where it is.
            let req = s.requests[x];
            let held = s.leases.dom().filter(|l: LeaseId| s.leases[l].held_by(x));
            let dirs = Map::new(held, |l: LeaseId| s.leases[l].holder->dir);
            assert(u.qps.contains(req.qp));
            assert forall|l: LeaseId| #[trigger] dirs.contains_key(l) implies u.is_program(l)
                && (dirs[l] is In ==> u.leases[l].writable()) by {
                assert(held.contains(l));
                assert(l != 0);
            }
            let posted = Req { qp: req.qp, phase: Phase::InFlight, waker: None, detached: false };
            let u1 = Abs::State {
                requests: u.requests.insert(x, posted),
                leases: u.leases_in_request(dirs, x),
                next_id: x + 1,
                ..u
            };
            assert(abs_step(u, u1, Abs::Step::post_request(dirs, req.qp)));
            lemma_then(abs_start(), u, u1, Abs::Step::post_request(dirs, req.qp));
            assert(u1.leases =~= goal.leases) by {
                assert forall|l: LeaseId| #[trigger] u1.leases.contains_key(l)
                    implies u1.leases[l] == goal.leases[l] by {
                    if held.contains(l) {
                        assert(l != 0);
                    }
                }
            }
            match req.phase {
                Phase::Complete(o) => {
                    let u2 = Abs::State {
                        outcomes: u1.outcomes.insert(x, o),
                        requests: u1.requests.insert(x, posted.completed(o)),
                        ..u1
                    };
                    assert(abs_step(u1, u2, Abs::Step::complete(x, o)));
                    lemma_then(abs_start(), u1, u2, Abs::Step::complete(x, o));
                    assert(u2.requests =~= goal.requests);
                    assert(u2.outcomes =~= goal.outcomes);
                    assert(u2 == goal);
                },
                Phase::InFlight => {
                    assert(u1.outcomes =~= goal.outcomes);
                    if req.detached {
                        let u2 = Abs::State { requests: u1.requests.insert(x, posted.detach()), ..u1 };
                        assert(abs_step(u1, u2, Abs::Step::detach(x)));
                        lemma_then(abs_start(), u1, u2, Abs::Step::detach(x));
                        assert(u2.requests =~= goal.requests);
                        assert(u2 == goal);
                    } else if req.waker is Some {
                        let w = req.waker->0;
                        let u2 = Abs::State { requests: u1.requests.insert(x, posted.with_waker(w)), ..u1 };
                        assert(abs_step(u1, u2, Abs::Step::register(x, w)));
                        lemma_then(abs_start(), u1, u2, Abs::Step::register(x, w));
                        assert(u2.requests =~= goal.requests);
                        assert(u2 == goal);
                    } else {
                        assert(u1.requests =~= goal.requests);
                        assert(u1 == goal);
                    }
                },
            }
        } else {
            // An unused name.
            let gap = Lease { mem: ISet::empty(), unique: true, any_bytes: true, holder: Holder::Program };
            let u1 = Abs::State { leases: u.leases.insert(x, gap), next_id: x + 1, ..u };
            assert(abs_step(u, u1, Abs::Step::alloc(ISet::empty(), true)));
            lemma_then(abs_start(), u, u1, Abs::Step::alloc(ISet::empty(), true));
            let u2 = Abs::State { leases: u1.leases.remove(x), ..u1 };
            assert(abs_step(u1, u2, Abs::Step::drop(x)));
            lemma_then(abs_start(), u1, u2, Abs::Step::drop(x));
            assert(u2.leases =~= goal.leases);
            assert(u2 == goal);
        }
    }
}

/// **A state as a composition builds it is one A reaches**: its queue pairs, then name by name, its
/// root cut first and dropped last.
pub proof fn lemma_assembled(s: Abs::State, bytes: ISet<int>)
    requires
        assembled(s, bytes),
    ensures
        abs_reaches(abs_start(), s),
{
    if s.next_id == 0 {
        assert(s.leases =~= Map::empty()) by {
            assert forall|l: LeaseId| !#[trigger] s.leases.contains_key(l) by {}
        }
        assert(s.requests =~= Map::empty()) by {
            assert forall|r: ReqId| !#[trigger] s.requests.contains_key(r) by {}
        }
        assert(s.outcomes =~= Map::empty()) by {
            assert forall|r: ReqId| !#[trigger] s.outcomes.contains_key(r) by {}
        }
        lemma_qps(s.qps);
        assert(s == Abs::State { qps: s.qps, ..abs_start() });
    } else {
        lemma_rooted(s, bytes);
        let rooted = with_root(s, bytes);
        assert(rooted.leases.remove(0) =~= s.leases);
        assert(abs_step(rooted, s, Abs::Step::drop(0)));
        lemma_then(abs_start(), rooted, s, Abs::Step::drop(0));
    }
}

// =========================================================================================
// The device

/// What an engine brings to the device: its state of A, the WQEs of each of its requests, and its
/// share of M.
pub struct EnginePart {
    pub abs: Abs::State,
    pub ranges: Map<ReqId, WqeRange>,
    pub share: Share,
}

/// Lease `l` of `a` is held by a request.
pub open spec fn lent(a: Abs::State, l: LeaseId) -> bool {
    a.leases.contains_key(l) && a.leases[l].holder is Request
}

/// What one engine's part says of itself, M being at `m`: A's invariants hold, it is coupled to M
/// on its share, it has no lease open to a peer, and none of its complete requests was given up.
pub open spec fn part_ok(p: EnginePart, m: Nic::State) -> bool {
    &&& abs_inv(p.abs)
    &&& coupled_on(p.abs, m, p.share, p.ranges)
    &&& forall|l: LeaseId| #[trigger] p.abs.leases.contains_key(l) ==> !(p.abs.leases[l].holder is Exposed)
    &&& forall|r: ReqId| #[trigger] p.abs.requests.contains_key(r) && p.abs.requests[r].phase is Complete
        ==> !p.abs.requests[r].detached
}

/// The requests of two engines are apart: a unique lease one holds shares no byte with a lease the
/// other holds. Their buffers' linear permissions say so.
pub open spec fn parts_apart(p: EnginePart, q: EnginePart) -> bool {
    forall|k: LeaseId, l: LeaseId| #![trigger p.abs.leases[k], q.abs.leases[l]]
        lent(p.abs, k) && lent(q.abs, l) && (p.abs.leases[k].unique || q.abs.leases[l].unique)
            ==> p.abs.leases[k].mem.disjoint(q.abs.leases[l].mem)
}

/// The buffers exposed to a peer are apart from each other and from what the engines' requests
/// hold.
pub open spec fn exposed_apart(parts: Seq<EnginePart>, exposed: Seq<ISet<int>>) -> bool {
    &&& forall|j: int, k: int| #![trigger exposed[j], exposed[k]]
        0 <= j < exposed.len() && 0 <= k < exposed.len() && j != k ==> exposed[j].disjoint(exposed[k])
    &&& forall|i: int, j: int, l: LeaseId| #![trigger parts[i].abs.leases[l], exposed[j]]
        0 <= i < parts.len() && 0 <= j < exposed.len() && lent(parts[i].abs, l)
            ==> parts[i].abs.leases[l].mem.disjoint(exposed[j])
}

/// M has nothing but what the engines and the exposed buffers account for: each queue pair is of an
/// engine's share, and each region open to a peer lies within a buffer exposed.
pub open spec fn accounted(parts: Seq<EnginePart>, exposed: Seq<ISet<int>>, m: Nic::State) -> bool {
    &&& forall|q: QpId| #[trigger] m.qps.contains_key(q)
        ==> exists|i: int| 0 <= i < parts.len() && #[trigger] parts[i].share.owns.contains(q)
    &&& forall|key: Key| #[trigger] m.mrs.contains_key(key) && (m.mrs[key].remote_read || m.mrs[key].remote_write)
        ==> exists|j: int| 0 <= j < exposed.len() && m.mrs[key].span.bytes().subset_of(#[trigger] exposed[j])
}

/// What the engines of the device and its exposed buffers are, M being at `m`: each part is what it
/// says of itself, their shares and requests are apart, the exposed buffers too, and M has nothing
/// else.
pub open spec fn device_ok(parts: Seq<EnginePart>, exposed: Seq<ISet<int>>, m: Nic::State) -> bool {
    &&& forall|i: int| 0 <= i < parts.len() ==> part_ok(#[trigger] parts[i], m)
    &&& forall|i: int, j: int| #![trigger parts[i], parts[j]] 0 <= i < parts.len() && 0 <= j < parts.len() && i != j
        ==> disjoint(parts[i].share, parts[j].share) && parts_apart(parts[i], parts[j])
    &&& exposed_apart(parts, exposed)
    &&& accounted(parts, exposed, m)
}

// The names the device's state of A gives: part `i` has those from `1 + width * i`, its leases'
// first, under their own names, then its requests'; the exposed buffers come after every part.

/// A bound on every name a part gives.
pub open spec fn bound(parts: Seq<EnginePart>) -> nat
    decreases parts.len(),
{
    if parts.len() == 0 {
        1
    } else {
        let below = bound(parts.drop_last());
        if parts.last().abs.next_id + 1 > below {
            parts.last().abs.next_id + 1
        } else {
            below
        }
    }
}

/// The names one part takes.
pub open spec fn width(parts: Seq<EnginePart>) -> nat {
    2 * bound(parts)
}

/// The name of lease `l` of part `i`.
pub open spec fn lease_name(parts: Seq<EnginePart>, i: int, l: LeaseId) -> nat {
    (1 + width(parts) * i + l) as nat
}

/// The name of request `r` of part `i`.
pub open spec fn req_name(parts: Seq<EnginePart>, i: int, r: ReqId) -> nat {
    (1 + width(parts) * i + bound(parts) + r) as nat
}

/// The name of exposed buffer `j`.
pub open spec fn exposed_name(parts: Seq<EnginePart>, j: int) -> nat {
    (1 + width(parts) * parts.len() + j) as nat
}

/// The first name after the parts'.
pub open spec fn parts_end(parts: Seq<EnginePart>) -> int {
    1 + width(parts) * parts.len() as int
}

/// The part name `id` is of.
pub open spec fn part_of(parts: Seq<EnginePart>, id: nat) -> int {
    (id - 1) / (width(parts) as int)
}

/// Where name `id` is in its part.
pub open spec fn offset_of(parts: Seq<EnginePart>, id: nat) -> int {
    (id - 1) % (width(parts) as int)
}

/// The bound bounds every part's names.
proof fn lemma_bound(parts: Seq<EnginePart>)
    ensures
        bound(parts) >= 1,
        forall|i: int| 0 <= i < parts.len() ==> (#[trigger] parts[i]).abs.next_id < bound(parts),
    decreases parts.len(),
{
    if parts.len() > 0 {
        let rest = parts.drop_last();
        lemma_bound(rest);
        assert forall|i: int| 0 <= i < parts.len() implies (#[trigger] parts[i]).abs.next_id < bound(parts) by {
            if i < rest.len() {
                assert(rest[i] == parts[i]);
            }
        }
    }
}

/// A name of a part says which part and where.
proof fn lemma_named(parts: Seq<EnginePart>, i: int, x: int)
    requires
        0 <= i < parts.len(),
        0 <= x < width(parts),
    ensures
        part_of(parts, (1 + width(parts) * i + x) as nat) == i,
        offset_of(parts, (1 + width(parts) * i + x) as nat) == x,
        1 + width(parts) * i + x < parts_end(parts),
{
    let w = width(parts) as int;
    let n = parts.len() as int;
    lemma_bound(parts);
    assert(w * i + x == i * w + x) by (nonlinear_arith);
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod_converse(w * i + x, w, i, x);
    assert(w * i + x < w * n) by (nonlinear_arith)
        requires
            0 <= x < w,
            0 <= i < n,
    ;
}

/// Every name of the parts is one, of a part and a place in it.
proof fn lemma_name(parts: Seq<EnginePart>, id: nat)
    requires
        1 <= id < parts_end(parts),
    ensures
        0 <= part_of(parts, id) < parts.len(),
        0 <= offset_of(parts, id) < width(parts),
        id == 1 + width(parts) * part_of(parts, id) + offset_of(parts, id),
{
    let w = width(parts) as int;
    let n = parts.len() as int;
    let x = id - 1;
    lemma_bound(parts);
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod(x, w);
    vstd::arithmetic::div_mod::lemma_mod_bound(x, w);
    vstd::arithmetic::div_mod::lemma_div_pos_is_pos(x, w);
    let q = x / w;
    let r = x % w;
    assert(q < n) by (nonlinear_arith)
        requires
            x == w * q + r,
            0 <= r < w,
            0 <= x < w * n,
            q >= 0,
    ;
}

/// The lease of name `id` in the device's state of A, if any: a part's lease held by a request,
/// under its request's new name, or an exposed buffer's.
pub open spec fn device_lease(parts: Seq<EnginePart>, exposed: Seq<ISet<int>>, id: nat) -> Option<Lease> {
    if 1 <= id < parts_end(parts) {
        let i = part_of(parts, id);
        let l = offset_of(parts, id) as nat;
        if l < bound(parts) && lent(parts[i].abs, l) {
            let lease = parts[i].abs.leases[l];
            Some(Lease {
                mem: lease.mem,
                unique: lease.unique,
                any_bytes: true,
                holder: Holder::Request { req: req_name(parts, i, lease.holder->req), dir: lease.holder->dir },
            })
        } else {
            None
        }
    } else if parts_end(parts) <= id < parts_end(parts) + exposed.len() {
        Some(Lease {
            mem: exposed[id - parts_end(parts)],
            unique: true,
            any_bytes: true,
            holder: Holder::Exposed { remote_write: true },
        })
    } else {
        None
    }
}

/// The part and request of name `id` in the device's state of A, if it names one.
pub open spec fn device_req(parts: Seq<EnginePart>, id: nat) -> Option<(int, ReqId)> {
    if 1 <= id < parts_end(parts) {
        let i = part_of(parts, id);
        let o = offset_of(parts, id);
        if o >= bound(parts) && parts[i].abs.requests.contains_key((o - bound(parts)) as nat) {
            Some((i, (o - bound(parts)) as nat))
        } else {
            None
        }
    } else {
        None
    }
}

/// The request of name `id` in the device's state of A: its part's own.
pub open spec fn req_at(parts: Seq<EnginePart>, id: nat) -> Req {
    let (i, r) = device_req(parts, id)->0;
    parts[i].abs.requests[r]
}

/// The WQEs of the request of name `id`: its part's request's.
pub open spec fn range_at(parts: Seq<EnginePart>, id: nat) -> WqeRange {
    let (i, r) = device_req(parts, id)->0;
    parts[i].ranges[r]
}

/// The queue pairs of the parts' states of A.
pub open spec fn device_qps(parts: Seq<EnginePart>) -> Set<QpId>
    decreases parts.len(),
{
    if parts.len() == 0 {
        Set::empty()
    } else {
        device_qps(parts.drop_last()).union(parts.last().abs.qps)
    }
}

/// The names of the device's state of A.
pub open spec fn device_names(parts: Seq<EnginePart>, exposed: Seq<ISet<int>>) -> Set<nat> {
    Set::<nat>::range(0, (parts_end(parts) + exposed.len()) as nat)
}

/// The device's state of A: every part's requests with their leases, and the exposed buffers.
pub open spec fn device_abs(parts: Seq<EnginePart>, exposed: Seq<ISet<int>>) -> Abs::State {
    let names = device_names(parts, exposed);
    let reqs = names.filter(|id: nat| device_req(parts, id) is Some);
    Abs::State {
        qps: device_qps(parts),
        leases: Map::new(
            names.filter(|id: nat| device_lease(parts, exposed, id) is Some),
            |id: nat| device_lease(parts, exposed, id)->0,
        ),
        requests: Map::new(reqs, |id: nat| req_at(parts, id)),
        next_id: (parts_end(parts) + exposed.len()) as nat,
        outcomes: Map::new(reqs.filter(|id: nat| req_at(parts, id).phase is Complete), |id: nat| req_at(parts, id).phase->0),
        wakes: Seq::empty(),
    }
}

/// The WQEs of each request of the device's state of A: those of its part's request.
pub open spec fn device_ranges(parts: Seq<EnginePart>, exposed: Seq<ISet<int>>) -> Map<ReqId, WqeRange> {
    Map::new(
        device_names(parts, exposed).filter(|id: nat| device_req(parts, id) is Some),
        |id: nat| range_at(parts, id),
    )
}

/// The bytes the shared leases of every part lie in: the root they are cut from.
pub open spec fn shared_bytes(parts: Seq<EnginePart>) -> ISet<int> {
    ISet::new(|x: int| exists|i: int, l: LeaseId| #![trigger parts[i].abs.leases[l]]
        0 <= i < parts.len() && lent(parts[i].abs, l) && !parts[i].abs.leases[l].unique
            && parts[i].abs.leases[l].mem.contains(x))
}

/// A queue pair of the parts' states of A is one of a part's.
proof fn lemma_device_qps(parts: Seq<EnginePart>, q: QpId)
    ensures
        device_qps(parts).contains(q) <==> exists|i: int| 0 <= i < parts.len() && (#[trigger] parts[i]).abs.qps.contains(q),
    decreases parts.len(),
{
    if parts.len() > 0 {
        let rest = parts.drop_last();
        let last = parts.len() - 1;
        lemma_device_qps(rest, q);
        assert(device_qps(parts) == device_qps(rest).union(parts[last].abs.qps));
        if device_qps(rest).contains(q) {
            let i = choose|i: int| 0 <= i < rest.len() && (#[trigger] rest[i]).abs.qps.contains(q);
            assert(parts[i] == rest[i]);
        }
        if exists|i: int| 0 <= i < parts.len() && (#[trigger] parts[i]).abs.qps.contains(q) {
            let i = choose|i: int| 0 <= i < parts.len() && (#[trigger] parts[i]).abs.qps.contains(q);
            if i < last {
                assert(rest[i] == parts[i]);
            }
        }
    }
}

/// Name `lease_name(parts, i, l)` is the lease of `l`, and `req_name(parts, i, r)` the request `r`
/// of part `i`.
proof fn lemma_names_of(parts: Seq<EnginePart>, exposed: Seq<ISet<int>>, i: int)
    requires
        0 <= i < parts.len(),
        abs_inv(parts[i].abs),
    ensures
        forall|l: LeaseId| #[trigger] lent(parts[i].abs, l) ==> {
            &&& device_abs(parts, exposed).leases.contains_key(lease_name(parts, i, l))
            &&& device_lease(parts, exposed, lease_name(parts, i, l)) == Some(Lease {
                mem: parts[i].abs.leases[l].mem,
                unique: parts[i].abs.leases[l].unique,
                any_bytes: true,
                holder: Holder::Request {
                    req: req_name(parts, i, parts[i].abs.leases[l].holder->req),
                    dir: parts[i].abs.leases[l].holder->dir,
                },
            })
        },
        forall|r: ReqId| #[trigger] parts[i].abs.requests.contains_key(r) ==> {
            &&& device_req(parts, req_name(parts, i, r)) == Some((i, r))
            &&& device_abs(parts, exposed).requests.contains_key(req_name(parts, i, r))
        },
{
    lemma_bound(parts);
    let b = bound(parts) as int;
    let a = parts[i].abs;
    assert forall|l: LeaseId| #[trigger] lent(parts[i].abs, l) implies {
        &&& device_abs(parts, exposed).leases.contains_key(lease_name(parts, i, l))
        &&& device_lease(parts, exposed, lease_name(parts, i, l)) == Some(Lease {
            mem: parts[i].abs.leases[l].mem,
            unique: parts[i].abs.leases[l].unique,
            any_bytes: true,
            holder: Holder::Request {
                req: req_name(parts, i, parts[i].abs.leases[l].holder->req),
                dir: parts[i].abs.leases[l].holder->dir,
            },
        })
    } by {
        assert(a.inv_ids());
        assert(l < a.next_id);
        lemma_named(parts, i, l as int);
        assert(device_names(parts, exposed).contains(lease_name(parts, i, l)));
    }
    assert forall|r: ReqId| #[trigger] parts[i].abs.requests.contains_key(r) implies {
        &&& device_req(parts, req_name(parts, i, r)) == Some((i, r))
        &&& device_abs(parts, exposed).requests.contains_key(req_name(parts, i, r))
    } by {
        assert(a.inv_ids());
        assert(r < a.next_id);
        lemma_named(parts, i, b + r);
        assert(device_names(parts, exposed).contains(req_name(parts, i, r)));
    }
}

/// What a lease of the device's state of A is: a part's lease held by a request, or an exposed
/// buffer.
proof fn lemma_lease_at(parts: Seq<EnginePart>, exposed: Seq<ISet<int>>, id: nat)
    requires
        device_abs(parts, exposed).leases.contains_key(id),
    ensures
        device_abs(parts, exposed).leases[id] == device_lease(parts, exposed, id)->0,
        id < parts_end(parts) ==> {
            let i = part_of(parts, id);
            let l = offset_of(parts, id) as nat;
            &&& 0 <= i < parts.len()
            &&& id == lease_name(parts, i, l)
            &&& l < bound(parts)
            &&& lent(parts[i].abs, l)
        },
        id >= parts_end(parts) ==> 0 <= id - parts_end(parts) < exposed.len()
            && id == exposed_name(parts, id - parts_end(parts)),
{
    if id < parts_end(parts) {
        lemma_name(parts, id);
    }
}

/// What a request of the device's state of A is: a part's own request.
proof fn lemma_req_at(parts: Seq<EnginePart>, exposed: Seq<ISet<int>>, id: nat)
    requires
        device_abs(parts, exposed).requests.contains_key(id),
    ensures
        device_req(parts, id) is Some,
        ({
            let (i, r) = device_req(parts, id)->0;
            &&& 0 <= i < parts.len()
            &&& parts[i].abs.requests.contains_key(r)
            &&& id == req_name(parts, i, r)
            &&& device_abs(parts, exposed).requests[id] == parts[i].abs.requests[r]
        }),
{
    lemma_name(parts, id);
}

/// The device's state of A satisfies A's invariants and is as a composition builds it.
proof fn lemma_device_assembled(parts: Seq<EnginePart>, exposed: Seq<ISet<int>>, m: Nic::State)
    requires
        device_ok(parts, exposed, m),
    ensures
        assembled(device_abs(parts, exposed), shared_bytes(parts)),
{
    let a = device_abs(parts, exposed);
    let bytes = shared_bytes(parts);
    lemma_bound(parts);
    assert forall|i: int| 0 <= i < parts.len() implies abs_inv(#[trigger] parts[i].abs) by {
        assert(part_ok(parts[i], m));
    }
    // inv_unique
    assert forall|k: LeaseId, l: LeaseId| #![trigger a.leases[k], a.leases[l]]
        a.leases.contains_key(k) && a.leases.contains_key(l) && k != l && (a.leases[k].unique || a.leases[l].unique)
        implies a.leases[k].mem.disjoint(a.leases[l].mem) by {
        lemma_lease_at(parts, exposed, k);
        lemma_lease_at(parts, exposed, l);
        if k < parts_end(parts) && l < parts_end(parts) {
            let (ik, ok) = (part_of(parts, k), offset_of(parts, k) as nat);
            let (il, ol) = (part_of(parts, l), offset_of(parts, l) as nat);
            if ik == il {
                assert(ok != ol);
                assert(parts[ik].abs.inv_unique());
                assert(parts[ik].abs.leases[ok].mem.disjoint(parts[ik].abs.leases[ol].mem));
            } else {
                assert(parts_apart(parts[ik], parts[il]));
                assert(parts[ik].abs.leases[ok].mem.disjoint(parts[il].abs.leases[ol].mem));
            }
        } else if k < parts_end(parts) {
            let (ik, ok) = (part_of(parts, k), offset_of(parts, k) as nat);
            let j = l - parts_end(parts);
            assert(parts[ik].abs.leases[ok].mem.disjoint(exposed[j]));
        } else if l < parts_end(parts) {
            let (il, ol) = (part_of(parts, l), offset_of(parts, l) as nat);
            let j = k - parts_end(parts);
            assert(parts[il].abs.leases[ol].mem.disjoint(exposed[j]));
        } else {
            let (j, jl) = (k - parts_end(parts), l - parts_end(parts));
            assert(exposed[j].disjoint(exposed[jl]));
        }
    }
    assert(a.inv_unique());
    // inv_nic_writes
    assert forall|l: LeaseId| #[trigger] a.leases.contains_key(l) implies match a.leases[l].holder {
        Holder::Program => true,
        Holder::Request { dir, .. } => dir is In ==> a.leases[l].writable(),
        Holder::Exposed { remote_write } => remote_write ==> a.leases[l].writable(),
    } by {
        lemma_lease_at(parts, exposed, l);
        if l < parts_end(parts) {
            let (i, o) = (part_of(parts, l), offset_of(parts, l) as nat);
            assert(parts[i].abs.inv_nic_writes());
            assert(parts[i].abs.leases.contains_key(o));
        }
    }
    assert(a.inv_nic_writes());
    // inv_places
    assert forall|l: LeaseId| #[trigger] a.leases.contains_key(l) && a.leases[l].holder is Request
        implies a.requests.contains_key(a.leases[l].holder->req) by {
        lemma_lease_at(parts, exposed, l);
        let (i, o) = (part_of(parts, l), offset_of(parts, l) as nat);
        assert(parts[i].abs.inv_places());
        assert(parts[i].abs.leases.contains_key(o));
        lemma_names_of(parts, exposed, i);
        assert(parts[i].abs.requests.contains_key(parts[i].abs.leases[o].holder->req));
    }
    assert(a.inv_places());
    // inv_requests and inv_wakers
    assert forall|r: ReqId| #[trigger] a.requests.contains_key(r) implies {
        let req = a.requests[r];
        &&& a.qps.contains(req.qp)
        &&& req.phase is InFlight <==> !a.outcomes.contains_key(r)
        &&& req.phase is Complete ==> a.outcomes[r] == req.phase->0
        &&& (req.phase is Complete || req.detached) ==> req.waker is None
    } by {
        lemma_req_at(parts, exposed, r);
        let (i, o) = device_req(parts, r)->0;
        assert(parts[i].abs.inv_requests());
        assert(parts[i].abs.inv_wakers());
        lemma_device_qps(parts, a.requests[r].qp);
    }
    assert(a.inv_requests());
    assert(a.inv_wakers());
    assert forall|l: LeaseId| #[trigger] a.leases.contains_key(l) implies l < a.next_id by {}
    assert forall|r: ReqId| #[trigger] a.requests.contains_key(r) implies r < a.next_id by {}
    assert forall|r: ReqId| #[trigger] a.outcomes.contains_key(r) implies r < a.next_id by {}
    assert(a.inv_ids());
    assert(abs_inv(a));
    // The shape.
    assert(!a.leases.contains_key(0) && !a.requests.contains_key(0));
    assert forall|x: nat| #[trigger] a.leases.contains_key(x) implies !a.requests.contains_key(x) by {
        lemma_lease_at(parts, exposed, x);
        if x < parts_end(parts) {
            lemma_name(parts, x);
        }
    }
    assert forall|r: ReqId| #[trigger] a.requests.contains_key(r) && a.requests[r].phase is Complete
        implies !a.requests[r].detached by {
        lemma_req_at(parts, exposed, r);
        let (i, o) = device_req(parts, r)->0;
        assert(part_ok(parts[i], m));
    }
    assert forall|l: LeaseId| #[trigger] a.leases.contains_key(l) && a.leases[l].holder is Request
        implies l < a.leases[l].holder->req by {
        lemma_lease_at(parts, exposed, l);
        let (i, o) = (part_of(parts, l), offset_of(parts, l) as nat);
        let w = width(parts) as int;
        assert(1 + w * i + o < 1 + w * i + bound(parts) + parts[i].abs.leases[o].holder->req);
    }
    assert forall|l: LeaseId| #[trigger] a.leases.contains_key(l) && !a.leases[l].unique
        implies a.leases[l].mem.subset_of(bytes) by {
        lemma_lease_at(parts, exposed, l);
        let (i, o) = (part_of(parts, l), offset_of(parts, l) as nat);
        assert forall|x: int| a.leases[l].mem.contains(x) implies bytes.contains(x) by {
            assert(parts[i].abs.leases[o] == parts[i].abs.leases[o]);
        }
    }
    assert forall|l: LeaseId| #[trigger] a.leases.contains_key(l) && a.leases[l].unique
        implies a.leases[l].mem.disjoint(bytes) by {
        lemma_lease_at(parts, exposed, l);
        assert forall|x: int| !(a.leases[l].mem.contains(x) && bytes.contains(x)) by {
            if a.leases[l].mem.contains(x) && bytes.contains(x) {
                let (i2, l2) = choose|i2: int, l2: LeaseId| #![trigger parts[i2].abs.leases[l2]]
                    0 <= i2 < parts.len() && lent(parts[i2].abs, l2) && !parts[i2].abs.leases[l2].unique
                        && parts[i2].abs.leases[l2].mem.contains(x);
                if l < parts_end(parts) {
                    let (i, o) = (part_of(parts, l), offset_of(parts, l) as nat);
                    if i == i2 {
                        assert(parts[i].abs.inv_unique());
                        assert(parts[i].abs.leases[o].mem.disjoint(parts[i].abs.leases[l2].mem));
                    } else {
                        assert(parts_apart(parts[i], parts[i2]));
                        assert(parts[i].abs.leases[o].mem.disjoint(parts[i2].abs.leases[l2].mem));
                    }
                } else {
                    let j = l - parts_end(parts);
                    assert(parts[i2].abs.leases[l2].mem.disjoint(exposed[j]));
                }
            }
        }
    }
}

/// The device's state of A is coupled to M: every part's requests, on its share, and the exposed
/// buffers for the regions open to a peer.
proof fn lemma_device_coupled(parts: Seq<EnginePart>, exposed: Seq<ISet<int>>, m: Nic::State, wrs: IMap<WqeRef, SendWr>)
    requires
        device_ok(parts, exposed, m),
        records_wrs(m, wrs),
    ensures
        coupled(device_abs(parts, exposed), m, device_ranges(parts, exposed), wrs),
{
    let a = device_abs(parts, exposed);
    let ranges = device_ranges(parts, exposed);
    lemma_bound(parts);
    assert forall|r: ReqId| #[trigger] a.requests.contains_key(r)
        implies ranges.contains_key(r) && ranges[r].qp == a.requests[r].qp by {
        lemma_req_at(parts, exposed, r);
        let (i, o) = device_req(parts, r)->0;
        assert(part_ok(parts[i], m));
    }
    assert forall|r: ReqId| #[trigger] ranges.contains_key(r) implies ranges[r].wf() by {
        lemma_name(parts, r);
        let (i, o) = device_req(parts, r)->0;
        assert(part_ok(parts[i], m));
    }
    assert forall|r1: ReqId, r2: ReqId| #![trigger ranges[r1], ranges[r2]]
        ranges.contains_key(r1) && ranges.contains_key(r2) && r1 != r2
        implies ranges[r1].disjoint(ranges[r2]) by {
        lemma_name(parts, r1);
        lemma_name(parts, r2);
        let (i1, o1) = device_req(parts, r1)->0;
        let (i2, o2) = device_req(parts, r2)->0;
        assert(part_ok(parts[i1], m) && part_ok(parts[i2], m));
        if i1 == i2 {
            assert(o1 != o2);
        } else {
            assert(disjoint(parts[i1].share, parts[i2].share));
            assert(parts[i1].share.owns.contains(parts[i1].ranges[o1].qp));
            assert(parts[i2].share.owns.contains(parts[i2].ranges[o2].qp));
        }
    }
    assert forall|r: ReqId| #[trigger] a.in_flight(r) implies ranges[r].pending(m) by {
        lemma_req_at(parts, exposed, r);
        let (i, o) = device_req(parts, r)->0;
        assert(part_ok(parts[i], m));
        assert(parts[i].abs.in_flight(o));
    }
    assert forall|r: ReqId| #[trigger] a.outcomes.contains_key(r) implies {
        &&& ranges[r].all_retired(m.retired)
        &&& ranges[r].aggregate(m.retired) == a.outcomes[r]
        &&& ranges[r].no_success_after_error(m.retired)
    } by {
        lemma_req_at(parts, exposed, r);
        let (i, o) = device_req(parts, r)->0;
        assert(part_ok(parts[i], m));
        assert(abs_inv(parts[i].abs));
        assert(parts[i].abs.inv_requests());
        assert(parts[i].abs.outcomes.contains_key(o));
    }
    // What the NIC reads, A gives it.
    assert forall|x: int| #[trigger] m.nic_may_read(x) implies a.nic_reads(x) by {
        if exists|q: QpId, k: int| #![trigger m.qps[q].sq[k]] m.qps.contains_key(q)
            && 0 <= k < m.qps[q].sq.len() && {
            let wr = m.qps[q].sq[k].wr;
            !wr.inline && !wr.opcode.writes_local() && covers(wr.sges, x)
        } {
            let (q, k) = choose|q: QpId, k: int| #![trigger m.qps[q].sq[k]] m.qps.contains_key(q)
                && 0 <= k < m.qps[q].sq.len() && {
                let wr = m.qps[q].sq[k].wr;
                !wr.inline && !wr.opcode.writes_local() && covers(wr.sges, x)
            };
            let i = choose|i: int| 0 <= i < parts.len() && #[trigger] parts[i].share.owns.contains(q);
            assert(part_ok(parts[i], m));
            assert(inside_reads(m, parts[i].share, x));
            let pa = parts[i].abs;
            let l = choose|l: LeaseId| #[trigger] pa.leases.contains_key(l) && pa.nic_reads_lease(l) && pa.leases[l].mem.contains(x);
            assert(pa.leases[l].holder is Request);
            lemma_names_of(parts, exposed, i);
            let id = lease_name(parts, i, l);
            assert(lent(pa, l));
            assert(a.leases.contains_key(id) && a.nic_reads_lease(id) && a.leases[id].mem.contains(x));
        } else {
            let key = choose|key: Key| #[trigger] m.mrs.contains_key(key) && m.mrs[key].remote_read
                && m.mrs[key].span.contains(x);
            let j = choose|j: int| 0 <= j < exposed.len() && m.mrs[key].span.bytes().subset_of(#[trigger] exposed[j]);
            assert(m.mrs[key].span.bytes().contains(x));
            let id = exposed_name(parts, j);
            assert(a.leases.contains_key(id) && a.nic_reads_lease(id) && a.leases[id].mem.contains(x));
        }
    }
    // What the NIC writes, A gives it.
    assert forall|x: int| #[trigger] m.nic_may_write(x) implies a.nic_writes(x) by {
        let sq = exists|q: QpId, k: int| #![trigger m.qps[q].sq[k]] m.qps.contains_key(q)
            && 0 <= k < m.qps[q].sq.len() && {
            let wr = m.qps[q].sq[k].wr;
            wr.opcode.writes_local() && covers(wr.sges, x)
        };
        let rq = exists|q: QpId, k: int| #![trigger m.qps[q].rq[k]] m.qps.contains_key(q)
            && 0 <= k < m.qps[q].rq.len() && covers(m.qps[q].rq[k].wr.sges, x);
        if sq || rq {
            let q = if sq {
                let (q, k) = choose|q: QpId, k: int| #![trigger m.qps[q].sq[k]] m.qps.contains_key(q)
                    && 0 <= k < m.qps[q].sq.len() && {
                    let wr = m.qps[q].sq[k].wr;
                    wr.opcode.writes_local() && covers(wr.sges, x)
                };
                q
            } else {
                let (q, k) = choose|q: QpId, k: int| #![trigger m.qps[q].rq[k]] m.qps.contains_key(q)
                    && 0 <= k < m.qps[q].rq.len() && covers(m.qps[q].rq[k].wr.sges, x);
                q
            };
            let i = choose|i: int| 0 <= i < parts.len() && #[trigger] parts[i].share.owns.contains(q);
            assert(part_ok(parts[i], m));
            assert(inside_writes(m, parts[i].share, x));
            let pa = parts[i].abs;
            let l = choose|l: LeaseId| #[trigger] pa.leases.contains_key(l) && pa.nic_writes_lease(l) && pa.leases[l].mem.contains(x);
            assert(pa.leases[l].holder is Request);
            lemma_names_of(parts, exposed, i);
            let id = lease_name(parts, i, l);
            assert(lent(pa, l));
            assert(a.leases.contains_key(id) && a.nic_writes_lease(id) && a.leases[id].mem.contains(x));
        } else {
            let key = choose|key: Key| #[trigger] m.mrs.contains_key(key) && m.mrs[key].remote_write
                && m.mrs[key].span.contains(x);
            let j = choose|j: int| 0 <= j < exposed.len() && m.mrs[key].span.bytes().subset_of(#[trigger] exposed[j]);
            assert(m.mrs[key].span.bytes().contains(x));
            let id = exposed_name(parts, j);
            assert(a.leases.contains_key(id) && a.nic_writes_lease(id) && a.leases[id].mem.contains(x));
        }
    }
}

/// **Theorem 1, of a device.** Engines coupled to M on disjoint shares, whose requests' buffers are
/// apart where one is unique, and buffers exposed to a peer, apart from those and from each other,
/// with nothing else in M: they stand for one state of A, reached from its start, in which each
/// engine's requests are as its own A has them, each lease of theirs with its bytes, uniqueness and
/// direction, and each exposed buffer is open to the peer. That state is coupled to M, so S1, F1
/// and R1 hold against M for the whole device.
pub proof fn theorem_1_device(parts: Seq<EnginePart>, exposed: Seq<ISet<int>>, m: Nic::State, wrs: IMap<WqeRef, SendWr>)
    requires
        device_ok(parts, exposed, m),
        records_wrs(m, wrs),
    ensures
        abs_reaches(abs_start(), device_abs(parts, exposed)),
        abs_inv(device_abs(parts, exposed)),
        coupled(device_abs(parts, exposed), m, device_ranges(parts, exposed), wrs),
        forall|i: int, r: ReqId| 0 <= i < parts.len() && #[trigger] parts[i].abs.requests.contains_key(r) ==> {
            let a = device_abs(parts, exposed);
            &&& a.requests.contains_key(req_name(parts, i, r))
            &&& a.requests[req_name(parts, i, r)] == parts[i].abs.requests[r]
            &&& device_ranges(parts, exposed)[req_name(parts, i, r)] == parts[i].ranges[r]
        },
        forall|i: int, l: LeaseId| 0 <= i < parts.len() && #[trigger] lent(parts[i].abs, l) ==> {
            let a = device_abs(parts, exposed);
            let lease = parts[i].abs.leases[l];
            &&& a.leases.contains_key(lease_name(parts, i, l))
            &&& a.leases[lease_name(parts, i, l)].mem == lease.mem
            &&& a.leases[lease_name(parts, i, l)].unique == lease.unique
            &&& a.leases[lease_name(parts, i, l)].holder == (Holder::Request {
                req: req_name(parts, i, lease.holder->req),
                dir: lease.holder->dir,
            })
        },
        forall|j: int| 0 <= j < exposed.len() ==> {
            let a = device_abs(parts, exposed);
            &&& a.leases.contains_key(#[trigger] exposed_name(parts, j))
            &&& a.leases[exposed_name(parts, j)].mem == exposed[j]
            &&& a.leases[exposed_name(parts, j)].holder == (Holder::Exposed { remote_write: true })
        },
{
    lemma_device_assembled(parts, exposed, m);
    lemma_assembled(device_abs(parts, exposed), shared_bytes(parts));
    lemma_device_coupled(parts, exposed, m, wrs);
    assert forall|i: int, r: ReqId| 0 <= i < parts.len() && #[trigger] parts[i].abs.requests.contains_key(r) implies {
        let a = device_abs(parts, exposed);
        &&& a.requests.contains_key(req_name(parts, i, r))
        &&& a.requests[req_name(parts, i, r)] == parts[i].abs.requests[r]
        &&& device_ranges(parts, exposed)[req_name(parts, i, r)] == parts[i].ranges[r]
    } by {
        assert(part_ok(parts[i], m));
        lemma_names_of(parts, exposed, i);
    }
    assert forall|i: int, l: LeaseId| 0 <= i < parts.len() && #[trigger] lent(parts[i].abs, l) implies {
        let a = device_abs(parts, exposed);
        let lease = parts[i].abs.leases[l];
        &&& a.leases.contains_key(lease_name(parts, i, l))
        &&& a.leases[lease_name(parts, i, l)].mem == lease.mem
        &&& a.leases[lease_name(parts, i, l)].unique == lease.unique
        &&& a.leases[lease_name(parts, i, l)].holder == (Holder::Request {
            req: req_name(parts, i, lease.holder->req),
            dir: lease.holder->dir,
        })
    } by {
        assert(part_ok(parts[i], m));
        lemma_names_of(parts, exposed, i);
    }
    assert forall|j: int| 0 <= j < exposed.len() implies {
        let a = device_abs(parts, exposed);
        &&& a.leases.contains_key(#[trigger] exposed_name(parts, j))
        &&& a.leases[exposed_name(parts, j)].mem == exposed[j]
        &&& a.leases[exposed_name(parts, j)].holder == (Holder::Exposed { remote_write: true })
    } by {
        lemma_bound(parts);
        assert(device_names(parts, exposed).contains(exposed_name(parts, j)));
    }
}

} // verus!
