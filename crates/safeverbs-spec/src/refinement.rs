//! The refinement theorem: under M, for any peer, the library behaves like A.
//!
//! To get it, the library's trusted core (`safeverbs-core`) implements [`Refines`] for its
//! whole state, rings, buffers and ghost state included: an invariant, an abstraction
//! function to A, its view of the NIC in M and each request's WQE interval, with the
//! trait's three obligations proved. [`theorem_1`] then gives the guarantees along every
//! execution.
//!
//! A request owns a nonempty interval of WQEs on one QP and queue (a batch owns several).
//! Retiring a proper prefix leaves it in flight with its leases; it completes when its last
//! WQE retires, with the first error in posting order, or the last WQE's success if none
//! failed (so a batch of READs reports the last READ's length). This is an obligation on
//! the implementation, not a restriction added to M.
//!
//! **Why `nic()` is honest.** The core's view of the NIC is a ghost token that only the
//! trusted verbs wrappers change. Each wrapper's contract is M: it *requires* the verb's
//! precondition (`nic_enabled(token, Nic::Step::post_send(..))` and so on) and *ensures* that
//! the token moved by the NIC's own steps ([`nic_env_reaches`]) and then by that verb
//! (`nic_step`). Verus checks every call site against the `requires`; that is S2.
//! [`lemma_nic_env_keeps_enabled`] shows the contract is consistent. Polling reports a CQE
//! exactly as M's `poll_cq` describes it.
//! The wrappers' bodies call rdma-core and are trusted, not verified: they are where M meets
//! the real NIC (see `docs/verification.md`).
//!
//! **Theorem 2**, for two endpoints that both run safeverbs' protocol layer (credit-based
//! flow control for sends, rkey leases for one-sided access), each refining A under M, with
//! the network the only nondeterminism left. Assume-guarantee: each side assumes the other
//! honours the credits and leases it granted and guarantees the same. Then no completion is
//! `RnrRetryExceeded` or `RemoteErr`, and every error completion is attributable to a fault
//! step of M (link down, peer crash, retry timeout).
#![allow(unused_imports)]

use crate::abs::*;
use crate::nic::*;
use crate::types::*;
use vstd::prelude::*;

verus! {

// =========================================================================================
// Executions

/// `run` is an execution of A: each state follows from the one before by one step.
pub open spec fn abs_run(run: Seq<Abs::State>) -> bool {
    forall|i: int| 0 <= i < run.len() - 1 ==> Abs::State::next(#[trigger] run[i], run[i + 1])
}

/// `a` reaches `b` in zero or more steps of A.
pub open spec fn abs_reaches(a: Abs::State, b: Abs::State) -> bool {
    exists|run: Seq<Abs::State>| run.len() > 0 && run[0] == a && run.last() == b && abs_run(run)
}

/// `run` is an execution of M.
pub open spec fn nic_run(run: Seq<Nic::State>) -> bool {
    forall|i: int| 0 <= i < run.len() - 1 ==> Nic::State::next(#[trigger] run[i], run[i + 1])
}

/// `m` reaches `n` in zero or more steps of M (verbs and NIC steps).
pub open spec fn nic_reaches(m: Nic::State, n: Nic::State) -> bool {
    exists|run: Seq<Nic::State>| run.len() > 0 && run[0] == m && run.last() == n && nic_run(run)
}

/// `m` reaches `n` by the NIC's own steps only (executions and faults, no verb): what may
/// happen between two calls of the library.
pub open spec fn nic_env_reaches(m: Nic::State, n: Nic::State) -> bool {
    exists|run: Seq<Nic::State>, steps: Seq<Nic::Step>| nic_env_run(run, steps) && run[0] == m && run.last() == n
}

/// `run` is an execution of M by the NIC steps `steps`.
pub open spec fn nic_env_run(run: Seq<Nic::State>, steps: Seq<Nic::Step>) -> bool {
    &&& run.len() > 0
    &&& steps.len() == run.len() - 1
    &&& forall|i: int| 0 <= i < steps.len()
        ==> is_nic_step(#[trigger] steps[i]) && nic_step(run[i], run[i + 1], steps[i])
}

// =========================================================================================
// How A and M fit together

/// WQE `w` is posted and not yet retired in `m`.
pub open spec fn posted(m: Nic::State, w: WqeRef) -> bool {
    &&& m.qps.contains_key(w.qp)
    &&& match w.queue {
        Queue::Send => m.qps[w.qp].sq_head <= w.seq < m.qps[w.qp].sq_head + m.qps[w.qp].sq.len(),
        Queue::Recv => m.qps[w.qp].rq_head <= w.seq < m.qps[w.qp].rq_head + m.qps[w.qp].rq.len(),
    }
}

/// The WQEs a request owns: `[first, end)` of one queue of one QP, in posting order.
pub struct WqeRange {
    pub qp: QpId,
    pub queue: Queue,
    pub first: nat,
    pub end: nat,
}

impl WqeRange {
    /// A request owns at least one WQE: its last one completes it.
    pub open spec fn wf(self) -> bool {
        self.first < self.end
    }

    /// The WQE of the range's queue with sequence number `seq`.
    pub open spec fn at(self, seq: nat) -> WqeRef {
        WqeRef { qp: self.qp, queue: self.queue, seq }
    }

    pub open spec fn last(self) -> WqeRef {
        self.at((self.end - 1) as nat)
    }

    /// No WQE belongs to both ranges.
    pub open spec fn disjoint(self, other: WqeRange) -> bool {
        self.qp != other.qp || self.queue != other.queue
            || self.end <= other.first || other.end <= self.first
    }

    pub open spec fn all_retired(self, retired: IMap<WqeRef, Outcome>) -> bool {
        forall|s: nat| self.first <= s < self.end ==> retired.contains_key(#[trigger] self.at(s))
    }

    /// The request's outcome once all its WQEs retired: the first error in posting order,
    /// or the last WQE's success if none failed (no lengths are added up).
    pub open spec fn aggregate(self, retired: IMap<WqeRef, Outcome>) -> Outcome
        recommends self.wf(), self.all_retired(retired),
        decreases self.end - self.first,
    {
        let first = retired[self.at(self.first)];
        if self.end <= self.first + 1 || first.status.is_err() {
            first
        } else {
            WqeRange { first: self.first + 1, ..self }.aggregate(retired)
        }
    }

    /// WQE `s` has the range's first error.
    pub open spec fn first_error(self, retired: IMap<WqeRef, Outcome>, s: nat) -> bool {
        &&& self.first <= s < self.end
        &&& retired[self.at(s)].status.is_err()
        &&& forall|j: nat| self.first <= j < s ==> (#[trigger] retired[self.at(j)]).status is Success
    }

    /// The WQEs before `cut` retired and the others, the last among them, are still posted.
    /// A request whose prefix retired, even with an error, is still in flight.
    pub open spec fn pending_at(self, m: Nic::State, cut: nat) -> bool {
        &&& self.wf()
        &&& self.first <= cut < self.end
        &&& forall|s: nat| self.first <= s < cut ==> {
            let w = #[trigger] self.at(s);
            m.retired.contains_key(w) && !posted(m, w)
        }
        &&& forall|s: nat| cut <= s < self.end ==> {
            let w = #[trigger] self.at(s);
            posted(m, w) && !m.retired.contains_key(w)
        }
    }

    pub open spec fn pending(self, m: Nic::State) -> bool {
        exists|cut: nat| self.pending_at(m, cut)
    }
}

/// The aggregate selects the earliest failure in posting order, or the final success.
pub proof fn lemma_aggregate(range: WqeRange, retired: IMap<WqeRef, Outcome>)
    requires
        range.wf(),
        range.all_retired(retired),
    ensures
        range.aggregate(retired).status.is_err() ==> exists|s: nat|
            range.first_error(retired, s) && range.aggregate(retired) == retired[range.at(s)],
        !range.aggregate(retired).status.is_err() ==> {
            &&& forall|s: nat| range.first <= s < range.end
                ==> (#[trigger] retired[range.at(s)]).status is Success
            &&& range.aggregate(retired) == retired[range.last()]
        },
    decreases range.end - range.first,
{
    let first = retired[range.at(range.first)];
    if first.status.is_err() {
        assert(range.first_error(retired, range.first));
    } else if range.end == range.first + 1 {
        assert forall|s: nat| range.first <= s < range.end
            implies (#[trigger] retired[range.at(s)]).status is Success by {
            assert(s == range.first);
        }
    } else {
        let tail = WqeRange { first: range.first + 1, ..range };
        assert forall|s: nat| tail.first <= s < tail.end
            implies retired.contains_key(#[trigger] tail.at(s)) by {
            assert(tail.at(s) == range.at(s));
        }
        lemma_aggregate(tail, retired);
        if tail.aggregate(retired).status.is_err() {
            let s = choose|s: nat| tail.first_error(retired, s)
                && tail.aggregate(retired) == retired[tail.at(s)];
            assert forall|j: nat| range.first <= j < s
                implies (#[trigger] retired[range.at(j)]).status is Success by {
                if j != range.first {
                    assert(tail.first <= j < s);
                    assert(tail.at(j) == range.at(j));
                }
            }
            assert(range.first_error(retired, s));
        } else {
            assert forall|s: nat| range.first <= s < range.end
                implies (#[trigger] retired[range.at(s)]).status is Success by {
                if s != range.first {
                    assert(tail.first <= s < tail.end);
                    assert(tail.at(s) == range.at(s));
                }
            }
            assert(tail.last() == range.last());
        }
    }
}

/// Later completions, including flush errors, cannot overwrite the first failure.
pub proof fn lemma_aggregate_first_error(
    range: WqeRange,
    retired: IMap<WqeRef, Outcome>,
    first_error: nat,
)
    requires
        range.wf(),
        range.all_retired(retired),
        range.first_error(retired, first_error),
    ensures
        range.aggregate(retired) == retired[range.at(first_error)],
    decreases range.end - range.first,
{
    if first_error > range.first {
        assert(retired[range.at(range.first)].status is Success);
        let tail = WqeRange { first: range.first + 1, ..range };
        assert forall|s: nat| tail.first <= s < tail.end
            implies retired.contains_key(#[trigger] tail.at(s)) by {
            assert(tail.at(s) == range.at(s));
        }
        assert forall|s: nat| tail.first <= s < first_error
            implies (#[trigger] retired[tail.at(s)]).status is Success by {
            assert(tail.at(s) == range.at(s));
        }
        lemma_aggregate_first_error(tail, retired, first_error);
    }
}

/// With no error, preserve the final success and its operation-defined metadata.
pub proof fn lemma_aggregate_success(range: WqeRange, retired: IMap<WqeRef, Outcome>)
    requires
        range.wf(),
        range.all_retired(retired),
        forall|s: nat| range.first <= s < range.end
            ==> (#[trigger] retired[range.at(s)]).status is Success,
    ensures
        range.aggregate(retired) == retired[range.last()],
        range.aggregate(retired).status is Success,
{
    lemma_aggregate(range, retired);
    if range.aggregate(retired).status.is_err() {
        let s = choose|s: nat| range.first_error(retired, s)
            && range.aggregate(retired) == retired[range.at(s)];
        assert(retired[range.at(s)].status is Success);
    }
}

/// An in-flight batch still owns its leases even if a proper prefix has retired.
pub proof fn lemma_pending_last(range: WqeRange, m: Nic::State)
    requires
        range.pending(m),
    ensures
        range.wf(),
        posted(m, range.last()),
        !m.retired.contains_key(range.last()),
        !range.all_retired(m.retired),
        forall|s: nat| range.first <= s < range.end ==> {
            let w = #[trigger] range.at(s);
            posted(m, w) || m.retired.contains_key(w)
        },
{
    let cut = choose|cut: nat| range.pending_at(m, cut);
    assert(cut <= ((range.end - 1) as nat) < range.end);
}

/// The coupling between A and M that the core's invariant must imply.
///
/// - **F1 against M:** every request owns a nonempty WQE interval. Distinct requests own
///   disjoint intervals, including after their outcomes are taken. While in flight, a
///   retired prefix is allowed but the final WQE is outstanding. Completion requires
///   every WQE retired and preserves the first error, or the final success.
/// - **S1 against M:** every byte M lets the NIC (or the peer) read or write is one A gives
///   to the NIC for that kind of access.
pub open spec fn coupled(a: Abs::State, m: Nic::State, ranges: Map<ReqId, WqeRange>) -> bool {
    &&& forall|r: ReqId| #[trigger] a.requests.contains_key(r)
        ==> ranges.contains_key(r) && ranges[r].qp == a.requests[r].qp
    &&& forall|r: ReqId| #[trigger] a.outcomes.contains_key(r) ==> ranges.contains_key(r)
    &&& forall|r: ReqId| #[trigger] ranges.contains_key(r) ==> ranges[r].wf()
    &&& forall|r1: ReqId, r2: ReqId| #![trigger ranges[r1], ranges[r2]]
        ranges.contains_key(r1) && ranges.contains_key(r2) && r1 != r2
            ==> ranges[r1].disjoint(ranges[r2])
    &&& forall|r: ReqId| #[trigger] a.in_flight(r) ==> ranges[r].pending(m)
    &&& forall|r: ReqId| #[trigger] a.outcomes.contains_key(r)
        ==> ranges[r].all_retired(m.retired) && ranges[r].aggregate(m.retired) == a.outcomes[r]
    &&& forall|x: int| #[trigger] m.nic_may_read(x) ==> a.nic_reads(x)
    &&& forall|x: int| #[trigger] m.nic_may_write(x) ==> a.nic_writes(x)
}

// =========================================================================================
// The obligations

/// What the core provides and proves. `Self` is the core's whole state (an abstract
/// description of it: the core relates its exec functions to `step`).
pub trait Refines: Sized {
    /// The core's invariant.
    spec fn inv(&self) -> bool;

    /// The abstraction function: the state of A this state stands for.
    spec fn abs(&self) -> Abs::State;

    /// The NIC's state in M, as the core's ghost token holds it.
    spec fn nic(&self) -> Nic::State;

    /// The nonempty WQE interval owned by each request (ghost, retained with its outcome).
    spec fn ranges(&self) -> Map<ReqId, WqeRange>;

    /// A start state: the device just opened, nothing created yet.
    spec fn init(&self) -> bool;

    /// One call of the public API, from `self` to `next`, with whatever the NIC and the peer
    /// did meanwhile.
    spec fn step(&self, next: &Self) -> bool;

    /// Start states satisfy the invariant and stand for start states of A and M.
    proof fn lemma_init(c: Self)
        requires
            c.init(),
        ensures
            c.inv(),
            abs_init(c.abs()),
            nic_init(c.nic()),
    ;

    /// Every call keeps the invariant, is zero or more steps of A, and moves the NIC by
    /// steps of M only (every verb it called met its precondition).
    proof fn lemma_step(c: Self, d: Self)
        requires
            c.inv(),
            c.step(&d),
        ensures
            d.inv(),
            abs_reaches(c.abs(), d.abs()),
            nic_reaches(c.nic(), d.nic()),
    ;

    /// The invariant couples A and M.
    proof fn lemma_coupled(c: Self)
        requires
            c.inv(),
        ensures
            coupled(c.abs(), c.nic(), c.ranges()),
    ;
}

// =========================================================================================
// Theorem 1

/// **Theorem 1.** Take any execution of the core: a start state, then any sequence of API
/// calls, with the NIC and the peer doing whatever M allows in between. At every point:
///
/// - the state stands for a state A reaches from its start, so A's invariants hold and with
///   them S1, F1, R1 and L1 (the lemmas of [`crate::guarantees`]);
/// - the NIC is in a state M reaches from its start by legal steps only: every verb the
///   library called met its precondition (**S2**), and M's own invariants hold (no CQ
///   overflow, every CQE names its WQE);
/// - A and M are coupled ([`coupled`]): each request owns a disjoint WQE interval, its
///   outcome aggregates M's outcomes only after all its WQEs retire, and the NIC touches
///   only memory A gives it ([`corollary_s1`], [`corollary_f1`], [`corollary_r1`]).
pub proof fn theorem_1<C: Refines>(run: Seq<C>)
    requires
        run.len() > 0,
        run[0].init(),
        forall|i: int| 0 <= i < run.len() - 1 ==> (#[trigger] run[i]).step(&run[i + 1]),
    ensures
        abs_init(run[0].abs()),
        nic_init(run[0].nic()),
        forall|i: int| 0 <= i < run.len() ==> {
            let c = #[trigger] run[i];
            &&& c.inv()
            &&& abs_reaches(run[0].abs(), c.abs()) && abs_inv(c.abs())
            &&& nic_reaches(run[0].nic(), c.nic()) && nic_inv(c.nic())
            &&& coupled(c.abs(), c.nic(), c.ranges())
        },
    decreases run.len(),
{
    C::lemma_init(run[0]);
    let a0 = run[0].abs();
    let m0 = run[0].nic();
    reveal(Abs::State::init);
    reveal(Nic::State::init);
    lemma_abs_init_invariant(a0);
    lemma_nic_init_invariant(m0);
    if run.len() == 1 {
        lemma_abs_reaches_refl(a0);
        lemma_nic_reaches_refl(m0);
        C::lemma_coupled(run[0]);
    } else {
        let prefix = run.drop_last();
        assert forall|i: int| 0 <= i < prefix.len() - 1 implies (#[trigger] prefix[i]).step(&prefix[i + 1]) by {
            assert(prefix[i] == run[i] && prefix[i + 1] == run[i + 1]);
        }
        theorem_1(prefix);
        let n = run.len() - 1;
        assert(run[n - 1] == prefix[n - 1]);
        let c = run[n - 1];
        let d = run[n];
        assert(c.step(&d));
        C::lemma_step(c, d);
        C::lemma_coupled(d);
        lemma_abs_reaches_trans(a0, c.abs(), d.abs());
        lemma_nic_reaches_trans(m0, c.nic(), d.nic());
        lemma_abs_reaches_invariant(c.abs(), d.abs());
        lemma_nic_reaches_invariant(c.nic(), d.nic());
        assert forall|i: int| 0 <= i < run.len() implies {
            let c = #[trigger] run[i];
            &&& c.inv()
            &&& abs_reaches(a0, c.abs()) && abs_inv(c.abs())
            &&& nic_reaches(m0, c.nic()) && nic_inv(c.nic())
            &&& coupled(c.abs(), c.nic(), c.ranges())
        } by {
            if i < n {
                assert(run[i] == prefix[i]);
            }
        }
    }
}

/// **S1 against M.** In a coupled state, nothing the program may write is touched by the NIC
/// or the peer as M defines their reach, and nothing the program may read is written by them.
pub proof fn corollary_s1(a: Abs::State, m: Nic::State, ranges: Map<ReqId, WqeRange>, x: int)
    requires
        abs_inv(a),
        coupled(a, m, ranges),
    ensures
        !(a.program_writes(x) && (m.nic_may_read(x) || m.nic_may_write(x))),
        !(a.program_reads(x) && m.nic_may_write(x)),
{
    crate::guarantees::lemma_s1_no_conflict(a, x);
}

/// **F1 against M.** The client takes the aggregate of its own fully retired interval.
/// No other request owns any WQE of that interval. An error is the first one in posting
/// order; success carries the final WQE's defined metadata.
pub proof fn corollary_f1(
    a: Abs::State,
    a2: Abs::State,
    m: Nic::State,
    ranges: Map<ReqId, WqeRange>,
    r: ReqId,
    o: Outcome,
)
    requires
        abs_inv(a),
        coupled(a, m, ranges),
        Abs::State::take(a, a2, r, o),
    ensures
        ranges.contains_key(r),
        ranges[r].wf(),
        ranges[r].all_retired(m.retired),
        o == ranges[r].aggregate(m.retired),
        forall|r2: ReqId| #[trigger] ranges.contains_key(r2) && r2 != r
            ==> ranges[r2].disjoint(ranges[r]),
        o.status.is_err() ==> exists|s: nat|
            ranges[r].first_error(m.retired, s) && o == m.retired[ranges[r].at(s)],
        !o.status.is_err() ==> {
            &&& forall|s: nat| ranges[r].first <= s < ranges[r].end
                ==> (#[trigger] m.retired[ranges[r].at(s)]).status is Success
            &&& o == m.retired[ranges[r].last()]
        },
{
    crate::guarantees::lemma_f1_take(a, a2, r, o);
    assert(a.outcomes.contains_key(r));
    lemma_aggregate(ranges[r], m.retired);
}

/// **R1 against M.** A lease in a request in flight has its NIC still at work: the request's
/// last WQE is posted, even if a prefix of its WQEs retired, with an error or not. A lease in
/// a complete request has the NIC done with it: all the request's WQEs retired. With
/// [`crate::guarantees::lemma_r1`], a lease leaves a request, to the program or released, only
/// after that.
pub proof fn corollary_r1(a: Abs::State, m: Nic::State, ranges: Map<ReqId, WqeRange>, l: LeaseId)
    requires
        abs_inv(a),
        coupled(a, m, ranges),
    ensures
        a.place(l) matches Place::InFlight(r) ==> {
            &&& posted(m, ranges[r].last())
            &&& !m.retired.contains_key(ranges[r].last())
            &&& !ranges[r].all_retired(m.retired)
            &&& !a.outcomes.contains_key(r)
        },
        a.place(l) matches Place::Completed(r) ==> ranges[r].all_retired(m.retired),
{
    match a.place(l) {
        Place::InFlight(r) => lemma_pending_last(ranges[r], m),
        Place::Completed(r) => assert(a.outcomes.contains_key(r)),
        _ => {},
    }
}

// =========================================================================================
// Executions: reflexive, transitive, and they keep the invariants (for the core's proofs too)

/// One step of A is an execution of A.
pub proof fn lemma_abs_reaches_next(a: Abs::State, b: Abs::State)
    requires
        abs_next(a, b),
    ensures
        abs_reaches(a, b),
{
    let run = seq![a, b];
    assert(run.last() == b);
    assert(abs_run(run));
}

/// One step of M is an execution of M.
pub proof fn lemma_nic_reaches_next(a: Nic::State, b: Nic::State)
    requires
        nic_next(a, b),
    ensures
        nic_reaches(a, b),
{
    let run = seq![a, b];
    assert(run.last() == b);
    assert(nic_run(run));
}

/// What the NIC does by itself between two calls keeps M's invariants and every verb's
/// precondition. This is what makes the trusted wrappers' contracts consistent: a wrapper
/// requires the precondition in the state the library last saw and ensures the verb
/// happened after whatever NIC steps came first.
pub proof fn lemma_nic_env_keeps_enabled(m: Nic::State, n: Nic::State, verb: Nic::Step)
    requires
        nic_inv(m),
        nic_env_reaches(m, n),
        is_verb(verb),
        nic_enabled(m, verb),
    ensures
        nic_inv(n),
        nic_enabled(n, verb),
        nic_reaches(m, n),
{
    let (run, steps) = choose|run: Seq<Nic::State>, steps: Seq<Nic::Step>|
        nic_env_run(run, steps) && run[0] == m && run.last() == n;
    lemma_nic_env_run(run, steps, verb);
    assert forall|i: int| 0 <= i < run.len() - 1 implies Nic::State::next(#[trigger] run[i], run[i + 1]) by {
        crate::nic::lemma_nic_step_is_next(run[i], run[i + 1], steps[i]);
    }
}

proof fn lemma_nic_env_run(run: Seq<Nic::State>, steps: Seq<Nic::Step>, verb: Nic::Step)
    requires
        nic_env_run(run, steps),
        nic_inv(run[0]),
        is_verb(verb),
        nic_enabled(run[0], verb),
    ensures
        nic_inv(run.last()),
        nic_enabled(run.last(), verb),
    decreases run.len(),
{
    if run.len() > 1 {
        let (pr, ps) = (run.drop_last(), steps.drop_last());
        assert forall|i: int| 0 <= i < ps.len() implies is_nic_step(#[trigger] ps[i]) && nic_step(pr[i], pr[i + 1], ps[i]) by {
            assert(ps[i] == steps[i] && pr[i] == run[i] && pr[i + 1] == run[i + 1]);
        }
        lemma_nic_env_run(pr, ps, verb);
        let k = steps.len() - 1;
        assert(pr.last() == run[k]);
        assert(nic_step(run[k], run[k + 1], steps[k]));
        crate::nic::lemma_nic_steps_keep_verbs_enabled(run[k], run[k + 1], steps[k], verb);
        crate::nic::lemma_nic_step_is_next(run[k], run[k + 1], steps[k]);
        crate::nic::lemma_step_keeps_invariant(run[k], run[k + 1]);
    }
}

pub proof fn lemma_abs_init_invariant(a: Abs::State)
    requires
        abs_init(a),
    ensures
        abs_inv(a),
{
    reveal(Abs::State::init);
    reveal(Abs::State::init_by);
    Abs::State::start_inductive(a);
}

pub proof fn lemma_nic_init_invariant(m: Nic::State)
    requires
        nic_init(m),
    ensures
        nic_inv(m),
{
    reveal(Nic::State::init);
    reveal(Nic::State::init_by);
    let caps = choose|caps: Caps| Nic::State::open(m, caps);
    Nic::State::open_inductive(m, caps);
}

pub proof fn lemma_abs_reaches_refl(a: Abs::State)
    ensures
        abs_reaches(a, a),
{
    let run = seq![a];
    assert(run.last() == a);
}

pub proof fn lemma_nic_reaches_refl(m: Nic::State)
    ensures
        nic_reaches(m, m),
{
    let run = seq![m];
    assert(run.last() == m);
}

pub proof fn lemma_abs_reaches_trans(a: Abs::State, b: Abs::State, c: Abs::State)
    requires
        abs_reaches(a, b),
        abs_reaches(b, c),
    ensures
        abs_reaches(a, c),
{
    let r1 = choose|run: Seq<Abs::State>| run.len() > 0 && run[0] == a && run.last() == b && abs_run(run);
    let r2 = choose|run: Seq<Abs::State>| run.len() > 0 && run[0] == b && run.last() == c && abs_run(run);
    let run = r1 + r2.drop_first();
    assert forall|i: int| 0 <= i < run.len() - 1 implies Abs::State::next(#[trigger] run[i], run[i + 1]) by {
        if i < r1.len() - 1 {
            assert(run[i] == r1[i] && run[i + 1] == r1[i + 1]);
        } else {
            let j = i - r1.len() + 1;
            assert(run[i] == r2[j] && run[i + 1] == r2[j + 1]);
        }
    }
    assert(run.last() == c);
}

pub proof fn lemma_nic_reaches_trans(a: Nic::State, b: Nic::State, c: Nic::State)
    requires
        nic_reaches(a, b),
        nic_reaches(b, c),
    ensures
        nic_reaches(a, c),
{
    let r1 = choose|run: Seq<Nic::State>| run.len() > 0 && run[0] == a && run.last() == b && nic_run(run);
    let r2 = choose|run: Seq<Nic::State>| run.len() > 0 && run[0] == b && run.last() == c && nic_run(run);
    let run = r1 + r2.drop_first();
    assert forall|i: int| 0 <= i < run.len() - 1 implies Nic::State::next(#[trigger] run[i], run[i + 1]) by {
        if i < r1.len() - 1 {
            assert(run[i] == r1[i] && run[i + 1] == r1[i + 1]);
        } else {
            let j = i - r1.len() + 1;
            assert(run[i] == r2[j] && run[i + 1] == r2[j + 1]);
        }
    }
    assert(run.last() == c);
}

pub proof fn lemma_abs_run_invariant(run: Seq<Abs::State>)
    requires
        run.len() > 0,
        run[0].invariant(),
        abs_run(run),
    ensures
        run.last().invariant(),
    decreases run.len(),
{
    if run.len() > 1 {
        let prefix = run.drop_last();
        assert forall|i: int| 0 <= i < prefix.len() - 1 implies Abs::State::next(#[trigger] prefix[i], prefix[i + 1]) by {
            assert(prefix[i] == run[i] && prefix[i + 1] == run[i + 1]);
        }
        lemma_abs_run_invariant(prefix);
        assert(Abs::State::next(run[run.len() - 2], run[run.len() - 1]));
        crate::abs::lemma_step_keeps_invariant(run[run.len() - 2], run.last());
    }
}

pub proof fn lemma_nic_run_invariant(run: Seq<Nic::State>)
    requires
        run.len() > 0,
        run[0].invariant(),
        nic_run(run),
    ensures
        run.last().invariant(),
    decreases run.len(),
{
    if run.len() > 1 {
        let prefix = run.drop_last();
        assert forall|i: int| 0 <= i < prefix.len() - 1 implies Nic::State::next(#[trigger] prefix[i], prefix[i + 1]) by {
            assert(prefix[i] == run[i] && prefix[i + 1] == run[i + 1]);
        }
        lemma_nic_run_invariant(prefix);
        assert(Nic::State::next(run[run.len() - 2], run[run.len() - 1]));
        crate::nic::lemma_step_keeps_invariant(run[run.len() - 2], run.last());
    }
}

pub proof fn lemma_abs_reaches_invariant(a: Abs::State, b: Abs::State)
    requires
        abs_inv(a),
        abs_reaches(a, b),
    ensures
        abs_inv(b),
{
    let run = choose|run: Seq<Abs::State>| run.len() > 0 && run[0] == a && run.last() == b && abs_run(run);
    lemma_abs_run_invariant(run);
}

pub proof fn lemma_nic_reaches_invariant(a: Nic::State, b: Nic::State)
    requires
        nic_inv(a),
        nic_reaches(a, b),
    ensures
        nic_inv(b),
{
    let run = choose|run: Seq<Nic::State>| run.len() > 0 && run[0] == a && run.last() == b && nic_run(run);
    lemma_nic_run_invariant(run);
}

} // verus!
