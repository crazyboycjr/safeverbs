//! The refinement theorem: under M, for any peer, the library behaves like A.
//!
//! The library's trusted core (`safeverbs-core`) will be verified against this module. Its
//! state, rings, buffers and ghost state included, must implement [`Refines`]: an invariant,
//! an abstraction function to A, its view of the NIC in M, and each request's WQE interval.
//! The core proves the three obligations of the trait; [`theorem_1`] then gives the
//! guarantees along every execution.
//!
//! A batch owns a nonempty interval on one QP and queue. Retiring a proper prefix leaves
//! the request in flight with its leases; completion waits for the last WQE. Its outcome
//! is the first failing WQE's outcome, or the last WQE's success if none failed. This is
//! an obligation on the implementation, not a restriction added to M. No concrete worker
//! implements this trait yet; these lemmas do not establish that missing refinement or
//! the byte-permission and inter-worker composition proofs.
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
//! **Theorem 2** (stated, not yet proved). Two endpoints both run safeverbs' protocol layer
//! (credit-based flow control for sends, rkey leases for one-sided access), each refining A
//! under M, with the network the only nondeterminism left. Assume-guarantee: each side
//! assumes the other honours the credits and leases it granted and guarantees the same.
//! Then no completion is `RnrRetryExceeded` or `RemoteErr`, and every error completion is
//! attributable to a fault step of M (link down, peer crash, retry timeout).
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

/// A request's WQEs on one QP and queue, in posting order: `[first, end)`.
pub struct WqeRange {
    pub qp: QpId,
    pub queue: Queue,
    pub first: nat,
    pub end: nat,
}

impl WqeRange {
    /// Empty intervals cannot carry a request or supply its final outcome.
    pub open spec fn wf(self) -> bool {
        self.first < self.end
    }

    pub open spec fn at(self, seq: nat) -> WqeRef {
        WqeRef { qp: self.qp, queue: self.queue, seq }
    }

    pub open spec fn last(self) -> WqeRef {
        self.at((self.end - 1) as nat)
    }

    pub open spec fn contains(self, w: WqeRef) -> bool {
        w.qp == self.qp && w.queue == self.queue && self.first <= w.seq < self.end
    }

    /// Adjacent intervals are disjoint; equal sequence numbers on different queues or
    /// QPs name different WQEs.
    pub open spec fn disjoint(self, other: WqeRange) -> bool {
        self.qp != other.qp || self.queue != other.queue
            || self.end <= other.first || other.end <= self.first
    }

    pub open spec fn singleton(w: WqeRef) -> WqeRange {
        WqeRange { qp: w.qp, queue: w.queue, first: w.seq, end: w.seq + 1 }
    }

    pub open spec fn all_retired(self, retired: IMap<WqeRef, Outcome>) -> bool {
        forall|s: nat| self.first <= s < self.end ==> retired.contains_key(#[trigger] self.at(s))
    }

    /// The aggregate is used only for a well-formed, fully retired interval. Scanning
    /// in posting order keeps the first error, including its defined metadata; if all
    /// WQEs succeeded, the final WQE supplies the success (no byte counts are summed).
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

    pub open spec fn first_error(self, retired: IMap<WqeRef, Outcome>, s: nat) -> bool {
        &&& self.first <= s < self.end
        &&& retired[self.at(s)].status.is_err()
        &&& forall|j: nat| self.first <= j < s ==> (#[trigger] retired[self.at(j)]).status is Success
    }

    /// At an API boundary the retired prefix ends at `cut`, and the entire remaining
    /// suffix is still posted. In particular, the last WQE has not retired. A prefix
    /// may already contain an error without making the request complete in A.
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

/// Disjoint ownership rules out a shared WQE, rather than merely different endpoints.
pub proof fn lemma_disjoint_wqe(left: WqeRange, right: WqeRange, w: WqeRef)
    requires
        left.disjoint(right),
    ensures
        !(left.contains(w) && right.contains(w)),
{
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

/// A single WQE has exactly the former outcome and outstanding-WQE semantics.
pub proof fn lemma_singleton(m: Nic::State, w: WqeRef)
    ensures
        WqeRange::singleton(w).wf(),
        WqeRange::singleton(w).last() == w,
        WqeRange::singleton(w).all_retired(m.retired) <==> m.retired.contains_key(w),
        m.retired.contains_key(w) ==> WqeRange::singleton(w).aggregate(m.retired) == m.retired[w],
        WqeRange::singleton(w).pending(m) <==> posted(m, w) && !m.retired.contains_key(w),
{
    let range = WqeRange::singleton(w);
    if m.retired.contains_key(w) {
        assert forall|s: nat| range.first <= s < range.end
            implies m.retired.contains_key(#[trigger] range.at(s)) by {
            assert(s == w.seq);
        }
    }
    if range.all_retired(m.retired) {
        assert(m.retired.contains_key(range.at(w.seq)));
    }
    if posted(m, w) && !m.retired.contains_key(w) {
        assert(range.pending_at(m, w.seq));
    }
    if range.pending(m) {
        let cut = choose|cut: nat| range.pending_at(m, cut);
        assert(cut == w.seq);
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
    assert(cut <= (range.end - 1) as nat < range.end);
}

/// Polling a CQE inside a proper prefix advances the retirement cut without completing
/// the batch. This follows from M's existing poll rule, for either queue, without changing M.
pub proof fn lemma_poll_prefix_pending(
    pre: Nic::State,
    post: Nic::State,
    c: CqId,
    range: WqeRange,
    cut: nat,
)
    requires
        range.pending_at(pre, cut),
        Nic::State::poll_cq(pre, post, c),
        pre.cqs[c].entries[0].qp == range.qp,
        pre.cqs[c].entries[0].queue == range.queue,
        cut <= pre.cqs[c].entries[0].seq < range.end - 1,
    ensures
        range.pending_at(post, pre.cqs[c].entries[0].seq + 1),
        !range.all_retired(post.retired),
{
    let e = pre.cqs[c].entries[0];
    let qp = pre.qps[range.qp];
    let next = e.seq + 1;
    assert(posted(pre, range.at(cut)));
    assert(posted(pre, range.last()));
    reveal(retires);
    match range.queue {
        Queue::Send => {
            assert(qp.sq_head <= cut <= e.seq < qp.sq_head + qp.sq.len());
            assert(post.qps[range.qp].sq_head == next);
            assert(post.qps[range.qp].sq.len() == qp.sq.len() - (next - qp.sq_head));
        }
        Queue::Recv => {
            assert(qp.rq_head <= cut <= e.seq < qp.rq_head + qp.rq.len());
            assert(post.qps[range.qp].rq_head == next);
            assert(post.qps[range.qp].rq.len() == qp.rq.len() - (next - qp.rq_head));
        }
    }
    assert forall|s: nat| range.first <= s < next implies {
        let w = #[trigger] range.at(s);
        post.retired.contains_key(w) && !posted(post, w)
    } by {
        if s < cut {
            assert(pre.retired.contains_key(range.at(s)));
        } else {
            assert(retires(qp, e).contains_key(range.at(s)));
        }
    }
    assert forall|s: nat| next <= s < range.end implies {
        let w = #[trigger] range.at(s);
        posted(post, w) && !post.retired.contains_key(w)
    } by {
        assert(posted(pre, range.at(s)));
        assert(!pre.retired.contains_key(range.at(s)));
        assert(!retires(qp, e).contains_key(range.at(s)));
    }
    assert(range.pending_at(post, next));
    lemma_pending_last(range, post);
}

/// The coupling between A and M that the core's invariant must imply.
///
/// - **F1 against M:** every request owns a nonempty WQE interval. Distinct requests own
///   disjoint intervals, including after their outcomes are taken. While in flight, a
///   retired prefix is allowed but the final WQE is outstanding. Completion requires
///   every WQE retired and preserves the first error, or the final success.
/// - **S1 against M:** every byte M lets the NIC (or the peer) read or write is one A gives
///   to the NIC for that kind of access.
pub open spec fn coupled(a: Abs::State, m: Nic::State, ranges: Map<Id, WqeRange>) -> bool {
    &&& forall|r: Id| #[trigger] a.leases.contains_key(r) && a.leases[r].is_request()
        ==> ranges.contains_key(r) && ranges[r].qp == a.req(r).qp
    &&& forall|r: Id| #[trigger] a.outcomes.contains_key(r) ==> ranges.contains_key(r)
    &&& forall|r: Id| #[trigger] ranges.contains_key(r) ==> ranges[r].wf()
    &&& forall|r1: Id, r2: Id| #![trigger ranges[r1], ranges[r2]]
        ranges.contains_key(r1) && ranges.contains_key(r2) && r1 != r2
            ==> ranges[r1].disjoint(ranges[r2])
    &&& forall|r: Id| #[trigger] a.leases.contains_key(r) && a.leases[r].in_flight()
        ==> ranges[r].pending(m)
    &&& forall|r: Id| #[trigger] a.outcomes.contains_key(r)
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
    spec fn ranges(&self) -> Map<Id, WqeRange>;

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
///   only memory A gives it ([`corollary_s1`], [`corollary_f1`]).
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
pub proof fn corollary_s1(a: Abs::State, m: Nic::State, ranges: Map<Id, WqeRange>, x: int)
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
    ranges: Map<Id, WqeRange>,
    r: Id,
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
        forall|r2: Id| #[trigger] ranges.contains_key(r2) && r2 != r
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

/// The former single-WQE completion correspondence is the singleton specialization.
pub proof fn corollary_f1_singleton(
    a: Abs::State,
    a2: Abs::State,
    m: Nic::State,
    ranges: Map<Id, WqeRange>,
    r: Id,
    w: WqeRef,
    o: Outcome,
)
    requires
        abs_inv(a),
        coupled(a, m, ranges),
        ranges.contains_key(r),
        ranges[r] == WqeRange::singleton(w),
        Abs::State::take(a, a2, r, o),
    ensures
        m.retired.contains_key(w),
        o == m.retired[w],
        forall|r2: Id| #[trigger] ranges.contains_key(r2) && r2 != r
            ==> !ranges[r2].contains(w),
{
    corollary_f1(a, a2, m, ranges, r, o);
    lemma_singleton(m, w);
    assert forall|r2: Id| #[trigger] ranges.contains_key(r2) && r2 != r
        implies !ranges[r2].contains(w) by {
        lemma_disjoint_wqe(ranges[r2], ranges[r], w);
    }
}

/// **R1 against M.** Prefix retirement cannot return or release an in-flight request's
/// lease: its last WQE is outstanding and A has not recorded an outcome.
pub proof fn corollary_inflight(
    a: Abs::State,
    m: Nic::State,
    ranges: Map<Id, WqeRange>,
    r: Id,
)
    requires
        abs_inv(a),
        coupled(a, m, ranges),
        a.in_flight(r),
    ensures
        ranges.contains_key(r),
        posted(m, ranges[r].last()),
        !m.retired.contains_key(ranges[r].last()),
        !ranges[r].all_retired(m.retired),
        !a.outcomes.contains_key(r),
        a.leases[r].mem == a.posted[r],
{
    lemma_pending_last(ranges[r], m);
}

// =========================================================================================
// Concrete batch executions (no premises)

/// M can produce two different errors for one batch. Polling the first leaves its last
/// WQE outstanding; polling the last permits completion, retaining RemoteErr over FlushErr.
/// The states start at M's open transition and use its existing verbs and execution rules.
#[verifier::spinoff_prover]
#[verifier::rlimit(30)]
pub proof fn example_batch_first_error() {
    broadcast use vstd::multiset::group_multiset_axioms;
    reveal_with_fuel(total_len, 2);
    let caps = Caps { max_qp_wr: 16, max_sge: 4, max_inline: 64, max_cqe: 64, max_msg: 1024 };
    let remote = Outcome { status: Status::RemoteErr, kind: CompletionKind::StatusOnly, byte_len: 0, imm: None };
    let flush = Outcome { status: Status::FlushErr, ..remote };
    let range = WqeRange { qp: 7, queue: Queue::Send, first: 0, end: 2 };

    let m0 = Nic::State {
        caps,
        pds: Set::empty(),
        mrs: Map::empty(),
        cqs: Map::empty(),
        qps: Map::empty(),
        retired: IMap::empty(),
    };
    assert(Nic::State::open(m0, caps));
    reveal(Nic::State::init);
    reveal(Nic::State::init_by);
    assert(Nic::State::init_by(m0, Nic::Config::open(caps)));
    lemma_nic_init_invariant(m0);

    let m1 = Nic::State { pds: m0.pds.insert(1), ..m0 };
    assert(Nic::State::alloc_pd(m0, m1, 1));
    crate::nic_proofs::lemma_alloc_pd(m0, m1, 1);
    let mr = Mr { pd: 1, span: Span { start: 0, len: 128 }, local_write: true, remote_read: false, remote_write: false };
    let m2 = Nic::State { mrs: m1.mrs.insert(5, mr), ..m1 };
    assert(Nic::State::reg_mr(m1, m2, 5, mr));
    crate::nic_proofs::lemma_reg_mr(m1, m2, 5, mr);
    let cq = Cq {
        cap: 4, entries: Seq::empty(),
        reserved: vstd::multiset::Multiset::empty(), used: vstd::multiset::Multiset::empty(),
    };
    let m3 = Nic::State { cqs: m2.cqs.insert(3, cq), ..m2 };
    assert(Nic::State::create_cq(m2, m3, 3, 4));
    crate::nic_proofs::lemma_create_cq(m2, m3, 3, 4);
    let attr = QpAttr { pd: 1, transport: Transport::Rc, send_cq: 3, recv_cq: 3, sq_depth: 2, rq_depth: 0, max_sge: 1, max_inline: 0 };
    let cqs1 = m3.cqs.insert(3, m3.cqs[3].reserve(7, 2));
    let cqs2 = cqs1.insert(3, cqs1[3].reserve(7, 0));
    crate::nic_proofs::lemma_copies(7, 2);
    assert(cqs2[3].reserved.len() == 2);
    let qp0 = Qp { attr, state: QpState::Reset, sq: Seq::empty(), sq_head: 0, sq_done: 0, rq: Seq::empty(), rq_head: 0, rq_done: 0 };
    let m4 = Nic::State { cqs: cqs2, qps: m3.qps.insert(7, qp0), ..m3 };
    assert(Nic::State::create_qp(m3, m4, 7, attr));
    crate::nic_proofs::lemma_create_qp(m3, m4, 7, attr);
    let m5 = Nic::State { qps: m4.qps.insert(7, qp0.with_state(QpState::Init)), ..m4 };
    assert(Nic::State::modify_qp(m4, m5, 7, QpState::Init));
    crate::nic_proofs::lemma_modify_qp(m4, m5, 7, QpState::Init);
    let m6 = Nic::State { qps: m5.qps.insert(7, qp0.with_state(QpState::Rtr)), ..m5 };
    assert(Nic::State::modify_qp(m5, m6, 7, QpState::Rtr));
    crate::nic_proofs::lemma_modify_qp(m5, m6, 7, QpState::Rtr);
    let qp1 = qp0.with_state(QpState::Rts);
    let m7 = Nic::State { qps: m6.qps.insert(7, qp1), ..m6 };
    assert(Nic::State::modify_qp(m6, m7, 7, QpState::Rts));
    crate::nic_proofs::lemma_modify_qp(m6, m7, 7, QpState::Rts);

    let w1 = SendWr { wr_id: 1, opcode: Opcode::Write, sges: seq![Sge { addr: 0, len: 64, lkey: 5 }], signaled: false, inline: false };
    let w2 = SendWr { wr_id: 2, opcode: Opcode::Write, sges: seq![Sge { addr: 64, len: 64, lkey: 5 }], signaled: true, inline: false };
    let wrs = seq![w1, w2];
    let qp2 = Qp { sq: qp1.sq + wrs.map_values(|wr: SendWr| SendSlot { wr, outcome: None }), ..qp1 };
    let m8 = Nic::State { qps: m7.qps.insert(7, qp2), ..m7 };
    assert(Nic::State::post_send(m7, m8, 7, wrs));
    crate::nic_proofs::lemma_post_send(m7, m8, 7, wrs);
    assert(range.pending_at(m8, 0));

    // Even an unsignaled WQE emits a CQE on error; M moves the QP into Err.
    let qp3 = Qp { sq: qp2.sq.update(0, SendSlot { wr: w1, outcome: Some(remote) }), sq_done: 1, state: QpState::Err, ..qp2 };
    let e1 = Cqe { qp: 7, queue: Queue::Send, wr_id: 1, outcome: remote, seq: 0 };
    let m9 = Nic::State { qps: m8.qps.insert(7, qp3), cqs: m8.cqs.insert(3, m8.cqs[3].push(e1)), ..m8 };
    assert(Nic::State::execute_send(m8, m9, 7, remote));
    crate::nic_proofs::lemma_execute_send(m8, m9, 7, remote);
    assert(range.pending_at(m9, 0));

    let qp4 = Qp { sq: qp3.sq.skip(1), sq_head: 1, sq_done: 0, ..qp3 };
    let m10 = Nic::State {
        qps: m9.qps.insert(7, qp4), cqs: m9.cqs.insert(3, m9.cqs[3].pop()),
        retired: m9.retired.union_prefer_right(retires(qp3, e1)), ..m9
    };
    assert(Nic::State::poll_cq(m9, m10, 3));
    lemma_poll_prefix_pending(m9, m10, 3, range, 0);
    crate::nic::lemma_poll(m9, m10, 3);
    crate::nic_proofs::lemma_poll_cq(m9, m10, 3);
    assert(m10.retired[range.at(0)] == remote);
    assert(range.pending_at(m10, 1));
    assert(!range.all_retired(m10.retired));

    // The final WQE flushes. Only after polling it is the whole interval retired.
    let qp5 = Qp { sq: qp4.sq.update(0, SendSlot { wr: w2, outcome: Some(flush) }), sq_done: 1, ..qp4 };
    let e2 = Cqe { qp: 7, queue: Queue::Send, wr_id: 2, outcome: flush, seq: 1 };
    let m11 = Nic::State { qps: m10.qps.insert(7, qp5), cqs: m10.cqs.insert(3, m10.cqs[3].push(e2)), ..m10 };
    assert(Nic::State::execute_send(m10, m11, 7, flush));
    crate::nic_proofs::lemma_execute_send(m10, m11, 7, flush);
    assert(range.pending_at(m11, 1));
    let qp6 = Qp { sq: qp5.sq.skip(1), sq_head: 2, sq_done: 0, ..qp5 };
    let m12 = Nic::State {
        qps: m11.qps.insert(7, qp6), cqs: m11.cqs.insert(3, m11.cqs[3].pop()),
        retired: m11.retired.union_prefer_right(retires(qp5, e2)), ..m11
    };
    assert(Nic::State::poll_cq(m11, m12, 3));
    crate::nic::lemma_poll(m11, m12, 3);
    crate::nic_proofs::lemma_poll_cq(m11, m12, 3);
    reveal(retires);
    assert(m12.retired[range.at(0)] == remote);
    assert(m12.retired[range.at(1)] == flush);
    assert forall|s: nat| range.first <= s < range.end
        implies m12.retired.contains_key(#[trigger] range.at(s)) by {
        assert(s == 0 || s == 1);
    }
    assert(range.first_error(m12.retired, 0));
    lemma_aggregate_first_error(range, m12.retired, 0);
    assert(range.aggregate(m12.retired) == remote);
    assert(range.aggregate(m12.retired) != m12.retired[range.last()]);
    if range.pending(m12) {
        lemma_pending_last(range, m12);
        assert(!posted(m12, range.last()));
    }
    assert(!range.pending(m12));
}

/// Success keeps the last outcome's metadata, and ownership is by QP, queue and interval.
pub proof fn example_batch_success_and_ownership() {
    let range = WqeRange { qp: 7, queue: Queue::Send, first: 4, end: 6 };
    let ack = Outcome { status: Status::Success, kind: CompletionKind::StatusOnly, byte_len: 0, imm: None };
    let read = Outcome { status: Status::Success, kind: CompletionKind::Read, byte_len: 64, imm: None };
    let retired = IMap::empty().insert(range.at(4), ack).insert(range.at(5), read);
    assert forall|s: nat| range.first <= s < range.end
        implies retired.contains_key(#[trigger] range.at(s)) by {
        assert(s == 4 || s == 5);
    }
    assert forall|s: nat| range.first <= s < range.end
        implies (#[trigger] retired[range.at(s)]).status is Success by {
        assert(s == 4 || s == 5);
    }
    lemma_aggregate_success(range, retired);
    assert(range.aggregate(retired) == read);
    assert(!WqeRange { end: 4, ..range }.wf());
    assert(range.disjoint(WqeRange { first: 6, end: 8, ..range }));
    assert(!range.disjoint(WqeRange { first: 5, end: 8, ..range }));
    assert(range.disjoint(WqeRange { qp: 8, ..range }));
    assert(range.disjoint(WqeRange { queue: Queue::Recv, ..range }));
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
