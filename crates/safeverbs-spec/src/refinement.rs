//! The refinement theorem: under M, for any peer, the library behaves like A.
//!
//! The library's trusted core (`safeverbs-core`) will be verified against this module. Its
//! state, rings, buffers and ghost state included, implements [`Refines`]: an invariant, an
//! abstraction function to A, its view of the NIC in M, and which WQE carries each request.
//! The core proves the three obligations of the trait; [`theorem_1`] then gives the
//! guarantees along every execution.
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

/// The coupling between A and M that the core's invariant must imply.
///
/// - **F1 against M:** every request has its own WQE (`wqe`, injective); while the request
///   is in flight its WQE is posted and not retired; once it has an outcome in A, that is
///   exactly the outcome M recorded when it retired the WQE.
/// - **S1 against M:** every byte M lets the NIC (or the peer) read or write is one A gives
///   to the NIC for that kind of access.
pub open spec fn coupled(a: Abs::State, m: Nic::State, wqe: Map<Id, WqeRef>) -> bool {
    &&& forall|r: Id| #[trigger] a.leases.contains_key(r) && a.leases[r].is_request() ==> wqe.contains_key(r)
    &&& forall|r: Id| #[trigger] a.outcomes.contains_key(r) ==> wqe.contains_key(r)
    &&& forall|r1: Id, r2: Id| #![trigger wqe[r1], wqe[r2]]
        wqe.contains_key(r1) && wqe.contains_key(r2) && r1 != r2 ==> wqe[r1] != wqe[r2]
    &&& forall|r: Id| #[trigger] a.leases.contains_key(r) && a.leases[r].in_flight()
        ==> posted(m, wqe[r]) && !m.retired.contains_key(wqe[r])
    &&& forall|r: Id| #[trigger] a.outcomes.contains_key(r)
        ==> m.retired.contains_key(wqe[r]) && m.retired[wqe[r]] == a.outcomes[r]
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

    /// The WQE that carries each request (ghost).
    spec fn wqe(&self) -> Map<Id, WqeRef>;

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
            coupled(c.abs(), c.nic(), c.wqe()),
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
/// - A and M are coupled ([`coupled`]): each request's outcome is the one M produced for its
///   WQE, and the NIC touches only memory A gives it ([`corollary_s1`], [`corollary_f1`]).
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
            &&& coupled(c.abs(), c.nic(), c.wqe())
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
            &&& coupled(c.abs(), c.nic(), c.wqe())
        } by {
            if i < n {
                assert(run[i] == prefix[i]);
            }
        }
    }
}

/// **S1 against M.** In a coupled state, nothing the program may write is touched by the NIC
/// or the peer as M defines their reach, and nothing the program may read is written by them.
pub proof fn corollary_s1(a: Abs::State, m: Nic::State, wqe: Map<Id, WqeRef>, x: int)
    requires
        abs_inv(a),
        coupled(a, m, wqe),
    ensures
        !(a.program_writes(x) && (m.nic_may_read(x) || m.nic_may_write(x))),
        !(a.program_reads(x) && m.nic_may_write(x)),
{
    crate::guarantees::lemma_s1_no_conflict(a, x);
}

/// **F1 against M.** The outcome the client takes for request `r` is the outcome M recorded
/// for `r`'s own WQE, and no other request has that WQE.
pub proof fn corollary_f1(
    a: Abs::State,
    a2: Abs::State,
    m: Nic::State,
    wqe: Map<Id, WqeRef>,
    r: Id,
    o: Outcome,
)
    requires
        abs_inv(a),
        coupled(a, m, wqe),
        Abs::State::take(a, a2, r, o),
    ensures
        wqe.contains_key(r),
        m.retired.contains_key(wqe[r]) && o == m.retired[wqe[r]],
        forall|r2: Id| #[trigger] wqe.contains_key(r2) && r2 != r ==> wqe[r2] != wqe[r],
{
    crate::guarantees::lemma_f1_take(a, a2, r, o);
    assert(a.outcomes.contains_key(r));
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
