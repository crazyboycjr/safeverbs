//! The guarantees, as lemmas about A (the abstract machine in [`crate::abs`]).
//!
//! Every lemma assumes `s.invariant()`, which holds in every state A can reach (its
//! invariants are inductive: `abs.rs` proves each transition preserves them). Two-state
//! lemmas talk about any single step `Abs::State::next(pre, post)`, whichever action it is.
//!
//! What a guarantee says of WQEs is stated against M in [`crate::refinement`]: what an outcome
//! means for a request's WQEs ([`crate::refinement::corollary_f1`]) and a refusal
//! ([`crate::refinement::corollary_f1_refused`]), and that the NIC is done with a lease that
//! left its request ([`crate::refinement::corollary_r1`]). S2 (no local misuse) is not a
//! property of A either: it relates the library to M and is part of the refinement theorem.
#![allow(unused_imports)]

use crate::abs::*;
use crate::types::*;
use vstd::prelude::*;

verus! {

// =========================================================================================
// S1: no undefined behaviour from the NIC.

/// **S1.** No byte is touched by the program and the NIC (or the peer) in conflicting ways:
/// whatever the program may write, the NIC may neither read nor write; whatever the program
/// may read, the NIC may not write. So the NIC never races with the program.
pub proof fn lemma_s1_no_conflict(s: Abs::State, a: int)
    requires
        s.invariant(),
    ensures
        !(s.program_writes(a) && (s.nic_reads(a) || s.nic_writes(a))),
        !(s.program_reads(a) && s.nic_writes(a)),
{
    if s.program_writes(a) && (s.nic_reads(a) || s.nic_writes(a)) {
        let k = choose|k: LeaseId| s.leases.contains_key(k) && s.is_program(k)
            && s.leases[k].unique && s.leases[k].mem.contains(a);
        let l = choose|l: LeaseId| s.leases.contains_key(l)
            && (s.nic_reads_lease(l) || s.nic_writes_lease(l)) && s.leases[l].mem.contains(a);
        assert(k != l);
        assert(s.leases[k].mem.disjoint(s.leases[l].mem));
    }
    if s.program_reads(a) && s.nic_writes(a) {
        let k = choose|k: LeaseId| s.leases.contains_key(k) && s.is_program(k)
            && s.leases[k].mem.contains(a);
        let l = choose|l: LeaseId| s.leases.contains_key(l) && s.nic_writes_lease(l)
            && s.leases[l].mem.contains(a);
        lemma_s1_nic_writes_any_bytes(s, l);
        assert(k != l);
        assert(s.leases[k].mem.disjoint(s.leases[l].mem));
    }
}

/// **S1, types.** Every lease the NIC or the peer may write is unique and accepts arbitrary
/// bytes. Other leases of the same request may be shared sources.
pub proof fn lemma_s1_nic_writes_any_bytes(s: Abs::State, l: LeaseId)
    requires
        s.invariant(),
        s.leases.contains_key(l),
        s.nic_writes_lease(l),
    ensures
        s.leases[l].any_bytes,
        s.leases[l].unique,
{
}

// =========================================================================================
// F1: at most one outcome per request, handed back with its own leases.

/// **F1, one outcome.** Once the NIC produced an outcome for a request, no step changes or
/// removes it. A request no longer held (taken, or released after a detached completion) never
/// comes back: names are never reused.
pub proof fn lemma_f1_one_outcome(pre: Abs::State, post: Abs::State, r: ReqId)
    requires
        pre.invariant(),
        Abs::State::next(pre, post),
    ensures
        pre.outcomes.contains_key(r) ==> post.outcomes.contains_key(r)
            && post.outcomes[r] == pre.outcomes[r],
        r < pre.next_id && !pre.requests.contains_key(r) ==> !post.requests.contains_key(r),
{
    reveal(Abs::State::next);
    reveal(Abs::State::next_by);
}

/// **F1, its own outcome and leases.** When the client takes request `r`'s outcome, it is the
/// outcome the NIC produced for `r`. Each lease of `r`, which are those it was posted with
/// ([`lemma_r1`]), comes back to the program under its own name, with its bytes and permissions,
/// and no other lease moves.
pub proof fn lemma_f1_take(pre: Abs::State, post: Abs::State, r: ReqId, o: Outcome)
    requires
        pre.invariant(),
        Abs::State::take(pre, post, r, o),
    ensures
        o == pre.outcomes[r],
        !post.requests.contains_key(r),
        forall|l: LeaseId| #[trigger] post.leases.contains_key(l) <==> pre.leases.contains_key(l),
        forall|l: LeaseId| #[trigger] pre.leases.contains_key(l) && pre.leases[l].held_by(r)
            ==> post.is_program(l) && post.leases[l].same_as(pre.leases[l]),
        forall|l: LeaseId| #[trigger] pre.leases.contains_key(l) && !pre.leases[l].held_by(r)
            ==> post.leases[l] == pre.leases[l],
{
}

// =========================================================================================
// R1: the library never loses track of a buffer.

/// The moves a lease may make. The program may put it in a request, lend it to the peer or
/// release it. The peer only gives it back. A request in flight keeps it until it completes,
/// which releases it if the request has no owner left. A complete request gives it back to the
/// program, or releases it if its future is dropped. Released is for good.
pub open spec fn allowed_move(from: Place, to: Place) -> bool {
    match (from, to) {
        (Place::Program, Place::InFlight(_)) => true,
        (Place::Program, Place::Exposed) => true,
        (Place::Program, Place::Released) => true,
        (Place::Exposed, Place::Program) => true,
        (Place::InFlight(r), Place::Completed(s)) => r == s,
        (Place::InFlight(_), Place::Released) => true,
        (Place::Completed(_), Place::Program) => true,
        (Place::Completed(_), Place::Released) => true,
        _ => from == to,
    }
}

/// **R1.** At every moment a lease is in exactly one place ([`Abs::State::place`], made
/// meaningful by the invariant `inv_places`). Every step leaves it there or moves it along
/// [`allowed_move`]; a new lease starts with the program. It enters a request only as that
/// request is posted, and leaves a request in flight only once the NIC produced that request's
/// outcome: dropping a future does not release it, and neither does a device that never
/// flushes, which leaves it in flight for good. While the program does not hold a lease, its
/// bytes and permissions stay as they are.
pub proof fn lemma_r1(pre: Abs::State, post: Abs::State, l: LeaseId)
    requires
        pre.invariant(),
        Abs::State::next(pre, post),
    ensures
        l < pre.next_id ==> allowed_move(pre.place(l), post.place(l)),
        l >= pre.next_id && post.leases.contains_key(l) ==> post.place(l) is Program,
        post.place(l) matches Place::InFlight(r) ==> post.place(l) == pre.place(l)
            || r == pre.next_id,
        pre.place(l) matches Place::InFlight(r) ==> post.place(l) == pre.place(l)
            || post.outcomes.contains_key(r),
        pre.leases.contains_key(l) && !(pre.place(l) is Program) && post.leases.contains_key(l)
            ==> post.leases[l].same_as(pre.leases[l]),
{
    reveal(Abs::State::next);
    reveal(Abs::State::next_by);
}

// =========================================================================================
// L1: no lost wakeup.

/// **L1.** A complete request holds no waker: whatever waker its future registered was
/// invoked when it completed (next lemma), or was dropped because the future was.
pub proof fn lemma_l1_no_waker_left(s: Abs::State, r: ReqId)
    requires
        s.invariant(),
        s.requests.contains_key(r),
        s.requests[r].phase is Complete,
    ensures
        s.requests[r].waker is None,
{
}

/// **L1, the wakeup.** Completing a request whose future registered waker `w` invokes `w`.
pub proof fn lemma_l1_completion_wakes(pre: Abs::State, post: Abs::State, r: ReqId, o: Outcome)
    requires
        pre.invariant(),
        Abs::State::complete(pre, post, r, o),
        pre.requests[r].waker is Some,
    ensures
        post.wakes == pre.wakes.push(pre.requests[r].waker->0),
{
}

} // verus!
