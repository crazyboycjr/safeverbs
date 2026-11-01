//! The guarantees, as lemmas about A (the abstract machine in [`crate::abs`]).
//!
//! Every lemma assumes `s.invariant()`, which holds in every state A can reach (its
//! invariants are inductive: `abs.rs` proves each transition preserves them). Two-state
//! lemmas talk about any single step `Abs::State::next(pre, post)`, whichever action it is.
//!
//! S2 (no local misuse) is not a property of A: it relates the library to the NIC model M
//! and is part of the refinement theorem ([`crate::refinement`]).
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
        let i = choose|i: Id| #[trigger] s.leases.contains_key(i) && s.leases[i].is_program()
            && s.leases[i].unique && s.leases[i].mem.contains(a);
        let j = if s.nic_reads(a) {
            choose|j: Id| #[trigger] s.leases.contains_key(j) && s.leases[j].nic_may_read()
                && s.leases[j].mem.contains(a)
        } else {
            choose|j: Id| #[trigger] s.leases.contains_key(j) && s.leases[j].nic_may_write()
                && s.leases[j].mem.contains(a)
        };
        assert(s.leases[i].mem.disjoint(s.leases[j].mem));
    }
    if s.program_reads(a) && s.nic_writes(a) {
        let i = choose|i: Id| #[trigger] s.leases.contains_key(i) && s.leases[i].is_program()
            && s.leases[i].mem.contains(a);
        let j = choose|j: Id| #[trigger] s.leases.contains_key(j) && s.leases[j].nic_may_write()
            && s.leases[j].mem.contains(a);
        assert(s.leases[j].mem.disjoint(s.leases[i].mem));
    }
}

/// **S1, types.** The NIC and the peer only write into memory whose element type accepts
/// every bit pattern (`zerocopy::FromBytes`), and that memory reaches the program again only
/// as such a type. Turning bytes into a type with invalid bit patterns goes through
/// `TryFromBytes`, which checks them; that check is ordinary safe Rust, outside this model.
pub proof fn lemma_s1_nic_writes_any_bytes(s: Abs::State, i: Id)
    requires
        s.invariant(),
        s.leases.contains_key(i),
        s.leases[i].nic_may_write(),
    ensures
        s.leases[i].any_bytes,
        s.leases[i].unique,
{
}

// =========================================================================================
// F1: completion correspondence.

/// **F1, one outcome.** Once the NIC produced an outcome for a request, no step changes or
/// removes it: each request gets at most one outcome. (That it gets one at all is liveness:
/// it needs the NIC to finish and someone to poll the CQ.)
pub proof fn lemma_f1_outcome_fixed(pre: Abs::State, post: Abs::State, r: Id)
    requires
        pre.invariant(),
        Abs::State::next(pre, post),
        pre.outcomes.contains_key(r),
    ensures
        post.outcomes.contains_key(r),
        post.outcomes[r] == pre.outcomes[r],
{
    reveal(Abs::State::next);
    reveal(Abs::State::next_by);
}

/// **F1, its own outcome and buffers.** When the client takes request `r`'s outcome, it is
/// the outcome the NIC produced for `r`, and the buffer that comes back is exactly the one
/// `r` was posted with.
pub proof fn lemma_f1_take(pre: Abs::State, post: Abs::State, r: Id, o: Outcome)
    requires
        pre.invariant(),
        Abs::State::take(pre, post, r, o),
    ensures
        o == pre.outcomes[r],
        post.is_program(pre.next_id),
        post.leases[pre.next_id].mem == pre.posted[r],
{
}

/// **F1, taken once.** A request that is no longer held (taken, or released after a detached
/// completion) never becomes a request again: ids are never reused.
pub proof fn lemma_f1_taken_once(pre: Abs::State, post: Abs::State, r: Id)
    requires
        pre.invariant(),
        Abs::State::next(pre, post),
        r < pre.next_id,
        !pre.is_request(r),
    ensures
        !post.is_request(r),
{
    reveal(Abs::State::next);
    reveal(Abs::State::next_by);
}

// =========================================================================================
// R1: no leak, no early release.

/// **R1, never before the NIC is done.** A request in flight keeps its lease, unchanged,
/// until the NIC produces its outcome: no client action (dropping the future included)
/// releases it earlier. With S1 this means its memory is never reused while the NIC may
/// still access it.
pub proof fn lemma_r1_kept_while_in_flight(pre: Abs::State, post: Abs::State, r: Id)
    requires
        pre.invariant(),
        Abs::State::next(pre, post),
        pre.in_flight(r),
    ensures
        post.in_flight(r) ==> post.leases[r].mem == pre.leases[r].mem,
        !post.in_flight(r) ==> post.outcomes.contains_key(r),
{
    reveal(Abs::State::next);
    reveal(Abs::State::next_by);
}

/// **R1, at the latest when its queue pair is dropped.** Every request lives on a queue
/// pair that still exists; dropping a queue pair needs all of its requests gone.
pub proof fn lemma_r1_requests_on_live_qps(s: Abs::State, r: Id)
    requires
        s.invariant(),
        s.is_request(r),
    ensures
        s.qps.contains(s.req(r).qp),
{
}

/// **R1, exactly once.** A lease that was returned or dropped never comes back, so no buffer
/// is returned or dropped twice.
pub proof fn lemma_r1_released_once(pre: Abs::State, post: Abs::State, i: Id)
    requires
        pre.invariant(),
        Abs::State::next(pre, post),
        i < pre.next_id,
        !pre.leases.contains_key(i),
    ensures
        !post.leases.contains_key(i),
{
    reveal(Abs::State::next);
    reveal(Abs::State::next_by);
}

// =========================================================================================
// L1: no lost wakeup.

/// **L1.** A complete request holds no waker: whatever waker its future registered was
/// invoked when it completed (next lemma), or was dropped because the future was.
pub proof fn lemma_l1_no_waker_left(s: Abs::State, r: Id)
    requires
        s.invariant(),
        s.is_request(r),
        s.req(r).phase is Complete,
    ensures
        s.req(r).waker is None,
{
}

/// **L1, the wakeup.** Completing a request whose future registered waker `w` invokes `w`.
pub proof fn lemma_l1_completion_wakes(pre: Abs::State, post: Abs::State, r: Id, o: Outcome)
    requires
        pre.invariant(),
        Abs::State::complete(pre, post, r, o),
        pre.req(r).waker is Some,
    ensures
        post.wakes == pre.wakes.push(pre.req(r).waker->0),
{
}

} // verus!
