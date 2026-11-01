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
        let i = choose|i: Id| s.groups.contains_key(i) && s.groups[i].writes_program(a);
        let k = choose|k: int| 0 <= k < s.groups[i].members.len()
            && s.groups[i].members[k].unique && s.groups[i].members[k].mem.contains(a);
        let j = choose|j: Id| s.groups.contains_key(j)
            && (s.groups[j].nic_may_read(a) || s.groups[j].nic_may_write(a));
        assert(s.groups[j].contains(a));
        let l = choose|l: int| 0 <= l < s.groups[j].members.len()
            && s.groups[j].members[l].mem.contains(a);
        assert(i != j);
        assert(s.groups[i].compatible(s.groups[j]));
        assert(s.groups[i].members[k].mem.disjoint(s.groups[j].members[l].mem));
    }
    if s.program_reads(a) && s.nic_writes(a) {
        let i = choose|i: Id| s.groups.contains_key(i) && s.groups[i].is_program()
            && s.groups[i].contains(a);
        let k = choose|k: int| 0 <= k < s.groups[i].members.len()
            && s.groups[i].members[k].mem.contains(a);
        let j = choose|j: Id| s.groups.contains_key(j) && s.groups[j].nic_may_write(a);
        let l = choose|l: int| 0 <= l < s.groups[j].members.len()
            && s.groups[j].members[l].mem.contains(a) && s.groups[j].write_member(l);
        lemma_s1_nic_writes_any_bytes(s, j, l);
        assert(i != j);
        assert(s.groups[j].compatible(s.groups[i]));
        assert(s.groups[j].members[l].mem.disjoint(s.groups[i].members[k].mem));
    }
}

/// **S1, types.** Every member written by the NIC or peer is individually unique and
/// accepts arbitrary bytes. Other members of the same request may be shared sources.
pub proof fn lemma_s1_nic_writes_any_bytes(s: Abs::State, i: Id, k: int)
    requires
        s.invariant(),
        s.groups.contains_key(i),
        0 <= k < s.groups[i].members.len(),
        s.groups[i].write_member(k),
    ensures
        s.groups[i].members[k].any_bytes,
        s.groups[i].members[k].unique,
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
        post.groups[pre.next_id].members == pre.posted[r],
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

/// Every live member identity has exactly one position in exactly one group. Shared
/// clones may overlap bytes, but cloning never duplicates a resource identity.
pub proof fn lemma_member_owned_once(s: Abs::State, i: Id, j: Id, k: int, l: int)
    requires
        s.invariant(),
        s.groups.contains_key(i), s.groups.contains_key(j),
        0 <= k < s.groups[i].members.len(),
        0 <= l < s.groups[j].members.len(),
        s.groups[i].members[k].id == s.groups[j].members[l].id,
    ensures
        i == j && k == l,
{
    if i != j {
        assert(s.groups[i].compatible(s.groups[j]));
    } else if k != l {
        assert(s.groups[i].wf());
    }
}

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
        post.in_flight(r) ==> post.groups[r].members == pre.groups[r].members,
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
        !pre.groups.contains_key(i),
    ensures
        !post.groups.contains_key(i),
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
