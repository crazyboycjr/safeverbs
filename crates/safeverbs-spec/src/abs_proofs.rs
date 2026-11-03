//! Proofs that A's invariants are inductive.
//!
//! Nothing here is needed to read A; `abs.rs` calls these lemmas from its `#[inductive]`
//! proofs. Only joining and separating groups need more than Verus finds by itself.
#![allow(unused_imports)]

use crate::abs::*;
use crate::types::*;
use vstd::prelude::*;

verus! {

/// Joining two compatible groups keeps their leases' identities distinct and every unique
/// lease disjoint from the others.
pub proof fn lemma_join_permissions(left: Group, right: Group)
    requires
        left.wf(), right.wf(), left.compatible(right),
    ensures
        (Group { members: left.members + right.members, holder: Holder::Program }).wf(),
{
    let joined = left.members + right.members;
    assert forall|k: int, l: int| #![trigger joined[k], joined[l]]
        0 <= k < joined.len() && 0 <= l < joined.len() && k != l implies {
            &&& joined[k].id != joined[l].id
            &&& joined[k].unique || joined[l].unique ==> joined[k].mem.disjoint(joined[l].mem)
        } by {
        if k < left.members.len() {
            assert(joined[k] == left.members[k]);
            if l < left.members.len() {
                assert(joined[l] == left.members[l]);
            } else {
                assert(joined[l] == right.members[l - left.members.len()]);
            }
        } else {
            assert(joined[k] == right.members[k - left.members.len()]);
            if l < left.members.len() {
                assert(joined[l] == left.members[l]);
            } else {
                assert(joined[l] == right.members[l - left.members.len()]);
            }
        }
    }
}

/// A group joined from two groups compatible with `other` is compatible with it.
pub proof fn lemma_join_compatible(left: Group, right: Group, other: Group)
    requires
        left.compatible(other), right.compatible(other),
    ensures
        (Group { members: left.members + right.members, holder: Holder::Program }).compatible(other),
        other.compatible(Group { members: left.members + right.members, holder: Holder::Program }),
{
    let joined = left.members + right.members;
    assert forall|k: int, l: int| #![trigger joined[k], other.members[l]]
        0 <= k < joined.len() && 0 <= l < other.members.len() implies {
            &&& joined[k].id != other.members[l].id
            &&& joined[k].unique || other.members[l].unique
                ==> joined[k].mem.disjoint(other.members[l].mem)
        } by {
        if k < left.members.len() { assert(joined[k] == left.members[k]); }
        else { assert(joined[k] == right.members[k - left.members.len()]); }
    }
    assert forall|k: int, l: int| #![trigger other.members[k], joined[l]]
        0 <= k < other.members.len() && 0 <= l < joined.len() implies {
            &&& other.members[k].id != joined[l].id
            &&& other.members[k].unique || joined[l].unique
                ==> other.members[k].mem.disjoint(joined[l].mem)
        } by {
        assert(joined[l].id != other.members[k].id);
        if other.members[k].unique || joined[l].unique {
            assert(joined[l].mem.disjoint(other.members[k].mem));
        }
    }
}

/// The two pieces of a separated group are well formed and compatible with each other, and
/// together they are the group: no lease is duplicated or loses its permissions.
pub proof fn lemma_separate_permissions(group: Group, at: int)
    requires
        group.wf(), 0 <= at <= group.members.len(),
    ensures
        (Group { members: group.members.take(at), holder: Holder::Program }).wf(),
        (Group { members: group.members.skip(at), holder: Holder::Program }).wf(),
        (Group { members: group.members.take(at), holder: Holder::Program }).compatible(
            Group { members: group.members.skip(at), holder: Holder::Program }),
        group.members.take(at) + group.members.skip(at) == group.members,
{
    let left = group.members.take(at);
    let right = group.members.skip(at);
    assert forall|k: int, l: int| #![trigger left[k], left[l]]
        0 <= k < left.len() && 0 <= l < left.len() && k != l implies {
            &&& left[k].id != left[l].id
            &&& left[k].unique || left[l].unique ==> left[k].mem.disjoint(left[l].mem)
        } by { assert(left[k] == group.members[k] && left[l] == group.members[l]); }
    assert forall|k: int, l: int| #![trigger right[k], right[l]]
        0 <= k < right.len() && 0 <= l < right.len() && k != l implies {
            &&& right[k].id != right[l].id
            &&& right[k].unique || right[l].unique ==> right[k].mem.disjoint(right[l].mem)
        } by { assert(right[k] == group.members[k + at] && right[l] == group.members[l + at]); }
    assert forall|k: int, l: int| #![trigger left[k], right[l]]
        0 <= k < left.len() && 0 <= l < right.len() implies {
            &&& left[k].id != right[l].id
            &&& left[k].unique || right[l].unique ==> left[k].mem.disjoint(right[l].mem)
        } by { assert(left[k] == group.members[k] && right[l] == group.members[l + at]); }
    assert(left + right =~= group.members);
}

/// Both pieces of a group compatible with `other` are compatible with it.
pub proof fn lemma_separate_compatible(group: Group, at: int, other: Group)
    requires
        group.compatible(other), 0 <= at <= group.members.len(),
    ensures
        (Group { members: group.members.take(at), holder: Holder::Program }).compatible(other),
        (Group { members: group.members.skip(at), holder: Holder::Program }).compatible(other),
        other.compatible(Group { members: group.members.take(at), holder: Holder::Program }),
        other.compatible(Group { members: group.members.skip(at), holder: Holder::Program }),
{
    let left = group.members.take(at);
    let right = group.members.skip(at);
    assert forall|k: int, l: int| #![trigger left[k], other.members[l]]
        0 <= k < left.len() && 0 <= l < other.members.len() implies {
            &&& left[k].id != other.members[l].id
            &&& left[k].unique || other.members[l].unique ==> left[k].mem.disjoint(other.members[l].mem)
        } by { assert(left[k] == group.members[k]); }
    assert forall|k: int, l: int| #![trigger right[k], other.members[l]]
        0 <= k < right.len() && 0 <= l < other.members.len() implies {
            &&& right[k].id != other.members[l].id
            &&& right[k].unique || other.members[l].unique ==> right[k].mem.disjoint(other.members[l].mem)
        } by { assert(right[k] == group.members[k + at]); }
    assert forall|k: int, l: int| #![trigger other.members[k], left[l]]
        0 <= k < other.members.len() && 0 <= l < left.len() implies {
            &&& other.members[k].id != left[l].id
            &&& other.members[k].unique || left[l].unique ==> other.members[k].mem.disjoint(left[l].mem)
        } by { assert(left[l] == group.members[l]); }
    assert forall|k: int, l: int| #![trigger other.members[k], right[l]]
        0 <= k < other.members.len() && 0 <= l < right.len() implies {
            &&& other.members[k].id != right[l].id
            &&& other.members[k].unique || right[l].unique ==> other.members[k].mem.disjoint(right[l].mem)
        } by { assert(right[l] == group.members[l + at]); }
}

/// Joining two of the program's groups keeps A's invariants.
pub proof fn lemma_join(pre: Abs::State, post: Abs::State, i: Id, j: Id)
    requires
        pre.invariant(),
        Abs::State::join(pre, post, i, j),
    ensures
        post.invariant(),
{
    lemma_join_permissions(pre.groups[i], pre.groups[j]);
    assert forall|g: Id| #[trigger] post.groups.contains_key(g) implies post.groups[g].wf() by {
        if g != pre.next_id { assert(pre.groups.contains_key(g)); }
    }
    assert forall|g: Id, h: Id| #![trigger post.groups[g], post.groups[h]]
        post.groups.contains_key(g) && post.groups.contains_key(h) && g != h
        implies post.groups[g].compatible(post.groups[h]) by {
        if g == pre.next_id {
            assert(h != i && h != j && pre.groups.contains_key(h));
            lemma_join_compatible(pre.groups[i], pre.groups[j], pre.groups[h]);
        } else if h == pre.next_id {
            assert(g != i && g != j && pre.groups.contains_key(g));
            lemma_join_compatible(pre.groups[i], pre.groups[j], pre.groups[g]);
        }
    }
    assert forall|g: Id| #[trigger] post.groups.contains_key(g) implies
        forall|k: int| 0 <= k < post.groups[g].members.len()
            ==> (#[trigger] post.groups[g].members[k]).id < post.next_id by {
        if g == pre.next_id {
            assert forall|k: int| 0 <= k < post.groups[g].members.len()
                implies (#[trigger] post.groups[g].members[k]).id < post.next_id by {
                if k < pre.groups[i].members.len() {
                    assert(post.groups[g].members[k] == pre.groups[i].members[k]);
                } else {
                    assert(post.groups[g].members[k] == pre.groups[j].members[k - pre.groups[i].members.len()]);
                }
            }
        }
    }
}

/// Separating one of the program's groups keeps A's invariants.
pub proof fn lemma_separate(pre: Abs::State, post: Abs::State, i: Id, at: int)
    requires
        pre.invariant(),
        Abs::State::separate(pre, post, i, at),
    ensures
        post.invariant(),
{
    lemma_separate_permissions(pre.groups[i], at);
    assert forall|g: Id| #[trigger] post.groups.contains_key(g) implies post.groups[g].wf() by {
        if g != pre.next_id && g != pre.next_id + 1 { assert(pre.groups.contains_key(g)); }
    }
    assert forall|g: Id, h: Id| #![trigger post.groups[g], post.groups[h]]
        post.groups.contains_key(g) && post.groups.contains_key(h) && g != h
        implies post.groups[g].compatible(post.groups[h]) by {
        if g == pre.next_id || g == pre.next_id + 1 {
            if h != pre.next_id && h != pre.next_id + 1 {
                assert(h != i && pre.groups.contains_key(h));
                lemma_separate_compatible(pre.groups[i], at, pre.groups[h]);
            }
        } else if h == pre.next_id || h == pre.next_id + 1 {
            assert(g != i && pre.groups.contains_key(g));
            lemma_separate_compatible(pre.groups[i], at, pre.groups[g]);
        }
    }
    assert forall|g: Id| #[trigger] post.groups.contains_key(g) implies
        forall|k: int| 0 <= k < post.groups[g].members.len()
            ==> (#[trigger] post.groups[g].members[k]).id < post.next_id by {
        if g == pre.next_id || g == pre.next_id + 1 {
            assert forall|k: int| 0 <= k < post.groups[g].members.len()
                implies (#[trigger] post.groups[g].members[k]).id < post.next_id by {
                if g == pre.next_id {
                    assert(post.groups[g].members[k] == pre.groups[i].members[k]);
                } else {
                    assert(post.groups[g].members[k] == pre.groups[i].members[k + at]);
                }
            }
        }
    }
}

/// Every step keeps A's invariants.
pub proof fn lemma_next(pre: Abs::State, post: Abs::State)
    requires
        pre.invariant(),
        Abs::State::next(pre, post),
    ensures
        post.invariant(),
{
    reveal(Abs::State::next);
    reveal(Abs::State::next_by);
    let step = choose|step: Abs::Step| Abs::State::next_by(pre, post, step);
    match step {
        Abs::Step::empty() => Abs::State::empty_inductive(pre, post),
        Abs::Step::join(i, j) => Abs::State::join_inductive(pre, post, i, j),
        Abs::Step::separate(i, at) => Abs::State::separate_inductive(pre, post, i, at),
        Abs::Step::create_qp(q) => Abs::State::create_qp_inductive(pre, post, q),
        Abs::Step::drop_qp(q) => Abs::State::drop_qp_inductive(pre, post, q),
        Abs::Step::alloc(mem, any_bytes) => Abs::State::alloc_inductive(pre, post, mem, any_bytes),
        Abs::Step::split(i, left) => Abs::State::split_inductive(pre, post, i, left),
        Abs::Step::freeze(i) => Abs::State::freeze_inductive(pre, post, i),
        Abs::Step::share(i, sub) => Abs::State::share_inductive(pre, post, i, sub),
        Abs::Step::thaw(i) => Abs::State::thaw_inductive(pre, post, i),
        Abs::Step::drop(i) => Abs::State::drop_inductive(pre, post, i),
        Abs::Step::post_request(i, q, dirs) => Abs::State::post_request_inductive(pre, post, i, q, dirs),
        Abs::Step::complete(r, o) => Abs::State::complete_inductive(pre, post, r, o),
        Abs::Step::register(r, w) => Abs::State::register_inductive(pre, post, r, w),
        Abs::Step::take(r, o) => Abs::State::take_inductive(pre, post, r, o),
        Abs::Step::detach(r) => Abs::State::detach_inductive(pre, post, r),
        Abs::Step::expose(i, remote_write) => Abs::State::expose_inductive(pre, post, i, remote_write),
        Abs::Step::unexpose(i) => Abs::State::unexpose_inductive(pre, post, i),
        Abs::Step::dummy_to_use_type_params(_) => {},
    }
}

} // verus!
