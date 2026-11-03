//! Actual send requests infer success from the final WR's operation and SGEs.
#![allow(unused_imports)]
use super::*;

verus! {

/// The recorded actual final WR authorizes precisely the native inferred success.
/// Refused results have no metadata and never authorize a reused WQE number.
pub(super) open spec fn send_implied<L>(r: Ring<L>, tails: Map<int, SendWr>) -> bool {
    forall|s: int| #[trigger] r.meta(s) is Some ==> {
        &&& tails.contains_key(s)
        &&& r.meta(s)->0.implied.outcome().status is Success
        &&& tails[s].opcode.allows_outcome(total_len(tails[s].sges), r.meta(s)->0.implied.outcome())
    }
}

pub(super) proof fn lemma_implied_empty<L>(r: Ring<L>)
    requires r.inv(), forall|s: int| #[trigger] r.req(s) is None,
        forall|s: int| #[trigger] r.staged(s) is None,
    ensures send_implied(r, Map::empty()),
{
    assert forall|s: int| #[trigger] r.meta(s) is Some implies false by { r.lemma_meta(s); }
}

/// Preserve the native success while cached errors, ownership or wakers change.
pub(super) proof fn lemma_implied_kept<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, next: Map<int, SendWr>,
)
    requires send_implied(pre, tails),
        forall|s: int| #[trigger] post.meta(s) is Some ==> pre.meta(s) is Some
            && post.meta(s)->0.implied == pre.meta(s)->0.implied && next[s] == tails[s]
            && next.contains_key(s),
    ensures send_implied(post, next),
{}

/// Closing a real request records its last WR and its operation-defined success.
pub(super) proof fn lemma_implied_added<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, added: int, last: SendWr,
)
    requires send_implied(pre, tails),
        post.meta(added) is Some,
        post.meta(added)->0.implied.outcome().status is Success,
        last.opcode.allows_outcome(total_len(last.sges), post.meta(added)->0.implied.outcome()),
        forall|s: int| #[trigger] post.meta(s) is Some && s != added ==> pre.meta(s) is Some
            && post.meta(s)->0.implied == pre.meta(s)->0.implied,
    ensures send_implied(post, tails.insert(added, last)),
{}

} // verus!
