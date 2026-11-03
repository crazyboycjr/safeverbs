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

/// The recorded final WR is the actual last native WQE, in M or not yet rung.
pub(super) open spec fn tail_bound<L>(
    r: Ring<L>, tails: Map<int, SendWr>, index: u16, qp: Qp, written: Seq<SendWr>,
) -> bool {
    forall|s: int| #[trigger] r.meta(s) is Some ==> {
        let meta = r.meta(s)->0;
        let last = meta.end as int - 1;
        &&& meta.first < meta.end
        &&& tails.contains_key(s)
        &&& tails[s].wr_id == spec_wr_id(index, false, last as u64)
        &&& r.req(s) is Some ==> meta.end <= r.spec_wqe_committed()
        &&& qp.sq_head <= last < r.spec_wqe_committed() ==> {
            &&& 0 <= last - qp.sq_head < qp.sq.len()
            &&& qp.sq[last - qp.sq_head].wr == tails[s]
        }
        &&& r.spec_wqe_committed() <= last ==> {
            &&& 0 <= last - r.spec_wqe_committed() < written.len()
            &&& written[last - r.spec_wqe_committed()] == tails[s]
        }
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


/// Advancing M's head or removing native results preserves every surviving tail.
pub(super) proof fn lemma_bound_kept<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, index: u16,
    qp: Qp, next: Qp, written: Seq<SendWr>,
)
    requires tail_bound(pre, tails, index, qp, written),
        post.spec_wqe_committed() == pre.spec_wqe_committed(),
        next.sq_head >= qp.sq_head,
        forall|j: int| 0 <= j < next.sq.len() ==> next.sq_head + j < qp.sq_head + qp.sq.len()
            && (#[trigger] next.sq[j]).wr == qp.sq[next.sq_head + j - qp.sq_head].wr,
        next.sq_head + next.sq.len() == qp.sq_head + qp.sq.len(),
        forall|s: int| #[trigger] post.meta(s) is Some ==> pre.meta(s) is Some
            && post.meta(s)->0.first == pre.meta(s)->0.first && post.meta(s)->0.end == pre.meta(s)->0.end,
        forall|s: int| #[trigger] post.req(s) is Some ==> pre.req(s) is Some,
    ensures tail_bound(post, tails, index, next, written),
{
    assert forall|s: int| #[trigger] post.meta(s) is Some implies {
        let meta = post.meta(s)->0;
        let last = meta.end as int - 1;
        &&& meta.first < meta.end
        &&& tails.contains_key(s)
        &&& tails[s].wr_id == spec_wr_id(index, false, last as u64)
        &&& post.req(s) is Some ==> meta.end <= post.spec_wqe_committed()
        &&& next.sq_head <= last < post.spec_wqe_committed() ==> {
            &&& 0 <= last - next.sq_head < next.sq.len()
            &&& next.sq[last - next.sq_head].wr == tails[s]
        }
        &&& post.spec_wqe_committed() <= last ==> {
            &&& 0 <= last - post.spec_wqe_committed() < written.len()
            &&& written[last - post.spec_wqe_committed()] == tails[s]
        }
    } by {
        let last = post.meta(s)->0.end as int - 1;
        if next.sq_head <= last < post.spec_wqe_committed() {
            assert(qp.sq[last - qp.sq_head].wr == tails[s]);
            assert(next.sq[last - next.sq_head].wr == qp.sq[last - qp.sq_head].wr);
        }
    }
}

/// Once all staged requests are refused, no surviving tail needs written WRs.
pub(super) proof fn lemma_bound_no_written<L>(
    r: Ring<L>, tails: Map<int, SendWr>, index: u16, qp: Qp, written: Seq<SendWr>,
)
    requires r.inv(), tail_bound(r, tails, index, qp, written),
        forall|s: int| #[trigger] r.staged(s) is None,
    ensures tail_bound(r, tails, index, qp, Seq::empty()),
{
    assert forall|s: int| #[trigger] r.meta(s) is Some implies r.meta(s)->0.end <= r.spec_wqe_committed() by {
        r.lemma_meta(s);
    }
}

/// The newly closed tail is the exact last WR just appended to the provider builder.
pub(super) proof fn lemma_bound_added<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, index: u16, qp: Qp,
    written: Seq<SendWr>, more: Seq<SendWr>, added: int, last: SendWr,
)
    requires tail_bound(pre, tails, index, qp, written),
        post.spec_wqe_committed() == pre.spec_wqe_committed(), more == written.push(last),
        post.meta(added) is Some, post.req(added) is None,
        post.meta(added)->0.first < post.meta(added)->0.end,
        post.meta(added)->0.end == post.spec_wqe_committed() + more.len(),
        last.wr_id == spec_wr_id(index, false, (post.meta(added)->0.end as int - 1) as u64),
        forall|s: int| #[trigger] post.meta(s) is Some && s != added ==> pre.meta(s) is Some
            && post.meta(s)->0.first == pre.meta(s)->0.first && post.meta(s)->0.end == pre.meta(s)->0.end,
        forall|s: int| #[trigger] post.req(s) is Some ==> pre.req(s) is Some,
    ensures tail_bound(post, tails.insert(added, last), index, qp, more),
{}

/// The doorbell transfers written tails into M without changing their WR values.
pub(super) proof fn lemma_bound_committed<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, index: u16,
    qp: Qp, next: Qp, written: Seq<SendWr>,
)
    requires pre.inv(), post.inv(), tail_bound(pre, tails, index, qp, written),
        qp.sq_head + qp.sq.len() == pre.spec_wqe_committed(),
        next.sq_head == qp.sq_head, next.sq == qp.sq + written.map_values(|wr: SendWr| SendSlot { wr, outcome: None }),
        post.spec_wqe_committed() == pre.spec_wqe_committed() + written.len(),
        forall|s: int| #[trigger] post.meta(s) == pre.meta(s),
    ensures tail_bound(post, tails, index, next, Seq::empty()),
{
    assert forall|s: int| #[trigger] post.meta(s) is Some implies {
        let meta = post.meta(s)->0;
        let last = meta.end as int - 1;
        &&& meta.first < meta.end
        &&& tails.contains_key(s)
        &&& tails[s].wr_id == spec_wr_id(index, false, last as u64)
        &&& post.req(s) is Some ==> meta.end <= post.spec_wqe_committed()
        &&& next.sq_head <= last < post.spec_wqe_committed() ==> {
            &&& 0 <= last - next.sq_head < next.sq.len()
            &&& next.sq[last - next.sq_head].wr == tails[s]
        }
        &&& post.spec_wqe_committed() <= last ==> {
            &&& 0 <= last - post.spec_wqe_committed() < Seq::<SendWr>::empty().len()
            &&& Seq::<SendWr>::empty()[last - post.spec_wqe_committed()] == tails[s]
        }
    } by {
        pre.lemma_meta(s);
        let last = post.meta(s)->0.end as int - 1;
        if pre.spec_wqe_committed() <= last {
            assert(next.sq[last - next.sq_head].wr == written[last - pre.spec_wqe_committed()]);
        }
    }
}

} // verus!
