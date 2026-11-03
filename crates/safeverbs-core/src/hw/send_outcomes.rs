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


/// Keep operation validity local to the proof that consumes an executed WR.
#[verifier::opaque]
pub(super) open spec fn valid_tail(last: SendWr, outcome: safeverbs_spec::types::Outcome) -> bool {
    last.opcode.allows_outcome(total_len(last.sges), outcome)
}

/// Once a native tail is retired, M retains its operation-defined metadata.
/// This is derived history, not an assumption about arbitrary retired-map entries.
#[verifier::opaque]
pub(super) open spec fn tail_history<L>(
    r: Ring<L>, tails: Map<int, SendWr>, m: Nic::State, q: QpId,
) -> bool {
    &&& m.qps.contains_key(q)
    &&& forall|s: int| #[trigger] r.meta(s) is Some && r.meta(s)->0.end <= m.qps[q].sq_head ==> {
        let last = WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Send,
            seq: (r.meta(s)->0.end - 1) as nat };
        &&& tails.contains_key(s)
        &&& m.retired.contains_key(last)
        &&& valid_tail(tails[s], m.retired[last])
    }
}

pub(super) proof fn lemma_history_empty<L>(r: Ring<L>, m: Nic::State, q: QpId)
    requires m.qps.contains_key(q), forall|s: int| #[trigger] r.meta(s) is None,
    ensures tail_history(r, Map::empty(), m, q),
{ reveal(tail_history); }

pub(super) proof fn lemma_history_framed<L>(
    r: Ring<L>, tails: Map<int, SendWr>, m: Nic::State, n: Nic::State, q: QpId,
)
    requires tail_history(r, tails, m, q), n.qps.contains_key(q),
        n.qps[q].sq_head == m.qps[q].sq_head,
        forall|w: WqeRef| w.qp == q && w.queue is Send && w.seq < m.qps[q].sq_head
            && #[trigger] m.retired.contains_key(w) ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w],
        forall|s: int| #[trigger] r.meta(s) is Some ==> r.meta(s)->0.first < r.meta(s)->0.end,
    ensures tail_history(r, tails, n, q),
{ reveal(tail_history); }

pub(super) proof fn lemma_history_kept<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, m: Nic::State, q: QpId,
)
    requires tail_history(pre, tails, m, q),
        forall|s: int| #[trigger] post.meta(s) is Some ==> pre.meta(s) is Some
            && post.meta(s)->0.end == pre.meta(s)->0.end,
    ensures tail_history(post, tails, m, q),
{ reveal(tail_history); }

pub(super) proof fn lemma_history_added<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, added: int, last: SendWr, m: Nic::State, q: QpId,
)
    requires tail_history(pre, tails, m, q),
        post.meta(added) is Some, post.meta(added)->0.end > m.qps[q].sq_head,
        forall|s: int| #[trigger] post.meta(s) is Some && s != added ==> pre.meta(s) is Some
            && post.meta(s)->0.end == pre.meta(s)->0.end,
    ensures tail_history(post, tails.insert(added, last), m, q),
{ reveal(tail_history); }

/// A real SQ CQE transfers executed WR metadata into M's retired prefix.
pub(super) proof fn lemma_history_polled<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, index: u16,
    m: Nic::State, n: Nic::State, q: QpId, written: Seq<SendWr>,
)
    requires tail_history(pre, tails, m, q), tail_bound(pre, tails, index, m.qps[q], written),
        queued_outcomes_ok(m, q), n.qps.contains_key(q),
        m.qps[q].sq_head <= n.qps[q].sq_head <= m.qps[q].sq_head + m.qps[q].sq.len(),
        forall|w: WqeRef| w.qp == q && w.queue is Send && w.seq < m.qps[q].sq_head
            && #[trigger] m.retired.contains_key(w) ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w],
        forall|k: int| 0 <= k < n.qps[q].sq_head - m.qps[q].sq_head ==> {
            let slot = #[trigger] m.qps[q].sq[k];
            let w = WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Send, seq: (m.qps[q].sq_head + k) as nat };
            &&& slot.outcome is Some
            &&& n.retired.contains_key(w) && n.retired[w] == slot.outcome->0
        },
        forall|s: int| #[trigger] post.meta(s) is Some ==> pre.meta(s) is Some
            && post.meta(s)->0.end == pre.meta(s)->0.end,
        m.qps[q].sq_head + m.qps[q].sq.len() == pre.spec_wqe_committed(),
    ensures tail_history(post, tails, n, q),
{
    reveal(tail_history);
    reveal(valid_tail);
    assert forall|s: int| #[trigger] post.meta(s) is Some && post.meta(s)->0.end <= n.qps[q].sq_head implies {
        let last = WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Send, seq: (post.meta(s)->0.end - 1) as nat };
        &&& tails.contains_key(s)
        &&& n.retired.contains_key(last)
        &&& valid_tail(tails[s], n.retired[last])
    } by {
        let end = pre.meta(s)->0.end as int;
        let w = WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Send, seq: (end - 1) as nat };
        if end <= m.qps[q].sq_head {
            assert(m.retired.contains_key(w));
        } else {
            let k = end - 1 - m.qps[q].sq_head;
            assert(m.qps[q].sq[k].wr == tails[s]);
            assert(m.qps[q].sq[k].outcome is Some);
        }
    }
}


/// Isolate M's poll update so the worker need not unfold retired maps globally.
pub(super) proof fn lemma_history_poll<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, index: u16,
    m: Nic::State, n: Nic::State, q: QpId, written: Seq<SendWr>, c: CqId,
)
    requires nic_inv(m), nic_step(m, n, Nic::Step::poll_cq(c)),
        m.cqs[c].entries[0].qp == q,
        tail_history(pre, tails, m, q), tail_bound(pre, tails, index, m.qps[q], written),
        queued_outcomes_ok(m, q), m.qps[q].sq_head + m.qps[q].sq.len() == pre.spec_wqe_committed(),
        forall|s: int| #[trigger] post.meta(s) is Some ==> pre.meta(s) is Some
            && post.meta(s)->0.end == pre.meta(s)->0.end,
    ensures tail_history(post, tails, n, q),
{
    let e = m.cqs[c].entries[0];
    let qp = m.qps[q];
    lemma_poll(m, n, c);
    assert forall|w: WqeRef| w.qp == q && w.queue is Send && w.seq < qp.sq_head
        && #[trigger] m.retired.contains_key(w)
        implies n.retired.contains_key(w) && n.retired[w] == m.retired[w] by { reveal(retires); }
    if e.queue is Send {
        assert forall|k: int| 0 <= k < n.qps[q].sq_head - qp.sq_head implies {
            let slot = #[trigger] qp.sq[k];
            let w = WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Send, seq: (qp.sq_head + k) as nat };
            &&& slot.outcome is Some
            &&& n.retired.contains_key(w) && n.retired[w] == slot.outcome->0
        } by { reveal(retires); }
        lemma_history_polled(pre, post, tails, index, m, n, q, written);
    } else {
        lemma_history_framed(pre, tails, m, n, q);
        lemma_history_kept(pre, post, tails, n, q);
    }
}


/// A successful actual retired tail has exactly the native inferred metadata.
pub(super) proof fn lemma_inferred_success<L>(
    r: Ring<L>, tails: Map<int, SendWr>, m: Nic::State, q: QpId, s: int,
)
    requires send_implied(r, tails), tail_history(r, tails, m, q),
        r.meta(s) is Some, r.meta(s)->0.first < r.meta(s)->0.end,
        r.meta(s)->0.end <= m.qps[q].sq_head,
        m.retired[WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Send,
            seq: (r.meta(s)->0.end - 1) as nat }].status is Success,
    ensures m.retired.contains_key(WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Send,
            seq: (r.meta(s)->0.end - 1) as nat }),
        r.meta(s)->0.implied.outcome() == m.retired[WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Send,
            seq: (r.meta(s)->0.end - 1) as nat }],
{
    reveal(tail_history);
    reveal(valid_tail);
    let last = WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Send, seq: (r.meta(s)->0.end - 1) as nat };
    let expected = r.meta(s)->0.implied.outcome();
    let actual = m.retired[last];
    if tails[s].opcode is Read {
        assert(expected.kind is Read && actual.kind is Read);
    } else {
        assert(expected.kind is StatusOnly && actual.kind is StatusOnly);
    }
    assert(expected == actual);
}

} // verus!
