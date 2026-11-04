//! Send results coupled to the WQEs M retired.
//!
//! A send request owns the WQEs `[first, end)` of its QP's send queue: one, or a batch. Its
//! ring slot says what it reports (`RequestMeta::outcome`): the success its last WR implies, for
//! a later CQE that retires it without a completion of its own, or the first error a CQE reported
//! inside that interval, at its position. That is proved against M (`send_results`): while the
//! request is in flight, no completion the ring has seen named a WQE at or after its last, and
//! what it says is the first error among the WQEs of its interval the ring heard of; once it is
//! complete, its outcome is M's aggregate of the interval (`WqeRange::aggregate`), the first
//! error in posting order or else the last WQE's success, which is what its last WR implies:
//! the length that READ asked for, or its status alone.
//! [`Hw::take`] hands back exactly that outcome, and an error's position is that of the first
//! failed WQE of the request (`Hw::send_taken_matches`). So does the first error of a request
//! without an owner, which outlives the request's slot (`Hw::detached_taken_matches`).
#![allow(unused_imports)]
use super::*;
use crate::ring::{DetachedRecord, RequestMeta};
use safeverbs_spec::types::Outcome;

verus! {

/// The recorded final WR authorizes exactly the inferred success.
/// Refused results have no metadata and never authorize a reused WQE number.
pub(super) open spec fn send_implied<L>(r: Ring<L>, tails: Map<int, SendWr>) -> bool {
    forall|s: int| #[trigger] r.meta(s) is Some ==> {
        &&& tails.contains_key(s)
        &&& r.meta(s)->0.outcome matches Ok(done)
            ==> tails[s].opcode.allows_outcome(total_len(tails[s].sges), done.model())
    }
}

/// The recorded final WR is the request's last WQE, in M or not yet rung; once M popped it, M's
/// outcome for it is one that WR allows.
pub(super) open spec fn tail_bound<L>(
    r: Ring<L>, tails: Map<int, SendWr>, qp: Qp, written: Seq<SendWr>,
    retired: IMap<WqeRef, Outcome>, q: QpId,
) -> bool {
    forall|s: int| #[trigger] r.meta(s) is Some ==> {
        let meta = r.meta(s)->0;
        let last = meta.end as int - 1;
        &&& meta.first < meta.end
        &&& tails.contains_key(s)
        &&& r.req(s) is Some ==> meta.end <= r.spec_wqe_committed()
        &&& qp.sq_head <= last < r.spec_wqe_committed() ==> {
            &&& 0 <= last - qp.sq_head < qp.sq.len()
            &&& qp.sq[last - qp.sq_head].wr == tails[s]
        }
        &&& r.spec_wqe_committed() <= last ==> {
            &&& 0 <= last - r.spec_wqe_committed() < written.len()
            &&& written[last - r.spec_wqe_committed()] == tails[s]
        }
        &&& last < qp.sq_head ==> {
            let w = WqeRef { qp: q, queue: Queue::Send, seq: last as nat };
            &&& retired.contains_key(w)
            &&& tails[s].opcode.allows_outcome(total_len(tails[s].sges), retired[w])
        }
    }
}

pub(super) proof fn lemma_implied_empty<L>(r: Ring<L>)
    requires r.inv(), forall|s: int| #[trigger] r.meta(s) is None,
    ensures send_implied(r, Map::empty()),
{
}

/// Preserve the native success while recorded errors, ownership or wakers change.
pub(super) proof fn lemma_implied_kept<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, next: Map<int, SendWr>,
)
    requires send_implied(pre, tails),
        forall|s: int| #[trigger] post.meta(s) is Some ==> pre.meta(s) is Some
            && (post.meta(s)->0.outcome is Ok ==> post.meta(s)->0.outcome == pre.meta(s)->0.outcome)
            && next[s] == tails[s] && next.contains_key(s),
    ensures send_implied(post, next),
{
}

/// Closing a real request records its last WR and its operation-defined success.
pub(super) proof fn lemma_implied_added<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, added: int, last: SendWr,
)
    requires send_implied(pre, tails),
        post.meta(added) is Some,
        post.meta(added)->0.outcome matches Ok(done)
            ==> last.opcode.allows_outcome(total_len(last.sges), done.model()),
        forall|s: int| #[trigger] post.meta(s) is Some && s != added ==> pre.meta(s) is Some
            && post.meta(s)->0.outcome == pre.meta(s)->0.outcome,
    ensures send_implied(post, tails.insert(added, last)),
{
}

/// WQE `n` of the send queue of QP `q` succeeded, as M retired it.
pub(super) open spec fn send_succeeded(retired: IMap<WqeRef, Outcome>, q: QpId, n: nat) -> bool {
    let w = WqeRef { qp: q, queue: Queue::Send, seq: n };
    retired.contains_key(w) && retired[w].status is Success
}

/// WQE `n` of the send queue of QP `q`, which `qp` held, M retired with an outcome its WR allows.
pub(super) open spec fn popped_ok(qp: Qp, retired: IMap<WqeRef, Outcome>, q: QpId, n: nat) -> bool {
    let w = WqeRef { qp: q, queue: Queue::Send, seq: n };
    let wr = qp.sq[n - qp.sq_head].wr;
    retired.contains_key(w) && wr.opcode.allows_outcome(total_len(wr.sges), retired[w])
}

/// Advancing M's head or removing native results preserves every surviving tail.
pub(super) proof fn lemma_bound_kept<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>,
    qp: Qp, next: Qp, written: Seq<SendWr>,
    retired: IMap<WqeRef, Outcome>, after: IMap<WqeRef, Outcome>, q: QpId,
)
    requires tail_bound(pre, tails, qp, written, retired, q),
        post.spec_wqe_committed() == pre.spec_wqe_committed(),
        qp.sq_head + qp.sq.len() == pre.spec_wqe_committed(),
        next.sq_head >= qp.sq_head,
        forall|j: int| 0 <= j < next.sq.len() ==> next.sq_head + j < qp.sq_head + qp.sq.len()
            && (#[trigger] next.sq[j]).wr == qp.sq[next.sq_head + j - qp.sq_head].wr,
        next.sq_head + next.sq.len() == qp.sq_head + qp.sq.len(),
        forall|s: int| #[trigger] post.meta(s) is Some ==> pre.meta(s) is Some
            && post.meta(s)->0.first == pre.meta(s)->0.first && post.meta(s)->0.end == pre.meta(s)->0.end,
        forall|s: int| #[trigger] post.req(s) is Some ==> pre.req(s) is Some,
        forall|w: WqeRef| w.qp == q && w.queue is Send && w.seq < qp.sq_head
            && #[trigger] retired.contains_key(w)
            ==> after.contains_key(w) && after[w] == retired[w],
        forall|n: nat| qp.sq_head <= n < next.sq_head ==> #[trigger] popped_ok(qp, after, q, n),
    ensures tail_bound(post, tails, next, written, after, q),
{
    assert forall|s: int| #[trigger] post.meta(s) is Some implies {
        let meta = post.meta(s)->0;
        let last = meta.end as int - 1;
        &&& meta.first < meta.end
        &&& tails.contains_key(s)
        &&& post.req(s) is Some ==> meta.end <= post.spec_wqe_committed()
        &&& next.sq_head <= last < post.spec_wqe_committed() ==> {
            &&& 0 <= last - next.sq_head < next.sq.len()
            &&& next.sq[last - next.sq_head].wr == tails[s]
        }
        &&& post.spec_wqe_committed() <= last ==> {
            &&& 0 <= last - post.spec_wqe_committed() < written.len()
            &&& written[last - post.spec_wqe_committed()] == tails[s]
        }
        &&& last < next.sq_head ==> {
            let w = WqeRef { qp: q, queue: Queue::Send, seq: last as nat };
            &&& after.contains_key(w)
            &&& tails[s].opcode.allows_outcome(total_len(tails[s].sges), after[w])
        }
    } by {
        let last = post.meta(s)->0.end as int - 1;
        assert(pre.meta(s)->0.end == post.meta(s)->0.end);
        if next.sq_head <= last < post.spec_wqe_committed() {
            assert(qp.sq[last - qp.sq_head].wr == tails[s]);
            assert(next.sq[last - next.sq_head].wr == qp.sq[last - qp.sq_head].wr);
        }
        if last < next.sq_head {
            let w = WqeRef { qp: q, queue: Queue::Send, seq: last as nat };
            if last < qp.sq_head {
                assert(retired.contains_key(w));
            } else {
                assert(popped_ok(qp, after, q, last as nat));
                assert(qp.sq_head <= last as nat);
                assert(qp.sq_head <= last < pre.spec_wqe_committed()) by {
                    assert(qp.sq_head + qp.sq.len() >= next.sq_head);
                }
            }
        }
    }
}

/// Once all staged requests are refused, no surviving tail needs written WRs.
pub(super) proof fn lemma_bound_no_written<L>(
    r: Ring<L>, tails: Map<int, SendWr>, qp: Qp, written: Seq<SendWr>,
    retired: IMap<WqeRef, Outcome>, q: QpId,
)
    requires r.inv(), tail_bound(r, tails, qp, written, retired, q),
        forall|s: int| #[trigger] r.staged(s) is None,
    ensures tail_bound(r, tails, qp, Seq::empty(), retired, q),
{
    assert forall|s: int| #[trigger] r.meta(s) is Some implies r.meta(s)->0.end <= r.spec_wqe_committed() by {
        r.lemma_meta(s);
    }
}

/// The newly closed tail is the exact last WR just appended to the provider builder.
pub(super) proof fn lemma_bound_added<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, qp: Qp,
    written: Seq<SendWr>, more: Seq<SendWr>, added: int, last: SendWr,
    retired: IMap<WqeRef, Outcome>, q: QpId,
)
    requires tail_bound(pre, tails, qp, written, retired, q),
        post.spec_wqe_committed() == pre.spec_wqe_committed(),
        qp.sq_head <= pre.spec_wqe_committed(),
        written.len() <= more.len(),
        forall|j: int| 0 <= j < written.len() ==> #[trigger] more[j] == written[j],
        post.meta(added) is Some, post.req(added) is None,
        post.meta(added)->0.first < post.meta(added)->0.end,
        post.meta(added)->0.first >= post.spec_wqe_committed(),
        post.meta(added)->0.end == post.spec_wqe_committed() + more.len(),
        more.last() == last,
        forall|s: int| #[trigger] post.meta(s) is Some && s != added ==> pre.meta(s) is Some
            && post.meta(s)->0.first == pre.meta(s)->0.first && post.meta(s)->0.end == pre.meta(s)->0.end,
        forall|s: int| #[trigger] post.req(s) is Some ==> pre.req(s) is Some,
    ensures tail_bound(post, tails.insert(added, last), qp, more, retired, q),
{
    let next = tails.insert(added, last);
    assert forall|s: int| #[trigger] post.meta(s) is Some implies {
        let meta = post.meta(s)->0;
        let end = meta.end as int - 1;
        &&& meta.first < meta.end
        &&& next.contains_key(s)
        &&& post.req(s) is Some ==> meta.end <= post.spec_wqe_committed()
        &&& qp.sq_head <= end < post.spec_wqe_committed() ==> {
            &&& 0 <= end - qp.sq_head < qp.sq.len()
            &&& qp.sq[end - qp.sq_head].wr == next[s]
        }
        &&& post.spec_wqe_committed() <= end ==> {
            &&& 0 <= end - post.spec_wqe_committed() < more.len()
            &&& more[end - post.spec_wqe_committed()] == next[s]
        }
        &&& end < qp.sq_head ==> {
            let w = WqeRef { qp: q, queue: Queue::Send, seq: end as nat };
            &&& retired.contains_key(w)
            &&& next[s].opcode.allows_outcome(total_len(next[s].sges), retired[w])
        }
    } by {
        if s != added {
            let end = post.meta(s)->0.end as int - 1;
            if post.spec_wqe_committed() <= end {
                assert(more[end - post.spec_wqe_committed()] == written[end - post.spec_wqe_committed()]);
            }
        }
    }
}

/// The doorbell transfers written tails into M without changing their WR values.
pub(super) proof fn lemma_bound_committed<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>,
    qp: Qp, next: Qp, written: Seq<SendWr>,
    retired: IMap<WqeRef, Outcome>, q: QpId,
)
    requires pre.inv(), post.inv(), tail_bound(pre, tails, qp, written, retired, q),
        qp.sq_head + qp.sq.len() == pre.spec_wqe_committed(),
        next.sq_head == qp.sq_head, next.sq == qp.sq + written.map_values(|wr: SendWr| SendSlot { wr, outcome: None }),
        post.spec_wqe_committed() == pre.spec_wqe_committed() + written.len(),
        forall|s: int| #[trigger] post.meta(s) == pre.meta(s),
    ensures tail_bound(post, tails, next, Seq::empty(), retired, q),
{
    assert forall|s: int| #[trigger] post.meta(s) is Some implies {
        let meta = post.meta(s)->0;
        let last = meta.end as int - 1;
        &&& meta.first < meta.end
        &&& tails.contains_key(s)
        &&& post.req(s) is Some ==> meta.end <= post.spec_wqe_committed()
        &&& next.sq_head <= last < post.spec_wqe_committed() ==> {
            &&& 0 <= last - next.sq_head < next.sq.len()
            &&& next.sq[last - next.sq_head].wr == tails[s]
        }
        &&& post.spec_wqe_committed() <= last ==> {
            &&& 0 <= last - post.spec_wqe_committed() < Seq::<SendWr>::empty().len()
            &&& Seq::<SendWr>::empty()[last - post.spec_wqe_committed()] == tails[s]
        }
        &&& last < next.sq_head ==> {
            let w = WqeRef { qp: q, queue: Queue::Send, seq: last as nat };
            &&& retired.contains_key(w)
            &&& tails[s].opcode.allows_outcome(total_len(tails[s].sges), retired[w])
        }
    } by {
        pre.lemma_meta(s);
        post.lemma_meta(s);
        let last = post.meta(s)->0.end as int - 1;
        if pre.spec_wqe_committed() <= last {
            assert(next.sq[last - next.sq_head].wr == written[last - pre.spec_wqe_committed()]);
        }
    }
}

// =============================================================================
// Results

/// The WQEs of the request with bookkeeping `meta` on QP `q`'s send queue, as M names them.
pub(super) open spec fn send_range(q: QpId, meta: RequestMeta) -> WqeRange {
    WqeRange { qp: q, queue: Queue::Send, first: meta.first as nat, end: meta.end as nat }
}

/// Where the prefix of `range` the ring has heard of ends, when the last completion it saw named
/// the WQE before `routed`.
pub(super) open spec fn retired_to(range: WqeRange, routed: nat) -> nat {
    if routed <= range.first {
        range.first
    } else if routed < range.end {
        routed
    } else {
        range.end
    }
}

/// `error` is the first error among the WQEs of `range` before `cut`, at its position.
pub(super) open spec fn is_first_error(
    range: WqeRange, cut: nat, retired: IMap<WqeRef, Outcome>, error: CompletionError,
) -> bool {
    error matches CompletionError::Failed { position, .. }
        && range.first + position < cut
        && range.first_error(retired, (range.first + position) as nat)
        && retired[range.at((range.first + position) as nat)] == crate::wc::result_model(Err(error))
}

/// `outcome` is what a request whose WQEs before `cut` M retired says of them: the first error
/// among them, or a success if they all succeeded. A proper prefix does not complete the request.
pub(super) open spec fn prefix_outcome(
    range: WqeRange, cut: nat, retired: IMap<WqeRef, Outcome>, outcome: CompletionResult,
) -> bool {
    &&& range.wf() && range.first <= cut <= range.end
    &&& forall|s: nat| range.first <= s < cut ==> retired.contains_key(#[trigger] range.at(s))
    &&& match outcome {
        Ok(_) => forall|s: nat| range.first <= s < cut
            ==> (#[trigger] retired[range.at(s)]).status is Success,
        Err(error) => is_first_error(range, cut, retired, error),
    }
}

/// What request `s` of ring `r` agrees with in M's state `m`. In flight, no completion the ring
/// has seen named its last WQE, and what it says is the first error of the prefix the ring heard
/// of; complete, all its WQEs retired, its outcome is their aggregate (the first error in posting
/// order, else the last WQE's success).
pub(super) open spec fn send_result<L>(r: Ring<L>, m: Nic::State, q: QpId, s: int) -> bool {
    let meta = r.meta(s)->0;
    let range = send_range(q, meta);
    &&& range.wf()
    &&& range.end - range.first <= u32::MAX
    &&& match r.req(s)->0.phase {
        safeverbs_spec::abs::Phase::InFlight => r.spec_routed() < range.end
            && prefix_outcome(range, retired_to(range, r.spec_routed()), m.retired, meta.outcome),
        safeverbs_spec::abs::Phase::Complete(o) => {
            &&& range.end <= m.qps[q].sq_head
            &&& prefix_outcome(range, range.end, m.retired, meta.outcome)
            &&& range.aggregate(m.retired) == o
        },
    }
}

/// The WQEs of the request a detached error came from, as M names them.
pub(super) open spec fn detached_range(q: QpId, d: DetachedRecord) -> WqeRange {
    WqeRange { qp: q, queue: Queue::Send, first: d.first_wqe@ as nat, end: d.end_wqe@ as nat }
}

/// The detached error of ring `r` is the first error among the WQEs of its request, all
/// retired in M; or a refusal, of a request M never saw.
pub(super) open spec fn detached_result<L>(r: Ring<L>, m: Nic::State, q: QpId) -> bool {
    match r.spec_detached_error() {
        None => true,
        Some(d) => d.error is Refused || {
            let range = detached_range(q, d);
            &&& range.end - range.first <= u32::MAX
            &&& range.end <= m.qps[q].sq_head
            &&& prefix_outcome(range, range.end, m.retired, Err(d.error))
        },
    }
}

/// Each send request agrees with the WQEs of its interval M retired ([`send_result`]), and so
/// does the detached error ([`detached_result`]).
#[verifier::opaque]
pub(super) open spec fn send_results<L>(r: Ring<L>, m: Nic::State, q: QpId) -> bool {
    &&& m.qps.contains_key(q)
    &&& forall|s: int| #[trigger] r.req(s) is Some ==> send_result(r, m, q, s)
    &&& detached_result(r, m, q)
}

/// The prefix's outcomes are kept, so is what it says.
proof fn lemma_prefix_same(
    range: WqeRange, cut: nat, before: IMap<WqeRef, Outcome>, after: IMap<WqeRef, Outcome>,
    outcome: CompletionResult,
)
    requires
        prefix_outcome(range, cut, before, outcome),
        forall|s: nat| range.first <= s < cut ==> after.contains_key(#[trigger] range.at(s))
            && after[range.at(s)] == before[range.at(s)],
    ensures
        prefix_outcome(range, cut, after, outcome),
{
    if let Err(error) = outcome {
        if let CompletionError::Failed { position, .. } = error {
            let s = (range.first + position) as nat;
            assert(after[range.at(s)] == before[range.at(s)]);
            assert forall|j: nat| range.first <= j < s implies (#[trigger] after[range.at(j)]).status is Success by {
                assert(after[range.at(j)] == before[range.at(j)]);
            }
        }
    }
}

/// More WQEs retired leave the first error, if there is one, as it is.
proof fn lemma_prefix_error_kept(
    range: WqeRange, cut: nat, to: nat, retired: IMap<WqeRef, Outcome>, error: CompletionError,
)
    requires
        prefix_outcome(range, cut, retired, Err(error)),
        cut <= to <= range.end,
        forall|s: nat| cut <= s < to ==> retired.contains_key(#[trigger] range.at(s)),
    ensures
        prefix_outcome(range, to, retired, Err(error)),
{
}

/// More successes leave a prefix of successes one.
proof fn lemma_prefix_successes(
    range: WqeRange, cut: nat, to: nat, retired: IMap<WqeRef, Outcome>, outcome: CompletionResult,
)
    requires
        prefix_outcome(range, cut, retired, outcome),
        outcome is Ok,
        cut <= to <= range.end,
        forall|s: nat| cut <= s < to ==> retired.contains_key(#[trigger] range.at(s))
            && retired[range.at(s)].status is Success,
    ensures
        prefix_outcome(range, to, retired, outcome),
{
}

/// An error after a prefix of successes is the first error.
proof fn lemma_prefix_first_error(
    range: WqeRange, cut: nat, at: nat, retired: IMap<WqeRef, Outcome>,
    outcome: CompletionResult, wc: Wc,
)
    requires
        prefix_outcome(range, cut, retired, outcome),
        outcome is Ok,
        cut <= at < range.end,
        at - range.first <= u32::MAX,
        forall|s: nat| cut <= s < at ==> retired.contains_key(#[trigger] range.at(s))
            && retired[range.at(s)].status is Success,
        retired.contains_key(range.at(at)),
        retired[range.at(at)] == wc.outcome(),
        !wc.spec_ok(),
    ensures
        prefix_outcome(range, at + 1, retired, Err(wc.spec_failure((at - range.first) as u32))),
{
    wc.lemma_failure((at - range.first) as u32);
    wc.lemma_outcome_ok();
    assert(range.first_error(retired, at));
}

/// Once the whole range retired, what it says is M's aggregate: the first error, or the last
/// WQE's success.
pub(super) proof fn lemma_prefix_aggregate(range: WqeRange, retired: IMap<WqeRef, Outcome>, outcome: CompletionResult)
    requires
        prefix_outcome(range, range.end, retired, outcome),
    ensures
        range.all_retired(retired),
        outcome is Err ==> range.aggregate(retired) == crate::wc::result_model(outcome),
        outcome is Ok ==> range.aggregate(retired) == retired[range.last()]
            && range.aggregate(retired).status is Success,
{
    match outcome {
        Err(error) => {
            if let CompletionError::Failed { position, .. } = error {
                lemma_aggregate_first_error(range, retired, (range.first + position) as nat);
            }
        },
        Ok(_) => {
            lemma_aggregate_success(range, retired);
        },
    }
}

/// The aggregate reads the outcomes of the range's own WQEs only.
proof fn lemma_aggregate_framed(range: WqeRange, before: IMap<WqeRef, Outcome>, after: IMap<WqeRef, Outcome>)
    requires
        range.wf(),
        range.all_retired(before),
        forall|s: nat| range.first <= s < range.end ==> after.contains_key(#[trigger] range.at(s))
            && after[range.at(s)] == before[range.at(s)],
    ensures
        range.all_retired(after),
        range.aggregate(after) == range.aggregate(before),
    decreases range.end - range.first,
{
    if range.first + 1 < range.end {
        let tail = WqeRange { first: range.first + 1, ..range };
        assert forall|s: nat| tail.first <= s < tail.end
            implies before.contains_key(#[trigger] tail.at(s)) by {
            assert(tail.at(s) == range.at(s));
        }
        assert forall|s: nat| tail.first <= s < tail.end
            implies after.contains_key(#[trigger] tail.at(s)) && after[tail.at(s)] == before[tail.at(s)] by {
            assert(tail.at(s) == range.at(s));
        }
        lemma_aggregate_framed(tail, before, after);
    }
    assert(after[range.at(range.first)] == before[range.at(range.first)]);
}

/// An operation has one successful outcome: its status only, or the length a READ asked for.
proof fn lemma_success_unique(op: safeverbs_spec::nic::Opcode, bytes: nat, a: Outcome, b: Outcome)
    requires
        a.status is Success,
        b.status is Success,
        op.allows_outcome(bytes, a),
        op.allows_outcome(bytes, b),
    ensures
        a == b,
{
    if op is Read {
        assert(a.kind is Read && b.kind is Read);
    } else {
        safeverbs_spec::nic_proofs::lemma_ack_outcome_unique(op, bytes, a, b);
    }
}

/// A new ring has no request and no detached error.
pub(super) proof fn lemma_results_empty<L>(r: Ring<L>, m: Nic::State, q: QpId)
    requires
        r.inv(),
        m.qps.contains_key(q),
        forall|s: int| #[trigger] r.meta(s) is None,
        r.spec_detached_error() is None,
    ensures
        send_results(r, m, q),
{
    reveal(send_results);
    assert forall|s: int| #[trigger] r.req(s) is None by {
        r.lemma_meta(s);
    }
}

/// Steps of M that keep its send queue's head, or move it on, and the outcomes M retired before it
/// keep the results.
pub(super) proof fn lemma_results_framed<L>(r: Ring<L>, m: Nic::State, n: Nic::State, q: QpId)
    requires
        send_results(r, m, q),
        n.qps.contains_key(q),
        n.qps[q].sq_head >= m.qps[q].sq_head,
        r.spec_routed() <= m.qps[q].sq_head,
        forall|w: WqeRef| w.qp == q && w.queue is Send && w.seq < m.qps[q].sq_head
            && #[trigger] m.retired.contains_key(w)
            ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w],
    ensures
        send_results(r, n, q),
{
    reveal(send_results);
    assert forall|s: int| #[trigger] r.req(s) is Some implies send_result(r, n, q, s) by {
        let outcome = r.meta(s)->0.outcome;
        let range = send_range(q, r.meta(s)->0);
        if r.req(s)->0.phase is InFlight {
            lemma_prefix_same(range, retired_to(range, r.spec_routed()), m.retired, n.retired, outcome);
        } else {
            lemma_prefix_same(range, range.end, m.retired, n.retired, outcome);
            lemma_aggregate_framed(range, m.retired, n.retired);
        }
    }
    if let Some(d) = r.spec_detached_error() {
        if d.error is Failed {
            let range = detached_range(q, d);
            lemma_prefix_same(range, range.end, m.retired, n.retired, Err(d.error));
        }
    }
}

/// Requests that stay keep their phase and bookkeeping, so their results; the detached error
/// stays too, is taken, or comes from a refused request, which M never saw.
pub(super) proof fn lemma_results_kept<L>(pre: Ring<L>, post: Ring<L>, m: Nic::State, q: QpId)
    requires
        send_results(pre, m, q),
        post.spec_routed() == pre.spec_routed(),
        forall|s: int| #[trigger] post.req(s) is Some ==> pre.req(s) is Some
            && post.req(s)->0.phase == pre.req(s)->0.phase && post.meta(s) == pre.meta(s),
        post.detached_refused(&pre) || post.spec_detached_error() is None,
    ensures
        send_results(post, m, q),
{
    reveal(send_results);
    assert forall|s: int| #[trigger] post.req(s) is Some implies send_result(post, m, q, s) by {
        assert(send_result(pre, m, q, s));
    }
}

/// A refused doorbell drops the staged requests, which M never saw; the others stay.
pub(super) proof fn lemma_results_refused<L>(pre: Ring<L>, post: Ring<L>, m: Nic::State, q: QpId)
    requires
        pre.inv(),
        send_results(pre, m, q),
        post.spec_routed() == pre.spec_routed(),
        forall|s: int| #[trigger] post.req(s) == pre.req(s),
        forall|s: int| #[trigger] post.meta(s) == if pre.staged(s) is Some {
            None
        } else {
            pre.meta(s)
        },
        post.detached_refused(&pre),
    ensures
        send_results(post, m, q),
{
    pre.lemma_view();
    lemma_results_kept(pre, post, m, q);
}

/// The doorbell puts the staged requests in flight, after every WQE M holds, with nothing
/// recorded.
pub(super) proof fn lemma_results_committed<L>(pre: Ring<L>, post: Ring<L>, m: Nic::State, q: QpId)
    requires
        pre.inv(),
        send_results(pre, m, q),
        post.spec_routed() == pre.spec_routed(),
        pre.spec_routed() <= pre.spec_wqe_committed(),
        forall|s: int| #[trigger] post.req(s) == if pre.staged(s) is Some {
            pre.staged(s)
        } else {
            pre.req(s)
        },
        post.same_meta(&pre),
        post.spec_detached_error() == pre.spec_detached_error(),
    ensures
        send_results(post, m, q),
{
    reveal(send_results);
    pre.lemma_view();
    pre.lemma_wqes();
    assert forall|s: int| #[trigger] post.req(s) is Some implies send_result(post, m, q, s) by {
        pre.lemma_meta(s);
        if pre.staged(s) is Some {
            assert(crate::refine::wf_at(pre.view(), s));
            assert((pre.view().staged)(s) == pre.staged(s));
            assert(post.req(s) == pre.staged(s));
            assert(post.req(s)->0.phase is InFlight);
        } else {
            assert(send_result(pre, m, q, s));
        }
    }
}

/// What the CQE of WQE `wqe`, the next the ring is to see, reported as `wc`, makes of a request in
/// flight whose WQEs all lie at or before it: all of them retired, and the outcome the ring retires
/// it with is their aggregate, a success being the one its last WR implies and an error the first
/// one, at its position.
pub(super) proof fn lemma_request_retired<L>(
    pre: Ring<L>, tails: Map<int, SendWr>, written: Seq<SendWr>,
    m: Nic::State, q: QpId, wqe: u64, wc: Wc, s: int,
)
    requires
        m.qps.contains_key(q),
        pre.inv(),
        pre.req(s) is Some,
        pre.req(s)->0.phase is InFlight,
        pre.meta(s)->0.end as int <= wqe as int + 1,
        send_result(pre, m, q, s),
        send_implied(pre, tails),
        tail_bound(pre, tails, m.qps[q], written, m.retired, q),
        pre.spec_routed() <= wqe < m.qps[q].sq_head,
        forall|n: nat| pre.spec_routed() <= n < wqe ==> #[trigger] send_succeeded(m.retired, q, n),
        m.retired.contains_key(WqeRef { qp: q, queue: Queue::Send, seq: wqe as nat }),
        m.retired[WqeRef { qp: q, queue: Queue::Send, seq: wqe as nat }] == wc.outcome(),
    ensures
        ({
            let meta = pre.meta(s)->0;
            let range = send_range(q, meta);
            let out = pre.retire_outcome(s, false, wqe, wc);
            &&& range.end <= m.qps[q].sq_head
            &&& prefix_outcome(range, range.end, m.retired, out)
            &&& range.aggregate(m.retired) == crate::wc::result_model(out)
            &&& out is Ok ==> tails[s].opcode.allows_outcome(total_len(tails[s].sges), range.aggregate(m.retired))
        }),
{
    let meta = pre.meta(s)->0;
    let range = send_range(q, meta);
    let outcome = meta.outcome;
    let routed = pre.spec_routed();
    let cut = retired_to(range, routed);
    let end = range.end;
    pre.lemma_view();
    pre.lemma_meta(s);
    pre.lemma_completion_observation(s, false, wqe, wc, pre.spec_retired() as int, true);
    wc.lemma_outcome_ok();
    assert forall|n: nat| cut <= n < end - 1 implies m.retired.contains_key(#[trigger] range.at(n))
        && m.retired[range.at(n)].status is Success by {
        assert(routed <= n < wqe);
        let w = WqeRef { qp: q, queue: Queue::Send, seq: n };
        assert(range.at(n) == w);
        assert(send_succeeded(m.retired, q, n));
    }
    let last_at = (end - 1) as nat;
    assert(range.at(last_at) == range.last());
    if end as int == wqe as int + 1 {
        assert(range.last() == (WqeRef { qp: q, queue: Queue::Send, seq: wqe as nat }));
    } else {
        assert(routed <= last_at < wqe);
        assert(send_succeeded(m.retired, q, last_at));
        assert(m.retired.contains_key(range.last()));
        assert(m.retired[range.last()].status is Success);
    }
    match outcome {
        Err(error) => {
            lemma_prefix_error_kept(range, cut, end, m.retired, error);
            lemma_prefix_aggregate(range, m.retired, outcome);
        },
        Ok(done) => {
            lemma_prefix_successes(range, cut, last_at, m.retired, outcome);
            if end as int == wqe as int + 1 && !wc.spec_ok() {
                lemma_prefix_first_error(range, last_at, last_at, m.retired, outcome, wc);
                assert(last_at + 1 == end);
                assert(last_at == meta.first + (wqe - meta.first));
                lemma_prefix_aggregate(range, m.retired, pre.retire_outcome(s, false, wqe, wc));
            } else {
                assert(m.retired[range.last()].status is Success);
                lemma_prefix_successes(range, last_at, end, m.retired, outcome);
                lemma_prefix_aggregate(range, m.retired, outcome);
                // M retired its last WQE with a success: that is what the WR implies.
                let w = range.last();
                assert(m.qps[q].sq_head > last_at);
                assert(tail_bound(pre, tails, m.qps[q], written, m.retired, q));
                assert(send_implied(pre, tails));
                let tail = tails[s];
                if last_at < m.qps[q].sq_head {
                    assert(tail.opcode.allows_outcome(total_len(tail.sges), m.retired[w]));
                }
                lemma_success_unique(tail.opcode, total_len(tail.sges), done.model(), m.retired[w]);
            }
        },
    }
}

/// A step of the walk of the CQE of WQE `wqe`, the next the ring is to see. The requests it
/// retires take M's aggregate of their intervals. The request still in flight around `wqe`
/// records its error if it is the first, the others stay as they were, and a request without an
/// owner it retires with an error may become the detached error.
pub(super) proof fn lemma_results_routed<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, written: Seq<SendWr>,
    m: Nic::State, q: QpId, wqe: u64, wc: Wc, spent: bool,
)
    requires
        m.qps.contains_key(q),
        pre.inv(),
        send_results(pre, m, q),
        send_implied(pre, tails),
        tail_bound(pre, tails, m.qps[q], written, m.retired, q),
        pre.spec_wqe_retired() <= wqe,
        pre.spec_routed() <= wqe < m.qps[q].sq_head,
        forall|n: nat| pre.spec_routed() <= n < wqe ==> #[trigger] send_succeeded(m.retired, q, n),
        m.retired.contains_key(WqeRef { qp: q, queue: Queue::Send, seq: wqe as nat }),
        m.retired[WqeRef { qp: q, queue: Queue::Send, seq: wqe as nat }] == wc.outcome(),
        post.walked(&pre, false, wqe, wc, spent),
    ensures
        send_results(post, m, q),
{
    let to = post.spec_retired() as int;
    let routed = pre.spec_routed();
    wc.lemma_outcome_ok();
    pre.lemma_view();
    post.lemma_view();
    reveal(send_results);
    assert forall|s: int| #[trigger] post.req(s) is Some implies send_result(post, m, q, s) by {
        let meta = pre.meta(s)->0;
        let range = send_range(q, meta);
        pre.lemma_meta(s);
        post.lemma_meta(s);
        pre.lemma_completion_observation(s, false, wqe, wc, to, spent);
        assert(post.meta(s) == pre.completion_meta(s, false, wqe, wc, to, spent));
        assert(pre.req(s) is Some) by {
            if pre.spec_retired() <= s < to {
                pre.lemma_retired_view(s, pre.retire_outcome(s, false, wqe, wc));
            }
        }
        assert(send_range(q, post.meta(s)->0) == range);
        assert(send_result(pre, m, q, s));
        if s < pre.spec_retired() {
        } else if s < to {
            // Retired by this CQE.
            pre.lemma_retired_view(s, pre.retire_outcome(s, false, wqe, wc));
            lemma_request_retired(pre, tails, written, m, q, wqe, wc, s);
        } else {
            // Still in flight: the CQE's error is its first, if it lies inside it.
            let cut = retired_to(range, routed);
            let next_routed = post.spec_routed();
            if spent {
                assert(next_routed == wqe + 1);
                assert(post.meta(s)->0.end > wqe as int + 1);
                if range.first <= wqe {
                    assert forall|n: nat| cut <= n < wqe implies m.retired.contains_key(#[trigger] range.at(n))
                        && m.retired[range.at(n)].status is Success by {
                        let w = WqeRef { qp: q, queue: Queue::Send, seq: n };
                        assert(range.at(n) == w);
                        assert(routed <= n < wqe);
                        assert(send_succeeded(m.retired, q, n));
                    }
                    assert(range.at(wqe as nat) == (WqeRef { qp: q, queue: Queue::Send, seq: wqe as nat }));
                    match meta.outcome {
                        Err(error) => {
                            lemma_prefix_error_kept(range, cut, (wqe + 1) as nat, m.retired, error);
                        },
                        Ok(_) => {
                            lemma_prefix_successes(range, cut, wqe as nat, m.retired, meta.outcome);
                            if wc.spec_ok() {
                                lemma_prefix_successes(range, wqe as nat, (wqe + 1) as nat, m.retired, meta.outcome);
                            } else {
                                lemma_prefix_first_error(range, wqe as nat, wqe as nat, m.retired, meta.outcome, wc);
                            }
                        },
                    }
                    post.lemma_pending_cache_observation(pre, wqe, wc, s);
                    assert(post.meta(s)->0.outcome == if meta.outcome is Ok && !wc.spec_ok() {
                        Err(wc.spec_failure((wqe - meta.first) as u32))
                    } else {
                        meta.outcome
                    });
                } else {
                    assert(retired_to(range, next_routed) == range.first);
                    assert(cut == range.first);
                }
            } else {
                assert(next_routed == routed);
                assert(post.meta(s) == pre.meta(s));
            }
        }
    }
    if let Some(d) = post.spec_detached_error() {
        if pre.spec_detached_error() == post.spec_detached_error() {
        } else {
            let s = d.seq as int;
            pre.lemma_meta(s);
            pre.lemma_completion_observation(s, false, wqe, wc, to, spent);
            assert(send_result(pre, m, q, s));
            lemma_request_retired(pre, tails, written, m, q, wqe, wc, s);
            assert(detached_range(q, d) == send_range(q, pre.meta(s)->0));
        }
    }
}

/// What a send request reports is M's aggregate of its WQEs: a success what its last WR defines,
/// an error the first in posting order, at its position.
pub open spec fn send_reports_outcome(
    range: WqeRange, retired: IMap<WqeRef, Outcome>, last: SendWr, result: CompletionResult,
) -> bool {
    &&& range.wf()
    &&& range.all_retired(retired)
    &&& {
        let o = range.aggregate(retired);
        match result {
            Ok(done) => done.model() == o && last.opcode.allows_outcome(total_len(last.sges), o),
            Err(CompletionError::Failed { status, position, .. }) => {
                &&& o.status.is_err()
                &&& o.status == status.model()
                &&& range.first_error(retired, (range.first + position) as nat)
                &&& retired[range.at((range.first + position) as nat)] == o
            },
            Err(CompletionError::Refused) => false,
        }
    }
}

/// What a complete request says, by the invariant, is what [`send_reports_outcome`] says.
pub(super) proof fn lemma_reports(
    range: WqeRange, retired: IMap<WqeRef, Outcome>, last: SendWr, outcome: CompletionResult,
)
    requires
        prefix_outcome(range, range.end, retired, outcome),
        !(outcome matches Err(CompletionError::Refused)),
        outcome matches Ok(done) ==> last.opcode.allows_outcome(total_len(last.sges), done.model()),
        outcome is Ok ==> range.aggregate(retired) == crate::wc::result_model(outcome),
    ensures
        send_reports_outcome(range, retired, last, outcome),
{
    lemma_prefix_aggregate(range, retired, outcome);
}

impl<C, N: Device> Hw<C, N> {
    /// The WQEs of send request `seq` of QP `index`, as M names them.
    pub(super) open spec fn send_range_of(&self, index: usize, seq: RingSeq) -> WqeRange {
        let e = self.qps@[index as int]->0;
        send_range(e.id(), e.send.meta(seq as int)->0)
    }

    /// `t` is what the send ring of QP `index` hands back of request `seq`.
    pub(super) open spec fn send_taken_from(&self, index: usize, seq: RingSeq, t: Taken<Payload<C, N>>) -> bool {
        self.qps@[index as int]->0.send.taken_from(seq as int, t)
    }

    /// The send result, context and buffers `take` removed. A request the NIC saw is complete
    /// and M retired all its WQEs: its success is M's aggregate of them, as its last WR's
    /// operation defines it (the length a READ asked for, the status alone for a SEND or
    /// WRITE); its error is M's first error among them, at the position it says. A refused
    /// request is one M never saw, with its own context and buffers all the same.
    pub closed spec fn send_taken_matches(&self, index: usize, seq: RingSeq, c: Completion<C, N>) -> bool {
        &&& self.has_qp(index as int)
        &&& {
            let e = self.qps@[index as int]->0;
            let range = send_range(e.id(), e.send.meta(seq as int)->0);
            let last = e.send_tails@[seq as int];
            let retired = self.nic().retired;
            let payload = Payload { ctx: Some(c.wr_id), leases: c.leases };
            if c.result matches Err(CompletionError::Refused) {
                e.send.req(seq as int) is None && e.send.refused(seq as int) == Some(payload)
            } else {
                &&& e.send.req(seq as int) == Some(crate::ring::ReqView {
                    phase: safeverbs_spec::abs::Phase::Complete(range.aggregate(retired)),
                    waker: None,
                    detached: false,
                    leases: payload,
                })
                &&& send_reports_outcome(range, retired, last, c.result)
            }
        }
    }

    /// The [`Completion`] made of what the ring handed back is what
    /// [`Hw::send_taken_matches`] says, and what it holds is in order.
    pub(super) proof fn lemma_send_finished(&self, index: usize, seq: RingSeq, t: Taken<Payload<C, N>>, c: Completion<C, N>)
        requires
            self.inv(),
            self.has_qp(index as int),
            self.send_taken_from(index, seq, t),
            t.leases == (Payload { ctx: Some(c.wr_id), leases: c.leases }),
            c.result == t.outcome,
        ensures
            c.leases.inv() && c.leases.fully_recorded(),
            self.send_taken_matches(index, seq, c),
    {
        reveal(send_results);
        assert(self.entry_ok(index as int, self.token@, self.nic()));
        let e = self.qps@[index as int]->0;
        e.send.lemma_meta(seq as int);
        if !(t.outcome matches Err(CompletionError::Refused)) {
            assert(send_result(e.send, self.nic(), e.id(), seq as int));
            let range = send_range(e.id(), e.send.meta(seq as int)->0);
            lemma_reports(range, self.nic().retired, e.send_tails@[seq as int], t.outcome);
        }
    }

    /// What [`Hw::take`] hands out of a send request, the event loop may hand out.
    pub(super) proof fn lemma_send_hands_out(&self, c: Completion<C, N>)
        requires
            c.queue is Send,
            c.leases.inv() && c.leases.fully_recorded(),
            self.send_taken_matches(c.qp, c.seq, c),
        ensures
            self.hands_out(c),
    {
    }

    /// What the ring handed back of a send request holds a context, if it is not a refused one.
    pub(super) proof fn lemma_send_taken(&self, index: usize, seq: RingSeq, t: Taken<Payload<C, N>>)
        requires
            self.inv(),
            self.has_qp(index as int),
            self.send_taken_from(index, seq, t),
        ensures
            t.leases.ctx is Some,
            t.leases.leases.inv() && t.leases.leases.fully_recorded(),
    {
        assert(self.entry_ok(index as int, self.token@, self.nic()));
    }

    /// The detached error of QP `index`'s send ring.
    pub(super) open spec fn detached_record_of(&self, index: usize) -> Option<DetachedRecord> {
        self.qps@[index as int]->0.send.spec_detached_error()
    }

    /// The first error [`Hw::take_send_error`] hands out: M's first error among the WQEs of
    /// its request, all retired, at the position it says; or a refusal, which M never saw.
    pub closed spec fn detached_taken_matches(&self, index: usize, taken: DetachedError) -> bool {
        &&& self.has_qp(index as int)
        &&& {
            let e = self.qps@[index as int]->0;
            let d = e.send.spec_detached_error()->0;
            let range = detached_range(e.id(), d);
            let retired = self.nic().retired;
            let o = range.aggregate(retired);
            &&& e.send.spec_detached_error() is Some
            &&& d.seq == taken.seq
            &&& d.error == taken.error
            &&& match taken.error {
                CompletionError::Refused => true,
                CompletionError::Failed { status, position, .. } => {
                    &&& range.wf() && range.all_retired(retired)
                    &&& o.status.is_err() && o.status == status.model()
                    &&& range.first_error(retired, (range.first + position) as nat)
                    &&& retired[range.at((range.first + position) as nat)] == o
                },
            }
        }
    }

    /// What [`Hw::take_send_error`] hands out is what [`Hw::detached_taken_matches`] says.
    pub(super) proof fn lemma_detached_finished(&self, index: usize, d: DetachedRecord)
        requires
            self.inv(),
            self.has_qp(index as int),
            self.detached_record_of(index) == Some(d),
        ensures
            self.detached_taken_matches(index, DetachedError { seq: d.seq, error: d.error }),
    {
        reveal(send_results);
        assert(self.entry_ok(index as int, self.token@, self.nic()));
        if d.error is Failed {
            let e = self.qps@[index as int]->0;
            lemma_prefix_aggregate(detached_range(e.id(), d), self.nic().retired, Err(d.error));
        }
    }
}

} // verus!
