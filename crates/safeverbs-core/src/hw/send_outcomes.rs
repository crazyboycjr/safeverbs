//! Send results coupled to the WQEs M retired.
//!
//! A send request owns the WQEs `[first, end)` of its QP's send queue: one, or a batch. Its
//! ring slot caches the first error a CQE reported inside that interval
//! (`RequestMeta::seen`), and keeps the success its last WR implies (`RequestMeta::implied`)
//! for a later CQE that retires it without a completion of its own. Both are proved against
//! M (`send_results`): while the request is in flight, its last WQE is still in M's queue
//! and its cache is the first error among the WQEs of its interval M retired; once it is
//! complete, its outcome is M's aggregate of the interval (`WqeRange::aggregate`), the first
//! error in posting order or else the last WQE's success, which is what its last WR implies:
//! the length that READ asked for, or its status alone.
//! [`Hw::take`] hands back exactly that outcome (`Hw::send_taken_matches`).
#![allow(unused_imports)]
use super::*;
use crate::ring::RequestMeta;
use safeverbs_spec::types::Outcome;

verus! {

/// The recorded final WR authorizes exactly the inferred success.
/// Refused results have no metadata and never authorize a reused WQE number.
pub(super) open spec fn send_implied<L>(r: Ring<L>, tails: Map<int, SendWr>) -> bool {
    forall|s: int| #[trigger] r.meta(s) is Some ==> {
        &&& tails.contains_key(s)
        &&& r.meta(s)->0.implied.outcome().status is Success
        &&& tails[s].opcode.allows_outcome(total_len(tails[s].sges), r.meta(s)->0.implied.outcome())
    }
}

/// The recorded final WR is the request's last WQE, in M or not yet rung.
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

// =============================================================================
// Results

/// The WQEs of the request with bookkeeping `meta` on QP `q`'s send queue, as M names them.
pub(super) open spec fn send_range(q: QpId, meta: RequestMeta) -> WqeRange {
    WqeRange { qp: q, queue: Queue::Send, first: meta.first as nat, end: meta.end as nat }
}

/// The error a request cached, as M reports it.
pub(super) open spec fn cached(seen: Option<Wc>) -> Option<Outcome> {
    match seen {
        Some(wc) => Some(wc.outcome()),
        None => None,
    }
}

/// Where the prefix of `range` that M retired ends, when its queue's head is `head`.
pub(super) open spec fn retired_to(range: WqeRange, head: nat) -> nat {
    if head <= range.first {
        range.first
    } else if head < range.end {
        head
    } else {
        range.end
    }
}

/// `seen` is what a request whose WQEs before `cut` M retired caches: the first error among
/// them, or nothing if they all succeeded. A proper prefix does not complete the request.
pub(super) open spec fn prefix_outcome(
    range: WqeRange, cut: nat, retired: IMap<WqeRef, Outcome>, seen: Option<Outcome>,
) -> bool {
    &&& range.wf() && range.first <= cut <= range.end
    &&& forall|s: nat| range.first <= s < cut ==> retired.contains_key(#[trigger] range.at(s))
    &&& match seen {
        None => forall|s: nat| range.first <= s < cut
            ==> (#[trigger] retired[range.at(s)]).status is Success,
        Some(o) => o.status.is_err() && exists|s: nat| range.first <= s < cut
            && #[trigger] range.first_error(retired, s) && retired[range.at(s)] == o,
    }
}

/// What request `s` of ring `r` agrees with in M's state `m`. In flight, its last WQE is
/// still in M's queue and its cache is the first error of the prefix M retired; complete, all
/// its WQEs retired, its outcome is their aggregate (the first error in posting order, else
/// the last WQE's success), and a success is the one its last WR implies.
pub(super) open spec fn send_result<L>(r: Ring<L>, m: Nic::State, q: QpId, s: int) -> bool {
    let meta = r.meta(s)->0;
    let range = send_range(q, meta);
    let head = m.qps[q].sq_head;
    &&& range.wf()
    &&& match r.req(s)->0.phase {
        safeverbs_spec::abs::Phase::InFlight => head < range.end
            && prefix_outcome(range, retired_to(range, head), m.retired, cached(meta.seen)),
        safeverbs_spec::abs::Phase::Complete(o) => {
            &&& range.end <= head
            &&& range.all_retired(m.retired)
            &&& range.aggregate(m.retired) == o
            &&& o.status is Success ==> o == meta.implied.outcome()
        },
    }
}

/// Each send request agrees with the WQEs of its interval M retired ([`send_result`]).
#[verifier::opaque]
pub(super) open spec fn send_results<L>(r: Ring<L>, m: Nic::State, q: QpId) -> bool {
    &&& m.qps.contains_key(q)
    &&& forall|s: int| #[trigger] r.req(s) is Some ==> send_result(r, m, q, s)
}

/// The prefix's outcomes are kept, so is what it caches.
proof fn lemma_prefix_same(
    range: WqeRange, cut: nat, before: IMap<WqeRef, Outcome>, after: IMap<WqeRef, Outcome>,
    seen: Option<Outcome>,
)
    requires
        prefix_outcome(range, cut, before, seen),
        forall|s: nat| range.first <= s < cut ==> after.contains_key(#[trigger] range.at(s))
            && after[range.at(s)] == before[range.at(s)],
    ensures
        prefix_outcome(range, cut, after, seen),
{
    match seen {
        Some(o) => {
            let s = choose|s: nat| range.first <= s < cut && #[trigger] range.first_error(before, s)
                && before[range.at(s)] == o;
            assert(range.first_error(after, s));
        },
        None => {},
    }
}

/// More successes leave the first error, if any, unchanged.
proof fn lemma_prefix_successes(
    range: WqeRange, cut: nat, to: nat, retired: IMap<WqeRef, Outcome>, seen: Option<Outcome>,
)
    requires
        prefix_outcome(range, cut, retired, seen),
        cut <= to <= range.end,
        forall|s: nat| cut <= s < to ==> retired.contains_key(#[trigger] range.at(s))
            && retired[range.at(s)].status is Success,
    ensures
        prefix_outcome(range, to, retired, seen),
{
    match seen {
        Some(o) => {
            let s = choose|s: nat| range.first <= s < cut && #[trigger] range.first_error(retired, s)
                && retired[range.at(s)] == o;
            assert(range.first <= s < to);
        },
        None => {},
    }
}

/// Once an error is cached, later errors or successes do not replace it.
proof fn lemma_prefix_error_kept(
    range: WqeRange, cut: nat, to: nat, retired: IMap<WqeRef, Outcome>, error: Outcome,
)
    requires
        prefix_outcome(range, cut, retired, Some(error)),
        cut <= to <= range.end,
        forall|s: nat| cut <= s < to ==> retired.contains_key(#[trigger] range.at(s)),
    ensures
        prefix_outcome(range, to, retired, Some(error)),
{
    let s = choose|s: nat| range.first <= s < cut && #[trigger] range.first_error(retired, s)
        && retired[range.at(s)] == error;
    assert(range.first <= s < to);
}

/// An error after a prefix of successes is the first error.
proof fn lemma_prefix_first_error(
    range: WqeRange, cut: nat, at: nat, retired: IMap<WqeRef, Outcome>, error: Outcome,
)
    requires
        prefix_outcome(range, cut, retired, None),
        cut <= at < range.end,
        forall|s: nat| cut <= s < at ==> retired.contains_key(#[trigger] range.at(s))
            && retired[range.at(s)].status is Success,
        retired.contains_key(range.at(at)),
        retired[range.at(at)] == error,
        error.status.is_err(),
    ensures
        prefix_outcome(range, at + 1, retired, Some(error)),
{
    assert(range.first_error(retired, at));
}

/// Once the whole range retired, the cache is M's aggregate: the first error, or the last
/// WQE's success.
proof fn lemma_prefix_aggregate(range: WqeRange, retired: IMap<WqeRef, Outcome>, seen: Option<Outcome>)
    requires
        prefix_outcome(range, range.end, retired, seen),
    ensures
        range.all_retired(retired),
        seen is Some ==> range.aggregate(retired) == seen->0,
        seen is None ==> range.aggregate(retired) == retired[range.last()]
            && range.aggregate(retired).status is Success,
{
    match seen {
        Some(o) => {
            let s = choose|s: nat| range.first <= s < range.end && #[trigger] range.first_error(retired, s)
                && retired[range.at(s)] == o;
            lemma_aggregate_first_error(range, retired, s);
        },
        None => {
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

/// Polling the CQE of WQE `at`, with outcome `o`, when the queue's head was `head` retires
/// the WQEs from `head` to `at`, the ones before `at` with a success. The prefix of `range`
/// M retired then reaches `at` (or the range's end), and `o` becomes its first error if it
/// is an error inside `range` and the prefix had none.
proof fn lemma_prefix_polled(
    range: WqeRange, head: nat, at: nat, before: IMap<WqeRef, Outcome>,
    after: IMap<WqeRef, Outcome>, seen: Option<Outcome>, o: Outcome,
)
    requires
        prefix_outcome(range, retired_to(range, head), before, seen),
        head <= at,
        forall|s: nat| range.first <= s < retired_to(range, head)
            ==> after.contains_key(#[trigger] range.at(s)) && after[range.at(s)] == before[range.at(s)],
        forall|s: nat| head <= s < at ==> after.contains_key(#[trigger] range.at(s))
            && after[range.at(s)].status is Success,
        after.contains_key(range.at(at)),
        after[range.at(at)] == o,
    ensures
        prefix_outcome(range, retired_to(range, at + 1), after,
            if seen is None && o.status.is_err() && range.first <= at < range.end { Some(o) } else { seen }),
{
    let cut = retired_to(range, head);
    lemma_prefix_same(range, cut, before, after, seen);
    if range.first <= at && at < range.end {
        match seen {
            Some(error) => lemma_prefix_error_kept(range, cut, at + 1, after, error),
            None => if o.status.is_err() {
                lemma_prefix_first_error(range, cut, at, after, o);
            } else {
                lemma_prefix_successes(range, cut, at + 1, after, None);
            },
        }
    } else if range.end <= at {
        match seen {
            Some(error) => lemma_prefix_error_kept(range, cut, range.end, after, error),
            None => lemma_prefix_successes(range, cut, range.end, after, None),
        }
    }
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

/// A new ring has no request.
pub(super) proof fn lemma_results_empty<L>(r: Ring<L>, m: Nic::State, q: QpId)
    requires
        r.inv(),
        m.qps.contains_key(q),
        forall|s: int| #[trigger] r.meta(s) is None,
    ensures
        send_results(r, m, q),
{
    reveal(send_results);
    assert forall|s: int| #[trigger] r.req(s) is None by {
        r.lemma_meta(s);
    }
}

/// Steps that keep the send queue's head and the outcomes M retired before it keep the
/// results.
pub(super) proof fn lemma_results_framed<L>(r: Ring<L>, m: Nic::State, n: Nic::State, q: QpId)
    requires
        send_results(r, m, q),
        n.qps.contains_key(q),
        n.qps[q].sq_head == m.qps[q].sq_head,
        forall|w: WqeRef| w.qp == q && w.queue is Send && w.seq < m.qps[q].sq_head
            && #[trigger] m.retired.contains_key(w)
            ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w],
    ensures
        send_results(r, n, q),
{
    reveal(send_results);
    assert forall|s: int| #[trigger] r.req(s) is Some implies send_result(r, n, q, s) by {
        let range = send_range(q, r.meta(s)->0);
        let head = m.qps[q].sq_head;
        if r.req(s)->0.phase is InFlight {
            lemma_prefix_same(range, retired_to(range, head), m.retired, n.retired,
                cached(r.meta(s)->0.seen));
        } else {
            lemma_aggregate_framed(range, m.retired, n.retired);
        }
    }
}

/// Requests that stay keep their phase and bookkeeping, so their results.
pub(super) proof fn lemma_results_kept<L>(pre: Ring<L>, post: Ring<L>, m: Nic::State, q: QpId)
    requires
        send_results(pre, m, q),
        forall|s: int| #[trigger] post.req(s) is Some ==> pre.req(s) is Some
            && post.req(s)->0.phase == pre.req(s)->0.phase && post.meta(s) == pre.meta(s),
    ensures
        send_results(post, m, q),
{
    reveal(send_results);
    assert forall|s: int| #[trigger] post.req(s) is Some implies pre.req(s) is Some by {}
}

/// A refused doorbell drops the staged requests, which M never saw; the others stay.
pub(super) proof fn lemma_results_refused<L>(pre: Ring<L>, post: Ring<L>, m: Nic::State, q: QpId)
    requires
        pre.inv(),
        send_results(pre, m, q),
        forall|s: int| #[trigger] post.req(s) == pre.req(s),
        forall|s: int| #[trigger] post.meta(s) == if pre.staged(s) is Some {
            None
        } else {
            pre.meta(s)
        },
    ensures
        send_results(post, m, q),
{
    pre.lemma_view();
    lemma_results_kept(pre, post, m, q);
}

/// The doorbell puts the staged requests in flight, after every WQE M holds, with nothing
/// cached.
pub(super) proof fn lemma_results_committed<L>(pre: Ring<L>, post: Ring<L>, m: Nic::State, q: QpId)
    requires
        pre.inv(),
        send_results(pre, m, q),
        m.qps[q].sq_head <= pre.spec_wqe_committed(),
        forall|s: int| #[trigger] post.req(s) == if pre.staged(s) is Some {
            pre.staged(s)
        } else {
            pre.req(s)
        },
        post.same_meta(&pre),
    ensures
        send_results(post, m, q),
{
    reveal(send_results);
    pre.lemma_view();
    assert forall|s: int| #[trigger] post.req(s) is Some implies send_result(post, m, q, s) by {
        pre.lemma_meta(s);
        if pre.staged(s) is Some {
            assert((pre.view().staged)(s) == pre.staged(s));
        }
    }
}

/// A CQE of this send queue for WQE `wqe`, and the ring's completion of it. The requests it
/// retires take M's aggregate of their intervals: their cached error, else the CQE's own
/// outcome if it ends them, else the success their last WR implies, which is the success M
/// retired for it. The request still in flight around `wqe` caches its error if it is the
/// first, and the others stay as they were.
pub(super) proof fn lemma_results_polled<L>(
    pre: Ring<L>, post: Ring<L>, tails: Map<int, SendWr>, index: u16, written: Seq<SendWr>,
    m: Nic::State, n: Nic::State, c: CqId, q: QpId, wqe: u64, wc: Wc,
)
    requires
        nic_inv(m),
        nic_step(m, n, Nic::Step::poll_cq(c)),
        m.cqs[c].entries[0].qp == q,
        m.cqs[c].entries[0].queue is Send,
        m.cqs[c].entries[0].seq == wqe,
        m.cqs[c].entries[0].outcome == wc.outcome(),
        sq_linked(pre, index, m.qps[q]),
        send_results(pre, m, q),
        send_implied(pre, tails),
        tail_bound(pre, tails, index, m.qps[q], written),
        queued_outcomes_ok(m, q),
        post.inv(),
        pre.spec_retired() <= post.spec_retired() <= pre.spec_committed(),
        forall|s: int| #[trigger] post.req(s) == if pre.spec_retired() <= s < post.spec_retired() {
            pre.retired_view(s, pre.retire_outcome(s, wqe, wc))
        } else {
            pre.req(s)
        },
        forall|s: int| #[trigger] post.meta(s) == pre.completion_meta(s, wqe, wc, post.spec_retired() as int),
        forall|s: int| pre.spec_retired() <= s < post.spec_retired() && #[trigger] pre.req(s) is Some
            ==> pre.meta(s)->0.end as int <= wqe as int + 1,
        forall|s: int| #[trigger] post.req(s) is Some && post.req(s)->0.phase is InFlight
            ==> post.meta(s)->0.end as int > wqe as int + 1,
        post.pending_cache(pre, wqe, wc),
    ensures
        send_results(post, n, q),
{
    let e = m.cqs[c].entries[0];
    let qp = m.qps[q];
    let head = qp.sq_head;
    lemma_poll(m, n, c);
    safeverbs_spec::nic_proofs::lemma_head_is_first(m, c);
    assert(m.qp_wf(qp));
    assert(n.qps[q].sq_head == wqe + 1);
    // The poll keeps the outcomes before the head and retires the WQEs from it to `wqe`.
    assert forall|w: WqeRef| w.qp == q && w.queue is Send && w.seq < head
        && #[trigger] m.retired.contains_key(w)
        implies n.retired.contains_key(w) && n.retired[w] == m.retired[w] by {
        reveal(retires);
    }
    assert forall|w: WqeRef| w.qp == q && w.queue is Send && head <= w.seq <= wqe
        implies #[trigger] n.retired.contains_key(w) && n.retired[w] == qp.sq[w.seq - head].outcome->0 by {
        reveal(retires);
    }
    wc.lemma_outcome_ok();
    pre.lemma_view();
    post.lemma_view();
    reveal(send_results);
    assert forall|s: int| #[trigger] post.req(s) is Some implies send_result(post, n, q, s) by {
        let meta = pre.meta(s)->0;
        let range = send_range(q, meta);
        pre.lemma_meta(s);
        post.lemma_meta(s);
        pre.lemma_completion_observation(s, wqe, wc, post.spec_retired() as int);
        assert(send_range(q, post.meta(s)->0) == range);
        if s < pre.spec_retired() {
            lemma_aggregate_framed(range, m.retired, n.retired);
        } else {
            let seen = cached(meta.seen);
            let o = wc.outcome();
            lemma_prefix_polled(range, head, wqe as nat, m.retired, n.retired, seen, o);
            if s < post.spec_retired() {
                // Retired by this CQE.
                pre.lemma_retired_view(s, pre.retire_outcome(s, wqe, wc));
                let polled = if seen is None && o.status.is_err() && range.first <= wqe < range.end {
                    Some(o)
                } else {
                    seen
                };
                assert(retired_to(range, wqe as nat + 1) == range.end);
                lemma_prefix_aggregate(range, n.retired, polled);
                if meta.seen is None {
                    // If M retired its last WQE with a success, that is what the WR implies.
                    let last = range.last();
                    let k = last.seq - head;
                    assert(0 <= k < qp.sq.len() && qp.sq[k].wr == tails[s]);
                    assert(qp.sq[k].outcome is Some);
                    assert(n.retired[last] == qp.sq[k].outcome->0);
                    if n.retired[last].status is Success {
                        lemma_success_unique(tails[s].opcode, total_len(tails[s].sges),
                            meta.implied.outcome(), n.retired[last]);
                    }
                }
            } else {
                // Still in flight: it caches the CQE's error if it is its first.
                post.lemma_pending_cache_observation(pre, wqe, wc, s);
            }
        }
    }
}

impl<N: Device> Hw<N> {
    /// The send result and leases `take` removed: M's aggregate of the request's WQE
    /// interval, all of it retired, a success being what its last WR's operation defines
    /// (the length a READ asked for); or a flush error for a request whose doorbell was
    /// refused, which M never saw.
    pub closed spec fn send_taken_matches(&self, index: usize, seq: RingSeq, wc: Wc, leases: Leases<N>) -> bool {
        &&& self.has_qp(index as int)
        &&& {
            let e = self.qps@[index as int]->0;
            let range = send_range(e.id(), e.send.meta(seq as int)->0);
            let last = e.send_tails@[seq as int];
            ||| e.send.req(seq as int) == Some(crate::ring::ReqView {
                    phase: safeverbs_spec::abs::Phase::Complete(wc.outcome()),
                    waker: None,
                    detached: false,
                    leases,
                })
                && range.wf() && range.all_retired(self.nic().retired)
                && range.aggregate(self.nic().retired) == wc.outcome()
                && (wc.outcome().status is Success
                    ==> last.opcode.allows_outcome(total_len(last.sges), wc.outcome()))
            ||| e.send.req(seq as int) is None && e.send.refused(seq as int) == Some(leases)
                && wc.spec_status() == crate::wc::Status::WR_FLUSH_ERR
        }
    }

    /// What `take` removes from a send ring is what [`Hw::send_taken_matches`] says.
    pub(super) proof fn lemma_send_taken(&self, index: usize, seq: RingSeq, wc: Wc, leases: Leases<N>)
        requires
            self.inv(),
            self.has_qp(index as int),
            self.qps@[index as int]->0.send.req(seq as int) == Some(crate::ring::ReqView {
                phase: safeverbs_spec::abs::Phase::Complete(wc.outcome()),
                waker: None,
                detached: false,
                leases,
            }) || (self.qps@[index as int]->0.send.req(seq as int) is None
                && self.qps@[index as int]->0.send.refused(seq as int) == Some(leases)
                && wc.spec_status() == crate::wc::Status::WR_FLUSH_ERR),
        ensures
            self.send_taken_matches(index, seq, wc, leases),
    {
        reveal(send_results);
        assert(self.entry_ok(index as int, self.token@, self.nic()));
        self.qps@[index as int]->0.send.lemma_meta(seq as int);
    }
}

} // verus!
