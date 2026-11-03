//! Receive-result coupling to the actual retired WQEs of M.
//!
//! Each receive request has one WQE. Unlike send batches, it never infers a
//! success from a later request's CQE, so its result must be that WQE's outcome.
#![allow(unused_imports)]

use super::*;

verus! {

/// Pending single-WQE receives have no cached result; complete requests match M.
#[verifier::opaque]
pub(super) open spec fn recv_outcomes<L>(r: Ring<L>, m: Nic::State, q: QpId) -> bool {
    &&& m.qps.contains_key(q)
    &&& forall|s: int| #[trigger] r.refused(s) is None
    &&& forall|s: int| #[trigger] r.req(s) is Some ==> {
        let req = r.req(s)->0;
        let meta = r.meta(s);
        &&& meta is Some
        &&& meta->0.end as int == meta->0.first as int + 1
        &&& match req.phase {
            safeverbs_spec::abs::Phase::InFlight =>
                m.qps[q].rq_head <= meta->0.first && meta->0.seen is None,
            safeverbs_spec::abs::Phase::Complete(o) => {
                let w = WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Recv, seq: meta->0.first as nat };
                w.seq < m.qps[q].rq_head && m.retired.contains_key(w) && m.retired[w] == o
            },
        }
    }
}

/// A newly created receive ring contains no request that could have an outcome.
pub(super) proof fn lemma_recv_empty<L>(r: Ring<L>, m: Nic::State, q: QpId)
    requires r.inv(), m.qps.contains_key(q), forall|s: int| #[trigger] r.req(s) is None,
        forall|s: int| #[trigger] r.refused(s) is None,
    ensures recv_outcomes(r, m, q),
{
    reveal(recv_outcomes);
}

/// An unchanged receive ring and its own M history retain result correspondence.
pub(super) proof fn lemma_recv_framed<L>(r: Ring<L>, m: Nic::State, n: Nic::State, q: QpId)
    requires recv_outcomes(r, m, q), n.qps.contains_key(q),
        n.qps[q].rq_head == m.qps[q].rq_head,
        forall|w: WqeRef| w.qp == q && w.queue is Recv && w.seq < m.qps[q].rq_head
            && #[trigger] m.retired.contains_key(w)
            ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w],
    ensures recv_outcomes(r, n, q),
{
    reveal(recv_outcomes);
    assert forall|s: int| #[trigger] r.req(s) is Some implies {
        let req = r.req(s)->0;
        let meta = r.meta(s);
        &&& meta is Some
        &&& meta->0.end as int == meta->0.first as int + 1
        &&& match req.phase {
            safeverbs_spec::abs::Phase::InFlight =>
                n.qps[q].rq_head <= meta->0.first && meta->0.seen is None,
            safeverbs_spec::abs::Phase::Complete(o) => {
                let w = WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Recv, seq: meta->0.first as nat };
                w.seq < n.qps[q].rq_head && n.retired.contains_key(w) && n.retired[w] == o
            },
        }
    } by {
        if r.req(s)->0.phase is Complete {
            let w = WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Recv, seq: r.meta(s)->0.first as nat };
            assert(m.retired.contains_key(w));
        }
    }
}

/// Removing results or changing wakers cannot change an existing receive outcome.
pub(super) proof fn lemma_recv_kept<L>(pre: Ring<L>, post: Ring<L>, m: Nic::State, q: QpId)
    requires recv_outcomes(pre, m, q),
        forall|s: int| #[trigger] post.refused(s) is Some ==> pre.refused(s) is Some,
        forall|s: int| #[trigger] post.req(s) is Some ==> pre.req(s) is Some
            && post.req(s)->0.phase == pre.req(s)->0.phase && post.meta(s) == pre.meta(s),
    ensures recv_outcomes(post, m, q),
{ reveal(recv_outcomes); }

/// A receive queue has no written-but-unposted requests between admissions.
pub(super) proof fn lemma_recv_no_staged<L>(r: Ring<L>)
    requires r.inv(), r.spec_wqe_posted() == r.spec_wqe_committed(),
    ensures forall|s: int| #[trigger] r.staged(s) is None,
{
    assert forall|s: int| #[trigger] r.staged(s) is None by { r.lemma_meta(s); }
}

/// Actual receive admission adds one pending singleton and preserves prior results.
pub(super) proof fn lemma_recv_added<L>(pre: Ring<L>, post: Ring<L>, m: Nic::State, q: QpId, added: int)
    requires recv_outcomes(pre, m, q),
        forall|s: int| #[trigger] post.refused(s) is Some ==> pre.refused(s) is Some,
        post.req(added) is Some, post.req(added)->0.phase is InFlight,
        post.meta(added) is Some, post.meta(added)->0.seen is None,
        post.meta(added)->0.end as int == post.meta(added)->0.first as int + 1,
        m.qps[q].rq_head <= post.meta(added)->0.first,
        forall|s: int| #[trigger] post.req(s) is Some && s != added ==> pre.req(s) is Some
            && post.req(s)->0.phase == pre.req(s)->0.phase && post.meta(s) == pre.meta(s),
    ensures recv_outcomes(post, m, q),
{
    reveal(recv_outcomes);
    assert forall|s: int| #[trigger] post.req(s) is Some implies {
        let req = post.req(s)->0;
        let meta = post.meta(s);
        &&& meta is Some
        &&& meta->0.end as int == meta->0.first as int + 1
        &&& match req.phase {
            safeverbs_spec::abs::Phase::InFlight => m.qps[q].rq_head <= meta->0.first && meta->0.seen is None,
            safeverbs_spec::abs::Phase::Complete(o) => {
                let w = WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Recv, seq: meta->0.first as nat };
                w.seq < m.qps[q].rq_head && m.retired.contains_key(w) && m.retired[w] == o
            },
        }
    } by {}
}

/// A real receive CQE completes its own singleton; no later request supplies data.
pub(super) proof fn lemma_recv_completed<L>(
    pre: Ring<L>, post: Ring<L>, m: Nic::State, n: Nic::State, q: QpId, wqe: u64, wc: Wc,
)
    requires
        pre.inv(), post.inv(), recv_outcomes(pre, m, q), n.qps.contains_key(q),
        forall|s: int| #[trigger] post.refused(s) is Some ==> pre.refused(s) is Some,
        wqe == m.qps[q].rq_head, n.qps[q].rq_head == wqe as nat + 1,
        pre.spec_wqe_retired() <= wqe < pre.spec_wqe_committed(),
        n.retired.contains_key(WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Recv, seq: wqe as nat }),
        n.retired[WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Recv, seq: wqe as nat }] == wc.outcome(),
        forall|w: WqeRef| w.qp == q && w.queue is Recv && w.seq < m.qps[q].rq_head
            && #[trigger] m.retired.contains_key(w)
            ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w],
        pre.spec_retired() <= post.spec_retired() <= pre.spec_committed(),
        forall|s: int| #[trigger] post.req(s) == if pre.spec_retired() <= s < post.spec_retired() {
            pre.retired_view(s, pre.retire_outcome(s, wqe, wc))
        } else { pre.req(s) },
        forall|s: int| #[trigger] post.meta(s)
            == pre.completion_meta(s, wqe, wc, post.spec_retired() as int),
        forall|s: int| pre.spec_retired() <= s < post.spec_retired() && #[trigger] pre.req(s) is Some
            ==> pre.meta(s)->0.end as int <= wqe as int + 1,
        forall|s: int| #[trigger] post.req(s) is Some && post.req(s)->0.phase is InFlight
            ==> post.meta(s)->0.end as int > wqe as int + 1,
    ensures recv_outcomes(post, n, q),
{
    reveal(recv_outcomes);
    pre.lemma_view();
    post.lemma_view();
    assert forall|s: int| #[trigger] post.req(s) is Some implies {
        let req = post.req(s)->0;
        let meta = post.meta(s);
        &&& meta is Some
        &&& meta->0.end as int == meta->0.first as int + 1
        &&& match req.phase {
            safeverbs_spec::abs::Phase::InFlight => n.qps[q].rq_head <= meta->0.first && meta->0.seen is None,
            safeverbs_spec::abs::Phase::Complete(o) => {
                let w = WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Recv, seq: meta->0.first as nat };
                w.seq < n.qps[q].rq_head && n.retired.contains_key(w) && n.retired[w] == o
            },
        }
    } by {
        pre.lemma_meta(s);
        post.lemma_meta(s);
        pre.lemma_completion_observation(s, wqe, wc, post.spec_retired() as int);
        if pre.spec_retired() <= s < post.spec_retired() {
            pre.lemma_retired_view(s, pre.retire_outcome(s, wqe, wc));
            assert(pre.req(s) is Some);
            assert(pre.req(s)->0.phase is InFlight);
            assert(pre.meta(s)->0.first == wqe);
            assert(pre.retire_outcome(s, wqe, wc) == wc);
            assert(post.req(s)->0.phase == safeverbs_spec::abs::Phase::Complete(wc.outcome()));
        } else {
            assert(pre.req(s) is Some);
            if pre.req(s)->0.phase is InFlight {
                assert(s >= post.spec_retired());
                assert(wqe < pre.meta(s)->0.first || pre.meta(s)->0.end as int <= wqe as int + 1);
                assert(post.meta(s) == pre.meta(s));
            } else {
                let w = WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Recv, seq: pre.meta(s)->0.first as nat };
                assert(m.retired.contains_key(w));
                assert(n.retired.contains_key(w));
            }
        }
    }
}

impl<N: Device> Hw<N> {
    /// A completed actual receive request's observed result, before it is taken.
    pub closed spec fn recv_result(&self, index: usize, seq: RingSeq) -> Option<safeverbs_spec::types::Outcome> {
        if !self.has_qp(index as int) { None } else {
            match self.qps@[index as int]->0.recv.req(seq as int) {
                Some(req) => match req.phase {
                    safeverbs_spec::abs::Phase::Complete(o) => Some(o),
                    _ => None,
                },
                None => None,
            }
        }
    }

    /// The actual singleton WQE named by a receive request, not a synthetic ID.
    pub closed spec fn recv_wqe(&self, index: usize, seq: RingSeq) -> Option<WqeRef> {
        if !self.has_qp(index as int) { None } else {
            let e = self.qps@[index as int]->0;
            match e.recv.meta(seq as int) {
                Some(meta) => Some(WqeRef { qp: e.id(), queue: safeverbs_spec::nic::Queue::Recv, seq: meta.first as nat }),
                None => None,
            }
        }
    }

    /// The actual receive result and resource value removed by `take`.
    pub closed spec fn recv_taken_matches(&self, index: usize, seq: RingSeq, wc: Wc, leases: Leases<N>) -> bool {
        &&& self.has_qp(index as int)
        &&& self.qps@[index as int]->0.recv.req(seq as int) is Some
        &&& self.qps@[index as int]->0.recv.req(seq as int)->0.leases == leases
        &&& self.recv_result(index, seq) == Some(wc.outcome())
        &&& self.recv_wqe(index, seq) is Some
        &&& self.nic().retired.contains_key(self.recv_wqe(index, seq)->0)
        &&& self.nic().retired[self.recv_wqe(index, seq)->0] == wc.outcome()
    }

    /// A receive cannot return the synthetic refusal result of a send doorbell.
    pub(super) proof fn lemma_recv_taken(&self, index: usize, seq: RingSeq, wc: Wc, leases: Leases<N>)
        requires self.inv(), self.has_qp(index as int),
            self.qps@[index as int]->0.recv.req(seq as int) == Some(crate::ring::ReqView {
                phase: safeverbs_spec::abs::Phase::Complete(wc.outcome()), waker: None, detached: false, leases,
            }) || (self.qps@[index as int]->0.recv.req(seq as int) is None
                && self.qps@[index as int]->0.recv.refused(seq as int) == Some(leases)),
        ensures self.recv_taken_matches(index, seq, wc, leases),
    {
        reveal(recv_outcomes);
        assert(self.entry_ok(index as int, self.token@, self.nic()));
        assert(self.recv_result(index, seq) == Some(wc.outcome()));
        self.lemma_recv_result(index, seq);
    }

    /// Every completed native receive result equals its own retired M outcome.
    pub proof fn lemma_recv_result(&self, index: usize, seq: RingSeq)
        requires self.inv(), self.recv_result(index, seq) is Some,
        ensures self.recv_wqe(index, seq) is Some,
            self.nic().retired.contains_key(self.recv_wqe(index, seq)->0),
            self.nic().retired[self.recv_wqe(index, seq)->0] == self.recv_result(index, seq)->0,
    {
        reveal(recv_outcomes);
        assert(self.entry_ok(index as int, self.token@, self.nic()));
    }
}

} // verus!
