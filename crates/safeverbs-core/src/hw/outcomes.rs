//! Receive results coupled to the WQEs M retired.
//!
//! Each receive request has one WQE. Unlike send batches, it never infers a
//! success from a later request's CQE, so its result must be that WQE's outcome.
#![allow(unused_imports)]

use super::*;

// On this branch the specs and proofs in this file still describe the representation before the
// fast data path (see docs/verification.md): the contracts were kept as they were, the body proofs
// of rewritten functions are at e28c2e8. A plain build erases them all.
verus! {

/// A receive request's one WQE: pending with nothing cached; or complete, its outcome being the
/// one M retired, and a success the payload or the notification M allows a receive.
pub(super) open spec fn recv_result<L>(r: Ring<L>, m: Nic::State, q: QpId, s: int) -> bool {
    let req = r.req(s)->0;
    let meta = r.meta(s);
    &&& meta is Some
    &&& meta->0.end as int == meta->0.first as int + 1
    &&& match req.phase {
        safeverbs_spec::abs::Phase::InFlight =>
            m.qps[q].rq_head <= meta->0.first && meta->0.seen is None,
        safeverbs_spec::abs::Phase::Complete(o) => {
            let w = WqeRef { qp: q, queue: safeverbs_spec::nic::Queue::Recv, seq: meta->0.first as nat };
            &&& w.seq < m.qps[q].rq_head && m.retired.contains_key(w) && m.retired[w] == o
            &&& o.status is Success ==> o.kind is Receive || (o.kind is WriteNotification && o.imm is Some)
        },
    }
}

/// Pending single-WQE receives have no cached result; complete requests match M.
#[verifier::opaque]
pub(super) open spec fn recv_outcomes<L>(r: Ring<L>, m: Nic::State, q: QpId) -> bool {
    &&& m.qps.contains_key(q)
    &&& forall|s: int| #[trigger] r.refused(s) is None
    &&& forall|s: int| #[trigger] r.req(s) is Some ==> recv_result(r, m, q, s)
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
    assert forall|s: int| #[trigger] r.req(s) is Some implies recv_result(r, n, q, s) by {
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

/// Posting a receive adds one pending singleton and preserves prior results.
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
    assert forall|s: int| #[trigger] post.req(s) is Some implies recv_result(post, m, q, s) by {
        if s != added {
            assert(recv_result(pre, m, q, s));
        }
    }
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
        wc.outcome().status is Success ==> wc.outcome().kind is Receive
            || (wc.outcome().kind is WriteNotification && wc.outcome().imm is Some),
    ensures recv_outcomes(post, n, q),
{
    reveal(recv_outcomes);
    pre.lemma_view();
    post.lemma_view();
    assert forall|s: int| #[trigger] post.req(s) is Some implies recv_result(post, n, q, s) by {
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
            assert(recv_result(pre, m, q, s));
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

impl<C, N: Device> Hw<C, N> {
    /// A completed receive request's observed result, before it is taken.
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

    /// The one WQE a receive request names, not a synthetic ID.
    pub closed spec fn recv_wqe(&self, index: usize, seq: RingSeq) -> Option<WqeRef> {
        if !self.has_qp(index as int) { None } else {
            let e = self.qps@[index as int]->0;
            match e.recv.meta(seq as int) {
                Some(meta) => Some(WqeRef { qp: e.id(), queue: safeverbs_spec::nic::Queue::Recv, seq: meta.first as nat }),
                None => None,
            }
        }
    }

    /// `t` is what the receive ring of QP `index` hands back of request `seq`.
    pub(super) open spec fn recv_taken_from(&self, index: usize, seq: RingSeq, t: Taken<Payload<C, N>>) -> bool {
        self.qps@[index as int]->0.recv.taken_from(seq as int, t)
    }

    /// The receive result, context and buffer removed by `take`: the outcome M retired for its
    /// one WQE, a message or a notification if it succeeded, and never a refusal, since a
    /// receive is posted at once or not at all.
    pub closed spec fn recv_taken_matches(&self, index: usize, seq: RingSeq, c: Completion<C, N>) -> bool {
        &&& self.has_qp(index as int)
        &&& self.qps@[index as int]->0.recv.req(seq as int) is Some
        &&& self.qps@[index as int]->0.recv.req(seq as int)->0.leases == (Payload { ctx: Some(c.ctx), leases: c.leases })
        &&& self.recv_wqe(index, seq) is Some
        &&& self.nic().retired.contains_key(self.recv_wqe(index, seq)->0)
        &&& {
            let o = self.nic().retired[self.recv_wqe(index, seq)->0];
            &&& self.recv_result(index, seq) == Some(o)
            &&& match c.result {
                Ok(done) => done.model() == o && done is Received,
                Err(CompletionError::Failed { status, position, .. }) =>
                    o.status.is_err() && o.status == status.model() && position == 0,
                Err(CompletionError::Refused) => false,
            }
        }
    }

    /// What the ring handed back of a receive holds a context and its buffer, and is no
    /// refused request.
    pub(super) proof fn lemma_recv_taken(&self, index: usize, seq: RingSeq, t: Taken<Payload<C, N>>)
        requires
            self.inv(),
            self.has_qp(index as int),
            self.recv_taken_from(index, seq, t),
        ensures
            t.leases.ctx is Some,
            t.leases.leases.inv() && t.leases.leases.fully_recorded(),
            !t.refused,
    {
        reveal(recv_outcomes);
        assert(self.entry_ok(index as int, self.token@, self.nic()));
    }

    /// `result` is what [`recv_result_of`] says of `t`.
    pub(super) open spec fn recv_result_from(
        &self, t: Taken<Payload<C, N>>, result: core::result::Result<Completed, CompletionError>,
    ) -> bool {
        match result {
            Ok(done) => t.wc.spec_ok() && (
                t.wc.outcome().kind is WriteNotification && t.wc.outcome().imm is None
                    || done.model() == t.wc.outcome()),
            Err(CompletionError::Failed { status, position, .. }) =>
                !t.wc.spec_ok() && status == t.wc.spec_status() && position == 0,
            Err(CompletionError::Refused) => false,
        }
    }

    /// The [`Completion`] made of what the ring handed back is what
    /// [`Hw::recv_taken_matches`] says.
    pub(super) proof fn lemma_recv_finished(&self, index: usize, seq: RingSeq, t: Taken<Payload<C, N>>, c: Completion<C, N>)
        requires
            self.inv(),
            self.has_qp(index as int),
            self.recv_taken_from(index, seq, t),
            t.leases == (Payload { ctx: Some(c.ctx), leases: c.leases }),
            self.recv_result_from(t, c.result),
        ensures
            c.leases.inv() && c.leases.fully_recorded(),
            self.recv_taken_matches(index, seq, c),
    {
        reveal(recv_outcomes);
        assert(self.entry_ok(index as int, self.token@, self.nic()));
        let e = self.qps@[index as int]->0;
        assert(recv_result(e.recv, self.nic(), e.id(), seq as int));
        t.wc.lemma_outcome_ok();
        t.wc.lemma_spec_status();
        if c.result is Ok {
            // M allows a receive a payload or a notification with immediate data.
            assert(t.wc.outcome().kind is Receive
                || (t.wc.outcome().kind is WriteNotification && t.wc.outcome().imm is Some));
        }
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
        let e = self.qps@[index as int]->0;
        assert(recv_result(e.recv, self.nic(), e.id(), seq as int));
    }
}

} // verus!
