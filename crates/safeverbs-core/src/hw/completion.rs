//! A completion on its way to its request. M pops the CQE when it is read; the ring of the queue it
//! names hears of it when it is routed, and in between the program may do anything. So the QPs stay
//! coupled to M with some CQEs read and not routed (`pending_ok`): a poll adds to them
//! ([`lemma_table_polled`]), and a step of a walk, routing the oldest, takes from them
//! ([`lemma_table_walked`]), with M as it is.
#![allow(unused_imports)]
use super::*;
#[cfg(verus_keep_ghost)]
use outcomes::recv_reports_outcome;
#[cfg(verus_keep_ghost)]
use send_outcomes::{send_range, send_reports_outcome};

verus! {

/// The queue M names by this name.
pub open spec fn queue_of(side: Side) -> Queue {
    match side {
        Side::Send => Queue::Send,
        Side::Recv => Queue::Recv,
    }
}

/// A ring went on with every request as it was but those that left it: what its owner took or gave
/// up, and the detached error, which may be taken or come from a refusal.
pub(super) open spec fn ring_kept<C, N: Device>(post: Ring<Payload<C, N>>, pre: Ring<Payload<C, N>>) -> bool {
    &&& post.inv()
    &&& post.same_wqes(&pre)
    &&& keeps_leases(post, pre)
    &&& ring_resources_ok(post)
    &&& forall|s: int| #[trigger] post.refused(s) is Some ==> pre.refused(s) is Some
    &&& forall|s: int| #[trigger] post.req(s) is Some ==> pre.req(s) is Some
        && post.req(s)->0.phase == pre.req(s)->0.phase
    &&& forall|s: int| #[trigger] post.meta(s) is Some ==> post.meta(s) == pre.meta(s)
    &&& post.detached_refused(&pre) || post.spec_detached_error() is None
}

/// Every request of `r` was in `older`, with what it holds and its WQEs, and every one refused.
pub(super) open spec fn same_requests<C, N: Device>(r: Ring<Payload<C, N>>, older: Ring<Payload<C, N>>) -> bool {
    &&& forall|s: int| #[trigger] r.req(s) is Some ==> {
        &&& older.req(s) is Some
        &&& r.req(s)->0.leases == older.req(s)->0.leases
        &&& r.meta(s)->0.first == older.meta(s)->0.first
        &&& r.meta(s)->0.end == older.meta(s)->0.end
    }
    &&& forall|s: int| #[trigger] r.refused(s) is Some ==> r.refused(s) == older.refused(s)
}

/// `same_requests` is transitive.
proof fn lemma_same_requests_trans<C, N: Device>(a: Ring<Payload<C, N>>, b: Ring<Payload<C, N>>, c: Ring<Payload<C, N>>)
    requires
        same_requests(a, b),
        same_requests(b, c),
    ensures
        same_requests(a, c),
{
    assert forall|s: int| #[trigger] a.req(s) is Some implies {
        &&& c.req(s) is Some
        &&& a.req(s)->0.leases == c.req(s)->0.leases
        &&& a.meta(s)->0.first == c.meta(s)->0.first
        &&& a.meta(s)->0.end == c.meta(s)->0.end
    } by {
        assert(b.req(s) is Some);
    }
    assert forall|s: int| #[trigger] a.refused(s) is Some implies a.refused(s) == c.refused(s) by {
        assert(b.refused(s) is Some);
    }
}

/// What a step of a walk leaves of a ring's requests.
proof fn lemma_walked_requests<C, N: Device>(post: Ring<Payload<C, N>>, pre: Ring<Payload<C, N>>, recv: bool, wqe: u64, wc: Wc, spent: bool)
    requires
        pre.inv(),
        post.walked(&pre, recv, wqe, wc, spent),
    ensures
        same_requests(post, pre),
{
    assert forall|s: int| #[trigger] post.req(s) is Some implies {
        &&& pre.req(s) is Some
        &&& post.req(s)->0.leases == pre.req(s)->0.leases
        &&& post.meta(s)->0.first == pre.meta(s)->0.first
        &&& post.meta(s)->0.end == pre.meta(s)->0.end
    } by {
        post.lemma_meta(s);
        pre.lemma_meta(s);
        pre.lemma_completion_observation(s, recv, wqe, wc, post.spec_retired() as int, spent);
        if pre.spec_retired() <= s < post.spec_retired() {
            pre.lemma_retired_view(s, pre.retire_outcome(s, recv, wqe, wc));
        }
    }
}

/// A step of a walk leaves every WQE still in flight with what its request holds.
proof fn lemma_walked_leases<C, N: Device>(post: Ring<Payload<C, N>>, pre: Ring<Payload<C, N>>, recv: bool, wqe: u64, wc: Wc, spent: bool)
    requires
        post.walked(&pre, recv, wqe, wc, spent),
    ensures
        keeps_leases(post, pre),
{
    assert forall|s: int, w: int| pre.dma(s, w) is Some && post.spec_wqe_retired() <= w < post.spec_wqe_posted()
        implies #[trigger] post.dma(s, w) is Some && post.dma(s, w)->0.leases == pre.dma(s, w)->0.leases by {
        assert(post.dma(s, w) == pre.dma(s, w));
    }
}

/// What taking a result leaves of a ring.
pub(super) proof fn lemma_take_kept<C, N: Device>(post: Ring<Payload<C, N>>, pre: Ring<Payload<C, N>>, seq: int, taken: bool)
    requires
        pre.inv(),
        post.inv(),
        ring_resources_ok(pre),
        post.left_by_take(&pre, seq, taken),
    ensures
        ring_kept(post, pre),
        same_requests(post, pre),
{
    lemma_resources_kept(pre, post);
    assert forall|s: int, w: int| pre.dma(s, w) is Some && post.spec_wqe_retired() <= w < post.spec_wqe_posted()
        implies #[trigger] post.dma(s, w) is Some && post.dma(s, w)->0.leases == pre.dma(s, w)->0.leases by {
        assert(post.dma(s, w) == pre.dma(s, w));
    }
    assert forall|s: int| #[trigger] post.req(s) is Some implies {
        &&& pre.req(s) is Some
        &&& post.req(s)->0.leases == pre.req(s)->0.leases
        &&& post.meta(s)->0.first == pre.meta(s)->0.first
        &&& post.meta(s)->0.end == pre.meta(s)->0.end
    } by {
        post.lemma_meta(s);
        pre.lemma_meta(s);
    }
}

/// What giving up a request leaves of a ring: nothing else changes.
pub(super) proof fn lemma_given_up_kept<C, N: Device>(post: Ring<Payload<C, N>>, pre: Ring<Payload<C, N>>, seq: int)
    requires
        pre.inv(),
        post.inv(),
        ring_resources_ok(pre),
        post.gave_up(&pre, seq),
    ensures
        ring_kept(post, pre),
{
    lemma_resources_given_up(pre, post, seq);
    assert forall|s: int, w: int| pre.dma(s, w) is Some && post.spec_wqe_retired() <= w < post.spec_wqe_posted()
        implies #[trigger] post.dma(s, w) is Some && post.dma(s, w)->0.leases == pre.dma(s, w)->0.leases by {
    }
}

/// What registering a waker leaves of a ring: nothing else changes.
pub(super) proof fn lemma_waker_kept<C, N: Device>(post: Ring<Payload<C, N>>, pre: Ring<Payload<C, N>>)
    requires
        pre.inv(),
        post.inv(),
        ring_resources_ok(pre),
        post.same_wqes(&pre),
        forall|s: int| #[trigger] post.req(s) is Some ==> pre.req(s) is Some
            && post.req(s)->0.phase == pre.req(s)->0.phase
            && post.req(s)->0.leases == pre.req(s)->0.leases
            && post.req(s)->0.detached == pre.req(s)->0.detached,
        forall|s: int| #[trigger] post.staged(s) is Some ==> pre.staged(s) is Some
            && post.staged(s)->0.leases == pre.staged(s)->0.leases
            && post.staged(s)->0.detached == pre.staged(s)->0.detached,
        forall|s: int| #[trigger] post.refused(s) == pre.refused(s),
        post.same_dma(&pre),
        post.same_meta(&pre),
        post.spec_detached_error() == pre.spec_detached_error(),
    ensures
        ring_kept(post, pre),
{
    assert forall|s: int| #[trigger] post.req(s) is Some implies resource_at(pre, s, post.req(s)->0.leases, post.req(s)->0.detached) by {}
    assert forall|s: int| #[trigger] post.staged(s) is Some implies resource_at(pre, s, post.staged(s)->0.leases, post.staged(s)->0.detached) by {}
    assert forall|s: int| #[trigger] post.refused(s) is Some implies resource_at(pre, s, post.refused(s)->0, false) by {}
    lemma_resources_kept(pre, post);
}

/// `ring_kept` is transitive, the detached error as it was.
pub(super) proof fn lemma_ring_kept_trans<C, N: Device>(a: Ring<Payload<C, N>>, b: Ring<Payload<C, N>>, c: Ring<Payload<C, N>>)
    requires
        ring_kept(a, b),
        ring_kept(b, c),
        a.spec_detached_error() == b.spec_detached_error(),
        b.spec_detached_error() == c.spec_detached_error(),
    ensures
        ring_kept(a, c),
{
    assert forall|s: int, w: int| c.dma(s, w) is Some && a.spec_wqe_retired() <= w < a.spec_wqe_posted()
        implies #[trigger] a.dma(s, w) is Some && a.dma(s, w)->0.leases == c.dma(s, w)->0.leases by {
        assert(b.dma(s, w) is Some);
    }
    assert forall|s: int| #[trigger] a.refused(s) is Some implies c.refused(s) is Some by {
        assert(b.refused(s) is Some);
    }
    assert forall|s: int| #[trigger] a.req(s) is Some implies c.req(s) is Some
        && a.req(s)->0.phase == c.req(s)->0.phase by {
        assert(b.req(s) is Some);
    }
    assert forall|s: int| #[trigger] a.meta(s) is Some implies a.meta(s) == c.meta(s) by {
        assert(b.meta(s) is Some);
    }
}

/// A ring that changed in nothing the coupling reads is kept.
pub(super) proof fn lemma_same_kept<C, N: Device>(post: Ring<Payload<C, N>>, pre: Ring<Payload<C, N>>)
    requires
        pre.inv(),
        post.inv(),
        ring_resources_ok(pre),
        post.same_wqes(&pre),
        post.same_but_results(&pre),
        post.spec_detached_error() == pre.spec_detached_error()
            || post.spec_detached_error() is None,
    ensures
        ring_kept(post, pre),
        same_requests(post, pre),
{
    lemma_resources_kept(pre, post);
    assert forall|s: int, w: int| pre.dma(s, w) is Some && post.spec_wqe_retired() <= w < post.spec_wqe_posted()
        implies #[trigger] post.dma(s, w) is Some && post.dma(s, w)->0.leases == pre.dma(s, w)->0.leases by {
        assert(post.dma(s, w) == pre.dma(s, w));
    }
    assert forall|s: int| #[trigger] post.req(s) is Some implies {
        &&& pre.req(s) is Some
        &&& post.req(s)->0.leases == pre.req(s)->0.leases
        &&& post.meta(s)->0.first == pre.meta(s)->0.first
        &&& post.meta(s)->0.end == pre.meta(s)->0.end
    } by {
        post.lemma_meta(s);
        pre.lemma_meta(s);
    }
}

impl<C, N: Device> HwQp<C, N> {
    /// The entry keeps its name in M and the configuration its typed key relies on
    /// ([`Hw::same_table`]).
    pub(super) open spec fn same_config(&self, pre: Self) -> bool {
        &&& self.id() == pre.id()
        &&& self.qp_type == pre.qp_type
        &&& self.atomics == pre.atomics
        &&& self.caps == pre.caps
        &&& self.pd_id == pre.pd_id
        &&& self.max_len == pre.max_len
        &&& self.reliable == pre.reliable
    }

    /// The same QP as `pre`, as far as the coupling reads it, but for its rings and what was
    /// written on it.
    pub(super) open spec fn same_qp(&self, pre: Self) -> bool {
        &&& self.same_config(pre)
        &&& qp_of(self.hw) == qp_of(pre.hw)
        &&& self.pd == pre.pd
        &&& self.phase == pre.phase
        &&& self.takes_work ==> self.phase == Phase::Ready
        &&& self.spare_leases@.len() == 0
        &&& self.spare_wqes@.len() == 0 || self.spare_wqes@.len() == self.send.spec_max_wqes()
        &&& self.building@.len() <= MAX_SGES
    }

    /// Hands the completion `wc` of WQE `e.seq`, in flight, to the ring of its queue, which retires
    /// every request it ends.
    #[inline(always)]
    pub(super) fn complete_cqe(&mut self, wc: &Wc, wakes: &mut WakeBatch, Ghost(e): Ghost<Cqe>)
        requires
            ring_of(*old(self), e.queue).inv(),
            (wc.wr_id & RECV_BIT != 0) == (e.queue is Recv),
            wc.wr_id & WQE_MASK == (e.seq as u64) & WQE_MASK,
            ring_of(*old(self), e.queue).spec_wqe_retired() <= e.seq
                < ring_of(*old(self), e.queue).spec_wqe_committed(),
            e.seq - ring_of(*old(self), e.queue).spec_wqe_retired() < WQE_SPAN,
        ensures
            *final(self) == (HwQp { send: final(self).send, recv: final(self).recv, ..*old(self) }),
            ring_of(*final(self), e.queue).walked(&ring_of(*old(self), e.queue), e.queue is Recv, e.seq as u64, *wc, true),
            ring_of(*final(self), e.queue).completed_with(&ring_of(*old(self), e.queue), e.queue is Recv, e.seq as u64, *wc, true),
            ring_of(*final(self), other_queue(e.queue)) == ring_of(*old(self), other_queue(e.queue)),
    {
        let recv = is_recv(wc.wr_id);
        let ring = if recv { &mut self.recv } else { &mut self.send };
        let ghost before = *ring;
        let wqe = wqe_of(wc.wr_id, ring.wqe_retired());
        proof {
            assert((e.seq as u64) & WQE_MASK == wc.wr_id & WQE_MASK);
            assert(wqe == e.seq);
        }
        let _known = ring.complete_with_wakes(recv, wqe, wc, wakes);
        proof {
            (*ring).lemma_completed_walked(&before, recv, wqe, *wc, true);
        }
    }

    /// A change of the rings that leaves their requests kept leaves the QP coupled.
    pub(super) proof fn lemma_kept(&self, pre: &Self, index: u16, m: Nic::State, sh: Share, written: Seq<SendWr>)
        requires
            pre.ok(index, m, sh, pre.started, written),
            self.same_qp(*pre),
            self.started == pre.started,
            self.send_tails == pre.send_tails,
            ring_kept(self.send, pre.send),
            ring_kept(self.recv, pre.recv),
        ensures
            self.ok(index, m, sh, self.started, written),
    {
        let q = pre.id();
        let qp = m.qps[q];
        lemma_queues_kept(*pre, *self, qp);
        assert forall|s: int| #[trigger] self.recv.req(s) is Some implies self.recv.meta(s) == pre.recv.meta(s) by {
            self.recv.lemma_meta(s);
        }
        assert forall|s: int| #[trigger] self.send.req(s) is Some implies self.send.meta(s) == pre.send.meta(s) by {
            self.send.lemma_meta(s);
        }
        outcomes::lemma_recv_kept(pre.recv, self.recv, m, q);
        send_outcomes::lemma_results_kept(pre.send, self.send, m, q);
        assert forall|s: int| #[trigger] self.send.meta(s) is Some implies pre.send.meta(s) is Some
            && (self.send.meta(s)->0.outcome is Ok ==> self.send.meta(s)->0.outcome == pre.send.meta(s)->0.outcome)
            && self.send_tails@[s] == pre.send_tails@[s] && self.send_tails@.contains_key(s) by {
        }
        send_outcomes::lemma_implied_kept(pre.send, self.send, pre.send_tails@, self.send_tails@);
        assert forall|s: int| #[trigger] self.send.meta(s) is Some implies pre.send.meta(s) is Some
            && self.send.meta(s)->0.first == pre.send.meta(s)->0.first
            && self.send.meta(s)->0.end == pre.send.meta(s)->0.end by {
        }
        assert forall|j: int| 0 <= j < qp.sq.len() implies qp.sq_head + j < qp.sq_head + qp.sq.len()
            && (#[trigger] qp.sq[j]).wr == qp.sq[qp.sq_head + j - qp.sq_head].wr by {
        }
        send_outcomes::lemma_bound_kept(pre.send, self.send, pre.send_tails@, qp, qp, written, m.retired, m.retired, q);
        self.send.lemma_wqes();
        pre.send.lemma_wqes();
        assert forall|j: int| 0 <= j < written.len()
            implies written_backed(*self, self.send.spec_wqe_committed() + j, #[trigger] written[j]) by {
            lemma_written_kept(*pre, *self, self.send.spec_wqe_committed() + j, written[j]);
        }
    }

    /// A step of a completion's walk, on the ring of its queue, leaves the QP coupled, with M as it
    /// is: what the walk retires it retires with M's outcomes.
    pub(super) proof fn lemma_walked(&self, pre: &Self, index: u16, m: Nic::State, sh: Share, written: Seq<SendWr>, wc: Wc, e: Cqe, spent: bool)
        requires
            pre.ok(index, m, sh, pre.started, written),
            self.same_qp(*pre),
            self.started == pre.started,
            self.send_tails == pre.send_tails,
            e.qp == pre.id(),
            ring_of(*self, e.queue).walked(&ring_of(*pre, e.queue), e.queue is Recv, e.seq as u64, wc, spent),
            ring_of(*self, other_queue(e.queue)) == ring_of(*pre, other_queue(e.queue)),
            e.seq < head_of(m.qps[e.qp], e.queue),
            ring_of(*pre, e.queue).spec_wqe_retired() <= e.seq,
            ring_of(*pre, e.queue).spec_routed() <= e.seq,
            m.retired.contains_key(cqe_wqe(e)),
            m.retired[cqe_wqe(e)] == wc.outcome(),
            e.queue is Recv && wc.outcome().status is Success ==> wc.outcome().kind is Receive
                || (wc.outcome().kind is WriteNotification && wc.outcome().imm is Some),
            e.queue is Recv ==> ring_of(*pre, e.queue).spec_routed() == e.seq,
            e.queue is Send ==> forall|n: nat| ring_of(*pre, e.queue).spec_routed() <= n < e.seq
                ==> #[trigger] send_outcomes::send_succeeded(m.retired, e.qp, n),
        ensures
            self.ok(index, m, sh, self.started, written),
    {
        let q = pre.id();
        let qp = m.qps[q];
        let wqe = e.seq as u64;
        let recv = e.queue is Recv;
        let (r0, r1) = (ring_of(*pre, e.queue), ring_of(*self, e.queue));
        r0.lemma_wqes();
        r1.lemma_wqes();
        lemma_resources_walked(r0, r1, recv, wqe, wc, spent);
        lemma_walked_leases(r1, r0, recv, wqe, wc, spent);
        assert(keeps_leases(ring_of(*self, other_queue(e.queue)), ring_of(*pre, other_queue(e.queue))));
        lemma_queues_kept(*pre, *self, qp);
        if recv {
            assert(self.send == pre.send);
            assert(self.recv == r1 && pre.recv == r0);
            outcomes::lemma_recv_routed(r0, r1, m, q, wqe, wc, spent);
        } else {
            assert(self.recv == pre.recv);
            assert(self.send == r1 && pre.send == r0);
            send_outcomes::lemma_results_routed(r0, r1, pre.send_tails@, written, m, q, wqe, wc, spent);
            assert forall|s: int| #[trigger] r1.meta(s) is Some implies r0.meta(s) is Some
                && (r1.meta(s)->0.outcome is Ok ==> r1.meta(s)->0.outcome == r0.meta(s)->0.outcome)
                && pre.send_tails@[s] == pre.send_tails@[s] && pre.send_tails@.contains_key(s) by {
                r0.lemma_meta(s);
                r1.lemma_meta(s);
                r0.lemma_view();
                r1.lemma_view();
                r0.lemma_completion_observation(s, false, wqe, wc, r1.spec_retired() as int, spent);
                assert(r1.meta(s) == r0.completion_meta(s, false, wqe, wc, r1.spec_retired() as int, spent));
                if spent && r1.req(s) is Some && r1.req(s)->0.phase is InFlight {
                    r1.lemma_pending_cache_observation(r0, wqe, wc, s);
                }
            }
            send_outcomes::lemma_implied_kept(r0, r1, pre.send_tails@, pre.send_tails@);
            assert forall|s: int| #[trigger] r1.meta(s) is Some implies r0.meta(s) is Some
                && r1.meta(s)->0.first == r0.meta(s)->0.first
                && r1.meta(s)->0.end == r0.meta(s)->0.end by {
                r0.lemma_completion_observation(s, false, wqe, wc, r1.spec_retired() as int, spent);
                assert(r1.meta(s) == r0.completion_meta(s, false, wqe, wc, r1.spec_retired() as int, spent));
            }
            assert forall|s: int| #[trigger] r1.req(s) is Some implies r0.req(s) is Some by {
                if r0.spec_retired() <= s < r1.spec_retired() {
                    r0.lemma_retired_view(s, r0.retire_outcome(s, false, wqe, wc));
                }
            }
            assert forall|j: int| 0 <= j < qp.sq.len() implies qp.sq_head + j < qp.sq_head + qp.sq.len()
                && (#[trigger] qp.sq[j]).wr == qp.sq[qp.sq_head + j - qp.sq_head].wr by {
            }
            send_outcomes::lemma_bound_kept(r0, r1, pre.send_tails@, qp, qp, written, m.retired, m.retired, q);
        }
        self.send.lemma_wqes();
        pre.send.lemma_wqes();
        assert forall|j: int| 0 <= j < written.len()
            implies written_backed(*self, self.send.spec_wqe_committed() + j, #[trigger] written[j]) by {
            lemma_written_kept(*pre, *self, self.send.spec_wqe_committed() + j, written[j]);
        }
    }

    /// M popping a CQE of this QP leaves it coupled: the ring's WQEs it pops stay with their requests
    /// until the ring hears of them.
    pub(super) proof fn lemma_popped(&self, index: u16, t: NicToken, before: Nic::State, after: Nic::State)
        requires
            nic_inv(before),
            share_wf(before, t.share()),
            self.ok(index, before, t.share(), t.started(self.id()), t.written(self.id())),
            nic_step(before, after, Nic::Step::poll_cq(t.share().cq)),
            before.cqs[t.share().cq].entries[0].qp == self.id(),
        ensures
            self.ok(index, after, t.share(), t.started(self.id()), t.written(self.id())),
    {
        let sh = t.share();
        let c = sh.cq;
        let e = before.cqs[c].entries[0];
        let q = e.qp;
        let qp = before.qps[q];
        let qp2 = after.qps[q];
        let written = t.written(q);
        safeverbs_spec::nic_proofs::lemma_head_is_first(before, c);
        lemma_poll(before, after, c);
        assert(before.qp_wf(qp));
        lemma_queued_outcomes_step(before, after, Nic::Step::poll_cq(c), q);
        reveal(retires);
        let nn = (e.seq + 1 - head_of(qp, e.queue)) as int;
        assert forall|w: WqeRef| w.qp == q && w.queue is Recv && w.seq < qp.rq_head
            && #[trigger] before.retired.contains_key(w)
            implies after.retired.contains_key(w) && after.retired[w] == before.retired[w] by {
        }
        assert forall|w: WqeRef| w.qp == q && w.queue is Send && w.seq < qp.sq_head
            && #[trigger] before.retired.contains_key(w)
            implies after.retired.contains_key(w) && after.retired[w] == before.retired[w] by {
        }
        if e.queue is Send {
            assert forall|k: int| 0 <= k < qp2.sq.len() implies (#[trigger] qp2.sq[k]).wr.wr_id == spec_wr_id(
                index,
                false,
                (qp2.sq_head + k) as u64,
            ) by {
                assert(qp2.sq[k] == qp.sq[k + nn]);
            }
            assert(sq_linked(self.send, index, qp2));
            assert(rq_linked(self.recv, index, qp2));
        } else {
            assert forall|k: int| 0 <= k < qp2.rq.len() implies (#[trigger] qp2.rq[k]).wr.wr_id == spec_wr_id(
                index,
                true,
                (qp2.rq_head + k) as u64,
            ) by {
                assert(qp2.rq[k] == qp.rq[k + nn]);
            }
            assert(sq_linked(self.send, index, qp2));
            assert(rq_linked(self.recv, index, qp2));
        }
        assert forall|k: int| 0 <= k < qp2.sq.len() implies {
            let wr = (#[trigger] qp2.sq[k]).wr;
            &&& !wr.inline
            &&& ring_backed(self.send, qp2.sq_head + k, wr.wr_id, Some(wr.opcode), wr.sges)
        } by {
            let old_k = if e.queue is Send { k + nn } else { k };
            assert(qp2.sq[k].wr == qp.sq[old_k].wr);
        }
        assert forall|k: int| 0 <= k < qp2.rq.len() implies {
            let wr = (#[trigger] qp2.rq[k]).wr;
            ring_backed(self.recv, qp2.rq_head + k, wr.wr_id, None, wr.sges)
        } by {
            let old_k = if e.queue is Recv { k + nn } else { k };
            assert(qp2.rq[k].wr == qp.rq[old_k].wr);
        }
        assert forall|j: int| 0 <= j < written.len() implies after.send_wr_ok(qp2.attr, #[trigger] written[j]) by {
            assert(before.send_wr_ok(qp.attr, written[j]));
        }
        outcomes::lemma_recv_framed(self.recv, before, after, q);
        send_outcomes::lemma_results_framed(self.send, before, after, q);
        assert forall|j: int| 0 <= j < qp2.sq.len() implies qp2.sq_head + j < qp.sq_head + qp.sq.len()
            && (#[trigger] qp2.sq[j]).wr == qp.sq[qp2.sq_head + j - qp.sq_head].wr by {
            if e.queue is Send {
                assert(qp2.sq[j] == qp.sq[j + nn]);
            }
        }
        assert forall|n: nat| qp.sq_head <= n < qp2.sq_head
            implies #[trigger] send_outcomes::popped_ok(qp, after.retired, q, n) by {
            let k = n - qp.sq_head;
            assert(qp.sq[k].outcome is Some);
        }
        send_outcomes::lemma_bound_kept(self.send, self.send, self.send_tails@, qp, qp2, written, before.retired, after.retired, q);
    }
}

/// What the next CQE read says of its request's ring, in the numbers its report has.
pub(super) proof fn lemma_next_cqe<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, name: nat, t: NicToken, m: Nic::State, pending: Seq<Cqe>, wc: Wc)
    requires
        table_ok(qps, name, t, m, pending),
        pending.len() > 0,
        reports(wc, pending[0]),
    ensures ({
        let e = pending[0];
        let i = cqe_index(e);
        let r = ring_of(qps[i]->0, e.queue);
        &&& wc.wr_id == e.wr_id
        &&& wc.outcome() == e.outcome
        &&& i == wc.wr_id >> INDEX_SHIFT
        &&& 0 <= i < qps.len()
        &&& qps[i] is Some
        &&& (wc.wr_id & RECV_BIT != 0) == (e.queue is Recv)
        &&& wc.wr_id & WQE_MASK == (e.seq as u64) & WQE_MASK
        &&& r.inv()
        &&& r.spec_wqe_retired() <= e.seq < r.spec_wqe_committed()
        &&& e.seq - r.spec_wqe_retired() < WQE_SPAN
        &&& r.spec_routed() <= e.seq
    }),
{
    let e = pending[0];
    let i = cqe_index(e);
    lemma_reports(wc, e);
    assert(cqe_ok(qps, m, e));
    assert(qp_ok(qps[i], i, t, m));
    let y = qps[i]->0;
    let qp = m.qps[e.qp];
    let r = ring_of(y, e.queue);
    r.lemma_wqes();
    assert(r.spec_wqe_committed() == head_of(qp, e.queue) + (if e.queue is Send { qp.sq.len() } else { qp.rq.len() }));
    lemma_wr_id_fields(i as u16, e.queue is Recv, e.seq as u64);
}

/// The next CQE read: a receive names the next WQE its ring is to hear of; on a send queue, every
/// WQE before it the ring has not heard of succeeded without a completion.
proof fn lemma_cqe_lag<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, pending: Seq<Cqe>)
    requires
        pending_ok(qps, m, pending),
        pending.len() > 0,
    ensures ({
        let e = pending[0];
        let r = ring_of(qps[cqe_index(e)]->0, e.queue);
        &&& e.queue is Recv ==> r.spec_routed() == e.seq
        &&& e.queue is Send ==> forall|n: nat| r.spec_routed() <= n < e.seq
            ==> #[trigger] send_outcomes::send_succeeded(m.retired, e.qp, n)
    }),
{
    let e = pending[0];
    let i = cqe_index(e);
    assert(cqe_ok(qps, m, e));
    let y = qps[i]->0;
    let r = ring_of(y, e.queue);
    assert(lag_ok(y, m, Queue::Send, pending) && lag_ok(y, m, Queue::Recv, pending));
    assert(e.queue is Send || e.queue is Recv);
    assert forall|n: nat| r.spec_routed() <= n < e.seq
        implies e.queue is Send && #[trigger] send_outcomes::send_succeeded(m.retired, e.qp, n) by {
        let w = WqeRef { qp: e.qp, queue: e.queue, seq: n };
        assert(lag_at(w, m, pending));
        if exists|j: int| 0 <= j < pending.len() && cqe_wqe(#[trigger] pending[j]) == w {
            let j = choose|j: int| 0 <= j < pending.len() && cqe_wqe(#[trigger] pending[j]) == w;
            if j > 0 {
                assert(pending[0].seq < pending[j].seq);
            }
        }
    }
    if e.queue is Recv && (r.spec_routed() as int) < e.seq as int {
        assert(send_outcomes::send_succeeded(m.retired, e.qp, r.spec_routed()));
    }
}

/// `same_qps` is transitive.
pub(super) proof fn lemma_same_qps_trans<C, N: Device>(a: Seq<Option<HwQp<C, N>>>, b: Seq<Option<HwQp<C, N>>>, c: Seq<Option<HwQp<C, N>>>)
    requires
        same_qps(a, b),
        same_qps(b, c),
    ensures
        same_qps(a, c),
{
    assert forall|i: int| 0 <= i < a.len() implies (#[trigger] a[i] is Some) == (c[i] is Some) by {
        assert(b[i] is Some == c[i] is Some);
    }
    assert forall|i: int| 0 <= i < a.len() && #[trigger] a[i] is Some implies a[i]->0.same_config(c[i]->0) by {
        assert(b[i] is Some);
    }
}

/// Tables that differ in one entry, which keeps its configuration, have the same QPs.
pub(super) proof fn lemma_same_qps_but_one<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, i: int)
    requires
        0 <= i < pre.len(),
        pre[i] is Some,
        post.len() == pre.len(),
        forall|k: int| 0 <= k < pre.len() && k != i ==> #[trigger] post[k] == pre[k],
        post[i] is Some,
        post[i]->0.same_config(pre[i]->0),
    ensures
        same_qps(post, pre),
{
    assert forall|k: int| 0 <= k < post.len() implies (#[trigger] post[k] is Some) == (pre[k] is Some) by {
        if k != i {
            assert(post[k] == pre[k]);
        }
    }
    assert forall|k: int| 0 <= k < post.len() && #[trigger] post[k] is Some implies post[k]->0.same_config(pre[k]->0) by {
        if k != i {
            assert(post[k] == pre[k]);
        }
    }
}

/// A step of the walk of the oldest CQE read, on its ring: the CQEs still to come say the same.
proof fn lemma_pending_walked<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, pending: Seq<Cqe>, spent: bool)
    requires
        pending_ok(pre, m, pending),
        pending.len() > 0,
        forall|i: int, j: int|
            #![trigger pre[i], pre[j]]
            0 <= i < pre.len() && 0 <= j < pre.len() && i != j && pre[i] is Some
                && pre[j] is Some ==> pre[i]->0.id() != pre[j]->0.id(),
        post.len() == pre.len(),
        forall|i: int| 0 <= i < pre.len() && i != cqe_index(pending[0]) ==> #[trigger] post[i] == pre[i],
        ({
            let e = pending[0];
            let i = cqe_index(e);
            let (y, x) = (pre[i]->0, post[i]->0);
            let (r0, r1) = (ring_of(y, e.queue), ring_of(x, e.queue));
            &&& post[i] is Some
            &&& x.id() == y.id()
            &&& ring_of(x, other_queue(e.queue)) == ring_of(y, other_queue(e.queue))
            &&& r1.spec_routed() == if spent { (e.seq + 1) as int } else { r0.spec_routed() as int }
            &&& r1.spec_wqe_retired() <= e.seq + 1
            &&& !spent ==> r1.spec_wqe_retired() <= e.seq
        }),
    ensures
        pending_ok(post, m, if spent { pending.drop_first() } else { pending }),
{
    let e = pending[0];
    let i = cqe_index(e);
    let rest = if spent { pending.drop_first() } else { pending };
    assert(cqe_ok(pre, m, e));
    let (y, x) = (pre[i]->0, post[i]->0);
    assert forall|j: int| 0 <= j < rest.len() implies #[trigger] cqe_ok(post, m, rest[j]) by {
        let k = if spent { j + 1 } else { j };
        assert(rest[j] == pending[k]);
        assert(cqe_ok(pre, m, pending[k]));
        let f = pending[k];
        if cqe_index(f) == i && f.queue == e.queue && k > 0 {
            assert(pending[0].seq < pending[k].seq);
        }
        if cqe_index(f) == i && f.queue != e.queue {
            assert(f.queue == other_queue(e.queue));
        }
    }
    assert forall|a: int, b: int|
        #![trigger rest[a], rest[b]]
        0 <= a < b < rest.len() && rest[a].qp == rest[b].qp && rest[a].queue == rest[b].queue
        implies rest[a].seq < rest[b].seq by {
        if spent {
            assert(rest[a] == pending[a + 1] && rest[b] == pending[b + 1]);
        }
    }
    assert forall|n: int| 0 <= n < post.len() && #[trigger] post[n] is Some implies {
        &&& lag_ok(post[n]->0, m, Queue::Send, rest)
        &&& lag_ok(post[n]->0, m, Queue::Recv, rest)
    } by {
        let z = pre[n]->0;
        assert(pre[n] is Some);
        assert(lag_ok(z, m, Queue::Send, pending) && lag_ok(z, m, Queue::Recv, pending));
        if n != i {
            assert(post[n] == pre[n]);
            assert(z.id() != y.id());
        }
        assert forall|b: int| 0 < b < pending.len() && pending[b].qp == pending[0].qp && pending[b].queue == pending[0].queue
            implies pending[0].seq < #[trigger] pending[b].seq by {
        }
        assert(e.queue == Queue::Send || e.queue == Queue::Recv);
        lemma_lag_walked(z, post[n]->0, m, Queue::Send, pending, rest, spent, n == i);
        lemma_lag_walked(z, post[n]->0, m, Queue::Recv, pending, rest, spent, n == i);
    }
}

/// [`lemma_pending_walked`] for one queue of one QP.
proof fn lemma_lag_walked<C, N: Device>(z: HwQp<C, N>, w: HwQp<C, N>, m: Nic::State, queue: Queue, pending: Seq<Cqe>, rest: Seq<Cqe>, spent: bool, walked_qp: bool)
    requires
        pending.len() > 0,
        lag_ok(z, m, queue, pending),
        z.id() == w.id(),
        rest == if spent { pending.drop_first() } else { pending },
        // The ring heard of the CQE, if it is the one it names, and of nothing else.
        ring_of(w, queue).spec_routed() == if walked_qp && queue == pending[0].queue && spent {
            (pending[0].seq + 1) as int
        } else {
            ring_of(z, queue).spec_routed() as int
        },
        walked_qp <==> z.id() == pending[0].qp,
        walked_qp && queue == pending[0].queue ==> ring_of(z, queue).spec_routed() <= pending[0].seq,
        forall|n: int| 0 < n < pending.len() && pending[n].qp == pending[0].qp && pending[n].queue == pending[0].queue
            ==> pending[0].seq < #[trigger] pending[n].seq,
    ensures
        lag_ok(w, m, queue, rest),
{
    assert forall|s: nat| ring_of(w, queue).spec_routed() <= s < head_of(m.qps[w.id()], queue)
        implies #[trigger] lag_at(WqeRef { qp: w.id(), queue, seq: s }, m, rest) by {
        let v = WqeRef { qp: w.id(), queue, seq: s };
        assert(lag_at(WqeRef { qp: z.id(), queue, seq: s }, m, pending));
        if !(queue is Send && m.retired[v].status is Success) {
            let j = choose|j: int| 0 <= j < pending.len() && cqe_wqe(#[trigger] pending[j]) == v;
            if spent {
                assert(j != 0);
                assert(rest[j - 1] == pending[j]);
                assert(cqe_wqe(rest[j - 1]) == v);
            } else {
                assert(cqe_wqe(rest[j]) == v);
            }
        }
    }
}

/// What the table needs to stay coupled through a change of entry `i` alone that keeps its QP in M.
proof fn lemma_table_entry<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, name: nat, t: NicToken, m: Nic::State, pending: Seq<Cqe>, rest: Seq<Cqe>, i: int)
    requires
        table_ok(pre, name, t, m, pending),
        0 <= i < pre.len(),
        pre[i] is Some,
        post.len() == pre.len(),
        forall|k: int| 0 <= k < pre.len() && k != i ==> #[trigger] post[k] == pre[k],
        post[i] is Some,
        post[i]->0.same_config(pre[i]->0),
        qp_ok(post[i], i, t, m),
        pending_ok(post, m, rest),
    ensures
        table_ok(post, name, t, m, rest),
        same_qps(post, pre),
{
    assert forall|k: int| 0 <= k < post.len() implies #[trigger] qp_ok(post[k], k, t, m) by {
        if k != i {
            assert(qp_ok(pre[k], k, t, m));
        }
    }
    lemma_same_qps_but_one(pre, post, i);
    lemma_names_kept(pre, post, name, t, t, m, m);
}

/// A step of a walk of the oldest CQE read: the table stays coupled, with M as it is, and the CQE
/// is gone if it is spent.
pub(super) proof fn lemma_table_walked<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, name: nat, t: NicToken, m: Nic::State, pending: Seq<Cqe>, wc: Wc, x: HwQp<C, N>, spent: bool)
    requires
        table_ok(pre, name, t, m, pending),
        pending.len() > 0,
        reports(wc, pending[0]),
        post == pre.update(cqe_index(pending[0]), Some(x)),
        x.same_qp(pre[cqe_index(pending[0])]->0),
        x.started == pre[cqe_index(pending[0])]->0.started,
        x.send_tails == pre[cqe_index(pending[0])]->0.send_tails,
        ring_of(x, pending[0].queue).walked(&ring_of(pre[cqe_index(pending[0])]->0, pending[0].queue), pending[0].queue is Recv, pending[0].seq as u64, wc, spent),
        ring_of(x, other_queue(pending[0].queue)) == ring_of(pre[cqe_index(pending[0])]->0, other_queue(pending[0].queue)),
    ensures
        table_ok(post, name, t, m, if spent { pending.drop_first() } else { pending }),
        same_qps(post, pre),
{
    let e = pending[0];
    let i = cqe_index(e);
    let y = pre[i]->0;
    lemma_reports(wc, e);
    assert(cqe_ok(pre, m, e));
    assert(qp_ok(pre[i], i, t, m));
    lemma_cqe_lag(pre, m, pending);
    let q = y.id();
    let qp = m.qps[q];
    let r = ring_of(y, e.queue);
    r.lemma_wqes();
    assert(r.spec_wqe_committed() == head_of(qp, e.queue) + (if e.queue is Send { qp.sq.len() } else { qp.rq.len() }));
    x.lemma_walked(&y, i as u16, m, t.share(), t.written(q), wc, e, spent);
    assert(post[i] == Some(x));
    assert(qp_ok(post[i], i, t, m));
    assert forall|k: int| 0 <= k < pre.len() && k != i implies #[trigger] post[k] == pre[k] by {}
    lemma_pending_walked(pre, post, m, pending, spent);
    lemma_table_entry(pre, post, name, t, m, pending, if spent { pending.drop_first() } else { pending }, i);
}

/// A change of entry `i` whose rings keep their requests leaves the table coupled.
pub(super) proof fn lemma_table_kept<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, name: nat, t: NicToken, m: Nic::State, pending: Seq<Cqe>, i: int)
    requires
        table_ok(pre, name, t, m, pending),
        0 <= i < pre.len(),
        pre[i] is Some,
        post.len() == pre.len(),
        forall|k: int| 0 <= k < pre.len() && k != i ==> #[trigger] post[k] == pre[k],
        post[i] is Some,
        post[i]->0.same_qp(pre[i]->0),
        post[i]->0.started == pre[i]->0.started,
        post[i]->0.send_tails == pre[i]->0.send_tails,
        ring_kept(post[i]->0.send, pre[i]->0.send),
        ring_kept(post[i]->0.recv, pre[i]->0.recv),
    ensures
        table_ok(post, name, t, m, pending),
        same_qps(post, pre),
{
    let (y, x) = (pre[i]->0, post[i]->0);
    assert(qp_ok(pre[i], i, t, m));
    x.lemma_kept(&y, i as u16, m, t.share(), t.written(y.id()));
    assert forall|k: int| 0 <= k < post.len() implies #[trigger] same_for_cqes(pre[k], post[k], m, m) by {
        if k != i {
            assert(post[k] == pre[k]);
        }
    }
    lemma_pending_same(pre, post, m, m, pending);
    lemma_table_entry(pre, post, name, t, m, pending, pending, i);
}

/// M pops a CQE: the table stays coupled, the rings as they are, with one more CQE read.
proof fn lemma_table_popped<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, name: nat, t: NicToken, before: Nic::State, after: Nic::State, pending: Seq<Cqe>)
    requires
        table_ok(qps, name, t, before, pending),
        nic_step(before, after, Nic::Step::poll_cq(t.share().cq)),
    ensures
        table_ok(qps, name, t, after, pending.push(before.cqs[t.share().cq].entries[0])),
        after.caps == before.caps,
        after.cqs[t.share().cq].cap == before.cqs[t.share().cq].cap,
        after.cqs[t.share().cq].reserved == before.cqs[t.share().cq].reserved,
{
    let sh = t.share();
    let c = sh.cq;
    let e = before.cqs[c].entries[0];
    let q = e.qp;
    let qp = before.qps[q];
    let qp2 = after.qps[q];
    safeverbs_spec::nic_proofs::lemma_head_is_first(before, c);
    lemma_poll(before, after, c);
    lemma_nic_step_is_next(before, after, Nic::Step::poll_cq(c));
    lemma_step_keeps_invariant(before, after);
    lemma_mine_keeps_share_wf(sh, ISet::empty(), before, after, Nic::Step::poll_cq(c));
    assert(before.qp_wf(qp));
    assert(sh.owns.contains(q));
    let i = choose|i: int| 0 <= i < qps.len() && #[trigger] qps[i] is Some && qps[i]->0.id() == q;
    let y = qps[i]->0;
    assert(qp_ok(qps[i], i, t, before));
    reveal(retires);
    assert forall|k: int| 0 <= k < qps.len() implies #[trigger] qp_ok(qps[k], k, t, after) by {
        assert(qp_ok(qps[k], k, t, before));
        if let Some(z) = qps[k] {
            if z.id() == q {
                z.lemma_popped(k as u16, t, before, after);
            } else {
                lemma_ok_same(z, k as u16, before, after, sh, t.started(z.id()), t.written(z.id()));
            }
        }
    }
    lemma_names_kept(qps, qps, name, t, t, before, after);
    // The new CQE names what M just popped.
    lemma_wr_id_fields(i as u16, e.queue is Recv, e.seq as u64);
    let more = pending.push(e);
    let r = ring_of(y, e.queue);
    assert(r.spec_routed() <= head_of(qp, e.queue));
    assert(r.spec_wqe_retired() <= head_of(qp, e.queue));
    r.lemma_wqes();
    if e.queue is Send {
        assert(qp.sq[e.seq - qp.sq_head].wr.wr_id == spec_wr_id(i as u16, false, e.seq as u64));
    } else {
        assert(qp.rq[e.seq - qp.rq_head].wr.wr_id == spec_wr_id(i as u16, true, e.seq as u64));
        assert(qp.rq[e.seq - qp.rq_head].outcome == Some(e.outcome));
        assert(qp.rq[e.seq - qp.rq_head].wr.allows_outcome(e.outcome));
    }
    // Those read before name WQEs M popped before, which it keeps.
    assert forall|j: int| 0 <= j < more.len() implies #[trigger] cqe_ok(qps, after, more[j]) by {
        if j < pending.len() {
            let f = pending[j];
            assert(more[j] == f);
            assert(cqe_ok(qps, before, f));
            assert(after.retired.contains_key(cqe_wqe(f)));
            assert(after.retired[cqe_wqe(f)] == before.retired[cqe_wqe(f)]);
        } else {
            assert(more[j] == e);
        }
    }
    assert forall|a: int, b: int|
        #![trigger more[a], more[b]]
        0 <= a < b < more.len() && more[a].qp == more[b].qp && more[a].queue == more[b].queue
        implies more[a].seq < more[b].seq by {
        assert(more[a] == pending[a]);
        if b < pending.len() {
            assert(more[b] == pending[b]);
        } else {
            assert(cqe_ok(qps, before, pending[a]));
            assert(more[b] == e);
        }
    }
    assert forall|n: int| 0 <= n < qps.len() && #[trigger] qps[n] is Some implies {
        &&& lag_ok(qps[n]->0, after, Queue::Send, more)
        &&& lag_ok(qps[n]->0, after, Queue::Recv, more)
    } by {
        let z = qps[n]->0;
        assert(qp_ok(qps[n], n, t, before));
        assert(lag_ok(z, before, Queue::Send, pending) && lag_ok(z, before, Queue::Recv, pending));
        lemma_lag_popped(z, before, after, c, Queue::Send, pending);
        lemma_lag_popped(z, before, after, c, Queue::Recv, pending);
    }
}

/// [`lemma_table_popped`] for one queue of one QP.
proof fn lemma_lag_popped<C, N: Device>(z: HwQp<C, N>, before: Nic::State, after: Nic::State, c: CqId, queue: Queue, pending: Seq<Cqe>)
    requires
        nic_inv(before),
        before.qps.contains_key(z.id()),
        ring_of(z, queue).spec_routed() <= head_of(before.qps[z.id()], queue),
        nic_step(before, after, Nic::Step::poll_cq(c)),
        lag_ok(z, before, queue, pending),
    ensures
        lag_ok(z, after, queue, pending.push(before.cqs[c].entries[0])),
{
    let e = before.cqs[c].entries[0];
    let more = pending.push(e);
    let q = z.id();
    let qp = before.qps[q];
    safeverbs_spec::nic_proofs::lemma_head_is_first(before, c);
    lemma_poll(before, after, c);
    assert(before.qp_wf(qp));
    reveal(retires);
    assert forall|s: nat| ring_of(z, queue).spec_routed() <= s < head_of(after.qps[q], queue)
        implies #[trigger] lag_at(WqeRef { qp: q, queue, seq: s }, after, more) by {
        let v = WqeRef { qp: q, queue, seq: s };
        if s < head_of(qp, queue) {
            assert(lag_at(v, before, pending));
            assert(before.retired.contains_key(v));
            assert(after.retired.contains_key(v) && after.retired[v] == before.retired[v]);
            if !(queue is Send && before.retired[v].status is Success) {
                let j = choose|j: int| 0 <= j < pending.len() && cqe_wqe(#[trigger] pending[j]) == v;
                assert(more[j] == pending[j]);
                assert(cqe_wqe(more[j]) == v);
            }
        } else {
            // A WQE this very poll popped: of the CQE's queue.
            assert(q == e.qp && queue == e.queue);
            if s == e.seq {
                assert(more[pending.len() as int] == e);
                assert(cqe_wqe(more[pending.len() as int]) == v);
            } else {
                let k = s - qp.sq_head;
                assert(queue is Send);
                assert(v == (WqeRef { qp: e.qp, queue: Queue::Send, seq: (qp.sq_head + k) as nat }));
                assert(!qp.sq[k].wr.signaled && after.retired.contains_key(v) && after.retired[v].status is Success);
            }
        }
    }
}

/// A poll that gave `wcs`: the table stays coupled, the rings as they are, with what it read after
/// the CQEs read before.
pub(super) proof fn lemma_table_polled<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, name: nat, t: NicToken, m: Nic::State, n: Nic::State, wcs: Seq<Wc>, pending: Seq<Cqe>) -> (cqes: Seq<Cqe>)
    requires
        table_ok(qps, name, t, m, pending),
        polled(t.share(), pinned(t), m, n, wcs),
    ensures
        cqes.len() == wcs.len(),
        forall|j: int| 0 <= j < cqes.len() ==> reports(wcs[j], #[trigger] cqes[j]),
        table_ok(qps, name, t, n, pending + cqes),
        n.caps == m.caps,
        n.cqs[t.share().cq].cap == m.cqs[t.share().cq].cap,
        n.cqs[t.share().cq].reserved == m.cqs[t.share().cq].reserved,
    decreases wcs.len(),
{
    let sh = t.share();
    if wcs.len() == 0 {
        lemma_table_rely(qps, name, t, m, n, pending);
        lemma_rely_frame(sh, pinned(t), m, n);
        assert(pending + Seq::<Cqe>::empty() =~= pending);
        Seq::empty()
    } else {
        let (mid, before) = choose|mid: Nic::State, before: Nic::State|
            {
                &&& polled(sh, pinned(t), m, mid, wcs.drop_last())
                &&& rely_reaches(sh, pinned(t), mid, before)
                &&& nic_step(before, n, Nic::Step::poll_cq(sh.cq))
                &&& reports(wcs.last(), before.cqs[sh.cq].entries[0])
            };
        let earlier = lemma_table_polled(qps, name, t, m, mid, wcs.drop_last(), pending);
        lemma_table_rely(qps, name, t, mid, before, pending + earlier);
        lemma_rely_frame(sh, pinned(t), mid, before);
        lemma_table_popped(qps, name, t, before, n, pending + earlier);
        let e = before.cqs[sh.cq].entries[0];
        let cqes = earlier.push(e);
        assert((pending + earlier).push(e) =~= pending + cqes);
        assert forall|j: int| 0 <= j < cqes.len() implies reports(wcs[j], #[trigger] cqes[j]) by {
            if j < earlier.len() {
                assert(cqes[j] == earlier[j]);
                assert(wcs.drop_last()[j] == wcs[j]);
                assert(reports(wcs.drop_last()[j], earlier[j]));
            } else {
                assert(wcs[j] == wcs.last());
            }
        }
        cqes
    }
}

impl<C, N: Device> Hw<C, N> {
    /// `self` is `older` with M and the QPs as they were, and every request it has it had, with
    /// what it holds and its WQEs.
    pub open(super) spec fn keeps_requests(&self, older: &Self) -> bool {
        &&& self.same_table(older)
        &&& self.token == older.token
        &&& forall|i: int| 0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some ==> {
            let (a, b) = (self.qps@[i]->0, older.qps@[i]->0);
            &&& a.send_tails == b.send_tails
            &&& same_requests(a.send, b.send)
            &&& same_requests(a.recv, b.recv)
        }
    }

    /// Any state keeps its own requests.
    pub(super) proof fn lemma_keeps_requests_refl(&self)
        ensures
            self.keeps_requests(self),
    {
    }

    /// `keeps_requests` is transitive.
    pub(super) proof fn lemma_keeps_requests_trans(&self, mid: &Self, older: &Self)
        requires
            self.keeps_requests(mid),
            mid.keeps_requests(older),
        ensures
            self.keeps_requests(older),
    {
        lemma_same_qps_trans(self.qps@, mid.qps@, older.qps@);
        assert forall|i: int| 0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some implies {
            let (a, b) = (self.qps@[i]->0, older.qps@[i]->0);
            &&& a.send_tails == b.send_tails
            &&& same_requests(a.send, b.send)
            &&& same_requests(a.recv, b.recv)
        } by {
            assert(mid.qps@[i] is Some);
            lemma_same_requests_trans(self.qps@[i]->0.send, mid.qps@[i]->0.send, older.qps@[i]->0.send);
            lemma_same_requests_trans(self.qps@[i]->0.recv, mid.qps@[i]->0.recv, older.qps@[i]->0.recv);
        }
    }

    /// The event loop hands out `c`: request `c.seq` of `self`'s QP `c.qp`, with its own context and
    /// its buffers, those that may leave it at least. A receive reports the outcome M retired for
    /// its one WQE; a send request M's aggregate of its WQEs, or that its doorbell was refused,
    /// which M never saw.
    pub open(super) spec fn hands_out(&self, c: Completion<C, N>) -> bool {
        &&& c.leases.inv()
        &&& c.leases.fully_recorded()
        &&& self.has_qp(c.qp as int)
        &&& {
            let e = self.qps@[c.qp as int]->0;
            let s = c.seq as int;
            let retired = self.nic().retired;
            match c.queue {
                Side::Recv => {
                    let w = WqeRef { qp: e.id(), queue: Queue::Recv, seq: e.recv.meta(s)->0.first as nat };
                    &&& e.recv.req(s) is Some
                    &&& e.recv.req(s)->0.leases.ctx == Some(c.wr_id)
                    &&& e.recv.req(s)->0.leases.leases.uniques() == c.leases.uniques()
                    &&& retired.contains_key(w)
                    &&& recv_reports_outcome(c.result, retired[w])
                },
                Side::Send => if c.result matches Err(CompletionError::Refused) {
                    &&& e.send.refused(s) is Some
                    &&& e.send.refused(s)->0.ctx == Some(c.wr_id)
                    &&& e.send.refused(s)->0.leases.uniques() == c.leases.uniques()
                } else {
                    &&& e.send.req(s) is Some
                    &&& e.send.req(s)->0.leases.ctx == Some(c.wr_id)
                    &&& e.send.req(s)->0.leases.leases.uniques() == c.leases.uniques()
                    &&& send_reports_outcome(send_range(e.id(), e.send.meta(s)->0), retired, e.send_tails@[s], c.result)
                },
            }
        }
    }

    /// What is handed out of a state keeping an older one's requests is handed out of the older one.
    pub(super) proof fn lemma_hands_out_earlier(&self, older: &Self, c: Completion<C, N>)
        requires
            self.hands_out(c),
            self.keeps_requests(older),
        ensures
            older.hands_out(c),
    {
        let i = c.qp as int;
        assert(self.qps@[i] is Some);
        let (a, b) = (self.qps@[i]->0, older.qps@[i]->0);
        assert(same_requests(a.send, b.send) && same_requests(a.recv, b.recv));
        let s = c.seq as int;
        if !(c.result matches Err(CompletionError::Refused)) {
            match c.queue {
                Side::Recv => assert(a.recv.req(s) is Some),
                Side::Send => assert(a.send.req(s) is Some),
            }
        }
    }

    /// The moves of the event loop's position along the CQEs read leave what it says of them.
    pub(super) proof fn lemma_polled_moved(&self, pre: &Self)
        requires
            pre.polled_ok(),
            pre.pending@.len() > 0,
            self.polled == pre.polled,
            self.polled_at == pre.polled_at + 1,
            self.polled_len == pre.polled_len,
            self.pending@ == pre.pending@.drop_first(),
        ensures
            self.polled_ok(),
    {
        assert forall|j: int| 0 <= j < self.pending@.len()
            implies reports(self.polled@[self.polled_at + j], #[trigger] self.pending@[j]) by {
            assert(self.pending@[j] == pre.pending@[j + 1]);
            assert(reports(pre.polled@[pre.polled_at + (j + 1)], pre.pending@[j + 1]));
            assert(self.polled_at + j == pre.polled_at + (j + 1));
        }
    }

    /// What request `s` of the next CQE's ring reports, if the CQE ends it: M's outcomes for its
    /// WQEs.
    pub(super) proof fn lemma_retired_reports(&self, s: int, wc: Wc)
        requires
            self.inv(),
            self.pending@.len() > 0,
            reports(wc, self.pending@[0]),
            ({
                let e = self.pending@[0];
                let r = ring_of(self.qps@[cqe_index(e)]->0, e.queue);
                &&& r.req(s) is Some
                &&& r.req(s)->0.phase is InFlight
                &&& r.meta(s)->0.end as int <= e.seq as int + 1
            }),
        ensures ({
            let e = self.pending@[0];
            let y = self.qps@[cqe_index(e)]->0;
            let r = ring_of(y, e.queue);
            let out = r.retire_outcome(s, e.queue is Recv, e.seq as u64, wc);
            let retired = self.nic().retired;
            let w = WqeRef { qp: e.qp, queue: Queue::Recv, seq: r.meta(s)->0.first as nat };
            &&& payload_ok(r.req(s)->0.leases, r.req(s)->0.detached)
            &&& e.queue is Recv ==> retired.contains_key(w) && recv_reports_outcome(out, retired[w])
            &&& e.queue is Send ==> send_reports_outcome(send_range(e.qp, r.meta(s)->0), retired, y.send_tails@[s], out)
        }),
    {
        let e = self.pending@[0];
        let i = cqe_index(e);
        let y = self.qps@[i]->0;
        let m = self.nic();
        let t = self.token@;
        let r = ring_of(y, e.queue);
        let wqe = e.seq as u64;
        lemma_reports(wc, e);
        assert(cqe_ok(self.qps@, m, e));
        assert(qp_ok(self.qps@[i], i, t, m));
        lemma_cqe_lag(self.qps@, m, self.pending@);
        r.lemma_meta(s);
        r.lemma_view();
        r.lemma_wqes();
        r.lemma_completion_observation(s, e.queue is Recv, wqe, wc, r.spec_retired() as int, true);
        let qp = m.qps[e.qp];
        if e.queue is Recv {
            reveal(outcomes::recv_outcomes);
            assert(outcomes::recv_result(y.recv, m, e.qp, s));
            assert(r.meta(s)->0.first == wqe);
            let w = WqeRef { qp: e.qp, queue: Queue::Recv, seq: wqe as nat };
            assert(w == cqe_wqe(e));
            let out = r.retire_outcome(s, true, wqe, wc);
            wc.lemma_outcome_ok();
            if wc.spec_ok() {
                wc.lemma_received();
            } else {
                wc.lemma_failure(0);
            }
            assert(recv_reports_outcome(out, m.retired[w]));
        } else {
            reveal(send_outcomes::send_results);
            assert(send_outcomes::send_result(y.send, m, e.qp, s));
            send_outcomes::lemma_request_retired(y.send, y.send_tails@, t.written(e.qp), m, e.qp, wqe, wc, s);
            let range = send_range(e.qp, r.meta(s)->0);
            let out = r.retire_outcome(s, false, wqe, wc);
            send_outcomes::lemma_reports(range, m.retired, y.send_tails@[s], out);
        }
    }

    /// What a step of the walk of the next CQE read leaves, its ring `walked`, the result it retired
    /// taken from there if `took`: the invariant, every request where it was.
    pub(super) proof fn lemma_step_left(&self, pre: &Self, wc: Wc, spent: bool, walked: Ring<Payload<C, N>>, seq: int, taken: bool, took: bool)
        requires
            pre.inv(),
            pre.pending@.len() > 0,
            reports(wc, pre.pending@[0]),
            ({
                let e = pre.pending@[0];
                let i = cqe_index(e);
                let y = pre.qps@[i]->0;
                let x = self.qps@[i]->0;
                let r = ring_of(y, e.queue);
                &&& walked.walked(&r, e.queue is Recv, e.seq as u64, wc, spent)
                &&& self.qps@.len() == pre.qps@.len()
                &&& forall|k: int| 0 <= k < pre.qps@.len() && k != i ==> #[trigger] self.qps@[k] == pre.qps@[k]
                &&& self.qps@[i] is Some
                &&& x.same_qp(y)
                &&& x.started == y.started
                &&& x.send_tails == y.send_tails
                &&& ring_of(x, other_queue(e.queue)) == ring_of(y, other_queue(e.queue))
                &&& took ==> ring_of(x, e.queue).inv() && ring_of(x, e.queue).left_by_take(&walked, seq, taken)
                &&& !took ==> ring_of(x, e.queue) == walked
            }),
            self.same_engine_fields(pre),
            self.polled == pre.polled,
            self.polled_len == pre.polled_len,
            self.polled_at == if spent { pre.polled_at + 1 } else { pre.polled_at as int },
            self.pending@ == if spent { pre.pending@.drop_first() } else { pre.pending@ },
        ensures
            self.inv_m(),
            self.keeps_requests(pre),
    {
        let e = pre.pending@[0];
        let i = cqe_index(e);
        let y = pre.qps@[i]->0;
        let x = self.qps@[i]->0;
        let r = ring_of(y, e.queue);
        let t = pre.token@;
        let m = pre.nic();
        let mid = if e.queue is Recv { HwQp { recv: walked, ..y } } else { HwQp { send: walked, ..y } };
        let mids = pre.qps@.update(i, Some(mid));
        assert(cqe_ok(pre.qps@, m, e));
        assert(qp_ok(pre.qps@[i], i, t, m));
        assert(ring_of(mid, e.queue) == walked);
        assert(ring_of(mid, other_queue(e.queue)) == ring_of(y, other_queue(e.queue)));
        lemma_table_walked(pre.qps@, mids, pre.next_name@, t, m, pre.pending@, wc, mid, spent);
        lemma_walked_requests(walked, r, e.queue is Recv, e.seq as u64, wc, spent);
        assert(qp_ok(mids[i], i, t, m));
        assert(ring_resources_ok(mid.send) && ring_resources_ok(mid.recv));
        assert(ring_of(mid, e.queue) == walked);
        if took {
            lemma_take_kept(ring_of(x, e.queue), walked, seq, taken);
        } else {
            lemma_same_kept(walked, walked);
        }
        lemma_same_kept(ring_of(y, other_queue(e.queue)), ring_of(y, other_queue(e.queue)));
        assert(x.send == if e.queue is Recv { mid.send } else { ring_of(x, e.queue) });
        assert(x.recv == if e.queue is Recv { ring_of(x, e.queue) } else { mid.recv });
        assert forall|k: int| 0 <= k < mids.len() && k != i implies #[trigger] self.qps@[k] == mids[k] by {}
        lemma_table_kept(mids, self.qps@, pre.next_name@, t, m, if spent { pre.pending@.drop_first() } else { pre.pending@ }, i);
        lemma_same_qps_trans(self.qps@, mids, pre.qps@);
        if spent {
            self.lemma_polled_moved(pre);
        }
        lemma_same_requests_trans(ring_of(x, e.queue), walked, r);
        assert forall|k: int| 0 <= k < self.qps@.len() && #[trigger] self.qps@[k] is Some implies {
            let (a, b) = (self.qps@[k]->0, pre.qps@[k]->0);
            &&& a.send_tails == b.send_tails
            &&& same_requests(a.send, b.send)
            &&& same_requests(a.recv, b.recv)
        } by {
            if k != i {
                assert(self.qps@[k] == pre.qps@[k]);
            } else if e.queue is Recv {
                assert(x.send == y.send && x.recv == ring_of(x, e.queue) && y.recv == r);
            } else {
                assert(x.recv == y.recv && x.send == ring_of(x, e.queue) && y.send == r);
            }
        }
    }

    /// The fast path of the event loop's walk took the oldest request of the next CQE's ring, its
    /// slot empty now: the invariant holds, every request is where it was, and the completion made
    /// of it is what it reports.
    pub(super) proof fn lemma_oldest_done(&self, pre: &Self, start: &Self, wc: Wc, spent: bool, c: Completion<C, N>)
        requires
            pre.inv(),
            pre.pending@.len() > 0,
            pre.keeps_requests(start),
            reports(wc, pre.pending@[0]),
            ({
                let e = pre.pending@[0];
                let i = cqe_index(e);
                let y = pre.qps@[i]->0;
                let x = self.qps@[i]->0;
                let r = ring_of(y, e.queue);
                let s = r.spec_retired() as int;
                let recv = e.queue is Recv;
                &&& ring_of(x, e.queue).retired_and_taken(&r)
                &&& ring_of(x, e.queue).spec_routed() == if spent { (e.seq + 1) as int } else { r.spec_routed() as int }
                &&& wc.spec_ok()
                &&& r.req(s) is Some
                &&& r.req(s)->0.phase is InFlight
                &&& !r.req(s)->0.detached
                &&& r.meta(s)->0.end as int <= e.seq as int + 1
                &&& spent == (r.meta(s)->0.end as int == e.seq as int + 1)
                &&& c.result == r.retire_outcome(s, recv, e.seq as u64, wc)
                &&& r.req(s)->0.leases == (Payload { ctx: Some(c.wr_id), leases: c.leases, ..r.req(s)->0.leases })
                &&& c.qp == i
                &&& c.seq == s
                &&& queue_of(c.queue) == e.queue
                &&& self.qps@.len() == pre.qps@.len()
                &&& forall|k: int| 0 <= k < pre.qps@.len() && k != i ==> #[trigger] self.qps@[k] == pre.qps@[k]
                &&& self.qps@[i] is Some
                &&& x.same_qp(y)
                &&& x.started == y.started
                &&& x.send_tails == y.send_tails
                &&& ring_of(x, other_queue(e.queue)) == ring_of(y, other_queue(e.queue))
            }),
            self.same_engine_fields(pre),
            self.polled == pre.polled,
            self.polled_len == pre.polled_len,
            self.polled_at == if spent { pre.polled_at + 1 } else { pre.polled_at as int },
            self.pending@ == if spent { pre.pending@.drop_first() } else { pre.pending@ },
        ensures
            self.inv_m(),
            self.keeps_requests(start),
            start.hands_out(c),
    {
        let e = pre.pending@[0];
        let i = cqe_index(e);
        let y = pre.qps@[i]->0;
        let x = self.qps@[i]->0;
        let r = ring_of(y, e.queue);
        let s = r.spec_retired() as int;
        let t = pre.token@;
        let m = pre.nic();
        lemma_reports(wc, e);
        assert(cqe_ok(pre.qps@, m, e));
        assert(qp_ok(pre.qps@[i], i, t, m));
        let walked = ring_of(x, e.queue);
        walked.lemma_oldest_walked(&r, e.queue is Recv, e.seq as u64, wc, spent);
        self.lemma_step_left(pre, wc, spent, walked, 0, false, false);
        self.lemma_keeps_requests_trans(pre, start);
        pre.lemma_retired_reports(s, wc);
        pre.lemma_hands_out_earlier(start, c);
    }

    /// What the ring hands back of the request a step of the walk of the next CQE retired or
    /// passed holds its checked buffers.
    pub(super) proof fn lemma_walked_taken(&self, wc: Wc, spent: bool, walked: Ring<Payload<C, N>>, seq: int, t: Taken<Payload<C, N>>)
        requires
            self.inv(),
            self.pending@.len() > 0,
            ({
                let e = self.pending@[0];
                let r = ring_of(self.qps@[cqe_index(e)]->0, e.queue);
                &&& walked.walked(&r, e.queue is Recv, e.seq as u64, wc, spent)
                &&& walked.taken_from(seq, t)
                &&& r.spec_retired() <= seq < walked.spec_retired()
            }),
        ensures
            t.leases.leases.inv(),
            t.leases.leases.fully_recorded(),
    {
        let e = self.pending@[0];
        let i = cqe_index(e);
        let y = self.qps@[i]->0;
        let r = ring_of(y, e.queue);
        assert(cqe_ok(self.qps@, self.nic(), e));
        assert(qp_ok(self.qps@[i], i, self.token@, self.nic()));
        assert(ring_resources_ok(r));
        if !(t.outcome matches Err(CompletionError::Refused)) {
            r.lemma_retired_view(seq, r.retire_outcome(seq, e.queue is Recv, e.seq as u64, wc));
            assert(r.req(seq) is Some);
        }
    }

    /// The completion made of what the ring hands back of that request is what it reports.
    pub(super) proof fn lemma_walked_hands_out(&self, wc: Wc, spent: bool, walked: Ring<Payload<C, N>>, seq: int, t: Taken<Payload<C, N>>, c: Completion<C, N>)
        requires
            self.inv(),
            self.pending@.len() > 0,
            reports(wc, self.pending@[0]),
            ({
                let e = self.pending@[0];
                let r = ring_of(self.qps@[cqe_index(e)]->0, e.queue);
                &&& walked.walked(&r, e.queue is Recv, e.seq as u64, wc, spent)
                &&& walked.taken_from(seq, t)
                &&& r.spec_retired() <= seq < walked.spec_retired()
                &&& c.qp == cqe_index(e)
                &&& c.seq == seq
                &&& queue_of(c.queue) == e.queue
            }),
            t.leases.ctx == Some(c.wr_id),
            t.leases.leases.uniques() == c.leases.uniques(),
            c.leases.inv(),
            c.leases.fully_recorded(),
            c.result == t.outcome,
        ensures
            self.hands_out(c),
    {
        let e = self.pending@[0];
        let i = cqe_index(e);
        let y = self.qps@[i]->0;
        let m = self.nic();
        let r = ring_of(y, e.queue);
        let recv = e.queue is Recv;
        let wqe = e.seq as u64;
        assert(cqe_ok(self.qps@, m, e));
        assert(qp_ok(self.qps@[i], i, self.token@, m));
        if t.outcome matches Err(CompletionError::Refused) {
            if recv {
                reveal(outcomes::recv_outcomes);
                assert(y.recv.refused(seq) is None);
            }
        } else {
            r.lemma_retired_view(seq, r.retire_outcome(seq, recv, wqe, wc));
            r.lemma_view();
            r.lemma_meta(seq);
            r.lemma_completion_observation(seq, recv, wqe, wc, walked.spec_retired() as int, spent);
            assert(r.req(seq) is Some);
            walked.lemma_meta(seq);
            assert(walked.meta(seq) == r.completion_meta(seq, recv, wqe, wc, walked.spec_retired() as int, spent));
            self.lemma_retired_reports(seq, wc);
        }
    }

    /// A table left as it was leaves the invariant.
    pub(super) proof fn lemma_untouched(&self, pre: &Self)
        requires
            pre.inv(),
            self.qps@ == pre.qps@,
            self.same_engine_fields(pre),
            self.polled == pre.polled,
            self.polled_at == pre.polled_at,
            self.polled_len == pre.polled_len,
            self.pending == pre.pending,
            self.refined == pre.refined,
        ensures
            self.inv(),
            self.keeps_requests(pre),
            self.same_table(pre),
    {
    }

    /// A change of entry `index` in what the coupling does not read leaves the invariant.
    pub(super) proof fn lemma_flags_only(&self, pre: &Self, index: int)
        requires
            pre.inv(),
            pre.has_qp(index),
            self.qps@.len() == pre.qps@.len(),
            forall|k: int| 0 <= k < pre.qps@.len() && k != index ==> #[trigger] self.qps@[k] == pre.qps@[k],
            self.qps@[index] is Some,
            self.qps@[index]->0.same_qp(pre.qps@[index]->0),
            self.qps@[index]->0.started == pre.qps@[index]->0.started,
            self.qps@[index]->0.send_tails == pre.qps@[index]->0.send_tails,
            self.qps@[index]->0.send == pre.qps@[index]->0.send,
            self.qps@[index]->0.recv == pre.qps@[index]->0.recv,
            self.same_engine_fields(pre),
            self.polled == pre.polled,
            self.polled_at == pre.polled_at,
            self.polled_len == pre.polled_len,
            self.pending == pre.pending,
            self.refined == pre.refined,
        ensures
            self.inv(),
            self.keeps_requests(pre),
            self.same_table(pre),
    {
        let t = pre.token@;
        let m = pre.nic();
        let y = pre.qps@[index]->0;
        refines::lemma_linked_same_rings(pre.qps@, self.qps@, m, pre.refined@);
        assert(qp_ok(pre.qps@[index], index, t, m));
        lemma_same_kept(y.send, y.send);
        lemma_same_kept(y.recv, y.recv);
        lemma_table_kept(pre.qps@, self.qps@, pre.next_name@, t, m, pre.pending@, index);
        assert forall|k: int| 0 <= k < self.qps@.len() && #[trigger] self.qps@[k] is Some implies {
            let (a, b) = (self.qps@[k]->0, pre.qps@[k]->0);
            &&& a.send_tails == b.send_tails
            &&& same_requests(a.send, b.send)
            &&& same_requests(a.recv, b.recv)
        } by {
            if k != index {
                assert(self.qps@[k] == pre.qps@[k]);
            }
        }
    }

    /// What taking a result of QP `index`'s ring of `side` leaves.
    pub(super) proof fn lemma_taken_left(&self, pre: &Self, index: int, side: Side, seq: int, taken: bool)
        requires
            pre.inv(),
            pre.has_qp(index),
            self.qps@.len() == pre.qps@.len(),
            forall|k: int| 0 <= k < pre.qps@.len() && k != index ==> #[trigger] self.qps@[k] == pre.qps@[k],
            self.qps@[index] is Some,
            ({
                let (x, y) = (self.qps@[index]->0, pre.qps@[index]->0);
                let queue = queue_of(side);
                &&& x.same_qp(y)
                &&& x.started == y.started
                &&& x.send_tails == y.send_tails
                &&& ring_of(x, queue).inv()
                &&& ring_of(x, queue).left_by_take(&ring_of(y, queue), seq, taken)
                &&& ring_of(x, other_queue(queue)) == ring_of(y, other_queue(queue))
            }),
            self.same_engine_fields(pre),
            self.polled == pre.polled,
            self.polled_at == pre.polled_at,
            self.polled_len == pre.polled_len,
            self.pending == pre.pending,
        ensures
            self.inv_m(),
            self.keeps_requests(pre),
    {
        let (x, y) = (self.qps@[index]->0, pre.qps@[index]->0);
        let queue = queue_of(side);
        let t = pre.token@;
        let m = pre.nic();
        assert(qp_ok(pre.qps@[index], index, t, m));
        lemma_take_kept(ring_of(x, queue), ring_of(y, queue), seq, taken);
        lemma_same_kept(ring_of(y, other_queue(queue)), ring_of(y, other_queue(queue)));
        lemma_table_kept(pre.qps@, self.qps@, pre.next_name@, t, m, pre.pending@, index);
        assert forall|k: int| 0 <= k < self.qps@.len() && #[trigger] self.qps@[k] is Some implies {
            let (a, b) = (self.qps@[k]->0, pre.qps@[k]->0);
            &&& a.send_tails == b.send_tails
            &&& same_requests(a.send, b.send)
            &&& same_requests(a.recv, b.recv)
        } by {
            if k != index {
                assert(self.qps@[k] == pre.qps@[k]);
            }
        }
    }

    /// The fields of the engine a walk leaves as they were.
    pub(super) open spec fn same_engine_fields(&self, pre: &Self) -> bool {
        &&& cq_of(self.cq) == cq_of(pre.cq)
        &&& self.caps == pre.caps
        &&& self.cq_size == pre.cq_size
        &&& self.reserved == pre.reserved
        &&& self.token == pre.token
        &&& self.next_name == pre.next_name
    }
}

} // verus!
