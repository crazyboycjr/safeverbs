//! A QP's part of a polled completion: the ring of the queue it names retires what it ends,
//! and the QP stays coupled to M. Routing the completion to the QP is [`Hw::progress`]'s.
#![allow(unused_imports)]
use super::*;

verus! {

impl<C, N: Device> HwQp<C, N> {
    /// The entry keeps its name in M and the configuration its typed key relies on
    /// ([`Hw::same_table`]).
    pub(super) open spec fn same_config(&self, pre: Self) -> bool {
        &&& self.id() == pre.id()
        &&& self.qp_type == pre.qp_type
        &&& self.atomics == pre.atomics
        &&& self.caps == pre.caps
        &&& self.pd_id == pre.pd_id
    }

    /// Hands the completion `wc` of this QP, the one `before`'s `poll_cq` step pops, to the
    /// ring of the queue its `wr_id` names, which retires every request it ends; whether one
    /// retired. The send ring's results stay M's (`send_outcomes`), and so do the receive
    /// ring's (`outcomes`).
    #[inline(always)]
    #[verifier::spinoff_prover]
    pub(super) fn complete_cqe(
        &mut self, wc: &Wc, wakes: &mut WakeBatch,
        Ghost(i): Ghost<int>, Ghost(t): Ghost<NicToken>,
        Ghost(before): Ghost<Nic::State>, Ghost(after): Ghost<Nic::State>,
    ) -> (changed: bool)
        requires nic_inv(before), share_wf(before, t.share()), 0 <= i < MAX_QPS,
            old(self).ok(i as u16, before, t.share(), t.pd(),
                t.started(old(self).id()), t.written(old(self).id())),
            nic_step(before, after, Nic::Step::poll_cq(t.share().cq)),
            reports(*wc, before.cqs[t.share().cq].entries[0]),
            before.cqs[t.share().cq].entries[0].qp == old(self).id(),
        ensures nic_inv(after), share_wf(after, t.share()),
            final(self).same_config(*old(self)),
            final(self).ok(i as u16, after, t.share(), t.pd(),
                t.started(old(self).id()), t.written(old(self).id())),
    {
        let ghost sh = t.share();
        let ghost c = sh.cq;
        let ghost e = before.cqs[c].entries[0];
        let ghost qp = before.qps[e.qp];
        let ghost e0 = *self;
        proof {
            lemma_reports(*wc, e);
            safeverbs_spec::nic_proofs::lemma_head_is_first(before, c);
            lemma_poll(before, after, c);
            lemma_wr_id_fields(i as u16, e.queue is Recv, e.seq as u64);
            assert(before.qp_wf(qp));
            if e.queue is Send {
                assert(qp.sq[e.seq - qp.sq_head].wr.wr_id == spec_wr_id(i as u16, false, e.seq as u64));
            } else {
                assert(qp.rq[e.seq - qp.rq_head].wr.wr_id == spec_wr_id(i as u16, true, e.seq as u64));
            }
        }
        let recv = is_recv(wc.wr_id);
        proof {
            assert(recv == (e.queue is Recv));
        }
        let retired;
        let changed;
        if recv {
            retired = self.recv.retired();
            proof {
                e0.recv.lemma_wqes();
                lemma_low_bits_determine(e.seq as u64, e.seq as u64, e0.recv.spec_wqe_retired());
            }
            let wqe = wqe_of(wc.wr_id, self.recv.wqe_retired());
            proof {
                assert(wqe == e.seq);
            }
            let _known = self.recv.complete_with_wakes(wqe, *wc, wakes);
            proof { lemma_resources_completed(e0.recv, self.recv, wqe, *wc); }
            changed = self.recv.retired() != retired;
            proof {
                assert(self.send == e0.send);
            }
        } else {
            retired = self.send.retired();
            proof {
                e0.send.lemma_wqes();
            }
            let wqe = wqe_of(wc.wr_id, self.send.wqe_retired());
            proof {
                assert(wqe == e.seq);
                assert(e0.send.spec_wqe_retired() <= wqe < e0.send.spec_wqe_committed());
            }
            let _known = self.send.complete_with_wakes(wqe, *wc, wakes);
            proof {
                lemma_resources_completed(e0.send, self.send, wqe, *wc);
                assert forall|s: int| #[trigger] self.send.meta(s) is Some implies
                    e0.send.meta(s) is Some
                    && self.send.meta(s)->0.implied == e0.send.meta(s)->0.implied
                    && self.send_tails@.contains_key(s)
                    && self.send_tails@[s] == e0.send_tails@[s] by {
                    e0.send.lemma_completion_observation(s, wqe, *wc, self.send.spec_retired() as int);
                }
                send_outcomes::lemma_implied_kept(e0.send, self.send, e0.send_tails@, self.send_tails@);
            }
            changed = self.send.retired() != retired;
            proof {
                assert(self.recv == e0.recv);
            }
        }
        proof {
            let q = e.qp;
            let qp2 = after.qps[q];
            let en = *self;
            let nn = (e.seq + 1 - (if e.queue is Send { qp.sq_head } else { qp.rq_head })) as int;
            lemma_nic_step_is_next(before, after, Nic::Step::poll_cq(c));
            lemma_step_keeps_invariant(before, after);
            lemma_mine_keeps_share_wf(sh, ISet::empty(), before, after, Nic::Step::poll_cq(c));
            if e.queue is Send {
                assert forall|k: int| 0 <= k < qp2.sq.len() implies (#[trigger] qp2.sq[k]).wr.wr_id == spec_wr_id(
                    i as u16,
                    false,
                    (qp2.sq_head + k) as u64,
                ) by {
                    assert(qp2.sq[k] == qp.sq[k + nn]);
                }
                assert(sq_linked(en.send, i as u16, qp2));
                assert(rq_linked(en.recv, i as u16, qp2));
            } else {
                assert forall|k: int| 0 <= k < qp2.rq.len() implies (#[trigger] qp2.rq[k]).wr.wr_id == spec_wr_id(
                    i as u16,
                    true,
                    (qp2.rq_head + k) as u64,
                ) by {
                    assert(qp2.rq[k] == qp.rq[k + nn]);
                }
                assert(sq_linked(en.send, i as u16, qp2));
                assert(rq_linked(en.recv, i as u16, qp2));
            }
            en.send.lemma_wqes();
            en.recv.lemma_wqes();
            assert forall|k: int| 0 <= k < qp2.sq.len() implies {
                let wr = (#[trigger] qp2.sq[k]).wr;
                &&& !wr.inline
                &&& ring_backed(en.send, qp2.sq_head + k, wr.wr_id, Some(wr.opcode), wr.sges)
            } by {
                let old_k = if e.queue is Send { k + nn } else { k };
                assert(qp2.sq[k].wr == qp.sq[old_k].wr);
                let wr = qp2.sq[k].wr;
                lemma_backed_kept(e0.send, en.send, qp2.sq_head + k, wr.wr_id, Some(wr.opcode), wr.sges);
            }
            assert forall|k: int| 0 <= k < qp2.rq.len() implies {
                let wr = (#[trigger] qp2.rq[k]).wr;
                ring_backed(en.recv, qp2.rq_head + k, wr.wr_id, None, wr.sges)
            } by {
                let old_k = if e.queue is Recv { k + nn } else { k };
                assert(qp2.rq[k].wr == qp.rq[old_k].wr);
                let wr = qp2.rq[k].wr;
                lemma_backed_kept(e0.recv, en.recv, qp2.rq_head + k, wr.wr_id, None, wr.sges);
            }
            let written = t.written(q);
            assert forall|j: int| 0 <= j < written.len() implies
                written_backed(en, en.send.spec_wqe_committed() + j, #[trigger] written[j]) by {
                lemma_written_kept(e0, en, en.send.spec_wqe_committed() + j, written[j]);
            }
            assert forall|j: int| 0 <= j < written.len() implies after.send_wr_ok(qp2.attr, #[trigger] written[j]) by {
                assert(before.send_wr_ok(qp.attr, written[j]));
            }
            assert forall|s: int| #[trigger] en.send.meta(s) is Some implies e0.send.meta(s) is Some
                && en.send.meta(s)->0.first == e0.send.meta(s)->0.first
                && en.send.meta(s)->0.end == e0.send.meta(s)->0.end by {
                if e.queue is Send {
                    e0.send.lemma_completion_observation(s, e.seq as u64, *wc, en.send.spec_retired() as int);
                }
            }
            assert forall|s: int| #[trigger] en.send.req(s) is Some implies e0.send.req(s) is Some by {
                if e.queue is Send && e0.send.spec_retired() <= s < en.send.spec_retired() {
                    e0.send.lemma_retired_view(s, e0.send.retire_outcome(s, e.seq as u64, *wc));
                }
            }
            send_outcomes::lemma_bound_kept(e0.send, en.send, e0.send_tails@, i as u16,
                qp, qp2, t.written(q));
            assert forall|w: WqeRef| w.qp == q && w.queue is Recv && w.seq < qp.rq_head
                && #[trigger] before.retired.contains_key(w)
                implies after.retired.contains_key(w) && after.retired[w] == before.retired[w] by {
                reveal(retires);
            }
            if e.queue is Send {
                send_outcomes::lemma_results_polled(e0.send, en.send, e0.send_tails@, i as u16,
                    t.written(q), before, after, c, q, e.seq as u64, *wc);
                outcomes::lemma_recv_framed(en.recv, before, after, q);
            } else {
                assert forall|w: WqeRef| w.qp == q && w.queue is Send && w.seq < qp.sq_head
                    && #[trigger] before.retired.contains_key(w)
                    implies after.retired.contains_key(w) && after.retired[w] == before.retired[w] by {
                    reveal(retires);
                }
                send_outcomes::lemma_results_framed(en.send, before, after, q);
                // M allows a receive a message or a notification with immediate data.
                assert(qp.rq[e.seq - qp.rq_head].outcome == Some(e.outcome));
                assert(qp.rq[e.seq - qp.rq_head].wr.allows_outcome(e.outcome));
                outcomes::lemma_recv_completed(e0.recv, en.recv, before, after, q, e.seq as u64, *wc);
            }
            assert(self.ok(i as u16, after, sh, t.pd(), t.started(q), t.written(q)));
        }
        changed
    }
}

} // verus!
