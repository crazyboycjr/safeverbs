//! A QP's part of a polled completion: the ring of the queue it names retires what it ends,
//! and the QP stays coupled to M. Routing the completion to the QP is [`Hw::progress`]'s.
#![allow(unused_imports)]
use super::*;

// On this branch the specs and proofs in this file still describe the representation before the
// fast data path (see docs/verification.md): the contracts were kept as they were, the body proofs
// of rewritten functions are at e28c2e8. A plain build erases them all.
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
    /// ring of the queue its `wr_id` names, which retires every request it ends. The send ring's
    /// results stay M's (`send_outcomes`), and so do the receive ring's (`outcomes`).
    #[inline(always)]
    #[verifier::spinoff_prover]
    pub(super) fn complete_cqe(
        &mut self, wc: &Wc, wakes: &mut WakeBatch,
        Ghost(i): Ghost<int>, Ghost(t): Ghost<NicToken>,
        Ghost(before): Ghost<Nic::State>, Ghost(after): Ghost<Nic::State>,
    )
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
        let recv = is_recv(wc.wr_id);
        let ring = if recv { &mut self.recv } else { &mut self.send };
        let wqe = wqe_of(wc.wr_id, ring.wqe_retired());
        let _known = ring.complete_with_wakes(recv, wqe, wc, wakes);
    }

}

} // verus!
