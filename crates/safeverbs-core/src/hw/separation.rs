//! Unique receive admission against every buffer the actual worker retains.
//!
//! The immutable worker borrow preserves its rings and resource snapshots exactly.
//! Only the incoming unique byte permission is mutably borrowed, with its identity
//! preserved. The native reference traversal must disappear under optimization.

use super::*;

verus! {

impl<N: Device> Hw<N> {
    /// One actual QP's closed rings, open leases and not-yet-written parts exclude
    /// the incoming unique SGE's bytes. Completed/refused owners are included.
    closed spec fn entry_separated_from(&self, index: int, incoming: Part<N>) -> bool {
        self.qps@[index] is Some ==> {
            let e = self.qps@[index]->0;
            &&& e.send.retained_separated_from(incoming)
            &&& e.recv.retained_separated_from(incoming)
            &&& e.open_leases.separated_from(incoming.byte_domain())
            &&& forall|j: int| 0 <= j < e.held_parts@.len()
                ==> (#[trigger] e.held_parts@[j]).byte_domain().disjoint(incoming.byte_domain())
            &&& forall|j: int| 0 <= j < e.building@.len()
                ==> (#[trigger] e.building@[j]).byte_domain().disjoint(incoming.byte_domain())
        }
    }

    /// Admission-time separation against all actual local worker storage.
    /// This says nothing about other workers or untracked program-held groups.
    pub closed spec fn admission_separated_from(&self, incoming: Part<N>) -> bool {
        forall|i: int| 0 <= i < self.qps@.len()
            ==> #[trigger] self.entry_separated_from(i, incoming)
    }

    /// The successful receive retains this exact logical input in its live request.
    pub closed spec fn recv_holds_input(&self, index: usize, seq: RingSeq, part: Part<N>) -> bool {
        self.has_qp(index as int) && {
            let req = self.qps@[index as int]->0.recv.req(seq as int);
            req is Some && req->0.phase is InFlight
                && req->0.leases.loans() == seq![crate::buf::Loan {
                    part, access: Some(safeverbs_spec::abs::Dir::In),
                }]
        }
    }

    /// Export the private receive observation after the actual ring update.
    pub(super) proof fn lemma_recv_holds_input(&self, index: usize, seq: RingSeq, part: Part<N>)
        requires self.has_qp(index as int),
            self.qps@[index as int]->0.recv.req(seq as int) is Some,
            self.qps@[index as int]->0.recv.req(seq as int)->0.phase is InFlight,
            self.qps@[index as int]->0.recv.req(seq as int)->0.leases.loans() == seq![crate::buf::Loan {
                part, access: Some(safeverbs_spec::abs::Dir::In),
            }],
        ensures self.recv_holds_input(index, seq, part),
    {}

    /// The actual traversal is read-only on the worker, preserving every snapshot.
    /// There is no native byte comparison; the linear token proofs are erased.
    #[inline(always)]
    pub(super) fn separate_unique_input(&self, incoming: &mut Part<N>)
        requires self.inv(), old(incoming).spec_writable(),
        ensures *final(incoming) == *old(incoming),
            self.admission_separated_from(*final(incoming)),
    {
        let mut i = 0;
        while i < self.qps.len()
            invariant
                incoming.spec_writable(),
                self.inv(), *incoming == *old(incoming), i <= self.qps@.len(),
                forall|k: int| 0 <= k < i
                    ==> #[trigger] self.entry_separated_from(k, *incoming),
            decreases self.qps@.len() - i,
        {
            proof { assert(self.entry_ok(i as int, self.token@, self.nic())); }
            if let Some(e) = &self.qps[i] {
                e.send.separate_unique_input(incoming);
                e.recv.separate_unique_input(incoming);
                e.open_leases.separate_unique_input(incoming);
                let mut j = 0;
                while j < e.held_parts.len()
                    invariant
                        incoming.spec_writable(),
                        *incoming == *old(incoming), j <= e.held_parts@.len(),
                        forall|k: int| 0 <= k < j
                            ==> (#[trigger] e.held_parts@[k]).byte_domain().disjoint(incoming.byte_domain()),
                    decreases e.held_parts@.len() - j,
                {
                    incoming.separate_unique_part(&e.held_parts[j]);
                    j += 1;
                }
                let mut j = 0;
                while j < e.building.len()
                    invariant
                        incoming.spec_writable(),
                        *incoming == *old(incoming), j <= e.building@.len(),
                        forall|k: int| 0 <= k < j
                            ==> (#[trigger] e.building@[k]).byte_domain().disjoint(incoming.byte_domain()),
                    decreases e.building@.len() - j,
                {
                    incoming.separate_unique_part(&e.building[j]);
                    j += 1;
                }
                proof { assert(self.entry_separated_from(i as int, *incoming)); }
            }
            i += 1;
        }
    }
}

} // verus!

#[cfg(all(test, not(verus_keep_ghost)))]
mod native {
    use super::*;
    use crate::model::Model;

    #[unsafe(no_mangle)]
    #[inline(never)]
    pub fn unique_admission_worker(worker: &Hw<Model>, incoming: &mut Part<Model>) {
        worker.separate_unique_input(incoming);
    }
}
