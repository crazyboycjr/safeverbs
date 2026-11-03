//! Unique admission against actual retained slot payloads, without modifying a ring.

use super::*;
use crate::buf::{Leases, Part};
use crate::nic::Nic;

verus! {

impl<L> Ring<L> {
    /// Every public payload observation comes from the actual indexed slot.
    proof fn lemma_payload_coverage(&self, s: int, w: int)
        requires self.inv(),
        ensures
            0 <= s % self.n() < self.slots@.len(),
            self.req(s) is Some ==>
                self.slots@[s % self.n()].leases == Some(self.req(s)->0.leases),
            self.staged(s) is Some ==>
                self.slots@[s % self.n()].leases == Some(self.staged(s)->0.leases),
            self.refused(s) is Some ==>
                self.slots@[s % self.n()].leases == self.refused(s),
            self.dma(s, w) is Some ==>
                self.slots@[s % self.n()].leases == self.dma(s, w),
    {
        reveal(Ring::inv_slots);
        vstd::arithmetic::div_mod::lemma_mod_bound(s, self.n());
        if self.req(s) is Some || self.staged(s) is Some || self.refused(s) is Some {
            if s < self.retired { assert(self.done_ok(s)); }
            else { assert(self.live_ok(s)); }
            assert(self.slot(s).leases is Some);
        }
    }
}

impl<N: Nic> Ring<Leases<N>> {
    /// Spatial exclusion proved for every actual slot retaining a payload.
    pub(crate) closed spec fn retained_separated_from(&self, incoming: Part<N>) -> bool {
        forall|i: int| 0 <= i < self.slots@.len()
            && (#[trigger] self.slots@[i]).leases is Some
            ==> self.slots@[i].leases->0.separated_from(incoming.byte_domain())
    }

    /// Check a unique incoming Part against all real payloads, including results
    /// waiting after retirement or refusal. The input and every ring snapshot stay exact.
    #[inline(always)]
    pub(crate) fn separate_unique_input(&self, incoming: &mut Part<N>)
        requires self.inv(), old(incoming).spec_writable(),
        ensures
            *final(incoming) == *old(incoming),
            self.retained_separated_from(*final(incoming)),
            forall|s: int| self.req(s) is Some ==>
                (#[trigger] self.req(s))->0.leases.separated_from(final(incoming).byte_domain()),
            forall|s: int| self.staged(s) is Some ==>
                (#[trigger] self.staged(s))->0.leases.separated_from(final(incoming).byte_domain()),
            forall|s: int| self.refused(s) is Some ==>
                (#[trigger] self.refused(s))->0.separated_from(final(incoming).byte_domain()),
            forall|s: int, w: int| self.dma(s, w) is Some ==>
                (#[trigger] self.dma(s, w))->0.separated_from(final(incoming).byte_domain()),
    {
        let mut i = 0;
        while i < self.slots.len()
            invariant
                incoming.spec_writable(),
                *incoming == *old(incoming),
                i <= self.slots@.len(),
                forall|j: int| 0 <= j < i && (#[trigger] self.slots@[j]).leases is Some
                    ==> self.slots@[j].leases->0.separated_from(incoming.byte_domain()),
            decreases self.slots@.len() - i,
        {
            if let Some(leases) = self.slots[i].leases.as_ref() {
                leases.separate_unique_input(incoming);
            }
            i += 1;
        }
        proof {
            assert(self.retained_separated_from(*incoming));
            assert forall|s: int| self.req(s) is Some implies
                (#[trigger] self.req(s))->0.leases.separated_from(incoming.byte_domain()) by {
                self.lemma_payload_coverage(s, 0);
            }
            assert forall|s: int| self.staged(s) is Some implies
                (#[trigger] self.staged(s))->0.leases.separated_from(incoming.byte_domain()) by {
                self.lemma_payload_coverage(s, 0);
            }
            assert forall|s: int| self.refused(s) is Some implies
                (#[trigger] self.refused(s))->0.separated_from(incoming.byte_domain()) by {
                self.lemma_payload_coverage(s, 0);
            }
            assert forall|s: int, w: int| self.dma(s, w) is Some implies
                (#[trigger] self.dma(s, w))->0.separated_from(incoming.byte_domain()) by {
                self.lemma_payload_coverage(s, w);
            }
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
    pub fn unique_admission_leases(leases: &Leases<Model>, incoming: &mut Part<Model>) {
        leases.separate_unique_input(incoming);
    }

    #[unsafe(no_mangle)]
    #[inline(never)]
    pub fn unique_admission_ring(ring: &Ring<Leases<Model>>, incoming: &mut Part<Model>) {
        ring.separate_unique_input(incoming);
    }
}
