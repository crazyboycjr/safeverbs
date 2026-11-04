//! A buffer coming in against the keepers a ring's slots retain, without changing it. The
//! traversal changes nothing, so it exists in Verus builds alone (`cfg(verus_keep_ghost)`).

use super::*;
#[cfg(verus_keep_ghost)] // Named by the proofs alone.
use crate::buf::{Lease, Resource};
use crate::nic::Nic;

verus! {

impl<L> Ring<L> {
    /// Every public payload observation comes from its indexed slot.
    proof fn lemma_payload_coverage(&self, s: int)
        requires self.inv(),
        ensures
            0 <= s % self.n() < self.slots@.len(),
            self.req(s) is Some ==>
                self.slots@[s % self.n()].leases == Some(self.req(s)->0.leases),
            self.staged(s) is Some ==>
                self.slots@[s % self.n()].leases == Some(self.staged(s)->0.leases),
            self.refused(s) is Some ==>
                self.slots@[s % self.n()].leases == self.refused(s),
    {
        reveal(Ring::inv_slots_but);
        vstd::arithmetic::div_mod::lemma_mod_bound(s, self.n());
        if self.staged(s) is Some {
            assert(self.live_ok(s));
        }
    }
}

impl<C, N: Nic> Ring<Payload<C, N>> {
    /// What keeps every request of the ring alive is apart from `incoming`, those waiting after
    /// retirement or refusal too.
    pub(crate) open spec fn retained_apart(&self, incoming: Resource<N>) -> bool {
        &&& forall|s: int| self.req(s) is Some ==>
            (#[trigger] self.req(s))->0.leases.leases.apart_from(incoming)
        &&& forall|s: int| self.staged(s) is Some ==>
            (#[trigger] self.staged(s))->0.leases.leases.apart_from(incoming)
        &&& forall|s: int| self.refused(s) is Some ==>
            (#[trigger] self.refused(s))->0.leases.apart_from(incoming)
    }

    /// What is apart from a keeper is apart from what it keeps.
    pub(crate) proof fn lemma_retained_backing(&self, keeper: Resource<N>, kept: Resource<N>)
        requires
            self.retained_apart(keeper),
            keeper.same_backing(kept),
        ensures
            self.retained_apart(kept),
    {
        assert forall|s: int| self.req(s) is Some implies
            (#[trigger] self.req(s))->0.leases.leases.apart_from(kept) by {
            self.req(s)->0.leases.leases.lemma_apart_backing(keeper, kept);
        }
        assert forall|s: int| self.staged(s) is Some implies
            (#[trigger] self.staged(s))->0.leases.leases.apart_from(kept) by {
            self.staged(s)->0.leases.leases.lemma_apart_backing(keeper, kept);
        }
        assert forall|s: int| self.refused(s) is Some implies
            (#[trigger] self.refused(s))->0.leases.apart_from(kept) by {
            self.refused(s)->0.leases.lemma_apart_backing(keeper, kept);
        }
    }

    /// Check an incoming buffer against all real payloads. The buffer and every ring snapshot stay
    /// exact.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    pub(crate) fn separate_lease(&self, incoming: &mut Lease<N>)
        requires self.inv(),
        ensures
            *final(incoming) == *old(incoming),
            self.retained_apart(final(incoming).observation()),
    {
        let mut i = 0;
        while i < self.slots.len()
            invariant
                *incoming == *old(incoming),
                i <= self.slots@.len(),
                forall|j: int| 0 <= j < i && (#[trigger] self.slots@[j]).leases is Some
                    ==> self.slots@[j].leases->0.leases.apart_from(incoming.observation()),
            decreases self.slots@.len() - i,
        {
            if let Some(payload) = self.slots[i].leases.as_ref() {
                payload.leases.separate_lease(incoming);
            }
            i += 1;
        }
        proof {
            assert forall|s: int| self.req(s) is Some implies
                (#[trigger] self.req(s))->0.leases.leases.apart_from(incoming.observation()) by {
                self.lemma_payload_coverage(s);
            }
            assert forall|s: int| self.staged(s) is Some implies
                (#[trigger] self.staged(s))->0.leases.leases.apart_from(incoming.observation()) by {
                self.lemma_payload_coverage(s);
            }
            assert forall|s: int| self.refused(s) is Some implies
                (#[trigger] self.refused(s))->0.leases.apart_from(incoming.observation()) by {
                self.lemma_payload_coverage(s);
            }
        }
    }
}

} // verus!
