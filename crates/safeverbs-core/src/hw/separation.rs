//! A buffer coming in against every buffer the engine retains.
//!
//! The immutable engine borrow preserves its rings and resource snapshots exactly. Only the incoming
//! unique byte permission is mutably borrowed, with its identity preserved. The traversal changes
//! nothing, so it exists in Verus builds alone (`cfg(verus_keep_ghost)`).

use super::*;
#[cfg(verus_keep_ghost)] // Named by the proofs alone.
use crate::buf::{Lease, Resource};

verus! {

impl<C, N: Device> Hw<C, N> {
    /// What keeps every request of the engine alive is apart from `incoming`. This says nothing about
    /// other engines.
    pub open(super) spec fn table_apart(&self, incoming: Resource<N>) -> bool {
        refines::table_apart(self.qps@, incoming)
    }

    /// What is apart from a keeper is apart from what it keeps.
    pub(crate) proof fn lemma_table_apart_backing(&self, keeper: Resource<N>, kept: Resource<N>)
        requires
            self.table_apart(keeper),
            keeper.same_backing(kept),
        ensures
            self.table_apart(kept),
    {
        refines::lemma_table_apart_backing(self.qps@, keeper, kept);
    }

    /// The traversal is read-only on the engine, preserving every snapshot.
    /// There is no native byte comparison; the linear token proofs are erased.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    pub(crate) fn separate_lease(&self, incoming: &mut Lease<N>)
        requires self.inv(),
        ensures *final(incoming) == *old(incoming),
            self.table_apart(final(incoming).observation()),
        no_unwind
    {
        let mut i = 0;
        while i < self.qps.len()
            invariant
                self.inv(), *incoming == *old(incoming), i <= self.qps@.len(),
                forall|k: int| 0 <= k < i && #[trigger] refines::has_entry(self.qps@, k) ==> {
                    &&& self.qps@[k]->0.send.retained_apart(incoming.observation())
                    &&& self.qps@[k]->0.recv.retained_apart(incoming.observation())
                },
            decreases self.qps@.len() - i,
        {
            proof { assert(self.entry_ok(i as int, self.token@, self.nic())); }
            if let Some(e) = &self.qps[i] {
                e.send.separate_lease(incoming);
                e.recv.separate_lease(incoming);
            }
            i += 1;
        }
    }
}

} // verus!
