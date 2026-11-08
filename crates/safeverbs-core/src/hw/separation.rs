//! A buffer coming in against every buffer the engine retains.
//!
//! The immutable engine borrow preserves its rings and resource snapshots exactly. Only the incoming
//! unique byte permission is mutably borrowed, with its identity preserved. The traversal changes
//! nothing, so it exists in Verus builds alone (`cfg(verus_keep_ghost)`).

use super::*;
#[cfg(verus_keep_ghost)] // Named by the proofs alone.
use crate::buf::{Lease, Resource};
#[cfg(verus_keep_ghost)]
use safeverbs_spec::compose::*;

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

    /// What keeps every request of this engine alive is apart from what keeps `other`'s.
    pub open(super) spec fn kept_apart_from<D>(&self, other: Hw<D, N>) -> bool {
        refines::table_kept_apart(self.qps@, other)
    }

    /// What keeps every request of this engine alive is apart from what keeps `other`'s: their
    /// linear permissions, each of this engine's borrowed exclusively in turn against `other`'s
    /// rings. It changes no keeper, and exists in Verus builds alone.
    #[cfg(verus_keep_ghost)]
    pub(crate) fn separate_from<D>(&mut self, other: &Hw<D, N>)
        requires
            old(self).inv(),
            other.inv(),
        ensures
            old(self).kept_apart_from(*other),
        no_unwind
    {
        let ghost before = self.qps@;
        let ghost start = *self;
        let mut i = 0;
        while i < self.qps.len()
            invariant
                other.inv(),
                start.inv(),
                start.qps@ == before,
                self.qps@.len() == before.len(),
                i <= before.len(),
                forall|j: int| i <= j < before.len() ==> #[trigger] self.qps@[j] == before[j],
                forall|j: int| 0 <= j < i && #[trigger] refines::has_entry(before, j) ==> {
                    &&& before[j]->0.send.kept_apart_from(*other)
                    &&& before[j]->0.recv.kept_apart_from(*other)
                },
            decreases before.len() - i,
        {
            proof { assert(start.entry_ok(i as int, start.token@, start.nic())); }
            let entry = element_mut(self.qps.as_mut_slice(), i);
            match entry {
                Some(e) => {
                    e.send.separate_from(other);
                    e.recv.separate_from(other);
                },
                None => {},
            }
            i += 1;
        }
    }

    /// **Two engines of one device.** Their shares are apart and their views of M are of one
    /// history (`one_history`), and their requests' buffers are apart where one is unique (their
    /// linear permissions, by [`Hw::separate_from`]). What it says is of the engines as they were;
    /// it exists in Verus builds alone.
    #[cfg(verus_keep_ghost)]
    pub(crate) fn separate_engines<D>(&mut self, other: &Hw<D, N>)
        requires
            old(self).inv(),
            other.inv(),
            old(self).token().device() == other.token().device(),
        ensures
            disjoint(old(self).token().share(), other.token().share()),
            old(self).token().time() <= other.token().time()
                ==> interleaved(old(self).token().share(), old(self).nic(), other.nic()),
            other.token().time() <= old(self).token().time()
                ==> interleaved(other.token().share(), other.nic(), old(self).nic()),
            parts_apart(old(self).part(), other.part()),
            parts_apart(other.part(), old(self).part()),
    {
        let ghost start = *self;
        proof {
            one_history(self.token.borrow_mut(), other.token.borrow());
        }
        self.separate_from(other);
        proof {
            start.lemma_parts_apart(*other);
        }
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
