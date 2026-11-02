//! Linear byte ownership with stable identity under borrowed disjointness proofs.
//!
//! The per-byte tokens describe allocation ownership, not initialized contents.
//! All operations are erased proofs; no map exists in the native buffer layout.

#![allow(dead_code)]
#![allow(unused_imports)] // Raw allocation conversion is entirely erased.

use vstd::prelude::*;
use vstd::raw_ptr::{PointsTo, PointsToRaw, Provenance};

verus! {

/// One linear permission per byte, all from the same allocation provenance.
pub(crate) tracked struct BytePermission {
    tracked bytes: Map<int, PointsTo<u8>>,
    ghost source: Provenance,
}

impl BytePermission {
    /// Stable identities of the linear tokens, exposed only as proof values.
    pub(crate) closed spec fn tokens(self) -> Map<int, PointsTo<u8>> {
        self.bytes
    }

    /// The exact set of byte addresses owned by this permission.
    pub(crate) closed spec fn dom(self) -> Set<int> {
        self.bytes.dom()
    }

    /// Allocation provenance, including for an empty permission.
    pub(crate) closed spec fn provenance(self) -> Provenance {
        self.source
    }

    /// Every map key names its byte token's address and allocation provenance.
    pub(crate) closed spec fn inv(self) -> bool {
        forall|address: int| #[trigger] self.bytes.dom().contains(address) ==>
            0 <= address <= usize::MAX
                && self.bytes[address].ptr().addr() == address
                && self.bytes[address].ptr()@.provenance == self.source
    }

    /// Consume an actual raw allocation range into its per-byte permissions.
    pub(crate) proof fn from_raw(
        tracked raw: PointsToRaw,
        start: usize,
        len: usize,
    ) -> (tracked permission: Self)
        requires
            raw.is_range(start as int, len as int),
            start as int + len as int <= usize::MAX as int + 1,
        ensures
            permission.inv(),
            permission.dom() == Set::range(start as int, start as int + len as int),
            permission.provenance() == raw.provenance(),
        decreases len,
    {
        broadcast use vstd::layout::align_of_u8;
        let source = raw.provenance();
        if len == 0 {
            let tracked bytes = Map::tracked_empty();
            assert(bytes.dom() =~= Set::range(start as int, start as int + len as int));
            Self { bytes, source }
        } else {
            let last = (start as int + len as int - 1) as usize;
            assert(start as int <= last as int <= usize::MAX);
            let tracked (one, rest) = raw.split(Set::range(last as int, last as int + 1));
            assert(rest.dom() =~= Set::range(start as int, last as int));
            let tracked byte = one.into_typed::<u8>(last);
            let tracked mut prefix = Self::from_raw(rest, start, (len - 1) as usize);
            prefix.bytes.tracked_insert(last as int, byte);
            assert(prefix.dom() =~= Set::range(start as int, start as int + len as int));
            prefix
        }
    }

    /// Recover one raw range for deallocation, consuming all of its byte tokens.
    pub(crate) proof fn into_raw(
        tracked mut self,
        start: usize,
        len: usize,
    ) -> (tracked raw: PointsToRaw)
        requires
            self.inv(),
            self.dom() == Set::range(start as int, start as int + len as int),
            start as int + len as int <= usize::MAX as int + 1,
        ensures
            raw.is_range(start as int, len as int),
            raw.provenance() == self.provenance(),
        decreases len,
    {
        if len == 0 {
            let tracked raw = PointsToRaw::empty(self.source);
            assert(raw.dom() =~= Set::range(start as int, start as int + len as int));
            raw
        } else {
            let last = (start as int + len as int - 1) as usize;
            assert(self.bytes.dom().contains(last as int));
            let tracked mut byte = self.bytes.tracked_remove(last as int);
            assert(self.inv());
            assert(self.dom() =~= Set::range(start as int, last as int));
            let tracked prefix = self.into_raw(start, (len - 1) as usize);
            byte.leak_contents();
            let tracked one = byte.into_raw();
            let tracked raw = prefix.join(one);
            assert(raw.dom() =~= Set::range(start as int, start as int + len as int));
            raw
        }
    }

    /// Extract exactly `keys`, preserving the identity of every byte token.
    pub(crate) proof fn split(
        tracked mut self,
        keys: Set<int>,
    ) -> (tracked parts: (Self, Self))
        requires
            self.inv(),
            keys.subset_of(self.dom()),
        ensures
            parts.0.inv(), parts.1.inv(),
            parts.0.provenance() == self.provenance(),
            parts.1.provenance() == self.provenance(),
            parts.0.dom() == keys,
            parts.1.dom() == self.dom().difference(keys),
            parts.0.dom().disjoint(parts.1.dom()),
            parts.0.tokens() == self.tokens().restrict(keys),
            parts.1.tokens() == self.tokens().remove_keys(keys),
            parts.0.tokens().union_prefer_right(parts.1.tokens()) == self.tokens(),
    {
        let ghost original = self;
        let tracked selected = self.bytes.tracked_remove_keys(keys);
        let tracked left = Self { bytes: selected, source: self.source };
        assert(left.inv());
        assert(self.inv());
        assert(left.dom() =~= keys);
        assert(self.dom() =~= original.dom().difference(keys));
        assert(left.bytes.union_prefer_right(self.bytes) =~= original.bytes);
        (left, self)
    }

    /// Recombine disjoint byte domains without replacing any existing token.
    pub(crate) proof fn join(
        tracked mut self,
        tracked other: Self,
    ) -> (tracked joined: Self)
        requires
            self.inv(), other.inv(),
            self.provenance() == other.provenance(),
            self.dom().disjoint(other.dom()),
        ensures
            joined.inv(),
            joined.provenance() == self.provenance(),
            joined.dom() == self.dom().union(other.dom()),
            joined.tokens() == self.tokens().union_prefer_right(other.tokens()),
    {
        self.bytes.tracked_union_prefer_right(other.bytes);
        assert(self.inv());
        self
    }

    /// Distinct borrowed permissions cannot own the same byte address.
    /// A shared frozen permission is borrowed without extracting its contents.
    pub(crate) proof fn disjoint(tracked &mut self, tracked other: &Self)
        requires
            old(self).inv(), other.inv(),
        ensures
            *final(self) == *old(self),
            final(self).dom().disjoint(other.dom()),
    {
        if !self.dom().disjoint(other.dom()) {
            let address = choose|address: int| #![auto]
                self.dom().contains(address) && other.dom().contains(address);
            let tracked mine = self.bytes.tracked_borrow_mut(address);
            let tracked theirs = other.bytes.tracked_borrow(address);
            mine.is_disjoint(theirs);
            assert(false);
        }
    }
}

} // verus!
