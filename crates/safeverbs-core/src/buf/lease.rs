//! The actual request-resource container, verified independently of the allocation
//! boundary. Shared deduplication preserves the retained frozen allocation; unique
//! resources move unchanged into and out of the container.

use std::sync::Arc;
use vstd::prelude::*;

use super::{Frozen, RawMut};
use crate::nic::{Nic, Sge};
use crate::verbs::Verbs;

verus! {

/// Allocation/typed-buffer ownership is the remaining buffer boundary.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
#[verifier::external_body]
#[verifier::reject_recursive_types(N)]
pub struct ExRawMut<N: Nic>(RawMut<N>);

/// Frozen allocation contents are opaque here; std Arc preserves their identity.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
#[verifier::external_body]
#[verifier::reject_recursive_types(N)]
pub struct ExFrozen<N: Nic>(Frozen<N>);

/// What keeps a buffer's memory alive while the NIC uses it.
#[verifier::reject_recursive_types(N)]
pub(crate) enum Lease<N: Nic> {
    /// A shared, frozen allocation.
    Shared(Arc<Frozen<N>>),
    /// An exclusive buffer, moved into its request.
    Unique(RawMut<N>),
}

impl<N: Nic> Lease<N> {
    pub(crate) open spec fn same_frozen(self, other: Self) -> bool {
        match (self, other) {
            (Lease::Shared(a), Lease::Shared(b)) => *a == *b,
            _ => false,
        }
    }

    pub(crate) closed spec fn observation(self) -> Resource<N> { Resource { resource: self } }
}

/// The exact retained resource as a proof value. Its allocation internals stay private.
#[verifier::reject_recursive_types(N)]
#[allow(dead_code)] // All construction is erased with proof code.
pub ghost struct Resource<N: Nic> {
    resource: Lease<N>,
}

impl<N: Nic> Resource<N> {
    /// Both resources retain the same shared frozen allocation.
    pub closed spec fn same_frozen(self, other: Self) -> bool {
        self.resource.same_frozen(other.resource)
    }
}

/// The only new primitive here: std Arc's pointer comparison. Equality implies
/// both references retain exactly the same frozen allocation.
#[verifier::external_body]
fn same_frozen_arc<N: Nic>(a: &Arc<Frozen<N>>, b: &Arc<Frozen<N>>) -> (same: bool)
    ensures same ==> **a == **b,
{
    Arc::ptr_eq(a, b)
}

/// A buffer on loan to the NIC: one SGE and the resource keeping it alive.
#[verifier::reject_recursive_types(N)]
pub struct Part<N: Nic = Verbs> {
    pub(crate) sge: Sge,
    pub(crate) pd: u64,
    pub(crate) lease: Lease<N>,
}

impl<N: Nic> Part<N> {
    /// The range passed to the NIC.
    pub closed spec fn spec_sge(self) -> Sge { self.sge }
    /// The resource's protection domain.
    pub closed spec fn spec_pd(self) -> u64 { self.pd }
    /// Whether the resource is exclusive.
    pub closed spec fn spec_writable(self) -> bool { self.lease is Unique }

    /// The actual resource moved into a request.
    pub closed spec fn resource(self) -> Resource<N> { self.lease.observation() }

    pub(crate) proof fn lemma_fields(self)
        ensures self.spec_sge() == self.sge, self.spec_pd() == self.pd,
            self.spec_writable() <==> self.lease is Unique,
            self.resource() == self.lease.observation(),
    {}

    /// Its byte length.
    pub fn len(&self) -> (len: u64)
        ensures len == self.spec_sge().length,
    { self.sge.length as u64 }

    /// Whether it has no bytes.
    pub fn is_empty(&self) -> (empty: bool)
        ensures empty <==> self.spec_sge().length == 0,
    { self.sge.length == 0 }

    /// Whether the NIC may write it (an exclusive buffer).
    pub fn is_writable(&self) -> (writable: bool)
        ensures writable == self.spec_writable(),
    { matches!(self.lease, Lease::Unique(_)) }
}

/// The resources of one request. The first stays inline; repeated consecutive
/// references to the same frozen allocation are retained once.
#[verifier::reject_recursive_types(N)]
pub struct Leases<N: Nic = Verbs> {
    first: Option<Lease<N>>,
    rest: Vec<Lease<N>>,
}

impl<N: Nic> Leases<N> {
    /// The actual retained resources, in their original order.
    pub(super) closed spec fn raw_view(self) -> Seq<Lease<N>> {
        (match self.first { Some(first) => seq![first], None => Seq::empty() }) + self.rest@
    }

    /// The exact ordered resources retained by this request.
    pub closed spec fn view(self) -> Seq<Resource<N>> {
        self.raw_view().map_values(|lease: Lease<N>| lease.observation())
    }

    /// The inline element is populated before the spill vector.
    pub closed spec fn inv(self) -> bool {
        self.first is Some || self.rest@.len() == 0
    }

    /// An empty resource group, with no allocation.
    pub fn new() -> (r: Self)
        ensures r.inv(), r.view() == Seq::<Resource<N>>::empty(),
    { Leases { first: None, rest: Vec::new() } }

    /// Retains an incoming resource or drops only a redundant shared reference.
    pub(crate) fn push(&mut self, lease: Lease<N>)
        ensures
            old(self).inv() ==> final(self).inv(),
            old(self).inv() ==> (
                final(self).view() == old(self).view().push(lease.observation())
                || (old(self).view().len() > 0
                    && old(self).view().last().same_frozen(lease.observation())
                    && final(self).view() == old(self).view())
            ),
    {
        let last = match self.rest.last() {
            Some(l) => Some(l),
            None => self.first.as_ref(),
        };
        if let (Some(Lease::Shared(a)), Lease::Shared(b)) = (last, &lease) {
            if same_frozen_arc(a, b) {
                return;
            }
        }
        if self.first.is_none() {
            self.first = Some(lease);
        } else {
            self.rest.push(lease);
        }
    }

    /// The number of retained resources.
    pub fn len(&self) -> (len: usize)
        requires self.view().len() <= usize::MAX,
        ensures len == self.view().len(),
    {
        self.first.is_some() as usize + self.rest.len()
    }

    /// Whether the group is empty.
    pub fn is_empty(&self) -> (empty: bool)
        ensures self.inv() ==> (empty <==> self.view().len() == 0),
    { self.first.is_none() }

    /// Selects the first unique resource and drops all remaining resources.
    /// Type interpretation is handled by the buffer wrapper after this move.
    pub(super) fn into_unique_raw(self) -> (r: Option<RawMut<N>>)
        ensures
            r is Some ==> exists|i: int| 0 <= i < self.raw_view().len()
                && self.raw_view()[i] == Lease::Unique(r->0)
                && forall|j: int| 0 <= j < i ==> !(self.raw_view()[j] is Unique),
            r is None ==> forall|i: int| 0 <= i < self.raw_view().len()
                ==> !(self.raw_view()[i] is Unique),
    {
        let ghost original = self.raw_view();
        let mut remaining = self;
        if matches!(&remaining.first, Some(Lease::Unique(_))) {
            return match remaining.first.take() {
                Some(Lease::Unique(raw)) => {
                    proof { assert(original[0] == Lease::Unique(raw)); }
                    Some(raw)
                },
                _ => None,
            };
        }
        let mut i = 0;
        while i < remaining.rest.len()
            invariant
                original == self.raw_view(),
                remaining.raw_view() == original,
                !(remaining.first is Some && remaining.first->0 is Unique),
                i <= remaining.rest@.len(),
                forall|j: int| 0 <= j < i ==> !(remaining.rest@[j] is Unique),
            decreases remaining.rest@.len() - i,
        {
            if matches!(&remaining.rest[i], Lease::Unique(_)) {
                let ghost position = i as int + if remaining.first is Some { 1int } else { 0int };
                proof {
                    assert(0 <= position < original.len());
                    assert(original[position] == remaining.rest@[i as int]);
                    assert forall|j: int| 0 <= j < position implies !(original[j] is Unique) by {
                        if remaining.first is Some {
                            if j > 0 { assert(original[j] == remaining.rest@[j - 1]); }
                        } else {
                            assert(original[j] == remaining.rest@[j]);
                        }
                    }
                }
                return match remaining.rest.remove(i) {
                    Lease::Unique(raw) => {
                        proof {
                            assert(original[position] == Lease::Unique(raw));
                            assert(exists|k: int| 0 <= k < original.len()
                                && original[k] == Lease::Unique(raw)
                                && forall|j: int| 0 <= j < k ==> !(original[j] is Unique));
                        }
                        Some(raw)
                    },
                    _ => None,
                };
            }
            i += 1;
        }
        None
    }
}

} // verus!
