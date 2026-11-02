//! The actual request-resource container, verified independently of the allocation
//! boundary. Shared deduplication preserves the retained frozen allocation; unique
//! resources move unchanged into and out of the container.

use safeverbs_spec::abs::Dir;
use std::sync::Arc;
use vstd::prelude::*;

use super::range::checked_sge_len;
use super::{Frozen, RawMut};
use crate::nic::{Nic, Sge};
use crate::verbs::Verbs;

verus! {

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
    /// The byte permission retained by this actual physical keeper.
    pub closed spec fn byte_domain(self) -> Set<int> {
        match self.resource {
            Lease::Unique(raw) => raw.byte_domain(),
            Lease::Shared(frozen) => (*frozen).byte_domain(),
        }
    }

    /// Backing identity preserves byte domains even after shared guard deduplication.
    pub proof fn lemma_backing_domain(self, other: Self)
        ensures self.same_backing(other) ==> self.byte_domain() == other.byte_domain(),
    {}

    /// Both resources retain the same shared frozen allocation.
    pub closed spec fn same_frozen(self, other: Self) -> bool {
        self.resource.same_frozen(other.resource)
    }

    /// One physical keeper backs the same exclusive resource or frozen allocation.
    pub closed spec fn same_backing(self, other: Self) -> bool {
        match (self.resource, other.resource) {
            (Lease::Unique(a), Lease::Unique(b)) => a == b,
            (Lease::Shared(a), Lease::Shared(b)) => *a == *b,
            _ => false,
        }
    }
}

/// One logical input, preserved even if its redundant physical Arc is dropped.
#[verifier::reject_recursive_types(N)]
#[allow(dead_code)]
pub ghost struct Loan<N: Nic> {
    /// The actual input metadata and resource, captured before deduplication.
    pub part: Part<N>,
    /// None for a standalone, unposted resource group.
    pub access: Option<Dir>,
}

/// The exact local SGE footprint of one WQE, with its request-local loan interval.
/// `None` identifies a receive; a send keeps M's operation-specific opcode.
#[allow(dead_code)] // This footprint is erased from ordinary builds.
pub ghost struct WqeLoans {
    pub wr_id: u64,
    pub opcode: Option<safeverbs_spec::nic::Opcode>,
    pub sges: Seq<Sge>,
    pub first: int,
}

impl WqeLoans {
    pub open spec fn access(self) -> Dir {
        match self.opcode {
            Some(op) => if op.writes_local() { Dir::In } else { Dir::Out },
            None => Dir::In,
        }
    }

    pub open spec fn end(self) -> int { self.first + self.sges.len() }

    /// The native pop loop retains SGE guards in reverse SGE order.
    pub open spec fn matches<N: Nic>(self, loans: Seq<Loan<N>>) -> bool {
        &&& 0 <= self.first <= self.end() <= loans.len()
        &&& forall|j: int| 0 <= j < self.sges.len() ==> {
            let loan = #[trigger] loans[self.end() - 1 - j];
            &&& loan.part.spec_sge() == self.sges[j]
            &&& loan.access == Some(self.access())
        }
    }
}

/// Every logical input below the frontier belongs to a recorded WQE.
pub open spec fn input_is_covered(wqes: Seq<WqeLoans>, i: int) -> bool {
    exists|w: int| 0 <= w < wqes.len()
        && (#[trigger] wqes[w]).first <= i < wqes[w].end()
}

/// A covered prefix is stable while more logical inputs are collected.
pub open spec fn covers_inputs(wqes: Seq<WqeLoans>, frontier: int) -> bool {
    forall|i: int| 0 <= i < frontier ==> #[trigger] input_is_covered(wqes, i)
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
    /// The actual SGE byte set is covered by the permission of its retained owner.
    #[verifier::type_invariant]
    pub closed spec fn inv(self) -> bool {
        match self.lease {
            Lease::Unique(raw) => raw.inv()
                && self.byte_domain() == raw.byte_domain()
                && self.sge.addr == raw.pointer().addr()
                && self.sge.length == raw.byte_len(),
            Lease::Shared(frozen) => (*frozen).inv()
                && self.byte_domain().subset_of((*frozen).byte_domain()),
        }
    }

    /// Exact local bytes named by this SGE (registration is a separate obligation).
    pub closed spec fn byte_domain(self) -> Set<int> {
        Set::range(self.sge.addr as int, self.sge.addr as int + self.sge.length)
    }

    /// No arbitrary SGE/keeper pair is assembled by the typed buffer wrapper.
    #[inline]
    pub(super) fn unique(raw: RawMut<N>, lkey: u32, pd: u64) -> (r: Self)
        requires raw.byte_len() <= u32::MAX,
        ensures r.inv(), r.resource() == Lease::Unique(raw).observation(),
    {
        proof { use_type_invariant(&raw); raw.lemma_domain(); }
        let length = match checked_sge_len(raw.len()) {
            Some(length) => length,
            None => panic!("a single SGE exceeds u32::MAX bytes"),
        };
        Part { sge: Sge { addr: raw.ptr().addr() as u64, length, lkey }, pd, lease: Lease::Unique(raw) }
    }

    /// A shared SGE is a bounded view of the frozen owner's actual permission.
    #[inline]
    pub(super) fn shared(frozen: Arc<Frozen<N>>, offset: usize, len: usize, lkey: u32, pd: u64) -> (r: Self)
        requires offset <= (*frozen).byte_len(), len <= (*frozen).byte_len() - offset,
            len <= u32::MAX,
        ensures r.inv(), r.resource() == Lease::Shared(frozen).observation(),
    {
        proof {
            use_type_invariant(&*frozen);
            frozen.lemma_contents();
            frozen.raw.lemma_domain();
        }
        let length = match checked_sge_len(len) {
            Some(length) => length,
            None => panic!("a single SGE exceeds u32::MAX bytes"),
        };
        let ptr = frozen.raw.ptr();
        let addr = ptr.addr() + offset;
        let part = Part { sge: Sge { addr: addr as u64, length, lkey }, pd, lease: Lease::Shared(frozen) };
        proof { assert(part.byte_domain().subset_of((*frozen).byte_domain())); }
        part
    }

    /// Bridge the SGE footprint to the actual linear owner, without registration assumptions.
    pub proof fn lemma_byte_backing(self)
        requires self.inv(),
        ensures self.byte_domain().subset_of(self.resource().byte_domain()),
            self.byte_domain().to_iset() == (safeverbs_spec::types::Span {
                start: self.spec_sge().addr as int, len: self.spec_sge().length as nat,
            }).bytes(),
    {
        assert(self.byte_domain().to_iset() =~= (safeverbs_spec::types::Span {
            start: self.spec_sge().addr as int, len: self.spec_sge().length as nat,
        }).bytes());
    }

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
    #[allow(dead_code)]
    loans: Ghost<Seq<Loan<N>>>,
    #[allow(dead_code)]
    wqes: Ghost<Seq<WqeLoans>>,
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

    closed spec fn repr_inv(self) -> bool {
        self.first is Some || self.rest@.len() == 0
    }

    /// The actual input parts, including logically distinct shared borrows.
    pub closed spec fn loans(self) -> Seq<Loan<N>> { self.loans@ }

    /// The posted or staged WQEs whose guards this request retains.
    pub closed spec fn wqes(self) -> Seq<WqeLoans> { self.wqes@ }

    /// Every input in this request has WQE provenance, rather than only some.
    /// Standalone, unposted `Leases::from(part)` intentionally need not satisfy it.
    pub open spec fn fully_recorded(self) -> bool {
        covers_inputs(self.wqes(), self.loans().len() as int)
    }

    closed spec fn wqes_ok(self) -> bool {
        &&& forall|i: int| 0 <= i < self.wqes().len()
            ==> (#[trigger] self.wqes()[i]).matches(self.loans())
        &&& forall|i: int, j: int| 0 <= i < j < self.wqes().len()
            ==> (#[trigger] self.wqes()[i]).end() <= (#[trigger] self.wqes()[j]).first
    }

    /// Every logical input has a retained physical lifetime guard. Incoming local
    /// writes require exclusive resources; shared deduplication changes neither fact.
    pub closed spec fn inv(self) -> bool {
        &&& self.repr_inv()
        &&& self.wqes_ok()
        &&& forall|i: int| 0 <= i < self.loans().len() ==> {
            let loan = #[trigger] self.loans()[i];
            &&& loan.part.inv()
            &&& exists|j: int| 0 <= j < self.view().len()
                && (#[trigger] self.view()[j]).same_backing(loan.part.resource())
            &&& loan.access == Some(Dir::In) ==> loan.part.spec_writable()
        }
    }

    /// Export the checked backing/permission facts without exposing private fields.
    pub proof fn lemma_loans(self)
        requires self.inv(),
        ensures
            forall|i: int| 0 <= i < self.loans().len() ==> {
                let loan = #[trigger] self.loans()[i];
                &&& loan.part.inv()
                &&& exists|j: int| 0 <= j < self.view().len()
                    && (#[trigger] self.view()[j]).same_backing(loan.part.resource())
                &&& loan.access == Some(Dir::In) ==> loan.part.spec_writable()
            },
    {}

    /// Every recorded SGE is covered by an actual retained raw-byte permission.
    pub proof fn lemma_byte_backing(self)
        requires self.inv(),
        ensures forall|i: int| 0 <= i < self.loans().len() ==> {
            let part = (#[trigger] self.loans()[i]).part;
            exists|j: int| 0 <= j < self.view().len()
                && (#[trigger] self.view()[j]).same_backing(part.resource())
                && part.byte_domain().subset_of(self.view()[j].byte_domain())
        },
    {
        self.lemma_loans();
        assert forall|i: int| 0 <= i < self.loans().len() implies {
            let part = (#[trigger] self.loans()[i]).part;
            exists|j: int| 0 <= j < self.view().len()
                && (#[trigger] self.view()[j]).same_backing(part.resource())
                && part.byte_domain().subset_of(self.view()[j].byte_domain())
        } by {
            let loan = self.loans()[i];
            let j = choose|j: int| 0 <= j < self.view().len()
                && self.view()[j].same_backing(loan.part.resource());
            loan.part.lemma_byte_backing();
            self.view()[j].lemma_backing_domain(loan.part.resource());
        }
    }

    /// An empty resource group, with no allocation.
    pub fn new() -> (r: Self)
        ensures r.inv(), r.view() == Seq::<Resource<N>>::empty(), r.loans().len() == 0,
            r.wqes().len() == 0, r.fully_recorded(),
    { Leases { first: None, rest: Vec::new(), loans: Ghost(Seq::empty()), wqes: Ghost(Seq::empty()) } }

    /// Adds provenance only after the checked collection loop established its footprint.
    #[inline]
    pub(crate) fn record_wqe(&mut self, Ghost(wqe): Ghost<WqeLoans>)
        requires old(self).inv(), wqe.matches(old(self).loans()),
            forall|i: int| 0 <= i < old(self).wqes().len()
                ==> (#[trigger] old(self).wqes()[i]).end() <= wqe.first,
        ensures final(self).inv(), final(self).loans() == old(self).loans(),
            final(self).view() == old(self).view(),
            final(self).wqes() == old(self).wqes().push(wqe),
            covers_inputs(old(self).wqes(), wqe.first) && wqe.end() == old(self).loans().len()
                ==> final(self).fully_recorded(),
    {
        let ghost before = *self;
        self.wqes = Ghost(self.wqes().push(wqe));
        proof {
            assert forall|i: int| 0 <= i < self.wqes().len()
                implies (#[trigger] self.wqes()[i]).matches(self.loans()) by {
                if i < before.wqes().len() {
                    assert(self.wqes()[i] == before.wqes()[i]);
                } else { assert(self.wqes()[i] == wqe); }
            }
            assert forall|i: int, j: int| 0 <= i < j < self.wqes().len()
                implies (#[trigger] self.wqes()[i]).end() <= (#[trigger] self.wqes()[j]).first by {
                if j < before.wqes().len() {
                    assert(before.wqes()[i].end() <= before.wqes()[j].first);
                } else {
                    assert(self.wqes()[j] == wqe);
                    assert(before.wqes()[i].end() <= wqe.first);
                }
            }
            assert(self.wqes_ok());
            assert(self.repr_inv());
            assert(self.view() == before.view());
            assert(self.loans() == before.loans());
            assert forall|i: int| 0 <= i < self.loans().len() implies {
                let loan = #[trigger] self.loans()[i];
                &&& loan.part.inv()
                &&& exists|j: int| 0 <= j < self.view().len()
                    && (#[trigger] self.view()[j]).same_backing(loan.part.resource())
                &&& loan.access == Some(Dir::In) ==> loan.part.spec_writable()
            } by {
                let j = choose|j: int| 0 <= j < before.view().len()
                    && before.view()[j].same_backing(before.loans()[i].part.resource());
                assert(self.view()[j] == before.view()[j]);
            }
            if covers_inputs(before.wqes(), wqe.first) && wqe.end() == before.loans().len() {
                assert forall|i: int| 0 <= i < self.loans().len()
                    implies #[trigger] input_is_covered(self.wqes(), i) by {
                    if i < wqe.first {
                        assert(input_is_covered(before.wqes(), i));
                        let w = choose|w: int| 0 <= w < before.wqes().len()
                            && (#[trigger] before.wqes()[w]).first <= i < before.wqes()[w].end();
                        assert(self.wqes()[w] == before.wqes()[w]);
                    } else {
                        assert(self.wqes()[before.wqes().len() as int] == wqe);
                        assert(wqe.first <= i < wqe.end());
                    }
                }
            }
        }
    }

    /// Export the checked WQE footprint and nonoverlapping loan-interval facts.
    pub proof fn lemma_wqes(self)
        requires self.inv(),
        ensures
            forall|i: int| 0 <= i < self.wqes().len()
                ==> (#[trigger] self.wqes()[i]).matches(self.loans()),
            forall|i: int, j: int| 0 <= i < j < self.wqes().len()
                ==> (#[trigger] self.wqes()[i]).end() <= (#[trigger] self.wqes()[j]).first,
    {}

    /// Retains an incoming resource or drops only a redundant shared reference.
    #[allow(clippy::collapsible_if)] // Keep the pattern and pointer primitive explicit for Verus.
    #[inline]
    fn push_resource(&mut self, lease: Lease<N>)
        ensures
            final(self).loans() == old(self).loans(),
            final(self).wqes() == old(self).wqes(),
            old(self).repr_inv() ==> final(self).repr_inv(),
            old(self).repr_inv() ==> (
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

    /// An unposted input group has no selected operation yet.
    #[inline]
    pub(crate) fn push(&mut self, part: Part<N>)
        ensures old(self).inv() ==> final(self).inv(),
            final(self).loans() == old(self).loans().push(Loan { part, access: None }),
            final(self).wqes() == old(self).wqes(),
    { self.push_loan(part, Ghost(None)); }

    /// Record the actual operation's access without storing it at runtime.
    #[inline]
    pub(crate) fn push_access(&mut self, part: Part<N>, Ghost(access): Ghost<Dir>)
        requires access is In ==> part.spec_writable(),
        ensures old(self).inv() ==> final(self).inv(),
            final(self).loans() == old(self).loans().push(Loan { part, access: Some(access) }),
            final(self).wqes() == old(self).wqes(),
    { self.push_loan(part, Ghost(Some(access))); }

    fn push_loan(&mut self, part: Part<N>, Ghost(access): Ghost<Option<Dir>>)
        requires access == Some(Dir::In) ==> part.spec_writable(),
        ensures
            old(self).inv() ==> final(self).inv(),
            final(self).loans() == old(self).loans().push(Loan { part, access }),
            final(self).wqes() == old(self).wqes(),
    {
        proof { use_type_invariant(&part); }
        let ghost before = *self;
        let ghost incoming = part;
        self.push_resource(part.lease);
        self.loans = Ghost(before.loans().push(Loan { part: incoming, access }));
        proof {
            if before.inv() {
                assert(self.repr_inv());
                assert forall|i: int| 0 <= i < self.wqes().len()
                    implies (#[trigger] self.wqes()[i]).matches(self.loans()) by {
                    let w = self.wqes()[i];
                    assert(w.matches(before.loans()));
                    assert forall|j: int| 0 <= j < w.sges.len() implies {
                        let loan = #[trigger] self.loans()[w.end() - 1 - j];
                        &&& loan.part.spec_sge() == w.sges[j]
                        &&& loan.access == Some(w.access())
                    } by {
                        assert(self.loans()[w.end() - 1 - j] == before.loans()[w.end() - 1 - j]);
                    }
                }
                assert forall|i: int| 0 <= i < self.loans().len() implies {
                    let loan = #[trigger] self.loans()[i];
                    &&& loan.part.inv()
                    &&& exists|j: int| 0 <= j < self.view().len()
                        && (#[trigger] self.view()[j]).same_backing(loan.part.resource())
                    &&& loan.access == Some(Dir::In) ==> loan.part.spec_writable()
                } by {
                    if i < before.loans().len() {
                        let loan = before.loans()[i];
                        let j = choose|j: int| 0 <= j < before.view().len()
                            && before.view()[j].same_backing(loan.part.resource());
                        assert(self.view()[j] == before.view()[j]);
                        assert(self.loans()[i] == loan);
                    } else {
                        assert(i == before.loans().len());
                        if self.view() == before.view().push(incoming.resource()) {
                            assert(self.view()[before.view().len() as int] == incoming.resource());
                            assert(incoming.resource().same_backing(incoming.resource()));
                        } else {
                            assert(before.view().len() > 0);
                            assert(self.view() == before.view());
                            assert(self.view().last().same_frozen(incoming.resource()));
                            assert(self.view().last().same_backing(incoming.resource()));
                        }
                    }
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Model;
    use crate::pd::Context;

    #[test]
    fn ghost_ledgers_add_no_native_size_or_alignment() {
        #[allow(dead_code)]
        struct NativeShape {
            first: Option<Lease<Model>>,
            rest: Vec<Lease<Model>>,
        }
        assert_eq!(size_of::<Leases<Model>>(), size_of::<NativeShape>());
        assert_eq!(align_of::<Leases<Model>>(), align_of::<NativeShape>());
    }

    #[test]
    fn repeated_overlapping_views_retain_one_shared_guard() {
        let pd = Context::new(Model::new(56)).alloc_pd().unwrap();
        let frozen = pd.alloc::<u8>(64).unwrap().freeze();
        let mut leases = Leases::new();
        for i in 0..256 {
            leases.push(frozen.slice(i % 16..32 + i % 16).into_part());
        }
        assert_eq!(leases.len(), 1);
        let frozen = frozen.try_thaw().unwrap_err();
        drop(leases);
        assert!(frozen.try_thaw().is_ok());
    }

    #[test]
    fn unique_extraction_returns_first_unique_and_releases_other_guards() {
        let model = Model::new(57);
        let pd = Context::new(model.clone()).alloc_pd().unwrap();
        let frozen = pd.alloc::<u8>(32).unwrap().freeze();
        let mut first = pd.alloc::<u8>(16).unwrap();
        first.fill(37);
        let ptr = first.as_ptr();
        let other = pd.alloc::<u8>(8).unwrap();
        let mut leases = Leases::new();
        leases.push(frozen.slice(..16).into_part());
        leases.push(first.into_part());
        leases.push(frozen.slice(8..24).into_part());
        leases.push(other.into_part());
        assert_eq!(leases.len(), 4);
        let first = leases.into_buf_mut::<u8>().unwrap();
        assert_eq!(first.as_ptr(), ptr);
        assert_eq!(&*first, &[37; 16]);
        assert_eq!(model.registered(), 2);
        assert!(frozen.try_thaw().is_ok());
        drop(first);
        assert_eq!(model.registered(), 0);
    }
}
