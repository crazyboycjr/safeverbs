//! What keeps a request's buffers alive, verified independently of the allocation
//! boundary. Shared deduplication preserves the retained frozen allocation; unique
//! resources move unchanged into and out of the container.
//!
//! The traversals that prove inputs disjoint (`Leases::disjoint_incoming` and the receive
//! admission's `separate_unique_*`) only borrow linear permissions and change nothing, so
//! they exist in Verus builds alone (`cfg(verus_keep_ghost)`); plain builds have no such code.

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

    /// A unique incoming owner is separated from a readonly retained keeper.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    fn separate_unique_input(&self, incoming: &mut Self)
        requires *old(incoming) is Unique,
        ensures
            *final(incoming) == *old(incoming),
            self.observation().byte_domain().disjoint(final(incoming).observation().byte_domain()),
        no_unwind
    {
        match incoming {
            Lease::Unique(raw) => {
                proof { use_type_invariant(&*raw); }
                match self {
                    Lease::Unique(other) => {
                        proof { use_type_invariant(other); }
                        raw.disjoint(other);
                    },
                    Lease::Shared(other) => {
                        proof { use_type_invariant(&**other); other.lemma_contents(); }
                        raw.disjoint(&other.raw);
                    },
                }
            },
            Lease::Shared(_) => {},
        }
    }

    /// Borrow both owners to establish separation without changing either keeper.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    fn disjoint(&mut self, other: &mut Self)
        ensures
            *final(self) == *old(self), *final(other) == *old(other),
            *final(self) is Unique || *final(other) is Unique ==>
                final(self).observation().byte_domain().disjoint(final(other).observation().byte_domain()),
    {
        match (self, other) {
            (Lease::Unique(a), Lease::Unique(b)) => {
                proof { use_type_invariant(&*a); use_type_invariant(&*b); }
                a.disjoint(b);
            },
            (Lease::Unique(a), Lease::Shared(b)) => {
                proof {
                    use_type_invariant(&*a); use_type_invariant(&**b);
                    b.lemma_contents();
                }
                a.disjoint(&b.raw);
            },
            (Lease::Shared(a), Lease::Unique(b)) => {
                proof {
                    use_type_invariant(&**a); use_type_invariant(&*b);
                    a.lemma_contents();
                }
                b.disjoint(&a.raw);
            },
            (Lease::Shared(_), Lease::Shared(_)) => {},
        }
    }
}

/// The exact retained resource as a proof value. Its allocation internals stay private.
#[verifier::reject_recursive_types(N)]
#[allow(dead_code)] // All construction is erased with proof code.
pub ghost struct Resource<N: Nic> {
    resource: Lease<N>,
}

impl<N: Nic> Resource<N> {
    /// Whether this physical keeper owns its bytes exclusively.
    pub closed spec fn unique(self) -> bool { self.resource is Unique }

    /// The byte permission retained by this physical keeper.
    pub closed spec fn byte_domain(self) -> Set<int> {
        match self.resource {
            Lease::Unique(raw) => raw.byte_domain(),
            Lease::Shared(frozen) => (*frozen).byte_domain(),
        }
    }

    /// Backing identity preserves byte domains even after shared guard deduplication.
    pub proof fn lemma_backing_domain(self, other: Self)
        ensures self.same_backing(other) ==>
            self.byte_domain() == other.byte_domain() && self.unique() == other.unique(),
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
    /// The input's metadata and resource, captured before deduplication.
    pub part: Part<N>,
    /// None for standalone, unposted leases.
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
    /// The SGE's bytes are covered by the permission of its retained owner.
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
            r.spec_sge() == (Sge {
                addr: raw.pointer().addr() as u64, length: raw.byte_len() as u32, lkey,
            }),
            r.spec_pd() == pd, r.spec_writable(),
    {
        proof { use_type_invariant(&raw); raw.lemma_domain(); }
        let length = match checked_sge_len(raw.len()) {
            Some(length) => length,
            None => panic!("a single SGE exceeds u32::MAX bytes"),
        };
        Part { sge: Sge { addr: raw.ptr().addr() as u64, length, lkey }, pd, lease: Lease::Unique(raw) }
    }

    /// A shared SGE is a bounded view of the frozen owner's permission.
    #[inline]
    pub(super) fn shared(frozen: Arc<Frozen<N>>, offset: usize, len: usize, lkey: u32, pd: u64) -> (r: Self)
        requires offset <= (*frozen).byte_len(), len <= (*frozen).byte_len() - offset,
            len <= u32::MAX,
        ensures r.inv(), r.resource() == Lease::Shared(frozen).observation(),
            r.spec_sge() == (Sge {
                addr: (frozen.pointer().addr() + offset) as u64, length: len as u32, lkey,
            }),
            r.spec_pd() == pd, !r.spec_writable(),
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

    /// Bridge the SGE footprint to its linear owner, without registration assumptions.
    pub proof fn lemma_byte_backing(self)
        requires self.inv(),
        ensures self.byte_domain().subset_of(self.resource().byte_domain()),
            self.resource().unique() == self.spec_writable(),
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

    /// The resource moved into a request.
    pub closed spec fn resource(self) -> Resource<N> { self.lease.observation() }

    pub(crate) proof fn lemma_fields(self)
        ensures self.spec_sge() == self.sge, self.spec_pd() == self.pd,
            self.spec_writable() <==> self.lease is Unique,
            self.resource() == self.lease.observation(),
    {}

    /// Borrow one keeper without changing the incoming Part or its token.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    pub(crate) fn separate_unique_lease(&mut self, other: &Lease<N>)
        requires old(self).spec_writable(),
        ensures
            *final(self) == *old(self),
            final(self).byte_domain().disjoint(other.observation().byte_domain()),
        no_unwind
    {
        proof { use_type_invariant(&*self); }
        other.separate_unique_input(&mut self.lease);
        proof { self.lemma_byte_backing(); }
    }

    /// A unique Part is separated from any other Part, including shared views.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    pub(crate) fn separate_unique_part(&mut self, other: &Self)
        requires old(self).spec_writable(),
        ensures
            *final(self) == *old(self),
            final(self).byte_domain().disjoint(other.byte_domain()),
        no_unwind
    {
        proof { use_type_invariant(other); other.lemma_byte_backing(); }
        self.separate_unique_lease(&other.lease);
    }

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
    /// The retained resources, in their original order.
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

    /// The input parts, including logically distinct shared borrows.
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

    /// Logical SGE inputs may overlap only when both retain shared owners.
    closed spec fn nonalias(self) -> bool {
        forall|i: int, j: int| #![trigger self.loans()[i], self.loans()[j]]
            0 <= i < j < self.loans().len()
                && (self.loans()[i].part.spec_writable() || self.loans()[j].part.spec_writable())
                ==> self.loans()[i].part.byte_domain().disjoint(self.loans()[j].part.byte_domain())
    }

    /// Every logical input has a retained physical lifetime guard. Incoming local
    /// writes require exclusive resources; shared deduplication changes neither fact.
    pub closed spec fn inv(self) -> bool {
        &&& self.repr_inv()
        &&& self.wqes_ok()
        &&& self.nonalias()
        &&& forall|i: int| 0 <= i < self.loans().len() ==> {
            let loan = #[trigger] self.loans()[i];
            &&& loan.part.inv()
            &&& exists|j: int| 0 <= j < self.view().len()
                && (#[trigger] self.view()[j]).same_backing(loan.part.resource())
            &&& loan.access == Some(Dir::In) ==> loan.part.spec_writable()
        }
    }

    /// The keepers exclude these bytes, and with the invariant so do the loans.
    pub open spec fn separated_from(self, bytes: Set<int>) -> bool {
        &&& forall|i: int| 0 <= i < self.view().len()
            ==> (#[trigger] self.view()[i]).byte_domain().disjoint(bytes)
        &&& self.inv() ==> forall|i: int| 0 <= i < self.loans().len()
            ==> (#[trigger] self.loans()[i]).part.byte_domain().disjoint(bytes)
    }

    /// Traverse only readonly native keepers, mutating just the incoming permission.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    pub(crate) fn separate_unique_input(&self, incoming: &mut Part<N>)
        requires old(incoming).spec_writable(),
        ensures
            *final(incoming) == *old(incoming),
            self.separated_from(final(incoming).byte_domain()),
    {
        if let Some(first) = self.first.as_ref() {
            incoming.separate_unique_lease(first);
        }
        let mut i = 0;
        while i < self.rest.len()
            invariant
                incoming.spec_writable(),
                *incoming == *old(incoming),
                i <= self.rest@.len(),
                self.first is Some ==>
                    self.first->0.observation().byte_domain().disjoint(incoming.byte_domain()),
                forall|j: int| 0 <= j < i ==> (#[trigger] self.rest@[j]).observation()
                    .byte_domain().disjoint(incoming.byte_domain()),
            decreases self.rest@.len() - i,
        {
            incoming.separate_unique_lease(&self.rest[i]);
            i += 1;
        }
        proof {
            assert forall|j: int| 0 <= j < self.view().len()
                implies (#[trigger] self.view()[j]).byte_domain().disjoint(incoming.byte_domain()) by {
                if self.first is Some {
                    if j > 0 { assert(self.raw_view()[j] == self.rest@[j - 1]); }
                } else { assert(self.raw_view()[j] == self.rest@[j]); }
            }
            if self.inv() {
                self.lemma_byte_backing();
                assert forall|j: int| 0 <= j < self.loans().len()
                    implies (#[trigger] self.loans()[j]).part.byte_domain().disjoint(incoming.byte_domain()) by {
                    let part = self.loans()[j].part;
                    let k = choose|k: int| 0 <= k < self.view().len()
                        && self.view()[k].same_backing(part.resource())
                        && part.byte_domain().subset_of(self.view()[k].byte_domain());
                    assert(self.view()[k].byte_domain().disjoint(incoming.byte_domain()));
                }
            }
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

    /// Unique ownership separates each SGE from every other input of the request.
    pub proof fn lemma_disjoint(self)
        requires self.inv(),
        ensures forall|i: int, j: int| #![trigger self.loans()[i], self.loans()[j]]
            0 <= i < self.loans().len() && 0 <= j < self.loans().len() && i != j
                && (self.loans()[i].part.spec_writable() || self.loans()[j].part.spec_writable())
                ==> self.loans()[i].part.byte_domain().disjoint(self.loans()[j].part.byte_domain()),
    {
        assert forall|i: int, j: int| #![trigger self.loans()[i], self.loans()[j]]
            0 <= i < self.loans().len() && 0 <= j < self.loans().len() && i != j
                && (self.loans()[i].part.spec_writable() || self.loans()[j].part.spec_writable())
                implies self.loans()[i].part.byte_domain().disjoint(self.loans()[j].part.byte_domain()) by {
            if j < i {
                assert(self.loans()[j].part.byte_domain().disjoint(self.loans()[i].part.byte_domain()));
            }
        }
    }

    /// Only references are traversed; the inlined native bodies have no effects.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    fn disjoint_incoming(&mut self, incoming: &mut Lease<N>)
        ensures
            final(self).first == old(self).first,
            final(self).rest@ == old(self).rest@,
            final(self).loans() == old(self).loans(),
            final(self).wqes() == old(self).wqes(),
            final(self).raw_view() == old(self).raw_view(),
            final(self).view() == old(self).view(),
            old(self).inv() ==> final(self).inv(),
            *final(incoming) == *old(incoming),
            forall|i: int| 0 <= i < final(self).raw_view().len() ==> {
                let kept = #[trigger] final(self).raw_view()[i];
                kept is Unique || *final(incoming) is Unique ==>
                    kept.observation().byte_domain().disjoint(final(incoming).observation().byte_domain())
            },
    {
        let ghost before = *self;
        if let Some(first) = self.first.as_mut() {
            first.disjoint(incoming);
        }
        let mut i = 0;
        while i < self.rest.len()
            invariant
                self.first == before.first,
                self.rest@ == before.rest@,
                self.loans() == before.loans(),
                self.wqes() == before.wqes(),
                *incoming == *old(incoming),
                i <= self.rest@.len(),
                self.first is Some ==> (
                    self.first->0 is Unique || *incoming is Unique ==>
                        self.first->0.observation().byte_domain().disjoint(incoming.observation().byte_domain())
                ),
                forall|j: int| 0 <= j < i ==> {
                    let kept = #[trigger] self.rest@[j];
                    kept is Unique || *incoming is Unique ==>
                        kept.observation().byte_domain().disjoint(incoming.observation().byte_domain())
                },
            decreases self.rest@.len() - i,
        {
            self.rest[i].disjoint(incoming);
            i += 1;
        }
        proof {
            assert(self.raw_view() == before.raw_view());
            assert(self.view() == before.view());
            if before.inv() {
                assert forall|j: int| 0 <= j < self.loans().len() implies {
                    let loan = #[trigger] self.loans()[j];
                    &&& loan.part.inv()
                    &&& exists|k: int| 0 <= k < self.view().len()
                        && (#[trigger] self.view()[k]).same_backing(loan.part.resource())
                    &&& loan.access == Some(Dir::In) ==> loan.part.spec_writable()
                } by {
                    let k = choose|k: int| 0 <= k < before.view().len()
                        && before.view()[k].same_backing(before.loans()[j].part.resource());
                    assert(self.loans()[j] == before.loans()[j]);
                    assert(self.view()[k] == before.view()[k]);
                }
                assert(self.inv());
            }
            assert forall|j: int| 0 <= j < self.raw_view().len() implies {
                let kept = #[trigger] self.raw_view()[j];
                kept is Unique || *incoming is Unique ==>
                    kept.observation().byte_domain().disjoint(incoming.observation().byte_domain())
            } by {
                if self.first is Some {
                    if j > 0 { assert(self.raw_view()[j] == self.rest@[j - 1]); }
                } else { assert(self.raw_view()[j] == self.rest@[j]); }
            }
        }
    }

    /// Transfer the freshly checked keeper separation to the old logical inputs.
    proof fn lemma_incoming(self, incoming: Part<N>)
        requires
            self.inv(), incoming.inv(),
            forall|i: int| 0 <= i < self.raw_view().len() ==> {
                let kept = #[trigger] self.raw_view()[i];
                kept is Unique || incoming.spec_writable() ==>
                    kept.observation().byte_domain().disjoint(incoming.resource().byte_domain())
            },
        ensures forall|i: int| 0 <= i < self.loans().len()
            && ((#[trigger] self.loans()[i]).part.spec_writable() || incoming.spec_writable())
            ==> self.loans()[i].part.byte_domain().disjoint(incoming.byte_domain()),
    {
        self.lemma_loans();
        incoming.lemma_byte_backing();
        assert forall|i: int| 0 <= i < self.loans().len()
            && ((#[trigger] self.loans()[i]).part.spec_writable() || incoming.spec_writable())
            implies self.loans()[i].part.byte_domain().disjoint(incoming.byte_domain()) by {
            let part = self.loans()[i].part;
            let j = choose|j: int| 0 <= j < self.view().len()
                && self.view()[j].same_backing(part.resource());
            part.lemma_byte_backing();
            self.view()[j].lemma_backing_domain(part.resource());
            assert(self.view()[j] == self.raw_view()[j].observation());
            assert(self.raw_view()[j] is Unique || incoming.spec_writable());
            assert(self.view()[j].byte_domain().disjoint(incoming.resource().byte_domain()));
        }
    }

    /// Every recorded SGE is covered by a retained raw-byte permission.
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

    /// No leases, with no allocation.
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

    /// Unposted leases have no selected operation yet.
    #[inline]
    pub(crate) fn push(&mut self, part: Part<N>)
        ensures old(self).inv() ==> final(self).inv(),
            final(self).loans() == old(self).loans().push(Loan { part, access: None }),
            final(self).wqes() == old(self).wqes(),
    { self.push_loan(part, Ghost(None)); }

    /// Record the operation's access without storing it at run time.
    #[inline]
    pub(crate) fn push_access(&mut self, part: Part<N>, Ghost(access): Ghost<Dir>)
        requires access is In ==> part.spec_writable(),
        ensures old(self).inv() ==> final(self).inv(),
            final(self).loans() == old(self).loans().push(Loan { part, access: Some(access) }),
            final(self).wqes() == old(self).wqes(),
    { self.push_loan(part, Ghost(Some(access))); }

    #[inline]
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
        let lease = part.lease;
        #[cfg(verus_keep_ghost)]
        let lease = {
            let mut lease = lease;
            self.disjoint_incoming(&mut lease);
            lease
        };
        proof { if before.inv() { self.lemma_incoming(incoming); } }
        self.push_resource(lease);
        self.loans = Ghost(before.loans().push(Loan { part: incoming, access }));
        proof {
            if before.inv() {
                assert(self.repr_inv());
                assert forall|i: int, j: int| #![trigger self.loans()[i], self.loans()[j]]
                    0 <= i < j < self.loans().len()
                        && (self.loans()[i].part.spec_writable() || self.loans()[j].part.spec_writable())
                        implies self.loans()[i].part.byte_domain().disjoint(self.loans()[j].part.byte_domain()) by {
                    if j < before.loans().len() {
                        assert(self.loans()[i] == before.loans()[i]);
                        assert(self.loans()[j] == before.loans()[j]);
                    } else {
                        assert(self.loans()[j].part == incoming);
                        assert(self.loans()[i] == before.loans()[i]);
                    }
                }
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

    /// Whether it holds no lease.
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
    fn split_unique_and_overlapping_shared_inputs_retain_their_owners() {
        let model = Model::new(108);
        let pd = Context::new(model.clone()).alloc_pd().unwrap();
        let mut allocation = pd.alloc::<u8>(64).unwrap();
        for (i, value) in allocation.iter_mut().enumerate() {
            *value = i as u8;
        }
        let (unique, shared) = allocation.split_at(16);
        let address = unique.as_ptr();
        let shared = shared.freeze();
        let mut leases = Leases::new();
        leases.push(shared.slice(..24).into_part());
        leases.push(shared.slice(8..32).into_part());
        leases.push(unique.into_part());
        leases.push(shared.slice(16..40).into_part());
        leases.push(shared.slice(24..48).into_part());
        assert_eq!(leases.len(), 3);
        let unique = leases.into_buf_mut::<u8>().unwrap();
        assert_eq!(unique.as_ptr(), address);
        assert_eq!(&unique[..], &(0..16u8).collect::<Vec<_>>()[..]);
        let shared = shared.try_thaw().unwrap();
        assert_eq!(&shared[..], &(16..64u8).collect::<Vec<_>>()[..]);
        drop(unique);
        assert_eq!(model.registered(), 1);
        drop(shared);
        assert_eq!(model.registered(), 0);
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
