//! What keeps a request's buffers alive, verified independently of the allocation
//! boundary. Shared deduplication preserves the retained frozen allocation; unique
//! resources move unchanged into and out of the container.
//!
//! The traversals that prove inputs apart (`Leases::disjoint_incoming` and the engine's
//! `separate_lease`) only borrow linear permissions and change nothing, so they exist in Verus
//! builds alone (`cfg(verus_keep_ghost)`); plain builds have no such code.
//
// The code keeps the plain forms the proofs reason about (explicit matches, a `Vec`'s view).
#![allow(clippy::single_match, clippy::ptr_arg)]

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

    /// An incoming buffer and a keeper this borrows, as linear permissions say: the first exclusive
    /// borrow is the incoming one's, if it is unique.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    pub(crate) fn separate_input(&self, incoming: &mut Self)
        ensures
            *final(incoming) == *old(incoming),
            self.observation().apart(final(incoming).observation()),
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
            Lease::Shared(frozen) => {
                frozen_apart(&*frozen, self);
                proof { Lease::Shared(*frozen).observation().lemma_apart_symmetric(self.observation()); }
            },
        }
    }

    /// A buffer and a frozen allocation: the buffer's permission is the exclusive borrow if it is
    /// unique.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    fn separate_frozen(&mut self, other: &Arc<Frozen<N>>)
        ensures
            *final(self) == *old(self),
            Lease::Shared(*other).observation().apart(final(self).observation()),
        no_unwind
    {
        match self {
            Lease::Unique(raw) => {
                proof {
                    use_type_invariant(&*raw);
                    use_type_invariant(&**other);
                    other.lemma_contents();
                }
                raw.disjoint(&other.raw);
            },
            Lease::Shared(_) => frozen_apart(other, &*self),
        }
    }

    /// Borrow both owners to establish separation without changing either keeper.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    fn disjoint(&mut self, other: &mut Self)
        ensures
            *final(self) == *old(self), *final(other) == *old(other),
            final(self).observation().apart(final(other).observation()),
        no_unwind
    {
        match self {
            Lease::Unique(a) => {
                proof { use_type_invariant(&*a); }
                match other {
                    Lease::Unique(b) => {
                        proof { use_type_invariant(&*b); }
                        a.disjoint(b);
                    },
                    Lease::Shared(b) => {
                        proof { use_type_invariant(&**b); b.lemma_contents(); }
                        a.disjoint(&b.raw);
                    },
                }
            },
            Lease::Shared(a) => {
                other.separate_frozen(&*a);
            },
        }
    }
}

/// Two real buffers own different bytes, unless they are one frozen allocation: a byte has one
/// linear permission, and a frozen allocation holds its own. vstd's `is_disjoint` says so with one
/// side exclusive; a frozen allocation is never exclusive, so here both sides may be shared.
#[cfg(verus_keep_ghost)]
#[verifier::external_body]
#[inline(always)]
pub(crate) fn frozen_apart<N: Nic>(frozen: &Arc<Frozen<N>>, other: &Lease<N>)
    ensures
        Lease::Shared(*frozen).observation().apart(other.observation()),
    no_unwind
{
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

    /// Two keepers own different bytes, unless they are one frozen allocation.
    pub open spec fn apart(self, other: Self) -> bool {
        self.same_frozen(other) || self.byte_domain().disjoint(other.byte_domain())
    }

    /// `apart` goes both ways.
    pub proof fn lemma_apart_symmetric(self, other: Self)
        ensures
            self.apart(other) == other.apart(self),
    {
        assert(self.same_frozen(other) == other.same_frozen(self));
        assert(self.byte_domain().disjoint(other.byte_domain()) == other.byte_domain().disjoint(self.byte_domain()));
    }

    /// One frozen allocation is shared, with the same bytes.
    pub proof fn lemma_same_frozen(self, other: Self)
        requires
            self.same_frozen(other),
        ensures
            !self.unique(),
            !other.unique(),
            self.byte_domain() == other.byte_domain(),
    {
    }

    /// What is apart from a keeper is apart from what it keeps.
    pub proof fn lemma_apart_backing(self, keeper: Self, kept: Self)
        requires
            self.apart(keeper),
            keeper.same_backing(kept),
        ensures
            self.apart(kept),
    {
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
pub(crate) fn same_frozen_arc<N: Nic>(a: &Arc<Frozen<N>>, b: &Arc<Frozen<N>>) -> (same: bool)
    ensures same ==> **a == **b,
{
    Arc::ptr_eq(a, b)
}

/// Releases what it is given.
#[inline]
fn release<T>(_value: T) {
}

/// `core::mem::replace`, which vstd does not specify.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn replace_value<T>(dest: &mut T, value: T) -> (before: T)
    ensures
        before == *old(dest),
        *final(dest) == value,
{
    core::mem::replace(dest, value)
}

/// `core::mem::forget`, which vstd does not specify, of what the caller proved holds nothing
/// (`nothing`): its destructor has nothing to do, and no landing pad comes with it.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn forget_free<T>(free: T, Ghost(nothing): Ghost<bool>)
    requires
        nothing,
{
    core::mem::forget(free);
}

/// `Vec::capacity`, which vstd does not specify.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn capacity_of<T>(v: &Vec<T>) -> (capacity: usize)
    ensures
        v@.len() <= capacity,
{
    v.capacity()
}

/// `<[T]>::swap` of a `Vec`, which vstd does not specify.
#[verifier::external_body]
fn swap_in<T>(v: &mut Vec<T>, a: usize, b: usize)
    requires
        a < old(v)@.len(),
        b < old(v)@.len(),
    ensures
        final(v)@ == old(v)@.update(a as int, old(v)@[b as int]).update(b as int, old(v)@[a as int]),
{
    v.swap(a, b);
}

/// A buffer on loan to the NIC: one SGE and the resource keeping it alive.
#[verifier::reject_recursive_types(N)]
pub struct Part<N: Nic = Verbs> {
    pub(crate) sge: Sge,
    pub(crate) pd: u64,
    pub(crate) lease: Lease<N>,
}

impl<N: Nic> Part<N> {
    /// The frozen allocation that keeps it alive, if it is one.
    #[inline(always)]
    pub(crate) fn frozen_of(&self) -> (frozen: Option<&Arc<Frozen<N>>>)
        ensures
            match frozen {
                Some(f) => self.lease == Lease::Shared(*f),
                None => self.lease is Unique,
            },
    {
        match &self.lease {
            Lease::Shared(frozen) => Some(frozen),
            _ => None,
        }
    }

    /// What keeps it alive: a frozen allocation, or another resource.
    #[inline(always)]
    pub(crate) fn into_frozen(self) -> (r: core::result::Result<Arc<Frozen<N>>, Lease<N>>)
        ensures
            match r {
                Ok(f) => self.lease == Lease::Shared(f),
                Err(lease) => lease == self.lease && self.lease is Unique,
            },
    {
        match self.lease {
            Lease::Shared(frozen) => Ok(frozen),
            other => Err(other),
        }
    }

    /// What keeps it alive is apart from everything the engine holds, as linear permissions say.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    pub(crate) fn separate_from_table<C>(self, hw: &crate::hw::Hw<C, N>) -> (same: Self)
        requires
            hw.inv(),
        ensures
            same == self,
            hw.table_apart(self.resource()),
    {
        proof { use_type_invariant(&self); }
        let Part { sge, pd, lease } = self;
        let mut lease = lease;
        hw.separate_lease(&mut lease);
        Part { sge, pd, lease }
    }

    /// **S1 of a buffer on loan to nobody yet.** While the program holds it, the NIC writes none of
    /// its bytes, and if it is unique reads none (`Hw::keeper_apart`).
    #[cfg(verus_keep_ghost)]
    pub(crate) fn apart_from_nic<C>(self, hw: &crate::hw::Hw<C, N>) -> (same: Self)
        requires
            hw.inv(),
        ensures
            same == self,
            hw.outside_ok() ==> forall|x: int| #[trigger] self.byte_domain().contains(x) ==> {
                &&& !hw.nic().nic_may_write(x)
                &&& self.spec_writable() ==> !hw.nic().nic_may_read(x)
            },
    {
        proof {
            use_type_invariant(&self);
            self.lemma_byte_backing();
            assert forall|x: int| #[trigger] self.byte_domain().contains(x)
                implies self.lease.observation().byte_domain().contains(x) by {}
        }
        let Part { sge, pd, lease } = self;
        let mut lease = lease;
        hw.keeper_apart(&mut lease);
        Part { sge, pd, lease }
    }

    /// What keeps it alive and a frozen allocation are apart.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    pub(crate) fn separate_frozen(&mut self, other: &Arc<Frozen<N>>)
        ensures
            *final(self) == *old(self),
            Lease::Shared(*other).observation().apart(final(self).resource()),
        no_unwind
    {
        proof { use_type_invariant(&*self); }
        self.lease.separate_frozen(other);
    }

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
            None => {
                // Released first, so that no buffer stays alive across the panic: the `Part` is
                // then built in place.
                release(raw);
                panic!("a single SGE exceeds u32::MAX bytes")
            },
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
            self.spec_writable() ==> self.byte_domain() == self.resource().byte_domain(),
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

/// The resources of one request: one, as most requests hold, inline. Repeated consecutive
/// references to the same frozen allocation are retained once.
#[verifier::reject_recursive_types(N)]
pub struct Leases<N: Nic = Verbs> {
    held: Held<N>,
    #[allow(dead_code)]
    loans: Ghost<Seq<Loan<N>>>,
    #[allow(dead_code)]
    wqes: Ghost<Seq<WqeLoans>>,
}

/// What a request's [`Leases`] hold.
#[verifier::reject_recursive_types(N)]
enum Held<N: Nic> {
    /// Nothing.
    Nothing,
    /// One resource.
    One(Lease<N>),
    /// Several, in their order.
    Many(Vec<Lease<N>>),
}

/// Logical SGE inputs may overlap only when both retain shared owners.
spec fn nonalias<N: Nic>(loans: Seq<Loan<N>>) -> bool {
    forall|i: int, j: int| #![trigger loans[i], loans[j]]
        0 <= i < j < loans.len() && (loans[i].part.spec_writable() || loans[j].part.spec_writable())
            ==> loans[i].part.byte_domain().disjoint(loans[j].part.byte_domain())
}

/// What keeps logical inputs alive is apart, input by input.
spec fn loans_apart<N: Nic>(loans: Seq<Loan<N>>) -> bool {
    forall|i: int, j: int| #![trigger loans[i], loans[j]]
        0 <= i < j < loans.len() ==> loans[i].part.resource().apart(loans[j].part.resource())
}

/// What [`Leases::push_resource`] does to a list of keepers: it takes over, and it keeps `r`.
proof fn lemma_pushed<N: Nic>(before: Seq<Resource<N>>, r: Resource<N>, after: Seq<Resource<N>>)
    requires
        after == before.push(r) || (before.len() > 0 && before.last().same_frozen(r) && after == before),
    ensures
        forall|x: Resource<N>| #[trigger] keeps(before, x) ==> keeps(after, x),
        keeps(after, r),
{
    if after == before.push(r) {
        assert forall|j: int| 0 <= j < before.len() implies keeps(after, #[trigger] before[j]) by {
            assert(after[j] == before[j]);
            lemma_keeps_element(after, before[j], j);
        }
        lemma_covers(before, after);
        lemma_keeps_element(after, r, before.len() as int);
    } else {
        assert forall|x: Resource<N>| #[trigger] keeps(before, x) implies keeps(after, x) by {}
        assert(after[before.len() - 1].same_backing(r));
    }
}

/// Whether a lease is of a unique resource.
spec fn is_unique<N: Nic>() -> spec_fn(Lease<N>) -> bool {
    |lease: Lease<N>| lease is Unique
}

/// The filter of one more element.
proof fn lemma_filter_step<T>(s: Seq<T>, i: int, test: spec_fn(T) -> bool)
    requires
        0 <= i < s.len(),
    ensures
        s.take(i + 1).filter(test) == if test(s[i]) {
            s.take(i).filter(test).push(s[i])
        } else {
            s.take(i).filter(test)
        },
{
    reveal(Seq::filter);
    let t = s.take(i + 1);
    assert(t.drop_last() =~= s.take(i));
    assert(t.last() == s[i]);
}

/// Keepers that keep what `before` and `r` kept keep what `before`, with `r` pushed, keeps.
proof fn lemma_pushed_covered<N: Nic>(before: Seq<Resource<N>>, r: Resource<N>, after: Seq<Resource<N>>)
    requires
        forall|x: Resource<N>| #[trigger] keeps(before, x) ==> keeps(after, x),
        keeps(after, r),
    ensures
        forall|x: Resource<N>| #[trigger] keeps(before.push(r), x) ==> keeps(after, x),
{
    assert forall|x: Resource<N>| #[trigger] keeps(before.push(r), x) implies keeps(after, x) by {
        let j = choose|j: int| 0 <= j < before.push(r).len() && (#[trigger] before.push(r)[j]).same_backing(x);
        if j < before.len() {
            assert(before[j] == before.push(r)[j]);
            assert(keeps(before, x));
        } else {
            assert(before.push(r)[j] == r);
            let k = choose|k: int| 0 <= k < after.len() && (#[trigger] after[k]).same_backing(r);
            assert(after[k].same_backing(x));
        }
    }
}

/// A list all of whose elements pass a test is its own filter.
proof fn lemma_filter_all<T>(s: Seq<T>, test: spec_fn(T) -> bool)
    requires
        forall|i: int| 0 <= i < s.len() ==> test(#[trigger] s[i]),
    ensures
        s.filter(test) == s,
    decreases s.len(),
{
    reveal(Seq::filter);
    if s.len() > 0 {
        lemma_filter_all(s.drop_last(), test);
        assert(s.drop_last().push(s.last()) =~= s);
    } else {
        assert(s.filter(test) =~= s);
    }
}

/// One of `keepers` keeps resource `r` alive.
spec fn keeps<N: Nic>(keepers: Seq<Resource<N>>, r: Resource<N>) -> bool {
    exists|j: int| 0 <= j < keepers.len() && (#[trigger] keepers[j]).same_backing(r)
}

/// Every logical input has a retained physical lifetime guard. Incoming local
/// writes require exclusive resources; shared deduplication changes neither fact.
spec fn loans_kept<N: Nic>(loans: Seq<Loan<N>>, keepers: Seq<Resource<N>>) -> bool {
    &&& nonalias(loans)
    &&& loans_apart(loans)
    &&& forall|i: int| 0 <= i < loans.len() ==> {
        let loan = #[trigger] loans[i];
        &&& loan.part.inv()
        &&& keeps(keepers, loan.part.resource())
        &&& loan.access == Some(Dir::In) ==> loan.part.spec_writable()
    }
}

/// Keepers that keep what the ones before kept take over from them.
proof fn lemma_covers<N: Nic>(before: Seq<Resource<N>>, after: Seq<Resource<N>>)
    requires
        forall|j: int| 0 <= j < before.len() ==> keeps(after, #[trigger] before[j]),
    ensures
        forall|r: Resource<N>| #[trigger] keeps(before, r) ==> keeps(after, r),
{
    assert forall|r: Resource<N>| #[trigger] keeps(before, r) implies keeps(after, r) by {
        let j = choose|j: int| 0 <= j < before.len() && (#[trigger] before[j]).same_backing(r);
        assert(keeps(after, before[j]));
        let k = choose|k: int| 0 <= k < after.len() && (#[trigger] after[k]).same_backing(before[j]);
        assert(after[k].same_backing(r));
    }
}

/// More keepers keep what they kept.
proof fn lemma_keeps_more<N: Nic>(before: Seq<Resource<N>>, r: Resource<N>)
    ensures
        forall|x: Resource<N>| #[trigger] keeps(before, x) ==> keeps(before.push(r), x),
{
    assert forall|x: Resource<N>| #[trigger] keeps(before, x) implies keeps(before.push(r), x) by {
        let j = choose|j: int| 0 <= j < before.len() && (#[trigger] before[j]).same_backing(x);
        assert(before.push(r)[j] == before[j]);
    }
}

/// A resource is its own backing, so a list keeps what it holds.
proof fn lemma_keeps_element<N: Nic>(keepers: Seq<Resource<N>>, r: Resource<N>, j: int)
    requires
        0 <= j < keepers.len(),
        keepers[j] == r,
    ensures
        keeps(keepers, r),
{
    assert(keepers[j].same_backing(r));
}

/// An input apart from the keepers, as far as uniqueness asks, with a keeper of its own among
/// those that take over, joins the inputs.
proof fn lemma_loan_added<N: Nic>(
    loans: Seq<Loan<N>>, keepers: Seq<Resource<N>>, part: Part<N>, access: Option<Dir>,
    post: Seq<Resource<N>>,
)
    requires
        part.inv(),
        access == Some(Dir::In) ==> part.spec_writable(),
        loans_kept(loans, keepers),
        forall|i: int| 0 <= i < keepers.len() ==> (#[trigger] keepers[i]).apart(part.resource()),
        forall|r: Resource<N>| #[trigger] keeps(keepers, r) ==> keeps(post, r),
        keeps(post, part.resource()),
    ensures
        loans_kept(loans.push(Loan { part, access }), post),
{
    let next = loans.push(Loan { part, access });
    part.lemma_byte_backing();
    assert forall|i: int, j: int| #![trigger next[i], next[j]]
        0 <= i < j < next.len() && (next[i].part.spec_writable() || next[j].part.spec_writable())
            implies next[i].part.byte_domain().disjoint(next[j].part.byte_domain()) by {
        assert(next[i] == loans[i]);
        if j < loans.len() {
            assert(next[j] == loans[j]);
        } else {
            let loan = loans[i];
            assert(next[j].part == part);
            assert(keeps(keepers, loan.part.resource()));
            let k = choose|k: int| 0 <= k < keepers.len() && (#[trigger] keepers[k]).same_backing(loan.part.resource());
            lemma_keeper_bytes(loan, keepers[k]);
            assert(keepers[k].apart(part.resource()));
            if keepers[k].same_frozen(part.resource()) {
                keepers[k].lemma_same_frozen(part.resource());
            }
            assert(keepers[k].byte_domain().disjoint(part.resource().byte_domain()));
        }
    }
    assert forall|i: int, j: int| #![trigger next[i], next[j]]
        0 <= i < j < next.len() implies next[i].part.resource().apart(next[j].part.resource()) by {
        assert(next[i] == loans[i]);
        if j < loans.len() {
            assert(next[j] == loans[j]);
        } else {
            let loan = loans[i];
            assert(next[j].part == part);
            assert(keeps(keepers, loan.part.resource()));
            let k = choose|k: int| 0 <= k < keepers.len() && (#[trigger] keepers[k]).same_backing(loan.part.resource());
            assert(keepers[k].apart(part.resource()));
            keepers[k].lemma_apart_symmetric(part.resource());
            part.resource().lemma_apart_backing(keepers[k], loan.part.resource());
            part.resource().lemma_apart_symmetric(loan.part.resource());
        }
    }
    assert forall|i: int| 0 <= i < next.len() implies {
        let loan = #[trigger] next[i];
        &&& loan.part.inv()
        &&& keeps(post, loan.part.resource())
        &&& loan.access == Some(Dir::In) ==> loan.part.spec_writable()
    } by {
        if i < loans.len() {
            assert(next[i] == loans[i]);
            assert(keeps(keepers, loans[i].part.resource()));
        }
    }
}

/// Backing the same resource as a loan keeps the loan's bytes, and uniqueness.
proof fn lemma_keeper_bytes<N: Nic>(loan: Loan<N>, keeper: Resource<N>)
    requires
        loan.part.inv(),
        keeper.same_backing(loan.part.resource()),
    ensures
        loan.part.byte_domain().subset_of(keeper.byte_domain()),
        keeper.unique() == loan.part.spec_writable(),
{
    loan.part.lemma_byte_backing();
    keeper.lemma_backing_domain(loan.part.resource());
}

impl<N: Nic> Leases<N> {
    /// The retained resources, in their original order.
    pub(super) closed spec fn raw_view(self) -> Seq<Lease<N>> {
        match self.held {
            Held::Nothing => Seq::empty(),
            Held::One(lease) => seq![lease],
            Held::Many(rest) => rest@,
        }
    }

    /// The exact ordered resources retained by this request.
    pub closed spec fn view(self) -> Seq<Resource<N>> {
        self.raw_view().map_values(|lease: Lease<N>| lease.observation())
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

    /// Every logical input has a retained physical lifetime guard, and its WQE's footprint is
    /// where it says.
    pub closed spec fn inv(self) -> bool {
        &&& self.wqes_ok()
        &&& loans_kept(self.loans(), self.view())
    }

    /// Every keeper is apart from `incoming`.
    pub open spec fn apart_from(self, incoming: Resource<N>) -> bool {
        forall|i: int| 0 <= i < self.view().len() ==> (#[trigger] self.view()[i]).apart(incoming)
    }

    /// What is apart from a keeper is apart from what it keeps.
    pub proof fn lemma_apart_backing(self, keeper: Resource<N>, kept: Resource<N>)
        requires
            self.apart_from(keeper),
            keeper.same_backing(kept),
        ensures
            self.apart_from(kept),
    {
        assert forall|i: int| 0 <= i < self.view().len() implies (#[trigger] self.view()[i]).apart(kept) by {
            self.view()[i].lemma_apart_backing(keeper, kept);
        }
    }

    /// Traverse only readonly native keepers, mutating just the incoming permission.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    pub(crate) fn separate_lease(&self, incoming: &mut Lease<N>)
        ensures
            *final(incoming) == *old(incoming),
            self.apart_from(final(incoming).observation()),
    {
        match &self.held {
            Held::Nothing => {},
            Held::One(first) => first.separate_input(incoming),
            Held::Many(rest) => {
                let mut i = 0;
                while i < rest.len()
                    invariant
                        *incoming == *old(incoming),
                        i <= rest@.len(),
                        forall|j: int| 0 <= j < i ==> (#[trigger] rest@[j]).observation().apart(incoming.observation()),
                    decreases rest@.len() - i,
                {
                    rest[i].separate_input(incoming);
                    i += 1;
                }
            },
        }
        proof {
            assert forall|j: int| 0 <= j < self.view().len()
                implies (#[trigger] self.view()[j]).apart(incoming.observation()) by {
                assert(self.view()[j] == self.raw_view()[j].observation());
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
    {
        assert forall|i: int| 0 <= i < self.loans().len() implies {
            let loan = #[trigger] self.loans()[i];
            exists|j: int| 0 <= j < self.view().len()
                && (#[trigger] self.view()[j]).same_backing(loan.part.resource())
        } by {
            assert(keeps(self.view(), self.loans()[i].part.resource()));
        }
    }

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

    /// What keeps its inputs alive is apart, input by input, and each is kept here.
    pub proof fn lemma_apart(self)
        requires self.inv(),
        ensures
            forall|i: int, j: int| #![trigger self.loans()[i], self.loans()[j]]
                0 <= i < self.loans().len() && 0 <= j < self.loans().len() && i != j
                    ==> self.loans()[i].part.resource().apart(self.loans()[j].part.resource()),
    {
        assert forall|i: int, j: int| #![trigger self.loans()[i], self.loans()[j]]
            0 <= i < self.loans().len() && 0 <= j < self.loans().len() && i != j
                implies self.loans()[i].part.resource().apart(self.loans()[j].part.resource()) by {
            if j < i {
                assert(self.loans()[j].part.resource().apart(self.loans()[i].part.resource()));
                self.loans()[j].part.resource().lemma_apart_symmetric(self.loans()[i].part.resource());
            }
        }
    }

    /// They hold the same, in the same way.
    pub(crate) closed spec fn same_contents(self, other: Self) -> bool {
        &&& self.raw_view() == other.raw_view()
        &&& self.loans == other.loans
        &&& self.wqes == other.wqes
        &&& self.held is Nothing <==> other.held is Nothing
        &&& self.held is One <==> other.held is One
        &&& self.held is Many <==> other.held is Many
    }

    /// Only references are traversed; the inlined native bodies have no effects.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    fn disjoint_incoming(&mut self, incoming: &mut Lease<N>)
        ensures
            final(self).same_contents(*old(self)),
            *final(incoming) == *old(incoming),
            forall|i: int| 0 <= i < final(self).raw_view().len()
                ==> (#[trigger] final(self).raw_view()[i]).observation().apart(final(incoming).observation()),
        no_unwind
    {
        let ghost before = *self;
        match &mut self.held {
            Held::Nothing => {},
            Held::One(first) => first.disjoint(incoming),
            Held::Many(rest) => {
                let ghost original = rest@;
                let mut i = 0;
                while i < rest.len()
                    invariant
                        rest@ == original,
                        *incoming == *old(incoming),
                        i <= rest@.len(),
                        forall|j: int| 0 <= j < i ==> (#[trigger] rest@[j]).observation().apart(incoming.observation()),
                    decreases rest@.len() - i,
                {
                    let ghost now = rest@;
                    rest[i].disjoint(incoming);
                    proof {
                        assert(rest@ =~= now);
                    }
                    i += 1;
                }
            },
        }
    }

    /// The keepers of `self` and `incoming` are apart, as far as linear permissions say: its
    /// resources change nothing.
    #[cfg(verus_keep_ghost)]
    #[inline(always)]
    pub(crate) fn disjoint_part(&mut self, incoming: &mut Part<N>)
        ensures
            final(self).same_contents(*old(self)),
            final(self).view() == old(self).view(),
            final(self).loans() == old(self).loans(),
            final(self).wqes() == old(self).wqes(),
            *final(incoming) == *old(incoming),
            final(self).apart_from(final(incoming).resource()),
        no_unwind
    {
        proof { use_type_invariant(&*incoming); }
        self.disjoint_incoming(&mut incoming.lease);
        proof {
            assert forall|i: int| 0 <= i < self.view().len()
                implies (#[trigger] self.view()[i]).apart(incoming.resource()) by {
                assert(self.view()[i] == self.raw_view()[i].observation());
            }
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
            lemma_keeper_bytes(loan, self.view()[j]);
        }
    }

    /// No leases, with no allocation.
    #[inline(always)]
    pub fn new() -> (r: Self)
        ensures r.inv(), r.view().len() == 0, r.loans().len() == 0, r.wqes().len() == 0,
            r.fully_recorded(), !r.spec_many(),
    { Leases { held: Held::Nothing, loans: Ghost(Seq::empty()), wqes: Ghost(Seq::empty()) } }

    /// The lease of one input part, as a request of one WQE holds it, with no allocation.
    #[inline(always)]
    pub(crate) fn one(part: Part<N>, Ghost(access): Ghost<Dir>) -> (r: Self)
        requires
            access is In ==> part.spec_writable(),
        ensures
            r.inv(),
            r.loans() == seq![Loan { part, access: Some(access) }],
            r.wqes().len() == 0,
            !r.spec_many(),
    {
        proof { use_type_invariant(&part); }
        let ghost loans = seq![Loan { part, access: Some(access) }];
        let r = Leases {
            held: Held::One(part.lease),
            loans: Ghost(loans),
            wqes: Ghost(Seq::empty()),
        };
        proof {
            assert(r.view()[0] == r.raw_view()[0].observation());
            assert(r.view()[0].same_backing(loans[0].part.resource()));
            assert(nonalias(r.loans()));
            assert forall|i: int| 0 <= i < r.loans().len() implies {
                let loan = #[trigger] r.loans()[i];
                &&& loan.part.inv()
                &&& keeps(r.view(), loan.part.resource())
                &&& loan.access == Some(Dir::In) ==> loan.part.spec_writable()
            } by {
                assert(r.loans()[i] == r.loans()[0]);
            }
        }
        r
    }

    /// Adds provenance only after the checked collection loop established its footprint.
    #[inline]
    pub(crate) fn record_wqe(&mut self, Ghost(wqe): Ghost<WqeLoans>)
        requires old(self).inv(), wqe.matches(old(self).loans()),
            forall|i: int| 0 <= i < old(self).wqes().len()
                ==> (#[trigger] old(self).wqes()[i]).end() <= wqe.first,
        ensures final(self).inv(), final(self).loans() == old(self).loans(),
            final(self).view() == old(self).view(),
            final(self).spec_many() == old(self).spec_many(),
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
            assert(self.view() == before.view());
            assert(self.loans() == before.loans());
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

    /// Retains an incoming resource or drops only a redundant shared reference. A list of several
    /// starts from `spare`, which keeps what it took to hold such a list before.
    #[allow(clippy::collapsible_if)] // Keep the pattern and pointer primitive explicit for Verus.
    #[inline(always)]
    fn push_resource(&mut self, lease: Lease<N>, spare: &mut Vec<Lease<N>>)
        requires
            old(spare)@.len() == 0,
        ensures
            final(self).loans() == old(self).loans(),
            final(self).wqes() == old(self).wqes(),
            final(self).view() == old(self).view().push(lease.observation())
                || (old(self).view().len() > 0
                    && old(self).view().last().same_frozen(lease.observation())
                    && final(self).view() == old(self).view()),
            final(spare)@.len() == 0,
    {
        let last = match &self.held {
            Held::Nothing => None,
            Held::One(first) => Some(first),
            Held::Many(rest) => rest.last(),
        };
        if let (Some(Lease::Shared(a)), Lease::Shared(b)) = (last, &lease) {
            if same_frozen_arc(a, b) {
                proof {
                    assert(self.view().last() == self.raw_view().last().observation());
                }
                return;
            }
        }
        if matches!(self.held, Held::Nothing) {
            let ghost before = *self;
            self.held = Held::One(lease);
            proof {
                assert(self.raw_view() =~= before.raw_view().push(lease));
                assert(self.view() =~= before.view().push(lease.observation()));
            }
        } else {
            let ghost before = *self;
            self.push_rest(lease, spare);
            proof {
                assert(self.view() =~= before.view().push(lease.observation()));
            }
        }
    }

    /// Retains a resource after the first one.
    #[inline(never)]
    fn push_rest(&mut self, lease: Lease<N>, spare: &mut Vec<Lease<N>>)
        requires
            old(spare)@.len() == 0,
        ensures
            final(self).raw_view() == old(self).raw_view().push(lease),
            final(self).loans == old(self).loans,
            final(self).wqes == old(self).wqes,
            final(spare)@.len() == 0,
    {
        let ghost before = *self;
        match &mut self.held {
            Held::Many(rest) => rest.push(lease),
            held => {
                let mut rest = replace_value(spare, Vec::new());
                if let Held::One(first) = replace_value(held, Held::Nothing) {
                    rest.push(first);
                }
                rest.push(lease);
                *held = Held::Many(rest);
            },
        }
        proof {
            assert(self.raw_view() =~= before.raw_view().push(lease));
        }
    }

    /// Unposted leases have no selected operation yet.
    #[inline]
    pub(crate) fn push(&mut self, part: Part<N>)
        ensures old(self).inv() ==> final(self).inv(),
            final(self).loans() == old(self).loans().push(Loan { part, access: None }),
            final(self).wqes() == old(self).wqes(),
    { self.push_loan(part, Ghost(None), &mut Vec::new()); }

    /// Whether one more resource takes no allocation: it holds none, or a list with room.
    #[inline(always)]
    fn has_room(&self) -> (room: bool)
        ensures
            room ==> !(self.held is One),
    {
        match &self.held {
            Held::Nothing => true,
            Held::One(_) => false,
            Held::Many(rest) => rest.len() < capacity_of(rest),
        }
    }

    /// Keeps a resource, for which [`Leases::has_room`] says it has room.
    #[inline(always)]
    fn keep_in_room(&mut self, lease: Lease<N>)
        requires
            !(old(self).held is One),
        ensures
            final(self).raw_view() == old(self).raw_view().push(lease),
            final(self).loans == old(self).loans,
            final(self).wqes == old(self).wqes,
    {
        let ghost before = *self;
        match &mut self.held {
            Held::Many(rest) => rest.push(lease),
            held => {
                let free = replace_value(held, Held::One(lease));
                forget_free(free, Ghost(free is Nothing));
            },
        }
        proof {
            assert(self.raw_view() =~= before.raw_view().push(lease));
        }
    }

    /// Whether it holds a list of several resources.
    #[inline(always)]
    pub fn is_many(&self) -> (many: bool)
        ensures
            many == self.spec_many(),
    {
        matches!(self.held, Held::Many(_))
    }

    /// [`Leases::is_many`], as a specification.
    pub closed spec fn spec_many(self) -> bool {
        self.held is Many
    }

    /// The unique resources it holds, in their order: what a program can have back.
    pub closed spec fn uniques(self) -> Seq<Resource<N>> {
        self.raw_view().filter(is_unique()).map_values(|lease: Lease<N>| lease.observation())
    }

    /// What a completed request hands back of its list of several resources: frozen ones have no
    /// way back and are released here, so that the list goes to `spare` for the next request of
    /// several, unless more than one unique buffer has to leave in it.
    #[cold]
    #[inline(never)]
    pub(crate) fn hand_back_many(&mut self, spare: &mut Vec<Lease<N>>)
        requires
            old(self).inv(),
            old(spare)@.len() == 0,
        ensures
            final(self).inv(),
            old(self).fully_recorded() ==> final(self).fully_recorded(),
            final(self).uniques() == old(self).uniques(),
            !old(self).spec_many() ==> *final(self) == *old(self),
            final(spare)@.len() == 0,
    {
        let ghost before = *self;
        let ghost held = before.raw_view();
        let mut rest = match replace_value(&mut self.held, Held::Nothing) {
            Held::Many(rest) => rest,
            other => {
                self.held = other;
                return;
            },
        };
        // The unique ones first, in their order; the frozen ones go.
        let mut kept = 0;
        let mut i = 0;
        while i < rest.len()
            invariant
                kept <= i <= rest@.len(),
                rest@.len() == held.len(),
                rest@.take(kept as int) == held.take(i as int).filter(is_unique()),
                forall|j: int| i <= j < rest@.len() ==> #[trigger] rest@[j] == held[j],
            decreases rest@.len() - i,
        {
            proof {
                lemma_filter_step(held, i as int, is_unique());
            }
            let ghost prev = rest@;
            if matches!(&rest[i], Lease::Unique(_)) {
                swap_in(&mut rest, kept, i);
                kept += 1;
                proof {
                    assert(rest@.take(kept as int) =~= prev.take(kept - 1).push(held[i as int]));
                }
            }
            i += 1;
        }
        rest.truncate(kept);
        proof {
            assert(held.take(i as int) =~= held);
        }
        self.loans = Ghost(Seq::empty());
        self.wqes = Ghost(Seq::empty());
        if kept > 1 {
            self.held = Held::Many(rest);
            proof { before.lemma_uniques(self); }
            return;
        }
        let ghost filtered = rest@;
        if let Some(unique) = rest.pop() {
            self.held = Held::One(unique);
        }
        if capacity_of(spare) < capacity_of(&rest) {
            *spare = rest;
        }
        proof {
            assert(self.raw_view() =~= filtered);
            before.lemma_uniques(self);
        }
    }

    /// What is left of a list handed back holds its unique resources, and no input.
    proof fn lemma_uniques(self, post: &Self)
        requires
            post.raw_view() == self.raw_view().filter(is_unique()),
            post.loans@.len() == 0,
            post.wqes@.len() == 0,
        ensures
            post.inv(),
            post.fully_recorded(),
            post.uniques() == self.uniques(),
    {
        let kept = self.raw_view().filter(is_unique());
        broadcast use vstd::seq_lib::group_filter_ensures;
        lemma_filter_all(kept, is_unique());
        assert(post.raw_view().filter(is_unique()) == kept);
        assert(post.wqes_ok());
        assert(nonalias(post.loans()));
    }

    #[inline(always)]
    fn push_loan(&mut self, part: Part<N>, Ghost(access): Ghost<Option<Dir>>, spare: &mut Vec<Lease<N>>)
        requires
            access == Some(Dir::In) ==> part.spec_writable(),
            old(spare)@.len() == 0,
        ensures
            old(self).inv() ==> final(self).inv(),
            final(self).loans() == old(self).loans().push(Loan { part, access }),
            final(self).wqes() == old(self).wqes(),
    {
        proof { use_type_invariant(&part); }
        let ghost before = *self;
        let ghost incoming = part;
        #[cfg(verus_keep_ghost)]
        let part = {
            let mut part = part;
            self.disjoint_part(&mut part);
            part
        };
        let lease = part.lease;
        self.push_resource(lease, spare);
        self.loans = Ghost(before.loans().push(Loan { part: incoming, access }));
        proof {
            if before.inv() {
                lemma_pushed(before.view(), incoming.resource(), self.view());
                lemma_loan_added(before.loans(), before.view(), incoming, access, self.view());
                assert(self.wqes_ok()) by {
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
                }
            }
        }
    }

    /// The number of retained resources.
    pub fn len(&self) -> (len: usize)
        requires self.view().len() <= usize::MAX,
        ensures len == self.view().len(),
    {
        match &self.held {
            Held::Nothing => 0,
            Held::One(_) => 1,
            Held::Many(rest) => rest.len(),
        }
    }

    /// Whether it holds no lease.
    pub fn is_empty(&self) -> (empty: bool)
        ensures empty == self.spec_holds_nothing(),
            empty ==> self.view().len() == 0,
    { matches!(self.held, Held::Nothing) }

    /// [`Leases::is_empty`], as a specification.
    pub closed spec fn spec_holds_nothing(self) -> bool {
        self.held is Nothing
    }

    /// Selects the first unique resource and drops all remaining resources.
    /// Type interpretation is handled by the buffer wrapper after this move.
    #[inline(always)]
    pub(super) fn into_unique_raw(self) -> (r: Option<RawMut<N>>)
        ensures
            r is Some ==> exists|i: int| 0 <= i < self.raw_view().len()
                && self.raw_view()[i] == Lease::Unique(r->0)
                && forall|j: int| 0 <= j < i ==> !(self.raw_view()[j] is Unique),
            r is None ==> forall|i: int| 0 <= i < self.raw_view().len()
                ==> !(self.raw_view()[i] is Unique),
    {
        let mut remaining = self;
        remaining.take_unique_raw()
    }

    /// Takes the first unique resource out, leaving the others.
    /// Type interpretation is handled by the buffer wrapper after this move.
    #[inline(always)]
    pub(super) fn take_unique_raw(&mut self) -> (r: Option<RawMut<N>>)
        ensures
            r is Some ==> exists|i: int| 0 <= i < old(self).raw_view().len()
                && old(self).raw_view()[i] == Lease::Unique(r->0)
                && forall|j: int| 0 <= j < i ==> !(old(self).raw_view()[j] is Unique),
            r is None ==> forall|i: int| 0 <= i < old(self).raw_view().len()
                ==> !(old(self).raw_view()[i] is Unique),
    {
        let ghost before = *self;
        match &self.held {
            Held::Nothing => return None,
            Held::One(Lease::Shared(_)) => return None,
            Held::One(Lease::Unique(_)) => {},
            Held::Many(_) => return self.take_unique_raw_from_many(),
        }
        match replace_value(&mut self.held, Held::Nothing) {
            Held::One(Lease::Unique(raw)) => {
                proof {
                    assert(before.raw_view()[0] == Lease::Unique(raw));
                }
                Some(raw)
            },
            other => {
                forget_free(other, Ghost(false));
                None
            },
        }
    }

    /// [`Leases::take_unique_raw`], among several resources.
    #[inline(never)]
    fn take_unique_raw_from_many(&mut self) -> (r: Option<RawMut<N>>)
        requires
            old(self).held is Many,
        ensures
            r is Some ==> exists|i: int| 0 <= i < old(self).raw_view().len()
                && #[trigger] old(self).raw_view()[i] == Lease::Unique(r->0)
                && forall|j: int| 0 <= j < i ==> !(old(self).raw_view()[j] is Unique),
            r is None ==> forall|i: int| 0 <= i < old(self).raw_view().len()
                ==> !(old(self).raw_view()[i] is Unique),
    {
        let ghost before = old(self).raw_view();
        let rest = match &mut self.held {
            Held::Many(rest) => rest,
            _ => return None,
        };
        let mut i = 0;
        while i < rest.len()
            invariant
                before == old(self).raw_view(),
                rest@ == before,
                i <= rest@.len(),
                forall|j: int| 0 <= j < i ==> !(before[j] is Unique),
            decreases rest@.len() - i,
        {
            if matches!(&rest[i], Lease::Unique(_)) {
                return match rest.remove(i) {
                    Lease::Unique(raw) => {
                        proof {
                            let ghost k = i as int;
                            assert(before[k] == Lease::Unique(raw));
                            assert(0 <= k < before.len() && before[k] == Lease::Unique(raw)
                                && forall|j: int| 0 <= j < k ==> !(before[j] is Unique));
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

/// The bytes of what can be lent (`Lend::peek`).
#[doc(hidden)]
#[verifier::reject_recursive_types(N)]
pub struct Peek<'a, N: Nic> {
    pub(crate) frozen: &'a Arc<Frozen<N>>,
    pub(crate) offset: usize,
    pub(crate) bytes: usize,
    pub(crate) lkey: u32,
}

impl<N: Nic> Peek<'_, N> {
    /// Its bytes lie in the allocation it names, which is all that its maker promises.
    pub(crate) closed spec fn inv(&self) -> bool {
        &&& (**self.frozen).inv()
        &&& self.offset + self.bytes <= (**self.frozen).byte_len()
    }

    /// Its scatter/gather element, for a length that fits one.
    pub(crate) closed spec fn spec_sge(&self) -> Sge {
        Sge {
            addr: ((**self.frozen).pointer().addr() + self.offset) as u64,
            length: self.bytes as u32,
            lkey: self.lkey,
        }
    }

    /// The part the program would get for its bytes in a protection domain.
    pub(crate) closed spec fn part(&self, pd: u64) -> Part<N> {
        Part { sge: self.spec_sge(), pd, lease: Lease::Shared(*self.frozen) }
    }

    /// Its bytes are a part of its allocation, for a length that fits an SGE.
    pub(crate) proof fn lemma_part(&self, pd: u64)
        requires
            self.inv(),
            self.bytes <= u32::MAX,
        ensures
            self.part(pd).inv(),
            !self.part(pd).spec_writable(),
            self.part(pd).spec_pd() == pd,
            self.part(pd).spec_sge() == self.spec_sge(),
    {
        self.frozen.lemma_contents();
        self.frozen.raw.lemma_domain();
        assert(self.part(pd).byte_domain().subset_of((**self.frozen).byte_domain()));
    }

    /// Its scatter/gather element, for a length that fits one.
    #[inline(always)]
    pub(crate) fn sge(&self) -> (sge: Sge)
        requires
            self.inv(),
            self.bytes <= u32::MAX,
        ensures
            sge == self.spec_sge(),
            sge.length == self.bytes,
    {
        proof {
            use_type_invariant(&**self.frozen);
            self.frozen.lemma_contents();
            self.frozen.raw.lemma_domain();
        }
        Sge {
            addr: (self.frozen.raw.ptr().addr() + self.offset) as u64,
            length: self.bytes as u32,
            lkey: self.lkey,
        }
    }
}

/// What a request being written keeps alive, and the inputs of its WQEs so far.
#[verifier::reject_recursive_types(N)]
pub(crate) struct Kept<N: Nic> {
    /// The frozen allocation of its last WQE, kept apart from the rest of what keeps its buffers
    /// alive, since most WQEs ask for it: one comparison.
    last: Option<Arc<Frozen<N>>>,
    /// What keeps its other buffers alive.
    leases: Leases<N>,
    /// Its WQEs' inputs.
    #[allow(dead_code)]
    loans: Ghost<Seq<Loan<N>>>,
}

impl<N: Nic> Kept<N> {
    /// Its keepers, the last one last.
    closed spec fn keepers(self) -> Seq<Resource<N>> {
        match self.last {
            Some(frozen) => self.leases.view().push(Lease::Shared(frozen).observation()),
            None => self.leases.view(),
        }
    }

    /// Every input has a keeper here, what keeps inputs alive is apart, and the last keeper is apart
    /// from the others.
    pub(crate) closed spec fn inv(self) -> bool {
        &&& self.leases.loans().len() == 0
        &&& self.leases.wqes().len() == 0
        &&& loans_kept(self.loans@, self.keepers())
        &&& self.last matches Some(frozen) ==> self.leases.apart_from(Lease::Shared(frozen).observation())
        &&& self.last_is_input()
    }

    /// They hold the same, in the same way.
    pub(crate) closed spec fn same_contents(self, other: Self) -> bool {
        &&& self.last == other.last
        &&& self.leases.same_contents(other.leases)
        &&& self.loans == other.loans
    }

    /// What is said of it depends on what it holds.
    pub(crate) proof fn lemma_same_contents(self, other: Self)
        requires
            self.same_contents(other),
        ensures
            self.inv() == other.inv(),
            self.loans() == other.loans(),
    {
        assert(self.keepers() == other.keepers());
    }

    /// Each keeper of it is apart from everything the engine holds, as linear permissions say, and
    /// so is each of its inputs.
    #[cfg(verus_keep_ghost)]
    pub(crate) fn separate_from_table<C>(&mut self, hw: &crate::hw::Hw<C, N>)
        requires
            hw.inv(),
            old(self).inv(),
        ensures
            final(self).same_contents(*old(self)),
            forall|i: int| 0 <= i < final(self).loans().len()
                ==> #[trigger] hw.table_apart(final(self).loans()[i].part.resource()),
    {
        let ghost before = *self;
        let taken = self.last.take();
        match taken {
            Some(frozen) => {
                let mut lease = Lease::Shared(frozen);
                hw.separate_lease(&mut lease);
                match lease {
                    Lease::Shared(same) => self.last = Some(same),
                    Lease::Unique(_) => {},
                }
            },
            None => {},
        }
        let ghost view = self.leases.view();
        let ghost raw = self.leases.raw_view();
        match &mut self.leases.held {
            Held::Nothing => {},
            Held::One(first) => hw.separate_lease(first),
            Held::Many(rest) => {
                let ghost original = rest@;
                let mut i = 0;
                while i < rest.len()
                    invariant
                        hw.inv(),
                        rest@ == original,
                        i <= rest@.len(),
                        forall|j: int| 0 <= j < i ==> #[trigger] hw.table_apart(rest@[j].observation()),
                    decreases rest@.len() - i,
                {
                    let ghost now = rest@;
                    hw.separate_lease(&mut rest[i]);
                    proof {
                        assert(rest@ =~= now);
                    }
                    i += 1;
                }
            },
        }
        proof {
            assert(self.same_contents(before));
            self.lemma_same_contents(before);
            assert forall|i: int| 0 <= i < self.loans().len()
                implies #[trigger] hw.table_apart(self.loans()[i].part.resource()) by {
                let kept = self.loans()[i].part.resource();
                assert(keeps(self.keepers(), kept));
                let k = choose|k: int| 0 <= k < self.keepers().len() && (#[trigger] self.keepers()[k]).same_backing(kept);
                if k < self.leases.view().len() {
                    assert(self.keepers()[k] == self.leases.view()[k]);
                    assert(self.leases.view()[k] == self.leases.raw_view()[k].observation());
                } else {
                    assert(self.keepers()[k] == Lease::Shared(self.last->0).observation());
                }
                hw.lemma_table_apart_backing(self.keepers()[k], kept);
            }
        }
    }

    /// Its last allocation is that of an input, so a `Peek` of it, which is no more than its bytes,
    /// is of that input's protection domain.
    closed spec fn last_is_input(self) -> bool {
        self.last matches Some(frozen) ==> exists|i: int|
            0 <= i < self.loans@.len() && (#[trigger] self.loans@[i]).part.lease == Lease::Shared(frozen)
    }

    /// The input it holds the last allocation for stays one when another joins.
    proof fn lemma_last_stays(self, before: &Self, part: Part<N>, access: Option<Dir>)
        requires
            before.last_is_input(),
            self.last == before.last,
            self.loans@ == before.loans@.push(Loan { part, access }),
        ensures
            self.last_is_input(),
    {
        if let Some(frozen) = self.last {
            let i = choose|i: int|
                0 <= i < before.loans@.len() && (#[trigger] before.loans@[i]).part.lease == Lease::Shared(frozen);
            assert(self.loans@[i] == before.loans@[i]);
        }
    }

    /// The inputs of its WQEs so far.
    pub(crate) closed spec fn loans(self) -> Seq<Loan<N>> {
        self.loans@
    }

    /// Nothing yet.
    #[inline(always)]
    pub(crate) fn new() -> (kept: Self)
        ensures
            kept.inv(),
            kept.loans().len() == 0,
    {
        Kept { last: None, leases: Leases::new(), loans: Ghost(Seq::empty()) }
    }

    /// Keeps `part` if that takes no allocation, else hands it back: a frozen allocation is the new
    /// last one, and the one before it joins the others if they have room.
    #[inline(always)]
    pub(crate) fn try_keep(&mut self, part: Part<N>, Ghost(access): Ghost<Dir>) -> (r: core::result::Result<(), Part<N>>)
        requires
            old(self).inv(),
            access is In ==> part.spec_writable(),
        ensures
            final(self).inv(),
            match r {
                Ok(()) => final(self).loans() == old(self).loans().push(Loan { part, access: Some(access) }),
                Err(back) => back == part && final(self).loans() == old(self).loans(),
            },
    {
        proof {
            use_type_invariant(&part);
            part.lemma_byte_backing();
        }
        let ghost incoming = part;
        let ghost before = *self;
        #[cfg(verus_keep_ghost)]
        let part = {
            let mut part = part;
            self.leases.disjoint_part(&mut part);
            match &self.last {
                Some(last) => part.separate_frozen(last),
                None => {},
            }
            part
        };
        match part.frozen_of() {
            Some(frozen) => match &self.last {
                Some(last) => {
                    if same_frozen_arc(last, frozen) {
                        self.loans = Ghost(before.loans@.push(Loan { part: incoming, access: Some(access) }));
                        proof {
                            before.lemma_last_keeps(incoming, Some(access));
                            self.lemma_last_stays(&before, incoming, Some(access));
                        }
                        return Ok(());
                    }
                    if !self.leases.has_room() {
                        proof {
                            assert(self.keepers() == before.keepers());
                        }
                        return Err(part);
                    }
                },
                None => {},
            },
            None => {
                if self.last.is_some() || !self.leases.is_empty() {
                    proof {
                        assert(self.keepers() == before.keepers());
                    }
                    return Err(part);
                }
            },
        }
        let ghost view = self.leases.view();
        match part.into_frozen() {
            Ok(frozen) => {
                let mut last = Some(frozen);
                core::mem::swap(&mut last, &mut self.last);
                match last {
                    Some(earlier) => {
                        self.leases.keep_in_room(Lease::Shared(earlier));
                        proof {
                            assert(self.leases.view() =~= view.push(Lease::Shared(earlier).observation()));
                            assert(before.keepers() == self.leases.view());
                        }
                    },
                    None => {
                        proof {
                            assert(before.keepers() == self.leases.view());
                        }
                    },
                }
                proof {
                    lemma_keeps_more(self.leases.view(), Lease::Shared(frozen).observation());
                    lemma_keeps_element(self.keepers(), Lease::Shared(frozen).observation(), self.leases.view().len() as int);
                    assert(self.keepers()[self.leases.view().len() as int].same_backing(incoming.resource()));
                    assert forall|j: int| 0 <= j < self.leases.view().len()
                        implies (#[trigger] self.leases.view()[j]).apart(Lease::Shared(frozen).observation()) by {
                        if j < view.len() {
                            assert(self.leases.view()[j] == view[j]);
                            assert(view[j].apart(incoming.resource()));
                        }
                    }
                }
            },
            Err(lease) => {
                self.leases.keep_in_room(lease);
                proof {
                    assert(self.leases.view() =~= view.push(lease.observation()));
                    assert(self.keepers() == self.leases.view());
                    assert(before.keepers().len() == 0);
                    assert(self.keepers()[0].same_backing(incoming.resource()));
                }
            },
        }
        self.loans = Ghost(before.loans@.push(Loan { part: incoming, access: Some(access) }));
        proof {
            self.lemma_new_keeper(&before, incoming, Some(access));
        }
        Ok(())
    }

    /// Adds the lease of a WQE of its QP's general path: a frozen allocation is the new last one.
    pub(crate) fn keep_general(&mut self, part: Part<N>, Ghost(access): Ghost<Dir>, spare: &mut Vec<Lease<N>>)
        requires
            old(self).inv(),
            access is In ==> part.spec_writable(),
            old(spare)@.len() == 0,
        ensures
            final(self).inv(),
            final(self).loans() == old(self).loans().push(Loan { part, access: Some(access) }),
            final(spare)@.len() == 0,
    {
        proof {
            use_type_invariant(&part);
            part.lemma_byte_backing();
        }
        let ghost incoming = part;
        let ghost before = *self;
        #[cfg(verus_keep_ghost)]
        let part = {
            let mut part = part;
            self.leases.disjoint_part(&mut part);
            match &self.last {
                Some(last) => part.separate_frozen(last),
                None => {},
            }
            part
        };
        let ghost view = self.leases.view();
        proof {
            assert forall|x: Resource<N>| #[trigger] keeps(view, x) implies keeps(view, x) by {}
        }
        match part.into_frozen() {
            Ok(frozen) => {
                let same = match &self.last {
                    Some(last) => same_frozen_arc(last, &frozen),
                    None => false,
                };
                if !same {
                    let mut last = Some(frozen);
                    core::mem::swap(&mut last, &mut self.last);
                    match last {
                        Some(earlier) => {
                            self.leases.push_resource(Lease::Shared(earlier), spare);
                            proof {
                                lemma_pushed(view, Lease::Shared(earlier).observation(), self.leases.view());
                                lemma_pushed_covered(view, Lease::Shared(earlier).observation(), self.leases.view());
                            }
                        },
                        None => {},
                    }
                    proof {
                        lemma_keeps_more(self.leases.view(), Lease::Shared(frozen).observation());
                        assert(self.keepers()[self.leases.view().len() as int].same_backing(incoming.resource()));
                        assert forall|j: int| 0 <= j < self.leases.view().len()
                            implies (#[trigger] self.leases.view()[j]).apart(Lease::Shared(frozen).observation()) by {
                            if j < view.len() {
                                assert(self.leases.view()[j] == view[j]);
                                assert(view[j].apart(incoming.resource()));
                            }
                        }
                    }
                } else {
                    proof {
                        assert(self.keepers()[self.leases.view().len() as int].same_backing(incoming.resource()));
                    }
                }
            },
            Err(lease) => {
                let mut last = None;
                core::mem::swap(&mut last, &mut self.last);
                match last {
                    Some(earlier) => {
                        self.leases.push_resource(Lease::Shared(earlier), spare);
                        proof {
                            lemma_pushed(view, Lease::Shared(earlier).observation(), self.leases.view());
                            lemma_pushed_covered(view, Lease::Shared(earlier).observation(), self.leases.view());
                        }
                    },
                    None => {},
                }
                let ghost with_earlier = self.leases.view();
                self.leases.push_resource(lease, spare);
                proof {
                    lemma_pushed(with_earlier, lease.observation(), self.leases.view());
                }
            },
        }
        self.loans = Ghost(before.loans@.push(Loan { part: incoming, access: Some(access) }));
        proof {
            self.lemma_new_keeper(&before, incoming, Some(access));
        }
    }

    /// Whether `peek`'s bytes are of the frozen allocation of its last WQE.
    #[inline(always)]
    pub(crate) fn keeps_peek(&self, peek: &Peek<'_, N>) -> (keeps: bool)
        ensures
            keeps ==> self.keeps_frozen(**peek.frozen),
    {
        match &self.last {
            Some(frozen) => same_frozen_arc(frozen, peek.frozen),
            None => false,
        }
    }

    /// The inputs the NIC writes are unique.
    pub(crate) proof fn lemma_loans(self)
        requires
            self.inv(),
        ensures
            forall|i: int| 0 <= i < self.loans().len() && (#[trigger] self.loans()[i]).access == Some(Dir::In)
                ==> self.loans()[i].part.spec_writable(),
    {
        assert forall|i: int| 0 <= i < self.loans().len() && (#[trigger] self.loans()[i]).access == Some(Dir::In)
            implies self.loans()[i].part.spec_writable() by {
            assert(loans_kept(self.loans(), self.keepers()));
        }
    }

    /// `frozen` is the frozen allocation of its last WQE.
    pub(crate) closed spec fn keeps_frozen(self, frozen: Frozen<N>) -> bool {
        self.last matches Some(last) && *last == frozen
    }

    /// `peek`'s bytes, which it keeps alive already, are an input more: nothing happens.
    #[inline(always)]
    #[allow(unused_variables)] // Read by ghost code alone.
    pub(crate) fn note_peek(&mut self, peek: &Peek<'_, N>, Ghost(pd): Ghost<u64>)
        requires
            old(self).inv(),
            old(self).keeps_frozen(**peek.frozen),
            peek.inv(),
            peek.bytes <= u32::MAX,
            forall|i: int| 0 <= i < old(self).loans().len() ==> (#[trigger] old(self).loans()[i]).part.spec_pd() == pd,
        ensures
            final(self).inv(),
            final(self).loans() == old(self).loans().push(Loan { part: peek.part(pd), access: Some(Dir::Out) }),
    {
        let ghost before = *self;
        self.loans = Ghost(before.loans@.push(Loan { part: peek.part(pd), access: Some(Dir::Out) }));
        proof {
            peek.lemma_part(pd);
            before.lemma_last_keeps(peek.part(pd), Some(Dir::Out));
            assert(self.keepers() == before.keepers());
            self.lemma_last_stays(&before, peek.part(pd), Some(Dir::Out));
        }
    }

    /// An input of the last keeper's allocation joins the inputs.
    proof fn lemma_last_keeps(self, part: Part<N>, access: Option<Dir>)
        requires
            self.inv(),
            part.inv(),
            !part.spec_writable(),
            access == Some(Dir::In) ==> part.spec_writable(),
            self.last matches Some(last) && part.resource().same_frozen(Lease::Shared(last).observation()),
        ensures
            loans_kept(self.loans@.push(Loan { part, access }), self.keepers()),
    {
        let last = self.last->0;
        let keepers = self.keepers();
        assert forall|i: int| 0 <= i < keepers.len() implies (#[trigger] keepers[i]).apart(part.resource()) by {
            if i < self.leases.view().len() {
                assert(keepers[i] == self.leases.view()[i]);
                keepers[i].lemma_apart_backing(Lease::Shared(last).observation(), part.resource());
            }
        }
        assert forall|r: Resource<N>| #[trigger] keeps(keepers, r) implies keeps(keepers, r) by {}
        assert(keepers[keepers.len() - 1].same_backing(part.resource()));
        lemma_loan_added(self.loans@, keepers, part, access, keepers);
    }

    /// What a new keeper leaves is what the invariant says: it takes over from the keepers before,
    /// holds the new input, and the new input is apart from the unique ones.
    proof fn lemma_new_keeper(self, before: &Self, part: Part<N>, access: Option<Dir>)
        requires
            before.inv(),
            part.inv(),
            access == Some(Dir::In) ==> part.spec_writable(),
            self.leases.loans().len() == 0,
            self.leases.wqes().len() == 0,
            self.loans@ == before.loans@.push(Loan { part, access }),
            forall|i: int| 0 <= i < before.keepers().len() ==> (#[trigger] before.keepers()[i]).apart(part.resource()),
            forall|r: Resource<N>| #[trigger] keeps(before.keepers(), r) ==> keeps(self.keepers(), r),
            keeps(self.keepers(), part.resource()),
            self.last matches Some(last) ==> self.leases.apart_from(Lease::Shared(last).observation()),
            self.last == before.last || self.last is None
                || (self.last matches Some(frozen) && part.lease == Lease::Shared(frozen)),
        ensures
            self.inv(),
    {
        lemma_loan_added(before.loans@, before.keepers(), part, access, self.keepers());
        if self.last == before.last {
            self.lemma_last_stays(before, part, access);
        } else if let Some(frozen) = self.last {
            assert(self.loans@[before.loans@.len() as int].part.lease == Lease::Shared(frozen));
        }
    }

    /// Its buffers, leaving it none, for the WQEs `wqes`. Starting a list of several takes the room
    /// in `spare`.
    #[inline(always)]
    pub(crate) fn take(&mut self, spare: &mut Vec<Lease<N>>, Ghost(wqes): Ghost<Seq<WqeLoans>>) -> (leases: Leases<N>)
        requires
            old(self).inv(),
            old(spare)@.len() == 0,
            forall|i: int| 0 <= i < wqes.len() ==> (#[trigger] wqes[i]).matches(old(self).loans()),
            forall|i: int, j: int| 0 <= i < j < wqes.len() ==> (#[trigger] wqes[i]).end() <= (#[trigger] wqes[j]).first,
        ensures
            final(self).inv(),
            final(self).loans().len() == 0,
            final(spare)@.len() == 0,
            leases.inv(),
            leases.loans() == old(self).loans(),
            leases.wqes() == wqes,
    {
        let ghost before = *self;
        let mut last = None;
        core::mem::swap(&mut last, &mut self.last);
        let mut leases = Leases::new();
        core::mem::swap(&mut leases, &mut self.leases);
        let ghost view = leases.view();
        match last {
            Some(frozen) => {
                let ghost lease = Lease::Shared(frozen);
                let ghost raw = leases.raw_view();
                if leases.has_room() {
                    leases.keep_in_room(Lease::Shared(frozen));
                    proof {
                        assert(leases.view() =~= view.push(lease.observation()));
                    }
                } else {
                    leases.push_resource(Lease::Shared(frozen), spare);
                }
                proof {
                    lemma_pushed(view, lease.observation(), leases.view());
                    assert forall|j: int| 0 <= j < before.keepers().len()
                        implies keeps(leases.view(), #[trigger] before.keepers()[j]) by {
                        if j < view.len() {
                            lemma_keeps_element(view, view[j], j);
                        }
                    }
                }
            },
            None => {
                proof {
                    assert forall|j: int| 0 <= j < before.keepers().len()
                        implies keeps(leases.view(), #[trigger] before.keepers()[j]) by {
                        lemma_keeps_element(view, view[j], j);
                    }
                }
            },
        }
        leases.loans = Ghost(before.loans@);
        leases.wqes = Ghost(wqes);
        self.loans = Ghost(Seq::empty());
        proof {
            lemma_covers(before.keepers(), leases.view());
            assert(leases.view() == leases.view());
            assert(nonalias(self.loans@));
            assert forall|i: int| 0 <= i < leases.loans().len() implies {
                let loan = #[trigger] leases.loans()[i];
                &&& loan.part.inv()
                &&& keeps(leases.view(), loan.part.resource())
                &&& loan.access == Some(Dir::In) ==> loan.part.spec_writable()
            } by {
                assert(keeps(before.keepers(), leases.loans()[i].part.resource()));
            }
        }
        leases
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
            held: Held<Model>,
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
