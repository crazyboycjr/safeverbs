//! Linear raw-byte permissions carried by the actual buffer ranges.
//!
//! The allocator is one primitive boundary. Splitting and pool withdrawal/return
//! are checked bookkeeping. These tokens describe domains and provenance, not
//! initialized typed contents or the final Arc-driven deallocation protocol.
#![allow(unused_imports)]

use core::alloc::Layout;
use std::sync::Arc;
use vstd::prelude::*;
use vstd::raw_ptr::{Dealloc, DeallocData, PointsToRaw};

use super::Region;
use super::permissions::BytePermission;
use super::range::ByteRange;
use crate::nic::Nic;

verus! {

/// Registration/finalization is the remaining lifetime boundary; range ownership
/// and byte permissions are visible, rather than the whole RawMut being opaque.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
#[verifier::external_body]
#[verifier::reject_recursive_types(N)]
pub struct ExRegion<N: Nic>(Region<N>);

/// The fallible zeroed counterpart of vstd's global allocator primitive.
/// No permission is returned for an invalid layout or a null allocation.
#[verifier::external_body]
fn allocate_zeroed_raw(size: usize, align: usize) -> (r: Option<(
    *mut u8, Tracked<PointsToRaw>, Tracked<Dealloc>,
)>)
    ensures r matches Some((ptr, perm, dealloc)) ==> {
        &&& size > 0
        &&& vstd::layout::valid_layout(size, align)
        &&& ptr.addr() != 0
        &&& ptr.addr() as int + size <= usize::MAX as int + 1
        &&& ptr.addr() as int % align as int == 0
        &&& perm@.is_range(ptr.addr() as int, size as int)
        &&& ptr@.provenance == perm@.provenance()
        &&& dealloc@@ == (DeallocData {
            addr: ptr.addr(), size: size as nat, align: align as nat,
            provenance: perm@.provenance(),
        })
    },
    opens_invariants none
{
    if size == 0 { return None; }
    let layout = match Layout::from_size_align(size, align) {
        Ok(layout) => layout,
        Err(_) => return None,
    };
    // SAFETY: the validated layout has a nonzero size.
    let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
    if ptr.is_null() { None } else {
        Some((ptr, Tracked::assume_new(), Tracked::assume_new()))
    }
}

/// An actual unique range and the linear permission for precisely its bytes.
pub(super) struct OwnedRange {
    ptr: *mut u8,
    len: usize,
    #[allow(dead_code)]
    geometry: Ghost<ByteRange>,
    #[allow(dead_code)]
    base: Ghost<*mut u8>,
    #[allow(dead_code)]
    permission: Tracked<BytePermission>,
}

impl OwnedRange {
    pub(super) closed spec fn geometry(self) -> ByteRange { self.geometry@ }
    pub(super) closed spec fn pointer(self) -> *mut u8 { self.ptr }
    pub(super) closed spec fn spec_len(self) -> usize { self.len }
    pub(super) closed spec fn base(self) -> *mut u8 { self.base@ }
    pub(super) closed spec fn permission(self) -> BytePermission { self.permission@ }

    #[verifier::type_invariant]
    pub(super) open spec fn inv(self) -> bool {
        &&& self.geometry().inv()
        &&& self.spec_len() == self.geometry().spec_len()
        &&& self.base().addr() != 0
        &&& self.base().addr() as int + self.geometry().spec_capacity() <= usize::MAX
        &&& self.permission().inv()
        &&& self.permission().dom() == self.geometry().byte_set(self.base().addr() as int)
        &&& self.permission().provenance() == self.base()@.provenance
        &&& self.pointer().addr() == self.base().addr() + self.geometry().spec_offset()
        &&& self.pointer()@.provenance == self.base()@.provenance
    }

    #[inline]
    pub(super) fn ptr(&self) -> (r: *mut u8)
        ensures r == self.pointer(),
    { self.ptr }

    #[inline]
    pub(super) fn offset(&self, base: *mut u8) -> (r: usize)
        requires self.inv(), base == self.base(),
        ensures r == self.geometry().spec_offset(),
    { self.ptr.addr() - base.addr() }

    #[inline]
    pub(super) fn len(&self) -> (r: usize)
        ensures r == self.spec_len(),
    { self.len }

    /// Retyping must check the absolute pointer alignment, not a relative offset.
    #[inline]
    #[allow(clippy::manual_is_multiple_of)] // Keep the native arithmetic explicit for Verus.
    pub(super) fn fits(&self, width: usize, alignment: usize) -> (r: bool)
        ensures r == (width > 0 && alignment > 0
            && self.pointer().addr() % alignment == 0 && self.spec_len() % width == 0),
    {
        width > 0 && alignment > 0 && self.ptr.addr() % alignment == 0 && self.len % width == 0
    }

    #[inline]
    pub(super) fn end(&self, base: *mut u8) -> (r: usize)
        requires self.inv(), base == self.base(),
        ensures r == self.geometry().spec_offset() + self.geometry().spec_len(),
    { self.ptr.addr() - base.addr() + self.len }

    /// The runtime split also consumes and partitions the actual permission.
    #[inline]
    pub(super) fn split_at(self, at: usize) -> (r: (Self, Self))
        requires self.inv(), at <= self.geometry().spec_len(),
        ensures
            r.0.inv(), r.1.inv(),
            r.0.base() == self.base(), r.1.base() == self.base(),
            r.0.geometry().spec_offset() == self.geometry().spec_offset(),
            r.0.geometry().spec_len() == at,
            r.1.geometry().spec_offset() == self.geometry().spec_offset() + at,
            r.1.geometry().spec_len() == self.geometry().spec_len() - at,
            r.0.permission().dom().disjoint(r.1.permission().dom()),
            r.0.permission().dom().union(r.1.permission().dom()) == self.permission().dom(),
    {
        assert!(at <= self.len);
        let ghost bytes = self.geometry();
        let ghost left = bytes.split_geometry(at).0;
        let ghost right = bytes.split_geometry(at).1;
        proof { bytes.lemma_split_geometry(at); }
        let right_ptr = self.ptr.with_addr(self.ptr.addr() + at);
        let tracked (p, q) = self.permission.get().split(left.byte_set(self.base@.addr() as int));
        proof {
            assert(q.dom() =~= right.byte_set(self.base@.addr() as int));
        }
        (
            OwnedRange { ptr: self.ptr, len: at, geometry: Ghost(left), base: self.base, permission: Tracked(p) },
            OwnedRange { ptr: right_ptr, len: self.len - at, geometry: Ghost(right), base: self.base, permission: Tracked(q) },
        )
    }

    /// Transfers the range's token into an erased pool for a streaming allocator.
    #[inline]
    pub(super) fn into_pool(self) -> (r: PermissionPool)
        requires self.inv(),
        ensures r.inv(), r.base() == self.base(),
            r.capacity() == self.geometry().spec_capacity(),
            r.dom() == self.permission().dom(),
    {
        PermissionPool {
            base: self.base, capacity: Ghost(self.geometry().spec_capacity()),
            permission: Tracked(Some(self.permission.get())),
        }
    }
}

/// A registered lifetime keeper with the actual unique byte-range permission.
#[verifier::reject_recursive_types(N)]
pub(crate) struct RawMut<N: Nic> {
    pub(super) region: Arc<Region<N>>,
    pub(super) range: OwnedRange,
}

impl<N: Nic> RawMut<N> {
    #[verifier::type_invariant]
    pub(crate) closed spec fn inv(self) -> bool { self.range.inv() }
    pub(crate) closed spec fn pointer(self) -> *mut u8 { self.range.pointer() }
    pub(crate) closed spec fn byte_len(self) -> usize { self.range.spec_len() }
    pub(crate) closed spec fn byte_domain(self) -> Set<int> { self.range.permission().dom() }
    pub(crate) closed spec fn provenance(self) -> vstd::raw_ptr::Provenance {
        self.range.permission().provenance()
    }

    /// Geometry required by a typed view, separately from initialized contents.
    pub(super) open spec fn fits_type<T>(self) -> bool {
        &&& vstd::layout::size_of::<T>() > 0
        &&& self.pointer().addr() as nat % vstd::layout::align_of::<T>() == 0
        &&& self.byte_len() as nat % vstd::layout::size_of::<T>() == 0
    }

    #[inline]
    pub(super) fn ptr(&self) -> (r: *mut u8)
        ensures r == self.pointer(),
    { self.range.ptr() }

    #[inline]
    pub(super) fn len(&self) -> (r: usize)
        ensures r == self.byte_len(),
    { self.range.len() }

    /// Checked geometry for reconstructing a typed slice, including wide alignments.
    #[inline]
    pub(super) fn fits<T>(&self) -> (r: bool)
        ensures r == self.fits_type::<T>(),
    {
        vstd::layout::layout_for_type_is_valid::<T>();
        self.range.fits(core::mem::size_of::<T>(), core::mem::align_of::<T>())
    }

    /// Freezing moves the actual linear permission into shared Arc contents.
    #[inline]
    pub(super) fn freeze(self) -> (r: Arc<Frozen<N>>)
        ensures (*r).inv(), r.raw == self,
    {
        Arc::new(Frozen::new(self))
    }

    /// Splitting the actual RawMut moves its token and preserves its native keeper.
    #[inline]
    pub(super) fn split_at(self, at: usize) -> (r: (Self, Self))
        requires self.inv(), at <= self.byte_len(),
        ensures r.0.inv(), r.1.inv(),
            *r.0.region == *self.region, *r.1.region == *self.region,
            r.0.pointer() == self.pointer(),
            r.0.byte_len() == at, r.1.byte_len() == self.byte_len() - at,
            r.0.provenance() == self.provenance(), r.1.provenance() == self.provenance(),
            r.0.byte_domain().disjoint(r.1.byte_domain()),
            r.0.byte_domain().union(r.1.byte_domain()) == self.byte_domain(),
    {
        let (left, right) = self.range.split_at(at);
        let right = RawMut { region: self.region.clone(), range: right };
        let left = RawMut { region: self.region, range: left };
        (left, right)
    }

    pub(crate) proof fn lemma_domain(&self)
        requires self.inv(),
        ensures self.byte_domain() == Set::range(self.pointer().addr() as int,
            self.pointer().addr() as int + self.byte_len()),
            self.pointer()@.provenance == self.provenance(),
            self.pointer().addr() as int + self.byte_len() <= usize::MAX,
    {
        assert(self.range.geometry().byte_set(self.range.base().addr() as int)
            =~= Set::range(self.pointer().addr() as int,
                self.pointer().addr() as int + self.byte_len()));
    }

    /// A unique owner and any separately borrowed owner have disjoint bytes.
    /// The borrowed token proof preserves the exact native and ghost contents.
    #[inline(always)]
    #[allow(dead_code)] // The request-level permission adapter uses this bridge.
    #[allow(unused_variables)] // The borrowed permission proof is entirely erased.
    pub(crate) fn disjoint(&mut self, other: &Self)
        requires old(self).inv(), other.inv(),
        ensures *final(self) == *old(self),
            final(self).byte_domain().disjoint(other.byte_domain()),
    {
        proof {
            self.range.permission.borrow_mut().disjoint(other.range.permission.borrow());
        }
    }
}

/// Shared freeze stores the same unique byte permission inside the Arc contents.
#[verifier::reject_recursive_types(N)]
pub(crate) struct Frozen<N: Nic> {
    pub(super) raw: RawMut<N>,
}

impl<N: Nic> Frozen<N> {
    #[verifier::type_invariant]
    pub(crate) closed spec fn inv(self) -> bool { self.raw.inv() }
    pub(crate) closed spec fn pointer(self) -> *mut u8 { self.raw.pointer() }
    pub(crate) closed spec fn byte_len(self) -> usize { self.raw.byte_len() }
    pub(crate) closed spec fn byte_domain(self) -> Set<int> { self.raw.byte_domain() }

    pub(super) proof fn lemma_contents(&self)
        requires self.inv(),
        ensures self.raw.inv(), self.pointer() == self.raw.pointer(),
            self.byte_len() == self.raw.byte_len(), self.byte_domain() == self.raw.byte_domain(),
    {}

    #[inline]
    pub(super) fn new(raw: RawMut<N>) -> (r: Self)
        ensures r.inv(), r.raw == raw,
    {
        proof { use_type_invariant(&raw); }
        Frozen { raw }
    }
}

/// The missing standard-library binding, matching vstd's Rc counterpart.
/// It transfers the same contents on success and promises no success condition.
#[verifier::external_body]
fn arc_try_unwrap<T>(arc: Arc<T>) -> (r: core::result::Result<T, Arc<T>>)
    ensures r matches Ok(value) ==> value == *arc,
        r matches Err(same) ==> *same == *arc,
{
    Arc::try_unwrap(arc)
}

/// A successful thaw returns the very permission frozen in this Arc.
#[inline]
pub(super) fn thaw<N: Nic>(frozen: Arc<Frozen<N>>) -> (r: core::result::Result<RawMut<N>, Arc<Frozen<N>>>)
    ensures r matches Ok(raw) ==> raw.inv() && raw == frozen.raw,
        r matches Err(same) ==> *same == *frozen,
{
    match arc_try_unwrap(frozen) {
        Ok(owned) => {
            proof { use_type_invariant(&owned); }
            Ok(owned.raw)
        },
        Err(shared) => Err(shared),
    }
}

/// Actual allocation plus permission creation, with no new token minted by callers.
#[allow(clippy::question_mark)] // Keep the fallible ownership transfer explicit for Verus.
pub(super) fn allocate(size: usize, align: usize) -> (r: Option<(
    *mut u8, OwnedRange, Tracked<Dealloc>,
)>)
    ensures r matches Some((ptr, range, dealloc)) ==> {
        &&& range.inv()
        &&& range.base() == ptr
        &&& range.geometry().spec_offset() == 0
        &&& range.geometry().spec_len() == size
        &&& range.geometry().spec_capacity() == size
        &&& dealloc@@ == (DeallocData {
            addr: ptr.addr(), size: size as nat, align: align as nat,
            provenance: range.permission().provenance(),
        })
    },
{
    let (ptr, permission, dealloc) = match allocate_zeroed_raw(size, align) {
        Some(allocation) => allocation,
        None => return None,
    };
    // Keep every byte offset and the one-past pointer representable. This is a
    // checked allocator path, not a stronger assumption about addresses.
    if ptr.addr() > usize::MAX - size {
        vstd::raw_ptr::deallocate(ptr, size, align, permission, dealloc);
        return None;
    }
    let _geometry = ByteRange::whole(size);
    let tracked permission = BytePermission::from_raw(permission.get(), ptr.addr(), size);
    Some((ptr, OwnedRange {
        ptr, len: size, geometry: Ghost(_geometry), base: Ghost(ptr), permission: Tracked(permission),
    }, dealloc))
}

/// On registration failure the original full owner is still available to free.
#[allow(unused_variables)] // The permission is consumed only in erased proof code.
pub(super) fn release_failed_allocation(
    ptr: *mut u8, size: usize, align: usize, range: OwnedRange, dealloc: Tracked<Dealloc>,
)
    requires range.inv(), range.base() == ptr,
        range.geometry().spec_offset() == 0, range.geometry().spec_len() == size,
        dealloc@@ == (DeallocData {
            addr: ptr.addr(), size: size as nat, align: align as nat,
            provenance: range.permission().provenance(),
        }),
{
    let tracked raw = range.permission.get().into_raw(ptr.addr(), size);
    vstd::raw_ptr::deallocate(ptr, size, align, Tracked(raw), dealloc);
}

/// A streaming allocator's free bytes. This adds no runtime field or counter.
pub(super) struct PermissionPool {
    base: Ghost<*mut u8>,
    #[allow(dead_code)]
    capacity: Ghost<usize>,
    #[allow(dead_code)]
    permission: Tracked<Option<BytePermission>>,
}

impl PermissionPool {
    pub(super) closed spec fn base(self) -> *mut u8 { self.base@ }
    pub(super) closed spec fn capacity(self) -> usize { self.capacity@ }
    pub(super) closed spec fn dom(self) -> Set<int> { self.permission@->0.dom() }

    pub(super) closed spec fn inv(self) -> bool {
        &&& self.permission@ is Some
        &&& self.base().addr() != 0
        &&& self.base().addr() as int + self.capacity() <= usize::MAX
        &&& self.permission@->0.inv()
        &&& self.permission@->0.provenance() == self.base()@.provenance
        &&& self.dom().subset_of(Set::range(self.base().addr() as int,
            self.base().addr() as int + self.capacity()))
    }

    #[inline]
    pub(super) fn take(&mut self, base: *mut u8, geometry: ByteRange) -> (r: OwnedRange)
        requires old(self).inv(), base == old(self).base(), geometry.inv(),
            geometry.spec_capacity() == old(self).capacity(),
            geometry.byte_set(old(self).base().addr() as int).subset_of(old(self).dom()),
        ensures final(self).inv(), final(self).base() == old(self).base(),
            final(self).capacity() == old(self).capacity(),
            r.inv(), r.base() == old(self).base(), r.geometry() == geometry,
            final(self).dom() == old(self).dom().difference(r.permission().dom()),
    {
        let tracked permission = self.permission.borrow_mut().tracked_take();
        let tracked (part, rest) = permission.split(geometry.byte_set(self.base().addr() as int));
        proof { *self.permission.borrow_mut() = Some(rest); }
        OwnedRange {
            ptr: base.with_addr(base.addr() + geometry.offset()), len: geometry.len(),
            geometry: Ghost(geometry), base: self.base, permission: Tracked(part),
        }
    }

    #[inline]
    #[allow(unused_variables)] // The return of ownership is entirely erased.
    pub(super) fn put(&mut self, range: OwnedRange)
        requires old(self).inv(), range.inv(), range.base() == old(self).base(),
            range.geometry().spec_capacity() == old(self).capacity(),
            old(self).dom().disjoint(range.permission().dom()),
        ensures final(self).inv(), final(self).base() == old(self).base(),
            final(self).capacity() == old(self).capacity(),
            final(self).dom() == old(self).dom().union(range.permission().dom()),
    {
        let tracked free = self.permission.borrow_mut().tracked_take();
        let tracked joined = free.join(range.permission.get());
        proof { *self.permission.borrow_mut() = Some(joined); }
    }
}

} // verus!

// SAFETY: this is an owner of untyped bytes, not a typed reference. Its pointer
// is only observed as metadata here; typed buffers impose their element type's
// Send/Sync bounds, and mutable access requires moving or uniquely borrowing it.
unsafe impl Send for OwnedRange {}
// SAFETY: sharing metadata gives no mutable access to the underlying bytes.
unsafe impl Sync for OwnedRange {}

const _: () = assert!(core::mem::size_of::<OwnedRange>() == 2 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<PermissionPool>() == 0);
const _: () = assert!(
    core::mem::size_of::<RawMut<crate::verbs::Verbs>>() == 3 * core::mem::size_of::<usize>()
);
