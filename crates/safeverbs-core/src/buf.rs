//! Registered memory: unique writable buffers ([`BufMut`]), shared frozen
//! ones ([`Buf`]), buffers exposed to peers ([`Exposed`]), a ring for
//! streaming messages ([`RingBuf`]) and the leases that keep memory alive
//! while the NIC uses it.
//!
//! Every buffer is a range of a *region*: memory allocated zeroed and
//! registered once in a protection domain. A region stays allocated and
//! registered while any buffer or lease of it lives; it is deregistered
//! before it is freed.
//!
//! The aliasing rules are the ones Rust already has, enforced by
//! ownership: a [`BufMut`] is the only handle to its bytes (like a
//! `Box<[T]>`), so the NIC may write it while a request owns it; a [`Buf`]
//! is shared and read only (like an `Arc<[T]>`), so it can be sent any
//! number of times at once; memory a peer may write is only reachable
//! through [`Exposed`], which never hands out a `&[T]`.
//!
//! What a buffer is, its bytes and its linear permission for them, is verified (`verus!` here and
//! in `buf/`). What is not is the bridge to the program's values: the zerocopy bounds of the methods
//! outside `verus!`, and the references [`Deref`] makes (`docs/verification.md`, trusted computing
//! base).

// The code keeps the plain forms the proofs reason about (explicit matches, lengths).
#![allow(clippy::question_mark, clippy::len_zero)]

use core::alloc::Layout;
use core::fmt;
use core::marker::PhantomData;
use core::mem::{ManuallyDrop, align_of, size_of};
use core::ops::{Bound, Deref, DerefMut, RangeBounds};
use std::collections::VecDeque;
use std::sync::Arc;

use vstd::prelude::*;
use vstd::raw_ptr::{Dealloc, PointsToRaw};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::error::{Error, Result};
use crate::nic::{Nic, RemoteBuf};
use crate::pd::Pd;
use crate::trusted::{clone_pd, close_exposure, pd_id_of, register_exposure, register_region};
use crate::verbs::Verbs;
use memory::OwnedRange;
use permissions::BytePermission;
#[cfg(verus_keep_ghost)]
use safeverbs_spec::abs::LeaseId;
#[cfg(verus_keep_ghost)]
use safeverbs_spec::compose::*;

mod memory;
mod permissions;
mod range;
mod view;
pub(crate) use memory::{Frozen, RawMut};
pub use view::{Buf, BufView};
mod lease;
#[allow(unused_imports)] // Named only by contracts erased from plain cargo builds.
pub(crate) use lease::Loan;
#[allow(unused_imports)] // Named only by contracts erased from plain cargo builds.
pub(crate) use lease::Resource;
#[allow(unused_imports)] // Erased WQE provenance used by the verified engine.
pub(crate) use lease::WqeLoans;
pub(crate) use lease::{Kept, Lease, capacity_of, forget_free, replace_value};
pub use lease::{Leases, Part, Peek};
#[cfg(verus_keep_ghost)]
pub(crate) use lease::{covers_inputs, input_is_covered};

verus! {

/// The alignment of every region: a cache line.
const REGION_ALIGN: usize = 64;

/// Linux's `ENOMEM`.
const ENOMEM: i32 = 12;

/// Zeroed memory registered in a protection domain.
#[verifier::reject_recursive_types(N)]
pub(crate) struct Region<N: Nic> {
    /// Deregistered first, in `drop`.
    mr: ManuallyDrop<N::Mr>,
    pub(crate) ptr: *mut u8,
    size: usize,
    align: usize,
    pub(crate) lkey: u32,
    /// The number of its protection domain ([`Pd::id`]), which every post compares.
    pub(crate) pd_id: u64,
    /// Retained with the MR and allocation if revocation fails or unwinds.
    pd: ManuallyDrop<Pd<N>>,
    /// What frees its allocation, once.
    #[allow(dead_code)]
    deallocation: Tracked<Option<Dealloc>>,
}

impl<N: Nic> Region<N> {
    /// Its allocation is the one `deallocation` frees, until it was freed, which leaves it no bytes.
    #[verifier::type_invariant]
    closed spec fn inv(self) -> bool {
        ||| self.size == 0
        ||| (self.deallocation@ matches Some(d) && {
            &&& d.addr() == self.ptr.addr()
            &&& d.size() == self.size
            &&& d.align() == self.align
            &&& d.provenance() == self.ptr@.provenance
        })
    }

    /// Allocates `size` zeroed bytes aligned to `align` and registers them in `pd` for local writes,
    /// or frees them.
    fn allocated(pd: &Pd<N>, size: usize, align: usize) -> (r: Result<RawMut<N>>)
        ensures
            r matches Ok(whole) ==> {
                &&& whole.range.geometry().spec_offset() == 0
                &&& whole.range.geometry().spec_len() == size
                &&& (*whole.region).size == size
            },
    {
        let (address, range, deallocation) = match memory::allocate(size, align) {
            Some(allocation) => allocation,
            None => return Err(Error::Os(ENOMEM)),
        };
        proof { use_type_invariant(&range); }
        match register_region(pd, address, size) {
            Ok((mr, lkey)) => {
                let Tracked(deallocation) = deallocation;
                let region = Region {
                    lkey,
                    mr: ManuallyDrop::new(mr),
                    ptr: address,
                    size,
                    align,
                    pd_id: pd_id_of(pd),
                    pd: ManuallyDrop::new(clone_pd(pd)),
                    deallocation: Tracked(Some(deallocation)),
                };
                Ok(RawMut::new(Arc::new(region), range))
            },
            Err(e) => {
                memory::release_failed_allocation(address, size, align, range, deallocation);
                Err(e)
            },
        }
    }

    /// Its allocation goes back to the allocator, with the permissions split from it. Its `Drop`
    /// comes here once; a region without bytes has none to free.
    #[cold]
    #[inline(never)]
    fn free_memory(&mut self) {
        proof {
            use_type_invariant(&*self);
        }
        let size = self.size;
        if size == 0 {
            return;
        }
        let raw = bytes_back(&*self);
        self.size = 0;
        let tracked deallocation = self.deallocation.borrow_mut().tracked_take();
        vstd::raw_ptr::deallocate(self.ptr, size, self.align, raw, Tracked(deallocation));
    }
}

/// **The permissions split from a region are its own again when it goes.** Each one lives in a
/// [`RawMut`], which holds an `Arc` of the region its range belongs to: `Region::drop` runs when the
/// last `Arc` goes, so when none of them is left. Each is gone with its holder, so its bytes are
/// nobody's.
#[verifier::external_body]
fn bytes_back<N: Nic>(_region: &Region<N>) -> (raw: Tracked<PointsToRaw>)
    ensures
        raw@.is_range(_region.ptr.addr() as int, _region.size as int),
        raw@.provenance() == _region.ptr@.provenance,
{
    Tracked::assume_new()
}

} // verus!

// SAFETY: the region is plain memory plus a registration handle; who may
// read or write which bytes is decided by the buffers over it, which carry
// the `Send`/`Sync` bounds of their element type.
unsafe impl<N: Nic> Send for Region<N> {}
// SAFETY: as above.
unsafe impl<N: Nic> Sync for Region<N> {}

impl<N: Nic> Region<N> {
    /// Allocates `bytes` zeroed bytes aligned to `align` (at least a cache
    /// line) and registers them in `pd` for local writes.
    fn whole(pd: &Pd<N>, bytes: usize, align: usize) -> Result<RawMut<N>> {
        if bytes == 0 {
            return Err(Error::Invalid("an empty buffer"));
        }
        let layout = Layout::from_size_align(bytes, align.max(REGION_ALIGN))
            .map_err(|_| Error::Invalid("buffer size"))?;
        Region::allocated(pd, layout.size(), layout.align())
    }
}

impl<N: Nic> Drop for Region<N> {
    fn drop(&mut self) {
        // Both the MR and PD fields are ManuallyDrop: a refused revocation or
        // backend unwind retains the allocation and the dependency chain together.
        if close_registration::<N>(&mut self.mr).is_err() {
            return;
        }
        // Successful revocation (or the legacy backend's completed MR destructor) excludes
        // further access through this registration.
        self.free_memory();
        // SAFETY: dropped once, here, as the allocation goes.
        unsafe { ManuallyDrop::drop(&mut self.pd) };
    }
}

/// Revokes and destroys an MR once. On error the handle remains live for retry.
/// The caller must retain its backing memory and dependencies during unwinding.
fn close_registration<N: Nic>(mr: &mut ManuallyDrop<N::Mr>) -> Result<()> {
    match N::close_mr(mr) {
        Ok(()) | Err(Error::Unsupported(_)) => {
            // SAFETY: the caller owns this handle and will not drop it again.
            // A legacy backend without explicit close must revoke in Drop by
            // Nic's contract. An explicitly closed handle has idempotent Drop.
            unsafe { ManuallyDrop::drop(mr) };
            Ok(())
        }
        Err(error) => Err(error),
    }
}

verus! {

/// A unique, writable buffer of registered memory: `[T]` that the NIC can
/// receive into, read into, or that can be frozen into a [`Buf`] to send.
///
/// Made by `Pd::alloc` (zeroed, so `T` is [`FromBytes`]), by splitting
/// another `BufMut`, or by thawing a `Buf` nobody else holds.
#[verifier::reject_recursive_types(N)]
pub struct BufMut<T, N: Nic = Verbs> {
    pub(crate) raw: RawMut<N>,
    _t: PhantomData<T>,
}

/// What lies `k` elements of a type into memory aligned for it is aligned for it, and `k` of them
/// and the others, whose length fits them, keep that.
proof fn lemma_elements(address: int, bytes: int, k: int, width: int, alignment: int)
    requires
        width > 0,
        alignment > 0,
        width % alignment == 0,
        address % alignment == 0,
        bytes % width == 0,
        bytes >= 0,
        0 <= k <= bytes / width,
    ensures
        0 <= k * width <= bytes,
        (k * width) % width == 0,
        (bytes - k * width) % width == 0,
        (address + k * width) % alignment == 0,
{
    use vstd::arithmetic::div_mod::*;
    use vstd::arithmetic::mul::*;
    lemma_fundamental_div_mod(bytes, width);
    lemma_mul_is_commutative(width, bytes / width);
    lemma_mul_inequality(k, bytes / width, width);
    lemma_mul_nonnegative(k, width);
    lemma_mod_multiples_basic(k, width);
    lemma_mul_is_distributive_sub(width, bytes / width, k);
    lemma_mul_is_commutative(width, k);
    lemma_mod_multiples_basic(bytes / width - k, width);
    lemma_mul_is_commutative(width, bytes / width - k);
    lemma_mul_mod_noop_right(k, width, alignment);
    lemma_small_mod(0, alignment as nat);
    lemma_mod_adds(address, k * width, alignment);
}

impl<T, N: Nic> BufMut<T, N> {
    /// Its bytes hold whole elements, aligned for them.
    #[verifier::type_invariant]
    closed spec fn inv(self) -> bool {
        self.raw.fits_type::<T>()
    }

    /// The bytes it owns, by its linear permission.
    pub closed spec fn byte_domain(self) -> Set<int> {
        self.raw.byte_domain()
    }

    /// The number of its elements.
    pub closed spec fn element_len(self) -> nat {
        self.raw.byte_len() as nat / vstd::layout::size_of::<T>()
    }

    /// Unique bytes that fit `T`. They hold valid `T` because every `BufMut` starts zeroed with
    /// `T: FromBytes` and the NIC only writes into buffers of `T: FromBytes`.
    #[inline]
    fn from_raw(raw: RawMut<N>) -> (r: BufMut<T, N>)
        requires
            raw.fits_type::<T>(),
        ensures
            r.raw == raw,
    {
        BufMut { raw, _t: PhantomData }
    }

    /// Unique bytes, as a `BufMut<T>` if they fit `T`.
    #[inline(always)]
    pub(crate) fn from_checked(raw: RawMut<N>) -> (r: Option<BufMut<T, N>>)
    {
        if raw.fits::<T>() {
            Some(BufMut::from_raw(raw))
        } else {
            release(raw);
            None
        }
    }

    /// The number of elements.
    pub fn len(&self) -> (r: usize)
        ensures
            r == self.element_len(),
    {
        proof {
            use_type_invariant(self);
        }
        self.raw.len() / core::mem::size_of::<T>()
    }

    /// Whether it has no elements.
    pub fn is_empty(&self) -> bool {
        self.raw.len() == 0
    }

    /// The protection domain it is registered in.
    #[inline]
    pub fn pd_id(&self) -> u64 {
        self.raw.region.pd_id
    }

    /// `[0, mid)` and `[mid, len)`, or itself if `mid` is past its end.
    pub(crate) fn try_split_at(self, mid: usize) -> (r: core::result::Result<(Self, Self), Self>)
        ensures
            r is Ok <==> mid <= self.element_len(),
            r matches Ok((a, b)) ==> {
                &&& a.byte_domain().disjoint(b.byte_domain())
                &&& a.byte_domain().union(b.byte_domain()) == self.byte_domain()
            },
            r matches Err(same) ==> same == self,
    {
        proof {
            use_type_invariant(&self);
            use_type_invariant(&self.raw);
        }
        let len = self.len();
        if mid > len {
            return Err(self);
        }
        vstd::layout::layout_for_type_is_valid::<T>();
        let width = core::mem::size_of::<T>();
        proof {
            lemma_elements(self.raw.pointer().addr() as int, self.raw.byte_len() as int, mid as int, width as int, vstd::layout::align_of::<T>() as int);
        }
        let (a, b) = self.raw.split_at(mid * width);
        Ok((BufMut::from_raw(a), BufMut::from_raw(b)))
    }

    /// Frozen, a shared [`Buf`] over all of it.
    #[inline]
    fn frozen(self) -> (r: Buf<T, N>)
        ensures
            (*r.frozen).inv(),
            (*r.frozen).raw == self.raw,
    {
        proof {
            use_type_invariant(&self);
        }
        Buf::from_frozen(self.raw.freeze())
    }

    /// One scatter/gather element over all of it; a panic if it has more bytes than one holds.
    #[inline(always)]
    pub(crate) fn part(self) -> (part: Part<N>)
        ensures
            part.spec_writable(),
            part.byte_domain() == self.byte_domain(),
    {
        if self.raw.len() > u32::MAX as usize {
            // Released first, so that no buffer stays alive across the panic: the `Part` is then
            // built in place.
            release(self);
            sge_too_long()
        }
        Part::unique(self.raw)
    }

    /// **S1 of a buffer the program holds.** The NIC reads and writes none of its bytes.
    #[cfg(verus_keep_ghost)]
    pub(crate) fn apart_from_nic<C>(self, hw: &crate::hw::Hw<C, N>) -> (same: Self)
        requires
            hw.inv(),
        ensures
            same == self,
            hw.outside_ok() ==> forall|x: int| #[trigger] self.byte_domain().contains(x) ==> {
                &&& !hw.nic().nic_may_write(x)
                &&& !hw.nic().nic_may_read(x)
            },
    {
        proof {
            use_type_invariant(&self);
        }
        BufMut { raw: lease::unique_apart_from_nic(self.raw, hw), _t: PhantomData }
    }

    /// A unique buffer the program holds shares no byte with a lease an engine's requests hold, on
    /// the device as a whole (`safeverbs_spec::compose::lemma_device_s1`).
    #[cfg(verus_keep_ghost)]
    pub(crate) fn apart_from_engine<C>(self, hw: &crate::hw::Hw<C, N>) -> (same: Self)
        requires
            hw.inv(),
        ensures
            same == self,
            forall|l: LeaseId| #[trigger] lent(hw.part().abs, l)
                ==> hw.part().abs.leases[l].mem.disjoint(self.byte_domain().to_iset()),
    {
        proof {
            use_type_invariant(&self);
        }
        BufMut { raw: lease::unique_apart_from_engine(self.raw, hw), _t: PhantomData }
    }
}

/// Releases what it is given.
#[inline]
pub(crate) fn release<T>(_value: T) {
}

/// A buffer of more bytes than one scatter/gather element holds, which is the program's error.
#[cold]
#[inline(never)]
#[verifier::external_body]
fn sge_too_long() -> ! {
    panic!("a single SGE exceeds u32::MAX bytes")
}

} // verus!

verus! {

/// What a request's WQE can send from: a [`Buf`], by value or by reference, or a [`BufView`]. A
/// request keeps one reference to the frozen allocation its consecutive WQEs come from, so by
/// reference they touch no reference count after the first.
pub trait Lend<N: Nic>: Sized {
    /// Its bytes, and what they belong to, with nothing touched.
    #[doc(hidden)]
    fn peek(&self) -> Peek<'_, N>;
    /// One scatter/gather element, keeping its allocation alive.
    #[doc(hidden)]
    fn lend(self) -> Part<N>;
}

impl<N: Nic> BufMut<u8, N> {
    /// Bytes, as a buffer of bytes.
    #[inline(always)]
    fn of_bytes(raw: RawMut<N>) -> (r: Self)
        ensures
            r.raw == raw,
    {
        proof {
            broadcast use {vstd::layout::layout_of_primitives, vstd::layout::align_of_u8};

            assert(raw.fits_type::<u8>());
        }
        BufMut::from_raw(raw)
    }
}

impl<N: Nic> Leases<N> {
    /// The first unique buffer of it, as a `BufMut<T>` if its bytes can hold `[T]`.
    #[inline(always)]
    pub(crate) fn into_unique_as<T>(self) -> (r: Option<BufMut<T, N>>) {
        match self.into_unique_raw() {
            Some(raw) => BufMut::from_checked(raw),
            None => None,
        }
    }

    /// [`Leases::into_unique_as`], leaving the others.
    #[inline(always)]
    pub(crate) fn take_unique_as<T>(&mut self) -> (r: Option<BufMut<T, N>>) {
        match self.take_unique_raw() {
            Some(raw) => BufMut::from_checked(raw),
            None => None,
        }
    }
}

impl<N: Nic> Part<N> {
    /// A unique buffer, as a `BufMut<T>` if its bytes can hold `[T]`.
    #[inline(always)]
    pub(crate) fn unique_as<T>(self) -> (r: Option<BufMut<T, N>>) {
        match self.lease {
            Lease::Unique(raw) => BufMut::from_checked(raw),
            Lease::Shared(_) => None,
        }
    }
}

} // verus!

impl<T, N: Nic> Clone for Buf<T, N> {
    fn clone(&self) -> Self {
        self.clone_view()
    }
}

impl<N: Nic> Pd<N> {
    /// Allocates and registers a zeroed buffer of `n` elements.
    pub fn alloc<T: FromBytes>(&self, n: usize) -> Result<BufMut<T, N>> {
        if size_of::<T>() == 0 {
            return Err(Error::Invalid("zero-sized elements"));
        }
        let bytes = n
            .checked_mul(size_of::<T>())
            .ok_or(Error::Invalid("buffer size"))?;
        let whole = Region::whole(self, bytes, align_of::<T>())?;
        // Whole elements, in memory aligned for them: this always holds.
        BufMut::from_checked(whole).ok_or(Error::Invalid("buffer size"))
    }

    /// A ring of `capacity` registered bytes for streaming messages.
    pub fn ring_buf(&self, capacity: usize) -> Result<RingBuf<N>> {
        RingBuf::in_pd(self, capacity)
    }
}

impl<T, N: Nic> BufMut<T, N> {
    /// Splits into `[0, mid)` and `[mid, len)`.
    ///
    /// # Panics
    ///
    /// If `mid > len`.
    pub fn split_at(self, mid: usize) -> (BufMut<T, N>, BufMut<T, N>) {
        let len = self.len();
        match self.try_split_at(mid) {
            Ok(parts) => parts,
            Err(_) => panic!("split at {mid} of a buffer of {len}"),
        }
    }

    /// Splits into buffers of `n` elements (the last may be shorter).
    /// (This shadows `[T]::chunks`; `buf[..].chunks(n)` gives borrowed
    /// views instead.)
    ///
    /// # Panics
    ///
    /// If `n == 0`.
    pub fn chunks(self, n: usize) -> Chunks<T, N> {
        assert!(n > 0, "chunks of zero elements");
        Chunks {
            rest: Some(self),
            n,
        }
    }

    /// Freezes it into a shared, read-only [`Buf`].
    ///
    /// Interior mutation would race with a NIC reading a shared frozen buffer.
    ///
    /// ```compile_fail
    /// use core::cell::Cell;
    /// use safeverbs_core::BufMut;
    /// fn cannot_freeze_cells(buf: BufMut<Cell<u8>>) {
    ///     let _ = buf.freeze();
    /// }
    /// ```
    pub fn freeze(self) -> Buf<T, N>
    where
        T: Immutable,
    {
        self.frozen()
    }

    /// Exposes it to peers for RDMA WRITE, READ and atomics: registers it
    /// again with remote access and hands out its address and remote key
    /// ([`Exposed::remote`]). Until reclaimed it cannot be read as `[T]`.
    pub fn expose(self) -> Result<Exposed<T, N>, BufError<BufMut<T, N>>>
    where
        T: IntoBytes,
    {
        self.exposed()
    }

    /// Lends it to the NIC as one scatter/gather element (for receives and
    /// READs into it).
    ///
    /// A part may also be used as an outbound source by the synchronous API,
    /// so every byte must be initialized, including after Rust writes a value.
    ///
    /// # Panics
    ///
    /// If the byte length exceeds `u32::MAX`, the native SGE limit. Split a larger
    /// buffer before lending it.
    ///
    /// ```compile_fail
    /// use safeverbs_core::BufMut;
    /// fn cannot_lend_padding<T: zerocopy::FromBytes>(buf: BufMut<T>) {
    ///     let _ = buf.into_part();
    /// }
    /// ```
    #[inline]
    pub fn into_part(self) -> Part<N>
    where
        T: IntoBytes,
    {
        self.part()
    }

    /// Its bytes.
    pub fn as_bytes(&self) -> &[u8]
    where
        T: IntoBytes + Immutable,
    {
        (**self).as_bytes()
    }

    /// Its bytes viewed as a `U`, for types with invalid bit patterns;
    /// `None` if they are not a valid `U` (or the sizes or alignment
    /// differ).
    pub fn try_ref<U>(&self) -> Option<&U>
    where
        T: IntoBytes + Immutable,
        U: TryFromBytes + KnownLayout + Immutable,
    {
        U::try_ref_from_bytes(self.as_bytes()).ok()
    }
}

impl<T, N: Nic> Deref for BufMut<T, N> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        // SAFETY: unique access to aligned bytes holding valid `T` (see
        // `from_raw`); no request owns them while the `BufMut` exists.
        unsafe { core::slice::from_raw_parts(self.raw.ptr().cast::<T>(), self.len()) }
    }
}

impl<T, N: Nic> DerefMut for BufMut<T, N> {
    fn deref_mut(&mut self) -> &mut [T] {
        // SAFETY: as for `deref`, and `&mut self` is unique.
        unsafe { core::slice::from_raw_parts_mut(self.raw.ptr().cast::<T>(), self.len()) }
    }
}

impl<T, N: Nic> fmt::Debug for BufMut<T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BufMut({:p}, {} × {} bytes)",
            self.raw.ptr(),
            self.len(),
            size_of::<T>()
        )
    }
}

/// The buffers of [`BufMut::chunks`].
pub struct Chunks<T, N: Nic = Verbs> {
    rest: Option<BufMut<T, N>>,
    n: usize,
}

impl<T, N: Nic> Iterator for Chunks<T, N> {
    type Item = BufMut<T, N>;
    fn next(&mut self) -> Option<BufMut<T, N>> {
        let rest = self.rest.take()?;
        if rest.is_empty() {
            return None;
        }
        if rest.len() <= self.n {
            return Some(rest);
        }
        let (a, b) = rest.split_at(self.n);
        self.rest = Some(b);
        Some(a)
    }
}

impl<T, N: Nic> Buf<T, N> {
    /// The elements `range` of it, sharing the memory.
    ///
    /// # Panics
    ///
    /// If the range is not within it.
    #[inline]
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Buf<T, N> {
        let start = match range.start_bound() {
            Bound::Included(&s) => s,
            Bound::Excluded(&s) => s.checked_add(1).expect("range start overflow"),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&e) => e.checked_add(1).expect("range end overflow"),
            Bound::Excluded(&e) => e,
            Bound::Unbounded => self.len(),
        };
        self.slice_elements(start, end)
            .unwrap_or_else(|| panic!("range {start}..{end} of a buffer of {}", self.len()))
    }

    /// A part of it by reference, for a batch: what [`Buf::slice`] gives, with no reference to
    /// count, and so nothing to do for the posts of a request after the first.
    ///
    /// # Panics
    ///
    /// As [`Buf::slice`].
    pub fn view(&self, range: impl RangeBounds<usize>) -> BufView<'_, T, N> {
        let start = match range.start_bound() {
            Bound::Included(&s) => s,
            Bound::Excluded(&s) => s.checked_add(1).expect("range start overflow"),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&e) => e.checked_add(1).expect("range end overflow"),
            Bound::Excluded(&e) => e,
            Bound::Unbounded => self.len(),
        };
        self.view_elements(start, end)
            .unwrap_or_else(|| panic!("range {start}..{end} of a buffer of {}", self.len()))
    }

    /// Whether no other `Buf` and no request holds the frozen buffer.
    pub fn is_unique(&self) -> bool {
        Arc::strong_count(&self.frozen) == 1
    }

    /// Lends it to the NIC as one scatter/gather element (for sends and
    /// WRITEs from it).
    ///
    /// # Panics
    ///
    /// If the byte length exceeds `u32::MAX`, the native SGE limit. Slice a larger
    /// buffer before lending it.
    #[inline]
    pub fn into_part(self) -> Part<N>
    where
        T: IntoBytes + Immutable,
    {
        match self.try_part() {
            Ok(part) => part,
            Err(view) => {
                // Released first, so that no `Result` stays alive across the panic: the
                // `Part` is then built in place.
                drop(view);
                panic!("a single SGE exceeds u32::MAX bytes")
            }
        }
    }

    /// Lends this frozen view as one SGE, returning it unchanged if its byte
    /// length exceeds `u32::MAX`. Slice a larger view before lending it.
    #[inline]
    pub fn try_into_part(self) -> Result<Part<N>, Self>
    where
        T: IntoBytes + Immutable,
    {
        self.try_part()
    }

    /// Its bytes.
    pub fn as_bytes(&self) -> &[u8]
    where
        T: IntoBytes + Immutable,
    {
        (**self).as_bytes()
    }

    /// Its bytes viewed as a `U`, for types with invalid bit patterns;
    /// `None` if they are not a valid `U` (or the sizes or alignment
    /// differ).
    pub fn try_ref<U>(&self) -> Option<&U>
    where
        T: IntoBytes + Immutable,
        U: TryFromBytes + KnownLayout + Immutable,
    {
        U::try_ref_from_bytes(self.as_bytes()).ok()
    }
}

impl<U: IntoBytes + Immutable, N: Nic> Lend<N> for Buf<U, N> {
    #[inline(always)]
    fn peek(&self) -> Peek<'_, N> {
        self.peek_bytes()
    }
    #[inline(always)]
    fn lend(self) -> Part<N> {
        self.into_part()
    }
}

impl<U: IntoBytes + Immutable, N: Nic> Lend<N> for &Buf<U, N> {
    #[inline(always)]
    fn peek(&self) -> Peek<'_, N> {
        self.peek_bytes()
    }
    #[inline(always)]
    fn lend(self) -> Part<N> {
        self.clone().into_part()
    }
}

impl<U: IntoBytes + Immutable, N: Nic> Lend<N> for BufView<'_, U, N> {
    #[inline(always)]
    fn peek(&self) -> Peek<'_, N> {
        self.peek_bytes()
    }
    #[inline(always)]
    fn lend(self) -> Part<N> {
        self.to_buf().into_part()
    }
}

impl<T, N: Nic> Deref for Buf<T, N> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        // SAFETY: frozen bytes are only ever read (by Rust and by the NIC's
        // sends) until thawed, which needs every `Buf` gone; they hold valid
        // `T` and the offset stays aligned.
        unsafe { core::slice::from_raw_parts(self.view_ptr().cast::<T>(), self.len()) }
    }
}

impl<T, N: Nic> fmt::Debug for Buf<T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Buf({:p}+{}, {} × {} bytes)",
            self.frozen.raw.ptr(),
            self.view.offset(),
            self.len(),
            size_of::<T>()
        )
    }
}

verus! {

/// A buffer exposed to peers: they may RDMA WRITE, READ and run atomics on
/// it with [`Exposed::remote`], at any moment, so no reference to its bytes
/// ever exists. [`Exposed::copy_to_slice`] copies them out while it stays
/// exposed; [`Exposed::reclaim`] revokes the peers' access and gives it back
/// as a [`BufMut`].
#[verifier::reject_recursive_types(N)]
pub struct Exposed<T, N: Nic = Verbs> {
    /// The buffer and its registration for remote access. Only `try_reclaim` and `drop` take
    /// them, from an `Exposed` they consume.
    inner: Option<Exposure<T, N>>,
}

/// An exposed buffer and its registration.
#[verifier::reject_recursive_types(N)]
struct Exposure<T, N: Nic> {
    /// Deregistered before the buffer goes, or kept with it if that fails.
    mr: N::Mr,
    buf: BufMut<T, N>,
    rkey: u32,
}

impl<T, N: Nic> Exposed<T, N> {
    /// The buffer it holds for the peer.
    pub closed spec fn held(self) -> Option<BufMut<T, N>> {
        match self.inner {
            Some(e) => Some(e.buf),
            None => None,
        }
    }

    /// The bytes it opens to the peer.
    pub closed spec fn bytes(self) -> ISet<int> {
        match self.inner {
            Some(e) => e.buf.byte_domain().to_iset(),
            None => ISet::empty(),
        }
    }

    /// Revokes the peers' access and gives the buffer back: A's `unexpose`, the buffer the
    /// program's again, as it was exposed. A refused revocation returns the exposed owner, whose
    /// registration remains live, for retry.
    pub fn try_reclaim(self) -> (r: Result<BufMut<T, N>, BufError<Self>>)
        ensures
            r matches Ok(buf) ==> self.held() == Some(buf),
            r matches Err(e) ==> e.buf.held() == self.held(),
    {
        let mut this = self;
        match this.inner.take() {
            Some(exposure) => {
                let Exposure { mr: registration, buf: owner, rkey: key } = exposure;
                match close_exposure::<N>(registration) {
                    Ok(()) => Ok(owner),
                    Err((error, mr)) => {
                        this.inner = Some(Exposure { mr, buf: owner, rkey: key });
                        Err(BufError { error, buf: this })
                    },
                }
            },
            None => Err(BufError { error: Error::Invalid("an exposure already reclaimed"), buf: this }),
        }
    }
}

impl<T, N: Nic> Exposed<T, N> {
    /// The exposed buffer is apart from every lease the requests of `engine` hold: its linear
    /// permission, borrowed exclusively against the engine's rings. It changes nothing, and exists
    /// in Verus builds alone.
    #[cfg(verus_keep_ghost)]
    pub(crate) fn separate_from<C>(&mut self, engine: &crate::hw::Hw<C, N>)
        requires
            engine.inv(),
        ensures
            *final(self) == *old(self),
            forall|l: LeaseId| #[trigger] lent(engine.part().abs, l)
                ==> engine.part().abs.leases[l].mem.disjoint(old(self).bytes()),
    {
        match self.inner.take() {
            Some(exposure) => {
                let Exposure { mr, buf, rkey } = exposure;
                proof {
                    use_type_invariant(&buf);
                }
                let BufMut { raw, _t } = buf;
                let raw = lease::unique_apart_from_engine(raw, engine);
                self.inner = Some(Exposure { mr, buf: BufMut { raw, _t }, rkey });
            },
            None => {},
        }
    }

    /// The exposed buffer owns no byte that `other` owns: their linear permissions, its own borrowed
    /// exclusively. It changes nothing, and exists in Verus builds alone.
    #[cfg(verus_keep_ghost)]
    fn separate_raw(&mut self, other: &RawMut<N>)
        requires
            other.inv(),
        ensures
            *final(self) == *old(self),
            old(self).bytes().disjoint(other.byte_domain().to_iset()),
    {
        match self.inner.take() {
            Some(exposure) => {
                let Exposure { mr, buf, rkey } = exposure;
                proof {
                    use_type_invariant(&buf);
                }
                let BufMut { raw, _t } = buf;
                let mut raw = raw;
                proof {
                    use_type_invariant(&raw);
                }
                raw.disjoint(other);
                proof {
                    assert forall|b: int| !(raw.byte_domain().to_iset().contains(b) && other.byte_domain().to_iset().contains(b)) by {
                        assert(!(raw.byte_domain().contains(b) && other.byte_domain().contains(b)));
                    }
                }
                self.inner = Some(Exposure { mr, buf: BufMut { raw, _t }, rkey });
            },
            None => {},
        }
    }

    /// Two exposed buffers own different bytes. It changes nothing, and exists in Verus builds alone.
    #[cfg(verus_keep_ghost)]
    pub(crate) fn separate_exposed<U>(&mut self, other: &Exposed<U, N>)
        ensures
            *final(self) == *old(self),
            old(self).bytes().disjoint(other.bytes()),
    {
        match &other.inner {
            Some(e) => {
                proof {
                    use_type_invariant(&e.buf);
                    use_type_invariant(&e.buf.raw);
                }
                self.separate_raw(&e.buf.raw);
            },
            None => {},
        }
    }

    /// An exposed buffer owns no byte of a unique buffer the program holds. It changes nothing, and
    /// exists in Verus builds alone.
    #[cfg(verus_keep_ghost)]
    pub(crate) fn separate_buf_mut<U>(&mut self, other: &BufMut<U, N>)
        ensures
            *final(self) == *old(self),
            old(self).bytes().disjoint(other.byte_domain().to_iset()),
    {
        proof {
            use_type_invariant(other);
            use_type_invariant(&other.raw);
        }
        self.separate_raw(&other.raw);
    }

    /// An exposed buffer owns no byte of a frozen buffer the program holds. It changes nothing, and
    /// exists in Verus builds alone.
    #[cfg(verus_keep_ghost)]
    pub(crate) fn separate_buf<U>(&mut self, other: &Buf<U, N>)
        ensures
            *final(self) == *old(self),
            old(self).bytes().disjoint(other.byte_domain().to_iset()),
    {
        proof {
            use_type_invariant(other);
            other.lemma_byte_backing();
            use_type_invariant(&*other.frozen);
            other.frozen.lemma_contents();
        }
        self.separate_raw(&other.frozen.raw);
        proof {
            assert forall|b: int| !(old(self).bytes().contains(b) && other.byte_domain().to_iset().contains(b)) by {
                if other.byte_domain().contains(b) {
                    assert((*other.frozen).byte_domain().contains(b));
                }
            }
        }
    }
}

impl<T, N: Nic> BufMut<T, N> {
    /// Registers the buffer for remote access, all of its bytes and no others, and hands it to the
    /// peer: A's `expose`. The program keeps no way to its bytes but the volatile copy.
    pub(crate) fn exposed(self) -> (r: Result<Exposed<T, N>, BufError<BufMut<T, N>>>)
        ensures
            r matches Ok(x) ==> x.held() == Some(self),
            r matches Err(e) ==> e.buf == self,
    {
        let registered = register_exposure(&self.raw.region.pd, self.raw.ptr(), self.raw.len());
        match registered {
            Ok((mr, rkey)) => Ok(Exposed { inner: Some(Exposure { mr, buf: self, rkey }) }),
            Err(error) => Err(BufError { error, buf: self }),
        }
    }
}

} // verus!

impl<T, N: Nic> Exposed<T, N> {
    /// What a peer needs to address it.
    pub fn remote(&self) -> RemoteBuf {
        match &self.inner {
            Some(e) => RemoteBuf {
                addr: e.buf.raw.ptr() as u64,
                len: e.buf.raw.len() as u64,
                rkey: e.rkey,
            },
            None => RemoteBuf {
                addr: 0,
                len: 0,
                rkey: 0,
            },
        }
    }

    /// The number of elements.
    pub fn len(&self) -> usize {
        match &self.inner {
            Some(e) => e.buf.len(),
            None => 0,
        }
    }

    /// Whether it has no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Copies its first `out.len()` elements into `out` with volatile reads,
    /// while peers keep their access and their remote key stays valid. The
    /// bytes are read as they are: `T` accepts any bytes, so this is safe
    /// whatever the peer does.
    ///
    /// The copy may be torn, if a peer writes meanwhile. A protocol learns
    /// that data is complete from a write-with-immediate notification
    /// ([`Received::WriteNotification`](crate::wc::Received::WriteNotification))
    /// or a SEND, then copies; never by polling these bytes: on some transports
    /// (MRC) data lands out of order, so a last byte that arrived says nothing
    /// of the others.
    ///
    /// # Panics
    ///
    /// If `out` is longer than it.
    pub fn copy_to_slice(&self, out: &mut [T])
    where
        T: FromBytes + IntoBytes,
    {
        assert!(
            out.len() <= self.len(),
            "{} elements out of {}",
            out.len(),
            self.len()
        );
        if let Some(e) = &self.inner {
            memory::volatile_copy(e.buf.raw.ptr(), out.as_mut_bytes());
        }
    }

    /// Revokes the peers' access (invalidating the remote key) and gives the
    /// buffer back. Use [`Self::try_reclaim`] to retain the owner on failure.
    ///
    /// # Panics
    ///
    /// If the backend refuses revocation. Its backing resources are retained
    /// whenever a subsequent cleanup attempt also fails.
    pub fn reclaim(self) -> BufMut<T, N> {
        match self.try_reclaim() {
            Ok(buf) => buf,
            Err(error) => panic!("cannot reclaim exposed memory: {}", error.error),
        }
    }
}

impl<T, N: Nic> Drop for Exposed<T, N> {
    fn drop(&mut self) {
        if let Some(exposure) = self.inner.take() {
            match close_exposure::<N>(exposure.mr) {
                Ok(()) => drop(exposure.buf),
                // The peer may still reach the bytes: keep them, with the registration.
                Err((_, mr)) => core::mem::forget((mr, exposure.buf)),
            }
        }
    }
}

impl<T, N: Nic> fmt::Debug for Exposed<T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Exposed({:?})", self.remote())
    }
}

/// An error that hands back the buffer of the operation that failed.
pub struct BufError<B> {
    /// What went wrong.
    pub error: Error,
    /// The owner returned on failure. It retains the operation's access state;
    /// for example, failed revocation returns a still-exposed buffer.
    pub buf: B,
}

impl<B> fmt::Debug for BufError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl<B> fmt::Display for BufError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, f)
    }
}

impl<B> std::error::Error for BufError<B> {}

impl<B> From<BufError<B>> for Error {
    fn from(e: BufError<B>) -> Error {
        e.error
    }
}

impl<B> From<BufError<B>> for std::io::Error {
    fn from(e: BufError<B>) -> std::io::Error {
        e.error.into()
    }
}

impl<N: Nic> fmt::Debug for Leases<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Leases({})", self.len())
    }
}

impl<N: Nic> Default for Leases<N> {
    fn default() -> Self {
        Leases::new()
    }
}

impl<N: Nic> Leases<N> {
    /// Takes back the first unique buffer (of a receive, READ or atomic) as
    /// a `BufMut<T>`; `None` if there is none or its bytes cannot hold
    /// `[T]`.
    #[inline]
    pub fn into_buf_mut<T: FromBytes>(self) -> Option<BufMut<T, N>> {
        self.into_unique_as()
    }

    /// Takes the first unique buffer out as a `BufMut<T>`, leaving the others; `None` if there is
    /// none or its bytes cannot hold `[T]` (it is released then).
    #[inline(always)]
    pub fn take_buf_mut<T: FromBytes>(&mut self) -> Option<BufMut<T, N>> {
        self.take_unique_as()
    }
}

impl<N: Nic> From<Part<N>> for Leases<N> {
    fn from(part: Part<N>) -> Leases<N> {
        let mut leases = Leases::default();
        leases.push(part);
        leases
    }
}

impl<N: Nic> Part<N> {
    /// Gives back a unique buffer as a `BufMut<T>`; `None` for a frozen one
    /// or if its bytes cannot hold `[T]`.
    pub fn into_buf_mut<T: FromBytes>(self) -> Option<BufMut<T, N>> {
        self.unique_as()
    }
}

verus! {

/// The bytes `.0..1` of a ring that a chunk holds.
spec fn span_of<N: Nic>(chunk: Arc<Frozen<N>>) -> (int, int) {
    let g = chunk.raw.range.geometry();
    (g.spec_offset() as int, g.spec_offset() + g.spec_len())
}

/// The chunks of a ring, oldest first, lie in its order: each part in increasing order, the ones
/// before `wrap` above the others. A head that wrapped never reaches the tail, and `wrap` is 0 if it
/// did not wrap.
spec fn in_order(s: Seq<(int, int)>, wrap: int, size: int) -> bool {
    &&& wrap == 0 || 0 < wrap < s.len()
    &&& forall|i: int| 0 <= i < s.len() ==> 0 <= (#[trigger] s[i]).0 <= s[i].1 <= size
    &&& forall|i: int, j: int| #![trigger s[i], s[j]] 0 <= i < j < wrap ==> s[i].1 <= s[j].0
    &&& forall|i: int, j: int| #![trigger s[i], s[j]] wrap <= i < j < s.len() ==> s[i].1 <= s[j].0
    &&& forall|i: int, j: int| #![trigger s[i], s[j]] 0 <= i < wrap <= j < s.len() ==> s[j].1 <= s[i].0
    &&& wrap != 0 ==> s[s.len() - 1].1 < s[0].0
}

/// The oldest chunk gone, the others are in order.
proof fn lemma_older_gone(s: Seq<(int, int)>, wrap: int, size: int)
    requires
        in_order(s, wrap, size),
        s.len() > 0,
    ensures
        in_order(s.drop_first(), if wrap == 0 { 0 } else { wrap - 1 }, size),
{
    let t = s.drop_first();
    let w = if wrap == 0 { 0 } else { wrap - 1 };
    assert forall|i: int| 0 <= i < t.len() implies 0 <= (#[trigger] t[i]).0 <= t[i].1 <= size by {
        assert(t[i] == s[i + 1]);
    }
    assert forall|i: int, j: int| #![trigger t[i], t[j]] 0 <= i < j < w implies t[i].1 <= t[j].0 by {
        assert(t[i] == s[i + 1] && t[j] == s[j + 1]);
    }
    assert forall|i: int, j: int| #![trigger t[i], t[j]] w <= i < j < t.len() implies t[i].1 <= t[j].0 by {
        assert(t[i] == s[i + 1] && t[j] == s[j + 1]);
    }
    assert forall|i: int, j: int| #![trigger t[i], t[j]] 0 <= i < w <= j < t.len() implies t[j].1 <= t[i].0 by {
        assert(t[i] == s[i + 1] && t[j] == s[j + 1]);
    }
    if w != 0 {
        assert(t[t.len() - 1] == s[s.len() - 1]);
        assert(t[0] == s[1]);
        assert(s[0].1 <= s[1].0);
    }
}

/// Two chunks in order hold different bytes, and each lies between the tail and the head as `place`
/// reads them.
proof fn lemma_positions(s: Seq<(int, int)>, wrap: int, size: int)
    requires
        in_order(s, wrap, size),
        s.len() > 0,
    ensures
        forall|i: int, j: int| #![trigger s[i], s[j]] 0 <= i < s.len() && 0 <= j < s.len() && i != j
            ==> s[i].1 <= s[j].0 || s[j].1 <= s[i].0,
        wrap == 0 ==> s[0].0 <= s[s.len() - 1].1,
        wrap == 0 ==> forall|i: int| 0 <= i < s.len() ==> s[0].0 <= #[trigger] s[i].0 && s[i].1 <= s[s.len() - 1].1,
        wrap != 0 ==> forall|i: int| 0 <= i < wrap ==> s[0].0 <= #[trigger] s[i].0,
        wrap != 0 ==> forall|i: int| wrap <= i < s.len() ==> #[trigger] s[i].1 <= s[s.len() - 1].1,
{
    assert forall|i: int, j: int| #![trigger s[i], s[j]] 0 <= i < s.len() && 0 <= j < s.len() && i != j
        implies s[i].1 <= s[j].0 || s[j].1 <= s[i].0 by {
        assert(s[i].0 <= s[i].1);
        assert(s[j].0 <= s[j].1);
    }
    assert(s[0].0 <= s[0].1);
    assert(s[s.len() - 1].0 <= s[s.len() - 1].1);
    if wrap == 0 {
        assert forall|i: int| 0 <= i < s.len() implies s[0].0 <= #[trigger] s[i].0 && s[i].1 <= s[s.len() - 1].1 by {
            assert(s[i].0 <= s[i].1);
            assert(s[s.len() - 1].0 <= s[s.len() - 1].1);
            if i != 0 {
                assert(s[0].1 <= s[i].0);
            }
            if i != s.len() - 1 {
                assert(s[i].1 <= s[s.len() - 1].0);
            }
        }
    } else {
        assert forall|i: int| 0 <= i < wrap implies s[0].0 <= #[trigger] s[i].0 by {
            assert(s[0].0 <= s[0].1);
            if i != 0 {
                assert(s[0].1 <= s[i].0);
            }
        }
        assert forall|i: int| wrap <= i < s.len() implies #[trigger] s[i].1 <= s[s.len() - 1].1 by {
            assert(s[s.len() - 1].0 <= s[s.len() - 1].1);
            if i != s.len() - 1 {
                assert(s[i].1 <= s[s.len() - 1].0);
            }
        }
    }
}

/// A chunk after the others is in order, with `wrap` as [`RingBuf::place`] says.
proof fn lemma_next(s: Seq<(int, int)>, wrap: int, size: int, next: (int, int), wrap2: int)
    requires
        in_order(s, wrap, size),
        0 <= next.0 <= next.1 <= size,
        wrap2 == wrap || (wrap == 0 && wrap2 == s.len() && s.len() > 0),
        s.len() == 0 ==> wrap2 == 0,
        wrap2 == wrap && s.len() > 0 ==> s[s.len() - 1].1 <= next.0,
        wrap2 != 0 ==> next.1 < s[0].0,
        wrap2 == s.len() && s.len() > 0 ==> next.0 == 0,
        wrap != 0 ==> next.1 < s[0].0,
    ensures
        in_order(s.push(next), wrap2, size),
{
    let t = s.push(next);
    if s.len() > 0 {
        lemma_positions(s, wrap, size);
    }
    assert forall|i: int| 0 <= i < t.len() implies 0 <= (#[trigger] t[i]).0 <= t[i].1 <= size by {
        if i < s.len() {
            assert(t[i] == s[i]);
        } else {
            assert(t[i] == next);
        }
    }
    assert forall|i: int, j: int| #![trigger t[i], t[j]] 0 <= i < j < wrap2 implies t[i].1 <= t[j].0 by {
        assert(t[i] == s[i] && t[j] == s[j]);
        if wrap == 0 {
            assert(s[i].1 <= s[j].0);
        }
    }
    assert forall|i: int, j: int| #![trigger t[i], t[j]] wrap2 <= i < j < t.len() implies t[i].1 <= t[j].0 by {
        assert(t[i] == s[i]);
        if j < s.len() {
            assert(t[j] == s[j]);
        } else {
            assert(t[j] == next);
        }
    }
    assert forall|i: int, j: int| #![trigger t[i], t[j]] 0 <= i < wrap2 <= j < t.len() implies t[j].1 <= t[i].0 by {
        assert(t[i] == s[i]);
        if j < s.len() {
            assert(t[j] == s[j]);
        } else {
            assert(t[j] == next);
        }
    }
    if wrap2 != 0 {
        assert(t[t.len() - 1] == next);
        assert(t[0] == s[0]);
    }
}

/// Reads whether anything but the ring holds a chunk (no meaning in the proofs: what frees a chunk
/// is [`thaw`](memory::thaw), which says for itself).
#[verifier::external_body]
#[inline(always)]
fn chunk_held<N: Nic>(chunk: &Arc<Frozen<N>>) -> bool {
    Arc::strong_count(chunk) != 1
}

/// A unique buffer's bytes, to fill: the reference `DerefMut` makes (`docs/verification.md`, trusted
/// computing base). Filling them changes nothing the proofs read.
#[verifier::external_body]
#[inline(always)]
fn bytes_to_fill<N: Nic>(buf: &mut BufMut<u8, N>) -> (bytes: &mut [u8])
    ensures
        *final(buf) == *old(buf),
{
    &mut *buf
}

/// A ring of registered bytes for streaming variable-size messages: each
/// message is written at the head into a chunk of its own, frozen and sent;
/// a chunk's space is reused once every `Buf` of it is gone (its requests
/// completed and the caller dropped its copies), oldest first.
#[verifier::reject_recursive_types(N)]
pub struct RingBuf<N: Nic = Verbs> {
    region: Arc<Region<N>>,
    /// Bytes it may place: all of them, or none while a message is filled, and so for good if the
    /// program's `fill` panicked. It holds on to what it knows for no longer than that.
    room: usize,
    /// The chunks handed out and not reclaimed, oldest first.
    chunks: VecDeque<Arc<Frozen<N>>>,
    /// The permission of its bytes that no chunk holds.
    #[allow(dead_code)]
    free: Tracked<BytePermission>,
    /// How many of its chunks lie above where the head wrapped (`in_order`).
    #[allow(dead_code)]
    wrap: Ghost<int>,
}

impl<N: Nic> RingBuf<N> {
    /// Its chunks in order, and its free permission for its bytes that no chunk holds. So a chunk
    /// cut from where `place` says takes a permission that exists.
    #[verifier::type_invariant]
    closed spec fn inv(self) -> bool {
        let region = *self.region;
        let chunks = self.chunks@;
        &&& self.free@.inv()
        &&& self.free@.provenance() == region.ptr@.provenance
        &&& region.size <= isize::MAX
        &&& region.ptr.addr() != 0
        &&& region.ptr.addr() as int + region.size <= usize::MAX
        &&& forall|i: int| 0 <= i < chunks.len() ==> {
            &&& (*#[trigger] chunks[i]).inv()
            &&& *chunks[i].raw.region == region
        }
        &&& self.room != 0 ==> {
            &&& self.room == region.size
            &&& in_order(chunks.map_values(|c: Arc<Frozen<N>>| span_of(c)), self.wrap@, region.size as int)
            &&& forall|x: int| #[trigger] self.free@.dom().contains(x) <==> self.is_free(x)
        }
    }

    /// Byte `x` is one of its, and no chunk holds it.
    closed spec fn is_free(self, x: int) -> bool {
        let base = (*self.region).ptr.addr() as int;
        let chunks = self.chunks@;
        &&& base <= x < base + (*self.region).size
        &&& forall|i: int| 0 <= i < chunks.len() ==> !(span_of(#[trigger] chunks[i]).0 <= x - base < span_of(chunks[i]).1)
    }

    /// A ring of `capacity` registered bytes in `pd`.
    fn in_pd(pd: &Pd<N>, capacity: usize) -> (r: Result<RingBuf<N>>) {
        if capacity == 0 {
            return Err(Error::Invalid("an empty buffer"));
        }
        let whole = match Region::allocated(pd, capacity, REGION_ALIGN) {
            Ok(whole) => whole,
            Err(e) => return Err(e),
        };
        proof {
            use_type_invariant(&whole);
            whole.lemma_in_region();
        }
        let ghost geometry = whole.range.geometry();
        let (region, free) = whole.into_parts();
        let chunks = VecDeque::new();
        let ghost wrap = 0int;
        let ring = RingBuf { region, room: capacity, chunks, free, wrap: Ghost(wrap) };
        Ok(ring)
    }

    /// Its capacity in bytes.
    pub fn capacity(&self) -> (n: usize) {
        self.region.size
    }

    /// Drops the oldest chunks nobody holds any more: their permissions are free again.
    fn reclaim(&mut self) {
        proof {
            use_type_invariant(&*self);
        }
        let size = self.room;
        if size == 0 {
            return;
        }
        while self.chunks.len() > 0
            invariant
                size == self.room,
                size != 0,
            decreases self.chunks@.len(),
        {
            proof {
                use_type_invariant(&*self);
            }
            if chunk_held(&self.chunks[0]) {
                return;
            }
            let ghost before = *self;
            let ghost spans = before.chunks@.map_values(|c: Arc<Frozen<N>>| span_of(c));
            self.room = 0;
            let mut chunks = replace_value(&mut self.chunks, VecDeque::new());
            let front = match chunks.pop_front() {
                Some(front) => front,
                None => {
                    self.chunks = chunks;
                    self.room = size;
                    proof {
                        assert(false);
                    }
                    return;
                },
            };
            match memory::thaw(front) {
                Ok(raw) => {
                    proof {
                        use_type_invariant(&raw);
                        raw.lemma_in_region();
                    }
                    let (_region, _freed) = raw.into_parts();
                    proof {
                        lemma_older_gone(spans, before.wrap@, size as int);
                        lemma_positions(spans, before.wrap@, size as int);
                        let base = (*before.region).ptr.addr() as int;
                        let span = span_of(before.chunks@[0]);
                        assert(spans[0] == span);
                        assert(_freed@.dom() == Set::range(base + span.0, base + span.1));
                        assert forall|x: int| #[trigger] _freed@.dom().contains(x) implies !self.free@.dom().contains(x) by {
                            assert(!before.is_free(x));
                        }
                        assert(self.free@.dom().disjoint(_freed@.dom()));
                        self.free.borrow_mut().put_back(_freed.get());
                    }
                    self.chunks = chunks;
                    self.wrap = Ghost(if before.wrap@ == 0 { 0int } else { before.wrap@ - 1 });
                    proof {
                        let later = self.chunks@;
                        assert(later =~= before.chunks@.drop_first());
                        assert(later.map_values(|c: Arc<Frozen<N>>| span_of(c)) =~= spans.drop_first());
                        assert forall|x: int| #[trigger] self.free@.dom().contains(x) <==> self.is_free(x) by {
                            let base = (*before.region).ptr.addr() as int;
                            assert forall|i: int| 0 <= i < later.len() implies #[trigger] later[i] == before.chunks@[i + 1] by {}
                            let span = span_of(before.chunks@[0]);
                            if self.is_free(x) && !before.is_free(x) {
                                let i = choose|i: int| 0 <= i < before.chunks@.len() && span_of(#[trigger] before.chunks@[i]).0 <= x - base < span_of(before.chunks@[i]).1;
                                if i != 0 {
                                    assert(later[i - 1] == before.chunks@[i]);
                                }
                            }
                            if before.is_free(x) {
                                assert forall|i: int| 0 <= i < later.len() implies !(span_of(#[trigger] later[i]).0 <= x - base < span_of(later[i]).1) by {
                                    assert(later[i] == before.chunks@[i + 1]);
                                }
                            }
                            if span.0 <= x - base < span.1 {
                                assert forall|i: int| 0 <= i < later.len() implies !(span_of(#[trigger] later[i]).0 <= x - base < span_of(later[i]).1) by {
                                    assert(later[i] == before.chunks@[i + 1]);
                                    assert(spans[i + 1] == span_of(later[i]));
                                }
                                assert(span_of(before.chunks@[0]) == span);
                            }
                        }
                    }
                    self.room = size;
                },
                Err(front) => {
                    chunks.push_front(front);
                    self.chunks = chunks;
                    proof {
                        assert(self.chunks@ =~= before.chunks@);
                    }
                    self.room = size;
                    return;
                },
            }
        }
    }

    /// Where `len` contiguous free bytes start, if anywhere, and the `wrap` of the chunks with a
    /// chunk of them after the others.
    fn place(&mut self, len: usize) -> (r: Option<(usize, Ghost<int>)>)
        ensures
            r matches Some((at, wrap)) ==> {
                &&& final(self).room != 0
                &&& at + len <= final(self).room
                &&& in_order(
                    final(self).chunks@.map_values(|c: Arc<Frozen<N>>| span_of(c)).push((at as int, at + len)),
                    wrap@,
                    final(self).room as int,
                )
            },
    {
        self.reclaim();
        proof {
            use_type_invariant(&*self);
        }
        let size = self.room;
        if size == 0 {
            return None;
        }
        let n = self.chunks.len();
        let ghost spans = self.chunks@.map_values(|c: Arc<Frozen<N>>| span_of(c));
        let ghost wrap = self.wrap@;
        if n == 0 {
            if len > size {
                return None;
            }
            proof {
                assert(spans =~= Seq::<(int, int)>::empty());
                lemma_next(spans, wrap, size as int, (0, len as int), 0);
            }
            return Some((0, Ghost(0int)));
        }
        proof {
            lemma_positions(spans, wrap, size as int);
            assert(spans[0] == span_of(self.chunks@[0]));
            assert(spans[n - 1] == span_of(self.chunks@[n - 1]));
        }
        // The ring owns its whole region: offsets are positions in the ring.
        let tail = self.chunks[0].raw.offset();
        let head = self.chunks[n - 1].raw.end();
        // Chunks lie from `tail` to `head`, or wrapped from `tail` to the end
        // and from the start to `head`. A wrapped head never reaches the
        // tail (strict comparisons), so `head >= tail` means not wrapped.
        if head >= tail {
            if size - head >= len {
                proof {
                    lemma_next(spans, wrap, size as int, (head as int, head + len), wrap);
                }
                Some((head, Ghost(wrap)))
            } else if tail > len {
                proof {
                    lemma_next(spans, wrap, size as int, (0, len as int), n as int);
                }
                Some((0, Ghost(n as int)))
            } else {
                None
            }
        } else if tail - head > len {
            proof {
                lemma_next(spans, wrap, size as int, (head as int, head + len), wrap);
            }
            Some((head, Ghost(wrap)))
        } else {
            None
        }
    }

    /// Writes a message of `len` bytes, filled by `fill`, at the head and
    /// returns it frozen; `None` while the ring has no room for it.
    ///
    /// If `fill` panics, the ring takes no more messages, and its chunks stay what they are: the
    /// message's bytes went with its permission.
    ///
    /// That every `F` may be called is what a program's closure is: only Verus can write one that
    /// asks more.
    pub fn write_with<F: FnOnce(&mut [u8])>(&mut self, len: usize, fill: F) -> (r: Option<Buf<u8, N>>)
        requires
            forall|any: F, bytes: &mut [u8]| any.requires((bytes,)),
    {
        let (at, wrap) = match self.place(len) {
            Some(placed) => placed,
            None => return None,
        };
        proof {
            use_type_invariant(&*self);
        }
        let size = self.room;
        let ghost spans = self.chunks@.map_values(|c: Arc<Frozen<N>>| span_of(c));
        let ghost base_addr = self.region.ptr.addr() as int;
        proof {
            // The bytes are free: no chunk holds them.
            let next = (at as int, at + len);
            let more = spans.push(next);
            assert(more[spans.len() as int] == next);
            if more.len() > 0 {
                lemma_positions(more, wrap@, size as int);
            }
            assert forall|x: int| Set::range(base_addr + at, base_addr + at + len).contains(x) implies #[trigger] self.free@.dom().contains(x) by {
                assert forall|i: int| 0 <= i < self.chunks@.len() implies !(span_of(#[trigger] self.chunks@[i]).0 <= x - base_addr < span_of(self.chunks@[i]).1) by {
                    assert(more[i] == spans[i]);
                    assert(spans[i] == span_of(self.chunks@[i]));
                    assert(more[i].1 <= more[spans.len() as int].0 || more[spans.len() as int].1 <= more[i].0);
                }
                assert(self.is_free(x));
            }
        }
        let ghost keys = Set::range(base_addr + at, base_addr + at + len);
        let ghost start = *self;
        self.room = 0;
        let tracked part = self.free.borrow_mut().take(keys);
        let region = self.region.clone();
        proof {
            use_type_invariant(&region);
        }
        let range = OwnedRange::of_permission(region.ptr, at, len, size, Tracked(part));
        let raw = RawMut::new(region, range);
        let mut buf = BufMut::of_bytes(raw);
        fill(bytes_to_fill(&mut buf));
        let frozen = buf.frozen();
        let ghost before_chunks = self.chunks@;
        let mut chunks = replace_value(&mut self.chunks, VecDeque::new());
        chunks.push_back(frozen.frozen.clone());
        proof {
            assert forall|i: int| 0 <= i < chunks@.len() implies {
                &&& (*#[trigger] chunks@[i]).inv()
                &&& *chunks@[i].raw.region == *self.region
            } by {
                if i < before_chunks.len() {
                    assert(chunks@[i] == before_chunks[i]);
                }
            }
        }
        self.chunks = chunks;
        self.wrap = wrap;
        proof {
            let later = self.chunks@;
            let n = before_chunks.len() as int;
            assert forall|i: int| 0 <= i < n implies #[trigger] later[i] == before_chunks[i] by {}
            assert(span_of(later[n]) == (at as int, at + len));
            assert(later.map_values(|c: Arc<Frozen<N>>| span_of(c)) =~= spans.push((at as int, at + len)));
            assert forall|x: int| #[trigger] self.free@.dom().contains(x) <==> self.is_free(x) by {
                if self.free@.dom().contains(x) {
                    assert(start.free@.dom().contains(x));
                    assert(start.is_free(x));
                    assert(!keys.contains(x));
                    assert forall|i: int| 0 <= i < later.len() implies !(span_of(#[trigger] later[i]).0 <= x - base_addr < span_of(later[i]).1) by {
                        if i < n {
                            assert(later[i] == before_chunks[i]);
                        }
                    }
                }
                if self.is_free(x) {
                    assert forall|i: int| 0 <= i < n implies !(span_of(#[trigger] before_chunks[i]).0 <= x - base_addr < span_of(before_chunks[i]).1) by {
                        assert(later[i] == before_chunks[i]);
                    }
                    assert(start.is_free(x));
                    assert(start.free@.dom().contains(x));
                    assert(!(span_of(later[n]).0 <= x - base_addr < span_of(later[n]).1));
                    assert(!keys.contains(x));
                }
            }
        }
        self.room = size;
        Some(frozen)
    }
}

} // verus!

impl<N: Nic> RingBuf<N> {
    /// Bytes in chunks not reclaimed yet.
    pub fn in_use(&mut self) -> usize {
        self.reclaim();
        let mut n = 0;
        for c in &self.chunks {
            n += c.raw.len();
        }
        n
    }

    /// Copies `message` in at the head and returns it frozen; `None` while
    /// the ring has no room for it.
    pub fn push(&mut self, message: &[u8]) -> Option<Buf<u8, N>> {
        self.write_with(message.len(), |b| b.copy_from_slice(message))
    }
}

#[cfg(test)]
mod revocation_tests {
    use super::*;
    use crate::model::Model;
    use crate::pd::Context;

    #[test]
    fn refused_reclaim_returns_the_live_exposed_owner_for_retry() {
        let model = Model::new(51);
        let ctx = Context::new(model.clone());
        let pd = ctx.alloc_pd().unwrap();
        let mut buf = pd.alloc::<u8>(16).unwrap();
        buf.copy_from_slice(b"reclaim on retry");
        let exposed = buf.expose().unwrap();
        let remote = exposed.remote();
        let key = Model::mr_keys(&exposed.inner.as_ref().unwrap().mr).0;
        model.refuse_mr_closes(key, 2);
        let mut owner = exposed;
        for _ in 0..2 {
            let failed = owner.try_reclaim().unwrap_err();
            assert_eq!(failed.error, Error::Os(11));
            owner = failed.buf;
            assert_eq!(owner.remote(), remote);
            assert_eq!(model.registered(), 2);
        }
        let buf = owner.try_reclaim().unwrap();
        assert_eq!(&*buf, b"reclaim on retry");
        assert_eq!(model.registered(), 1);
        drop(buf);
        assert_eq!(model.registered(), 0);
    }

    #[test]
    fn failed_exposed_drop_retains_backing_and_both_registrations() {
        let model = Model::new(52);
        let ctx = Context::new(model.clone());
        let pd = ctx.alloc_pd().unwrap();
        let exposed = pd.alloc::<u8>(16).unwrap().expose().unwrap();
        let inner = exposed.inner.as_ref().unwrap();
        let backing = Arc::downgrade(&inner.buf.raw.region);
        model.refuse_mr_closes(Model::mr_keys(&inner.mr).0, 1);
        drop(exposed);
        assert_eq!(model.registered(), 2);
        assert!(backing.upgrade().is_some());
        // This tiny intentional leak models the required retention when revocation
        // cannot be established; freeing it would violate the NIC's live access.
    }

    #[test]
    fn successful_exposed_drop_releases_both_registrations() {
        let model = Model::new(53);
        let ctx = Context::new(model.clone());
        let pd = ctx.alloc_pd().unwrap();
        let exposed = pd.alloc::<u8>(16).unwrap().expose().unwrap();
        let backing = Arc::downgrade(&exposed.inner.as_ref().unwrap().buf.raw.region);
        drop(exposed);
        assert_eq!(model.registered(), 0);
        assert!(backing.upgrade().is_none());
    }
}
