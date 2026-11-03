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

use core::alloc::Layout;
use core::fmt;
use core::marker::PhantomData;
use core::mem::{ManuallyDrop, align_of, size_of};
use core::ops::{Bound, Deref, DerefMut, RangeBounds};
use core::ptr::NonNull;
use std::alloc::dealloc;
use std::collections::VecDeque;
use std::sync::Arc;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::error::{Error, Result};
use crate::nic::{Access, Nic, RemoteBuf};
use crate::pd::Pd;
use crate::verbs::Verbs;

mod range;
use range::ByteRange;
mod memory;
mod permissions;
mod view;
pub(crate) use memory::{Frozen, RawMut};
use memory::{OwnedRange, PermissionPool};
pub use view::Buf;
mod lease;
pub(crate) use lease::Lease;
#[allow(unused_imports)] // Named only by contracts erased from plain cargo builds.
pub(crate) use lease::Loan;
#[allow(unused_imports)] // Erased WQE provenance used by the verified worker.
pub(crate) use lease::WqeLoans;
#[cfg(verus_keep_ghost)]
pub(crate) use lease::covers_inputs;
pub use lease::{Leases, Part};

/// Linux's `ENOMEM`.
const ENOMEM: i32 = 12;

/// The alignment of every region: a cache line.
const REGION_ALIGN: usize = 64;

/// Zeroed memory registered in a protection domain.
pub(crate) struct Region<N: Nic> {
    /// Deregistered first, in `drop`.
    mr: ManuallyDrop<N::Mr>,
    ptr: NonNull<u8>,
    layout: Layout,
    lkey: u32,
    /// Retained with the MR and allocation if revocation fails or unwinds.
    pd: ManuallyDrop<Pd<N>>,
    /// Allocation identity retained with the registration (proof only).
    #[allow(dead_code)]
    deallocation: vstd::prelude::Tracked<vstd::raw_ptr::Dealloc>,
}

// SAFETY: the region is plain memory plus a registration handle; who may
// read or write which bytes is decided by the buffers over it, which carry
// the `Send`/`Sync` bounds of their element type.
unsafe impl<N: Nic> Send for Region<N> {}
// SAFETY: as above.
unsafe impl<N: Nic> Sync for Region<N> {}

impl<N: Nic> Region<N> {
    /// Allocates `bytes` zeroed bytes aligned to `align` (at least a cache
    /// line) and registers them in `pd` for local writes.
    fn new(pd: &Pd<N>, bytes: usize, align: usize) -> Result<(Arc<Region<N>>, OwnedRange)> {
        if bytes == 0 {
            return Err(Error::Invalid("an empty buffer"));
        }
        let layout = Layout::from_size_align(bytes, align.max(REGION_ALIGN))
            .map_err(|_| Error::Invalid("buffer size"))?;
        let (address, range, deallocation) =
            memory::allocate(layout.size(), layout.align()).ok_or(Error::Os(ENOMEM))?;
        let ptr = NonNull::new(address).expect("the allocation primitive returns nonnull memory");
        // SAFETY: the memory stays allocated until `drop`, which deregisters
        // it first.
        let mr = unsafe {
            pd.context()
                .nic()
                .reg_mr(pd.raw(), ptr.as_ptr(), bytes, Access::LOCAL_WRITE)
        };
        match mr {
            Ok(mr) => Ok((
                Arc::new(Region {
                    lkey: N::mr_keys(&mr).0,
                    mr: ManuallyDrop::new(mr),
                    ptr,
                    layout,
                    pd: ManuallyDrop::new(pd.clone()),
                    deallocation,
                }),
                range,
            )),
            Err(e) => {
                memory::release_failed_allocation(
                    address,
                    layout.size(),
                    layout.align(),
                    range,
                    deallocation,
                );
                Err(e)
            }
        }
    }
}

impl<N: Nic> Drop for Region<N> {
    fn drop(&mut self) {
        // Both the MR and PD fields are ManuallyDrop: a refused revocation or
        // backend unwind retains the allocation and the dependency chain together.
        if close_registration::<N>(&mut self.mr).is_err() {
            return;
        }
        // SAFETY: successful revocation (or the legacy backend's completed MR
        // destructor) excludes further access through this registration.
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
            ManuallyDrop::drop(&mut self.pd);
        }
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

impl<N: Nic> RawMut<N> {
    fn pd(&self) -> &Pd<N> {
        &self.region.pd
    }
}

/// A unique, writable buffer of registered memory: `[T]` that the NIC can
/// receive into, read into, or that can be frozen into a [`Buf`] to send.
///
/// Made by `Pd::alloc` (zeroed, so `T` is [`FromBytes`]), by splitting
/// another `BufMut`, or by thawing a `Buf` nobody else holds.
pub struct BufMut<T, N: Nic = Verbs> {
    raw: RawMut<N>,
    _t: PhantomData<T>,
}

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
        let (region, range) = Region::new(self, bytes, align_of::<T>())?;
        Ok(BufMut {
            raw: RawMut { region, range },
            _t: PhantomData,
        })
    }

    /// A ring of `capacity` registered bytes for streaming messages.
    pub fn ring_buf(&self, capacity: usize) -> Result<RingBuf<N>> {
        let (region, range) = Region::new(self, capacity, 1)?;
        Ok(RingBuf {
            region,
            capacity,
            chunks: VecDeque::new(),
            free: range.into_pool(),
        })
    }
}

impl<T, N: Nic> BufMut<T, N> {
    /// Re-types unique bytes; they hold valid `T` because every `BufMut`
    /// starts zeroed with `T: FromBytes` and the NIC only writes into
    /// buffers of `T: FromBytes`.
    fn from_raw(raw: RawMut<N>) -> BufMut<T, N> {
        debug_assert!(raw.fits::<T>());
        BufMut {
            raw,
            _t: PhantomData,
        }
    }

    /// The number of elements.
    pub fn len(&self) -> usize {
        self.raw.range.len() / size_of::<T>()
    }

    /// Whether it has no elements.
    pub fn is_empty(&self) -> bool {
        self.raw.range.len() == 0
    }

    /// The protection domain it is registered in.
    pub fn pd_id(&self) -> u64 {
        self.raw.pd().id()
    }

    /// Splits into `[0, mid)` and `[mid, len)`.
    ///
    /// # Panics
    ///
    /// If `mid > len`.
    pub fn split_at(self, mid: usize) -> (BufMut<T, N>, BufMut<T, N>) {
        assert!(
            mid <= self.len(),
            "split at {mid} of a buffer of {}",
            self.len()
        );
        let (a, b) = self.raw.split_at(mid * size_of::<T>());
        (BufMut::from_raw(a), BufMut::from_raw(b))
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
        Buf::from_frozen(self.raw.freeze())
    }

    /// Exposes it to peers for RDMA WRITE, READ and atomics: registers it
    /// again with remote access and hands out its address and remote key
    /// ([`Exposed::remote`]). Until reclaimed it cannot be read as `[T]`.
    pub fn expose(self) -> Result<Exposed<T, N>, BufError<BufMut<T, N>>>
    where
        T: IntoBytes,
    {
        let access = Access::LOCAL_WRITE
            | Access::REMOTE_WRITE
            | Access::REMOTE_READ
            | Access::REMOTE_ATOMIC;
        let pd = self.raw.pd().clone();
        // SAFETY: the region stays allocated while `Exposed` holds it; the
        // remote registration is revoked before `raw` lets go of it,
        // and `Exposed` never gives out the bytes as Rust values.
        let mr = unsafe {
            pd.context()
                .nic()
                .reg_mr(pd.raw(), self.raw.ptr(), self.raw.range.len(), access)
        };
        match mr {
            Ok(mr) => Ok(Exposed {
                rkey: N::mr_keys(&mr).1,
                mr: ManuallyDrop::new(mr),
                raw: ManuallyDrop::new(self.raw),
                _t: PhantomData,
            }),
            Err(error) => Err(BufError { error, buf: self }),
        }
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
    pub fn into_part(self) -> Part<N>
    where
        T: IntoBytes,
    {
        let lkey = self.raw.region.lkey;
        let pd = self.raw.pd().id();
        Part::unique(self.raw, lkey, pd)
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
    /// The protection domain it is registered in.
    pub fn pd_id(&self) -> u64 {
        self.frozen.raw.pd().id()
    }

    /// The elements `range` of it, sharing the memory.
    ///
    /// # Panics
    ///
    /// If the range is not within it.
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

    /// Whether no other `Buf` and no request holds the frozen buffer.
    pub fn is_unique(&self) -> bool {
        Arc::strong_count(&self.frozen) == 1
    }

    /// Thaws the whole buffer that was frozen back into a [`BufMut`], if no
    /// other `Buf` (of any part of it) and no request in flight holds it;
    /// else gives it back unchanged.
    pub fn try_thaw(self) -> Result<BufMut<T, N>, Buf<T, N>> {
        match self.try_thaw_raw() {
            Ok(raw) => Ok(BufMut::from_raw(raw)),
            Err(same) => Err(same),
        }
    }

    /// Lends it to the NIC as one scatter/gather element (for sends and
    /// WRITEs from it).
    ///
    /// # Panics
    ///
    /// If the byte length exceeds `u32::MAX`, the native SGE limit. Slice a larger
    /// buffer before lending it.
    pub fn into_part(self) -> Part<N>
    where
        T: IntoBytes + Immutable,
    {
        match self.try_into_part() {
            Ok(part) => part,
            Err(_) => panic!("a single SGE exceeds u32::MAX bytes"),
        }
    }

    /// Lends this frozen view as one SGE, returning it unchanged if its byte
    /// length exceeds `u32::MAX`. Slice a larger view before lending it.
    pub fn try_into_part(self) -> Result<Part<N>, Self>
    where
        T: IntoBytes + Immutable,
    {
        let lkey = self.frozen.raw.region.lkey;
        let pd = self.pd_id();
        self.try_part_with_keys(lkey, pd)
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

/// A buffer exposed to peers: they may RDMA WRITE, READ and run atomics on
/// it with [`Exposed::remote`], so it is never readable as `[T]`; read it
/// only after [`Exposed::reclaim`] revokes the peers' access. The unsafe
/// [`Exposed::copy_to_slice`] escape hatch requires caller-established quiescence.
pub struct Exposed<T, N: Nic = Verbs> {
    /// Kept together with `raw` if revocation fails.
    mr: ManuallyDrop<N::Mr>,
    raw: ManuallyDrop<RawMut<N>>,
    rkey: u32,
    _t: PhantomData<T>,
}

impl<T, N: Nic> Exposed<T, N> {
    /// What a peer needs to address it.
    pub fn remote(&self) -> RemoteBuf {
        RemoteBuf {
            addr: self.raw.ptr() as u64,
            len: self.raw.range.len() as u64,
            rkey: self.rkey,
        }
    }

    /// The number of elements.
    pub fn len(&self) -> usize {
        self.raw.range.len() / size_of::<T>()
    }

    /// Whether it has no elements.
    pub fn is_empty(&self) -> bool {
        self.raw.range.len() == 0
    }

    /// Copies its current contents into `out` (the first `out.len()`
    /// elements) with volatile reads after the caller has synchronized peer access.
    ///
    /// # Safety
    ///
    /// No NIC, software backend or peer may write the source bytes during
    /// this copy. The caller must establish completion and exclude concurrent
    /// and future writes for its duration. Volatile reads do not synchronize
    /// with DMA or other Rust threads. Prefer [`Self::reclaim`] to revoke
    /// remote access before using ordinary safe reads.
    ///
    /// ```compile_fail
    /// use safeverbs_core::Exposed;
    /// fn unsynchronized_copy(buf: &Exposed<u8>, out: &mut [u8]) {
    ///     buf.copy_to_slice(out);
    /// }
    /// ```
    ///
    /// # Panics
    ///
    /// If `out` is longer than it.
    pub unsafe fn copy_to_slice(&self, out: &mut [T])
    where
        T: FromBytes + IntoBytes,
    {
        assert!(
            out.len() <= self.len(),
            "{} elements out of {}",
            out.len(),
            self.len()
        );
        let dst = out.as_mut_bytes();
        let src = self.raw.ptr();
        let mut i = 0;
        while i < dst.len() {
            // SAFETY: inside the buffer and quiescent by the caller's contract.
            dst[i] = unsafe { core::ptr::read_volatile(src.add(i)) };
            i += 1;
        }
    }

    /// Revokes the peers' access and gives the buffer back. A refused revocation
    /// returns the exposed owner, whose registration remains live, for retry.
    pub fn try_reclaim(self) -> Result<BufMut<T, N>, BufError<Self>> {
        // A backend panic must also retain the allocation and registration.
        let mut this = ManuallyDrop::new(self);
        if let Err(error) = close_registration::<N>(&mut this.mr) {
            return Err(BufError {
                error,
                buf: ManuallyDrop::into_inner(this),
            });
        }
        // SAFETY: the registration was revoked and its handle destroyed above;
        // `this` will not drop, and this is the only extraction of its raw owner.
        let raw = unsafe { ManuallyDrop::take(&mut this.raw) };
        Ok(BufMut::from_raw(raw))
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
        if close_registration::<N>(&mut self.mr).is_ok() {
            // SAFETY: remote access has been revoked; the raw owner is dropped
            // exactly once. On failure both ManuallyDrop fields stay retained.
            unsafe { ManuallyDrop::drop(&mut self.raw) };
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
    pub fn into_buf_mut<T: FromBytes>(self) -> Option<BufMut<T, N>> {
        match self.into_unique_raw() {
            Some(raw) if raw.fits::<T>() => Some(BufMut::from_raw(raw)),
            _ => None,
        }
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
        match self.lease {
            Lease::Unique(raw) if raw.fits::<T>() => Some(BufMut::from_raw(raw)),
            _ => None,
        }
    }
}

/// A ring of registered bytes for streaming variable-size messages: each
/// message is written at the head into a chunk of its own, frozen and sent;
/// a chunk's space is reused once every `Buf` of it is gone (its requests
/// retired and the caller dropped its copies), oldest first.
pub struct RingBuf<N: Nic = Verbs> {
    region: Arc<Region<N>>,
    capacity: usize,
    /// The chunks handed out and not reclaimed, oldest first.
    chunks: VecDeque<Arc<Frozen<N>>>,
    /// Actual free-byte permissions, erased in a native build.
    free: PermissionPool,
}

impl<N: Nic> RingBuf<N> {
    /// Its capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Bytes in chunks not reclaimed yet.
    pub fn in_use(&mut self) -> usize {
        self.reclaim();
        let mut n = 0;
        for c in &self.chunks {
            n += c.raw.range.len();
        }
        n
    }

    /// Drops the oldest chunks nobody holds any more.
    fn reclaim(&mut self) {
        while let Some(front) = self.chunks.front() {
            if Arc::strong_count(front) != 1 {
                break;
            }
            let chunk = self.chunks.pop_front().expect("the front chunk exists");
            match memory::thaw(chunk) {
                Ok(raw) => self.free.put(raw.range),
                Err(chunk) => {
                    self.chunks.push_front(chunk);
                    break;
                }
            }
        }
    }

    /// Where `len` contiguous free bytes start, if anywhere.
    fn place(&mut self, len: usize) -> Option<usize> {
        self.reclaim();
        let (Some(front), Some(back)) = (self.chunks.front(), self.chunks.back()) else {
            return (len <= self.capacity).then_some(0);
        };
        // The ring owns its whole region: offsets are positions in the ring.
        let tail = front.raw.range.offset(self.region.ptr.as_ptr());
        let head = back.raw.range.end(self.region.ptr.as_ptr());
        // Chunks lie from `tail` to `head`, or wrapped from `tail` to the end
        // and from the start to `head`. A wrapped head never reaches the
        // tail (strict comparisons), so `head >= tail` means not wrapped.
        if head >= tail {
            if self.capacity - head >= len {
                Some(head)
            } else if tail > len {
                Some(0)
            } else {
                None
            }
        } else if tail - head > len {
            Some(head)
        } else {
            None
        }
    }

    /// Writes a message of `len` bytes, filled by `fill`, at the head and
    /// returns it frozen; `None` while the ring has no room for it.
    pub fn write_with(&mut self, len: usize, fill: impl FnOnce(&mut [u8])) -> Option<Buf<u8, N>> {
        let at = self.place(len)?;
        let mut buf: BufMut<u8, N> = BufMut::from_raw(RawMut {
            region: self.region.clone(),
            range: self.free.take(
                self.region.ptr.as_ptr(),
                ByteRange::new(at, len, self.capacity).expect("a chunk fits the ring"),
            ),
        });
        fill(&mut buf);
        let buf = buf.freeze();
        self.chunks.push_back(buf.frozen.clone());
        Some(buf)
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
        let key = Model::mr_keys(&exposed.mr).0;
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
        let backing = Arc::downgrade(&exposed.raw.region);
        model.refuse_mr_closes(Model::mr_keys(&exposed.mr).0, 1);
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
        let backing = Arc::downgrade(&exposed.raw.region);
        drop(exposed);
        assert_eq!(model.registered(), 0);
        assert!(backing.upgrade().is_none());
    }
}
