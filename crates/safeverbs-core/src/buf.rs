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
use std::alloc::{alloc_zeroed, dealloc};
use std::collections::VecDeque;
use std::sync::Arc;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::error::{Error, Result};
use crate::nic::{Access, Nic, RemoteBuf, Sge};
use crate::pd::Pd;
use crate::verbs::Verbs;

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
    pd: Pd<N>,
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
    fn new(pd: &Pd<N>, bytes: usize, align: usize) -> Result<Arc<Region<N>>> {
        if bytes == 0 {
            return Err(Error::Invalid("an empty buffer"));
        }
        let layout = Layout::from_size_align(bytes, align.max(REGION_ALIGN))
            .map_err(|_| Error::Invalid("buffer size"))?;
        // SAFETY: the layout has a non-zero size.
        let ptr = NonNull::new(unsafe { alloc_zeroed(layout) }).ok_or(Error::Os(ENOMEM))?;
        // SAFETY: the memory stays allocated until `drop`, which deregisters
        // it first.
        let mr = unsafe {
            pd.context()
                .nic()
                .reg_mr(pd.raw(), ptr.as_ptr(), bytes, Access::LOCAL_WRITE)
        };
        match mr {
            Ok(mr) => Ok(Arc::new(Region {
                lkey: N::mr_keys(&mr).0,
                mr: ManuallyDrop::new(mr),
                ptr,
                layout,
                pd: pd.clone(),
            })),
            Err(e) => {
                // SAFETY: allocated above with this layout.
                unsafe { dealloc(ptr.as_ptr(), layout) };
                Err(e)
            }
        }
    }
}

impl<N: Nic> Drop for Region<N> {
    fn drop(&mut self) {
        // SAFETY: no lease is left (they hold the region), so no work
        // request uses the memory: deregister, then free.
        unsafe {
            ManuallyDrop::drop(&mut self.mr);
            dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

/// Unique access to `len` bytes at `offset` of a region.
pub(crate) struct RawMut<N: Nic> {
    region: Arc<Region<N>>,
    offset: usize,
    len: usize,
}

impl<N: Nic> RawMut<N> {
    fn ptr(&self) -> *mut u8 {
        // SAFETY: `offset <= layout.size()` by construction.
        unsafe { self.region.ptr.as_ptr().add(self.offset) }
    }

    fn sge(&self) -> Sge {
        Sge {
            addr: self.ptr() as u64,
            length: self.len as u32,
            lkey: self.region.lkey,
        }
    }

    fn pd(&self) -> &Pd<N> {
        &self.region.pd
    }

    /// Splits at byte `at` (at most `len`).
    fn split_at(self, at: usize) -> (RawMut<N>, RawMut<N>) {
        assert!(at <= self.len);
        let right = RawMut {
            region: self.region.clone(),
            offset: self.offset + at,
            len: self.len - at,
        };
        let left = RawMut {
            region: self.region,
            offset: self.offset,
            len: at,
        };
        (left, right)
    }

    /// Whether the bytes can hold `[T]` (aligned, a whole number of `T`).
    fn fits<T>(&self) -> bool {
        size_of::<T>() > 0
            && self.offset.is_multiple_of(align_of::<T>())
            && self.len.is_multiple_of(size_of::<T>())
    }
}

/// A frozen range: shared and read only until thawed.
pub(crate) struct Frozen<N: Nic> {
    raw: RawMut<N>,
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

/// A shared, read-only buffer of registered memory, cheap to clone and to
/// slice (like `bytes::Bytes`): send it, or any part of it, as many times
/// as wanted, at once.
pub struct Buf<T, N: Nic = Verbs> {
    frozen: Arc<Frozen<N>>,
    /// Bytes from the start of the frozen range.
    offset: usize,
    /// Elements.
    len: usize,
    _t: PhantomData<Arc<[T]>>,
}

impl<T, N: Nic> Clone for Buf<T, N> {
    fn clone(&self) -> Self {
        Buf {
            frozen: self.frozen.clone(),
            offset: self.offset,
            len: self.len,
            _t: PhantomData,
        }
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
        let region = Region::new(self, bytes, align_of::<T>())?;
        Ok(BufMut {
            raw: RawMut {
                region,
                offset: 0,
                len: bytes,
            },
            _t: PhantomData,
        })
    }

    /// A ring of `capacity` registered bytes for streaming messages.
    pub fn ring_buf(&self, capacity: usize) -> Result<RingBuf<N>> {
        Ok(RingBuf {
            region: Region::new(self, capacity, 1)?,
            capacity,
            chunks: VecDeque::new(),
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
        self.raw.len / size_of::<T>()
    }

    /// Whether it has no elements.
    pub fn is_empty(&self) -> bool {
        self.raw.len == 0
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
    pub fn freeze(self) -> Buf<T, N> {
        let len = self.len();
        Buf {
            frozen: Arc::new(Frozen { raw: self.raw }),
            offset: 0,
            len,
            _t: PhantomData,
        }
    }

    /// Exposes it to peers for RDMA WRITE, READ and atomics: registers it
    /// again with remote access and hands out its address and remote key
    /// ([`Exposed::remote`]). Until reclaimed it cannot be read as `[T]`.
    pub fn expose(self) -> Result<Exposed<T, N>, BufError<BufMut<T, N>>> {
        let access = Access::LOCAL_WRITE
            | Access::REMOTE_WRITE
            | Access::REMOTE_READ
            | Access::REMOTE_ATOMIC;
        let pd = self.raw.pd().clone();
        // SAFETY: the region stays allocated while `Exposed` holds it; the
        // region is deregistered before `raw` lets go of it (field order),
        // and `Exposed` never gives out the bytes as Rust values.
        let mr = unsafe {
            pd.context()
                .nic()
                .reg_mr(pd.raw(), self.raw.ptr(), self.raw.len, access)
        };
        match mr {
            Ok(mr) => Ok(Exposed {
                rkey: N::mr_keys(&mr).1,
                mr,
                raw: self.raw,
                _t: PhantomData,
            }),
            Err(error) => Err(BufError { error, buf: self }),
        }
    }

    /// Lends it to the NIC as one scatter/gather element (for receives and
    /// READs into it).
    pub fn into_part(self) -> Part<N> {
        Part {
            sge: self.raw.sge(),
            pd: self.raw.pd().id(),
            lease: Lease::Unique(self.raw),
        }
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
    /// The number of elements.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether it has no elements.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

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
            Bound::Excluded(&s) => s + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&e) => e + 1,
            Bound::Excluded(&e) => e,
            Bound::Unbounded => self.len,
        };
        assert!(
            start <= end && end <= self.len,
            "range {start}..{end} of a buffer of {}",
            self.len
        );
        Buf {
            frozen: self.frozen.clone(),
            offset: self.offset + start * size_of::<T>(),
            len: end - start,
            _t: PhantomData,
        }
    }

    /// Whether no other `Buf` and no request holds the frozen buffer.
    pub fn is_unique(&self) -> bool {
        Arc::strong_count(&self.frozen) == 1
    }

    /// Thaws the whole buffer that was frozen back into a [`BufMut`], if no
    /// other `Buf` (of any part of it) and no request in flight holds it;
    /// else gives it back unchanged.
    pub fn try_thaw(self) -> Result<BufMut<T, N>, Buf<T, N>> {
        let Buf {
            frozen,
            offset,
            len,
            ..
        } = self;
        match Arc::try_unwrap(frozen) {
            Ok(frozen) => Ok(BufMut::from_raw(frozen.raw)),
            Err(frozen) => Err(Buf {
                frozen,
                offset,
                len,
                _t: PhantomData,
            }),
        }
    }

    /// Lends it to the NIC as one scatter/gather element (for sends and
    /// WRITEs from it).
    pub fn into_part(self) -> Part<N> {
        let sge = Sge {
            addr: self.frozen.raw.ptr() as u64 + self.offset as u64,
            length: (self.len * size_of::<T>()) as u32,
            lkey: self.frozen.raw.region.lkey,
        };
        Part {
            sge,
            pd: self.pd_id(),
            lease: Lease::Shared(self.frozen),
        }
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
        unsafe {
            core::slice::from_raw_parts(
                self.frozen.raw.ptr().add(self.offset).cast::<T>(),
                self.len,
            )
        }
    }
}

impl<T, N: Nic> fmt::Debug for Buf<T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Buf({:p}+{}, {} × {} bytes)",
            self.frozen.raw.ptr(),
            self.offset,
            self.len,
            size_of::<T>()
        )
    }
}

/// A buffer exposed to peers: they may RDMA WRITE, READ and run atomics on
/// it with [`Exposed::remote`], so it is never readable as `[T]`; read it
/// with volatile copies ([`Exposed::copy_to_slice`]) or [`Exposed::reclaim`]
/// it, which revokes the peers' access first.
pub struct Exposed<T, N: Nic = Verbs> {
    /// The remote-access registration; dropped (deregistered) before `raw`.
    mr: N::Mr,
    raw: RawMut<N>,
    rkey: u32,
    _t: PhantomData<T>,
}

impl<T, N: Nic> Exposed<T, N> {
    /// What a peer needs to address it.
    pub fn remote(&self) -> RemoteBuf {
        RemoteBuf {
            addr: self.raw.ptr() as u64,
            len: self.raw.len as u64,
            rkey: self.rkey,
        }
    }

    /// The number of elements.
    pub fn len(&self) -> usize {
        self.raw.len / size_of::<T>()
    }

    /// Whether it has no elements.
    pub fn is_empty(&self) -> bool {
        self.raw.len == 0
    }

    /// Copies its current contents into `out` (the first `out.len()`
    /// elements) with volatile reads, since peers may be writing it.
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
        let dst = out.as_mut_bytes();
        let src = self.raw.ptr();
        let mut i = 0;
        while i < dst.len() {
            // SAFETY: inside the buffer; volatile because the NIC may write
            // it behind the compiler's back.
            dst[i] = unsafe { core::ptr::read_volatile(src.add(i)) };
            i += 1;
        }
    }

    /// Revokes the peers' access (deregisters the remote registration, which
    /// invalidates the remote key) and gives the buffer back.
    pub fn reclaim(self) -> BufMut<T, N> {
        let Exposed { mr, raw, .. } = self;
        drop(mr);
        BufMut::from_raw(raw)
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
    /// The buffer, no longer used by the NIC.
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

/// What keeps a buffer's memory alive and untouched while the NIC uses it.
pub(crate) enum Lease<N: Nic> {
    /// A frozen buffer (sends, WRITEs).
    Shared(Arc<Frozen<N>>),
    /// A unique buffer (receives, READs, atomics).
    Unique(RawMut<N>),
}

/// The leases of one request. The first is kept inline; repeated leases of
/// the same frozen buffer are kept once.
pub struct Leases<N: Nic = Verbs> {
    first: Option<Lease<N>>,
    rest: Vec<Lease<N>>,
}

impl<N: Nic> fmt::Debug for Leases<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Leases({})", self.len())
    }
}

impl<N: Nic> Default for Leases<N> {
    fn default() -> Self {
        Leases {
            first: None,
            rest: Vec::new(),
        }
    }
}

impl<N: Nic> Leases<N> {
    /// Keeps `lease`, unless it is a frozen buffer the last lease already
    /// holds.
    pub(crate) fn push(&mut self, lease: Lease<N>) {
        let last = match self.rest.last() {
            Some(l) => Some(l),
            None => self.first.as_ref(),
        };
        if let (Some(Lease::Shared(a)), Lease::Shared(b)) = (last, &lease)
            && Arc::ptr_eq(a, b)
        {
            return;
        }
        if self.first.is_none() {
            self.first = Some(lease);
        } else {
            self.rest.push(lease);
        }
    }

    /// The number of leases kept.
    pub fn len(&self) -> usize {
        self.first.is_some() as usize + self.rest.len()
    }

    /// Whether none is kept.
    pub fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    /// Takes back the first unique buffer (of a receive, READ or atomic) as
    /// a `BufMut<T>`; `None` if there is none or its bytes cannot hold
    /// `[T]`.
    pub fn into_buf_mut<T: FromBytes>(mut self) -> Option<BufMut<T, N>> {
        let mut found = None;
        if let Some(Lease::Unique(_)) = &self.first {
            found = self.first.take();
        } else {
            let mut i = 0;
            while i < self.rest.len() {
                if let Lease::Unique(_) = &self.rest[i] {
                    found = Some(self.rest.remove(i));
                    break;
                }
                i += 1;
            }
        }
        match found {
            Some(Lease::Unique(raw)) if raw.fits::<T>() => Some(BufMut::from_raw(raw)),
            _ => None,
        }
    }
}

impl<N: Nic> From<Part<N>> for Leases<N> {
    fn from(part: Part<N>) -> Leases<N> {
        let mut leases = Leases::default();
        leases.push(part.lease);
        leases
    }
}

/// A buffer on loan to the NIC: one scatter/gather element and the lease
/// that keeps it valid.
pub struct Part<N: Nic = Verbs> {
    pub(crate) sge: Sge,
    pub(crate) pd: u64,
    pub(crate) lease: Lease<N>,
}

impl<N: Nic> Part<N> {
    /// Its length in bytes.
    pub fn len(&self) -> u64 {
        self.sge.length as u64
    }

    /// Whether it is empty.
    pub fn is_empty(&self) -> bool {
        self.sge.length == 0
    }

    /// Whether the NIC may write it (a unique buffer).
    pub fn is_writable(&self) -> bool {
        matches!(self.lease, Lease::Unique(_))
    }

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
            n += c.raw.len;
        }
        n
    }

    /// Drops the oldest chunks nobody holds any more.
    fn reclaim(&mut self) {
        while let Some(front) = self.chunks.front() {
            if Arc::strong_count(front) != 1 {
                break;
            }
            self.chunks.pop_front();
        }
    }

    /// Where `len` contiguous free bytes start, if anywhere.
    fn place(&mut self, len: usize) -> Option<usize> {
        self.reclaim();
        let (Some(front), Some(back)) = (self.chunks.front(), self.chunks.back()) else {
            return (len <= self.capacity).then_some(0);
        };
        // The ring owns its whole region: offsets are positions in the ring.
        let tail = front.raw.offset;
        let head = back.raw.offset + back.raw.len;
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
            offset: at,
            len,
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
