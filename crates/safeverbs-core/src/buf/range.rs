//! Executable byte-range arithmetic used by registered buffers.
//!
//! This proves bounds, exact splitting and lossless SGE length conversion. It does
//! not construct allocation permissions, establish exclusive ownership, or prove
//! registration lifetime. The capacity is ghost state; the runtime range remains
//! two machine words.

use vstd::prelude::*;

verus! {

/// A byte range within one allocation. Copying this geometry grants no memory access.
#[derive(Clone, Copy)]
pub(super) struct ByteRange {
    offset: usize,
    len: usize,
    capacity: Ghost<usize>,
}

impl ByteRange {
    pub(super) open spec fn inv(self) -> bool {
        self.spec_offset() <= self.spec_capacity()
            && self.spec_len() <= self.spec_capacity() - self.spec_offset()
    }

    pub(super) closed spec fn spec_offset(self) -> usize { self.offset }
    pub(super) closed spec fn spec_len(self) -> usize { self.len }
    pub(super) closed spec fn spec_capacity(self) -> usize { self.capacity@ }

    #[inline]
    pub(super) fn whole(len: usize) -> (r: Self)
        ensures
            r.inv(), r.spec_offset() == 0, r.spec_len() == len,
            r.spec_capacity() == len,
    {
        ByteRange { offset: 0, len, capacity: Ghost(len) }
    }

    #[inline]
    pub(super) fn new(offset: usize, len: usize, capacity: usize) -> (r: Option<Self>)
        ensures
            r is Some <==> offset <= capacity && len <= capacity - offset,
            r is Some ==> r->0.inv() && r->0.spec_offset() == offset
                && r->0.spec_len() == len && r->0.spec_capacity() == capacity,
    {
        if offset > capacity || len > capacity - offset {
            None
        } else {
            Some(ByteRange { offset, len, capacity: Ghost(capacity) })
        }
    }

    #[inline]
    pub(super) fn offset(&self) -> (r: usize)
        ensures r == self.spec_offset(),
    { self.offset }

    #[inline]
    pub(super) fn len(&self) -> (r: usize)
        ensures r == self.spec_len(),
    { self.len }

    #[inline]
    pub(super) fn end(&self) -> (r: usize)
        requires self.inv(),
        ensures r == self.spec_offset() + self.spec_len(), r <= self.spec_capacity(),
    { self.offset + self.len }

    /// Split without losing bytes, overlapping the pieces or exceeding the allocation.
    #[inline]
    pub(super) fn split_at(self, at: usize) -> (r: (Self, Self))
        requires self.inv(), at <= self.spec_len(),
        ensures
            r.0.inv(), r.1.inv(),
            r.0.spec_offset() == self.spec_offset(), r.0.spec_len() == at,
            r.1.spec_offset() == self.spec_offset() + at,
            r.1.spec_len() == self.spec_len() - at,
            r.0.spec_capacity() == self.spec_capacity(),
            r.1.spec_capacity() == self.spec_capacity(),
            r.0.spec_offset() + r.0.spec_len() == r.1.spec_offset(),
            r.1.spec_offset() + r.1.spec_len() == self.spec_offset() + self.spec_len(),
    {
        assert!(at <= self.len);
        (
            ByteRange { offset: self.offset, len: at, capacity: self.capacity },
            ByteRange { offset: self.offset + at, len: self.len - at, capacity: self.capacity },
        )
    }

    /// Select whole elements from a byte range, rejecting invalid bounds and sizes.
    #[inline]
    pub(super) fn slice_elements(self, start: usize, end: usize, width: usize) -> (r: Option<Self>)
        requires self.inv(),
        ensures
            r is Some <==> width > 0 && start <= end && end <= self.spec_len() / width,
            r is Some ==> r->0.inv()
                && r->0.spec_offset() == self.spec_offset() + start * width
                && r->0.spec_len() == (end - start) * width
                && r->0.spec_capacity() == self.spec_capacity(),
    {
        if width == 0 || start > end || end > self.len / width {
            None
        } else {
            proof {
                vstd::arithmetic::div_mod::lemma_fundamental_div_mod(self.len as int, width as int);
                vstd::arithmetic::div_mod::lemma_mod_pos_bound(self.len as int, width as int);
                vstd::arithmetic::mul::lemma_mul_inequality(end as int, self.len as int / width as int, width as int);
                vstd::arithmetic::mul::lemma_mul_is_commutative(self.len as int / width as int, width as int);
                assert(end * width <= self.len);
                vstd::arithmetic::mul::lemma_mul_inequality(start as int, end as int, width as int);
                assert(start * width <= end * width);
                assert((end - start) * width == end * width - start * width) by(nonlinear_arith);
            }
            Some(ByteRange {
                offset: self.offset + start * width,
                len: (end - start) * width,
                capacity: self.capacity,
            })
        }
    }
}

/// A native SGE has a 32-bit byte count. Reject oversize ranges instead of truncating.
#[inline]
pub(super) fn checked_sge_len(len: usize) -> (r: Option<u32>)
    ensures
        r is Some <==> len <= u32::MAX,
        r is Some ==> r->0 as usize == len,
{
    if len > u32::MAX as usize { None } else { Some(len as u32) }
}

} // verus!

#[cfg(test)]
mod tests {
    use super::*;

    /// No allocation is needed to exercise the native field boundary.
    #[test]
    fn sge_lengths_never_truncate() {
        assert_eq!(checked_sge_len(0), Some(0));
        assert_eq!(checked_sge_len(u32::MAX as usize), Some(u32::MAX));
        if usize::BITS > 32 {
            assert_eq!(checked_sge_len(u32::MAX as usize + 1), None);
            assert_eq!(checked_sge_len(usize::MAX), None);
        }
    }

    /// Synthetic capacities cover address-space boundaries without allocating memory.
    #[test]
    fn range_boundaries_do_not_wrap() {
        assert!(ByteRange::new(usize::MAX, 1, usize::MAX).is_none());
        assert!(ByteRange::new(1, usize::MAX, usize::MAX).is_none());
        let range = ByteRange::new(usize::MAX - 8, 8, usize::MAX).unwrap();
        let (left, right) = range.split_at(3);
        assert_eq!(
            (left.offset(), left.len(), left.end()),
            (usize::MAX - 8, 3, usize::MAX - 5)
        );
        assert_eq!(
            (right.offset(), right.len(), right.end()),
            (usize::MAX - 5, 5, usize::MAX)
        );
        let (empty, whole) = right.split_at(0);
        assert_eq!((empty.len(), empty.end()), (0, whole.offset()));
    }
}
