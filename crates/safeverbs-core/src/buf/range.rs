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

    /// The same address range used by A and M, relative to an allocation's base.
    pub(super) open spec fn span(self, base: int) -> safeverbs_spec::types::Span {
        safeverbs_spec::types::Span { start: base + self.spec_offset(), len: self.spec_len() as nat }
    }

    /// A finite byte domain suitable for vstd's raw-pointer permissions.
    pub(super) open spec fn byte_set(self, base: int) -> Set<int> {
        Set::range(base + self.spec_offset(), base + self.spec_offset() + self.spec_len())
    }

    pub(super) proof fn lemma_span_set(self, base: int)
        ensures self.byte_set(base).to_iset() == self.span(base).bytes(),
    {
        assert(self.byte_set(base).to_iset() =~= self.span(base).bytes());
    }

    /// A frozen view names only bytes in the retained allocation range.
    pub(super) proof fn lemma_view_bytes(self, view: Self, base: int)
        requires view.span(base).within(self.span(base)),
        ensures view.byte_set(base).subset_of(self.byte_set(base)),
    {
        assert(view.byte_set(base).subset_of(self.byte_set(base)));
    }

    /// The exact geometry returned by `split_at` partitions its byte permissions.
    pub(super) proof fn lemma_split_bytes(self, left: Self, right: Self, base: int)
        requires
            left.spec_offset() == self.spec_offset(),
            left.spec_offset() + left.spec_len() == right.spec_offset(),
            right.spec_offset() + right.spec_len() == self.spec_offset() + self.spec_len(),
        ensures
            left.byte_set(base).disjoint(right.byte_set(base)),
            left.byte_set(base).subset_of(self.byte_set(base)),
            left.byte_set(base).union(right.byte_set(base)) == self.byte_set(base),
            self.byte_set(base).difference(left.byte_set(base)) == right.byte_set(base),
    {
        assert(left.byte_set(base).disjoint(right.byte_set(base)));
        assert(left.byte_set(base).subset_of(self.byte_set(base)));
        assert(left.byte_set(base).union(right.byte_set(base)) =~= self.byte_set(base));
        assert(self.byte_set(base).difference(left.byte_set(base)) =~= right.byte_set(base));
    }

    /// Split an existing linear raw-byte token, preserving its allocation provenance.
    /// This does not create the initial token or attach tokens to runtime buffers.
    pub(super) proof fn split_permission(
        self, left: Self, right: Self, base: int,
        tracked perm: vstd::raw_ptr::PointsToRaw,
    ) -> (tracked r: (vstd::raw_ptr::PointsToRaw, vstd::raw_ptr::PointsToRaw))
        requires
            perm.dom() == self.byte_set(base),
            left.spec_offset() == self.spec_offset(),
            left.spec_offset() + left.spec_len() == right.spec_offset(),
            right.spec_offset() + right.spec_len() == self.spec_offset() + self.spec_len(),
        ensures
            r.0.dom() == left.byte_set(base), r.1.dom() == right.byte_set(base),
            r.0.provenance() == perm.provenance(), r.1.provenance() == perm.provenance(),
    {
        self.lemma_split_bytes(left, right, base);
        perm.split(left.byte_set(base))
    }

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

}

/// A frozen view keeps its element count directly, as the original buffer did.
/// Byte geometry and element width are erased; `len()` needs no division.
#[derive(Clone, Copy)]
pub(super) struct ElementRange {
    offset: usize,
    len: usize,
    // Its reads disappear with the proof code in a plain cargo build.
    #[allow(dead_code)]
    bytes: Ghost<ByteRange>,
    width: Ghost<usize>,
}

impl ElementRange {
    pub(super) closed spec fn spec_offset(self) -> usize { self.offset }
    pub(super) closed spec fn spec_len(self) -> usize { self.len }
    pub(super) closed spec fn spec_bytes(self) -> ByteRange { self.bytes@ }
    pub(super) closed spec fn spec_width(self) -> usize { self.width@ }

    pub(super) open spec fn inv(self) -> bool {
        &&& self.spec_width() > 0
        &&& self.spec_bytes().inv()
        &&& self.spec_offset() == self.spec_bytes().spec_offset()
        &&& self.spec_len() * self.spec_width() == self.spec_bytes().spec_len()
    }

    /// The caller supplies whole elements, the same invariant as `RawMut::fits`.
    #[inline]
    pub(super) fn from_bytes(bytes: ByteRange, width: usize) -> (r: Self)
        requires bytes.inv(), width > 0, bytes.spec_len() % width == 0,
        ensures r.inv(), r.spec_bytes() == bytes, r.spec_width() == width,
    {
        proof {
            vstd::arithmetic::div_mod::lemma_fundamental_div_mod(bytes.len as int, width as int);
            vstd::arithmetic::mul::lemma_mul_is_commutative(bytes.len as int / width as int, width as int);
        }
        ElementRange { offset: bytes.offset, len: bytes.len / width, bytes: Ghost(bytes), width: Ghost(width) }
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
    pub(super) fn byte_len(&self, width: usize) -> (r: usize)
        requires self.inv(), width == self.spec_width(),
        ensures r == self.spec_bytes().spec_len(),
    { self.len * width }

    #[inline]
    pub(super) fn slice(self, start: usize, end: usize, width: usize) -> (r: Option<Self>)
        requires self.inv(), width == self.spec_width(),
        ensures
            r is Some <==> start <= end && end <= self.spec_len(),
            r is Some ==> r->0.inv() && r->0.spec_width() == width
                && r->0.spec_offset() == self.spec_offset() + start * width
                && r->0.spec_len() == end - start
                && r->0.spec_bytes().span(0).within(self.spec_bytes().span(0)),
    {
        if start > end || end > self.len {
            None
        } else {
            proof {
                vstd::arithmetic::mul::lemma_mul_inequality(start as int, end as int, width as int);
                vstd::arithmetic::mul::lemma_mul_inequality(end as int, self.len as int, width as int);
                vstd::arithmetic::mul::lemma_mul_is_distributive_sub_other_way(width as int, end as int, start as int);
            }
            let offset = self.offset + start * width;
            let len = end - start;
            Some(ElementRange {
                offset, len,
                bytes: Ghost(ByteRange {
                    offset, len: (len * width) as usize, capacity: self.bytes@.capacity,
                }),
                width: self.width,
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

// Proof metadata must not add a third word to every buffer and retained lease.
const _: () = assert!(core::mem::size_of::<ByteRange>() == 2 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<ElementRange>() == 2 * core::mem::size_of::<usize>());

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
