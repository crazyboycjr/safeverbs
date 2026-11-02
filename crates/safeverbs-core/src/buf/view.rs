//! Actual frozen views, with geometry tied to the retained byte permission.
//!
//! These checks concern ownership and layout. Initialized `T` values, the
//! zerocopy trait contracts and the public slice-reference bridge are separate.+#![allow(unused_imports)]

use core::marker::PhantomData;
use std::sync::Arc;
use vstd::prelude::*;

use super::Part;
use super::memory::{Frozen, RawMut, thaw};
use super::range::{ByteRange, ElementRange};
use crate::nic::Nic;
use crate::verbs::Verbs;

verus! {

/// A shared, read-only buffer of registered memory, cheap to clone and to
/// slice (like `bytes::Bytes`): send it, or any part of it, as many times
/// as wanted, at once.
#[verifier::reject_recursive_types(N)]
pub struct Buf<T, N: Nic = Verbs> {
    pub(super) frozen: Arc<Frozen<N>>,
    /// Byte offset and element count, with erased byte geometry.
    pub(super) view: ElementRange,
    pub(super) _t: PhantomData<Arc<[T]>>,
}

impl<T, N: Nic> Buf<T, N> {
    /// Bounded whole-element geometry backed by the actual frozen permission.
    #[verifier::type_invariant]
    pub closed spec fn inv(self) -> bool {
        &&& (*self.frozen).inv()
        &&& self.frozen.raw.fits_type::<T>()
        &&& self.view.inv()
        &&& self.view.spec_width() == vstd::layout::size_of::<T>()
        &&& self.view.spec_offset() as nat % self.view.spec_width() as nat == 0
        &&& self.view.spec_bytes().spec_capacity() == self.frozen.byte_len()
    }

    /// The exact bytes of this view, which may overlap other shared views.
    pub closed spec fn byte_domain(self) -> Set<int> {
        self.view.spec_bytes().byte_set(self.frozen.pointer().addr() as int)
    }

    /// The retained native element count.
    pub closed spec fn element_len(self) -> usize { self.view.spec_len() }

    /// Every byte in the view is owned by its actual frozen allocation keeper.
    pub(super) proof fn lemma_byte_backing(&self)
        requires self.inv(),
        ensures self.byte_domain().subset_of((*self.frozen).byte_domain()),
    {
        self.frozen.lemma_contents();
        self.frozen.raw.lemma_domain();
        assert(self.byte_domain().subset_of((*self.frozen).byte_domain()));
    }

    /// Starts with the whole frozen range, using its retained permission.
    #[inline]
    pub(super) fn from_frozen(frozen: Arc<Frozen<N>>) -> (r: Self)
        requires (*frozen).inv(), frozen.raw.fits_type::<T>(),
        ensures r.inv(), *r.frozen == *frozen,
            r.byte_domain() == (*frozen).byte_domain(),
    {
        proof { frozen.lemma_contents(); frozen.raw.lemma_domain(); }
        let view = ElementRange::from_bytes(ByteRange::whole(frozen.raw.len()), core::mem::size_of::<T>());
        let result = Buf { frozen, view, _t: PhantomData };
        proof { assert(result.byte_domain() =~= (*frozen).byte_domain()); }
        result
    }

    /// The number of elements, retained directly rather than divided from bytes.
    #[inline]
    pub fn len(&self) -> (r: usize)
        ensures r == self.element_len(),
    { self.view.len() }

    /// Whether it has no elements.
    #[inline]
    pub fn is_empty(&self) -> (r: bool)
        ensures r == (self.element_len() == 0),
    { self.view.len() == 0 }

    /// Computes the native view pointer with checked bounds and typed alignment.
    /// This does not create a reference or establish initialized contents.
    #[inline]
    pub(super) fn view_ptr(&self) -> (r: *mut u8)
        ensures
            r.addr() == self.frozen.pointer().addr() + self.view.spec_offset(),
            r@.provenance == self.frozen.pointer()@.provenance,
            r.addr() as nat % vstd::layout::align_of::<T>() == 0,
            r.addr() as int + self.view.spec_bytes().spec_len()
                <= self.frozen.pointer().addr() as int + self.frozen.byte_len(),
            self.view.spec_bytes().spec_len() <= isize::MAX,
    {
        vstd::layout::layout_for_type_is_valid::<T>();
        let ptr = self.frozen.raw.ptr();
        proof {
            use_type_invariant(self);
            self.frozen.lemma_contents();
            self.frozen.raw.lemma_domain();
            let offset = self.view.spec_offset() as int;
            let width = vstd::layout::size_of::<T>() as int;
            let alignment = vstd::layout::align_of::<T>() as int;
            vstd::arithmetic::div_mod::lemma_fundamental_div_mod(offset, width);
            vstd::arithmetic::mul::lemma_mul_is_commutative(width, offset / width);
            assert(offset == (offset / width) * width);
            assert(width % alignment == 0);
            vstd::arithmetic::div_mod::lemma_small_mod(0, alignment as nat);
            vstd::arithmetic::div_mod::lemma_mul_mod_noop_right(offset / width, width, alignment);
            assert(((offset / width) * width) % alignment == 0);
            assert(offset % alignment == 0);
            assert(ptr.addr() as int % alignment == 0);
            vstd::arithmetic::div_mod::lemma_mod_adds(ptr.addr() as int, offset, alignment);
            assert((ptr.addr() as int + offset) % alignment == 0);
        }
        ptr.with_addr(ptr.addr() + self.view.offset())
    }

    /// The native clone retains identical geometry and the same frozen contents.
    #[inline]
    pub(super) fn clone_view(&self) -> (r: Self)
        ensures r.inv(), *r.frozen == *self.frozen,
            r.view == self.view, r.byte_domain() == self.byte_domain(),
    {
        proof { use_type_invariant(self); }
        Buf { frozen: self.frozen.clone(), view: self.view, _t: PhantomData }
    }

    /// The actual slice constructor. Rust range syntax is normalized by the caller.
    #[inline]
    pub(super) fn slice_elements(&self, start: usize, end: usize) -> (r: Option<Self>)
        ensures
            r is Some <==> start <= end && end <= self.element_len(),
            r is Some ==> r->0.inv() && *r->0.frozen == *self.frozen
                && r->0.element_len() == end - start
                && r->0.byte_domain().subset_of(self.byte_domain()),
    {
        proof { use_type_invariant(self); }
        match self.view.slice(start, end, core::mem::size_of::<T>()) {
            Some(view) => {
                proof {
                    vstd::arithmetic::div_mod::lemma_mod_multiples_vanish(start as int,
                        self.view.spec_offset() as int, self.view.spec_width() as int);
                    vstd::arithmetic::mul::lemma_mul_is_commutative(start as int, self.view.spec_width() as int);
                    assert(view.spec_offset() as nat % view.spec_width() as nat == 0);
                }
                let result = Buf { frozen: self.frozen.clone(), view, _t: PhantomData };
                proof {
                    assert(result.byte_domain().subset_of(self.byte_domain()));
                }
                Some(result)
            },
            None => None,
        }
    }

    /// Reclaims the whole frozen owner, even when this handle is only a subview.
    #[inline]
    pub(super) fn try_thaw_raw(self) -> (r: Result<RawMut<N>, Self>)
        ensures
            r matches Ok(raw) ==> raw.inv() && raw == self.frozen.raw
                && raw.fits_type::<T>(),
            r matches Err(same) ==> same.inv() && *same.frozen == *self.frozen
                && same.view == self.view && same.byte_domain() == self.byte_domain(),
    {
        proof { use_type_invariant(&self); }
        match thaw(self.frozen) {
            Ok(raw) => Ok(raw),
            Err(frozen) => Err(Buf { frozen, view: self.view, _t: PhantomData }),
        }
    }

    /// Builds the SGE only from the checked view and its actual frozen keeper.
    /// The caller supplies registration metadata, not byte geometry.
    #[inline]
    pub(super) fn part_with_keys(self, lkey: u32, pd: u64) -> (r: Part<N>)
        requires self.view.spec_bytes().spec_len() <= u32::MAX,
        ensures r.inv(),
    {
        proof { use_type_invariant(&self); }
        Part::shared(self.frozen, self.view.offset(), self.view.byte_len(core::mem::size_of::<T>()), lkey, pd)
    }
}

} // verus!

// The checked view retains the native Arc/offset/element-count representation.
const _: () = assert!(core::mem::size_of::<Buf<u8>>() == 3 * core::mem::size_of::<usize>());
