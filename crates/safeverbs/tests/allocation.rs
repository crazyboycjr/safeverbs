//! Public allocation and ownership regressions over the simulated NIC.
//! These are runtime checks, not a proof of allocation-lifetime refinement.

use core::alloc::{GlobalAlloc, Layout};
use core::cell::Cell;
use core::mem::size_of;
use std::alloc::System;

use safeverbs::{Context, Error, Model, Pd};

/// Observe backing allocation cleanup without accessing private buffer fields.
struct ObservedAllocator;

#[global_allocator]
static ALLOCATOR: ObservedAllocator = ObservedAllocator;

#[derive(Clone, Copy, Default)]
struct BackingEvents {
    allocations: usize,
    address: usize,
    frees: usize,
}

thread_local! {
    static BACKING: Cell<Option<BackingEvents>> = const { Cell::new(None) };
}

// SAFETY: every allocation operation delegates to System with the same arguments.
// Observation uses only thread-local Cells and never allocates or panics.
unsafe impl GlobalAlloc for ObservedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplies a valid allocation layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplies a valid allocation layout.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            let _ = BACKING.try_with(|events| {
                if let Some(mut value) = events.get() {
                    value.allocations = value.allocations.saturating_add(1);
                    if value.address == 0 {
                        value.address = ptr as usize;
                    }
                    events.set(Some(value));
                }
            });
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let _ = BACKING.try_with(|events| {
            if let Some(mut value) = events.get().filter(|value| value.address == ptr as usize) {
                value.frees = value.frees.saturating_add(1);
                events.set(Some(value));
            }
        });
        // SAFETY: the caller supplies the allocation's pointer and layout.
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller supplies a live allocation and valid new size.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Track this thread's zeroed backing allocation, resetting even after a panic.
fn observe_backing<T>(run: impl FnOnce() -> T) -> (T, BackingEvents) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            BACKING.with(|events| events.set(None));
        }
    }

    BACKING.with(|events| {
        assert!(events.get().is_none(), "nested allocation observation");
        events.set(Some(BackingEvents::default()));
    });
    let reset = Reset;
    let result = run();
    let events = BACKING.with(|events| events.take().unwrap());
    drop(reset);
    (result, events)
}

fn device(seed: u64) -> (Model, Pd<Model>) {
    let model = Model::new(seed);
    let pd = Context::new(model.clone()).alloc_pd().unwrap();
    (model, pd)
}

#[test]
fn refused_allocation_frees_backing_and_the_next_allocation_succeeds() {
    let (model, pd) = device(101);
    model.clone().refuse_next_mr_registration();

    let (result, events) = observe_backing(|| pd.alloc::<u64>(37));
    assert_eq!(result.err(), Some(Error::Os(11)));
    assert_eq!(model.registered(), 0);
    assert_eq!(events.allocations, 1);
    assert_eq!(
        events.frees, 1,
        "registration refusal leaked its allocation"
    );

    let mut buf = pd.alloc::<u64>(37).unwrap();
    assert_eq!(buf.len(), 37);
    assert_eq!(buf.pd_id(), pd.id());
    assert!(buf.iter().all(|&word| word == 0));
    buf.fill(0x1234_5678_9abc_def0);
    assert_eq!(model.registered(), 1);
    drop(buf);
    assert_eq!(model.registered(), 0);
}

#[test]
fn refused_exposure_returns_the_same_writable_subrange() {
    let (model, pd) = device(102);
    let (_, events) = observe_backing(|| {
        let mut buf = pd.alloc::<u16>(10).unwrap();
        for (i, word) in buf.iter_mut().enumerate() {
            *word = i as u16;
        }
        let (buf, sibling) = buf.split_at(4);
        let address = buf.as_ptr();
        let len = buf.len();
        let pd_id = buf.pd_id();
        model.refuse_next_mr_registration();

        let failure = buf.expose().unwrap_err();
        assert_eq!(failure.error, Error::Os(11));
        let mut buf = failure.buf;
        assert_eq!(buf.as_ptr(), address);
        assert_eq!(buf.len(), len);
        assert_eq!(buf.pd_id(), pd_id);
        assert_eq!(&buf[..], &[0, 1, 2, 3]);
        assert_eq!(model.registered(), 1);
        buf[1] = 99;
        assert_eq!(&sibling[..], &[4, 5, 6, 7, 8, 9]);

        let exposed = buf.expose().unwrap();
        assert_eq!(model.registered(), 2);
        assert_eq!(exposed.remote().addr, address as u64);
        assert_eq!(exposed.remote().len, (len * size_of::<u16>()) as u64);
        let buf = exposed.try_reclaim().unwrap();
        assert_eq!(buf.as_ptr(), address);
        assert_eq!(&buf[..], &[0, 99, 2, 3]);
        assert_eq!(model.registered(), 1);
        drop(buf);
        assert_eq!(model.registered(), 1, "the sibling still owns its bytes");
        drop(sibling);
        assert_eq!(model.registered(), 0);
    });
    assert_eq!(events.allocations, 1);
    assert_eq!(events.frees, 1);
}

#[test]
fn invalid_allocations_do_not_reach_registration() {
    let (model, pd) = device(103);
    model.refuse_next_mr_registration();
    let (_, events) = observe_backing(|| {
        assert_eq!(
            pd.alloc::<u8>(0).err(),
            Some(Error::Invalid("an empty buffer"))
        );
        assert_eq!(
            pd.ring_buf(0).err(),
            Some(Error::Invalid("an empty buffer"))
        );
        assert_eq!(
            pd.alloc::<()>(1).err(),
            Some(Error::Invalid("zero-sized elements"))
        );
        assert_eq!(
            pd.alloc::<u64>(usize::MAX).err(),
            Some(Error::Invalid("buffer size"))
        );
        assert_eq!(
            pd.alloc::<u8>(isize::MAX as usize + 1).err(),
            Some(Error::Invalid("buffer size"))
        );
    });
    assert_eq!(events.allocations, 0);
    assert_eq!(model.registered(), 0);
    assert_eq!(pd.alloc::<u8>(8).err(), Some(Error::Os(11)));
    let buf = pd.alloc::<u8>(8).unwrap();
    assert_eq!(&buf[..], &[0; 8]);
    drop(buf);
    assert_eq!(model.registered(), 0);
}

#[test]
fn split_ranges_and_repeated_frozen_views_keep_their_owners() {
    let (model, pd) = device(104);
    let (_, events) = observe_backing(|| {
        let buf = pd.alloc::<u32>(12).unwrap();
        let (empty_left, buf) = buf.split_at(0);
        let len = buf.len();
        let (buf, empty_right) = buf.split_at(len);
        let (mut left, rest) = buf.split_at(4);
        let (mut middle, mut right) = rest.split_at(3);
        left.fill(11);
        middle.fill(22);
        right.fill(33);
        let address = middle.as_ptr();
        let frozen = middle.freeze();
        let view = frozen.slice(1..3).slice(..1);
        let repeated: Vec<_> = (0..8).map(|_| view.slice(..)).collect();
        let frozen = frozen.try_thaw().unwrap_err();
        assert_eq!(&view[..], &[22]);
        assert!(repeated.iter().all(|view| view.len() == 1 && view[0] == 22));
        drop(view);
        let frozen = frozen.try_thaw().unwrap_err();
        drop(repeated);

        let mut middle = frozen.try_thaw().unwrap();
        assert_eq!(middle.as_ptr(), address);
        assert_eq!(&middle[..], &[22; 3]);
        middle.fill(44);
        assert_eq!(&left[..], &[11; 4]);
        assert_eq!(&right[..], &[33; 5]);
        assert!(empty_left.is_empty());
        assert!(empty_right.is_empty());
        drop((left, middle, right, empty_left));
        assert_eq!(
            model.registered(),
            1,
            "the empty end still retains its owner"
        );
        drop(empty_right);
        assert_eq!(model.registered(), 0);
    });
    assert_eq!(events.allocations, 1);
    assert_eq!(events.frees, 1);
}

#[test]
fn a_full_ring_chunk_waits_for_its_last_slice_before_each_reuse() {
    let (model, pd) = device(105);
    let (_, events) = observe_backing(|| {
        let mut ring = pd.ring_buf(8).unwrap();
        for byte in 1..=8 {
            let chunk = ring.push(&[byte; 8]).unwrap();
            let clone = chunk.clone();
            let view = chunk.slice(1..7).slice(2..4);
            drop(chunk);
            assert_eq!(ring.in_use(), 8);
            assert!(ring.push(&[0]).is_none());
            drop(clone);
            assert_eq!(&view[..], &[byte; 2]);
            assert!(ring.push(&[0]).is_none());
            drop(view);
            assert_eq!(ring.in_use(), 0);
        }
        assert_eq!(model.registered(), 1);
        drop(ring);
        assert_eq!(model.registered(), 0);
    });
    assert_eq!(events.allocations, 1);
    assert_eq!(events.frees, 1);
}

#[test]
fn wrapped_ring_reuse_preserves_chunks_with_live_clones() {
    let (model, pd) = device(106);
    let mut ring = pd.ring_buf(16).unwrap();
    let first = ring.push(b"abcdefgh").unwrap();
    let second = ring.push(b"ijklmnop").unwrap();
    let held = second.slice(2..6);
    drop(first);
    let wrapped = ring.push(b"QRSTUV").unwrap();
    assert_eq!(&second[..], b"ijklmnop");
    assert_eq!(&wrapped[..], b"QRSTUV");
    assert_eq!(ring.in_use(), 14);
    drop(second);
    assert!(ring.push(b"xyz").is_none());
    assert_eq!(&held[..], b"klmn");
    drop(held);

    let tail = ring.push(b"0123456789").unwrap();
    assert_eq!(&wrapped[..], b"QRSTUV");
    assert_eq!(&tail[..], b"0123456789");
    assert_eq!(ring.in_use(), 16);
    drop((wrapped, tail));
    assert_eq!(ring.in_use(), 0);
    drop(ring);
    assert_eq!(model.registered(), 0);
}

#[test]
fn a_chunk_outlives_its_ring_and_thaws_after_its_last_other_view() {
    let (model, pd) = device(107);
    let (_, events) = observe_backing(|| {
        let mut ring = pd.ring_buf(16).unwrap();
        let chunk = ring.push(b"abcdefgh").unwrap();
        let view = chunk.slice(2..6);
        drop(ring);
        assert_eq!(model.registered(), 1);
        let view = view.try_thaw().unwrap_err();
        assert_eq!(&view[..], b"cdef");
        drop(chunk);

        let mut buf = view.try_thaw().unwrap();
        assert_eq!(
            &buf[..],
            b"abcdefgh",
            "thaw recovers the whole frozen chunk"
        );
        buf.fill(7);
        drop(buf);
        assert_eq!(model.registered(), 0);
    });
    assert_eq!(events.allocations, 1);
    assert_eq!(events.frees, 1);
}
