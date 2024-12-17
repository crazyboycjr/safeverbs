// scatter gather DMA
// -> Lifetime cannot satisfy out needs. That's why we have MemorySegment<P>
// allow non-overlappped post_send and post_recv
// allow overlapped post_recvs in unsafe
// Disallow:
//   Overlapping post_recv
//   Concurrent post_recv and post_send for overlapped memory region

// TODO: implement it with balance tree
use crate::{RO, RW};
use core::any::TypeId;
use core::ops::{Range, RangeBounds};
use rudac::tree;
use rudac::util::Interval;
use std::collections::HashMap;
use std::ops::Bound::*;

pub(crate) struct IntervalTree {
    ro: tree::IntervalTree<usize>,
    // to handle multiple identical intervals.
    ro_cnt: HashMap<Range<usize>, usize>,
    rw: tree::IntervalTree<usize>,
}

impl IntervalTree {
    pub(crate) fn new() -> Self {
        IntervalTree {
            ro: tree::IntervalTree::init(),
            ro_cnt: HashMap::default(),
            rw: tree::IntervalTree::init(),
        }
    }

    fn check_overlap(range: Range<usize>, intervals: &tree::IntervalTree<usize>) -> bool {
        intervals.overlaps(&Interval::new(Included(range.start), Excluded(range.end)))
    }

    fn insert(range: Range<usize>, intervals: &mut tree::IntervalTree<usize>) {
        intervals.insert(Interval::new(
            range.start_bound().cloned(),
            range.end_bound().cloned(),
        ))
    }

    pub(crate) fn try_insert_readonly(&mut self, range: Range<usize>) -> Result<(), ()> {
        // returns Err if it conflicts with any rw_interval
        // for all r, where r.start < range.end, find the maximal of r.end
        if Self::check_overlap(range.clone(), &self.rw) {
            return Err(());
        }
        *self.ro_cnt.entry(range.clone()).or_default() += 1;
        Self::insert(range, &mut self.ro);
        Ok(())
    }

    pub(crate) fn try_insert_readwrite(&mut self, range: Range<usize>) -> Result<(), ()> {
        // returns Err if it conflicts with any interval
        if Self::check_overlap(range.clone(), &self.rw) {
            return Err(());
        }
        if Self::check_overlap(range.clone(), &self.ro) {
            return Err(());
        }
        Self::insert(range, &mut self.rw);
        Ok(())
    }

    fn _remove(range: &Range<usize>, intervals: &mut tree::IntervalTree<usize>) -> Result<(), ()> {
        intervals.delete(&Interval::new(Included(range.start), Excluded(range.end)));
        Ok(())
    }

    pub(crate) fn remove<P: 'static>(&mut self, range: &Range<usize>) -> Result<(), ()> {
        if TypeId::of::<P>() == TypeId::of::<RO>() {
            use std::collections::hash_map::Entry;
            match self.ro_cnt.entry(range.clone()) {
                Entry::Occupied(mut entry) => {
                    *entry.get_mut() -= 1;
                    if *entry.get_mut() == 0 {
                        entry.remove();
                    }
                }
                Entry::Vacant(_) => {
                    // deleting an element that does not exist
                    return Err(());
                }
            }
            Self::_remove(range, &mut self.ro)
        } else if TypeId::of::<P>() == TypeId::of::<RW>() {
            Self::_remove(range, &mut self.rw)
        } else {
            unreachable!()
        }
    }
}
