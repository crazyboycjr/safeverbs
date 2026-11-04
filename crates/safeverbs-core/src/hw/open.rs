//! A request being written: nothing of it reaches the hardware or the ring until it is closed,
//! when its signaling is known, so that neither has a state for it, and the program may do
//! anything between two of its WQEs.
//!
//! What it is written into comes from its QP and goes back there (`Hw::open_request`,
//! `Hw::give_back`): nothing is allocated per request. WQEs of one frozen buffer keep one reference
//! to it, and a list of several buffers is reused too, by the requests that hand it back.
#![allow(unused_imports)]
use super::*;
use crate::buf::{Frozen, Lease, Lend, Peek, same_frozen_arc};
use crate::nic::RemoteBuf;
use std::sync::Arc;

verus! {

/// A WQE of a request being written: its operation and its scatter/gather list, which is its
/// `sge` if it has one, as WQEs mostly do, else lies in `OpenRequest::gathered` from `first_sge`.
#[derive(Clone, Copy)]
pub struct OpenWqe {
    pub(super) op: WrOp,
    pub(super) sge: Sge,
    pub(super) first_sge: u32,
    pub(super) sges: u32,
}

/// What `OpenRequest::wqes` is filled with until it is written.
pub(super) const EMPTY_WQE: OpenWqe = OpenWqe {
    op: WrOp::Send,
    sge: Sge { addr: 0, length: 0, lkey: 0 },
    first_sge: 0,
    sges: 0,
};

/// A request being written, a value of its own: `Engine::open_request` makes one, its WQEs go in
/// without the engine while all is well, and `Engine::close_open` or `Engine::submit_open` send
/// it. Dropping it drops its WQEs, which never left it.
#[verifier::reject_recursive_types(N)]
pub struct OpenRequest<N: Device> {
    /// Its WQEs, the first `n` of them; as long as its QP's requests may be.
    pub(super) wqes: Vec<OpenWqe>,
    /// How many it has.
    pub(super) n: usize,
    /// How many it may have before its QP has to be asked: 0 sends every WQE there.
    pub(super) limit: usize,
    /// The longest message of its QP.
    pub(super) max_len: u32,
    /// The protection domain of its QP.
    pub(super) pd_id: u64,
    /// The frozen allocation of its last WQE, kept apart from the rest of what keeps its buffers
    /// alive, since most WQEs ask for it: one comparison.
    pub(super) last: Option<Arc<Frozen<N>>>,
    /// What keeps its other buffers alive.
    pub(super) leases: Leases<N>,
    /// The scatter/gather lists of its WQEs of several elements.
    pub(super) gathered: Vec<Sge>,
}

/// Whether a WQE doing `op` on `len` bytes needs nothing more of its QP than the length and the
/// protection domain.
#[inline(always)]
fn plain_op(op: &WrOp, len: u64) -> bool {
    match *op {
        WrOp::Send | WrOp::SendImm(_) => true,
        WrOp::Write(r) | WrOp::WriteImm(r, _) => len <= r.len,
        WrOp::Read(_) | WrOp::FetchAdd { .. } | WrOp::CompareSwap { .. } => false,
    }
}

impl<N: Device> OpenRequest<N> {
    /// One that holds nothing and takes nothing.
    pub fn closed() -> (r: Self) {
        OpenRequest {
            wqes: Vec::new(),
            n: 0,
            limit: 0,
            max_len: 0,
            pd_id: 0,
            last: None,
            leases: no_leases(),
            gathered: Vec::new(),
        }
    }

    /// How many WQEs it has.
    #[inline(always)]
    pub fn len(&self) -> (n: usize) {
        self.n
    }

    /// Whether it has no WQE.
    #[inline(always)]
    pub fn is_empty(&self) -> (empty: bool) {
        self.n == 0
    }

    /// Its regions are registered (its leases say so).
    #[inline(always)]
    pub(super) fn keeps_registered(&self, pd: &Pd<N>, Tracked(token): Tracked<&mut NicToken>) {
        registered_all(&self.leases, pd, Tracked(token));
    }

    /// Whether it still holds what it keeps from its QP.
    #[inline(always)]
    pub(crate) fn holds_storage(&self) -> (holds: bool) {
        self.wqes.len() > 0
    }

    /// Whether the next WQE goes in without its QP being asked.
    #[inline(always)]
    pub fn has_room(&self) -> (room: bool) {
        self.n < self.limit
    }

    /// From now on every WQE goes to its QP.
    pub fn stop(&mut self) {
        self.limit = 0;
    }

    /// What WQE `i`, one of those it has, does.
    #[inline(always)]
    pub(super) fn op_of(&self, i: usize) -> (op: &WrOp) {
        // SAFETY: `i < n`, and `n` is at most the length of `wqes`.
        unsafe { &self.wqes.as_slice().get_unchecked(i).op }
    }

    /// The scatter/gather list of WQE `i`, one of those it has.
    #[inline(always)]
    pub(super) fn sges_of(&self, i: usize) -> (sges: &[Sge]) {
        // SAFETY: `i < n`, and `n` is at most the length of `wqes`.
        let wqe = unsafe { self.wqes.as_slice().get_unchecked(i) };
        if wqe.sges == 1 {
            core::slice::from_ref(&wqe.sge)
        } else {
            let from = wqe.first_sge as usize;
            vstd::slice::slice_subrange(self.gathered.as_slice(), from, from + wqe.sges as usize)
        }
    }

    /// The bytes of its last WQE, which has a gather list.
    pub(super) fn last_gathered_bytes(&self) -> (bytes: u32) {
        let last = &self.wqes[self.n - 1];
        let from = last.first_sge as usize;
        let mut bytes = 0u32;
        let mut i = 0;
        while i < last.sges as usize
        {
            bytes += self.gathered[from + i].length;
            i += 1;
        }
        bytes
    }

    /// Its buffers, leaving it none. Starting a list of several takes the room in `spare`.
    #[inline(always)]
    pub(super) fn take_leases_with(&mut self, spare: &mut Vec<Lease<N>>) -> (leases: Leases<N>) {
        self.n = 0;
        let mut last = None;
        core::mem::swap(&mut last, &mut self.last);
        let mut leases = no_leases();
        core::mem::swap(&mut leases, &mut self.leases);
        match last {
            Some(frozen) => {
                if leases.has_room() {
                    leases.keep_in_room(Lease::Shared(frozen));
                } else {
                    leases.keep_frozen(frozen, spare);
                }
            },
            None => {},
        }
        leases
    }

    /// Its buffers, leaving it none (of a request that is not going out).
    pub fn take_leases(&mut self) -> (leases: Leases<N>) {
        self.take_leases_with(&mut Vec::new())
    }

    /// Adds a WQE doing `op` on `part` if nothing is out of the common: the checks pass, it has
    /// room, and keeping `part` takes no allocation. Otherwise `part` comes back and nothing
    /// changed, for its QP to say (`Engine::push_open`).
    #[inline(always)]
    pub fn try_push(&mut self, op: WrOp, part: Part<N>) -> (r: core::result::Result<(), Part<N>>) {
        let sge = part_sge_of(&part);
        let k = self.n;
        if k < self.limit && plain_op(&op, sge.length as u64) && part_pd_of(&part) == self.pd_id
            && sge.length <= self.max_len
        {
            match self.try_keep(part) {
                Ok(()) => {
                    write_wqe(&mut self.wqes, k, OpenWqe { op, sge, first_sge: 0, sges: 1 });
                    self.n = k + 1;
                    Ok(())
                },
                Err(part) => Err(part),
            }
        } else {
            Err(part)
        }
    }

    /// Keeps `part` if that takes no allocation, else hands it back: a frozen allocation is the new
    /// last one, and the one before it joins the others if they have room.
    #[inline(always)]
    fn try_keep(&mut self, part: Part<N>) -> (r: core::result::Result<(), Part<N>>) {
        match part.frozen_of() {
            Some(frozen) => {
                match &self.last {
                    Some(last) => {
                        if same_frozen_arc(last, frozen) {
                            return Ok(());
                        }
                        if !self.leases.has_room() {
                            return Err(part);
                        }
                    },
                    None => {},
                }
            },
            None => {
                if self.last.is_some() || !self.leases.is_empty() {
                    return Err(part);
                }
            },
        }
        match part.into_frozen() {
            Ok(frozen) => {
                let mut last = Some(frozen);
                core::mem::swap(&mut last, &mut self.last);
                match last {
                    Some(earlier) => self.leases.keep_in_room(Lease::Shared(earlier)),
                    None => {},
                }
            },
            Err(lease) => self.leases.keep_in_room(lease),
        }
        Ok(())
    }

    /// Whether a WQE of `peek`'s bytes may go in with nothing more to keep: they are of the frozen
    /// allocation of its last WQE, of its protection domain then, and it has room.
    #[inline(always)]
    fn takes_peek(&self, peek: &Peek<'_, N>) -> (takes: bool) {
        self.n < self.limit && peek.bytes <= self.max_len as usize && match &self.last {
            Some(frozen) => same_frozen_arc(frozen, peek.frozen),
            None => false,
        }
    }

    /// [`OpenRequest::try_push`] of bytes of the frozen allocation of its last WQE, which is
    /// already kept alive: nothing to count, and nothing to store but the WQE. Whether it did.
    #[inline(always)]
    pub(crate) fn try_push_peek(&mut self, op: WrOp, peek: Peek<'_, N>) -> (pushed: bool) {
        if self.takes_peek(&peek) && plain_op(&op, peek.bytes as u64) {
            let k = self.n;
            write_wqe(&mut self.wqes, k, OpenWqe { op, sge: peek.sge(), first_sge: 0, sges: 1 });
            self.n = k + 1;
            true
        } else {
            false
        }
    }

    /// [`OpenRequest::try_push_peek`] of a plain WRITE to `dst`, written where it stays.
    #[inline(always)]
    pub(crate) fn try_push_peek_write(&mut self, dst: RemoteBuf, peek: Peek<'_, N>) -> (pushed: bool) {
        if self.takes_peek(&peek) && peek.bytes as u64 <= dst.len {
            let k = self.n;
            let sge = peek.sge();
            let wqe = wqe_at(&mut self.wqes, k);
            wqe.op = WrOp::Write(dst);
            wqe.sge = sge;
            wqe.sges = 1;
            self.n = k + 1;
            true
        } else {
            false
        }
    }

    /// Adds the lease of a WQE of its QP's general path: a frozen allocation is the new last one.
    pub(super) fn keep_general(&mut self, part: Part<N>, Ghost(access): Ghost<safeverbs_spec::abs::Dir>, spare: &mut Vec<Lease<N>>) {
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
                        Some(earlier) => self.leases.keep_frozen(earlier, spare),
                        None => {},
                    }
                }
            },
            Err(lease) => {
                let mut last = None;
                core::mem::swap(&mut last, &mut self.last);
                match last {
                    Some(earlier) => self.leases.keep_frozen(earlier, spare),
                    None => {},
                }
                self.leases.keep_lease(lease, spare);
            },
        }
    }
}

/// WQE `k`, which `k < limit <= wqes.len()` allows: no bounds check, and so no panic edge with the
/// program's buffer in hand.
#[inline(always)]
fn wqe_at(wqes: &mut Vec<OpenWqe>, k: usize) -> (wqe: &mut OpenWqe)
    requires
        k < old(wqes)@.len(),
{
    // SAFETY: an `OpenRequest`'s `limit` is at most the length of its `wqes`, which `Hw::open_request`
    // sees to, and `k` is below it.
    unsafe { wqes.as_mut_slice().get_unchecked_mut(k) }
}

/// Writes WQE `k`, as [`wqe_at`] allows.
#[inline(always)]
fn write_wqe(wqes: &mut Vec<OpenWqe>, k: usize, wqe: OpenWqe)
    requires
        k < old(wqes)@.len(),
{
    *wqe_at(wqes, k) = wqe;
}

} // verus!

impl<N: Device> OpenRequest<N> {
    /// Adds a WQE of `op` over what `buf` lends, when it is of the frozen allocation of the last
    /// WQE, which is already kept alive: nothing to count, and nothing to store but the WQE.
    /// Whether it did.
    #[inline(always)]
    pub fn try_push_lent<B: Lend<N>>(&mut self, op: WrOp, buf: &B) -> bool {
        self.try_push_peek(op, buf.peek())
    }

    /// [`OpenRequest::try_push_lent`] of a plain WRITE to `dst`, written where it stays.
    #[inline(always)]
    pub fn try_push_write<B: Lend<N>>(&mut self, dst: RemoteBuf, buf: &B) -> bool {
        self.try_push_peek_write(dst, buf.peek())
    }
}
