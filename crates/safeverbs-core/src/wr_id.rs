//! The `wr_id` of a WQE, and how a completion is routed back by it.
//!
//! One CQ serves every QP of a worker, so the `wr_id` the hardware reports
//! must name the QP, the queue and the WQE. It holds the QP's index in the
//! worker in its top 16 bits, the queue in the next bit (set for a receive)
//! and the low 47 bits of the WQE's number in that queue's ring
//! ([`wr_id`]). The index and the queue come back with a shift and a mask
//! ([`index_of`], [`is_recv`]); the full number from the oldest WQE in
//! flight ([`wqe_of`]): the WQEs in flight span at most the queue's depth
//! (below 2^32), so exactly one number at or after the oldest one, and
//! less than 2^47 after it, has those low bits.
//!
//! Proved (inside `verus!`): the three decoders invert [`wr_id`]
//! (`lemma_wr_id_fields` and the postconditions). This is the arithmetic
//! half of the coupling between a request and its WQE in M: the worker sets
//! each WQE's `wr_id` with [`wr_id`] and recovers the WQE a completion names
//! with the decoders.

use vstd::prelude::*;

verus! {

/// The mask of the WQE's number in a `wr_id`.
pub const WQE_MASK: u64 = 0x7fff_ffff_ffff;

/// The bit of a `wr_id` that marks a receive.
pub const RECV_BIT: u64 = 0x8000_0000_0000;

/// Where the QP's index starts in a `wr_id`.
pub const INDEX_SHIFT: u64 = 48;

/// How far after the oldest WQE in flight a completion's WQE may lie and
/// still be recovered by [`wqe_of`].
#[allow(dead_code)]
pub const WQE_SPAN: u64 = 0x8000_0000_0000;

/// The `wr_id` of WQE `wqe` of QP `index`'s receive queue if `recv`, else
/// of its send queue.
pub open spec fn spec_wr_id(index: u16, recv: bool, wqe: u64) -> u64 {
    ((index as u64) << INDEX_SHIFT) | (if recv {
        RECV_BIT
    } else {
        0u64
    }) | (wqe & WQE_MASK)
}

/// The fields of a `wr_id` come back: the index, the queue and the low
/// bits of the WQE's number.
pub proof fn lemma_wr_id_fields(index: u16, recv: bool, wqe: u64)
    ensures
        spec_wr_id(index, recv, wqe) >> INDEX_SHIFT == index as u64,
        (spec_wr_id(index, recv, wqe) & RECV_BIT != 0) == recv,
        spec_wr_id(index, recv, wqe) & WQE_MASK == wqe & WQE_MASK,
{
    let i = index as u64;
    let r: u64 = if recv {
        RECV_BIT
    } else {
        0u64
    };
    assert(i <= 0xffff);
    assert(r == 0 || r == 0x8000_0000_0000u64);
    assert(i <= 0xffff && (r == 0 || r == 0x8000_0000_0000u64) ==> (((i << 48u64) | r | (wqe
        & 0x7fff_ffff_ffffu64)) >> 48u64 == i && (((i << 48u64) | r | (wqe & 0x7fff_ffff_ffffu64))
        & 0x8000_0000_0000u64 != 0) == (r != 0) && ((i << 48u64) | r | (wqe & 0x7fff_ffff_ffffu64))
        & 0x7fff_ffff_ffffu64 == wqe & 0x7fff_ffff_ffffu64)) by (bit_vector);
}

/// Two WQE numbers with the same low bits, both at or after `retired` and
/// less than [`WQE_SPAN`] after it, are the same.
pub proof fn lemma_low_bits_determine(a: u64, b: u64, retired: u64)
    requires
        retired <= a,
        retired <= b,
        a - retired < WQE_SPAN,
        b - retired < WQE_SPAN,
        a & WQE_MASK == b & WQE_MASK,
    ensures
        a == b,
{
    assert(retired <= a && retired <= b && sub(a, retired) < 0x8000_0000_0000u64 && sub(b, retired)
        < 0x8000_0000_0000u64 && a & 0x7fff_ffff_ffffu64 == b & 0x7fff_ffff_ffffu64 ==> a == b)
        by (bit_vector);
}

/// The `wr_id` of WQE `wqe` of QP `index`'s receive queue if `recv`, else
/// of its send queue.
#[inline]
pub fn wr_id(index: u16, recv: bool, wqe: u64) -> (id: u64)
    ensures
        id == spec_wr_id(index, recv, wqe),
        id >> INDEX_SHIFT == index as u64,
        (id & RECV_BIT != 0) == recv,
        id & WQE_MASK == wqe & WQE_MASK,
{
    proof {
        lemma_wr_id_fields(index, recv, wqe);
    }
    ((index as u64) << INDEX_SHIFT) | (if recv {
        RECV_BIT
    } else {
        0
    }) | (wqe & WQE_MASK)
}

/// The QP index a `wr_id` names.
#[inline]
pub fn index_of(wr_id: u64) -> (index: usize)
    ensures
        index == wr_id >> INDEX_SHIFT,
        index < 0x1_0000,
{
    assert(wr_id >> 48u64 <= 0xffffu64) by (bit_vector);
    (wr_id >> INDEX_SHIFT) as usize
}

/// Whether a `wr_id` names a receive.
#[inline]
pub fn is_recv(wr_id: u64) -> (recv: bool)
    ensures
        recv == (wr_id & RECV_BIT != 0),
{
    wr_id & RECV_BIT != 0
}

/// The ring's number of the WQE a completion names, from its low bits and
/// the oldest WQE in flight (`retired`): the one at or after `retired`, less
/// than [`WQE_SPAN`] after it, with those low bits. (A WQE before `retired`
/// maps far ahead, where the ring ignores it.) How far after `retired` it lies is the difference
/// of the low bits, modulo 2^47, and a subtraction of the whole words has those low bits.
#[inline(always)]
pub fn wqe_of(wr_id: u64, retired: u64) -> (wqe: u64)
    ensures
        forall|w: u64|
            retired <= w && w - retired < WQE_SPAN && w & WQE_MASK == wr_id & WQE_MASK ==> wqe
                == w,
{
    retired.wrapping_add(wr_id.wrapping_sub(retired) & WQE_MASK)
}

} // verus!
