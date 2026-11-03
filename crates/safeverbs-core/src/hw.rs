//! The hardware side of a worker, verified against the NIC model M: its CQ,
//! its QPs' hardware queues and rings, and its share of M.
//!
//! A [`Hw`] holds everything of a worker that M sees: the CQ, each QP's
//! hardware handle with the rings of its two queues, and the worker's token
//! (`crate::trusted::NicToken`: M's state as the worker last saw it, and its
//! share of M). Every call into the NIC goes through the trusted verbs of
//! `crate::trusted`, whose preconditions are M's, so Verus checks S2 at each
//! call. The invariant (`Hw::inv`) couples the rings to M:
//!
//! - **Routing.** QP `i` of the table is M's QP `id`; the WQEs M holds in
//!   its send queue are the ring's WQEs from (at or after) the ring's oldest
//!   in flight to the last committed, and the `k`-th has the `wr_id` of QP
//!   `i`, the send queue and WQE number `sq_head + k`
//!   (`crate::wr_id::spec_wr_id`); the same for the receive queue. So the
//!   completion M's `poll_cq` pops names the QP, the queue and the WQE that
//!   [`Hw::progress`] routes it to.
//! - **Local memory.** Every actual SQ/RQ WQE matches its live native
//!   request interval's exact opcode and SGE ledger. Written WRs retain
//!   their guards in staged requests or the open ledger; the final held
//!   WQE retains its parts directly. Refused results never authorize DMA,
//!   even when their WQE numbers are reused. The byte lemmas expose the
//!   corresponding source/destination loan and physical lifetime guard.
//! - **Capacity.** The CQ's entries reserved in M are exactly what the
//!   worker counts ([`Hw::reserved`]); a QP is created only if its depths fit
//!   ([`Hw::create_qp`]), and each ring keeps at most its depth of WQEs in
//!   flight, so the CQ never overruns (M's `lemma_cq_never_overflows`).
//! - **Teardown.** A QP is destroyed only when neither ring has a WQE in
//!   flight, which the coupling turns into M's empty queues, `destroy_qp`'s
//!   precondition ([`Hw::destroy`]).
//! - **State.** The worker posts work only on a QP it connected or moved to
//!   the error state, which M sees in `Rts` or `Err`.
//!
//! The unverified worker (`crate::worker`) drives a `Hw` through its public
//! methods only; each requires and keeps the invariant.
//
// The imports are used by ghost code too; `global` erases to a block. The
// code keeps the plain forms the proofs reason about (explicit matches,
// lengths, loops).
#![allow(unused_imports, unused_braces)]
#![allow(
    clippy::single_match,
    clippy::len_zero,
    clippy::manual_map,
    clippy::explicit_counter_loop,
    clippy::question_mark,
    clippy::needless_return
)]

use safeverbs_spec::nic::*;
use safeverbs_spec::refinement::*;
use safeverbs_spec::share::*;
use safeverbs_spec::types::{CqId, Key, PdId, QpId};
use vstd::prelude::*;

use crate::buf::{BufError, Leases, Part};
use crate::error::{Error, Result};
use crate::nic::{
    ConnectAttr, DeviceCaps, Endpoint, Nic as Device, NicCq, NicQp, QpCaps, QpType, Sge, WrOp,
};
use crate::pd::Pd;
use crate::ring::{Ring, Seq as RingSeq};
use crate::trusted::*;
use crate::wc::{Opcode, Wc};
use crate::wr_id::*;

mod outcomes;
mod separation;

verus! {

global size_of usize == 8;

/// The most QPs of one worker: their index takes the top 16 bits of a
/// `wr_id`.
pub const MAX_QPS: usize = 0x1_0000;

/// The most scatter/gather elements of one WQE (they are kept on the
/// stack).
pub const MAX_SGES: usize = 16;

/// What the worker knows of a hardware QP's state: it decides which verbs
/// the worker may call (M's preconditions: work is posted only in RTS or
/// ERR, and a QP connects from RESET).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    /// Created (RESET): it may connect; it takes no work.
    Reset,
    /// Connected (RTS, or moved to ERR by the NIC since), or moved to ERR
    /// by the worker: it takes work (in ERR, to flush it).
    Ready,
    /// A connection failed part way (INIT or RTR): it neither connects
    /// again nor takes work, until it is moved to the error state.
    Broken,
}

/// A QP of a worker: its hardware handle and the rings of its queues.
#[verifier::reject_recursive_types(N)]
pub struct HwQp<N: Device> {
    hw: N::Qp,
    send: Ring<Leases<N>>,
    recv: Ring<Leases<N>>,
    caps: QpCaps,
    qp_type: QpType,
    /// Atomics are allowed (a reliable connection on a device with them).
    atomics: bool,
    phase: Phase,
    /// Whether WQEs were written since the last doorbell (`start` called).
    started: bool,
    /// Being removed: its owners are gone and it waits for the device to
    /// flush its work.
    removed: bool,
    /// Removal waits to post the drain WQE on its send queue (for room).
    drain_pending: bool,
    /// Whether a doorbell was refused since the event loop last looked.
    refused: bool,
    /// Whether the QP is in the worker's list of QPs with requests to hand
    /// out.
    dirty: bool,
    /// M's name of the QP.
    #[allow(dead_code)]
    id: Ghost<QpId>,
    /// The protection domain's number ([`Pd::id`]).
    #[allow(dead_code)]
    pd_id: Ghost<u64>,
    /// The WQE the open request holds back until it knows whether it is
    /// its last (so that the last can be written signaled), with its parts.
    held: Option<Held>,
    held_parts: Vec<Part<N>>,
    /// The parts of the WQE being gathered.
    building: Vec<Part<N>>,
    /// The leases of the open request's WQEs written so far.
    open_leases: Leases<N>,
    /// The local byte count of the final held WQE, for inferred success.
    held_bytes: u64,
    /// The actual final WR written for each native send request (erased).
    #[allow(dead_code)]
    send_tails: Ghost<Map<int, SendWr>>,
}

/// A WQE held back: its `wr_id`, operation and scatter/gather list.
#[derive(Clone, Copy)]
struct Held {
    wr_id: u64,
    op: WrOp,
    sges: [Sge; MAX_SGES],
    n: usize,
}

/// Exact logical inputs appended by the native pop loop, in its actual order.
pub(crate) open spec fn collected_parts<N: Device>(
    parts: Seq<Part<N>>, remaining: int, access: safeverbs_spec::abs::Dir,
) -> Seq<crate::buf::Loan<N>> {
    Seq::new((parts.len() - remaining) as nat, |i: int| crate::buf::Loan {
        part: parts[parts.len() - 1 - i], access: Some(access),
    })
}

/// Moves all SGE guards into their request without changing the native algorithm.
/// The erased ledger records even a shared input whose redundant Arc is dropped.
#[inline]
fn keep_parts<N: Device>(
    leases: &mut Leases<N>, parts: &mut Vec<Part<N>>,
    Ghost(wqe): Ghost<crate::buf::WqeLoans>,
)
    requires old(leases).inv(), old(leases).fully_recorded(),
        wqe.first == old(leases).loans().len(),
        wqe.sges.len() == old(parts)@.len(),
        forall|i: int| 0 <= i < old(parts)@.len()
            ==> wqe.sges[i] == part_sge(#[trigger] old(parts)@[i])
                && (wqe.access() is In ==> part_writable(old(parts)@[i])),
    ensures final(parts)@.len() == 0,
        final(leases).inv(),
        final(leases).fully_recorded(),
        final(leases).loans() == old(leases).loans()
            + collected_parts(old(parts)@, 0, wqe.access()),
        final(leases).wqes() == old(leases).wqes().push(wqe),
{
    let ghost access = wqe.access();
    let ghost original = parts@;
    let ghost prefix = leases.loans();
    let ghost old_wqes = leases.wqes();
    proof {
        leases.lemma_wqes();
        assert(collected_parts(original, original.len() as int, access) =~= Seq::empty());
    }
    while parts.len() > 0
        invariant
            leases.inv(),
            leases.wqes() == old_wqes,
            crate::buf::covers_inputs(old_wqes, prefix.len() as int),
            parts@.len() <= original.len(),
            parts@ == original.take(parts@.len() as int),
            leases.loans() == prefix + collected_parts(original, parts@.len() as int, access),
            forall|i: int| 0 <= i < original.len()
                ==> access is In ==> part_writable(#[trigger] original[i]),
            forall|i: int| 0 <= i < original.len()
                ==> wqe.sges[i] == part_sge(#[trigger] original[i]),
            wqe.first == prefix.len(), wqe.sges.len() == original.len(),
            forall|i: int| 0 <= i < old_wqes.len()
                ==> (#[trigger] old_wqes[i]).end() <= prefix.len(),
        decreases parts@.len(),
    {
        let ghost before = parts@;
        match parts.pop() {
            Some(part) => {
                proof { assert(part == original[before.len() - 1]); }
                keep_lease(leases, part, Ghost(access));
            },
            None => {},
        }
        proof {
            assert(parts@ =~= original.take(parts@.len() as int));
            let old_tail = collected_parts(original, before.len() as int, access);
            let new_tail = collected_parts(original, parts@.len() as int, access);
            assert(new_tail =~= old_tail.push(crate::buf::Loan {
                part: original[before.len() - 1], access: Some(access),
            }));
            assert(prefix + new_tail =~= (prefix + old_tail).push(crate::buf::Loan {
                part: original[before.len() - 1], access: Some(access),
            }));
        }
    }
    proof {
        assert forall|j: int| 0 <= j < wqe.sges.len() implies {
            let loan = #[trigger] leases.loans()[wqe.end() - 1 - j];
            &&& loan.part.spec_sge() == wqe.sges[j]
            &&& loan.access == Some(wqe.access())
        } by {
            let k = original.len() - 1 - j;
            assert(leases.loans()[wqe.end() - 1 - j] == collected_parts(original, 0, access)[k]);
            assert(leases.loans()[wqe.end() - 1 - j].part == original[j]);
        }
    }
    leases.record_wqe(Ghost(wqe));
}

/// The WR M sees for a held WQE.
spec fn held_wr(h: Held, signaled: bool) -> SendWr {
    send_wr(h.wr_id, signaled, h.op, h.sges@.take(h.n as int))
}

/// A held WQE with parts `parts`, number `num`, on a QP with attributes
/// `attr`, is one M accepts once its parts' regions are registered: its
/// `wr_id`, its parts' ranges, domain and uniqueness, its length.
spec fn held_ok<N: Device>(h: Held, parts: Seq<Part<N>>, index: u16, num: int, attr: QpAttr, pd_id: u64, max_msg: nat) -> bool {
    &&& h.wr_id == spec_wr_id(index, false, num as u64)
    &&& 1 <= h.n <= MAX_SGES
    &&& parts.len() == h.n
    &&& forall|j: int|
        0 <= j < h.n ==> h.sges@[j] == part_sge(#[trigger] parts[j]) && part_pd(parts[j]) == pd_id
            && (opcode_model(h.op).writes_local() ==> part_writable(parts[j]))
    &&& h.n <= attr.max_sge
    &&& opcode_model(h.op).allowed_on(attr.transport)
    &&& total_len(h.sges@.take(h.n as int).map_values(|sge: Sge| sge_model(sge))) <= max_msg
}

/// The hardware side of a worker: its CQ, its QPs and its share of M.
#[verifier::reject_recursive_types(N)]
pub struct Hw<N: Device> {
    cq: N::Cq,
    pd: Pd<N>,
    domain: Option<N::Domain>,
    caps: DeviceCaps,
    qps: Vec<Option<HwQp<N>>>,
    /// The QPs with retired requests the event loop has not handed out.
    dirty: Vec<u16>,
    /// Entries of the CQ.
    cq_size: u32,
    /// CQ entries reserved by the QPs attached.
    reserved: u32,
    token: Tracked<NicToken>,
    /// The next QP name to use (`NicToken::name`).
    next_name: Ghost<nat>,
}

// =============================================================================
// The coupling, as specifications

/// The send ring `r` of QP `index` is coupled to M's QP `qp`: the same
/// depth; M's send queue holds the ring's WQEs from at or after its oldest
/// in flight to its last committed; each has the `wr_id` of its number.
pub open spec fn sq_linked<L>(r: Ring<L>, index: u16, qp: Qp) -> bool {
    &&& r.inv()
    &&& r.spec_depth() == qp.attr.sq_depth
    &&& r.spec_wqe_retired() <= qp.sq_head
    &&& qp.sq_head + qp.sq.len() == r.spec_wqe_committed()
    &&& forall|k: int|
        0 <= k < qp.sq.len() ==> (#[trigger] qp.sq[k]).wr.wr_id == spec_wr_id(
            index,
            false,
            (qp.sq_head + k) as u64,
        )
}

/// The receive ring `r` of QP `index` is coupled to M's QP `qp`, as
/// [`sq_linked`].
pub open spec fn rq_linked<L>(r: Ring<L>, index: u16, qp: Qp) -> bool {
    &&& r.inv()
    &&& r.spec_depth() == qp.attr.rq_depth
    &&& r.spec_wqe_retired() <= qp.rq_head
    &&& qp.rq_head + qp.rq.len() == r.spec_wqe_committed()
    &&& r.spec_wqe_posted() == r.spec_wqe_committed()
    &&& !r.spec_is_open()
    &&& forall|k: int|
        0 <= k < qp.rq.len() ==> (#[trigger] qp.rq[k]).wr.wr_id == spec_wr_id(
            index,
            true,
            (qp.rq_head + k) as u64,
        )
}

/// Every concrete resource value held by the ring has its checked logical loan
/// and WQE provenance. Refused requests keep their original, unposted ledger.
pub(crate) open spec fn ring_resources_ok<N: Device>(r: Ring<Leases<N>>) -> bool {
    &&& forall|s: int| #[trigger] r.req(s) is Some ==> request_resources_ok(r.req(s)->0.leases)
    &&& forall|s: int| #[trigger] r.staged(s) is Some ==> request_resources_ok(r.staged(s)->0.leases)
    &&& forall|s: int| #[trigger] r.refused(s) is Some ==> request_resources_ok(r.refused(s)->0)
}

pub(crate) open spec fn request_resources_ok<N: Device>(leases: Leases<N>) -> bool {
    leases.inv() && leases.fully_recorded()
}

/// Exact native opcode and SGE metadata in the request's checked ledger.
pub(crate) open spec fn ledger_has<N: Device>(
    leases: Leases<N>, id: u64, op: Option<safeverbs_spec::nic::Opcode>,
    sges: Seq<safeverbs_spec::nic::Sge>,
) -> bool {
    exists|j: int| 0 <= j < leases.wqes().len() && {
        let entry = #[trigger] leases.wqes()[j];
        &&& entry.wr_id == id
        &&& entry.opcode == op
        &&& entry.sges.map_values(|s: Sge| sge_model(s)) == sges
    }
}

/// The native record at this index is an explicit witness for its metadata.
proof fn lemma_ledger_entry<N: Device>(leases: Leases<N>, j: int)
    requires 0 <= j < leases.wqes().len(),
    ensures ledger_has(leases, leases.wqes()[j].wr_id, leases.wqes()[j].opcode,
        leases.wqes()[j].sges.map_values(|s: Sge| sge_model(s))),
{
    assert(leases.wqes()[j] == leases.wqes()[j]);
}

/// A WQE is retained by its actual live request interval, never by a refused
/// result whose historical WQE number happens to match.
pub(crate) open spec fn ring_backed<N: Device>(
    r: Ring<Leases<N>>, w: int, id: u64,
    op: Option<safeverbs_spec::nic::Opcode>, sges: Seq<safeverbs_spec::nic::Sge>,
) -> bool {
    exists|s: int| #[trigger] r.dma(s, w) is Some
        && ledger_has(r.dma(s, w)->0, id, op, sges)
}

proof fn lemma_backed_kept<N: Device>(
    pre: Ring<Leases<N>>, post: Ring<Leases<N>>, w: int, id: u64,
    op: Option<safeverbs_spec::nic::Opcode>, sges: Seq<safeverbs_spec::nic::Sge>,
)
    requires post.keeps_dma(&pre), post.spec_wqe_retired() <= w < post.spec_closed_end(),
        ring_backed(pre, w, id, op, sges),
    ensures ring_backed(post, w, id, op, sges),
{
    let s = choose|s: int| pre.dma(s, w) is Some
        && ledger_has(pre.dma(s, w)->0, id, op, sges);
    assert(post.dma(s, w) == pre.dma(s, w));
}

/// The operation-specific local role, including receive destinations.
pub(crate) open spec fn local_role(op: Option<safeverbs_spec::nic::Opcode>) -> safeverbs_spec::abs::Dir {
    match op {
        Some(op) => if op.writes_local() { safeverbs_spec::abs::Dir::In } else { safeverbs_spec::abs::Dir::Out },
        None => safeverbs_spec::abs::Dir::In,
    }
}

/// One exact native SGE is retained by a logical input and a physical guard.
/// This does not invent allocation authority or arbitrary-byte type validity.
pub(crate) open spec fn retained_sge<N: Device>(
    leases: Leases<N>, dir: safeverbs_spec::abs::Dir, sge: safeverbs_spec::nic::Sge,
) -> bool {
    exists|l: int, g: int| 0 <= l < leases.loans().len() && 0 <= g < leases.view().len() && {
        let loan = #[trigger] leases.loans()[l];
        &&& sge_model(loan.part.spec_sge()) == sge
        &&& loan.access == Some(dir)
        &&& loan.part.inv()
        &&& (#[trigger] leases.view()[g]).same_backing(loan.part.resource())
        &&& loan.part.byte_domain().subset_of(leases.view()[g].byte_domain())
        &&& dir is In ==> loan.part.spec_writable()
    }
}

/// Byte containment uses the exact native address and byte length.
pub(crate) open spec fn retained_byte<N: Device>(
    leases: Leases<N>, dir: safeverbs_spec::abs::Dir, a: int,
) -> bool {
    exists|sge: safeverbs_spec::nic::Sge| #[trigger] retained_sge(leases, dir, sge)
        && sge.span().contains(a)
}

/// The byte lies in the linear permission of a keeper retained by this request.
pub(crate) open spec fn byte_backed<N: Device>(leases: Leases<N>, a: int) -> bool {
    exists|g: int| 0 <= g < leases.view().len()
        && (#[trigger] leases.view()[g]).byte_domain().contains(a)
}

proof fn lemma_retained_permission<N: Device>(leases: Leases<N>, dir: safeverbs_spec::abs::Dir, a: int)
    requires retained_byte(leases, dir, a),
    ensures byte_backed(leases, a),
{
    let sge = choose|sge: safeverbs_spec::nic::Sge| retained_sge(leases, dir, sge)
        && sge.span().contains(a);
    let (l, g) = choose|l: int, g: int| 0 <= l < leases.loans().len() && 0 <= g < leases.view().len() && {
        let loan = #[trigger] leases.loans()[l];
        &&& sge_model(loan.part.spec_sge()) == sge
        &&& loan.access == Some(dir)
        &&& loan.part.inv()
        &&& (#[trigger] leases.view()[g]).same_backing(loan.part.resource())
        &&& loan.part.byte_domain().subset_of(leases.view()[g].byte_domain())
        &&& dir is In ==> loan.part.spec_writable()
    };
    leases.loans()[l].part.lemma_byte_backing();
    assert(leases.loans()[l].part.byte_domain().contains(a));
    assert(leases.view()[g].byte_domain().contains(a));
}

proof fn lemma_covered_guard<N: Device>(
    leases: Leases<N>, dir: safeverbs_spec::abs::Dir,
    sges: Seq<safeverbs_spec::nic::Sge>, a: int,
)
    requires covers(sges, a), forall|i: int| 0 <= i < sges.len()
        ==> retained_sge(leases, dir, #[trigger] sges[i]),
    ensures retained_byte(leases, dir, a), byte_backed(leases, a),
{
    let i = choose|i: int| 0 <= i < sges.len() && sges[i].span().contains(a);
    assert(retained_sge(leases, dir, sges[i]));
    lemma_retained_permission(leases, dir, a);
}

/// Every SGE of an exact ledger entry has its operation's source/destination
/// keeper, including logical shared inputs deduplicated by the native collector.
pub(crate) proof fn lemma_ledger_guards<N: Device>(
    leases: Leases<N>, id: u64, op: Option<safeverbs_spec::nic::Opcode>,
    sges: Seq<safeverbs_spec::nic::Sge>,
)
    requires leases.inv(), ledger_has(leases, id, op, sges),
    ensures forall|i: int| 0 <= i < sges.len()
        ==> retained_sge(leases, local_role(op), #[trigger] sges[i]),
{
    leases.lemma_wqes();
    leases.lemma_loans();
    leases.lemma_byte_backing();
    let j = choose|j: int| 0 <= j < leases.wqes().len() && {
        let entry = #[trigger] leases.wqes()[j];
        &&& entry.wr_id == id && entry.opcode == op
        &&& entry.sges.map_values(|s: Sge| sge_model(s)) == sges
    };
    let entry = leases.wqes()[j];
    assert(entry.matches(leases.loans()));
    assert forall|i: int| 0 <= i < sges.len()
        implies retained_sge(leases, local_role(op), #[trigger] sges[i]) by {
        let l = entry.end() - 1 - i;
        let loan = leases.loans()[l];
        assert(loan.part.spec_sge() == entry.sges[i]);
        assert(sge_model(loan.part.spec_sge()) == sges[i]);
        let g = choose|g: int| 0 <= g < leases.view().len()
            && leases.view()[g].same_backing(loan.part.resource())
            && loan.part.byte_domain().subset_of(leases.view()[g].byte_domain());
        assert(leases.view()[g].same_backing(loan.part.resource()));
    }
}

/// Resolve a real WQE to its live request sequence and exact retained guards.
/// Refused results cannot be selected, even if they have equal WQE metadata.
pub(crate) proof fn lemma_ring_guards<N: Device>(
    r: Ring<Leases<N>>, w: int, id: u64,
    op: Option<safeverbs_spec::nic::Opcode>, sges: Seq<safeverbs_spec::nic::Sge>,
) -> (s: int)
    requires r.inv(), ring_resources_ok(r), ring_backed(r, w, id, op, sges),
    ensures r.dma(s, w) is Some, request_resources_ok(r.dma(s, w)->0),
        r.refused(s) is None,
        (r.req(s) is Some && r.req(s)->0.phase is InFlight
            && r.req(s)->0.leases == r.dma(s, w)->0)
            || (r.staged(s) is Some && r.staged(s)->0.leases == r.dma(s, w)->0),
        forall|i: int| 0 <= i < sges.len()
            ==> retained_sge(r.dma(s, w)->0, local_role(op), #[trigger] sges[i]),
{
    let s = choose|s: int| r.dma(s, w) is Some && ledger_has(r.dma(s, w)->0, id, op, sges);
    r.lemma_dma(s, w);
    lemma_ledger_guards(r.dma(s, w)->0, id, op, sges);
    s
}

/// A resource at a sequence keeps its identity across the three ring views.
pub(crate) open spec fn resource_at<N: Device>(r: Ring<Leases<N>>, s: int, l: Leases<N>) -> bool {
    ||| r.req(s) is Some && r.req(s)->0.leases == l
    ||| r.staged(s) is Some && r.staged(s)->0.leases == l
    ||| r.refused(s) == Some(l)
}

proof fn lemma_resources_kept<N: Device>(pre: Ring<Leases<N>>, post: Ring<Leases<N>>)
    requires ring_resources_ok(pre),
        forall|s: int| #[trigger] post.req(s) is Some ==> resource_at(pre, s, post.req(s)->0.leases),
        forall|s: int| #[trigger] post.staged(s) is Some ==> resource_at(pre, s, post.staged(s)->0.leases),
        forall|s: int| #[trigger] post.refused(s) is Some ==> resource_at(pre, s, post.refused(s)->0),
    ensures ring_resources_ok(post),
{
    assert forall|s: int| #[trigger] post.req(s) is Some implies request_resources_ok(post.req(s)->0.leases) by {}
    assert forall|s: int| #[trigger] post.staged(s) is Some implies request_resources_ok(post.staged(s)->0.leases) by {}
    assert forall|s: int| #[trigger] post.refused(s) is Some implies request_resources_ok(post.refused(s)->0) by {}
}

proof fn lemma_resources_added<N: Device>(pre: Ring<Leases<N>>, post: Ring<Leases<N>>, seq: int, leases: Leases<N>)
    requires ring_resources_ok(pre), request_resources_ok(leases),
        forall|s: int| #[trigger] post.req(s) is Some ==> resource_at(pre, s, post.req(s)->0.leases),
        forall|s: int| #[trigger] post.staged(s) is Some
            ==> (s == seq && post.staged(s)->0.leases == leases)
                || resource_at(pre, s, post.staged(s)->0.leases),
        forall|s: int| #[trigger] post.refused(s) is Some ==> resource_at(pre, s, post.refused(s)->0),
    ensures ring_resources_ok(post),
{
    assert forall|s: int| #[trigger] post.req(s) is Some implies request_resources_ok(post.req(s)->0.leases) by {}
    assert forall|s: int| #[trigger] post.staged(s) is Some implies request_resources_ok(post.staged(s)->0.leases) by {}
    assert forall|s: int| #[trigger] post.refused(s) is Some implies request_resources_ok(post.refused(s)->0) by {}
}

/// Completion changes outcomes and ownership, while surviving requests retain
/// the exact resource value that was staged and committed.
proof fn lemma_resources_completed<N: Device>(pre: Ring<Leases<N>>, post: Ring<Leases<N>>, wqe: u64, wc: Wc)
    requires pre.inv(), ring_resources_ok(pre),
        pre.spec_retired() <= post.spec_retired() <= pre.spec_committed(),
        forall|s: int| #[trigger] post.req(s) == if pre.spec_retired() <= s < post.spec_retired() {
            pre.retired_view(s, pre.retire_outcome(s, wqe, wc))
        } else { pre.req(s) },
        forall|s: int| #[trigger] post.staged(s) == pre.staged(s),
        forall|s: int| #[trigger] post.refused(s) == pre.refused(s),
    ensures ring_resources_ok(post),
{
    assert forall|s: int| #[trigger] post.req(s) is Some
        implies resource_at(pre, s, post.req(s)->0.leases) by {
        if pre.spec_retired() <= s < post.spec_retired() {
            pre.lemma_retired_view(s, pre.retire_outcome(s, wqe, wc));
        }
    }
    lemma_resources_kept(pre, post);
}

/// The native cached first error interpreted over exactly the retired prefix
/// of this request's own M interval. A proper prefix does not complete it.
spec fn prefix_outcome(
    range: WqeRange, cut: nat, retired: IMap<WqeRef, safeverbs_spec::types::Outcome>,
    seen: Option<safeverbs_spec::types::Outcome>,
) -> bool {
    &&& range.wf() && range.first <= cut <= range.end
    &&& forall|s: nat| range.first <= s < cut ==> retired.contains_key(#[trigger] range.at(s))
    &&& match seen {
        None => forall|s: nat| range.first <= s < cut
            ==> (#[trigger] retired[range.at(s)]).status is Success,
        Some(o) => o.status.is_err() && exists|s: nat| range.first <= s < cut
            && #[trigger] range.first_error(retired, s) && retired[range.at(s)] == o,
    }
}

proof fn lemma_prefix_same(
    range: WqeRange, cut: nat, before: IMap<WqeRef, safeverbs_spec::types::Outcome>,
    after: IMap<WqeRef, safeverbs_spec::types::Outcome>, seen: Option<safeverbs_spec::types::Outcome>,
)
    requires prefix_outcome(range, cut, before, seen),
        forall|s: nat| range.first <= s < cut ==> after.contains_key(#[trigger] range.at(s))
            && after[range.at(s)] == before[range.at(s)],
    ensures prefix_outcome(range, cut, after, seen),
{
    match seen {
        Some(o) => {
            let s = choose|s: nat| range.first <= s < cut && range.first_error(before, s)
                && before[range.at(s)] == o;
            assert(range.first_error(after, s));
        },
        None => {},
    }
}

/// Additional successes leave the prior first error, if any, unchanged.
proof fn lemma_prefix_successes(
    range: WqeRange, cut: nat, to: nat, retired: IMap<WqeRef, safeverbs_spec::types::Outcome>,
    seen: Option<safeverbs_spec::types::Outcome>,
)
    requires prefix_outcome(range, cut, retired, seen), cut <= to <= range.end,
        forall|s: nat| cut <= s < to ==> retired.contains_key(#[trigger] range.at(s))
            && retired[range.at(s)].status is Success,
    ensures prefix_outcome(range, to, retired, seen),
{
    match seen {
        Some(o) => {
            let s = choose|s: nat| range.first <= s < cut && range.first_error(retired, s)
                && retired[range.at(s)] == o;
            assert(range.first <= s < to);
        },
        None => {},
    }
}

/// Once an error was observed, later errors or successes cannot replace it.
proof fn lemma_prefix_error_kept(
    range: WqeRange, cut: nat, to: nat, retired: IMap<WqeRef, safeverbs_spec::types::Outcome>,
    error: safeverbs_spec::types::Outcome,
)
    requires prefix_outcome(range, cut, retired, Some(error)), cut <= to <= range.end,
        forall|s: nat| cut <= s < to ==> retired.contains_key(#[trigger] range.at(s)),
    ensures prefix_outcome(range, to, retired, Some(error)),
{
    let s = choose|s: nat| range.first <= s < cut && range.first_error(retired, s)
        && retired[range.at(s)] == error;
    assert(range.first <= s < to);
}

/// The CQE at the end of a newly retired prefix supplies its first failure.
proof fn lemma_prefix_first_error(
    range: WqeRange, cut: nat, at: nat, retired: IMap<WqeRef, safeverbs_spec::types::Outcome>,
    error: safeverbs_spec::types::Outcome,
)
    requires prefix_outcome(range, cut, retired, None), cut <= at < range.end,
        forall|s: nat| cut <= s < at ==> retired.contains_key(#[trigger] range.at(s))
            && retired[range.at(s)].status is Success,
        retired.contains_key(range.at(at)), retired[range.at(at)] == error, error.status.is_err(),
    ensures prefix_outcome(range, at + 1, retired, Some(error)),
{
    assert(range.first_error(retired, at));
}

/// The completed native cache agrees with M's existing interval aggregate.
proof fn lemma_prefix_aggregate(
    range: WqeRange, retired: IMap<WqeRef, safeverbs_spec::types::Outcome>,
    seen: Option<safeverbs_spec::types::Outcome>,
)
    requires prefix_outcome(range, range.end, retired, seen),
    ensures range.all_retired(retired),
        seen is Some ==> range.aggregate(retired) == seen->0,
        seen is None ==> range.aggregate(retired) == retired[range.last()]
            && range.aggregate(retired).status is Success,
{
    match seen {
        Some(o) => {
            let s = choose|s: nat| range.first <= s < range.end && range.first_error(retired, s)
                && retired[range.at(s)] == o;
            lemma_aggregate_first_error(range, retired, s);
        },
        None => { lemma_aggregate_success(range, retired); },
    }
}

/// A derived execution-history invariant, not an additional assumption of M.
/// M's existing execution transitions choose only operation-valid outcomes.
spec fn queued_outcomes_ok(m: Nic::State, q: QpId) -> bool {
    !m.qps.contains_key(q) || {
        let qp = m.qps[q];
        &&& forall|k: int| 0 <= k < qp.sq.len() && (#[trigger] qp.sq[k]).outcome is Some
            ==> qp.sq[k].wr.opcode.allows_outcome(total_len(qp.sq[k].wr.sges), qp.sq[k].outcome->0)
        &&& forall|k: int| 0 <= k < qp.rq.len() && (#[trigger] qp.rq[k]).outcome is Some
            ==> qp.rq[k].wr.allows_outcome(qp.attr.transport, qp.rq[k].outcome->0)
    }
}

/// Every actual M step preserves validity of already executed local metadata.
proof fn lemma_queued_outcomes_step(m: Nic::State, n: Nic::State, step: Nic::Step, q: QpId)
    requires nic_inv(m), nic_step(m, n, step), queued_outcomes_ok(m, q),
    ensures queued_outcomes_ok(n, q),
{
    // State-machine weak transitions expose their updates after their existing
    // CQ-capacity safety assertions have been discharged, as in M's own proofs.
    match step {
        Nic::Step::execute_send(p, _) => safeverbs_spec::nic_proofs::lemma_room_for_send_cqe(m, p),
        Nic::Step::execute_recv(p, _) => safeverbs_spec::nic_proofs::lemma_room_for_recv_cqe(m, p),
        _ => {},
    }

    if n.qps.contains_key(q) {
        let b = n.qps[q];
        assert forall|k: int| 0 <= k < b.sq.len() && (#[trigger] b.sq[k]).outcome is Some
            implies b.sq[k].wr.opcode.allows_outcome(total_len(b.sq[k].wr.sges), b.sq[k].outcome->0) by {
            match step {
                Nic::Step::create_qp(p, _) => {},
                Nic::Step::execute_send(p, o) => {
                    if p == q && k == m.qps[q].sq_done {
                        assert(b.sq[k].outcome == Some(o));
                    } else { assert(b.sq[k] == m.qps[q].sq[k]); }
                },
                Nic::Step::poll_cq(c) => {
                    let e = m.cqs[c].entries[0];
                    if e.qp == q && e.queue is Send {
                        let old_k = k + e.seq + 1 - m.qps[q].sq_head;
                        assert(b.sq[k] == m.qps[q].sq[old_k]);
                    } else { assert(b.sq[k] == m.qps[q].sq[k]); }
                },
                Nic::Step::post_send(p, _) => {
                    if p == q && k >= m.qps[q].sq.len() { assert(b.sq[k].outcome is None); }
                    else { assert(b.sq[k] == m.qps[q].sq[k]); }
                },
                _ => { assert(b.sq[k] == m.qps[q].sq[k]); },
            }
        }
        assert forall|k: int| 0 <= k < b.rq.len() && (#[trigger] b.rq[k]).outcome is Some
            implies b.rq[k].wr.allows_outcome(b.attr.transport, b.rq[k].outcome->0) by {
            match step {
                Nic::Step::create_qp(p, _) => {},
                Nic::Step::execute_recv(p, o) => {
                    if p == q && k == m.qps[q].rq_done {
                        assert(b.rq[k].outcome == Some(o));
                    } else { assert(b.rq[k] == m.qps[q].rq[k]); }
                },
                Nic::Step::poll_cq(c) => {
                    let e = m.cqs[c].entries[0];
                    if e.qp == q && e.queue is Recv {
                        let old_k = k + e.seq + 1 - m.qps[q].rq_head;
                        assert(b.rq[k] == m.qps[q].rq[old_k]);
                    } else { assert(b.rq[k] == m.qps[q].rq[k]); }
                },
                Nic::Step::post_recv(p, _) => {
                    if p == q && k >= m.qps[q].rq.len() { assert(b.rq[k].outcome is None); }
                    else { assert(b.rq[k] == m.qps[q].rq[k]); }
                },
                _ => { assert(b.rq[k] == m.qps[q].rq[k]); },
            }
        }
    }
}

proof fn lemma_queued_outcomes_run(run: Seq<Nic::State>, steps: Seq<Nic::Step>, q: QpId)
    requires run.len() > 0, steps.len() + 1 == run.len(), nic_inv(run[0]), queued_outcomes_ok(run[0], q),
        forall|i: int| 0 <= i < steps.len() ==> nic_step(#[trigger] run[i], run[i + 1], steps[i]),
    ensures nic_inv(run.last()), queued_outcomes_ok(run.last(), q),
    decreases steps.len(),
{
    if steps.len() > 0 {
        let before = run.drop_last();
        let prefix = steps.drop_last();
        assert forall|i: int| 0 <= i < prefix.len()
            implies nic_step(#[trigger] before[i], before[i + 1], prefix[i]) by {}
        lemma_queued_outcomes_run(before, prefix, q);
        lemma_queued_outcomes_step(before.last(), run.last(), steps.last(), q);
        lemma_nic_step_is_next(before.last(), run.last(), steps.last());
        lemma_step_keeps_invariant(before.last(), run.last());
    }
}

proof fn lemma_queued_outcomes_rely(sh: Share, pins: ISet<Key>, m: Nic::State, n: Nic::State, q: QpId)
    requires nic_inv(m), queued_outcomes_ok(m, q), rely_reaches(sh, pins, m, n),
    ensures queued_outcomes_ok(n, q),
{
    let (run, steps) = choose|run: Seq<Nic::State>, steps: Seq<Nic::Step>|
        rely_run(sh, pins, run, steps) && run[0] == m && run.last() == n;
    lemma_queued_outcomes_run(run, steps, q);
}

/// What the worker knows of the QP's state holds in M.
pub open spec fn phase_ok(phase: Phase, state: QpState) -> bool {
    match phase {
        Phase::Reset => state is Reset,
        Phase::Ready => state is Rts || state is Err,
        Phase::Broken => !(state is Rts),
    }
}

/// The send WRs written since the last doorbell are the ring's WQEs handed
/// out and not committed (but the one held back, if any), each with its
/// number's `wr_id`, each one M accepts.
pub open spec fn writing_ok<L>(
    r: Ring<L>,
    index: u16,
    m: Nic::State,
    attr: QpAttr,
    started: bool,
    written: Seq<SendWr>,
    held: bool,
) -> bool {
    &&& !started ==> written.len() == 0
    &&& written.len() + (if held {
        1int
    } else {
        0int
    }) == r.spec_wqe_posted() - r.spec_wqe_committed()
    &&& forall|j: int|
        0 <= j < written.len() ==> (#[trigger] written[j]).wr_id == spec_wr_id(
            index,
            false,
            (r.spec_wqe_committed() + j) as u64,
        )
    &&& forall|j: int| 0 <= j < written.len() ==> m.send_wr_ok(attr, #[trigger] written[j])
}

/// QP `q` is writing WRs, or has some written.
pub open spec fn writes(t: NicToken, q: QpId) -> bool {
    t.started(q) || t.written(q).len() > 0
}

/// The local byte footprint of each actual M WQE has a concrete keeper.
spec fn queues_backed<N: Device>(e: HwQp<N>, qp: Qp) -> bool {
    &&& forall|k: int| 0 <= k < qp.sq.len() ==> {
        let wr = (#[trigger] qp.sq[k]).wr;
        &&& !wr.inline
        &&& ring_backed(e.send, qp.sq_head + k, wr.wr_id, Some(wr.opcode), wr.sges)
    }
    &&& forall|k: int| 0 <= k < qp.rq.len() ==> {
        let wr = (#[trigger] qp.rq[k]).wr;
        ring_backed(e.recv, qp.rq_head + k, wr.wr_id, None, wr.sges)
    }
}

proof fn lemma_queues_kept<N: Device>(pre: HwQp<N>, post: HwQp<N>, qp: Qp)
    requires queues_backed(pre, qp),
        post.send.keeps_dma(&pre.send), post.recv.keeps_dma(&pre.recv),
        post.send.spec_wqe_retired() <= qp.sq_head,
        qp.sq_head + qp.sq.len() <= post.send.spec_wqe_committed(),
        post.recv.spec_wqe_retired() <= qp.rq_head,
        qp.rq_head + qp.rq.len() <= post.recv.spec_wqe_committed(),
        post.send.inv(), post.recv.inv(),
    ensures queues_backed(post, qp),
{
    post.send.lemma_wqes();
    post.recv.lemma_wqes();
    assert forall|k: int| 0 <= k < qp.sq.len() implies {
        let wr = (#[trigger] qp.sq[k]).wr;
        &&& !wr.inline
        &&& ring_backed(post.send, qp.sq_head + k, wr.wr_id, Some(wr.opcode), wr.sges)
    } by {
        let wr = qp.sq[k].wr;
        lemma_backed_kept(pre.send, post.send, qp.sq_head + k, wr.wr_id, Some(wr.opcode), wr.sges);
    }
    assert forall|k: int| 0 <= k < qp.rq.len() implies {
        let wr = (#[trigger] qp.rq[k]).wr;
        ring_backed(post.recv, qp.rq_head + k, wr.wr_id, None, wr.sges)
    } by {
        let wr = qp.rq[k].wr;
        lemma_backed_kept(pre.recv, post.recv, qp.rq_head + k, wr.wr_id, None, wr.sges);
    }
}

/// Written WQEs belong either to a closed staged request or to the open
/// request's ledger. The final held WQE is backed separately by held_parts.
spec fn written_backed<N: Device>(e: HwQp<N>, w: int, wr: SendWr) -> bool {
    &&& !wr.inline
    &&& (ring_backed(e.send, w, wr.wr_id, Some(wr.opcode), wr.sges)
        || (e.send.spec_is_open()
            && e.send.spec_closed_end() <= w < e.send.spec_wqe_posted()
            && ledger_has(e.open_leases, wr.wr_id, Some(wr.opcode), wr.sges)))
}

/// Extending a ledger preserves the exact metadata of all its prior WQEs.
proof fn lemma_ledger_extended<N: Device>(
    pre: Leases<N>, post: Leases<N>, id: u64,
    op: Option<safeverbs_spec::nic::Opcode>, sges: Seq<safeverbs_spec::nic::Sge>,
)
    requires ledger_has(pre, id, op, sges), pre.wqes().len() <= post.wqes().len(),
        forall|j: int| 0 <= j < pre.wqes().len()
            ==> #[trigger] post.wqes()[j] == pre.wqes()[j],
    ensures ledger_has(post, id, op, sges),
{
    let j = choose|j: int| 0 <= j < pre.wqes().len() && {
        let entry = #[trigger] pre.wqes()[j];
        &&& entry.wr_id == id && entry.opcode == op
        &&& entry.sges.map_values(|s: Sge| sge_model(s)) == sges
    };
    assert(post.wqes()[j] == pre.wqes()[j]);
}

proof fn lemma_written_kept<N: Device>(pre: HwQp<N>, post: HwQp<N>, w: int, wr: SendWr)
    requires written_backed(pre, w, wr), pre.send.inv(), post.send.inv(),
        post.send.keeps_dma(&pre.send), post.send.spec_wqe_retired() <= w,
        post.send.spec_closed_end() == pre.send.spec_closed_end(),
        pre.send.spec_wqe_posted() <= post.send.spec_wqe_posted(),
        pre.send.spec_is_open() ==> post.send.spec_is_open(),
        pre.open_leases.wqes().len() <= post.open_leases.wqes().len(),
        forall|j: int| 0 <= j < pre.open_leases.wqes().len()
            ==> #[trigger] post.open_leases.wqes()[j] == pre.open_leases.wqes()[j],
    ensures written_backed(post, w, wr),
{
    if ring_backed(pre.send, w, wr.wr_id, Some(wr.opcode), wr.sges) {
        let s = choose|s: int| pre.send.dma(s, w) is Some
            && ledger_has(pre.send.dma(s, w)->0, wr.wr_id, Some(wr.opcode), wr.sges);
        pre.send.lemma_dma(s, w);
        lemma_backed_kept(pre.send, post.send, w, wr.wr_id, Some(wr.opcode), wr.sges);
    } else {
        lemma_ledger_extended(pre.open_leases, post.open_leases, wr.wr_id, Some(wr.opcode), wr.sges);
    }
}

impl<N: Device> HwQp<N> {
    /// M's name of the QP.
    pub closed spec fn id(&self) -> QpId {
        self.id@
    }

    /// The QP is coupled to M's state `m` (with share `sh`, protection
    /// domain `pd`, and the WRs `written` since its start) at index `index`.
    pub closed spec fn ok(
        &self,
        index: u16,
        m: Nic::State,
        sh: Share,
        pd: PdId,
        started: bool,
        written: Seq<SendWr>,
    ) -> bool {
        let qp = m.qps[self.id@];
        &&& sh.owns.contains(self.id@)
        &&& m.qps.contains_key(self.id@)
        &&& qp_of(self.hw) == self.id@
        &&& qp.attr == qp_attr(pd, self.qp_type, sh.cq, self.caps)
        &&& 1 <= self.caps.max_send_sge && 1 <= self.caps.max_recv_sge
        &&& self.atomics ==> self.qp_type == QpType::Rc
        &&& sq_linked(self.send, index, qp)
        &&& rq_linked(self.recv, index, qp)
        &&& outcomes::recv_outcomes(self.recv, m, self.id())
        &&& ring_resources_ok(self.send)
        &&& ring_resources_ok(self.recv)
        &&& queued_outcomes_ok(m, self.id@)
        &&& queues_backed(*self, qp)
        &&& forall|j: int| 0 <= j < written.len()
            ==> written_backed(*self, self.send.spec_wqe_committed() + j, #[trigger] written[j])
        &&& phase_ok(self.phase, qp.state)
        &&& self.phase != Phase::Ready ==> qp.sq.len() == 0 && qp.rq.len() == 0
            && self.send.spec_wqe_committed() == self.send.spec_wqe_retired()
            && self.recv.spec_wqe_committed() == self.recv.spec_wqe_retired()
        &&& self.started == started
        &&& writing_ok(self.send, index, m, qp.attr, started, written, self.held is Some)
        &&& self.held is Some <==> self.send.spec_is_open()
        &&& self.held is Some ==> self.send.spec_open_wqes() >= 1
        &&& self.held is None ==> self.held_parts@.len() == 0
        &&& self.held is None ==> self.held_bytes == 0
        &&& self.held matches Some(h) ==> self.held_bytes == sges_len(h.sges@.take(h.n as int))
            && self.held_bytes <= u32::MAX
        &&& self.held matches Some(h) ==> held_ok(
            h,
            self.held_parts@,
            index,
            self.send.spec_wqe_committed() + written.len(),
            qp.attr,
            self.pd_id@,
            m.caps.max_msg,
        )
        &&& self.building@.len() <= MAX_SGES
        &&& self.open_leases.inv()
        &&& self.open_leases.fully_recorded()
    }
}

impl<N: Device> Hw<N> {
    /// The worker's token.
    pub closed spec fn token(&self) -> NicToken {
        self.token@
    }

    /// M's state, as the worker last saw it.
    pub closed spec fn nic(&self) -> Nic::State {
        self.token@.nic()
    }

    /// Entry `i` of the table is coupled to M's state `m`, with token `t`'s
    /// share and writing.
    pub closed spec fn entry_ok(&self, i: int, t: NicToken, m: Nic::State) -> bool {
        match self.qps@[i] {
            Some(e) => e.ok(i as u16, m, t.share(), t.pd(), t.started(e.id()), t.written(e.id()))
                && e.pd_id@ == pd_id(self.pd),
            None => true,
        }
    }

    /// The invariant, with M's state `m` in place of the token's view (and
    /// the token's share, names and writing).
    pub closed spec fn inv_with(&self, t: NicToken, m: Nic::State) -> bool {
        let sh = t.share();
        &&& nic_inv(m)
        &&& share_wf(m, sh)
        &&& cq_of(self.cq) == sh.cq
        &&& m.cqs[sh.cq].cap == self.cq_size
        &&& m.cqs[sh.cq].reserved.len() == self.reserved
        &&& self.reserved <= self.cq_size
        &&& pd_of(self.pd) == t.pd()
        &&& m.pds.contains(t.pd())
        &&& caps_model(m.caps, self.caps)
        &&& self.qps@.len() <= MAX_QPS
        &&& forall|i: int| 0 <= i < self.qps@.len() ==> #[trigger] self.entry_ok(i, t, m)
        // One entry per QP name, and every QP of the share M has is one.
        &&& forall|i: int, j: int|
            #![trigger self.qps@[i], self.qps@[j]]
            0 <= i < self.qps@.len() && 0 <= j < self.qps@.len() && i != j && self.qps@[i] is Some
                && self.qps@[j] is Some ==> self.qps@[i]->0.id() != self.qps@[j]->0.id()
        &&& forall|q: QpId|
            #[trigger] m.qps.contains_key(q) && sh.owns.contains(q) ==> exists|i: int|
                0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id()
                    == q
        // Only the table's QPs write; names from `next_name` on are unused.
        &&& forall|q: QpId|
            #[trigger] writes(t, q) ==> exists|i: int|
                0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id()
                    == q
        &&& forall|a: nat| #[trigger] sh.owns.contains(t.name(a))
        &&& forall|a: nat, b: nat| #[trigger] t.name(a) == #[trigger] t.name(b) ==> a == b
        &&& forall|n: nat| n >= self.next_name@ ==> !(#[trigger] m.qps.contains_key(t.name(n)))
        &&& forall|n: nat, w: WqeRef|
            n >= self.next_name@ && #[trigger] m.retired.contains_key(w) ==> w.qp != #[trigger] t.name(
                n,
            )
    }

    /// `self` has the same QPs as `other` at the same indices.
    pub closed spec fn same_table(&self, other: &Self) -> bool {
        &&& self.caps == other.caps
        &&& self.pd == other.pd
        &&& self.qps@.len() == other.qps@.len()
        &&& forall|i: int|
            0 <= i < self.qps@.len() ==> (#[trigger] self.qps@[i] is Some) == (other.qps@[i] is Some)
        &&& forall|i: int|
            0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some ==> {
                let (a, b) = (self.qps@[i]->0, other.qps@[i]->0);
                &&& a.id@ == b.id@
                &&& a.qp_type == b.qp_type
                &&& a.atomics == b.atomics
                &&& a.caps == b.caps
                &&& a.pd_id == b.pd_id
            }
    }

    /// QP `index` is in the table.
    pub closed spec fn has_qp(&self, index: int) -> bool {
        0 <= index < self.qps@.len() && self.qps@[index] is Some
    }

    /// The invariant: the worker's state is coupled to M (see the module
    /// documentation).
    pub closed spec fn inv(&self) -> bool {
        self.inv_with(self.token@, self.token@.nic())
    }
}

// =============================================================================
// What the others' steps keep

/// A send WR M accepted still is after steps that keep the regions it names.
proof fn lemma_send_wr_framed(sh: Share, pins: ISet<Key>, m: Nic::State, n: Nic::State, attr: QpAttr, wr: SendWr)
    requires
        framed(sh, pins, m, n),
        m.send_wr_ok(attr, wr),
        !wr.inline ==> forall|i: int| 0 <= i < wr.sges.len() ==> pins.contains(#[trigger] wr.sges[i].lkey),
    ensures
        n.send_wr_ok(attr, wr),
{
    if !wr.inline {
        assert forall|i: int| 0 <= i < wr.sges.len() implies n.registered(attr.pd, #[trigger] wr.sges[i], wr.opcode.writes_local()) by {
            assert(m.registered(attr.pd, wr.sges[i], wr.opcode.writes_local()));
            assert(pins.contains(wr.sges[i].lkey));
        }
    }
}

/// The regions a written WR names are pinned.
proof fn lemma_written_pinned(t: NicToken, q: QpId, j: int)
    requires
        0 <= j < t.written(q).len(),
    ensures
        !t.written(q)[j].inline ==> forall|i: int| 0 <= i < t.written(q)[j].sges.len() ==> pinned(t).contains(#[trigger] t.written(q)[j].sges[i].lkey),
{
    let wr = t.written(q)[j];
    if !wr.inline {
        assert forall|i: int| 0 <= i < wr.sges.len() implies pinned(t).contains(#[trigger] wr.sges[i].lkey) by {
            let k = wr.sges[i].lkey;
            assert(names_key(wr.sges, k));
            assert(send_keys(t.written(q)).contains(k));
        }
    }
}

impl<N: Device> HwQp<N> {
    /// Coupling survives steps that do not touch the worker's share.
    proof fn lemma_framed(
        &self,
        index: u16,
        t: NicToken,
        m: Nic::State,
        n: Nic::State,
    )
        requires
            self.ok(index, m, t.share(), t.pd(), t.started(self.id()), t.written(self.id())),
            framed(t.share(), pinned(t), m, n),
            queued_outcomes_ok(n, self.id()),
        ensures
            self.ok(index, n, t.share(), t.pd(), t.started(self.id()), t.written(self.id())),
    {
        let q = self.id@;
        let (a, b) = (m.qps[q], n.qps[q]);
        assert(qp_kept(a, b));
        outcomes::lemma_recv_framed(self.recv, m, n, q);
        assert forall|k: int| 0 <= k < b.sq.len() implies (#[trigger] b.sq[k]).wr.wr_id == spec_wr_id(
            index,
            false,
            (b.sq_head + k) as u64,
        ) by {
            assert(b.sq[k].wr == a.sq[k].wr);
        }
        assert forall|k: int| 0 <= k < b.rq.len() implies (#[trigger] b.rq[k]).wr.wr_id == spec_wr_id(
            index,
            true,
            (b.rq_head + k) as u64,
        ) by {
            assert(b.rq[k].wr == a.rq[k].wr);
        }
        let written = t.written(q);
        assert forall|j: int| 0 <= j < written.len() implies n.send_wr_ok(b.attr, #[trigger] written[j]) by {
            lemma_written_pinned(t, q, j);
            lemma_send_wr_framed(t.share(), pinned(t), m, n, a.attr, written[j]);
        }
    }
}

impl<N: Device> Hw<N> {
    /// The invariant survives steps that do not touch the worker's share
    /// (nor the regions its written WRs name).
    proof fn lemma_rely(&self, t: NicToken, m: Nic::State, n: Nic::State)
        requires
            self.inv_with(t, m),
            rely_reaches(t.share(), pinned(t), m, n),
        ensures
            self.inv_with(t, n),
    {
        lemma_rely_frame(t.share(), pinned(t), m, n);
        let sh = t.share();
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] self.entry_ok(i, t, n) by {
            assert(self.entry_ok(i, t, m));
            if let Some(e) = self.qps@[i] {
                lemma_queued_outcomes_rely(sh, pinned(t), m, n, e.id());
                e.lemma_framed(i as u16, t, m, n);
            }
        }
        assert forall|q: QpId| #[trigger] n.qps.contains_key(q) && sh.owns.contains(q) implies exists|i: int|
            0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == q by {
            assert(m.qps.contains_key(q));
        }
        assert forall|k: nat| k >= self.next_name@ implies !(#[trigger] n.qps.contains_key(t.name(k))) by {
            assert(sh.owns.contains(t.name(k)));
        }
        assert forall|k: nat, w: WqeRef| k >= self.next_name@ && #[trigger] n.retired.contains_key(w)
            implies w.qp != #[trigger] t.name(k) by {
            if w.qp == t.name(k) {
                assert(sh.owns.contains(w.qp));
            }
        }
    }
}

impl<N: Device> Hw<N> {
    /// An actual local WQE in this worker's M state can access byte `a`.
    /// Remote-rkey access belongs to the separate registration authority.
    pub(crate) closed spec fn local_access(&self, index: int, recv: bool, k: int, a: int) -> bool {
        self.has_qp(index) && {
            let e = self.qps@[index]->0;
            let qp = self.nic().qps[e.id()];
            if recv { 0 <= k < qp.rq.len() && covers(qp.rq[k].wr.sges, a) }
            else { 0 <= k < qp.sq.len() && covers(qp.sq[k].wr.sges, a) }
        }
    }

    /// The actual live request `s`, its retained keeper and the operation's
    /// local role back this byte of the actual WQE in M.
    pub(crate) closed spec fn local_guarded(&self, index: int, recv: bool, k: int, a: int, s: int) -> bool {
        let e = self.qps@[index]->0;
        let qp = self.nic().qps[e.id()];
        let r = if recv { e.recv } else { e.send };
        let w = if recv { qp.rq_head + k } else { qp.sq_head + k };
        let dir = if recv { local_role(None) } else { local_role(Some(qp.sq[k].wr.opcode)) };
        &&& r.dma(s, w) is Some
        &&& r.req(s) is Some && r.req(s)->0.phase is InFlight
        &&& r.req(s)->0.leases == r.dma(s, w)->0
        &&& r.refused(s) is None
        &&& retained_byte(r.dma(s, w)->0, dir, a)
        &&& byte_backed(r.dma(s, w)->0, a)
    }

    /// Every NIC access through this worker's actual SQ/RQ WQEs has a
    /// retained source/destination guard in its unique live request interval.
    pub(crate) proof fn lemma_local_guard(&self, index: int, recv: bool, k: int, a: int) -> (s: int)
        requires self.inv(), self.local_access(index, recv, k, a),
        ensures self.local_guarded(index, recv, k, a, s),
    {
        assert(self.entry_ok(index, self.token@, self.nic()));
        let e = self.qps@[index]->0;
        let qp = self.nic().qps[e.id()];
        if recv {
            let wr = qp.rq[k].wr;
            let w = qp.rq_head + k;
            let s = lemma_ring_guards(e.recv, w, wr.wr_id, None, wr.sges);
            e.recv.lemma_dma(s, w);
            lemma_covered_guard(e.recv.dma(s, w)->0, local_role(None), wr.sges, a);
            s
        } else {
            let wr = qp.sq[k].wr;
            let w = qp.sq_head + k;
            let s = lemma_ring_guards(e.send, w, wr.wr_id, Some(wr.opcode), wr.sges);
            e.send.lemma_dma(s, w);
            lemma_covered_guard(e.send.dma(s, w)->0, local_role(Some(wr.opcode)), wr.sges, a);
            s
        }
    }

    /// A WR already written to the provider, with local bytes in its SGEs.
    pub(crate) closed spec fn written_access(&self, index: int, j: int, a: int) -> bool {
        self.has_qp(index) && {
            let e = self.qps@[index]->0;
            let written = self.token@.written(e.id());
            0 <= j < written.len() && covers(written[j].sges, a)
        }
    }

    /// A written WR retains its byte either in a staged native request or in
    /// the open request at the ring's actual next request sequence number.
    pub(crate) closed spec fn written_guarded(&self, index: int, j: int, a: int, s: int) -> bool {
        let e = self.qps@[index]->0;
        let wr = self.token@.written(e.id())[j];
        let w = e.send.spec_wqe_committed() + j;
        let dir = local_role(Some(wr.opcode));
        ||| e.send.dma(s, w) is Some && e.send.staged(s) is Some
            && e.send.staged(s)->0.leases == e.send.dma(s, w)->0
            && retained_byte(e.send.dma(s, w)->0, dir, a)
            && byte_backed(e.send.dma(s, w)->0, a)
        ||| s == e.send.spec_posted() && e.send.spec_is_open()
            && e.send.spec_closed_end() <= w < e.send.spec_wqe_posted()
            && retained_byte(e.open_leases, dir, a)
            && byte_backed(e.open_leases, a)
    }

    /// Staging before a doorbell retains exactly the same native-byte guards
    /// as posting. No request or byte authority is synthesized here.
    pub(crate) proof fn lemma_written_guard(&self, index: int, j: int, a: int) -> (s: int)
        requires self.inv(), self.written_access(index, j, a),
        ensures self.written_guarded(index, j, a, s),
    {
        assert(self.entry_ok(index, self.token@, self.nic()));
        let e = self.qps@[index]->0;
        let wr = self.token@.written(e.id())[j];
        let w = e.send.spec_wqe_committed() + j;
        if ring_backed(e.send, w, wr.wr_id, Some(wr.opcode), wr.sges) {
            let s = lemma_ring_guards(e.send, w, wr.wr_id, Some(wr.opcode), wr.sges);
            e.send.lemma_dma(s, w);
            lemma_covered_guard(e.send.dma(s, w)->0, local_role(Some(wr.opcode)), wr.sges, a);
            s
        } else {
            lemma_ledger_guards(e.open_leases, wr.wr_id, Some(wr.opcode), wr.sges);
            lemma_covered_guard(e.open_leases, local_role(Some(wr.opcode)), wr.sges, a);
            e.send.spec_posted() as int
        }
    }

    /// An SGE of the final WQE still held back from the provider.
    pub(crate) closed spec fn held_index(&self, index: int, i: int) -> bool {
        self.has_qp(index) && self.qps@[index]->0.held is Some
            && 0 <= i < self.qps@[index]->0.held->0.n
    }

    /// The held SGE's native metadata and required destination role agree
    /// with the part retained directly by the worker.
    pub(crate) closed spec fn held_guarded(&self, index: int, i: int) -> bool {
        let e = self.qps@[index]->0;
        &&& 0 <= i < e.held_parts@.len()
        &&& e.held_parts@[i].spec_sge() == e.held->0.sges@[i]
        &&& local_role(Some(opcode_model(e.held->0.op))) is In
            ==> e.held_parts@[i].spec_writable()
    }

    /// The final unsubmitted WQE retains its parts directly, in SGE order.
    pub(crate) proof fn lemma_held_guard(&self, index: int, i: int)
        requires self.inv(), self.held_index(index, i),
        ensures self.held_guarded(index, i),
    {
        assert(self.entry_ok(index, self.token@, self.nic()));
    }

}

// =============================================================================
// Steps of the worker's own

/// Coupling depends only on the QP's state in M, M's limits and its
/// regions.
proof fn lemma_ok_same<N: Device>(
    e: HwQp<N>,
    index: u16,
    m1: Nic::State,
    m2: Nic::State,
    sh: Share,
    pd: PdId,
    started: bool,
    written: Seq<SendWr>,
)
    requires
        e.ok(index, m1, sh, pd, started, written),
        m2.qps.contains_key(e.id()),
        m2.qps[e.id()] == m1.qps[e.id()],
        m2.caps == m1.caps,
        m2.mrs == m1.mrs,
        forall|w: WqeRef| w.qp == e.id() && w.queue is Recv && w.seq < m1.qps[e.id()].rq_head
            && #[trigger] m1.retired.contains_key(w)
            ==> m2.retired.contains_key(w) && m2.retired[w] == m1.retired[w],
    ensures
        e.ok(index, m2, sh, pd, started, written),
{
    let qp = m1.qps[e.id()];
    outcomes::lemma_recv_framed(e.recv, m1, m2, e.id());
    assert forall|j: int| 0 <= j < written.len() implies m2.send_wr_ok(qp.attr, #[trigger] written[j]) by {
        assert(m1.send_wr_ok(qp.attr, written[j]));
    }
}

impl<N: Device> Hw<N> {
    /// Two tokens of the same worker with the same writing give the same
    /// invariant.
    proof fn lemma_same_token(&self, t1: NicToken, t2: NicToken, m: Nic::State)
        requires
            t2.same_worker(&t1),
            t2.same_writings(&t1),
            self.inv_with(t1, m),
        ensures
            self.inv_with(t2, m),
    {
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] self.entry_ok(i, t2, m) by {
            assert(self.entry_ok(i, t1, m));
            if let Some(e) = self.qps@[i] {
                assert(t2.started(e.id()) == t1.started(e.id()));
                assert(t2.written(e.id()) == t1.written(e.id()));
            }
        }
        assert forall|q: QpId| #[trigger] writes(t2, q) implies exists|i: int|
            0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == q by {
            assert(t2.started(q) == t1.started(q) && t2.written(q) == t1.written(q));
            assert(writes(t1, q));
        }
        assert forall|a: nat| #[trigger] t2.share().owns.contains(t2.name(a)) by {
            assert(t2.name(a) == t1.name(a));
        }
        assert forall|a: nat, b: nat| #[trigger] t2.name(a) == #[trigger] t2.name(b) implies a == b by {
            assert(t2.name(a) == t1.name(a) && t2.name(b) == t1.name(b));
        }
        assert forall|k: nat| k >= self.next_name@ implies !(#[trigger] m.qps.contains_key(t2.name(k))) by {
            assert(t2.name(k) == t1.name(k));
        }
        assert forall|k: nat, w: WqeRef| k >= self.next_name@ && #[trigger] m.retired.contains_key(w)
            implies w.qp != #[trigger] t2.name(k) by {
            assert(t2.name(k) == t1.name(k));
        }
    }

    /// The invariant depends on the token passed, not the one held.
    proof fn lemma_inv_fields(&self, other: &Self, t: NicToken, m: Nic::State)
        requires
            other.inv_with(t, m),
            cq_of(self.cq) == cq_of(other.cq),
            self.pd == other.pd,
            self.caps == other.caps,
            self.qps@ == other.qps@,
            self.cq_size == other.cq_size,
            self.reserved == other.reserved,
            self.next_name == other.next_name,
        ensures
            self.inv_with(t, m),
    {
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] self.entry_ok(i, t, m) by {
            assert(other.entry_ok(i, t, m));
        }
    }

    /// A worker of `pd` with a CQ of `cq_size` entries, on a thread domain
    /// of its own where the device has them.
    pub fn new(pd: Pd<N>, cq_size: u32) -> (r: Result<Hw<N>>)
        ensures
            r matches Ok(hw) ==> hw.inv() && hw.spec_reserved() == 0 && hw.spec_cq_size() == cq_size,
    {
        let caps = device_caps(&pd);
        if cq_size == 0 || cq_size > caps.max_cqe {
            return Err(Error::Invalid("a CQ needs 1..=max_cqe entries"));
        }
        let (cq, caps, token) = match create_cq(&pd, cq_size) {
            Ok(created) => created,
            Err(e) => return Err(e),
        };
        let domain = alloc_domain(&pd);
        let hw = Hw {
            cq,
            pd,
            domain,
            caps,
            qps: Vec::new(),
            dirty: Vec::new(),
            cq_size,
            reserved: 0,
            token,
            next_name: Ghost(0),
        };
        proof {
            let t = hw.token@;
            assert forall|q: QpId| #[trigger] writes(t, q) implies exists|i: int|
                0 <= i < hw.qps@.len() && #[trigger] hw.qps@[i] is Some && hw.qps@[i]->0.id() == q by {
                assert(!t.started(q) && t.written(q) == Seq::<SendWr>::empty());
                assert(!writes(t, q));
            }
            assert forall|q: QpId| #[trigger] t.nic().qps.contains_key(q) && t.share().owns.contains(q) implies exists|i: int|
                0 <= i < hw.qps@.len() && #[trigger] hw.qps@[i] is Some && hw.qps@[i]->0.id() == q by {
                // Nothing of the share exists yet.
                assert(!t.nic().qps.contains_key(q));
            }
        }
        Ok(hw)
    }

    /// [`Hw::reserved`], as a specification.
    pub closed spec fn spec_reserved(&self) -> u32 {
        self.reserved
    }

    /// [`Hw::cq_size`], as a specification.
    pub closed spec fn spec_cq_size(&self) -> u32 {
        self.cq_size
    }

    /// The CQ entries its QPs reserve (the sum of their queue depths).
    pub fn reserved(&self) -> (n: u32)
        ensures
            n == self.spec_reserved(),
    {
        self.reserved
    }

    /// The entries of its CQ.
    pub fn cq_size(&self) -> (n: u32)
        ensures
            n == self.spec_cq_size(),
    {
        self.cq_size
    }

    /// The protection domain of its QPs.
    pub fn pd(&self) -> &Pd<N> {
        &self.pd
    }

    /// Whether its QPs are created on a thread domain.
    pub fn is_lock_free(&self) -> bool {
        self.domain.is_some()
    }
}

impl<N: Device> Hw<N> {
    /// Creates a QP of type `qp_type` (in the RESET state) with queues of
    /// `caps` and requests of at most `max_batch` WQEs, attached to the
    /// worker's CQ; returns its index.
    ///
    /// The sizes are checked against what the device supports, and the
    /// QP's depths must fit in what the CQ has not reserved yet: this is
    /// M's reservation, so the CQ cannot overrun.
    #[verifier::spinoff_prover]
    pub fn create_qp(&mut self, qp_type: QpType, caps: QpCaps, max_batch: u32) -> (r: Result<usize>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
    {
        let dev = self.caps;
        if caps.max_send_wr == 0 || caps.max_recv_wr == 0 {
            return Err(Error::Invalid("queues need at least one WQE"));
        }
        if caps.max_send_wr > dev.max_qp_wr || caps.max_recv_wr > dev.max_qp_wr {
            return Err(Error::Invalid("queue deeper than the device allows (max_qp_wr)"));
        }
        let max_sge = if dev.max_sge < MAX_SGES as u32 {
            dev.max_sge
        } else {
            MAX_SGES as u32
        };
        if caps.max_send_sge == 0 || caps.max_recv_sge == 0 {
            return Err(Error::Invalid("WQEs need at least one scatter/gather element"));
        }
        if caps.max_send_sge > max_sge || caps.max_recv_sge > max_sge {
            return Err(
                Error::Invalid(
                    "more scatter/gather elements than the device (max_sge) or safeverbs allows",
                ),
            );
        }
        if max_batch == 0 || max_batch > caps.max_send_wr {
            return Err(Error::Invalid("a batch needs 1..=max_send_wr WQEs"));
        }
        let need = caps.max_send_wr as u64 + caps.max_recv_wr as u64;
        let free = (self.cq_size - self.reserved) as u64;
        if need > free {
            return Err(Error::CqFull { need, free });
        }
        // The first free index, or a new one.
        let mut index = 0;
        while index < self.qps.len()
            invariant
                index <= self.qps@.len(),
                forall|i: int| 0 <= i < index ==> (#[trigger] self.qps@[i]) is Some,
            ensures
                index < self.qps@.len() ==> self.qps@[index as int] is None,
            decreases self.qps@.len() - index,
        {
            if self.qps[index].is_none() {
                break;
            }
            index += 1;
        }
        if index == self.qps.len() && index >= MAX_QPS {
            return Err(Error::Invalid("a worker holds at most 65536 QPs"));
        }
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        let ghost q = t0.name(self.next_name@);
        let ghost attr = qp_attr(t0.pd(), qp_type, sh.cq, caps);
        let ghost start = *self;
        proof {
            // The name is fresh, the depths fit the device and the CQ.
            let cq = m0.cqs[sh.cq];
            let cqs1 = m0.cqs.insert(attr.send_cq, cq.reserve(q, attr.sq_depth));
            let cqs2 = cqs1.insert(attr.recv_cq, cqs1[attr.recv_cq].reserve(q, attr.rq_depth));
            safeverbs_spec::nic_proofs::lemma_copies(q, attr.sq_depth);
            safeverbs_spec::nic_proofs::lemma_copies(q, attr.rq_depth);
            broadcast use vstd::multiset::group_multiset_axioms;
            assert(cqs2[sh.cq].reserved.len() == cq.reserved.len() + attr.sq_depth + attr.rq_depth);
            assert(nic_enabled(m0, Nic::Step::create_qp(q, attr)));
        }
        let hw = match create_qp(
            &self.pd,
            self.domain.as_ref(),
            &self.cq,
            qp_type,
            &caps,
            Ghost(q),
            Tracked(self.token.borrow_mut()),
        ) {
            Ok(hw) => hw,
            Err(e) => {
                proof {
                    start.lemma_rely(t0, m0, self.token@.nic());
                    start.lemma_same_token(t0, self.token@, self.token@.nic());
                    self.lemma_inv_fields(&start, self.token@, self.token@.nic());
                }
                return Err(e);
            },
        };
        let reliable = match qp_type {
            QpType::Rc => true,
            QpType::Uc => false,
        };
        // Twice the depth: requests in flight never take more than the depth
        // (each has a WQE at least); the rest is room for results not taken
        // yet.
        let entry = HwQp {
            hw,
            send: Ring::new(2 * caps.max_send_wr as usize, caps.max_send_wr, max_batch),
            recv: Ring::new(2 * caps.max_recv_wr as usize, caps.max_recv_wr, 1),
            caps,
            qp_type,
            atomics: reliable && dev.atomics,
            phase: Phase::Reset,
            started: false,
            removed: false,
            drain_pending: false,
            refused: false,
            dirty: false,
            id: Ghost(q),
            pd_id: Ghost(pd_id(self.pd)),
            held: None,
            held_parts: Vec::with_capacity(MAX_SGES),
            building: Vec::with_capacity(MAX_SGES),
            open_leases: no_leases(),
            held_bytes: 0,
            send_tails: Ghost(Map::empty()),
        };
        let ghost before = *self;
        if index == self.qps.len() {
            self.qps.push(Some(entry));
        } else {
            self.qps.set(index, Some(entry));
        }
        self.reserved += need as u32;
        self.next_name = Ghost(self.next_name@ + 1);
        proof {
            let t = self.token@;
            let mid = choose|mid: Nic::State| rely_reaches(sh, pinned(t0), m0, mid) && nic_step(mid, t.nic(), Nic::Step::create_qp(q, attr));
            let n = t.nic();
            start.lemma_rely(t0, m0, mid);
            start.lemma_same_token(t0, t, mid);
            before.lemma_inv_fields(&start, t, mid);
            lemma_nic_step_is_next(mid, n, Nic::Step::create_qp(q, attr));
            lemma_step_keeps_invariant(mid, n);
            lemma_mine_keeps_share_wf(sh, pinned(t0), mid, n, Nic::Step::create_qp(q, attr));
            safeverbs_spec::nic_proofs::lemma_copies(q, attr.sq_depth);
            safeverbs_spec::nic_proofs::lemma_copies(q, attr.rq_depth);
            outcomes::lemma_recv_empty(self.qps@[index as int]->0.recv, n, q);
            broadcast use vstd::multiset::group_multiset_axioms;
            // The new QP is not one of the table's: those exist in M already.
            assert forall|i: int| 0 <= i < before.qps@.len() && #[trigger] before.qps@[i] is Some implies before.qps@[i]->0.id() != q by {
                assert(before.entry_ok(i, t, mid));
            }
            assert(!mid.qps.contains_key(q));
            assert(!writes(t, q)) by {
                if writes(t, q) {
                    let i = choose|i: int| 0 <= i < before.qps@.len() && #[trigger] before.qps@[i] is Some && before.qps@[i]->0.id() == q;
                    assert(before.entry_ok(i, t, mid));
                }
            }
            assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] self.entry_ok(i, t, n) by {
                if i == index {
                    let e = self.qps@[i]->0;
                    assert(e.id() == q);
                    assert(n.qps[q].attr == attr);
                } else {
                    assert(self.qps@[i] == before.qps@[i]);
                    assert(before.entry_ok(i, t, mid));
                    if let Some(e) = self.qps@[i] {
                        lemma_ok_same(e, i as u16, mid, n, sh, t.pd(), t.started(e.id()), t.written(e.id()));
                    }
                }
            }
            assert forall|i: int, j: int|
                #![trigger self.qps@[i], self.qps@[j]]
                0 <= i < self.qps@.len() && 0 <= j < self.qps@.len() && i != j && self.qps@[i] is Some
                    && self.qps@[j] is Some implies self.qps@[i]->0.id() != self.qps@[j]->0.id() by {
                if i != index && j != index {
                    assert(self.qps@[i] == before.qps@[i] && self.qps@[j] == before.qps@[j]);
                } else if i == index {
                    assert(self.qps@[j] == before.qps@[j]);
                } else {
                    assert(self.qps@[i] == before.qps@[i]);
                }
            }
            assert forall|p: QpId| #[trigger] n.qps.contains_key(p) && sh.owns.contains(p) implies exists|i: int|
                0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == p by {
                if p == q {
                    assert(self.qps@[index as int] is Some);
                } else {
                    assert(mid.qps.contains_key(p));
                    let i = choose|i: int| 0 <= i < before.qps@.len() && #[trigger] before.qps@[i] is Some && before.qps@[i]->0.id() == p;
                    assert(i != index);
                    assert(self.qps@[i] == before.qps@[i]);
                }
            }
            assert forall|p: QpId| #[trigger] writes(t, p) implies exists|i: int|
                0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == p by {
                let i = choose|i: int| 0 <= i < before.qps@.len() && #[trigger] before.qps@[i] is Some && before.qps@[i]->0.id() == p;
                assert(i != index);
                assert(self.qps@[i] == before.qps@[i]);
            }
            assert forall|k: nat| k >= self.next_name@ implies !(#[trigger] n.qps.contains_key(t.name(k))) by {
                assert(t.name(k) != q);
            }
        }
        Ok(index)
    }
}

// =============================================================================
// Changing one QP

/// QP `b` is QP `a` with maybe another state: the same attributes, the same
/// WQEs, the executed ones with the same outcomes, maybe more executed.
pub open spec fn kept_but_state(a: Qp, b: Qp) -> bool {
    &&& b.attr == a.attr
    &&& b.sq_head == a.sq_head && b.sq.len() == a.sq.len() && a.sq_done <= b.sq_done
    &&& b.rq_head == a.rq_head && b.rq.len() == a.rq.len() && a.rq_done <= b.rq_done
    &&& forall|k: int| 0 <= k < a.sq.len() ==> (#[trigger] b.sq[k]).wr == a.sq[k].wr
    &&& forall|k: int| 0 <= k < a.rq.len() ==> (#[trigger] b.rq[k]).wr == a.rq[k].wr
}

/// `framed`, but QP `q` of the share may have changed state (as `modify_qp`
/// changes it): the same as `framed` for everything else.
pub open spec fn framed_but(sh: Share, pins: ISet<Key>, m: Nic::State, n: Nic::State, q: QpId) -> bool {
    &&& n.caps == m.caps
    &&& m.pds.subset_of(n.pds)
    &&& forall|k: Key| #[trigger] pins.contains(k) && m.mrs.contains_key(k)
        ==> n.mrs.contains_key(k) && n.mrs[k] == m.mrs[k]
    &&& forall|p: QpId| #[trigger] sh.owns.contains(p)
        ==> (n.qps.contains_key(p) <==> m.qps.contains_key(p))
    &&& forall|p: QpId| #[trigger] sh.owns.contains(p) && m.qps.contains_key(p) && p != q
        ==> qp_kept(m.qps[p], n.qps[p])
    &&& m.qps.contains_key(q) ==> kept_but_state(m.qps[q], n.qps[q])
    &&& n.cqs.contains_key(sh.cq)
    &&& n.cqs[sh.cq].cap == m.cqs[sh.cq].cap
    &&& n.cqs[sh.cq].reserved == m.cqs[sh.cq].reserved
    &&& forall|w: WqeRef| #[trigger] sh.owns.contains(w.qp) ==> (n.retired.contains_key(w)
        <==> m.retired.contains_key(w))
    &&& forall|w: WqeRef| #[trigger] sh.owns.contains(w.qp) && m.retired.contains_key(w)
        ==> n.retired[w] == m.retired[w]
}

/// What `modify_qp` steps of the worker's own QP `q` (to states other than
/// `Reset`), with rely in between, keep: the share as `framed_but` says, and
/// `q` in the last state asked for, or `Err`.
proof fn lemma_modified(sh: Share, pins: ISet<Key>, m: Nic::State, n: Nic::State, q: QpId, tos: Seq<QpState>)
    requires
        nic_inv(m),
        share_wf(m, sh),
        sh.owns.contains(q),
        m.qps.contains_key(q),
        modified(sh, pins, m, n, q, tos),
        forall|i: int| 0 <= i < tos.len() ==> !(#[trigger] tos[i] is Reset),
    ensures
        nic_inv(n),
        share_wf(n, sh),
        framed_but(sh, pins, m, n, q),
        tos.len() == 0 ==> state_kept(m.qps[q].state, n.qps[q].state),
        tos.len() > 0 ==> n.qps[q].state == tos.last() || n.qps[q].state is Err,
        forall|p: QpId| #[trigger] queued_outcomes_ok(m, p) ==> queued_outcomes_ok(n, p),
    decreases tos.len(),
{
    if tos.len() == 0 {
        lemma_rely_frame(sh, pins, m, n);
        assert forall|p: QpId| #[trigger] queued_outcomes_ok(m, p) implies queued_outcomes_ok(n, p) by {
            lemma_queued_outcomes_rely(sh, pins, m, n, p);
        }
    } else {
        let to = tos.last();
        let (mid, before) = choose|mid: Nic::State, before: Nic::State|
            modified(sh, pins, m, mid, q, tos.drop_last()) && rely_reaches(sh, pins, mid, before)
                && nic_step(before, n, Nic::Step::modify_qp(q, to));
        assert forall|i: int| 0 <= i < tos.drop_last().len() implies !(#[trigger] tos.drop_last()[i] is Reset) by {
            assert(tos.drop_last()[i] == tos[i]);
        }
        lemma_modified(sh, pins, m, mid, q, tos.drop_last());
        lemma_rely_frame(sh, pins, mid, before);
        assert(tos[tos.len() - 1] == to);
        lemma_nic_step_is_next(before, n, Nic::Step::modify_qp(q, to));
        lemma_step_keeps_invariant(before, n);
        lemma_mine_keeps_share_wf(sh, pins, before, n, Nic::Step::modify_qp(q, to));
        assert forall|p: QpId| #[trigger] queued_outcomes_ok(m, p) implies queued_outcomes_ok(n, p) by {
            lemma_queued_outcomes_rely(sh, pins, mid, before, p);
            lemma_queued_outcomes_step(before, n, Nic::Step::modify_qp(q, to), p);
        }
        // Compose: m to mid (framed but q), mid to before (framed), before to
        // n (only q's state).
        assert forall|p: QpId| #[trigger] sh.owns.contains(p) && m.qps.contains_key(p) && p != q
            implies qp_kept(m.qps[p], n.qps[p]) by {
            let (a, b, c) = (m.qps[p], mid.qps[p], before.qps[p]);
            assert(qp_kept(a, b) && qp_kept(b, c) && n.qps[p] == c);
            assert forall|k: int| 0 <= k < a.sq.len() implies (#[trigger] c.sq[k]).wr == a.sq[k].wr by {
                assert(b.sq[k].wr == a.sq[k].wr);
            }
            assert forall|k: int| 0 <= k < a.rq.len() implies (#[trigger] c.rq[k]).wr == a.rq[k].wr by {
                assert(b.rq[k].wr == a.rq[k].wr);
            }
            assert forall|k: int| 0 <= k < a.sq_done implies (#[trigger] c.sq[k]).outcome == a.sq[k].outcome by {
                assert(b.sq[k].outcome == a.sq[k].outcome);
            }
            assert forall|k: int| 0 <= k < a.rq_done implies (#[trigger] c.rq[k]).outcome == a.rq[k].outcome by {
                assert(b.rq[k].outcome == a.rq[k].outcome);
            }
        }
        let (a, b, c, d) = (m.qps[q], mid.qps[q], before.qps[q], n.qps[q]);
        assert(kept_but_state(a, b) && qp_kept(b, c));
        assert(d == c.with_state(d.state));
        assert forall|k: int| 0 <= k < a.sq.len() implies (#[trigger] d.sq[k]).wr == a.sq[k].wr by {
            assert(b.sq[k].wr == a.sq[k].wr && c.sq[k].wr == b.sq[k].wr);
        }
        assert forall|k: int| 0 <= k < a.rq.len() implies (#[trigger] d.rq[k]).wr == a.rq[k].wr by {
            assert(b.rq[k].wr == a.rq[k].wr && c.rq[k].wr == b.rq[k].wr);
        }
        assert forall|w: WqeRef| #[trigger] sh.owns.contains(w.qp) implies (n.retired.contains_key(w)
            <==> m.retired.contains_key(w)) by {
            assert(mid.retired.contains_key(w) == m.retired.contains_key(w));
            assert(before.retired.contains_key(w) == mid.retired.contains_key(w));
        }
    }
}

impl<N: Device> Hw<N> {
    /// After a step that changed only entry `index`'s QP in M (and the
    /// entry), the invariant holds again.
    #[verifier::spinoff_prover]
    proof fn lemma_touch_one(&self, start: &Self, t: NicToken, mid: Nic::State, n: Nic::State, index: int)
        requires
            start.inv_with(t, mid),
            nic_inv(n),
            share_wf(n, t.share()),
            0 <= index < start.qps@.len(),
            start.qps@[index] is Some,
            self.qps@.len() == start.qps@.len(),
            forall|i: int| 0 <= i < self.qps@.len() && i != index ==> #[trigger] self.qps@[i] == start.qps@[i],
            self.qps@[index] is Some,
            self.qps@[index]->0.id() == start.qps@[index]->0.id(),
            self.entry_ok(index, t, n),
            self.cq == start.cq,
            self.pd == start.pd,
            self.caps == start.caps,
            self.cq_size == start.cq_size,
            self.reserved == start.reserved,
            self.next_name == start.next_name,
            n.qps.dom() =~= mid.qps.dom(),
            forall|p: QpId| #[trigger] mid.qps.contains_key(p) && p != start.qps@[index]->0.id() ==> n.qps[p] == mid.qps[p],
            n.caps == mid.caps,
            n.mrs == mid.mrs,
            n.pds == mid.pds,
            n.retired == mid.retired,
            n.cqs[t.share().cq].cap == mid.cqs[t.share().cq].cap,
            n.cqs[t.share().cq].reserved == mid.cqs[t.share().cq].reserved,
        ensures
            self.inv_with(t, n),
    {
        let q = start.qps@[index]->0.id();
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] self.entry_ok(i, t, n) by {
            if i != index {
                assert(start.entry_ok(i, t, mid));
                if let Some(e) = self.qps@[i] {
                    assert(e.id() != q);
                    lemma_ok_same(e, i as u16, mid, n, t.share(), t.pd(), t.started(e.id()), t.written(e.id()));
                }
            }
        }
        assert forall|i: int, j: int|
            #![trigger self.qps@[i], self.qps@[j]]
            0 <= i < self.qps@.len() && 0 <= j < self.qps@.len() && i != j && self.qps@[i] is Some
                && self.qps@[j] is Some implies self.qps@[i]->0.id() != self.qps@[j]->0.id() by {
            if i != index && j != index {
                assert(self.qps@[i] == start.qps@[i] && self.qps@[j] == start.qps@[j]);
            } else if i == index {
                assert(self.qps@[j] == start.qps@[j]);
            } else {
                assert(self.qps@[i] == start.qps@[i]);
            }
        }
        assert forall|p: QpId| #[trigger] n.qps.contains_key(p) && t.share().owns.contains(p) implies exists|i: int|
            0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == p by {
            assert(mid.qps.contains_key(p));
            let i = choose|i: int| 0 <= i < start.qps@.len() && #[trigger] start.qps@[i] is Some && start.qps@[i]->0.id() == p;
            if i != index {
                assert(self.qps@[i] == start.qps@[i]);
            }
        }
        assert forall|p: QpId| #[trigger] writes(t, p) implies exists|i: int|
            0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == p by {
            let i = choose|i: int| 0 <= i < start.qps@.len() && #[trigger] start.qps@[i] is Some && start.qps@[i]->0.id() == p;
            if i != index {
                assert(self.qps@[i] == start.qps@[i]);
            }
        }
        assert forall|k: nat| k >= self.next_name@ implies !(#[trigger] n.qps.contains_key(t.name(k))) by {
            assert(!mid.qps.contains_key(t.name(k)));
        }
    }

    /// Moves QP `index` to the error state: every request in flight
    /// completes with a flush error. Work posted from then on is flushed
    /// too.
    #[verifier::spinoff_prover]
    pub fn set_error(&mut self, index: usize) -> (r: Result<()>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        if index >= self.qps.len() {
            no_qp(index);
            return Err(Error::Invalid("no such QP"));
        }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        proof {
            assert(start.entry_ok(index as int, t0, m0));
        }
        let r = match &mut self.qps[index] {
            Some(e) => {
                if e.removed {
                    no_qp(index);
                }
                let ghost q = e.id@;
                let r = to_error(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                if r.is_ok() {
                    e.phase = Phase::Ready;
                }
                r
            },
            None => {
                no_qp(index);
                Err(Error::Invalid("no such QP"))
            },
        };
        proof {
            let t = self.token@;
            let n = t.nic();
            let e0 = start.qps@[index as int]->0;
            let q = e0.id();
            if r is Ok {
                let mid = choose|mid: Nic::State| rely_reaches(sh, pinned(t0), m0, mid) && nic_step(mid, n, Nic::Step::modify_qp(q, QpState::Err));
                start.lemma_rely(t0, m0, mid);
                start.lemma_same_token(t0, t, mid);
                lemma_nic_step_is_next(mid, n, Nic::Step::modify_qp(q, QpState::Err));
                lemma_step_keeps_invariant(mid, n);
                lemma_mine_keeps_share_wf(sh, pinned(t0), mid, n, Nic::Step::modify_qp(q, QpState::Err));
                assert(start.entry_ok(index as int, t, mid));
                let e = self.qps@[index as int]->0;
                let written = t.written(q);
                assert forall|j: int| 0 <= j < written.len() implies n.send_wr_ok(n.qps[q].attr, #[trigger] written[j]) by {
                    assert(mid.send_wr_ok(mid.qps[q].attr, written[j]));
                }
                outcomes::lemma_recv_framed(e.recv, mid, n, q);
                assert(self.entry_ok(index as int, t, n));
                self.lemma_touch_one(&start, t, mid, n, index as int);
            } else {
                start.lemma_rely(t0, m0, n);
                start.lemma_same_token(t0, t, n);
                assert(start.entry_ok(index as int, t, n));
                self.lemma_touch_one(&start, t, n, n, index as int);
            }
        }
        r
    }
}

impl<N: Device> Hw<N> {
    /// After steps that frame the share but QP `q` of entry `index` (and a
    /// change of that entry), the invariant holds again.
    #[verifier::spinoff_prover]
    proof fn lemma_but_one(&self, start: &Self, t: NicToken, m: Nic::State, n: Nic::State, index: int)
        requires
            start.inv_with(t, m),
            nic_inv(n),
            share_wf(n, t.share()),
            0 <= index < start.qps@.len(),
            start.qps@[index] is Some,
            framed_but(t.share(), pinned(t), m, n, start.qps@[index]->0.id()),
            self.qps@.len() == start.qps@.len(),
            forall|i: int| 0 <= i < self.qps@.len() && i != index ==> #[trigger] self.qps@[i] == start.qps@[i],
            self.qps@[index] is Some,
            self.qps@[index]->0.id() == start.qps@[index]->0.id(),
            self.entry_ok(index, t, n),
            self.cq == start.cq,
            self.pd == start.pd,
            self.caps == start.caps,
            self.cq_size == start.cq_size,
            self.reserved == start.reserved,
            self.next_name == start.next_name,
            forall|p: QpId| #[trigger] queued_outcomes_ok(m, p) ==> queued_outcomes_ok(n, p),
        ensures
            self.inv_with(t, n),
    {
        let q = start.qps@[index]->0.id();
        let sh = t.share();
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] self.entry_ok(i, t, n) by {
            if i != index {
                assert(start.entry_ok(i, t, m));
                if let Some(e) = self.qps@[i] {
                    let p = e.id();
                    assert(p != q);
                    let (a, b) = (m.qps[p], n.qps[p]);
                    assert(qp_kept(a, b));
                    outcomes::lemma_recv_framed(e.recv, m, n, p);
                    assert forall|k: int| 0 <= k < b.sq.len() implies (#[trigger] b.sq[k]).wr.wr_id == spec_wr_id(
                        i as u16,
                        false,
                        (b.sq_head + k) as u64,
                    ) by {
                        assert(b.sq[k].wr == a.sq[k].wr);
                    }
                    assert forall|k: int| 0 <= k < b.rq.len() implies (#[trigger] b.rq[k]).wr.wr_id == spec_wr_id(
                        i as u16,
                        true,
                        (b.rq_head + k) as u64,
                    ) by {
                        assert(b.rq[k].wr == a.rq[k].wr);
                    }
                    let written = t.written(p);
                    assert forall|j: int| 0 <= j < written.len() implies n.send_wr_ok(b.attr, #[trigger] written[j]) by {
                        lemma_written_pinned(t, p, j);
                        let wr = written[j];
                        assert(m.send_wr_ok(a.attr, wr));
                        if !wr.inline {
                            assert forall|x: int| 0 <= x < wr.sges.len() implies n.registered(a.attr.pd, #[trigger] wr.sges[x], wr.opcode.writes_local()) by {
                                assert(m.registered(a.attr.pd, wr.sges[x], wr.opcode.writes_local()));
                                assert(pinned(t).contains(wr.sges[x].lkey));
                            }
                        }
                    }
                }
            }
        }
        assert forall|i: int, j: int|
            #![trigger self.qps@[i], self.qps@[j]]
            0 <= i < self.qps@.len() && 0 <= j < self.qps@.len() && i != j && self.qps@[i] is Some
                && self.qps@[j] is Some implies self.qps@[i]->0.id() != self.qps@[j]->0.id() by {
            if i != index && j != index {
                assert(self.qps@[i] == start.qps@[i] && self.qps@[j] == start.qps@[j]);
            } else if i == index {
                assert(self.qps@[j] == start.qps@[j]);
            } else {
                assert(self.qps@[i] == start.qps@[i]);
            }
        }
        assert forall|p: QpId| #[trigger] n.qps.contains_key(p) && sh.owns.contains(p) implies exists|i: int|
            0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == p by {
            assert(m.qps.contains_key(p));
            let i = choose|i: int| 0 <= i < start.qps@.len() && #[trigger] start.qps@[i] is Some && start.qps@[i]->0.id() == p;
            if i != index {
                assert(self.qps@[i] == start.qps@[i]);
            }
        }
        assert forall|p: QpId| #[trigger] writes(t, p) implies exists|i: int|
            0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == p by {
            let i = choose|i: int| 0 <= i < start.qps@.len() && #[trigger] start.qps@[i] is Some && start.qps@[i]->0.id() == p;
            if i != index {
                assert(self.qps@[i] == start.qps@[i]);
            }
        }
        assert forall|k: nat| k >= self.next_name@ implies !(#[trigger] n.qps.contains_key(t.name(k))) by {
            assert(sh.owns.contains(t.name(k)));
            assert(!m.qps.contains_key(t.name(k)));
        }
        assert forall|k: nat, w: WqeRef| k >= self.next_name@ && #[trigger] n.retired.contains_key(w)
            implies w.qp != #[trigger] t.name(k) by {
            if w.qp == t.name(k) {
                assert(sh.owns.contains(w.qp));
                assert(m.retired.contains_key(w));
            }
        }
    }

    /// Connects QP `index` to `remote` (RESET to INIT, RTR and RTS). A QP
    /// connects once: from RESET, as created. If connecting fails part way,
    /// the QP takes no work until it is moved to the error state.
    #[verifier::spinoff_prover]
    pub fn connect(&mut self, index: usize, attr: &ConnectAttr, remote: &Endpoint) -> (r: Result<()>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        if index >= self.qps.len() {
            no_qp(index);
            return Err(Error::Invalid("no such QP"));
        }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        proof {
            assert(start.entry_ok(index as int, t0, m0));
        }
        let phase = match &self.qps[index] {
            Some(e) => e.phase,
            None => Phase::Broken,
        };
        if !matches!(phase, Phase::Reset) {
            return Err(Error::Invalid("a QP connects once, from RESET"));
        }
        proof {
            assert(self.qps@[index as int]->0.phase == Phase::Reset);
        }
        let r = match &mut self.qps[index] {
            Some(e) => {
                if e.removed {
                    no_qp(index);
                }
                let ghost q = e.id@;
                proof {
                    assert(e.phase == Phase::Reset);
                    assert(phase_ok(e.phase, m0.qps[q].state));
                }
                let r = connect(&mut e.hw, attr, remote, Ghost(q), Tracked(self.token.borrow_mut()));
                e.phase = if r.is_ok() {
                    Phase::Ready
                } else {
                    Phase::Broken
                };
                r
            },
            None => {
                no_qp(index);
                Err(Error::Invalid("no such QP"))
            },
        };
        proof {
            let t = self.token@;
            let n = t.nic();
            let q = start.qps@[index as int]->0.id();
            let tos = if r is Ok {
                connect_states()
            } else {
                let k = choose|k: int| 0 <= k < 3 && modified(sh, pinned(t0), m0, n, q, connect_states().take(k));
                connect_states().take(k)
            };
            assert forall|i: int| 0 <= i < tos.len() implies !(#[trigger] tos[i] is Reset) by {
                assert(tos[i] == connect_states()[i]);
            }
            lemma_modified(sh, pinned(t0), m0, n, q, tos);
            let written = t0.written(q);
            let (a, b) = (m0.qps[q], n.qps[q]);
            assert forall|j: int| 0 <= j < written.len() implies n.send_wr_ok(b.attr, #[trigger] written[j]) by {
                lemma_written_pinned(t0, q, j);
                let wr = written[j];
                assert(m0.send_wr_ok(a.attr, wr));
                if !wr.inline {
                    assert forall|x: int| 0 <= x < wr.sges.len() implies n.registered(a.attr.pd, #[trigger] wr.sges[x], wr.opcode.writes_local()) by {
                        assert(m0.registered(a.attr.pd, wr.sges[x], wr.opcode.writes_local()));
                        assert(pinned(t0).contains(wr.sges[x].lkey));
                    }
                }
            }
            let cs = connect_states();
            assert(cs[0] == QpState::Init && cs[1] == QpState::Rtr && cs[2] == QpState::Rts);
            if tos.len() > 0 {
                assert(tos.last() == tos[tos.len() - 1]);
                assert(tos[tos.len() - 1] == cs[tos.len() - 1]);
            }
            let e = self.qps@[index as int]->0;
            let e0 = start.qps@[index as int]->0;
            assert(e0.ok(index as u16, m0, sh, t0.pd(), t0.started(q), t0.written(q)));
            assert(phase_ok(e.phase, b.state));
            assert(sq_linked(e.send, index as u16, b));
            assert(rq_linked(e.recv, index as u16, b));
            assert(writing_ok(e.send, index as u16, n, b.attr, t0.started(q), written, e.held is Some));
            outcomes::lemma_recv_framed(e.recv, m0, n, q);
            assert(self.entry_ok(index as int, t0, n));
            self.lemma_but_one(&start, t0, m0, n, index as int);
            self.lemma_same_token(t0, t, n);
        }
        r
    }
}

impl<N: Device> Hw<N> {
    /// Posts a receive into `part`, a unique buffer, on QP `index`;
    /// `detached` if nobody will take it back by its sequence number.
    /// Refused when the QP takes no work (`Error::NotConnected`), the part is
    /// of another protection domain or frozen, or the receive queue is full.
    #[verifier::spinoff_prover]
    pub fn post_recv(&mut self, index: usize, part: Part<N>, detached: bool) -> (r: core::result::Result<RingSeq, BufError<Part<N>>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            r matches Ok(seq) ==> old(self).admission_separated_from(part)
                && final(self).recv_holds_input(index, seq, part),
            r matches Err(error) ==> error.buf == part,
    {
        if index >= self.qps.len() {
            no_qp(index);
            return Err(BufError { error: Error::Invalid("no such QP"), buf: part });
        }
        proof {
            assert(self.entry_ok(index as int, self.token@, self.token@.nic()));
        }
        let (ready, room, clogged) = match &self.qps[index] {
            Some(e) => (matches!(e.phase, Phase::Ready), e.recv.can_post(), e.recv.is_clogged()),
            None => {
                no_qp(index);
                (false, false, false)
            },
        };
        if !ready {
            return Err(BufError { error: Error::NotConnected, buf: part });
        }
        if part_pd_of(&part) != pd_id_of(&self.pd) {
            return Err(BufError { error: Error::PdMismatch, buf: part });
        }
        if !part_writable_of(&part) {
            return Err(BufError { error: Error::Invalid("receive into a frozen buffer"), buf: part });
        }
        if !room {
            let error = if clogged {
                Error::Clogged
            } else {
                Error::QueueFull
            };
            return Err(BufError { error, buf: part });
        }
        let ghost admitted = part;
        let ghost admission = *self;
        let mut part = part;
        self.separate_unique_input(&mut part);
        proof { assert(admission.admission_separated_from(admitted)); }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        // The buffer's region is registered (its lease says so).
        registered(&part, &self.pd, Tracked(self.token.borrow_mut()));
        let ghost t1 = self.token@;
        let ghost m1 = t1.nic();
        proof {
            start.lemma_rely(t0, m0, m1);
            start.lemma_same_token(t0, t1, m1);
            self.lemma_inv_fields(&start, t1, m1);
            assert(self.entry_ok(index as int, t1, m1));
        }
        let ghost mid1 = *self;
        let sge = part_sge_of(&part);
        let r = match &mut self.qps[index] {
            Some(e) => {
                if e.removed {
                    no_qp(index);
                }
                let ghost q = e.id@;
                let ghost e1 = *e;
                proof {
                    e1.recv.lemma_wqes();
                    outcomes::lemma_recv_no_staged(e1.recv);
                }
                let seq = match e.recv.begin() {
                    Some(seq) => seq,
                    None => {
                        proof { assert(false); }
                        0
                    },
                };
                proof {
                    e.recv.lemma_wqes();
                }
                let wqe = match e.recv.push_wqe() {
                    Some(wqe) => wqe,
                    None => {
                        proof { assert(false); }
                        0
                    },
                };
                let id = wr_id(index as u16, true, wqe);
                let sges = [sge];
                proof {
                    let qp = m1.qps[q];
                    e1.recv.lemma_wqes();
                    assert(sges@ =~= seq![sge]);
                    let wr = recv_wr(id, sges@);
                    assert(wr.sges[0] == sge_model(sge));
                    assert(m1.recv_wr_ok(qp.attr, wr));
                    assert(nic_enabled(m1, Nic::Step::post_recv(q, seq![wr])));
                }
                match post_recv(&mut e.hw, id, &sges, Ghost(q), Tracked(self.token.borrow_mut())) {
                    Ok(()) => {
                        let implied = Wc::success(Opcode::RECV, sge.length);
                        let mut leases = no_leases();
                        keep_lease(&mut leases, part, Ghost(safeverbs_spec::abs::Dir::In));
                        leases.record_wqe(Ghost(crate::buf::WqeLoans {
                            wr_id: id, opcode: None, sges: sges@, first: 0,
                        }));
                        let ghost resource = leases;
                        proof {
                            lemma_ledger_entry(leases, 0);
                            assert(resource.loans() == seq![crate::buf::Loan {
                                part: admitted, access: Some(safeverbs_spec::abs::Dir::In),
                            }]);
                        }
                        let ghost before_end = e.recv;
                        proof { lemma_resources_kept(e1.recv, e.recv); }
                        e.recv.end(true, detached, implied, leases);
                        proof { lemma_resources_added(before_end, e.recv, seq as int, resource); }
                        let ghost staged = e.recv;
                        proof {
                            staged.lemma_wqes();
                            assert(before_end.same_dma(&e1.recv));
                            assert(staged.keeps_dma(&e1.recv));
                        }
                        e.recv.commit();
                        proof {
                            lemma_resources_kept(staged, e.recv);
                            e.recv.lemma_wqes();
                            assert(e.recv.keeps_dma(&e1.recv));
                        }
                        proof {
                            let wr = recv_wr(id, sges@);
                            assert(e.recv.dma(seq as int, wqe as int) == Some(resource));
                            assert(ledger_has(resource, id, None, wr.sges));
                            assert(ring_backed(e.recv, wqe as int, id, None, wr.sges));
                            assert(e.recv.req(seq as int) is Some);
                            assert(e.recv.req(seq as int)->0.phase is InFlight);
                            assert(e.recv.req(seq as int)->0.leases == resource);
                            assert(e.recv.meta(seq as int) is Some);
                            assert(e.recv.meta(seq as int)->0.first == wqe);
                            assert(e.recv.meta(seq as int)->0.end as int == wqe as int + 1);
                            assert(e.recv.meta(seq as int)->0.seen is None);
                        }
                        Ok(seq)
                    },
                    Err(error) => {
                        e.recv.abort();
                        proof { lemma_resources_kept(e1.recv, e.recv); }
                        Err(BufError { error, buf: part })
                    },
                }
            },
            None => {
                no_qp(index);
                Err(BufError { error: Error::Invalid("no such QP"), buf: part })
            },
        };
        proof {
            let t = self.token@;
            let n = t.nic();
            let q = mid1.qps@[index as int]->0.id();
            let e1 = mid1.qps@[index as int]->0;
            let e = self.qps@[index as int]->0;
            let wr = recv_wr(spec_wr_id(index as u16, true, e1.recv.spec_wqe_posted()), seq![sge]);
            let pins = pinned(t1).union(recv_keys(seq![wr]));
            if r is Ok {
                let mid = choose|mid: Nic::State| rely_reaches(sh, pins, m1, mid) && nic_step(mid, n, Nic::Step::post_recv(q, seq![wr]));
                lemma_rely_fewer_pins(sh, pins, pinned(t1), m1, mid);
                mid1.lemma_rely(t1, m1, mid);
                lemma_nic_step_is_next(mid, n, Nic::Step::post_recv(q, seq![wr]));
                lemma_step_keeps_invariant(mid, n);
                lemma_mine_keeps_share_wf(sh, pins, mid, n, Nic::Step::post_recv(q, seq![wr]));
                assert(mid1.entry_ok(index as int, t1, mid));
                let (a, b) = (mid.qps[q], n.qps[q]);
                e.recv.lemma_wqes();
                assert forall|k: int| 0 <= k < b.rq.len() implies (#[trigger] b.rq[k]).wr.wr_id == spec_wr_id(
                    index as u16,
                    true,
                    (b.rq_head + k) as u64,
                ) by {
                    if k < a.rq.len() {
                        assert(b.rq[k] == a.rq[k]);
                    }
                }
                assert(rq_linked(e.recv, index as u16, b));
                outcomes::lemma_recv_framed(e1.recv, mid, n, q);
                outcomes::lemma_recv_added(e1.recv, e.recv, n, q, r->Ok_0 as int);
                assert forall|k: int| 0 <= k < b.rq.len() implies
                    ring_backed(e.recv, b.rq_head + k, (#[trigger] b.rq[k]).wr.wr_id,
                        None, b.rq[k].wr.sges) by {
                    if k < a.rq.len() {
                        lemma_backed_kept(e1.recv, e.recv, b.rq_head + k,
                            b.rq[k].wr.wr_id, None, b.rq[k].wr.sges);
                    } else {
                        let s = e1.recv.spec_posted() as int;
                        assert(e.recv.dma(s, b.rq_head + k) is Some);
                        assert(ledger_has(e.recv.dma(s, b.rq_head + k)->0, wr.wr_id, None, wr.sges));
                    }
                }
                let written = t1.written(q);
                assert forall|j: int| 0 <= j < written.len() implies n.send_wr_ok(b.attr, #[trigger] written[j]) by {
                    assert(mid.send_wr_ok(a.attr, written[j]));
                }
                e.send.lemma_wqes();
                e.recv.lemma_wqes();
                assert forall|j: int| 0 <= j < t1.written(q).len() implies
                    written_backed(e, e.send.spec_wqe_committed() + j, #[trigger] t1.written(q)[j]) by {
                    lemma_written_kept(e1, e, e1.send.spec_wqe_committed() + j, t1.written(q)[j]);
                }
                assert(self.entry_ok(index as int, t1, n));
                self.lemma_touch_one(&mid1, t1, mid, n, index as int);
                self.lemma_same_token(t1, t, n);
            } else {
                mid1.lemma_rely(t1, m1, n);
                assert(mid1.entry_ok(index as int, t1, n));
                assert(rq_linked(e.recv, index as u16, n.qps[q]));
                outcomes::lemma_recv_kept(e1.recv, e.recv, n, q);
                lemma_queues_kept(e1, e, n.qps[q]);
                e.send.lemma_wqes();
                e.recv.lemma_wqes();
                assert forall|j: int| 0 <= j < t1.written(q).len() implies
                    written_backed(e, e.send.spec_wqe_committed() + j, #[trigger] t1.written(q)[j]) by {
                    lemma_written_kept(e1, e, e1.send.spec_wqe_committed() + j, t1.written(q)[j]);
                }
                assert(self.entry_ok(index as int, t1, n));
                self.lemma_touch_one(&mid1, t1, n, n, index as int);
                self.lemma_same_token(t1, t, n);
            }
        }
        proof {
            if r is Ok {
                assert(admission.admission_separated_from(admitted));
                self.lemma_recv_holds_input(index, r->Ok_0, admitted);
                assert(self.recv_holds_input(index, r->Ok_0, admitted));
            }
        }
        r
    }
}

// =============================================================================
// Completions

/// Completions taken from the CQ per poll.
pub const POLL_BATCH: usize = 16;

/// A placeholder completion.
pub(crate) const EMPTY_WC: Wc = Wc {
    wr_id: 0,
    status: crate::wc::Status(0),
    opcode: Opcode(0),
    byte_len: 0,
    imm: None,
    vendor_err: 0,
    qp_num: 0,
};

/// The steps of a poll that gave `wcs`, in order: for each completion, the
/// state before its `poll_cq` step and the state after it; steps that do
/// not touch the share come before the first and between the others.
pub open spec fn poll_steps(
    sh: Share,
    pins: ISet<Key>,
    m: Nic::State,
    m2: Nic::State,
    wcs: Seq<Wc>,
    befores: Seq<Nic::State>,
    afters: Seq<Nic::State>,
) -> bool {
    &&& befores.len() == wcs.len()
    &&& afters.len() == wcs.len()
    &&& forall|j: int|
        0 <= j < wcs.len() ==> nic_step(#[trigger] befores[j], afters[j], Nic::Step::poll_cq(sh.cq))
            && reports(wcs[j], befores[j].cqs[sh.cq].entries[0])
    &&& wcs.len() == 0 ==> rely_reaches(sh, pins, m, m2)
    &&& wcs.len() > 0 ==> rely_reaches(sh, pins, m, befores[0]) && afters.last() == m2
    &&& forall|j: int| 1 <= j < wcs.len() ==> rely_reaches(sh, pins, #[trigger] afters[j - 1], befores[j])
}

/// A poll's steps, in order.
proof fn lemma_poll_steps(sh: Share, pins: ISet<Key>, m: Nic::State, m2: Nic::State, wcs: Seq<Wc>) -> (r: (
    Seq<Nic::State>,
    Seq<Nic::State>,
))
    requires
        polled(sh, pins, m, m2, wcs),
    ensures
        poll_steps(sh, pins, m, m2, wcs, r.0, r.1),
    decreases wcs.len(),
{
    if wcs.len() == 0 {
        (Seq::empty(), Seq::empty())
    } else {
        let (mid, before) = choose|mid: Nic::State, before: Nic::State|
            {
                &&& polled(sh, pins, m, mid, wcs.drop_last())
                &&& rely_reaches(sh, pins, mid, before)
                &&& nic_step(before, m2, Nic::Step::poll_cq(sh.cq))
                &&& reports(wcs.last(), before.cqs[sh.cq].entries[0])
            };
        let (b, a) = lemma_poll_steps(sh, pins, m, mid, wcs.drop_last());
        let befores = b.push(before);
        let afters = a.push(m2);
        let k = wcs.len() - 1;
        assert forall|j: int| 0 <= j < wcs.len() implies nic_step(#[trigger] befores[j], afters[j], Nic::Step::poll_cq(sh.cq))
            && reports(wcs[j], befores[j].cqs[sh.cq].entries[0]) by {
            if j < k {
                assert(befores[j] == b[j] && afters[j] == a[j] && wcs[j] == wcs.drop_last()[j]);
            } else {
                assert(wcs[j] == wcs.last());
            }
        }
        if k == 0 {
            lemma_rely_trans(sh, pins, m, mid, before);
        } else {
            assert(rely_reaches(sh, pins, m, b[0]));
            assert(befores[0] == b[0]);
        }
        assert forall|j: int| 1 <= j < wcs.len() implies rely_reaches(sh, pins, #[trigger] afters[j - 1], befores[j]) by {
            if j < k {
                assert(afters[j - 1] == a[j - 1] && befores[j] == b[j]);
            } else {
                assert(afters[j - 1] == a.last());
            }
        }
        (befores, afters)
    }
}

impl<N: Device> Hw<N> {
    /// Routes one completion, the one `before`'s `poll_cq` step reports, to
    /// its QP's ring: its `wr_id` names the QP's index, the queue and the
    /// WQE, which the ring retires (with every earlier request it ends).
    #[verifier::spinoff_prover]
    fn dispatch(&mut self, wc: &Wc, Ghost(t): Ghost<NicToken>, Ghost(before): Ghost<Nic::State>, Ghost(after): Ghost<Nic::State>)
        requires
            old(self).inv_with(t, before),
            nic_step(before, after, Nic::Step::poll_cq(t.share().cq)),
            reports(*wc, before.cqs[t.share().cq].entries[0]),
        ensures
            final(self).inv_with(t, after),
            final(self).token == old(self).token,
            final(self).qps@.len() == old(self).qps@.len(),
            final(self).same_table(old(self)),
    {
        let ghost sh = t.share();
        let ghost c = sh.cq;
        let ghost start = *self;
        let ghost e = before.cqs[c].entries[0];
        proof {
            lemma_reports(*wc, e);
            safeverbs_spec::nic_proofs::lemma_head_is_first(before, c);
            lemma_poll(before, after, c);
            assert(share_wf(before, sh));
            assert(sh.owns.contains(e.qp));
        }
        let ghost i = choose|i: int| 0 <= i < start.qps@.len() && #[trigger] start.qps@[i] is Some && start.qps@[i]->0.id() == e.qp;
        let ghost qp = before.qps[e.qp];
        proof {
            assert(start.entry_ok(i, t, before));
            lemma_wr_id_fields(i as u16, e.queue is Recv, e.seq as u64);
            if e.queue is Send {
                assert(qp.sq[e.seq - qp.sq_head].wr.wr_id == spec_wr_id(i as u16, false, e.seq as u64));
            } else {
                assert(qp.rq[e.seq - qp.rq_head].wr.wr_id == spec_wr_id(i as u16, true, e.seq as u64));
            }
        }
        let index = index_of(wc.wr_id);
        let recv = is_recv(wc.wr_id);
        proof {
            assert(index == i);
            assert(recv == (e.queue is Recv));
        }
        if index >= self.qps.len() {
            return;
        }
        match &mut self.qps[index] {
            Some(qp_entry) => {
                let ghost e0 = *qp_entry;
                let retired;
                let changed;
                if recv {
                    let ghost send0 = qp_entry.send;
                    retired = qp_entry.recv.retired();
                    proof {
                        e0.recv.lemma_wqes();
                        lemma_low_bits_determine(e.seq as u64, e.seq as u64, e0.recv.spec_wqe_retired());
                    }
                    let wqe = wqe_of(wc.wr_id, qp_entry.recv.wqe_retired());
                    proof {
                        assert(wqe == e.seq);
                    }
                    let _known = qp_entry.recv.complete(wqe, *wc);
                    proof { lemma_resources_completed(e0.recv, qp_entry.recv, wqe, *wc); }
                    changed = qp_entry.recv.retired() != retired;
                    proof {
                        assert(qp_entry.send == e0.send);
                    }
                } else {
                    retired = qp_entry.send.retired();
                    proof {
                        e0.send.lemma_wqes();
                    }
                    let wqe = wqe_of(wc.wr_id, qp_entry.send.wqe_retired());
                    proof {
                        assert(wqe == e.seq);
                    }
                    let _known = qp_entry.send.complete(wqe, *wc);
                    proof { lemma_resources_completed(e0.send, qp_entry.send, wqe, *wc); }
                    changed = qp_entry.send.retired() != retired;
                    proof {
                        assert(qp_entry.recv == e0.recv);
                    }
                }
                if changed && !qp_entry.dirty && !qp_entry.removed {
                    qp_entry.dirty = true;
                    self.dirty.push(index as u16);
                }
            },
            None => {},
        }
        proof {
            let q = e.qp;
            let qp2 = after.qps[q];
            let en = self.qps@[i]->0;
            let e0 = start.qps@[i]->0;
            let nn = (e.seq + 1 - (if e.queue is Send { qp.sq_head } else { qp.rq_head })) as int;
            lemma_nic_step_is_next(before, after, Nic::Step::poll_cq(c));
            lemma_step_keeps_invariant(before, after);
            lemma_mine_keeps_share_wf(sh, ISet::empty(), before, after, Nic::Step::poll_cq(c));
            if e.queue is Send {
                assert forall|k: int| 0 <= k < qp2.sq.len() implies (#[trigger] qp2.sq[k]).wr.wr_id == spec_wr_id(
                    i as u16,
                    false,
                    (qp2.sq_head + k) as u64,
                ) by {
                    assert(qp2.sq[k] == qp.sq[k + nn]);
                }
                assert(sq_linked(en.send, i as u16, qp2));
                assert(rq_linked(en.recv, i as u16, qp2));
            } else {
                assert forall|k: int| 0 <= k < qp2.rq.len() implies (#[trigger] qp2.rq[k]).wr.wr_id == spec_wr_id(
                    i as u16,
                    true,
                    (qp2.rq_head + k) as u64,
                ) by {
                    assert(qp2.rq[k] == qp.rq[k + nn]);
                }
                assert(sq_linked(en.send, i as u16, qp2));
                assert(rq_linked(en.recv, i as u16, qp2));
            }
            en.send.lemma_wqes();
            en.recv.lemma_wqes();
            assert forall|k: int| 0 <= k < qp2.sq.len() implies {
                let wr = (#[trigger] qp2.sq[k]).wr;
                &&& !wr.inline
                &&& ring_backed(en.send, qp2.sq_head + k, wr.wr_id, Some(wr.opcode), wr.sges)
            } by {
                let old_k = if e.queue is Send { k + nn } else { k };
                assert(qp2.sq[k].wr == qp.sq[old_k].wr);
                let wr = qp2.sq[k].wr;
                lemma_backed_kept(e0.send, en.send, qp2.sq_head + k, wr.wr_id, Some(wr.opcode), wr.sges);
            }
            assert forall|k: int| 0 <= k < qp2.rq.len() implies {
                let wr = (#[trigger] qp2.rq[k]).wr;
                ring_backed(en.recv, qp2.rq_head + k, wr.wr_id, None, wr.sges)
            } by {
                let old_k = if e.queue is Recv { k + nn } else { k };
                assert(qp2.rq[k].wr == qp.rq[old_k].wr);
                let wr = qp2.rq[k].wr;
                lemma_backed_kept(e0.recv, en.recv, qp2.rq_head + k, wr.wr_id, None, wr.sges);
            }
            let written = t.written(q);
            assert forall|j: int| 0 <= j < written.len() implies
                written_backed(en, en.send.spec_wqe_committed() + j, #[trigger] written[j]) by {
                lemma_written_kept(e0, en, en.send.spec_wqe_committed() + j, written[j]);
            }
            assert forall|j: int| 0 <= j < written.len() implies after.send_wr_ok(qp2.attr, #[trigger] written[j]) by {
                assert(before.send_wr_ok(qp.attr, written[j]));
            }
            assert forall|w: WqeRef| w.qp == q && w.queue is Recv && w.seq < qp.rq_head
                && #[trigger] before.retired.contains_key(w)
                implies after.retired.contains_key(w) && after.retired[w] == before.retired[w] by {
                reveal(retires);
            }
            if e.queue is Recv {
                outcomes::lemma_recv_completed(e0.recv, en.recv, before, after, q, e.seq as u64, *wc);
            } else {
                outcomes::lemma_recv_framed(en.recv, before, after, q);
            }
            assert(self.entry_ok(i, t, after));
            // `poll_cq` changed only QP `q` and the CQ's entries.
            assert(after.cqs[c].reserved == before.cqs[c].reserved);
            assert forall|w: WqeRef| #[trigger] after.retired.contains_key(w) implies before.retired.contains_key(w) || w.qp == q by {
                reveal(retires);
            }
            assert forall|w: WqeRef| w.qp != q && #[trigger] before.retired.contains_key(w)
                implies after.retired.contains_key(w) && after.retired[w] == before.retired[w] by {
                reveal(retires);
            }
            self.lemma_poll_one(&start, t, before, after, i);
        }
    }

    /// After a `poll_cq` step that changed only entry `index`'s QP and
    /// retired WQEs of it (and a change of that entry), the invariant holds
    /// again.
    #[verifier::spinoff_prover]
    proof fn lemma_poll_one(&self, start: &Self, t: NicToken, mid: Nic::State, n: Nic::State, index: int)
        requires
            start.inv_with(t, mid),
            nic_inv(n),
            share_wf(n, t.share()),
            0 <= index < start.qps@.len(),
            start.qps@[index] is Some,
            self.qps@.len() == start.qps@.len(),
            forall|i: int| 0 <= i < self.qps@.len() && i != index ==> #[trigger] self.qps@[i] == start.qps@[i],
            self.qps@[index] is Some,
            self.qps@[index]->0.id() == start.qps@[index]->0.id(),
            self.entry_ok(index, t, n),
            self.cq == start.cq,
            self.pd == start.pd,
            self.caps == start.caps,
            self.cq_size == start.cq_size,
            self.reserved == start.reserved,
            self.next_name == start.next_name,
            n.qps.dom() =~= mid.qps.dom(),
            forall|p: QpId| #[trigger] mid.qps.contains_key(p) && p != start.qps@[index]->0.id() ==> n.qps[p] == mid.qps[p],
            n.caps == mid.caps,
            n.mrs == mid.mrs,
            n.pds == mid.pds,
            forall|w: WqeRef| #[trigger] n.retired.contains_key(w) ==> mid.retired.contains_key(w) || w.qp == start.qps@[index]->0.id(),
            forall|w: WqeRef| w.qp != start.qps@[index]->0.id() && #[trigger] mid.retired.contains_key(w)
                ==> n.retired.contains_key(w) && n.retired[w] == mid.retired[w],
            n.cqs[t.share().cq].cap == mid.cqs[t.share().cq].cap,
            n.cqs[t.share().cq].reserved == mid.cqs[t.share().cq].reserved,
        ensures
            self.inv_with(t, n),
    {
        let q = start.qps@[index]->0.id();
        assert(start.entry_ok(index, t, mid));
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] self.entry_ok(i, t, n) by {
            if i != index {
                assert(start.entry_ok(i, t, mid));
                if let Some(e) = self.qps@[i] {
                    assert(e.id() != q);
                    lemma_ok_same(e, i as u16, mid, n, t.share(), t.pd(), t.started(e.id()), t.written(e.id()));
                }
            }
        }
        assert forall|i: int, j: int|
            #![trigger self.qps@[i], self.qps@[j]]
            0 <= i < self.qps@.len() && 0 <= j < self.qps@.len() && i != j && self.qps@[i] is Some
                && self.qps@[j] is Some implies self.qps@[i]->0.id() != self.qps@[j]->0.id() by {
            if i != index && j != index {
                assert(self.qps@[i] == start.qps@[i] && self.qps@[j] == start.qps@[j]);
            } else if i == index {
                assert(self.qps@[j] == start.qps@[j]);
            } else {
                assert(self.qps@[i] == start.qps@[i]);
            }
        }
        assert forall|p: QpId| #[trigger] n.qps.contains_key(p) && t.share().owns.contains(p) implies exists|i: int|
            0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == p by {
            assert(mid.qps.contains_key(p));
            let i = choose|i: int| 0 <= i < start.qps@.len() && #[trigger] start.qps@[i] is Some && start.qps@[i]->0.id() == p;
            if i != index {
                assert(self.qps@[i] == start.qps@[i]);
            }
        }
        assert forall|p: QpId| #[trigger] writes(t, p) implies exists|i: int|
            0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == p by {
            let i = choose|i: int| 0 <= i < start.qps@.len() && #[trigger] start.qps@[i] is Some && start.qps@[i]->0.id() == p;
            if i != index {
                assert(self.qps@[i] == start.qps@[i]);
            }
        }
        assert forall|k: nat| k >= self.next_name@ implies !(#[trigger] n.qps.contains_key(t.name(k))) by {
            assert(!mid.qps.contains_key(t.name(k)));
        }
        assert forall|k: nat, w: WqeRef| k >= self.next_name@ && #[trigger] n.retired.contains_key(w)
            implies w.qp != #[trigger] t.name(k) by {
            assert(mid.qps.contains_key(q));
            assert(!mid.qps.contains_key(t.name(k)));
        }
    }

    /// Polls the CQ once, retiring what completed in the rings of its QPs
    /// (and waking the wakers registered there); returns the number of
    /// completions.
    #[verifier::spinoff_prover]
    pub fn progress(&mut self) -> (n: usize)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        let mut wcs = [EMPTY_WC; POLL_BATCH];
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        let ghost start = *self;
        let n = poll(&mut self.cq, &mut wcs, Tracked(self.token.borrow_mut()));
        let ghost t = self.token@;
        let ghost m2 = t.nic();
        let ghost done = wcs@.take(n as int);
        let ghost (befores, afters) = lemma_poll_steps(sh, pinned(t0), m0, m2, done);
        proof {
            self.lemma_inv_fields(&start, t0, m0);
            assert(done.len() == n);
        }
        let mut i = 0;
        while i < n
            invariant
                0 <= i <= n,
                sh == t0.share(),
                n <= wcs@.len(),
                done == wcs@.take(n as int),
                done.len() == n,
                poll_steps(sh, pinned(t0), m0, m2, done, befores, afters),
                self.token@ == t,
                t.same_worker(&t0),
                t.same_writings(&t0),
                self.qps@.len() == start.qps@.len(),
                self.same_table(&start),
                self.inv_with(t0, if i == 0 { m0 } else { afters[i - 1] }),
            decreases n - i,
        {
            proof {
                assert(0 <= i < done.len());
                assert(done[i as int] == wcs@[i as int]);
                if i == 0 {
                    assert(rely_reaches(sh, pinned(t0), m0, befores[0]));
                    self.lemma_rely(t0, m0, befores[0]);
                } else {
                    let j = i as int;
                    assert(rely_reaches(sh, pinned(t0), afters[j - 1], befores[j]));
                    self.lemma_rely(t0, afters[j - 1], befores[j]);
                }
                assert(nic_step(befores[i as int], afters[i as int], Nic::Step::poll_cq(t0.share().cq)));
                assert(reports(wcs@[i as int], befores[i as int].cqs[t0.share().cq].entries[0]));
            }
            self.dispatch(&wcs[i], Ghost(t0), Ghost(befores[i as int]), Ghost(afters[i as int]));
            i += 1;
        }
        proof {
            if n == 0 {
                self.lemma_rely(t0, m0, m2);
            } else {
                assert(afters[n - 1] == afters.last());
            }
            self.lemma_same_token(t0, t, m2);
        }
        n
    }
}

// =============================================================================
// Doorbells

impl<N: Device> Hw<N> {
    /// Rings QP `index`'s doorbell for every request written since the last
    /// one; with `keep`, hands back the leases of that staged request if the
    /// device refuses. If the device refuses (or the QP takes no work, which
    /// is refused as a device would refuse it: `Error::NotConnected`), none
    /// of them is posted: each with an owner retires at once with a flush
    /// error, each without releases its leases.
    #[verifier::spinoff_prover]
    pub fn commit(&mut self, index: usize, keep: Option<RingSeq>) -> (r: core::result::Result<(), (Error, Option<Leases<N>>)>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            r matches Err((_, Some(leases))) ==> leases.inv() && leases.fully_recorded(),
    {
        if index >= self.qps.len() {
            no_qp(index);
            return Ok(());
        }
        proof {
            assert(self.entry_ok(index as int, self.token@, self.token@.nic()));
        }
        let (started, open, ready) = match &self.qps[index] {
            Some(e) => (e.started, e.send.is_open(), matches!(e.phase, Phase::Ready)),
            None => {
                no_qp(index);
                (false, false, false)
            },
        };
        // An open request is the caller's to finish first: the borrow of a
        // request rules this out.
        if !started || open {
            return Ok(());
        }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        let r = match &mut self.qps[index] {
            Some(e) => {
                let ghost q = e.id@;
                let ghost e0 = *e;
                e.started = false;
                let r = if ready {
                    proof {
                        let qp = m0.qps[q];
                        e0.send.lemma_wqes();
                        assert(nic_enabled(m0, Nic::Step::post_send(q, t0.written(q))));
                    }
                    match doorbell(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut())) {
                        Ok(()) => {
                            e.send.commit();
                            Ok(())
                        },
                        Err(err) => {
                            let kept = match keep {
                                Some(seq) => e.send.abort_keeping(seq),
                                None => {
                                    e.send.abort();
                                    None
                                },
                            };
                            Err((err, kept))
                        },
                    }
                } else {
                    abort(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                    let kept = match keep {
                        Some(seq) => e.send.abort_keeping(seq),
                        None => {
                            e.send.abort();
                            None
                        },
                    };
                    Err((Error::NotConnected, kept))
                };
                if r.is_err() {
                    e.refused = true;
                    if !e.dirty {
                        e.dirty = true;
                        self.dirty.push(index as u16);
                    }
                }
                r
            },
            None => {
                no_qp(index);
                Ok(())
            },
        };
        proof {
            let t = self.token@;
            let n = t.nic();
            let e0 = start.qps@[index as int]->0;
            let q = e0.id();
            let e = self.qps@[index as int]->0;
            assert(start.entry_ok(index as int, t0, m0));
            if ready && r is Ok {
                let wrs = t0.written(q);
                let mid = choose|mid: Nic::State| rely_reaches(sh, pinned(t0), m0, mid) && nic_step(mid, n, Nic::Step::post_send(q, wrs));
                start.lemma_rely(t0, m0, mid);
                lemma_nic_step_is_next(mid, n, Nic::Step::post_send(q, wrs));
                lemma_step_keeps_invariant(mid, n);
                lemma_mine_keeps_share_wf(sh, pinned(t0), mid, n, Nic::Step::post_send(q, wrs));
                assert(start.entry_ok(index as int, t0, mid));
                let (a, b) = (mid.qps[q], n.qps[q]);
                e.send.lemma_wqes();
                lemma_queues_kept(e0, e, a);
                assert forall|k: int| 0 <= k < b.sq.len() implies {
                    let wr = (#[trigger] b.sq[k]).wr;
                    &&& !wr.inline
                    &&& ring_backed(e.send, b.sq_head + k, wr.wr_id, Some(wr.opcode), wr.sges)
                } by {
                    if k < a.sq.len() {
                        assert(b.sq[k] == a.sq[k]);
                    } else {
                        let j = k - a.sq.len();
                        assert(b.sq[k].wr == wrs[j]);
                        assert(written_backed(e0, e0.send.spec_wqe_committed() + j, wrs[j]));
                        assert(!e0.send.spec_is_open());
                        lemma_backed_kept(e0.send, e.send, b.sq_head + k,
                            wrs[j].wr_id, Some(wrs[j].opcode), wrs[j].sges);
                    }
                }
                assert forall|k: int| 0 <= k < b.sq.len() implies (#[trigger] b.sq[k]).wr.wr_id == spec_wr_id(
                    index as u16,
                    false,
                    (b.sq_head + k) as u64,
                ) by {
                    if k < a.sq.len() {
                        assert(b.sq[k] == a.sq[k]);
                    } else {
                        assert(b.sq[k].wr == wrs[k - a.sq.len()]);
                    }
                }
                assert(sq_linked(e.send, index as u16, b));
                outcomes::lemma_recv_framed(e.recv, mid, n, q);
                assert(self.entry_ok(index as int, t, n));
                self.lemma_commit_frame(&start, t0, t, mid, n, index as int);
            } else {
                start.lemma_rely(t0, m0, n);
                assert(start.entry_ok(index as int, t0, n));
                assert(sq_linked(e.send, index as u16, n.qps[q]));
                lemma_queues_kept(e0, e, n.qps[q]);
                assert(self.entry_ok(index as int, t, n));
                self.lemma_commit_frame(&start, t0, t, n, n, index as int);
            }
        }
        r
    }
}

impl<N: Device> Hw<N> {
    /// After a step that changed only entry `index`'s QP in M, the entry,
    /// and the token's writing of that QP (now done), the invariant holds
    /// again.
    #[verifier::spinoff_prover]
    proof fn lemma_commit_frame(&self, start: &Self, t0: NicToken, t: NicToken, mid: Nic::State, n: Nic::State, index: int)
        requires
            start.inv_with(t0, mid),
            nic_inv(n),
            share_wf(n, t.share()),
            0 <= index < start.qps@.len(),
            start.qps@[index] is Some,
            t.same_worker(&t0),
            t.same_writing_but(&t0, start.qps@[index]->0.id()),
            self.qps@.len() == start.qps@.len(),
            forall|i: int| 0 <= i < self.qps@.len() && i != index ==> #[trigger] self.qps@[i] == start.qps@[i],
            self.qps@[index] is Some,
            self.qps@[index]->0.id() == start.qps@[index]->0.id(),
            self.entry_ok(index, t, n),
            cq_of(self.cq) == cq_of(start.cq),
            self.pd == start.pd,
            self.caps == start.caps,
            self.cq_size == start.cq_size,
            self.reserved == start.reserved,
            self.next_name == start.next_name,
            n.qps.dom() =~= mid.qps.dom(),
            forall|p: QpId| #[trigger] mid.qps.contains_key(p) && p != start.qps@[index]->0.id() ==> n.qps[p] == mid.qps[p],
            n.caps == mid.caps,
            n.mrs == mid.mrs,
            n.pds == mid.pds,
            n.retired == mid.retired,
            n.cqs[t.share().cq].cap == mid.cqs[t.share().cq].cap,
            n.cqs[t.share().cq].reserved == mid.cqs[t.share().cq].reserved,
        ensures
            self.inv_with(t, n),
    {
        let q = start.qps@[index]->0.id();
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] self.entry_ok(i, t, n) by {
            if i != index {
                assert(start.entry_ok(i, t0, mid));
                if let Some(e) = self.qps@[i] {
                    assert(e.id() != q);
                    assert(t.started(e.id()) == t0.started(e.id()));
                    assert(t.written(e.id()) == t0.written(e.id()));
                    lemma_ok_same(e, i as u16, mid, n, t.share(), t.pd(), t.started(e.id()), t.written(e.id()));
                }
            }
        }
        assert forall|i: int, j: int|
            #![trigger self.qps@[i], self.qps@[j]]
            0 <= i < self.qps@.len() && 0 <= j < self.qps@.len() && i != j && self.qps@[i] is Some
                && self.qps@[j] is Some implies self.qps@[i]->0.id() != self.qps@[j]->0.id() by {
            if i != index && j != index {
                assert(self.qps@[i] == start.qps@[i] && self.qps@[j] == start.qps@[j]);
            } else if i == index {
                assert(self.qps@[j] == start.qps@[j]);
            } else {
                assert(self.qps@[i] == start.qps@[i]);
            }
        }
        assert forall|p: QpId| #[trigger] n.qps.contains_key(p) && t.share().owns.contains(p) implies exists|i: int|
            0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == p by {
            assert(mid.qps.contains_key(p));
            let i = choose|i: int| 0 <= i < start.qps@.len() && #[trigger] start.qps@[i] is Some && start.qps@[i]->0.id() == p;
            if i != index {
                assert(self.qps@[i] == start.qps@[i]);
            }
        }
        assert forall|p: QpId| #[trigger] writes(t, p) implies exists|i: int|
            0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == p by {
            if p == q {
                assert(self.qps@[index] is Some);
            } else {
                assert(t.started(p) == t0.started(p));
                assert(t.written(p) == t0.written(p));
                assert(writes(t0, p));
                let i = choose|i: int| 0 <= i < start.qps@.len() && #[trigger] start.qps@[i] is Some && start.qps@[i]->0.id() == p;
                if i != index {
                    assert(self.qps@[i] == start.qps@[i]);
                }
            }
        }
        assert forall|a: nat| #[trigger] t.share().owns.contains(t.name(a)) by {
            assert(t.name(a) == t0.name(a));
        }
        assert forall|a: nat, b: nat| #[trigger] t.name(a) == #[trigger] t.name(b) implies a == b by {
            assert(t.name(a) == t0.name(a) && t.name(b) == t0.name(b));
        }
        assert forall|k: nat| k >= self.next_name@ implies !(#[trigger] n.qps.contains_key(t.name(k))) by {
            assert(t.name(k) == t0.name(k));
            assert(!mid.qps.contains_key(t0.name(k)));
        }
        assert forall|k: nat, w: WqeRef| k >= self.next_name@ && #[trigger] n.retired.contains_key(w)
            implies w.qp != #[trigger] t.name(k) by {
            assert(t.name(k) == t0.name(k));
        }
    }
}

// =============================================================================
// Requests

/// The bytes of a scatter/gather list, as M counts them.
spec fn sges_len(sges: Seq<Sge>) -> nat {
    total_len(sges.map_values(|sge: Sge| sge_model(sge)))
}

/// M accepts a held WQE once its parts' regions are registered.
proof fn lemma_held_wr_ok<N: Device>(h: Held, parts: Seq<Part<N>>, index: u16, num: int, m: Nic::State, attr: QpAttr, pd_id: u64, signaled: bool)
    requires
        held_ok(h, parts, index, num, attr, pd_id, m.caps.max_msg),
        forall|j: int|
            0 <= j < parts.len() ==> m.registered(attr.pd, sge_model(part_sge(#[trigger] parts[j])), part_writable(parts[j])),
    ensures
        m.send_wr_ok(attr, held_wr(h, signaled)),
{
    let wr = held_wr(h, signaled);
    let sges = h.sges@.take(h.n as int);
    assert forall|i: int| 0 <= i < wr.sges.len() implies m.registered(attr.pd, #[trigger] wr.sges[i], wr.opcode.writes_local()) by {
        assert(wr.sges[i] == sge_model(sges[i]));
        assert(sges[i] == h.sges@[i]);
        assert(h.sges@[i] == part_sge(parts[i]));
        assert(m.registered(attr.pd, sge_model(part_sge(parts[i])), part_writable(parts[i])));
    }
}

impl<N: Device> Hw<N> {
    /// Whether QP `index` has a request open (between its first WQE and
    /// [`Hw::close`]).
    pub fn is_open(&self, index: usize) -> bool
        requires
            self.inv(),
    {
        if index >= self.qps.len() {
            return false;
        }
        proof {
            assert(self.entry_ok(index as int, self.token@, self.token@.nic()));
        }
        match &self.qps[index] {
            Some(e) => e.send.is_open(),
            None => false,
        }
    }

    /// Closes QP `index`'s open request: stages it in the ring and writes its
    /// last WQE, signaled if `signal` or if the ring says so; `detached` if
    /// nobody will take it back. Returns its sequence number, `None` without
    /// an open request. It goes out at the next doorbell.
    #[verifier::spinoff_prover]
    pub fn close(&mut self, index: usize, signal: bool, detached: bool) -> (r: Option<RingSeq>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        if index >= self.qps.len() {
            return None;
        }
        proof {
            assert(self.entry_ok(index as int, self.token@, self.token@.nic()));
        }
        let open = match &self.qps[index] {
            Some(e) => e.send.is_open(),
            None => false,
        };
        if !open {
            return None;
        }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        // The held WQE's regions are registered (its parts' leases say so).
        match &self.qps[index] {
            Some(e) => registered_all(&e.held_parts, &self.pd, Tracked(self.token.borrow_mut())),
            None => {},
        }
        let ghost t1 = self.token@;
        let ghost m1 = t1.nic();
        proof {
            start.lemma_rely(t0, m0, m1);
            start.lemma_same_token(t0, t1, m1);
            self.lemma_inv_fields(&start, t1, m1);
        }
        let ghost mid1 = *self;
        let r = match &mut self.qps[index] {
            Some(e) => {
                let ghost q = e.id@;
                let ghost e1 = *e;
                proof {
                    assert(mid1.entry_ok(index as int, t1, m1));
                }
                let h = match e.held {
                    Some(h) => h,
                    None => {
                        proof { assert(false); }
                        return None;
                    },
                };
                let ghost footprint = crate::buf::WqeLoans {
                    wr_id: h.wr_id, opcode: Some(opcode_model(h.op)),
                    sges: h.sges@.take(h.n as int), first: e.open_leases.loans().len() as int,
                };
                keep_parts(&mut e.open_leases, &mut e.held_parts, Ghost(footprint));
                let mut leases = no_leases();
                core::mem::swap(&mut leases, &mut e.open_leases);
                let bytes = e.held_bytes as u32;
                let implied = Wc::success(opcode_of(&h.op), bytes);
                proof {
                    assert(implied.spec_byte_len() == if opcode_model(h.op).writes_local() {
                        Some(sges_len(h.sges@.take(h.n as int)) as u32)
                    } else { None });
                    e1.send.lemma_wqes();
                }
                let ghost resource = leases;
                let (seq, signaled) = e.send.end(signal, detached, implied, leases);
                proof { lemma_resources_added(e1.send, e.send, seq as int, resource); }
                if !e.started {
                    crate::trusted::start(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                    e.started = true;
                }
                let sges = vstd::slice::slice_subrange(h.sges.as_slice(), 0, h.n);
                post(&mut e.hw, h.wr_id, signaled, &h.op, sges, Ghost(q), Tracked(self.token.borrow_mut()));
                e.send_tails = Ghost(e.send_tails@.insert(seq as int, held_wr(h, signaled)));
                e.held = None;
                e.held_bytes = 0;
                proof {
                    assert(sges@ == h.sges@.take(h.n as int));
                    lemma_held_wr_ok(h, e1.held_parts@, index as u16, e1.send.spec_wqe_committed() + t1.written(q).len(), m1, m1.qps[q].attr, e1.pd_id@, signaled);
                }
                proof {
                    e.send.lemma_wqes();
                    lemma_queues_kept(e1, *e, m1.qps[q]);
                    let written = self.token@.written(q);
                    assert forall|j: int| 0 <= j < written.len() implies
                        written_backed(*e, e.send.spec_wqe_committed() + j, #[trigger] written[j]) by {
                        let w = e.send.spec_wqe_committed() + j;
                        let wr = written[j];
                        if j < t1.written(q).len() {
                            assert(wr == t1.written(q)[j]);
                            assert(written_backed(e1, w, wr));
                            if ring_backed(e1.send, w, wr.wr_id, Some(wr.opcode), wr.sges) {
                                lemma_backed_kept(e1.send, e.send, w, wr.wr_id, Some(wr.opcode), wr.sges);
                            } else {
                                lemma_ledger_extended(e1.open_leases, resource, wr.wr_id, Some(wr.opcode), wr.sges);
                                assert(e.send.dma(seq as int, w) == Some(resource));
                            }
                        } else {
                            assert(wr == held_wr(h, signaled));
                            assert(e.send.dma(seq as int, w) == Some(resource));
                            assert(resource.wqes().last() == footprint);
                            assert(ledger_has(resource, wr.wr_id, Some(wr.opcode), wr.sges));
                        }
                    }
                }
                Some(seq)
            },
            None => None,
        };
        proof {
            let t = self.token@;
            let q = mid1.qps@[index as int]->0.id();
            let e = self.qps@[index as int]->0;
            let e1 = mid1.qps@[index as int]->0;
            assert(mid1.entry_ok(index as int, t1, m1));
            let written = t.written(q);
            let attr = m1.qps[q].attr;
            assert forall|j: int| 0 <= j < written.len() implies m1.send_wr_ok(attr, #[trigger] written[j]) by {
                if j < t1.written(q).len() {
                    assert(written[j] == t1.written(q)[j]);
                }
            }
            assert(writing_ok(e.send, index as u16, m1, attr, t.started(q), written, false));
            assert(self.entry_ok(index as int, t, m1));
            self.lemma_commit_frame(&mid1, t1, t, m1, m1, index as int);
        }
        r
    }
}

/// The bytes of the first `i` elements, one more element at a time.
proof fn lemma_sges_len_step(s: Seq<Sge>, i: int)
    requires
        0 <= i < s.len(),
    ensures
        sges_len(s.take(i + 1)) == sges_len(s.take(i)) + s[i].length,
{
    let f = |sge: Sge| sge_model(sge);
    let a = s.take(i + 1).map_values(f);
    assert(a.drop_last() =~= s.take(i).map_values(f));
    assert(a.last() == sge_model(s[i]));
}

/// `same_table` is transitive.
proof fn lemma_same_table_trans<N: Device>(a: &Hw<N>, b: &Hw<N>, c: &Hw<N>)
    requires
        a.same_table(b),
        b.same_table(c),
    ensures
        a.same_table(c),
{
    assert(a.caps == c.caps && a.pd == c.pd);
    assert forall|i: int| 0 <= i < a.qps@.len() implies (#[trigger] a.qps@[i] is Some) == (c.qps@[i] is Some) by {
        assert(b.qps@[i] is Some == c.qps@[i] is Some);
    }
    assert forall|i: int| 0 <= i < a.qps@.len() && #[trigger] a.qps@[i] is Some implies {
        let (x, z) = (a.qps@[i]->0, c.qps@[i]->0);
        &&& x.id@ == z.id@
        &&& x.qp_type == z.qp_type
        &&& x.atomics == z.atomics
        &&& x.caps == z.caps
        &&& x.pd_id == z.pd_id
    } by {
        assert(b.qps@[i] is Some);
    }
}

impl<N: Device> Hw<N> {
    /// Whether QP `index`'s send ring could take a request of one WQE now.
    closed spec fn room(&self, index: int) -> bool {
        let r = self.qps@[index]->0.send;
        &&& !r.spec_is_open()
        &&& r.spec_has_free_slot()
        &&& r.spec_wqe_posted() - r.spec_wqe_retired() < r.spec_depth()
    }

    /// QP `index`'s send ring can take one more WQE: a new request (no
    /// request open, a free slot, room for a WQE) or the open one has room.
    closed spec fn can_hold(&self, index: int) -> bool {
        let r = self.qps@[index]->0.send;
        ||| self.room(index)
        ||| r.spec_has_wqe_room()
    }

    /// A WQE doing `op` on `n` parts is one QP `index` may take: within its
    /// scatter/gather limit, an operation its transport allows.
    closed spec fn wqe_fits(&self, index: int, op: WrOp, n: usize) -> bool {
        let e = self.qps@[index]->0;
        let attr = qp_attr(0, e.qp_type, 0, e.caps);
        &&& n <= attr.max_sge
        &&& opcode_model(op).allowed_on(attr.transport)
    }

    /// Waits until a new send request of at least one WQE fits on QP
    /// `index`: rings its doorbell for what is written and polls the CQ
    /// until a slot and a WQE are free. Fails if every slot holds a result
    /// nobody took, or if the doorbell is refused.
    #[verifier::exec_allows_no_decreases_clause]
    #[verifier::spinoff_prover]
    fn make_send_room(&mut self, index: usize) -> (r: Result<()>)
        requires
            old(self).inv(),
            old(self).has_qp(index as int),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            r is Ok ==> final(self).room(index as int),
    {
        let ghost start = *self;
        loop
            invariant
                self.inv(),
                self.same_table(&start),
                start.has_qp(index as int),
                start == *old(self),
        {
            proof {
                assert(self.entry_ok(index as int, self.token@, self.token@.nic()));
            }
            let (can, clogged) = match &self.qps[index] {
                Some(e) => (e.send.can_post(), e.send.is_clogged()),
                None => (false, false),
            };
            if can {
                return Ok(());
            }
            if clogged {
                return Err(Error::Clogged);
            }
            let ghost s0 = *self;
            match self.commit(index, None) {
                Ok(()) => {},
                Err((e, _)) => {
                    proof {
                        lemma_same_table_trans(self, &s0, &start);
                    }
                    return Err(e);
                },
            }
            let ghost s1 = *self;
            self.progress();
            proof {
                lemma_same_table_trans(&s1, &s0, &start);
                lemma_same_table_trans(self, &s1, &start);
            }
        }
    }

    /// Adds `part` to the scatter/gather list of the WQE being written on QP
    /// `index`; refused (the part handed back) past [`MAX_SGES`] parts.
    #[verifier::spinoff_prover]
    pub fn gather(&mut self, index: usize, part: Part<N>) -> (r: core::result::Result<(), BufError<Part<N>>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        if index >= self.qps.len() {
            no_qp(index);
            return Err(BufError { error: Error::Invalid("no such QP"), buf: part });
        }
        let ghost start = *self;
        let ghost t = self.token@;
        let ghost m = t.nic();
        proof {
            assert(start.entry_ok(index as int, t, m));
        }
        let r = match &mut self.qps[index] {
            Some(e) => {
                if e.building.len() >= MAX_SGES {
                    let max = e.caps.max_send_sge as usize;
                    Err(BufError { error: Error::TooManySges { got: MAX_SGES + 1, max }, buf: part })
                } else {
                    e.building.push(part);
                    Ok(())
                }
            },
            None => {
                no_qp(index);
                Err(BufError { error: Error::Invalid("no such QP"), buf: part })
            },
        };
        proof {
            assert(self.entry_ok(index as int, t, m));
            self.lemma_touch_one(&start, t, m, m, index as int);
        }
        r
    }

    /// Drops the parts gathered on QP `index` but one, which it hands back if
    /// it was the only one.
    fn give_back(parts: &mut Vec<Part<N>>) -> (r: Option<Part<N>>)
        ensures
            final(parts)@.len() == 0,
    {
        let r = if parts.len() == 1 {
            parts.pop()
        } else {
            None
        };
        while parts.len() > 0
            decreases parts@.len(),
        {
            parts.pop();
        }
        r
    }

    /// Finishes the WQE gathered on QP `index` with operation `op`: checks
    /// it (what the QP may do, the parts' domain and uniqueness, the
    /// lengths), opens a request for it or splits the open one when it is
    /// full (waiting for room: this polls the CQ), writes the WQE held so far
    /// and holds this one. On error nothing is added; the part is handed
    /// back if there was one, else the parts are dropped.
    #[verifier::spinoff_prover]
    pub fn push(&mut self, index: usize, op: WrOp) -> (r: core::result::Result<(), (Error, Option<Part<N>>)>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        if index >= self.qps.len() {
            no_qp(index);
            return Err((Error::Invalid("no such QP"), None));
        }
        let pd_id = pd_id_of(&self.pd);
        let max_msg = self.caps.max_msg;
        let ghost start = *self;
        let ghost t = self.token@;
        let ghost m = t.nic();
        proof {
            assert(start.entry_ok(index as int, t, m));
        }
        // The parts, out of the entry.
        let mut parts: Vec<Part<N>> = Vec::new();
        let (rc, atomics, max_sge) = match &mut self.qps[index] {
            Some(e) => {
                core::mem::swap(&mut parts, &mut e.building);
                let max_sge = if e.caps.max_send_sge <= e.caps.max_recv_sge {
                    e.caps.max_send_sge
                } else {
                    e.caps.max_recv_sge
                };
                (matches!(e.qp_type, QpType::Rc), e.atomics, max_sge)
            },
            None => {
                no_qp(index);
                (false, false, 0)
            },
        };
        proof {
            assert(self.entry_ok(index as int, t, m));
            self.lemma_touch_one(&start, t, m, m, index as int);
        }
        // What the QP may do: READ and atomics on reliable connections only,
        // atomics where the device has them.
        let n = parts.len();
        let check = if n == 0 || n > max_sge as usize {
            Err(Error::TooManySges { got: n, max: max_sge as usize })
        } else if matches!(op, WrOp::Read(_)) && !rc {
            Err(Error::Unsupported("RDMA READ on this QP"))
        } else if (matches!(op, WrOp::FetchAdd { .. }) || matches!(op, WrOp::CompareSwap { .. })) && !atomics {
            Err(Error::Unsupported("atomics on this QP"))
        } else {
            Ok(())
        };
        if let Err(error) = check {
            let back = Self::give_back(&mut parts);
            return Err((error, back));
        }
        let writes_local = matches!(op, WrOp::Read(_)) || matches!(op, WrOp::FetchAdd { .. })
            || matches!(op, WrOp::CompareSwap { .. });
        let limit: u64 = if max_msg < 0x7fff_ffff {
            max_msg as u64
        } else {
            0x7fff_ffff
        };
        // The scatter/gather list, its owners and its length.
        let mut sges = [Sge { addr: 0, length: 0, lkey: 0 }; MAX_SGES];
        let mut total: u64 = 0;
        let mut i = 0;
        let mut bad: Option<Error> = None;
        while i < n
            invariant_except_break
                bad is None,
            invariant
                n == parts@.len(),
                1 <= n <= MAX_SGES,
                0 <= i <= n,
                bad is None ==> total == sges_len(sges@.take(i as int)) && total <= limit,
                bad is None ==> forall|j: int|
                    0 <= j < i ==> sges@[j] == part_sge(#[trigger] parts@[j]) && part_pd(parts@[j]) == pd_id
                        && (writes_local ==> part_writable(parts@[j])),
                limit <= 0x7fff_ffff,
            ensures
                bad is None ==> i == n,
            decreases n - i,
        {
            let sge = part_sge_of(&parts[i]);
            if part_pd_of(&parts[i]) != pd_id {
                bad = Some(Error::PdMismatch);
                break;
            }
            if writes_local && !part_writable_of(&parts[i]) {
                bad = Some(Error::Invalid("the NIC would write a frozen buffer"));
                break;
            }
            let ghost before = sges@;
            sges[i] = sge;
            proof {
                assert(sges@.take(i as int) =~= before.take(i as int));
                lemma_sges_len_step(sges@, i as int);
            }
            total += sge.length as u64;
            if total > limit {
                bad = Some(Error::TooLong { len: total, max: limit });
                break;
            }
            i += 1;
        }
        if let Some(error) = bad {
            let back = Self::give_back(&mut parts);
            return Err((error, back));
        }
        proof {
            assert(i == n);
            assert(total == sges_len(sges@.take(n as int)));
        }
        let remote = match op {
            WrOp::Write(r) | WrOp::WriteImm(r, _) | WrOp::Read(r) => if total > r.len {
                Err(Error::TooLong { len: total, max: r.len })
            } else {
                Ok(())
            },
            WrOp::FetchAdd { remote, .. } | WrOp::CompareSwap { remote, .. } => if total != 8 || n != 1 {
                Err(Error::Invalid("an atomic needs one local buffer of 8 bytes"))
            } else if remote.len < 8 || remote.addr % 8 != 0 {
                Err(Error::Invalid("an atomic needs 8 aligned remote bytes"))
            } else {
                Ok(())
            },
            WrOp::Send | WrOp::SendImm(_) => Ok(()),
        };
        if let Err(error) = remote {
            let back = Self::give_back(&mut parts);
            return Err((error, back));
        }
        // A request with room for the WQE: a new one, or the open one.
        proof {
            assert(self.entry_ok(index as int, self.token@, self.token@.nic()));
        }
        let (open, wqe_room) = match &self.qps[index] {
            Some(e) => (e.send.is_open(), e.send.wqe_room()),
            None => (false, 0),
        };
        if !open || wqe_room == 0 {
            if open {
                self.close(index, true, true);
            }
            if let Err(error) = self.make_send_room(index) {
                let back = Self::give_back(&mut parts);
                return Err((error, back));
            }
        }
        proof {
            let e = self.qps@[index as int]->0;
            assert(self.entry_ok(index as int, self.token@, self.token@.nic()));
            lemma_same_table_trans(self, &start, &start);
        }
        self.hold(index, op, parts, sges, n, total);
        Ok(())
    }

    /// The new WQE of [`Hw::push`], checked, in a request with room for it:
    /// writes the WQE held so far, if any, and holds this one.
    #[verifier::spinoff_prover]
    fn hold(&mut self, index: usize, op: WrOp, parts: Vec<Part<N>>, sges: [Sge; MAX_SGES], n: usize, total: u64)
        requires
            old(self).inv(),
            old(self).has_qp(index as int),
            old(self).can_hold(index as int),
            parts@.len() == n,
            1 <= n <= MAX_SGES,
            forall|j: int|
                0 <= j < n ==> sges@[j] == part_sge(#[trigger] parts@[j]) && part_pd(parts@[j]) == pd_id(old(self).pd)
                    && (opcode_model(op).writes_local() ==> part_writable(parts@[j])),
            total == sges_len(sges@.take(n as int)),
            total <= old(self).caps.max_msg,
            old(self).wqe_fits(index as int, op, n),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        let mut parts = parts;
        // The WQE held so far (if any) is written now: its regions are
        // registered (its parts' leases say so).
        proof {
            assert(self.entry_ok(index as int, self.token@, self.token@.nic()));
        }
        let ghost s2 = *self;
        let ghost t2 = self.token@;
        let ghost m2 = t2.nic();
        let has_held = match &self.qps[index] {
            Some(e) => e.held.is_some(),
            None => false,
        };
        if has_held {
            match &self.qps[index] {
                Some(e) => registered_all(&e.held_parts, &self.pd, Tracked(self.token.borrow_mut())),
                None => {},
            }
        }
        let ghost t3 = self.token@;
        let ghost m3 = t3.nic();
        proof {
            if has_held {
                s2.lemma_rely(t2, m2, m3);
                s2.lemma_same_token(t2, t3, m3);
                self.lemma_inv_fields(&s2, t3, m3);
            }
            assert(self.entry_ok(index as int, t3, m3));
        }
        let ghost s3 = *self;
        match &mut self.qps[index] {
            Some(e) => {
                let ghost q = e.id@;
                let ghost e3 = *e;
                proof {
                    e3.send.lemma_wqes();
                }
                if !e.send.is_open() {
                    match e.send.begin() {
                        Some(_) => {},
                        None => {
                            proof { assert(false); }
                        },
                    }
                }
                proof {
                    e.send.lemma_wqes();
                }
                let wqe = match e.send.push_wqe() {
                    Some(wqe) => wqe,
                    None => {
                        proof { assert(false); }
                        0
                    },
                };
                match e.held {
                    Some(h) => {
                        if !e.started {
                            crate::trusted::start(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                            e.started = true;
                        }
                        let held_sges = vstd::slice::slice_subrange(h.sges.as_slice(), 0, h.n);
                        post(&mut e.hw, h.wr_id, false, &h.op, held_sges, Ghost(q), Tracked(self.token.borrow_mut()));
                        proof {
                            assert(held_sges@ == h.sges@.take(h.n as int));
                            lemma_held_wr_ok(h, e3.held_parts@, index as u16, e3.send.spec_wqe_committed() + t3.written(q).len(), m3, m3.qps[q].attr, e3.pd_id@, false);
                        }
                        let ghost footprint = crate::buf::WqeLoans {
                            wr_id: h.wr_id, opcode: Some(opcode_model(h.op)),
                            sges: h.sges@.take(h.n as int), first: e.open_leases.loans().len() as int,
                        };
                        keep_parts(&mut e.open_leases, &mut e.held_parts, Ghost(footprint));
                    },
                    None => {},
                }
                e.held = Some(Held { wr_id: wr_id(index as u16, false, wqe), op, sges, n });
                core::mem::swap(&mut e.held_parts, &mut parts);
                core::mem::swap(&mut e.building, &mut parts);
                e.held_bytes = total;
            },
            None => {},
        }
        proof {
            let t4 = self.token@;
            let e3 = s3.qps@[index as int]->0;
            let e = self.qps@[index as int]->0;
            let q = e3.id();
            let attr = m3.qps[q].attr;
            let written = t4.written(q);
            assert(s3.entry_ok(index as int, t3, m3));
            assert forall|j: int| 0 <= j < written.len() implies m3.send_wr_ok(attr, #[trigger] written[j]) by {
                if j < t3.written(q).len() {
                    assert(written[j] == t3.written(q)[j]);
                }
            }
            assert(writing_ok(e.send, index as u16, m3, attr, t4.started(q), written, true));
            e.send.lemma_wqes();
            lemma_queues_kept(e3, e, m3.qps[q]);
            assert forall|j: int| 0 <= j < written.len() implies
                written_backed(e, e.send.spec_wqe_committed() + j, #[trigger] written[j]) by {
                let w = e.send.spec_wqe_committed() + j;
                let wr = written[j];
                if j < t3.written(q).len() {
                    assert(wr == t3.written(q)[j]);
                    lemma_written_kept(e3, e, w, wr);
                } else {
                    let h = e3.held->0;
                    assert(wr == held_wr(h, false));
                    assert(e.open_leases.wqes().last().wr_id == wr.wr_id);
                    assert(e.open_leases.wqes().last().opcode == Some(wr.opcode));
                    assert(e.open_leases.wqes().last().sges.map_values(|s: Sge| sge_model(s)) == wr.sges);
                    assert(ledger_has(e.open_leases, wr.wr_id, Some(wr.opcode), wr.sges));
                }
            }
            assert(opcode_model(op).allowed_on(attr.transport));
            assert(e.held->0.sges == sges && e.held->0.n == n);
            assert(total == sges_len(sges@.take(n as int)));
            assert(caps_model(m3.caps, self.caps));
            assert(total <= m3.caps.max_msg);
            assert(held_ok(e.held->0, e.held_parts@, index as u16, e.send.spec_wqe_committed() + written.len(), attr, e.pd_id@, m3.caps.max_msg));
            lemma_resources_kept(e3.send, e.send);
            assert(self.entry_ok(index as int, t4, m3));
            self.lemma_commit_frame(&s3, t3, t4, m3, m3, index as int);
        }
    }
}

// =============================================================================
// The rings, for their owners and the event loop

/// Which ring of a QP.
#[derive(Clone, Copy)]
pub enum Side {
    /// The send queue's.
    Send,
    /// The receive queue's.
    Recv,
}

impl<N: Device> Hw<N> {
    /// Whether `index` names a QP of the worker (not one being removed).
    pub fn is_qp(&self, index: usize) -> (b: bool)
        requires
            self.inv(),
    {
        if index >= self.qps.len() {
            return false;
        }
        match &self.qps[index] {
            Some(e) => !e.removed,
            None => false,
        }
    }

    /// QP `index`'s ring of `side`, if it is a QP of the worker.
    pub fn ring(&self, index: usize, side: Side) -> Option<&Ring<Leases<N>>>
        requires
            self.inv(),
    {
        if index >= self.qps.len() {
            return None;
        }
        match &self.qps[index] {
            Some(e) => match side {
                Side::Send => Some(&e.send),
                Side::Recv => Some(&e.recv),
            },
            None => None,
        }
    }

    /// QP `index`'s queue sizes.
    pub fn qp_caps(&self, index: usize) -> Option<QpCaps>
        requires
            self.inv(),
    {
        if index >= self.qps.len() {
            return None;
        }
        match &self.qps[index] {
            Some(e) => Some(e.caps),
            None => None,
        }
    }

    /// QP `index`'s number on the device, and whether the driver takes no
    /// locks for it.
    pub fn qp_num(&self, index: usize) -> Option<(u32, bool)>
        requires
            self.inv(),
    {
        if index >= self.qps.len() {
            return None;
        }
        match &self.qps[index] {
            Some(e) => Some((qp_num_of(&e.hw), qp_lock_free(&e.hw))),
            None => None,
        }
    }

    /// Takes the outcome and leases of retired (or refused) request `seq`
    /// of QP `index`'s ring of `side`.
    #[verifier::spinoff_prover]
    pub fn take(&mut self, index: usize, side: Side, seq: RingSeq) -> (r: Option<(Wc, Leases<N>)>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            r matches Some((_, leases)) ==> leases.inv() && leases.fully_recorded(),
    {
        if index >= self.qps.len() {
            return None;
        }
        let ghost start = *self;
        let ghost t = self.token@;
        let ghost m = t.nic();
        proof {
            assert(start.entry_ok(index as int, t, m));
        }
        let r = match &mut self.qps[index] {
            Some(e) => match side {
                Side::Send => e.send.take(seq),
                Side::Recv => e.recv.take(seq),
            },
            None => None,
        };
        proof {
            if start.qps@[index as int] is Some {
                let old_e = start.qps@[index as int]->0;
                let e = self.qps@[index as int]->0;
                lemma_resources_kept(old_e.send, e.send);
                lemma_resources_kept(old_e.recv, e.recv);
                let pre = start.qps@[index as int]->0;
                let post = self.qps@[index as int]->0;
                lemma_queues_kept(pre, post, m.qps[pre.id()]);
                outcomes::lemma_recv_kept(pre.recv, post.recv, m, pre.id());
                post.send.lemma_wqes();
                assert forall|j: int| 0 <= j < t.written(pre.id()).len()
                    implies written_backed(post, post.send.spec_wqe_committed() + j,
                        #[trigger] t.written(pre.id())[j]) by {
                    lemma_written_kept(pre, post, pre.send.spec_wqe_committed() + j, t.written(pre.id())[j]);
                }
                assert(self.entry_ok(index as int, t, m));
                self.lemma_touch_one(&start, t, m, m, index as int);
            } else {
                assert(self.qps@ =~= start.qps@);
                self.lemma_inv_fields(&start, t, m);
            }
        }
        r
    }

    /// Gives up request `seq` of QP `index`'s ring of `side`: its leases are
    /// released when it retires (now, if it has).
    #[verifier::spinoff_prover]
    pub fn detach(&mut self, index: usize, side: Side, seq: RingSeq)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        if index >= self.qps.len() {
            return;
        }
        let ghost start = *self;
        let ghost t = self.token@;
        let ghost m = t.nic();
        proof {
            assert(start.entry_ok(index as int, t, m));
        }
        match &mut self.qps[index] {
            Some(e) => match side {
                Side::Send => e.send.detach(seq),
                Side::Recv => e.recv.detach(seq),
            },
            None => {},
        }
        proof {
            if start.qps@[index as int] is Some {
                let pre = start.qps@[index as int]->0;
                let post = self.qps@[index as int]->0;
                lemma_queues_kept(pre, post, m.qps[pre.id()]);
                outcomes::lemma_recv_kept(pre.recv, post.recv, m, pre.id());
                post.send.lemma_wqes();
                assert forall|j: int| 0 <= j < t.written(pre.id()).len()
                    implies written_backed(post, post.send.spec_wqe_committed() + j,
                        #[trigger] t.written(pre.id())[j]) by {
                    lemma_written_kept(pre, post, pre.send.spec_wqe_committed() + j, t.written(pre.id())[j]);
                }
                assert(self.entry_ok(index as int, t, m));
                self.lemma_touch_one(&start, t, m, m, index as int);
            } else {
                assert(self.qps@ =~= start.qps@);
                self.lemma_inv_fields(&start, t, m);
            }
        }
    }

    /// Wakes `waker` when request `seq` of QP `index`'s ring of `side`
    /// retires.
    #[verifier::spinoff_prover]
    pub fn set_waker(&mut self, index: usize, side: Side, seq: RingSeq, waker: &core::task::Waker)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        if index >= self.qps.len() {
            return;
        }
        let ghost start = *self;
        let ghost t = self.token@;
        let ghost m = t.nic();
        proof {
            assert(start.entry_ok(index as int, t, m));
        }
        match &mut self.qps[index] {
            Some(e) => {
                let ghost send = e.send;
                let ghost recv = e.recv;
                match side {
                    Side::Send => e.send.set_waker(seq, waker),
                    Side::Recv => e.recv.set_waker(seq, waker),
                }
                proof {
                    lemma_resources_kept(send, e.send);
                    lemma_resources_kept(recv, e.recv);
                }
            },
            None => {},
        }
        proof {
            if start.qps@[index as int] is Some {
                let pre = start.qps@[index as int]->0;
                let post = self.qps@[index as int]->0;
                lemma_queues_kept(pre, post, m.qps[pre.id()]);
                outcomes::lemma_recv_kept(pre.recv, post.recv, m, pre.id());
                post.send.lemma_wqes();
                assert forall|j: int| 0 <= j < t.written(pre.id()).len()
                    implies written_backed(post, post.send.spec_wqe_committed() + j,
                        #[trigger] t.written(pre.id())[j]) by {
                    lemma_written_kept(pre, post, pre.send.spec_wqe_committed() + j, t.written(pre.id())[j]);
                }
                assert(self.entry_ok(index as int, t, m));
                self.lemma_touch_one(&start, t, m, m, index as int);
            } else {
                assert(self.qps@ =~= start.qps@);
                self.lemma_inv_fields(&start, t, m);
            }
        }
    }

    /// The first error completion of a send request without an owner on QP
    /// `index`, once.
    #[verifier::spinoff_prover]
    pub fn take_send_error(&mut self, index: usize) -> (r: Option<Wc>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        if index >= self.qps.len() {
            return None;
        }
        let ghost start = *self;
        let ghost t = self.token@;
        let ghost m = t.nic();
        proof {
            assert(start.entry_ok(index as int, t, m));
        }
        let r = match &mut self.qps[index] {
            Some(e) => e.send.take_detached_error(),
            None => None,
        };
        proof {
            if start.qps@[index as int] is Some {
                let pre = start.qps@[index as int]->0;
                let post = self.qps@[index as int]->0;
                lemma_queues_kept(pre, post, m.qps[pre.id()]);
                post.send.lemma_wqes();
                assert forall|j: int| 0 <= j < t.written(pre.id()).len()
                    implies written_backed(post, post.send.spec_wqe_committed() + j,
                        #[trigger] t.written(pre.id())[j]) by {
                    lemma_written_kept(pre, post, pre.send.spec_wqe_committed() + j, t.written(pre.id())[j]);
                }
                assert(self.entry_ok(index as int, t, m));
                self.lemma_touch_one(&start, t, m, m, index as int);
            } else {
                assert(self.qps@ =~= start.qps@);
                self.lemma_inv_fields(&start, t, m);
            }
        }
        r
    }

    /// The next QP listed with requests for the event loop to hand out,
    /// with whether a doorbell of it was refused since (both flags cleared):
    /// `None` once the list is empty, `Some(None)` for a QP removed since.
    #[verifier::spinoff_prover]
    pub fn pop_dirty(&mut self) -> (r: Option<Option<(usize, bool)>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        let ghost start = *self;
        let ghost t = self.token@;
        let ghost m = t.nic();
        let popped = self.dirty.pop();
        proof {
            self.lemma_inv_fields(&start, t, m);
        }
        let index = match popped {
            Some(i) => i as usize,
            None => return None,
        };
        if index >= self.qps.len() {
            return Some(None);
        }
        let ghost s0 = *self;
        proof {
            assert(s0.entry_ok(index as int, t, m));
        }
        let r = match &mut self.qps[index] {
            Some(e) => {
                e.dirty = false;
                let refused = e.refused;
                e.refused = false;
                Some((index, refused))
            },
            None => None,
        };
        proof {
            if s0.qps@[index as int] is Some {
                assert(self.entry_ok(index as int, t, m));
                self.lemma_touch_one(&s0, t, m, m, index as int);
            } else {
                assert(self.qps@ =~= s0.qps@);
                self.lemma_inv_fields(&s0, t, m);
            }
        }
        Some(r)
    }
}

// =============================================================================
// Removing QPs, and the worker

impl<N: Device> Hw<N> {
    /// Starts removing QP `index` (see `crate::worker`, "Removing QPs"):
    /// discards the request being written and the WQEs written since the
    /// last doorbell, gives up every request (each releases its buffers once
    /// the NIC is done with it) and, if anything is in flight, moves the QP
    /// to the error state, so that the NIC flushes it, and asks for a drain
    /// WQE. From then on the index names no QP for the caller.
    #[verifier::spinoff_prover]
    pub fn start_drain(&mut self, index: usize)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        if index >= self.qps.len() {
            return;
        }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        proof {
            assert(start.entry_ok(index as int, t0, m0));
        }
        let live = match &self.qps[index] {
            Some(e) => !e.removed,
            None => false,
        };
        if !live {
            return;
        }
        let busy = self.discard(index);
        let ghost s1 = *self;
        if busy {
            // ERR is reachable from every state (M's `modify_qp`). A device
            // that refuses it is left to the timeout: the QP is then kept.
            if self.set_error(index).is_ok() {
                let ghost s2 = *self;
                self.set_flags(index, Some(true), false);
                proof {
                    lemma_same_table_trans(self, &s2, &s1);
                }
            }
        }
        let ghost s3 = *self;
        self.set_flags(index, None, true);
        proof {
            lemma_same_table_trans(&s3, &s1, &start);
            lemma_same_table_trans(self, &s3, &start);
        }
    }

    /// Marks QP `index` as waiting to post its drain WQE (`Some(true)`: if
    /// its send queue has WQEs in flight) or not (`Some(false)`), and as
    /// removed (if `removed`).
    #[verifier::spinoff_prover]
    fn set_flags(&mut self, index: usize, drain: Option<bool>, removed: bool)
        requires
            old(self).inv(),
            old(self).has_qp(index as int),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        let ghost start = *self;
        let ghost t = self.token@;
        let ghost m = t.nic();
        proof {
            assert(start.entry_ok(index as int, t, m));
        }
        match &mut self.qps[index] {
            Some(e) => {
                match drain {
                    Some(true) => e.drain_pending = e.send.in_flight_wqes() > 0,
                    Some(false) => e.drain_pending = false,
                    None => {},
                }
                if removed {
                    e.removed = true;
                    e.dirty = false;
                    e.refused = false;
                }
            },
            None => {},
        }
        proof {
            assert(self.entry_ok(index as int, t, m));
            self.lemma_touch_one(&start, t, m, m, index as int);
        }
    }

    /// The first part of [`Hw::start_drain`]: discards what QP `index` was
    /// writing and gives up its requests; whether anything is in flight.
    #[verifier::spinoff_prover]
    fn discard(&mut self, index: usize) -> (busy: bool)
        requires
            old(self).inv(),
            old(self).has_qp(index as int),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        proof {
            assert(start.entry_ok(index as int, t0, m0));
        }
        let busy = match &mut self.qps[index] {
            Some(e) => {
                let ghost q = e.id@;
                let open = e.send.is_open();
                if open {
                    e.held = None;
                    while e.held_parts.len() > 0
                        decreases e.held_parts@.len(),
                    {
                        e.held_parts.pop();
                    }
                    let mut leases = no_leases();
                    core::mem::swap(&mut leases, &mut e.open_leases);
                    e.held_bytes = 0;
                }
                if e.started {
                    e.started = false;
                    abort(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                }
                // Nothing staged or open is posted: refused (their owners are
                // gone too).
                let ghost before_abort = e.send;
                e.send.abort();
                proof { lemma_resources_kept(before_abort, e.send); }
                let ghost s1 = e.send;
                let mut seq = e.send.head();
                while seq < e.send.posted()
                    invariant
                        e.send.inv(),
                        ring_resources_ok(e.send),
                        e.send.same_wqes(&s1),
                        e.send.same_dma(&s1),
                        e.send.same_open(&s1),
                        e.send.spec_posted() == s1.spec_posted(),
                    decreases e.send.spec_posted() - seq,
                {
                    let ghost before_detach = e.send;
                    e.send.detach(seq);
                    proof { lemma_resources_kept(before_detach, e.send); }
                    seq += 1;
                }
                let ghost r1 = e.recv;
                let mut seq = e.recv.head();
                while seq < e.recv.posted()
                    invariant
                        e.recv.inv(),
                        outcomes::recv_outcomes(e.recv, m0, q),
                        ring_resources_ok(e.recv),
                        e.recv.same_wqes(&r1),
                        e.recv.same_dma(&r1),
                        e.recv.same_open(&r1),
                        e.recv.spec_posted() == r1.spec_posted(),
                    decreases e.recv.spec_posted() - seq,
                {
                    let ghost before_detach = e.recv;
                    e.recv.detach(seq);
                    proof {
                        lemma_resources_kept(before_detach, e.recv);
                        outcomes::lemma_recv_kept(before_detach, e.recv, m0, q);
                    }
                    seq += 1;
                }
                e.send.in_flight_wqes() > 0 || e.recv.in_flight_wqes() > 0
            },
            None => false,
        };
        proof {
            let t = self.token@;
            let n = t.nic();
            let e0 = start.qps@[index as int]->0;
            let q = e0.id();
            let e = self.qps@[index as int]->0;
            if e0.started {
                start.lemma_rely(t0, m0, n);
                lemma_rely_frame(t0.share(), pinned(t0), m0, n);
            } else {
                assert(t == t0);
                start.lemma_inv_fields(&start, t0, m0);
            }
            assert(start.inv_with(t0, n));
            assert(start.entry_ok(index as int, t0, n));
            lemma_queues_kept(e0, e, n.qps[q]);
            outcomes::lemma_recv_framed(e.recv, m0, n, q);
            assert(self.entry_ok(index as int, t, n));
            self.lemma_commit_frame(&start, t0, t, n, n, index as int);
        }
        busy
    }

    /// Whether QP `index`, being removed, still has work in flight (or its
    /// drain WQE to post).
    pub fn draining(&self, index: usize) -> bool
        requires
            self.inv(),
    {
        if index >= self.qps.len() {
            return false;
        }
        proof {
            assert(self.entry_ok(index as int, self.token@, self.token@.nic()));
        }
        match &self.qps[index] {
            Some(e) => e.removed && (e.drain_pending || e.send.in_flight_wqes() > 0
                || e.recv.in_flight_wqes() > 0),
            None => false,
        }
    }
}

impl<N: Device> Hw<N> {
    /// Writes QP `index`'s drain WQE, if it waits for one and has room: a
    /// signaled SEND of nothing, without an owner. Whether it was written
    /// (it goes out at the next doorbell).
    #[verifier::spinoff_prover]
    fn stage_drain(&mut self, index: usize) -> (written: bool)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            written ==> final(self).has_qp(index as int),
    {
        if index >= self.qps.len() {
            return false;
        }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        proof {
            assert(start.entry_ok(index as int, t0, m0));
        }
        let ready = match &self.qps[index] {
            Some(e) => e.removed && e.drain_pending && matches!(e.phase, Phase::Ready) && e.send.can_post(),
            None => false,
        };
        if !ready {
            return false;
        }
        match &mut self.qps[index] {
            Some(e) => {
                let ghost q = e.id@;
                let ghost e0 = *e;
                proof {
                    e0.send.lemma_wqes();
                }
                match e.send.begin() {
                    Some(_) => {},
                    None => {
                        proof { assert(false); }
                    },
                }
                proof {
                    e.send.lemma_wqes();
                }
                let wqe = match e.send.push_wqe() {
                    Some(wqe) => wqe,
                    None => {
                        proof { assert(false); }
                        0
                    },
                };
                let implied = Wc::success(Opcode::SEND, 0);
                let mut leases = no_leases();
                leases.record_wqe(Ghost(crate::buf::WqeLoans {
                    wr_id: spec_wr_id(index as u16, false, wqe),
                    opcode: Some(safeverbs_spec::nic::Opcode::Send),
                    sges: Seq::empty(), first: 0,
                }));
                let ghost resource = leases;
                proof {
                    lemma_ledger_entry(leases, 0);
                    assert(leases.wqes()[0].sges.map_values(|s: Sge| sge_model(s)) =~= Seq::empty());
                }
                let ghost before_end = e.send;
                proof { lemma_resources_kept(e0.send, e.send); }
                let (_seq, signaled) = e.send.end(true, true, implied, leases);
                proof { lemma_resources_added(before_end, e.send, _seq as int, resource); }
                if !e.started {
                    crate::trusted::start(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                    e.started = true;
                }
                let nothing: [Sge; 0] = [];
                let none = nothing.as_slice();
                post(&mut e.hw, wr_id(index as u16, false, wqe), signaled, &WrOp::Send, none, Ghost(q), Tracked(self.token.borrow_mut()));
                e.send_tails = Ghost(e.send_tails@.insert(_seq as int,
                    send_wr(spec_wr_id(index as u16, false, wqe), signaled, WrOp::Send, none@)));
                proof {
                    assert(none@ =~= Seq::<Sge>::empty());
                    let wr = send_wr(spec_wr_id(index as u16, false, wqe), signaled, WrOp::Send, none@);
                    assert(wr.sges =~= Seq::<safeverbs_spec::nic::Sge>::empty());
                    assert(m0.send_wr_ok(m0.qps[q].attr, wr));
                    let w = e0.send.spec_wqe_posted() as int;
                    assert(e.send.dma(_seq as int, w) == Some(resource));
                    assert(ledger_has(resource, wr.wr_id, Some(wr.opcode), wr.sges));
                    assert(ring_backed(e.send, w, wr.wr_id, Some(wr.opcode), wr.sges));
                }
            },
            None => {},
        }
        proof {
            let t = self.token@;
            let e0 = start.qps@[index as int]->0;
            let q = e0.id();
            let e = self.qps@[index as int]->0;
            let attr = m0.qps[q].attr;
            let written = t.written(q);
            assert forall|j: int| 0 <= j < written.len() implies m0.send_wr_ok(attr, #[trigger] written[j]) by {
                if j < t0.written(q).len() {
                    assert(written[j] == t0.written(q)[j]);
                }
            }
            e.send.lemma_wqes();
            lemma_queues_kept(e0, e, m0.qps[q]);
            assert forall|j: int| 0 <= j < written.len() implies
                written_backed(e, e.send.spec_wqe_committed() + j, #[trigger] written[j]) by {
                let w = e.send.spec_wqe_committed() + j;
                let wr = written[j];
                if j < t0.written(q).len() {
                    assert(wr == t0.written(q)[j]);
                    assert(written_backed(e0, w, wr));
                    assert(!e0.send.spec_is_open());
                    lemma_backed_kept(e0.send, e.send, w, wr.wr_id, Some(wr.opcode), wr.sges);
                }
            }
            assert(self.entry_ok(index as int, t, m0));
            self.lemma_commit_frame(&start, t0, t, m0, m0, index as int);
        }
        true
    }

    /// Posts QP `index`'s drain WQE if it waits for one and has room (see
    /// `crate::worker`, "Removing QPs"): it completes after every WQE before
    /// it, with a flush error, and polling that completion retires them all,
    /// including unsignaled ones that succeeded before the error. A refused
    /// doorbell is tried again later.
    pub fn post_drain(&mut self, index: usize)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        let ghost start = *self;
        if self.stage_drain(index) {
            let ghost s1 = *self;
            if self.commit(index, None).is_ok() {
                let ghost s2 = *self;
                self.set_flags(index, Some(false), false);
                proof {
                    lemma_same_table_trans(self, &s2, &s1);
                }
            }
            proof {
                lemma_same_table_trans(self, &s1, &start);
            }
        }
    }

    /// After removing entry `index`'s QP from M (and the entry from the
    /// table, its share of the CQ from the count), the invariant holds
    /// again.
    #[verifier::spinoff_prover]
    proof fn lemma_remove_one(&self, start: &Self, t: NicToken, mid: Nic::State, n: Nic::State, index: int)
        requires
            start.inv_with(t, mid),
            nic_inv(n),
            share_wf(n, t.share()),
            0 <= index < start.qps@.len(),
            start.qps@[index] is Some,
            !writes(t, start.qps@[index]->0.id()),
            self.qps@.len() == start.qps@.len(),
            forall|i: int| 0 <= i < self.qps@.len() && i != index ==> #[trigger] self.qps@[i] == start.qps@[i],
            self.qps@[index] is None,
            cq_of(self.cq) == cq_of(start.cq),
            self.pd == start.pd,
            self.caps == start.caps,
            self.cq_size == start.cq_size,
            self.next_name == start.next_name,
            n.qps == mid.qps.remove(start.qps@[index]->0.id()),
            n.caps == mid.caps,
            n.mrs == mid.mrs,
            n.pds == mid.pds,
            n.retired == mid.retired,
            n.cqs[t.share().cq].cap == mid.cqs[t.share().cq].cap,
            n.cqs[t.share().cq].reserved.len() == self.reserved,
            self.reserved <= self.cq_size,
        ensures
            self.inv_with(t, n),
    {
        let q = start.qps@[index]->0.id();
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] self.entry_ok(i, t, n) by {
            if i != index {
                assert(start.entry_ok(i, t, mid));
                if let Some(e) = self.qps@[i] {
                    assert(e.id() != q);
                    lemma_ok_same(e, i as u16, mid, n, t.share(), t.pd(), t.started(e.id()), t.written(e.id()));
                }
            }
        }
        assert forall|i: int, j: int|
            #![trigger self.qps@[i], self.qps@[j]]
            0 <= i < self.qps@.len() && 0 <= j < self.qps@.len() && i != j && self.qps@[i] is Some
                && self.qps@[j] is Some implies self.qps@[i]->0.id() != self.qps@[j]->0.id() by {
            assert(self.qps@[i] == start.qps@[i] && self.qps@[j] == start.qps@[j]);
        }
        assert forall|p: QpId| #[trigger] n.qps.contains_key(p) && t.share().owns.contains(p) implies exists|i: int|
            0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == p by {
            assert(mid.qps.contains_key(p) && p != q);
            let i = choose|i: int| 0 <= i < start.qps@.len() && #[trigger] start.qps@[i] is Some && start.qps@[i]->0.id() == p;
            assert(i != index);
            assert(self.qps@[i] == start.qps@[i]);
        }
        assert forall|p: QpId| #[trigger] writes(t, p) implies exists|i: int|
            0 <= i < self.qps@.len() && #[trigger] self.qps@[i] is Some && self.qps@[i]->0.id() == p by {
            let i = choose|i: int| 0 <= i < start.qps@.len() && #[trigger] start.qps@[i] is Some && start.qps@[i]->0.id() == p;
            assert(i != index);
            assert(self.qps@[i] == start.qps@[i]);
        }
        assert forall|k: nat| k >= self.next_name@ implies !(#[trigger] n.qps.contains_key(t.name(k))) by {
            assert(!mid.qps.contains_key(t.name(k)));
        }
    }

    /// Destroys QP `index`, being removed, if neither of its rings has a WQE
    /// in flight, which is M's `destroy_qp` precondition (empty queues): the
    /// hardware QP first, then the rings, which release its buffers, then
    /// its share of the CQ. Whether it was destroyed.
    #[verifier::spinoff_prover]
    pub fn destroy(&mut self, index: usize) -> (destroyed: bool)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
    {
        if index >= self.qps.len() {
            return false;
        }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        proof {
            assert(start.entry_ok(index as int, t0, m0));
        }
        let idle = match &self.qps[index] {
            Some(e) => e.removed && !e.started && !e.send.is_open() && e.send.in_flight_wqes() == 0
                && e.recv.in_flight_wqes() == 0,
            None => false,
        };
        if !idle {
            return false;
        }
        let mut slot: Option<HwQp<N>> = None;
        core::mem::swap(&mut slot, &mut self.qps[index]);
        let entry = match slot {
            Some(entry) => entry,
            None => {
                proof { assert(false); }
                return false;
            },
        };
        let ghost q = entry.id@;
        let ghost need = entry.caps.max_send_wr + entry.caps.max_recv_wr;
        proof {
            let qp = m0.qps[q];
            entry.send.lemma_wqes();
            entry.recv.lemma_wqes();
            assert(nic_enabled(m0, Nic::Step::destroy_qp(q)));
            assert(m0.cq_wf(sh.cq));
            assert(m0.cqs[sh.cq].reserved.count(q) == m0.reserved_by(sh.cq, q));
            broadcast use vstd::multiset::group_multiset_axioms;
            assert(need <= self.reserved);
        }
        let need = entry.caps.max_send_wr + entry.caps.max_recv_wr;
        // The hardware QP first; the rest of the entry, the rings with the
        // buffers, goes when this returns, once the NIC holds none of them.
        destroy_qp(entry.hw, Ghost(q), Tracked(self.token.borrow_mut()));
        self.reserved -= need;
        proof {
            let t = self.token@;
            let n = t.nic();
            let mid = choose|mid: Nic::State| rely_reaches(sh, pinned(t0), m0, mid) && nic_step(mid, n, Nic::Step::destroy_qp(q));
            start.lemma_rely(t0, m0, mid);
            start.lemma_same_token(t0, t, mid);
            lemma_nic_step_is_next(mid, n, Nic::Step::destroy_qp(q));
            lemma_step_keeps_invariant(mid, n);
            lemma_mine_keeps_share_wf(sh, pinned(t0), mid, n, Nic::Step::destroy_qp(q));
            assert(start.entry_ok(index as int, t, mid));
            let qp = mid.qps[q];
            let cq = mid.cqs[sh.cq];
            safeverbs_spec::nic_proofs::lemma_copies(q, qp.attr.sq_depth);
            safeverbs_spec::nic_proofs::lemma_copies(q, qp.attr.rq_depth);
            broadcast use vstd::multiset::group_multiset_axioms;
            assert(mid.cq_wf(sh.cq));
            assert(cq.reserved.count(q) == mid.reserved_by(sh.cq, q));
            let c1 = cq.unreserve(q, qp.attr.sq_depth);
            assert(copies(q, qp.attr.sq_depth).subset_of(cq.reserved));
            assert(copies(q, qp.attr.rq_depth).subset_of(c1.reserved));
            assert(n.cqs[sh.cq].reserved.len() == cq.reserved.len() - qp.attr.sq_depth - qp.attr.rq_depth);
            assert(!writes(t, q));
            self.lemma_remove_one(&start, t, mid, n, index as int);
        }
        true
    }

    /// Ends the worker: destroys its CQ if no QP is left (each destroyed
    /// before: M's `destroy_cq` needs none attached). Otherwise hands the
    /// worker back: a QP the device never flushed keeps it, and the caller
    /// leaks it all (no memory the NIC may use is ever freed).
    pub fn finish(self) -> (r: core::result::Result<(), Self>)
        requires
            self.inv(),
    {
        let mut i = 0;
        while i < self.qps.len()
            invariant
                self.inv(),
                0 <= i <= self.qps@.len(),
                forall|j: int| 0 <= j < i ==> (#[trigger] self.qps@[j]) is None,
            decreases self.qps@.len() - i,
        {
            if self.qps[i].is_some() {
                return Err(self);
            }
            i += 1;
        }
        proof {
            let t = self.token@;
            let m = t.nic();
            let sh = t.share();
            assert forall|q: QpId| #[trigger] m.qps.contains_key(q) implies m.qps[q].attr.send_cq != sh.cq
                && m.qps[q].attr.recv_cq != sh.cq by {
                if m.qps[q].attr.send_cq == sh.cq || m.qps[q].attr.recv_cq == sh.cq {
                    assert(sh.owns.contains(q));
                }
            }
        }
        let Hw { cq: the_cq, token: the_token, .. } = self;
        destroy_cq(the_cq, the_token);
        Ok(())
    }
}

impl<N: Device> Hw<N> {
    /// Drops the parts gathered on QP `index` for a WQE that will not be
    /// pushed.
    #[verifier::spinoff_prover]
    pub fn drop_gathered(&mut self, index: usize)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        if index >= self.qps.len() {
            return;
        }
        let ghost start = *self;
        let ghost t = self.token@;
        let ghost m = t.nic();
        proof {
            assert(start.entry_ok(index as int, t, m));
        }
        match &mut self.qps[index] {
            Some(e) => {
                Self::give_back(&mut e.building);
            },
            None => {},
        }
        proof {
            if start.qps@[index as int] is Some {
                assert(self.entry_ok(index as int, t, m));
                self.lemma_touch_one(&start, t, m, m, index as int);
            } else {
                assert(self.qps@ =~= start.qps@);
                self.lemma_inv_fields(&start, t, m);
            }
        }
    }

    /// Waits until a new send request of at least one WQE fits on QP
    /// `index`: rings its doorbell for what is written and polls the CQ until
    /// a slot and a WQE are free.
    pub fn make_room(&mut self, index: usize) -> (r: Result<()>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
    {
        if index >= self.qps.len() || self.qps[index].is_none() {
            no_qp(index);
            return Err(Error::Invalid("no such QP"));
        }
        self.make_send_room(index)
    }
}

/// The opcode a successful completion of `op` reports.
fn opcode_of(op: &WrOp) -> (opcode: Opcode)
    ensures opcode == match *op {
        WrOp::Send | WrOp::SendImm(_) => Opcode::SEND,
        WrOp::Write(_) | WrOp::WriteImm(..) => Opcode::RDMA_WRITE,
        WrOp::Read(_) => Opcode::RDMA_READ,
        WrOp::FetchAdd { .. } => Opcode::FETCH_ADD,
        WrOp::CompareSwap { .. } => Opcode::COMP_SWAP,
    },
{
    match op {
        WrOp::Send | WrOp::SendImm(_) => Opcode::SEND,
        WrOp::Write(_) | WrOp::WriteImm(..) => Opcode::RDMA_WRITE,
        WrOp::Read(_) => Opcode::RDMA_READ,
        WrOp::FetchAdd { .. } => Opcode::FETCH_ADD,
        WrOp::CompareSwap { .. } => Opcode::COMP_SWAP,
    }
}

} // verus!
