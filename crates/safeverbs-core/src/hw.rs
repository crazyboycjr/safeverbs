//! The hardware side of an engine, verified against the NIC model M: its CQ,
//! its QPs' hardware queues and rings, and its share of M.
//!
//! A [`Hw`] holds everything of an engine that M sees: the CQ, each QP's
//! hardware handle with the rings of its two queues, and the engine's token
//! (`crate::trusted::NicToken`: M's state as the engine last saw it, and its
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
//! - **Local memory.** Every SQ/RQ WQE matches its live request
//!   interval's exact opcode and SGE ledger. Written WRs retain
//!   their guards in staged requests or the open ledger; the final held
//!   WQE retains its parts directly. Refused results never authorize DMA,
//!   even when their WQE numbers are reused. The byte lemmas expose the
//!   corresponding source/destination loan and physical lifetime guard.
//! - **Outcomes.** A receive request's result is the outcome M retired for
//!   its WQE (`outcomes`). A send request, one WQE or a batch, caches the
//!   first error M retired in its interval while it is in flight, and
//!   completes with M's aggregate of the interval: the first error in
//!   posting order, at its position in the interval, else the last WQE's
//!   success (`send_outcomes`). [`Hw::take`] hands back exactly that, with
//!   the request's own context and buffers, or says that its doorbell was
//!   refused, which M never saw. A detached request's first error is held to
//!   the same ([`Hw::take_send_error`]).
//! - **Capacity.** The CQ's entries reserved in M are exactly what the
//!   engine counts ([`Hw::reserved`]); a QP is created only if its depths fit
//!   ([`Hw::create_qp`]), and each ring keeps at most its depth of WQEs in
//!   flight, so the CQ never overruns (M's `lemma_cq_never_overflows`).
//! - **Teardown.** A QP is destroyed only when neither ring has a WQE in
//!   flight, which the coupling turns into M's empty queues, `destroy_qp`'s
//!   precondition ([`Hw::destroy`]).
//! - **State.** The engine posts work only on a QP it connected or moved to
//!   the error state, which M sees in `Rts` or `Err`.
//!
//! The unverified engine (`crate::engine`) drives a `Hw` through its public
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

use crate::buf::{BufError, Kept, Lease, Leases, Part, forget_free};
use crate::error::{Error, Result};
use crate::nic::{
    ConnectAttr, DeviceCaps, Endpoint, Nic as Device, NicCq, NicQp, QpCaps, QpType, Sge, WrOp,
};
use crate::pd::{Context, Pd};
use crate::ring::wake::WakeBatch;
use crate::ring::{Payload, Ring, Seq as RingSeq, Taken};
use crate::trusted::*;
use crate::verbs::Verbs;
use crate::wc::{Completed, CompletionError, CompletionResult, Opcode, Received, Wc};
use crate::wr_id::*;

mod completion;
mod open;
mod outcomes;
mod send_outcomes;
mod separation;

use open::write_wqe;
pub use open::{OpenRequest, OpenWqe};

verus! {

global size_of usize == 8;

/// The most QPs of one engine: their index takes the top 16 bits of a
/// `wr_id`.
pub const MAX_QPS: usize = 0x1_0000;

/// The most scatter/gather elements of one WQE (they are kept on the
/// stack).
pub const MAX_SGES: usize = 16;

/// One of a QP's two work queues.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(u32)] // No padding in a `Completion`, which moving one would copy.
pub enum Side {
    /// The send queue: SENDs, WRITEs, READs, atomics.
    Send,
    /// The receive queue.
    Recv,
}

/// A request that completed, as [`Engine::poll`](crate::engine::Engine::poll) and
/// [`Engine::take_send`](crate::engine::Engine::take_send) hand it out: an owned value, with only
/// what its operation defines.
#[verifier::reject_recursive_types(N)]
pub struct Completion<C = (), N: Device = Verbs> {
    /// The value it was posted with (its context).
    pub wr_id: C,
    /// What it did, or why it did not.
    pub result: CompletionResult,
    /// The QP's index in the engine
    /// ([`QpKey::index`](crate::engine::QpKey::index)).
    pub qp: usize,
    /// The QP's number on the device.
    pub qp_num: u32,
    /// The queue it was posted on.
    pub queue: Side,
    /// Its sequence number in that queue.
    pub seq: RingSeq,
    /// Its buffers. What [`Engine::poll`](crate::engine::Engine::poll) hands out of a request of
    /// several holds only the unique ones: frozen ones have no way back and are released as it
    /// comes back, so that the list stays with the QP for its next request of several.
    pub leases: Leases<N>,
}

/// The first error of a send request without an owner.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DetachedError {
    /// The request.
    pub seq: RingSeq,
    /// Why it did not complete successfully.
    pub error: CompletionError,
}

/// What the engine knows of a hardware QP's state: it decides which verbs
/// the engine may call (M's preconditions: work is posted only in RTS or
/// ERR, and a QP connects from RESET).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    /// Created (RESET): it may connect; it takes no work.
    Reset,
    /// Connected (RTS, or moved to ERR by the NIC since), or moved to ERR
    /// by the engine: it takes work (in ERR, to flush it).
    Ready,
    /// A connection failed part way (INIT or RTR): it neither connects
    /// again nor takes work, until it is moved to the error state.
    Broken,
}

/// A QP of an engine: its hardware handle and the rings of its queues.
#[verifier::reject_recursive_types(C)]
#[verifier::reject_recursive_types(N)]
pub struct HwQp<C, N: Device> {
    hw: N::Qp,
    send: Ring<Payload<C, N>>,
    recv: Ring<Payload<C, N>>,
    /// The number of its protection domain ([`Pd::id`]), which every post compares.
    pd_id: u64,
    /// The longest message it may send.
    max_len: u32,
    /// Its number on the device.
    qp_num: u32,
    /// What the data path checks instead of `phase` and `removed`, together: it takes work
    /// (it is `Phase::Ready`) and is not being removed.
    takes_work: bool,
    /// READ and atomics are allowed (a reliable connection).
    reliable: bool,
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
    caps: QpCaps,
    /// Its transport (erased from the data path: `reliable` says what that asks).
    #[allow(dead_code)]
    qp_type: QpType,
    /// Its protection domain, alive as long as it is.
    pd: Pd<N>,
    /// M's name of the QP.
    #[allow(dead_code)]
    id: Ghost<QpId>,
    /// What the next request being written takes, kept from the last one, so that nothing is
    /// allocated per request: where its WQEs go (empty while one has it), where the scatter/gather
    /// lists of several elements go, and a list for several buffers.
    spare_wqes: Vec<OpenWqe>,
    spare_sges: Vec<Sge>,
    spare_leases: Vec<Lease<N>>,
    /// The parts of a gather list, until it is checked.
    building: Vec<Part<N>>,
    /// The final WR written for each send request (erased).
    #[allow(dead_code)]
    send_tails: Ghost<Map<int, SendWr>>,
}

/// A receive that the data path's own way does not take: its buffer and its context.
pub type UnpostedRecv<C, N> = (Part<N>, Option<C>);

/// A request of one WQE that the data path's own way does not take: all of it.
pub struct Unposted<C, N: Device> {
    /// What it does.
    pub op: WrOp,
    /// Its buffer.
    pub part: Part<N>,
    /// The context it was given.
    pub ctx: Option<C>,
}

/// A request that could not be posted: why, and its context, if it has one, and buffers.
pub struct Refusal<C, N: Device> {
    /// What went wrong.
    pub error: Error,
    /// The context it was given.
    pub ctx: Option<C>,
    /// Its buffers.
    pub leases: Leases<N>,
}

/// What giving up a QP's requests let go of whose destructors are the program's: the `wr_id`s of
/// requests not delivered and the wakers of futures gone. They may reach back into a handle over
/// the engine (a `wr_id` may hold a QP), so whoever holds the engine behind a borrow takes this
/// and drops it after letting go.
pub struct LetGo<C> {
    /// The contexts of the requests given up.
    pub wr_ids: Vec<C>,
    /// The wakers of those requests.
    pub wakers: Vec<core::task::Waker>,
}

impl<C> LetGo<C> {
    /// Nothing yet.
    pub(crate) fn new() -> Self {
        LetGo { wr_ids: Vec::new(), wakers: Vec::new() }
    }

    /// What giving up one request let go of.
    fn keep(&mut self, ctx: Option<C>, waker: Option<core::task::Waker>) {
        match ctx {
            Some(ctx) => self.wr_ids.push(ctx),
            None => {},
        }
        match waker {
            Some(waker) => self.wakers.push(waker),
            None => {},
        }
    }
}

/// The hardware side of an engine: its CQ, its QPs and its share of M.
#[verifier::reject_recursive_types(C)]
#[verifier::reject_recursive_types(N)]
pub struct Hw<C, N: Device> {
    /// What giving up requests let go of, until it is taken.
    let_go: LetGo<C>,
    cq: N::Cq,
    /// The device, alive as long as the CQ is.
    ctx: Context<N>,
    caps: DeviceCaps,
    qps: Vec<Option<HwQp<C, N>>>,
    /// The CQEs read from the CQ and not routed yet: `polled[polled_at..polled_len]`, oldest
    /// first. The first may have retired some requests already. Here, the program may post
    /// and poll between two of them, and everything sees them in order.
    polled: [Wc; POLL_MAX],
    polled_at: usize,
    polled_len: usize,
    /// Results for the event loop may wait in the rings: a poll that kept them there
    /// ([`Hw::progress`]) or a refused doorbell put them there.
    kept: bool,
    /// Where the event loop looks next for them: by QP index, twice, a QP's send ring first.
    kept_at: usize,
    /// Panics of the wakers run on the event loop's path, until its CQEs are done.
    panics: WakeBatch,
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
pub(crate) open spec fn ring_resources_ok<C, N: Device>(r: Ring<Payload<C, N>>) -> bool {
    &&& forall|s: int| #[trigger] r.req(s) is Some ==> payload_ok(r.req(s)->0.leases, r.req(s)->0.detached)
    &&& forall|s: int| #[trigger] r.staged(s) is Some ==> payload_ok(r.staged(s)->0.leases, r.staged(s)->0.detached)
    &&& forall|s: int| #[trigger] r.refused(s) is Some ==> payload_ok(r.refused(s)->0, false)
}

/// A request's payload: its checked buffers and, exactly if it has an owner, a
/// context. So a request without an owner runs no user destructor as it retires.
pub(crate) open spec fn payload_ok<C, N: Device>(p: Payload<C, N>, detached: bool) -> bool {
    &&& request_resources_ok(p.leases)
    &&& (p.ctx is Some) == !detached
}

/// Every live WQE stays with its request and that request's buffers; the
/// context may have gone with the owner.
pub(crate) open spec fn keeps_leases<C, N: Device>(post: Ring<Payload<C, N>>, pre: Ring<Payload<C, N>>) -> bool {
    forall|s: int, w: int| pre.dma(s, w) is Some && post.spec_wqe_retired() <= w < post.spec_closed_end()
        ==> #[trigger] post.dma(s, w) is Some && post.dma(s, w)->0.leases == pre.dma(s, w)->0.leases
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

/// A WQE is retained by its live request's interval, never by a refused
/// result whose historical WQE number happens to match.
pub(crate) open spec fn ring_backed<C, N: Device>(
    r: Ring<Payload<C, N>>, w: int, id: u64,
    op: Option<safeverbs_spec::nic::Opcode>, sges: Seq<safeverbs_spec::nic::Sge>,
) -> bool {
    exists|s: int| #[trigger] r.dma(s, w) is Some
        && ledger_has(r.dma(s, w)->0.leases, id, op, sges)
}

proof fn lemma_backed_kept<C, N: Device>(
    pre: Ring<Payload<C, N>>, post: Ring<Payload<C, N>>, w: int, id: u64,
    op: Option<safeverbs_spec::nic::Opcode>, sges: Seq<safeverbs_spec::nic::Sge>,
)
    requires keeps_leases(post, pre), post.spec_wqe_retired() <= w < post.spec_closed_end(),
        ring_backed(pre, w, id, op, sges),
    ensures ring_backed(post, w, id, op, sges),
{
    let s = choose|s: int| pre.dma(s, w) is Some
        && ledger_has(pre.dma(s, w)->0.leases, id, op, sges);
    assert(post.dma(s, w)->0.leases == pre.dma(s, w)->0.leases);
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
pub(crate) proof fn lemma_ring_guards<C, N: Device>(
    r: Ring<Payload<C, N>>, w: int, id: u64,
    op: Option<safeverbs_spec::nic::Opcode>, sges: Seq<safeverbs_spec::nic::Sge>,
) -> (s: int)
    requires r.inv(), ring_resources_ok(r), ring_backed(r, w, id, op, sges),
    ensures r.dma(s, w) is Some, request_resources_ok(r.dma(s, w)->0.leases),
        r.refused(s) is None,
        (r.req(s) is Some && r.req(s)->0.phase is InFlight
            && r.req(s)->0.leases == r.dma(s, w)->0)
            || (r.staged(s) is Some && r.staged(s)->0.leases == r.dma(s, w)->0),
        forall|i: int| 0 <= i < sges.len()
            ==> retained_sge(r.dma(s, w)->0.leases, local_role(op), #[trigger] sges[i]),
{
    let s = choose|s: int| r.dma(s, w) is Some && ledger_has(r.dma(s, w)->0.leases, id, op, sges);
    r.lemma_dma(s, w);
    lemma_ledger_guards(r.dma(s, w)->0.leases, id, op, sges);
    s
}

/// A resource at a sequence keeps its identity, and its owner, across the three ring views.
pub(crate) open spec fn resource_at<C, N: Device>(r: Ring<Payload<C, N>>, s: int, l: Payload<C, N>, detached: bool) -> bool {
    ||| r.req(s) is Some && r.req(s)->0.leases == l && r.req(s)->0.detached == detached
    ||| r.staged(s) is Some && r.staged(s)->0.leases == l && r.staged(s)->0.detached == detached
    ||| r.refused(s) == Some(l) && !detached
}

proof fn lemma_resources_kept<C, N: Device>(pre: Ring<Payload<C, N>>, post: Ring<Payload<C, N>>)
    requires ring_resources_ok(pre),
        forall|s: int| #[trigger] post.req(s) is Some ==> resource_at(pre, s, post.req(s)->0.leases, post.req(s)->0.detached),
        forall|s: int| #[trigger] post.staged(s) is Some ==> resource_at(pre, s, post.staged(s)->0.leases, post.staged(s)->0.detached),
        forall|s: int| #[trigger] post.refused(s) is Some ==> resource_at(pre, s, post.refused(s)->0, false),
    ensures ring_resources_ok(post),
{
    assert forall|s: int| #[trigger] post.req(s) is Some implies payload_ok(post.req(s)->0.leases, post.req(s)->0.detached) by {}
    assert forall|s: int| #[trigger] post.staged(s) is Some implies payload_ok(post.staged(s)->0.leases, post.staged(s)->0.detached) by {}
    assert forall|s: int| #[trigger] post.refused(s) is Some implies payload_ok(post.refused(s)->0, false) by {}
}

proof fn lemma_resources_added<C, N: Device>(pre: Ring<Payload<C, N>>, post: Ring<Payload<C, N>>, seq: int, leases: Payload<C, N>, detached: bool)
    requires ring_resources_ok(pre), payload_ok(leases, detached),
        forall|s: int| #[trigger] post.req(s) is Some ==> resource_at(pre, s, post.req(s)->0.leases, post.req(s)->0.detached),
        forall|s: int| #[trigger] post.staged(s) is Some
            ==> (s == seq && post.staged(s)->0.leases == leases && post.staged(s)->0.detached == detached)
                || resource_at(pre, s, post.staged(s)->0.leases, post.staged(s)->0.detached),
        forall|s: int| #[trigger] post.refused(s) is Some ==> resource_at(pre, s, post.refused(s)->0, false),
    ensures ring_resources_ok(post),
{
    assert forall|s: int| #[trigger] post.req(s) is Some implies payload_ok(post.req(s)->0.leases, post.req(s)->0.detached) by {}
    assert forall|s: int| #[trigger] post.staged(s) is Some implies payload_ok(post.staged(s)->0.leases, post.staged(s)->0.detached) by {}
    assert forall|s: int| #[trigger] post.refused(s) is Some implies payload_ok(post.refused(s)->0, false) by {}
}

/// Completion changes outcomes and ownership, while surviving requests retain
/// the exact resource value that was staged and committed.
proof fn lemma_resources_completed<C, N: Device>(pre: Ring<Payload<C, N>>, post: Ring<Payload<C, N>>, wqe: u64, wc: Wc)
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
        implies resource_at(pre, s, post.req(s)->0.leases, post.req(s)->0.detached) by {
        if pre.spec_retired() <= s < post.spec_retired() {
            pre.lemma_retired_view(s, pre.retire_outcome(s, wqe, wc));
        }
    }
    lemma_resources_kept(pre, post);
}

/// Giving up a request's owner takes its context with it, and nothing else.
proof fn lemma_resources_given_up<C, N: Device>(pre: Ring<Payload<C, N>>, post: Ring<Payload<C, N>>, seq: int)
    requires ring_resources_ok(pre),
        forall|s: int| #[trigger] post.req(s) == if s == seq && pre.req(s) is Some {
            if pre.req(s)->0.phase is Complete {
                None
            } else {
                Some(crate::ring::ReqView { leases: pre.req(s)->0.leases.without_ctx(), ..pre.req(s)->0.detach() })
            }
        } else { pre.req(s) },
        forall|s: int| #[trigger] post.staged(s) == if s == seq && pre.staged(s) is Some {
            Some(crate::ring::ReqView { leases: pre.staged(s)->0.leases.without_ctx(), ..pre.staged(s)->0.detach() })
        } else { pre.staged(s) },
        forall|s: int| #[trigger] post.refused(s) == if s == seq { None } else { pre.refused(s) },
    ensures ring_resources_ok(post),
{
    assert forall|s: int| #[trigger] post.req(s) is Some implies payload_ok(post.req(s)->0.leases, post.req(s)->0.detached) by {
        assert(pre.req(s) is Some);
    }
    assert forall|s: int| #[trigger] post.staged(s) is Some implies payload_ok(post.staged(s)->0.leases, post.staged(s)->0.detached) by {
        assert(pre.staged(s) is Some);
    }
    assert forall|s: int| #[trigger] post.refused(s) is Some implies payload_ok(post.refused(s)->0, false) by {
        assert(pre.refused(s) is Some);
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
            ==> qp.rq[k].wr.allows_outcome(qp.rq[k].outcome->0)
    }
}

/// Every M step keeps the metadata of executed WQEs valid.
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
            implies b.rq[k].wr.allows_outcome(b.rq[k].outcome->0) by {
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

/// What the engine knows of the QP's state holds in M.
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

/// The local byte footprint of each WQE in M has a keeper.
spec fn queues_backed<C, N: Device>(e: HwQp<C, N>, qp: Qp) -> bool {
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

proof fn lemma_queues_kept<C, N: Device>(pre: HwQp<C, N>, post: HwQp<C, N>, qp: Qp)
    requires queues_backed(pre, qp),
        keeps_leases(post.send, pre.send), keeps_leases(post.recv, pre.recv),
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
spec fn written_backed<C, N: Device>(e: HwQp<C, N>, w: int, wr: SendWr) -> bool {
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

proof fn lemma_written_kept<C, N: Device>(pre: HwQp<C, N>, post: HwQp<C, N>, w: int, wr: SendWr)
    requires written_backed(pre, w, wr), pre.send.inv(), post.send.inv(),
        keeps_leases(post.send, pre.send), post.send.spec_wqe_retired() <= w,
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
            && ledger_has(pre.send.dma(s, w)->0.leases, wr.wr_id, Some(wr.opcode), wr.sges);
        pre.send.lemma_dma(s, w);
        lemma_backed_kept(pre.send, post.send, w, wr.wr_id, Some(wr.opcode), wr.sges);
    } else {
        lemma_ledger_extended(pre.open_leases, post.open_leases, wr.wr_id, Some(wr.opcode), wr.sges);
    }
}

impl<C, N: Device> HwQp<C, N> {
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
        &&& send_outcomes::send_results(self.send, m, self.id())
        &&& send_outcomes::send_implied(self.send, self.send_tails@)
        &&& send_outcomes::tail_bound(self.send, self.send_tails@, index, qp, written)
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

impl<C, N: Device> Hw<C, N> {
    /// The engine's token.
    pub closed spec fn token(&self) -> NicToken {
        self.token@
    }

    /// M's state, as the engine last saw it.
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

    /// The invariant: the engine's state is coupled to M (see the module
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

impl<C, N: Device> HwQp<C, N> {
    /// Coupling survives steps that do not touch the engine's share.
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
        send_outcomes::lemma_results_framed(self.send, m, n, q);
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

impl<C, N: Device> Hw<C, N> {
    /// The invariant survives steps that do not touch the engine's share
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

impl<C, N: Device> Hw<C, N> {
    /// A local WQE in this engine's M state can access byte `a`.
    /// Remote-rkey access belongs to the separate registration authority.
    pub(crate) closed spec fn local_access(&self, index: int, recv: bool, k: int, a: int) -> bool {
        self.has_qp(index) && {
            let e = self.qps@[index]->0;
            let qp = self.nic().qps[e.id()];
            if recv { 0 <= k < qp.rq.len() && covers(qp.rq[k].wr.sges, a) }
            else { 0 <= k < qp.sq.len() && covers(qp.sq[k].wr.sges, a) }
        }
    }

    /// The live request `s`, its retained keeper and the operation's
    /// local role back this byte of the WQE in M.
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
        &&& retained_byte(r.dma(s, w)->0.leases, dir, a)
        &&& byte_backed(r.dma(s, w)->0.leases, a)
    }

    /// Every NIC access through this engine's SQ/RQ WQEs has a
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
            lemma_covered_guard(e.recv.dma(s, w)->0.leases, local_role(None), wr.sges, a);
            s
        } else {
            let wr = qp.sq[k].wr;
            let w = qp.sq_head + k;
            let s = lemma_ring_guards(e.send, w, wr.wr_id, Some(wr.opcode), wr.sges);
            e.send.lemma_dma(s, w);
            lemma_covered_guard(e.send.dma(s, w)->0.leases, local_role(Some(wr.opcode)), wr.sges, a);
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
    /// the open request at the ring's next request sequence number.
    pub(crate) closed spec fn written_guarded(&self, index: int, j: int, a: int, s: int) -> bool {
        let e = self.qps@[index]->0;
        let wr = self.token@.written(e.id())[j];
        let w = e.send.spec_wqe_committed() + j;
        let dir = local_role(Some(wr.opcode));
        ||| e.send.dma(s, w) is Some && e.send.staged(s) is Some
            && e.send.staged(s)->0.leases == e.send.dma(s, w)->0
            && retained_byte(e.send.dma(s, w)->0.leases, dir, a)
            && byte_backed(e.send.dma(s, w)->0.leases, a)
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
            lemma_covered_guard(e.send.dma(s, w)->0.leases, local_role(Some(wr.opcode)), wr.sges, a);
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
    /// with the part retained directly by the engine.
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
// Steps of the engine's own

/// Coupling depends only on the QP's state in M, M's limits and its
/// regions.
proof fn lemma_ok_same<C, N: Device>(
    e: HwQp<C, N>,
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
        forall|w: WqeRef| w.qp == e.id() && w.seq < (if w.queue is Recv { m1.qps[e.id()].rq_head } else { m1.qps[e.id()].sq_head })
            && #[trigger] m1.retired.contains_key(w)
            ==> m2.retired.contains_key(w) && m2.retired[w] == m1.retired[w],
    ensures
        e.ok(index, m2, sh, pd, started, written),
{
    let qp = m1.qps[e.id()];
    outcomes::lemma_recv_framed(e.recv, m1, m2, e.id());
    send_outcomes::lemma_results_framed(e.send, m1, m2, e.id());
    assert forall|j: int| 0 <= j < written.len() implies m2.send_wr_ok(qp.attr, #[trigger] written[j]) by {
        assert(m1.send_wr_ok(qp.attr, written[j]));
    }
}

impl<C, N: Device> Hw<C, N> {
    /// Two tokens of the same engine with the same writing give the same
    /// invariant.
    proof fn lemma_same_token(&self, t1: NicToken, t2: NicToken, m: Nic::State)
        requires
            t2.same_engine(&t1),
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

    /// An engine's hardware side on the device of `ctx`, with a CQ of `cq_size` entries.
    pub fn new(ctx: Context<N>, cq_size: u32) -> (r: Result<Hw<C, N>>)
        ensures
            r matches Ok(hw) ==> hw.inv() && hw.spec_reserved() == 0 && hw.spec_cq_size() == cq_size,
    {
        let caps = device_caps(&ctx);
        if cq_size == 0 || cq_size > caps.max_cqe {
            return Err(Error::Invalid("a CQ needs 1..=max_cqe entries"));
        }
        let (cq, caps, token) = match create_cq(&ctx, cq_size) {
            Ok(created) => created,
            Err(e) => return Err(e),
        };
        let hw = Hw {
            let_go: LetGo::new(),
            cq,
            ctx,
            caps,
            qps: Vec::new(),
            polled: [EMPTY_WC; POLL_MAX],
            polled_at: 0,
            polled_len: 0,
            kept: false,
            kept_at: 0,
            panics: WakeBatch::new(),
            cq_size,
            reserved: 0,
            token,
            next_name: Ghost(0),
        };
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

    /// The device.
    pub fn context(&self) -> &Context<N> {
        &self.ctx
    }

}

impl<C, N: Device> Hw<C, N> {
    #[verifier::spinoff_prover]
    pub fn create_qp(&mut self, pd: &Pd<N>, domain: Option<&N::Domain>, qp_type: QpType, caps: QpCaps, max_batch: u32) -> (r: Result<usize>)
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
        {
            if self.qps[index].is_none() {
                break;
            }
            index += 1;
        }
        if index == self.qps.len() && index >= MAX_QPS {
            return Err(Error::Invalid("an engine holds at most 65536 QPs"));
        }
        let ghost q = self.token@.name(self.next_name@);
        let hw = match create_qp(
            pd,
            domain,
            &self.cq,
            qp_type,
            &caps,
            Ghost(q),
            Tracked(self.token.borrow_mut()),
        ) {
            Ok(hw) => hw,
            Err(e) => {
                return Err(e);
            },
        };
        let reliable = match qp_type {
            QpType::Rc => true,
            QpType::Uc => false,
        };
        let max_len = if dev.max_msg < 0x7fff_ffff {
            dev.max_msg
        } else {
            0x7fff_ffff
        };
        let qp_num = qp_num_of(&hw);
        // Twice the depth: requests in flight never take more than the depth
        // (each has a WQE at least); the rest is room for results not taken
        // yet.
        let entry = HwQp {
            hw,
            send: Ring::new(2 * caps.max_send_wr as usize, caps.max_send_wr, max_batch),
            recv: Ring::new(2 * caps.max_recv_wr as usize, caps.max_recv_wr, 1),
            pd_id: pd_id_of(pd),
            max_len,
            qp_num,
            takes_work: false,
            reliable,
            atomics: reliable && dev.atomics,
            phase: Phase::Reset,
            started: false,
            removed: false,
            drain_pending: false,
            refused: false,
            caps,
            qp_type,
            pd: pd.clone(),
            id: Ghost(q),
            spare_wqes: Vec::new(),
            spare_sges: Vec::new(),
            spare_leases: Vec::new(),
            building: Vec::new(),
            send_tails: Ghost(Map::empty()),
        };
        if index == self.qps.len() {
            self.qps.push(Some(entry));
        } else {
            self.qps.set(index, Some(entry));
        }
        self.reserved += need as u32;
        self.next_name = Ghost(self.next_name@ + 1);
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

/// What `modify_qp` steps of the engine's own QP `q` (to states other than
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

impl<C, N: Device> Hw<C, N> {
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
        match &mut self.qps[index] {
            Some(e) => {
                if e.removed {
                    no_qp(index);
                }
                let ghost q = e.id();
                let r = to_error(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                if r.is_ok() {
                    e.phase = Phase::Ready;
                    e.takes_work = true;
                }
                r
            },
            None => {
                no_qp(index);
                Err(Error::Invalid("no such QP"))
            },
        }
    }
}

impl<C, N: Device> Hw<C, N> {
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
                    send_outcomes::lemma_results_framed(e.send, m, n, p);
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
        let phase = match &self.qps[index] {
            Some(e) => e.phase,
            None => Phase::Broken,
        };
        if !matches!(phase, Phase::Reset) {
            return Err(Error::Invalid("a QP connects once, from RESET"));
        }
        match &mut self.qps[index] {
            Some(e) => {
                if e.removed {
                    no_qp(index);
                }
                let ghost q = e.id();
                let r = connect(&mut e.hw, attr, remote, Ghost(q), Tracked(self.token.borrow_mut()));
                e.phase = if r.is_ok() {
                    Phase::Ready
                } else {
                    Phase::Broken
                };
                e.takes_work = r.is_ok();
                r
            },
            None => {
                no_qp(index);
                Err(Error::Invalid("no such QP"))
            },
        }
    }
}

impl<C, N: Device> Hw<C, N> {
    #[inline(always)]
    #[verifier::spinoff_prover]
    pub fn post_recv(&mut self, index: usize, part: Part<N>, ctx: Option<C>) -> (r: core::result::Result<core::result::Result<RingSeq, Refusal<C, N>>, UnpostedRecv<C, N>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            r matches Ok(seq) ==> old(self).admission_separated_from(part)
                && final(self).recv_holds_input(index, seq, part),
            r matches Err((error, back)) ==> error.buf == part && back == ctx,
    {
        if index < self.qps.len() {
            match &mut self.qps[index] {
                Some(e) => {
                    // One comparison each: the QP takes work, the buffer is of its protection
                    // domain and may be written, the queue has room.
                    #[allow(clippy::collapsible_match)] // Explicit for Verus.
                    if e.takes_work && part_pd_of(&part) == e.pd_id && part_writable_of(&part) && e.recv.can_post() {
                        let ghost q = e.id();
                        // The buffer's region is registered (its lease says so).
                        registered(&part, &e.pd, Tracked(self.token.borrow_mut()));
                        let sge = part_sge_of(&part);
                        let wqe = e.recv.next_wqe();
                        let id = wr_id(index as u16, true, wqe);
                        let sges = [sge];
                        // The slot first: from then on it keeps the memory alive.
                        let mut leases = Leases::one(part, Ghost(safeverbs_spec::abs::Dir::In));
                        leases.record_wqe(Ghost(crate::buf::WqeLoans {
                            wr_id: id, opcode: None, sges: sges@, first: 0,
                        }));
                        let detached = ctx.is_none();
                        let implied = Ok(Completed::Received(Received::Message { len: sge.length, imm: None }));
                        e.recv.fill(1, detached, implied, Payload { ctx, leases });
                        return match post_recv(&mut e.hw, id, &sges, Ghost(q), Tracked(self.token.borrow_mut())) {
                            Ok(()) => {
                                let seq = e.recv.stage_filled(1, true);
                                e.recv.commit();
                                Ok(Ok(seq))
                            },
                            Err(error) => Ok(Err(not_posted(&mut e.recv, error))),
                        };
                    }
                },
                None => {},
            }
        }
        Err((part, ctx))
    }

    /// Why a receive is refused, on QP `index`: it takes no work (`Error::NotConnected`), the
    /// part is of another protection domain or frozen, or the receive queue is full.
    #[cold]
    #[inline(never)]
    pub fn recv_refused(&mut self, index: usize, part: Part<N>, ctx: Option<C>) -> (r: Refusal<C, N>)
    {
        if index >= self.qps.len() {
            no_qp(index);
            return Refusal { error: Error::Invalid("no such QP"), ctx, leases: Leases::one(part, Ghost(safeverbs_spec::abs::Dir::Out)) };
        }
        let error = match &self.qps[index] {
            Some(e) => {
                if e.removed {
                    no_qp(index);
                }
                if !matches!(e.phase, Phase::Ready) {
                    Error::NotConnected
                } else if part_pd_of(&part) != e.pd_id {
                    Error::PdMismatch
                } else if !part_writable_of(&part) {
                    Error::Invalid("receive into a frozen buffer")
                } else if e.recv.is_clogged() {
                    Error::Clogged
                } else {
                    Error::QueueFull
                }
            },
            None => {
                no_qp(index);
                Error::Invalid("no such QP")
            },
        };
        Refusal { error, ctx, leases: Leases::one(part, Ghost(safeverbs_spec::abs::Dir::Out)) }
    }
}

// =============================================================================
// Completions

/// Completions [`Hw::progress`] takes from the CQ per poll.
pub const POLL_BATCH: usize = 16;

/// The most CQEs one event loop's poll reads.
pub const POLL_MAX: usize = 32;

/// A placeholder completion.
pub(crate) const EMPTY_WC: Wc = Wc {
    wr_id: 0,
    status: crate::wc::Status(0),
    opcode: Opcode(0),
    byte_len: 0,
    imm: None,
    vendor_err: 0,
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

impl<C, N: Device> Hw<C, N> {
    /// Routes one completion to its QP's ring: its `wr_id` names the QP's index, the queue and the
    /// WQE, which the ring retires (with every earlier request it ends).
    #[inline(always)]
    #[verifier::spinoff_prover]
    #[allow(clippy::ptr_arg)] // Vstd specifies indexing a `Vec`.
    fn dispatch(qps: &mut Vec<Option<HwQp<C, N>>>, wc: &Wc, wakes: &mut WakeBatch, Ghost(t): Ghost<NicToken>, Ghost(before): Ghost<Nic::State>, Ghost(after): Ghost<Nic::State>)
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
        let index = index_of(wc.wr_id);
        if index >= qps.len() {
            return;
        }
        match &mut qps[index] {
            Some(qp_entry) => qp_entry.complete_cqe(wc, wakes, Ghost(index as int), Ghost(t), Ghost(before), Ghost(after)),
            None => {},
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

    /// Polls the CQ once, retiring what completed in the rings of its QPs (and waking the wakers
    /// registered there), where the results wait for their owners; what the event loop read and
    /// did not route yet goes first, in its order. Returns the number of completions.
    #[verifier::spinoff_prover]
    pub fn progress(&mut self) -> (n: usize)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        let mut wakes = WakeBatch::new();
        let mut n = self.polled_len - self.polled_at;
        while self.polled_at < self.polled_len
        {
            Self::dispatch(&mut self.qps, &self.polled[self.polled_at], &mut wakes, Ghost(t0), Ghost(befores[i as int]), Ghost(afters[i as int]));
            self.polled_at += 1;
        }
        self.polled_at = 0;
        self.polled_len = 0;
        let read = poll(&mut self.cq, &mut self.polled, POLL_BATCH, Tracked(self.token.borrow_mut()));
        let mut i = 0;
        while i < read
        {
            Self::dispatch(&mut self.qps, &self.polled[i], &mut wakes, Ghost(t0), Ghost(befores[i as int]), Ghost(afters[i as int]));
            i += 1;
        }
        n += read;
        // Results wait in the rings, if the event loop wants them.
        self.kept = true;
        self.kept_at = 0;
        wakes.finish();
        n
    }

    /// The first half of the event loop's poll: reads at most `n` CQEs ([`POLL_MAX`] at most) and
    /// updates the CQ's consumer index, unless some read before are not routed yet. Then
    /// [`Hw::next_completion`] hands out what they retire, while the program may post anything:
    /// the CQ's capacity is safe, since the CQEs have left it. Returns how many it read.
    #[inline(always)]
    pub fn read_completions(&mut self, n: usize) -> (read: usize)
    {
        if self.polled_at < self.polled_len || self.kept {
            return 0;
        }
        let limit = if n < POLL_MAX { n } else { POLL_MAX };
        let read = poll(&mut self.cq, &mut self.polled, limit, Tracked(self.token.borrow_mut()));
        self.polled_at = 0;
        self.polled_len = read;
        read
    }

    /// The second half of the event loop's poll: the next request that completed with an owner,
    /// in each queue's order, from the results waiting in the rings and then, in one pass, from
    /// the CQEs read: each is routed, its ring walked up to it, and what that retires handed out.
    /// `None` when nothing is left. A panic of a waker registered on the requests resumes then.
    #[inline(always)]
    pub fn next_completion(&mut self) -> (r: Option<Completion<C, N>>)
    {
        while self.polled_at < self.polled_len
        {
            let at = self.polled_at;
            let wc = &self.polled[at];
            let id = wc.wr_id;
            let index = index_of(id);
            let e = if index < self.qps.len() {
                match &mut self.qps[index] {
                    Some(e) => e,
                    None => {
                        self.polled_at = at + 1;
                        continue;
                    },
                }
            } else {
                self.polled_at = at + 1;
                continue;
            };
            let recv = is_recv(id);
            let ring = if recv { &mut e.recv } else { &mut e.send };
            let wqe = wqe_of(id, ring.wqe_retired());
            if !ring.wqe_in_flight(wqe) {
                self.polled_at = at + 1;
                continue;
            }
            let seq = ring.retired();
            let qp_num = e.qp_num;
            match ring.retire_oldest(recv, wqe, wc) {
                Some(slot) => {
                    if slot.end_is(wqe + 1) {
                        self.polled_at = at + 1;
                    }
                    let result = slot.outcome;
                    match slot.take_leases() {
                        Some(Payload { ctx: Some(wr_id), leases }) => {
                            let queue = if recv { Side::Recv } else { Side::Send };
                            return Some(Completion { wr_id, result, qp: index, qp_num, queue, seq, leases });
                        },
                        // A request with an owner holds a context, so nothing is lost here.
                        other => forget_free(other, Ghost(false)),
                    }
                },
                None => {
                    let other = self.next_general(at, index, recv, wqe);
                    if other.is_some() {
                        return other;
                    }
                },
            }
        }
        if self.kept {
            let kept = self.next_kept();
            if kept.is_some() {
                return kept;
            }
        }
        if self.panics.pending() {
            self.resume_panics();
        }
        None
    }

    /// What [`Hw::next_completion`] does with a CQE out of the common: one step of its walk, on the
    /// oldest request of its queue, which retires it if it ends at or before the CQE's WQE, and
    /// hands it out if it has an owner. A CQE that ends inside it is spent.
    #[inline(never)]
    fn next_general(&mut self, at: usize, index: usize, recv: bool, wqe: u64) -> (r: Option<Completion<C, N>>)
    {
        let wc = &self.polled[at];
        let e = match &mut self.qps[index] {
            Some(e) => e,
            None => return None,
        };
        let (ring, queue) = if recv { (&mut e.recv, Side::Recv) } else { (&mut e.send, Side::Send) };
        let seq = ring.retired();
        match ring.retire_next(recv, wqe, wc, &mut self.panics) {
            Some(ended) => {
                if ended {
                    self.polled_at = at + 1;
                }
            },
            None => {
                self.polled_at = at + 1;
                return None;
            },
        }
        match ring.take_retired(seq) {
            Some(Taken { outcome, leases: Payload { ctx: Some(wr_id), mut leases } }) => {
                if leases.is_many() {
                    leases.hand_back_many(&mut e.spare_leases);
                }
                Some(Completion { wr_id, result: outcome, qp: index, qp_num: e.qp_num, queue, seq, leases })
            },
            // Nothing, if it had no owner; a request with an owner holds a context.
            _ => None,
        }
    }

    /// Resumes the first panic of a waker the event loop's path ran.
    #[cold]
    #[inline(never)]
    fn resume_panics(&mut self)
    {
        let mut panics = WakeBatch::new();
        core::mem::swap(&mut panics, &mut self.panics);
        panics.finish();
    }

    /// The next result waiting in the rings for the event loop: of a poll that kept them there,
    /// or of a refused doorbell, which comes at once, ahead of the requests before it.
    #[cold]
    #[inline(never)]
    fn next_kept(&mut self) -> (r: Option<Completion<C, N>>)
    {
        while self.kept_at < 2 * self.qps.len()
        {
            let index = self.kept_at / 2;
            let side = if self.kept_at & 1 == 0 { Side::Send } else { Side::Recv };
            let range = match &self.qps[index] {
                Some(e) => match side {
                    Side::Send => (e.send.head(), if e.refused { e.send.committed() } else { e.send.retired() }),
                    Side::Recv => (e.recv.head(), e.recv.retired()),
                },
                None => (0, 0),
            };
            let mut seq = range.0;
            while seq < range.1
            {
                let done = self.take(index, side, seq);
                if done.is_some() {
                    return done;
                }
                seq += 1;
            }
            if matches!(side, Side::Send) {
                match &mut self.qps[index] {
                    Some(e) => e.refused = false,
                    None => {},
                }
            }
            self.kept_at += 1;
        }
        self.kept_at = 0;
        self.kept = false;
        None
    }
}

// =============================================================================
// Doorbells

impl<C, N: Device> Hw<C, N> {
    #[inline]
    #[verifier::spinoff_prover]
    pub fn commit(&mut self, index: usize, keep: Option<RingSeq>) -> (r: core::result::Result<(), (Error, Option<Payload<C, N>>)>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            r matches Err((_, Some(kept))) ==> kept.leases.inv() && kept.leases.fully_recorded(),
    {
        if index >= self.qps.len() {
            no_qp(index);
            return Ok(());
        }
        match &mut self.qps[index] {
            Some(e) => {
                if !e.started {
                    return Ok(());
                }
                e.started = false;
                let ghost q = e.id();
                let refused = if matches!(e.phase, Phase::Ready) {
                    match doorbell(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut())) {
                        Ok(()) => {
                            e.send.commit();
                            return Ok(());
                        },
                        Err(err) => err,
                    }
                } else {
                    abort(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                    Error::NotConnected
                };
                // Only a refused doorbell wakes anyone, its staged requests' wakers;
                // a waker's panic resumes once the bookkeeping is done.
                let mut wakes = WakeBatch::new();
                let kept = match keep {
                    Some(seq) => e.send.abort_keeping_with_wakes(seq, &mut wakes),
                    None => {
                        e.send.abort_with_wakes(&mut wakes);
                        None
                    },
                };
                e.refused = true;
                self.kept = true;
                self.kept_at = 0;
                wakes.finish();
                Err((refused, kept))
            },
            None => {
                no_qp(index);
                Ok(())
            },
        }
    }

    /// One WQE as a request of its own, on the data path's own way: check, write the WQE, ring the
    /// doorbell if `ring`, write the slot. `signal` asks for a completion (the ring also asks when
    /// it must). Without a `ctx`, the request has no owner. If it fails, nothing of it is posted,
    /// and its `ctx` and buffers come back.
    ///
    /// Whatever is out of the common hands everything back, untouched, as [`Unposted`]: the QP
    /// takes no work, a request is being gathered, no room, a check fails, an atomic.
    /// [`Hw::post_one_slow`] sorts all that out, as a request of the general kind.
    #[inline(always)]
    #[verifier::spinoff_prover]
    pub fn post_one(&mut self, index: usize, op: WrOp, part: Part<N>, ctx: Option<C>, signal: bool, ring: bool) -> (r: core::result::Result<core::result::Result<RingSeq, Refusal<C, N>>, Unposted<C, N>>)
    {
        if index < self.qps.len() {
            match &mut self.qps[index] {
                Some(e) => {
                    let sge = part_sge_of(&part);
                    // What the operation asks beyond a length that fits, one comparison each.
                    let fits = match op {
                        WrOp::Send | WrOp::SendImm(_) => true,
                        WrOp::Write(r) | WrOp::WriteImm(r, _) => sge.length as u64 <= r.len,
                        WrOp::Read(r) => e.reliable && part_writable_of(&part) && sge.length as u64 <= r.len,
                        WrOp::FetchAdd { .. } | WrOp::CompareSwap { .. } => false,
                    };
                    if fits && e.takes_work && !(ring && e.started) && part_pd_of(&part) == e.pd_id && sge.length <= e.max_len && e.send.can_post() {
                        let ghost q = e.id();
                        let implied = implied_outcome(&op, sge.length);
                        // Its region is registered (its lease says so).
                        registered(&part, &e.pd, Tracked(self.token.borrow_mut()));
                        let id = wr_id(index as u16, false, e.send.next_wqe());
                        let signaled = signal || e.send.must_signal(1);
                        let sges = [sge];
                        // The slot first: from then on it keeps the memory alive.
                        let mut leases = Leases::one(part, Ghost(if op_writes_local(op) { safeverbs_spec::abs::Dir::In } else { safeverbs_spec::abs::Dir::Out }));
                        leases.record_wqe(Ghost(crate::buf::WqeLoans {
                            wr_id: id, opcode: Some(opcode_model(op)), sges: sges@, first: 0,
                        }));
                        let detached = ctx.is_none();
                        e.send.fill(1, detached, implied, Payload { ctx, leases });
                        if ring || !e.started {
                            start(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                        }
                        post(&mut e.hw, id, signaled, &op, &sges, Ghost(q), Tracked(self.token.borrow_mut()));
                        if ring {
                            match doorbell(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut())) {
                                Ok(()) => {
                                    let seq = e.send.stage_filled(1, signaled);
                                    e.send.commit();
                                    return Ok(Ok(seq));
                                },
                                Err(error) => return Ok(Err(not_posted(&mut e.send, error))),
                            }
                        }
                        e.started = true;
                        return Ok(Ok(e.send.stage_filled(1, signaled)));
                    }
                },
                None => {},
            }
        }
        Err(Unposted { op, part, ctx })
    }

    /// [`Hw::post_one`], out of the common: the request of one WQE of the general kind, which
    /// waits for room if it must (this polls the CQ).
    #[cold]
    #[inline(never)]
    pub fn post_one_slow(&mut self, index: usize, unposted: Unposted<C, N>, signal: bool, ring: bool) -> (r: core::result::Result<RingSeq, Refusal<C, N>>)
    {
        let op = unposted.op;
        let part = unposted.part;
        let ctx = unposted.ctx;
        let mut open = self.open_request(index);
        match self.push_other(index, &mut open, op, part) {
            Ok(()) => {},
            Err(e) => {
                self.give_back(index, &mut open);
                return Err(Refusal { error: e.error, ctx, leases: Leases::one(e.buf, Ghost(safeverbs_spec::abs::Dir::Out)) });
            },
        }
        match self.make_send_room(index, 1) {
            Ok(()) => {},
            Err(error) => {
                let leases = open.take_leases();
                self.give_back(index, &mut open);
                return Err(Refusal { error, ctx, leases });
            },
        }
        let closed = self.close(index, &mut open, signal, ctx);
        let seq = match closed {
            Ok(seq) => seq,
            Err(ctx) => {
                let leases = open.take_leases();
                self.give_back(index, &mut open);
                return Err(Refusal { error: Error::QueueFull, ctx, leases });
            },
        };
        self.give_back(index, &mut open);
        if ring {
            self.commit_own(index, seq)
        } else {
            Ok(seq)
        }
    }

    /// Rings QP `index`'s doorbell for the request `seq` just staged and those before it. If the
    /// device refuses, it hands back that request's context and buffers.
    #[inline(never)]
    pub fn commit_own(&mut self, index: usize, seq: RingSeq) -> (r: core::result::Result<RingSeq, Refusal<C, N>>)
    {
        match self.commit(index, Some(seq)) {
            Ok(()) => Ok(seq),
            Err((error, kept)) => Err(match kept {
                Some(payload) => Refusal { error, ctx: payload.ctx, leases: payload.leases },
                None => Refusal { error, ctx: None, leases: Leases::new() },
            }),
        }
    }
}

impl<C, N: Device> Hw<C, N> {
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
            t.same_engine(&t0),
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

impl<C, N: Device> Hw<C, N> {
    /// Closes `open` on QP `index`: its signaling is known now, so its WQEs are written, the last
    /// signaled if `signal` or if the ring says so, and it is staged in the ring with `ctx` for its
    /// owner; without one if nobody will take it back. Returns its sequence number; if it has no
    /// WQE, or the queue no room for them, `ctx` back. It goes out at the next doorbell.
    #[verifier::spinoff_prover]
    pub fn close(&mut self, index: usize, open: &mut OpenRequest<N>, signal: bool, ctx: Option<C>) -> (r: core::result::Result<RingSeq, Option<C>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            r matches Err(back) ==> back == ctx,
    {
        let n = open.n;
        if n == 0 || index >= self.qps.len() {
            return Err(ctx);
        }
        match &mut self.qps[index] {
            Some(e) => {
                if !e.send.can_post() || n as u64 > e.send.wqe_room() as u64 || !e.takes(open) {
                    return Err(ctx);
                }
                let ghost q = e.id();
                // Its regions are registered (its leases say so).
                open.keeps_registered(&e.pd, Tracked(self.token.borrow_mut()));
                let signaled = signal || e.send.must_signal(n as u64);
                if !e.started {
                    start(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                    e.started = true;
                }
                let first = e.send.next_wqe();
                // Every WQE but the last, which is signaled if the request is.
                let mut i = 0;
                while i + 1 < n
                {
                    let sges = open.sges_of(i);
                    post(&mut e.hw, wr_id(index as u16, false, first + i as u64), false, open.op_of(i), sges, Ghost(q), Tracked(self.token.borrow_mut()));
                    i += 1;
                }
                let sges = open.sges_of(i);
                post(&mut e.hw, wr_id(index as u16, false, first + i as u64), signaled, open.op_of(i), sges, Ghost(q), Tracked(self.token.borrow_mut()));
                let last = &open.wqes[n - 1];
                let bytes = if last.sges == 1 { last.sge.length } else { open.last_gathered_bytes() };
                let implied = implied_outcome(&last.op, bytes);
                let leases = open.take_leases_with(&mut e.spare_leases, Ghost(Seq::empty()));
                let many = leases.is_many();
                let detached = ctx.is_none();
                let seq = e.send.stage(n as u64, signaled, detached, implied, Payload { ctx, leases });
                if many {
                    e.send.take_general_path(seq);
                }
                open.gathered.clear();
                Ok(seq)
            },
            None => Err(ctx),
        }
    }

    /// A request to write on QP `index`, with what the QP keeps for it. A QP that takes no work
    /// has its WQEs say so one by one: they are collected too, and the doorbell is refused.
    #[inline(always)]
    pub fn open_request(&mut self, index: usize) -> OpenRequest<N> {
        if index < self.qps.len() {
            match &mut self.qps[index] {
                Some(e) if e.spare_wqes.len() > 0 => {
                    let mut wqes = Vec::new();
                    core::mem::swap(&mut wqes, &mut e.spare_wqes);
                    return OpenRequest {
                        wqes,
                        n: 0,
                        limit: e.open_limit(),
                        max_len: e.max_len,
                        pd_id: e.pd_id,
                        kept: Kept::new(),
                        gathered: Vec::new(),
                        reads: false,
                        max_sges: 1,
                        ledger: Ghost(Seq::empty()),
                    };
                },
                _ => {},
            }
        }
        self.open_request_other(index)
    }

    /// [`Hw::open_request`] on a QP whose storage is out, or not made yet.
    #[cold]
    #[inline(never)]
    fn open_request_other(&mut self, index: usize) -> OpenRequest<N> {
        if index < self.qps.len() {
            match &mut self.qps[index] {
                Some(e) => {
                    let wqes = vec![open::EMPTY_WQE; e.send.max_wqes() as usize];
                    return OpenRequest {
                        wqes,
                        n: 0,
                        limit: e.open_limit(),
                        max_len: e.max_len,
                        pd_id: e.pd_id,
                        kept: Kept::new(),
                        gathered: Vec::new(),
                        reads: false,
                        max_sges: 1,
                        ledger: Ghost(Seq::empty()),
                    };
                },
                None => {},
            }
        }
        OpenRequest::closed()
    }

    /// What `open` keeps from QP `index` goes back there for the next request, if it has no WQE:
    /// the WQEs it has stay where they are, to go out or to be dropped with it.
    #[inline(always)]
    pub fn give_back(&mut self, index: usize, open: &mut OpenRequest<N>) {
        if open.n == 0 && index < self.qps.len() {
            match &mut self.qps[index] {
                Some(e) => {
                    if e.spare_wqes.len() == 0 && open.wqes.len() == e.send.max_wqes() as usize {
                        core::mem::swap(&mut e.spare_wqes, &mut open.wqes);
                    }
                    open.limit = 0;
                    if open.gathered.capacity() > 0 {
                        core::mem::swap(&mut e.spare_sges, &mut open.gathered);
                    }
                },
                None => {},
            }
        }
    }

    /// [`Hw::close`], first waiting for room (this polls the CQ).
    pub fn close_waiting(&mut self, index: usize, open: &mut OpenRequest<N>, signal: bool, ctx: Option<C>) -> (r: core::result::Result<RingSeq, Option<C>>) {
        if open.n > 0 && !self.open_fits(index, open) && self.open_belongs(index, open) {
            let _ = self.make_send_room(index, open.n as u32);
        }
        self.close(index, open, signal, ctx)
    }

    /// `open` as a request of its own: closed, signaled if `signal`, with `ctx` to come back with
    /// its completion, and the doorbell; what it keeps from the QP goes back. If the queue has no
    /// room it waits for some (this polls the CQ). If that or the doorbell fails, its context and
    /// its buffers come back.
    #[inline]
    pub fn submit_open(&mut self, index: usize, open: &mut OpenRequest<N>, signal: bool, ctx: C) -> (r: core::result::Result<RingSeq, Refusal<C, N>>) {
        match self.close(index, open, signal, Some(ctx)) {
            Ok(seq) => {
                self.give_back(index, open);
                self.commit_own(index, seq)
            },
            Err(ctx) => self.submit_open_other(index, open, signal, ctx),
        }
    }

    /// [`Hw::submit_open`] out of the common: no WQE, or no room.
    #[cold]
    #[inline(never)]
    fn submit_open_other(&mut self, index: usize, open: &mut OpenRequest<N>, signal: bool, ctx: Option<C>) -> (r: core::result::Result<RingSeq, Refusal<C, N>>) {
        if open.is_empty() {
            self.give_back(index, open);
            return Err(Refusal { error: Error::Invalid("a request without a WQE"), ctx, leases: Leases::new() });
        }
        if !self.open_belongs(index, open) {
            let leases = open.take_leases();
            return Err(Refusal { error: Error::Invalid("a request written for another QP"), ctx, leases });
        }
        match self.make_send_room(index, open.n as u32) {
            Ok(()) => {},
            Err(error) => {
                let leases = open.take_leases();
                self.give_back(index, open);
                return Err(Refusal { error, ctx, leases });
            },
        }
        match self.close(index, open, signal, ctx) {
            Ok(seq) => {
                self.give_back(index, open);
                self.commit_own(index, seq)
            },
            Err(ctx) => {
                let leases = open.take_leases();
                self.give_back(index, open);
                Err(Refusal { error: Error::QueueFull, ctx, leases })
            },
        }
    }

    /// What an owner holds of a completed request of several buffers: frozen ones are released, and
    /// the list goes back to QP `index` for the next such request, unless several unique buffers
    /// have to leave in it.
    #[cold]
    #[inline(never)]
    pub fn hand_back_leases(&mut self, index: usize, leases: &mut Leases<N>) {
        if index < self.qps.len() {
            match &mut self.qps[index] {
                Some(e) => leases.hand_back_many(&mut e.spare_leases),
                None => {},
            }
        }
    }

    /// Takes what giving up requests let go of.
    pub fn take_let_go(&mut self) -> (let_go: LetGo<C>) {
        let mut let_go = LetGo::new();
        core::mem::swap(&mut let_go, &mut self.let_go);
        let_go
    }

    /// Whether `open` can be closed now on QP `index`: its WQEs fit in the queue.
    #[inline(always)]
    pub fn open_fits(&self, index: usize, open: &OpenRequest<N>) -> (fits: bool) {
        if index < self.qps.len() {
            match &self.qps[index] {
                Some(e) => e.send.can_post() && open.n as u64 <= e.send.wqe_room() as u64,
                None => false,
            }
        } else {
            false
        }
    }

    /// Whether QP `index` takes what `open` was written for.
    fn open_belongs(&self, index: usize, open: &OpenRequest<N>) -> (belongs: bool) {
        if index < self.qps.len() {
            match &self.qps[index] {
                Some(e) => e.takes(open),
                None => false,
            }
        } else {
            false
        }
    }
}

impl<C, N: Device> HwQp<C, N> {
    /// Whether it takes what `open` was written for, which is a value of its own: of its protection
    /// domain, as long as its messages, in as many WQEs as a request of it may have. Anything else
    /// a WQE asks of a QP is out of the common.
    #[inline(always)]
    fn takes(&self, open: &OpenRequest<N>) -> (takes: bool) {
        open.pd_id == self.pd_id && open.max_len <= self.max_len && open.n <= self.send.max_wqes() as usize
            && (!(open.reads || open.max_sges > 1) || self.takes_other(open))
    }

    /// [`HwQp::takes`] of WQEs that the NIC reads the answer of, or of several elements.
    #[cold]
    #[inline(never)]
    fn takes_other(&self, open: &OpenRequest<N>) -> (takes: bool) {
        (!open.reads || self.reliable)
            && open.max_sges <= self.caps.max_send_sge && open.max_sges <= self.caps.max_recv_sge
    }

    /// How many WQEs a request being written may take without asking: as many as a request may
    /// have; none while it takes no work. The queue's room is looked at when it goes out, since a
    /// request is written with nothing held.
    #[inline(always)]
    fn open_limit(&self) -> (limit: usize) {
        if self.takes_work {
            self.send.max_wqes() as usize
        } else {
            0
        }
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
proof fn lemma_same_table_trans<C, N: Device>(a: &Hw<C, N>, b: &Hw<C, N>, c: &Hw<C, N>)
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

impl<C, N: Device> Hw<C, N> {
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
    fn make_send_room(&mut self, index: usize, wqes: u32) -> (r: Result<()>)
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
                Some(e) => (e.send.can_post() && wqes <= e.send.wqe_room(), e.send.is_clogged()),
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
        match &mut self.qps[index] {
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
        }
    }

    /// Checks a WQE doing `op` on `parts` against QP `index`: what the QP may do, the parts'
    /// domain and uniqueness, the lengths.
    #[inline]
    fn check_wqe(e: &HwQp<C, N>, op: &WrOp, parts: &[Part<N>]) -> (r: Result<u64>)
    {
        let n = parts.len();
        let max_sge = if e.caps.max_send_sge <= e.caps.max_recv_sge {
            e.caps.max_send_sge
        } else {
            e.caps.max_recv_sge
        };
        // What the QP may do: READ and atomics on reliable connections only,
        // atomics where the device has them.
        if n == 0 || n > max_sge as usize {
            return Err(Error::TooManySges { got: n, max: max_sge as usize });
        }
        if matches!(op, WrOp::Read(_)) && !e.reliable {
            return Err(Error::Unsupported("RDMA READ on this QP"));
        }
        let atomic = matches!(op, WrOp::FetchAdd { .. }) || matches!(op, WrOp::CompareSwap { .. });
        if atomic && !e.atomics {
            return Err(Error::Unsupported("atomics on this QP"));
        }
        let writes_local = matches!(op, WrOp::Read(_)) || atomic;
        // The scatter/gather list's owners and its length.
        let mut total: u64 = 0;
        let mut i = 0;
        while i < n
        {
            if part_pd_of(&parts[i]) != e.pd_id {
                return Err(Error::PdMismatch);
            }
            if writes_local && !part_writable_of(&parts[i]) {
                return Err(Error::Invalid("the NIC would write a frozen buffer"));
            }
            total += part_sge_of(&parts[i]).length as u64;
            if total > e.max_len as u64 {
                return Err(Error::TooLong { len: total, max: e.max_len as u64 });
            }
            i += 1;
        }
        match *op {
            WrOp::Write(r) | WrOp::WriteImm(r, _) | WrOp::Read(r) => if total > r.len {
                Err(Error::TooLong { len: total, max: r.len })
            } else {
                Ok(total)
            },
            WrOp::FetchAdd { remote, .. } | WrOp::CompareSwap { remote, .. } => if total != 8 || n != 1 {
                Err(Error::Invalid("an atomic needs one local buffer of 8 bytes"))
            } else if remote.len < 8 || remote.addr % 8 != 0 {
                Err(Error::Invalid("an atomic needs 8 aligned remote bytes"))
            } else {
                Ok(total)
            },
            WrOp::Send | WrOp::SendImm(_) => Ok(total),
        }
    }

    /// Makes room for one more WQE in `open`: a request that is full goes out as a request of its
    /// own (signaled, without an owner), once it fits (this polls the CQ).
    #[inline]
    fn open_room(&mut self, index: usize, open: &mut OpenRequest<N>) -> (r: Result<()>)
    {
        if !self.open_belongs(index, open) {
            return Err(Error::Invalid("a request written for another QP"));
        }
        let k = open.n as u32;
        let full = match &self.qps[index] {
            Some(e) => k >= e.send.max_wqes(),
            None => false,
        };
        if full {
            match self.make_send_room(index, k) {
                Ok(()) => {},
                Err(error) => return Err(error),
            }
            let _ = self.close(index, open, true, None);
        }
        if open.n < open.wqes.len() {
            Ok(())
        } else {
            Err(Error::Invalid("a request that takes no WQE"))
        }
    }

    /// Adds a WQE doing `op` on `part` to `open`, whatever it takes: its QP is asked what
    /// [`OpenRequest::try_push`] could not tell, and a request that is full goes out. On error
    /// nothing is added and the part is handed back.
    #[verifier::spinoff_prover]
    #[inline(never)]
    pub fn push_other(&mut self, index: usize, open: &mut OpenRequest<N>, op: WrOp, part: Part<N>) -> (r: core::result::Result<(), BufError<Part<N>>>)
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
        let checked = match &self.qps[index] {
            Some(e) => {
                if e.removed {
                    no_qp(index);
                }
                Self::check_wqe(e, &op, core::slice::from_ref(&part))
            },
            None => {
                no_qp(index);
                return Err(BufError { error: Error::Invalid("no such QP"), buf: part });
            },
        };
        match checked {
            Ok(_) => {},
            Err(error) => return Err(BufError { error, buf: part }),
        }
        match self.open_room(index, open) {
            Ok(()) => {},
            Err(error) => return Err(BufError { error, buf: part }),
        }
        match &mut self.qps[index] {
            Some(e) => {
                let k = open.n;
                write_wqe(&mut open.wqes, k, OpenWqe { op, sge: part_sge_of(&part), first_sge: 0, sges: 1 });
                open.n = k + 1;
                open.note_op(&op);
                open.kept.keep_general(part, Ghost(if op_writes_local(op) { safeverbs_spec::abs::Dir::In } else { safeverbs_spec::abs::Dir::Out }), &mut e.spare_leases);
            },
            None => {},
        }
        Ok(())
    }

    /// Finishes the WQE gathered on QP `index` with operation `op`, as [`Hw::push_other`] does a WQE
    /// of one part. On error nothing is added and the parts are dropped.
    #[verifier::spinoff_prover]
    pub fn push_gathered(&mut self, index: usize, open: &mut OpenRequest<N>, op: WrOp) -> (r: Result<()>)
    {
        if index >= self.qps.len() {
            no_qp(index);
            return Err(Error::Invalid("no such QP"));
        }
        let checked = match &self.qps[index] {
            Some(e) => {
                if e.removed {
                    no_qp(index);
                }
                Self::check_wqe(e, &op, e.building.as_slice())
            },
            None => {
                no_qp(index);
                return Err(Error::Invalid("no such QP"));
            },
        };
        match checked {
            Ok(_) => {},
            Err(error) => {
                self.drop_gathered(index);
                return Err(error);
            },
        }
        match self.open_room(index, open) {
            Ok(()) => {},
            Err(error) => {
                self.drop_gathered(index);
                return Err(error);
            },
        }
        match &mut self.qps[index] {
            Some(e) => {
                let sges = e.building.len();
                if open.gathered.capacity() == 0 {
                    core::mem::swap(&mut open.gathered, &mut e.spare_sges);
                }
                if open.gathered.len() > u32::MAX as usize - MAX_SGES {
                    e.building.clear();
                    return Err(Error::Invalid("a request of too many scatter/gather elements"));
                }
                let first_sge = open.gathered.len() as u32;
                let mut i = 0;
                while i < sges
                {
                    open.gathered.push(part_sge_of(&e.building[i]));
                    i += 1;
                }
                let k = open.n;
                write_wqe(&mut open.wqes, k, OpenWqe { op, sge: part_sge_of(&e.building[0]), first_sge, sges: sges as u32 });
                open.n = k + 1;
                open.note_op(&op);
                if sges as u32 > open.max_sges {
                    open.max_sges = sges as u32;
                }
                // In the order the hardware reads them is no matter to the leases.
                while e.building.len() > 0
                {
                    match e.building.pop() {
                        Some(part) => open.kept.keep_general(part, Ghost(if op_writes_local(op) { safeverbs_spec::abs::Dir::In } else { safeverbs_spec::abs::Dir::Out }), &mut e.spare_leases),
                        None => {},
                    }
                }
            },
            None => {},
        }
        Ok(())
    }

}

// =============================================================================
// The rings, for their owners and the event loop

impl<C, N: Device> Hw<C, N> {
    /// Whether `index` names a QP of the engine (not one being removed).
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

    /// QP `index`'s ring of `side`, if it is a QP of the engine.
    pub fn ring(&self, index: usize, side: Side) -> Option<&Ring<Payload<C, N>>>
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

    /// Request `seq` of QP `index`'s ring of `side` holds a result for its owner: complete, or
    /// refused. [`Hw::take`] then hands it back: nothing an owner waits for is lost.
    pub closed spec fn takeable(&self, index: usize, side: Side, seq: RingSeq) -> bool {
        self.has_qp(index as int) && {
            let e = self.qps@[index as int]->0;
            let r = if side is Send { e.send } else { e.recv };
            ||| r.req(seq as int) is Some && r.req(seq as int)->0.phase is Complete
            ||| r.refused(seq as int) is Some
        }
    }

    /// QP `index`'s protection domain.
    pub fn qp_pd(&self, index: usize) -> Option<&Pd<N>>
    {
        if index >= self.qps.len() {
            return None;
        }
        match &self.qps[index] {
            Some(e) => Some(&e.pd),
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

    /// Takes the result, context and buffers of retired (or refused) request
    /// `seq` of QP `index`'s ring of `side`.
    #[inline(always)]
    #[verifier::spinoff_prover]
    pub fn take(&mut self, index: usize, side: Side, seq: RingSeq) -> (r: Option<Completion<C, N>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            r matches Some(c) ==> c.qp == index && c.queue == side && c.seq == seq
                && c.leases.inv() && c.leases.fully_recorded(),
            side is Recv ==> (r matches Some(c) ==> old(self).recv_taken_matches(index, seq, c)),
            side is Send ==> (r matches Some(c) ==> old(self).send_taken_matches(index, seq, c)),
            old(self).takeable(index, side, seq) ==> r is Some,
    {
        if index >= self.qps.len() {
            return None;
        }
        match &mut self.qps[index] {
            Some(e) => {
                let taken = match side {
                    Side::Send => e.send.take(seq),
                    Side::Recv => e.recv.take(seq),
                };
                // Field by field: nothing of the request is copied twice on its way out.
                match taken {
                    Some(Taken { outcome, leases: Payload { ctx: Some(wr_id), leases } }) => {
                        Some(Completion { qp: index, qp_num: e.qp_num, queue: side, seq, wr_id, result: outcome, leases })
                    },
                    // A request with an owner holds a context, so nothing is lost here.
                    _ => None,
                }
            },
            None => None,
        }
    }

    /// Gives up request `seq` of QP `index`'s ring of `side`: its buffers are
    /// released when it retires (now, if it has). Its context comes back.
    #[verifier::spinoff_prover]
    pub fn detach(&mut self, index: usize, side: Side, seq: RingSeq) -> (given_up: (Option<C>, Option<core::task::Waker>))
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
    {
        if index >= self.qps.len() {
            return (None, None);
        }
        match &mut self.qps[index] {
            Some(e) => match side {
                Side::Send => e.send.give_up(seq),
                Side::Recv => e.recv.give_up(seq),
            },
            None => (None, None),
        }
    }

    /// Wakes `waker` when request `seq` of QP `index`'s ring of `side`
    /// retires.
    #[verifier::spinoff_prover]
    pub fn set_waker(&mut self, index: usize, side: Side, seq: RingSeq, waker: &core::task::Waker) -> (replaced: Option<core::task::Waker>)
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
        let replaced = match &mut self.qps[index] {
            Some(e) => {
                let ghost send = e.send;
                let ghost recv = e.recv;
                let replaced = match side {
                    Side::Send => e.send.set_waker(seq, waker),
                    Side::Recv => e.recv.set_waker(seq, waker),
                };
                proof {
                    lemma_resources_kept(send, e.send);
                    lemma_resources_kept(recv, e.recv);
                }
                replaced
            },
            None => None,
        };
        proof {
            if start.qps@[index as int] is Some {
                let pre = start.qps@[index as int]->0;
                let post = self.qps@[index as int]->0;
                lemma_queues_kept(pre, post, m.qps[pre.id()]);
                outcomes::lemma_recv_kept(pre.recv, post.recv, m, pre.id());
                send_outcomes::lemma_results_kept(pre.send, post.send, m, pre.id());
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
        replaced
    }

    /// The first error of a send request without an owner on QP `index`,
    /// once: M's first error among that request's WQEs, at its position, or a
    /// refusal, which M never saw.
    #[verifier::spinoff_prover]
    pub fn take_send_error(&mut self, index: usize) -> (r: Option<DetachedError>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            r matches Some(error) ==> old(self).detached_taken_matches(index, error),
    {
        if index >= self.qps.len() {
            return None;
        }
        let record = match &mut self.qps[index] {
            Some(e) => e.send.take_detached_error(),
            None => None,
        };
        match record {
            Some(d) => Some(DetachedError { seq: d.seq, error: d.error }),
            None => None,
        }
    }

}

// =============================================================================
// Removing QPs, and the engine

impl<C, N: Device> Hw<C, N> {
    /// Starts removing QP `index` (see `crate::engine`, "Removing QPs"):
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
        match &mut self.qps[index] {
            Some(e) => {
                match drain {
                    Some(true) => e.drain_pending = e.send.in_flight_wqes() > 0,
                    Some(false) => e.drain_pending = false,
                    None => {},
                }
                if removed {
                    e.removed = true;
                    e.takes_work = false;
                    e.refused = false;
                }
            },
            None => {},
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
        let mut wakes = WakeBatch::new();
        let busy = match &mut self.qps[index] {
            Some(e) => {
                let ghost q = e.id();
                if e.started {
                    e.started = false;
                    abort(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                }
                // Nothing staged is posted: refused (their owners are gone too).
                e.send.abort_with_wakes(&mut wakes);
                let mut seq = e.send.head();
                while seq < e.send.posted()
                {
                    // A context's destructor is the program's: it runs when the engine is let
                    // go of, never during a poll.
                    let (ctx, waker) = e.send.give_up(seq);
                    self.let_go.keep(ctx, waker);
                    seq += 1;
                }
                let mut seq = e.recv.head();
                while seq < e.recv.posted()
                {
                    let (ctx, waker) = e.recv.give_up(seq);
                    self.let_go.keep(ctx, waker);
                    seq += 1;
                }
                e.send.in_flight_wqes() > 0 || e.recv.in_flight_wqes() > 0
            },
            None => false,
        };
        wakes.finish();
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

impl<C, N: Device> Hw<C, N> {
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
        let ready = match &self.qps[index] {
            Some(e) => e.removed && e.drain_pending && matches!(e.phase, Phase::Ready) && e.send.can_post(),
            None => false,
        };
        if !ready {
            return false;
        }
        match &mut self.qps[index] {
            Some(e) => {
                let ghost q = e.id();
                let wqe = e.send.next_wqe();
                if !e.started {
                    start(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                    e.started = true;
                }
                let nothing: [Sge; 0] = [];
                let none = nothing.as_slice();
                post(&mut e.hw, wr_id(index as u16, false, wqe), true, &WrOp::Send, none, Ghost(q), Tracked(self.token.borrow_mut()));
                let mut leases = no_leases();
                leases.record_wqe(Ghost(crate::buf::WqeLoans {
                    wr_id: spec_wr_id(index as u16, false, wqe),
                    opcode: Some(safeverbs_spec::nic::Opcode::Send),
                    sges: Seq::empty(), first: 0,
                }));
                let _seq = e.send.stage(1, true, true, Ok(Completed::Sent), Payload { ctx: None, leases });
            },
            None => {},
        }
        true
    }

    /// Posts QP `index`'s drain WQE if it waits for one and has room (see
    /// `crate::engine`, "Removing QPs"): it completes after every WQE before
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
        let idle = match &self.qps[index] {
            Some(e) => e.removed && !e.started && e.send.in_flight_wqes() == 0
                && e.recv.in_flight_wqes() == 0,
            None => false,
        };
        if !idle {
            return false;
        }
        let mut slot: Option<HwQp<C, N>> = None;
        core::mem::swap(&mut slot, &mut self.qps[index]);
        let entry = match slot {
            Some(entry) => entry,
            None => {
                return false;
            },
        };
        let need = entry.caps.max_send_wr + entry.caps.max_recv_wr;
        let ghost q = entry.id();
        // The hardware QP first; the rest of the entry, the rings with the
        // buffers, goes when this returns, once the NIC holds none of them.
        destroy_qp(entry.hw, Ghost(q), Tracked(self.token.borrow_mut()));
        self.reserved -= need;
        true
    }

    /// Ends the engine: destroys its CQ if no QP is left (each destroyed
    /// before: M's `destroy_cq` needs none attached). Otherwise hands the
    /// engine back: a QP the device never flushed keeps it, and the caller
    /// leaks it all (no memory the NIC may use is ever freed).
    #[allow(clippy::result_large_err)] // Once, at the very end: nothing to save.
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

impl<C, N: Device> Hw<C, N> {
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
        match &mut self.qps[index] {
            Some(e) => e.building.clear(),
            None => {},
        }
    }

    /// The number of slots of the QP table: every QP's index is below it.
    pub fn qp_slots(&self) -> usize {
        self.qps.len()
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
        self.make_send_room(index, 1)
    }
}

/// The device refused the only request written: what its slot got comes back.
#[cold]
#[inline(never)]
fn not_posted<C, N: Device>(ring: &mut Ring<Payload<C, N>>, error: Error) -> (r: Refusal<C, N>)
{
    match ring.unfill() {
        Some(payload) => Refusal { error, ctx: payload.ctx, leases: payload.leases },
        None => Refusal { error, ctx: None, leases: Leases::new() },
    }
}

/// The success a completion of `op` on `len` bytes reports, and the one its request reports if
/// a later completion retires it: the NIC reports the status alone of a SEND or a WRITE, and a
/// READ or an atomic reports the length it asked for.
#[inline(always)]
fn implied_outcome(op: &WrOp, len: u32) -> (r: CompletionResult)
{
    match op {
        WrOp::Send | WrOp::SendImm(_) | WrOp::Write(_) | WrOp::WriteImm(..) => Ok(Completed::Sent),
        WrOp::Read(_) | WrOp::FetchAdd { .. } | WrOp::CompareSwap { .. } => Ok(Completed::Read { len }),
    }
}

/// Whether the NIC writes the local buffer of `op`.
spec fn op_writes_local(op: WrOp) -> bool {
    opcode_model(op).writes_local()
}

} // verus!

impl<C, N: Device> Hw<C, N> {
    /// The bytes of a QP in the table.
    pub(crate) const QP_BYTES: usize = core::mem::size_of::<HwQp<C, N>>();
}
