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
//!   the event loop routes it to.
//! - **CQEs read and not routed.** The event loop reads CQEs first and routes them one by one, the
//!   program posting and polling in between, so M is ahead of the rings. Each CQE read is kept
//!   beside its report (`Hw::pending`): it names a QP of the table and a WQE in flight there, and
//!   every WQE M popped that a ring has not heard of succeeded without a completion of its own, or
//!   is named by a CQE to come (`pending_ok`). Until its ring hears of it, a WQE's buffers stay
//!   with its request, and M's outcome waits in `retired` for it.
//! - **Local memory.** Every SQ/RQ WQE matches its live request
//!   interval's exact opcode and SGE ledger. Written WRs retain
//!   their guards in staged requests. Refused results never authorize DMA,
//!   even when their WQE numbers are reused. The byte lemmas expose the
//!   corresponding source/destination loan and physical lifetime guard.
//! - **Outcomes.** A receive request's result is the outcome M retired for
//!   its WQE (`outcomes`). A send request, one WQE or a batch, caches the
//!   first error M retired in its interval while it is in flight, and
//!   completes with M's aggregate of the interval: the first error in
//!   posting order, at its position in the interval, else the last WQE's
//!   success (`send_outcomes`). [`Hw::take`] hands back exactly that, with
//!   the request's own context and buffers, or says that its doorbell was
//!   refused, which M never saw, and so does [`Hw::next_completion`] as it routes
//!   the CQEs read (`Hw::hands_out`). A detached request's first error is held to
//!   the same ([`Hw::take_send_error`]).
//! - **Capacity.** The CQ's entries reserved in M are exactly what the
//!   engine counts ([`Hw::reserved`]); a QP is created only if its depths fit
//!   ([`Hw::create_qp`]), and each ring keeps at most its depth of WQEs in
//!   flight, so the CQ never overruns (M's `lemma_cq_never_overflows`).
//! - **Teardown.** A QP is destroyed only when neither ring has a WQE in
//!   flight, which the coupling turns into M's empty queues, `destroy_qp`'s
//!   precondition, and no CQE read names it ([`Hw::destroy`]).
//! - **State.** The engine posts work only on a QP it connected or moved to
//!   the error state, which M sees in `Rts` or `Err`.
//! - **A.** The engine stands for a state of the abstract machine, and each
//!   public method moves it by A's own steps ([`refines`]): Theorem 1, for one
//!   engine and the buffers it is lent.
//!
//! An engine (`crate::engine`) drives a `Hw` through its public methods only; each requires and
//! keeps the invariant, and that of the request being written it is given (`open`).
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
    clippy::needless_return,
    clippy::let_and_return
)]

use safeverbs_spec::nic::*;
#[cfg(verus_keep_ghost)]
use safeverbs_spec::nic_proofs::{lemma_queued_outcomes_step, queued_outcomes_ok};
use safeverbs_spec::refinement::*;
use safeverbs_spec::share::*;
use safeverbs_spec::types::{CqId, Key, PdId, QpId};
use vstd::prelude::*;

use core::mem::ManuallyDrop;

use crate::buf::{BufError, Kept, Lease, Leases, Part, capacity_of, forget_free, replace_value};
use crate::error::{Error, Result};
use crate::nic::{
    ConnectAttr, DeviceCaps, Endpoint, Nic as Device, NicCq, NicQp, QpCaps, QpType, Sge, WrOp,
};
use crate::pd::{Context, Pd};
#[cfg(verus_keep_ghost)]
use crate::refine::{free_lease, may_post_holds, roots_outside};
use crate::ring::wake::WakeBatch;
use crate::ring::{Payload, Ring, Seq as RingSeq, Taken};
use crate::trusted::*;
use crate::verbs::Verbs;
use crate::wc::{Completed, CompletionError, CompletionResult, Opcode, Received, Wc};
use crate::wr_id::*;

mod completion;
mod open;
mod outcomes;
pub mod refines;
mod send_outcomes;
mod separation;

pub(crate) use open::Writing;
pub use open::{OpenRequest, OpenWqe};
#[cfg(verus_keep_ghost)]
use open::{lemma_sges_len_one, parts_sges};
use open::{one_of, write_wqe};

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
#[verifier::reject_recursive_types(N)]
pub struct Unposted<C, N: Device> {
    /// What it does.
    pub op: WrOp,
    /// Its buffer.
    pub part: Part<N>,
    /// The context it was given.
    pub ctx: Option<C>,
}

/// A request that could not be posted: why, and its context, if it has one, and buffers.
#[verifier::reject_recursive_types(N)]
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
    pub(crate) fn new() -> Self
        no_unwind
    {
        LetGo { wr_ids: vec_new(), wakers: vec_new() }
    }

    /// What giving up one request let go of.
    fn keep(&mut self, ctx: Option<C>, waker: Option<core::task::Waker>)
        no_unwind
    {
        match ctx {
            Some(ctx) => vec_push(&mut self.wr_ids, ctx),
            None => {},
        }
        match waker {
            Some(waker) => vec_push(&mut self.wakers, waker),
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
    /// Destroyed by hand, by [`Hw::finish`], or never, if a QP on it is kept.
    cq: ManuallyDrop<N::Cq>,
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
    /// The CQEs M popped that `polled[polled_at..polled_len]` reports (erased).
    #[allow(dead_code)]
    pending: Ghost<Seq<Cqe>>,
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
    #[allow(dead_code)]
    token: Tracked<NicToken>,
    /// The next QP name to use (`NicToken::name`).
    next_name: Ghost<nat>,
    /// What the engine stands for in A (erased): see [`refines`].
    #[allow(dead_code)]
    refined: Ghost<refines::Refinement>,
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
    &&& r.spec_routed() <= qp.sq_head
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
    &&& r.spec_routed() <= qp.rq_head
    &&& qp.rq_head + qp.rq.len() == r.spec_wqe_committed()
    &&& r.spec_wqe_posted() == r.spec_wqe_committed()
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
    &&& refines::holds_cover(p.holds@, p.leases.loans())
    &&& refines::holds_kept(p.holds@, p.leases)
}

/// Every live WQE stays with its request and that request's buffers; the
/// context may have gone with the owner.
pub(crate) open spec fn keeps_leases<C, N: Device>(post: Ring<Payload<C, N>>, pre: Ring<Payload<C, N>>) -> bool {
    forall|s: int, w: int| pre.dma(s, w) is Some && post.spec_wqe_retired() <= w < post.spec_wqe_posted()
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
    requires keeps_leases(post, pre), post.spec_wqe_retired() <= w < post.spec_wqe_posted(),
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

/// A step of a completion's walk changes outcomes and ownership, while surviving requests retain
/// the exact resource value that was staged and committed.
proof fn lemma_resources_walked<C, N: Device>(pre: Ring<Payload<C, N>>, post: Ring<Payload<C, N>>, recv: bool, wqe: u64, wc: Wc, spent: bool)
    requires pre.inv(), ring_resources_ok(pre), post.walked(&pre, recv, wqe, wc, spent),
    ensures ring_resources_ok(post),
{
    assert forall|s: int| #[trigger] post.req(s) is Some
        implies resource_at(pre, s, post.req(s)->0.leases, post.req(s)->0.detached) by {
        if pre.spec_retired() <= s < post.spec_retired() {
            pre.lemma_retired_view(s, pre.retire_outcome(s, recv, wqe, wc));
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
/// out and not committed, each with its number's `wr_id`, each one M accepts.
pub open spec fn writing_ok<L>(
    r: Ring<L>,
    index: u16,
    m: Nic::State,
    attr: QpAttr,
    started: bool,
    written: Seq<SendWr>,
) -> bool {
    &&& !started ==> written.len() == 0
    &&& written.len() == r.spec_wqe_posted() - r.spec_wqe_committed()
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

/// Written WQEs belong to a staged request.
spec fn written_backed<C, N: Device>(e: HwQp<C, N>, w: int, wr: SendWr) -> bool {
    &&& !wr.inline
    &&& ring_backed(e.send, w, wr.wr_id, Some(wr.opcode), wr.sges)
}

/// `written_backed` follows the ring's WQEs.
proof fn lemma_written_kept<C, N: Device>(pre: HwQp<C, N>, post: HwQp<C, N>, w: int, wr: SendWr)
    requires written_backed(pre, w, wr), keeps_leases(post.send, pre.send),
        post.send.spec_wqe_retired() <= w < post.send.spec_wqe_posted(),
    ensures written_backed(post, w, wr),
{
    lemma_backed_kept(pre.send, post.send, w, wr.wr_id, Some(wr.opcode), wr.sges);
}

// =============================================================================
// The CQEs read and not routed yet

/// The ring of queue `queue` of QP `e`.
spec fn ring_of<C, N: Device>(e: HwQp<C, N>, queue: Queue) -> Ring<Payload<C, N>> {
    match queue {
        Queue::Send => e.send,
        Queue::Recv => e.recv,
    }
}

/// The WQE CQE `e` names.
pub open spec fn cqe_wqe(e: Cqe) -> WqeRef {
    WqeRef { qp: e.qp, queue: e.queue, seq: e.seq }
}

/// The index in the table CQE `e`'s `wr_id` names.
pub open spec fn cqe_index(e: Cqe) -> int {
    (e.wr_id >> INDEX_SHIFT) as int
}

/// A CQE M popped and no ring heard of: it names a QP of the table by its `wr_id`, and a WQE of it
/// M popped, in flight in its ring and past all the ring heard of; M retired that WQE with its
/// outcome, one M allows a receive if it succeeded on a receive queue.
spec fn cqe_ok<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, e: Cqe) -> bool {
    let i = cqe_index(e);
    let w = cqe_wqe(e);
    &&& 0 <= i < qps.len()
    &&& qps[i] is Some
    &&& qps[i]->0.id() == e.qp
    &&& e.wr_id == spec_wr_id(i as u16, e.queue is Recv, e.seq as u64)
    &&& e.seq < head_of(m.qps[e.qp], e.queue)
    &&& ring_of(qps[i]->0, e.queue).spec_wqe_retired() <= e.seq
    &&& ring_of(qps[i]->0, e.queue).spec_routed() <= e.seq
    &&& m.retired.contains_key(w)
    &&& m.retired[w] == e.outcome
    &&& e.queue is Recv && e.outcome.status is Success ==> e.outcome.kind is Receive
        || (e.outcome.kind is WriteNotification && e.outcome.imm is Some)
}

/// WQE `w`, which M popped and its ring has not heard of, is retired, and succeeded without a
/// completion, which only happens on a send queue, or a CQE to come names it.
spec fn lag_at(w: WqeRef, m: Nic::State, pending: Seq<Cqe>) -> bool {
    &&& m.retired.contains_key(w)
    &&& (w.queue is Send && m.retired[w].status is Success)
        || exists|j: int| 0 <= j < pending.len() && cqe_wqe(#[trigger] pending[j]) == w
}

/// Every WQE of queue `queue` of QP `e` that M popped and its ring has not heard of is explained.
spec fn lag_ok<C, N: Device>(e: HwQp<C, N>, m: Nic::State, queue: Queue, pending: Seq<Cqe>) -> bool {
    forall|s: nat| ring_of(e, queue).spec_routed() <= s < head_of(m.qps[e.id()], queue)
        ==> #[trigger] lag_at(WqeRef { qp: e.id(), queue, seq: s }, m, pending)
}

/// The CQEs read and not routed yet, oldest first, are all M popped that the rings have not heard
/// of: see [`cqe_ok`] and [`lag_ok`]. Those of one queue are in posting order.
spec fn pending_ok<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, pending: Seq<Cqe>) -> bool {
    &&& forall|j: int| 0 <= j < pending.len() ==> #[trigger] cqe_ok(qps, m, pending[j])
    &&& forall|a: int, b: int|
        #![trigger pending[a], pending[b]]
        0 <= a < b < pending.len() && pending[a].qp == pending[b].qp && pending[a].queue == pending[b].queue
            ==> pending[a].seq < pending[b].seq
    &&& forall|i: int| 0 <= i < qps.len() && #[trigger] qps[i] is Some ==> {
        &&& lag_ok(qps[i]->0, m, Queue::Send, pending)
        &&& lag_ok(qps[i]->0, m, Queue::Recv, pending)
    }
}

impl<C, N: Device> HwQp<C, N> {
    /// M's name of the QP.
    pub closed spec fn id(&self) -> QpId {
        self.id@
    }

    /// The QP is coupled to M's state `m` (with share `sh` and the WRs `written` since its start)
    /// at index `index`.
    pub closed spec fn ok(
        &self,
        index: u16,
        m: Nic::State,
        sh: Share,
        started: bool,
        written: Seq<SendWr>,
    ) -> bool {
        let qp = m.qps[self.id@];
        &&& sh.owns.contains(self.id@)
        &&& m.qps.contains_key(self.id@)
        &&& qp_of(self.hw) == self.id@
        &&& qp.attr == qp_attr(pd_of(self.pd), self.qp_type, sh.cq, self.caps)
        &&& self.pd_id == pd_id(self.pd)
        &&& self.reliable == (self.qp_type == QpType::Rc)
        &&& self.max_len <= m.caps.max_msg
        &&& 1 <= self.caps.max_send_sge && 1 <= self.caps.max_recv_sge
        &&& self.atomics ==> self.qp_type == QpType::Rc
        &&& self.takes_work ==> self.phase == Phase::Ready
        &&& self.spare_leases@.len() == 0
        &&& self.spare_wqes@.len() == 0 || self.spare_wqes@.len() == self.send.spec_max_wqes()
        &&& sq_linked(self.send, index, qp)
        &&& rq_linked(self.recv, index, qp)
        &&& outcomes::recv_outcomes(self.recv, m, self.id())
        &&& send_outcomes::send_results(self.send, m, self.id())
        &&& send_outcomes::send_implied(self.send, self.send_tails@)
        &&& send_outcomes::tail_bound(self.send, self.send_tails@, qp, written, m.retired, self.id())
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
        &&& writing_ok(self.send, index, m, qp.attr, started, written)
        &&& self.building@.len() <= MAX_SGES
    }
}

/// Entry `i` of the table `qps` is coupled to M's state `m`, with token `t`'s share and writing.
spec fn qp_ok<C, N: Device>(entry: Option<HwQp<C, N>>, i: int, t: NicToken, m: Nic::State) -> bool {
    match entry {
        Some(e) => e.ok(i as u16, m, t.share(), t.started(e.id()), t.written(e.id())),
        None => true,
    }
}

/// The same QPs at the same indices.
spec fn same_qps<C, N: Device>(a: Seq<Option<HwQp<C, N>>>, b: Seq<Option<HwQp<C, N>>>) -> bool {
    &&& a.len() == b.len()
    &&& forall|i: int| 0 <= i < a.len() ==> (#[trigger] a[i] is Some) == (b[i] is Some)
    &&& forall|i: int| 0 <= i < a.len() && #[trigger] a[i] is Some ==> a[i]->0.same_config(b[i]->0)
}

/// The names of the table `qps`: one entry per QP name, every QP of the share M has is one, only
/// the table's QPs write, and names from `next_name` on are unused.
spec fn names_ok<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, next_name: nat, t: NicToken, m: Nic::State) -> bool {
    let sh = t.share();
    &&& forall|i: int, j: int|
        #![trigger qps[i], qps[j]]
        0 <= i < qps.len() && 0 <= j < qps.len() && i != j && qps[i] is Some
            && qps[j] is Some ==> qps[i]->0.id() != qps[j]->0.id()
    &&& forall|q: QpId|
        #[trigger] m.qps.contains_key(q) && sh.owns.contains(q) ==> exists|i: int|
            0 <= i < qps.len() && #[trigger] qps[i] is Some && qps[i]->0.id() == q
    &&& forall|q: QpId|
        #[trigger] writes(t, q) ==> exists|i: int|
            0 <= i < qps.len() && #[trigger] qps[i] is Some && qps[i]->0.id() == q
    &&& forall|a: nat| #[trigger] sh.owns.contains(t.name(a))
    &&& forall|a: nat, b: nat| #[trigger] t.name(a) == #[trigger] t.name(b) ==> a == b
    &&& forall|n: nat| n >= next_name ==> !(#[trigger] m.qps.contains_key(t.name(n)))
    &&& forall|n: nat, w: WqeRef|
        n >= next_name && #[trigger] m.retired.contains_key(w) ==> w.qp != #[trigger] t.name(n)
}

/// The table `qps` is coupled to M's state `m`, with the CQEs `pending` read and not routed.
spec fn table_ok<C, N: Device>(
    qps: Seq<Option<HwQp<C, N>>>, next_name: nat, t: NicToken, m: Nic::State,
    pending: Seq<Cqe>,
) -> bool {
    &&& nic_inv(m)
    &&& share_wf(m, t.share())
    &&& qps.len() <= MAX_QPS
    &&& forall|i: int| 0 <= i < qps.len() ==> #[trigger] qp_ok(qps[i], i, t, m)
    &&& names_ok(qps, next_name, t, m)
    &&& pending_ok(qps, m, pending)
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
        qp_ok(self.qps@[i], i, t, m)
    }

    /// The invariant, with M's state `m` in place of the token's view (and
    /// the token's share, names and writing).
    pub closed spec fn inv_with(&self, t: NicToken, m: Nic::State) -> bool {
        let sh = t.share();
        &&& cq_of(self.cq@) == sh.cq
        &&& m.cqs[sh.cq].cap == self.cq_size
        &&& m.cqs[sh.cq].reserved.len() == self.reserved
        &&& self.reserved <= self.cq_size
        &&& caps_model(m.caps, self.caps)
        &&& table_ok(self.qps@, self.next_name@, t, m, self.pending@)
    }

    /// `self` has the same QPs as `other` at the same indices.
    pub closed spec fn same_table(&self, other: &Self) -> bool {
        &&& self.caps == other.caps
        &&& same_qps(self.qps@, other.qps@)
    }

    /// The context of request `seq` of QP `index`'s send queue, if it is staged there.
    pub closed spec fn staged_ctx(&self, index: int, seq: int) -> Option<Option<C>> {
        if self.has_qp(index) {
            match self.qps@[index]->0.send.staged(seq) {
                Some(r) => Some(r.leases.ctx),
                None => None,
            }
        } else {
            None
        }
    }

    /// A new engine, on a device just opened some time ago.
    pub closed spec fn born(&self) -> bool {
        exists|m0: Nic::State| nic_init(m0) && #[trigger] self.born_from(m0)
    }

    /// QP `index` is still there, with the buffers its requests hold: the keeping of a QP the device
    /// did not flush.
    pub closed spec fn keeps_qp(&self, index: int) -> bool {
        self.has_qp(index)
    }

    /// QP `index` is in the table.
    pub closed spec fn has_qp(&self, index: int) -> bool {
        0 <= index < self.qps@.len() && self.qps@[index] is Some
    }

    /// The engine's state is coupled to M (see the module documentation).
    pub closed spec fn inv_m(&self) -> bool {
        &&& self.inv_with(self.token@, self.token@.nic())
        &&& self.polled_ok()
    }

    /// The invariant: the engine's state is coupled to M, and stands for a state of A
    /// ([`refines::linked`]).
    pub closed spec fn inv(&self) -> bool {
        &&& self.inv_m()
        &&& refines::linked(self.qps@, self.token@.nic(), self.refined@)
    }

    /// The CQEs read and not routed are what the reports not routed report.
    closed spec fn polled_ok(&self) -> bool {
        &&& self.polled_at <= self.polled_len <= POLL_MAX
        &&& self.pending@.len() == self.polled_len - self.polled_at
        &&& forall|j: int| 0 <= j < self.pending@.len()
            ==> reports(self.polled@[self.polled_at + j], #[trigger] self.pending@[j])
    }
}

// =============================================================================
// What the others' steps keep

/// What `framed` says of QP `q` of the share, which is all its coupling reads.
spec fn framed_qp(pins: ISet<Key>, m: Nic::State, n: Nic::State, q: QpId) -> bool {
    &&& n.caps == m.caps
    &&& forall|k: Key| #[trigger] pins.contains(k) && m.mrs.contains_key(k)
        ==> n.mrs.contains_key(k) && n.mrs[k] == m.mrs[k]
    &&& n.qps.contains_key(q)
    &&& qp_kept(m.qps[q], n.qps[q])
    &&& forall|w: WqeRef| w.qp == q && #[trigger] m.retired.contains_key(w)
        ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w]
}

/// A send WR M accepted still is after steps that keep the regions it names.
proof fn lemma_send_wr_framed(pins: ISet<Key>, m: Nic::State, n: Nic::State, q: QpId, attr: QpAttr, wr: SendWr)
    requires
        framed_qp(pins, m, n, q),
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
            self.ok(index, m, t.share(), t.started(self.id()), t.written(self.id())),
            framed_qp(pinned(t), m, n, self.id()),
            queued_outcomes_ok(n, self.id()),
        ensures
            self.ok(index, n, t.share(), t.started(self.id()), t.written(self.id())),
    {
        let q = self.id@;
        let (a, b) = (m.qps[q], n.qps[q]);
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
            lemma_send_wr_framed(pinned(t), m, n, q, a.attr, written[j]);
        }
        assert forall|j: int| 0 <= j < b.sq.len() implies b.sq_head + j < a.sq_head + a.sq.len()
            && (#[trigger] b.sq[j]).wr == a.sq[b.sq_head + j - a.sq_head].wr by {
            assert(b.sq[j].wr == a.sq[j].wr);
        }
        send_outcomes::lemma_bound_kept(self.send, self.send, self.send_tails@, a, b, written, m.retired, n.retired, q);
    }
}

/// What the CQEs read and not routed read of an entry and its QP in M stays: its name, what its
/// rings heard of, M's heads and the outcomes M retired.
spec fn same_for_cqes<C, N: Device>(a: Option<HwQp<C, N>>, b: Option<HwQp<C, N>>, m: Nic::State, n: Nic::State) -> bool {
    match a {
        None => b is None,
        Some(x) => b is Some && {
            let y = b->0;
            let q = x.id();
            &&& y.id() == q
            &&& y.send.spec_wqe_retired() == x.send.spec_wqe_retired()
            &&& y.recv.spec_wqe_retired() == x.recv.spec_wqe_retired()
            &&& y.send.spec_routed() == x.send.spec_routed()
            &&& y.recv.spec_routed() == x.recv.spec_routed()
            &&& n.qps[q].sq_head == m.qps[q].sq_head
            &&& n.qps[q].rq_head == m.qps[q].rq_head
            &&& forall|w: WqeRef| w.qp == q && #[trigger] m.retired.contains_key(w)
                ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w]
        },
    }
}

/// What a QP's rings have not heard of stays explained if the two say the same of it.
proof fn lemma_lag_same<C, N: Device>(x: HwQp<C, N>, y: HwQp<C, N>, m: Nic::State, n: Nic::State, pending: Seq<Cqe>)
    requires
        lag_ok(x, m, Queue::Send, pending),
        lag_ok(x, m, Queue::Recv, pending),
        same_for_cqes(Some(x), Some(y), m, n),
    ensures
        lag_ok(y, n, Queue::Send, pending),
        lag_ok(y, n, Queue::Recv, pending),
{
    assert forall|s: nat| ring_of(y, Queue::Send).spec_routed() <= s < head_of(n.qps[y.id()], Queue::Send)
        implies #[trigger] lag_at(WqeRef { qp: y.id(), queue: Queue::Send, seq: s }, n, pending) by {
        assert(lag_at(WqeRef { qp: x.id(), queue: Queue::Send, seq: s }, m, pending));
    }
    assert forall|s: nat| ring_of(y, Queue::Recv).spec_routed() <= s < head_of(n.qps[y.id()], Queue::Recv)
        implies #[trigger] lag_at(WqeRef { qp: y.id(), queue: Queue::Recv, seq: s }, n, pending) by {
        assert(lag_at(WqeRef { qp: x.id(), queue: Queue::Recv, seq: s }, m, pending));
    }
}

/// The CQEs read and not routed say the same of tables that are the same to them.
proof fn lemma_pending_same<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, n: Nic::State, pending: Seq<Cqe>)
    requires
        pending_ok(pre, m, pending),
        post.len() == pre.len(),
        forall|i: int| 0 <= i < pre.len() ==> #[trigger] same_for_cqes(pre[i], post[i], m, n),
    ensures
        pending_ok(post, n, pending),
{
    assert forall|j: int| 0 <= j < pending.len() implies #[trigger] cqe_ok(post, n, pending[j]) by {
        let e = pending[j];
        assert(cqe_ok(pre, m, e));
        assert(same_for_cqes(pre[cqe_index(e)], post[cqe_index(e)], m, n));
    }
    assert forall|i: int| 0 <= i < post.len() && #[trigger] post[i] is Some implies {
        &&& lag_ok(post[i]->0, n, Queue::Send, pending)
        &&& lag_ok(post[i]->0, n, Queue::Recv, pending)
    } by {
        assert(same_for_cqes(pre[i], post[i], m, n));
        lemma_lag_same(pre[i]->0, post[i]->0, m, n, pending);
    }
}

/// So do tables that differ in one entry no CQE read names, and whose rings have heard of all M
/// popped if it has one, as a new QP's have.
proof fn lemma_pending_but_one<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, n: Nic::State, pending: Seq<Cqe>, index: int)
    requires
        pending_ok(pre, m, pending),
        pre.len() <= post.len() <= pre.len() + 1,
        0 <= index < post.len(),
        pre.len() < post.len() ==> index == pre.len(),
        forall|i: int| 0 <= i < pre.len() && i != index ==> #[trigger] same_for_cqes(pre[i], post[i], m, n),
        forall|j: int| 0 <= j < pending.len() ==> cqe_index(#[trigger] pending[j]) != index,
        post[index] matches Some(e) ==> head_of(n.qps[e.id()], Queue::Send) <= e.send.spec_routed()
            && head_of(n.qps[e.id()], Queue::Recv) <= e.recv.spec_routed(),
    ensures
        pending_ok(post, n, pending),
{
    assert forall|j: int| 0 <= j < pending.len() implies #[trigger] cqe_ok(post, n, pending[j]) by {
        let e = pending[j];
        assert(cqe_ok(pre, m, e));
        assert(same_for_cqes(pre[cqe_index(e)], post[cqe_index(e)], m, n));
    }
    assert forall|i: int| 0 <= i < post.len() && #[trigger] post[i] is Some implies {
        &&& lag_ok(post[i]->0, n, Queue::Send, pending)
        &&& lag_ok(post[i]->0, n, Queue::Recv, pending)
    } by {
        if i == index {
            let e = post[i]->0;
            assert(ring_of(e, Queue::Send) == e.send);
            assert(ring_of(e, Queue::Recv) == e.recv);
        } else {
            assert(same_for_cqes(pre[i], post[i], m, n));
            lemma_lag_same(pre[i]->0, post[i]->0, m, n, pending);
        }
    }
}

/// What the table says of names stays true of a table with the same QPs, if M has no QP of the share
/// it did not have, the writers are the table's, and nothing is retired of a name M had no use of.
proof fn lemma_names_kept<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, name: nat, t0: NicToken, t: NicToken, m: Nic::State, n: Nic::State)
    requires
        names_ok(pre, name, t0, m),
        same_qps(post, pre),
        t.same_engine(&t0),
        forall|p: QpId| #[trigger] n.qps.contains_key(p) && t.share().owns.contains(p) ==> m.qps.contains_key(p),
        forall|p: QpId| #[trigger] writes(t, p) ==> exists|k: int|
            0 <= k < pre.len() && #[trigger] pre[k] is Some && pre[k]->0.id() == p,
        forall|w: WqeRef| #[trigger] n.retired.contains_key(w) && t.share().owns.contains(w.qp)
            ==> m.retired.contains_key(w) || m.qps.contains_key(w.qp),
    ensures
        names_ok(post, name, t, n),
{
    let sh = t.share();
    assert forall|i: int, j: int|
        #![trigger post[i], post[j]]
        0 <= i < post.len() && 0 <= j < post.len() && i != j && post[i] is Some
            && post[j] is Some implies post[i]->0.id() != post[j]->0.id() by {
        assert(pre[i] is Some && pre[j] is Some);
    }
    assert forall|p: QpId| #[trigger] n.qps.contains_key(p) && sh.owns.contains(p) implies exists|k: int|
        0 <= k < post.len() && #[trigger] post[k] is Some && post[k]->0.id() == p by {
        assert(m.qps.contains_key(p));
        let k = choose|k: int| 0 <= k < pre.len() && #[trigger] pre[k] is Some && pre[k]->0.id() == p;
        assert(post[k] is Some);
    }
    assert forall|p: QpId| #[trigger] writes(t, p) implies exists|k: int|
        0 <= k < post.len() && #[trigger] post[k] is Some && post[k]->0.id() == p by {
        let k = choose|k: int| 0 <= k < pre.len() && #[trigger] pre[k] is Some && pre[k]->0.id() == p;
        assert(post[k] is Some);
    }
    assert forall|a: nat| #[trigger] sh.owns.contains(t.name(a)) by {
        assert(t.name(a) == t0.name(a));
    }
    assert forall|a: nat, b: nat| #[trigger] t.name(a) == #[trigger] t.name(b) implies a == b by {
        assert(t.name(a) == t0.name(a) && t.name(b) == t0.name(b));
    }
    assert forall|k: nat| k >= name implies !(#[trigger] n.qps.contains_key(t.name(k))) by {
        assert(t.name(k) == t0.name(k));
        assert(sh.owns.contains(t.name(k)));
    }
    assert forall|k: nat, w: WqeRef| k >= name && #[trigger] n.retired.contains_key(w)
        implies w.qp != #[trigger] t.name(k) by {
        assert(t.name(k) == t0.name(k));
        if w.qp == t.name(k) {
            assert(sh.owns.contains(w.qp));
        }
    }
}

/// The table survives steps that do not touch the engine's share (nor the regions its written WRs
/// name).
proof fn lemma_table_rely<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, name: nat, t: NicToken, m: Nic::State, n: Nic::State, pending: Seq<Cqe>)
    requires
        table_ok(qps, name, t, m, pending),
        rely_reaches(t.share(), pinned(t), m, n),
    ensures
        table_ok(qps, name, t, n, pending),
{
    lemma_rely_frame(t.share(), pinned(t), m, n);
    let sh = t.share();
    assert forall|i: int| 0 <= i < qps.len() implies #[trigger] qp_ok(qps[i], i, t, n) by {
        assert(qp_ok(qps[i], i, t, m));
        if let Some(e) = qps[i] {
            lemma_queued_outcomes_rely(sh, pinned(t), m, n, e.id());
            assert(sh.owns.contains(e.id()));
            assert forall|w: WqeRef| w.qp == e.id() && #[trigger] m.retired.contains_key(w)
                implies n.retired.contains_key(w) && n.retired[w] == m.retired[w] by {
                assert(sh.owns.contains(w.qp));
            }
            e.lemma_framed(i as u16, t, m, n);
        }
    }
    lemma_names_kept(qps, qps, name, t, t, m, n);
    assert forall|i: int| 0 <= i < qps.len() implies #[trigger] same_for_cqes(qps[i], qps[i], m, n) by {
        assert(qp_ok(qps[i], i, t, m));
        if let Some(e) = qps[i] {
            assert(sh.owns.contains(e.id()));
            assert(m.qps.contains_key(e.id()));
            assert(qp_kept(m.qps[e.id()], n.qps[e.id()]));
            assert forall|w: WqeRef| w.qp == e.id() && #[trigger] m.retired.contains_key(w)
                implies n.retired.contains_key(w) && n.retired[w] == m.retired[w] by {
                assert(sh.owns.contains(w.qp));
            }
        }
    }
    lemma_pending_same(qps, qps, m, n, pending);
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
        lemma_table_rely(self.qps@, self.next_name@, t, m, n, self.pending@);
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

    /// A written WR retains its byte in a staged native request.
    pub(crate) closed spec fn written_guarded(&self, index: int, j: int, a: int, s: int) -> bool {
        let e = self.qps@[index]->0;
        let wr = self.token@.written(e.id())[j];
        let w = e.send.spec_wqe_committed() + j;
        let dir = local_role(Some(wr.opcode));
        &&& e.send.dma(s, w) is Some
        &&& e.send.staged(s) is Some
        &&& e.send.staged(s)->0.leases == e.send.dma(s, w)->0
        &&& retained_byte(e.send.dma(s, w)->0.leases, dir, a)
        &&& byte_backed(e.send.dma(s, w)->0.leases, a)
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
        assert(written_backed(e, w, wr));
        let s = lemma_ring_guards(e.send, w, wr.wr_id, Some(wr.opcode), wr.sges);
        e.send.lemma_dma(s, w);
        lemma_covered_guard(e.send.dma(s, w)->0.leases, local_role(Some(wr.opcode)), wr.sges, a);
        s
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
    started: bool,
    written: Seq<SendWr>,
)
    requires
        e.ok(index, m1, sh, started, written),
        m2.qps.contains_key(e.id()),
        m2.qps[e.id()] == m1.qps[e.id()],
        m2.caps == m1.caps,
        m2.mrs == m1.mrs,
        forall|w: WqeRef| w.qp == e.id() && w.seq < head_of(m1.qps[e.id()], w.queue)
            && #[trigger] m1.retired.contains_key(w)
            ==> m2.retired.contains_key(w) && m2.retired[w] == m1.retired[w],
    ensures
        e.ok(index, m2, sh, started, written),
{
    let qp = m1.qps[e.id()];
    assert forall|w: WqeRef| w.qp == e.id() && w.queue is Recv && w.seq < qp.rq_head
        && #[trigger] m1.retired.contains_key(w)
        implies m2.retired.contains_key(w) && m2.retired[w] == m1.retired[w] by {
        assert(head_of(qp, w.queue) == qp.rq_head);
    }
    assert forall|w: WqeRef| w.qp == e.id() && w.queue is Send && w.seq < qp.sq_head
        && #[trigger] m1.retired.contains_key(w)
        implies m2.retired.contains_key(w) && m2.retired[w] == m1.retired[w] by {
        assert(head_of(qp, w.queue) == qp.sq_head);
    }
    outcomes::lemma_recv_framed(e.recv, m1, m2, e.id());
    send_outcomes::lemma_results_framed(e.send, m1, m2, e.id());
    send_outcomes::lemma_bound_kept(e.send, e.send, e.send_tails@, qp, qp, written, m1.retired, m2.retired, e.id());
    assert forall|j: int| 0 <= j < written.len() implies m2.send_wr_ok(qp.attr, #[trigger] written[j]) by {
        assert(m1.send_wr_ok(qp.attr, written[j]));
    }
}

/// Two tokens of the same engine with the same writing say the same of the table.
proof fn lemma_table_token<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, name: nat, t1: NicToken, t2: NicToken, m: Nic::State, pending: Seq<Cqe>)
    requires
        t2.same_engine(&t1),
        t2.same_writings(&t1),
        table_ok(qps, name, t1, m, pending),
    ensures
        table_ok(qps, name, t2, m, pending),
{
    assert forall|i: int| 0 <= i < qps.len() implies #[trigger] qp_ok(qps[i], i, t2, m) by {
        assert(qp_ok(qps[i], i, t1, m));
        if let Some(e) = qps[i] {
            assert(t2.started(e.id()) == t1.started(e.id()));
            assert(t2.written(e.id()) == t1.written(e.id()));
        }
    }
    assert forall|q: QpId| #[trigger] writes(t2, q) implies exists|i: int|
        0 <= i < qps.len() && #[trigger] qps[i] is Some && qps[i]->0.id() == q by {
        assert(t2.started(q) == t1.started(q) && t2.written(q) == t1.written(q));
        assert(writes(t1, q));
    }
    lemma_names_kept(qps, qps, name, t1, t2, m, m);
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
        lemma_table_token(self.qps@, self.next_name@, t1, t2, m, self.pending@);
    }

    /// The invariant depends on the token passed, not the one held.
    proof fn lemma_inv_fields(&self, other: &Self, t: NicToken, m: Nic::State)
        requires
            other.inv_with(t, m),
            cq_of(self.cq@) == cq_of(other.cq@),
            self.caps == other.caps,
            self.qps@ == other.qps@,
            self.cq_size == other.cq_size,
            self.reserved == other.reserved,
            self.next_name == other.next_name,
            self.pending == other.pending,
        ensures
            self.inv_with(t, m),
    {
    }

    /// An engine's hardware side on the device of `ctx`, with a CQ of `cq_size` entries.
    pub fn new(ctx: Context<N>, cq_size: u32) -> (r: Result<Hw<C, N>>)
        ensures
            r matches Ok(hw) ==> hw.inv() && hw.spec_reserved() == 0 && hw.spec_cq_size() == cq_size && hw.born(),
    {
        let caps = device_caps(&ctx);
        if cq_size == 0 || cq_size > caps.max_cqe {
            return Err(Error::Invalid("a CQ needs 1..=max_cqe entries"));
        }
        let (cq, caps, token) = match create_cq(&ctx, cq_size) {
            Ok(created) => created,
            Err(e) => return Err(e),
        };
        let ghost m0 = choose|m0: Nic::State| nic_init(m0) && nic_reaches(m0, token@.nic());
        let ghost refined = refines::lemma_linked_new::<C, N>(token@.nic(), m0);
        proof {
            lemma_nic_init_invariant(m0);
            lemma_nic_reaches_invariant(m0, token@.nic());
        }
        let hw = Hw {
            let_go: LetGo::new(),
            cq: ManuallyDrop::new(cq),
            ctx,
            caps,
            qps: Vec::new(),
            polled: [EMPTY_WC; POLL_MAX],
            polled_at: 0,
            polled_len: 0,
            pending: Ghost(Seq::empty()),
            kept: false,
            kept_at: 0,
            panics: WakeBatch::new(),
            cq_size,
            reserved: 0,
            token,
            next_name: Ghost(0),
            refined: Ghost(refined),
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
            assert(hw.born_from(m0));
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
        no_unwind
    {
        self.reserved
    }

    /// The entries of its CQ.
    pub fn cq_size(&self) -> (n: u32)
        ensures
            n == self.spec_cq_size(),
        no_unwind
    {
        self.cq_size
    }

    /// The device.
    pub fn context(&self) -> &Context<N>
        no_unwind
    {
        &self.ctx
    }

}

impl<C, N: Device> Hw<C, N> {
    #[verifier::spinoff_prover]
    #[verifier::rlimit(30)]
    pub fn create_qp(&mut self, pd: &Pd<N>, domain: Option<&N::Domain>, qp_type: QpType, caps: QpCaps, max_batch: u32) -> (r: Result<usize>)
        requires
            old(self).inv(),
            domain matches Some(d) ==> domain_over(*d) == pd_id(*pd),
        ensures
            final(self).inv(),
            final(self).moved_from(old(self)),
            r matches Ok(index) ==> index < MAX_QPS,
        no_unwind
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
        if !same_device(&self.ctx, pd) {
            return Err(Error::Invalid("a protection domain of another device"));
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
            return Err(Error::Invalid("an engine holds at most 65536 QPs"));
        }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        // Its protection domain exists in M.
        pd_alive(&self.ctx, pd, Tracked(self.token.borrow_mut()));
        let ghost t1 = self.token@;
        let ghost m1 = t1.nic();
        let ghost q = t1.name(self.next_name@);
        let ghost attr = qp_attr(pd_of(*pd), qp_type, sh.cq, caps);
        proof {
            start.lemma_rely(t0, m0, m1);
            start.lemma_same_token(t0, t1, m1);
            // The name is fresh, the depths fit the device and the CQ.
            let cq = m1.cqs[sh.cq];
            let cqs1 = m1.cqs.insert(attr.send_cq, cq.reserve(q, attr.sq_depth));
            let cqs2 = cqs1.insert(attr.recv_cq, cqs1[attr.recv_cq].reserve(q, attr.rq_depth));
            safeverbs_spec::nic_proofs::lemma_copies(q, attr.sq_depth);
            safeverbs_spec::nic_proofs::lemma_copies(q, attr.rq_depth);
            broadcast use vstd::multiset::group_multiset_axioms;
            assert(cqs2[sh.cq].reserved.len() == cq.reserved.len() + attr.sq_depth + attr.rq_depth);
            assert(nic_enabled(m1, Nic::Step::create_qp(q, attr)));
        }
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
                proof {
                    start.lemma_rely(t0, m0, m1);
                    start.lemma_same_token(t0, t1, m1);
                    start.lemma_rely(t1, m1, self.token@.nic());
                    start.lemma_same_token(t1, self.token@, self.token@.nic());
                    self.lemma_inv_fields(&start, self.token@, self.token@.nic());
                    let g1 = refines::lemma_linked_rely(start.qps@, start.next_name@, t0, m0, m1, start.pending@, pinned(t0), start.refined@);
                    let g2 = refines::lemma_linked_rely(start.qps@, start.next_name@, t1, m1, self.token@.nic(), start.pending@, pinned(t1), g1);
                    g2.lemma_extends_trans(g1, start.refined@);
                    lemma_nic_reaches_trans(m0, m1, self.token@.nic());
                    self.refined@ = g2;
                }
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
            pd: clone_pd(pd),
            id: Ghost(q),
            spare_wqes: vec_new(),
            spare_sges: vec_new(),
            spare_leases: vec_new(),
            building: vec_new(),
            send_tails: Ghost(Map::empty()),
        };
        let ghost before = *self;
        if index == self.qps.len() {
            vec_push(&mut self.qps, Some(entry));
        } else {
            let _none = replace_value(&mut self.qps[index], Some(entry));
        }
        self.reserved += need as u32;
        self.next_name = Ghost(self.next_name@ + 1);
        proof {
            let t = self.token@;
            let mid = choose|mid: Nic::State| rely_reaches(sh, pinned(t1), m1, mid) && nic_step(mid, t.nic(), Nic::Step::create_qp(q, attr));
            let n = t.nic();
            start.lemma_rely(t0, m0, m1);
            start.lemma_same_token(t0, t1, m1);
            start.lemma_rely(t1, m1, mid);
            start.lemma_same_token(t1, t, mid);
            before.lemma_inv_fields(&start, t, mid);
            lemma_nic_step_is_next(mid, n, Nic::Step::create_qp(q, attr));
            lemma_step_keeps_invariant(mid, n);
            lemma_mine_keeps_share_wf(sh, pinned(t1), mid, n, Nic::Step::create_qp(q, attr));
            safeverbs_spec::nic_proofs::lemma_copies(q, attr.sq_depth);
            safeverbs_spec::nic_proofs::lemma_copies(q, attr.rq_depth);
            outcomes::lemma_recv_empty(self.qps@[index as int]->0.recv, n, q);
            send_outcomes::lemma_implied_empty(self.qps@[index as int]->0.send);
            send_outcomes::lemma_results_empty(self.qps@[index as int]->0.send, n, q);
            broadcast use vstd::multiset::group_multiset_axioms;
            // The new QP is not one of the table's: those exist in M already.
            assert forall|i: int| 0 <= i < before.qps@.len() && #[trigger] before.qps@[i] is Some implies before.qps@[i]->0.id() != q by {
                assert(qp_ok(before.qps@[i], i, t, mid));
            }
            assert(!mid.qps.contains_key(q));
            assert(!writes(t, q)) by {
                if writes(t, q) {
                    let i = choose|i: int| 0 <= i < before.qps@.len() && #[trigger] before.qps@[i] is Some && before.qps@[i]->0.id() == q;
                    assert(qp_ok(before.qps@[i], i, t, mid));
                }
            }
            assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] qp_ok(self.qps@[i], i, t, n) by {
                if i == index {
                    let e = self.qps@[i]->0;
                    assert(e.id() == q);
                    assert(n.qps[q].attr == attr);
                } else {
                    assert(self.qps@[i] == before.qps@[i]);
                    assert(qp_ok(before.qps@[i], i, t, mid));
                    if let Some(e) = self.qps@[i] {
                        lemma_ok_same(e, i as u16, mid, n, sh, t.started(e.id()), t.written(e.id()));
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
            // No CQE read names the new entry; the others are as they were.
            assert forall|j: int| 0 <= j < self.pending@.len() implies cqe_index(#[trigger] self.pending@[j]) != index by {
                assert(cqe_ok(before.qps@, mid, self.pending@[j]));
            }
            assert forall|i: int| 0 <= i < before.qps@.len() && i != index implies #[trigger] same_for_cqes(before.qps@[i], self.qps@[i], mid, n) by {
                assert(qp_ok(before.qps@[i], i, t, mid));
                assert(self.qps@[i] == before.qps@[i]);
            }
            lemma_pending_but_one(before.qps@, self.qps@, mid, n, self.pending@, index as int);
            // A's `create_qp`.
            let g1 = refines::lemma_linked_rely(start.qps@, start.next_name@, t0, m0, m1, start.pending@, pinned(t0), start.refined@);
            let g2 = refines::lemma_linked_rely(start.qps@, start.next_name@, t1, m1, mid, start.pending@, pinned(t1), g1);
            refines::lemma_verb_keeps(start.qps@, start.next_name@, t, mid, n, start.pending@, q);
            let g3 = refines::lemma_linked_verb(start.qps@, mid, n, Nic::Step::create_qp(q, attr), g2);
            let g4 = refines::lemma_linked_created(start.qps@, self.qps@, n, g3, index as int);
            g2.lemma_extends_trans(g1, start.refined@);
            g3.lemma_extends_trans(g2, start.refined@);
            g4.lemma_extends_trans(g3, start.refined@);
            lemma_nic_reaches_trans(m0, m1, mid);
            lemma_nic_reaches_trans(m0, mid, n);
            self.refined@ = g4;
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
        nic_reaches(m, n),
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
        lemma_nic_reaches_next(before, n);
        lemma_nic_reaches_trans(m, mid, before);
        lemma_nic_reaches_trans(m, before, n);
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
    /// After a step that changed only entry `index`'s QP in M, the entry, and the token's writing
    /// of that QP, the invariant holds again.
    #[verifier::spinoff_prover]
    proof fn lemma_one(&self, start: &Self, t0: NicToken, t: NicToken, mid: Nic::State, n: Nic::State, index: int)
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
            self.qps@[index]->0.same_config(start.qps@[index]->0),
            self.entry_ok(index, t, n),
            cq_of(self.cq@) == cq_of(start.cq@),
            self.caps == start.caps,
            self.cq_size == start.cq_size,
            self.reserved == start.reserved,
            self.next_name == start.next_name,
            self.pending == start.pending,
            n.qps.dom() =~= mid.qps.dom(),
            forall|p: QpId| #[trigger] mid.qps.contains_key(p) && p != start.qps@[index]->0.id() ==> n.qps[p] == mid.qps[p],
            n.caps == mid.caps,
            n.mrs == mid.mrs,
            n.retired == mid.retired,
            n.cqs[t.share().cq].cap == mid.cqs[t.share().cq].cap,
            n.cqs[t.share().cq].reserved == mid.cqs[t.share().cq].reserved,
            same_for_cqes(start.qps@[index], self.qps@[index], mid, n),
        ensures
            self.inv_with(t, n),
            self.same_table(start),
    {
        let q = start.qps@[index]->0.id();
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] qp_ok(self.qps@[i], i, t, n) by {
            if i != index {
                assert(qp_ok(start.qps@[i], i, t0, mid));
                if let Some(e) = self.qps@[i] {
                    assert(e.id() != q);
                    assert(t.started(e.id()) == t0.started(e.id()));
                    assert(t.written(e.id()) == t0.written(e.id()));
                    lemma_ok_same(e, i as u16, mid, n, t.share(), t.started(e.id()), t.written(e.id()));
                }
            }
        }
        completion::lemma_same_qps_but_one(start.qps@, self.qps@, index);
        assert forall|p: QpId| #[trigger] writes(t, p) implies exists|k: int|
            0 <= k < start.qps@.len() && #[trigger] start.qps@[k] is Some && start.qps@[k]->0.id() == p by {
            if p == q {
                assert(start.qps@[index] is Some);
            } else {
                assert(t.started(p) == t0.started(p));
                assert(t.written(p) == t0.written(p));
                assert(writes(t0, p));
            }
        }
        lemma_names_kept(start.qps@, self.qps@, self.next_name@, t0, t, mid, n);
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] same_for_cqes(start.qps@[i], self.qps@[i], mid, n) by {
            if i != index {
                assert(qp_ok(start.qps@[i], i, t0, mid));
                assert(self.qps@[i] == start.qps@[i]);
                if let Some(e) = self.qps@[i] {
                    assert(e.id() != q);
                }
            }
        }
        lemma_pending_same(start.qps@, self.qps@, mid, n, self.pending@);
    }

    #[verifier::spinoff_prover]
    pub fn set_error(&mut self, index: usize) -> (r: Result<()>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
        no_unwind
    {
        if index >= self.qps.len() {
            return Err(Error::Invalid("no such QP"));
        }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, t0, m0));
        }
        let r = match &mut self.qps[index] {
            Some(e) => {
                let ghost q = e.id();
                let r = to_error(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                if r.is_ok() {
                    e.phase = Phase::Ready;
                    e.takes_work = true;
                }
                r
            },
            None => {
                proof {
                    assert(self.qps@ =~= start.qps@);
                }
                return Err(Error::Invalid("no such QP"));
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
                assert(qp_ok(start.qps@[index as int], index as int, t, mid));
                let e = self.qps@[index as int]->0;
                let written = t.written(q);
                assert forall|j: int| 0 <= j < written.len() implies n.send_wr_ok(n.qps[q].attr, #[trigger] written[j]) by {
                    assert(mid.send_wr_ok(mid.qps[q].attr, written[j]));
                }
                lemma_queued_outcomes_step(mid, n, Nic::Step::modify_qp(q, QpState::Err), q);
                outcomes::lemma_recv_framed(e.recv, mid, n, q);
                send_outcomes::lemma_results_framed(e.send, mid, n, q);
                send_outcomes::lemma_bound_kept(e.send, e.send, e.send_tails@, mid.qps[q], n.qps[q], written, mid.retired, n.retired, q);
                assert(self.entry_ok(index as int, t, n));
                self.lemma_one(&start, t, t, mid, n, index as int);
                let g1 = refines::lemma_linked_rely(start.qps@, start.next_name@, t0, m0, mid, start.pending@, pinned(t0), start.refined@);
                refines::lemma_verb_keeps(start.qps@, start.next_name@, t, mid, n, start.pending@, q);
                let g2 = refines::lemma_linked_verb(start.qps@, mid, n, Nic::Step::modify_qp(q, QpState::Err), g1);
                refines::lemma_linked_same_rings(start.qps@, self.qps@, n, g2);
                g2.lemma_extends_trans(g1, start.refined@);
                lemma_nic_reaches_trans(m0, mid, n);
                self.refined@ = g2;
            } else {
                start.lemma_rely(t0, m0, n);
                start.lemma_same_token(t0, t, n);
                assert(qp_ok(start.qps@[index as int], index as int, t, n));
                self.lemma_one(&start, t, t, n, n, index as int);
                let g1 = refines::lemma_linked_rely(start.qps@, start.next_name@, t0, m0, n, start.pending@, pinned(t0), start.refined@);
                refines::lemma_linked_same_rings(start.qps@, self.qps@, n, g1);
                self.refined@ = g1;
            }
        }
        r
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
            self.qps@[index]->0.same_config(start.qps@[index]->0),
            self.qps@[index]->0.send == start.qps@[index]->0.send,
            self.qps@[index]->0.recv == start.qps@[index]->0.recv,
            self.entry_ok(index, t, n),
            self.cq == start.cq,
            self.caps == start.caps,
            self.cq_size == start.cq_size,
            self.reserved == start.reserved,
            self.next_name == start.next_name,
            self.pending == start.pending,
            forall|p: QpId| #[trigger] queued_outcomes_ok(m, p) ==> queued_outcomes_ok(n, p),
        ensures
            self.inv_with(t, n),
            self.same_table(start),
    {
        let q = start.qps@[index]->0.id();
        let sh = t.share();
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] qp_ok(self.qps@[i], i, t, n) by {
            if i != index {
                assert(qp_ok(start.qps@[i], i, t, m));
                if let Some(e) = self.qps@[i] {
                    let p = e.id();
                    assert(p != q);
                    assert(sh.owns.contains(p));
                    assert forall|w: WqeRef| w.qp == p && #[trigger] m.retired.contains_key(w)
                        implies n.retired.contains_key(w) && n.retired[w] == m.retired[w] by {
                        assert(sh.owns.contains(w.qp));
                    }
                    assert(queued_outcomes_ok(n, p));
                    e.lemma_framed(i as u16, t, m, n);
                }
            }
        }
        completion::lemma_same_qps_but_one(start.qps@, self.qps@, index);
        assert forall|w: WqeRef| #[trigger] n.retired.contains_key(w) && sh.owns.contains(w.qp)
            implies m.retired.contains_key(w) || m.qps.contains_key(w.qp) by {
            assert(sh.owns.contains(w.qp));
        }
        lemma_names_kept(start.qps@, self.qps@, self.next_name@, t, t, m, n);
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] same_for_cqes(start.qps@[i], self.qps@[i], m, n) by {
            assert(qp_ok(start.qps@[i], i, t, m));
            if i != index {
                assert(self.qps@[i] == start.qps@[i]);
            }
            if let Some(e) = start.qps@[i] {
                assert(sh.owns.contains(e.id()));
                assert forall|w: WqeRef| w.qp == e.id() && #[trigger] m.retired.contains_key(w)
                    implies n.retired.contains_key(w) && n.retired[w] == m.retired[w] by {
                    assert(sh.owns.contains(w.qp));
                }
                if i != index {
                    assert(e.id() != q);
                    assert(qp_kept(m.qps[e.id()], n.qps[e.id()]));
                }
            }
        }
        lemma_pending_same(start.qps@, self.qps@, m, n, self.pending@);
    }

    #[verifier::spinoff_prover]
    pub fn connect(&mut self, index: usize, attr: &ConnectAttr, remote: &Endpoint) -> (r: Result<()>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
        no_unwind
    {
        if index >= self.qps.len() {
            return Err(Error::Invalid("no such QP"));
        }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, t0, m0));
        }
        let phase = match &self.qps[index] {
            Some(e) => e.phase,
            None => Phase::Broken,
        };
        if !matches!(phase, Phase::Reset) {
            return Err(Error::Invalid("a QP connects once, from RESET"));
        }
        let r = match &mut self.qps[index] {
            Some(e) => {
                let ghost q = e.id();
                proof {
                    assert(phase_ok(e.phase, m0.qps[q].state));
                }
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
            assert(e0.ok(index as u16, m0, sh, t0.started(q), t0.written(q)));
            assert(phase_ok(e.phase, b.state));
            assert(sq_linked(e.send, index as u16, b));
            assert(rq_linked(e.recv, index as u16, b));
            assert(writing_ok(e.send, index as u16, n, b.attr, t0.started(q), written));
            outcomes::lemma_recv_framed(e.recv, m0, n, q);
            send_outcomes::lemma_results_framed(e.send, m0, n, q);
            assert forall|j: int| 0 <= j < b.sq.len() implies b.sq_head + j < a.sq_head + a.sq.len()
                && (#[trigger] b.sq[j]).wr == a.sq[b.sq_head + j - a.sq_head].wr by {
                assert(b.sq[j].wr == a.sq[j].wr);
            }
            send_outcomes::lemma_bound_kept(e.send, e.send, e.send_tails@, a, b, written, m0.retired, n.retired, q);
            assert(self.entry_ok(index as int, t0, n));
            self.lemma_but_one(&start, t0, m0, n, index as int);
            self.lemma_same_token(t0, t, n);
            assert forall|i: int| refines::has_entry(start.qps@, i)
                implies refines::keeps_queues(m0, n, #[trigger] start.qps@[i]->0.id()) by {
                assert(qp_ok(start.qps@[i], i, t0, m0));
                assert(sh.owns.contains(start.qps@[i]->0.id()));
            }
            let g = refines::lemma_linked_moved(start.qps@, m0, n, start.refined@);
            refines::lemma_linked_same_rings(start.qps@, self.qps@, n, g);
            self.refined@ = g;
        }
        r
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
            final(self).moved_from(old(self)),
            r matches Ok(Err(refused)) ==> refused.ctx == ctx,
            r matches Err((back, c)) ==> back == part && c == ctx,
        no_unwind
    {
        #[cfg(verus_keep_ghost)]
        let part = part.separate_from_table(&*self);
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        if index < self.qps.len() {
            proof {
                assert(qp_ok(start.qps@[index as int], index as int, t0, m0));
            }
            match &mut self.qps[index] {
                Some(e) => {
                    // One comparison each: the QP takes work, the buffer is of its protection
                    // domain and may be written, the queue has room.
                    #[allow(clippy::collapsible_match)] // Explicit for Verus.
                    if e.takes_work && part_pd_of(&part) == e.pd_id && part_writable_of(&part) && e.recv.can_post() {
                        let ghost q = e.id();
                        let ghost e0 = *e;
                        // The buffer's region is registered (its lease says so).
                        registered(&part, &e.pd, Tracked(self.token.borrow_mut()));
                        let ghost t1 = self.token@;
                        let ghost m1 = t1.nic();
                        let ghost g1 = refines::lemma_linked_rely(start.qps@, start.next_name@, t0, m0, m1, start.pending@, pinned(t0), start.refined@);
                        proof {
                            start.lemma_rely(t0, m0, m1);
                            start.lemma_same_token(t0, t1, m1);
                            assert(qp_ok(start.qps@[index as int], index as int, t1, m1));
                            e0.recv.lemma_wqes();
                        }
                        let sge = part_sge_of(&part);
                        let wqe = e.recv.next_wqe();
                        let id = wr_id(index as u16, true, wqe);
                        let sges = one_of(&sge);
                        let ghost part_of_one = part;
                        // The slot first: from then on it keeps the memory alive.
                        let mut leases = Leases::one(part, Ghost(safeverbs_spec::abs::Dir::In));
                        let ghost entry = crate::buf::WqeLoans {
                            wr_id: id, opcode: None, sges: sges@, first: 0,
                        };
                        proof {
                            assert(sges@ =~= seq![sge]);
                            assert(entry.matches(leases.loans()));
                        }
                        leases.record_wqe(Ghost(entry));
                        let detached = ctx.is_none();
                        let implied = Ok(Completed::Received(Received::Message { len: sge.length, imm: None }));
                        let ghost named = start.lemma_name_one(m1, g1, leases, part_of_one);
                        let payload = Payload { ctx, leases, holds: Ghost(named.1) };
                        let ghost resource = payload;
                        proof {
                            lemma_ledger_entry(resource.leases, 0);
                            let wr = recv_wr(id, sges@);
                            assert(wr.sges[0] == sge_model(sge));
                            assert(m1.recv_wr_ok(m1.qps[q].attr, wr));
                            assert(nic_enabled(m1, Nic::Step::post_recv(q, seq![wr])));
                        }
                        e.recv.fill(1, detached, implied, payload);
                        let ghost filled = e.recv;
                        match post_recv(&mut e.hw, id, sges, Ghost(q), Tracked(self.token.borrow_mut())) {
                            Ok(()) => {
                                let seq = e.recv.stage_filled(1, true);
                                let ghost staged = e.recv;
                                e.recv.commit();
                                proof {
                                    staged.lemma_filled_staged(&filled, &e0.recv, 1, crate::ring::ReqView::new(detached, resource), implied);
                                    let g = self.lemma_recv_posted(&start, t1, index as int, seq, staged, resource, detached, implied, id, sges@, named.0);
                                    named.0.lemma_extends_trans(g1, start.refined@);
                                    g.lemma_extends_trans(named.0, start.refined@);
                                    lemma_nic_reaches_trans(m0, m1, self.token@.nic());
                                    self.refined@ = g;
                                }
                                return Ok(Ok(seq));
                            },
                            Err(error) => {
                                let refused = not_posted(&mut e.recv, error);
                                proof {
                                    self.lemma_not_posted(&start, t1, index as int);
                                    let g = refines::lemma_linked_rely(start.qps@, start.next_name@, t1, m1, self.token@.nic(), start.pending@, pinned(t1), g1);
                                    refines::lemma_linked_same_view(start.qps@, self.qps@, self.token@.nic(), g, index as int, Queue::Recv);
                                    g.lemma_extends_trans(g1, start.refined@);
                                    lemma_nic_reaches_trans(m0, m1, self.token@.nic());
                                    self.refined@ = g;
                                }
                                return Ok(Err(refused));
                            },
                        }
                    }
                },
                None => {},
            }
        }
        proof {
            assert(self.qps@ =~= start.qps@);
        }
        Err((part, ctx))
    }

    /// What a receive posted leaves: the WQE M has more is its request's, in flight.
    #[verifier::spinoff_prover]
    proof fn lemma_recv_posted(&self, start: &Self, t1: NicToken, index: int, seq: RingSeq, staged: Ring<Payload<C, N>>, resource: Payload<C, N>, detached: bool, implied: CompletionResult, id: u64, sges: Seq<Sge>, g: refines::Refinement) -> (g2: refines::Refinement)
        requires
            start.inv_with(t1, t1.nic()),
            start.has_qp(index),
            refines::linked(start.qps@, t1.nic(), g),
            may_post_holds(g.abs, resource.holds@),
            forall|l: safeverbs_spec::abs::LeaseId| #[trigger] resource.holds@.contains_key(l)
                ==> free_lease(refines::table_view(start.qps@, t1.nic()), g.abs, refines::lent_holds(), l),
            roots_outside(refines::table_view(start.qps@, t1.nic()), g.abs, refines::lent_holds(), resource.holds@),
            ({
                let e0 = start.qps@[index]->0;
                let e = self.qps@[index]->0;
                let wr = recv_wr(id, sges);
                let t = self.token@;
                let pins = pinned(t1).union(recv_keys(seq![wr]));
                &&& t.same_engine(&t1)
                &&& t.same_writings(&t1)
                &&& exists|mid: Nic::State| rely_reaches(t1.share(), pins, t1.nic(), mid)
                    && nic_step(mid, t.nic(), Nic::Step::post_recv(e0.id(), seq![wr]))
                &&& self.qps@.len() == start.qps@.len()
                &&& forall|i: int| 0 <= i < self.qps@.len() && i != index ==> #[trigger] self.qps@[i] == start.qps@[i]
                &&& self.qps@[index] is Some
                &&& e.same_qp(e0)
                &&& e0.phase == Phase::Ready
                &&& e.started == e0.started
                &&& e.send == e0.send
                &&& e.send_tails == e0.send_tails
                &&& seq == e0.recv.spec_posted()
                &&& staged.just_staged(&e0.recv, 1, crate::ring::ReqView::new(detached, resource), implied)
                &&& e.recv.just_committed(&staged)
                &&& payload_ok(resource, detached)
                &&& implied matches Ok(Completed::Received(Received::Message { .. }))
                &&& id == spec_wr_id(index as u16, true, e0.recv.spec_wqe_posted())
                &&& ledger_has(resource.leases, id, None, wr.sges)
            }),
            cq_of(self.cq@) == t1.share().cq,
            self.caps == start.caps,
            self.cq_size == start.cq_size,
            self.reserved == start.reserved,
            self.next_name == start.next_name,
            self.pending == start.pending,
        ensures
            self.inv_with(self.token@, self.token@.nic()),
            self.same_table(start),
            self.qps@[index]->0.recv.req(seq as int) == Some(crate::ring::ReqView::new(detached, resource)),
            refines::linked(self.qps@, self.token@.nic(), g2),
            g2.extends(g),
            nic_reaches(t1.nic(), self.token@.nic()),
    {
        let e0 = start.qps@[index]->0;
        let e = self.qps@[index]->0;
        let q = e0.id();
        let t = self.token@;
        let n = t.nic();
        let sh = t1.share();
        let m1 = t1.nic();
        let wr = recv_wr(id, sges);
        let pins = pinned(t1).union(recv_keys(seq![wr]));
        let mid = choose|mid: Nic::State| rely_reaches(sh, pins, m1, mid) && nic_step(mid, n, Nic::Step::post_recv(q, seq![wr]));
        lemma_rely_fewer_pins(sh, pins, pinned(t1), m1, mid);
        start.lemma_rely(t1, m1, mid);
        assert(qp_ok(start.qps@[index], index, t1, mid));
        lemma_nic_step_is_next(mid, n, Nic::Step::post_recv(q, seq![wr]));
        lemma_step_keeps_invariant(mid, n);
        lemma_mine_keeps_share_wf(sh, pins, mid, n, Nic::Step::post_recv(q, seq![wr]));
        assert(qp_ok(start.qps@[index], index, t1, mid));
        let (a, b) = (mid.qps[q], n.qps[q]);
        let i = index as u16;
        e0.recv.lemma_wqes();
        staged.lemma_wqes();
        e.recv.lemma_wqes();
        e0.recv.lemma_view();
        assert forall|k: int| 0 <= k < b.rq.len() implies (#[trigger] b.rq[k]).wr.wr_id == spec_wr_id(i, true, (b.rq_head + k) as u64) by {
            if k < a.rq.len() {
                assert(b.rq[k] == a.rq[k]);
            }
        }
        assert(rq_linked(e.recv, i, b));
        lemma_queued_outcomes_step(mid, n, Nic::Step::post_recv(q, seq![wr]), q);
        outcomes::lemma_recv_framed(e0.recv, mid, n, q);
        assert forall|s: int| #[trigger] e.recv.req(s) is Some && s != seq as int implies e0.recv.req(s) is Some
            && e.recv.req(s)->0.phase == e0.recv.req(s)->0.phase && e.recv.meta(s) == e0.recv.meta(s) by {
            assert(e0.recv.staged(s) is None) by {
                outcomes::lemma_recv_no_staged(e0.recv);
            }
        }
        outcomes::lemma_recv_added(e0.recv, e.recv, n, q, seq as int);
        assert(keeps_leases(e.recv, e0.recv)) by {
            assert forall|s: int, w: int| e0.recv.dma(s, w) is Some && e.recv.spec_wqe_retired() <= w < e.recv.spec_wqe_posted()
                implies #[trigger] e.recv.dma(s, w) is Some && e.recv.dma(s, w)->0.leases == e0.recv.dma(s, w)->0.leases by {
                e0.recv.lemma_dma(s, w);
            }
        }
        assert forall|k: int| 0 <= k < b.rq.len() implies
            ring_backed(e.recv, b.rq_head + k, (#[trigger] b.rq[k]).wr.wr_id, None, b.rq[k].wr.sges) by {
            if k < a.rq.len() {
                assert(b.rq[k] == a.rq[k]);
                lemma_backed_kept(e0.recv, e.recv, b.rq_head + k, b.rq[k].wr.wr_id, None, b.rq[k].wr.sges);
            } else {
                let s = seq as int;
                assert(e.recv.dma(s, b.rq_head + k) is Some);
                assert(ledger_has(e.recv.dma(s, b.rq_head + k)->0.leases, wr.wr_id, None, wr.sges));
            }
        }
        let written = t1.written(q);
        assert forall|j: int| 0 <= j < written.len() implies n.send_wr_ok(b.attr, #[trigger] written[j]) by {
            assert(mid.send_wr_ok(a.attr, written[j]));
        }
        assert forall|j: int| 0 <= j < written.len() implies
            written_backed(e, e.send.spec_wqe_committed() + j, #[trigger] written[j]) by {
            assert(written_backed(e0, e0.send.spec_wqe_committed() + j, written[j]));
        }
        send_outcomes::lemma_results_framed(e.send, mid, n, q);
        assert forall|j: int| 0 <= j < b.sq.len() implies b.sq_head + j < a.sq_head + a.sq.len()
            && (#[trigger] b.sq[j]).wr == a.sq[b.sq_head + j - a.sq_head].wr by {
        }
        send_outcomes::lemma_bound_kept(e.send, e.send, e.send_tails@, a, b, written, mid.retired, n.retired, q);
        assert(ring_resources_ok(e.recv)) by {
            lemma_resources_added(e0.recv, staged, seq as int, resource, detached);
            assert forall|s: int| #[trigger] e.recv.req(s) is Some implies resource_at(staged, s, e.recv.req(s)->0.leases, e.recv.req(s)->0.detached) by {}
            assert forall|s: int| #[trigger] e.recv.refused(s) is Some implies resource_at(staged, s, e.recv.refused(s)->0, false) by {}
            lemma_resources_kept(staged, e.recv);
        }
        assert(e.ok(i, n, sh, t1.started(q), written));
        assert(self.entry_ok(index, t1, n));
        self.lemma_one(start, t1, t, mid, n, index);
        // A's `post_request`.
        let g1 = refines::lemma_linked_rely(start.qps@, start.next_name@, t1, m1, mid, start.pending@, pinned(t1), g);
        refines::lemma_roots_moved(refines::table_view(start.qps@, m1), g.abs, refines::table_view(start.qps@, mid), g1.abs, resource.holds@);
        refines::lemma_recv_queued(mid, n, q, seq![wr]);
        refines::lemma_verb_keeps(start.qps@, start.next_name@, t1, mid, n, start.pending@, q);
        let g2 = refines::lemma_linked_one_posted(start.qps@, self.qps@, mid, n, Nic::Step::post_recv(q, seq![wr]), g1, index, Queue::Recv, staged, 1, crate::ring::ReqView::new(detached, resource), implied);
        g2.lemma_extends_trans(g1, g);
        lemma_nic_reaches_trans(m1, mid, n);
        g2
    }

    /// What a doorbell or a receive the device refused leaves: nothing M sees, and the slot free.
    #[verifier::spinoff_prover]
    proof fn lemma_not_posted(&self, start: &Self, t1: NicToken, index: int)
        requires
            start.inv_with(t1, t1.nic()),
            start.has_qp(index),
            ({
                let e0 = start.qps@[index]->0;
                let e = self.qps@[index]->0;
                let t = self.token@;
                &&& t.same_engine(&t1)
                &&& t.same_writings(&t1)
                &&& rely_reaches(t1.share(), pinned(t1), t1.nic(), t.nic())
                &&& self.qps@.len() == start.qps@.len()
                &&& forall|i: int| 0 <= i < self.qps@.len() && i != index ==> #[trigger] self.qps@[i] == start.qps@[i]
                &&& self.qps@[index] is Some
                &&& e.same_qp(e0)
                &&& e.started == e0.started
                &&& e.send_tails == e0.send_tails
                &&& e.send.inv()
                &&& e.send.same_but_free(&e0.send)
                &&& e.recv.inv()
                &&& e.recv.same_but_free(&e0.recv)
            }),
            cq_of(self.cq@) == t1.share().cq,
            self.caps == start.caps,
            self.cq_size == start.cq_size,
            self.reserved == start.reserved,
            self.next_name == start.next_name,
            self.pending == start.pending,
        ensures
            self.inv_with(self.token@, self.token@.nic()),
            self.same_table(start),
    {
        let e0 = start.qps@[index]->0;
        let e = self.qps@[index]->0;
        let t = self.token@;
        let n = t.nic();
        start.lemma_rely(t1, t1.nic(), n);
        lemma_rely_frame(t1.share(), pinned(t1), t1.nic(), n);
        start.lemma_same_token(t1, t, n);
        assert(qp_ok(start.qps@[index], index, t, n));
        completion::lemma_same_kept(e.send, e0.send);
        completion::lemma_same_kept(e.recv, e0.recv);
        e.lemma_kept(&e0, index as u16, n, t.share(), t.written(e0.id()));
        assert(self.entry_ok(index, t, n));
        self.lemma_one(start, t, t, n, n, index);
    }

    /// Why a receive is refused, on QP `index`: it takes no work (`Error::NotConnected`), the
    /// part is of another protection domain or frozen, or the receive queue is full.
    #[cold]
    #[inline(never)]
    pub fn recv_refused(&mut self, index: usize, part: Part<N>, ctx: Option<C>) -> (r: Refusal<C, N>)
        requires
            old(self).inv(),
        ensures
            *final(self) == *old(self),
            r.ctx == ctx,
        no_unwind
    {
        if index >= self.qps.len() {
            return Refusal { error: Error::Invalid("no such QP"), ctx, leases: Leases::one(part, Ghost(safeverbs_spec::abs::Dir::Out)) };
        }
        proof {
            assert(qp_ok(self.qps@[index as int], index as int, self.token@, self.token@.nic()));
        }
        let error = match &self.qps[index] {
            Some(e) => {
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

impl<C, N: Device> Hw<C, N> {
    /// Routes one completion to its QP's ring: its `wr_id` names the QP's index, the queue and the
    /// WQE, which the ring retires (with every earlier request it ends).
    #[inline(always)]
    #[verifier::spinoff_prover]
    #[allow(clippy::ptr_arg)] // Vstd specifies indexing a `Vec`.
    #[allow(clippy::too_many_arguments)] // The ghost ones are erased.
    fn dispatch(qps: &mut Vec<Option<HwQp<C, N>>>, wc: &Wc, wakes: &mut WakeBatch, Ghost(name): Ghost<nat>, Ghost(t): Ghost<NicToken>, Ghost(m): Ghost<Nic::State>, Ghost(pending): Ghost<Seq<Cqe>>, Ghost(g): Ghost<refines::Refinement>)
        requires
            table_ok(old(qps)@, name, t, m, pending),
            pending.len() > 0,
            reports(*wc, pending[0]),
            refines::linked(old(qps)@, m, g),
        ensures
            table_ok(final(qps)@, name, t, m, pending.drop_first()),
            same_qps(final(qps)@, old(qps)@),
            refines::linked(final(qps)@, m, g),
        no_unwind
    {
        let ghost start = qps@;
        proof {
            completion::lemma_next_cqe(start, name, t, m, pending, *wc);
            assert(qp_ok(start[cqe_index(pending[0])], cqe_index(pending[0]), t, m));
        }
        let index = index_of(wc.wr_id);
        if index >= qps.len() {
            return;
        }
        match &mut qps[index] {
            Some(qp_entry) => qp_entry.complete_cqe(wc, wakes, Ghost(pending[0])),
            None => {},
        }
        proof {
            completion::lemma_table_walked(start, qps@, name, t, m, pending, *wc, qps@[index as int]->0, true);
            assert(cqe_ok(start, m, pending[0]));
            refines::lemma_linked_walked(start, qps@, m, g, index as int, pending[0].queue, pending[0].seq as u64, *wc, true);
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
            final(self).moved_from(old(self)),
        no_unwind
    {
        let ghost start = *self;
        let mut n = self.polled_len - self.polled_at;
        while self.polled_at < self.polled_len
            invariant
                self.inv(),
                same_qps(self.qps@, start.qps@),
                self.caps == start.caps,
                n <= POLL_MAX,
                self.refined == start.refined,
                self.token == start.token,
            decreases self.polled_len - self.polled_at,
        {
            proof {
                assert(reports(self.polled@[self.polled_at + 0], self.pending@[0]));
            }
            let ghost qps = self.qps@;
            let ghost before = *self;
            Self::dispatch(&mut self.qps, &self.polled[self.polled_at], &mut self.panics, Ghost(self.next_name@), Ghost(self.token@), Ghost(self.token@.nic()), Ghost(self.pending@), Ghost(self.refined@));
            self.polled_at += 1;
            self.pending = Ghost(self.pending@.drop_first());
            proof {
                completion::lemma_same_qps_trans(self.qps@, qps, start.qps@);
                self.lemma_polled_moved(&before);
            }
        }
        self.polled_at = 0;
        self.polled_len = 0;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost before_poll = *self;
        let read = poll(&mut self.cq, &mut self.polled, POLL_BATCH, Tracked(self.token.borrow_mut()));
        let ghost t = self.token@;
        let ghost m = t.nic();
        let ghost wcs = self.polled@.take(read as int);
        let ghost cqes = completion::lemma_table_polled(self.qps@, self.next_name@, t0, m0, m, wcs, self.pending@);
        let ghost refined = refines::lemma_linked_polled(self.qps@, self.next_name@, t0, m0, m, wcs, self.pending@, self.refined@);
        proof {
            assert(self.pending@ + cqes =~= cqes);
            lemma_table_token(self.qps@, self.next_name@, t0, t, m, cqes);
            assert(cqes.skip(0) =~= cqes);
            self.refined@ = refined;
            self.lemma_moved_trans(&before_poll, &start);
        }
        let mut i = 0;
        while i < read
            invariant
                0 <= i <= read <= POLL_BATCH,
                wcs == self.polled@.take(read as int),
                cqes.len() == read,
                forall|j: int| 0 <= j < cqes.len() ==> reports(wcs[j], #[trigger] cqes[j]),
                table_ok(self.qps@, self.next_name@, t, m, cqes.skip(i as int)),
                t == self.token@,
                m == t.nic(),
                n <= POLL_MAX,
                same_qps(self.qps@, start.qps@),
                self.caps == start.caps,
                self.polled_at == 0,
                self.polled_len == 0,
                self.pending@.len() == 0,
                cq_of(self.cq@) == t.share().cq,
                m.cqs[t.share().cq].cap == self.cq_size,
                m.cqs[t.share().cq].reserved.len() == self.reserved,
                self.reserved <= self.cq_size,
                caps_model(m.caps, self.caps),
                refines::linked(self.qps@, m, self.refined@),
                self.moved_from(&start),
            decreases read - i,
        {
            proof {
                assert(wcs[i as int] == self.polled@[i as int]);
                assert(cqes.skip(i as int)[0] == cqes[i as int]);
                assert(reports(wcs[i as int], cqes[i as int]));
                assert(cqes.skip(i as int).drop_first() =~= cqes.skip(i + 1));
            }
            let ghost qps = self.qps@;
            let ghost before = *self;
            Self::dispatch(&mut self.qps, &self.polled[i], &mut self.panics, Ghost(self.next_name@), Ghost(t), Ghost(m), Ghost(cqes.skip(i as int)), Ghost(self.refined@));
            i += 1;
            proof {
                completion::lemma_same_qps_trans(self.qps@, qps, start.qps@);
            }
        }
        proof {
            assert(cqes.skip(read as int) =~= self.pending@);
        }
        n += read;
        // Results wait in the rings, if the event loop wants them.
        self.kept = true;
        self.kept_at = 0;
        n
    }

    /// The first half of the event loop's poll: reads at most `n` CQEs ([`POLL_MAX`] at most) and
    /// updates the CQ's consumer index, unless some read before are not routed yet. Then
    /// [`Hw::next_completion`] hands out what they retire, while the program may post anything:
    /// the CQ's capacity is safe, since the CQEs have left it. Returns how many it read.
    #[inline(always)]
    #[verifier::spinoff_prover]
    pub fn read_completions(&mut self, n: usize) -> (read: usize)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
        no_unwind
    {
        if self.polled_at < self.polled_len || self.kept {
            return 0;
        }
        let limit = if n < POLL_MAX { n } else { POLL_MAX };
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let read = poll(&mut self.cq, &mut self.polled, limit, Tracked(self.token.borrow_mut()));
        self.polled_at = 0;
        self.polled_len = read;
        proof {
            let t = self.token@;
            let wcs = self.polled@.take(read as int);
            let cqes = completion::lemma_table_polled(self.qps@, self.next_name@, t0, m0, t.nic(), wcs, self.pending@);
            assert(self.pending@ + cqes =~= cqes);
            lemma_table_token(self.qps@, self.next_name@, t0, t, t.nic(), cqes);
            self.refined@ = refines::lemma_linked_polled(self.qps@, self.next_name@, t0, m0, t.nic(), wcs, self.pending@, self.refined@);
            self.pending = Ghost(cqes);
            assert forall|j: int| 0 <= j < cqes.len()
                implies reports(self.polled@[self.polled_at + j], #[trigger] cqes[j]) by {
                assert(wcs[j] == self.polled@[j]);
            }
        }
        read
    }

    /// The second half of the event loop's poll: the next request that completed with an owner,
    /// in each queue's order, from the results waiting in the rings and then, in one pass, from
    /// the CQEs read: each is routed, its ring walked up to it, and what that retires handed out.
    /// `None` when nothing is left. A panic of a waker registered on the requests resumes then.
    #[inline(always)]
    #[verifier::spinoff_prover]
    pub fn next_completion(&mut self) -> (r: Option<Completion<C, N>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).keeps_requests(old(self)),
            final(self).moved_from(old(self)),
            r matches Some(c) ==> old(self).hands_out(c),
        no_unwind
    {
        let ghost start = *self;
        proof {
            self.lemma_keeps_requests_refl();
        }
        while self.polled_at < self.polled_len
            invariant
                self.inv(),
                self.keeps_requests(&start),
                self.moved_from(&start),
                start == *old(self),
            decreases self.pending@.len(), self.walk_to_go(),
        {
            let at = self.polled_at;
            let wc = &self.polled[at];
            let id = wc.wr_id;
            let index = index_of(id);
            let ghost pre = *self;
            let ghost cqe = pre.pending@[0];
            proof {
                assert(reports(pre.polled@[pre.polled_at + 0], pre.pending@[0]));
                completion::lemma_next_cqe(pre.qps@, pre.next_name@, pre.token@, pre.nic(), pre.pending@, *wc);
                assert(qp_ok(pre.qps@[cqe_index(cqe)], cqe_index(cqe), pre.token@, pre.nic()));
            }
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
            proof {
                assert((cqe.seq as u64) & WQE_MASK == id & WQE_MASK);
                assert(wqe == cqe.seq);
            }
            if !ring.wqe_in_flight(wqe) {
                self.polled_at = at + 1;
                continue;
            }
            let seq = ring.retired();
            let qp_num = e.qp_num;
            let ghost before_ring = *ring;
            match ring.retire_oldest(recv, wqe, wc) {
                Some(slot) => {
                    let ghost spent = slot.end_wqe == wqe + 1;
                    let ghost out = slot.outcome;
                    proof {
                        pre.lemma_retired_reports(seq as int, *wc);
                    }
                    if slot.end_is(wqe + 1) {
                        self.polled_at = at + 1;
                    }
                    let result = slot.outcome;
                    match slot.take_leases() {
                        Some(Payload { ctx: Some(wr_id), leases, .. }) => {
                            let queue = if recv { Side::Recv } else { Side::Send };
                            self.pending = Ghost(if spent { pre.pending@.drop_first() } else { pre.pending@ });
                            let c = Completion { wr_id, result, qp: index, qp_num, queue, seq, leases };
                            proof {
                                let x = self.qps@[index as int]->0;
                                ring_of(x, cqe.queue).lemma_retired_and_taken(&before_ring);
                                self.lemma_oldest_done(&pre, &start, *wc, spent, c);
                                self.refined@ = self.lemma_oldest_refined(&pre, *wc);
                                self.lemma_moved_trans(&pre, &start);
                            }
                            return Some(c);
                        },
                        // A request with an owner holds a context, so nothing is lost here.
                        other => forget_free(other, Ghost(false)),
                    }
                },
                None => {
                    proof {
                        assert(self.qps@ =~= pre.qps@);
                        self.lemma_untouched(&pre);
                        self.lemma_keeps_requests_trans(&pre, &start);
                    }
                    let ghost mid = *self;
                    let other = self.next_general(at, index, recv, wqe);
                    proof {
                        mid.lemma_keeps_requests_refl();
                        self.lemma_keeps_requests_trans(&mid, &start);
                        self.lemma_moved_trans(&mid, &start);
                        if other is Some {
                            mid.lemma_hands_out_earlier(&start, other->0);
                        }
                    }
                    if other.is_some() {
                        return other;
                    }
                },
            }
        }
        if self.kept {
            let ghost mid = *self;
            let kept = self.next_kept();
            proof {
                self.lemma_keeps_requests_trans(&mid, &start);
                self.lemma_moved_trans(&mid, &start);
                if kept is Some {
                    mid.lemma_hands_out_earlier(&start, kept->0);
                }
            }
            if kept.is_some() {
                return kept;
            }
        }
        None
    }

    /// How many requests the walk of the next CQE may yet retire: those of its ring in flight.
    closed spec fn walk_to_go(&self) -> nat {
        if self.pending@.len() == 0 {
            0
        } else {
            let e = self.pending@[0];
            let r = ring_of(self.qps@[cqe_index(e)]->0, e.queue);
            (r.spec_committed() - r.spec_retired()) as nat
        }
    }

    /// What [`Hw::next_completion`] does with a CQE out of the common: one step of its walk, on the
    /// oldest request of its queue, which retires it if it ends at or before the CQE's WQE, and
    /// hands it out if it has an owner. A CQE that ends inside it is spent.
    #[inline(never)]
    #[verifier::spinoff_prover]
    fn next_general(&mut self, at: usize, index: usize, recv: bool, wqe: u64) -> (r: Option<Completion<C, N>>)
        requires
            old(self).inv(),
            old(self).pending@.len() > 0,
            at == old(self).polled_at,
            index == cqe_index(old(self).pending@[0]),
            recv == (old(self).pending@[0].queue is Recv),
            wqe == old(self).pending@[0].seq,
        ensures
            final(self).inv(),
            final(self).keeps_requests(old(self)),
            final(self).moved_from(old(self)),
            final(self).pending@.len() < old(self).pending@.len()
                || (final(self).pending@ == old(self).pending@ && final(self).walk_to_go() < old(self).walk_to_go()),
            r matches Some(c) ==> old(self).hands_out(c),
        no_unwind
    {
        let ghost pre = *self;
        let ghost y = pre.qps@[index as int]->0;
        proof {
            assert(reports(pre.polled@[pre.polled_at + 0], pre.pending@[0]));
            completion::lemma_next_cqe(pre.qps@, pre.next_name@, pre.token@, pre.nic(), pre.pending@, pre.polled@[pre.polled_at + 0]);
            assert(qp_ok(pre.qps@[index as int], index as int, pre.token@, pre.nic()));
        }
        let wc = &self.polled[at];
        let e = match &mut self.qps[index] {
            Some(e) => e,
            None => return None,
        };
        let (ring, queue) = if recv { (&mut e.recv, Side::Recv) } else { (&mut e.send, Side::Send) };
        let seq = ring.retired();
        let ghost before = *ring;
        let stepped = ring.retire_next(recv, wqe, wc, &mut self.panics);
        let ghost walked = *ring;
        let ghost spent = stepped is None || stepped->0;
        proof {
            walked.lemma_completed_walked(&before, recv, wqe, *wc, spent);
        }
        match stepped {
            Some(ended) => {
                if ended {
                    self.polled_at = at + 1;
                    self.pending = Ghost(pre.pending@.drop_first());
                }
            },
            None => {
                self.polled_at = at + 1;
                self.pending = Ghost(pre.pending@.drop_first());
                proof {
                    self.lemma_step_left(&pre, *wc, true, walked, 0, false, false);
                    self.refined@ = self.lemma_step_refined(&pre, *wc, true, walked, 0, false, false);
                }
                return None;
            },
        }
        let taken = ring.take_retired(seq);
        let ghost after = *ring;
        let ghost done = taken;
        match taken {
            Some(Taken { outcome, leases: Payload { ctx: Some(wr_id), mut leases, .. } }) => {
                let ghost full = leases;
                proof {
                    pre.lemma_walked_taken(*wc, spent, walked, seq as int, done->0);
                }
                if leases.is_many() {
                    leases.hand_back_many(&mut e.spare_leases);
                }
                let c = Completion { wr_id, result: outcome, qp: index, qp_num: e.qp_num, queue, seq, leases };
                proof {
                    self.lemma_step_left(&pre, *wc, spent, walked, seq as int, true, true);
                    pre.lemma_walked_hands_out(*wc, spent, walked, seq as int, done->0, c);
                    self.refined@ = self.lemma_step_refined(&pre, *wc, spent, walked, seq as int, true, true);
                }
                Some(c)
            },
            // Nothing, if it had no owner; a request with an owner holds a context.
            _ => {
                proof {
                    self.lemma_step_left(&pre, *wc, spent, walked, seq as int, done is Some, true);
                    self.refined@ = self.lemma_step_refined(&pre, *wc, spent, walked, seq as int, done is Some, true);
                }
                None
            },
        }
    }

    /// Whether a waker it ran panicked, and the panic waits for the engine to resume it.
    #[inline(always)]
    pub fn panicked(&self) -> (panicked: bool)
        no_unwind
    {
        self.panics.pending()
    }

    /// The panics of the wakers it ran, to resume once the engine is let go of: nothing resumes
    /// while the invariant may not hold.
    #[cold]
    #[inline(never)]
    pub(crate) fn take_panics(&mut self) -> (panics: WakeBatch)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
        no_unwind
    {
        replace_value(&mut self.panics, WakeBatch::new())
    }

    /// Where the event loop looks for results is as it was.
    pub closed spec fn same_walk(&self, other: &Self) -> bool {
        self.kept_at == other.kept_at
    }

    /// The next result waiting in the rings for the event loop: of a poll that kept them there,
    /// or of a refused doorbell, which comes at once, ahead of the requests before it.
    #[cold]
    #[inline(never)]
    #[verifier::spinoff_prover]
    fn next_kept(&mut self) -> (r: Option<Completion<C, N>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).keeps_requests(old(self)),
            final(self).moved_from(old(self)),
            r matches Some(c) ==> old(self).hands_out(c),
        no_unwind
    {
        let ghost start = *self;
        proof {
            self.lemma_keeps_requests_refl();
        }
        while self.kept_at < 2 * self.qps.len()
            invariant
                self.inv(),
                self.keeps_requests(&start),
                self.moved_from(&start),
                start == *old(self),
            decreases 2 * self.qps@.len() - self.kept_at,
        {
            proof {
                assert(table_ok(self.qps@, self.next_name@, self.token@, self.nic(), self.pending@));
            }
            let index = self.kept_at / 2;
            let side = if self.kept_at & 1 == 0 { Side::Send } else { Side::Recv };
            proof {
                assert(qp_ok(self.qps@[index as int], index as int, self.token@, self.nic()));
            }
            let range = match &self.qps[index] {
                Some(e) => match side {
                    Side::Send => (e.send.head(), if e.refused { e.send.committed() } else { e.send.retired() }),
                    Side::Recv => (e.recv.head(), e.recv.retired()),
                },
                None => (0, 0),
            };
            let mut seq = range.0;
            let ghost at = self.kept_at;
            let ghost len = self.qps@.len();
            while seq < range.1
                invariant
                    self.inv(),
                    self.keeps_requests(&start),
                    self.moved_from(&start),
                    start == *old(self),
                    self.kept_at == at,
                    self.qps@.len() == len,
                    at < 2 * len,
                    index == at / 2,
                decreases range.1 - seq,
            {
                let ghost mid = *self;
                let done = self.take(index, side, seq);
                proof {
                    assert(self.kept_at == mid.kept_at);
                    self.lemma_keeps_requests_trans(&mid, &start);
                    self.lemma_moved_trans(&mid, &start);
                    if done is Some {
                        match side {
                            Side::Send => mid.lemma_send_hands_out(done->0),
                            Side::Recv => mid.lemma_recv_hands_out(done->0),
                        }
                        mid.lemma_hands_out_earlier(&start, done->0);
                    }
                }
                if done.is_some() {
                    return done;
                }
                seq += 1;
            }
            if matches!(side, Side::Send) {
                let ghost mid = *self;
                proof {
                    assert(qp_ok(mid.qps@[index as int], index as int, mid.token@, mid.nic()));
                }
                match &mut self.qps[index] {
                    Some(e) => e.refused = false,
                    None => {},
                }
                proof {
                    if mid.qps@[index as int] is Some {
                        self.lemma_flags_only(&mid, index as int);
                    } else {
                        assert(self.qps@ =~= mid.qps@);
                        self.lemma_untouched(&mid);
                    }
                    self.lemma_keeps_requests_trans(&mid, &start);
                    self.lemma_moved_trans(&mid, &start);
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

/// A QP that wrote nothing before pins nothing it did not pin with what it wrote since.
proof fn lemma_pins_more(t1: NicToken, t3: NicToken, q: QpId)
    requires
        t3.same_writing_but(&t1, q),
        t1.written(q) == Seq::<SendWr>::empty(),
    ensures
        pinned(t1).subset_of(pinned(t3)),
{
    assert forall|k: Key| pinned(t1).contains(k) implies pinned(t3).contains(k) by {
        let p = choose|p: QpId| #[trigger] send_keys(t1.written(p)).contains(k);
        if p == q {
            let i = choose|i: int| 0 <= i < t1.written(q).len() && !(#[trigger] t1.written(q)[i]).inline
                && names_key(t1.written(q)[i].sges, k);
            assert(false);
        }
        assert(t3.written(p) == t1.written(p));
        assert(send_keys(t3.written(p)).contains(k));
    }
}

impl<C, N: Device> HwQp<C, N> {
    /// A request of the WQEs `more`, written after `w0`, staged on its send ring: the QP stays
    /// coupled, with those WQEs its request's, in M once the doorbell rings.
    #[verifier::spinoff_prover]
    proof fn lemma_staged(&self, pre: &Self, index: u16, m: Nic::State, sh: Share, w0: Seq<SendWr>, more: Seq<SendWr>, view: crate::ring::ReqView<Payload<C, N>>, outcome: CompletionResult)
        requires
            pre.ok(index, m, sh, pre.started, w0),
            self.same_qp(*pre),
            self.started,
            self.recv == pre.recv,
            self.send.just_staged(&pre.send, more.len() as u64, view, outcome),
            self.send_tails@ == pre.send_tails@.insert(pre.send.spec_posted() as int, more.last()),
            1 <= more.len(),
            more.len() <= u32::MAX,
            forall|j: int| 0 <= j < more.len() ==> {
                let wr = #[trigger] more[j];
                &&& wr.wr_id == spec_wr_id(index, false, (pre.send.spec_wqe_posted() + j) as u64)
                &&& m.send_wr_ok(m.qps[pre.id()].attr, wr)
                &&& !wr.inline
                &&& ledger_has(view.leases.leases, wr.wr_id, Some(wr.opcode), wr.sges)
            },
            payload_ok(view.leases, view.detached),
            outcome matches Ok(done) ==> more.last().opcode.allows_outcome(total_len(more.last().sges), done.model()),
            outcome is Ok,
        ensures
            self.ok(index, m, sh, true, w0 + more),
    {
        let q = pre.id();
        let qp = m.qps[q];
        let seq = pre.send.spec_posted() as int;
        let written = w0 + more;
        let (r0, r1) = (pre.send, self.send);
        r0.lemma_wqes();
        r1.lemma_wqes();
        r0.lemma_view();
        r0.lemma_meta(seq);
        r1.lemma_meta(seq);
        assert(r0.staged(seq) is None);
        lemma_resources_added(r0, r1, seq, view.leases, view.detached);
        assert(keeps_leases(r1, r0)) by {
            assert forall|s: int, w: int| r0.dma(s, w) is Some && r1.spec_wqe_retired() <= w < r1.spec_wqe_posted()
                implies #[trigger] r1.dma(s, w) is Some && r1.dma(s, w)->0.leases == r0.dma(s, w)->0.leases by {
                if s == seq {
                    r0.lemma_dma(s, w);
                }
            }
        }
        assert(keeps_leases(self.recv, pre.recv));
        lemma_queues_kept(*pre, *self, qp);
        assert forall|s: int| #[trigger] r1.meta(s) is Some && s != seq implies r0.meta(s) is Some
            && r1.meta(s)->0.outcome == r0.meta(s)->0.outcome by {
        }
        send_outcomes::lemma_implied_added(r0, r1, pre.send_tails@, seq, more.last());
        assert forall|j: int| 0 <= j < w0.len() implies #[trigger] written[j] == w0[j] by {}
        assert(written.last() == more.last());
        send_outcomes::lemma_bound_added(r0, r1, pre.send_tails@, qp, w0, written, seq, more.last(), m.retired, q);
        send_outcomes::lemma_results_kept(r0, r1, m, q);
        assert forall|j: int| 0 <= j < written.len()
            implies written_backed(*self, r1.spec_wqe_committed() + j, #[trigger] written[j]) by {
            let w = r1.spec_wqe_committed() + j;
            if j < w0.len() {
                assert(written_backed(*pre, w, w0[j]));
                lemma_written_kept(*pre, *self, w, w0[j]);
            } else {
                let k = j - w0.len();
                assert(written[j] == more[k]);
                assert(r0.spec_wqe_posted() + k == w);
                assert(r1.dma(seq, w) == Some(view.leases));
                let wr = more[k];
                assert(ledger_has(r1.dma(seq, w)->0.leases, wr.wr_id, Some(wr.opcode), wr.sges));
            }
        }
        assert forall|j: int| 0 <= j < written.len() implies (#[trigger] written[j]).wr_id == spec_wr_id(
            index,
            false,
            (r1.spec_wqe_committed() + j) as u64,
        ) by {
            if j >= w0.len() {
                assert(written[j] == more[j - w0.len()]);
            }
        }
        assert forall|j: int| 0 <= j < written.len() implies m.send_wr_ok(qp.attr, #[trigger] written[j]) by {
            if j >= w0.len() {
                assert(written[j] == more[j - w0.len()]);
            }
        }
    }

    /// The doorbell rang: the WRs written are in M, and the ring's requests in flight.
    #[verifier::spinoff_prover]
    proof fn lemma_rang(&self, pre: &Self, index: u16, mid: Nic::State, n: Nic::State, sh: Share, written: Seq<SendWr>)
        requires
            pre.ok(index, mid, sh, pre.started, written),
            pre.phase == Phase::Ready,
            nic_inv(mid),
            nic_step(mid, n, Nic::Step::post_send(pre.id(), written)),
            self.same_qp(*pre),
            !self.started,
            self.recv == pre.recv,
            self.send_tails == pre.send_tails,
            self.send.just_committed(&pre.send),
        ensures
            self.ok(index, n, sh, false, Seq::empty()),
    {
        let q = pre.id();
        let (a, b) = (mid.qps[q], n.qps[q]);
        let (r0, r1) = (pre.send, self.send);
        r0.lemma_wqes();
        r1.lemma_wqes();
        r0.lemma_view();
        lemma_queued_outcomes_step(mid, n, Nic::Step::post_send(q, written), q);
        assert forall|s: int| #[trigger] r1.req(s) is Some implies resource_at(r0, s, r1.req(s)->0.leases, r1.req(s)->0.detached) by {}
        lemma_resources_kept(r0, r1);
        assert(keeps_leases(r1, r0));
        assert(keeps_leases(self.recv, pre.recv));
        lemma_queues_kept(*pre, *self, a);
        assert forall|k: int| 0 <= k < b.sq.len() implies {
            let wr = (#[trigger] b.sq[k]).wr;
            &&& !wr.inline
            &&& ring_backed(r1, b.sq_head + k, wr.wr_id, Some(wr.opcode), wr.sges)
        } by {
            if k < a.sq.len() {
                assert(b.sq[k] == a.sq[k]);
            } else {
                let j = k - a.sq.len();
                assert(b.sq[k].wr == written[j]);
                assert(written_backed(*pre, r0.spec_wqe_committed() + j, written[j]));
                lemma_backed_kept(r0, r1, b.sq_head + k, written[j].wr_id, Some(written[j].opcode), written[j].sges);
            }
        }
        assert forall|k: int| 0 <= k < b.sq.len() implies (#[trigger] b.sq[k]).wr.wr_id == spec_wr_id(
            index,
            false,
            (b.sq_head + k) as u64,
        ) by {
            if k < a.sq.len() {
                assert(b.sq[k] == a.sq[k]);
            } else {
                assert(b.sq[k].wr == written[k - a.sq.len()]);
            }
        }
        assert(sq_linked(r1, index, b));
        outcomes::lemma_recv_framed(self.recv, mid, n, q);
        send_outcomes::lemma_bound_committed(r0, r1, self.send_tails@, a, b, written, mid.retired, q);
        send_outcomes::lemma_results_committed(r0, r1, mid, q);
        send_outcomes::lemma_results_framed(r1, mid, n, q);
        assert forall|s: int| #[trigger] r1.meta(s) is Some implies r0.meta(s) is Some
            && (r1.meta(s)->0.outcome is Ok ==> r1.meta(s)->0.outcome == r0.meta(s)->0.outcome)
            && self.send_tails@[s] == pre.send_tails@[s] && self.send_tails@.contains_key(s) by {
        }
        send_outcomes::lemma_implied_kept(r0, r1, pre.send_tails@, self.send_tails@);
        assert(writing_ok(r1, index, n, b.attr, false, Seq::empty()));
    }

    /// The written WRs were discarded: what the ring staged is refused, and M never saw it.
    #[verifier::spinoff_prover]
    proof fn lemma_refused(&self, pre: &Self, index: u16, n: Nic::State, sh: Share, written: Seq<SendWr>, keep: int)
        requires
            pre.ok(index, n, sh, pre.started, written),
            self.same_qp(*pre),
            !self.started,
            self.recv == pre.recv,
            self.send_tails == pre.send_tails,
            self.send.just_aborted(&pre.send, keep),
        ensures
            self.ok(index, n, sh, false, Seq::empty()),
    {
        let q = pre.id();
        let qp = n.qps[q];
        let (r0, r1) = (pre.send, self.send);
        r0.lemma_wqes();
        r1.lemma_wqes();
        assert forall|s: int| #[trigger] r1.req(s) is Some implies resource_at(r0, s, r1.req(s)->0.leases, r1.req(s)->0.detached) by {}
        assert forall|s: int| #[trigger] r1.staged(s) is Some implies resource_at(r0, s, r1.staged(s)->0.leases, r1.staged(s)->0.detached) by {}
        assert forall|s: int| #[trigger] r1.refused(s) is Some implies resource_at(r0, s, r1.refused(s)->0, false) by {}
        lemma_resources_kept(r0, r1);
        assert(keeps_leases(r1, r0));
        assert(keeps_leases(self.recv, pre.recv));
        lemma_queues_kept(*pre, *self, qp);
        assert forall|j: int| 0 <= j < qp.sq.len() implies qp.sq_head + j < qp.sq_head + qp.sq.len()
            && (#[trigger] qp.sq[j]).wr == qp.sq[qp.sq_head + j - qp.sq_head].wr by {
        }
        send_outcomes::lemma_bound_kept(r0, r1, pre.send_tails@, qp, qp, written, n.retired, n.retired, q);
        send_outcomes::lemma_bound_no_written(r1, self.send_tails@, qp, written, n.retired, q);
        send_outcomes::lemma_results_refused(r0, r1, n, q);
        assert forall|s: int| #[trigger] r1.meta(s) is Some implies r0.meta(s) is Some
            && (r1.meta(s)->0.outcome is Ok ==> r1.meta(s)->0.outcome == r0.meta(s)->0.outcome)
            && self.send_tails@[s] == pre.send_tails@[s] && self.send_tails@.contains_key(s) by {
        }
        send_outcomes::lemma_implied_kept(r0, r1, pre.send_tails@, self.send_tails@);
        assert(writing_ok(r1, index, n, qp.attr, false, Seq::empty()));
    }
}

impl<C, N: Device> Hw<C, N> {
    #[inline]
    #[verifier::spinoff_prover]
    pub fn commit(&mut self, index: usize, keep: Option<RingSeq>) -> (r: core::result::Result<(), (Error, Option<Payload<C, N>>)>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            r matches Err((_, Some(kept))) ==> kept.leases.inv() && kept.leases.fully_recorded(),
            r matches Err((_, kept)) ==> (keep matches Some(seq) ==> (old(self).staged_ctx(index as int, seq as int) matches Some(ctx)
                ==> (kept matches Some(payload) && payload.ctx == ctx))),
        no_unwind
    {
        if index >= self.qps.len() {
            return Err((Error::Invalid("no such QP"), None));
        }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, t0, m0));
        }
        match &mut self.qps[index] {
            Some(e) => {
                if !e.started {
                    proof {
                        assert(self.qps@ =~= start.qps@);
                    }
                    return Ok(());
                }
                e.started = false;
                let ghost q = e.id();
                let ghost e0 = *e;
                let refused = if matches!(e.phase, Phase::Ready) {
                    proof {
                        let qp = m0.qps[q];
                        e0.send.lemma_wqes();
                        assert(nic_enabled(m0, Nic::Step::post_send(q, t0.written(q))));
                    }
                    match doorbell(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut())) {
                        Ok(()) => {
                            e.send.commit();
                            proof {
                                let t = self.token@;
                                let n = t.nic();
                                let wrs = t0.written(q);
                                let mid = choose|mid: Nic::State| rely_reaches(sh, pinned(t0), m0, mid) && nic_step(mid, n, Nic::Step::post_send(q, wrs));
                                start.lemma_rely(t0, m0, mid);
                                lemma_nic_step_is_next(mid, n, Nic::Step::post_send(q, wrs));
                                lemma_step_keeps_invariant(mid, n);
                                lemma_mine_keeps_share_wf(sh, pinned(t0), mid, n, Nic::Step::post_send(q, wrs));
                                assert(qp_ok(start.qps@[index as int], index as int, t0, mid));
                                (*e).lemma_rang(&start.qps@[index as int]->0, index as u16, mid, n, sh, wrs);
                                assert(self.entry_ok(index as int, t, n));
                                self.lemma_one(&start, t0, t, mid, n, index as int);
                                // A's `post_request` of each request staged.
                                let g1 = refines::lemma_linked_rely(start.qps@, start.next_name@, t0, m0, mid, start.pending@, pinned(t0), start.refined@);
                                refines::lemma_send_queued(mid, n, q, wrs);
                                refines::lemma_verb_keeps(start.qps@, start.next_name@, t0, mid, n, start.pending@, q);
                                let g2 = refines::lemma_linked_doorbell(start.qps@, self.qps@, mid, n, g1, index as int, wrs);
                                g2.lemma_extends_trans(g1, start.refined@);
                                lemma_nic_reaches_trans(m0, mid, n);
                                self.refined@ = g2;
                            }
                            return Ok(());
                        },
                        Err(err) => err,
                    }
                } else {
                    abort(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                    Error::NotConnected
                };
                // Only a refused doorbell wakes anyone, its staged requests' wakers;
                // a waker's panic resumes once the engine is let go of.
                let kept = match keep {
                    Some(seq) => e.send.abort_keeping_with_wakes(seq, &mut self.panics),
                    None => {
                        e.send.abort_with_wakes(&mut self.panics);
                        None
                    },
                };
                e.refused = true;
                self.kept = true;
                self.kept_at = 0;
                proof {
                    let t = self.token@;
                    let n = t.nic();
                    let e_start = start.qps@[index as int]->0;
                    start.lemma_rely(t0, m0, n);
                    lemma_rely_frame(sh, pinned(t0), m0, n);
                    assert(qp_ok(start.qps@[index as int], index as int, t0, n));
                    let kept_seq = match keep {
                        Some(seq) => seq as int,
                        None => e_start.send.spec_posted() as int,
                    };
                    self.qps@[index as int]->0.lemma_refused(&e_start, index as u16, n, sh, t0.written(q), kept_seq);
                    assert(self.entry_ok(index as int, t, n));
                    self.lemma_one(&start, t0, t, n, n, index as int);
                    // What was staged comes to nothing for A.
                    let g1 = refines::lemma_linked_rely(start.qps@, start.next_name@, t0, m0, n, start.pending@, pinned(t0), start.refined@);
                    let g2 = refines::lemma_linked_refused(start.qps@, self.qps@, n, g1, index as int, Queue::Send, kept_seq);
                    g2.lemma_extends_trans(g1, start.refined@);
                    self.refined@ = g2;
                }
                Err((refused, kept))
            },
            None => {
                proof {
                    assert(self.qps@ =~= start.qps@);
                }
                Err((Error::Invalid("no such QP"), None))
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
    #[verifier::rlimit(30)]
    pub fn post_one(&mut self, index: usize, op: WrOp, part: Part<N>, ctx: Option<C>, signal: bool, ring: bool) -> (r: core::result::Result<core::result::Result<RingSeq, Refusal<C, N>>, Unposted<C, N>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            r matches Ok(Err(refused)) ==> refused.ctx == ctx,
            r matches Err(back) ==> back.op == op && back.part == part && back.ctx == ctx,
        no_unwind
    {
        #[cfg(verus_keep_ghost)]
        let part = part.separate_from_table(&*self);
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        if index < self.qps.len() {
            proof {
                assert(qp_ok(start.qps@[index as int], index as int, t0, m0));
            }
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
                        let ghost e0 = *e;
                        let implied = implied_outcome(&op, sge.length);
                        // Its region is registered (its lease says so).
                        registered(&part, &e.pd, Tracked(self.token.borrow_mut()));
                        let ghost t1 = self.token@;
                        let ghost m1 = t1.nic();
                        let ghost g1 = refines::lemma_linked_rely(start.qps@, start.next_name@, t0, m0, m1, start.pending@, pinned(t0), start.refined@);
                        let ghost lent = part;
                        proof {
                            start.lemma_rely(t0, m0, m1);
                            start.lemma_same_token(t0, t1, m1);
                            assert(qp_ok(start.qps@[index as int], index as int, t1, m1));
                            e0.send.lemma_wqes();
                        }
                        let id = wr_id(index as u16, false, e.send.next_wqe());
                        let signaled = signal || e.send.must_signal(1);
                        let sges = one_of(&sge);
                        // The slot first: from then on it keeps the memory alive.
                        let mut leases = Leases::one(part, Ghost(if op_writes_local(op) { safeverbs_spec::abs::Dir::In } else { safeverbs_spec::abs::Dir::Out }));
                        let ghost entry = crate::buf::WqeLoans {
                            wr_id: id, opcode: Some(opcode_model(op)), sges: sges@, first: 0,
                        };
                        proof {
                            assert(sges@ =~= seq![sge]);
                            assert(entry.matches(leases.loans()));
                        }
                        leases.record_wqe(Ghost(entry));
                        let detached = ctx.is_none();
                        let ghost named = start.lemma_name_one(m1, g1, leases, lent);
                        let payload = Payload { ctx, leases, holds: Ghost(named.1) };
                        let ghost resource = payload;
                        let ghost wr = send_wr(id, signaled, op, sges@);
                        proof {
                            lemma_ledger_wr(resource.leases, 0, id, signaled, op, sges@);
                            open::lemma_one_wr(id, signaled, op, sges@, sge);
                            assert(wr.sges[0] == sge_model(sge));
                            assert(m1.send_wr_ok(m1.qps[q].attr, wr));
                        }
                        e.send.fill(1, detached, implied, payload);
                        let ghost filled = e.send;
                        if ring || !e.started {
                            crate::trusted::start(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                        }
                        post(&mut e.hw, id, signaled, &op, sges, Ghost(q), Tracked(self.token.borrow_mut()));
                        if ring {
                            let ghost t3 = self.token@;
                            proof {
                                assert(t1.written(q) =~= Seq::<SendWr>::empty());
                                assert(t3.written(q) =~= seq![wr]);
                                let qp = m1.qps[q];
                                assert(nic_enabled(m1, Nic::Step::post_send(q, seq![wr])));
                            }
                            match doorbell(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut())) {
                                Ok(()) => {
                                    let seq = e.send.stage_filled(1, signaled);
                                    let ghost staged = *e;
                                    e.send.commit();
                                    e.send_tails = Ghost(e0.send_tails@.insert(seq as int, wr));
                                    proof {
                                        staged.send.lemma_filled_staged(&filled, &e0.send, 1, crate::ring::ReqView::new(detached, resource), implied);
                                        let g = self.lemma_posted_rung(&start, t1, t3, index as int, staged, resource, detached, implied, wr, named.0);
                                        named.0.lemma_extends_trans(g1, start.refined@);
                                        g.lemma_extends_trans(named.0, start.refined@);
                                        lemma_nic_reaches_trans(m0, m1, self.token@.nic());
                                        self.refined@ = g;
                                    }
                                    return Ok(Ok(seq));
                                },
                                Err(error) => {
                                    let refused = not_posted(&mut e.send, error);
                                    proof {
                                        lemma_pins_more(t1, t3, q);
                                        lemma_rely_fewer_pins(sh, pinned(t3), pinned(t1), m1, self.token@.nic());
                                        self.lemma_not_posted(&start, t1, index as int);
                                        let g = refines::lemma_linked_rely(start.qps@, start.next_name@, t1, m1, self.token@.nic(), start.pending@, pinned(t1), g1);
                                        refines::lemma_linked_same_view(start.qps@, self.qps@, self.token@.nic(), g, index as int, Queue::Send);
                                        g.lemma_extends_trans(g1, start.refined@);
                                        lemma_nic_reaches_trans(m0, m1, self.token@.nic());
                                        self.refined@ = g;
                                    }
                                    return Ok(Err(refused));
                                },
                            }
                        }
                        e.started = true;
                        let seq = e.send.stage_filled(1, signaled);
                        e.send_tails = Ghost(e0.send_tails@.insert(seq as int, wr));
                        proof {
                            let t = self.token@;
                            let w0 = t1.written(q);
                            let more = seq![wr];
                            e.send.lemma_filled_staged(&filled, &e0.send, 1, crate::ring::ReqView::new(detached, resource), implied);
                            assert(self.token@.written(q) =~= w0 + more);
                            (*e).lemma_staged(&e0, index as u16, m1, sh, w0, more, crate::ring::ReqView::new(detached, resource), implied);
                            assert(self.entry_ok(index as int, t, m1));
                            self.lemma_one(&start, t1, t, m1, m1, index as int);
                            refines::lemma_linked_staged(start.qps@, self.qps@, m1, named.0, index as int, Queue::Send, 1, crate::ring::ReqView::new(detached, resource), implied);
                            named.0.lemma_extends_trans(g1, start.refined@);
                            self.refined@ = named.0;
                        }
                        return Ok(Ok(seq));
                    }
                },
                None => {},
            }
        }
        proof {
            assert(self.qps@ =~= start.qps@);
        }
        Err(Unposted { op, part, ctx })
    }

    /// What a request of one WQE posted with its doorbell leaves.
    #[verifier::spinoff_prover]
    proof fn lemma_posted_rung(&self, start: &Self, t1: NicToken, t3: NicToken, index: int, staged: HwQp<C, N>, resource: Payload<C, N>, detached: bool, implied: CompletionResult, wr: SendWr, g: refines::Refinement) -> (g2: refines::Refinement)
        requires
            start.inv_with(t1, t1.nic()),
            start.has_qp(index),
            refines::linked(start.qps@, t1.nic(), g),
            may_post_holds(g.abs, resource.holds@),
            forall|l: safeverbs_spec::abs::LeaseId| #[trigger] resource.holds@.contains_key(l)
                ==> free_lease(refines::table_view(start.qps@, t1.nic()), g.abs, refines::lent_holds(), l),
            roots_outside(refines::table_view(start.qps@, t1.nic()), g.abs, refines::lent_holds(), resource.holds@),
            ({
                let e0 = start.qps@[index]->0;
                let q = e0.id();
                let e = self.qps@[index]->0;
                let t = self.token@;
                let m1 = t1.nic();
                &&& t3.same_engine(&t1)
                &&& t3.same_writing_but(&t1, q)
                &&& t3.nic() == m1
                &&& t3.started(q)
                &&& t3.written(q) == seq![wr]
                &&& !t1.started(q)
                &&& t.same_engine(&t3)
                &&& t.same_writing_but(&t3, q)
                &&& !t.started(q)
                &&& t.written(q) == Seq::<SendWr>::empty()
                &&& exists|mid: Nic::State| rely_reaches(t1.share(), pinned(t3), m1, mid)
                    && nic_step(mid, t.nic(), Nic::Step::post_send(q, seq![wr]))
                &&& self.qps@.len() == start.qps@.len()
                &&& forall|i: int| 0 <= i < self.qps@.len() && i != index ==> #[trigger] self.qps@[i] == start.qps@[i]
                &&& self.qps@[index] is Some
                &&& staged.same_qp(e0)
                &&& e0.phase == Phase::Ready
                &&& staged.recv == e0.recv
                &&& staged.send.just_staged(&e0.send, 1, crate::ring::ReqView::new(detached, resource), implied)
                &&& e.same_qp(staged)
                &&& !e.started
                &&& e.recv == staged.recv
                &&& e.send_tails@ == e0.send_tails@.insert(e0.send.spec_posted() as int, wr)
                &&& e.send.just_committed(&staged.send)
                &&& wr.wr_id == spec_wr_id(index as u16, false, e0.send.spec_wqe_posted())
                &&& m1.send_wr_ok(m1.qps[q].attr, wr)
                &&& !wr.inline
                &&& ledger_has(resource.leases, wr.wr_id, Some(wr.opcode), wr.sges)
                &&& payload_ok(resource, detached)
                &&& implied is Ok
                &&& implied matches Ok(done) ==> wr.opcode.allows_outcome(total_len(wr.sges), done.model())
            }),
            cq_of(self.cq@) == t1.share().cq,
            self.caps == start.caps,
            self.cq_size == start.cq_size,
            self.reserved == start.reserved,
            self.next_name == start.next_name,
            self.pending == start.pending,
        ensures
            self.inv_with(self.token@, self.token@.nic()),
            self.same_table(start),
            refines::linked(self.qps@, self.token@.nic(), g2),
            g2.extends(g),
            nic_reaches(t1.nic(), self.token@.nic()),
    {
        let e0 = start.qps@[index]->0;
        let q = e0.id();
        let e = self.qps@[index]->0;
        let t = self.token@;
        let n = t.nic();
        let sh = t1.share();
        let m1 = t1.nic();
        let more = seq![wr];
        let tails = HwQp { send_tails: e.send_tails, started: true, ..staged };
        assert(qp_ok(start.qps@[index], index, t1, m1));
        assert(t1.written(q) =~= Seq::<SendWr>::empty());
        assert(Seq::<SendWr>::empty() + more =~= more);
        tails.lemma_staged(&e0, index as u16, m1, sh, Seq::empty(), more, crate::ring::ReqView::new(detached, resource), implied);
        // M moved by others' steps, and the WR's region stays: it is pinned.
        let mid = choose|mid: Nic::State| rely_reaches(sh, pinned(t3), m1, mid) && nic_step(mid, n, Nic::Step::post_send(q, seq![wr]));
        lemma_pins_more(t1, t3, q);
        lemma_rely_fewer_pins(sh, pinned(t3), pinned(t1), m1, mid);
        start.lemma_rely(t1, m1, mid);
        lemma_rely_frame(sh, pinned(t3), m1, mid);
        lemma_queued_outcomes_rely(sh, pinned(t3), m1, mid, q);
        assert(tails.ok(index as u16, m1, t3.share(), t3.started(q), t3.written(q)));
        tails.lemma_framed(index as u16, t3, m1, mid);
        lemma_nic_step_is_next(mid, n, Nic::Step::post_send(q, seq![wr]));
        lemma_step_keeps_invariant(mid, n);
        lemma_mine_keeps_share_wf(sh, pinned(t3), mid, n, Nic::Step::post_send(q, seq![wr]));
        e.lemma_rang(&tails, index as u16, mid, n, sh, seq![wr]);
        assert(self.entry_ok(index as int, t, n));
        // The other QPs' writings are as they were.
        assert(t.same_writing_but(&t1, q)) by {
            assert forall|p: QpId| p != q implies #[trigger] t.started(p) == t1.started(p) by {
                assert(t.started(p) == t3.started(p));
            }
            assert forall|p: QpId| p != q implies #[trigger] t.written(p) == t1.written(p) by {
                assert(t.written(p) == t3.written(p));
            }
        }
        assert(t.same_engine(&t1)) by {
            assert forall|k: nat| #[trigger] t.name(k) == t1.name(k) by {
                assert(t.name(k) == t3.name(k));
            }
        }
        self.lemma_one(start, t1, t, mid, n, index);
        // A's `post_request`.
        assert(qp_ok(start.qps@[index], index, t1, mid));
        e0.send.lemma_wqes();
        let g1 = refines::lemma_linked_rely(start.qps@, start.next_name@, t1, m1, mid, start.pending@, pinned(t1), g);
        refines::lemma_roots_moved(refines::table_view(start.qps@, m1), g.abs, refines::table_view(start.qps@, mid), g1.abs, resource.holds@);
        refines::lemma_send_queued(mid, n, q, seq![wr]);
        refines::lemma_verb_keeps(start.qps@, start.next_name@, t1, mid, n, start.pending@, q);
        let g2 = refines::lemma_linked_one_posted(start.qps@, self.qps@, mid, n, Nic::Step::post_send(q, seq![wr]), g1, index, Queue::Send, staged.send, 1, crate::ring::ReqView::new(detached, resource), implied);
        g2.lemma_extends_trans(g1, g);
        lemma_nic_reaches_trans(m1, mid, n);
        g2
    }

    /// [`Hw::post_one`], out of the common: the request of one WQE of the general kind, which
    /// waits for room if it must (this polls the CQ).
    #[cold]
    #[inline(never)]
    #[verifier::spinoff_prover]
    pub fn post_one_slow(&mut self, index: usize, unposted: Unposted<C, N>, signal: bool, ring: bool) -> (r: core::result::Result<RingSeq, Refusal<C, N>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            r matches Err(refused) ==> refused.ctx == unposted.ctx,
        no_unwind
    {
        let ghost start = *self;
        let op = unposted.op;
        let part = unposted.part;
        let ctx = unposted.ctx;
        let mut open = self.open_request(index);
        let ghost s0 = *self;
        match self.push_other(index, &mut open, op, part) {
            Ok(()) => {},
            Err(e) => {
                let ghost s1 = *self;
                self.give_back(index, &mut open);
                proof {
                    lemma_same_table_trans(&s1, &s0, &start);
                    lemma_same_table_trans(self, &s1, &start);
                }
                return Err(Refusal { error: e.error, ctx, leases: Leases::one(e.buf, Ghost(safeverbs_spec::abs::Dir::Out)) });
            },
        }
        let ghost s1 = *self;
        proof {
            lemma_same_table_trans(&s1, &s0, &start);
            assert(s1.has_qp(index as int));
        }
        proof {
            assert(qp_ok(s1.qps@[index as int], index as int, s1.token@, s1.nic()));
            s1.qps@[index as int]->0.send.lemma_wqes();
        }
        match self.make_send_room(index, 1) {
            Ok(()) => {},
            Err(error) => {
                let leases = open.take_leases();
                let ghost s2 = *self;
                self.give_back(index, &mut open);
                proof {
                    lemma_same_table_trans(&s2, &s1, &start);
                    lemma_same_table_trans(self, &s2, &start);
                }
                return Err(Refusal { error, ctx, leases });
            },
        }
        let ghost s2 = *self;
        let closed = self.close(index, &mut open, signal, ctx);
        let seq = match closed {
            Ok(seq) => seq,
            Err(ctx) => {
                let leases = open.take_leases();
                let ghost s3 = *self;
                self.give_back(index, &mut open);
                proof {
                    lemma_same_table_trans(&s2, &s1, &start);
                    lemma_same_table_trans(&s3, &s2, &start);
                    lemma_same_table_trans(self, &s3, &start);
                }
                return Err(Refusal { error: Error::QueueFull, ctx, leases });
            },
        };
        let ghost s3 = *self;
        self.give_back(index, &mut open);
        let ghost s4 = *self;
        proof {
            lemma_same_table_trans(&s2, &s1, &start);
            lemma_same_table_trans(&s3, &s2, &start);
            lemma_same_table_trans(&s4, &s3, &start);
        }
        if ring {
            let r = self.commit_own(index, seq);
            proof {
                lemma_same_table_trans(self, &s4, &start);
            }
            r
        } else {
            Ok(seq)
        }
    }

    /// Rings QP `index`'s doorbell for the request `seq` just staged and those before it. If the
    /// device refuses, it hands back that request's context and buffers.
    #[inline(never)]
    pub fn commit_own(&mut self, index: usize, seq: RingSeq) -> (r: core::result::Result<RingSeq, Refusal<C, N>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            r matches Err(refused) ==> (old(self).staged_ctx(index as int, seq as int) matches Some(ctx) ==> refused.ctx == ctx),
        no_unwind
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

// =============================================================================
// Requests

/// The bytes of a scatter/gather list, as M counts them.
pub(super) open spec fn sges_len(sges: Seq<Sge>) -> nat {
    total_len(sges.map_values(|sge: Sge| sge_model(sge)))
}

impl<C, N: Device> Hw<C, N> {
    /// Closes `open` on QP `index`: its signaling is known now, so its WQEs are written, the last
    /// signaled if `signal` or if the ring says so, and it is staged in the ring with `ctx` for its
    /// owner; without one if nobody will take it back. Returns its sequence number; if it has no
    /// WQE, or the queue no room for them, or the QP does not take what it was written for, `ctx`
    /// back. It goes out at the next doorbell.
    #[verifier::spinoff_prover]
    #[verifier::rlimit(30)]
    pub fn close(&mut self, index: usize, open: &mut Writing<N>, signal: bool, ctx: Option<C>) -> (r: core::result::Result<RingSeq, Option<C>>)
        requires
            old(self).inv(),
            old(open).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            final(open).inv(),
            final(open).same_fields(old(open)),
            r matches Err(back) ==> back == ctx,
            r matches Ok(seq) ==> final(self).staged_ctx(index as int, seq as int) == Some(ctx),
        no_unwind
    {
        let n = open.n;
        if n == 0 || index >= self.qps.len() {
            return Err(ctx);
        }
        #[cfg(verus_keep_ghost)]
        open.separate_from_table(&*self);
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        let ghost before_open = *open;
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, t0, m0));
        }
        match &mut self.qps[index] {
            Some(e) => {
                if !e.send.can_post() || n as u64 > e.send.wqe_room() as u64 || !e.takes(open) {
                    proof {
                        assert(self.qps@ =~= start.qps@);
                    }
                    return Err(ctx);
                }
                let ghost q = e.id();
                let ghost e0 = *e;
                proof {
                    e0.send.lemma_wqes();
                    assert forall|j: int| 0 <= j < open.kept.loans().len()
                        implies part_pd((#[trigger] open.kept.loans()[j]).part) == pd_id(e.pd) by {
                    }
                }
                // Its regions are registered (its leases say so).
                open.keeps_registered(&e.pd, Tracked(self.token.borrow_mut()));
                let ghost t1 = self.token@;
                let ghost m1 = t1.nic();
                let ghost g1 = refines::lemma_linked_rely(start.qps@, start.next_name@, t0, m0, m1, start.pending@, pinned(t0), start.refined@);
                proof {
                    start.lemma_rely(t0, m0, m1);
                    start.lemma_same_token(t0, t1, m1);
                    assert(qp_ok(start.qps@[index as int], index as int, t1, m1));
                }
                let signaled = signal || e.send.must_signal(n as u64);
                if !e.started {
                    crate::trusted::start(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                    e.started = true;
                }
                let first = e.send.next_wqe();
                let ghost w0 = self.token@.written(q);
                proof {
                    assert(first as int + n as int <= u64::MAX);
                }
                // Every WQE but the last, which is signaled if the request is.
                let mut i = 0;
                while i + 1 < n
                    invariant
                        i + 1 <= n,
                        n == open.n,
                        open.inv(),
                        first as int + n as int <= u64::MAX,
                        qp_of(e.hw) == q,
                        self.token@.started(q),
                        self.token@.nic() == m1,
                        self.token@.same_engine(&t1),
                        self.token@.same_writing_but(&t1, q),
                        self.token@.written(q).len() == w0.len() + i,
                        forall|j: int| 0 <= j < w0.len() ==> #[trigger] self.token@.written(q)[j] == w0[j],
                        forall|k: int| 0 <= k < i ==> #[trigger] self.token@.written(q)[w0.len() + k]
                            == open.wr_of(k, index as u16, first, false),
                    decreases n - i,
                {
                    let sges = open.sges_of(i);
                    let ghost written = self.token@.written(q);
                    post(&mut e.hw, wr_id(index as u16, false, first + i as u64), false, open.op_of(i), sges, Ghost(q), Tracked(self.token.borrow_mut()));
                    proof {
                        assert forall|j: int| 0 <= j < w0.len() implies #[trigger] self.token@.written(q)[j] == w0[j] by {
                            assert(written[j] == w0[j]);
                        }
                        assert forall|k: int| 0 <= k < i + 1 implies #[trigger] self.token@.written(q)[w0.len() + k]
                            == open.wr_of(k, index as u16, first, false) by {
                            if k < i {
                                assert(written[w0.len() + k] == open.wr_of(k, index as u16, first, false));
                            }
                        }
                    }
                    i += 1;
                }
                let sges = open.sges_of(i);
                let ghost written_all = self.token@.written(q);
                post(&mut e.hw, wr_id(index as u16, false, first + i as u64), signaled, open.op_of(i), sges, Ghost(q), Tracked(self.token.borrow_mut()));
                let last = &open.wqes[n - 1];
                let bytes = if last.sges == 1 { last.sge.length } else { open.last_gathered_bytes() };
                let implied = implied_outcome(&last.op, bytes);
                let ghost wqes = open.posted_ledger(index as u16, first);
                let ghost more = open.posted_wrs(index as u16, first, signaled);
                let ghost taken_from = *open;
                proof {
                    let qp = m1.qps[q];
                    open.lemma_posted(index as u16, first, signaled, m1, qp.attr);
                    if last.sges == 1 {
                        lemma_sges_len_one(last.sge);
                    }
                    assert(self.token@.written(q) =~= w0 + more) by {
                        assert forall|j: int| 0 <= j < w0.len() + more.len()
                            implies #[trigger] self.token@.written(q)[j] == (w0 + more)[j] by {
                            if j < w0.len() {
                                assert(written_all[j] == w0[j]);
                            } else if j < w0.len() + i {
                                assert(written_all[w0.len() + (j - w0.len())] == open.wr_of(j - w0.len(), index as u16, first, false));
                            }
                        }
                    }
                }
                let leases = open.take_leases_with(&mut e.spare_leases, Ghost(wqes));
                let many = leases.is_many();
                let detached = ctx.is_none();
                proof {
                    refines::lemma_inputs_directed(leases);
                }
                proof {
                    start.lemma_table_kept();
                    assert forall|j: int| 0 <= j < leases.loans().len()
                        implies #[trigger] refines::table_apart(start.qps@, leases.loans()[j].part.resource()) by {
                        assert(start.table_apart(taken_from.kept.loans()[j].part.resource()));
                    }
                }
                let ghost named = refines::lemma_name_request(start.qps@, m1, g1, leases);
                let payload = Payload { ctx, leases, holds: Ghost(named.1) };
                let ghost resource = payload;
                let seq = e.send.stage(n as u64, signaled, detached, implied, payload);
                e.send_tails = Ghost(e0.send_tails@.insert(seq as int, more.last()));
                if many {
                    e.send.take_general_path(seq);
                }
                vec_clear(&mut open.gathered);
                proof {
                    let t = self.token@;
                    let view = crate::ring::ReqView::new(detached, resource);
                    taken_from.lemma_ledger_of_posted(resource.leases, index as u16, first, signaled);
                    (*e).lemma_staged(&e0, index as u16, m1, sh, w0, more, view, implied);
                    assert(self.entry_ok(index as int, t, m1));
                    self.lemma_one(&start, t1, t, m1, m1, index as int);
                    refines::lemma_linked_staged(start.qps@, self.qps@, m1, named.0, index as int, Queue::Send, n as u64, view, implied);
                    named.0.lemma_extends_trans(g1, start.refined@);
                    self.refined@ = named.0;
                }
                Ok(seq)
            },
            None => {
                proof {
                    assert(self.qps@ =~= start.qps@);
                }
                Err(ctx)
            },
        }
    }

    /// A request to write on QP `index`, with what the QP keeps for it. A QP that takes no work
    /// has its WQEs say so one by one: they are collected too, and the doorbell is refused.
    #[inline(always)]
    #[verifier::spinoff_prover]
    pub fn open_request(&mut self, index: usize) -> (open: Writing<N>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            open.inv(),
            open.has_no_wqe(),
        no_unwind
    {
        if index < self.qps.len() {
            let ghost start = *self;
            proof {
                assert(qp_ok(start.qps@[index as int], index as int, start.token@, start.nic()));
            }
            match &mut self.qps[index] {
                Some(e) => {
                    if e.spare_wqes.len() > 0 {
                        let mut wqes = vec_new();
                        core::mem::swap(&mut wqes, &mut e.spare_wqes);
                        let open = Writing {
                            wqes,
                            n: 0,
                            limit: e.open_limit(),
                            max_len: e.max_len,
                            pd_id: e.pd_id,
                            kept: Kept::new(),
                            gathered: vec_new(),
                            reads: false,
                            max_sges: 1,
                            ledger: Ghost(Seq::empty()),
                        };
                        proof {
                            self.lemma_flags_only(&start, index as int);
                        }
                        return open;
                    }
                    proof {
                        assert(self.qps@ =~= start.qps@);
                        self.lemma_untouched(&start);
                    }
                },
                None => {
                    proof {
                        assert(self.qps@ =~= start.qps@);
                        self.lemma_untouched(&start);
                    }
                },
            }
        }
        self.open_request_other(index)
    }

    /// [`Hw::open_request`] on a QP whose storage is out, or not made yet.
    #[cold]
    #[inline(never)]
    fn open_request_other(&mut self, index: usize) -> (open: Writing<N>)
        requires
            old(self).inv(),
        ensures
            *final(self) == *old(self),
            open.inv(),
            open.has_no_wqe(),
        no_unwind
    {
        if index < self.qps.len() {
            proof {
                assert(qp_ok(self.qps@[index as int], index as int, self.token@, self.token@.nic()));
            }
            match &self.qps[index] {
                Some(e) => {
                    let max = e.send.max_wqes() as usize;
                    let mut wqes = vec_with_capacity(max);
                    let mut i = 0;
                    while i < max
                        invariant
                            i <= max,
                            wqes@.len() == i,
                        decreases max - i,
                    {
                        vec_push(&mut wqes, open::EMPTY_WQE);
                        i += 1;
                    }
                    return Writing {
                        wqes,
                        n: 0,
                        limit: e.open_limit(),
                        max_len: e.max_len,
                        pd_id: e.pd_id,
                        kept: Kept::new(),
                        gathered: vec_new(),
                        reads: false,
                        max_sges: 1,
                        ledger: Ghost(Seq::empty()),
                    };
                },
                None => {},
            }
        }
        Writing::closed()
    }

    /// What `open` keeps from QP `index` goes back there for the next request, if it has no WQE:
    /// the WQEs it has stay where they are, to go out or to be dropped with it.
    #[inline(always)]
    #[verifier::spinoff_prover]
    pub fn give_back(&mut self, index: usize, open: &mut Writing<N>)
        requires
            old(self).inv(),
            old(open).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            final(open).inv(),
            forall|seq: int| #[trigger] final(self).staged_ctx(index as int, seq) == old(self).staged_ctx(index as int, seq),
        no_unwind
    {
        if open.n == 0 && index < self.qps.len() {
            let ghost start = *self;
            proof {
                assert(qp_ok(start.qps@[index as int], index as int, start.token@, start.nic()));
            }
            match &mut self.qps[index] {
                Some(e) => {
                    if e.spare_wqes.len() == 0 && open.wqes.len() == e.send.max_wqes() as usize {
                        core::mem::swap(&mut e.spare_wqes, &mut open.wqes);
                    }
                    open.limit = 0;
                    if capacity_of(&open.gathered) > 0 {
                        core::mem::swap(&mut e.spare_sges, &mut open.gathered);
                    }
                    proof {
                        self.lemma_flags_only(&start, index as int);
                    }
                },
                None => {
                    proof {
                        assert(self.qps@ =~= start.qps@);
                        self.lemma_untouched(&start);
                    }
                },
            }
        }
    }

    /// [`Hw::close`], first waiting for room (this polls the CQ).
    #[verifier::spinoff_prover]
    pub fn close_waiting(&mut self, index: usize, open: &mut Writing<N>, signal: bool, ctx: Option<C>) -> (r: core::result::Result<RingSeq, Option<C>>)
        requires
            old(self).inv(),
            old(open).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            final(open).inv(),
            r matches Err(back) ==> back == ctx,
        no_unwind
    {
        let ghost start = *self;
        if open.n > 0 && !self.open_fits(index, open) && self.open_belongs(index, open) {
            proof {
                assert(qp_ok(self.qps@[index as int], index as int, self.token@, self.token@.nic()));
                self.qps@[index as int]->0.send.lemma_wqes();
            }
            let _ = self.make_send_room(index, open.n as u32);
        }
        let ghost mid = *self;
        let r = self.close(index, open, signal, ctx);
        proof {
            lemma_same_table_trans(self, &mid, &start);
        }
        r
    }

    /// `open` as a request of its own: closed, signaled if `signal`, with `ctx` to come back with
    /// its completion, and the doorbell; what it keeps from the QP goes back. If the queue has no
    /// room it waits for some (this polls the CQ). If that or the doorbell fails, its context and
    /// its buffers come back.
    #[inline]
    #[verifier::spinoff_prover]
    pub fn submit_open(&mut self, index: usize, open: &mut Writing<N>, signal: bool, ctx: C) -> (r: core::result::Result<RingSeq, Refusal<C, N>>)
        requires
            old(self).inv(),
            old(open).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            final(open).inv(),
            r matches Err(refused) ==> refused.ctx == Some(ctx),
        no_unwind
    {
        let ghost start = *self;
        match self.close(index, open, signal, Some(ctx)) {
            Ok(seq) => {
                let ghost s1 = *self;
                self.give_back(index, open);
                let ghost s2 = *self;
                let r = self.commit_own(index, seq);
                proof {
                    lemma_same_table_trans(&s2, &s1, &start);
                    lemma_same_table_trans(self, &s2, &start);
                }
                r
            },
            Err(ctx) => {
                let ghost s1 = *self;
                let r = self.submit_open_other(index, open, signal, ctx);
                proof {
                    lemma_same_table_trans(self, &s1, &start);
                }
                r
            },
        }
    }

    /// [`Hw::submit_open`] out of the common: no WQE, or no room.
    #[cold]
    #[inline(never)]
    #[verifier::spinoff_prover]
    fn submit_open_other(&mut self, index: usize, open: &mut Writing<N>, signal: bool, ctx: Option<C>) -> (r: core::result::Result<RingSeq, Refusal<C, N>>)
        requires
            old(self).inv(),
            old(open).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            final(open).inv(),
            r matches Err(refused) ==> refused.ctx == ctx,
        no_unwind
    {
        let ghost start = *self;
        if open.is_empty() {
            self.give_back(index, open);
            return Err(Refusal { error: Error::Invalid("a request without a WQE"), ctx, leases: Leases::new() });
        }
        if !self.open_belongs(index, open) {
            let leases = open.take_leases();
            return Err(Refusal { error: Error::Invalid("a request written for another QP"), ctx, leases });
        }
        proof {
            assert(qp_ok(self.qps@[index as int], index as int, self.token@, self.token@.nic()));
            self.qps@[index as int]->0.send.lemma_wqes();
        }
        match self.make_send_room(index, open.n as u32) {
            Ok(()) => {},
            Err(error) => {
                let leases = open.take_leases();
                let ghost s1 = *self;
                self.give_back(index, open);
                proof {
                    lemma_same_table_trans(self, &s1, &start);
                }
                return Err(Refusal { error, ctx, leases });
            },
        }
        let ghost s1 = *self;
        match self.close(index, open, signal, ctx) {
            Ok(seq) => {
                let ghost s2 = *self;
                self.give_back(index, open);
                let ghost s3 = *self;
                let r = self.commit_own(index, seq);
                proof {
                    lemma_same_table_trans(&s2, &s1, &start);
                    lemma_same_table_trans(&s3, &s2, &start);
                    lemma_same_table_trans(self, &s3, &start);
                }
                r
            },
            Err(ctx) => {
                let leases = open.take_leases();
                let ghost s2 = *self;
                self.give_back(index, open);
                proof {
                    lemma_same_table_trans(&s2, &s1, &start);
                    lemma_same_table_trans(self, &s2, &start);
                }
                Err(Refusal { error: Error::QueueFull, ctx, leases })
            },
        }
    }

    /// What an owner holds of a completed request of several buffers: frozen ones are released, and
    /// the list goes back to QP `index` for the next such request, unless several unique buffers
    /// have to leave in it.
    #[cold]
    #[inline(never)]
    #[verifier::spinoff_prover]
    pub fn hand_back_leases(&mut self, index: usize, leases: &mut Leases<N>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            final(leases).uniques() == old(leases).uniques(),
        no_unwind
    {
        if index < self.qps.len() {
            let ghost start = *self;
            proof {
                assert(qp_ok(start.qps@[index as int], index as int, start.token@, start.nic()));
            }
            match &mut self.qps[index] {
                Some(e) => {
                    leases.hand_back_many(&mut e.spare_leases);
                    proof {
                        self.lemma_flags_only(&start, index as int);
                    }
                },
                None => {
                    proof {
                        assert(self.qps@ =~= start.qps@);
                        self.lemma_untouched(&start);
                    }
                },
            }
        }
    }

    /// Takes what giving up requests let go of.
    pub fn take_let_go(&mut self) -> (let_go: LetGo<C>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
        no_unwind
    {
        let mut let_go = LetGo::new();
        core::mem::swap(&mut let_go, &mut self.let_go);
        let_go
    }

    /// Whether `open` can be closed now on QP `index`: its WQEs fit in the queue.
    #[inline(always)]
    pub fn open_fits(&self, index: usize, open: &Writing<N>) -> (fits: bool)
        requires
            self.inv(),
        no_unwind
    {
        if index < self.qps.len() {
            proof {
                assert(qp_ok(self.qps@[index as int], index as int, self.token@, self.token@.nic()));
            }
            match &self.qps[index] {
                Some(e) => e.send.can_post() && open.n as u64 <= e.send.wqe_room() as u64,
                None => false,
            }
        } else {
            false
        }
    }

    /// Whether QP `index` takes what `open` was written for.
    fn open_belongs(&self, index: usize, open: &Writing<N>) -> (belongs: bool)
        requires
            self.inv(),
            open.inv(),
        ensures
            belongs ==> self.has_qp(index as int) && self.qps@[index as int]->0.spec_takes(*open),
        no_unwind
    {
        if index < self.qps.len() {
            proof {
                assert(qp_ok(self.qps@[index as int], index as int, self.token@, self.token@.nic()));
            }
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
    /// It takes what `open` was written for, see [`HwQp::takes`].
    spec fn spec_takes(&self, open: Writing<N>) -> bool {
        &&& open.pd_id == self.pd_id
        &&& open.max_len == self.max_len
        &&& open.n <= self.send.spec_max_wqes()
        &&& open.reads ==> self.reliable
        &&& open.max_sges <= self.caps.max_send_sge
        &&& open.max_sges <= self.caps.max_recv_sge
    }

    /// Whether it takes what `open` was written for, which is a value of its own: of its protection
    /// domain, as long as its messages, in as many WQEs as a request of it may have. Anything else
    /// a WQE asks of a QP is out of the common.
    #[inline(always)]
    fn takes(&self, open: &Writing<N>) -> (takes: bool)
        requires
            self.send.inv(),
            1 <= open.max_sges,
            1 <= self.caps.max_send_sge,
            1 <= self.caps.max_recv_sge,
        ensures
            takes == self.spec_takes(*open),
        no_unwind
    {
        open.pd_id == self.pd_id && open.max_len == self.max_len && open.n <= self.send.max_wqes() as usize
            && (!(open.reads || open.max_sges > 1) || self.takes_other(open))
    }

    /// [`HwQp::takes`] of WQEs that the NIC reads the answer of, or of several elements.
    #[cold]
    #[inline(never)]
    fn takes_other(&self, open: &Writing<N>) -> (takes: bool)
        ensures
            takes == ((open.reads ==> self.reliable)
                && open.max_sges <= self.caps.max_send_sge && open.max_sges <= self.caps.max_recv_sge),
        no_unwind
    {
        (!open.reads || self.reliable)
            && open.max_sges <= self.caps.max_send_sge && open.max_sges <= self.caps.max_recv_sge
    }

    /// How many WQEs a request being written may take without asking: as many as a request may
    /// have; none while it takes no work. The queue's room is looked at when it goes out, since a
    /// request is written with nothing held.
    #[inline(always)]
    fn open_limit(&self) -> (limit: usize)
        requires
            self.send.inv(),
        ensures
            limit <= self.send.spec_max_wqes(),
            limit == 0 || limit == self.send.spec_max_wqes(),
        no_unwind
    {
        if self.takes_work {
            self.send.max_wqes() as usize
        } else {
            0
        }
    }
}

/// The bytes of the first `i` elements, one more element at a time.
pub(super) proof fn lemma_sges_len_step(s: Seq<Sge>, i: int)
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

/// A prefix of a list has no more bytes than the list.
pub(super) proof fn lemma_sges_len_prefix(s: Seq<Sge>, i: int)
    requires
        0 <= i <= s.len(),
    ensures
        sges_len(s.take(i)) <= sges_len(s),
    decreases s.len() - i,
{
    if i < s.len() {
        lemma_sges_len_step(s, i);
        lemma_sges_len_prefix(s, i + 1);
    } else {
        assert(s.take(i) =~= s);
    }
}

/// The WR of a ledger's entry is in that ledger.
pub(super) proof fn lemma_ledger_wr<N: Device>(leases: Leases<N>, j: int, id: u64, signaled: bool, op: WrOp, sges: Seq<Sge>)
    requires
        0 <= j < leases.wqes().len(),
        leases.wqes()[j].wr_id == id,
        leases.wqes()[j].opcode == Some(opcode_model(op)),
        leases.wqes()[j].sges == sges,
    ensures
        ledger_has(leases, id, Some(opcode_model(op)), send_wr(id, signaled, op, sges).sges),
{
    assert(leases.wqes()[j].sges.map_values(|s: Sge| sge_model(s)) =~= send_wr(id, signaled, op, sges).sges);
}

/// `same_table` is transitive.
proof fn lemma_same_table_trans<C, N: Device>(a: &Hw<C, N>, b: &Hw<C, N>, c: &Hw<C, N>)
    requires
        a.same_table(b),
        b.same_table(c),
        a.moved_from(b),
        b.moved_from(c),
    ensures
        a.same_table(c),
        a.moved_from(c),
{
    completion::lemma_same_qps_trans(a.qps@, b.qps@, c.qps@);
    a.lemma_moved_trans(b, c);
}

impl<C, N: Device> Hw<C, N> {
    /// Waits until a new send request of `wqes` WQEs fits on QP `index`: rings its doorbell for
    /// what is written and polls the CQ until the WQEs are free. Fails if it has no free slot, which
    /// no completion gives it (its slots are twice its depth, so they are held by results nobody
    /// took, or its counters have reached their end), or if the doorbell is refused.
    ///
    /// It waits only for a signaled WQE out (`Ring::lemma_room_or_signal`), whose completion makes
    /// room: that the device completes it is the premise of liveness, and all that this loop's end
    /// rests on.
    #[verifier::exec_allows_no_decreases_clause]
    #[verifier::spinoff_prover]
    fn make_send_room(&mut self, index: usize, wqes: u32) -> (r: Result<()>)
        requires
            old(self).inv(),
            old(self).has_qp(index as int),
            1 <= wqes <= old(self).qps@[index as int]->0.send.spec_max_wqes(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
        no_unwind
    {
        let ghost start = *self;
        loop
            invariant
                self.inv(),
                self.same_table(&start),
                self.moved_from(&start),
                start.has_qp(index as int),
                start == *old(self),
                index < self.qps@.len(),
                self.qps@[index as int] is Some,
                1 <= wqes <= self.qps@[index as int]->0.send.spec_max_wqes(),
        {
            proof {
                assert(qp_ok(self.qps@[index as int], index as int, self.token@, self.token@.nic()));
            }
            let (room, free) = match &self.qps[index] {
                Some(e) => (e.send.can_post() && wqes <= e.send.wqe_room(), e.send.has_free_slot()),
                None => (false, false),
            };
            if room {
                return Ok(());
            }
            if !free {
                return Err(Error::Clogged);
            }
            proof {
                let ring = self.qps@[index as int]->0.send;
                ring.lemma_room_or_signal();
                assert(ring.spec_signal_out());
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
            final(self).moved_from(old(self)),
        no_unwind
    {
        if index >= self.qps.len() {
            return Err(BufError { error: Error::Invalid("no such QP"), buf: part });
        }
        let ghost start = *self;
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, start.token@, start.nic()));
        }
        match &mut self.qps[index] {
            Some(e) => {
                if e.building.len() >= MAX_SGES {
                    let max = e.caps.max_send_sge as usize;
                    proof {
                        assert(self.qps@ =~= start.qps@);
                    }
                    Err(BufError { error: Error::TooManySges { got: MAX_SGES + 1, max }, buf: part })
                } else {
                    vec_push(&mut e.building, part);
                    proof {
                        self.lemma_flags_only(&start, index as int);
                    }
                    Ok(())
                }
            },
            None => {
                proof {
                    assert(self.qps@ =~= start.qps@);
                }
                Err(BufError { error: Error::Invalid("no such QP"), buf: part })
            },
        }
    }

    /// Checks a WQE doing `op` on `parts` against QP `index`: what the QP may do, the parts'
    /// domain and uniqueness, the lengths.
    #[inline]
    fn check_wqe(e: &HwQp<C, N>, op: &WrOp, parts: &[Part<N>]) -> (r: Result<u64>)
        requires
            e.atomics ==> e.reliable,
        ensures
            r matches Ok(total) ==> {
                let list = parts_sges(parts@);
                &&& 1 <= parts@.len()
                &&& parts@.len() <= e.caps.max_send_sge
                &&& parts@.len() <= e.caps.max_recv_sge
                &&& total == sges_len(list)
                &&& total <= e.max_len
                &&& opcode_model(*op).writes_local() ==> e.reliable
                &&& forall|i: int| 0 <= i < parts@.len() ==> part_pd(#[trigger] parts@[i]) == e.pd_id
                &&& opcode_model(*op).writes_local() ==> forall|i: int| 0 <= i < parts@.len() ==> part_writable(#[trigger] parts@[i])
            },
        no_unwind
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
        let ghost list = parts_sges(parts@);
        proof {
            assert(list.take(0).map_values(|sge: Sge| sge_model(sge)) =~= Seq::<safeverbs_spec::nic::Sge>::empty());
        }
        while i < n
            invariant
                n == parts@.len(),
                list == parts_sges(parts@),
                i <= n,
                total == sges_len(list.take(i as int)),
                total <= e.max_len,
                writes_local == opcode_model(*op).writes_local(),
                forall|j: int| 0 <= j < i ==> part_pd(#[trigger] parts@[j]) == e.pd_id,
                writes_local ==> forall|j: int| 0 <= j < i ==> part_writable(#[trigger] parts@[j]),
            decreases n - i,
        {
            if part_pd_of(&parts[i]) != e.pd_id {
                return Err(Error::PdMismatch);
            }
            if writes_local && !part_writable_of(&parts[i]) {
                return Err(Error::Invalid("the NIC would write a frozen buffer"));
            }
            proof {
                lemma_sges_len_step(list, i as int);
            }
            total += part_sge_of(&parts[i]).length as u64;
            if total > e.max_len as u64 {
                return Err(Error::TooLong { len: total, max: e.max_len as u64 });
            }
            i += 1;
        }
        proof {
            assert(list.take(n as int) =~= list);
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
    /// own (signaled, without an owner), once it fits (this polls the CQ). Refused if QP `index`
    /// does not take what it was written for, or if it holds no room for a WQE.
    #[inline]
    #[verifier::spinoff_prover]
    fn open_room(&mut self, index: usize, open: &mut Writing<N>) -> (r: Result<()>)
        requires
            old(self).inv(),
            old(self).has_qp(index as int),
            old(open).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            final(open).same_fields(old(open)),
            r is Ok ==> final(open).n < final(open).wqes@.len()
                && final(open).n < usize::MAX
                && final(open).pd_id == final(self).qps@[index as int]->0.pd_id
                && final(open).max_len == final(self).qps@[index as int]->0.max_len,
        no_unwind
    {
        if !self.open_belongs(index, open) {
            return Err(Error::Invalid("a request written for another QP"));
        }
        proof {
            assert(qp_ok(self.qps@[index as int], index as int, self.token@, self.token@.nic()));
        }
        let k = open.n as u32;
        proof {
            self.qps@[index as int]->0.send.lemma_wqes();
        }
        let full = match &self.qps[index] {
            Some(e) => k >= e.send.max_wqes(),
            None => false,
        };
        if full {
            let ghost s0 = *self;
            match self.make_send_room(index, k) {
                Ok(()) => {},
                Err(error) => return Err(error),
            }
            let ghost s1 = *self;
            let _ = self.close(index, open, true, None);
            proof {
                lemma_same_table_trans(self, &s1, &s0);
            }
        }
        if open.n < open.wqes.len() {
            Ok(())
        } else {
            Err(Error::Invalid("a request that takes no WQE"))
        }
    }

    /// Adds a WQE doing `op` on `part` to `open`, whatever it takes: its QP is asked what
    /// [`Writing::try_push`] could not tell, and a request that is full goes out. On error
    /// nothing is added and the part is handed back.
    #[verifier::spinoff_prover]
    #[inline(never)]
    pub fn push_other(&mut self, index: usize, open: &mut Writing<N>, op: WrOp, part: Part<N>) -> (r: core::result::Result<(), BufError<Part<N>>>)
        requires
            old(self).inv(),
            old(open).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            final(open).inv(),
            r is Ok ==> final(self).has_qp(index as int),
            r matches Err(e) ==> e.buf == part,
        no_unwind
    {
        if index >= self.qps.len() {
            return Err(BufError { error: Error::Invalid("no such QP"), buf: part });
        }
        proof {
            assert(qp_ok(self.qps@[index as int], index as int, self.token@, self.token@.nic()));
        }
        let checked = match &self.qps[index] {
            Some(e) => {
                Self::check_wqe(e, &op, one_of(&part))
            },
            None => {
                return Err(BufError { error: Error::Invalid("no such QP"), buf: part });
            },
        };
        match checked {
            Ok(_) => {},
            Err(error) => return Err(BufError { error, buf: part }),
        }
        proof {
            open::lemma_parts_one(part);
            assert(part_pd(seq![part][0]) == self.qps@[index as int]->0.pd_id);
        }
        let ghost before_room = *self;
        match self.open_room(index, open) {
            Ok(()) => {},
            Err(error) => return Err(BufError { error, buf: part }),
        }
        let ghost start = *self;
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, start.token@, start.nic()));
            assert(start.qps@[index as int] is Some);
            assert(start.qps@[index as int]->0.same_config(before_room.qps@[index as int]->0));
        }
        match &mut self.qps[index] {
            Some(e) => {
                let k = open.n;
                let ghost before = *open;
                let sge = part_sge_of(&part);
                write_wqe(&mut open.wqes, k, OpenWqe { op, sge, first_sge: 0, sges: 1 });
                open.n = k + 1;
                if !open::plain_op(&op, 0) {
                    open.reads = true;
                }
                let ghost access = if op_writes_local(op) { safeverbs_spec::abs::Dir::In } else { safeverbs_spec::abs::Dir::Out };
                open.kept.keep_general(part, Ghost(access), &mut e.spare_leases);
                let ghost entry = crate::buf::WqeLoans {
                    wr_id: 0,
                    opcode: Some(opcode_model(op)),
                    sges: seq![sge],
                    first: before.kept.loans().len() as int,
                };
                open.ledger = Ghost(before.ledger@.push(entry));
                proof {
                    (*open).lemma_one_more(&before, part, access, entry);
                    self.lemma_flags_only(&start, index as int);
                    lemma_same_table_trans(self, &start, &before_room);
                }
            },
            None => {},
        }
        Ok(())
    }

    /// The list the parts of a WQE were gathered in goes back to QP `index`, empty, for the next one.
    #[verifier::spinoff_prover]
    fn reuse_gathered(&mut self, index: usize, parts: Vec<Part<N>>)
        requires
            old(self).inv(),
            old(self).has_qp(index as int),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
        no_unwind
    {
        let mut parts = parts;
        vec_clear(&mut parts);
        let ghost start = *self;
        match &mut self.qps[index] {
            Some(e) => {
                e.building = parts;
                proof {
                    assert(qp_ok(start.qps@[index as int], index as int, start.token@, start.nic()));
                    self.lemma_flags_only(&start, index as int);
                }
            },
            None => {},
        }
    }

    /// Finishes the WQE gathered on QP `index` with operation `op`, as [`Hw::push_other`] does a WQE
    /// of one part. On error nothing is added and the parts are dropped.
    #[verifier::spinoff_prover]
    pub fn push_gathered(&mut self, index: usize, open: &mut Writing<N>, op: WrOp) -> (r: Result<()>)
        requires
            old(self).inv(),
            old(open).inv(),
        ensures
            final(self).inv(),
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
            final(open).inv(),
        no_unwind
    {
        if index >= self.qps.len() {
            return Err(Error::Invalid("no such QP"));
        }
        let ghost start = *self;
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, start.token@, start.nic()));
        }
        // The parts leave the QP's list while whatever makes room for them goes on.
        let mut parts = vec_new();
        let checked = match &mut self.qps[index] {
            Some(e) => {
                core::mem::swap(&mut parts, &mut e.building);
                Self::check_wqe(e, &op, parts.as_slice())
            },
            None => {
                proof {
                    assert(self.qps@ =~= start.qps@);
                }
                return Err(Error::Invalid("no such QP"));
            },
        };
        proof {
            self.lemma_flags_only(&start, index as int);
        }
        let ghost s1 = *self;
        match checked {
            Ok(_) => {},
            Err(error) => {
                self.reuse_gathered(index, parts);
                proof {
                    lemma_same_table_trans(self, &s1, &start);
                }
                return Err(error);
            },
        }
        match self.open_room(index, open) {
            Ok(()) => {},
            Err(error) => {
                let ghost s2 = *self;
                self.reuse_gathered(index, parts);
                proof {
                    lemma_same_table_trans(&s2, &s1, &start);
                    lemma_same_table_trans(self, &s2, &start);
                }
                return Err(error);
            },
        }
        let ghost s2 = *self;
        proof {
            lemma_same_table_trans(&s2, &s1, &start);
            assert(qp_ok(s2.qps@[index as int], index as int, s2.token@, s2.nic()));
            assert(s2.qps@[index as int] is Some);
            assert(s2.qps@[index as int]->0.same_config(start.qps@[index as int]->0));
        }
        let pushed = match &mut self.qps[index] {
            Some(e) => {
                let sges = parts.len();
                let ghost swapped_from = *open;
                if capacity_of(&open.gathered) == 0 {
                    core::mem::swap(&mut open.gathered, &mut e.spare_sges);
                }
                proof {
                    (*open).lemma_gathered_swapped(&swapped_from);
                }
                if open.gathered.len() > u32::MAX as usize - MAX_SGES {
                    Err(Error::Invalid("a request of too many scatter/gather elements"))
                } else {
                    let ghost before = *open;
                    let ghost built = parts@;
                    let ghost base_gathered = open.gathered@;
                    let first_sge = open.gathered.len() as u32;
                    let mut i = 0;
                    while i < sges
                        invariant
                            i <= sges,
                            sges == parts@.len(),
                            built == parts@,
                            first_sge == base_gathered.len(),
                            sges <= MAX_SGES,
                            base_gathered.len() <= u32::MAX - MAX_SGES,
                            open.gathered@.len() == base_gathered.len() + i,
                            forall|k: int| 0 <= k < base_gathered.len() ==> #[trigger] open.gathered@[k] == base_gathered[k],
                            forall|k: int| 0 <= k < i ==> #[trigger] open.gathered@[base_gathered.len() + k] == part_sge(built[k]),
                        decreases sges - i,
                    {
                        vec_push(&mut open.gathered, part_sge_of(&parts[i]));
                        i += 1;
                    }
                    let k = open.n;
                    write_wqe(&mut open.wqes, k, OpenWqe { op, sge: part_sge_of(&parts[0]), first_sge, sges: sges as u32 });
                    open.n = k + 1;
                    if !open::plain_op(&op, 0) {
                        open.reads = true;
                    }
                    if sges as u32 > open.max_sges {
                        open.max_sges = sges as u32;
                    }
                    let ghost access = if op_writes_local(op) { safeverbs_spec::abs::Dir::In } else { safeverbs_spec::abs::Dir::Out };
                    let ghost base_loans = open.kept.loans();
                    // In the order the hardware reads them is no matter to the leases.
                    while parts.len() > 0
                        invariant
                            open.kept.inv(),
                            e.spare_leases@.len() == 0,
                            built.len() == sges,
                            parts@.len() <= sges,
                            parts@ == built.take(parts@.len() as int),
                            open.kept.loans().len() == base_loans.len() + (sges - parts@.len()),
                            forall|j: int| 0 <= j < base_loans.len() ==> #[trigger] open.kept.loans()[j] == base_loans[j],
                            forall|j: int| parts@.len() <= j < sges
                                ==> #[trigger] open.kept.loans()[base_loans.len() + (sges - 1 - j)]
                                    == (crate::buf::Loan { part: built[j], access: Some(access) }),
                            access is In ==> forall|j: int| 0 <= j < sges ==> part_writable(#[trigger] built[j]),
                        decreases parts@.len(),
                    {
                        match vec_pop(&mut parts) {
                            Some(part) => open.kept.keep_general(part, Ghost(access), &mut e.spare_leases),
                            None => {},
                        }
                    }
                    let ghost entry = crate::buf::WqeLoans {
                        wr_id: 0,
                        opcode: Some(opcode_model(op)),
                        sges: parts_sges(built),
                        first: base_loans.len() as int,
                    };
                    open.ledger = Ghost(before.ledger@.push(entry));
                    proof {
                        (*open).lemma_gathered_more(&before, built, access, entry, base_gathered);
                    }
                    Ok(())
                }
            },
            None => Ok(()),
        };
        proof {
            self.lemma_flags_only(&s2, index as int);
        }
        let ghost s3 = *self;
        self.reuse_gathered(index, parts);
        proof {
            lemma_same_table_trans(&s3, &s2, &start);
            lemma_same_table_trans(self, &s3, &start);
        }
        pushed
    }
}

// =============================================================================
// The rings, for their owners and the event loop

impl<C, N: Device> Hw<C, N> {
    /// Whether `index` names a QP of the engine (not one being removed).
    pub fn is_qp(&self, index: usize) -> (b: bool)
        requires
            self.inv(),
        ensures
            b ==> self.keeps_qp(index as int),
        no_unwind
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
        no_unwind
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

        no_unwind
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
        no_unwind
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
        no_unwind
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
            final(self).moved_from(old(self)),
            final(self).keeps_requests(old(self)),
            final(self).same_walk(old(self)),
            r matches Some(c) ==> c.qp == index && c.queue == side && c.seq == seq
                && c.leases.inv() && c.leases.fully_recorded(),
            side is Recv ==> (r matches Some(c) ==> old(self).recv_taken_matches(index, seq, c)),
            side is Send ==> (r matches Some(c) ==> old(self).send_taken_matches(index, seq, c)),
            old(self).takeable(index, side, seq) ==> r is Some,
        no_unwind
    {
        let ghost start = *self;
        proof {
            start.lemma_keeps_requests_refl();
        }
        if index >= self.qps.len() {
            return None;
        }
        let ghost t = self.token@;
        let ghost m = t.nic();
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, t, m));
        }
        match &mut self.qps[index] {
            Some(e) => {
                let ghost e0 = *e;
                let taken = match side {
                    Side::Send => e.send.take(seq),
                    Side::Recv => e.recv.take(seq),
                };
                let ghost done = taken;
                // Field by field: nothing of the request is copied twice on its way out.
                match taken {
                    Some(Taken { outcome, leases: Payload { ctx: Some(wr_id), leases, .. } }) => {
                        let c = Completion { qp: index, qp_num: e.qp_num, queue: side, seq, wr_id, result: outcome, leases };
                        proof {
                            self.lemma_taken_left(&start, index as int, side, seq as int, true);
                            match side {
                                Side::Send => start.lemma_send_finished(index, seq, done->0, c),
                                Side::Recv => start.lemma_recv_finished(index, seq, done->0, c),
                            }
                            self.refined@ = self.lemma_taken_refined(&start, index as int, side, seq as int, true);
                        }
                        Some(c)
                    },
                    // A request with an owner holds a context, so nothing is lost here.
                    _ => {
                        proof {
                            assert(start.qps@[index as int] is Some);
                            self.lemma_taken_left(&start, index as int, side, seq as int, done is Some);
                            if done is Some {
                                match side {
                                    Side::Send => start.lemma_send_taken(index, seq, done->0),
                                    Side::Recv => start.lemma_recv_taken(index, seq, done->0),
                                }
                                assert(false);
                            }
                            self.refined@ = self.lemma_taken_refined(&start, index as int, side, seq as int, false);
                        }
                        None
                    },
                }
            },
            None => {
                proof {
                    assert(self.qps@ =~= start.qps@);
                    self.lemma_untouched(&start);
                }
                None
            },
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
            final(self).moved_from(old(self)),
        no_unwind
    {
        if index >= self.qps.len() {
            return (None, None);
        }
        let ghost start = *self;
        let ghost t = self.token@;
        let ghost m = t.nic();
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, t, m));
        }
        match &mut self.qps[index] {
            Some(e) => {
                let ghost e0 = *e;
                let given_up = match side {
                    Side::Send => e.send.give_up(seq),
                    Side::Recv => e.recv.give_up(seq),
                };
                proof {
                    let e1 = self.qps@[index as int]->0;
                    match side {
                        Side::Send => {
                            completion::lemma_given_up_kept(e1.send, e0.send, seq as int);
                            completion::lemma_same_kept(e1.recv, e0.recv);
                        },
                        Side::Recv => {
                            completion::lemma_given_up_kept(e1.recv, e0.recv, seq as int);
                            completion::lemma_same_kept(e1.send, e0.send);
                        },
                    }
                    completion::lemma_table_kept(start.qps@, self.qps@, start.next_name@, t, m, start.pending@, index as int);
                    refines::lemma_ok_complete_popped(e0, index as u16, m, t.share(), t.started(e0.id()), t.written(e0.id()), completion::queue_of(side));
                    self.refined@ = refines::lemma_linked_given_up(start.qps@, self.qps@, m, start.refined@, index as int, completion::queue_of(side), seq as int);
                }
                given_up
            },
            None => {
                proof {
                    assert(self.qps@ =~= start.qps@);
                }
                (None, None)
            },
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
            final(self).moved_from(old(self)),
        no_unwind
    {
        if index >= self.qps.len() {
            return None;
        }
        let ghost start = *self;
        let ghost t = self.token@;
        let ghost m = t.nic();
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, t, m));
        }
        match &mut self.qps[index] {
            Some(e) => {
                let ghost e0 = *e;
                let replaced = match side {
                    Side::Send => e.send.set_waker(seq, waker),
                    Side::Recv => e.recv.set_waker(seq, waker),
                };
                proof {
                    let e1 = self.qps@[index as int]->0;
                    match side {
                        Side::Send => {
                            completion::lemma_waker_kept(e1.send, e0.send);
                            completion::lemma_same_kept(e1.recv, e0.recv);
                        },
                        Side::Recv => {
                            completion::lemma_waker_kept(e1.recv, e0.recv);
                            completion::lemma_same_kept(e1.send, e0.send);
                        },
                    }
                    completion::lemma_table_kept(start.qps@, self.qps@, start.next_name@, t, m, start.pending@, index as int);
                    refines::lemma_ok_complete_popped(e0, index as u16, m, t.share(), t.started(e0.id()), t.written(e0.id()), completion::queue_of(side));
                    self.refined@ = refines::lemma_linked_waker(start.qps@, self.qps@, m, start.refined@, index as int, completion::queue_of(side), seq as int, crate::ring::waker_id(*waker));
                }
                replaced
            },
            None => {
                proof {
                    assert(self.qps@ =~= start.qps@);
                }
                None
            },
        }
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
            final(self).moved_from(old(self)),
            r matches Some(error) ==> old(self).detached_taken_matches(index, error),
        no_unwind
    {
        if index >= self.qps.len() {
            return None;
        }
        let ghost start = *self;
        let ghost t = self.token@;
        let ghost m = t.nic();
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, t, m));
        }
        match &mut self.qps[index] {
            Some(e) => {
                let ghost e0 = *e;
                let record = e.send.take_detached_error();
                proof {
                    let e1 = self.qps@[index as int]->0;
                    completion::lemma_same_kept(e1.send, e0.send);
                    completion::lemma_same_kept(e1.recv, e0.recv);
                    completion::lemma_table_kept(start.qps@, self.qps@, start.next_name@, t, m, start.pending@, index as int);
                    refines::lemma_linked_same_view(start.qps@, self.qps@, m, start.refined@, index as int, Queue::Send);
                }
                match record {
                    Some(d) => {
                        proof {
                            start.lemma_detached_finished(index, d);
                        }
                        Some(DetachedError { seq: d.seq, error: d.error })
                    },
                    None => None,
                }
            },
            None => {
                proof {
                    assert(self.qps@ =~= start.qps@);
                }
                None
            },
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
            final(self).moved_from(old(self)),
        no_unwind
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
            final(self).moved_from(old(self)),
        no_unwind
    {
        let ghost start = *self;
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, start.token@, start.nic()));
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
                    e.takes_work = false;
                    e.refused = false;
                }
                proof {
                    self.lemma_flags_only(&start, index as int);
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
            final(self).moved_from(old(self)),
        no_unwind
    {
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, t0, m0));
        }
        let busy = match &mut self.qps[index] {
            Some(e) => {
                let ghost q = e.id();
                let ghost e0 = *e;
                if e.started {
                    e.started = false;
                    abort(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                }
                // Nothing staged is posted: refused (their owners are gone too).
                e.send.abort_with_wakes(&mut self.panics);
                let ghost e1 = *e;
                let ghost t = self.token@;
                let ghost n = t.nic();
                let ghost mut g = start.refined@;
                proof {
                    if e0.started {
                        start.lemma_rely(t0, m0, n);
                        lemma_rely_frame(t0.share(), pinned(t0), m0, n);
                        g = refines::lemma_linked_rely(start.qps@, start.next_name@, t0, m0, n, start.pending@, pinned(t0), start.refined@);
                    } else {
                        lemma_abs_reaches_refl(g.abs);
                    }
                    assert(qp_ok(start.qps@[index as int], index as int, t0, n));
                    e1.lemma_refused(&e0, index as u16, n, t0.share(), t0.written(q), e0.send.spec_posted() as int);
                    completion::lemma_same_kept(e1.send, e1.send);
                    completion::lemma_same_kept(e1.recv, e1.recv);
                    // What was staged comes to nothing for A.
                    let refused = refines::lemma_linked_refused(start.qps@, start.qps@.update(index as int, Some(e1)), n, g, index as int, Queue::Send, e0.send.spec_posted() as int);
                    refused.lemma_extends_trans(g, start.refined@);
                    g = refused;
                    refines::lemma_ok_complete_popped(e1, index as u16, n, t0.share(), false, Seq::empty(), Queue::Send);
                    refines::lemma_ok_complete_popped(e1, index as u16, n, t0.share(), false, Seq::empty(), Queue::Recv);
                }
                let mut seq = e.send.head();
                while seq < e.send.posted()
                    invariant
                        completion::ring_kept(e.send, e1.send),
                        e.send.spec_detached_error() == e1.send.spec_detached_error(),
                        refines::has_entry(start.qps@, index as int),
                        refines::linked(start.qps@.update(index as int, Some(*e)), n, g),
                        refines::complete_popped(e.send, n, q, Queue::Send),
                        g.extends(start.refined@),
                        e.id() == q,
                        e.recv == e1.recv,
                    decreases e.send.spec_posted() - seq,
                {
                    let ghost before = e.send;
                    let ghost was = *e;
                    // A context's destructor is the program's: it runs when the engine is let
                    // go of, never during a poll.
                    let (ctx, waker) = e.send.give_up(seq);
                    self.let_go.keep(ctx, waker);
                    proof {
                        completion::lemma_given_up_kept(e.send, before, seq as int);
                        completion::lemma_ring_kept_trans(e.send, before, e1.send);
                        let given = refines::lemma_linked_given_up(start.qps@.update(index as int, Some(was)), start.qps@.update(index as int, Some(*e)), n, g, index as int, Queue::Send, seq as int);
                        given.lemma_extends_trans(g, start.refined@);
                        g = given;
                    }
                    seq += 1;
                }
                let ghost old_send = e.send;
                let mut seq = e.recv.head();
                while seq < e.recv.posted()
                    invariant
                        completion::ring_kept(e.recv, e1.recv),
                        e.recv.spec_detached_error() == e1.recv.spec_detached_error(),
                        refines::has_entry(start.qps@, index as int),
                        refines::linked(start.qps@.update(index as int, Some(*e)), n, g),
                        refines::complete_popped(e.recv, n, q, Queue::Recv),
                        g.extends(start.refined@),
                        e.id() == q,
                        e.send == old_send,
                    decreases e.recv.spec_posted() - seq,
                {
                    let ghost before = e.recv;
                    let ghost was = *e;
                    let (ctx, waker) = e.recv.give_up(seq);
                    self.let_go.keep(ctx, waker);
                    proof {
                        completion::lemma_given_up_kept(e.recv, before, seq as int);
                        completion::lemma_ring_kept_trans(e.recv, before, e1.recv);
                        let given = refines::lemma_linked_given_up(start.qps@.update(index as int, Some(was)), start.qps@.update(index as int, Some(*e)), n, g, index as int, Queue::Recv, seq as int);
                        given.lemma_extends_trans(g, start.refined@);
                        g = given;
                    }
                    seq += 1;
                }
                let busy = e.send.in_flight_wqes() > 0 || e.recv.in_flight_wqes() > 0;
                proof {
                    (*e).lemma_kept(&e1, index as u16, n, t0.share(), Seq::empty());
                    assert(t.started(q) == e.started);
                    assert(t.written(q) == Seq::<SendWr>::empty());
                    assert(self.entry_ok(index as int, t, n));
                    self.lemma_one(&start, t0, t, n, n, index as int);
                    assert(self.qps@ =~= start.qps@.update(index as int, Some(*e)));
                    self.refined@ = g;
                }
                busy
            },
            None => false,
        };
        busy
    }

    /// Whether QP `index`, being removed, still has work in flight (or its
    /// drain WQE to post).
    pub fn draining(&self, index: usize) -> bool
        requires
            self.inv(),
        no_unwind
    {
        if index >= self.qps.len() {
            return false;
        }
        proof {
            assert(qp_ok(self.qps@[index as int], index as int, self.token@, self.token@.nic()));
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
            final(self).moved_from(old(self)),
            written ==> final(self).has_qp(index as int),
        no_unwind
    {
        if index >= self.qps.len() {
            return false;
        }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, t0, m0));
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
                let ghost e0 = *e;
                proof {
                    e0.send.lemma_wqes();
                }
                let wqe = e.send.next_wqe();
                if !e.started {
                    crate::trusted::start(&mut e.hw, Ghost(q), Tracked(self.token.borrow_mut()));
                    e.started = true;
                }
                let ghost w0 = self.token@.written(q);
                let nothing: Vec<Sge> = vec_new();
                let none = nothing.as_slice();
                post(&mut e.hw, wr_id(index as u16, false, wqe), true, &WrOp::Send, none, Ghost(q), Tracked(self.token.borrow_mut()));
                let ghost wr = send_wr(spec_wr_id(index as u16, false, wqe), true, WrOp::Send, none@);
                let mut leases = no_leases();
                let ghost entry = crate::buf::WqeLoans {
                    wr_id: spec_wr_id(index as u16, false, wqe),
                    opcode: Some(safeverbs_spec::nic::Opcode::Send),
                    sges: Seq::empty(), first: 0,
                };
                leases.record_wqe(Ghost(entry));
                let payload = Payload { ctx: None, leases, holds: Ghost(Map::empty()) };
                let ghost resource = payload;
                let _seq = e.send.stage(1, true, true, Ok(Completed::Sent), payload);
                e.send_tails = Ghost(e0.send_tails@.insert(_seq as int, wr));
                proof {
                    let t = self.token@;
                    let more = seq![wr];
                    assert(none@ =~= Seq::<Sge>::empty());
                    assert(wr.sges =~= Seq::<safeverbs_spec::nic::Sge>::empty());
                    lemma_ledger_wr(resource.leases, 0, wr.wr_id, true, WrOp::Send, none@);
                    assert(m0.send_wr_ok(m0.qps[q].attr, wr));
                    assert(t.written(q) =~= w0 + more);
                    assert(more.last() == wr);
                    (*e).lemma_staged(&e0, index as u16, m0, sh, w0, more, crate::ring::ReqView::new(true, resource), Ok(Completed::Sent));
                    assert(self.entry_ok(index as int, t, m0));
                    self.lemma_one(&start, t0, t, m0, m0, index as int);
                    refines::lemma_linked_staged(start.qps@, self.qps@, m0, start.refined@, index as int, Queue::Send, 1, crate::ring::ReqView::new(true, resource), Ok(Completed::Sent));
                }
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
            final(self).moved_from(old(self)),
        no_unwind
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
            start.qps@[index]->0.send.spec_wqe_retired() == start.qps@[index]->0.send.spec_wqe_committed(),
            start.qps@[index]->0.recv.spec_wqe_retired() == start.qps@[index]->0.recv.spec_wqe_committed(),
            cq_of(self.cq@) == cq_of(start.cq@),
            self.caps == start.caps,
            self.cq_size == start.cq_size,
            self.next_name == start.next_name,
            self.pending == start.pending,
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
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] qp_ok(self.qps@[i], i, t, n) by {
            if i != index {
                assert(qp_ok(start.qps@[i], i, t, mid));
                if let Some(e) = self.qps@[i] {
                    assert(e.id() != q);
                    lemma_ok_same(e, i as u16, mid, n, t.share(), t.started(e.id()), t.written(e.id()));
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
        // No CQE read names it: they name WQEs in flight.
        assert(qp_ok(start.qps@[index], index, t, mid));
        assert forall|j: int| 0 <= j < self.pending@.len() implies cqe_index(#[trigger] self.pending@[j]) != index by {
            let e = self.pending@[j];
            assert(cqe_ok(start.qps@, mid, e));
            let y = start.qps@[index]->0;
            if cqe_index(e) == index {
                let qp = mid.qps[e.qp];
                assert(e.seq < head_of(qp, e.queue));
            }
        }
        assert forall|i: int| 0 <= i < start.qps@.len() && i != index implies #[trigger] same_for_cqes(start.qps@[i], self.qps@[i], mid, n) by {
            assert(qp_ok(start.qps@[i], i, t, mid));
            assert(self.qps@[i] == start.qps@[i]);
            if let Some(e) = start.qps@[i] {
                assert(e.id() != q);
            }
        }
        lemma_pending_but_one(start.qps@, self.qps@, mid, n, self.pending@, index);
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
            final(self).moved_from(old(self)),
            !destroyed ==> *final(self) == *old(self),
        no_unwind
    {
        if index >= self.qps.len() {
            return false;
        }
        let ghost start = *self;
        let ghost t0 = self.token@;
        let ghost m0 = t0.nic();
        let ghost sh = t0.share();
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, t0, m0));
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
            assert(qp_ok(start.qps@[index as int], index as int, t, mid));
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
            // A's `detach`, `drop` and `drop_qp`: nothing is in flight, so nothing is made up.
            let e0 = start.qps@[index as int]->0;
            let post = start.qps@.update(index as int, None);
            assert(self.qps@ =~= post);
            let g1 = refines::lemma_linked_rely(start.qps@, start.next_name@, t0, m0, mid, start.pending@, pinned(t0), start.refined@);
            refines::lemma_ok_complete_popped(e0, index as u16, mid, sh, t.started(q), t.written(q), Queue::Send);
            refines::lemma_ok_complete_popped(e0, index as u16, mid, sh, t.started(q), t.written(q), Queue::Recv);
            let g2 = refines::lemma_linked_destroyed(start.qps@, post, mid, g1, index as int);
            assert forall|j: int| refines::has_entry(post, j) implies refines::keeps_queues(mid, n, #[trigger] post[j]->0.id()) by {
                assert(post[j] == start.qps@[j]);
                assert(qp_ok(start.qps@[j], j, t, mid));
                assert(start.qps@[j]->0.id() != q);
            }
            let g3 = refines::lemma_linked_verb(post, mid, n, Nic::Step::destroy_qp(q), g2);
            g2.lemma_extends_trans(g1, start.refined@);
            g3.lemma_extends_trans(g2, start.refined@);
            lemma_nic_reaches_trans(m0, mid, n);
            self.refined@ = g3;
        }
        true
    }

    /// Ends the engine: destroys its CQ if no QP is left (each destroyed
    /// before: M's `destroy_cq` needs none attached). Otherwise hands the
    /// engine back: a QP the device never flushed keeps it, and the caller
    /// leaks it all (no memory the NIC may use is ever freed).
    pub fn finish(&mut self) -> (finished: bool)
        requires
            old(self).inv(),
    {
        let mut i = 0;
        while i < self.qps.len()
            invariant
                *self == *old(self),
                old(self).inv(),
                0 <= i <= self.qps@.len(),
                forall|j: int| 0 <= j < i ==> (#[trigger] self.qps@[j]) is None,
            decreases self.qps@.len() - i,
        {
            if self.qps[i].is_some() {
                return false;
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
        destroy_cq(&mut self.cq, Tracked(self.token.borrow_mut()));
        true
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
            final(self).moved_from(old(self)),
        no_unwind
    {
        if index >= self.qps.len() {
            return;
        }
        let ghost start = *self;
        proof {
            assert(qp_ok(start.qps@[index as int], index as int, start.token@, start.nic()));
        }
        match &mut self.qps[index] {
            Some(e) => {
                vec_clear(&mut e.building);
                proof {
                    self.lemma_flags_only(&start, index as int);
                }
            },
            None => {
                proof {
                    assert(self.qps@ =~= start.qps@);
                }
            },
        }
    }

    /// The number of slots of the QP table: every QP's index is below it.
    pub fn qp_slots(&self) -> usize
        no_unwind
    {
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
            final(self).same_table(old(self)),
            final(self).moved_from(old(self)),
        no_unwind
    {
        if index >= self.qps.len() || self.qps[index].is_none() {
            return Err(Error::Invalid("no such QP"));
        }
        proof {
            assert(qp_ok(self.qps@[index as int], index as int, self.token@, self.token@.nic()));
            self.qps@[index as int]->0.send.lemma_wqes();
        }
        self.make_send_room(index, 1)
    }
}

/// The device refused the only request written: what its slot got comes back.
#[cold]
#[inline(never)]
fn not_posted<C, N: Device>(ring: &mut Ring<Payload<C, N>>, error: Error) -> (r: Refusal<C, N>)
    requires
        old(ring).is_filled(),
    ensures
        final(ring).inv(),
        final(ring).same_but_free(old(ring)),
        r.ctx == old(ring).filled_view().leases.ctx,
    no_unwind
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
    ensures
        r is Ok,
        r matches Ok(done) ==> opcode_model(*op).allows_outcome(len as nat, done.model()),
    no_unwind
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

/// The way the NIC touches the local buffer of `op`.
pub open spec fn send_access(op: WrOp) -> safeverbs_spec::abs::Dir {
    if opcode_model(op).writes_local() {
        safeverbs_spec::abs::Dir::In
    } else {
        safeverbs_spec::abs::Dir::Out
    }
}

} // verus!

impl<C, N: Device> Hw<C, N> {
    /// The bytes of a QP in the table.
    pub(crate) const QP_BYTES: usize = core::mem::size_of::<HwQp<C, N>>();

    /// A QP the device never flushed stays: nothing of it is freed, and the device stays open for
    /// it. Verus sees no drop, so this is what says so, at the one place a QP's entry could go
    /// without [`Hw::destroy`].
    pub(crate) fn leak(&mut self) {
        core::mem::forget(core::mem::take(&mut self.qps));
        core::mem::forget(self.ctx.clone());
    }

    /// The send ring of QP `index`, as tests shape it.
    #[cfg(test)]
    pub(crate) fn send_ring_mut(&mut self, index: usize) -> &mut Ring<Payload<C, N>> {
        &mut self.qps[index].as_mut().expect("a QP").send
    }
}
