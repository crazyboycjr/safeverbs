//! The hardware side of a worker, verified against the NIC model M: its CQ,
//! its QPs' hardware queues and rings, and its share of M.
//!
//! A [`Hw`] holds everything of a worker that M sees: the CQ, each QP's
//! hardware handle with the rings of its two queues, and the worker's token
//! (`crate::trusted::NicToken`: M's state as the worker last saw it, and its
//! share of M). Every call into the NIC goes through the trusted verbs of
//! `crate::trusted`, whose preconditions are M's, so Verus checks S2 at each
//! call. The invariant ([`Hw::inv`]) couples the rings to M:
//!
//! - **Routing.** QP `i` of the table is M's QP `id`; the WQEs M holds in
//!   its send queue are the ring's WQEs from (at or after) the ring's oldest
//!   in flight to the last committed, and the `k`-th has the `wr_id` of QP
//!   `i`, the send queue and WQE number `sq_head + k`
//!   (`crate::wr_id::spec_wr_id`); the same for the receive queue. So the
//!   completion M's `poll_cq` pops names the QP, the queue and the WQE that
//!   [`Hw::progress`] routes it to.
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
// The imports are used by ghost code too; `global` erases to a block.
#![allow(unused_imports, unused_braces)]

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
    id: Ghost<QpId>,
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

/// What the worker knows of the QP's state holds in M.
pub open spec fn phase_ok(phase: Phase, state: QpState) -> bool {
    match phase {
        Phase::Reset => state is Reset,
        Phase::Ready => state is Rts || state is Err,
        Phase::Broken => !(state is Rts),
    }
}

/// The send WRs written since the last doorbell are the ring's WQEs handed
/// out and not committed, each with its number's `wr_id`, each one M
/// accepts.
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
        &&& phase_ok(self.phase, qp.state)
        &&& self.phase != Phase::Ready ==> qp.sq.len() == 0 && qp.rq.len() == 0
            && self.send.spec_wqe_committed() == self.send.spec_wqe_retired()
            && self.recv.spec_wqe_committed() == self.recv.spec_wqe_retired()
        &&& self.started == started
        &&& writing_ok(self.send, index, m, qp.attr, started, written)
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
            Some(e) => e.ok(i as u16, m, t.share(), t.pd(), t.started(e.id()), t.written(e.id())),
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
        ensures
            self.ok(index, n, t.share(), t.pd(), t.started(self.id()), t.written(self.id())),
    {
        let q = self.id@;
        let (a, b) = (m.qps[q], n.qps[q]);
        assert(qp_kept(a, b));
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
    ensures
        e.ok(index, m2, sh, pd, started, written),
{
    let qp = m1.qps[e.id()];
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
            self.cq == other.cq,
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
        };
        let ghost before = *self;
        if index == self.qps.len() {
            self.qps.push(Some(entry));
        } else {
            self.qps.set(index, Some(entry));
        }
        self.reserved = self.reserved + need as u32;
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
    decreases tos.len(),
{
    if tos.len() == 0 {
        lemma_rely_frame(sh, pins, m, n);
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
    pub fn to_error(&mut self, index: usize) -> (r: Result<()>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
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
    pub fn connect(&mut self, index: usize, attr: &ConnectAttr, remote: &Endpoint) -> (r: Result<()>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
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
            assert(writing_ok(e.send, index as u16, n, b.attr, t0.started(q), written));
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
    pub fn post_recv(&mut self, index: usize, part: Part<N>, detached: bool) -> (r: core::result::Result<RingSeq, BufError<Part<N>>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
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
                        keep_lease(&mut leases, part);
                        e.recv.end(true, detached, implied, leases);
                        e.recv.commit();
                        Ok(seq)
                    },
                    Err(error) => {
                        e.recv.abort();
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
                let written = t1.written(q);
                assert forall|j: int| 0 <= j < written.len() implies n.send_wr_ok(b.attr, #[trigger] written[j]) by {
                    assert(mid.send_wr_ok(a.attr, written[j]));
                }
                assert(self.entry_ok(index as int, t1, n));
                self.lemma_touch_one(&mid1, t1, mid, n, index as int);
                self.lemma_same_token(t1, t, n);
            } else {
                mid1.lemma_rely(t1, m1, n);
                assert(mid1.entry_ok(index as int, t1, n));
                assert(rq_linked(e.recv, index as u16, n.qps[q]));
                assert(self.entry_ok(index as int, t1, n));
                self.lemma_touch_one(&mid1, t1, n, n, index as int);
                self.lemma_same_token(t1, t, n);
            }
        }
        r
    }
}

} // verus!
