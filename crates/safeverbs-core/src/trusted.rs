//! The trusted verbs: each call of the data path into the NIC, with its
//! contract taken from the NIC model M (`safeverbs_spec::nic`).
//!
//! A verb's precondition is M's (`nic_enabled`): Verus checks it at every
//! call site, which is S2. Its postcondition says that M moved by steps that
//! do not touch the caller's share (`rely_reaches`: the NIC's own steps,
//! executions and faults, and the verbs of other workers on their own CQs
//! and QPs) and then by the verb's step (`nic_step`), or, when the device
//! refuses, only by such steps. The
//! bodies are the calls of [`NicQp`] and [`NicCq`] and are not verified:
//! together with M they are the assumption. Every item of this module that
//! Verus trusts is listed in `scripts/trusted.txt`, and the trusted computing
//! base in `docs/verification.md` says why.
//!
//! M posts a list of send WRs at once (`post_send`); the hardware's
//! `ibv_wr_start`, `ibv_wr_*` and `ibv_wr_complete` write them one by one
//! and ring the doorbell at the end. The token keeps the WRs written since
//! the start, and the doorbell posts that list.
//!
//! The NIC's state is a ghost token, [`NicToken`], that only these wrappers
//! change and that nothing else can make. A token is one worker's: it holds
//! M's state as that worker last saw it and the worker's share of M
//! (`safeverbs_spec::share`: its CQ and the names of its QPs). Each verb is
//! on the token's own share (`mine`), so it is within what the other
//! workers rely on, and its precondition, checked in the state the worker
//! last saw, still holds when the verb happens
//! (`safeverbs_spec::share::lemma_rely_keeps_enabled`): that is why the
//! contracts below are consistent. The worker and its queue pairs are not
//! verified yet, so nothing calls them so far: they are the interface those
//! will be verified against (see `docs/verification.md`, "Stated, not yet
//! proved").
//
// The imports are used by ghost code too.
#![allow(unused_imports)]

// Globs: the spec functions are erased in a plain build, so they cannot be
// named in a `use` (the explicit imports below shadow the model's `Sge`).
use safeverbs_spec::nic::*;
use safeverbs_spec::refinement::*;
use safeverbs_spec::share::*;
use safeverbs_spec::types::{CqId, Key, QpId};
use vstd::prelude::*;

use crate::error::{Error, Result};
use crate::nic::{NicCq, NicQp, RemoteBuf, Sge, WrOp};
use crate::wc::Wc;

verus! {

// =============================================================================
// The exec types the contracts read (their definitions, as written)

/// [`Sge`], as defined.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
pub struct ExSge(Sge);

/// [`RemoteBuf`], as defined.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
pub struct ExRemoteBuf(RemoteBuf);

/// [`WrOp`], as defined.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
pub struct ExWrOp(WrOp);

/// [`Error`], opaque.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
#[verifier::external_body]
pub struct ExError(Error);

/// [`NicQp`], without specifications of its own (the wrappers carry them).
#[cfg(verus_keep_ghost)]
#[verifier::external_trait_specification]
pub trait ExNicQp: Send {
    /// The trait specified.
    type ExternalTraitSpecificationFor: NicQp;
}

/// [`NicCq`], without specifications of its own (the wrappers carry them).
#[cfg(verus_keep_ghost)]
#[verifier::external_trait_specification]
pub trait ExNicCq: Send {
    /// The trait specified.
    type ExternalTraitSpecificationFor: NicCq;
}

// =============================================================================
// What the exec values are in M

/// A scatter/gather element as M sees it.
pub open spec fn sge_model(sge: Sge) -> safeverbs_spec::nic::Sge {
    safeverbs_spec::nic::Sge { addr: sge.addr as int, len: sge.length as nat, lkey: sge.lkey as nat }
}

/// What a send WR does, as M sees it. M has no atomics; locally an atomic
/// is an RDMA READ of 8 bytes: the NIC writes the local buffer.
pub open spec fn opcode_model(op: WrOp) -> Opcode {
    match op {
        WrOp::Send => Opcode::Send,
        WrOp::SendImm(imm) => Opcode::SendImm(imm),
        WrOp::Write(_) => Opcode::Write,
        WrOp::WriteImm(_, imm) => Opcode::WriteImm(imm),
        WrOp::Read(_) => Opcode::Read,
        WrOp::FetchAdd { .. } => Opcode::Read,
        WrOp::CompareSwap { .. } => Opcode::Read,
    }
}

/// A send WR as M sees it (safeverbs never posts inline data).
pub open spec fn send_wr(wr_id: u64, signaled: bool, op: WrOp, sges: Seq<Sge>) -> SendWr {
    SendWr {
        wr_id,
        opcode: opcode_model(op),
        sges: sges.map_values(|sge: Sge| sge_model(sge)),
        signaled,
        inline: false,
    }
}

/// A receive WR as M sees it.
pub open spec fn recv_wr(wr_id: u64, sges: Seq<Sge>) -> RecvWr {
    RecvWr { wr_id, sges: sges.map_values(|sge: Sge| sge_model(sge)) }
}

/// A work completion reports CQE `e` of queue pair `qp`: its `wr_id` and its
/// outcome.
pub open spec fn reports(wc: Wc, e: Cqe) -> bool {
    &&& wc.wr_id == e.wr_id
    &&& wc.outcome() == e.outcome
}

// =============================================================================
// The token

/// What one worker knows of the NIC: M's state as it last saw it, its share
/// of M, and for each of its queue pairs the send WRs written since its last
/// start. Only the trusted wrappers make or change one: its fields are
/// private, so no other code can forge one.
// The fields are ghost: only the proofs read them.
#[allow(dead_code)]
pub tracked struct NicToken {
    ghost nic: Nic::State,
    ghost share: Share,
    ghost started: Set<QpId>,
    ghost written: Map<QpId, Seq<SendWr>>,
}

impl NicToken {
    /// The NIC's state in M, as the worker last saw it.
    pub closed spec fn nic(&self) -> Nic::State {
        self.nic
    }

    /// The worker's share of M: its CQ and the names of its QPs.
    pub closed spec fn share(&self) -> Share {
        self.share
    }

    /// `self` is `other` after calls of the same worker: the same share.
    pub open spec fn same_share(&self, other: &NicToken) -> bool {
        self.share() == other.share()
    }

    /// Queue pair `q` is writing send WRs (between start and the doorbell).
    pub closed spec fn started(&self, q: QpId) -> bool {
        self.started.contains(q)
    }

    /// The send WRs written to queue pair `q` since its start.
    pub closed spec fn written(&self, q: QpId) -> Seq<SendWr> {
        if self.written.contains_key(q) {
            self.written[q]
        } else {
            Seq::empty()
        }
    }

    /// `self` and `other` agree on queue pair `q`'s writing.
    pub open spec fn same_writing(&self, other: &NicToken, q: QpId) -> bool {
        &&& self.started(q) == other.started(q)
        &&& self.written(q) == other.written(q)
    }

    /// `self` and `other` agree on the writing of every queue pair but `q`.
    pub open spec fn same_writing_but(&self, other: &NicToken, q: QpId) -> bool {
        forall|p: QpId| p != q ==> #[trigger] self.started(p) == other.started(p) && self.written(p)
            == other.written(p)
    }
}

/// No region pinned: what the others may do between two calls that name no
/// memory.
pub open spec fn no_pins() -> ISet<Key> {
    ISet::empty()
}

/// `polled(sh, m, m2, wcs)`: polling the CQ of share `sh` took M from `m` to
/// `m2` and gave `wcs`: one `poll_cq` step for each, in order, each reporting
/// the CQE it popped, with steps that do not touch the share around them.
pub open spec fn polled(sh: Share, m: Nic::State, m2: Nic::State, wcs: Seq<Wc>) -> bool
    decreases wcs.len(),
{
    if wcs.len() == 0 {
        rely_reaches(sh, no_pins(), m, m2)
    } else {
        exists|mid: Nic::State, before: Nic::State|
            {
                &&& polled(sh, m, mid, wcs.drop_last())
                &&& rely_reaches(sh, no_pins(), mid, before)
                &&& nic_step(before, m2, Nic::Step::poll_cq(sh.cq))
                &&& reports(wcs.last(), before.cqs[sh.cq].entries[0])
            }
    }
}

// =============================================================================
// The verbs

/// `ibv_wr_start`: queue pair `q` starts writing send WRs.
#[verifier::external_body]
pub fn start<Q: NicQp>(qp: &mut Q, Ghost(q): Ghost<QpId>, Tracked(token): Tracked<&mut NicToken>)
    requires
        !old(token).started(q),
    ensures
        final(token).nic() == old(token).nic(),
        final(token).same_share(old(token)),
        final(token).started(q),
        final(token).written(q) == Seq::<SendWr>::empty(),
        final(token).same_writing_but(old(token), q),
{
    // SAFETY: not started (the precondition).
    unsafe { qp.start() }
}

/// `ibv_wr_*`: writes one send WR; it is posted at the doorbell.
#[verifier::external_body]
pub fn post<Q: NicQp>(
    qp: &mut Q,
    wr_id: u64,
    signaled: bool,
    op: &WrOp,
    sges: &[Sge],
    Ghost(q): Ghost<QpId>,
    Tracked(token): Tracked<&mut NicToken>,
)
    requires
        old(token).started(q),
    ensures
        final(token).nic() == old(token).nic(),
        final(token).same_share(old(token)),
        final(token).started(q),
        final(token).written(q) == old(token).written(q).push(send_wr(wr_id, signaled, *op, sges@)),
        final(token).same_writing_but(old(token), q),
{
    // SAFETY: started (the precondition). The memory stays valid until the
    // WR completes: the doorbell's precondition says it is registered, and
    // the ring keeps its leases until it retires.
    unsafe { qp.post(wr_id, signaled, op, sges) }
}

/// `ibv_wr_complete`: rings the doorbell, posting the send WRs written since
/// the start (M's `post_send` of that list), or, on error, none of them.
#[verifier::external_body]
pub fn doorbell<Q: NicQp>(
    qp: &mut Q,
    Ghost(q): Ghost<QpId>,
    Tracked(token): Tracked<&mut NicToken>,
) -> (r: Result<()>)
    requires
        old(token).started(q),
        nic_inv(old(token).nic()),
        share_wf(old(token).nic(), old(token).share()),
        mine(
            old(token).share(),
            send_keys(old(token).written(q)),
            Nic::Step::post_send(q, old(token).written(q)),
        ),
        nic_enabled(old(token).nic(), Nic::Step::post_send(q, old(token).written(q))),
    ensures
        final(token).same_share(old(token)),
        !final(token).started(q),
        final(token).written(q) == Seq::<SendWr>::empty(),
        final(token).same_writing_but(old(token), q),
        r is Ok ==> exists|mid: Nic::State|
            rely_reaches(old(token).share(), send_keys(old(token).written(q)), old(token).nic(), mid)
                && nic_step(
                mid,
                final(token).nic(),
                Nic::Step::post_send(q, old(token).written(q)),
            ),
        r is Err ==> rely_reaches(
            old(token).share(),
            send_keys(old(token).written(q)),
            old(token).nic(),
            final(token).nic(),
        ),
{
    // SAFETY: started (the precondition).
    unsafe { qp.complete() }
}

/// `ibv_wr_abort`: discards the send WRs written since the start.
#[verifier::external_body]
pub fn abort<Q: NicQp>(qp: &mut Q, Ghost(q): Ghost<QpId>, Tracked(token): Tracked<&mut NicToken>)
    requires
        old(token).started(q),
    ensures
        final(token).same_share(old(token)),
        !final(token).started(q),
        final(token).written(q) == Seq::<SendWr>::empty(),
        final(token).same_writing_but(old(token), q),
        rely_reaches(old(token).share(), no_pins(), old(token).nic(), final(token).nic()),
{
    // SAFETY: started (the precondition).
    unsafe { qp.abort() }
}

/// `ibv_post_recv` of one WR (M's `post_recv` of a list of one), or, on
/// error, nothing.
#[verifier::external_body]
pub fn post_recv<Q: NicQp>(
    qp: &mut Q,
    wr_id: u64,
    sges: &[Sge],
    Ghost(q): Ghost<QpId>,
    Tracked(token): Tracked<&mut NicToken>,
) -> (r: Result<()>)
    requires
        nic_inv(old(token).nic()),
        share_wf(old(token).nic(), old(token).share()),
        mine(
            old(token).share(),
            recv_keys(seq![recv_wr(wr_id, sges@)]),
            Nic::Step::post_recv(q, seq![recv_wr(wr_id, sges@)]),
        ),
        nic_enabled(old(token).nic(), Nic::Step::post_recv(q, seq![recv_wr(wr_id, sges@)])),
    ensures
        final(token).same_share(old(token)),
        forall|p: QpId| #[trigger] final(token).started(p) == old(token).started(p),
        forall|p: QpId| #[trigger] final(token).written(p) == old(token).written(p),
        r is Ok ==> exists|mid: Nic::State|
            rely_reaches(
                old(token).share(),
                recv_keys(seq![recv_wr(wr_id, sges@)]),
                old(token).nic(),
                mid,
            ) && nic_step(
                mid,
                final(token).nic(),
                Nic::Step::post_recv(q, seq![recv_wr(wr_id, sges@)]),
            ),
        r is Err ==> rely_reaches(
            old(token).share(),
            recv_keys(seq![recv_wr(wr_id, sges@)]),
            old(token).nic(),
            final(token).nic(),
        ),
{
    // SAFETY: the memory stays valid until the receive completes: the
    // precondition says it is registered, and the ring keeps its lease until
    // it retires.
    unsafe { qp.post_recv(wr_id, sges) }
}

/// `ibv_poll_cq` on the CQ of the token's share: up to `out.len()` CQEs,
/// each popped by M's `poll_cq` and reported in `out`, in order.
#[verifier::external_body]
pub fn poll<C: NicCq>(cq: &mut C, out: &mut [Wc], Tracked(token): Tracked<&mut NicToken>) -> (n:
    usize)
    ensures
        n <= old(out)@.len(),
        final(out)@.len() == old(out)@.len(),
        final(token).same_share(old(token)),
        polled(old(token).share(), old(token).nic(), final(token).nic(), final(out)@.take(n as int)),
        forall|q: QpId| #[trigger] final(token).started(q) == old(token).started(q),
        forall|q: QpId| #[trigger] final(token).written(q) == old(token).written(q),
{
    cq.poll(out)
}

} // verus!
