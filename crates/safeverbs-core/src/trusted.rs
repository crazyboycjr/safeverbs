//! The trusted verbs: each call of the core into the NIC, with its contract
//! taken from the NIC model M (`safeverbs_spec::nic`).
//!
//! A verb's precondition is M's (`nic_enabled`): Verus checks it at every
//! call site, which is S2. Its postcondition says that M moved by steps that
//! do not touch the caller's share (`rely_reaches`: the NIC's own steps,
//! executions and faults, and the verbs of other workers on their own CQs
//! and QPs) and then by the verb's step (`nic_step`), or, when the device
//! refuses, only by such steps. The bodies are the calls of [`Device`],
//! [`NicQp`] and [`NicCq`] and are not verified: together with M they are
//! the assumption. Every item of this module that Verus trusts is listed in
//! `scripts/trusted.txt`, and the trusted computing base in
//! `docs/verification.md` says why.
//!
//! M posts a list of send WRs at once (`post_send`); the hardware's
//! `ibv_wr_start`, `ibv_wr_*` and `ibv_wr_complete` write them one by one
//! and ring the doorbell at the end. The token keeps the WRs written since
//! the start, and the doorbell posts that list.
//!
//! The NIC's state is a ghost token, [`NicToken`], that only these wrappers
//! make and change. A token is one worker's: it holds M's state as that
//! worker last saw it, the worker's share of M (`safeverbs_spec::share`: its
//! CQ and the names of its QPs), the protection domain of its QPs and the
//! names it gives new QPs. Each verb is on the token's own share (`mine`),
//! so it is within what the other workers rely on, and its precondition,
//! checked in the state the worker last saw, still holds when the verb
//! happens (`safeverbs_spec::share::lemma_rely_keeps_enabled`): that is why
//! the contracts below are consistent. Between two calls the other workers
//! deregister no region the worker's written WRs name (`pinned`): those
//! are held by leases, which keep their regions registered ([`registered`]
//! is where the core learns it). The worker's hardware side
//! ([`crate::hw`]) is verified against these contracts.
//
// The imports are used by ghost code too.
#![allow(unused_imports)]

// Globs: the spec functions are erased in a plain build, so they cannot be
// named in a `use` (the explicit imports below shadow the model's `Sge`).
use safeverbs_spec::nic::*;
use safeverbs_spec::refinement::*;
use safeverbs_spec::share::*;
use safeverbs_spec::types::{CqId, Key, PdId, QpId};
use vstd::prelude::*;

use crate::buf::{BufError, Leases, Part};
use crate::error::{Error, Result};
// The device trait as `Device`: `Nic` names M's state machine here.
use crate::nic::{
    ConnectAttr, DeviceCaps, Endpoint, Nic as Device, NicCq, NicQp, QpCaps, QpType, RemoteBuf, Sge,
    WrOp,
};
use crate::pd::{Context, Pd};
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

/// [`QpCaps`], as defined.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
pub struct ExQpCaps(QpCaps);

/// [`DeviceCaps`], as defined.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
pub struct ExDeviceCaps(DeviceCaps);

/// [`QpType`], as defined.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
pub struct ExQpType(QpType);

/// [`Error`], as defined.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
pub struct ExError(Error);

/// [`BufError`], as defined.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
#[verifier::reject_recursive_types(B)]
pub struct ExBufError<B>(BufError<B>);

/// [`ConnectAttr`], opaque (only the device reads it).
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
#[verifier::external_body]
pub struct ExConnectAttr(ConnectAttr);

/// [`Endpoint`], opaque (only the device reads it).
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
#[verifier::external_body]
pub struct ExEndpoint(Endpoint);

/// [`Pd`], opaque.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
#[verifier::external_body]
#[verifier::reject_recursive_types(N)]
pub struct ExPd<N: Device>(Pd<N>);

/// [`Nic`], without specifications of its own (the wrappers carry them).
#[cfg(verus_keep_ghost)]
#[verifier::external_trait_specification]
pub trait ExNic: Send + Sync + Sized + 'static {
    /// The trait specified.
    type ExternalTraitSpecificationFor: Device;

    /// A protection domain.
    type Pd: Send + Sync + 'static;

    /// A registered memory region.
    type Mr: Send + Sync + 'static;

    /// A completion queue.
    type Cq: NicCq + 'static;

    /// A queue pair.
    type Qp: NicQp + 'static;

    /// A thread domain.
    type Td: Send + 'static;
    /// A parent domain.
    type Domain: Send + 'static;
}

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
pub closed spec fn reports(wc: Wc, e: Cqe) -> bool {
    &&& wc.wr_id == e.wr_id
    &&& wc.outcome() == e.outcome
}

/// Unpack a completion report inside the core without exporting raw Wc representation.
pub(crate) proof fn lemma_reports(wc: Wc, e: Cqe)
    requires
        reports(wc, e),
    ensures
        wc.wr_id == e.wr_id,
        wc.outcome() == e.outcome,
{
}

/// The transport M sees for a QP type.
pub open spec fn transport_model(t: QpType) -> Transport {
    match t {
        QpType::Rc => Transport::Rc,
        QpType::Uc => Transport::Uc,
    }
}

/// The attributes M sees for a QP the worker creates in protection domain
/// `pd` with both queues on CQ `cq`. M has one limit of scatter/gather
/// elements for both queues: the smaller of the two (the worker checks
/// every WQE against it). safeverbs posts no inline data, so M sees none.
pub open spec fn qp_attr(pd: PdId, qp_type: QpType, cq: CqId, caps: QpCaps) -> QpAttr {
    QpAttr {
        pd,
        transport: transport_model(qp_type),
        send_cq: cq,
        recv_cq: cq,
        sq_depth: caps.max_send_wr as nat,
        rq_depth: caps.max_recv_wr as nat,
        max_sge: if caps.max_send_sge <= caps.max_recv_sge {
            caps.max_send_sge as nat
        } else {
            caps.max_recv_sge as nat
        },
        max_inline: 0,
    }
}

/// The device's limits `d`, as the library reads them, are M's.
pub open spec fn caps_model(c: Caps, d: DeviceCaps) -> bool {
    &&& c.max_qp_wr == d.max_qp_wr as nat
    &&& c.max_sge == d.max_sge as nat
    &&& c.max_cqe == d.max_cqe as nat
    &&& c.max_msg == d.max_msg as nat
}

/// The name M gives the hardware QP `qp`.
pub uninterp spec fn qp_of<Q>(qp: Q) -> QpId;

/// The name M gives the hardware CQ `cq`.
pub uninterp spec fn cq_of<C>(cq: C) -> CqId;

/// The name M gives protection domain `pd` (its clones share it).
pub uninterp spec fn pd_of<N: Device>(pd: Pd<N>) -> PdId;

/// [`Pd::id`], as a specification.
pub uninterp spec fn pd_id<N: Device>(pd: Pd<N>) -> u64;

/// The range of a buffer part, as a specification.
pub open spec fn part_sge<N: Device>(part: Part<N>) -> Sge { part.spec_sge() }

/// The protection domain a buffer part is registered in ([`Pd::id`]), as a
/// specification.
pub open spec fn part_pd<N: Device>(part: Part<N>) -> u64 { part.spec_pd() }

/// Whether a buffer part is unique (the NIC may write it), as a
/// specification.
pub open spec fn part_writable<N: Device>(part: Part<N>) -> bool { part.spec_writable() }

/// Access to the local bytes, as opposed to the operation's remote effects.
pub open spec fn send_access(op: WrOp) -> safeverbs_spec::abs::Dir {
    if opcode_model(op).writes_local() { safeverbs_spec::abs::Dir::In }
    else { safeverbs_spec::abs::Dir::Out }
}

// =============================================================================
// The token

/// What one worker knows of the NIC: M's state as it last saw it, its share
/// of M, the protection domain of its QPs, the names it gives new QPs, and
/// for each of its queue pairs the send WRs written since its last start.
/// Only the trusted wrappers make or change one: its fields are private, so
/// no other code can forge one.
// The fields are ghost: only the proofs read them.
#[allow(dead_code)]
pub tracked struct NicToken {
    ghost nic: Nic::State,
    ghost share: Share,
    ghost pd: PdId,
    ghost name: spec_fn(nat) -> QpId,
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

    /// The protection domain of the worker's QPs.
    pub closed spec fn pd(&self) -> PdId {
        self.pd
    }

    /// The name of the worker's `n`-th QP: names it owns, all different,
    /// none used before the worker was made.
    pub closed spec fn name(&self, n: nat) -> QpId {
        (self.name)(n)
    }

    /// `self` is `other` after calls of the same worker: the same share,
    /// protection domain and names.
    pub open spec fn same_worker(&self, other: &NicToken) -> bool {
        &&& self.share() == other.share()
        &&& self.pd() == other.pd()
        &&& forall|n: nat| #[trigger] self.name(n) == other.name(n)
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
        &&& forall|p: QpId| p != q ==> #[trigger] self.started(p) == other.started(p)
        &&& forall|p: QpId| p != q ==> #[trigger] self.written(p) == other.written(p)
    }

    /// `self` and `other` agree on the writing of every queue pair.
    pub open spec fn same_writings(&self, other: &NicToken) -> bool {
        &&& forall|p: QpId| #[trigger] self.started(p) == other.started(p)
        &&& forall|p: QpId| #[trigger] self.written(p) == other.written(p)
    }
}

/// The regions the worker's written WRs name: their leases keep them
/// registered, so no other worker deregisters one meanwhile.
pub open spec fn pinned(t: NicToken) -> ISet<Key> {
    ISet::new(|k: Key| exists|q: QpId| #[trigger] send_keys(t.written(q)).contains(k))
}

/// `polled(sh, pins, m, m2, wcs)`: polling the CQ of share `sh` took M from
/// `m` to `m2` and gave `wcs`: one `poll_cq` step for each, in order, each
/// reporting the CQE it popped, with steps that do not touch the share (nor
/// `pins`) around them.
pub open spec fn polled(sh: Share, pins: ISet<Key>, m: Nic::State, m2: Nic::State, wcs: Seq<Wc>) -> bool
    decreases wcs.len(),
{
    if wcs.len() == 0 {
        rely_reaches(sh, pins, m, m2)
    } else {
        exists|mid: Nic::State, before: Nic::State|
            {
                &&& polled(sh, pins, m, mid, wcs.drop_last())
                &&& rely_reaches(sh, pins, mid, before)
                &&& nic_step(before, m2, Nic::Step::poll_cq(sh.cq))
                &&& reports(wcs.last(), before.cqs[sh.cq].entries[0])
            }
    }
}

/// `modified(sh, pins, m, m2, q, tos)`: queue pair `q` was moved to each
/// state of `tos` in turn (M's `modify_qp`), with steps that do not touch
/// the share around them.
pub open spec fn modified(
    sh: Share,
    pins: ISet<Key>,
    m: Nic::State,
    m2: Nic::State,
    q: QpId,
    tos: Seq<QpState>,
) -> bool
    decreases tos.len(),
{
    if tos.len() == 0 {
        rely_reaches(sh, pins, m, m2)
    } else {
        exists|mid: Nic::State, before: Nic::State|
            {
                &&& modified(sh, pins, m, mid, q, tos.drop_last())
                &&& rely_reaches(sh, pins, mid, before)
                &&& nic_step(before, m2, Nic::Step::modify_qp(q, tos.last()))
            }
    }
}

/// The states `connect` moves a queue pair through.
pub open spec fn connect_states() -> Seq<QpState> {
    seq![QpState::Init, QpState::Rtr, QpState::Rts]
}

/// A fresh worker: its names are its own, all different, and none is used
/// yet; it has no QP and writes nothing.
pub open spec fn fresh_worker(t: NicToken) -> bool {
    &&& forall|n: nat| #[trigger] t.share().owns.contains(t.name(n))
    &&& forall|a: nat, b: nat| #[trigger] t.name(a) == #[trigger] t.name(b) ==> a == b
    &&& forall|q: QpId| #[trigger] t.share().owns.contains(q) ==> !t.nic().qps.contains_key(q)
    &&& forall|n: nat| !(#[trigger] t.nic().qps.contains_key(t.name(n)))
    &&& forall|n: nat, w: WqeRef| #[trigger] t.nic().retired.contains_key(w) ==> w.qp != #[trigger] t.name(n)
    &&& forall|q: QpId| !(#[trigger] t.started(q)) && t.written(q) == Seq::<SendWr>::empty()
}

// =============================================================================
// Creating and destroying a worker's objects (the control path)

/// The device's limits (`ibv_query_device`, and the ports' message size),
/// as a specification.
pub uninterp spec fn spec_device_caps<N: Device>(pd: Pd<N>) -> DeviceCaps;

/// The limits of the device of `pd`.
#[verifier::external_body]
pub fn device_caps<N: Device>(ctx: &Context<N>) -> (caps: DeviceCaps)
    ensures
        caps == spec_device_caps(*ctx),
{
    ctx.nic().caps()
}

/// `ibv_create_cq_ex` (single-threaded) on the device of `ctx`: an engine's CQ
/// of `entries` entries, and its token, with the device's limits as M has
/// them. M's `create_cq` with a name never used, after whatever the device
/// went through before; the worker's share is that CQ and QP names no other
/// worker has, none used before. This is where tokens come from.
#[verifier::external_body]
pub fn create_cq<N: Device>(ctx: &Context<N>, entries: u32) -> (r: Result<
    (N::Cq, DeviceCaps, Tracked<NicToken>),
>)
    requires
        1 <= entries <= spec_device_caps(*ctx).max_cqe,
    ensures
        r matches Ok((cq, caps, t)) ==> {
            let m = t@.nic();
            let sh = t@.share();
            &&& nic_inv(m)
            &&& share_wf(m, sh)
            &&& cq_of(cq) == sh.cq
            &&& m.cqs[sh.cq].cap == entries
            &&& m.cqs[sh.cq].reserved.len() == 0
            &&& caps == spec_device_caps(*ctx)
            &&& caps_model(m.caps, caps)
            &&& fresh_worker(t@)
        },
{
    let nic = ctx.nic();
    let caps = nic.caps();
    let cq = nic.create_cq(entries, true)?;
    Ok((cq, caps, Tracked::assume_new()))
}

/// The QP number the device gave a hardware QP (no meaning in M).
#[verifier::external_body]
pub fn qp_num_of<Q: NicQp>(qp: &Q) -> u32 {
    qp.qp_num()
}

/// Whether a hardware QP was created without the driver's locks (no
/// meaning in M).
#[verifier::external_body]
pub fn qp_lock_free<Q: NicQp>(qp: &Q) -> bool {
    qp.is_lock_free()
}

/// Panics: a QP key that names no QP of the worker (a logic error of the
/// caller, not a memory-safety one). Never returns.
#[verifier::external_body]
pub fn no_qp(index: usize)
    ensures
        false,
{
    panic!("no QP {index} in this worker")
}

/// `ibv_create_qp_ex` on the worker's CQ and protection domain (on its
/// thread domain if any): M's `create_qp` of `q`, or nothing on error.
#[verifier::external_body]
pub fn create_qp<N: Device>(
    pd: &Pd<N>,
    domain: Option<&N::Domain>,
    cq: &N::Cq,
    qp_type: QpType,
    caps: &QpCaps,
    Ghost(q): Ghost<QpId>,
    Tracked(token): Tracked<&mut NicToken>,
) -> (r: Result<N::Qp>)
    requires
        cq_of(*cq) == old(token).share().cq,
        pd_of(*pd) == old(token).pd(),
        nic_inv(old(token).nic()),
        share_wf(old(token).nic(), old(token).share()),
        old(token).share().owns.contains(q),
        nic_enabled(
            old(token).nic(),
            Nic::Step::create_qp(q, qp_attr(old(token).pd(), qp_type, old(token).share().cq, *caps)),
        ),
    ensures
        final(token).same_worker(old(token)),
        final(token).same_writings(old(token)),
        r matches Ok(qp) ==> qp_of(qp) == q && exists|mid: Nic::State|
            rely_reaches(old(token).share(), pinned(*old(token)), old(token).nic(), mid)
                && nic_step(
                mid,
                final(token).nic(),
                Nic::Step::create_qp(
                    q,
                    qp_attr(old(token).pd(), qp_type, old(token).share().cq, *caps),
                ),
            ),
        r is Err ==> rely_reaches(
            old(token).share(),
            pinned(*old(token)),
            old(token).nic(),
            final(token).nic(),
        ),
{
    pd.context().nic().create_qp(pd.raw(), domain, cq, cq, qp_type, caps)
}

/// `ibv_modify_qp` to INIT, RTR and RTS: M's three `modify_qp` steps, or on
/// error the first ones.
#[verifier::external_body]
pub fn connect<Q: NicQp>(
    qp: &mut Q,
    attr: &ConnectAttr,
    remote: &Endpoint,
    Ghost(q): Ghost<QpId>,
    Tracked(token): Tracked<&mut NicToken>,
) -> (r: Result<()>)
    requires
        qp_of(*old(qp)) == q,
        nic_inv(old(token).nic()),
        share_wf(old(token).nic(), old(token).share()),
        old(token).share().owns.contains(q),
        old(token).nic().qps.contains_key(q),
        old(token).nic().qps[q].state is Reset,
    ensures
        qp_of(*final(qp)) == q,
        final(token).same_worker(old(token)),
        final(token).same_writings(old(token)),
        r is Ok ==> modified(
            old(token).share(),
            pinned(*old(token)),
            old(token).nic(),
            final(token).nic(),
            q,
            connect_states(),
        ),
        r is Err ==> exists|k: int|
            0 <= k < 3 && modified(
                old(token).share(),
                pinned(*old(token)),
                old(token).nic(),
                final(token).nic(),
                q,
                connect_states().take(k),
            ),
{
    qp.connect(attr, remote)
}

/// `ibv_modify_qp` to ERR: M's `modify_qp` of `q` to `Err`, or nothing on
/// error.
#[verifier::external_body]
pub fn to_error<Q: NicQp>(qp: &mut Q, Ghost(q): Ghost<QpId>, Tracked(token): Tracked<&mut NicToken>) -> (r:
    Result<()>)
    requires
        qp_of(*old(qp)) == q,
        nic_inv(old(token).nic()),
        share_wf(old(token).nic(), old(token).share()),
        old(token).share().owns.contains(q),
        nic_enabled(old(token).nic(), Nic::Step::modify_qp(q, QpState::Err)),
    ensures
        qp_of(*final(qp)) == q,
        final(token).same_worker(old(token)),
        final(token).same_writings(old(token)),
        r is Ok ==> exists|mid: Nic::State|
            rely_reaches(old(token).share(), pinned(*old(token)), old(token).nic(), mid)
                && nic_step(mid, final(token).nic(), Nic::Step::modify_qp(q, QpState::Err)),
        r is Err ==> rely_reaches(
            old(token).share(),
            pinned(*old(token)),
            old(token).nic(),
            final(token).nic(),
        ),
{
    qp.to_error()
}

/// `ibv_destroy_qp`: M's `destroy_qp`, which needs its queues empty.
#[verifier::external_body]
pub fn destroy_qp<Q: NicQp>(qp: Q, Ghost(q): Ghost<QpId>, Tracked(token): Tracked<&mut NicToken>)
    requires
        qp_of(qp) == q,
        nic_inv(old(token).nic()),
        share_wf(old(token).nic(), old(token).share()),
        old(token).share().owns.contains(q),
        nic_enabled(old(token).nic(), Nic::Step::destroy_qp(q)),
    ensures
        final(token).same_worker(old(token)),
        final(token).same_writings(old(token)),
        exists|mid: Nic::State|
            rely_reaches(old(token).share(), pinned(*old(token)), old(token).nic(), mid) && nic_step(
                mid,
                final(token).nic(),
                Nic::Step::destroy_qp(q),
            ),
{
    drop(qp)
}

/// `ibv_destroy_cq` of the worker's CQ: M's `destroy_cq`, which needs no QP
/// attached. The token goes with it.
#[verifier::external_body]
pub fn destroy_cq<C: NicCq>(cq: C, Tracked(token): Tracked<NicToken>)
    requires
        cq_of(cq) == token.share().cq,
        nic_inv(token.nic()),
        share_wf(token.nic(), token.share()),
        nic_enabled(token.nic(), Nic::Step::destroy_cq(token.share().cq)),
{
    drop(cq)
}

// =============================================================================
// Buffers (`buf.rs`, not verified yet)

/// [`Pd::id`].
#[verifier::external_body]
pub fn pd_id_of<N: Device>(pd: &Pd<N>) -> (id: u64)
    ensures
        id == pd_id(*pd),
{
    pd.id()
}

/// A part's range of registered memory.
#[inline(always)]
pub fn part_sge_of<N: Device>(part: &Part<N>) -> (sge: Sge)
    ensures
        sge == part_sge(*part),
{
    proof { part.lemma_fields(); }
    part.sge
}

/// A part's protection domain ([`Pd::id`]).
#[inline(always)]
pub fn part_pd_of<N: Device>(part: &Part<N>) -> (pd: u64)
    ensures
        pd == part_pd(*part),
{
    proof { part.lemma_fields(); }
    part.pd
}

/// Whether a part is unique, so that the NIC may write it.
#[inline(always)]
pub fn part_writable_of<N: Device>(part: &Part<N>) -> (writable: bool)
    ensures
        writable == part_writable(*part),
{
    proof { part.lemma_fields(); }
    part.is_writable()
}

/// What a buffer's lease promises (`buf.rs`): its bytes lie in a region
/// registered in its protection domain, locally writable if the buffer is
/// unique, and the region stays registered while the lease lives. Learning
/// it moves the token's view to the present, since the region may be newer
/// than what the worker last saw.
#[verifier::external_body]
#[inline(always)]
pub fn registered<N: Device>(_part: &Part<N>, _pd: &Pd<N>, Tracked(token): Tracked<&mut NicToken>)
    requires
        part_pd(*_part) == pd_id(*_pd),
        pd_of(*_pd) == old(token).pd(),
    ensures
        final(token).same_worker(old(token)),
        final(token).same_writings(old(token)),
        rely_reaches(old(token).share(), pinned(*old(token)), old(token).nic(), final(token).nic()),
        final(token).nic().registered(
            final(token).pd(),
            sge_model(part_sge(*_part)),
            part_writable(*_part),
        ),
{
}

/// [`registered`] for several parts at once: every one's region is
/// registered in the present view.
#[verifier::external_body]
#[inline(always)]
pub fn registered_all<N: Device>(_leases: &Leases<N>, _pd: &Pd<N>, Tracked(token): Tracked<&mut NicToken>)
    requires
        forall|j: int| 0 <= j < _parts@.len() ==> part_pd(#[trigger] _parts@[j]) == pd_id(*_pd),
        pd_of(*_pd) == old(token).pd(),
    ensures
        final(token).same_worker(old(token)),
        final(token).same_writings(old(token)),
        rely_reaches(old(token).share(), pinned(*old(token)), old(token).nic(), final(token).nic()),
        forall|j: int|
            0 <= j < _parts@.len() ==> final(token).nic().registered(
                final(token).pd(),
                sge_model(part_sge(#[trigger] _parts@[j])),
                part_writable(_parts@[j]),
            ),
{
}

/// No leases (`Leases::default`).
#[inline(always)]
pub fn no_leases<N: Device>() -> (r: Leases<N>)
    ensures r.inv(), r.view().len() == 0, r.loans().len() == 0, r.wqes().len() == 0,
        r.fully_recorded(),
{
    Leases::new()
}

/// Keeps a part's lease among `leases` (`Leases::push`).
#[inline(always)]
pub fn keep_lease<N: Device>(leases: &mut Leases<N>, part: Part<N>, Ghost(access): Ghost<safeverbs_spec::abs::Dir>)
    requires access is In ==> part_writable(part),
    ensures
        old(leases).inv() ==> final(leases).inv(),
        final(leases).loans() == old(leases).loans().push(crate::buf::Loan { part, access: Some(access) }),
        final(leases).wqes() == old(leases).wqes(),
{
    proof { part.lemma_fields(); }
    leases.push_access(part, Ghost(access))
}

// =============================================================================
// The verbs of the data path

/// `ibv_wr_start`: queue pair `q` starts writing send WRs.
#[verifier::external_body]
#[inline(always)]
pub fn start<Q: NicQp>(qp: &mut Q, Ghost(q): Ghost<QpId>, Tracked(token): Tracked<&mut NicToken>)
    requires
        qp_of(*old(qp)) == q,
        !old(token).started(q),
    ensures
        qp_of(*final(qp)) == q,
        final(token).nic() == old(token).nic(),
        final(token).same_worker(old(token)),
        final(token).started(q),
        final(token).written(q) == Seq::<SendWr>::empty(),
        final(token).same_writing_but(old(token), q),
{
    // SAFETY: not started (the precondition).
    unsafe { qp.start() }
}

/// `ibv_wr_*`: writes one send WR; it is posted at the doorbell.
#[verifier::external_body]
#[inline(always)]
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
        qp_of(*old(qp)) == q,
        old(token).started(q),
    ensures
        qp_of(*final(qp)) == q,
        final(token).nic() == old(token).nic(),
        final(token).same_worker(old(token)),
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
#[inline(always)]
pub fn doorbell<Q: NicQp>(
    qp: &mut Q,
    Ghost(q): Ghost<QpId>,
    Tracked(token): Tracked<&mut NicToken>,
) -> (r: Result<()>)
    requires
        qp_of(*old(qp)) == q,
        old(token).started(q),
        nic_inv(old(token).nic()),
        share_wf(old(token).nic(), old(token).share()),
        old(token).share().owns.contains(q),
        nic_enabled(old(token).nic(), Nic::Step::post_send(q, old(token).written(q))),
    ensures
        qp_of(*final(qp)) == q,
        final(token).same_worker(old(token)),
        !final(token).started(q),
        final(token).written(q) == Seq::<SendWr>::empty(),
        final(token).same_writing_but(old(token), q),
        r is Ok ==> exists|mid: Nic::State|
            rely_reaches(old(token).share(), pinned(*old(token)), old(token).nic(), mid) && nic_step(
                mid,
                final(token).nic(),
                Nic::Step::post_send(q, old(token).written(q)),
            ),
        r is Err ==> rely_reaches(
            old(token).share(),
            pinned(*old(token)),
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
        qp_of(*old(qp)) == q,
        old(token).started(q),
    ensures
        qp_of(*final(qp)) == q,
        final(token).same_worker(old(token)),
        !final(token).started(q),
        final(token).written(q) == Seq::<SendWr>::empty(),
        final(token).same_writing_but(old(token), q),
        rely_reaches(old(token).share(), pinned(*old(token)), old(token).nic(), final(token).nic()),
{
    // SAFETY: started (the precondition).
    unsafe { qp.abort() }
}

/// `ibv_post_recv` of one WR (M's `post_recv` of a list of one), or, on
/// error, nothing.
#[verifier::external_body]
#[inline(always)]
pub fn post_recv<Q: NicQp>(
    qp: &mut Q,
    wr_id: u64,
    sges: &[Sge],
    Ghost(q): Ghost<QpId>,
    Tracked(token): Tracked<&mut NicToken>,
) -> (r: Result<()>)
    requires
        qp_of(*old(qp)) == q,
        nic_inv(old(token).nic()),
        share_wf(old(token).nic(), old(token).share()),
        old(token).share().owns.contains(q),
        nic_enabled(old(token).nic(), Nic::Step::post_recv(q, seq![recv_wr(wr_id, sges@)])),
    ensures
        qp_of(*final(qp)) == q,
        final(token).same_worker(old(token)),
        final(token).same_writings(old(token)),
        r is Ok ==> exists|mid: Nic::State|
            rely_reaches(
                old(token).share(),
                pinned(*old(token)).union(recv_keys(seq![recv_wr(wr_id, sges@)])),
                old(token).nic(),
                mid,
            ) && nic_step(
                mid,
                final(token).nic(),
                Nic::Step::post_recv(q, seq![recv_wr(wr_id, sges@)]),
            ),
        r is Err ==> rely_reaches(
            old(token).share(),
            pinned(*old(token)),
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
#[inline(always)]
pub fn poll<C: NicCq>(cq: &mut C, out: &mut [Wc], limit: usize, Tracked(token): Tracked<&mut NicToken>) -> (n:
    usize)
    requires
        cq_of(*old(cq)) == old(token).share().cq,
        limit <= old(out)@.len(),
    ensures
        cq_of(*final(cq)) == cq_of(*old(cq)),
        n <= limit,
        final(out)@.len() == old(out)@.len(),
        final(token).same_worker(old(token)),
        final(token).same_writings(old(token)),
        polled(
            old(token).share(),
            pinned(*old(token)),
            old(token).nic(),
            final(token).nic(),
            final(out)@.take(n as int),
        ),
{
    cq.poll(&mut out[..limit])
}

} // verus!
