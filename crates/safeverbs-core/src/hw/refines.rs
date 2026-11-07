//! An engine refines A: Theorem 1 for its verified state.
//!
//! What A sees of an engine is one `refine::View` of all its rings (`table_view`), a request by its key
//! (`key`: its entry of the table, its queue, its sequence number), so that what happens to one
//! request leaves the others as they are, in every ring. Its ghost state (`Refinement`) is the
//! state of A it stands for, the name in A of each of its requests, each request's WQEs and the
//! WR of each send WQE.
//!
//! - **A request is complete in A when M popped its last WQE**, which is when `complete` happens:
//!   "the environment's move; the library learns it by polling the CQ". The ring hears of it when
//!   the CQE is routed, later, and the program may do anything in between, so what A sees of a
//!   request depends on M (`seen`): the WQEs M popped, not the ring's position. Routing a CQE is no
//!   step of A. So a request A has in flight has its last WQE posted, as `coupled` says.
//! - **A buffer comes to A when it is lent** (`lemma_name_request`). A lease needs no name until it
//!   meets the engine, and until then the buffer's linear byte permission is all there is to it:
//!   what the program does with its buffers goes through no engine. Its permission says its bytes
//!   are apart from everything the engine holds (`Hw::separate_lease`), so A's own steps make it a
//!   lease of the program's (`lemma_adopt_loans`): `alloc` of its bytes, `freeze` for a frozen one,
//!   after `drop` of what A still has over them that no request keeps. No buffer can stand for that.
//! - **The leases of a request** are named when it is written, and kept with it (`Payload::holds`):
//!   a unique buffer's lease goes into the request; a shared one gets a new lease, `share`d from
//!   the lease of its frozen allocation, its root, which comes to nothing with the request. A share
//!   keeps its root (`rooted`), so that another request can share the same bytes.
//! - **Any buffer the program holds is a lease of A's** (`Hw::adopt`): the same steps reach a state
//!   of A that the engine stands for too, so S1 is A's of the buffers themselves
//!   (`Hw::keeper_apart`).
//!
//! The invariant (`linked`) is part of `Hw::inv`. Each operation of `Hw` moves the state of A by
//! the steps of the lemmas here, and says so (`Hw::moved_from`). What `Refines` asks of it is at the
//! end (`EngineState`), with the theorem (`theorem_1_engine`) and a run of the operations under
//! it (`example_run`). What the engine cannot see stays a premise: what the NIC may touch beyond its
//! queues (`Hw::outside_ok`).
//!
//! Everything here is ghost code: a plain build erases it.
#![allow(unused_imports)]
use super::*;
use crate::buf::Resource;
use crate::refine::{Hold, Holds, View as Seen};
#[cfg(verus_keep_ghost)]
use crate::refine::{
    completed_where, detached, free_lease, has_root, holds_apart, kept_free, kept_requests,
    leases_of, lemma_abort, lemma_adopt, lemma_adopted_frozen, lemma_complete_where, lemma_detach,
    lemma_end, lemma_kept_free_trans, lemma_placed_apart, lemma_post_one, lemma_register,
    lemma_release, lemma_same, lemma_share, lemma_take, lemma_take_refused, may_post_holds,
    names_request, placed, refused_range, registered, root_outside, roots_outside, same,
    same_but_program, share_state, sits, staged_with, wf_at, without, without_range,
};
use crate::ring::{ReqView, RequestMeta};
use safeverbs_spec::abs::{Abs, Dir, Holder, Lease, LeaseId, Req, ReqId, WakerId};
#[cfg(verus_keep_ghost)]
use safeverbs_spec::abs::{abs_inv, abs_step};
#[cfg(verus_keep_ghost)]
use safeverbs_spec::nic_proofs::{
    history_ok, known, lemma_history_init, lemma_history_reaches, lemma_pending_of_last,
    lemma_polled_wqes, lemma_request_reaches, lemma_request_step, wr_log,
};
use safeverbs_spec::types::Outcome;

verus! {

// =============================================================================
// One view of all the rings

/// The numbers of a queue's requests stay under this.
pub(super) open spec fn key_span() -> int {
    0x1_0000_0000_0000_0000
}

/// The key of request `s` of queue `queue` of entry `i` of the table.
pub(super) open spec fn key(i: int, queue: Queue, s: int) -> int {
    (2 * i + if queue is Recv { 1int } else { 0int }) * 0x1_0000_0000_0000_0000 + s
}

/// The entry of the table of key `k`.
pub(super) open spec fn key_entry(k: int) -> int {
    (k / 0x1_0000_0000_0000_0000) / 2
}

/// The queue of key `k`.
pub(super) open spec fn key_queue(k: int) -> Queue {
    if (k / 0x1_0000_0000_0000_0000) % 2 == 0 {
        Queue::Send
    } else {
        Queue::Recv
    }
}

/// The request's number of key `k`.
pub(super) open spec fn key_seq(k: int) -> int {
    k % 0x1_0000_0000_0000_0000
}

/// A key says what it was made of.
pub(super) proof fn lemma_key_fields(i: int, queue: Queue, s: int)
    requires
        0 <= s < key_span(),
    ensures
        key_entry(key(i, queue, s)) == i,
        key_queue(key(i, queue, s)) == queue,
        key_seq(key(i, queue, s)) == s,
{
    let b = 2 * i + if queue is Recv { 1int } else { 0int };
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod_converse(key(i, queue, s), 0x1_0000_0000_0000_0000, b, s);
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod_converse(b, 2, i, if queue is Recv { 1int } else { 0int });
}

/// Every key is made of what it says.
pub(super) proof fn lemma_key_made(k: int)
    ensures
        key(key_entry(k), key_queue(k), key_seq(k)) == k,
        0 <= key_seq(k) < key_span(),
{
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod(k, 0x1_0000_0000_0000_0000);
    vstd::arithmetic::div_mod::lemma_mod_bound(k, 0x1_0000_0000_0000_0000);
    let b = k / 0x1_0000_0000_0000_0000;
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod(b, 2);
    vstd::arithmetic::div_mod::lemma_mod_bound(b, 2);
}

/// What a request holds, as A names it: its queue pair, and its leases.
#[allow(dead_code)] // Built by ghost code alone.
pub ghost struct Lent {
    pub qp: QpId,
    pub holds: Holds,
}

/// A request's leases, from what A sees of it.
pub(super) open spec fn lent_holds() -> spec_fn(int, Lent) -> Holds {
    |_s: int, l: Lent| l.holds
}

/// A request's queue pair, from what A sees of it.
pub(super) open spec fn lent_qp() -> spec_fn(Lent) -> QpId {
    |l: Lent| l.qp
}

/// The WQEs of a request with bookkeeping `meta` on queue `queue` of `q`, as M names them.
pub(super) open spec fn range_of(q: QpId, queue: Queue, meta: RequestMeta) -> WqeRange {
    WqeRange { qp: q, queue, first: meta.first as nat, end: meta.end as nat }
}

/// M popped every WQE of `range`: the NIC is done with its request.
pub(super) open spec fn popped(m: Nic::State, range: WqeRange) -> bool {
    range.end <= head_of(m.qps[range.qp], range.queue)
}

/// What A sees of request `s` of ring `r`, on queue `queue` of QP `q`, M being at `m`. Once M popped
/// all its WQEs it is complete, with M's aggregate of them, or gone if it has no owner, whatever
/// the ring has heard; until then it is in flight.
pub(super) open spec fn seen<C, N: Device>(r: Ring<Payload<C, N>>, m: Nic::State, q: QpId, queue: Queue, s: int) -> Option<ReqView<Lent>> {
    match r.req(s) {
        None => None,
        Some(rv) => {
            let range = range_of(q, queue, r.meta(s)->0);
            if popped(m, range) {
                if rv.detached {
                    None
                } else {
                    Some(ReqView {
                        phase: safeverbs_spec::abs::Phase::Complete(range.aggregate(m.retired)),
                        waker: None,
                        detached: false,
                        leases: Lent { qp: q, holds: rv.leases.holds@ },
                    })
                }
            } else {
                Some(ReqView {
                    phase: safeverbs_spec::abs::Phase::InFlight,
                    waker: rv.waker,
                    detached: rv.detached,
                    leases: Lent { qp: q, holds: rv.leases.holds@ },
                })
            }
        },
    }
}

/// What A sees of staged request `s` of ring `r`.
pub(super) open spec fn staged_seen<C, N: Device>(r: Ring<Payload<C, N>>, q: QpId, s: int) -> Option<ReqView<Lent>> {
    match r.staged(s) {
        None => None,
        Some(rv) => Some(ReqView { phase: rv.phase, waker: rv.waker, detached: rv.detached, leases: Lent { qp: q, holds: rv.leases.holds@ } }),
    }
}

/// What A sees of refused request `s` of ring `r`.
pub(super) open spec fn refused_seen<C, N: Device>(r: Ring<Payload<C, N>>, q: QpId, s: int) -> Option<Lent> {
    match r.refused(s) {
        None => None,
        Some(p) => Some(Lent { qp: q, holds: p.holds@ }),
    }
}

/// Entry `i` of the table has a QP.
pub(super) open spec fn has_entry<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, i: int) -> bool {
    0 <= i < qps.len() && qps[i] is Some
}

/// What A sees of request `k` of the table.
pub(super) open spec fn table_req<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, k: int) -> Option<ReqView<Lent>> {
    if has_entry(qps, key_entry(k)) {
        let e = qps[key_entry(k)]->0;
        seen(ring_of(e, key_queue(k)), m, e.id(), key_queue(k), key_seq(k))
    } else {
        None
    }
}

/// What A sees of staged request `k` of the table.
pub(super) open spec fn table_staged<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, k: int) -> Option<ReqView<Lent>> {
    if has_entry(qps, key_entry(k)) {
        let e = qps[key_entry(k)]->0;
        staged_seen(ring_of(e, key_queue(k)), e.id(), key_seq(k))
    } else {
        None
    }
}

/// What A sees of refused request `k` of the table.
pub(super) open spec fn table_refused<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, k: int) -> Option<Lent> {
    if has_entry(qps, key_entry(k)) {
        let e = qps[key_entry(k)]->0;
        refused_seen(ring_of(e, key_queue(k)), e.id(), key_seq(k))
    } else {
        None
    }
}

/// What A sees of the table, M being at `m`.
pub(super) open spec fn table_view<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State) -> Seen<Lent> {
    Seen {
        req: |k: int| table_req(qps, m, k),
        staged: |k: int| table_staged(qps, k),
        refused: |k: int| table_refused(qps, k),
    }
}

/// The WQEs of request `k` of the table.
pub(super) open spec fn key_range<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, k: int) -> WqeRange {
    let e = qps[key_entry(k)]->0;
    range_of(e.id(), key_queue(k), ring_of(e, key_queue(k)).meta(key_seq(k))->0)
}

// =============================================================================
// The ghost state and the invariant

/// What an engine stands for, beyond its rings and M.
#[allow(dead_code)] // Read by ghost code alone.
pub ghost struct Refinement {
    /// The state of A.
    pub abs: Abs::State,
    /// The name in A of each request A has, by key.
    pub reqs: Map<int, ReqId>,
    /// The WQEs of every request A ever had.
    pub ranges: Map<ReqId, WqeRange>,
    /// The WR of every send WQE M knows.
    pub wrs: IMap<WqeRef, SendWr>,
}

impl Refinement {
    /// `self` comes after `older`: by steps of A, a request keeping its WQEs and a WQE its WR.
    pub open spec fn extends(self, older: Self) -> bool {
        &&& abs_reaches(older.abs, self.abs)
        &&& older.ranges.submap_of(self.ranges)
        &&& older.wrs.submap_of(self.wrs)
    }

    /// `extends` is transitive.
    pub proof fn lemma_extends_trans(self, mid: Self, older: Self)
        requires
            self.extends(mid),
            mid.extends(older),
        ensures
            self.extends(older),
    {
        lemma_abs_reaches_trans(older.abs, mid.abs, self.abs);
        assert forall|r: ReqId| #[trigger] older.ranges.dom().contains(r)
            implies self.ranges.dom().contains(r) && older.ranges[r] == self.ranges[r] by {
            assert(mid.ranges.dom().contains(r));
            assert(self.ranges.dom().contains(r));
            assert(older.ranges[r] == mid.ranges[r]);
        }
        assert forall|w: WqeRef| #[trigger] older.wrs.dom().contains(w)
            implies self.wrs.dom().contains(w) && older.wrs[w] == self.wrs[w] by {
            assert(mid.wrs.dom().contains(w));
            assert(self.wrs.dom().contains(w));
            assert(older.wrs[w] == mid.wrs[w]);
        }
    }
}

/// A's queue pairs are those of the table.
pub(super) open spec fn qps_match<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, a: Abs::State) -> bool {
    &&& forall|i: int| has_entry(qps, i) ==> a.qps.contains(#[trigger] qps[i]->0.id())
    &&& forall|q: QpId| #[trigger] a.qps.contains(q) ==> exists|i: int| has_entry(qps, i) && #[trigger] qps[i]->0.id() == q
}

/// M knows every WQE of `range`.
pub(super) open spec fn range_known(m: Nic::State, range: WqeRange) -> bool {
    forall|s: nat| range.first <= s < range.end ==> known(m, #[trigger] range.at(s))
}

/// What A's requests and outcomes say of M, through `ranges`: `coupled`, as far as it is not read
/// off the rings, and what it takes to keep it.
pub(super) open spec fn ranges_ok(g: Refinement, m: Nic::State) -> bool {
    let a = g.abs;
    &&& forall|r: ReqId| #[trigger] a.requests.contains_key(r)
        ==> g.ranges.contains_key(r) && g.ranges[r].qp == a.requests[r].qp
    &&& forall|r: ReqId| #[trigger] a.outcomes.contains_key(r) ==> g.ranges.contains_key(r)
    &&& forall|r: ReqId| #[trigger] g.ranges.contains_key(r) ==> {
        &&& r < a.next_id
        &&& g.ranges[r].wf()
        &&& g.ranges[r].queue is Recv ==> g.ranges[r].end == g.ranges[r].first + 1
        &&& range_known(m, g.ranges[r])
    }
    &&& forall|r1: ReqId, r2: ReqId| #![trigger g.ranges[r1], g.ranges[r2]]
        g.ranges.contains_key(r1) && g.ranges.contains_key(r2) && r1 != r2
            ==> g.ranges[r1].disjoint(g.ranges[r2])
    &&& forall|r: ReqId| #[trigger] a.in_flight(r) ==> {
        &&& safeverbs_spec::refinement::posted(m, g.ranges[r].last())
        &&& g.ranges[r].queue is Send ==> g.ranges[r].errors_stick(m)
    }
    &&& forall|r: ReqId| #[trigger] a.outcomes.contains_key(r) ==> {
        &&& g.ranges[r].all_retired(m.retired)
        &&& g.ranges[r].aggregate(m.retired) == a.outcomes[r]
        &&& g.ranges[r].no_success_after_error(m.retired)
    }
}

/// Each request A has is the one of its key, with the WQEs its ring says.
pub(super) open spec fn live_ranges<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement) -> bool {
    forall|k: int| #[trigger] table_req(qps, m, k) is Some ==> {
        &&& g.ranges.contains_key(g.reqs[k])
        &&& g.ranges[g.reqs[k]] == key_range(qps, k)
    }
}

/// The table stands for `g`, M being at `m`: A sees its requests where they are.
pub(super) open spec fn linked<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement) -> bool {
    &&& placed(table_view(qps, m), g.abs, lent_qp(), g.reqs, lent_holds())
    &&& nic_inv(m)
    &&& history_ok(m)
    &&& wr_log(m, g.wrs)
    &&& qps_match(qps, g.abs)
    &&& ranges_ok(g, m)
    &&& live_ranges(qps, m, g)
}

/// Lease `l` stands for `part`, lent to the NIC for `dir`: a unique part has its lease with exactly
/// its bytes, a shared one shares a shared lease over them; the NIC writes only what accepts any
/// bytes.
pub(super) open spec fn stands_for<N: Device>(lease: Lease, part: Part<N>, dir: Dir) -> bool {
    let bytes = sge_model(part.spec_sge()).span().bytes();
    &&& if part.spec_writable() {
        lease.unique && lease.mem == bytes
    } else {
        !lease.unique && bytes.subset_of(lease.mem)
    }
    &&& dir is In ==> lease.any_bytes
}

/// The program holds a lease that stands for `part`, lent for `dir`, and no request keeps it.
pub(super) open spec fn lent_ok<N: Device>(v: Seen<Lent>, a: Abs::State, part: Part<N>, dir: Dir) -> bool {
    exists|l: LeaseId| #[trigger] free_lease(v, a, lent_holds(), l) && stands_for(a.leases[l], part, dir)
}

/// The program holds free the lease of `keeper`, what keeps a buffer alive: a unique one over its
/// bytes, or the root of its frozen allocation.
pub(super) open spec fn adopted<N: Device>(v: Seen<Lent>, a: Abs::State, keeper: Resource<N>) -> bool {
    let bytes = keeper.byte_domain().to_iset();
    if keeper.unique() {
        exists|l: LeaseId| #[trigger] free_lease(v, a, lent_holds(), l) && a.leases[l].unique
            && a.leases[l].any_bytes && a.leases[l].mem == bytes
    } else {
        has_root(v, a, lent_holds(), bytes)
    }
}

/// A lease of a request's against a buffer coming in: what keeps it alive is apart from the buffer,
/// or it shares the buffer's frozen allocation.
pub(crate) open spec fn hold_apart<N: Device>(hold: Hold, incoming: Resource<N>) -> bool {
    let bytes = incoming.byte_domain().to_iset();
    &&& hold.mem.subset_of(hold.kept)
    &&& hold.kept.disjoint(bytes) || (hold.shared && !incoming.unique() && hold.kept == bytes)
}

/// What keeps every request of the table alive is apart from `incoming`.
pub(super) open spec fn table_apart<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, incoming: Resource<N>) -> bool {
    forall|i: int| #[trigger] has_entry(qps, i) ==> {
        &&& qps[i]->0.send.retained_apart(incoming)
        &&& qps[i]->0.recv.retained_apart(incoming)
    }
}

/// What is apart from a keeper is apart from what it keeps.
pub(super) proof fn lemma_table_apart_backing<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, keeper: Resource<N>, kept: Resource<N>)
    requires
        table_apart(qps, keeper),
        keeper.same_backing(kept),
    ensures
        table_apart(qps, kept),
{
    assert forall|i: int| #[trigger] has_entry(qps, i) implies {
        &&& qps[i]->0.send.retained_apart(kept)
        &&& qps[i]->0.recv.retained_apart(kept)
    } by {
        qps[i]->0.send.lemma_retained_backing(keeper, kept);
        qps[i]->0.recv.lemma_retained_backing(keeper, kept);
    }
}

/// Each lease a request holds is of one of its keepers: its bytes lie in the keeper's, and it is a
/// share exactly if that keeper is frozen.
pub(crate) open spec fn holds_kept<N: Device>(h: Holds, leases: Leases<N>) -> bool {
    forall|l: LeaseId| #[trigger] h.contains_key(l) ==> {
        &&& h[l].mem.subset_of(h[l].kept)
        &&& exists|j: int| 0 <= j < leases.view().len()
            && (#[trigger] leases.view()[j]).byte_domain().to_iset() == h[l].kept
            && leases.view()[j].unique() == !h[l].shared
    }
}

/// The requests of the table keep what their leases say.
pub(super) open spec fn table_kept<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>) -> bool {
    forall|i: int| #[trigger] has_entry(qps, i) ==> {
        &&& ring_resources_ok(qps[i]->0.send)
        &&& ring_resources_ok(qps[i]->0.recv)
    }
}

/// A set's disjointness, between infinite sets.
proof fn lemma_disjoint_iset(a: vstd::set::Set<int>, b: vstd::set::Set<int>)
    requires
        a.disjoint(b),
    ensures
        a.to_iset().disjoint(b.to_iset()),
{
    assert forall|x: int| !(a.to_iset().contains(x) && b.to_iset().contains(x)) by {
        assert(!(a.contains(x) && b.contains(x)));
    }
}

/// A lease of a request whose keepers are apart from `incoming`.
proof fn lemma_hold_kept_apart<N: Device>(h: Holds, leases: Leases<N>, incoming: Resource<N>, l: LeaseId)
    requires
        holds_kept(h, leases),
        leases.apart_from(incoming),
        h.contains_key(l),
    ensures
        hold_apart(h[l], incoming),
{
    let j = choose|j: int| 0 <= j < leases.view().len()
        && (#[trigger] leases.view()[j]).byte_domain().to_iset() == h[l].kept
        && leases.view()[j].unique() == !h[l].shared;
    let keeper = leases.view()[j];
    assert(keeper.apart(incoming));
    if keeper.same_frozen(incoming) {
        keeper.lemma_same_frozen(incoming);
    } else {
        lemma_disjoint_iset(keeper.byte_domain(), incoming.byte_domain());
    }
}

/// A lease a request of the table holds or keeps, against a buffer apart from what the table keeps.
proof fn lemma_table_hold_apart<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, incoming: Resource<N>, k: int, l: LeaseId)
    requires
        table_kept(qps),
        table_apart(qps, incoming),
        leases_of(table_view(qps, m), lent_holds(), k).contains_key(l),
    ensures
        hold_apart(leases_of(table_view(qps, m), lent_holds(), k)[l], incoming),
{
    let v = table_view(qps, m);
    let i = key_entry(k);
    let queue = key_queue(k);
    let s = key_seq(k);
    assert(has_entry(qps, i));
    let e = qps[i]->0;
    let r = ring_of(e, queue);
    assert(ring_resources_ok(r) && r.retained_apart(incoming)) by {
        if queue is Send {
            assert(r == e.send);
        } else {
            assert(r == e.recv);
        }
    }
    if (v.req)(k) is Some {
        assert(table_req(qps, m, k) == seen(r, m, e.id(), queue, s));
        let rv = r.req(s)->0;
        assert(r.req(s) is Some);
        assert(payload_ok(rv.leases, rv.detached));
        assert(rv.leases.leases.apart_from(incoming));
        assert(leases_of(v, lent_holds(), k) == rv.leases.holds@);
        lemma_hold_kept_apart(rv.leases.holds@, rv.leases.leases, incoming, l);
    } else if (v.staged)(k) is Some {
        let rv = r.staged(s)->0;
        assert(r.staged(s) is Some);
        assert(payload_ok(rv.leases, rv.detached));
        assert(rv.leases.leases.apart_from(incoming));
        assert(leases_of(v, lent_holds(), k) == rv.leases.holds@);
        lemma_hold_kept_apart(rv.leases.holds@, rv.leases.leases, incoming, l);
    } else {
        let p = r.refused(s)->0;
        assert(r.refused(s) is Some);
        assert(payload_ok(p, false));
        assert(p.leases.apart_from(incoming));
        assert(leases_of(v, lent_holds(), k) == p.holds@);
        lemma_hold_kept_apart(p.holds@, p.leases, incoming, l);
    }
}

/// **Buffers come to A.** The program holds free a lease of each of these inputs, by A's own steps,
/// from what their linear permissions say of them: apart from every request, and from each other.
proof fn lemma_adopt_loans<N: Device>(v: Seen<Lent>, a: Abs::State, reqs: Map<int, ReqId>, loans: Seq<crate::buf::Loan<N>>, n: int) -> (a2: Abs::State)
    requires
        placed(v, a, lent_qp(), reqs, lent_holds()),
        0 <= n <= loans.len(),
        forall|i: int, k: int, l: LeaseId| 0 <= i < n && #[trigger] leases_of(v, lent_holds(), k).contains_key(l)
            ==> hold_apart(leases_of(v, lent_holds(), k)[l], (#[trigger] loans[i]).part.resource()),
        forall|i: int, j: int| #![trigger loans[i], loans[j]] 0 <= i < n && 0 <= j < n && i != j
            ==> loans[i].part.resource().apart(loans[j].part.resource()),
    ensures
        abs_reaches(a, a2),
        placed(v, a2, lent_qp(), reqs, lent_holds()),
        a2.requests == a.requests,
        a2.outcomes == a.outcomes,
        a2.qps == a.qps,
        a.next_id <= a2.next_id,
        forall|i: int| 0 <= i < n && (#[trigger] loans[i]).part.spec_sge().length > 0
            ==> adopted(v, a2, loans[i].part.resource()),
    decreases n,
{
    if n == 0 {
        lemma_abs_reaches_refl(a);
        a
    } else {
        let a1 = lemma_adopt_loans(v, a, reqs, loans, n - 1);
        let last = loans[n - 1].part;
        if last.spec_sge().length == 0 {
            a1
        } else {
            let a2 = lemma_adopt_keeper(v, a1, reqs, last.resource());
            lemma_abs_reaches_trans(a, a1, a2);
            assert forall|i: int| 0 <= i < n && (#[trigger] loans[i]).part.spec_sge().length > 0
                implies adopted(v, a2, loans[i].part.resource()) by {
                if i < n - 1 {
                    assert(loans[i].part.resource().apart(last.resource()));
                    lemma_adopted_kept(v, a1, a2, loans[i].part.resource(), last.resource());
                }
            }
            a2
        }
    }
}

/// One buffer comes to A, or is there: what keeps it alive is apart from every request. What the
/// program holds apart from it stays as it was, and nothing changes if A has its root.
pub(super) proof fn lemma_adopt_keeper<N: Device>(v: Seen<Lent>, a: Abs::State, reqs: Map<int, ReqId>, keeper: Resource<N>) -> (a2: Abs::State)
    requires
        placed(v, a, lent_qp(), reqs, lent_holds()),
        forall|k: int, l: LeaseId| #[trigger] leases_of(v, lent_holds(), k).contains_key(l)
            ==> hold_apart(leases_of(v, lent_holds(), k)[l], keeper),
    ensures
        abs_reaches(a, a2),
        placed(v, a2, lent_qp(), reqs, lent_holds()),
        a2.requests == a.requests,
        a2.outcomes == a.outcomes,
        a2.qps == a.qps,
        a.next_id <= a2.next_id,
        adopted(v, a2, keeper),
        !keeper.unique() && has_root(v, a, lent_holds(), keeper.byte_domain().to_iset()) ==> a2 == a,
        forall|k: LeaseId| #[trigger] free_lease(v, a, lent_holds(), k) && a.leases[k].mem.disjoint(keeper.byte_domain().to_iset())
            ==> free_lease(v, a2, lent_holds(), k) && a2.leases[k] == a.leases[k],
        same_but_program(a, a2),
{
    let bytes = keeper.byte_domain().to_iset();
    if !keeper.unique() && has_root(v, a, lent_holds(), bytes) {
        lemma_abs_reaches_refl(a);
        return a;
    }
    assert(holds_apart(v, lent_holds(), bytes)) by {
        assert forall|k: int, l: LeaseId| #[trigger] leases_of(v, lent_holds(), k).contains_key(l) implies {
            &&& leases_of(v, lent_holds(), k)[l].mem.disjoint(bytes)
            &&& leases_of(v, lent_holds(), k)[l].kept.disjoint(bytes)
        } by {
            let hold = leases_of(v, lent_holds(), k)[l];
            assert(hold_apart(hold, keeper));
            if !hold.kept.disjoint(bytes) {
                assert(hold.shared);
                assert(has_root(v, a, lent_holds(), hold.kept));
            }
            assert forall|x: int| !(hold.mem.contains(x) && bytes.contains(x)) by {
                assert(hold.mem.contains(x) ==> hold.kept.contains(x));
            }
        }
    }
    let a1 = lemma_adopt(v, a, lent_qp(), reqs, lent_holds(), bytes);
    let name = a.next_id;
    if keeper.unique() {
        assert(free_lease(v, a1, lent_holds(), name));
        a1
    } else {
        let a2 = lemma_adopted_frozen(v, a1, lent_qp(), reqs, lent_holds(), name);
        lemma_abs_reaches_trans(a, a1, a2);
        crate::refine::lemma_same_but_program_trans(a, a1, a2);
        assert(free_lease(v, a2, lent_holds(), name));
        assert forall|k: LeaseId| #[trigger] free_lease(v, a, lent_holds(), k) && a.leases[k].mem.disjoint(bytes)
            implies free_lease(v, a2, lent_holds(), k) && a2.leases[k] == a.leases[k] by {
            assert(free_lease(v, a1, lent_holds(), k));
            assert(k != name);
        }
        a2
    }
}

/// A keeper apart from one that came to A stays adopted: nothing it holds was dropped. Had they been
/// one frozen allocation, its root would have been there already.
proof fn lemma_adopted_kept<N: Device>(v: Seen<Lent>, a: Abs::State, a2: Abs::State, keeper: Resource<N>, incoming: Resource<N>)
    requires
        adopted(v, a, keeper),
        keeper.apart(incoming),
        !incoming.unique() && has_root(v, a, lent_holds(), incoming.byte_domain().to_iset()) ==> a2 == a,
        forall|k: LeaseId| #[trigger] free_lease(v, a, lent_holds(), k) && a.leases[k].mem.disjoint(incoming.byte_domain().to_iset())
            ==> free_lease(v, a2, lent_holds(), k) && a2.leases[k] == a.leases[k],
    ensures
        adopted(v, a2, keeper),
{
    let bytes = keeper.byte_domain().to_iset();
    if keeper.same_frozen(incoming) {
        keeper.lemma_same_frozen(incoming);
    } else {
        lemma_disjoint_iset(keeper.byte_domain(), incoming.byte_domain());
        if keeper.unique() {
            let l = choose|l: LeaseId| #[trigger] free_lease(v, a, lent_holds(), l) && a.leases[l].unique
                && a.leases[l].any_bytes && a.leases[l].mem == bytes;
            assert(free_lease(v, a2, lent_holds(), l));
        } else {
            let r = choose|r: LeaseId| #[trigger] free_lease(v, a, lent_holds(), r) && !a.leases[r].unique && a.leases[r].mem == bytes;
            assert(free_lease(v, a2, lent_holds(), r));
        }
    }
}

/// What an adopted keeper's lease is to an input it keeps: it stands for the input, lent for `dir`.
proof fn lemma_adopted_lent<N: Device>(v: Seen<Lent>, a: Abs::State, part: Part<N>, dir: Dir)
    requires
        adopted(v, a, part.resource()),
        part.inv(),
        dir is In ==> part.spec_writable(),
    ensures
        lent_ok(v, a, part, dir),
{
    let bytes = sge_model(part.spec_sge()).span().bytes();
    let whole = part.resource().byte_domain().to_iset();
    part.lemma_byte_backing();
    assert(part.byte_domain().to_iset() =~= bytes);
    if part.spec_writable() {
        let l = choose|l: LeaseId| #[trigger] free_lease(v, a, lent_holds(), l) && a.leases[l].unique
            && a.leases[l].any_bytes && a.leases[l].mem == whole;
        assert(stands_for(a.leases[l], part, dir));
    } else {
        let r = choose|r: LeaseId| #[trigger] free_lease(v, a, lent_holds(), r) && !a.leases[r].unique && a.leases[r].mem == whole;
        assert forall|x: int| bytes.contains(x) implies whole.contains(x) by {
            assert(part.byte_domain().contains(x));
        }
        assert(stands_for(a.leases[r], part, dir));
    }
}

/// The bytes of a request's input are in a lease it holds, in its direction, if it has bytes.
pub(crate) open spec fn loan_covered<N: Device>(h: Holds, loan: crate::buf::Loan<N>) -> bool {
    loan.part.spec_sge().length > 0 ==> exists|l: LeaseId| #[trigger] h.contains_key(l)
        && loan.access == Some(h[l].dir)
        && sge_model(loan.part.spec_sge()).span().bytes().subset_of(h[l].mem)
}

/// Every input of a request is in a lease it holds.
pub(crate) open spec fn holds_cover<N: Device>(h: Holds, loans: Seq<crate::buf::Loan<N>>) -> bool {
    forall|i: int| 0 <= i < loans.len() ==> #[trigger] loan_covered(h, loans[i])
}

// =============================================================================
// M moves, the rings as they are

/// M's queues of `q` keep their heads and lose no WQE.
pub(super) open spec fn keeps_queues(m: Nic::State, n: Nic::State, q: QpId) -> bool {
    &&& m.qps.contains_key(q)
    &&& n.qps.contains_key(q)
    &&& n.qps[q].sq_head == m.qps[q].sq_head
    &&& m.qps[q].sq.len() <= n.qps[q].sq.len()
    &&& n.qps[q].rq_head == m.qps[q].rq_head
    &&& m.qps[q].rq.len() <= n.qps[q].rq.len()
}

/// A queue that keeps its head and loses no WQE keeps what is posted posted.
proof fn lemma_posted_kept(m: Nic::State, n: Nic::State, w: WqeRef)
    requires
        safeverbs_spec::refinement::posted(m, w),
        keeps_queues(m, n, w.qp),
    ensures
        safeverbs_spec::refinement::posted(n, w),
{
}

/// A's view of the table depends on M through the heads of the table's queues and the outcomes of
/// what they popped.
proof fn lemma_view_moved<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, n: Nic::State, g: Refinement)
    requires
        linked(qps, m, g),
        forall|i: int| has_entry(qps, i) ==> keeps_queues(m, n, #[trigger] qps[i]->0.id()),
        forall|w: WqeRef| #[trigger] m.retired.contains_key(w)
            ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w],
    ensures
        same(table_view(qps, m), table_view(qps, n)),
{
    assert forall|k: int| #[trigger] (table_view(qps, m).req)(k) == (table_view(qps, n).req)(k) by {
        lemma_table_req_moved(qps, m, n, g, k);
    }
    assert forall|k: int| #[trigger] (table_view(qps, m).staged)(k) == (table_view(qps, n).staged)(k) by {}
    assert forall|k: int| #[trigger] (table_view(qps, m).refused)(k) == (table_view(qps, n).refused)(k) by {}
}

/// [`lemma_view_moved`] for one request.
proof fn lemma_table_req_moved<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, n: Nic::State, g: Refinement, k: int)
    requires
        linked(qps, m, g),
        forall|i: int| has_entry(qps, i) ==> keeps_queues(m, n, #[trigger] qps[i]->0.id()),
        forall|w: WqeRef| #[trigger] m.retired.contains_key(w)
            ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w],
    ensures
        table_req(qps, m, k) == table_req(qps, n, k),
{
    let i = key_entry(k);
    if has_entry(qps, i) {
        let e = qps[i]->0;
        let queue = key_queue(k);
        let r = ring_of(e, queue);
        let s = key_seq(k);
        assert(keeps_queues(m, n, e.id()));
        if let Some(rv) = r.req(s) {
            let range = range_of(e.id(), queue, r.meta(s)->0);
            assert(head_of(m.qps[e.id()], queue) == head_of(n.qps[e.id()], queue));
            if popped(m, range) && !rv.detached {
                assert(table_req(qps, m, k) is Some);
                assert(g.ranges[g.reqs[k]] == key_range(qps, k));
                assert(g.ranges.contains_key(g.reqs[k]));
                lemma_aggregate_stays(range, m, n);
            }
        }
    }
}

/// A range M popped has its aggregate for good.
pub(super) proof fn lemma_aggregate_stays(range: WqeRange, m: Nic::State, n: Nic::State)
    requires
        range.wf(),
        history_ok(m),
        m.qps.contains_key(range.qp),
        popped(m, range),
        forall|w: WqeRef| #[trigger] m.retired.contains_key(w)
            ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w],
    ensures
        range.all_retired(m.retired),
        range.aggregate(n.retired) == range.aggregate(m.retired),
{
    lemma_popped_retired(range, m);
    assert forall|s: nat| range.first <= s < range.end implies n.retired.contains_key(#[trigger] range.at(s))
        && n.retired[range.at(s)] == m.retired[range.at(s)] by {
        assert(m.retired.contains_key(range.at(s)));
    }
    send_outcomes::lemma_aggregate_framed(range, m.retired, n.retired);
}

/// The WQEs M popped retired.
proof fn lemma_popped_retired(range: WqeRange, m: Nic::State)
    requires
        history_ok(m),
        m.qps.contains_key(range.qp),
        popped(m, range),
    ensures
        range.all_retired(m.retired),
{
    assert forall|s: nat| range.first <= s < range.end implies m.retired.contains_key(#[trigger] range.at(s)) by {
        assert(range.at(s).seq < head_of(m.qps[range.at(s).qp], range.at(s).queue));
    }
}

/// What `ranges_ok` says of M stays through a run that keeps the queues of A's requests.
proof fn lemma_ranges_moved<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, n: Nic::State, g: Refinement, wrs: IMap<WqeRef, SendWr>)
    requires
        linked(qps, m, g),
        nic_reaches(m, n),
        history_ok(n),
        forall|i: int| has_entry(qps, i) ==> keeps_queues(m, n, #[trigger] qps[i]->0.id()),
        forall|w: WqeRef| #[trigger] m.retired.contains_key(w)
            ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w],
        forall|w: WqeRef| #[trigger] known(m, w) ==> known(n, w),
    ensures
        ranges_ok(Refinement { wrs, ..g }, n),
{
    let a = g.abs;
    let v = table_view(qps, m);
    assert forall|r: ReqId| #[trigger] g.ranges.contains_key(r) implies range_known(n, g.ranges[r]) by {
        assert forall|s: nat| g.ranges[r].first <= s < g.ranges[r].end implies known(n, #[trigger] g.ranges[r].at(s)) by {
            assert(range_known(m, g.ranges[r]));
            assert(known(m, g.ranges[r].at(s)));
        }
    }
    assert forall|r: ReqId| #[trigger] a.in_flight(r) implies {
        &&& safeverbs_spec::refinement::posted(n, g.ranges[r].last())
        &&& g.ranges[r].queue is Send ==> g.ranges[r].errors_stick(n)
    } by {
        let range = g.ranges[r];
        assert(names_request(v, g.reqs, r));
        let k = choose|k: int| #[trigger] (v.req)(k) is Some && g.reqs[k] == r;
        assert(table_req(qps, m, k) is Some);
        assert(range == key_range(qps, k));
        let i = key_entry(k);
        assert(has_entry(qps, i));
        assert(keeps_queues(m, n, qps[i]->0.id()));
        assert(range.qp == qps[i]->0.id());
        assert(safeverbs_spec::refinement::posted(m, range.last()));
        lemma_posted_kept(m, n, range.last());
        if range.queue is Send {
            lemma_pending_of_last(range, m);
            lemma_request_reaches(range, m, n);
            if !(range.pending(n) && range.errors_stick(n)) {
                assert(n.retired.contains_key(range.at((range.end - 1) as nat)));
                assert(range.at((range.end - 1) as nat) == range.last());
                assert(range.last().seq < head_of(n.qps[range.last().qp], range.last().queue));
            }
        }
    }
    assert forall|r: ReqId| #[trigger] a.outcomes.contains_key(r) implies {
        &&& g.ranges[r].all_retired(n.retired)
        &&& g.ranges[r].aggregate(n.retired) == a.outcomes[r]
        &&& g.ranges[r].no_success_after_error(n.retired)
    } by {
        let range = g.ranges[r];
        assert forall|s: nat| range.first <= s < range.end implies n.retired.contains_key(#[trigger] range.at(s))
            && n.retired[range.at(s)] == m.retired[range.at(s)] by {
            assert(m.retired.contains_key(range.at(s)));
        }
        send_outcomes::lemma_aggregate_framed(range, m.retired, n.retired);
        assert forall|j: nat, l: nat| #![trigger n.retired[range.at(j)], n.retired[range.at(l)]]
            range.first <= j < l < range.end && n.retired[range.at(j)].status.is_err()
            implies n.retired[range.at(l)].status.is_err() by {
            assert(n.retired[range.at(j)] == m.retired[range.at(j)]);
            assert(n.retired[range.at(l)] == m.retired[range.at(l)]);
        }
    }
}

/// M moved by a run that keeps the queues of the table: A stands where it stood, with the WRs M
/// posted in its log.
pub(super) proof fn lemma_linked_moved<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, n: Nic::State, g: Refinement) -> (g2: Refinement)
    requires
        linked(qps, m, g),
        nic_reaches(m, n),
        forall|i: int| has_entry(qps, i) ==> keeps_queues(m, n, #[trigger] qps[i]->0.id()),
    ensures
        linked(qps, n, g2),
        g2 == (Refinement { wrs: g2.wrs, ..g }),
        g2.extends(g),
        same(table_view(qps, m), table_view(qps, n)),
        kept_free(table_view(qps, m), g.abs, table_view(qps, n), g2.abs, lent_holds()),
{
    let wrs = lemma_history_reaches(m, n, g.wrs);
    let g2 = Refinement { wrs, ..g };
    lemma_view_moved(qps, m, n, g);
    lemma_same(table_view(qps, m), table_view(qps, n), g.abs, lent_qp(), g.reqs, lent_holds());
    lemma_ranges_moved(qps, m, n, g, wrs);
    assert forall|k: int| #[trigger] table_req(qps, n, k) is Some implies {
        &&& g2.ranges.contains_key(g2.reqs[k])
        &&& g2.ranges[g2.reqs[k]] == key_range(qps, k)
    } by {
        assert((table_view(qps, m).req)(k) == (table_view(qps, n).req)(k));
    }
    lemma_abs_reaches_refl(g.abs);
    g2
}

/// M moved by steps that do not touch the engine's share: A stands where it stood.
pub(super) proof fn lemma_linked_rely<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, name: nat, t: NicToken, m: Nic::State, n: Nic::State, pending: Seq<Cqe>, pins: ISet<Key>, g: Refinement) -> (g2: Refinement)
    requires
        linked(qps, m, g),
        table_ok(qps, name, t, m, pending),
        rely_reaches(t.share(), pins, m, n),
    ensures
        linked(qps, n, g2),
        g2 == (Refinement { wrs: g2.wrs, ..g }),
        g2.extends(g),
        nic_reaches(m, n),
        same(table_view(qps, m), table_view(qps, n)),
        kept_free(table_view(qps, m), g.abs, table_view(qps, n), g2.abs, lent_holds()),
{
    lemma_rely_frame(t.share(), pins, m, n);
    assert forall|i: int| has_entry(qps, i) implies keeps_queues(m, n, #[trigger] qps[i]->0.id()) by {
        assert(qp_ok(qps[i], i, t, m));
        assert(t.share().owns.contains(qps[i]->0.id()));
    }
    lemma_linked_moved(qps, m, n, g)
}

/// A verb that keeps the queues of the table: A stands where it stood.
pub(super) proof fn lemma_linked_verb<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, n: Nic::State, step: Nic::Step, g: Refinement) -> (g2: Refinement)
    requires
        linked(qps, m, g),
        nic_step(m, n, step),
        forall|i: int| has_entry(qps, i) ==> keeps_queues(m, n, #[trigger] qps[i]->0.id()),
    ensures
        linked(qps, n, g2),
        g2 == (Refinement { wrs: g2.wrs, ..g }),
        g2.extends(g),
        nic_reaches(m, n),
        same(table_view(qps, m), table_view(qps, n)),
        kept_free(table_view(qps, m), g.abs, table_view(qps, n), g2.abs, lent_holds()),
{
    lemma_nic_step_is_next(m, n, step);
    lemma_nic_reaches_next(m, n);
    lemma_linked_moved(qps, m, n, g)
}

// =============================================================================
// M pops a CQE: A completes what that retires

/// Request `k` of the table is in flight in A, and M has now popped its last WQE.
pub(super) open spec fn newly_popped<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, before: Nic::State, after: Nic::State, k: int) -> bool {
    &&& table_req(qps, before, k) is Some
    &&& table_req(qps, before, k)->0.phase is InFlight
    &&& popped(after, key_range(qps, k))
}

/// Which requests M's pop completes in A.
pub(super) open spec fn popped_done<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, before: Nic::State, after: Nic::State) -> spec_fn(int) -> bool {
    |k: int| newly_popped(qps, before, after, k)
}

/// With what outcome.
pub(super) open spec fn popped_outcome<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, after: Nic::State) -> spec_fn(int) -> Outcome {
    |k: int| key_range(qps, k).aggregate(after.retired)
}

/// The keys of one queue of one entry are the interval of its first.
proof fn lemma_key_interval(k: int, i: int, queue: Queue)
    ensures
        (key(i, queue, 0) <= k < key(i, queue, 0) + key_span()) == (key_entry(k) == i && key_queue(k) == queue),
{
    lemma_key_made(k);
    if key(i, queue, 0) <= k < key(i, queue, 0) + key_span() {
        lemma_key_fields(i, queue, k - key(i, queue, 0));
        assert(key(i, queue, k - key(i, queue, 0)) == k);
    }
}

/// M's heads after a pop: the CQE's queue moved on, and it alone.
proof fn lemma_popped_heads<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, before: Nic::State, after: Nic::State, c: CqId, i: int, j: int, queue: Queue)
    requires
        nic_inv(before),
        history_ok(before),
        nic_step(before, after, Nic::Step::poll_cq(c)),
        has_entry(qps, i),
        has_entry(qps, j),
        qps[i]->0.id() == before.cqs[c].entries[0].qp,
        before.qps.contains_key(qps[j]->0.id()),
        i != j ==> qps[i]->0.id() != qps[j]->0.id(),
    ensures
        after.qps.contains_key(qps[j]->0.id()),
        head_of(before.qps[qps[j]->0.id()], queue) <= head_of(after.qps[qps[j]->0.id()], queue),
        !(i == j && queue == before.cqs[c].entries[0].queue)
            ==> head_of(before.qps[qps[j]->0.id()], queue) == head_of(after.qps[qps[j]->0.id()], queue),
{
    lemma_polled_wqes(before, after, c);
    let e = before.cqs[c].entries[0];
    if queue != e.queue {
        assert(queue == other_queue(e.queue));
    }
}

/// What A sees of request `k` once its `complete`s are done is what M's pop makes of it.
proof fn lemma_popped_req<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, before: Nic::State, after: Nic::State, c: CqId, g: Refinement, i: int, k: int)
    requires
        linked(qps, before, g),
        nic_step(before, after, Nic::Step::poll_cq(c)),
        has_entry(qps, i),
        qps[i]->0.id() == before.cqs[c].entries[0].qp,
        forall|j: int| has_entry(qps, j) ==> before.qps.contains_key(#[trigger] qps[j]->0.id()),
        forall|a: int, b: int| #![trigger qps[a], qps[b]] has_entry(qps, a) && has_entry(qps, b) && a != b
            ==> qps[a]->0.id() != qps[b]->0.id(),
    ensures
        (completed_where(table_view(qps, before), key(i, before.cqs[c].entries[0].queue, 0),
            key(i, before.cqs[c].entries[0].queue, 0) + key_span(), popped_done(qps, before, after), popped_outcome(qps, after)).req)(k)
            == table_req(qps, after, k),
{
    let e = before.cqs[c].entries[0];
    let v = table_view(qps, before);
    let from = key(i, e.queue, 0);
    let to = from + key_span();
    let done = popped_done(qps, before, after);
    lemma_polled_wqes(before, after, c);
    lemma_key_interval(k, i, e.queue);
    let j = key_entry(k);
    if has_entry(qps, j) {
        let x = qps[j]->0;
        let queue = key_queue(k);
        let s = key_seq(k);
        let r = ring_of(x, queue);
        lemma_popped_heads(qps, before, after, c, i, j, queue);
        if let Some(rv) = r.req(s) {
            let range = range_of(x.id(), queue, r.meta(s)->0);
            assert(key_range(qps, k) == range);
            if table_req(qps, before, k) is Some && popped(before, range) {
                assert(g.ranges[g.reqs[k]] == key_range(qps, k));
                assert(g.ranges.contains_key(g.reqs[k]));
                lemma_aggregate_stays(range, before, after);
                assert(!done(k));
            } else if popped(before, range) {
                assert(!done(k));
            } else if popped(after, range) {
                assert(table_req(qps, before, k) is Some);
                assert(from <= k < to);
                assert(done(k));
            } else {
                assert(!done(k));
            }
        } else {
            assert(!done(k));
        }
    } else {
        assert(!done(k));
    }
}

/// What `ranges_ok` says of M, after a pop and the `complete`s it asks for.
proof fn lemma_ranges_popped<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, before: Nic::State, after: Nic::State, c: CqId, g: Refinement, g2: Refinement, i: int, from: int, to: int)
    requires
        linked(qps, before, g),
        nic_inv(before),
        nic_step(before, after, Nic::Step::poll_cq(c)),
        history_ok(after),
        has_entry(qps, i),
        qps[i]->0.id() == before.cqs[c].entries[0].qp,
        forall|j: int| has_entry(qps, j) ==> before.qps.contains_key(#[trigger] qps[j]->0.id()),
        forall|a: int, b: int| #![trigger qps[a], qps[b]] has_entry(qps, a) && has_entry(qps, b) && a != b
            ==> qps[a]->0.id() != qps[b]->0.id(),
        forall|w: WqeRef| #[trigger] before.retired.contains_key(w)
            ==> after.retired.contains_key(w) && after.retired[w] == before.retired[w],
        forall|w: WqeRef| #[trigger] known(before, w) ==> known(after, w),
        g2.reqs == g.reqs,
        g2.ranges == g.ranges,
        placed(table_view(qps, after), g2.abs, lent_qp(), g2.reqs, lent_holds()),
        g2.abs.next_id == g.abs.next_id,
        forall|t: ReqId| #[trigger] g2.abs.requests.contains_key(t) ==> g.abs.requests.contains_key(t)
            && g2.abs.requests[t].qp == g.abs.requests[t].qp,
        forall|t: ReqId| #[trigger] g2.abs.in_flight(t) ==> g.abs.in_flight(t),
        forall|t: ReqId| #[trigger] g.abs.outcomes.contains_key(t) ==> g2.abs.outcomes.contains_key(t)
            && g2.abs.outcomes[t] == g.abs.outcomes[t],
        forall|t: ReqId| #[trigger] g2.abs.outcomes.contains_key(t) && !g.abs.outcomes.contains_key(t)
            ==> exists|s: int| from <= s < to && (popped_done(qps, before, after))(s) && #[trigger] (table_view(qps, before).req)(s) is Some
                && g.reqs[s] == t && g2.abs.outcomes[t] == (popped_outcome(qps, after))(s),
    ensures
        ranges_ok(g2, after),
{
    let (a, a2) = (g.abs, g2.abs);
    let (done, o) = (popped_done(qps, before, after), popped_outcome(qps, after));
    lemma_polled_wqes(before, after, c);
    let v = table_view(qps, before);
    let v2 = table_view(qps, after);
    assert forall|r: ReqId| #[trigger] g2.abs.outcomes.contains_key(r) implies g2.ranges.contains_key(r) by {
        if !a.outcomes.contains_key(r) {
            let s = choose|s: int| from <= s < to && done(s) && #[trigger] (v.req)(s) is Some
                && g.reqs[s] == r && a2.outcomes[r] == o(s);
            assert(table_req(qps, before, s) is Some);
        }
    }
    assert forall|r: ReqId| #[trigger] g2.ranges.contains_key(r) implies range_known(after, g2.ranges[r]) by {
        assert(range_known(before, g.ranges[r]));
        assert forall|s: nat| g2.ranges[r].first <= s < g2.ranges[r].end implies known(after, #[trigger] g2.ranges[r].at(s)) by {
            assert(known(before, g.ranges[r].at(s)));
        }
    }
    assert forall|r: ReqId| #[trigger] a2.in_flight(r) implies {
        &&& safeverbs_spec::refinement::posted(after, g2.ranges[r].last())
        &&& g2.ranges[r].queue is Send ==> g2.ranges[r].errors_stick(after)
    } by {
        let range = g.ranges[r];
        assert(a.in_flight(r));
        assert(names_request(v2, g.reqs, r));
        let k = choose|k: int| #[trigger] (v2.req)(k) is Some && g.reqs[k] == r;
        assert(sits(v2, a2, lent_qp(), g.reqs, lent_holds(), k));
        assert(table_req(qps, after, k) is Some);
        lemma_popped_keeps_req(qps, before, after, c, i, k);
        assert(g.ranges[g.reqs[k]] == key_range(qps, k));
        assert(range == key_range(qps, k));
        assert(!popped(after, range));
        let last = range.last();
        assert(safeverbs_spec::refinement::posted(before, last));
        let e = before.cqs[c].entries[0];
        if last.queue != e.queue {
            assert(last.queue == other_queue(e.queue));
        }
        assert(safeverbs_spec::refinement::posted(after, last));
        if range.queue is Send {
            lemma_pending_of_last(range, before);
            lemma_request_step(range, before, after, Nic::Step::poll_cq(c));
            if !range.pending(after) {
                assert(after.retired.contains_key(range.at((range.end - 1) as nat)));
                assert(range.at((range.end - 1) as nat) == last);
            }
        }
    }
    assert forall|r: ReqId| #[trigger] a2.outcomes.contains_key(r) implies {
        &&& g2.ranges[r].all_retired(after.retired)
        &&& g2.ranges[r].aggregate(after.retired) == a2.outcomes[r]
        &&& g2.ranges[r].no_success_after_error(after.retired)
    } by {
        let range = g.ranges[r];
        if a.outcomes.contains_key(r) {
            assert forall|s: nat| range.first <= s < range.end implies after.retired.contains_key(#[trigger] range.at(s))
                && after.retired[range.at(s)] == before.retired[range.at(s)] by {
                assert(before.retired.contains_key(range.at(s)));
            }
            send_outcomes::lemma_aggregate_framed(range, before.retired, after.retired);
            assert forall|j: nat, l: nat| #![trigger after.retired[range.at(j)], after.retired[range.at(l)]]
                range.first <= j < l < range.end && after.retired[range.at(j)].status.is_err()
                implies after.retired[range.at(l)].status.is_err() by {
                assert(after.retired[range.at(j)] == before.retired[range.at(j)]);
                assert(after.retired[range.at(l)] == before.retired[range.at(l)]);
            }
        } else {
            let s = choose|s: int| from <= s < to && done(s) && #[trigger] (v.req)(s) is Some
                && g.reqs[s] == r && a2.outcomes[r] == o(s);
            assert(newly_popped(qps, before, after, s));
            assert(table_req(qps, before, s) is Some);
            assert(g.ranges[g.reqs[s]] == key_range(qps, s));
            assert(sits(v, a, lent_qp(), g.reqs, lent_holds(), s));
            assert(a.in_flight(r));
            assert(has_entry(qps, key_entry(s)));
            assert(before.qps.contains_key(qps[key_entry(s)]->0.id()));
            lemma_key_entry_keeps(qps, before, after, c, i, key_entry(s));
            lemma_popped_retired(range, after);
            if range.queue is Send {
                lemma_pending_of_last(range, before);
                lemma_request_step(range, before, after, Nic::Step::poll_cq(c));
                if !range.all_retired(after.retired) {
                    assert(false);
                }
            }
        }
    }
}

/// The QP of an entry stays through a pop.
proof fn lemma_key_entry_keeps<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, before: Nic::State, after: Nic::State, c: CqId, i: int, j: int)
    requires
        nic_inv(before),
        history_ok(before),
        nic_step(before, after, Nic::Step::poll_cq(c)),
        has_entry(qps, j),
        before.qps.contains_key(qps[j]->0.id()),
    ensures
        after.qps.contains_key(qps[j]->0.id()),
{
    lemma_polled_wqes(before, after, c);
}

/// A request A still sees after a pop it saw before.
proof fn lemma_popped_keeps_req<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, before: Nic::State, after: Nic::State, c: CqId, i: int, k: int)
    requires
        nic_inv(before),
        history_ok(before),
        nic_step(before, after, Nic::Step::poll_cq(c)),
        has_entry(qps, i),
        qps[i]->0.id() == before.cqs[c].entries[0].qp,
        forall|j: int| has_entry(qps, j) ==> before.qps.contains_key(#[trigger] qps[j]->0.id()),
        forall|a: int, b: int| #![trigger qps[a], qps[b]] has_entry(qps, a) && has_entry(qps, b) && a != b
            ==> qps[a]->0.id() != qps[b]->0.id(),
        table_req(qps, after, k) is Some,
    ensures
        table_req(qps, before, k) is Some,
{
    let j = key_entry(k);
    let x = qps[j]->0;
    let queue = key_queue(k);
    let r = ring_of(x, queue);
    lemma_popped_heads(qps, before, after, c, i, j, queue);
    let range = range_of(x.id(), queue, r.meta(key_seq(k))->0);
    assert(popped(before, range) ==> popped(after, range));
}

/// M pops a CQE of queue `queue` of entry `i` of the table: A's `complete` of each request whose
/// last WQE that retires, with M's aggregate of its WQEs.
pub(super) proof fn lemma_linked_popped<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, before: Nic::State, after: Nic::State, c: CqId, g: Refinement, i: int) -> (g2: Refinement)
    requires
        linked(qps, before, g),
        nic_step(before, after, Nic::Step::poll_cq(c)),
        has_entry(qps, i),
        qps[i]->0.id() == before.cqs[c].entries[0].qp,
        forall|j: int| has_entry(qps, j) ==> before.qps.contains_key(#[trigger] qps[j]->0.id()),
        forall|a: int, b: int| #![trigger qps[a], qps[b]] has_entry(qps, a) && has_entry(qps, b) && a != b
            ==> qps[a]->0.id() != qps[b]->0.id(),
    ensures
        linked(qps, after, g2),
        g2.extends(g),
        nic_reaches(before, after),
        kept_free(table_view(qps, before), g.abs, table_view(qps, after), g2.abs, lent_holds()),
{
    let e = before.cqs[c].entries[0];
    let v = table_view(qps, before);
    let a = g.abs;
    lemma_nic_step_is_next(before, after, Nic::Step::poll_cq(c));
    lemma_nic_reaches_next(before, after);
    let wrs = lemma_history_reaches(before, after, g.wrs);
    let from = key(i, e.queue, 0);
    let to = from + key_span();
    let done = popped_done(qps, before, after);
    let o = popped_outcome(qps, after);
    assert forall|s: int| from <= s < to && done(s) && #[trigger] (v.req)(s) is Some
        implies (v.req)(s)->0.phase is InFlight by {
    }
    let a2 = lemma_complete_where(v, a, lent_qp(), g.reqs, lent_holds(), from, to, done, o);
    let w = completed_where(v, from, to, done, o);
    // That is what A sees of the table now.
    assert(same(w, table_view(qps, after))) by {
        assert forall|k: int| #[trigger] (w.req)(k) == (table_view(qps, after).req)(k) by {
            lemma_popped_req(qps, before, after, c, g, i, k);
        }
    }
    lemma_same(w, table_view(qps, after), a2, lent_qp(), g.reqs, lent_holds());
    lemma_kept_free_trans(v, a, w, a2, table_view(qps, after), a2, lent_holds());
    let g2 = Refinement { abs: a2, wrs, ..g };
    lemma_ranges_popped(qps, before, after, c, g, g2, i, from, to);
    assert forall|k: int| #[trigger] table_req(qps, after, k) is Some implies {
        &&& g2.ranges.contains_key(g2.reqs[k])
        &&& g2.ranges[g2.reqs[k]] == key_range(qps, k)
    } by {
        lemma_popped_keeps_req(qps, before, after, c, i, k);
    }
    g2
}

/// A poll that gave `wcs`: A's `complete` of each request its CQEs retire.
pub(super) proof fn lemma_linked_polled<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, name: nat, t: NicToken, m: Nic::State, n: Nic::State, wcs: Seq<Wc>, pending: Seq<Cqe>, g: Refinement) -> (g2: Refinement)
    requires
        linked(qps, m, g),
        table_ok(qps, name, t, m, pending),
        polled(t.share(), pinned(t), m, n, wcs),
    ensures
        linked(qps, n, g2),
        g2.extends(g),
        nic_reaches(m, n),
        kept_free(table_view(qps, m), g.abs, table_view(qps, n), g2.abs, lent_holds()),
    decreases wcs.len(),
{
    let sh = t.share();
    if wcs.len() == 0 {
        lemma_linked_rely(qps, name, t, m, n, pending, pinned(t), g)
    } else {
        let (mid, before) = choose|mid: Nic::State, before: Nic::State|
            {
                &&& polled(sh, pinned(t), m, mid, wcs.drop_last())
                &&& rely_reaches(sh, pinned(t), mid, before)
                &&& nic_step(before, n, Nic::Step::poll_cq(sh.cq))
                &&& reports(wcs.last(), before.cqs[sh.cq].entries[0])
            };
        let g1 = lemma_linked_polled(qps, name, t, m, mid, wcs.drop_last(), pending, g);
        let earlier = completion::lemma_table_polled(qps, name, t, m, mid, wcs.drop_last(), pending);
        let g2 = lemma_linked_rely(qps, name, t, mid, before, pending + earlier, pinned(t), g1);
        lemma_table_rely(qps, name, t, mid, before, pending + earlier);
        let c = sh.cq;
        let e = before.cqs[c].entries[0];
        safeverbs_spec::nic_proofs::lemma_head_is_first(before, c);
        assert(sh.owns.contains(e.qp));
        let i = choose|i: int| 0 <= i < qps.len() && #[trigger] qps[i] is Some && qps[i]->0.id() == e.qp;
        assert forall|j: int| has_entry(qps, j) implies before.qps.contains_key(#[trigger] qps[j]->0.id()) by {
            assert(qp_ok(qps[j], j, t, before));
        }
        let g3 = lemma_linked_popped(qps, before, n, c, g2, i);
        lemma_nic_reaches_trans(m, mid, before);
        lemma_nic_reaches_trans(m, before, n);
        g2.lemma_extends_trans(g1, g);
        g3.lemma_extends_trans(g2, g);
        lemma_kept_free_trans(table_view(qps, m), g.abs, table_view(qps, mid), g1.abs, table_view(qps, before), g2.abs, lent_holds());
        lemma_kept_free_trans(table_view(qps, m), g.abs, table_view(qps, before), g2.abs, table_view(qps, n), g3.abs, lent_holds());
        g3
    }
}

// =============================================================================
// One ring changes, M as it is

/// `post` is `pre` but for ring `queue` of entry `i`, which keeps its QP.
pub(super) open spec fn same_but_ring<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, i: int, queue: Queue) -> bool {
    &&& post.len() == pre.len()
    &&& forall|j: int| 0 <= j < pre.len() && j != i ==> #[trigger] post[j] == pre[j]
    &&& has_entry(pre, i)
    &&& post[i] is Some
    &&& post[i]->0.id() == pre[i]->0.id()
    &&& ring_of(post[i]->0, other_queue(queue)) == ring_of(pre[i]->0, other_queue(queue))
}

/// A's view of the requests of the other rings is as it was.
proof fn lemma_other_keys<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, i: int, queue: Queue, k: int)
    requires
        same_but_ring(pre, post, i, queue),
        !(key_entry(k) == i && key_queue(k) == queue),
    ensures
        table_req(post, m, k) == table_req(pre, m, k),
        table_staged(post, k) == table_staged(pre, k),
        table_refused(post, k) == table_refused(pre, k),
        table_req(pre, m, k) is Some ==> key_range(post, k) == key_range(pre, k),
{
    let j = key_entry(k);
    if j == i {
        assert(key_queue(k) == other_queue(queue));
    } else if 0 <= j < pre.len() {
        assert(post[j] == pre[j]);
    }
}

/// `w` is what A sees of table `post`, which differs from `pre` in one ring, if it says so of that
/// ring's requests and agrees with `pre` on the others.
proof fn lemma_same_by_ring<C, N: Device>(w: Seen<Lent>, pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, i: int, queue: Queue)
    requires
        same_but_ring(pre, post, i, queue),
        forall|k: int| !(key_entry(k) == i && key_queue(k) == queue) ==> {
            &&& (w.req)(k) == table_req(pre, m, k)
            &&& (w.staged)(k) == table_staged(pre, k)
            &&& (w.refused)(k) == table_refused(pre, k)
        },
        forall|s: int| 0 <= s < key_span() ==> {
            &&& (w.req)(#[trigger] key(i, queue, s)) == seen(ring_of(post[i]->0, queue), m, post[i]->0.id(), queue, s)
            &&& (w.staged)(key(i, queue, s)) == staged_seen(ring_of(post[i]->0, queue), post[i]->0.id(), s)
            &&& (w.refused)(key(i, queue, s)) == refused_seen(ring_of(post[i]->0, queue), post[i]->0.id(), s)
        },
    ensures
        same(w, table_view(post, m)),
{
    assert forall|k: int| #[trigger] (w.req)(k) == (table_view(post, m).req)(k) by {
        lemma_key_view(w, pre, post, m, i, queue, k);
    }
    assert forall|k: int| #[trigger] (w.staged)(k) == (table_view(post, m).staged)(k) by {
        lemma_key_view(w, pre, post, m, i, queue, k);
    }
    assert forall|k: int| #[trigger] (w.refused)(k) == (table_view(post, m).refused)(k) by {
        lemma_key_view(w, pre, post, m, i, queue, k);
    }
}

/// [`lemma_same_by_ring`] of one key.
proof fn lemma_key_view<C, N: Device>(w: Seen<Lent>, pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, i: int, queue: Queue, k: int)
    requires
        same_but_ring(pre, post, i, queue),
        forall|k: int| !(key_entry(k) == i && key_queue(k) == queue) ==> {
            &&& (w.req)(k) == table_req(pre, m, k)
            &&& (w.staged)(k) == table_staged(pre, k)
            &&& (w.refused)(k) == table_refused(pre, k)
        },
        forall|s: int| 0 <= s < key_span() ==> {
            &&& (w.req)(#[trigger] key(i, queue, s)) == seen(ring_of(post[i]->0, queue), m, post[i]->0.id(), queue, s)
            &&& (w.staged)(key(i, queue, s)) == staged_seen(ring_of(post[i]->0, queue), post[i]->0.id(), s)
            &&& (w.refused)(key(i, queue, s)) == refused_seen(ring_of(post[i]->0, queue), post[i]->0.id(), s)
        },
    ensures
        (w.req)(k) == table_req(post, m, k),
        (w.staged)(k) == table_staged(post, k),
        (w.refused)(k) == table_refused(post, k),
{
    lemma_key_made(k);
    if key_entry(k) == i && key_queue(k) == queue {
        assert(key(i, queue, key_seq(k)) == k);
    } else {
        lemma_other_keys(pre, post, m, i, queue, k);
    }
}

/// What the invariant reads of the table but A's view stays through a change of one ring whose
/// requests keep their WQEs.
proof fn lemma_rest_by_ring<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, g2: Refinement, i: int, queue: Queue)
    requires
        same_but_ring(pre, post, i, queue),
        live_ranges(pre, m, g),
        qps_match(pre, g.abs),
        g2.abs.qps == g.abs.qps,
        forall|k: int| #[trigger] table_req(pre, m, k) is Some ==> g2.ranges.contains_key(g.reqs[k]) && g2.ranges[g.reqs[k]] == g.ranges[g.reqs[k]]
            && g2.reqs[k] == g.reqs[k],
        forall|s: int| 0 <= s < key_span() && #[trigger] seen(ring_of(post[i]->0, queue), m, post[i]->0.id(), queue, s) is Some ==> {
            let (r0, r1) = (ring_of(pre[i]->0, queue), ring_of(post[i]->0, queue));
            &&& seen(r0, m, pre[i]->0.id(), queue, s) is Some
            &&& r1.meta(s)->0.first == r0.meta(s)->0.first
            &&& r1.meta(s)->0.end == r0.meta(s)->0.end
        },
    ensures
        live_ranges(post, m, g2),
        qps_match(post, g2.abs),
{
    assert forall|k: int| #[trigger] table_req(post, m, k) is Some implies {
        &&& g2.ranges.contains_key(g2.reqs[k])
        &&& g2.ranges[g2.reqs[k]] == key_range(post, k)
    } by {
        lemma_key_made(k);
        if key_entry(k) == i && key_queue(k) == queue {
            let s = key_seq(k);
            assert(table_req(post, m, k) == seen(ring_of(post[i]->0, queue), m, post[i]->0.id(), queue, s));
            assert(table_req(pre, m, k) == seen(ring_of(pre[i]->0, queue), m, pre[i]->0.id(), queue, s));
            assert(table_req(pre, m, k) is Some);
            assert(key_range(post, k) == key_range(pre, k));
        } else {
            lemma_other_keys(pre, post, m, i, queue, k);
        }
    }
    assert forall|j: int| has_entry(post, j) implies g2.abs.qps.contains(#[trigger] post[j]->0.id()) by {
        if j == i {
            assert(g.abs.qps.contains(pre[i]->0.id()));
        } else {
            assert(post[j] == pre[j]);
            assert(has_entry(pre, j));
            assert(g.abs.qps.contains(pre[j]->0.id()));
        }
    }
    assert forall|q: QpId| #[trigger] g2.abs.qps.contains(q) implies exists|j: int| has_entry(post, j) && #[trigger] post[j]->0.id() == q by {
        let j = choose|j: int| has_entry(pre, j) && #[trigger] pre[j]->0.id() == q;
        if j == i {
            assert(has_entry(post, i) && post[i]->0.id() == q);
        } else {
            assert(post[j] == pre[j]);
            assert(has_entry(post, j) && post[j]->0.id() == q);
        }
    }
}

/// `ranges_ok` through steps of A that have no new request or outcome.
proof fn lemma_ranges_kept(g: Refinement, a2: Abs::State, m: Nic::State)
    requires
        ranges_ok(g, m),
        a2.outcomes == g.abs.outcomes,
        g.abs.next_id <= a2.next_id,
        forall|r: ReqId| #[trigger] a2.requests.contains_key(r) ==> {
            &&& g.abs.requests.contains_key(r)
            &&& a2.requests[r].qp == g.abs.requests[r].qp
            &&& a2.in_flight(r) == g.abs.in_flight(r)
        },
    ensures
        ranges_ok(Refinement { abs: a2, ..g }, m),
{
    let g2 = Refinement { abs: a2, ..g };
    assert forall|r: ReqId| #[trigger] g2.ranges.contains_key(r) implies r < a2.next_id by {
        assert(r < g.abs.next_id);
    }
    assert forall|r: ReqId| #[trigger] a2.in_flight(r) implies {
        &&& safeverbs_spec::refinement::posted(m, g2.ranges[r].last())
        &&& g2.ranges[r].queue is Send ==> g2.ranges[r].errors_stick(m)
    } by {
        assert(g.abs.in_flight(r));
    }
}

/// A request its ring has complete is one M popped: the ring heard of the CQE M popped.
proof fn lemma_complete_popped<C, N: Device>(e: HwQp<C, N>, index: u16, m: Nic::State, sh: Share, started: bool, written: Seq<SendWr>, queue: Queue, s: int)
    requires
        e.ok(index, m, sh, started, written),
        ring_of(e, queue).req(s) is Some,
        ring_of(e, queue).req(s)->0.phase is Complete,
    ensures
        popped(m, range_of(e.id(), queue, ring_of(e, queue).meta(s)->0)),
{
    match queue {
        Queue::Send => {
            reveal(send_outcomes::send_results);
            assert(send_outcomes::send_result(e.send, m, e.id(), s));
        },
        Queue::Recv => {
            reveal(outcomes::recv_outcomes);
            assert(outcomes::recv_result(e.recv, m, e.id(), s));
        },
    }
}

/// `hold` is the lease of a request for its input `part`: what keeps it alive is the part's keeper.
pub(crate) open spec fn hold_of<N: Device>(hold: Hold, part: Part<N>) -> bool {
    let kept = part.resource().byte_domain().to_iset();
    &&& hold.kept == kept
    &&& hold.shared == !part.spec_writable()
    &&& hold.mem.subset_of(kept)
}

/// The leases of a request's inputs, named: what the fold over them has so far.
proof fn lemma_name_loans<N: Device>(v: Seen<Lent>, a: Abs::State, reqs: Map<int, ReqId>, loans: Seq<crate::buf::Loan<N>>, n: int) -> (r: (Abs::State, Holds))
    requires
        placed(v, a, lent_qp(), reqs, lent_holds()),
        0 <= n <= loans.len(),
        forall|i: int| 0 <= i < n ==> (#[trigger] loans[i]).access is Some,
        forall|i: int| 0 <= i < n ==> (#[trigger] loans[i]).part.inv(),
        forall|i: int| 0 <= i < n && (#[trigger] loans[i]).access == Some(Dir::In) ==> loans[i].part.spec_writable(),
        forall|i: int, j: int| #![trigger loans[i], loans[j]] 0 <= i < n && 0 <= j < n && i != j
            && (loans[i].part.spec_writable() || loans[j].part.spec_writable())
            ==> sge_model(loans[i].part.spec_sge()).span().bytes().disjoint(sge_model(loans[j].part.spec_sge()).span().bytes()),
        forall|i: int| 0 <= i < n && (#[trigger] loans[i]).part.spec_sge().length > 0
            ==> lent_ok(v, a, loans[i].part, loans[i].access->0),
    ensures
        forall|l: LeaseId| #[trigger] r.1.contains_key(l) ==> exists|i: int| 0 <= i < n
            && (#[trigger] loans[i]).part.spec_sge().length > 0 && hold_of(r.1[l], loans[i].part),
        forall|l: LeaseId| #[trigger] r.1.contains_key(l) && a.leases.contains_key(l) ==> a.leases[l].unique,
        abs_reaches(a, r.0),
        placed(v, r.0, lent_qp(), reqs, lent_holds()),
        r.0.requests == a.requests,
        r.0.outcomes == a.outcomes,
        r.0.qps == a.qps,
        a.next_id <= r.0.next_id,
        may_post_holds(r.0, r.1),
        forall|l: LeaseId| #[trigger] r.1.contains_key(l) ==> free_lease(v, r.0, lent_holds(), l),
        forall|i: int| 0 <= i < n ==> #[trigger] loan_covered(r.1, loans[i]),
        forall|l: LeaseId| #[trigger] free_lease(v, a, lent_holds(), l)
            ==> free_lease(v, r.0, lent_holds(), l) && r.0.leases[l] == a.leases[l],
        forall|l: LeaseId| #[trigger] r.1.contains_key(l) && r.0.leases[l].unique ==> exists|i: int| 0 <= i < n
            && (#[trigger] loans[i]).part.spec_sge().length > 0
            && r.0.leases[l].mem == sge_model(loans[i].part.spec_sge()).span().bytes(),
    decreases n,
{
    if n == 0 {
        lemma_abs_reaches_refl(a);
        (a, Map::empty())
    } else {
        let loan = loans[n - 1];
        let (a1, h) = lemma_name_loans(v, a, reqs, loans, n - 1);
        let bytes = sge_model(loan.part.spec_sge()).span().bytes();
        if loan.part.spec_sge().length == 0 {
            (a1, h)
        } else {
            let dir = loan.access->0;
            let l = choose|l: LeaseId| #[trigger] free_lease(v, a, lent_holds(), l) && stands_for(a.leases[l], loan.part, dir);
            assert(free_lease(v, a1, lent_holds(), l) && a1.leases[l] == a.leases[l]);
            if loan.part.spec_writable() {
                assert(!h.contains_key(l)) by {
                    if h.contains_key(l) {
                        // Its bytes would be those of an input before, and they have some.
                        let i = choose|i: int| 0 <= i < n - 1
                            && (#[trigger] loans[i]).part.spec_sge().length > 0
                            && a1.leases[l].mem == sge_model(loans[i].part.spec_sge()).span().bytes();
                        assert(loans[i] == loans[i]);
                        let x = loan.part.spec_sge().addr as int;
                        assert(bytes.contains(x));
                        assert(sge_model(loans[i].part.spec_sge()).span().bytes().contains(x));
                    }
                }
                let h2 = h.insert(l, Hold { dir, mem: a1.leases[l].mem, kept: a1.leases[l].mem, shared: false });
                assert forall|k: LeaseId| #[trigger] h2.contains_key(k) && a.leases.contains_key(k) implies a.leases[k].unique by {
                    if k != l {
                        assert(h.contains_key(k));
                    }
                }
                loan.part.lemma_byte_backing();
                assert(loan.part.byte_domain().to_iset() =~= bytes);
                assert(hold_of(h2[l], loan.part));
                assert forall|k: LeaseId| #[trigger] h2.contains_key(k) implies exists|i: int| 0 <= i < n
                    && (#[trigger] loans[i]).part.spec_sge().length > 0 && hold_of(h2[k], loans[i].part) by {
                    if k == l {
                        assert(0 <= n - 1 < n && loans[n - 1] == loan);
                    } else {
                        assert(h.contains_key(k) && h2[k] == h[k]);
                        let i = choose|i: int| 0 <= i < n - 1
                            && (#[trigger] loans[i]).part.spec_sge().length > 0 && hold_of(h[k], loans[i].part);
                        assert(0 <= i < n);
                    }
                }
                assert forall|i: int| 0 <= i < n implies #[trigger] loan_covered(h2, loans[i]) by {
                    if i < n - 1 {
                        assert(loan_covered(h, loans[i]));
                        if loans[i].part.spec_sge().length > 0 {
                            let k = choose|k: LeaseId| #[trigger] h.contains_key(k) && loans[i].access == Some(h[k].dir)
                                && sge_model(loans[i].part.spec_sge()).span().bytes().subset_of(h[k].mem);
                            assert(h2.contains_key(k) && h2[k] == h[k]);
                        }
                    } else {
                        assert(h2.contains_key(l) && h2[l].mem == bytes);
                        assert(bytes.subset_of(h2[l].mem));
                    }
                }
                assert forall|k: LeaseId| #[trigger] h2.contains_key(k) && a1.leases[k].unique implies exists|i: int| 0 <= i < n
                    && (#[trigger] loans[i]).part.spec_sge().length > 0
                    && a1.leases[k].mem == sge_model(loans[i].part.spec_sge()).span().bytes() by {
                    if k == l {
                        assert(0 <= n - 1 < n && loans[n - 1] == loan);
                    } else {
                        assert(h.contains_key(k));
                        let i = choose|i: int| 0 <= i < n - 1
                            && (#[trigger] loans[i]).part.spec_sge().length > 0
                            && a1.leases[k].mem == sge_model(loans[i].part.spec_sge()).span().bytes();
                        assert(0 <= i < n);
                    }
                }
                (a1, h2)
            } else {
                lemma_share(v, a1, lent_qp(), reqs, lent_holds(), l, bytes);
                let a2 = share_state(a1, l, bytes);
                let x = a1.next_id;
                assert(dir is Out);
                loan.part.lemma_byte_backing();
                assert(loan.part.byte_domain().to_iset() =~= bytes);
                let whole = loan.part.resource().byte_domain().to_iset();
                assert forall|y: int| bytes.contains(y) implies whole.contains(y) by {
                    assert(loan.part.byte_domain().contains(y));
                }
                let h2 = h.insert(x, Hold { dir, mem: bytes, kept: whole, shared: true });
                assert(hold_of(h2[x], loan.part));
                assert forall|k: LeaseId| #[trigger] h2.contains_key(k) implies exists|i: int| 0 <= i < n
                    && (#[trigger] loans[i]).part.spec_sge().length > 0 && hold_of(h2[k], loans[i].part) by {
                    if k == x {
                        assert(0 <= n - 1 < n && loans[n - 1] == loan);
                    } else {
                        assert(h.contains_key(k) && h2[k] == h[k]);
                        let i = choose|i: int| 0 <= i < n - 1
                            && (#[trigger] loans[i]).part.spec_sge().length > 0 && hold_of(h[k], loans[i].part);
                        assert(0 <= i < n);
                    }
                }
                assert forall|k: LeaseId| #[trigger] h2.contains_key(k) && a.leases.contains_key(k) implies a.leases[k].unique by {
                    if k == x {
                        assert(a.next_id <= a1.next_id);
                    }
                }
                lemma_abs_reaches_trans(a, a1, a2);
                assert forall|k: LeaseId| #[trigger] h.contains_key(k) implies k != x && a2.leases[k] == a1.leases[k] by {
                    assert(free_lease(v, a1, lent_holds(), k));
                    assert(a1.leases.contains_key(k));
                }
                assert forall|i: int| 0 <= i < n implies #[trigger] loan_covered(h2, loans[i]) by {
                    if i < n - 1 {
                        assert(loan_covered(h, loans[i]));
                        if loans[i].part.spec_sge().length > 0 {
                            let k = choose|k: LeaseId| #[trigger] h.contains_key(k) && loans[i].access == Some(h[k].dir)
                                && sge_model(loans[i].part.spec_sge()).span().bytes().subset_of(h[k].mem);
                            assert(h2.contains_key(k) && h2[k] == h[k]);
                        }
                    } else {
                        assert(h2.contains_key(x) && h2[x].mem == bytes);
                        assert(bytes.subset_of(h2[x].mem));
                    }
                }
                assert forall|k: LeaseId| #[trigger] h2.contains_key(k) && a2.leases[k].unique implies exists|i: int| 0 <= i < n
                    && (#[trigger] loans[i]).part.spec_sge().length > 0
                    && a2.leases[k].mem == sge_model(loans[i].part.spec_sge()).span().bytes() by {
                    assert(k != x);
                    assert(h.contains_key(k));
                    assert(a1.leases[k].unique);
                    let i = choose|i: int| 0 <= i < n - 1
                        && (#[trigger] loans[i]).part.spec_sge().length > 0
                        && a1.leases[k].mem == sge_model(loans[i].part.spec_sge()).span().bytes();
                    assert(0 <= i < n);
                }
                assert forall|k: LeaseId| #[trigger] h2.contains_key(k) implies free_lease(v, a2, lent_holds(), k) by {
                    if k != x {
                        assert(free_lease(v, a1, lent_holds(), k));
                    }
                }
                assert forall|k: LeaseId| #[trigger] free_lease(v, a, lent_holds(), k)
                    implies free_lease(v, a2, lent_holds(), k) && a2.leases[k] == a.leases[k] by {
                    assert(free_lease(v, a1, lent_holds(), k));
                }
                (a2, h2)
            }
        }
    }
}

/// The leases of a request about to be written, for its inputs: each comes to A from what its linear
/// permission says of it, then a unique part's own lease is the request's, and a new share of its
/// root a shared one's. The request is not there yet: the table stays linked.
pub(super) proof fn lemma_name_request<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, leases: Leases<N>) -> (r: (Refinement, Holds))
    requires
        linked(qps, m, g),
        table_kept(qps),
        leases.inv(),
        forall|i: int| 0 <= i < leases.loans().len() ==> (#[trigger] leases.loans()[i]).access is Some,
        forall|i: int| 0 <= i < leases.loans().len() ==> #[trigger] table_apart(qps, leases.loans()[i].part.resource()),
    ensures
        linked(qps, m, r.0),
        r.0 == (Refinement { abs: r.0.abs, ..g }),
        r.0.extends(g),
        may_post_holds(r.0.abs, r.1),
        forall|l: LeaseId| #[trigger] r.1.contains_key(l) ==> free_lease(table_view(qps, m), r.0.abs, lent_holds(), l),
        roots_outside(table_view(qps, m), r.0.abs, lent_holds(), r.1),
        holds_cover(r.1, leases.loans()),
        holds_kept(r.1, leases),
{
    let v = table_view(qps, m);
    let loans = leases.loans();
    leases.lemma_loans();
    leases.lemma_disjoint();
    leases.lemma_apart();
    assert forall|i: int, k: int, l: LeaseId| 0 <= i < loans.len() && #[trigger] leases_of(v, lent_holds(), k).contains_key(l)
        implies hold_apart(leases_of(v, lent_holds(), k)[l], (#[trigger] loans[i]).part.resource()) by {
        assert(table_apart(qps, loans[i].part.resource()));
        lemma_table_hold_apart(qps, m, loans[i].part.resource(), k, l);
    }
    let a1 = lemma_adopt_loans(v, g.abs, g.reqs, loans, loans.len() as int);
    assert forall|i: int, j: int| #![trigger loans[i], loans[j]] 0 <= i < loans.len() && 0 <= j < loans.len() && i != j
        && (loans[i].part.spec_writable() || loans[j].part.spec_writable())
        implies sge_model(loans[i].part.spec_sge()).span().bytes().disjoint(sge_model(loans[j].part.spec_sge()).span().bytes()) by {
        loans[i].part.lemma_byte_backing();
        loans[j].part.lemma_byte_backing();
        let (x, y) = (loans[i].part.byte_domain(), loans[j].part.byte_domain());
        assert(x.disjoint(y));
        lemma_disjoint_iset(x, y);
    }
    assert forall|i: int| 0 <= i < loans.len() && (#[trigger] loans[i]).part.spec_sge().length > 0
        implies lent_ok(v, a1, loans[i].part, loans[i].access->0) by {
        lemma_adopted_lent(v, a1, loans[i].part, loans[i].access->0);
    }
    let (a2, h) = lemma_name_loans(v, a1, g.reqs, loans, loans.len() as int);
    lemma_abs_reaches_trans(g.abs, a1, a2);
    let g2 = Refinement { abs: a2, ..g };
    lemma_ranges_kept(g, a2, m);
    assert forall|l: LeaseId| #[trigger] h.contains_key(l) implies {
        &&& h[l].mem.subset_of(h[l].kept)
        &&& exists|j: int| 0 <= j < leases.view().len()
            && (#[trigger] leases.view()[j]).byte_domain().to_iset() == h[l].kept
            && leases.view()[j].unique() == !h[l].shared
    } by {
        let i = choose|i: int| 0 <= i < loans.len()
            && (#[trigger] loans[i]).part.spec_sge().length > 0 && hold_of(h[l], loans[i].part);
        let part = loans[i].part;
        let j = choose|j: int| 0 <= j < leases.view().len() && (#[trigger] leases.view()[j]).same_backing(part.resource());
        leases.view()[j].lemma_backing_domain(part.resource());
        part.lemma_byte_backing();
    }
    // A root is a lease of the program's and shared, which none of the new leases was.
    assert forall|bytes: ISet<int>| #[trigger] has_root(v, a1, lent_holds(), bytes) implies root_outside(v, a2, lent_holds(), h, bytes) by {
        let w = choose|w: LeaseId| #[trigger] free_lease(v, a1, lent_holds(), w) && !a1.leases[w].unique && a1.leases[w].mem == bytes;
        assert(free_lease(v, a2, lent_holds(), w));
        assert(!h.contains_key(w));
    }
    assert forall|s: int, l: LeaseId| #[trigger] leases_of(v, lent_holds(), s).contains_key(l) && leases_of(v, lent_holds(), s)[l].shared
        implies root_outside(v, a2, lent_holds(), h, leases_of(v, lent_holds(), s)[l].kept) by {
        assert(has_root(v, a1, lent_holds(), leases_of(v, lent_holds(), s)[l].kept));
    }
    assert forall|l: LeaseId| #[trigger] h.contains_key(l) && h[l].shared implies root_outside(v, a2, lent_holds(), h, h[l].kept) by {
        let i = choose|i: int| 0 <= i < loans.len()
            && (#[trigger] loans[i]).part.spec_sge().length > 0 && hold_of(h[l], loans[i].part);
        assert(adopted(v, a1, loans[i].part.resource()));
        assert(!loans[i].part.resource().unique()) by {
            loans[i].part.lemma_byte_backing();
        }
        assert(has_root(v, a1, lent_holds(), h[l].kept));
    }
    (g2, h)
}

/// What A sees of request `s` of ring `queue` of entry `i` is what that ring says.
proof fn lemma_table_at<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State, i: int, queue: Queue, s: int)
    requires
        has_entry(qps, i),
        0 <= s < key_span(),
    ensures ({
        let (e, k) = (qps[i]->0, key(i, queue, s));
        &&& table_req(qps, m, k) == seen(ring_of(e, queue), m, e.id(), queue, s)
        &&& table_staged(qps, k) == staged_seen(ring_of(e, queue), e.id(), s)
        &&& table_refused(qps, k) == refused_seen(ring_of(e, queue), e.id(), s)
    }),
{
    lemma_key_fields(i, queue, s);
}

/// One ring of the table changed and A moved to `a2`, with no new request or outcome: `w` is what A
/// sees now, placed there, and the requests A sees keep their WQEs.
proof fn lemma_linked_ring<C, N: Device>(w: Seen<Lent>, pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, a2: Abs::State, i: int, queue: Queue)
    requires
        linked(pre, m, g),
        same_but_ring(pre, post, i, queue),
        placed(w, a2, lent_qp(), g.reqs, lent_holds()),
        kept_requests(g.abs, a2),
        forall|k: int| !(key_entry(k) == i && key_queue(k) == queue) ==> {
            &&& (w.req)(k) == table_req(pre, m, k)
            &&& (w.staged)(k) == table_staged(pre, k)
            &&& (w.refused)(k) == table_refused(pre, k)
        },
        forall|s: int| 0 <= s < key_span() ==> {
            &&& (w.req)(#[trigger] key(i, queue, s)) == seen(ring_of(post[i]->0, queue), m, post[i]->0.id(), queue, s)
            &&& (w.staged)(key(i, queue, s)) == staged_seen(ring_of(post[i]->0, queue), post[i]->0.id(), s)
            &&& (w.refused)(key(i, queue, s)) == refused_seen(ring_of(post[i]->0, queue), post[i]->0.id(), s)
        },
        forall|s: int| 0 <= s < key_span() && #[trigger] seen(ring_of(post[i]->0, queue), m, post[i]->0.id(), queue, s) is Some ==> {
            let (r0, r1) = (ring_of(pre[i]->0, queue), ring_of(post[i]->0, queue));
            &&& seen(r0, m, pre[i]->0.id(), queue, s) is Some
            &&& r1.meta(s)->0.first == r0.meta(s)->0.first
            &&& r1.meta(s)->0.end == r0.meta(s)->0.end
        },
    ensures
        linked(post, m, Refinement { abs: a2, ..g }),
        same(w, table_view(post, m)),
{
    lemma_same_by_ring(w, pre, post, m, i, queue);
    lemma_same(w, table_view(post, m), a2, lent_qp(), g.reqs, lent_holds());
    lemma_ranges_kept(g, a2, m);
    lemma_rest_by_ring(pre, post, m, g, Refinement { abs: a2, ..g }, i, queue);
}

/// [`lemma_linked_ring`], request `seq` the only one of the ring that changed.
proof fn lemma_linked_one_key<C, N: Device>(w: Seen<Lent>, pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, a2: Abs::State, i: int, queue: Queue, seq: int)
    requires
        linked(pre, m, g),
        same_but_ring(pre, post, i, queue),
        0 <= seq < key_span(),
        placed(w, a2, lent_qp(), g.reqs, lent_holds()),
        kept_requests(g.abs, a2),
        forall|t: int| t != key(i, queue, seq) ==> {
            &&& (w.req)(t) == table_req(pre, m, t)
            &&& (w.staged)(t) == table_staged(pre, t)
            &&& (w.refused)(t) == table_refused(pre, t)
        },
        forall|u: int| u != seq ==> {
            &&& #[trigger] ring_of(post[i]->0, queue).req(u) == ring_of(pre[i]->0, queue).req(u)
            &&& ring_of(post[i]->0, queue).staged(u) == ring_of(pre[i]->0, queue).staged(u)
            &&& ring_of(post[i]->0, queue).refused(u) == ring_of(pre[i]->0, queue).refused(u)
            &&& ring_of(post[i]->0, queue).meta(u) == ring_of(pre[i]->0, queue).meta(u)
        },
        ({
            let r1 = ring_of(post[i]->0, queue);
            let k = key(i, queue, seq);
            let q = pre[i]->0.id();
            &&& (w.req)(k) == seen(r1, m, q, queue, seq)
            &&& (w.staged)(k) == staged_seen(r1, q, seq)
            &&& (w.refused)(k) == refused_seen(r1, q, seq)
            &&& seen(r1, m, q, queue, seq) is Some ==> {
                let r0 = ring_of(pre[i]->0, queue);
                &&& seen(r0, m, q, queue, seq) is Some
                &&& r1.meta(seq)->0.first == r0.meta(seq)->0.first
                &&& r1.meta(seq)->0.end == r0.meta(seq)->0.end
            }
        }),
    ensures
        linked(post, m, Refinement { abs: a2, ..g }),
{
    let (r0, r1) = (ring_of(pre[i]->0, queue), ring_of(post[i]->0, queue));
    let q = pre[i]->0.id();
    assert forall|t: int| !(key_entry(t) == i && key_queue(t) == queue) implies {
        &&& (w.req)(t) == table_req(pre, m, t)
        &&& (w.staged)(t) == table_staged(pre, t)
        &&& (w.refused)(t) == table_refused(pre, t)
    } by {
        lemma_key_fields(i, queue, seq);
        assert(t != key(i, queue, seq));
    }
    assert forall|u: int| 0 <= u < key_span() implies {
        &&& (w.req)(#[trigger] key(i, queue, u)) == seen(r1, m, q, queue, u)
        &&& (w.staged)(key(i, queue, u)) == staged_seen(r1, q, u)
        &&& (w.refused)(key(i, queue, u)) == refused_seen(r1, q, u)
    } by {
        lemma_key_fields(i, queue, u);
        lemma_table_at(pre, m, i, queue, u);
        if u != seq {
            assert(r1.req(u) == r0.req(u));
            assert(key(i, queue, u) != key(i, queue, seq)) by {
                lemma_key_fields(i, queue, seq);
            }
        }
    }
    assert forall|u: int| 0 <= u < key_span() && #[trigger] seen(r1, m, q, queue, u) is Some implies {
        &&& seen(r0, m, q, queue, u) is Some
        &&& r1.meta(u)->0.first == r0.meta(u)->0.first
        &&& r1.meta(u)->0.end == r0.meta(u)->0.end
    } by {
        if u != seq {
            assert(r1.req(u) == r0.req(u));
        }
    }
    lemma_linked_ring(w, pre, post, m, g, a2, i, queue);
}

/// A request is staged on ring `queue` of entry `i`, holding leases the program holds free: no step
/// of A.
pub(super) proof fn lemma_linked_staged<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, i: int, queue: Queue, wqes: u64, view: ReqView<Payload<C, N>>, outcome: CompletionResult)
    requires
        linked(pre, m, g),
        same_but_ring(pre, post, i, queue),
        ring_of(pre[i]->0, queue).inv(),
        ring_of(post[i]->0, queue).just_staged(&ring_of(pre[i]->0, queue), wqes, view, outcome),
        view == ReqView::new(view.detached, view.leases),
        may_post_holds(g.abs, view.leases.holds@),
        forall|l: LeaseId| #[trigger] view.leases.holds@.contains_key(l)
            ==> free_lease(table_view(pre, m), g.abs, lent_holds(), l),
        roots_outside(table_view(pre, m), g.abs, lent_holds(), view.leases.holds@),
    ensures
        linked(post, m, g),
{
    let (r0, r1) = (ring_of(pre[i]->0, queue), ring_of(post[i]->0, queue));
    let q = pre[i]->0.id();
    let s = r0.spec_posted() as int;
    let k = key(i, queue, s);
    let v = table_view(pre, m);
    let lent = Lent { qp: q, holds: view.leases.holds@ };
    lemma_key_fields(i, queue, s);
    r0.lemma_view();
    assert(table_req(pre, m, k) is None);
    assert(table_staged(pre, k) is None);
    assert(table_refused(pre, k) is None);
    assert(!crate::refine::holds(v, k));
    assert(g.abs.qps.contains(pre[i]->0.id()));
    assert forall|t: int| #[trigger] leases_of(v, lent_holds(), t).dom().disjoint((lent_holds())(k, lent).dom()) by {
        assert forall|l: LeaseId| !(leases_of(v, lent_holds(), t).dom().contains(l) && (lent_holds())(k, lent).dom().contains(l)) by {
            if (lent_holds())(k, lent).dom().contains(l) {
                assert(view.leases.holds@.contains_key(l));
                assert(free_lease(v, g.abs, lent_holds(), l));
                assert(!leases_of(v, lent_holds(), t).contains_key(l));
            }
        }
    }
    assert(may_post_holds(g.abs, (lent_holds())(k, lent)));
    assert((lent_holds())(k, lent) == view.leases.holds@);
    lemma_end(v, g.abs, lent_qp(), g.reqs, lent_holds(), k, view.detached, lent);
    let w = staged_with(v, k, view.detached, lent);
    assert forall|t: int| !(key_entry(t) == i && key_queue(t) == queue) implies {
        &&& (w.req)(t) == table_req(pre, m, t)
        &&& (w.staged)(t) == table_staged(pre, t)
        &&& (w.refused)(t) == table_refused(pre, t)
    } by {
    }
    assert forall|u: int| 0 <= u < key_span() implies {
        &&& (w.req)(#[trigger] key(i, queue, u)) == seen(r1, m, q, queue, u)
        &&& (w.staged)(key(i, queue, u)) == staged_seen(r1, q, u)
        &&& (w.refused)(key(i, queue, u)) == refused_seen(r1, q, u)
    } by {
        lemma_key_fields(i, queue, u);
        if r0.req(u) is Some {
            assert(r1.meta(u) == r0.meta(u));
        }
    }
    lemma_same_by_ring(w, pre, post, m, i, queue);
    lemma_same(w, table_view(post, m), g.abs, lent_qp(), g.reqs, lent_holds());
    assert forall|u: int| 0 <= u < key_span() && #[trigger] seen(r1, m, q, queue, u) is Some implies {
        &&& seen(r0, m, q, queue, u) is Some
        &&& r1.meta(u)->0.first == r0.meta(u)->0.first
        &&& r1.meta(u)->0.end == r0.meta(u)->0.end
    } by {
        assert(r1.req(u) == r0.req(u));
        assert(r1.meta(u) == r0.meta(u));
    }
    lemma_rest_by_ring(pre, post, m, g, g, i, queue);
}

/// What a request A is about to have says of M: its WQEs there, and its queue of its own.
pub(super) open spec fn new_range_ok(n: Nic::State, lent: Lent, range: WqeRange) -> bool {
    &&& range.wf()
    &&& range.queue is Recv ==> range.end == range.first + 1
    &&& range_known(n, range)
    &&& safeverbs_spec::refinement::posted(n, range.last())
    &&& range.queue is Send ==> range.errors_stick(n)
    &&& range.qp == lent.qp
}

/// Posting staged requests `[from, to)`: A's `post_request` of each, each a new request with its WQEs.
proof fn lemma_post_range(v: Seen<Lent>, g: Refinement, n: Nic::State, from: int, to: int, rangeof: spec_fn(int) -> WqeRange) -> (g2: Refinement)
    requires
        placed(v, g.abs, lent_qp(), g.reqs, lent_holds()),
        ranges_ok(g, n),
        from <= to,
        forall|k: int| from <= k < to ==> (#[trigger] (v.staged)(k)) is Some
            && new_range_ok(n, (v.staged)(k)->0.leases, rangeof(k)),
        forall|r: ReqId, k: int| #[trigger] g.ranges.contains_key(r) && from <= k < to ==> g.ranges[r].disjoint(#[trigger] rangeof(k)),
        forall|k1: int, k2: int| #![trigger rangeof(k1), rangeof(k2)] from <= k1 < k2 < to ==> rangeof(k1).disjoint(rangeof(k2)),
    ensures
        placed(crate::refine::posted_range(v, from, to), g2.abs, lent_qp(), g2.reqs, lent_holds()),
        ranges_ok(g2, n),
        g2.extends(g),
        g2.wrs == g.wrs,
        g2.abs.qps == g.abs.qps,
        kept_free(v, g.abs, crate::refine::posted_range(v, from, to), g2.abs, lent_holds()),
        forall|k: int| from <= k < to ==> g2.ranges.contains_key(#[trigger] g2.reqs[k]) && g2.ranges[g2.reqs[k]] == rangeof(k),
        forall|k: int| !(from <= k < to) ==> #[trigger] g2.reqs[k] == g.reqs[k],
        forall|r: ReqId| #[trigger] g2.ranges.contains_key(r) ==> (g.ranges.contains_key(r) && g2.ranges[r] == g.ranges[r])
            || exists|k: int| from <= k < to && #[trigger] rangeof(k) == g2.ranges[r],
    decreases to - from,
{
    if to == from {
        lemma_same(v, crate::refine::posted_range(v, from, to), g.abs, lent_qp(), g.reqs, lent_holds());
        lemma_abs_reaches_refl(g.abs);
        g
    } else {
        let s = to - 1;
        let g1 = lemma_post_range(v, g, n, from, s, rangeof);
        let v1 = crate::refine::posted_range(v, from, s);
        assert((v1.staged)(s) == (v.staged)(s));
        let a2 = lemma_post_one(v1, g1.abs, lent_qp(), g1.reqs, lent_holds(), s);
        let r = g1.abs.next_id;
        let g2 = Refinement {
            abs: a2,
            reqs: g1.reqs.insert(s, r),
            ranges: g1.ranges.insert(r, rangeof(s)),
            wrs: g1.wrs,
        };
        let w = crate::refine::posted_range(v, from, to);
        lemma_same(crate::refine::posted(v1, s), w, a2, lent_qp(), g2.reqs, lent_holds());
        lemma_kept_free_trans(v, g.abs, v1, g1.abs, crate::refine::posted(v1, s), a2, lent_holds());
        lemma_kept_free_trans(v, g.abs, crate::refine::posted(v1, s), a2, w, a2, lent_holds());
        lemma_abs_reaches_trans(g.abs, g1.abs, a2);
        assert(!g1.ranges.contains_key(r));
        assert(new_range_ok(n, (v.staged)(s)->0.leases, rangeof(s)));
        // The ranges of `ranges_ok`.
        assert forall|t: ReqId| #[trigger] a2.requests.contains_key(t)
            implies g2.ranges.contains_key(t) && g2.ranges[t].qp == a2.requests[t].qp by {
            if t != r {
                assert(g1.abs.requests.contains_key(t));
            }
        }
        assert forall|t: ReqId| #[trigger] g2.ranges.contains_key(t) implies {
            &&& t < a2.next_id
            &&& g2.ranges[t].wf()
            &&& g2.ranges[t].queue is Recv ==> g2.ranges[t].end == g2.ranges[t].first + 1
            &&& range_known(n, g2.ranges[t])
        } by {
            if t != r {
                assert(g1.ranges.contains_key(t));
            }
        }
        assert forall|t1: ReqId, t2: ReqId| #![trigger g2.ranges[t1], g2.ranges[t2]]
            g2.ranges.contains_key(t1) && g2.ranges.contains_key(t2) && t1 != t2
            implies g2.ranges[t1].disjoint(g2.ranges[t2]) by {
            if t1 != r && t2 != r {
                assert(g1.ranges[t1].disjoint(g1.ranges[t2]));
            } else {
                let t = if t1 == r { t2 } else { t1 };
                assert(g1.ranges.contains_key(t));
                if !(g.ranges.contains_key(t) && g1.ranges[t] == g.ranges[t]) {
                    let k = choose|k: int| from <= k < s && #[trigger] rangeof(k) == g1.ranges[t];
                    assert(rangeof(k).disjoint(rangeof(s)));
                } else {
                    assert(g.ranges[t].disjoint(rangeof(s)));
                }
            }
        }
        assert forall|t: ReqId| #[trigger] a2.in_flight(t) implies {
            &&& safeverbs_spec::refinement::posted(n, g2.ranges[t].last())
            &&& g2.ranges[t].queue is Send ==> g2.ranges[t].errors_stick(n)
        } by {
            if t != r {
                assert(g1.abs.in_flight(t));
            }
        }
        assert forall|t: ReqId| #[trigger] a2.outcomes.contains_key(t) implies {
            &&& g2.ranges[t].all_retired(n.retired)
            &&& g2.ranges[t].aggregate(n.retired) == a2.outcomes[t]
            &&& g2.ranges[t].no_success_after_error(n.retired)
        } by {
            assert(g1.abs.outcomes.contains_key(t));
            assert(g1.ranges.contains_key(t));
        }
        assert forall|k: int| from <= k < to implies g2.ranges.contains_key(#[trigger] g2.reqs[k]) && g2.ranges[g2.reqs[k]] == rangeof(k) by {
            if k != s {
                assert(g1.ranges.contains_key(g1.reqs[k]));
                assert(g1.reqs[k] != r);
            }
        }
        assert forall|t: ReqId| #[trigger] g2.ranges.contains_key(t) implies (g.ranges.contains_key(t) && g2.ranges[t] == g.ranges[t])
            || exists|k: int| from <= k < to && #[trigger] rangeof(k) == g2.ranges[t] by {
            if t == r {
                assert(from <= s < to && rangeof(s) == g2.ranges[t]);
            } else {
                assert(g1.ranges.contains_key(t));
                if !(g.ranges.contains_key(t) && g1.ranges[t] == g.ranges[t]) {
                    let k = choose|k: int| from <= k < s && #[trigger] rangeof(k) == g1.ranges[t];
                    assert(from <= k < to && rangeof(k) == g2.ranges[t]);
                }
            }
        }
        assert forall|k: int| !(from <= k < to) implies #[trigger] g2.reqs[k] == g.reqs[k] by {
            assert(k != s);
            assert(g1.reqs[k] == g.reqs[k]);
        }
        assert(g.ranges.submap_of(g2.ranges)) by {
            assert forall|t: ReqId| #[trigger] g.ranges.contains_key(t) implies g2.ranges.contains_key(t) && g2.ranges[t] == g.ranges[t] by {
                assert(g.ranges.submap_of(g1.ranges));
                assert(g1.ranges.contains_key(t));
                assert(t != r);
            }
        }
        g2
    }
}

/// What M's queue holds at the end, with no outcome yet: what its doorbell just posted, past what
/// the ring had committed.
pub(super) open spec fn just_posted(n: Nic::State, q: QpId, queue: Queue, from: nat, to: nat) -> bool {
    &&& n.qps.contains_key(q)
    &&& head_of(n.qps[q], queue) <= from
    &&& forall|s: nat| from <= s < to ==> #[trigger] safeverbs_spec::refinement::posted(n, WqeRef { qp: q, queue, seq: s })
    &&& queue is Send ==> forall|s: nat| from <= s < to ==> (#[trigger] n.qps[q].sq[s - n.qps[q].sq_head]).outcome is None
}

/// The range of a staged request is one A may have, M having just posted its WQEs.
proof fn lemma_new_range<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, n: Nic::State, i: int, queue: Queue, s: int)
    requires
        history_ok(n),
        has_entry(pre, i),
        ring_of(pre[i]->0, queue).inv(),
        ring_of(pre[i]->0, queue).staged(s) is Some,
        just_posted(n, pre[i]->0.id(), queue, ring_of(pre[i]->0, queue).spec_wqe_committed() as nat, ring_of(pre[i]->0, queue).spec_wqe_posted() as nat),
        queue is Recv ==> ring_of(pre[i]->0, queue).meta(s)->0.end as int == ring_of(pre[i]->0, queue).meta(s)->0.first as int + 1,
    ensures ({
        let r0 = ring_of(pre[i]->0, queue);
        new_range_ok(n, Lent { qp: pre[i]->0.id(), holds: r0.staged(s)->0.leases.holds@ }, range_of(pre[i]->0.id(), queue, r0.meta(s)->0))
    }),
{
    let r0 = ring_of(pre[i]->0, queue);
    let q = pre[i]->0.id();
    r0.lemma_meta(s);
    let range = range_of(q, queue, r0.meta(s)->0);
    assert(r0.spec_wqe_committed() <= range.first < range.end <= r0.spec_wqe_posted());
    assert forall|x: nat| range.first <= x < range.end implies known(n, #[trigger] range.at(x)) by {
        assert(safeverbs_spec::refinement::posted(n, WqeRef { qp: q, queue, seq: x }));
    }
    assert(safeverbs_spec::refinement::posted(n, WqeRef { qp: q, queue, seq: (range.end - 1) as nat }));
    if queue is Send {
        assert forall|j: nat| range.first <= j < range.end && #[trigger] range.failed(n, j) implies {
            &&& n.qps[range.qp].state is Err
            &&& forall|l: nat| j < l < range.end && (#[trigger] range.outcome(n, l)) is Some
                ==> range.failed(n, l)
        } by {
            let w = range.at(j);
            assert(safeverbs_spec::refinement::posted(n, WqeRef { qp: q, queue, seq: j }));
            assert(w == WqeRef { qp: q, queue, seq: j });
            if n.retired.contains_key(w) {
                assert(w.seq < head_of(n.qps[w.qp], w.queue));
            }
            assert((n.qps[q].sq[j - n.qps[q].sq_head]).outcome is None);
        }
    }
}

/// The ring committed its staged requests, M holding their WQEs: A's `post_request` of each.
pub(super) proof fn lemma_linked_committed<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, n: Nic::State, g: Refinement, i: int, queue: Queue) -> (g2: Refinement)
    requires
        linked(pre, n, g),
        same_but_ring(pre, post, i, queue),
        ring_of(pre[i]->0, queue).inv(),
        ring_of(post[i]->0, queue).just_committed(&ring_of(pre[i]->0, queue)),
        just_posted(n, pre[i]->0.id(), queue, ring_of(pre[i]->0, queue).spec_wqe_committed() as nat, ring_of(pre[i]->0, queue).spec_wqe_posted() as nat),
        forall|r: ReqId| #[trigger] g.ranges.contains_key(r) && g.ranges[r].qp == pre[i]->0.id() && g.ranges[r].queue == queue
            ==> g.ranges[r].end <= ring_of(pre[i]->0, queue).spec_wqe_committed(),
        queue is Recv ==> forall|s: int| #[trigger] ring_of(pre[i]->0, queue).staged(s) is Some
            ==> ring_of(pre[i]->0, queue).meta(s)->0.end as int == ring_of(pre[i]->0, queue).meta(s)->0.first as int + 1,
    ensures
        linked(post, n, g2),
        g2.extends(g),
        g2.wrs == g.wrs,
        kept_free(table_view(pre, n), g.abs, table_view(post, n), g2.abs, lent_holds()),
{
    let (r0, r1) = (ring_of(pre[i]->0, queue), ring_of(post[i]->0, queue));
    let q = pre[i]->0.id();
    let v = table_view(pre, n);
    let base = key(i, queue, 0);
    let from = base + r0.spec_committed();
    let to = base + r0.spec_posted();
    let rangeof = |k: int| key_range(pre, k);
    r0.lemma_view();
    r0.lemma_wqes();
    assert forall|k: int| from <= k < to implies (#[trigger] (v.staged)(k)) is Some
        && new_range_ok(n, (v.staged)(k)->0.leases, rangeof(k)) by {
        let s = k - base;
        lemma_key_fields(i, queue, s);
        assert(key(i, queue, s) == k);
        assert(r0.staged(s) is Some);
        lemma_new_range(pre, n, i, queue, s);
    }
    assert forall|r: ReqId, k: int| #[trigger] g.ranges.contains_key(r) && from <= k < to
        implies g.ranges[r].disjoint(#[trigger] rangeof(k)) by {
        let s = k - base;
        lemma_key_fields(i, queue, s);
        assert(key(i, queue, s) == k);
        r0.lemma_meta(s);
        assert(r0.spec_wqe_committed() <= r0.meta(s)->0.first);
    }
    assert forall|k1: int, k2: int| #![trigger rangeof(k1), rangeof(k2)] from <= k1 < k2 < to
        implies rangeof(k1).disjoint(rangeof(k2)) by {
        let (s1, s2) = (k1 - base, k2 - base);
        lemma_key_fields(i, queue, s1);
        lemma_key_fields(i, queue, s2);
        assert(key(i, queue, s1) == k1 && key(i, queue, s2) == k2);
        assert(r0.staged(s1) is Some && r0.staged(s2) is Some);
        r0.lemma_meta(s1);
        r0.lemma_meta(s2);
        r0.lemma_meta_ordered(s1, s2);
    }
    let g2 = lemma_post_range(v, g, n, from, to, rangeof);
    let w = crate::refine::posted_range(v, from, to);
    assert forall|t: int| !(key_entry(t) == i && key_queue(t) == queue) implies {
        &&& (w.req)(t) == table_req(pre, n, t)
        &&& (w.staged)(t) == table_staged(pre, t)
        &&& (w.refused)(t) == table_refused(pre, t)
    } by {
        lemma_key_interval(t, i, queue);
    }
    assert forall|u: int| 0 <= u < key_span() implies {
        &&& (w.req)(#[trigger] key(i, queue, u)) == seen(r1, n, q, queue, u)
        &&& (w.staged)(key(i, queue, u)) == staged_seen(r1, q, u)
        &&& (w.refused)(key(i, queue, u)) == refused_seen(r1, q, u)
    } by {
        lemma_committed_key(pre, post, n, i, queue, u);
    }
    lemma_same_by_ring(w, pre, post, n, i, queue);
    lemma_same(w, table_view(post, n), g2.abs, lent_qp(), g2.reqs, lent_holds());
    lemma_kept_free_trans(v, g.abs, w, g2.abs, table_view(post, n), g2.abs, lent_holds());
    // Each request A has now has the WQEs its ring says.
    assert forall|t: int| #[trigger] table_req(post, n, t) is Some implies {
        &&& g2.ranges.contains_key(g2.reqs[t])
        &&& g2.ranges[g2.reqs[t]] == key_range(post, t)
    } by {
        lemma_key_interval(t, i, queue);
        lemma_key_made(t);
        if from <= t < to {
            let s = t - base;
            lemma_key_fields(i, queue, s);
            assert(key(i, queue, s) == t);
            assert(g2.ranges.contains_key(g2.reqs[t]));
            assert(r1.meta(s) == r0.meta(s));
            assert(rangeof(t) == key_range(pre, t));
            assert(key_range(pre, t) == key_range(post, t));
        } else if key_entry(t) == i && key_queue(t) == queue {
            let s = key_seq(t);
            assert(key(i, queue, s) == t);
            lemma_committed_key(pre, post, n, i, queue, s);
            assert(table_req(pre, n, t) is Some);
            assert(g2.reqs[t] == g.reqs[t]);
            assert(g.ranges.contains_key(g.reqs[t]));
            assert(g2.ranges.contains_key(g.reqs[t]));
            assert(g2.ranges[g.reqs[t]] == g.ranges[g.reqs[t]]);
            assert(r1.meta(s) == r0.meta(s));
            assert(key_range(pre, t) == key_range(post, t));
        } else {
            lemma_other_keys(pre, post, n, i, queue, t);
            assert(g2.reqs[t] == g.reqs[t]);
            assert(g.ranges.contains_key(g.reqs[t]));
            assert(g2.ranges.contains_key(g.reqs[t]));
            assert(g2.ranges[g.reqs[t]] == g.ranges[g.reqs[t]]);
        }
    }
    assert forall|j: int| has_entry(post, j) implies g2.abs.qps.contains(#[trigger] post[j]->0.id()) by {
        if j == i {
            assert(g.abs.qps.contains(pre[i]->0.id()));
        } else {
            assert(post[j] == pre[j]);
            assert(has_entry(pre, j));
            assert(g.abs.qps.contains(pre[j]->0.id()));
        }
    }
    assert forall|p: QpId| #[trigger] g2.abs.qps.contains(p) implies exists|j: int| has_entry(post, j) && #[trigger] post[j]->0.id() == p by {
        let j = choose|j: int| has_entry(pre, j) && #[trigger] pre[j]->0.id() == p;
        if j != i {
            assert(post[j] == pre[j]);
        }
        assert(has_entry(post, j) && post[j]->0.id() == p);
    }
    g2
}

/// What a commit does to request `u` of its ring, as A sees it.
proof fn lemma_committed_key<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, n: Nic::State, i: int, queue: Queue, u: int)
    requires
        same_but_ring(pre, post, i, queue),
        ring_of(pre[i]->0, queue).inv(),
        ring_of(post[i]->0, queue).just_committed(&ring_of(pre[i]->0, queue)),
        just_posted(n, pre[i]->0.id(), queue, ring_of(pre[i]->0, queue).spec_wqe_committed() as nat, ring_of(pre[i]->0, queue).spec_wqe_posted() as nat),
        0 <= u < key_span(),
    ensures ({
        let (r0, r1) = (ring_of(pre[i]->0, queue), ring_of(post[i]->0, queue));
        let q = pre[i]->0.id();
        let base = key(i, queue, 0);
        let w = crate::refine::posted_range(table_view(pre, n), base + r0.spec_committed(), base + r0.spec_posted());
        &&& (w.req)(key(i, queue, u)) == seen(r1, n, q, queue, u)
        &&& (w.staged)(key(i, queue, u)) == staged_seen(r1, q, u)
        &&& (w.refused)(key(i, queue, u)) == refused_seen(r1, q, u)
    }),
{
    let (r0, r1) = (ring_of(pre[i]->0, queue), ring_of(post[i]->0, queue));
    let q = pre[i]->0.id();
    let k = key(i, queue, u);
    lemma_key_fields(i, queue, u);
    r0.lemma_view();
    r0.lemma_wqes();
    assert(r1.meta(u) == r0.meta(u));
    assert(table_req(pre, n, k) == seen(r0, n, q, queue, u));
    assert(table_staged(pre, k) == staged_seen(r0, q, u));
    assert(table_refused(pre, k) == refused_seen(r0, q, u));
    if r0.staged(u) is Some {
        r0.lemma_meta(u);
        assert(crate::refine::wf_at(r0.view(), u));
        assert((r0.view().staged)(u) == r0.staged(u));
        assert(r0.staged(u)->0.phase is InFlight);
        assert(!popped(n, range_of(q, queue, r0.meta(u)->0)));
    }
}

/// The doorbell of ring `queue` of entry `i` was refused: what was staged is refused, but `keep`,
/// which goes back to the program; no step of A but the `drop` of what comes to nothing.
pub(super) proof fn lemma_linked_refused<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, i: int, queue: Queue, keep: int) -> (g2: Refinement)
    requires
        linked(pre, m, g),
        same_but_ring(pre, post, i, queue),
        ring_of(pre[i]->0, queue).inv(),
        ring_of(post[i]->0, queue).just_aborted(&ring_of(pre[i]->0, queue), keep),
        0 <= keep < key_span(),
    ensures
        linked(post, m, g2),
        g2 == (Refinement { abs: g2.abs, ..g }),
        g2.extends(g),
        kept_free(table_view(pre, m), g.abs, table_view(post, m), g2.abs, lent_holds()),
{
    let (r0, r1) = (ring_of(pre[i]->0, queue), ring_of(post[i]->0, queue));
    let q = pre[i]->0.id();
    let v = table_view(pre, m);
    let base = key(i, queue, 0);
    let from = base + r0.spec_committed();
    let to = base + r0.spec_posted();
    r0.lemma_view();
    assert forall|k: int| from <= k < to implies #[trigger] (v.staged)(k) is Some by {
        let s = k - base;
        lemma_key_fields(i, queue, s);
        assert(key(i, queue, s) == k);
    }
    let a2 = lemma_abort(v, g.abs, lent_qp(), g.reqs, lent_holds(), from, to, key(i, queue, keep));
    let w = refused_range(v, from, to, key(i, queue, keep));
    assert forall|t: int| !(key_entry(t) == i && key_queue(t) == queue) implies {
        &&& (w.req)(t) == table_req(pre, m, t)
        &&& (w.staged)(t) == table_staged(pre, t)
        &&& (w.refused)(t) == table_refused(pre, t)
    } by {
        lemma_key_interval(t, i, queue);
    }
    assert forall|u: int| 0 <= u < key_span() implies {
        &&& (w.req)(#[trigger] key(i, queue, u)) == seen(r1, m, q, queue, u)
        &&& (w.staged)(key(i, queue, u)) == staged_seen(r1, q, u)
        &&& (w.refused)(key(i, queue, u)) == refused_seen(r1, q, u)
    } by {
        lemma_key_fields(i, queue, u);
        let k = key(i, queue, u);
        assert(table_staged(pre, k) == staged_seen(r0, q, u));
        assert(table_refused(pre, k) == refused_seen(r0, q, u));
        assert(table_req(pre, m, k) == seen(r0, m, q, queue, u));
        if r0.req(u) is Some {
            assert(crate::refine::wf_at(r0.view(), u));
            assert(r0.staged(u) is None);
            assert(r1.meta(u) == r0.meta(u));
        }
    }
    lemma_same_by_ring(w, pre, post, m, i, queue);
    lemma_same(w, table_view(post, m), a2, lent_qp(), g.reqs, lent_holds());
    lemma_kept_free_trans(v, g.abs, w, a2, table_view(post, m), a2, lent_holds());
    let g2 = Refinement { abs: a2, ..g };
    lemma_ranges_kept(g, a2, m);
    assert forall|u: int| 0 <= u < key_span() && #[trigger] seen(r1, m, q, queue, u) is Some implies {
        &&& seen(r0, m, q, queue, u) is Some
        &&& r1.meta(u)->0.first == r0.meta(u)->0.first
        &&& r1.meta(u)->0.end == r0.meta(u)->0.end
    } by {
        assert(crate::refine::wf_at(r0.view(), u));
        assert(r1.req(u) == r0.req(u));
        assert(r1.meta(u) == r0.meta(u));
    }
    lemma_rest_by_ring(pre, post, m, g, g2, i, queue);
    lemma_abs_reaches_refl(g.abs);
    assert(g.ranges.submap_of(g2.ranges) && g.wrs.submap_of(g2.wrs));
    g2
}

/// Ring `r` has no request complete that M did not pop.
pub(super) open spec fn complete_popped<C, N: Device>(r: Ring<Payload<C, N>>, m: Nic::State, q: QpId, queue: Queue) -> bool {
    forall|s: int| #[trigger] r.req(s) is Some && r.req(s)->0.phase is Complete
        ==> popped(m, range_of(q, queue, r.meta(s)->0))
}

/// A QP coupled to M has none, on either queue.
pub(super) proof fn lemma_ok_complete_popped<C, N: Device>(e: HwQp<C, N>, index: u16, m: Nic::State, sh: Share, started: bool, written: Seq<SendWr>, queue: Queue)
    requires
        e.ok(index, m, sh, started, written),
    ensures
        complete_popped(ring_of(e, queue), m, e.id(), queue),
{
    assert forall|s: int| #[trigger] ring_of(e, queue).req(s) is Some && ring_of(e, queue).req(s)->0.phase is Complete
        implies popped(m, range_of(e.id(), queue, ring_of(e, queue).meta(s)->0)) by {
        lemma_complete_popped(e, index, m, sh, started, written, queue, s);
    }
}

/// What taking a result leaves of ring `queue` of entry `i`: A's `take`, and the `drop` of its shared
/// leases, or those `drop`s alone for a refused one.
pub(super) proof fn lemma_linked_taken<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, i: int, queue: Queue, seq: int, taken: bool) -> (g2: Refinement)
    requires
        linked(pre, m, g),
        same_but_ring(pre, post, i, queue),
        ring_of(pre[i]->0, queue).inv(),
        complete_popped(ring_of(pre[i]->0, queue), m, pre[i]->0.id(), queue),
        ring_of(post[i]->0, queue).left_by_take(&ring_of(pre[i]->0, queue), seq, taken),
        taken && ring_of(pre[i]->0, queue).req(seq) is Some ==> ring_of(pre[i]->0, queue).req(seq)->0.phase is Complete,
        !taken ==> ring_of(pre[i]->0, queue).refused(seq) is None,
        0 <= seq < key_span(),
    ensures
        linked(post, m, g2),
        g2 == (Refinement { abs: g2.abs, ..g }),
        g2.extends(g),
{
    let (r0, r1) = (ring_of(pre[i]->0, queue), ring_of(post[i]->0, queue));
    let q = pre[i]->0.id();
    let v = table_view(pre, m);
    let k = key(i, queue, seq);
    lemma_key_fields(i, queue, seq);
    lemma_table_at(pre, m, i, queue, seq);
    r0.lemma_view();
    assert(crate::refine::wf_at(r0.view(), seq));
    let (a2, w) = if r0.refused(seq) is Some {
        let a2 = lemma_take_refused(v, g.abs, lent_qp(), g.reqs, lent_holds(), k);
        (a2, crate::refine::without_refused(v, k))
    } else if taken && r0.req(seq) is Some {
        let rv = r0.req(seq)->0;
        let range = range_of(q, queue, r0.meta(seq)->0);
        assert(popped(m, range));
        let o = range.aggregate(m.retired);
        let lent = Lent { qp: q, holds: rv.leases.holds@ };
        let a2 = lemma_take(v, g.abs, lent_qp(), g.reqs, lent_holds(), k, o, lent);
        (a2, without(v, k))
    } else {
        lemma_abs_reaches_refl(g.abs);
        lemma_same(v, v, g.abs, lent_qp(), g.reqs, lent_holds());
        (g.abs, v)
    };
    assert(kept_requests(g.abs, a2));
    lemma_linked_one_key(w, pre, post, m, g, a2, i, queue, seq);
    Refinement { abs: a2, ..g }
}

/// The event loop's way with the oldest request of a ring, M having popped it, its slot emptied:
/// A's `take`.
pub(super) proof fn lemma_linked_oldest<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, i: int, queue: Queue) -> (g2: Refinement)
    requires
        linked(pre, m, g),
        same_but_ring(pre, post, i, queue),
        ring_of(pre[i]->0, queue).inv(),
        ring_of(post[i]->0, queue).retired_and_taken(&ring_of(pre[i]->0, queue)),
        ({
            let r0 = ring_of(pre[i]->0, queue);
            let s = r0.spec_retired() as int;
            &&& r0.req(s) is Some
            &&& !r0.req(s)->0.detached
            &&& popped(m, range_of(pre[i]->0.id(), queue, r0.meta(s)->0))
        }),
    ensures
        linked(post, m, g2),
        g2 == (Refinement { abs: g2.abs, ..g }),
        g2.extends(g),
{
    let r0 = ring_of(pre[i]->0, queue);
    let q = pre[i]->0.id();
    let s = r0.spec_retired() as int;
    let k = key(i, queue, s);
    ring_of(post[i]->0, queue).lemma_retired_and_taken(&r0);
    lemma_key_fields(i, queue, s);
    lemma_table_at(pre, m, i, queue, s);
    let rv = r0.req(s)->0;
    let o = range_of(q, queue, r0.meta(s)->0).aggregate(m.retired);
    let lent = Lent { qp: q, holds: rv.leases.holds@ };
    let a2 = lemma_take(table_view(pre, m), g.abs, lent_qp(), g.reqs, lent_holds(), k, o, lent);
    lemma_linked_one_key(without(table_view(pre, m), k), pre, post, m, g, a2, i, queue, s);
    Refinement { abs: a2, ..g }
}

/// A completion routed to its ring, M having popped its WQE: no step of A, which knew. M popped each
/// request it retires.
pub(super) proof fn lemma_linked_walked<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, i: int, queue: Queue, wqe: u64, wc: Wc, spent: bool)
    requires
        linked(pre, m, g),
        same_but_ring(pre, post, i, queue),
        ring_of(pre[i]->0, queue).inv(),
        ring_of(post[i]->0, queue).completed_with(&ring_of(pre[i]->0, queue), queue is Recv, wqe, wc, spent),
        (wqe as int) < head_of(m.qps[pre[i]->0.id()], queue),
    ensures
        linked(post, m, g),
        kept_free(table_view(pre, m), g.abs, table_view(post, m), g.abs, lent_holds()),
{
    let (r0, r1) = (ring_of(pre[i]->0, queue), ring_of(post[i]->0, queue));
    let q = pre[i]->0.id();
    let v = table_view(pre, m);
    assert forall|u: int| 0 <= u < key_span() implies {
        &&& (v.req)(#[trigger] key(i, queue, u)) == seen(r1, m, q, queue, u)
        &&& (v.staged)(key(i, queue, u)) == staged_seen(r1, q, u)
        &&& (v.refused)(key(i, queue, u)) == refused_seen(r1, q, u)
    } by {
        lemma_table_at(pre, m, i, queue, u);
        lemma_walked_seen(r0, r1, m, q, queue, wqe, wc, spent, u);
    }
    assert forall|u: int| 0 <= u < key_span() && #[trigger] seen(r1, m, q, queue, u) is Some implies {
        &&& seen(r0, m, q, queue, u) is Some
        &&& r1.meta(u)->0.first == r0.meta(u)->0.first
        &&& r1.meta(u)->0.end == r0.meta(u)->0.end
    } by {
        lemma_walked_seen(r0, r1, m, q, queue, wqe, wc, spent, u);
    }
    lemma_same(v, v, g.abs, lent_qp(), g.reqs, lent_holds());
    lemma_linked_ring(v, pre, post, m, g, g.abs, i, queue);
    assert(Refinement { abs: g.abs, ..g } == g);
    lemma_same(v, table_view(post, m), g.abs, lent_qp(), g.reqs, lent_holds());
}

/// [`lemma_linked_walked`] of one request.
proof fn lemma_walked_seen<C, N: Device>(r0: Ring<Payload<C, N>>, r1: Ring<Payload<C, N>>, m: Nic::State, q: QpId, queue: Queue, wqe: u64, wc: Wc, spent: bool, u: int)
    requires
        r0.inv(),
        r1.completed_with(&r0, queue is Recv, wqe, wc, spent),
        (wqe as int) < head_of(m.qps[q], queue),
    ensures
        seen(r0, m, q, queue, u) == seen(r1, m, q, queue, u),
        seen(r1, m, q, queue, u) is Some ==> {
            &&& r1.meta(u)->0.first == r0.meta(u)->0.first
            &&& r1.meta(u)->0.end == r0.meta(u)->0.end
        },
{
    let recv = queue is Recv;
    let to = r1.spec_retired() as int;
    r0.lemma_view();
    r1.lemma_meta(u);
    r0.lemma_meta(u);
    r0.lemma_completion_observation(u, recv, wqe, wc, to, spent);
    assert(r1.meta(u) == r0.completion_meta(u, recv, wqe, wc, to, spent));
    if r0.spec_retired() <= u < to {
        r0.lemma_retired_view(u, r0.retire_outcome(u, recv, wqe, wc));
    }
}

/// An owner gives up request `seq` of ring `queue` of entry `i`: A's `detach`, or the `drop` of a
/// refused one's leases, or nothing for a staged one.
pub(super) proof fn lemma_linked_given_up<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, i: int, queue: Queue, seq: int) -> (g2: Refinement)
    requires
        linked(pre, m, g),
        same_but_ring(pre, post, i, queue),
        ring_of(pre[i]->0, queue).inv(),
        complete_popped(ring_of(pre[i]->0, queue), m, pre[i]->0.id(), queue),
        ring_of(post[i]->0, queue).gave_up(&ring_of(pre[i]->0, queue), seq),
        0 <= seq < key_span(),
    ensures
        linked(post, m, g2),
        g2 == (Refinement { abs: g2.abs, ..g }),
        g2.extends(g),
        complete_popped(ring_of(post[i]->0, queue), m, pre[i]->0.id(), queue),
{
    let (r0, r1) = (ring_of(pre[i]->0, queue), ring_of(post[i]->0, queue));
    let q = pre[i]->0.id();
    let v = table_view(pre, m);
    let k = key(i, queue, seq);
    lemma_key_fields(i, queue, seq);
    lemma_table_at(pre, m, i, queue, seq);
    let a2 = lemma_detach(v, g.abs, lent_qp(), g.reqs, lent_holds(), k);
    r0.lemma_view();
    assert(crate::refine::wf_at(r0.view(), seq));
    if r0.req(seq) is Some && r0.req(seq)->0.phase is Complete {
        assert(popped(m, range_of(q, queue, r0.meta(seq)->0)));
    }
    assert(r0.req(seq) is Some ==> r1.meta(seq) == r0.meta(seq) || r0.req(seq)->0.phase is Complete);
    lemma_linked_one_key(detached(v, k), pre, post, m, g, a2, i, queue, seq);
    assert forall|u: int| #[trigger] r1.req(u) is Some && r1.req(u)->0.phase is Complete
        implies popped(m, range_of(q, queue, r1.meta(u)->0)) by {
        assert(r0.req(u) is Some);
        assert(r1.meta(u) == r0.meta(u));
    }
    Refinement { abs: a2, ..g }
}

/// What registering a waker on request `seq` leaves of its ring.
pub(super) open spec fn waker_set<C, N: Device>(r1: Ring<Payload<C, N>>, r0: Ring<Payload<C, N>>, seq: int, wk: WakerId) -> bool {
    &&& forall|s: int| #[trigger] r1.req(s) == if s == seq && r0.req(s) is Some && r0.req(s)->0.phase is InFlight && !r0.req(s)->0.detached {
        Some(r0.req(s)->0.with_waker(wk))
    } else {
        r0.req(s)
    }
    &&& forall|s: int| #[trigger] r1.staged(s) == if s == seq && r0.staged(s) is Some && !r0.staged(s)->0.detached {
        Some(r0.staged(s)->0.with_waker(wk))
    } else {
        r0.staged(s)
    }
    &&& forall|s: int| #[trigger] r1.refused(s) == r0.refused(s)
    &&& r1.same_meta(&r0)
}

/// A future registers a waker on request `seq` of ring `queue` of entry `i`: A's `register` if A has
/// it in flight; if M popped it already, A does not hear of it (the ring wakes it, as it routes the
/// CQE), and of one only staged, no step.
pub(super) proof fn lemma_linked_waker<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, i: int, queue: Queue, seq: int, wk: WakerId) -> (g2: Refinement)
    requires
        linked(pre, m, g),
        same_but_ring(pre, post, i, queue),
        ring_of(pre[i]->0, queue).inv(),
        complete_popped(ring_of(pre[i]->0, queue), m, pre[i]->0.id(), queue),
        waker_set(ring_of(post[i]->0, queue), ring_of(pre[i]->0, queue), seq, wk),
        0 <= seq < key_span(),
    ensures
        linked(post, m, g2),
        g2 == (Refinement { abs: g2.abs, ..g }),
        g2.extends(g),
{
    let (r0, r1) = (ring_of(pre[i]->0, queue), ring_of(post[i]->0, queue));
    let q = pre[i]->0.id();
    let v = table_view(pre, m);
    let k = key(i, queue, seq);
    lemma_key_fields(i, queue, seq);
    lemma_table_at(pre, m, i, queue, seq);
    let a2 = lemma_register(v, g.abs, lent_qp(), g.reqs, lent_holds(), k, wk);
    assert(r1.meta(seq) == r0.meta(seq));
    if r0.req(seq) is Some && r0.req(seq)->0.phase is Complete {
        assert(popped(m, range_of(q, queue, r0.meta(seq)->0)));
    }
    assert forall|u: int| u != seq implies {
        &&& #[trigger] r1.req(u) == r0.req(u)
        &&& r1.staged(u) == r0.staged(u)
        &&& r1.refused(u) == r0.refused(u)
        &&& r1.meta(u) == r0.meta(u)
    } by {
    }
    lemma_linked_one_key(registered(v, k, wk), pre, post, m, g, a2, i, queue, seq);
    Refinement { abs: a2, ..g }
}

/// A ring whose requests look the same: nothing changes for A.
pub(super) proof fn lemma_linked_same_view<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, i: int, queue: Queue)
    requires
        linked(pre, m, g),
        same_but_ring(pre, post, i, queue),
        forall|s: int| #[trigger] ring_of(post[i]->0, queue).req(s) == ring_of(pre[i]->0, queue).req(s),
        forall|s: int| #[trigger] ring_of(post[i]->0, queue).staged(s) == ring_of(pre[i]->0, queue).staged(s),
        forall|s: int| #[trigger] ring_of(post[i]->0, queue).refused(s) == ring_of(pre[i]->0, queue).refused(s),
        ring_of(post[i]->0, queue).same_meta(&ring_of(pre[i]->0, queue)),
    ensures
        linked(post, m, g),
{
    let (r0, r1) = (ring_of(pre[i]->0, queue), ring_of(post[i]->0, queue));
    let q = pre[i]->0.id();
    let v = table_view(pre, m);
    assert forall|u: int| 0 <= u < key_span() implies {
        &&& (v.req)(#[trigger] key(i, queue, u)) == seen(r1, m, q, queue, u)
        &&& (v.staged)(key(i, queue, u)) == staged_seen(r1, q, u)
        &&& (v.refused)(key(i, queue, u)) == refused_seen(r1, q, u)
    } by {
        lemma_table_at(pre, m, i, queue, u);
        assert(r1.req(u) == r0.req(u));
        assert(r1.meta(u) == r0.meta(u));
    }
    assert forall|u: int| 0 <= u < key_span() && #[trigger] seen(r1, m, q, queue, u) is Some implies {
        &&& seen(r0, m, q, queue, u) is Some
        &&& r1.meta(u)->0.first == r0.meta(u)->0.first
        &&& r1.meta(u)->0.end == r0.meta(u)->0.end
    } by {
        assert(r1.req(u) == r0.req(u));
        assert(r1.meta(u) == r0.meta(u));
    }
    lemma_linked_ring(v, pre, post, m, g, g.abs, i, queue);
    assert(Refinement { abs: g.abs, ..g } == g);
}

/// `post` has the rings and QP names of `pre`.
pub(super) open spec fn same_rings<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>) -> bool {
    &&& post.len() == pre.len()
    &&& forall|j: int| 0 <= j < pre.len() ==> (#[trigger] post[j] is Some) == (pre[j] is Some)
    &&& forall|j: int| 0 <= j < pre.len() && #[trigger] pre[j] is Some ==> {
        &&& post[j]->0.id() == pre[j]->0.id()
        &&& post[j]->0.send == pre[j]->0.send
        &&& post[j]->0.recv == pre[j]->0.recv
    }
}

/// What the invariant reads of the table is its rings and its QPs' names.
pub(super) proof fn lemma_linked_same_rings<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement)
    requires
        linked(pre, m, g),
        same_rings(pre, post),
    ensures
        linked(post, m, g),
        kept_free(table_view(pre, m), g.abs, table_view(post, m), g.abs, lent_holds()),
{
    let (v, w) = (table_view(pre, m), table_view(post, m));
    assert forall|k: int| #[trigger] table_req(pre, m, k) == table_req(post, m, k)
        && table_staged(pre, k) == table_staged(post, k)
        && table_refused(pre, k) == table_refused(post, k) by {
        let j = key_entry(k);
        if 0 <= j < pre.len() && pre[j] is Some {
            assert(post[j] is Some);
            assert(ring_of(post[j]->0, key_queue(k)) == ring_of(pre[j]->0, key_queue(k)));
        } else if 0 <= j < pre.len() {
            assert(post[j] is None);
        }
    }
    assert(same(v, w)) by {
        assert forall|k: int| #[trigger] (v.req)(k) == (w.req)(k) by {
            assert(table_req(pre, m, k) == table_req(post, m, k));
        }
        assert forall|k: int| #[trigger] (v.staged)(k) == (w.staged)(k) by {
            assert(table_req(pre, m, k) == table_req(post, m, k));
        }
        assert forall|k: int| #[trigger] (v.refused)(k) == (w.refused)(k) by {
            assert(table_req(pre, m, k) == table_req(post, m, k));
        }
    }
    lemma_same(v, w, g.abs, lent_qp(), g.reqs, lent_holds());
    assert forall|k: int| #[trigger] table_req(post, m, k) is Some implies {
        &&& g.ranges.contains_key(g.reqs[k])
        &&& g.ranges[g.reqs[k]] == key_range(post, k)
    } by {
        assert(table_req(pre, m, k) == table_req(post, m, k));
        assert(has_entry(pre, key_entry(k)));
        assert(ring_of(post[key_entry(k)]->0, key_queue(k)) == ring_of(pre[key_entry(k)]->0, key_queue(k)));
    }
    assert forall|j: int| has_entry(post, j) implies g.abs.qps.contains(#[trigger] post[j]->0.id()) by {
        assert(has_entry(pre, j));
        assert(g.abs.qps.contains(pre[j]->0.id()));
    }
    assert forall|p: QpId| #[trigger] g.abs.qps.contains(p) implies exists|j: int| has_entry(post, j) && #[trigger] post[j]->0.id() == p by {
        let j = choose|j: int| has_entry(pre, j) && #[trigger] pre[j]->0.id() == p;
        assert(has_entry(post, j) && post[j]->0.id() == p);
    }
}

// =============================================================================
// A request goes out

/// The next WQE M's queue `queue` of `qp` would take.
pub(super) open spec fn queue_end(qp: Qp, queue: Queue) -> nat {
    match queue {
        Queue::Send => qp.sq_head + qp.sq.len(),
        Queue::Recv => qp.rq_head + qp.rq.len(),
    }
}

/// The WQEs of a request A knows of come before what M's queue would take next.
proof fn lemma_ranges_below(g: Refinement, m: Nic::State, q: QpId, queue: Queue)
    requires
        ranges_ok(g, m),
        history_ok(m),
        m.qps.contains_key(q),
    ensures
        forall|r: ReqId| #[trigger] g.ranges.contains_key(r) && g.ranges[r].qp == q && g.ranges[r].queue == queue
            ==> g.ranges[r].end <= queue_end(m.qps[q], queue),
{
    assert forall|r: ReqId| #[trigger] g.ranges.contains_key(r) && g.ranges[r].qp == q && g.ranges[r].queue == queue
        implies g.ranges[r].end <= queue_end(m.qps[q], queue) by {
        let range = g.ranges[r];
        let w = range.at((range.end - 1) as nat);
        assert(range_known(m, range));
        assert(known(m, w));
        if m.retired.contains_key(w) {
            assert(w.seq < head_of(m.qps[w.qp], w.queue));
        }
    }
}

/// A verb on `q` alone, which keeps `q`'s queues, keeps those of the table.
pub(super) proof fn lemma_verb_keeps<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, name: nat, t: NicToken, m: Nic::State, n: Nic::State, pending: Seq<Cqe>, q: QpId)
    requires
        table_ok(qps, name, t, m, pending),
        n.qps == m.qps.insert(q, n.qps[q]),
        m.qps.contains_key(q) ==> keeps_queues(m, n, q),
    ensures
        forall|i: int| has_entry(qps, i) ==> keeps_queues(m, n, #[trigger] qps[i]->0.id()),
{
    assert forall|i: int| has_entry(qps, i) implies keeps_queues(m, n, #[trigger] qps[i]->0.id()) by {
        assert(qp_ok(qps[i], i, t, m));
    }
}

/// What `post_send` says of the WQEs it adds.
pub(super) proof fn lemma_send_queued(m: Nic::State, n: Nic::State, q: QpId, wrs: Seq<SendWr>)
    requires
        nic_step(m, n, Nic::Step::post_send(q, wrs)),
    ensures
        n.qps == m.qps.insert(q, n.qps[q]),
        keeps_queues(m, n, q),
        just_posted(n, q, Queue::Send, queue_end(m.qps[q], Queue::Send), queue_end(m.qps[q], Queue::Send) + wrs.len()),
{
    let qp = m.qps[q];
    let from = queue_end(qp, Queue::Send);
    let slots = wrs.map_values(|wr: SendWr| SendSlot { wr, outcome: None });
    assert(n.qps[q].sq == qp.sq + slots);
    assert forall|s: nat| from <= s < from + wrs.len()
        implies (#[trigger] n.qps[q].sq[s - n.qps[q].sq_head]).outcome is None by {
        assert(n.qps[q].sq[s - qp.sq_head] == slots[s - from]);
    }
}

/// What `post_recv` says of the WQEs it adds.
pub(super) proof fn lemma_recv_queued(m: Nic::State, n: Nic::State, q: QpId, wrs: Seq<RecvWr>)
    requires
        nic_step(m, n, Nic::Step::post_recv(q, wrs)),
    ensures
        n.qps == m.qps.insert(q, n.qps[q]),
        keeps_queues(m, n, q),
        just_posted(n, q, Queue::Recv, queue_end(m.qps[q], Queue::Recv), queue_end(m.qps[q], Queue::Recv) + wrs.len()),
{
}

/// QP `e` with ring `r` for its queue `queue`.
pub(super) open spec fn with_ring<C, N: Device>(e: HwQp<C, N>, queue: Queue, r: Ring<Payload<C, N>>) -> HwQp<C, N> {
    match queue {
        Queue::Send => HwQp { send: r, ..e },
        Queue::Recv => HwQp { recv: r, ..e },
    }
}

/// [`with_ring`] changes that ring alone.
proof fn lemma_with_ring<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, i: int, queue: Queue, r: Ring<Payload<C, N>>)
    requires
        has_entry(pre, i),
    ensures ({
        let post = pre.update(i, Some(with_ring(pre[i]->0, queue, r)));
        &&& same_but_ring(pre, post, i, queue)
        &&& ring_of(post[i]->0, queue) == r
        &&& post[i]->0.id() == pre[i]->0.id()
    }),
{
    assert(queue == Queue::Send || queue == Queue::Recv);
}

/// A request of one doorbell of its own: `staged`, its WQEs in M by step `step`, then committed.
pub(super) proof fn lemma_linked_one_posted<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, n: Nic::State, step: Nic::Step, g: Refinement, i: int, queue: Queue, staged: Ring<Payload<C, N>>, wqes: u64, view: ReqView<Payload<C, N>>, outcome: CompletionResult) -> (g2: Refinement)
    requires
        linked(pre, m, g),
        nic_step(m, n, step),
        forall|j: int| has_entry(pre, j) ==> keeps_queues(m, n, #[trigger] pre[j]->0.id()),
        same_but_ring(pre, post, i, queue),
        ring_of(pre[i]->0, queue).inv(),
        ring_of(pre[i]->0, queue).spec_wqe_posted() == ring_of(pre[i]->0, queue).spec_wqe_committed(),
        staged.just_staged(&ring_of(pre[i]->0, queue), wqes, view, outcome),
        ring_of(post[i]->0, queue).just_committed(&staged),
        view == ReqView::new(view.detached, view.leases),
        may_post_holds(g.abs, view.leases.holds@),
        forall|l: LeaseId| #[trigger] view.leases.holds@.contains_key(l)
            ==> free_lease(table_view(pre, m), g.abs, lent_holds(), l),
        roots_outside(table_view(pre, m), g.abs, lent_holds(), view.leases.holds@),
        queue_end(m.qps[pre[i]->0.id()], queue) == ring_of(pre[i]->0, queue).spec_wqe_committed(),
        just_posted(n, pre[i]->0.id(), queue, ring_of(pre[i]->0, queue).spec_wqe_committed() as nat, (ring_of(pre[i]->0, queue).spec_wqe_committed() + wqes) as nat),
        queue is Recv ==> wqes == 1,
    ensures
        linked(post, n, g2),
        g2.extends(g),
        nic_reaches(m, n),
{
    let q = pre[i]->0.id();
    let r0 = ring_of(pre[i]->0, queue);
    let g1 = lemma_linked_verb(pre, m, n, step, g);
    let mids = pre.update(i, Some(with_ring(pre[i]->0, queue, staged)));
    lemma_with_ring(pre, i, queue, staged);
    assert(may_post_holds(g1.abs, view.leases.holds@));
    lemma_roots_moved(table_view(pre, m), g.abs, table_view(pre, n), g1.abs, view.leases.holds@);
    lemma_linked_staged(pre, mids, n, g1, i, queue, wqes, view, outcome);
    lemma_ranges_below(g, m, q, queue);
    r0.lemma_view();
    assert forall|r: ReqId| #[trigger] g1.ranges.contains_key(r) && g1.ranges[r].qp == mids[i]->0.id() && g1.ranges[r].queue == queue
        implies g1.ranges[r].end <= staged.spec_wqe_committed() by {
    }
    if queue is Recv {
        assert forall|s: int| #[trigger] staged.staged(s) is Some
            implies staged.meta(s)->0.end as int == staged.meta(s)->0.first as int + 1 by {
            r0.lemma_meta(s);
        }
    }
    let g2 = lemma_linked_committed(mids, post, n, g1, i, queue);
    g2.lemma_extends_trans(g1, g);
    g2
}

/// The doorbell of the send queue of entry `i`, M taking the WRs `wrs` written since the last: A's
/// `post_request` of each request staged.
pub(super) proof fn lemma_linked_doorbell<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, n: Nic::State, g: Refinement, i: int, wrs: Seq<SendWr>) -> (g2: Refinement)
    requires
        linked(pre, m, g),
        nic_step(m, n, Nic::Step::post_send(pre[i]->0.id(), wrs)),
        forall|j: int| has_entry(pre, j) ==> keeps_queues(m, n, #[trigger] pre[j]->0.id()),
        same_but_ring(pre, post, i, Queue::Send),
        pre[i]->0.send.inv(),
        post[i]->0.send.just_committed(&pre[i]->0.send),
        queue_end(m.qps[pre[i]->0.id()], Queue::Send) == pre[i]->0.send.spec_wqe_committed(),
        pre[i]->0.send.spec_wqe_posted() == pre[i]->0.send.spec_wqe_committed() + wrs.len(),
    ensures
        linked(post, n, g2),
        g2.extends(g),
        nic_reaches(m, n),
        kept_free(table_view(pre, m), g.abs, table_view(post, n), g2.abs, lent_holds()),
{
    let q = pre[i]->0.id();
    let g1 = lemma_linked_verb(pre, m, n, Nic::Step::post_send(q, wrs), g);
    lemma_send_queued(m, n, q, wrs);
    lemma_ranges_below(g, m, q, Queue::Send);
    let g2 = lemma_linked_committed(pre, post, n, g1, i, Queue::Send);
    g2.lemma_extends_trans(g1, g);
    lemma_kept_free_trans(table_view(pre, m), g.abs, table_view(pre, n), g1.abs, table_view(post, n), g2.abs, lent_holds());
    g2
}

/// Every input of a request whose WQEs are all recorded belongs to a WQE, which gives it a direction.
pub(super) proof fn lemma_inputs_directed<N: Device>(leases: Leases<N>)
    requires
        leases.inv(),
        leases.fully_recorded(),
    ensures
        forall|i: int| 0 <= i < leases.loans().len() ==> (#[trigger] leases.loans()[i]).access is Some,
{
    leases.lemma_wqes();
    assert forall|i: int| 0 <= i < leases.loans().len() implies (#[trigger] leases.loans()[i]).access is Some by {
        assert(crate::buf::input_is_covered(leases.wqes(), i));
        let w = choose|w: int| 0 <= w < leases.wqes().len()
            && (#[trigger] leases.wqes()[w]).first <= i < leases.wqes()[w].end();
        let entry = leases.wqes()[w];
        assert(entry.matches(leases.loans()));
        let j = entry.end() - 1 - i;
        assert(0 <= j < entry.sges.len());
        assert(leases.loans()[entry.end() - 1 - j] == leases.loans()[i]);
    }
}

/// M moved, and what A sees of the table with it, as it was: the roots stay.
pub(super) proof fn lemma_roots_moved(v: Seen<Lent>, a: Abs::State, w: Seen<Lent>, a2: Abs::State, h: Holds)
    requires
        same(v, w),
        kept_free(v, a, w, a2, lent_holds()),
        roots_outside(v, a, lent_holds(), h),
    ensures
        roots_outside(w, a2, lent_holds(), h),
{
    assert forall|s: int| #[trigger] leases_of(w, lent_holds(), s) == leases_of(v, lent_holds(), s) by {
        assert((v.req)(s) == (w.req)(s) && (v.staged)(s) == (w.staged)(s) && (v.refused)(s) == (w.refused)(s));
    }
    assert forall|bytes: ISet<int>| #[trigger] root_outside(v, a, lent_holds(), h, bytes) implies root_outside(w, a2, lent_holds(), h, bytes) by {
        let r = choose|r: LeaseId| #[trigger] free_lease(v, a, lent_holds(), r) && !a.leases[r].unique && a.leases[r].mem == bytes
            && !h.contains_key(r);
        assert(free_lease(w, a2, lent_holds(), r));
    }
    assert forall|s: int, l: LeaseId| #[trigger] leases_of(w, lent_holds(), s).contains_key(l) && leases_of(w, lent_holds(), s)[l].shared
        implies root_outside(w, a2, lent_holds(), h, leases_of(w, lent_holds(), s)[l].kept) by {
        assert(root_outside(v, a, lent_holds(), h, leases_of(v, lent_holds(), s)[l].kept));
    }
    assert forall|l: LeaseId| #[trigger] h.contains_key(l) && h[l].shared implies root_outside(w, a2, lent_holds(), h, h[l].kept) by {
        assert(root_outside(v, a, lent_holds(), h, h[l].kept));
    }
}

// =============================================================================
// QPs come and go

/// A new QP, with rings that have no request, at entry `index`: A's `create_qp`.
pub(super) proof fn lemma_linked_created<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, index: int) -> (g2: Refinement)
    requires
        linked(pre, m, g),
        pre.len() <= post.len() <= pre.len() + 1,
        0 <= index < post.len(),
        pre.len() < post.len() ==> index == pre.len(),
        index < pre.len() ==> pre[index] is None,
        forall|j: int| 0 <= j < pre.len() && j != index ==> #[trigger] post[j] == pre[j],
        post[index] is Some,
        forall|j: int| has_entry(pre, j) ==> #[trigger] pre[j]->0.id() != post[index]->0.id(),
        forall|s: int| #[trigger] post[index]->0.send.req(s) is None,
        forall|s: int| #[trigger] post[index]->0.send.staged(s) is None,
        forall|s: int| #[trigger] post[index]->0.send.refused(s) is None,
        forall|s: int| #[trigger] post[index]->0.recv.req(s) is None,
        forall|s: int| #[trigger] post[index]->0.recv.staged(s) is None,
        forall|s: int| #[trigger] post[index]->0.recv.refused(s) is None,
    ensures
        linked(post, m, g2),
        g2 == (Refinement { abs: g2.abs, ..g }),
        g2.extends(g),
{
    let q = post[index]->0.id();
    let a = g.abs;
    let v = table_view(pre, m);
    assert(!a.qps.contains(q)) by {
        if a.qps.contains(q) {
            let j = choose|j: int| has_entry(pre, j) && #[trigger] pre[j]->0.id() == q;
        }
    }
    let a2 = Abs::State { qps: a.qps.insert(q), ..a };
    assert(abs_step(a, a2, Abs::Step::create_qp(q)));
    crate::refine::lemma_step_one(a, a2, Abs::Step::create_qp(q));
    assert forall|s: int, l: LeaseId| #[trigger] leases_of(v, lent_holds(), s).contains_key(l)
        implies a2.leases.contains_key(l) && a2.leases[l] == a.leases[l] by {
        crate::refine::lemma_held_exists(v, a, lent_qp(), g.reqs, lent_holds(), s, l);
    }
    assert forall|s: int| #[trigger] (v.staged)(s) is Some implies a2.qps.contains((lent_qp())((v.staged)(s)->0.leases)) by {
        assert(sits(v, a, lent_qp(), g.reqs, lent_holds(), s));
    }
    crate::refine::lemma_rooted_kept(v, a, a2, lent_holds());
    lemma_placed_apart(v, a, a2, lent_qp(), g.reqs, lent_holds());
    lemma_ranges_kept(g, a2, m);
    let w = table_view(post, m);
    assert forall|k: int| #[trigger] table_req(post, m, k) == table_req(pre, m, k)
        && table_staged(post, k) == table_staged(pre, k)
        && table_refused(post, k) == table_refused(pre, k) by {
        let j = key_entry(k);
        if j == index {
            assert(!has_entry(pre, j));
            assert(key_queue(k) == Queue::Send || key_queue(k) == Queue::Recv);
        } else if 0 <= j < pre.len() {
            assert(post[j] == pre[j]);
        } else {
            assert(!has_entry(post, j));
            assert(!has_entry(pre, j));
        }
    }
    assert(same(v, w)) by {
        assert forall|k: int| #[trigger] (v.req)(k) == (w.req)(k) by {
            assert(table_req(post, m, k) == table_req(pre, m, k));
        }
        assert forall|k: int| #[trigger] (v.staged)(k) == (w.staged)(k) by {
            assert(table_req(post, m, k) == table_req(pre, m, k));
        }
        assert forall|k: int| #[trigger] (v.refused)(k) == (w.refused)(k) by {
            assert(table_req(post, m, k) == table_req(pre, m, k));
        }
    }
    lemma_same(v, w, a2, lent_qp(), g.reqs, lent_holds());
    let g2 = Refinement { abs: a2, ..g };
    assert forall|k: int| #[trigger] table_req(post, m, k) is Some implies {
        &&& g2.ranges.contains_key(g2.reqs[k])
        &&& g2.ranges[g2.reqs[k]] == key_range(post, k)
    } by {
        assert(table_req(post, m, k) == table_req(pre, m, k));
        assert(table_req(pre, m, k) is Some);
        assert(has_entry(pre, key_entry(k)));
        assert(key_entry(k) != index);
        assert(post[key_entry(k)] == pre[key_entry(k)]);
    }
    assert forall|j: int| has_entry(post, j) implies a2.qps.contains(#[trigger] post[j]->0.id()) by {
        if j != index {
            assert(post[j] == pre[j]);
            assert(has_entry(pre, j));
            assert(a.qps.contains(pre[j]->0.id()));
        }
    }
    assert forall|p: QpId| #[trigger] a2.qps.contains(p) implies exists|j: int| has_entry(post, j) && #[trigger] post[j]->0.id() == p by {
        if p == q {
            assert(has_entry(post, index));
        } else {
            let j = choose|j: int| has_entry(pre, j) && #[trigger] pre[j]->0.id() == p;
            assert(j != index);
            assert(post[j] == pre[j]);
            assert(has_entry(post, j) && post[j]->0.id() == p);
        }
    }
    g2
}

/// A queue with nothing in flight has no request A sees in flight.
proof fn lemma_gone_queue_complete<C, N: Device>(e: HwQp<C, N>, m: Nic::State, queue: Queue, s: int)
    requires
        e.send.inv(),
        e.recv.inv(),
        complete_popped(e.send, m, e.id(), Queue::Send),
        complete_popped(e.recv, m, e.id(), Queue::Recv),
        e.send.spec_wqe_retired() == e.send.spec_wqe_committed(),
        e.recv.spec_wqe_retired() == e.recv.spec_wqe_committed(),
        seen(ring_of(e, queue), m, e.id(), queue, s) is Some,
    ensures
        seen(ring_of(e, queue), m, e.id(), queue, s)->0.phase is Complete,
{
    let r = ring_of(e, queue);
    r.lemma_meta(s);
    r.lemma_wqes();
    assert(queue == Queue::Send || queue == Queue::Recv);
    if r.req(s)->0.phase is Complete {
        assert(popped(m, range_of(e.id(), queue, r.meta(s)->0)));
    }
}

/// Queue pair `index`, with nothing in flight and nothing staged, goes: A's `detach` of each request
/// it still holds, `drop` of each lease of a refused one and `drop_qp`, M at `m`. Nothing is made
/// up: no request of it is in flight, so none needs an outcome M did not give it.
pub(super) proof fn lemma_linked_destroyed<C, N: Device>(pre: Seq<Option<HwQp<C, N>>>, post: Seq<Option<HwQp<C, N>>>, m: Nic::State, g: Refinement, index: int) -> (g2: Refinement)
    requires
        linked(pre, m, g),
        has_entry(pre, index),
        post == pre.update(index, None),
        pre[index]->0.send.inv(),
        pre[index]->0.recv.inv(),
        complete_popped(pre[index]->0.send, m, pre[index]->0.id(), Queue::Send),
        complete_popped(pre[index]->0.recv, m, pre[index]->0.id(), Queue::Recv),
        pre[index]->0.send.spec_wqe_retired() == pre[index]->0.send.spec_wqe_committed(),
        pre[index]->0.recv.spec_wqe_retired() == pre[index]->0.recv.spec_wqe_committed(),
        pre[index]->0.send.spec_wqe_posted() == pre[index]->0.send.spec_wqe_committed(),
        pre[index]->0.recv.spec_wqe_posted() == pre[index]->0.recv.spec_wqe_committed(),
        forall|a: int, b: int| #![trigger pre[a], pre[b]] has_entry(pre, a) && has_entry(pre, b) && a != b
            ==> pre[a]->0.id() != pre[b]->0.id(),
    ensures
        linked(post, m, g2),
        g2 == (Refinement { abs: g2.abs, ..g }),
        g2.extends(g),
{
    let e = pre[index]->0;
    let q = e.id();
    let v = table_view(pre, m);
    // Nothing of it is in flight or staged, as A sees it.
    assert forall|k: int| #[trigger] (v.req)(k) is Some && key_entry(k) == index
        implies (v.req)(k)->0.phase is Complete by {
        lemma_gone_queue_complete(e, m, key_queue(k), key_seq(k));
    }
    assert forall|k: int| #[trigger] (v.staged)(k) is Some implies key_entry(k) != index by {
        if key_entry(k) == index {
            let r = ring_of(e, key_queue(k));
            r.lemma_meta(key_seq(k));
            r.lemma_wqes();
            assert(key_queue(k) == Queue::Send || key_queue(k) == Queue::Recv);
        }
    }
    let from1 = key(index, Queue::Send, 0);
    let to1 = from1 + key_span();
    let from2 = key(index, Queue::Recv, 0);
    let to2 = from2 + key_span();
    assert forall|s: int| from1 <= s < to1 && #[trigger] (v.req)(s) is Some implies (v.req)(s)->0.phase is Complete by {
        lemma_key_interval(s, index, Queue::Send);
    }
    assert forall|s: int| from1 <= s < to1 implies #[trigger] (v.staged)(s) is None by {
        lemma_key_interval(s, index, Queue::Send);
    }
    let a1 = lemma_release(v, g.abs, lent_qp(), g.reqs, lent_holds(), from1, to1);
    let v1 = without_range(v, from1, to1);
    assert forall|s: int| from2 <= s < to2 && #[trigger] (v1.req)(s) is Some implies (v1.req)(s)->0.phase is Complete by {
        lemma_key_interval(s, index, Queue::Recv);
    }
    assert forall|s: int| from2 <= s < to2 implies #[trigger] (v1.staged)(s) is None by {
        lemma_key_interval(s, index, Queue::Recv);
    }
    let a2 = lemma_release(v1, a1, lent_qp(), g.reqs, lent_holds(), from2, to2);
    let v2 = without_range(v1, from2, to2);
    lemma_abs_reaches_trans(g.abs, a1, a2);
    crate::refine::lemma_kept_requests_trans(g.abs, a1, a2);
    // None of its requests is left in A.
    assert forall|r: ReqId| #[trigger] a2.requests.contains_key(r) implies a2.requests[r].qp != q by {
        assert(names_request(v2, g.reqs, r));
        let k = choose|k: int| #[trigger] (v2.req)(k) is Some && g.reqs[k] == r;
        lemma_key_interval(k, index, Queue::Send);
        lemma_key_interval(k, index, Queue::Recv);
        assert(key_queue(k) == Queue::Send || key_queue(k) == Queue::Recv);
        assert((v.req)(k) is Some);
        assert(table_req(pre, m, k) is Some);
        assert(has_entry(pre, key_entry(k)));
        assert(key_entry(k) != index);
        assert(pre[key_entry(k)]->0.id() != q);
        assert(sits(v2, a2, lent_qp(), g.reqs, lent_holds(), k));
    }
    let a3 = Abs::State { qps: a2.qps.remove(q), ..a2 };
    assert(a2.qps.contains(q)) by {
        assert(a2.qps == g.abs.qps);
        assert(g.abs.qps.contains(pre[index]->0.id()));
    }
    assert(abs_step(a2, a3, Abs::Step::drop_qp(q)));
    crate::refine::lemma_step_one(a2, a3, Abs::Step::drop_qp(q));
    lemma_abs_reaches_trans(g.abs, a2, a3);
    assert forall|s: int, l: LeaseId| #[trigger] leases_of(v2, lent_holds(), s).contains_key(l)
        implies a3.leases.contains_key(l) && a3.leases[l] == a2.leases[l] by {
        crate::refine::lemma_held_exists(v2, a2, lent_qp(), g.reqs, lent_holds(), s, l);
    }
    assert forall|s: int| #[trigger] (v2.staged)(s) is Some implies a3.qps.contains((lent_qp())((v2.staged)(s)->0.leases)) by {
        assert(sits(v2, a2, lent_qp(), g.reqs, lent_holds(), s));
        assert((v.staged)(s) is Some);
        assert(key_entry(s) != index);
        assert(has_entry(pre, key_entry(s)));
        assert(pre[key_entry(s)]->0.id() != q);
    }
    crate::refine::lemma_rooted_kept(v2, a2, a3, lent_holds());
    lemma_placed_apart(v2, a2, a3, lent_qp(), g.reqs, lent_holds());
    // What A sees of the table is what it sees of the others.
    let w = table_view(post, m);
    assert(same(v2, w)) by {
        assert forall|k: int| #[trigger] (v2.req)(k) == (w.req)(k) by {
            lemma_key_interval(k, index, Queue::Send);
            lemma_key_interval(k, index, Queue::Recv);
            assert(key_queue(k) == Queue::Send || key_queue(k) == Queue::Recv);
            if key_entry(k) != index && 0 <= key_entry(k) < pre.len() {
                assert(post[key_entry(k)] == pre[key_entry(k)]);
            }
        }
        assert forall|k: int| #[trigger] (v2.staged)(k) == (w.staged)(k) by {
            if key_entry(k) != index && 0 <= key_entry(k) < pre.len() {
                assert(post[key_entry(k)] == pre[key_entry(k)]);
            } else if key_entry(k) == index {
                assert((v.staged)(k) is None);
            }
        }
        assert forall|k: int| #[trigger] (v2.refused)(k) == (w.refused)(k) by {
            lemma_key_interval(k, index, Queue::Send);
            lemma_key_interval(k, index, Queue::Recv);
            assert(key_queue(k) == Queue::Send || key_queue(k) == Queue::Recv);
            if key_entry(k) != index && 0 <= key_entry(k) < pre.len() {
                assert(post[key_entry(k)] == pre[key_entry(k)]);
            }
        }
    }
    lemma_same(v2, w, a3, lent_qp(), g.reqs, lent_holds());
    let g2 = Refinement { abs: a3, ..g };
    lemma_ranges_kept(g, a3, m);
    assert forall|k: int| #[trigger] table_req(post, m, k) is Some implies {
        &&& g2.ranges.contains_key(g2.reqs[k])
        &&& g2.ranges[g2.reqs[k]] == key_range(post, k)
    } by {
        assert(key_entry(k) != index);
        assert(post[key_entry(k)] == pre[key_entry(k)]);
        assert(table_req(pre, m, k) is Some);
    }
    assert forall|j: int| has_entry(post, j) implies a3.qps.contains(#[trigger] post[j]->0.id()) by {
        assert(j != index);
        assert(post[j] == pre[j]);
        assert(has_entry(pre, j));
        assert(g.abs.qps.contains(pre[j]->0.id()));
        assert(pre[j]->0.id() != q);
    }
    assert forall|p: QpId| #[trigger] a3.qps.contains(p) implies exists|j: int| has_entry(post, j) && #[trigger] post[j]->0.id() == p by {
        assert(g.abs.qps.contains(p));
        let j = choose|j: int| has_entry(pre, j) && #[trigger] pre[j]->0.id() == p;
        assert(j != index);
        assert(post[j] == pre[j]);
        assert(has_entry(post, j) && post[j]->0.id() == p);
    }
    g2
}

// =============================================================================
// The event loop

/// A walk leaves complete only what M popped: the CQE it routes is one M popped.
proof fn lemma_walked_popped<C, N: Device>(r0: Ring<Payload<C, N>>, r1: Ring<Payload<C, N>>, m: Nic::State, q: QpId, queue: Queue, wqe: u64, wc: Wc, spent: bool)
    requires
        r0.inv(),
        r1.completed_with(&r0, queue is Recv, wqe, wc, spent),
        (wqe as int) < head_of(m.qps[q], queue),
        complete_popped(r0, m, q, queue),
    ensures
        complete_popped(r1, m, q, queue),
{
    let recv = queue is Recv;
    let to = r1.spec_retired() as int;
    assert forall|s: int| #[trigger] r1.req(s) is Some && r1.req(s)->0.phase is Complete
        implies popped(m, range_of(q, queue, r1.meta(s)->0)) by {
        r0.lemma_view();
        r1.lemma_meta(s);
        r0.lemma_meta(s);
        r0.lemma_completion_observation(s, recv, wqe, wc, to, spent);
        assert(r1.meta(s) == r0.completion_meta(s, recv, wqe, wc, to, spent));
        if r0.spec_retired() <= s < to {
            r0.lemma_retired_view(s, r0.retire_outcome(s, recv, wqe, wc));
            assert(r0.req(s) is Some);
            assert(r0.meta(s)->0.end as int <= wqe as int + 1);
        } else {
            assert(r1.req(s) == r0.req(s));
            assert(popped(m, range_of(q, queue, r0.meta(s)->0)));
        }
    }
}

impl<C, N: Device> Hw<C, N> {
    /// A step of the walk of the next CQE read, the result it retired taken if `took`: no step of A
    /// but `take`, for what is taken.
    pub(super) proof fn lemma_step_refined(&self, pre: &Self, wc: Wc, spent: bool, walked: Ring<Payload<C, N>>, seq: int, taken: bool, took: bool) -> (g: Refinement)
        requires
            pre.inv(),
            pre.pending@.len() > 0,
            reports(wc, pre.pending@[0]),
            ({
                let e = pre.pending@[0];
                let i = cqe_index(e);
                let y = pre.qps@[i]->0;
                let x = self.qps@[i]->0;
                let r = ring_of(y, e.queue);
                &&& walked.completed_with(&r, e.queue is Recv, e.seq as u64, wc, spent)
                &&& self.qps@.len() == pre.qps@.len()
                &&& forall|k: int| 0 <= k < pre.qps@.len() && k != i ==> #[trigger] self.qps@[k] == pre.qps@[k]
                &&& self.qps@[i] is Some
                &&& x.id() == y.id()
                &&& ring_of(x, other_queue(e.queue)) == ring_of(y, other_queue(e.queue))
                &&& took ==> ring_of(x, e.queue).left_by_take(&walked, seq, taken)
                &&& took && taken && walked.req(seq) is Some ==> walked.req(seq)->0.phase is Complete
                &&& took && !taken ==> walked.refused(seq) is None
                &&& took ==> 0 <= seq < key_span()
                &&& !took ==> ring_of(x, e.queue) == walked
            }),
        ensures
            linked(self.qps@, pre.nic(), g),
            g == (Refinement { abs: g.abs, ..pre.refined@ }),
            g.extends(pre.refined@),
    {
        let e = pre.pending@[0];
        let i = cqe_index(e);
        let y = pre.qps@[i]->0;
        let queue = e.queue;
        let r = ring_of(y, queue);
        let m = pre.nic();
        let t = pre.token@;
        let wqe = e.seq as u64;
        completion::lemma_next_cqe(pre.qps@, pre.next_name@, t, m, pre.pending@, wc);
        assert(cqe_ok(pre.qps@, m, e));
        let mids = pre.qps@.update(i, Some(with_ring(y, queue, walked)));
        lemma_with_ring(pre.qps@, i, queue, walked);
        lemma_linked_walked(pre.qps@, mids, m, pre.refined@, i, queue, wqe, wc, spent);
        assert forall|k: int| 0 <= k < mids.len() && k != i implies #[trigger] self.qps@[k] == mids[k] by {}
        if took {
            assert(qp_ok(pre.qps@[i], i, t, m));
            lemma_ok_complete_popped(y, i as u16, m, t.share(), t.started(y.id()), t.written(y.id()), queue);
            lemma_walked_popped(r, walked, m, y.id(), queue, wqe, wc, spent);
            lemma_linked_taken(mids, self.qps@, m, pre.refined@, i, queue, seq, taken)
        } else {
            lemma_linked_same_view(mids, self.qps@, m, pre.refined@, i, queue);
            lemma_abs_reaches_refl(pre.refined@.abs);
            pre.refined@
        }
    }

    /// The fast path of the walk took the oldest request of the next CQE's ring: A's `take`.
    pub(super) proof fn lemma_oldest_refined(&self, pre: &Self, wc: Wc) -> (g: Refinement)
        requires
            pre.inv(),
            pre.pending@.len() > 0,
            reports(wc, pre.pending@[0]),
            ({
                let e = pre.pending@[0];
                let i = cqe_index(e);
                let y = pre.qps@[i]->0;
                let x = self.qps@[i]->0;
                let r = ring_of(y, e.queue);
                let s = r.spec_retired() as int;
                &&& ring_of(x, e.queue).retired_and_taken(&r)
                &&& r.req(s) is Some
                &&& !r.req(s)->0.detached
                &&& r.meta(s)->0.end as int <= e.seq as int + 1
                &&& self.qps@.len() == pre.qps@.len()
                &&& forall|k: int| 0 <= k < pre.qps@.len() && k != i ==> #[trigger] self.qps@[k] == pre.qps@[k]
                &&& self.qps@[i] is Some
                &&& x.id() == y.id()
                &&& ring_of(x, other_queue(e.queue)) == ring_of(y, other_queue(e.queue))
            }),
        ensures
            linked(self.qps@, pre.nic(), g),
            g == (Refinement { abs: g.abs, ..pre.refined@ }),
            g.extends(pre.refined@),
    {
        let e = pre.pending@[0];
        let i = cqe_index(e);
        completion::lemma_next_cqe(pre.qps@, pre.next_name@, pre.token@, pre.nic(), pre.pending@, wc);
        assert(cqe_ok(pre.qps@, pre.nic(), e));
        lemma_linked_oldest(pre.qps@, self.qps@, pre.nic(), pre.refined@, i, e.queue)
    }

    /// A result taken from QP `index`'s ring of `side`: A's `take`, or the `drop`s of a refused one.
    pub(super) proof fn lemma_taken_refined(&self, pre: &Self, index: int, side: Side, seq: int, taken: bool) -> (g: Refinement)
        requires
            pre.inv(),
            pre.has_qp(index),
            self.qps@.len() == pre.qps@.len(),
            forall|k: int| 0 <= k < pre.qps@.len() && k != index ==> #[trigger] self.qps@[k] == pre.qps@[k],
            self.qps@[index] is Some,
            ({
                let (x, y) = (self.qps@[index]->0, pre.qps@[index]->0);
                let queue = completion::queue_of(side);
                &&& x.id() == y.id()
                &&& ring_of(x, queue).left_by_take(&ring_of(y, queue), seq, taken)
                &&& ring_of(x, other_queue(queue)) == ring_of(y, other_queue(queue))
                &&& taken && ring_of(y, queue).req(seq) is Some ==> ring_of(y, queue).req(seq)->0.phase is Complete
                &&& !taken ==> ring_of(y, queue).refused(seq) is None
            }),
            0 <= seq < key_span(),
        ensures
            linked(self.qps@, pre.nic(), g),
            g == (Refinement { abs: g.abs, ..pre.refined@ }),
            g.extends(pre.refined@),
    {
        let y = pre.qps@[index]->0;
        let t = pre.token@;
        let m = pre.nic();
        let queue = completion::queue_of(side);
        assert(qp_ok(pre.qps@[index], index, t, m));
        assert(queue == Queue::Send || queue == Queue::Recv);
        lemma_ok_complete_popped(y, index as u16, m, t.share(), t.started(y.id()), t.written(y.id()), queue);
        lemma_linked_taken(pre.qps@, self.qps@, m, pre.refined@, index, queue, seq, taken)
    }

    /// What the event loop hands out of a request the NIC saw is what A says of that request: A has
    /// it complete, and its result reports that outcome.
    pub(super) proof fn lemma_hands_out_in_a(&self, c: Completion<C, N>)
        requires
            self.inv(),
            self.hands_out(c),
            !(c.result matches Err(CompletionError::Refused)),
        ensures ({
            let g = self.refinement();
            let r = g.reqs[key(c.qp as int, completion::queue_of(c.queue), c.seq as int)];
            let o = g.ranges[r].aggregate(self.nic().retired);
            &&& g.abs.requests.contains_key(r)
            &&& g.abs.requests[r].phase == safeverbs_spec::abs::Phase::Complete(o)
            &&& g.abs.outcomes.contains_key(r)
            &&& g.abs.outcomes[r] == o
            &&& match c.result {
                Ok(done) => done.model() == o,
                Err(CompletionError::Failed { status, .. }) => o.status == status.model(),
                Err(CompletionError::Refused) => false,
            }
        }),
    {
        let g = self.refined@;
        let m = self.nic();
        let i = c.qp as int;
        let s = c.seq as int;
        let queue = completion::queue_of(c.queue);
        let qps = self.qps@;
        let e = qps[i]->0;
        let r = ring_of(e, queue);
        let k = key(i, queue, s);
        let v = table_view(qps, m);
        let range = range_of(e.id(), queue, r.meta(s)->0);
        lemma_key_fields(i, queue, s);
        lemma_table_at(qps, m, i, queue, s);
        assert(qp_ok(qps[i], i, self.token@, m));
        assert(queue == Queue::Send || queue == Queue::Recv);
        assert(r.req(s) is Some);
        assert(ring_resources_ok(r));
        assert(payload_ok(r.req(s)->0.leases, r.req(s)->0.detached));
        assert(table_req(qps, m, k) is Some);
        assert(g.ranges.contains_key(g.reqs[k]) && g.ranges[g.reqs[k]] == key_range(qps, k));
        assert(key_range(qps, k) == range);
        assert(range.wf() && (range.queue is Recv ==> range.end == range.first + 1));
        // Its last WQE retired: M popped it.
        let last = range.at((range.end - 1) as nat);
        assert(m.retired.contains_key(last));
        assert(m.qps.contains_key(e.id()));
        assert(last.seq < head_of(m.qps[last.qp], last.queue));
        assert(popped(m, range));
        assert(sits(v, g.abs, lent_qp(), g.reqs, lent_holds(), k));
    }
}

/// What the NIC touches depends on no lease of the program's: bringing buffers to A leaves it as it
/// was.
proof fn lemma_nic_touch_kept(a: Abs::State, a2: Abs::State, x: int)
    requires
        a2.requests == a.requests,
        same_but_program(a, a2),
    ensures
        a2.nic_reads(x) == a.nic_reads(x),
        a2.nic_writes(x) == a.nic_writes(x),
{
    if a.nic_reads(x) {
        let l = choose|l: LeaseId| #[trigger] a.leases.contains_key(l) && a.nic_reads_lease(l) && a.leases[l].mem.contains(x);
        assert(a2.nic_reads_lease(l) && a2.leases.contains_key(l));
    }
    if a2.nic_reads(x) {
        let l = choose|l: LeaseId| #[trigger] a2.leases.contains_key(l) && a2.nic_reads_lease(l) && a2.leases[l].mem.contains(x);
        assert(a.nic_reads_lease(l) && a.leases.contains_key(l));
    }
    if a.nic_writes(x) {
        let l = choose|l: LeaseId| #[trigger] a.leases.contains_key(l) && a.nic_writes_lease(l) && a.leases[l].mem.contains(x);
        assert(a2.nic_writes_lease(l) && a2.leases.contains_key(l));
    }
    if a2.nic_writes(x) {
        let l = choose|l: LeaseId| #[trigger] a2.leases.contains_key(l) && a2.nic_writes_lease(l) && a2.leases[l].mem.contains(x);
        assert(a.nic_writes_lease(l) && a.leases.contains_key(l));
    }
}

// =============================================================================
// `coupled`

/// A byte of a WQE of ring `queue` of entry `i` that M holds is in a lease of its request, which A
/// has in flight, in that WQE's direction.
proof fn lemma_wqe_byte<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, name: nat, t: NicToken, m: Nic::State, pending: Seq<Cqe>, g: Refinement, i: int, queue: Queue, w: int, id: u64, op: Option<safeverbs_spec::nic::Opcode>, sges: Seq<safeverbs_spec::nic::Sge>, x: int) -> (l: LeaseId)
    requires
        linked(qps, m, g),
        table_ok(qps, name, t, m, pending),
        has_entry(qps, i),
        ring_backed(ring_of(qps[i]->0, queue), w, id, op, sges),
        head_of(m.qps[qps[i]->0.id()], queue) <= w < ring_of(qps[i]->0, queue).spec_wqe_committed(),
        covers(sges, x),
    ensures
        g.abs.leases.contains_key(l),
        g.abs.leases[l].mem.contains(x),
        g.abs.leases[l].holder is Request,
        g.abs.in_flight(g.abs.leases[l].holder->req),
        g.abs.leases[l].holder->dir == local_role(op),
{
    let e = qps[i]->0;
    let q = e.id();
    let r = ring_of(e, queue);
    let a = g.abs;
    let v = table_view(qps, m);
    assert(qp_ok(qps[i], i, t, m));
    assert(queue == Queue::Send || queue == Queue::Recv);
    assert(ring_resources_ok(r));
    let s = lemma_ring_guards(r, w, id, op, sges);
    r.lemma_dma(s, w);
    let rv = r.req(s)->0;
    let meta = r.meta(s)->0;
    let holds = rv.leases.holds@;
    assert(payload_ok(rv.leases, rv.detached));
    // The WQE's bytes are in an input of its request, in a lease it holds.
    let k_sge = choose|n: int| 0 <= n < sges.len() && (#[trigger] sges[n]).span().contains(x);
    assert(retained_sge(rv.leases.leases, local_role(op), sges[k_sge]));
    let (n, g_) = choose|n: int, g_: int| 0 <= n < rv.leases.leases.loans().len() && 0 <= g_ < rv.leases.leases.view().len() && {
        let loan = #[trigger] rv.leases.leases.loans()[n];
        &&& sge_model(loan.part.spec_sge()) == sges[k_sge]
        &&& loan.access == Some(local_role(op))
        &&& loan.part.inv()
        &&& (#[trigger] rv.leases.leases.view()[g_]).same_backing(loan.part.resource())
        &&& loan.part.byte_domain().subset_of(rv.leases.leases.view()[g_].byte_domain())
        &&& local_role(op) is In ==> loan.part.spec_writable()
    };
    let loan = rv.leases.leases.loans()[n];
    assert(holds_cover(holds, rv.leases.leases.loans()));
    assert(loan_covered(holds, loan));
    assert(loan.part.spec_sge().length > 0);
    let l = choose|l: LeaseId| #[trigger] holds.contains_key(l) && loan.access == Some(holds[l].dir)
        && sge_model(loan.part.spec_sge()).span().bytes().subset_of(holds[l].mem);
    assert(sge_model(loan.part.spec_sge()).span().bytes().contains(x));
    // A has it in flight: its last WQE is not popped.
    assert(0 <= s < key_span()) by {
        r.lemma_view();
    }
    let k = key(i, queue, s);
    lemma_key_fields(i, queue, s);
    assert(!popped(m, range_of(q, queue, meta)));
    assert(table_req(qps, m, k) == seen(r, m, q, queue, s));
    assert(sits(v, a, lent_qp(), g.reqs, lent_holds(), k));
    assert((v.req)(k) is Some);
    assert((lent_holds())(k, (v.req)(k)->0.leases) == holds);
    assert(a.leases.contains_key(l));
    assert(a.leases[l].holder == (Holder::Request { req: g.reqs[k], dir: holds[l].dir }));
    assert(a.requests[g.reqs[k]].phase is InFlight);
    l
}

/// The invariant couples A and M, as far as the engine's own queues go; what the NIC may touch
/// beyond them is the theorem's premise.
pub(super) proof fn lemma_linked_coupled<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, name: nat, t: NicToken, m: Nic::State, pending: Seq<Cqe>, g: Refinement)
    requires
        linked(qps, m, g),
        table_ok(qps, name, t, m, pending),
        outside_covered(g.abs, m, t.share()),
    ensures
        coupled(g.abs, m, g.ranges, g.wrs),
{
    let a = g.abs;
    let sh = t.share();
    assert forall|r: ReqId| #[trigger] a.in_flight(r) implies g.ranges[r].pending(m) by {
        lemma_pending_of_last(g.ranges[r], m);
    }
    assert forall|x: int| #[trigger] m.nic_may_read(x) implies a.nic_reads(x) by {
        if !outside_reads(m, sh, x) {
            let (q, k) = choose|q: QpId, k: int| #![trigger m.qps[q].sq[k]] m.qps.contains_key(q)
                && 0 <= k < m.qps[q].sq.len() && {
                let wr = m.qps[q].sq[k].wr;
                !wr.inline && !wr.opcode.writes_local() && covers(wr.sges, x)
            };
            assert(sh.owns.contains(q));
            let i = choose|i: int| 0 <= i < qps.len() && #[trigger] qps[i] is Some && qps[i]->0.id() == q;
            let wr = m.qps[q].sq[k].wr;
            assert(qp_ok(qps[i], i, t, m));
            let l = lemma_wqe_byte(qps, name, t, m, pending, g, i, Queue::Send, m.qps[q].sq_head + k, wr.wr_id, Some(wr.opcode), wr.sges, x);
            assert(a.nic_reads_lease(l));
        }
    }
    assert forall|x: int| #[trigger] m.nic_may_write(x) implies a.nic_writes(x) by {
        if !outside_writes(m, sh, x) {
            if exists|q: QpId, k: int| #![trigger m.qps[q].sq[k]] m.qps.contains_key(q)
                && 0 <= k < m.qps[q].sq.len() && {
                let wr = m.qps[q].sq[k].wr;
                wr.opcode.writes_local() && covers(wr.sges, x)
            } {
                let (q, k) = choose|q: QpId, k: int| #![trigger m.qps[q].sq[k]] m.qps.contains_key(q)
                    && 0 <= k < m.qps[q].sq.len() && {
                    let wr = m.qps[q].sq[k].wr;
                    wr.opcode.writes_local() && covers(wr.sges, x)
                };
                assert(sh.owns.contains(q));
                let i = choose|i: int| 0 <= i < qps.len() && #[trigger] qps[i] is Some && qps[i]->0.id() == q;
                let wr = m.qps[q].sq[k].wr;
                assert(qp_ok(qps[i], i, t, m));
                let l = lemma_wqe_byte(qps, name, t, m, pending, g, i, Queue::Send, m.qps[q].sq_head + k, wr.wr_id, Some(wr.opcode), wr.sges, x);
                assert(a.nic_writes_lease(l));
            } else {
                let (q, k) = choose|q: QpId, k: int| #![trigger m.qps[q].rq[k]] m.qps.contains_key(q)
                    && 0 <= k < m.qps[q].rq.len() && covers(m.qps[q].rq[k].wr.sges, x);
                assert(sh.owns.contains(q));
                let i = choose|i: int| 0 <= i < qps.len() && #[trigger] qps[i] is Some && qps[i]->0.id() == q;
                let wr = m.qps[q].rq[k].wr;
                assert(qp_ok(qps[i], i, t, m));
                let l = lemma_wqe_byte(qps, name, t, m, pending, g, i, Queue::Recv, m.qps[q].rq_head + k, wr.wr_id, None, wr.sges, x);
                assert(a.nic_writes_lease(l));
            }
        }
    }
}

// =============================================================================
// What an engine says of itself

/// The ghost state of an engine with no QP yet, M at `m`.
pub(super) proof fn lemma_linked_new<C, N: Device>(m: Nic::State, m0: Nic::State) -> (g: Refinement)
    requires
        nic_init(m0),
        nic_reaches(m0, m),
    ensures
        linked(Seq::<Option<HwQp<C, N>>>::empty(), m, g),
        g.abs == abs_start(),
        g.reqs == Map::<int, ReqId>::empty(),
        g.ranges == Map::<ReqId, WqeRange>::empty(),
{
    lemma_abs_start();
    lemma_history_init(m0);
    lemma_nic_init_invariant(m0);
    let wrs = lemma_history_reaches(m0, m, IMap::empty());
    let g = Refinement { abs: abs_start(), reqs: Map::empty(), ranges: Map::empty(), wrs };
    let qps = Seq::<Option<HwQp<C, N>>>::empty();
    let v = table_view(qps, m);
    assert forall|s: int| #[trigger] wf_at(v, s) by {}
    assert forall|s: int| #[trigger] sits(v, g.abs, lent_qp(), g.reqs, lent_holds(), s) by {}
    assert forall|s: int, l: LeaseId| !(#[trigger] leases_of(v, lent_holds(), s).contains_key(l)) by {
        assert((v.req)(s) is None && (v.staged)(s) is None && (v.refused)(s) is None);
    }
    g
}

impl<C, N: Device> Hw<C, N> {
    /// What the engine stands for.
    pub closed spec fn refinement(&self) -> Refinement {
        self.refined@
    }

    /// `self` is `older` after calls of `Hw`: A moved by its own steps, M by its own, a request
    /// kept its WQEs, a WQE its WR. All the public methods that take the engine mutably say so.
    pub open(super) spec fn moved_from(&self, older: &Self) -> bool {
        ||| self.refined == older.refined && self.token@.nic() == older.token@.nic()
        ||| self.refined@.extends(older.refined@) && self.token@.nic() == older.token@.nic()
        ||| self.refined@.extends(older.refined@) && nic_reaches(older.token@.nic(), self.token@.nic())
    }

    /// What `moved_from` says, whichever way it came about.
    pub proof fn lemma_moved(&self, older: &Self)
        requires
            self.moved_from(older),
        ensures
            self.refinement().extends(older.refinement()),
            nic_reaches(older.nic(), self.nic()),
    {
        lemma_abs_reaches_refl(self.refined@.abs);
        lemma_nic_reaches_refl(self.token@.nic());
    }

    /// No change of the table is the same table.
    pub proof fn lemma_same_table_refl(&self)
        ensures
            self.same_table(self),
    {
    }

    /// No step at all is a step of `moved_from`.
    pub proof fn lemma_moved_refl(&self)
        ensures
            self.moved_from(self),
    {
    }

    /// `moved_from` is transitive.
    pub proof fn lemma_moved_trans(&self, mid: &Self, older: &Self)
        requires
            self.moved_from(mid),
            mid.moved_from(older),
        ensures
            self.moved_from(older),
    {
        self.lemma_moved(mid);
        mid.lemma_moved(older);
        self.refined@.lemma_extends_trans(mid.refined@, older.refined@);
        lemma_nic_reaches_trans(older.token@.nic(), mid.token@.nic(), self.token@.nic());
    }

    /// What the NIC may touch beyond the engine's own queues is A's to give (`outside_covered`): the
    /// premise of each state of a run, about what the engine cannot see.
    pub open(super) spec fn outside_ok(&self) -> bool {
        outside_covered(self.refined@.abs, self.token@.nic(), self.token@.share())
    }

    /// M has no QP but the engine's, and no region a peer may touch.
    pub open(super) spec fn alone(&self) -> bool {
        let m = self.token@.nic();
        &&& forall|q: QpId| #[trigger] m.qps.contains_key(q) ==> self.token@.share().owns.contains(q)
        &&& forall|key: Key| #[trigger] m.mrs.contains_key(key) ==> !m.mrs[key].remote_read && !m.mrs[key].remote_write
    }

    /// Alone, the premise of the theorem says nothing.
    pub proof fn lemma_alone_covered(&self)
        requires
            self.alone(),
        ensures
            self.outside_ok(),
    {
    }

    /// `coupled`, of an engine.
    pub proof fn lemma_coupled(&self)
        requires
            self.inv(),
            self.outside_ok(),
        ensures
            coupled(self.refinement().abs, self.nic(), self.refinement().ranges, self.refinement().wrs),
    {
        lemma_linked_coupled(self.qps@, self.next_name@, self.token@, self.token@.nic(), self.pending@, self.refined@);
    }

    /// **Any buffer the program holds is a lease of A's.** A state of A that the engine stands for
    /// too, reached by A's own steps from the one it stands for, has the buffer's lease free in the
    /// program's hands: what the buffers say, from their linear permissions alone.
    #[cfg(verus_keep_ghost)]
    pub(super) fn adopt(&self, keeper: &mut crate::buf::Lease<N>) -> (g: Ghost<Refinement>)
        requires
            self.inv(),
        ensures
            *final(keeper) == *old(keeper),
            linked(self.qps@, self.token@.nic(), g@),
            g@ == (Refinement { abs: g@.abs, ..self.refined@ }),
            g@.extends(self.refined@),
            adopted(table_view(self.qps@, self.token@.nic()), g@.abs, final(keeper).observation()),
            forall|x: int| #[trigger] g@.abs.nic_reads(x) == self.refined@.abs.nic_reads(x),
            forall|x: int| #[trigger] g@.abs.nic_writes(x) == self.refined@.abs.nic_writes(x),
    {
        self.separate_lease(keeper);
        let ghost mut a = self.refined@.abs;
        proof {
            let qps = self.qps@;
            let m = self.token@.nic();
            let g = self.refined@;
            let v = table_view(qps, m);
            self.lemma_table_kept();
            let incoming = keeper.observation();
            assert forall|k: int, l: LeaseId| #[trigger] leases_of(v, lent_holds(), k).contains_key(l)
                implies hold_apart(leases_of(v, lent_holds(), k)[l], incoming) by {
                lemma_table_hold_apart(qps, m, incoming, k, l);
            }
            a = lemma_adopt_keeper(v, g.abs, g.reqs, incoming);
            lemma_ranges_kept(g, a, m);
            assert forall|x: int| #[trigger] a.nic_reads(x) == g.abs.nic_reads(x) by {
                lemma_nic_touch_kept(g.abs, a, x);
            }
            assert forall|x: int| #[trigger] a.nic_writes(x) == g.abs.nic_writes(x) by {
                lemma_nic_touch_kept(g.abs, a, x);
            }
        }
        Ghost(Refinement { abs: a, ..self.refined@ })
    }

    /// **S1, of a buffer the program holds.** While it holds a buffer, the NIC writes none of its
    /// bytes, and if the buffer is unique reads none: `corollary_s1`, A's S1 against M, in the state
    /// of A that has its lease.
    #[cfg(verus_keep_ghost)]
    pub(crate) fn keeper_apart(&self, keeper: &mut crate::buf::Lease<N>)
        requires
            self.inv(),
        ensures
            *final(keeper) == *old(keeper),
            self.outside_ok() ==> forall|x: int| #[trigger] old(keeper).observation().byte_domain().contains(x) ==> {
                &&& !self.nic().nic_may_write(x)
                &&& old(keeper).observation().unique() ==> !self.nic().nic_may_read(x)
            },
    {
        let g = self.adopt(keeper);
        proof {
            if self.outside_ok() {
                let m = self.token@.nic();
                let sh = self.token@.share();
                assert(outside_covered(g@.abs, m, sh)) by {
                    assert forall|x: int| #[trigger] outside_reads(m, sh, x) implies g@.abs.nic_reads(x) by {
                        assert(self.refined@.abs.nic_reads(x));
                    }
                    assert forall|x: int| #[trigger] outside_writes(m, sh, x) implies g@.abs.nic_writes(x) by {
                        assert(self.refined@.abs.nic_writes(x));
                    }
                }
                lemma_linked_coupled(self.qps@, self.next_name@, self.token@, m, self.pending@, g@);
                let kept = keeper.observation();
                let whole = kept.byte_domain().to_iset();
                let v = table_view(self.qps@, m);
                assert forall|x: int| #[trigger] kept.byte_domain().contains(x) implies {
                    &&& !m.nic_may_write(x)
                    &&& kept.unique() ==> !m.nic_may_read(x)
                } by {
                    corollary_s1(g@.abs, m, g@.ranges, g@.wrs, x);
                    assert(whole.contains(x));
                    if kept.unique() {
                        let l = choose|l: LeaseId| #[trigger] free_lease(v, g@.abs, lent_holds(), l) && g@.abs.leases[l].unique
                            && g@.abs.leases[l].any_bytes && g@.abs.leases[l].mem == whole;
                        assert(g@.abs.leases.contains_key(l) && g@.abs.is_program(l) && g@.abs.leases[l].unique && g@.abs.leases[l].mem.contains(x));
                        assert(g@.abs.program_writes(x));
                        assert(g@.abs.program_reads(x));
                    } else {
                        let r = choose|r: LeaseId| #[trigger] free_lease(v, g@.abs, lent_holds(), r) && !g@.abs.leases[r].unique && g@.abs.leases[r].mem == whole;
                        assert(g@.abs.leases.contains_key(r) && g@.abs.is_program(r) && g@.abs.leases[r].mem.contains(x));
                        assert(g@.abs.program_reads(x));
                    }
                }
            }
        }
    }

    /// The lease of a request of one input, about to be written on this table, M at `m`, A at `g`.
    pub(super) proof fn lemma_name_one(&self, m: Nic::State, g: Refinement, leases: Leases<N>, part: Part<N>) -> (r: (Refinement, Holds))
        requires
            self.inv(),
            linked(self.qps@, m, g),
            leases.inv(),
            leases.loans().len() == 1,
            leases.loans()[0].part == part,
            leases.loans()[0].access is Some,
            table_apart(self.qps@, part.resource()),
        ensures
            linked(self.qps@, m, r.0),
            r.0 == (Refinement { abs: r.0.abs, ..g }),
            r.0.extends(g),
            may_post_holds(r.0.abs, r.1),
            forall|l: LeaseId| #[trigger] r.1.contains_key(l) ==> free_lease(table_view(self.qps@, m), r.0.abs, lent_holds(), l),
            roots_outside(table_view(self.qps@, m), r.0.abs, lent_holds(), r.1),
            holds_cover(r.1, leases.loans()),
            holds_kept(r.1, leases),
    {
        self.lemma_table_kept();
        assert forall|i: int| 0 <= i < leases.loans().len() implies #[trigger] table_apart(self.qps@, leases.loans()[i].part.resource()) by {
            assert(i == 0);
        }
        assert forall|i: int| 0 <= i < leases.loans().len() implies (#[trigger] leases.loans()[i]).access is Some by {
            assert(i == 0);
        }
        lemma_name_request(self.qps@, m, g, leases)
    }

    /// The requests of the table keep what their leases say.
    pub(super) proof fn lemma_table_kept(&self)
        requires
            self.inv(),
        ensures
            table_kept(self.qps@),
    {
        assert forall|i: int| #[trigger] has_entry(self.qps@, i) implies {
            &&& ring_resources_ok(self.qps@[i]->0.send)
            &&& ring_resources_ok(self.qps@[i]->0.recv)
        } by {
            assert(qp_ok(self.qps@[i], i, self.token@, self.token@.nic()));
        }
    }

    /// A new engine, M having reached `m0`'s successor by its own steps: A at its start.
    pub open(super) spec fn born_from(&self, m0: Nic::State) -> bool {
        &&& self.inv()
        &&& self.refined@.abs == abs_start()
        &&& self.refined@.ranges == Map::<ReqId, WqeRange>::empty()
        &&& nic_reaches(m0, self.token@.nic())
    }
}

/// The table of `older` is the table of `mid`'s older one.
pub broadcast proof fn lemma_same_table_chain<C, N: Device>(last: &Hw<C, N>, mid: &Hw<C, N>, older: &Hw<C, N>)
    requires
        #[trigger] last.same_table(mid),
        #[trigger] mid.same_table(older),
    ensures
        last.same_table(older),
{
    completion::lemma_same_qps_trans(last.qps@, mid.qps@, older.qps@);
}

/// With the same table, a QP is still there.
pub broadcast proof fn lemma_kept_qp<C, N: Device>(now: &Hw<C, N>, older: &Hw<C, N>, index: int)
    requires
        #[trigger] now.same_table(older),
        #[trigger] older.keeps_qp(index),
    ensures
        now.keeps_qp(index),
{
    assert(now.qps@[index] is Some == older.qps@[index] is Some);
}

/// `moved_from` is transitive: what lets an engine's type invariant follow its hardware side.
pub broadcast proof fn lemma_moved_chain<C, N: Device>(last: &Hw<C, N>, mid: &Hw<C, N>, older: &Hw<C, N>)
    requires
        #[trigger] last.moved_from(mid),
        #[trigger] mid.moved_from(older),
    ensures
        last.moved_from(older),
{
    last.lemma_moved_trans(mid, older);
}

/// An engine, as `Refines` asks of it: before it exists, the device just opened; then what its
/// public methods make of it.
#[verifier::reject_recursive_types(C)]
#[verifier::reject_recursive_types(N)]
#[allow(dead_code, clippy::large_enum_variant)] // Ghost code alone uses it.
pub ghost enum EngineState<C, N: Device> {
    /// The device just opened, no engine yet.
    Start { nic: Nic::State },
    /// The engine.
    Running { hw: Hw<C, N> },
}

impl<C, N: Device> Refines for EngineState<C, N> {
    open spec fn inv(&self) -> bool {
        match *self {
            EngineState::Start { nic } => nic_init(nic),
            EngineState::Running { hw } => hw.inv() && hw.outside_ok(),
        }
    }

    open spec fn abs(&self) -> Abs::State {
        match *self {
            EngineState::Start { .. } => abs_start(),
            EngineState::Running { hw } => hw.refinement().abs,
        }
    }

    open spec fn nic(&self) -> Nic::State {
        match *self {
            EngineState::Start { nic } => nic,
            EngineState::Running { hw } => hw.nic(),
        }
    }

    open spec fn ranges(&self) -> Map<ReqId, WqeRange> {
        match *self {
            EngineState::Start { .. } => Map::empty(),
            EngineState::Running { hw } => hw.refinement().ranges,
        }
    }

    open spec fn wrs(&self) -> IMap<WqeRef, SendWr> {
        match *self {
            EngineState::Start { .. } => IMap::empty(),
            EngineState::Running { hw } => hw.refinement().wrs,
        }
    }

    open spec fn init(&self) -> bool {
        match *self {
            EngineState::Start { nic } => nic_init(nic),
            EngineState::Running { .. } => false,
        }
    }

    /// `Hw::new` makes the engine; a public method of `Hw` moves it, keeping its invariant; and what
    /// the NIC may touch beyond the engine's queues stays A's to give ([`Hw::outside_ok`]), which
    /// only the control path and the other engines can see to.
    open spec fn step(&self, next: &Self) -> bool {
        match (*self, *next) {
            (EngineState::Start { nic }, EngineState::Running { hw }) => hw.born_from(nic) && hw.outside_ok(),
            (EngineState::Running { hw: a }, EngineState::Running { hw: b }) => {
                &&& b.inv()
                &&& b.moved_from(&a)
                &&& b.outside_ok()
            },
            _ => false,
        }
    }

    proof fn lemma_init(c: Self) {
        lemma_abs_start();
    }

    proof fn lemma_step(c: Self, d: Self) {
        match (c, d) {
            (EngineState::Start { nic }, EngineState::Running { hw }) => {
                lemma_abs_reaches_refl(abs_start());
                assert(hw.refinement().abs == abs_start());
                assert(c.ranges().submap_of(d.ranges()));
            },
            (EngineState::Running { hw: a }, EngineState::Running { hw: b }) => {
                b.lemma_moved(&a);
            },
            _ => {},
        }
    }

    proof fn lemma_coupled(c: Self) {
        match c {
            EngineState::Start { nic } => lemma_start_coupled(nic),
            EngineState::Running { hw } => hw.lemma_coupled(),
        }
    }
}

/// **Theorem 1, for an engine.** Any execution of an engine, from the device just opened, with
/// whatever the NIC and the peer do meanwhile, and whatever the program does with its buffers, stands
/// for an execution of A and of M, A and M stay coupled, and every verb met its precondition.
pub proof fn theorem_1_engine<C, N: Device>(run: Seq<EngineState<C, N>>)
    requires
        run.len() > 0,
        run[0].init(),
        forall|i: int| 0 <= i < run.len() - 1 ==> (#[trigger] run[i]).step(&run[i + 1]),
    ensures
        forall|i: int| 0 <= i < run.len() ==> {
            let c = #[trigger] run[i];
            &&& abs_reaches(run[0].abs(), c.abs()) && abs_inv(c.abs())
            &&& nic_reaches(run[0].nic(), c.nic()) && nic_inv(c.nic())
            &&& coupled(c.abs(), c.nic(), c.ranges(), c.wrs())
        },
{
    theorem_1(run);
}

/// **Theorem 1, of any engine a program holds, whatever it did with it.** An engine's type invariant
/// keeps the state it was made in and that it came from there by `moved_from`, so its state now is the
/// end of a run from the device just opened: A's and M's invariants hold, it stands for what they
/// reach, every verb met its precondition, and A and M are coupled. What the NIC may touch beyond the
/// engine's queues stays the premise it is ([`Hw::outside_ok`]).
pub proof fn theorem_1_held<C, N: Device>(first: Hw<C, N>, now: Hw<C, N>)
    requires
        first.born(),
        now.inv(),
        now.moved_from(&first),
        first.outside_ok(),
        now.outside_ok(),
    ensures
        abs_reaches(abs_start(), now.refinement().abs),
        abs_inv(now.refinement().abs),
        exists|m0: Nic::State| nic_init(m0) && nic_reaches(m0, now.nic()),
        nic_inv(now.nic()),
        coupled(now.refinement().abs, now.nic(), now.refinement().ranges, now.refinement().wrs),
{
    let m0 = choose|m0: Nic::State| nic_init(m0) && #[trigger] first.born_from(m0);
    let run = seq![
        EngineState::<C, N>::Start { nic: m0 },
        EngineState::Running { hw: first },
        EngineState::Running { hw: now },
    ];
    assert forall|i: int| 0 <= i < run.len() - 1 implies (#[trigger] run[i]).step(&run[i + 1]) by {
        assert(run[0] == EngineState::<C, N>::Start { nic: m0 });
        assert(run[1] == EngineState::<C, N>::Running { hw: first });
        assert(run[2] == EngineState::<C, N>::Running { hw: now });
    }
    theorem_1_engine(run);
    assert(run[2] == EngineState::<C, N>::Running { hw: now });
    assert(run[0] == EngineState::<C, N>::Start { nic: m0 });
}

/// **For an engine alone on its device, with no region open to a peer,** there is nothing the
/// premise of [`theorem_1_held`] could be about: M has no WQE but those of the engine's queues, and no
/// region a peer may touch.
pub proof fn theorem_1_alone<C, N: Device>(first: Hw<C, N>, now: Hw<C, N>)
    requires
        first.born(),
        now.inv(),
        now.moved_from(&first),
        first.alone(),
        now.alone(),
    ensures
        abs_reaches(abs_start(), now.refinement().abs),
        abs_inv(now.refinement().abs),
        exists|m0: Nic::State| nic_init(m0) && nic_reaches(m0, now.nic()),
        nic_inv(now.nic()),
        coupled(now.refinement().abs, now.nic(), now.refinement().ranges, now.refinement().wrs),
{
    first.lemma_alone_covered();
    now.lemma_alone_covered();
    theorem_1_held(first, now);
}

/// Theorem 1 through the public operations of [`Engine`](crate::engine::Engine) on typed buffers:
/// what they ensure is a step of [`EngineState`], so a run of them from the device just opened stands
/// for runs of A and M, coupled at every point. Until a buffer is lent it is one A would give the
/// program, so the NIC touches none of its bytes. What the NIC may touch beyond the engine's queues
/// stays the premise it is (`Hw::outside_ok`).
#[cfg(verus_keep_ghost)]
#[allow(dead_code)]
fn example_run<C, N: Device, T: crate::transport::Transport>(
    ctx: Context<N>,
    qp: crate::engine::QpKey<T>,
    received: crate::buf::BufMut<u8, N>,
    sent: crate::buf::Buf<u8, N>,
    context: (C, C),
) {
    let mut engine = match crate::engine::Engine::<C, N>::new(&ctx, 16) {
        Ok(engine) => engine,
        Err(_) => return,
    };
    let received = received.apart_from_nic(engine.hw());
    sent.apart_from_nic(engine.hw());
    let (one, two) = context;
    let _ = engine.post_recv(qp, received.part(), one);
    if let Ok(part) = sent.try_part() {
        let _ = engine.stage(qp, WrOp::Send, part, two, true);
    }
    let _ = engine.commit(qp);
    let _ = engine.progress();
    let ghost h5 = engine.state();
    let _ = engine.stands();
    let handed_out = engine.next_completion();
    let first = engine.stands();
    proof {
        // What the program gets is what A says of its request.
        if let Some(c) = handed_out {
            if !(c.result matches Err(CompletionError::Refused)) {
                h5.lemma_hands_out_in_a(c);
            }
        }
        // And wherever it stops, the state it left is the end of a run.
        let last = engine.state();
        if first@.alone() && last.alone() {
            theorem_1_alone(first@, last);
        }
    }
}

} // verus!
