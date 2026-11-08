//! Liveness of the engine's own waits: for room on a send queue ([`Hw::make_send_room`]) and for
//! a removed QP's drain (`Engine::drain`).
//!
//! Each pass of such a wait polls the CQ. What is left of the engine's work is a measure
//! ([`table_rank`]): for each QP, twice the WQEs the NIC has not executed, with the send WQEs
//! written and not rung, then the WQEs its rings have not heard of, and some while its drain WQE
//! waits to be posted. No pass raises it ([`Hw::ranked_from`]); each WQE the NIC executes lowers
//! it, and so do each CQE routed and the drain WQE posted. A pass after which the wait goes on
//! leaves the device owing it a step: a CQE to return to a poll, or a WQE to execute. Under the
//! premise that the device pays what it owes ([`safeverbs_spec::live`]), no run of passes goes on
//! for ever: the measure settles, and then nothing is paid.
#![allow(unused_imports)]
use super::*;
#[cfg(verus_keep_ghost)]
use safeverbs_spec::live::*;

verus! {

// =============================================================================
// The measure

/// What is left of entry `e`'s work, M being at `m`: twice the WQEs of its QP the NIC has not
/// executed, with the send WQEs written and not rung, then the WQEs its rings have not heard of,
/// and 4 while its drain WQE waits to be posted.
pub open(super) spec fn rank<C, N: Device>(e: HwQp<C, N>, m: Nic::State) -> int {
    let q = e.id();
    2 * (wqes_posted(m, q) - wqes_executed(m, q) + e.send.spec_wqe_posted() - e.send.spec_wqe_committed())
        + (e.send.spec_wqe_posted() - e.send.spec_routed())
        + (e.recv.spec_wqe_posted() - e.recv.spec_routed())
        + if e.drain_pending { 4int } else { 0int }
}

/// [`rank`] of an entry of the table; none for an empty one.
pub open(super) spec fn entry_rank<C, N: Device>(entry: Option<HwQp<C, N>>, m: Nic::State) -> int {
    match entry {
        Some(e) => rank(e, m),
        None => 0,
    }
}

/// What is left of the work of the QPs of table `qps`.
pub open(super) spec fn table_rank<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State) -> int
    decreases qps.len(),
{
    if qps.len() == 0 {
        0
    } else {
        table_rank(qps.drop_last(), m) + entry_rank(qps.last(), m)
    }
}

/// Entry `now` of a table is entry `then` of the table before, its QP given no WQE but what was
/// written on it, and still being removed if it was: what is left of its work fell by two for
/// each WQE the NIC executed, and by one more if its drain WQE went out.
pub open(super) spec fn entry_ranked<C, N: Device>(now: Option<HwQp<C, N>>, then: Option<HwQp<C, N>>, n: Nic::State, m: Nic::State) -> bool {
    match (now, then) {
        (Some(e), Some(f)) => {
            let q = f.id();
            &&& e.id() == q
            &&& f.removed ==> e.removed
            &&& counts_grow(m.qps[q], n.qps[q])
            &&& rank(e, n) + 2 * (wqes_executed(n, q) - wqes_executed(m, q))
                + (if f.drain_pending && !e.drain_pending { 1int } else { 0int }) <= rank(f, m)
        },
        (None, None) => true,
        _ => false,
    }
}

impl<C, N: Device> Hw<C, N> {
    /// `self` is `older` after calls that posted nothing but what was written and the drain WQEs:
    /// [`entry_ranked`] of each entry.
    pub open(super) spec fn ranked_from(&self, older: &Self) -> bool {
        &&& self.qps@.len() == older.qps@.len()
        &&& forall|i: int| 0 <= i < self.qps@.len()
            ==> #[trigger] entry_ranked(self.qps@[i], older.qps@[i], self.token@.nic(), older.token@.nic())
    }

    /// What is left of the work of its QPs.
    pub open(super) spec fn rank(&self) -> int {
        table_rank(self.qps@, self.token@.nic())
    }

    /// No call at all.
    pub proof fn lemma_ranked_refl(&self)
        ensures
            self.ranked_from(self),
    {
    }

    /// Calls after calls.
    pub proof fn lemma_ranked_trans(&self, mid: &Self, older: &Self)
        requires
            self.ranked_from(mid),
            mid.ranked_from(older),
        ensures
            self.ranked_from(older),
    {
        assert forall|i: int| 0 <= i < self.qps@.len()
            implies #[trigger] entry_ranked(self.qps@[i], older.qps@[i], self.token@.nic(), older.token@.nic()) by {
            assert(entry_ranked(self.qps@[i], mid.qps@[i], self.token@.nic(), mid.token@.nic()));
            assert(entry_ranked(mid.qps@[i], older.qps@[i], mid.token@.nic(), older.token@.nic()));
        }
    }

    /// Since `older`, the NIC executed a WQE of QP `j`, or its drain WQE went out.
    pub open(super) spec fn moved_on(&self, older: &Self, j: int) -> bool {
        &&& 0 <= j < self.qps@.len()
        &&& older.qps@[j] is Some
        &&& {
            let (e, f) = (self.qps@[j]->0, older.qps@[j]->0);
            ||| wqes_executed(older.token@.nic(), f.id()) < wqes_executed(self.token@.nic(), f.id())
            ||| f.drain_pending && !e.drain_pending
        }
    }

    /// Every CQE read was routed.
    pub open(super) spec fn all_routed(&self) -> bool {
        self.pending@.len() == 0
    }

    /// Nothing is written and not rung on QP `index`'s send queue.
    pub open(super) spec fn rung(&self, index: int) -> bool {
        rung_in(self.qps@, index)
    }

    /// The engine's CQ holds a CQE: one a poll of it returns.
    pub open(super) spec fn cq_holds(&self) -> bool {
        self.token@.nic().cqs[self.token@.share().cq].entries.len() > 0
    }

    /// A change of entry `k` alone, M having moved so that every other QP of the share only executed
    /// WQEs: the other entries are ranked.
    pub(super) proof fn lemma_others_ranked(&self, older: &Self, k: int)
        requires
            table_ok(older.qps@, older.next_name@, older.token@, older.token@.nic(), older.pending@),
            self.qps@.len() == older.qps@.len(),
            forall|i: int| 0 <= i < older.qps@.len() && i != k ==> #[trigger] self.qps@[i] == older.qps@[i],
            forall|p: QpId| #[trigger] older.token@.share().owns.contains(p) && older.token@.nic().qps.contains_key(p)
                && !(0 <= k < older.qps@.len() && older.qps@[k] is Some && older.qps@[k]->0.id() == p)
                ==> executes_only(older.token@.nic().qps[p], self.token@.nic().qps[p]),
        ensures
            forall|i: int| 0 <= i < self.qps@.len() && i != k
                ==> #[trigger] entry_ranked(self.qps@[i], older.qps@[i], self.token@.nic(), older.token@.nic()),
    {
        let (t, m, n) = (older.token@, older.token@.nic(), self.token@.nic());
        assert forall|i: int| 0 <= i < self.qps@.len() && i != k
            implies #[trigger] entry_ranked(self.qps@[i], older.qps@[i], n, m) by {
            assert(qp_ok(older.qps@[i], i, t, m));
            if let Some(e) = older.qps@[i] {
                if 0 <= k < older.qps@.len() && older.qps@[k] is Some {
                    assert(older.qps@[k]->0.id() != e.id());
                }
            }
        }
    }

    /// The measure falls along calls of `ranked_from`: by two for each WQE the NIC executed on QP
    /// `j`, and by one if its drain WQE went out.
    pub proof fn lemma_ranked_rank(&self, older: &Self, j: int)
        requires
            self.ranked_from(older),
        ensures
            self.rank() <= older.rank(),
            self.moved_on(older, j) ==> self.rank() < older.rank(),
    {
        let (n, m) = (self.token@.nic(), older.token@.nic());
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] entry_rank(self.qps@[i], n)
            <= entry_rank(older.qps@[i], m) by {
            assert(entry_ranked(self.qps@[i], older.qps@[i], n, m));
        }
        if 0 <= j < self.qps@.len() {
            assert(entry_ranked(self.qps@[j], older.qps@[j], n, m));
        }
        lemma_table_rank_le(self.qps@, older.qps@, n, m, j);
    }

    /// The measure counts what is left, so it is no less than nothing.
    pub proof fn lemma_rank_nonneg(&self)
        requires
            self.inv(),
        ensures
            self.rank() >= 0,
    {
        let (t, m) = (self.token@, self.token@.nic());
        assert forall|i: int| 0 <= i < self.qps@.len() implies #[trigger] entry_rank(self.qps@[i], m) >= 0 by {
            assert(qp_ok(self.qps@[i], i, t, m));
            if let Some(e) = self.qps@[i] {
                let qp = m.qps[e.id()];
                e.send.lemma_wqes();
                e.recv.lemma_wqes();
                assert(m.qp_wf(qp));
            }
        }
        lemma_table_rank_nonneg(self.qps@, m);
    }
}

/// Two tables of the same QPs, each entry of `post` with no more left than in `pre`: neither has
/// the table, and it has less if one entry has.
pub proof fn lemma_table_rank_le<C, N: Device>(post: Seq<Option<HwQp<C, N>>>, pre: Seq<Option<HwQp<C, N>>>, n: Nic::State, m: Nic::State, j: int)
    requires
        post.len() == pre.len(),
        forall|i: int| 0 <= i < pre.len() ==> #[trigger] entry_rank(post[i], n) <= entry_rank(pre[i], m),
    ensures
        table_rank(post, n) <= table_rank(pre, m),
        0 <= j < pre.len() && entry_rank(post[j], n) < entry_rank(pre[j], m)
            ==> table_rank(post, n) < table_rank(pre, m),
    decreases pre.len(),
{
    if pre.len() > 0 {
        let (a, b) = (post.drop_last(), pre.drop_last());
        assert forall|i: int| 0 <= i < b.len() implies #[trigger] entry_rank(a[i], n) <= entry_rank(b[i], m) by {
            assert(a[i] == post[i] && b[i] == pre[i]);
        }
        lemma_table_rank_le(a, b, n, m, j);
        assert(entry_rank(post[pre.len() - 1], n) <= entry_rank(pre[pre.len() - 1], m));
        if 0 <= j < b.len() {
            assert(a[j] == post[j] && b[j] == pre[j]);
        }
    }
}

/// A table whose entries have nothing negative left has nothing negative left.
proof fn lemma_table_rank_nonneg<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, m: Nic::State)
    requires
        forall|i: int| 0 <= i < qps.len() ==> #[trigger] entry_rank(qps[i], m) >= 0,
    ensures
        table_rank(qps, m) >= 0,
    decreases qps.len(),
{
    if qps.len() > 0 {
        let a = qps.drop_last();
        assert forall|i: int| 0 <= i < a.len() implies #[trigger] entry_rank(a[i], m) >= 0 by {
            assert(a[i] == qps[i]);
        }
        lemma_table_rank_nonneg(a, m);
        assert(entry_rank(qps[qps.len() - 1], m) >= 0);
    }
}

/// A table that differs from another in entry `k` alone has as much left but for that entry.
pub proof fn lemma_table_rank_one<C, N: Device>(post: Seq<Option<HwQp<C, N>>>, pre: Seq<Option<HwQp<C, N>>>, m: Nic::State, k: int)
    requires
        post.len() == pre.len(),
        0 <= k < pre.len(),
        forall|i: int| 0 <= i < pre.len() && i != k ==> #[trigger] post[i] == pre[i],
    ensures
        table_rank(post, m) - entry_rank(post[k], m) == table_rank(pre, m) - entry_rank(pre[k], m),
    decreases pre.len(),
{
    let (a, b) = (post.drop_last(), pre.drop_last());
    if k < pre.len() - 1 {
        assert forall|i: int| 0 <= i < b.len() && i != k implies #[trigger] a[i] == b[i] by {
            assert(a[i] == post[i] && b[i] == pre[i]);
        }
        lemma_table_rank_one(a, b, m, k);
        assert(post.last() == pre.last());
        assert(a[k] == post[k] && b[k] == pre[k]);
    } else {
        assert(a =~= b) by {
            assert forall|i: int| 0 <= i < b.len() implies a[i] == b[i] by {
                assert(a[i] == post[i] && b[i] == pre[i]);
            }
        }
    }
}

impl<C, N: Device> Hw<C, N> {
    /// A poll of the CQ, the table as it was: the NIC executed some WQEs meanwhile, and nothing else
    /// changed what is left.
    pub(super) proof fn lemma_polled_ranked(&self, older: &Self, wcs: Seq<Wc>)
        requires
            table_ok(older.qps@, older.next_name@, older.token@, older.token@.nic(), older.pending@),
            self.qps == older.qps,
            polled(older.token@.share(), pinned(older.token@), older.token@.nic(), self.token@.nic(), wcs),
        ensures
            self.ranked_from(older),
    {
        let (t, m, n) = (older.token@, older.token@.nic(), self.token@.nic());
        assert forall|i: int| 0 <= i < self.qps@.len()
            implies #[trigger] entry_ranked(self.qps@[i], older.qps@[i], n, m) by {
            assert(qp_ok(older.qps@[i], i, t, m));
            if let Some(e) = older.qps@[i] {
                lemma_polled_counts(t.share(), pinned(t), m, n, wcs, e.id());
            }
        }
    }
}

impl<C, N: Device> Hw<C, N> {
    /// QP `k`'s doorbell rang: what was written went to M as it was, so what is left of each QP's
    /// work fell only by what the NIC executed meanwhile.
    pub(super) proof fn lemma_rang_ranked(&self, start: &Self, k: int, mid: Nic::State, wrs: Seq<SendWr>)
        requires
            start.inv(),
            0 <= k < start.qps@.len(),
            start.qps@[k] is Some,
            self.qps@.len() == start.qps@.len(),
            forall|i: int| 0 <= i < start.qps@.len() && i != k ==> #[trigger] self.qps@[i] == start.qps@[i],
            self.qps@[k] is Some,
            ({
                let (e0, e1) = (start.qps@[k]->0, self.qps@[k]->0);
                let t0 = start.token@;
                &&& e1.id() == e0.id()
                &&& e1.recv == e0.recv
                &&& e1.removed == e0.removed
                &&& e1.drain_pending == e0.drain_pending
                &&& e1.send.just_committed(&e0.send)
                &&& wrs == t0.written(e0.id())
                &&& rely_reaches(t0.share(), pinned(t0), t0.nic(), mid)
                &&& nic_step(mid, self.token@.nic(), Nic::Step::post_send(e0.id(), wrs))
            }),
        ensures
            self.ranked_from(start),
    {
        let (t0, m0, n) = (start.token@, start.token@.nic(), self.token@.nic());
        let sh = t0.share();
        let e0 = start.qps@[k]->0;
        let q = e0.id();
        assert(qp_ok(start.qps@[k], k, t0, m0));
        lemma_rely_frame(sh, pinned(t0), m0, mid);
        assert forall|p: QpId| #[trigger] sh.owns.contains(p) && m0.qps.contains_key(p)
            && !(0 <= k < start.qps@.len() && start.qps@[k] is Some && start.qps@[k]->0.id() == p)
            implies executes_only(m0.qps[p], n.qps[p]) by {
            lemma_rely_counts(sh, pinned(t0), m0, mid, p);
            lemma_post_counts(mid, n, q, wrs);
        }
        self.lemma_others_ranked(start, k);
        lemma_rely_counts(sh, pinned(t0), m0, mid, q);
        lemma_post_counts(mid, n, q, wrs);
        e0.send.lemma_wqes();
        assert(entry_ranked(self.qps@[k], start.qps@[k], n, m0));
    }

    /// QP `k`'s doorbell was refused: what was written is gone, so what is left of its work fell by
    /// it, and each QP's by what the NIC executed meanwhile.
    pub(super) proof fn lemma_refused_ranked(&self, start: &Self, k: int, keep: int)
        requires
            start.inv(),
            0 <= k < start.qps@.len(),
            start.qps@[k] is Some,
            self.qps@.len() == start.qps@.len(),
            forall|i: int| 0 <= i < start.qps@.len() && i != k ==> #[trigger] self.qps@[i] == start.qps@[i],
            self.qps@[k] is Some,
            ({
                let (e0, e1) = (start.qps@[k]->0, self.qps@[k]->0);
                let t0 = start.token@;
                &&& e1.id() == e0.id()
                &&& e1.recv == e0.recv
                &&& e1.removed == e0.removed
                &&& e1.drain_pending == e0.drain_pending
                &&& e1.send.just_aborted(&e0.send, keep)
                &&& rely_reaches(t0.share(), pinned(t0), t0.nic(), self.token@.nic())
            }),
        ensures
            self.ranked_from(start),
    {
        let (t0, m0, n) = (start.token@, start.token@.nic(), self.token@.nic());
        let sh = t0.share();
        let e0 = start.qps@[k]->0;
        let q = e0.id();
        assert(qp_ok(start.qps@[k], k, t0, m0));
        assert forall|p: QpId| #[trigger] sh.owns.contains(p) && m0.qps.contains_key(p)
            && !(0 <= k < start.qps@.len() && start.qps@[k] is Some && start.qps@[k]->0.id() == p)
            implies executes_only(m0.qps[p], n.qps[p]) by {
            lemma_rely_counts(sh, pinned(t0), m0, n, p);
        }
        self.lemma_others_ranked(start, k);
        lemma_rely_counts(sh, pinned(t0), m0, n, q);
        e0.send.lemma_wqes();
        assert(entry_ranked(self.qps@[k], start.qps@[k], n, m0));
    }
}

/// Nothing is written and not rung on the send queue of entry `index` of table `qps`.
pub open(super) spec fn rung_in<C, N: Device>(qps: Seq<Option<HwQp<C, N>>>, index: int) -> bool {
    0 <= index < qps.len() && qps[index] is Some
        ==> qps[index]->0.send.spec_wqe_posted() == qps[index]->0.send.spec_wqe_committed()
}

// =============================================================================
// The wait for room

impl<C, N: Device> Hw<C, N> {
    /// QP `index`'s send queue has a slot for a request of `wqes` WQEs, and no room for its WQEs:
    /// what [`Hw::make_send_room`] waits on.
    pub open(super) spec fn waits_for_room(&self, index: int, wqes: u32) -> bool {
        &&& 0 <= index < self.qps@.len()
        &&& self.qps@[index] is Some
        &&& {
            let r = self.qps@[index]->0.send;
            &&& r.spec_has_free_slot()
            &&& !(r.spec_wqe_posted() - r.spec_wqe_retired() < r.spec_depth()
                && wqes <= r.spec_depth() - (r.spec_wqe_posted() - r.spec_wqe_retired()))
        }
    }

    /// The device owes QP `index` a step: the engine's CQ holds a CQE for a poll to return, or the QP
    /// holds a send WQE the NIC has not executed.
    pub open(super) spec fn owed_send(&self, index: int) -> bool {
        ||| self.cq_holds()
        ||| exists|w: WqeRef| w.qp == self.qps@[index]->0.id() && w.queue is Send
            && #[trigger] unexecuted(self.token@.nic(), w)
    }

    /// **A wait for room that goes on is owed a step.** A send queue without room has a signaled
    /// WQE out (`Ring::lemma_room_or_signal`), and so the device owes it a step
    /// ([`Hw::lemma_signal_owed`]).
    pub(super) proof fn lemma_room_owed(&self, index: int, wqes: u32)
        requires
            self.inv(),
            self.all_routed(),
            self.rung(index),
            self.waits_for_room(index, wqes),
            1 <= wqes <= self.qps@[index]->0.send.spec_max_wqes(),
        ensures
            self.owed_send(index),
    {
        assert(qp_ok(self.qps@[index], index, self.token@, self.token@.nic()));
        self.qps@[index]->0.send.lemma_room_or_signal();
        self.lemma_signal_owed(index);
    }

    /// **A signaled WQE out is owed a step.** With every CQE read routed and nothing written and not
    /// rung on QP `index`'s send queue, the request its ring signaled last, while out, is in flight
    /// past everything the ring heard of, and M holds it signaled (`send_outcomes::signals_ok`): the
    /// NIC has not executed it, or its CQE waits in the CQ.
    pub(super) proof fn lemma_signal_owed(&self, index: int)
        requires
            self.inv(),
            self.all_routed(),
            self.rung(index),
            self.has_qp(index),
            self.qps@[index]->0.send.spec_signal_out(),
        ensures
            self.owed_send(index),
    {
        let (t, m) = (self.token@, self.token@.nic());
        let sh = t.share();
        let e = self.qps@[index]->0;
        let q = e.id();
        assert(qp_ok(self.qps@[index], index, t, m));
        assert(e.ok(index as u16, m, sh, t.started(q), t.written(q)));
        let qp = m.qps[q];
        let r = e.send;
        reveal(send_outcomes::send_results);
        r.lemma_wqes();
        r.lemma_signal();
        r.lemma_view();
        // The request the ring signaled last is in flight, past all the ring heard of.
        let s = r.spec_sig_seq();
        r.lemma_meta(s);
        assert(r.req(s) is Some && r.req(s)->0.phase is InFlight);
        assert(send_outcomes::send_result(r, m, q, s));
        assert(lag_ok(e, m, Queue::Send, self.pending@));
        let x = (r.spec_sig_end() - 1) as nat;
        assert(qp.sq_head <= x < qp.sq_head + qp.sq.len());
        let w = WqeRef { qp: q, queue: Queue::Send, seq: x };
        // Its WR is signaled.
        assert(qp.sq[x - qp.sq_head].wr == e.send_tails@[s]);
        assert(qp.sq[x - qp.sq_head].wr.signaled);
        if !unexecuted(m, w) {
            // Executed: its CQE waits.
            assert(m.qp_wf(qp));
            assert(m.cqes_present(q));
            let k = x - qp.sq_head;
            assert(qp.sq[k].generates());
            assert(qp.attr.send_cq == sh.cq);
        }
    }

    /// A pass of [`Hw::make_send_room`]'s wait on QP `index`, from `self` to `next`, `found` if its
    /// poll routed a completion: what is left of the engine's work falls when one is found, and
    /// never rises; the QP stays in M; and the device owes the wait a step if it goes on.
    pub open(super) spec fn room_pass(&self, next: &Self, found: bool, index: int, wqes: u32) -> bool {
        &&& next.rank() >= 0
        &&& next.ranked_from(self)
        &&& found ==> next.rank() < self.rank()
        &&& next.waits_for_room(index, wqes) ==> next.owed_send(index)
        &&& next.holds_qp(index)
    }

    /// M has the QP of entry `index`, if there is one.
    pub open(super) spec fn holds_qp(&self, index: int) -> bool {
        0 <= index < self.qps@.len() && self.qps@[index] is Some
            ==> self.token@.nic().qps.contains_key(self.qps@[index]->0.id())
    }

    /// M has the QPs of the table.
    pub(super) proof fn lemma_holds_qp(&self, index: int)
        requires
            self.inv(),
        ensures
            self.holds_qp(index),
    {
        if 0 <= index < self.qps@.len() {
            assert(qp_ok(self.qps@[index], index, self.token@, self.token@.nic()));
        }
    }
}

/// What a pass of the wait for room leaves.
proof fn lemma_room_step<C, N: Device>(passes: spec_fn(nat) -> (Hw<C, N>, bool), index: int, wqes: u32, k: nat)
    requires
        forall|j: nat| #[trigger] passes(j).0.room_pass(&passes(j + 1).0, passes(j).1, index, wqes),
    ensures
        passes(k + 1).0.ranked_from(&passes(k).0),
        passes(k + 1).0.rank() >= 0,
        passes(k + 1).0.rank() <= passes(k).0.rank(),
        passes(k).1 ==> passes(k + 1).0.rank() < passes(k).0.rank(),
        passes(k + 1).0.waits_for_room(index, wqes) ==> passes(k + 1).0.owed_send(index),
        passes(k + 1).0.holds_qp(index),
{
    assert(passes(k).0.room_pass(&passes(k + 1).0, passes(k).1, index, wqes));
    passes(k + 1).0.lemma_ranked_rank(&passes(k).0, -1);
}

/// The QP of entry `index` stays along calls that rank.
proof fn lemma_qp_kept<C, N: Device>(hs: spec_fn(nat) -> Hw<C, N>, index: int, a: nat, b: nat)
    requires
        forall|k: nat| #[trigger] hs(k + 1).ranked_from(&hs(k)),
        a <= b,
        0 <= index < hs(a).qps@.len(),
        hs(a).qps@[index] is Some,
    ensures
        0 <= index < hs(b).qps@.len(),
        hs(b).qps@[index] is Some,
        hs(b).qps@[index]->0.id() == hs(a).qps@[index]->0.id(),
    decreases b - a,
{
    if a < b {
        let k = (b - 1) as nat;
        lemma_qp_kept(hs, index, a, k);
        assert(hs(k + 1).ranked_from(&hs(k)));
        assert(entry_ranked(hs(b).qps@[index], hs(k).qps@[index], hs(b).token@.nic(), hs(k).token@.nic()));
    }
}

/// **The wait for room ends.** Take the passes of [`Hw::make_send_room`]'s wait on QP `index`
/// without end: each a [`Hw::room_pass`], each followed by the wait going on. Suppose that the
/// device executes every send WQE it holds (`executes_sends`), and that a poll of a CQ that holds a
/// CQE returns one. That is impossible: what is left of the engine's work never rises, so it
/// settles, and the next step the device owes the wait lowers it.
pub proof fn theorem_room_ends<C, N: Device>(passes: spec_fn(nat) -> (Hw<C, N>, bool), index: int, wqes: u32)
    requires
        passes(0).0.rank() >= 0,
        forall|k: nat| #[trigger] passes(k).0.room_pass(&passes(k + 1).0, passes(k).1, index, wqes),
        forall|k: nat| #[trigger] passes(k + 1).0.waits_for_room(index, wqes),
        executes_sends(|k: nat| passes(k).0.nic()),
        forall|k: nat| #[trigger] passes(k).0.cq_holds() ==> passes(k).1,
    ensures
        false,
{
    let hs = |k: nat| passes(k).0;
    let nic = |k: nat| passes(k).0.nic();
    assert forall|k: nat| #[trigger] hs(k + 1).ranked_from(&hs(k)) by {
        lemma_room_step(passes, index, wqes, k);
    }
    let rank = |k: nat| passes(k).0.rank() as nat;
    assert forall|k: nat| #[trigger] rank(k + 1) <= rank(k) by {
        lemma_room_step(passes, index, wqes, k);
        if k > 0 {
            lemma_room_step(passes, index, wqes, (k - 1) as nat);
        }
    }
    let settled = lemma_settles(rank);
    let k0 = settled + 1;
    let h = passes(k0).0;
    lemma_room_step(passes, index, wqes, settled);
    lemma_room_step(passes, index, wqes, k0);
    assert(passes(settled + 1).0.waits_for_room(index, wqes));
    assert(h.owed_send(index));
    assert(rank(k0) == rank(settled) && rank(k0 + 1) == rank(settled));
    if h.cq_holds() {
        // The poll of the next pass returns it, and the measure falls.
        assert(passes(k0).0.cq_holds());
    } else {
        let w = choose|w: WqeRef| w.qp == h.qps@[index]->0.id() && w.queue is Send && #[trigger] unexecuted(h.token@.nic(), w);
        assert(unexecuted(nic(k0), w) && w.queue is Send);
        let l = choose|l: nat| l >= k0 && !unexecuted(#[trigger] nic(l), w);
        let k = lemma_stops(|k: nat| unexecuted(nic(k), w), k0, l);
        let (a, b) = (passes(k).0, passes(k + 1).0);
        lemma_qp_kept(hs, index, k0, k);
        lemma_qp_kept(hs, index, k0, k + 1);
        lemma_room_step(passes, index, wqes, k);
        lemma_room_step(passes, index, wqes, (k - 1) as nat);
        assert(entry_ranked(b.qps@[index], a.qps@[index], b.token@.nic(), a.token@.nic()));
        lemma_executed_count(a.token@.nic(), b.token@.nic(), w);
        assert(b.moved_on(&a, index));
        b.lemma_ranked_rank(&a, index);
        assert(rank(k) == rank(settled) && rank(k + 1) == rank(settled));
    }
}

// =============================================================================
// The drain

impl<C, N: Device> Hw<C, N> {
    /// QP `index` is in M's error state and ready to take work there, as [`Hw::set_error`] leaves
    /// it.
    pub open(super) spec fn in_error(&self, index: int) -> bool {
        &&& self.has_qp(index)
        &&& self.token@.nic().qps[self.qps@[index]->0.id()].state is Err
        &&& self.qps@[index]->0.phase == Phase::Ready
    }

    /// QP `index` is not being removed, and nothing is written on it and not rung.
    pub open(super) spec fn unwritten(&self, index: int) -> bool {
        &&& self.has_qp(index)
        &&& !self.qps@[index]->0.removed
        &&& !self.qps@[index]->0.started
    }

    /// QP `index`, if there is one, is being removed.
    pub open(super) spec fn removed_at(&self, index: int) -> bool {
        self.has_qp(index) ==> self.qps@[index]->0.removed
    }

    /// Nothing is written on QP `index`, if there is one, and not rung.
    pub open(super) spec fn unstarted(&self, index: int) -> bool {
        self.has_qp(index) ==> !self.qps@[index]->0.started
    }

    /// Calls on QP `index` since `older` left the other entries and the CQEs read as they were.
    pub open(super) spec fn others_kept(&self, older: &Self, index: int) -> bool {
        &&& self.qps@.len() == older.qps@.len()
        &&& forall|i: int| 0 <= i < older.qps@.len() && i != index ==> #[trigger] self.qps@[i] == older.qps@[i]
        &&& self.pending == older.pending
    }

    /// QP `index`'s removal is as it was in `older`.
    pub open(super) spec fn removal_kept(&self, older: &Self, index: int) -> bool {
        older.has_qp(index) ==> {
            let (e, f) = (self.qps@[index]->0, older.qps@[index]->0);
            &&& e.removed == f.removed
            &&& e.drain_pending == f.drain_pending
            &&& e.flushing == f.flushing
            &&& e.turned_down == f.turned_down
        }
    }

    /// QP `index`'s doorbell rang over `older`: what was written on its send queue is in flight, and
    /// the request its ring signaled last is as it was.
    pub open(super) spec fn rang_over(&self, older: &Self, index: int) -> bool {
        older.has_qp(index) ==> {
            let (r, r0) = (self.qps@[index]->0.send, older.qps@[index]->0.send);
            &&& r.spec_wqe_committed() == r0.spec_wqe_posted()
            &&& r.spec_sig_end() == r0.spec_sig_end()
        }
    }

    /// QP `index`'s drain WQE is written and not rung: its removal moved it to the error state, and
    /// the WQE is the last request written, signaled.
    pub open(super) spec fn drain_written(&self, index: int) -> bool {
        &&& self.has_qp(index)
        &&& {
            let e = self.qps@[index]->0;
            &&& e.flushing@ && e.started && e.drain_pending
            &&& e.send.spec_sig_end() == e.send.spec_wqe_posted()
        }
    }

    /// QP `index`'s drain WQE was written over `older`, or before it, M as it was: what is left of
    /// the QP's work rose at most by that WQE, written and not rung, and its ring not hearing of it.
    pub open(super) spec fn drain_staged(&self, older: &Self, index: int) -> bool {
        &&& self.others_kept(older, index)
        &&& self.token@.nic() == older.token@.nic()
        &&& older.has_qp(index)
        &&& self.qps@[index]->0.id() == older.qps@[index]->0.id()
        &&& self.qps@[index]->0.removed == older.qps@[index]->0.removed
        &&& self.qps@[index]->0.drain_pending == older.qps@[index]->0.drain_pending
        &&& rank(self.qps@[index]->0, self.token@.nic()) <= rank(older.qps@[index]->0, older.token@.nic()) + 3
    }

    /// What a try at QP `index`'s drain WQE leaves ([`Hw::post_drain`]): nothing written and not
    /// rung once its removal moved it to the error state, and a drain WQE that still waits has no
    /// free slot or no room, or the device turned its doorbell down.
    pub open(super) spec fn drain_tried(&self, index: int) -> bool {
        self.has_qp(index) ==> {
            let e = self.qps@[index]->0;
            let r = e.send;
            &&& e.flushing@ ==> !e.started
            &&& e.drain_pending && !e.turned_down@ ==> !(r.spec_has_free_slot()
                && r.spec_wqe_posted() - r.spec_wqe_retired() < r.spec_depth())
        }
    }

    /// QP `index` is being removed and has work in flight, or its drain WQE to post: what
    /// [`Hw::draining`] says, and what `Engine::drain` waits on.
    pub open(super) spec fn drains_at(&self, index: int) -> bool {
        &&& self.has_qp(index)
        &&& {
            let e = self.qps@[index]->0;
            &&& e.removed
            &&& e.drain_pending || e.send.spec_wqe_posted() > e.send.spec_wqe_retired()
                || e.recv.spec_wqe_posted() > e.recv.spec_wqe_retired()
        }
    }

    /// One of the QPs from `from` up to `to` drains.
    pub open(super) spec fn drains(&self, from: int, to: int) -> bool {
        exists|i: int| from <= i < to && #[trigger] self.drains_at(i)
    }

    /// QP `index` is being removed, with nothing in flight and nothing written: what
    /// [`Hw::destroy`] destroys.
    pub open(super) spec fn destroyable(&self, index: int) -> bool {
        &&& self.has_qp(index)
        &&& {
            let e = self.qps@[index]->0;
            &&& e.removed && !e.started
            &&& e.send.spec_wqe_posted() == e.send.spec_wqe_retired()
            &&& e.recv.spec_wqe_posted() == e.recv.spec_wqe_retired()
        }
    }

    /// The device turned down a verb of QP `index`'s removal.
    pub open(super) spec fn turned_down_at(&self, index: int) -> bool {
        self.has_qp(index) && self.qps@[index]->0.turned_down@
    }

    /// The device turned down a verb of the removal of one of the QPs from `from` up to `to`.
    pub open(super) spec fn turned_down_in(&self, from: int, to: int) -> bool {
        exists|i: int| from <= i < to && #[trigger] self.turned_down_at(i)
    }

    /// QP `index`'s drain WQE waits for a slot of its send ring: none is free (they are held by
    /// requests the device refused, behind work in flight, or its counters reached their end).
    pub open(super) spec fn lacks_slot_at(&self, index: int) -> bool {
        self.has_qp(index) && self.qps@[index]->0.drain_pending && !self.qps@[index]->0.send.spec_has_free_slot()
    }

    /// The drain WQE of one of the QPs from `from` up to `to` waits for a slot.
    pub open(super) spec fn lacks_slot_in(&self, from: int, to: int) -> bool {
        exists|i: int| from <= i < to && #[trigger] self.lacks_slot_at(i)
    }

    /// The device owes the drain of QP `index` a step: the engine's CQ holds a CQE for a poll to
    /// return, or the QP, in the error state, holds a WQE the NIC has not executed.
    pub open(super) spec fn owed_flush(&self, index: int) -> bool {
        &&& self.has_qp(index)
        &&& {
            let (m, q) = (self.token@.nic(), self.qps@[index]->0.id());
            ||| self.cq_holds()
            ||| m.qps[q].state is Err && exists|w: WqeRef| w.qp == q && #[trigger] unexecuted(m, w)
        }
    }

    /// The device owes the drain of one of the QPs from `from` up to `to` a step.
    pub open(super) spec fn owed_in(&self, from: int, to: int) -> bool {
        exists|i: int| from <= i < to && #[trigger] self.owed_flush(i)
    }

    /// The doorbell of QP `index`'s drain WQE rang: from `start` (the WQE written over it, `staged`,
    /// then rung, `rung`) to `self` (it waits no more), what is left of the QP's work fell by two
    /// for each WQE the NIC executed, and by one more.
    pub(super) proof fn lemma_drain_rung(&self, rung: &Self, staged: &Self, start: &Self, index: int)
        requires
            staged.drain_staged(start, index),
            staged.drain_written(index),
            rung.ranked_from(staged),
            rung.others_kept(staged, index),
            rung.removal_kept(staged, index),
            self.others_kept(rung, index),
            self.token@.nic() == rung.token@.nic(),
            self.has_qp(index),
            ({
                let (e, f) = (self.qps@[index]->0, rung.qps@[index]->0);
                &&& e == HwQp { drain_pending: false, ..f }
            }),
        ensures
            self.ranked_from(start),
    {
        let (m, n) = (start.token@.nic(), self.token@.nic());
        assert forall|i: int| 0 <= i < self.qps@.len()
            implies #[trigger] entry_ranked(self.qps@[i], start.qps@[i], n, m) by {
            assert(entry_ranked(rung.qps@[i], staged.qps@[i], n, m));
            if i != index {
                assert(self.qps@[i] == rung.qps@[i]);
                assert(staged.qps@[i] == start.qps@[i]);
            }
        }
    }

    /// A QP being removed that does not drain is one [`Hw::destroy`] destroys: nothing is written
    /// on it ([`drain_ok`]).
    pub proof fn lemma_drained(&self, index: int)
        requires
            self.inv(),
            self.keeps_qp(index),
            self.removed_at(index),
            self.drained_in(index, index + 1),
        ensures
            self.destroyable(index),
    {
        assert(!self.drains_at(index));
        assert(qp_ok(self.qps@[index], index, self.token@, self.token@.nic()));
    }

    /// A QP being removed stays so along calls that rank.
    pub proof fn lemma_ranked_removed(&self, older: &Self)
        requires
            self.ranked_from(older),
        ensures
            forall|i: int| #[trigger] older.removed_at(i) ==> self.removed_at(i),
    {
        assert forall|i: int| #[trigger] older.removed_at(i) implies self.removed_at(i) by {
            if 0 <= i < self.qps@.len() {
                assert(entry_ranked(self.qps@[i], older.qps@[i], self.token@.nic(), older.token@.nic()));
            }
        }
    }
}

impl<C, N: Device> Hw<C, N> {
    /// No QP from `from` up to `to` drains.
    pub open(super) spec fn drained_in(&self, from: int, to: int) -> bool {
        forall|i: int| from <= i < to ==> !#[trigger] self.drains_at(i)
    }

    /// `self` is `older` after tries at the drain WQEs of the QPs from `from` up to `to`
    /// ([`Hw::post_drain`]): what is left of the engine's work did not rise, unless the device
    /// turned down a verb of a removal; and the CQEs read are as they were.
    pub open(super) spec fn tried_from(&self, older: &Self, from: int, to: int) -> bool {
        &&& self.turned_down_in(from, to) || self.ranked_from(older)
        &&& forall|i: int| from <= i < to ==> #[trigger] self.drain_tried(i)
        &&& self.qps@.len() == older.qps@.len()
        &&& self.pending == older.pending
    }

    /// `self` is `older` after a poll of the CQ, `found` if it routed a completion: what is left of
    /// the engine's work fell then, and never rose; every CQE read is routed.
    pub open(super) spec fn polled_from(&self, older: &Self, found: bool) -> bool {
        &&& self.ranked_from(older)
        &&& found ==> self.rank() < older.rank()
        &&& self.all_routed()
    }

    /// A pass of `Engine::drain`'s wait on the QPs from `from` up to `to`, from `self` to `next`,
    /// each just before a poll of the CQ, `found` if the poll of `self` routed a completion: unless
    /// the device turned down a verb of a removal, what is left of the engine's work falls when one
    /// is found and never rises, and if the drain goes on, the device owes it a step or a drain WQE
    /// lacks a slot; and M keeps the QPs.
    pub open(super) spec fn drain_pass(&self, next: &Self, found: bool, from: int, to: int) -> bool {
        &&& next.turned_down_in(from, to) || {
            &&& next.rank() >= 0
            &&& next.ranked_from(self)
            &&& found ==> next.rank() < self.rank()
            &&& next.drains(from, to) ==> next.owed_in(from, to) || next.lacks_slot_in(from, to)
        }
        &&& forall|i: int| from <= i < to ==> #[trigger] next.holds_qp(i)
    }

    /// No try yet.
    pub proof fn lemma_tried_none(&self, from: int)
        ensures
            self.tried_from(self, from, from),
            self.drained_in(from, from),
    {
        self.lemma_ranked_refl();
    }

    /// One more try in a pass of `Engine::drain`: `self` is `mid` after [`Hw::post_drain`] of QP
    /// `index`, which drains then if `draining`; `busy` says whether one before it drains.
    pub proof fn lemma_tried_one(&self, mid: &Self, older: &Self, from: int, index: int, busy: bool, draining: bool)
        requires
            from <= index,
            mid.tried_from(older, from, index),
            busy ==> mid.drains(from, index),
            !busy ==> mid.drained_in(from, index),
            self.drain_tried(index),
            self.others_kept(mid, index),
            self.turned_down_at(index) || self.ranked_from(mid),
            draining == self.drains_at(index),
        ensures
            self.tried_from(older, from, index + 1),
            busy || draining ==> self.drains(from, index + 1),
            !(busy || draining) ==> self.drained_in(from, index + 1),
    {
        assert forall|i: int| from <= i < index + 1 implies #[trigger] self.drain_tried(i) by {
            if i < index {
                assert(mid.drain_tried(i));
            }
        }
        if mid.turned_down_in(from, index) {
            let i = choose|i: int| from <= i < index && #[trigger] mid.turned_down_at(i);
            assert(self.turned_down_at(i));
        } else if !self.turned_down_at(index) {
            self.lemma_ranked_trans(mid, older);
        }
        if busy {
            let i = choose|i: int| from <= i < index && #[trigger] mid.drains_at(i);
            assert(self.drains_at(i));
        }
        if !(busy || draining) {
            assert forall|i: int| from <= i < index + 1 implies !#[trigger] self.drains_at(i) by {
                if i < index {
                    assert(!mid.drains_at(i));
                }
            }
        }
    }

    /// **Receives in flight are owed a step.** With every CQE read routed, a receive queue with WQEs
    /// in flight has a request in flight past everything its ring heard of
    /// (`Ring::lemma_in_flight`), so its WQE is in M's queue: the NIC has not executed it, or its
    /// CQE waits in the CQ (every receive has one).
    pub(super) proof fn lemma_recv_owed(&self, index: int)
        requires
            self.inv(),
            self.all_routed(),
            self.has_qp(index),
            self.qps@[index]->0.recv.spec_wqe_posted() > self.qps@[index]->0.recv.spec_wqe_retired(),
        ensures
            self.cq_holds() || exists|w: WqeRef| w.qp == self.qps@[index]->0.id()
                && #[trigger] unexecuted(self.token@.nic(), w),
    {
        let (t, m) = (self.token@, self.token@.nic());
        let sh = t.share();
        let e = self.qps@[index]->0;
        let q = e.id();
        assert(qp_ok(self.qps@[index], index, t, m));
        assert(e.ok(index as u16, m, sh, t.started(q), t.written(q)));
        let qp = m.qps[q];
        let r = e.recv;
        reveal(outcomes::recv_outcomes);
        r.lemma_wqes();
        let s = r.lemma_in_flight();
        r.lemma_meta(s);
        assert(outcomes::recv_result(r, m, q, s));
        assert(lag_ok(e, m, Queue::Recv, self.pending@));
        let x = r.meta(s)->0.first as nat;
        assert(qp.rq_head <= x < qp.rq_head + qp.rq.len());
        let w = WqeRef { qp: q, queue: Queue::Recv, seq: x };
        if unexecuted(m, w) {
            assert(w.qp == q && unexecuted(m, w));
        } else {
            // Executed: its CQE waits.
            assert(m.qp_wf(qp));
            assert(m.cqes_present(q));
            let k = x - qp.rq_head;
            assert(0 <= k < qp.rq_done);
            assert(qp.rq[k].outcome is Some);
            assert(qp.attr.recv_cq == sh.cq);
        }
    }

    /// **A drain that goes on is owed a step**, unless the device turned down a verb of the removal
    /// or its drain WQE lacks a slot. With every CQE read routed and QP `index`'s drain WQE tried:
    /// a QP its removal did not move to the error state has nothing in flight unless the device
    /// turned the move down ([`drain_ok`]); one moved there has nothing written and not rung, so a
    /// drain WQE waiting for room has a signaled WQE out (`Ring::lemma_room_or_signal`), once rung
    /// its send queue ends with a signaled WQE ([`drain_ok`]), and its receives are owed a step
    /// ([`Hw::lemma_recv_owed`]).
    pub(super) proof fn lemma_drain_owed(&self, index: int)
        requires
            self.inv(),
            self.all_routed(),
            self.drain_tried(index),
            self.drains_at(index),
        ensures
            self.owed_flush(index) || self.turned_down_at(index) || self.lacks_slot_at(index),
    {
        let (t, m) = (self.token@, self.token@.nic());
        let e = self.qps@[index]->0;
        let q = e.id();
        assert(qp_ok(self.qps@[index], index, t, m));
        assert(e.ok(index as u16, m, t.share(), t.started(q), t.written(q)));
        assert(drain_ok(e, m.qps[q]));
        e.send.lemma_wqes();
        e.recv.lemma_wqes();
        if e.flushing@ && !e.turned_down@ && !self.lacks_slot_at(index) {
            // Nothing is written and not rung.
            assert(self.rung(index));
            if e.drain_pending {
                e.send.lemma_room_or_signal();
                self.lemma_signal_owed(index);
            } else if e.send.spec_wqe_posted() > e.send.spec_wqe_retired() {
                self.lemma_signal_owed(index);
            } else {
                self.lemma_recv_owed(index);
            }
        }
    }

    /// What `Engine::drain` knows at the poll of a pass makes the pass before a
    /// [`Hw::drain_pass`]: `polled` is `before`, the state at that pass's poll, after it, and `self`
    /// is `polled` after tries at the drain WQEs of the QPs from `from` up to `to`.
    pub proof fn lemma_drain_pass(&self, polled: &Self, before: &Self, found: bool, from: int, to: int)
        requires
            self.inv(),
            polled.polled_from(before, found),
            self.tried_from(polled, from, to),
        ensures
            before.drain_pass(self, found, from, to),
    {
        assert forall|i: int| from <= i < to implies #[trigger] self.holds_qp(i) by {
            self.lemma_holds_qp(i);
        }
        if !self.turned_down_in(from, to) {
            self.lemma_rank_nonneg();
            self.lemma_ranked_trans(polled, before);
            self.lemma_ranked_rank(polled, -1);
            if self.drains(from, to) {
                let i = choose|i: int| from <= i < to && #[trigger] self.drains_at(i);
                assert(self.drain_tried(i));
                self.lemma_drain_owed(i);
                if self.lacks_slot_at(i) {
                    assert(self.lacks_slot_in(from, to));
                } else {
                    assert(self.owed_flush(i));
                    assert(self.owed_in(from, to));
                }
            }
        }
    }
}

/// What a pass of the drain leaves, the device turning nothing down.
proof fn lemma_drain_step<C, N: Device>(passes: spec_fn(nat) -> (Hw<C, N>, bool), from: int, to: int, k: nat)
    requires
        forall|j: nat| #[trigger] passes(j).0.drain_pass(&passes(j + 1).0, passes(j).1, from, to),
        !passes(k + 1).0.turned_down_in(from, to),
    ensures
        passes(k + 1).0.ranked_from(&passes(k).0),
        passes(k + 1).0.rank() >= 0,
        passes(k + 1).0.rank() <= passes(k).0.rank(),
        passes(k).1 ==> passes(k + 1).0.rank() < passes(k).0.rank(),
        passes(k + 1).0.drains(from, to) ==> passes(k + 1).0.owed_in(from, to) || passes(k + 1).0.lacks_slot_in(from, to),
        forall|i: int| from <= i < to ==> #[trigger] passes(k + 1).0.holds_qp(i),
{
    assert(passes(k).0.drain_pass(&passes(k + 1).0, passes(k).1, from, to));
    passes(k + 1).0.lemma_ranked_rank(&passes(k).0, -1);
}

/// **The drain ends.** Take the passes of `Engine::drain`'s wait on the QPs from `from` up to `to`
/// without end, each just before its poll: each a [`Hw::drain_pass`], each with a QP still
/// draining. Suppose that the device turns down no verb of their removals, that no drain WQE
/// lacks a slot, that the device flushes the WQEs of every QP in the error state (`flushes`), and
/// that a poll of a CQ that holds a CQE returns one. That is impossible: what is left of the
/// engine's work never rises, so it settles, and the next step the device owes the drain lowers
/// it.
pub proof fn theorem_drain_ends<C, N: Device>(passes: spec_fn(nat) -> (Hw<C, N>, bool), from: int, to: int)
    requires
        passes(0).0.rank() >= 0,
        forall|k: nat| #[trigger] passes(k).0.drain_pass(&passes(k + 1).0, passes(k).1, from, to),
        forall|k: nat| #[trigger] passes(k).0.drains(from, to),
        forall|k: nat| !(#[trigger] passes(k).0.turned_down_in(from, to)),
        forall|k: nat| !(#[trigger] passes(k).0.lacks_slot_in(from, to)),
        flushes(|k: nat| passes(k).0.nic()),
        forall|k: nat| #[trigger] passes(k).0.cq_holds() ==> passes(k).1,
    ensures
        false,
{
    let hs = |k: nat| passes(k).0;
    let nic = |k: nat| passes(k).0.nic();
    assert forall|k: nat| #[trigger] hs(k + 1).ranked_from(&hs(k)) by {
        assert(!passes(k + 1).0.turned_down_in(from, to));
        lemma_drain_step(passes, from, to, k);
    }
    let rank = |k: nat| passes(k).0.rank() as nat;
    assert forall|k: nat| #[trigger] rank(k + 1) <= rank(k) by {
        assert(!passes(k + 1).0.turned_down_in(from, to));
        lemma_drain_step(passes, from, to, k);
        if k > 0 {
            assert(!passes(k).0.turned_down_in(from, to));
            lemma_drain_step(passes, from, to, (k - 1) as nat);
        }
    }
    let settled = lemma_settles(rank);
    let k0 = settled + 1;
    let h = passes(k0).0;
    assert(!passes(k0).0.turned_down_in(from, to));
    assert(!passes(k0 + 1).0.turned_down_in(from, to));
    lemma_drain_step(passes, from, to, settled);
    lemma_drain_step(passes, from, to, k0);
    assert(h.drains(from, to));
    assert(!h.lacks_slot_in(from, to));
    assert(h.owed_in(from, to));
    let i = choose|i: int| from <= i < to && #[trigger] h.owed_flush(i);
    assert(rank(k0) == rank(settled) && rank(k0 + 1) == rank(settled));
    if h.cq_holds() {
        // The poll of this pass returns it, and the measure falls.
        assert(passes(k0).0.cq_holds());
    } else {
        let q = h.qps@[i]->0.id();
        let w = choose|w: WqeRef| w.qp == q && #[trigger] unexecuted(h.token@.nic(), w);
        assert(unexecuted(nic(k0), w) && nic(k0).qps[w.qp].state is Err);
        let l = choose|l: nat| l >= k0 && !unexecuted(#[trigger] nic(l), w);
        let k = lemma_stops(|k: nat| unexecuted(nic(k), w), k0, l);
        let (a, b) = (passes(k).0, passes(k + 1).0);
        lemma_qp_kept(hs, i, k0, k);
        lemma_qp_kept(hs, i, k0, k + 1);
        assert(!passes(k + 1).0.turned_down_in(from, to));
        assert(!passes(k).0.turned_down_in(from, to));
        assert((k - 1) as nat + 1 == k);
        lemma_drain_step(passes, from, to, k);
        lemma_drain_step(passes, from, to, (k - 1) as nat);
        assert(entry_ranked(b.qps@[i], a.qps@[i], b.token@.nic(), a.token@.nic()));
        assert(b.holds_qp(i));
        lemma_executed_count(a.token@.nic(), b.token@.nic(), w);
        assert(b.moved_on(&a, i));
        b.lemma_ranked_rank(&a, i);
        assert(rank(k) == rank(settled) && rank(k + 1) == rank(settled));
    }
}

// =============================================================================
// What polling does to the counts

/// Polling the CQ retires executed WQEs and lets the NIC execute more: each QP of the share keeps
/// the WQEs it was given, has executed more, and stays in the error state.
pub proof fn lemma_polled_counts(sh: Share, pins: ISet<Key>, m: Nic::State, n: Nic::State, wcs: Seq<Wc>, q: QpId)
    requires
        nic_inv(m),
        share_wf(m, sh),
        polled(sh, pins, m, n, wcs),
        sh.owns.contains(q),
        m.qps.contains_key(q),
    ensures
        nic_inv(n),
        share_wf(n, sh),
        n.qps.contains_key(q),
        executes_only(m.qps[q], n.qps[q]),
        m.qps[q].state is Err ==> n.qps[q].state is Err,
    decreases wcs.len(),
{
    if wcs.len() == 0 {
        lemma_rely_frame(sh, pins, m, n);
        lemma_rely_counts(sh, pins, m, n, q);
    } else {
        let (mid, before) = choose|mid: Nic::State, before: Nic::State|
            {
                &&& polled(sh, pins, m, mid, wcs.drop_last())
                &&& rely_reaches(sh, pins, mid, before)
                &&& nic_step(before, n, Nic::Step::poll_cq(sh.cq))
                &&& reports(wcs.last(), before.cqs[sh.cq].entries[0])
            };
        lemma_polled_counts(sh, pins, m, mid, wcs.drop_last(), q);
        lemma_rely_frame(sh, pins, mid, before);
        lemma_rely_counts(sh, pins, mid, before, q);
        lemma_poll_counts(before, n, sh.cq);
        lemma_nic_step_is_next(before, n, Nic::Step::poll_cq(sh.cq));
        lemma_step_keeps_invariant(before, n);
        lemma_mine_keeps_share_wf(sh, ISet::empty(), before, n, Nic::Step::poll_cq(sh.cq));
    }
}

} // verus!
