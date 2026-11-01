//! Proofs that M's invariants are inductive and that its CQs never overflow.
//!
//! Nothing here is needed to read M; `nic.rs` calls these lemmas from its `#[inductive]`
//! and `assert` proofs.
#![allow(unused_imports)]

use crate::nic::*;
use crate::types::*;
use vstd::multiset::*;
use vstd::prelude::*;

verus! {

// =========================================================================================
// Multisets and counting

pub proof fn lemma_copies(q: QpId, n: nat)
    ensures
        copies(q, n).len() == n,
        copies(q, n).count(q) == n,
        forall|x: QpId| x != q ==> #[trigger] copies(q, n).count(x) == 0,
    decreases n,
{
    broadcast use vstd::multiset::group_multiset_axioms;
    if n > 0 {
        lemma_copies(q, (n - 1) as nat);
    }
}

pub proof fn lemma_subset_len(a: Multiset<QpId>, b: Multiset<QpId>)
    requires
        a.subset_of(b),
    ensures
        a.len() <= b.len(),
{
    broadcast use vstd::multiset::group_multiset_axioms;
    assert(b =~= a.add(b.sub(a)));
}

pub proof fn lemma_count_le(s: Seq<SendSlot>)
    ensures
        count_generating(s) <= s.len(),
{
    s.lemma_filter_len(gen_pred());
}

pub proof fn lemma_count_push(s: Seq<SendSlot>, x: SendSlot)
    ensures
        count_generating(s.push(x)) == count_generating(s) + (if x.generates() { 1nat } else { 0nat }),
{
    reveal(Seq::filter);
    assert(s.push(x).drop_last() =~= s);
}

pub proof fn lemma_count_add(a: Seq<SendSlot>, b: Seq<SendSlot>)
    ensures
        count_generating(a + b) == count_generating(a) + count_generating(b),
{
    Seq::filter_distributes_over_add(a, b, gen_pred());
}

pub proof fn lemma_count_none(s: Seq<SendSlot>)
    requires
        forall|i: int| 0 <= i < s.len() ==> !(#[trigger] s[i]).generates(),
    ensures
        count_generating(s) == 0,
    decreases s.len(),
{
    reveal(Seq::filter);
    if s.len() > 0 {
        lemma_count_none(s.drop_last());
    }
}

/// The CQEs waiting for a send queue are at most the executed WQEs.
pub proof fn lemma_sq_cqes_le(qp: Qp)
    requires
        qp.sq_done <= qp.sq.len(),
    ensures
        qp.sq_cqes() <= qp.sq_done,
{
    lemma_count_le(qp.sq.take(qp.sq_done as int));
}

// =========================================================================================
// The CQ always has room (the `assert`s of the NIC steps)

proof fn lemma_room(pre: Nic::State, q: QpId, c: CqId)
    requires
        pre.invariant(),
        pre.qps.contains_key(q),
        pre.cqs.contains_key(c),
        pre.cqs[c].used.count(q) < pre.cqs[c].reserved.count(q),
    ensures
        pre.cqs[c].entries.len() < pre.cqs[c].cap,
{
    broadcast use vstd::multiset::group_multiset_axioms;
    let cq = pre.cqs[c];
    assert(pre.cq_wf(c));
    let more = cq.used.insert(q);
    assert forall|x: QpId| more.count(x) <= cq.reserved.count(x) by {
        if x != q {
            assert(cq.used.count(x) == pre.pending(c, x));
            assert(cq.reserved.count(x) == pre.reserved_by(c, x));
            if pre.qps.contains_key(x) {
                let qx = pre.qps[x];
                assert(pre.qp_wf(qx));
                lemma_sq_cqes_le(qx);
            }
        }
    }
    lemma_subset_len(more, cq.reserved);
}

pub proof fn lemma_room_for_send_cqe(pre: Nic::State, q: QpId)
    requires
        pre.invariant(),
        pre.qps.contains_key(q),
        pre.qps[q].sq_done < pre.qps[q].sq.len(),
    ensures
        pre.cqs[pre.qps[q].attr.send_cq].entries.len() < pre.cqs[pre.qps[q].attr.send_cq].cap,
{
    let qp = pre.qps[q];
    let c = qp.attr.send_cq;
    assert(pre.qp_wf(qp));
    assert(pre.cq_wf(c));
    lemma_sq_cqes_le(qp);
    assert(pre.cqs[c].used.count(q) == pre.pending(c, q));
    assert(pre.cqs[c].reserved.count(q) == pre.reserved_by(c, q));
    lemma_room(pre, q, c);
}

pub proof fn lemma_room_for_recv_cqe(pre: Nic::State, q: QpId)
    requires
        pre.invariant(),
        pre.qps.contains_key(q),
        pre.qps[q].rq_done < pre.qps[q].rq.len(),
    ensures
        pre.cqs[pre.qps[q].attr.recv_cq].entries.len() < pre.cqs[pre.qps[q].attr.recv_cq].cap,
{
    let qp = pre.qps[q];
    let c = qp.attr.recv_cq;
    assert(pre.qp_wf(qp));
    assert(pre.cq_wf(c));
    lemma_sq_cqes_le(qp);
    assert(pre.cqs[c].used.count(q) == pre.pending(c, q));
    assert(pre.cqs[c].reserved.count(q) == pre.reserved_by(c, q));
    lemma_room(pre, q, c);
}

// =========================================================================================
// Frames: a CQ, or a queue pair's CQEs, that a step did not touch

/// CQ `c2` is well formed after a step that changed no queue pair and kept `c2`.
proof fn lemma_cq_unchanged(pre: Nic::State, post: Nic::State, c2: CqId)
    requires
        pre.invariant(),
        post.qps == pre.qps,
        pre.cqs.contains_key(c2),
        post.cqs.contains_key(c2),
        post.cqs[c2] == pre.cqs[c2],
    ensures
        post.cq_wf(c2),
{
    let cq = pre.cqs[c2];
    assert(pre.cq_wf(c2));
    assert forall|m: int| 0 <= m < cq.entries.len() implies post.cqe_wf(c2, #[trigger] cq.entries[m]) by {
        assert(pre.cqe_wf(c2, cq.entries[m]));
    }
    assert forall|q: QpId| #[trigger] cq.used.count(q) == post.pending(c2, q) by {
        assert(cq.used.count(q) == pre.pending(c2, q));
    }
    assert forall|q: QpId| #[trigger] cq.reserved.count(q) == post.reserved_by(c2, q) by {
        assert(cq.reserved.count(q) == pre.reserved_by(c2, q));
    }
}

/// Queue pair `q`'s CQEs are all present after a step that changed no queue pair and kept
/// its CQs.
proof fn lemma_cqes_present_unchanged(pre: Nic::State, post: Nic::State, q: QpId)
    requires
        pre.invariant(),
        post.qps == pre.qps,
        pre.qps.contains_key(q),
        post.cqs[pre.qps[q].attr.send_cq] == pre.cqs[pre.qps[q].attr.send_cq],
        post.cqs[pre.qps[q].attr.recv_cq] == pre.cqs[pre.qps[q].attr.recv_cq],
    ensures
        post.cqes_present(q),
{
    assert(pre.cqes_present(q));
}

// =========================================================================================
// Inductiveness, one lemma per transition

pub proof fn lemma_alloc_pd(pre: Nic::State, post: Nic::State, pd: PdId)
    requires pre.invariant(), Nic::State::alloc_pd(pre, post, pd),
    ensures post.invariant(),
{
}

pub proof fn lemma_reg_mr(pre: Nic::State, post: Nic::State, key: Key, mr: Mr)
    requires pre.invariant(), Nic::State::reg_mr(pre, post, key, mr),
    ensures post.invariant(),
{
}

pub proof fn lemma_dereg_mr(pre: Nic::State, post: Nic::State, key: Key)
    requires pre.invariant(), Nic::State::dereg_mr(pre, post, key),
    ensures post.invariant(),
{
}

pub proof fn lemma_create_cq(pre: Nic::State, post: Nic::State, c: CqId, cap: nat)
    requires pre.invariant(), Nic::State::create_cq(pre, post, c, cap),
    ensures post.invariant(),
{
    broadcast use vstd::multiset::group_multiset_axioms;
    assert forall|q: QpId| post.qps.contains_key(q) implies post.qp_wf(#[trigger] post.qps[q]) by {
        assert(pre.qp_wf(pre.qps[q]));
    }
    assert forall|c2: CqId| post.cqs.contains_key(c2) implies #[trigger] post.cq_wf(c2) by {
        if c2 != c {
            lemma_cq_unchanged(pre, post, c2);
        } else {
            assert forall|q: QpId| #[trigger] post.cqs[c].used.count(q) == post.pending(c, q) by {
                if post.qps.contains_key(q) {
                    assert(pre.qp_wf(pre.qps[q]));
                }
            }
            assert forall|q: QpId| #[trigger] post.cqs[c].reserved.count(q) == post.reserved_by(c, q) by {
                if post.qps.contains_key(q) {
                    assert(pre.qp_wf(pre.qps[q]));
                }
            }
        }
    }
    assert forall|q: QpId| post.qps.contains_key(q) implies #[trigger] post.cqes_present(q) by {
        lemma_cqes_present_unchanged(pre, post, q);
    }
}

pub proof fn lemma_destroy_cq(pre: Nic::State, post: Nic::State, c: CqId)
    requires pre.invariant(), Nic::State::destroy_cq(pre, post, c),
    ensures post.invariant(),
{
    assert forall|q: QpId| post.qps.contains_key(q) implies post.qp_wf(#[trigger] post.qps[q]) by {
        assert(pre.qp_wf(pre.qps[q]));
    }
    assert forall|c2: CqId| post.cqs.contains_key(c2) implies #[trigger] post.cq_wf(c2) by {
        lemma_cq_unchanged(pre, post, c2);
    }
    assert forall|q: QpId| post.qps.contains_key(q) implies #[trigger] post.cqes_present(q) by {
        assert(pre.qp_wf(pre.qps[q]));
        lemma_cqes_present_unchanged(pre, post, q);
    }
}

pub proof fn lemma_create_qp(pre: Nic::State, post: Nic::State, q: QpId, attr: QpAttr)
    requires pre.invariant(), Nic::State::create_qp(pre, post, q, attr),
    ensures post.invariant(),
{
    broadcast use vstd::multiset::group_multiset_axioms;
    lemma_copies(q, attr.sq_depth);
    lemma_copies(q, attr.rq_depth);
    let qp = post.qps[q];
    assert(qp.sq.take(0) =~= Seq::<SendSlot>::empty());
    lemma_count_none(qp.sq.take(0));
    assert forall|q2: QpId| post.qps.contains_key(q2) implies post.qp_wf(#[trigger] post.qps[q2]) by {
        if q2 != q {
            assert(pre.qp_wf(pre.qps[q2]));
        }
    }
    assert forall|q2: QpId| post.qps.contains_key(q2) implies #[trigger] post.cqes_present(q2) by {
        if q2 != q {
            assert(pre.cqes_present(q2));
            assert(pre.qp_wf(pre.qps[q2]));
            let qp2 = pre.qps[q2];
            assert(post.cqs[qp2.attr.send_cq].entries == pre.cqs[qp2.attr.send_cq].entries);
            assert(post.cqs[qp2.attr.recv_cq].entries == pre.cqs[qp2.attr.recv_cq].entries);
        }
    }
    assert forall|c2: CqId| post.cqs.contains_key(c2) implies #[trigger] post.cq_wf(c2) by {
        let cq = pre.cqs[c2];
        assert(pre.cq_wf(c2));
        assert(post.cqs[c2].entries == cq.entries && post.cqs[c2].used == cq.used);
        assert forall|m: int| 0 <= m < cq.entries.len() implies post.cqe_wf(c2, #[trigger] cq.entries[m]) by {
            assert(pre.cqe_wf(c2, cq.entries[m]));
        }
        assert forall|x: QpId| #[trigger] post.cqs[c2].used.count(x) == post.pending(c2, x) by {
            assert(cq.used.count(x) == pre.pending(c2, x));
        }
        assert forall|x: QpId| #[trigger] post.cqs[c2].reserved.count(x) == post.reserved_by(c2, x) by {
            assert(cq.reserved.count(x) == pre.reserved_by(c2, x));
        }
    }
}

pub proof fn lemma_modify_qp(pre: Nic::State, post: Nic::State, q: QpId, to: QpState)
    requires pre.invariant(), Nic::State::modify_qp(pre, post, q, to),
    ensures post.invariant(),
{
}

pub proof fn lemma_destroy_qp(pre: Nic::State, post: Nic::State, q: QpId)
    requires pre.invariant(), Nic::State::destroy_qp(pre, post, q),
    ensures post.invariant(),
{
    broadcast use vstd::multiset::group_multiset_axioms;
    let qp = pre.qps[q];
    assert(pre.qp_wf(qp));
    lemma_copies(q, qp.attr.sq_depth);
    lemma_copies(q, qp.attr.rq_depth);
    assert(qp.sq.take(0) =~= Seq::<SendSlot>::empty());
    lemma_count_none(qp.sq.take(0));
    assert forall|q2: QpId| post.qps.contains_key(q2) implies post.qp_wf(#[trigger] post.qps[q2]) by {
        assert(pre.qp_wf(pre.qps[q2]));
    }
    assert forall|q2: QpId| post.qps.contains_key(q2) implies #[trigger] post.cqes_present(q2) by {
        assert(pre.cqes_present(q2));
        assert(pre.qp_wf(pre.qps[q2]));
        let qp2 = pre.qps[q2];
        assert(post.cqs[qp2.attr.send_cq].entries == pre.cqs[qp2.attr.send_cq].entries);
        assert(post.cqs[qp2.attr.recv_cq].entries == pre.cqs[qp2.attr.recv_cq].entries);
    }
    assert forall|c2: CqId| post.cqs.contains_key(c2) implies #[trigger] post.cq_wf(c2) by {
        let cq = pre.cqs[c2];
        assert(pre.cq_wf(c2));
        assert(post.cqs[c2].entries == cq.entries && post.cqs[c2].used == cq.used);
        assert forall|m: int| 0 <= m < cq.entries.len() implies post.cqe_wf(c2, #[trigger] cq.entries[m]) by {
            assert(pre.cqe_wf(c2, cq.entries[m]));
        }
        assert forall|x: QpId| #[trigger] post.cqs[c2].used.count(x) == post.pending(c2, x) by {
            assert(cq.used.count(x) == pre.pending(c2, x));
        }
        assert(copies(q, qp.attr.sq_depth).subset_of(pre.cqs[qp.attr.send_cq].reserved));
        assert forall|x: QpId| #[trigger] post.cqs[c2].reserved.count(x) == post.reserved_by(c2, x) by {
            assert(cq.reserved.count(x) == pre.reserved_by(c2, x));
        }
        lemma_subset_len(post.cqs[c2].reserved, cq.reserved);
    }
}

pub proof fn lemma_post_send(pre: Nic::State, post: Nic::State, q: QpId, wrs: Seq<SendWr>)
    requires pre.invariant(), Nic::State::post_send(pre, post, q, wrs),
    ensures post.invariant(),
{
    let qp = pre.qps[q];
    let qp2 = post.qps[q];
    assert(pre.qp_wf(qp));
    assert(qp2.sq.take(qp2.sq_done as int) =~= qp.sq.take(qp.sq_done as int));
    assert forall|k: int| 0 <= k < qp.sq.len() implies #[trigger] qp2.sq[k] == qp.sq[k] by {}
    assert forall|q2: QpId| post.qps.contains_key(q2) implies post.qp_wf(#[trigger] post.qps[q2]) by {
        assert(pre.qp_wf(pre.qps[q2]));
    }
    assert forall|c2: CqId| post.cqs.contains_key(c2) implies #[trigger] post.cq_wf(c2) by {
        let cq = pre.cqs[c2];
        assert(pre.cq_wf(c2));
        assert forall|m: int| 0 <= m < cq.entries.len() implies post.cqe_wf(c2, #[trigger] cq.entries[m]) by {
            assert(pre.cqe_wf(c2, cq.entries[m]));
        }
        assert forall|x: QpId| #[trigger] cq.used.count(x) == post.pending(c2, x) by {
            assert(cq.used.count(x) == pre.pending(c2, x));
        }
        assert forall|x: QpId| #[trigger] cq.reserved.count(x) == post.reserved_by(c2, x) by {
            assert(cq.reserved.count(x) == pre.reserved_by(c2, x));
        }
    }
    assert forall|q2: QpId| post.qps.contains_key(q2) implies #[trigger] post.cqes_present(q2) by {
        assert(pre.cqes_present(q2));
    }
}

pub proof fn lemma_post_recv(pre: Nic::State, post: Nic::State, q: QpId, wrs: Seq<RecvWr>)
    requires pre.invariant(), Nic::State::post_recv(pre, post, q, wrs),
    ensures post.invariant(),
{
}

pub proof fn lemma_poll_cq(pre: Nic::State, post: Nic::State, c: CqId)
    requires pre.invariant(), Nic::State::poll_cq(pre, post, c),
    ensures post.invariant(),
{
    lemma_poll_qps(pre, post, c);
    assert forall|c2: CqId| post.cqs.contains_key(c2) implies #[trigger] post.cq_wf(c2) by {
        lemma_poll_cq_wf(pre, post, c, c2);
    }
    assert forall|q2: QpId| post.qps.contains_key(q2) implies #[trigger] post.cqes_present(q2) by {
        lemma_poll_present(pre, post, c, q2);
    }
}

/// The head CQE of CQ `c` is the first CQE its queue has waiting: every earlier unretired
/// WQE of that queue executed without producing one.
pub proof fn lemma_head_is_first(pre: Nic::State, c: CqId)
    requires
        pre.invariant(),
        pre.cqs.contains_key(c),
        pre.cqs[c].entries.len() > 0,
    ensures
        ({
            let e = pre.cqs[c].entries[0];
            let qp = pre.qps[e.qp];
            &&& pre.cqe_wf(c, e)
            &&& e.queue is Send ==> forall|j: int| 0 <= j < e.seq - qp.sq_head
                ==> !(#[trigger] qp.sq[j]).generates()
            &&& e.queue is Recv ==> e.seq == qp.rq_head
        }),
{
    let cq = pre.cqs[c];
    let e = cq.entries[0];
    assert(pre.cq_wf(c));
    assert(pre.cqe_wf(c, e));
    let q = e.qp;
    let qp = pre.qps[q];
    assert(pre.cqes_present(q));
    assert(pre.qp_wf(qp));
    if e.queue is Send {
        assert forall|j: int| 0 <= j < e.seq - qp.sq_head implies !(#[trigger] qp.sq[j]).generates() by {
            if qp.sq[j].generates() {
                let m = choose|m: int| 0 <= m < pre.cqs[qp.attr.send_cq].entries.len() && {
                    let e2 = #[trigger] pre.cqs[qp.attr.send_cq].entries[m];
                    e2.qp == q && e2.queue is Send && e2.seq == qp.sq_head + j
                };
                if m > 0 {
                    assert(cq.entries[0].seq < cq.entries[m].seq);
                }
            }
        }
    } else {
        if e.seq - qp.rq_head > 0 {
            assert(qp.rq[0].outcome is Some);
            let m = choose|m: int| 0 <= m < pre.cqs[qp.attr.recv_cq].entries.len() && {
                let e2 = #[trigger] pre.cqs[qp.attr.recv_cq].entries[m];
                e2.qp == q && e2.queue is Recv && e2.seq == qp.rq_head + 0
            };
            if m > 0 {
                assert(cq.entries[0].seq < cq.entries[m].seq);
            }
        }
    }
}

/// How many WQEs `poll_cq` retires from the queue of the head CQE.
pub open spec fn retired(pre: Nic::State, c: CqId) -> int {
    let e = pre.cqs[c].entries[0];
    let qp = pre.qps[e.qp];
    match e.queue {
        Queue::Send => e.seq + 1 - qp.sq_head,
        Queue::Recv => e.seq + 1 - qp.rq_head,
    }
}

/// After `poll_cq`, queue pairs are well formed, still registered, and the polled queue
/// pair waits for one CQE less on `c`.
proof fn lemma_poll_qps(pre: Nic::State, post: Nic::State, c: CqId)
    requires
        pre.invariant(),
        Nic::State::poll_cq(pre, post, c),
    ensures
        post.inv_qps(),
        post.inv_registered(),
        post.pending(c, pre.cqs[c].entries[0].qp) + 1 == pre.pending(c, pre.cqs[c].entries[0].qp),
{
    lemma_head_is_first(pre, c);
    let e = pre.cqs[c].entries[0];
    let q = e.qp;
    let qp = pre.qps[q];
    let qp2 = post.qps[q];
    let n = retired(pre, c);
    assert(pre.qp_wf(qp));
    if e.queue is Send {
        let k = e.seq - qp.sq_head;
        assert forall|i: int| 0 <= i < qp2.sq.len() implies #[trigger] qp2.sq[i] == qp.sq[i + n] by {}
        assert(qp.sq.take(qp.sq_done as int) =~= qp.sq.take(k).push(qp.sq[k]) + qp2.sq.take(qp2.sq_done as int));
        lemma_count_add(qp.sq.take(k).push(qp.sq[k]), qp2.sq.take(qp2.sq_done as int));
        lemma_count_push(qp.sq.take(k), qp.sq[k]);
        lemma_count_none(qp.sq.take(k));
        assert(qp2.sq_cqes() + 1 == qp.sq_cqes());
    } else {
        assert forall|i: int| 0 <= i < qp2.rq.len() implies #[trigger] qp2.rq[i] == qp.rq[i + n] by {}
        assert(qp2.sq_cqes() == qp.sq_cqes());
    }
    assert forall|q2: QpId| post.qps.contains_key(q2) implies post.qp_wf(#[trigger] post.qps[q2]) by {
        assert(pre.qp_wf(pre.qps[q2]));
    }
    assert forall|q2: QpId| #[trigger] post.qps.contains_key(q2) implies {
        let b = post.qps[q2];
        &&& forall|k: int| 0 <= k < b.sq.len() ==> post.send_wr_ok(b.attr, (#[trigger] b.sq[k]).wr)
        &&& forall|k: int| 0 <= k < b.rq.len() ==> post.recv_wr_ok(b.attr, (#[trigger] b.rq[k]).wr)
    } by {
        let a = pre.qps[q2];
        let b = post.qps[q2];
        assert forall|k: int| 0 <= k < b.sq.len() implies post.send_wr_ok(b.attr, (#[trigger] b.sq[k]).wr) by {
            if q2 == q && e.queue is Send {
                assert(b.sq[k] == a.sq[k + n]);
                assert(pre.send_wr_ok(a.attr, a.sq[k + n].wr));
            } else {
                assert(pre.send_wr_ok(a.attr, a.sq[k].wr));
            }
        }
        assert forall|k: int| 0 <= k < b.rq.len() implies post.recv_wr_ok(b.attr, (#[trigger] b.rq[k]).wr) by {
            if q2 == q && e.queue is Recv {
                assert(b.rq[k] == a.rq[k + n]);
                assert(pre.recv_wr_ok(a.attr, a.rq[k + n].wr));
            } else {
                assert(pre.recv_wr_ok(a.attr, a.rq[k].wr));
            }
        }
    }
}

/// After `poll_cq`, every CQ is well formed.
proof fn lemma_poll_cq_wf(pre: Nic::State, post: Nic::State, c: CqId, c2: CqId)
    requires
        pre.invariant(),
        Nic::State::poll_cq(pre, post, c),
        post.cqs.contains_key(c2),
    ensures
        post.cq_wf(c2),
{
    broadcast use vstd::multiset::group_multiset_axioms;
    lemma_poll_qps(pre, post, c);
    lemma_head_is_first(pre, c);
    let cq = pre.cqs[c2];
    let es = post.cqs[c2].entries;
    let e = pre.cqs[c].entries[0];
    let q = e.qp;
    let qp = pre.qps[q];
    let qp2 = post.qps[q];
    let n = retired(pre, c);
    assert(pre.cq_wf(c2));
    assert(pre.qp_wf(qp));
    // The polled queue pair's other queue is untouched.
    assert(e.queue is Send ==> qp2.rq == qp.rq && qp2.rq_head == qp.rq_head && qp2.rq_done == qp.rq_done);
    assert(e.queue is Recv ==> qp2.sq == qp.sq && qp2.sq_head == qp.sq_head && qp2.sq_done == qp.sq_done);
    assert forall|x: QpId| #[trigger] post.cqs[c2].reserved.count(x) == post.reserved_by(c2, x) by {
        assert(cq.reserved.count(x) == pre.reserved_by(c2, x));
    }
    assert forall|x: QpId| #[trigger] post.cqs[c2].used.count(x) == post.pending(c2, x) by {
        assert(cq.used.count(x) == pre.pending(c2, x));
        if x == q && c2 != c {
            assert(pre.qp_wf(qp));
        }
    }
    if c2 != c {
        assert(es == cq.entries);
        assert forall|m: int| 0 <= m < es.len() implies post.cqe_wf(c2, #[trigger] es[m]) by {
            assert(pre.cqe_wf(c2, cq.entries[m]));
        }
    } else {
        assert forall|m: int| 0 <= m < es.len() implies #[trigger] es[m] == cq.entries[m + 1] by {}
        assert forall|m: int| 0 <= m < es.len() implies post.cqe_wf(c2, #[trigger] es[m]) by {
            let e2 = cq.entries[m + 1];
            assert(es[m] == e2);
            assert(pre.cqe_wf(c2, e2));
            if e2.qp == q && e2.queue == e.queue {
                assert(cq.entries[0].seq < cq.entries[m + 1].seq);
                if e.queue is Send {
                    assert(qp2.sq[e2.seq - qp2.sq_head] == qp.sq[e2.seq - qp.sq_head]);
                } else {
                    assert(qp2.rq[e2.seq - qp2.rq_head] == qp.rq[e2.seq - qp.rq_head]);
                }
            }
        }
        assert forall|m1: int, m2: int| #![trigger es[m1], es[m2]]
            0 <= m1 < m2 < es.len() && es[m1].qp == es[m2].qp && es[m1].queue == es[m2].queue
            implies es[m1].seq < es[m2].seq by {
            assert(es[m1] == cq.entries[m1 + 1] && es[m2] == cq.entries[m2 + 1]);
        }
        assert(cq.used.count(q) >= 1);
    }
}

/// After `poll_cq`, every CQE that should wait still does.
proof fn lemma_poll_present(pre: Nic::State, post: Nic::State, c: CqId, q2: QpId)
    requires
        pre.invariant(),
        Nic::State::poll_cq(pre, post, c),
        post.qps.contains_key(q2),
    ensures
        post.cqes_present(q2),
{
    lemma_poll_present_send(pre, post, c, q2);
    lemma_poll_present_recv(pre, post, c, q2);
}

/// Where a pre-state CQE sits after `poll_cq`: one place earlier on `c`, the same elsewhere.
proof fn lemma_poll_shift(pre: Nic::State, post: Nic::State, c: CqId, x: CqId, m: int)
    requires
        Nic::State::poll_cq(pre, post, c),
        pre.cqs.contains_key(x),
        x == c ==> 0 < m < pre.cqs[x].entries.len(),
        x != c ==> 0 <= m < pre.cqs[x].entries.len(),
    ensures
        x == c ==> post.cqs[x].entries.len() + 1 == pre.cqs[x].entries.len()
            && post.cqs[x].entries[m - 1] == pre.cqs[x].entries[m],
        x != c ==> post.cqs[x] == pre.cqs[x],
{
}

proof fn lemma_poll_present_send(pre: Nic::State, post: Nic::State, c: CqId, q2: QpId)
    requires
        pre.invariant(),
        Nic::State::poll_cq(pre, post, c),
        post.qps.contains_key(q2),
    ensures
        ({
            let b = post.qps[q2];
            forall|k: int| 0 <= k < b.sq_done && (#[trigger] b.sq[k]).generates() ==>
                exists|m: int| 0 <= m < post.cqs[b.attr.send_cq].entries.len() && {
                    let e2 = #[trigger] post.cqs[b.attr.send_cq].entries[m];
                    e2.qp == q2 && e2.queue is Send && e2.seq == b.sq_head + k
                }
        }),
{
    lemma_head_is_first(pre, c);
    let e = pre.cqs[c].entries[0];
    let q = e.qp;
    let a = pre.qps[q2];
    let b = post.qps[q2];
    assert(pre.cqes_present(q2));
    assert(pre.qp_wf(a));
    let sc = a.attr.send_cq;
    let ns = if q2 == q && e.queue is Send { retired(pre, c) } else { 0 };
    assert(b.sq_head == a.sq_head + ns && b.sq_done + ns == a.sq_done && b.attr == a.attr);
    assert forall|k: int| 0 <= k < b.sq.len() implies #[trigger] b.sq[k] == a.sq[k + ns] by {}
    assert forall|k: int| 0 <= k < b.sq_done && (#[trigger] b.sq[k]).generates() implies
        exists|m: int| 0 <= m < post.cqs[sc].entries.len() && {
            let e2 = #[trigger] post.cqs[sc].entries[m];
            e2.qp == q2 && e2.queue is Send && e2.seq == b.sq_head + k
        } by {
        assert(a.sq[k + ns].generates());
        let m = choose|m: int| 0 <= m < pre.cqs[sc].entries.len() && {
            let e2 = #[trigger] pre.cqs[sc].entries[m];
            e2.qp == q2 && e2.queue is Send && e2.seq == a.sq_head + (k + ns)
        };
        if sc == c {
            assert(m != 0);
            lemma_poll_shift(pre, post, c, sc, m);
            assert(post.cqs[sc].entries[m - 1] == pre.cqs[sc].entries[m]);
        } else {
            lemma_poll_shift(pre, post, c, sc, m);
            assert(post.cqs[sc].entries[m] == pre.cqs[sc].entries[m]);
        }
    }
}

proof fn lemma_poll_present_recv(pre: Nic::State, post: Nic::State, c: CqId, q2: QpId)
    requires
        pre.invariant(),
        Nic::State::poll_cq(pre, post, c),
        post.qps.contains_key(q2),
    ensures
        ({
            let b = post.qps[q2];
            forall|k: int| 0 <= k < b.rq_done ==> #[trigger] b.rq[k].outcome is Some &&
                exists|m: int| 0 <= m < post.cqs[b.attr.recv_cq].entries.len() && {
                    let e2 = #[trigger] post.cqs[b.attr.recv_cq].entries[m];
                    e2.qp == q2 && e2.queue is Recv && e2.seq == b.rq_head + k
                }
        }),
{
    lemma_head_is_first(pre, c);
    let e = pre.cqs[c].entries[0];
    let q = e.qp;
    let a = pre.qps[q2];
    let b = post.qps[q2];
    assert(pre.cqes_present(q2));
    assert(pre.qp_wf(a));
    let rc = a.attr.recv_cq;
    let nr = if q2 == q && e.queue is Recv { retired(pre, c) } else { 0 };
    assert(b.rq_head == a.rq_head + nr && b.rq_done + nr == a.rq_done && b.attr == a.attr);
    assert forall|k: int| 0 <= k < b.rq.len() implies #[trigger] b.rq[k] == a.rq[k + nr] by {}
    assert forall|k: int| 0 <= k < b.rq_done implies #[trigger] b.rq[k].outcome is Some &&
        exists|m: int| 0 <= m < post.cqs[rc].entries.len() && {
            let e2 = #[trigger] post.cqs[rc].entries[m];
            e2.qp == q2 && e2.queue is Recv && e2.seq == b.rq_head + k
        } by {
        assert(a.rq[k + nr].outcome is Some);
        let m = choose|m: int| 0 <= m < pre.cqs[rc].entries.len() && {
            let e2 = #[trigger] pre.cqs[rc].entries[m];
            e2.qp == q2 && e2.queue is Recv && e2.seq == a.rq_head + (k + nr)
        };
        if rc == c {
            assert(m != 0);
            lemma_poll_shift(pre, post, c, rc, m);
            assert(post.cqs[rc].entries[m - 1] == pre.cqs[rc].entries[m]);
        } else {
            lemma_poll_shift(pre, post, c, rc, m);
            assert(post.cqs[rc].entries[m] == pre.cqs[rc].entries[m]);
        }
    }
}

pub proof fn lemma_execute_send(pre: Nic::State, post: Nic::State, q: QpId, o: Outcome)
    requires pre.invariant(), Nic::State::execute_send(pre, post, q, o),
    ensures post.invariant(),
{
    broadcast use vstd::multiset::group_multiset_axioms;
    let qp = pre.qps[q];
    let qp2 = post.qps[q];
    let d = qp.sq_done as int;
    let c = qp.attr.send_cq;
    lemma_room_for_send_cqe(pre, q);
    assert(pre.qp_wf(qp));
    assert(pre.cq_wf(c));
    assert(pre.cqes_present(q));
    let done = qp2.sq[d];
    let gen = done.generates();
    assert(qp2.sq.take(d + 1) =~= qp.sq.take(d).push(done));
    lemma_count_push(qp.sq.take(d), done);
    assert forall|k: int| 0 <= k < qp.sq.len() && k != d implies #[trigger] qp2.sq[k] == qp.sq[k] by {}
    let new_e = Cqe { qp: q, queue: Queue::Send, wr_id: done.wr.wr_id, outcome: o, seq: qp.sq_head + qp.sq_done };
    assert(gen ==> post.cqs[c].entries == pre.cqs[c].entries.push(new_e));
    assert(!gen ==> post.cqs == pre.cqs);
    assert forall|q2: QpId| post.qps.contains_key(q2) implies post.qp_wf(#[trigger] post.qps[q2]) by {
        assert(pre.qp_wf(pre.qps[q2]));
    }
    assert forall|c2: CqId| post.cqs.contains_key(c2) implies #[trigger] post.cq_wf(c2) by {
        let cq = pre.cqs[c2];
        assert(pre.cq_wf(c2));
        assert forall|m: int| 0 <= m < cq.entries.len() implies post.cqe_wf(c2, #[trigger] cq.entries[m]) by {
            assert(pre.cqe_wf(c2, cq.entries[m]));
        }
        assert forall|x: QpId| #[trigger] post.cqs[c2].used.count(x) == post.pending(c2, x) by {
            assert(cq.used.count(x) == pre.pending(c2, x));
        }
        assert forall|x: QpId| #[trigger] post.cqs[c2].reserved.count(x) == post.reserved_by(c2, x) by {
            assert(cq.reserved.count(x) == pre.reserved_by(c2, x));
        }
        if gen && c2 == c {
            let es = post.cqs[c].entries;
            assert(es[cq.entries.len() as int] == new_e);
            assert forall|m: int| 0 <= m < es.len() implies post.cqe_wf(c2, #[trigger] es[m]) by {
                if m < cq.entries.len() {
                    assert(es[m] == cq.entries[m]);
                    assert(pre.cqe_wf(c2, cq.entries[m]));
                }
            }
            assert forall|m1: int, m2: int| #![trigger es[m1], es[m2]]
                0 <= m1 < m2 < es.len() && es[m1].qp == es[m2].qp && es[m1].queue == es[m2].queue
                implies es[m1].seq < es[m2].seq by {
                if m2 == cq.entries.len() {
                    assert(pre.cqe_wf(c2, cq.entries[m1]));
                } else {
                    assert(es[m1] == cq.entries[m1] && es[m2] == cq.entries[m2]);
                }
            }
        }
    }
    assert forall|q2: QpId| post.qps.contains_key(q2) implies #[trigger] post.cqes_present(q2) by {
        lemma_execute_send_present(pre, post, q, o, q2);
    }
}

/// After `execute_send`, every CQE that should wait still does.
proof fn lemma_execute_send_present(pre: Nic::State, post: Nic::State, q: QpId, o: Outcome, q2: QpId)
    requires
        pre.invariant(),
        Nic::State::execute_send(pre, post, q, o),
        post.qps.contains_key(q2),
    ensures
        post.cqes_present(q2),
{
    lemma_room_for_send_cqe(pre, q);
    let qp = pre.qps[q];
    let d = qp.sq_done as int;
    let c = qp.attr.send_cq;
    let done = post.qps[q].sq[d];
    let new_e = Cqe { qp: q, queue: Queue::Send, wr_id: done.wr.wr_id, outcome: o, seq: qp.sq_head + qp.sq_done };
    let a = pre.qps[q2];
    let b = post.qps[q2];
    assert(pre.cqes_present(q2));
    assert(pre.qp_wf(a));
    let sc = a.attr.send_cq;
    let rc = a.attr.recv_cq;
    // Entries only grow, at the end of `c`.
    assert forall|x: CqId, m: int| pre.cqs.contains_key(x) && 0 <= m < pre.cqs[x].entries.len() implies
        #[trigger] post.cqs[x].entries[m] == pre.cqs[x].entries[m] && m < post.cqs[x].entries.len() by {}
    assert forall|k: int| 0 <= k < b.sq_done && (#[trigger] b.sq[k]).generates() implies
        exists|m: int| 0 <= m < post.cqs[sc].entries.len() && {
            let e = #[trigger] post.cqs[sc].entries[m];
            e.qp == q2 && e.queue is Send && e.seq == b.sq_head + k
        } by {
        if q2 == q && k == d {
            let m = pre.cqs[c].entries.len() as int;
            assert(post.cqs[sc].entries[m] == new_e);
        } else {
            assert(b.sq[k] == a.sq[k]);
            let m = choose|m: int| 0 <= m < pre.cqs[sc].entries.len() && {
                let e = #[trigger] pre.cqs[sc].entries[m];
                e.qp == q2 && e.queue is Send && e.seq == a.sq_head + k
            };
            assert(post.cqs[sc].entries[m] == pre.cqs[sc].entries[m]);
        }
    }
    assert forall|k: int| 0 <= k < b.rq_done implies #[trigger] b.rq[k].outcome is Some &&
        exists|m: int| 0 <= m < post.cqs[rc].entries.len() && {
            let e = #[trigger] post.cqs[rc].entries[m];
            e.qp == q2 && e.queue is Recv && e.seq == b.rq_head + k
        } by {
        assert(b.rq[k] == a.rq[k]);
        let m = choose|m: int| 0 <= m < pre.cqs[rc].entries.len() && {
            let e = #[trigger] pre.cqs[rc].entries[m];
            e.qp == q2 && e.queue is Recv && e.seq == a.rq_head + k
        };
        assert(post.cqs[rc].entries[m] == pre.cqs[rc].entries[m]);
    }
}

pub proof fn lemma_execute_recv(pre: Nic::State, post: Nic::State, q: QpId, o: Outcome)
    requires pre.invariant(), Nic::State::execute_recv(pre, post, q, o),
    ensures post.invariant(),
{
    broadcast use vstd::multiset::group_multiset_axioms;
    let qp = pre.qps[q];
    let qp2 = post.qps[q];
    let d = qp.rq_done as int;
    let c = qp.attr.recv_cq;
    lemma_room_for_recv_cqe(pre, q);
    assert(pre.qp_wf(qp));
    assert(pre.cq_wf(c));
    assert(pre.cqes_present(q));
    assert(qp2.sq.take(qp2.sq_done as int) =~= qp.sq.take(qp.sq_done as int));
    assert forall|k: int| 0 <= k < qp.rq.len() && k != d implies #[trigger] qp2.rq[k] == qp.rq[k] by {}
    let new_e = Cqe { qp: q, queue: Queue::Recv, wr_id: qp.rq[d].wr.wr_id, outcome: o, seq: qp.rq_head + qp.rq_done };
    assert(post.cqs[c].entries == pre.cqs[c].entries.push(new_e));
    assert forall|q2: QpId| post.qps.contains_key(q2) implies post.qp_wf(#[trigger] post.qps[q2]) by {
        assert(pre.qp_wf(pre.qps[q2]));
    }
    assert forall|c2: CqId| post.cqs.contains_key(c2) implies #[trigger] post.cq_wf(c2) by {
        let cq = pre.cqs[c2];
        assert(pre.cq_wf(c2));
        assert forall|m: int| 0 <= m < cq.entries.len() implies post.cqe_wf(c2, #[trigger] cq.entries[m]) by {
            assert(pre.cqe_wf(c2, cq.entries[m]));
        }
        assert forall|x: QpId| #[trigger] post.cqs[c2].used.count(x) == post.pending(c2, x) by {
            assert(cq.used.count(x) == pre.pending(c2, x));
        }
        assert forall|x: QpId| #[trigger] post.cqs[c2].reserved.count(x) == post.reserved_by(c2, x) by {
            assert(cq.reserved.count(x) == pre.reserved_by(c2, x));
        }
        if c2 == c {
            let es = post.cqs[c].entries;
            assert(es[cq.entries.len() as int] == new_e);
            assert forall|m: int| 0 <= m < es.len() implies post.cqe_wf(c2, #[trigger] es[m]) by {
                if m < cq.entries.len() {
                    assert(es[m] == cq.entries[m]);
                    assert(pre.cqe_wf(c2, cq.entries[m]));
                }
            }
            assert forall|m1: int, m2: int| #![trigger es[m1], es[m2]]
                0 <= m1 < m2 < es.len() && es[m1].qp == es[m2].qp && es[m1].queue == es[m2].queue
                implies es[m1].seq < es[m2].seq by {
                if m2 == cq.entries.len() {
                    assert(pre.cqe_wf(c2, cq.entries[m1]));
                } else {
                    assert(es[m1] == cq.entries[m1] && es[m2] == cq.entries[m2]);
                }
            }
        }
    }
    assert forall|q2: QpId| post.qps.contains_key(q2) implies #[trigger] post.cqes_present(q2) by {
        lemma_execute_recv_present(pre, post, q, o, q2);
    }
}

/// After `execute_recv`, every CQE that should wait still does.
proof fn lemma_execute_recv_present(pre: Nic::State, post: Nic::State, q: QpId, o: Outcome, q2: QpId)
    requires
        pre.invariant(),
        Nic::State::execute_recv(pre, post, q, o),
        post.qps.contains_key(q2),
    ensures
        post.cqes_present(q2),
{
    lemma_room_for_recv_cqe(pre, q);
    let qp = pre.qps[q];
    let d = qp.rq_done as int;
    let c = qp.attr.recv_cq;
    let new_e = Cqe { qp: q, queue: Queue::Recv, wr_id: qp.rq[d].wr.wr_id, outcome: o, seq: qp.rq_head + qp.rq_done };
    let a = pre.qps[q2];
    let b = post.qps[q2];
    assert(pre.cqes_present(q2));
    assert(pre.qp_wf(a));
    let sc = a.attr.send_cq;
    let rc = a.attr.recv_cq;
    assert forall|x: CqId, m: int| pre.cqs.contains_key(x) && 0 <= m < pre.cqs[x].entries.len() implies
        #[trigger] post.cqs[x].entries[m] == pre.cqs[x].entries[m] && m < post.cqs[x].entries.len() by {}
    assert forall|k: int| 0 <= k < b.sq_done && (#[trigger] b.sq[k]).generates() implies
        exists|m: int| 0 <= m < post.cqs[sc].entries.len() && {
            let e = #[trigger] post.cqs[sc].entries[m];
            e.qp == q2 && e.queue is Send && e.seq == b.sq_head + k
        } by {
        assert(b.sq[k] == a.sq[k]);
        let m = choose|m: int| 0 <= m < pre.cqs[sc].entries.len() && {
            let e = #[trigger] pre.cqs[sc].entries[m];
            e.qp == q2 && e.queue is Send && e.seq == a.sq_head + k
        };
        assert(post.cqs[sc].entries[m] == pre.cqs[sc].entries[m]);
    }
    assert forall|k: int| 0 <= k < b.rq_done implies #[trigger] b.rq[k].outcome is Some &&
        exists|m: int| 0 <= m < post.cqs[rc].entries.len() && {
            let e = #[trigger] post.cqs[rc].entries[m];
            e.qp == q2 && e.queue is Recv && e.seq == b.rq_head + k
        } by {
        if q2 == q && k == d {
            let m = pre.cqs[c].entries.len() as int;
            assert(post.cqs[rc].entries[m] == new_e);
        } else {
            assert(b.rq[k] == a.rq[k]);
            let m = choose|m: int| 0 <= m < pre.cqs[rc].entries.len() && {
                let e = #[trigger] pre.cqs[rc].entries[m];
                e.qp == q2 && e.queue is Recv && e.seq == a.rq_head + k
            };
            assert(post.cqs[rc].entries[m] == pre.cqs[rc].entries[m]);
        }
    }
}

pub proof fn lemma_fault(pre: Nic::State, post: Nic::State, q: QpId)
    requires pre.invariant(), Nic::State::fault(pre, post, q),
    ensures post.invariant(),
{
}

} // verus!
