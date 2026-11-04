//! Proofs that M's invariants are inductive and that its CQs never overflow.
//!
//! Nothing here is needed to read M; `nic.rs` calls these lemmas from its `#[inductive]`
//! and `assert` proofs.
#![allow(unused_imports)]

use crate::nic::*;
use crate::refinement::*;
use crate::types::*;
use vstd::multiset::*;
use vstd::prelude::*;

verus! {

/// A successful SEND or WRITE has one outcome, whether its own CQE reports it or a later
/// CQE implies it: F1 never compares a length or immediate the hardware leaves undefined.
pub proof fn lemma_ack_outcome_unique(op: Opcode, bytes: nat, a: Outcome, b: Outcome)
    requires
        !(op is Read),
        a.status is Success,
        b.status is Success,
        op.allows_outcome(bytes, a),
        op.allows_outcome(bytes, b),
    ensures
        a == b,
{
}

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
            assert(in_posting_order(post.cqs[c].entries)) by {
                reveal(in_posting_order);
            }
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

#[verifier::spinoff_prover]
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

#[verifier::spinoff_prover]
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
#[verifier::spinoff_prover]
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
    reveal(in_posting_order);
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
#[verifier::spinoff_prover]
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
/// After `poll_cq`, every CQ is well formed.
proof fn lemma_poll_cq_wf(pre: Nic::State, post: Nic::State, c: CqId, c2: CqId)
    requires
        pre.invariant(),
        Nic::State::poll_cq(pre, post, c),
        post.cqs.contains_key(c2),
    ensures
        post.cq_wf(c2),
{
    lemma_poll_cq_counts(pre, post, c, c2);
    lemma_poll_cq_entries(pre, post, c, c2);
}

/// After `poll_cq`, CQ `c2`'s bookkeeping still counts its entries and reservations.
#[verifier::spinoff_prover]
proof fn lemma_poll_cq_counts(pre: Nic::State, post: Nic::State, c: CqId, c2: CqId)
    requires
        pre.invariant(),
        Nic::State::poll_cq(pre, post, c),
        post.cqs.contains_key(c2),
    ensures
        ({
            let cq = post.cqs[c2];
            &&& cq.entries.len() == cq.used.len()
            &&& cq.reserved.len() <= cq.cap
            &&& forall|q: QpId| #[trigger] cq.used.count(q) == post.pending(c2, q)
            &&& forall|q: QpId| #[trigger] cq.reserved.count(q) == post.reserved_by(c2, q)
        }),
{
    broadcast use vstd::multiset::group_multiset_axioms;
    lemma_poll_qps(pre, post, c);
    lemma_head_is_first(pre, c);
    let cq = pre.cqs[c2];
    let e = pre.cqs[c].entries[0];
    let q = e.qp;
    let qp = pre.qps[q];
    let qp2 = post.qps[q];
    assert(pre.cq_wf(c2));
    assert(pre.qp_wf(qp));
    assert(e.queue is Send ==> qp2.rq == qp.rq && qp2.rq_head == qp.rq_head && qp2.rq_done == qp.rq_done);
    assert(e.queue is Recv ==> qp2.sq == qp.sq && qp2.sq_head == qp.sq_head && qp2.sq_done == qp.sq_done);
    assert forall|x: QpId| #[trigger] post.cqs[c2].reserved.count(x) == post.reserved_by(c2, x) by {
        assert(cq.reserved.count(x) == pre.reserved_by(c2, x));
    }
    assert forall|x: QpId| #[trigger] post.cqs[c2].used.count(x) == post.pending(c2, x) by {
        assert(cq.used.count(x) == pre.pending(c2, x));
    }
    if c2 == c {
        assert(cq.used.count(q) >= 1);
    }
}

/// After `poll_cq`, every entry of CQ `c2` still names its WQE, in order.
#[verifier::spinoff_prover]
proof fn lemma_poll_cq_entries(pre: Nic::State, post: Nic::State, c: CqId, c2: CqId)
    requires
        pre.invariant(),
        Nic::State::poll_cq(pre, post, c),
        post.cqs.contains_key(c2),
    ensures
        ({
            let es = post.cqs[c2].entries;
            &&& forall|m: int| 0 <= m < es.len() ==> post.cqe_wf(c2, #[trigger] es[m])
            &&& in_posting_order(es)
        }),
{
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
    assert(e.queue is Send ==> qp2.rq == qp.rq && qp2.rq_head == qp.rq_head && qp2.rq_done == qp.rq_done);
    assert(e.queue is Recv ==> qp2.sq == qp.sq && qp2.sq_head == qp.sq_head && qp2.sq_done == qp.sq_done);
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
                assert(cq.entries[0].seq < cq.entries[m + 1].seq) by {
                    reveal(in_posting_order);
                }
                if e.queue is Send {
                    assert(qp2.sq[e2.seq - qp2.sq_head] == qp.sq[e2.seq - qp.sq_head]);
                } else {
                    assert(qp2.rq[e2.seq - qp2.rq_head] == qp.rq[e2.seq - qp.rq_head]);
                }
            }
        }
        assert(in_posting_order(es)) by {
            reveal(in_posting_order);
            assert forall|m1: int, m2: int| #![trigger es[m1], es[m2]]
                0 <= m1 < m2 < es.len() && es[m1].qp == es[m2].qp && es[m1].queue == es[m2].queue
                implies es[m1].seq < es[m2].seq by {
                assert(es[m1] == cq.entries[m1 + 1] && es[m2] == cq.entries[m2 + 1]);
            }
        }
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

#[verifier::spinoff_prover]
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

#[verifier::spinoff_prover]
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

#[verifier::spinoff_prover]
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
            assert(in_posting_order(es)) by {
                reveal(in_posting_order);
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
    }
    assert forall|q2: QpId| post.qps.contains_key(q2) implies #[trigger] post.cqes_present(q2) by {
        lemma_execute_send_present(pre, post, q, o, q2);
    }
}

/// After `execute_send`, every CQE that should wait still does.
#[verifier::spinoff_prover]
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

#[verifier::spinoff_prover]
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
            assert(in_posting_order(es)) by {
                reveal(in_posting_order);
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
    }
    assert forall|q2: QpId| post.qps.contains_key(q2) implies #[trigger] post.cqes_present(q2) by {
        lemma_execute_recv_present(pre, post, q, o, q2);
    }
}

/// After `execute_recv`, every CQE that should wait still does.
#[verifier::spinoff_prover]
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

// =========================================================================================
// The properties `nic.rs` states

pub proof fn lemma_next(pre: Nic::State, post: Nic::State)
    requires
        pre.invariant(),
        Nic::State::next(pre, post),
    ensures
        post.invariant(),
{
    reveal(Nic::State::next);
    reveal(Nic::State::next_by);
    let step = choose|step: Nic::Step| Nic::State::next_by(pre, post, step);
    match step {
        Nic::Step::alloc_pd(pd) => lemma_alloc_pd(pre, post, pd),
        Nic::Step::reg_mr(key, mr) => lemma_reg_mr(pre, post, key, mr),
        Nic::Step::dereg_mr(key) => lemma_dereg_mr(pre, post, key),
        Nic::Step::create_cq(c, cap) => lemma_create_cq(pre, post, c, cap),
        Nic::Step::destroy_cq(c) => lemma_destroy_cq(pre, post, c),
        Nic::Step::create_qp(q, attr) => lemma_create_qp(pre, post, q, attr),
        Nic::Step::modify_qp(q, to) => lemma_modify_qp(pre, post, q, to),
        Nic::Step::destroy_qp(q) => lemma_destroy_qp(pre, post, q),
        Nic::Step::post_send(q, wrs) => lemma_post_send(pre, post, q, wrs),
        Nic::Step::post_recv(q, wrs) => lemma_post_recv(pre, post, q, wrs),
        Nic::Step::poll_cq(c) => lemma_poll_cq(pre, post, c),
        Nic::Step::execute_send(q, o) => lemma_execute_send(pre, post, q, o),
        Nic::Step::execute_recv(q, o) => lemma_execute_recv(pre, post, q, o),
        Nic::Step::fault(q) => lemma_fault(pre, post, q),
        Nic::Step::dummy_to_use_type_params(_) => {},
    }
}

pub proof fn lemma_cq_bound(s: Nic::State, c: CqId)
    requires
        s.invariant(),
        s.cqs.contains_key(c),
    ensures
        s.cqs[c].entries.len() <= s.cqs[c].cap,
{
    broadcast use vstd::multiset::group_multiset_axioms;
    let cq = s.cqs[c];
    assert(s.cq_wf(c));
    assert forall|x: QpId| cq.used.count(x) <= cq.reserved.count(x) by {
        assert(cq.used.count(x) == s.pending(c, x));
        assert(cq.reserved.count(x) == s.reserved_by(c, x));
        if s.qps.contains_key(x) {
            assert(s.qp_wf(s.qps[x]));
            lemma_sq_cqes_le(s.qps[x]);
        }
    }
    lemma_subset_len(cq.used, cq.reserved);
}

pub proof fn lemma_poll_reports(pre: Nic::State, post: Nic::State, c: CqId)
    requires
        pre.invariant(),
        Nic::State::poll_cq(pre, post, c),
    ensures
        ({
            let e = pre.cqs[c].entries[0];
            let qp = pre.qps[e.qp];
            let me = WqeRef { qp: e.qp, queue: e.queue, seq: e.seq };
            &&& pre.qps.contains_key(e.qp)
            &&& e.queue is Send ==> e.wr_id == qp.sq[e.seq - qp.sq_head].wr.wr_id
            &&& e.queue is Recv ==> e.wr_id == qp.rq[e.seq - qp.rq_head].wr.wr_id && e.seq == qp.rq_head
            &&& post.retired.contains_key(me) && post.retired[me] == e.outcome
            &&& e.queue is Send ==> forall|j: int| 0 <= j < e.seq - qp.sq_head ==> {
                let w = WqeRef { qp: e.qp, queue: Queue::Send, seq: (qp.sq_head + j) as nat };
                &&& !(#[trigger] qp.sq[j]).wr.signaled
                &&& post.retired.contains_key(w) && post.retired[w].status is Success
            }
        }),
{
    lemma_head_is_first(pre, c);
    let e = pre.cqs[c].entries[0];
    let qp = pre.qps[e.qp];
    assert(pre.qp_wf(qp));
    let fresh = retires(qp, e);
    reveal(retires);
    assert(post.retired == pre.retired.union_prefer_right(fresh));
    let me = WqeRef { qp: e.qp, queue: e.queue, seq: e.seq };
    assert(fresh.contains_key(me));
    if e.queue is Send {
        assert forall|j: int| 0 <= j < e.seq - qp.sq_head implies {
            let w = WqeRef { qp: e.qp, queue: Queue::Send, seq: (qp.sq_head + j) as nat };
            &&& !(#[trigger] qp.sq[j]).wr.signaled
            &&& post.retired.contains_key(w) && post.retired[w].status is Success
        } by {
            let w = WqeRef { qp: e.qp, queue: Queue::Send, seq: (qp.sq_head + j) as nat };
            assert(!qp.sq[j].generates());
            assert(qp.sq[j].outcome is Some);
            assert(fresh.contains_key(w));
            assert(fresh[w] == qp.sq[j].outcome->0);
        }
    }
}

pub proof fn lemma_touches_registered(s: Nic::State, a: int)
    requires
        s.invariant(),
        s.nic_may_read(a) || s.nic_may_write(a),
    ensures
        exists|key: Key| #[trigger] s.mrs.contains_key(key) && s.mrs[key].span.contains(a),
{
    let in_sq = exists|q: QpId, k: int| #![trigger s.qps[q].sq[k]] s.qps.contains_key(q)
        && 0 <= k < s.qps[q].sq.len() && !s.qps[q].sq[k].wr.inline && covers(s.qps[q].sq[k].wr.sges, a);
    let in_rq = exists|q: QpId, k: int| #![trigger s.qps[q].rq[k]] s.qps.contains_key(q)
        && 0 <= k < s.qps[q].rq.len() && covers(s.qps[q].rq[k].wr.sges, a);
    if in_sq {
        let (q, k) = choose|q: QpId, k: int| #![trigger s.qps[q].sq[k]] s.qps.contains_key(q)
            && 0 <= k < s.qps[q].sq.len() && !s.qps[q].sq[k].wr.inline && covers(s.qps[q].sq[k].wr.sges, a);
        let qp = s.qps[q];
        let wr = qp.sq[k].wr;
        assert(s.send_wr_ok(qp.attr, wr));
        let i = choose|i: int| 0 <= i < wr.sges.len() && (#[trigger] wr.sges[i]).span().contains(a);
        assert(s.registered(qp.attr.pd, wr.sges[i], wr.opcode.writes_local()));
        assert(s.mrs[wr.sges[i].lkey].span.contains(a));
    } else if in_rq {
        let (q, k) = choose|q: QpId, k: int| #![trigger s.qps[q].rq[k]] s.qps.contains_key(q)
            && 0 <= k < s.qps[q].rq.len() && covers(s.qps[q].rq[k].wr.sges, a);
        let qp = s.qps[q];
        let wr = qp.rq[k].wr;
        assert(s.recv_wr_ok(qp.attr, wr));
        let i = choose|i: int| 0 <= i < wr.sges.len() && (#[trigger] wr.sges[i]).span().contains(a);
        assert(s.registered(qp.attr.pd, wr.sges[i], true));
        assert(s.mrs[wr.sges[i].lkey].span.contains(a));
    } else if s.nic_may_write(a) {
        // An RDMA read's buffers: never inline, so covered by `in_sq` unless it is a region.
        if exists|q: QpId, k: int| #![trigger s.qps[q].sq[k]] s.qps.contains_key(q)
            && 0 <= k < s.qps[q].sq.len() && s.qps[q].sq[k].wr.opcode.writes_local()
            && covers(s.qps[q].sq[k].wr.sges, a) {
            let (q, k) = choose|q: QpId, k: int| #![trigger s.qps[q].sq[k]] s.qps.contains_key(q)
                && 0 <= k < s.qps[q].sq.len() && s.qps[q].sq[k].wr.opcode.writes_local()
                && covers(s.qps[q].sq[k].wr.sges, a);
            assert(s.send_wr_ok(s.qps[q].attr, s.qps[q].sq[k].wr));
        }
    }
}

/// The proof of `lemma_nic_steps_keep_verbs_enabled`: a NIC step changes no region, no
/// reservation, no queue length and no protection domain, and only moves a queue pair to
/// `Err` from `Rtr` or `Rts`.
pub proof fn lemma_keeps_enabled(pre: Nic::State, post: Nic::State, step: Nic::Step, verb: Nic::Step)
    requires
        nic_inv(pre),
        is_nic_step(step),
        nic_step(pre, post, step),
        is_verb(verb),
        nic_enabled(pre, verb),
    ensures
        nic_enabled(post, verb),
{
    // The CQ has room, so the step's updates are known.
    match step {
        Nic::Step::execute_send(q, o) => lemma_room_for_send_cqe(pre, q),
        Nic::Step::execute_recv(q, o) => lemma_room_for_recv_cqe(pre, q),
        _ => {},
    }
    // What every NIC step keeps.
    assert(post.caps == pre.caps && post.pds == pre.pds && post.mrs == pre.mrs);
    assert(post.retired == pre.retired);
    assert(post.qps.dom() =~= pre.qps.dom());
    assert(post.cqs.dom() =~= pre.cqs.dom());
    assert forall|q: QpId| #[trigger] post.qps.contains_key(q) implies {
        let a = pre.qps[q];
        let b = post.qps[q];
        &&& b.attr == a.attr
        &&& b.sq.len() == a.sq.len() && b.rq.len() == a.rq.len()
        &&& forall|k: int| 0 <= k < a.sq.len() ==> (#[trigger] b.sq[k]).wr == a.sq[k].wr
        &&& forall|k: int| 0 <= k < a.rq.len() ==> (#[trigger] b.rq[k]).wr == a.rq[k].wr
        &&& b.state == a.state || (b.state is Err && (a.state is Rtr || a.state is Rts || a.state is Err))
    } by {}
    assert forall|c: CqId| #[trigger] post.cqs.contains_key(c) implies {
        &&& post.cqs[c].cap == pre.cqs[c].cap
        &&& post.cqs[c].reserved == pre.cqs[c].reserved
        &&& post.cqs[c].entries.len() >= pre.cqs[c].entries.len()
    } by {}
    match verb {
        Nic::Step::post_send(q, wrs) => {
            assert forall|i: int| 0 <= i < wrs.len() implies post.send_wr_ok(post.qps[q].attr, #[trigger] wrs[i]) by {
                assert(pre.send_wr_ok(pre.qps[q].attr, wrs[i]));
            }
        },
        Nic::Step::post_recv(q, wrs) => {
            assert forall|i: int| 0 <= i < wrs.len() implies post.recv_wr_ok(post.qps[q].attr, #[trigger] wrs[i]) by {
                assert(pre.recv_wr_ok(pre.qps[q].attr, wrs[i]));
            }
        },
        Nic::Step::dereg_mr(key) => {
            if post.mr_in_use(key) {
                if exists|q: QpId, k: int| #![trigger post.qps[q].sq[k]] post.qps.contains_key(q)
                    && 0 <= k < post.qps[q].sq.len() && !post.qps[q].sq[k].wr.inline
                    && names_key(post.qps[q].sq[k].wr.sges, key) {
                    let (q, k) = choose|q: QpId, k: int| #![trigger post.qps[q].sq[k]] post.qps.contains_key(q)
                        && 0 <= k < post.qps[q].sq.len() && !post.qps[q].sq[k].wr.inline
                        && names_key(post.qps[q].sq[k].wr.sges, key);
                    assert(pre.qps[q].sq[k].wr == post.qps[q].sq[k].wr);
                } else {
                    let (q, k) = choose|q: QpId, k: int| #![trigger post.qps[q].rq[k]] post.qps.contains_key(q)
                        && 0 <= k < post.qps[q].rq.len() && names_key(post.qps[q].rq[k].wr.sges, key);
                    assert(pre.qps[q].rq[k].wr == post.qps[q].rq[k].wr);
                }
            }
        },
        Nic::Step::destroy_cq(c) => {
            assert forall|q: QpId| #[trigger] post.qps.contains_key(q)
                implies post.qps[q].attr.send_cq != c && post.qps[q].attr.recv_cq != c by {
                assert(pre.qps.contains_key(q));
            }
        },
        _ => {},
    }
}

// =========================================================================================
// A request's WQEs under M's rules

/// `pending_at` depends on the retired map and on where the send queue ends.
proof fn lemma_pending_kept(range: WqeRange, m: Nic::State, n: Nic::State, cut: nat)
    requires
        range.queue is Send,
        range.pending_at(m, cut),
        n.qps.contains_key(range.qp),
        n.qps[range.qp].sq_head == m.qps[range.qp].sq_head,
        m.qps[range.qp].sq.len() <= n.qps[range.qp].sq.len(),
        forall|s: nat| range.first <= s < range.end
            ==> (n.retired.contains_key(#[trigger] range.at(s)) <==> m.retired.contains_key(range.at(s))),
    ensures
        range.pending_at(n, cut),
{
    assert(posted(m, range.at((range.end - 1) as nat)));
    assert forall|s: nat| range.first <= s < cut implies {
        let w = #[trigger] range.at(s);
        n.retired.contains_key(w) && !posted(n, w)
    } by {
        assert(!posted(m, range.at(s)));
    }
    assert forall|s: nat| cut <= s < range.end implies {
        let w = #[trigger] range.at(s);
        posted(n, w) && !n.retired.contains_key(w)
    } by {
        assert(posted(m, range.at(s)));
    }
}

/// `errors_stick` depends on the outcomes of the request's WQEs and on `Err`.
proof fn lemma_errors_stick_kept(range: WqeRange, m: Nic::State, n: Nic::State)
    requires
        range.errors_stick(m),
        forall|s: nat| range.first <= s < range.end ==> #[trigger] range.outcome(n, s) == range.outcome(m, s),
        m.qps[range.qp].state is Err ==> n.qps[range.qp].state is Err,
    ensures
        range.errors_stick(n),
{
    assert forall|j: nat| range.first <= j < range.end && #[trigger] range.failed(n, j) implies {
        &&& n.qps[range.qp].state is Err
        &&& forall|k: nat| j < k < range.end && (#[trigger] range.outcome(n, k)) is Some
            ==> range.failed(n, k)
    } by {
        assert(range.outcome(n, j) == range.outcome(m, j));
        assert(range.failed(m, j));
        assert forall|k: nat| j < k < range.end && (#[trigger] range.outcome(n, k)) is Some
            implies range.failed(n, k) by {
            assert(range.outcome(n, k) == range.outcome(m, k));
            assert(range.failed(m, k));
        }
    }
}

/// A step that leaves alone the request's WQEs in `retired` and in the send queue of its queue
/// pair, whatever it adds after them, and keeps `Err`.
proof fn lemma_request_framed(range: WqeRange, m: Nic::State, n: Nic::State, cut: nat)
    requires
        range.queue is Send,
        range.pending_at(m, cut),
        range.errors_stick(m),
        n.qps.contains_key(range.qp),
        n.qps[range.qp].sq_head == m.qps[range.qp].sq_head,
        m.qps[range.qp].sq.len() <= n.qps[range.qp].sq.len(),
        forall|k: int| 0 <= k < m.qps[range.qp].sq.len()
            ==> #[trigger] n.qps[range.qp].sq[k] == m.qps[range.qp].sq[k],
        m.qps[range.qp].state is Err ==> n.qps[range.qp].state is Err,
        forall|s: nat| range.first <= s < range.end
            ==> (n.retired.contains_key(#[trigger] range.at(s)) <==> m.retired.contains_key(range.at(s))),
        forall|s: nat| range.first <= s < range.end && m.retired.contains_key(#[trigger] range.at(s))
            ==> n.retired[range.at(s)] == m.retired[range.at(s)],
    ensures
        range.pending_at(n, cut),
        range.errors_stick(n),
{
    lemma_pending_kept(range, m, n, cut);
    assert forall|s: nat| range.first <= s < range.end
        implies #[trigger] range.outcome(n, s) == range.outcome(m, s) by {
        assert(s < cut ==> m.retired.contains_key(range.at(s)));
        assert(s >= cut ==> posted(m, range.at(s)));
    }
    lemma_errors_stick_kept(range, m, n);
}

/// The NIC executes a WQE of the request's queue pair. In `Err` it fails, whichever it is, and
/// a failure moves the queue pair to `Err`.
proof fn lemma_request_executed(range: WqeRange, m: Nic::State, n: Nic::State, o: Outcome, cut: nat)
    requires
        range.queue is Send,
        nic_inv(m),
        Nic::State::execute_send(m, n, range.qp, o),
        range.pending_at(m, cut),
        range.errors_stick(m),
    ensures
        range.pending_at(n, cut),
        range.errors_stick(n),
{
    let q = range.qp;
    let qp = m.qps[q];
    lemma_room_for_send_cqe(m, q);
    assert(m.qp_wf(qp));
    lemma_pending_kept(range, m, n, cut);
    let done = qp.sq_head + qp.sq_done;
    assert forall|s: nat| range.first <= s < range.end && s != done
        implies #[trigger] range.outcome(n, s) == range.outcome(m, s) by {
        assert(s < cut ==> m.retired.contains_key(range.at(s)));
        assert(s >= cut ==> posted(m, range.at(s)) && n.qps[q].sq[s - qp.sq_head] == qp.sq[s - qp.sq_head]);
    }
    assert forall|j: nat| range.first <= j < range.end && #[trigger] range.failed(n, j) implies {
        &&& n.qps[q].state is Err
        &&& forall|k: nat| j < k < range.end && (#[trigger] range.outcome(n, k)) is Some
            ==> range.failed(n, k)
    } by {
        assert forall|k: nat| j < k < range.end && (#[trigger] range.outcome(n, k)) is Some
            implies range.failed(n, k) by {
            if j == done {
                // The WQE just executed: those after it have not.
                assert(posted(m, range.at(k)));
                assert(range.outcome(n, k) == range.outcome(m, k));
                assert(qp.sq[k - qp.sq_head].outcome is None);
            } else {
                // An earlier failure: the queue pair is in `Err`, so what executes now fails.
                assert(range.outcome(n, j) == range.outcome(m, j));
                assert(range.failed(m, j));
                if k == done {
                    assert(posted(m, range.at(k)));
                } else {
                    assert(range.outcome(n, k) == range.outcome(m, k));
                    assert(range.failed(m, k));
                }
            }
        }
        if j != done {
            assert(range.outcome(n, j) == range.outcome(m, j));
            assert(range.failed(m, j));
        }
    }
}

/// A CQE of the request's send queue is polled: the WQEs it retires keep their outcomes.
proof fn lemma_request_polled(range: WqeRange, m: Nic::State, n: Nic::State, c: CqId, cut: nat)
    requires
        range.queue is Send,
        nic_inv(m),
        Nic::State::poll_cq(m, n, c),
        m.cqs[c].entries[0].qp == range.qp,
        m.cqs[c].entries[0].queue is Send,
        range.pending_at(m, cut),
        range.errors_stick(m),
    ensures
        range.pending(n) && range.errors_stick(n)
            || range.all_retired(n.retired) && range.no_success_after_error(n.retired),
{
    let qp = m.qps[range.qp];
    let e = m.cqs[c].entries[0];
    lemma_head_is_first(m, c);
    assert(m.qp_wf(qp));
    reveal(retires);
    let fresh = retires(qp, e);
    assert(n.retired == m.retired.union_prefer_right(fresh));
    assert(posted(m, range.at((range.end - 1) as nat)));
    assert forall|s: nat| range.first <= s < range.end implies {
        let w = #[trigger] range.at(s);
        &&& n.retired.contains_key(w) <==> s <= e.seq || s < cut
        &&& range.outcome(n, s) == range.outcome(m, s)
    } by {
        let w = range.at(s);
        if s < cut {
            assert(!posted(m, w) && m.retired.contains_key(w));
            assert(!fresh.contains_key(w));
        } else {
            assert(posted(m, w) && !m.retired.contains_key(w));
            if s <= e.seq {
                assert(fresh.contains_key(w));
                assert(qp.sq[s - qp.sq_head].outcome is Some);
            } else {
                assert(!fresh.contains_key(w));
                assert(n.qps[range.qp].sq[s - n.qps[range.qp].sq_head] == qp.sq[s - qp.sq_head]);
            }
        }
    }
    if e.seq + 1 >= range.end {
        assert forall|s: nat| range.first <= s < range.end
            implies n.retired.contains_key(#[trigger] range.at(s)) by {}
        assert forall|j: nat, k: nat| #![trigger n.retired[range.at(j)], n.retired[range.at(k)]]
            range.first <= j < k < range.end && n.retired[range.at(j)].status.is_err()
            implies n.retired[range.at(k)].status.is_err() by {
            assert(range.outcome(n, j) == range.outcome(m, j));
            assert(range.outcome(n, k) == range.outcome(m, k));
            assert(range.failed(m, j));
        }
    } else {
        let next = if cut < e.seq + 1 { (e.seq + 1) as nat } else { cut };
        assert(range.pending_at(n, next)) by {
            assert forall|s: nat| range.first <= s < next implies {
                let w = #[trigger] range.at(s);
                n.retired.contains_key(w) && !posted(n, w)
            } by {}
            assert forall|s: nat| next <= s < range.end implies {
                let w = #[trigger] range.at(s);
                posted(n, w) && !n.retired.contains_key(w)
            } by {}
        }
        lemma_errors_stick_kept(range, m, n);
    }
}

/// **What M gives a request.** Any step of M leaves a request whose WQEs it holds with its WQEs
/// held, the ones after a failure failing too, or with all of them retired, none successful
/// after one that failed. A queue pair stays in `Err` while its queue holds a WQE, and in `Err`
/// every WQE fails. So a request's WQEs fail from its first error on, whichever they are.
pub proof fn lemma_request_step(range: WqeRange, m: Nic::State, n: Nic::State, step: Nic::Step)
    requires
        range.queue is Send,
        nic_inv(m),
        nic_step(m, n, step),
        range.pending(m),
        range.errors_stick(m),
    ensures
        range.pending(n) || range.all_retired(n.retired),
        range.pending(n) ==> range.errors_stick(n),
        range.all_retired(n.retired) ==> range.no_success_after_error(n.retired),
{
    let q = range.qp;
    let cut = choose|cut: nat| range.pending_at(m, cut);
    assert(posted(m, range.at((range.end - 1) as nat)));
    match step {
        Nic::Step::execute_send(p, o) => {
            lemma_room_for_send_cqe(m, p);
            if p == q {
                lemma_request_executed(range, m, n, o, cut);
            } else {
                lemma_request_framed(range, m, n, cut);
            }
        },
        Nic::Step::poll_cq(c) => {
            let e = m.cqs[c].entries[0];
            if e.qp == q && e.queue is Send {
                lemma_request_polled(range, m, n, c, cut);
            } else {
                lemma_head_is_first(m, c);
                reveal(retires);
                lemma_request_framed(range, m, n, cut);
            }
        },
        Nic::Step::execute_recv(p, o) => {
            lemma_room_for_recv_cqe(m, p);
            lemma_request_framed(range, m, n, cut);
        },
        _ => lemma_request_framed(range, m, n, cut),
    }
    if range.pending(n) {
        lemma_pending_last(range, n);
    }
}

// =========================================================================================
// M's history: what its rules keep along a run, beyond its invariants

/// The outcomes queue pair `q`'s queues hold are what the WRs of their WQEs allow.
pub open spec fn queued_outcomes_ok(m: Nic::State, q: QpId) -> bool {
    !m.qps.contains_key(q) || {
        let qp = m.qps[q];
        &&& forall|k: int| 0 <= k < qp.sq.len() && (#[trigger] qp.sq[k]).outcome is Some
            ==> qp.sq[k].wr.opcode.allows_outcome(total_len(qp.sq[k].wr.sges), qp.sq[k].outcome->0)
        &&& forall|k: int| 0 <= k < qp.rq.len() && (#[trigger] qp.rq[k]).outcome is Some
            ==> qp.rq[k].wr.allows_outcome(qp.rq[k].outcome->0)
    }
}

/// Every step keeps the outcomes queue pair `q`'s queues hold valid.
pub proof fn lemma_queued_outcomes_step(m: Nic::State, n: Nic::State, step: Nic::Step, q: QpId)
    requires
        nic_inv(m),
        nic_step(m, n, step),
        queued_outcomes_ok(m, q),
    ensures
        queued_outcomes_ok(n, q),
{
    // The NIC's steps expose their updates once their capacity assertions hold.
    match step {
        Nic::Step::execute_send(p, _) => lemma_room_for_send_cqe(m, p),
        Nic::Step::execute_recv(p, _) => lemma_room_for_recv_cqe(m, p),
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
                    } else {
                        assert(b.sq[k] == m.qps[q].sq[k]);
                    }
                },
                Nic::Step::poll_cq(c) => {
                    let e = m.cqs[c].entries[0];
                    if e.qp == q && e.queue is Send {
                        let old_k = k + e.seq + 1 - m.qps[q].sq_head;
                        assert(b.sq[k] == m.qps[q].sq[old_k]);
                    } else {
                        assert(b.sq[k] == m.qps[q].sq[k]);
                    }
                },
                Nic::Step::post_send(p, _) => {
                    if p == q && k >= m.qps[q].sq.len() {
                        assert(b.sq[k].outcome is None);
                    } else {
                        assert(b.sq[k] == m.qps[q].sq[k]);
                    }
                },
                _ => {
                    assert(b.sq[k] == m.qps[q].sq[k]);
                },
            }
        }
        assert forall|k: int| 0 <= k < b.rq.len() && (#[trigger] b.rq[k]).outcome is Some
            implies b.rq[k].wr.allows_outcome(b.rq[k].outcome->0) by {
            match step {
                Nic::Step::create_qp(p, _) => {},
                Nic::Step::execute_recv(p, o) => {
                    if p == q && k == m.qps[q].rq_done {
                        assert(b.rq[k].outcome == Some(o));
                    } else {
                        assert(b.rq[k] == m.qps[q].rq[k]);
                    }
                },
                Nic::Step::poll_cq(c) => {
                    let e = m.cqs[c].entries[0];
                    if e.qp == q && e.queue is Recv {
                        let old_k = k + e.seq + 1 - m.qps[q].rq_head;
                        assert(b.rq[k] == m.qps[q].rq[old_k]);
                    } else {
                        assert(b.rq[k] == m.qps[q].rq[k]);
                    }
                },
                Nic::Step::post_recv(p, _) => {
                    if p == q && k >= m.qps[q].rq.len() {
                        assert(b.rq[k].outcome is None);
                    } else {
                        assert(b.rq[k] == m.qps[q].rq[k]);
                    }
                },
                _ => {
                    assert(b.rq[k] == m.qps[q].rq[k]);
                },
            }
        }
    }
}

/// M knows WQE `w`: it is posted, or it retired.
pub open spec fn known(m: Nic::State, w: WqeRef) -> bool {
    posted(m, w) || m.retired.contains_key(w)
}

/// What M's rules keep of its history: outcomes its WRs allow; what retired lies before its
/// queue's head, and everything before that head retired.
pub open spec fn history_ok(m: Nic::State) -> bool {
    &&& forall|q: QpId| #[trigger] queued_outcomes_ok(m, q)
    &&& forall|w: WqeRef| #[trigger] m.retired.contains_key(w) && m.qps.contains_key(w.qp)
        ==> w.seq < head_of(m.qps[w.qp], w.queue)
    &&& forall|w: WqeRef| m.qps.contains_key(w.qp) && w.seq < head_of(m.qps[w.qp], w.queue)
        ==> #[trigger] m.retired.contains_key(w)
}

/// M's start has no history to speak of.
pub proof fn lemma_history_init(m: Nic::State)
    requires
        nic_init(m),
    ensures
        history_ok(m),
        wr_log(m, IMap::empty()),
{
    reveal(Nic::State::init);
    reveal(Nic::State::init_by);
    let caps = choose|caps: Caps| Nic::State::open(m, caps);
    assert(Nic::State::open(m, caps));
    assert forall|w: WqeRef| !(#[trigger] posted(m, w)) by {}
}

/// The WQEs `poll_cq` retires are those from its queue's head to the CQE's.
pub proof fn lemma_polled_wqes(m: Nic::State, n: Nic::State, c: CqId)
    requires
        nic_inv(m),
        history_ok(m),
        Nic::State::poll_cq(m, n, c),
    ensures
        ({
            let e = m.cqs[c].entries[0];
            let qp = m.qps[e.qp];
            let qp2 = n.qps[e.qp];
            let head = head_of(qp, e.queue);
            &&& m.qps.contains_key(e.qp)
            &&& head <= e.seq
            &&& head_of(qp2, e.queue) == e.seq + 1
            &&& head_of(qp2, other_queue(e.queue)) == head_of(qp, other_queue(e.queue))
            &&& n.qps.dom() == m.qps.dom()
            &&& forall|q: QpId| q != e.qp ==> #[trigger] n.qps[q] == m.qps[q]
            &&& forall|w: WqeRef| #[trigger] n.retired.contains_key(w) <==> m.retired.contains_key(w)
                || (w.qp == e.qp && w.queue == e.queue && head <= w.seq <= e.seq)
            &&& forall|w: WqeRef| #[trigger] m.retired.contains_key(w) ==> n.retired[w] == m.retired[w]
            &&& forall|w: WqeRef| #![trigger n.retired[w]]
                w.qp == e.qp && w.queue is Send && e.queue is Send && head <= w.seq <= e.seq
                ==> qp.sq[w.seq - head].outcome == Some(n.retired[w])
            &&& e.queue is Send ==> qp2.sq.len() == qp.sq.len() - (e.seq + 1 - head)
            &&& e.queue is Send ==> forall|k: int| 0 <= k < qp2.sq.len()
                ==> #[trigger] qp2.sq[k] == qp.sq[k + (e.seq + 1 - head)]
            &&& e.queue is Send ==> qp2.rq == qp.rq && qp2.rq_head == qp.rq_head
            &&& e.queue is Send ==> e.seq - head < qp.sq_done
            &&& e.queue is Recv ==> qp2.rq.len() == qp.rq.len() - (e.seq + 1 - head)
            &&& e.queue is Recv ==> qp2.sq == qp.sq && qp2.sq_head == qp.sq_head
        }),
{
    let e = m.cqs[c].entries[0];
    let qp = m.qps[e.qp];
    lemma_head_is_first(m, c);
    assert(m.qp_wf(qp));
    reveal(retires);
    let fresh = retires(qp, e);
    assert(n.retired == m.retired.union_prefer_right(fresh));
    assert forall|w: WqeRef| #[trigger] m.retired.contains_key(w) implies n.retired[w] == m.retired[w] by {
        if fresh.contains_key(w) {
            assert(w.seq < head_of(m.qps[w.qp], w.queue));
        }
    }
    assert(n.qps.dom() =~= m.qps.dom());
}

/// One step of M keeps its history; nothing that retired changes, and what M knows it keeps
/// knowing.
pub proof fn lemma_history_step(m: Nic::State, n: Nic::State, step: Nic::Step)
    requires
        nic_inv(m),
        nic_step(m, n, step),
        history_ok(m),
    ensures
        history_ok(n),
        forall|w: WqeRef| #[trigger] m.retired.contains_key(w)
            ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w],
        forall|w: WqeRef| #[trigger] known(m, w) ==> known(n, w),
{
    assert forall|q: QpId| #[trigger] queued_outcomes_ok(n, q) by {
        assert(queued_outcomes_ok(m, q));
        lemma_queued_outcomes_step(m, n, step, q);
    }
    match step {
        Nic::Step::execute_send(p, _) => lemma_room_for_send_cqe(m, p),
        Nic::Step::execute_recv(p, _) => lemma_room_for_recv_cqe(m, p),
        Nic::Step::poll_cq(c) => {
            lemma_polled_wqes(m, n, c);
            let e = m.cqs[c].entries[0];
            assert forall|w: WqeRef| #[trigger] n.retired.contains_key(w) && n.qps.contains_key(w.qp)
                implies w.seq < head_of(n.qps[w.qp], w.queue) by {
                if m.retired.contains_key(w) {
                    assert(w.seq < head_of(m.qps[w.qp], w.queue));
                }
                if w.queue != e.queue {
                    assert(w.queue == other_queue(e.queue));
                }
            }
            assert forall|w: WqeRef| n.qps.contains_key(w.qp) && w.seq < head_of(n.qps[w.qp], w.queue)
                implies #[trigger] n.retired.contains_key(w) by {
                if w.qp == e.qp && w.queue != e.queue {
                    assert(w.queue == other_queue(e.queue));
                }
                if !(w.qp == e.qp && w.queue == e.queue && head_of(m.qps[e.qp], e.queue) <= w.seq) {
                    assert(m.retired.contains_key(w));
                }
            }
            assert forall|w: WqeRef| #[trigger] known(m, w) implies known(n, w) by {
                if w.qp == e.qp && w.queue != e.queue {
                    assert(w.queue == other_queue(e.queue));
                }
            }
        },
        _ => {},
    }
}

/// `wrs` holds the WRs of the send WQEs M knows, and no other.
pub open spec fn wr_log(m: Nic::State, wrs: IMap<WqeRef, SendWr>) -> bool {
    &&& records_wrs(m, wrs)
    &&& forall|w: WqeRef| #[trigger] wrs.contains_key(w) ==> w.queue is Send && known(m, w)
}

/// One step of M: its log grows by the WRs it posts, and no entry changes.
pub proof fn lemma_wr_log_step(m: Nic::State, n: Nic::State, step: Nic::Step, wrs: IMap<WqeRef, SendWr>) -> (more: IMap<WqeRef, SendWr>)
    requires
        nic_inv(m),
        nic_step(m, n, step),
        history_ok(m),
        wr_log(m, wrs),
    ensures
        wr_log(n, more),
        wrs.submap_of(more),
{
    lemma_history_step(m, n, step);
    match step {
        Nic::Step::post_send(q, list) => {
            let qp = m.qps[q];
            let base = qp.sq_head + qp.sq.len();
            let added = IMap::new(
                |w: WqeRef| w.qp == q && w.queue is Send && base <= w.seq < base + list.len(),
                |w: WqeRef| list[w.seq - base],
            );
            let more = wrs.union_prefer_right(added);
            assert forall|w: WqeRef| #[trigger] wrs.contains_key(w) implies !added.contains_key(w) by {
                assert(known(m, w));
                if m.retired.contains_key(w) && w.qp == q {
                    assert(w.seq < head_of(m.qps[w.qp], w.queue));
                }
            }
            assert forall|w: WqeRef| #[trigger] posted(n, w) && w.queue is Send implies {
                &&& more.contains_key(w)
                &&& more[w] == n.qps[w.qp].sq[w.seq - n.qps[w.qp].sq_head].wr
            } by {
                if w.qp == q && w.seq >= base {
                    assert(added.contains_key(w));
                } else {
                    assert(posted(m, w));
                }
            }
            assert forall|w: WqeRef| #[trigger] n.retired.contains_key(w) && w.queue is Send implies {
                &&& more.contains_key(w)
                &&& more[w].opcode.allows_outcome(total_len(more[w].sges), n.retired[w])
            } by {
                assert(wrs.contains_key(w));
            }
            assert forall|w: WqeRef| #[trigger] more.contains_key(w) implies w.queue is Send && known(n, w) by {
                if wrs.contains_key(w) {
                    assert(known(m, w));
                } else {
                    assert(added.contains_key(w));
                    assert(posted(n, w));
                }
            }
            more
        },
        Nic::Step::poll_cq(c) => {
            lemma_polled_wqes(m, n, c);
            let e = m.cqs[c].entries[0];
            let qp = m.qps[e.qp];
            let head = head_of(qp, e.queue);
            assert forall|w: WqeRef| #[trigger] posted(n, w) && w.queue is Send implies {
                &&& wrs.contains_key(w)
                &&& wrs[w] == n.qps[w.qp].sq[w.seq - n.qps[w.qp].sq_head].wr
            } by {
                assert(posted(m, w));
            }
            assert forall|w: WqeRef| #[trigger] n.retired.contains_key(w) && w.queue is Send implies {
                &&& wrs.contains_key(w)
                &&& wrs[w].opcode.allows_outcome(total_len(wrs[w].sges), n.retired[w])
            } by {
                if !m.retired.contains_key(w) {
                    assert(w.qp == e.qp && w.queue == e.queue && head <= w.seq <= e.seq);
                    assert(e.queue is Send);
                    assert(posted(m, w));
                    let k = w.seq - head;
                    assert(qp.sq[k].outcome == Some(n.retired[w]));
                    assert(queued_outcomes_ok(m, e.qp));
                }
            }
            assert forall|w: WqeRef| #[trigger] wrs.contains_key(w) implies w.queue is Send && known(n, w) by {
                assert(known(m, w));
            }
            wrs
        },
        Nic::Step::execute_send(p, o) => {
            lemma_room_for_send_cqe(m, p);
            assert forall|w: WqeRef| #[trigger] posted(n, w) && w.queue is Send implies {
                &&& wrs.contains_key(w)
                &&& wrs[w] == n.qps[w.qp].sq[w.seq - n.qps[w.qp].sq_head].wr
            } by {
                assert(posted(m, w));
            }
            assert forall|w: WqeRef| #[trigger] wrs.contains_key(w) implies w.queue is Send && known(n, w) by {
                assert(known(m, w));
            }
            wrs
        },
        Nic::Step::execute_recv(p, o) => {
            lemma_room_for_recv_cqe(m, p);
            assert forall|w: WqeRef| #[trigger] posted(n, w) && w.queue is Send implies {
                &&& wrs.contains_key(w)
                &&& wrs[w] == n.qps[w.qp].sq[w.seq - n.qps[w.qp].sq_head].wr
            } by {
                assert(posted(m, w));
            }
            assert forall|w: WqeRef| #[trigger] wrs.contains_key(w) implies w.queue is Send && known(n, w) by {
                assert(known(m, w));
            }
            wrs
        },
        _ => {
            assert forall|w: WqeRef| #[trigger] posted(n, w) && w.queue is Send implies {
                &&& wrs.contains_key(w)
                &&& wrs[w] == n.qps[w.qp].sq[w.seq - n.qps[w.qp].sq_head].wr
            } by {
                assert(posted(m, w));
            }
            assert forall|w: WqeRef| #[trigger] wrs.contains_key(w) implies w.queue is Send && known(n, w) by {
                assert(known(m, w));
            }
            wrs
        },
    }
}

/// The step behind a step of M.
proof fn lemma_next_step(m: Nic::State, n: Nic::State) -> (step: Nic::Step)
    requires
        Nic::State::next(m, n),
    ensures
        nic_step(m, n, step),
{
    reveal(Nic::State::next);
    reveal(Nic::State::next_by);
    choose|step: Nic::Step| Nic::State::next_by(m, n, step)
}

/// A prefix of an execution of M is one.
proof fn lemma_run_prefix(run: Seq<Nic::State>)
    requires
        run.len() > 1,
        nic_run(run),
    ensures
        nic_run(run.drop_last()),
        run.drop_last().last() == run[run.len() - 2],
        Nic::State::next(run[run.len() - 2], run.last()),
{
    let prefix = run.drop_last();
    assert forall|i: int| 0 <= i < prefix.len() - 1 implies Nic::State::next(#[trigger] prefix[i], prefix[i + 1]) by {
        assert(prefix[i] == run[i] && prefix[i + 1] == run[i + 1]);
    }
    assert(Nic::State::next(run[run.len() - 2], run[run.len() - 2 + 1]));
}

proof fn lemma_history_run(run: Seq<Nic::State>, wrs: IMap<WqeRef, SendWr>) -> (more: IMap<WqeRef, SendWr>)
    requires
        run.len() > 0,
        nic_run(run),
        nic_inv(run[0]),
        history_ok(run[0]),
        wr_log(run[0], wrs),
    ensures
        nic_inv(run.last()),
        history_ok(run.last()),
        wr_log(run.last(), more),
        wrs.submap_of(more),
        forall|w: WqeRef| #[trigger] run[0].retired.contains_key(w)
            ==> run.last().retired.contains_key(w) && run.last().retired[w] == run[0].retired[w],
        forall|w: WqeRef| #[trigger] known(run[0], w) ==> known(run.last(), w),
    decreases run.len(),
{
    if run.len() == 1 {
        wrs
    } else {
        lemma_run_prefix(run);
        let prefix = run.drop_last();
        let log = lemma_history_run(prefix, wrs);
        let (a, b) = (prefix.last(), run.last());
        let step = lemma_next_step(a, b);
        let more = lemma_wr_log_step(a, b, step, log);
        lemma_history_step(a, b, step);
        crate::nic::lemma_step_keeps_invariant(a, b);
        assert forall|w: WqeRef| #[trigger] run[0].retired.contains_key(w)
            implies b.retired.contains_key(w) && b.retired[w] == run[0].retired[w] by {
            assert(prefix[0] == run[0]);
            assert(a.retired.contains_key(w));
        }
        assert forall|w: WqeRef| #[trigger] known(run[0], w) implies known(b, w) by {
            assert(prefix[0] == run[0]);
            assert(known(a, w));
        }
        assert(prefix[0] == run[0]);
        more
    }
}

/// A run of M keeps its history, its log only grows, and what retired stays as it retired.
pub proof fn lemma_history_reaches(m: Nic::State, n: Nic::State, wrs: IMap<WqeRef, SendWr>) -> (more: IMap<WqeRef, SendWr>)
    requires
        nic_inv(m),
        history_ok(m),
        wr_log(m, wrs),
        nic_reaches(m, n),
    ensures
        nic_inv(n),
        history_ok(n),
        wr_log(n, more),
        wrs.submap_of(more),
        forall|w: WqeRef| #[trigger] m.retired.contains_key(w)
            ==> n.retired.contains_key(w) && n.retired[w] == m.retired[w],
        forall|w: WqeRef| #[trigger] known(m, w) ==> known(n, w),
{
    let run = choose|run: Seq<Nic::State>| run.len() > 0 && run[0] == m && run.last() == n && nic_run(run);
    lemma_history_run(run, wrs)
}

proof fn lemma_request_run(range: WqeRange, run: Seq<Nic::State>)
    requires
        range.queue is Send,
        run.len() > 0,
        nic_run(run),
        nic_inv(run[0]),
        history_ok(run[0]),
        range.pending(run[0]),
        range.errors_stick(run[0]),
    ensures
        nic_inv(run.last()),
        history_ok(run.last()),
        range.pending(run.last()) && range.errors_stick(run.last())
            || range.all_retired(run.last().retired) && range.no_success_after_error(run.last().retired),
    decreases run.len(),
{
    if run.len() > 1 {
        lemma_run_prefix(run);
        let prefix = run.drop_last();
        assert(prefix[0] == run[0]);
        lemma_request_run(range, prefix);
        let (a, b) = (prefix.last(), run.last());
        let step = lemma_next_step(a, b);
        lemma_history_step(a, b, step);
        crate::nic::lemma_step_keeps_invariant(a, b);
        if range.pending(a) && range.errors_stick(a) {
            lemma_request_step(range, a, b, step);
        } else {
            assert forall|s: nat| range.first <= s < range.end
                implies b.retired.contains_key(#[trigger] range.at(s)) by {
                assert(a.retired.contains_key(range.at(s)));
            }
            assert forall|j: nat, k: nat| #![trigger b.retired[range.at(j)], b.retired[range.at(k)]]
                range.first <= j < k < range.end && b.retired[range.at(j)].status.is_err()
                implies b.retired[range.at(k)].status.is_err() by {
                assert(a.retired.contains_key(range.at(j)) && a.retired.contains_key(range.at(k)));
                assert(a.retired[range.at(j)].status.is_err());
            }
        }
    }
}

/// **What M gives a request, along a run.** A send request whose WQEs M holds, those after a
/// failure failing too, stays so; or all its WQEs retired, none successful after one that failed.
pub proof fn lemma_request_reaches(range: WqeRange, m: Nic::State, n: Nic::State)
    requires
        range.queue is Send,
        nic_inv(m),
        history_ok(m),
        nic_reaches(m, n),
        range.pending(m),
        range.errors_stick(m),
    ensures
        range.pending(n) && range.errors_stick(n)
            || range.all_retired(n.retired) && range.no_success_after_error(n.retired),
{
    let run = choose|run: Seq<Nic::State>| run.len() > 0 && run[0] == m && run.last() == n && nic_run(run);
    lemma_request_run(range, run);
}

/// A request whose last WQE is posted is pending: what lies before its queue's head retired, and
/// the rest is posted.
pub proof fn lemma_pending_of_last(range: WqeRange, m: Nic::State)
    requires
        range.wf(),
        history_ok(m),
        posted(m, range.last()),
    ensures
        range.pending(m),
{
    let qp = m.qps[range.qp];
    let head = head_of(qp, range.queue);
    let cut = if range.first <= head { head } else { range.first };
    assert forall|s: nat| range.first <= s < cut implies {
        let w = #[trigger] range.at(s);
        m.retired.contains_key(w) && !posted(m, w)
    } by {
        assert(s < head);
        assert(m.retired.contains_key(range.at(s)));
    }
    assert forall|s: nat| cut <= s < range.end implies {
        let w = #[trigger] range.at(s);
        posted(m, w) && !m.retired.contains_key(w)
    } by {
        let w = range.at(s);
        assert(head <= s);
        if m.retired.contains_key(w) {
            assert(s < head_of(m.qps[w.qp], w.queue));
        }
    }
    assert(range.pending_at(m, cut));
}

} // verus!
