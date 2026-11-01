//! Worked examples: concrete executions of A and M, checked step by step.
//!
//! They show that the transitions can actually happen (no precondition is accidentally
//! unsatisfiable, which would make the lemmas about them vacuous) and illustrate the rules:
//! a buffer's lease moves into a request and back, a waker is invoked on completion, and an
//! unsignaled write is retired, as a success, by the CQE of a later signaled one.
#![allow(unused_imports)]

use crate::abs::*;
use crate::nic::*;
use crate::types::*;
use vstd::multiset::*;
use vstd::prelude::*;

verus! {

/// A: allocate a buffer, freeze it, send it, poll once (`Pending`, registering a waker), let
/// the NIC complete it (the waker runs), take the outcome and the buffer back.
#[verifier::spinoff_prover]
pub proof fn example_send_in_a() {
    let mem = Span { start: 0, len: 64 }.bytes();
    let ok = Outcome { status: Status::Success, byte_len: 64, imm: None };

    let s0 = Abs::State {
        qps: Set::empty(),
        leases: Map::empty(),
        next_id: 0,
        outcomes: Map::empty(),
        posted: Map::empty(),
        wakes: Seq::empty(),
    };
    assert(Abs::State::start(s0));

    let s1 = Abs::State { qps: s0.qps.insert(7), ..s0 };
    assert(Abs::State::create_qp(s0, s1, 7));

    let buf = Lease { mem, unique: true, any_bytes: true, holder: Holder::Program };
    let s2 = Abs::State { leases: s1.leases.insert(0, buf), next_id: 1, ..s1 };
    assert(Abs::State::alloc(s1, s2, mem, true));

    let frozen = Lease { mem, unique: false, any_bytes: true, holder: Holder::Program };
    let s3 = Abs::State { leases: s2.leases.insert(0, frozen), ..s2 };
    assert(Abs::State::freeze(s2, s3, 0));

    let req = Req { qp: 7, dir: Dir::Out, phase: Phase::InFlight, waker: None, detached: false };
    let s4 = Abs::State {
        leases: s3.leases.remove(0).insert(1, frozen.with_req(req)),
        posted: s3.posted.insert(1, mem),
        next_id: 2,
        ..s3
    };
    assert(Abs::State::post_request(s3, s4, 0, 7, Dir::Out));

    let s5 = Abs::State { leases: s4.leases.insert(1, frozen.with_req(req.with_waker(9))), ..s4 };
    assert(Abs::State::register(s4, s5, 1, 9));

    let done = req.with_waker(9).completed(ok);
    let s6 = Abs::State {
        outcomes: s5.outcomes.insert(1, ok),
        wakes: s5.wakes.push(9),
        leases: s5.leases.insert(1, frozen.with_req(done)),
        ..s5
    };
    assert(Abs::State::complete(s5, s6, 1, ok));

    let s7 = Abs::State {
        leases: s6.leases.remove(1).insert(2, frozen.with_req(done).with_holder(Holder::Program)),
        next_id: 3,
        ..s6
    };
    assert(Abs::State::take(s6, s7, 1, ok));

    // The waker ran, and the program holds the buffer again.
    assert(s7.wakes == seq![9nat]);
    assert(s7.is_program(2) && s7.leases[2].mem == mem);
}

/// A: post a receive, drop its future before it completes. Nothing is cancelled: the
/// request keeps the buffer until the NIC completes it, and only then is the lease released.
pub proof fn example_dropped_receive_in_a() {
    let mem = Span { start: 4096, len: 4096 }.bytes();
    let got = Outcome { status: Status::Success, byte_len: 100, imm: Some(5) };
    let buf = Lease { mem, unique: true, any_bytes: true, holder: Holder::Program };
    let s0 = Abs::State {
        qps: Set::empty().insert(7),
        leases: Map::empty().insert(0, buf),
        next_id: 1,
        outcomes: Map::empty(),
        posted: Map::empty(),
        wakes: Seq::empty(),
    };

    let req = Req { qp: 7, dir: Dir::In, phase: Phase::InFlight, waker: None, detached: false };
    let s1 = Abs::State {
        leases: s0.leases.remove(0).insert(1, buf.with_req(req)),
        posted: s0.posted.insert(1, mem),
        next_id: 2,
        ..s0
    };
    assert(Abs::State::post_request(s0, s1, 0, 7, Dir::In));

    // The future is dropped: the request is still in flight and still holds the buffer.
    let s2 = Abs::State { leases: s1.leases.insert(1, buf.with_req(req.detach())), ..s1 };
    assert(Abs::State::detach(s1, s2, 1));
    assert(s2.in_flight(1) && s2.leases[1].mem == mem);
    assert(s2.nic_writes(4096));

    // The NIC completes it: the outcome is recorded and the lease released, together.
    let s3 = Abs::State { outcomes: s2.outcomes.insert(1, got), leases: s2.leases.remove(1), ..s2 };
    assert(Abs::State::complete(s2, s3, 1, got));
    assert(!s3.leases.contains_key(1) && s3.outcomes[1] == got);
}

/// M: post an unsignaled RDMA write and a signaled one with one doorbell; the NIC executes
/// both, only the second produces a CQE, and polling it retires both, the first as a success.
/// (Concrete states make big terms: this example gets its own resource limit.)
#[verifier::spinoff_prover]
#[verifier::rlimit(30)]
pub proof fn example_unsignaled_in_m() {
    broadcast use vstd::multiset::group_multiset_axioms;
    reveal_with_fuel(total_len, 2);
    let caps = Caps { max_qp_wr: 16, max_sge: 4, max_inline: 64, max_cqe: 64, max_msg: 1024 };
    let ok = Outcome { status: Status::Success, byte_len: 0, imm: None };

    let m0 = Nic::State {
        caps,
        pds: Set::empty(),
        mrs: Map::empty(),
        cqs: Map::empty(),
        qps: Map::empty(),
        retired: IMap::empty(),
    };
    assert(Nic::State::open(m0, caps));

    let m1 = Nic::State { pds: m0.pds.insert(1), ..m0 };
    assert(Nic::State::alloc_pd(m0, m1, 1));

    let mr = Mr { pd: 1, span: Span { start: 0, len: 4096 }, local_write: true, remote_read: false, remote_write: false };
    let m2 = Nic::State { mrs: m1.mrs.insert(5, mr), ..m1 };
    assert(Nic::State::reg_mr(m1, m2, 5, mr));

    let cq = Cq { cap: 16, entries: Seq::empty(), reserved: Multiset::empty(), used: Multiset::empty() };
    let m3 = Nic::State { cqs: m2.cqs.insert(3, cq), ..m2 };
    assert(Nic::State::create_cq(m2, m3, 3, 16));

    let attr = QpAttr { pd: 1, transport: Transport::Rc, send_cq: 3, recv_cq: 3, sq_depth: 4, rq_depth: 4, max_sge: 1, max_inline: 0 };
    let cqs1 = m3.cqs.insert(3, m3.cqs[3].reserve(7, 4));
    let cqs2 = cqs1.insert(3, cqs1[3].reserve(7, 4));
    crate::nic_proofs::lemma_copies(7, 4);
    assert(cqs2[3].reserved.len() == 8);
    let qp0 = Qp { attr, state: QpState::Reset, sq: Seq::empty(), sq_head: 0, sq_done: 0, rq: Seq::empty(), rq_head: 0, rq_done: 0 };
    let m4 = Nic::State { cqs: cqs2, qps: m3.qps.insert(7, qp0), ..m3 };
    assert(Nic::State::create_qp(m3, m4, 7, attr));

    let m5 = Nic::State { qps: m4.qps.insert(7, qp0.with_state(QpState::Init)), ..m4 };
    assert(Nic::State::modify_qp(m4, m5, 7, QpState::Init));
    let m6 = Nic::State { qps: m5.qps.insert(7, qp0.with_state(QpState::Rtr)), ..m5 };
    assert(Nic::State::modify_qp(m5, m6, 7, QpState::Rtr));
    let qp1 = qp0.with_state(QpState::Rts);
    let m7 = Nic::State { qps: m6.qps.insert(7, qp1), ..m6 };
    assert(Nic::State::modify_qp(m6, m7, 7, QpState::Rts));

    // Two RDMA writes from [0, 64) and [64, 128), one doorbell; only the second is signaled.
    let w1 = SendWr { wr_id: 1, opcode: Opcode::Write, sges: seq![Sge { addr: 0, len: 64, lkey: 5 }], signaled: false, inline: false };
    let w2 = SendWr { wr_id: 2, opcode: Opcode::Write, sges: seq![Sge { addr: 64, len: 64, lkey: 5 }], signaled: true, inline: false };
    let wrs = seq![w1, w2];
    let slots = wrs.map_values(|wr: SendWr| SendSlot { wr, outcome: None });
    let qp2 = Qp { sq: qp1.sq + slots, ..qp1 };
    let m8 = Nic::State { qps: m7.qps.insert(7, qp2), ..m7 };
    assert(Nic::State::post_send(m7, m8, 7, wrs));

    // The NIC executes the first: unsignaled and successful, so no CQE.
    let d1 = SendSlot { wr: w1, outcome: Some(ok) };
    assert(!d1.generates());
    let qp3 = Qp { sq: qp2.sq.update(0, d1), sq_done: 1, state: qp2.state, ..qp2 };
    let m9 = Nic::State { qps: m8.qps.insert(7, qp3), ..m8 };
    assert(Nic::State::execute_send(m8, m9, 7, ok));

    // Then the second: signaled, so a CQE for sequence number 1.
    let d2 = SendSlot { wr: w2, outcome: Some(ok) };
    let e2 = Cqe { qp: 7, queue: Queue::Send, wr_id: 2, outcome: ok, seq: 1 };
    let qp4 = Qp { sq: qp3.sq.update(1, d2), sq_done: 2, state: qp3.state, ..qp3 };
    let m10 = Nic::State { qps: m9.qps.insert(7, qp4), cqs: m9.cqs.insert(3, m9.cqs[3].push(e2)), ..m9 };
    assert(Nic::State::execute_send(m9, m10, 7, ok));

    // Polling that one CQE retires both writes.
    let qp5 = Qp { sq: qp4.sq.skip(2), sq_head: 2, sq_done: 0, ..qp4 };
    let m11 = Nic::State {
        cqs: m10.cqs.insert(3, m10.cqs[3].pop()),
        retired: m10.retired.union_prefer_right(retires(qp4, e2)),
        qps: m10.qps.insert(7, qp5),
        ..m10
    };
    assert(Nic::State::poll_cq(m10, m11, 3));

    // M's invariants hold all along, so its lemmas apply: the unsignaled write (sequence
    // number 0) was retired as a success, and the send queue is empty again.
    reveal(Nic::State::init);
    reveal(Nic::State::init_by);
    assert(Nic::State::init_by(m0, Nic::Config::open(caps)));
    crate::refinement::lemma_nic_init_invariant(m0);
    crate::nic_proofs::lemma_alloc_pd(m0, m1, 1);
    crate::nic_proofs::lemma_reg_mr(m1, m2, 5, mr);
    crate::nic_proofs::lemma_create_cq(m2, m3, 3, 16);
    crate::nic_proofs::lemma_create_qp(m3, m4, 7, attr);
    crate::nic_proofs::lemma_modify_qp(m4, m5, 7, QpState::Init);
    crate::nic_proofs::lemma_modify_qp(m5, m6, 7, QpState::Rtr);
    crate::nic_proofs::lemma_modify_qp(m6, m7, 7, QpState::Rts);
    crate::nic_proofs::lemma_post_send(m7, m8, 7, wrs);
    crate::nic_proofs::lemma_execute_send(m8, m9, 7, ok);
    crate::nic_proofs::lemma_execute_send(m9, m10, 7, ok);
    crate::nic::lemma_poll(m10, m11, 3);
    assert(!m10.qps[7].sq[0].wr.signaled);
    assert(m11.retired[WqeRef { qp: 7, queue: Queue::Send, seq: 0 }].status is Success);
    assert(m11.retired[WqeRef { qp: 7, queue: Queue::Send, seq: 1 }] == ok);
    assert(m11.qps[7].sq.len() == 0);
}

} // verus!
