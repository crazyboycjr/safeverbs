//! Worked examples: concrete executions of A and M, checked step by step.
//!
//! They show that the transitions can happen (no precondition is accidentally unsatisfiable,
//! which would make the lemmas about them vacuous) and illustrate the rules: a buffer's lease
//! moves into a request and back, a waker is invoked on completion, a dropped future keeps its
//! lease in flight, an unsignaled write is retired, as a success, by the CQE of a later signaled
//! one, and a batch completes with its first error once all its WQEs retired.
#![allow(unused_imports)]

use crate::abs::*;
use crate::guarantees::*;
use crate::nic::*;
use crate::nic_proofs::*;
use crate::refinement::*;
use crate::types::*;
use vstd::multiset::*;
use vstd::prelude::*;

verus! {

/// M distinguishes payload, local read data and remote write notifications. Undefined
/// provider lengths and immediate words are never part of the promised SQ outcome.
pub proof fn example_operation_metadata() {
    let ack = Outcome { status: Status::Success, kind: CompletionKind::StatusOnly, byte_len: 0, imm: None };
    assert(Opcode::Send.allows_outcome(64, ack));
    assert(Opcode::SendImm(7).allows_outcome(64, ack));
    assert(Opcode::Write.allows_outcome(4096, ack));
    assert(Opcode::WriteImm(7).allows_outcome(4096, ack));
    let read = Outcome { status: Status::Success, kind: CompletionKind::Read, byte_len: 64, imm: None };
    assert(Opcode::Read.allows_outcome(64, read));
    assert(!Opcode::Read.allows_outcome(65, read));
    assert(!Opcode::Write.allows_outcome(64, read));
    let notify = Outcome { status: Status::Success, kind: CompletionKind::WriteNotification, byte_len: 4096, imm: Some(7) };
    let empty = RecvWr { wr_id: 0, sges: Seq::empty() };
    assert(empty.allows_outcome(notify));
    let payload = Outcome { kind: CompletionKind::Receive, ..notify };
    reveal(total_len);
    assert(!empty.allows_outcome(payload));
    let failed = Outcome { status: Status::RemoteErr, ..ack };
    assert(Opcode::Read.allows_outcome(64, failed));
    assert(empty.allows_outcome(failed));
}

/// A request that has just been posted.
spec fn fresh_req() -> Req {
    Req { qp: 7, phase: Phase::InFlight, waker: None, detached: false }
}

/// A: allocate a buffer, freeze it, send it, poll once (`Pending`, registering a waker), let
/// the NIC complete it (the waker runs), take the outcome and the buffer back.
#[verifier::spinoff_prover]
pub proof fn example_send_in_a() {
    let mem = Span { start: 0, len: 64 }.bytes();
    let ok = Outcome { status: Status::Success, kind: CompletionKind::StatusOnly, byte_len: 0, imm: None };

    let s0 = Abs::State {
        qps: Set::empty(),
        leases: Map::empty(),
        requests: Map::empty(),
        next_id: 0,
        outcomes: Map::empty(),
        wakes: Seq::empty(),
    };
    assert(Abs::State::start(s0));

    let s1 = Abs::State { qps: s0.qps.insert(7), ..s0 };
    assert(Abs::State::create_qp(s0, s1, 7));

    let buf = Lease { mem, unique: true, any_bytes: true, holder: Holder::Program };
    let s2 = Abs::State { leases: s1.leases.insert(0, buf), next_id: 1, ..s1 };
    assert(Abs::State::alloc(s1, s2, mem, true));

    let frozen = Lease { unique: false, ..buf };
    let s3 = Abs::State { leases: s2.leases.insert(0, frozen), ..s2 };
    assert(Abs::State::freeze(s2, s3, 0));

    let dirs = Map::empty().insert(0nat, Dir::Out);
    let s4 = Abs::State {
        requests: s3.requests.insert(1, fresh_req()),
        leases: s3.leases_in_request(dirs, 1),
        next_id: 2,
        ..s3
    };
    assert(Abs::State::post_request(s3, s4, dirs, 7));
    assert(s4.place(0) == Place::InFlight(1));

    let s5 = Abs::State { requests: s4.requests.insert(1, fresh_req().with_waker(9)), ..s4 };
    assert(Abs::State::register(s4, s5, 1, 9));

    let s6 = Abs::State {
        outcomes: s5.outcomes.insert(1, ok),
        wakes: s5.wakes.push(9),
        requests: s5.requests.insert(1, fresh_req().with_waker(9).completed(ok)),
        ..s5
    };
    assert(Abs::State::complete(s5, s6, 1, ok));
    assert(s6.place(0) == Place::Completed(1));

    let s7 = Abs::State { requests: s6.requests.remove(1), leases: s6.leases_returned(1), ..s6 };
    assert(Abs::State::take(s6, s7, 1, ok));

    // The waker ran, and the program holds the buffer again, under its own name.
    assert(s7.wakes == seq![9nat]);
    assert(s7.is_program(0) && s7.leases[0].mem == mem);
}

/// A: post a receive, drop its future before it completes. Nothing is cancelled: the
/// request keeps the buffer until the NIC completes it, and only then is the lease released.
pub proof fn example_dropped_receive_in_a() {
    let mem = Span { start: 4096, len: 4096 }.bytes();
    let got = Outcome { status: Status::Success, kind: CompletionKind::Receive, byte_len: 100, imm: Some(5) };
    let buf = Lease { mem, unique: true, any_bytes: true, holder: Holder::Program };
    let s0 = Abs::State {
        qps: Set::empty().insert(7),
        leases: Map::empty().insert(0, buf),
        requests: Map::empty(),
        next_id: 1,
        outcomes: Map::empty(),
        wakes: Seq::empty(),
    };

    let dirs = Map::empty().insert(0nat, Dir::In);
    let s1 = Abs::State {
        requests: s0.requests.insert(1, fresh_req()),
        leases: s0.leases_in_request(dirs, 1),
        next_id: 2,
        ..s0
    };
    assert(Abs::State::post_request(s0, s1, dirs, 7));
    assert(s0.invariant());
    Abs::State::post_request_inductive(s0, s1, dirs, 7);

    // The future is dropped: the request is still in flight and still holds the buffer.
    let s2 = Abs::State { requests: s1.requests.insert(1, fresh_req().detach()), ..s1 };
    assert(Abs::State::detach(s1, s2, 1));
    Abs::State::detach_inductive(s1, s2, 1);
    assert(s2.place(0) == Place::InFlight(1));
    assert(s2.leases.contains_key(0) && s2.leases[0].mem.contains(4096) && s2.nic_writes(4096));

    // The NIC completes it: the outcome is recorded and the lease released, together (R1).
    let s3 = Abs::State { outcomes: s2.outcomes.insert(1, got), requests: s2.requests.remove(1), leases: s2.leases_but(1), ..s2 };
    assert(abs_step(s2, s3, Abs::Step::complete(1, got)));
    lemma_abs_step_is_next(s2, s3, Abs::Step::complete(1, got));
    lemma_r1(s2, s3, 0);
    assert(s3.place(0) is Released && s3.outcomes[1] == got);
}

/// QP 7 of the M examples, connected: RC, in protection domain 1, room for four sends and
/// four receives, both completing on CQ 3.
spec fn qp7() -> Qp {
    Qp {
        attr: QpAttr {
            pd: 1, transport: Transport::Rc, send_cq: 3, recv_cq: 3,
            sq_depth: 4, rq_depth: 4, max_sge: 1, max_inline: 0,
        },
        state: QpState::Rts,
        sq: Seq::empty(), sq_head: 0, sq_done: 0,
        rq: Seq::empty(), rq_head: 0, rq_done: 0,
    }
}

/// M with QP 7 connected, CQ 3 (16 entries) reserving its eight completions, and region 5
/// covering bytes `[0, 4096)`, writable locally.
spec fn connected() -> Nic::State {
    let cq = Cq { cap: 16, entries: Seq::empty(), reserved: Multiset::empty(), used: Multiset::empty() };
    let mr = Mr { pd: 1, span: Span { start: 0, len: 4096 }, local_write: true, remote_read: false, remote_write: false };
    Nic::State {
        caps: Caps { max_qp_wr: 16, max_sge: 4, max_inline: 64, max_cqe: 64, max_msg: 1024 },
        pds: Set::empty().insert(1),
        mrs: Map::empty().insert(5, mr),
        cqs: Map::empty().insert(3, cq.reserve(7, 4).reserve(7, 4)),
        qps: Map::empty().insert(7, qp7()),
        retired: IMap::empty(),
    }
}

/// M reaches `connected()` from its start by legal verbs: opening the device, allocating
/// the protection domain, registering the region, creating the CQ and the queue pair and
/// connecting it. So M's invariants hold there.
#[verifier::spinoff_prover]
#[verifier::rlimit(30)]
proof fn lemma_connected()
    ensures
        connected().invariant(),
{
    broadcast use vstd::multiset::group_multiset_axioms;
    let s = connected();
    let m0 = Nic::State { pds: Set::empty(), mrs: Map::empty(), cqs: Map::empty(), qps: Map::empty(), ..s };
    assert(Nic::State::open(m0, s.caps));
    reveal(Nic::State::init);
    reveal(Nic::State::init_by);
    assert(Nic::State::init_by(m0, Nic::Config::open(s.caps)));
    lemma_nic_init_invariant(m0);
    let m1 = Nic::State { pds: s.pds, ..m0 };
    assert(Nic::State::alloc_pd(m0, m1, 1));
    lemma_alloc_pd(m0, m1, 1);
    let m2 = Nic::State { mrs: s.mrs, ..m1 };
    assert(Nic::State::reg_mr(m1, m2, 5, s.mrs[5]));
    lemma_reg_mr(m1, m2, 5, s.mrs[5]);
    let cq = Cq { cap: 16, entries: Seq::empty(), reserved: Multiset::empty(), used: Multiset::empty() };
    let m3 = Nic::State { cqs: Map::empty().insert(3, cq), ..m2 };
    assert(Nic::State::create_cq(m2, m3, 3, 16));
    lemma_create_cq(m2, m3, 3, 16);
    let qp = qp7().with_state(QpState::Reset);
    let cqs1 = m3.cqs.insert(3, m3.cqs[3].reserve(7, 4));
    assert(cqs1.insert(3, cqs1[3].reserve(7, 4)) =~= s.cqs);
    lemma_copies(7, 4);
    let m4 = Nic::State { cqs: s.cqs, qps: Map::empty().insert(7, qp), ..m3 };
    assert(Nic::State::create_qp(m3, m4, 7, qp.attr));
    lemma_create_qp(m3, m4, 7, qp.attr);
    let m5 = Nic::State { qps: m4.qps.insert(7, qp.with_state(QpState::Init)), ..m4 };
    assert(Nic::State::modify_qp(m4, m5, 7, QpState::Init));
    lemma_modify_qp(m4, m5, 7, QpState::Init);
    let m6 = Nic::State { qps: m5.qps.insert(7, qp.with_state(QpState::Rtr)), ..m5 };
    assert(Nic::State::modify_qp(m5, m6, 7, QpState::Rtr));
    lemma_modify_qp(m5, m6, 7, QpState::Rtr);
    let m7 = Nic::State { qps: m6.qps.insert(7, qp7()), ..m6 };
    assert(Nic::State::modify_qp(m6, m7, 7, QpState::Rts));
    lemma_modify_qp(m6, m7, 7, QpState::Rts);
    assert(m7.qps =~= s.qps);
    assert(m7 == s);
}

/// Two RDMA writes, from `[0, 64)` and `[64, 128)`, posted with one doorbell; only the
/// second asks for a completion.
spec fn two_writes() -> Seq<SendWr> {
    seq![
        SendWr { wr_id: 1, opcode: Opcode::Write, sges: seq![Sge { addr: 0, len: 64, lkey: 5 }], signaled: false, inline: false },
        SendWr { wr_id: 2, opcode: Opcode::Write, sges: seq![Sge { addr: 64, len: 64, lkey: 5 }], signaled: true, inline: false },
    ]
}

/// M: the NIC executes the two writes, only the second produces a CQE, and polling it
/// retires both, the first as a success.
#[verifier::spinoff_prover]
#[verifier::rlimit(30)]
pub proof fn example_unsignaled_in_m() {
    reveal_with_fuel(total_len, 2);
    let ok = Outcome { status: Status::Success, kind: CompletionKind::StatusOnly, byte_len: 0, imm: None };
    let (m0, wrs) = (connected(), two_writes());
    lemma_connected();
    let qp1 = Qp { sq: wrs.map_values(|wr: SendWr| SendSlot { wr, outcome: None }), ..qp7() };
    let m1 = Nic::State { qps: m0.qps.insert(7, qp1), ..m0 };
    assert(Nic::State::post_send(m0, m1, 7, wrs));
    crate::nic_proofs::lemma_post_send(m0, m1, 7, wrs);

    // The first: unsignaled and successful, so no CQE.
    let qp2 = Qp { sq: qp1.sq.update(0, SendSlot { wr: wrs[0], outcome: Some(ok) }), sq_done: 1, ..qp1 };
    let m2 = Nic::State { qps: m1.qps.insert(7, qp2), ..m1 };
    assert(Nic::State::execute_send(m1, m2, 7, ok));
    crate::nic_proofs::lemma_execute_send(m1, m2, 7, ok);

    // The second: signaled, so a CQE for sequence number 1.
    let e = Cqe { qp: 7, queue: Queue::Send, wr_id: 2, outcome: ok, seq: 1 };
    let qp3 = Qp { sq: qp2.sq.update(1, SendSlot { wr: wrs[1], outcome: Some(ok) }), sq_done: 2, ..qp2 };
    let m3 = Nic::State { qps: m2.qps.insert(7, qp3), cqs: m2.cqs.insert(3, m2.cqs[3].push(e)), ..m2 };
    assert(Nic::State::execute_send(m2, m3, 7, ok));
    crate::nic_proofs::lemma_execute_send(m2, m3, 7, ok);

    // Polling that one CQE retires both writes: by M's lemma, the unsignaled one (sequence
    // number 0) succeeded, and the send queue is empty again.
    let m4 = Nic::State {
        qps: m3.qps.insert(7, Qp { sq: qp3.sq.skip(2), sq_head: 2, sq_done: 0, ..qp3 }),
        cqs: m3.cqs.insert(3, m3.cqs[3].pop()),
        retired: m3.retired.union_prefer_right(retires(qp3, e)),
        ..m3
    };
    assert(Nic::State::poll_cq(m3, m4, 3));
    crate::nic::lemma_poll(m3, m4, 3);
    assert(!m3.qps[7].sq[0].wr.signaled);
    assert(m4.retired[WqeRef { qp: 7, queue: Queue::Send, seq: 0 }].status is Success);
    assert(m4.retired[WqeRef { qp: 7, queue: Queue::Send, seq: 1 }] == ok);
    assert(m4.qps[7].sq.len() == 0);
}

/// M: the two writes are one request (a batch) and the first fails. Its CQE retires only a
/// prefix of the batch, which stays in flight. The second write flushes, and once it
/// retires too, the batch's outcome is its first error, not the last write's flush.
#[verifier::spinoff_prover]
#[verifier::rlimit(30)]
pub proof fn example_batch_first_error() {
    reveal_with_fuel(total_len, 2);
    reveal(retires);
    let remote = Outcome { status: Status::RemoteErr, kind: CompletionKind::StatusOnly, byte_len: 0, imm: None };
    let flush = Outcome { status: Status::FlushErr, ..remote };
    let range = WqeRange { qp: 7, queue: Queue::Send, first: 0, end: 2 };
    let (m0, wrs) = (connected(), two_writes());
    lemma_connected();
    let qp1 = Qp { sq: wrs.map_values(|wr: SendWr| SendSlot { wr, outcome: None }), ..qp7() };
    let m1 = Nic::State { qps: m0.qps.insert(7, qp1), ..m0 };
    assert(Nic::State::post_send(m0, m1, 7, wrs));
    crate::nic_proofs::lemma_post_send(m0, m1, 7, wrs);
    assert(range.pending_at(m1, 0));

    // A failed WQE produces a CQE even unsignaled, and moves the QP to `Err`.
    let e1 = Cqe { qp: 7, queue: Queue::Send, wr_id: 1, outcome: remote, seq: 0 };
    let qp2 = Qp { sq: qp1.sq.update(0, SendSlot { wr: wrs[0], outcome: Some(remote) }), sq_done: 1, state: QpState::Err, ..qp1 };
    let m2 = Nic::State { qps: m1.qps.insert(7, qp2), cqs: m1.cqs.insert(3, m1.cqs[3].push(e1)), ..m1 };
    assert(Nic::State::execute_send(m1, m2, 7, remote));
    crate::nic_proofs::lemma_execute_send(m1, m2, 7, remote);

    // Polling it retires the first write only: the batch is still in flight.
    let qp3 = Qp { sq: qp2.sq.skip(1), sq_head: 1, sq_done: 0, ..qp2 };
    let m3 = Nic::State {
        qps: m2.qps.insert(7, qp3), cqs: m2.cqs.insert(3, m2.cqs[3].pop()),
        retired: m2.retired.union_prefer_right(retires(qp2, e1)), ..m2
    };
    assert(Nic::State::poll_cq(m2, m3, 3));
    crate::nic_proofs::lemma_poll_cq(m2, m3, 3);
    assert(m3.retired.contains_key(range.at(0)) && !m3.retired.contains_key(range.at(1)));
    assert(range.pending_at(m3, 1));

    // The second write flushes. Once it retires, the batch's outcome is `RemoteErr`.
    let e2 = Cqe { qp: 7, queue: Queue::Send, wr_id: 2, outcome: flush, seq: 1 };
    let qp4 = Qp { sq: qp3.sq.update(0, SendSlot { wr: wrs[1], outcome: Some(flush) }), sq_done: 1, ..qp3 };
    let m4 = Nic::State { qps: m3.qps.insert(7, qp4), cqs: m3.cqs.insert(3, m3.cqs[3].push(e2)), ..m3 };
    assert(Nic::State::execute_send(m3, m4, 7, flush));
    crate::nic_proofs::lemma_execute_send(m3, m4, 7, flush);
    let m5 = Nic::State {
        qps: m4.qps.insert(7, Qp { sq: qp4.sq.skip(1), sq_head: 2, sq_done: 0, ..qp4 }),
        cqs: m4.cqs.insert(3, m4.cqs[3].pop()),
        retired: m4.retired.union_prefer_right(retires(qp4, e2)), ..m4
    };
    assert(Nic::State::poll_cq(m4, m5, 3));
    assert(m5.retired[range.at(0)] == remote && m5.retired[range.last()] == flush);
    assert(range.all_retired(m5.retired)) by {
        assert forall|s: nat| range.first <= s < range.end
            implies m5.retired.contains_key(#[trigger] range.at(s)) by { assert(s == 0 || s == 1); }
    }
    assert(range.first_error(m5.retired, 0));
    crate::refinement::lemma_aggregate_first_error(range, m5.retired, 0);
    assert(range.aggregate(m5.retired) == remote);
}

/// A: one request holds a READ destination and a frozen SEND source, of which the program
/// keeps a clone. The NIC writes the one and reads the other while the program may read the
/// clone. Taking the outcome returns each lease under its own name: the destination is the
/// program's to write again, the source stays shared.
#[verifier::spinoff_prover]
#[verifier::rlimit(20)]
pub proof fn example_mixed_request() {
    let source = Lease { mem: Span { start: 0, len: 64 }.bytes(), unique: false, any_bytes: true, holder: Holder::Program };
    let destination = Lease { mem: Span { start: 128, len: 64 }.bytes(), unique: true, any_bytes: true, holder: Holder::Program };
    let remote = Outcome { status: Status::RemoteErr, kind: CompletionKind::StatusOnly, byte_len: 0, imm: None };
    let a0 = Abs::State {
        qps: Set::empty().insert(7),
        leases: Map::empty().insert(0, source).insert(1, destination).insert(2, source),
        requests: Map::empty(),
        next_id: 3,
        outcomes: Map::empty(),
        wakes: Seq::empty(),
    };
    assert(abs_inv(a0));
    let dirs = Map::empty().insert(0nat, Dir::Out).insert(1nat, Dir::In);
    assert(a0.may_post(dirs));
    assert(dirs.insert(0, Dir::In).contains_key(0) && !a0.may_post(dirs.insert(0, Dir::In)));
    let a1 = Abs::State {
        requests: a0.requests.insert(3, fresh_req()),
        leases: a0.leases_in_request(dirs, 3),
        next_id: 4,
        ..a0
    };
    assert(Abs::State::post_request(a0, a1, dirs, 7));
    Abs::State::post_request_inductive(a0, a1, dirs, 7);

    // S1: the NIC writes byte 128 and reads byte 0, which the program reads too, through the clone.
    assert(a1.leases.contains_key(0) && a1.leases.contains_key(1) && a1.leases.contains_key(2));
    assert(a1.leases[1].mem.contains(128) && a1.nic_writes_lease(1) && a1.nic_writes(128));
    assert(a1.leases[0].mem.contains(0) && a1.nic_reads_lease(0) && a1.nic_reads(0));
    assert(a1.leases[2].mem.contains(0) && a1.program_reads(0));
    lemma_s1_no_conflict(a1, 128);
    assert(!a1.program_reads(128));

    let a2 = Abs::State {
        outcomes: a1.outcomes.insert(3, remote),
        requests: a1.requests.insert(3, fresh_req().completed(remote)),
        ..a1
    };
    assert(Abs::State::complete(a1, a2, 3, remote));
    Abs::State::complete_inductive(a1, a2, 3, remote);
    let a3 = Abs::State { requests: a2.requests.remove(3), leases: a2.leases_returned(3), ..a2 };
    assert(Abs::State::take(a2, a3, 3, remote));
    lemma_f1_take(a2, a3, 3, remote);
    assert(a3.leases.contains_key(1) && a3.leases[1].mem.contains(128) && a3.program_writes(128));
    assert(!a3.leases[0].unique && !a3.leases[2].unique);
}

} // verus!
