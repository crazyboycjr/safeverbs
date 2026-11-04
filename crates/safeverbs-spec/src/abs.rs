//! **A**: the abstract SafeVerbs machine, the specification users rely on.
//!
//! A knows nothing about queues, doorbells, CQEs or ring slots. It sees *leases* and
//! *requests*. A lease is a set of bytes and what may be done to them, with one holder at a
//! time: the program (a `BufMut` is a unique lease, every `Buf` view a shared one), a posted
//! request, or the peer (a region registered for remote access). A request holds the leases it
//! was posted with, each with its own direction, so one batch may hold exclusive READ
//! destinations and shared send sources. Every client action of the library is a transition of
//! A; the NIC's only move is `complete`, which gives a request its outcome.
//!
//! The guarantees S1, F1, R1 and L1 are invariants of this machine (proved here and in
//! [`crate::guarantees`]); the refinement theorem ([`crate::refinement`]) states what the
//! library proves to behave like A.
//!
//! The module is called `abs` because `abstract` is a reserved word in Rust. The proof that
//! its invariants are inductive is in [`crate::abs_proofs`].
#![allow(unused_imports)]

use crate::types::*;
use verus_state_machines_macros::state_machine;
use vstd::prelude::*;

verus! {

/// Names a lease. Never reused: every new name takes `next_id`.
pub type LeaseId = nat;

/// Names a request. Never reused: every new name takes `next_id`.
pub type ReqId = nat;

/// Names a `core::task::Waker` (two wakers of the same task may share a name).
pub type WakerId = nat;

/// Which way the NIC touches a lease of a request.
pub enum Dir {
    /// The NIC reads it: a send, the source of an RDMA write.
    Out,
    /// The NIC writes it: a receive, the target of an RDMA read.
    In,
}

/// Where a request is.
pub enum Phase {
    /// Posted; the NIC may be accessing its leases.
    InFlight,
    /// The NIC is done and produced this outcome; not yet taken by the client.
    Complete(Outcome),
}

/// A request posted to a queue pair: one future (or one entry of a stream). It has one outcome
/// and one waker, whatever number of leases it holds.
pub struct Req {
    pub qp: QpId,
    pub phase: Phase,
    /// The waker the future registered when it last returned `Pending`.
    pub waker: Option<WakerId>,
    /// The future was dropped. Dropping never cancels: the request stays in flight and its
    /// leases are released when it completes.
    pub detached: bool,
}

/// Who holds a lease.
pub enum Holder {
    /// The program: through a `BufMut` (a unique lease) or a `Buf` view.
    Program,
    /// Request `req`, until the client takes its outcome (or, if detached, until it completes).
    /// While it is in flight, the NIC touches the lease as `dir` says.
    Request { req: ReqId, dir: Dir },
    /// The peer may access it by rkey: read it, and write it too if `remote_write`. The program
    /// holds no view of it: its one way in is the volatile copy, S1's one exception.
    Exposed { remote_write: bool },
}

/// A set of bytes, what may be done to them, and who holds them. A shared clone is another
/// lease, even when it covers exactly the same bytes.
pub struct Lease {
    pub mem: ISet<int>,
    /// No other lease overlaps it (a `BufMut`, or what one became).
    pub unique: bool,
    /// The element type accepts every bit pattern (`zerocopy::FromBytes`), so the NIC may
    /// write anything into it.
    pub any_bytes: bool,
    pub holder: Holder,
}

/// Where a lease is (R1). A lease is in one place at a time: [`Abs::State::place`] is a function.
pub enum Place {
    /// With the program.
    Program,
    /// With the peer, by rkey.
    Exposed,
    /// In request `r`, in flight: the NIC may be using it. A device that has not flushed keeps
    /// it here.
    InFlight(ReqId),
    /// In request `r`, complete: the NIC is done with it, and the client has not taken it yet.
    Completed(ReqId),
    /// Released, for good. (A name not given yet has no lease either.)
    Released,
}

impl Lease {
    /// A request or the peer may hold it for the NIC to write: it is unique and accepts any
    /// bytes.
    pub open spec fn writable(self) -> bool {
        self.unique && self.any_bytes
    }

    /// Request `r` holds it.
    pub open spec fn held_by(self, r: ReqId) -> bool {
        self.holder is Request && self.holder->req == r
    }

    pub open spec fn with_holder(self, holder: Holder) -> Lease {
        Lease { holder, ..self }
    }

    /// The same bytes and permissions, whoever holds them.
    pub open spec fn same_as(self, other: Lease) -> bool {
        self.mem == other.mem && self.unique == other.unique && self.any_bytes == other.any_bytes
    }
}

impl Req {
    pub open spec fn completed(self, o: Outcome) -> Req {
        Req { phase: Phase::Complete(o), waker: None, ..self }
    }

    pub open spec fn with_waker(self, w: WakerId) -> Req {
        Req { waker: Some(w), ..self }
    }

    pub open spec fn detach(self) -> Req {
        Req { waker: None, detached: true, ..self }
    }
}

} // verus!

state_machine! {
    Abs {
        fields {
            /// Queue pairs that exist.
            pub qps: Set<QpId>,
            /// Every lease not released, by name.
            pub leases: Map<LeaseId, Lease>,
            /// Every request not taken or released, by name.
            pub requests: Map<ReqId, Req>,
            /// The next fresh name.
            pub next_id: nat,
            /// Every outcome ever produced, by request: written once (F1).
            pub outcomes: Map<ReqId, Outcome>,
            /// Every waker invocation, in order (L1).
            pub wakes: Seq<WakerId>,
        }

        // ---------------------------------------------------------------------------------
        // Helpers

        pub open spec fn is_program(self, l: LeaseId) -> bool {
            self.leases.contains_key(l) && self.leases[l].holder is Program
        }

        pub open spec fn in_flight(self, r: ReqId) -> bool {
            self.requests.contains_key(r) && self.requests[r].phase is InFlight
        }

        /// `mem` overlaps no lease but (possibly) `except`.
        pub open spec fn free_except(self, mem: ISet<int>, except: Option<LeaseId>) -> bool {
            forall|l: LeaseId| #[trigger] self.leases.contains_key(l) && except != Some(l)
                ==> self.leases[l].mem.disjoint(mem)
        }

        /// These leases may go into a request, in these directions: the program holds them,
        /// and the NIC may write only a unique lease whose type accepts any bytes.
        pub open spec fn may_post(self, dirs: Map<LeaseId, Dir>) -> bool {
            forall|l: LeaseId| #[trigger] dirs.contains_key(l) ==> self.is_program(l)
                && (dirs[l] is In ==> self.leases[l].writable())
        }

        /// The leases, with those of `dirs` in request `r`.
        pub open spec fn leases_in_request(self, dirs: Map<LeaseId, Dir>, r: ReqId) -> Map<LeaseId, Lease> {
            self.leases.map_entries(|l: LeaseId, lease: Lease|
                if dirs.contains_key(l) {
                    lease.with_holder(Holder::Request { req: r, dir: dirs[l] })
                } else {
                    lease
                })
        }

        /// The leases, with those of request `r` back with the program.
        pub open spec fn leases_returned(self, r: ReqId) -> Map<LeaseId, Lease> {
            self.leases.map_values(|lease: Lease|
                if lease.held_by(r) { lease.with_holder(Holder::Program) } else { lease })
        }

        /// The leases, without those of request `r`.
        pub open spec fn leases_but(self, r: ReqId) -> Map<LeaseId, Lease> {
            self.leases.filter_keys(|l: LeaseId| !self.leases[l].held_by(r))
        }

        /// Where lease `l` is (R1).
        pub open spec fn place(self, l: LeaseId) -> Place {
            if !self.leases.contains_key(l) {
                Place::Released
            } else {
                match self.leases[l].holder {
                    Holder::Program => Place::Program,
                    Holder::Exposed { .. } => Place::Exposed,
                    Holder::Request { req, .. } => if self.in_flight(req) {
                        Place::InFlight(req)
                    } else {
                        Place::Completed(req)
                    },
                }
            }
        }

        // Who may touch byte `a` (what S1 is about).

        /// The NIC reads lease `l` for a request in flight, or the peer by rkey.
        pub open spec fn nic_reads_lease(self, l: LeaseId) -> bool {
            match self.leases[l].holder {
                Holder::Program => false,
                Holder::Request { req, dir } => self.in_flight(req) && dir is Out,
                Holder::Exposed { .. } => true,
            }
        }

        /// The NIC writes lease `l` for a request in flight, or the peer by rkey.
        pub open spec fn nic_writes_lease(self, l: LeaseId) -> bool {
            match self.leases[l].holder {
                Holder::Program => false,
                Holder::Request { req, dir } => self.in_flight(req) && dir is In,
                Holder::Exposed { remote_write } => remote_write,
            }
        }

        /// The program may read byte `a` (through a `BufMut` or a `Buf`).
        pub open spec fn program_reads(self, a: int) -> bool {
            exists|l: LeaseId| #[trigger] self.leases.contains_key(l) && self.is_program(l)
                && self.leases[l].mem.contains(a)
        }

        /// The program may write byte `a` (through a `BufMut`).
        pub open spec fn program_writes(self, a: int) -> bool {
            exists|l: LeaseId| #[trigger] self.leases.contains_key(l) && self.is_program(l)
                && self.leases[l].unique && self.leases[l].mem.contains(a)
        }

        /// The NIC or the peer may read byte `a`.
        pub open spec fn nic_reads(self, a: int) -> bool {
            exists|l: LeaseId| #[trigger] self.leases.contains_key(l) && self.nic_reads_lease(l)
                && self.leases[l].mem.contains(a)
        }

        /// The NIC or the peer may write byte `a`.
        pub open spec fn nic_writes(self, a: int) -> bool {
            exists|l: LeaseId| #[trigger] self.leases.contains_key(l) && self.nic_writes_lease(l)
                && self.leases[l].mem.contains(a)
        }

        // ---------------------------------------------------------------------------------
        // Start

        init! {
            start() {
                init qps = Set::empty();
                init leases = Map::empty();
                init requests = Map::empty();
                init next_id = 0;
                init outcomes = Map::empty();
                init wakes = Seq::empty();
            }
        }

        // ---------------------------------------------------------------------------------
        // Queue pairs

        transition! {
            create_qp(q: QpId) {
                require !pre.qps.contains(q);
                update qps = pre.qps.insert(q);
            }
        }

        /// Dropping a queue pair. Futures hold the queue pair, so none is alive; the library
        /// drains the requests left in flight (each completes, with `FlushErr` at worst, and
        /// its leases are released) before the queue pair goes.
        transition! {
            drop_qp(q: QpId) {
                require pre.qps.contains(q);
                require forall|r: ReqId| #[trigger] pre.requests.contains_key(r) ==> pre.requests[r].qp != q;
                update qps = pre.qps.remove(q);
            }
        }

        // ---------------------------------------------------------------------------------
        // Buffers

        /// `pd.alloc::<T>(n)`: fresh memory becomes a `BufMut<T>`.
        transition! {
            alloc(mem: ISet<int>, any_bytes: bool) {
                require pre.free_except(mem, None);
                update leases = pre.leases.insert(pre.next_id,
                    Lease { mem, unique: true, any_bytes, holder: Holder::Program });
                update next_id = pre.next_id + 1;
            }
        }

        /// `BufMut::split_at` (and `chunks`): one unique lease becomes two.
        transition! {
            split(l: LeaseId, left: ISet<int>) {
                require pre.is_program(l) && pre.leases[l].unique;
                require left.subset_of(pre.leases[l].mem);
                let lease = pre.leases[l];
                update leases = pre.leases.remove(l)
                    .insert(pre.next_id, Lease { mem: left, ..lease })
                    .insert(pre.next_id + 1, Lease { mem: lease.mem.difference(left), ..lease });
                update next_id = pre.next_id + 2;
            }
        }

        /// `BufMut::freeze`: the buffer becomes shared and read-only.
        transition! {
            freeze(l: LeaseId) {
                require pre.is_program(l) && pre.leases[l].unique;
                update leases = pre.leases.insert(l, Lease { unique: false, ..pre.leases[l] });
            }
        }

        /// `Buf::clone` and `Buf::slice`: another read-only view of (part of) a shared buffer.
        transition! {
            share(l: LeaseId, sub: ISet<int>) {
                require pre.is_program(l) && !pre.leases[l].unique;
                require sub.subset_of(pre.leases[l].mem);
                update leases = pre.leases.insert(pre.next_id, Lease { mem: sub, ..pre.leases[l] });
                update next_id = pre.next_id + 1;
            }
        }

        /// `Buf::try_thaw` succeeds: no other view and no request or exposure holds the bytes.
        transition! {
            thaw(l: LeaseId) {
                require pre.is_program(l) && !pre.leases[l].unique;
                require pre.free_except(pre.leases[l].mem, Some(l));
                update leases = pre.leases.insert(l, Lease { unique: true, ..pre.leases[l] });
            }
        }

        /// Dropping a `BufMut` or a `Buf` view.
        transition! {
            drop(l: LeaseId) {
                require pre.is_program(l);
                update leases = pre.leases.remove(l);
            }
        }

        // ---------------------------------------------------------------------------------
        // Requests

        /// Posting a request (a send, write, receive or read, a batch, one entry of a sink or
        /// stream): these leases of the program move into it, each with its direction. A
        /// request may hold none.
        transition! {
            post_request(dirs: Map<LeaseId, Dir>, q: QpId) {
                require pre.qps.contains(q);
                require pre.may_post(dirs);
                let r = pre.next_id;
                update requests = pre.requests.insert(r,
                    Req { qp: q, phase: Phase::InFlight, waker: None, detached: false });
                update leases = pre.leases_in_request(dirs, r);
                update next_id = r + 1;
            }
        }

        /// The NIC finished request `r` with outcome `o` (the environment's move; the library
        /// learns it by polling the CQ). A registered waker is invoked; a detached request's
        /// leases are released.
        transition! {
            complete(r: ReqId, o: Outcome) {
                require pre.in_flight(r);
                let req = pre.requests[r];
                update outcomes = pre.outcomes.insert(r, o);
                update wakes = if req.waker is Some { pre.wakes.push(req.waker->0) } else { pre.wakes };
                update requests = if req.detached { pre.requests.remove(r) }
                    else { pre.requests.insert(r, req.completed(o)) };
                update leases = if req.detached { pre.leases_but(r) } else { pre.leases };
            }
        }

        /// The future of `r` was polled and returned `Pending`, registering waker `w`.
        transition! {
            register(r: ReqId, w: WakerId) {
                require pre.in_flight(r) && !pre.requests[r].detached;
                update requests = pre.requests.insert(r, pre.requests[r].with_waker(w));
            }
        }

        /// The future of `r` returned `Ready(o)`: the client takes the outcome, and each lease
        /// of the request comes back to the program under its own name.
        transition! {
            take(r: ReqId, o: Outcome) {
                require pre.requests.contains_key(r) && !pre.requests[r].detached;
                require pre.requests[r].phase == Phase::Complete(o);
                update requests = pre.requests.remove(r);
                update leases = pre.leases_returned(r);
            }
        }

        /// The future of `r` was dropped. A complete request's leases are released now, an
        /// in-flight one's when it completes; nothing is cancelled.
        transition! {
            detach(r: ReqId) {
                require pre.requests.contains_key(r) && !pre.requests[r].detached;
                let done = pre.requests[r].phase is Complete;
                update requests = if done { pre.requests.remove(r) }
                    else { pre.requests.insert(r, pre.requests[r].detach()) };
                update leases = if done { pre.leases_but(r) } else { pre.leases };
            }
        }

        // ---------------------------------------------------------------------------------
        // Remote access

        /// Registering a buffer for remote access and handing its rkey to the peer. Remote
        /// write needs a unique buffer whose type accepts any bytes.
        transition! {
            expose(l: LeaseId, remote_write: bool) {
                require pre.is_program(l);
                require remote_write ==> pre.leases[l].writable();
                update leases = pre.leases.insert(l,
                    pre.leases[l].with_holder(Holder::Exposed { remote_write }));
            }
        }

        /// Deregistering it (the rkey stops working): the program gets the buffer back.
        transition! {
            unexpose(l: LeaseId) {
                require pre.leases.contains_key(l) && pre.leases[l].holder is Exposed;
                update leases = pre.leases.insert(l, pre.leases[l].with_holder(Holder::Program));
            }
        }

        // ---------------------------------------------------------------------------------
        // Invariants

        /// Names below `next_id` only.
        #[invariant]
        pub fn inv_ids(&self) -> bool {
            &&& forall|l: LeaseId| #[trigger] self.leases.contains_key(l) ==> l < self.next_id
            &&& forall|r: ReqId| #[trigger] self.requests.contains_key(r) ==> r < self.next_id
            &&& forall|r: ReqId| #[trigger] self.outcomes.contains_key(r) ==> r < self.next_id
        }

        /// The heart of S1: a unique lease overlaps no other lease.
        #[invariant]
        pub fn inv_unique(&self) -> bool {
            forall|k: LeaseId, l: LeaseId| #![trigger self.leases[k], self.leases[l]]
                self.leases.contains_key(k) && self.leases.contains_key(l) && k != l
                    && (self.leases[k].unique || self.leases[l].unique)
                    ==> self.leases[k].mem.disjoint(self.leases[l].mem)
        }

        /// Memory the NIC may write is in unique leases that accept any bytes.
        #[invariant]
        pub fn inv_nic_writes(&self) -> bool {
            forall|l: LeaseId| #[trigger] self.leases.contains_key(l) ==> match self.leases[l].holder {
                Holder::Program => true,
                Holder::Request { dir, .. } => dir is In ==> self.leases[l].writable(),
                Holder::Exposed { remote_write } => remote_write ==> self.leases[l].writable(),
            }
        }

        /// The heart of R1: a lease in a request names a request that exists, so [`Place`] says
        /// where it is.
        #[invariant]
        pub fn inv_places(&self) -> bool {
            forall|l: LeaseId| #[trigger] self.leases.contains_key(l) && self.leases[l].holder is Request
                ==> self.requests.contains_key(self.leases[l].holder->req)
        }

        /// A request lives on a queue pair that exists, and has an outcome exactly when it is
        /// complete (the one recorded).
        #[invariant]
        pub fn inv_requests(&self) -> bool {
            forall|r: ReqId| #[trigger] self.requests.contains_key(r) ==> {
                let req = self.requests[r];
                &&& self.qps.contains(req.qp)
                &&& req.phase is InFlight <==> !self.outcomes.contains_key(r)
                &&& req.phase is Complete ==> self.outcomes[r] == req.phase->0
            }
        }

        /// A complete or detached request holds no waker.
        #[invariant]
        pub fn inv_wakers(&self) -> bool {
            forall|r: ReqId| #[trigger] self.requests.contains_key(r) ==> {
                let req = self.requests[r];
                (req.phase is Complete || req.detached) ==> req.waker is None
            }
        }

        // ---------------------------------------------------------------------------------
        // Inductiveness

        #[inductive(start)]
        pub fn start_inductive(post: Self) {}

        #[inductive(create_qp)]
        pub fn create_qp_inductive(pre: Self, post: Self, q: QpId) {}

        #[inductive(drop_qp)]
        pub fn drop_qp_inductive(pre: Self, post: Self, q: QpId) {}

        #[inductive(alloc)]
        pub fn alloc_inductive(pre: Self, post: Self, mem: ISet<int>, any_bytes: bool) {}

        #[inductive(split)]
        pub fn split_inductive(pre: Self, post: Self, l: LeaseId, left: ISet<int>) {}

        #[inductive(freeze)]
        pub fn freeze_inductive(pre: Self, post: Self, l: LeaseId) {}

        #[inductive(share)]
        pub fn share_inductive(pre: Self, post: Self, l: LeaseId, sub: ISet<int>) {}

        #[inductive(thaw)]
        pub fn thaw_inductive(pre: Self, post: Self, l: LeaseId) {}

        #[inductive(drop)]
        pub fn drop_inductive(pre: Self, post: Self, l: LeaseId) {}

        #[inductive(post_request)]
        pub fn post_request_inductive(pre: Self, post: Self, dirs: Map<LeaseId, Dir>, q: QpId) {}

        #[inductive(complete)]
        pub fn complete_inductive(pre: Self, post: Self, r: ReqId, o: Outcome) {}

        #[inductive(register)]
        pub fn register_inductive(pre: Self, post: Self, r: ReqId, w: WakerId) {}

        #[inductive(take)]
        pub fn take_inductive(pre: Self, post: Self, r: ReqId, o: Outcome) {}

        #[inductive(detach)]
        pub fn detach_inductive(pre: Self, post: Self, r: ReqId) {}

        #[inductive(expose)]
        pub fn expose_inductive(pre: Self, post: Self, l: LeaseId, remote_write: bool) {}

        #[inductive(unexpose)]
        pub fn unexpose_inductive(pre: Self, post: Self, l: LeaseId) {}
    }
}

verus! {

// =========================================================================================
// Names for other crates
//
// `state_machine!` emits its relations, `invariant()` and the `_enabled` predicates only when
// Verus keeps ghost bodies, which it does not for the copy of this crate that dependent crates
// compile against. These wrappers carry them across (their meaning comes along in Verus's
// exported definitions).

/// `s` satisfies A's invariants (`Abs::State::invariant`).
pub open spec fn abs_inv(s: Abs::State) -> bool {
    s.invariant()
}

/// `s` is a start state of A.
pub open spec fn abs_init(s: Abs::State) -> bool {
    Abs::State::init(s)
}

/// `post` follows from `pre` by one step of A, any step.
pub open spec fn abs_next(pre: Abs::State, post: Abs::State) -> bool {
    Abs::State::next(pre, post)
}

/// `post` follows from `pre` by `step`.
pub open spec fn abs_step(pre: Abs::State, post: Abs::State, step: Abs::Step) -> bool {
    match step {
        Abs::Step::create_qp(q) => Abs::State::create_qp(pre, post, q),
        Abs::Step::drop_qp(q) => Abs::State::drop_qp(pre, post, q),
        Abs::Step::alloc(mem, any_bytes) => Abs::State::alloc(pre, post, mem, any_bytes),
        Abs::Step::split(l, left) => Abs::State::split(pre, post, l, left),
        Abs::Step::freeze(l) => Abs::State::freeze(pre, post, l),
        Abs::Step::share(l, sub) => Abs::State::share(pre, post, l, sub),
        Abs::Step::thaw(l) => Abs::State::thaw(pre, post, l),
        Abs::Step::drop(l) => Abs::State::drop(pre, post, l),
        Abs::Step::post_request(dirs, q) => Abs::State::post_request(pre, post, dirs, q),
        Abs::Step::complete(r, o) => Abs::State::complete(pre, post, r, o),
        Abs::Step::register(r, w) => Abs::State::register(pre, post, r, w),
        Abs::Step::take(r, o) => Abs::State::take(pre, post, r, o),
        Abs::Step::detach(r) => Abs::State::detach(pre, post, r),
        Abs::Step::expose(l, remote_write) => Abs::State::expose(pre, post, l, remote_write),
        Abs::Step::unexpose(l) => Abs::State::unexpose(pre, post, l),
        Abs::Step::dummy_to_use_type_params(_) => false,
    }
}

/// A step by `step` is a step of A.
pub proof fn lemma_abs_step_is_next(pre: Abs::State, post: Abs::State, step: Abs::Step)
    requires
        abs_step(pre, post, step),
    ensures
        abs_next(pre, post),
{
    reveal(Abs::State::next);
    reveal(Abs::State::next_by);
    assert(Abs::State::next_by(pre, post, step));
}

/// A's invariants hold in every state it can reach: every step keeps them.
pub proof fn lemma_step_keeps_invariant(pre: Abs::State, post: Abs::State)
    requires
        pre.invariant(),
        Abs::State::next(pre, post),
    ensures
        post.invariant(),
{
    crate::abs_proofs::lemma_next(pre, post);
}

} // verus!
