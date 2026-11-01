//! **A**: the abstract SafeVerbs machine, the specification users rely on.
//!
//! A knows nothing about queues, doorbells, CQEs or ring slots. It sees *leases*: a set of
//! bytes held by someone. The program holds leases (a `BufMut` is a unique lease, every `Buf`
//! view a shared one), a posted request holds the lease of the buffer it was given, and a
//! region registered for remote access is held by the peer. Every client action of the
//! library is a transition of A that moves leases between holders; the NIC's only move is
//! `complete`, which gives a request its outcome.
//!
//! The guarantees S1, F1, R1 and L1 are invariants of this machine (proved here and in
//! [`crate::guarantees`]); the refinement theorem ([`crate::refinement`]) says the real
//! library behaves like A.
//!
//! The module is called `abs` because `abstract` is a reserved word in Rust.
#![allow(unused_imports)]

use crate::types::*;
use verus_state_machines_macros::state_machine;
use vstd::prelude::*;

verus! {

/// Names leases (and so requests). Never reused: every new lease takes `next_id`.
pub type Id = nat;

/// Names a `core::task::Waker` (two wakers of the same task may share a name).
pub type WakerId = nat;

/// Which way the NIC touches a request's buffer.
pub enum Dir {
    /// The NIC reads it: a send, the source of an RDMA write.
    Out,
    /// The NIC writes it: a receive, the target of an RDMA read.
    In,
}

/// Where a request is.
pub enum Phase {
    /// Posted; the NIC may be accessing the buffer.
    InFlight,
    /// The NIC is done and produced this outcome; not yet taken by the client.
    Complete(Outcome),
}

/// A request posted to a queue pair: one future (or one entry of a stream).
pub struct Req {
    pub qp: QpId,
    pub dir: Dir,
    pub phase: Phase,
    /// The waker the future registered when it last returned `Pending`.
    pub waker: Option<WakerId>,
    /// The future was dropped. Dropping never cancels: the request stays in flight and its
    /// lease is released when it completes.
    pub detached: bool,
}

/// Who holds a lease.
pub enum Holder {
    /// The program: through a `BufMut` (if the lease is unique) or one `Buf` view.
    Program,
    /// A request holds it until the client takes the outcome (or, if detached, until it completes).
    Request { req: Req },
    /// The peer may access it by rkey: read it, and write it too if `remote_write`.
    Exposed { remote_write: bool },
}

/// A set of bytes, held by one holder.
pub struct Lease {
    pub mem: ISet<int>,
    /// No other lease overlaps it (a `BufMut`, or what one became).
    pub unique: bool,
    /// The element type accepts every bit pattern (`zerocopy::FromBytes`), so the NIC may
    /// write anything into it.
    pub any_bytes: bool,
    pub holder: Holder,
}

impl Lease {
    pub open spec fn is_program(self) -> bool {
        self.holder is Program
    }

    pub open spec fn is_request(self) -> bool {
        self.holder is Request
    }

    pub open spec fn in_flight(self) -> bool {
        self.holder is Request && self.holder->req.phase is InFlight
    }

    /// The NIC (for a request in flight) or the peer (for an exposed region) may read `mem`.
    pub open spec fn nic_may_read(self) -> bool {
        self.in_flight() || self.holder is Exposed
    }

    /// The NIC or the peer may write `mem`.
    pub open spec fn nic_may_write(self) -> bool {
        (self.in_flight() && self.holder->req.dir is In) || (self.holder is Exposed
            && self.holder->remote_write)
    }

    pub open spec fn with_holder(self, holder: Holder) -> Lease {
        Lease { mem: self.mem, unique: self.unique, any_bytes: self.any_bytes, holder }
    }

    pub open spec fn with_req(self, req: Req) -> Lease {
        self.with_holder(Holder::Request { req })
    }
}

impl Req {
    pub open spec fn completed(self, o: Outcome) -> Req {
        Req { qp: self.qp, dir: self.dir, phase: Phase::Complete(o), waker: None, detached: self.detached }
    }

    pub open spec fn with_waker(self, w: WakerId) -> Req {
        Req { qp: self.qp, dir: self.dir, phase: self.phase, waker: Some(w), detached: self.detached }
    }

    pub open spec fn detach(self) -> Req {
        Req { qp: self.qp, dir: self.dir, phase: self.phase, waker: None, detached: true }
    }
}

} // verus!

state_machine! {
    Abs {
        fields {
            /// Queue pairs that exist.
            pub qps: Set<QpId>,
            /// Every lease that exists, by id.
            pub leases: Map<Id, Lease>,
            /// The next fresh id.
            pub next_id: Id,
            /// Every outcome ever produced, by request: written once (F1).
            pub outcomes: Map<Id, Outcome>,
            /// The bytes each request was posted with (F1: they come back with its outcome).
            pub posted: Map<Id, ISet<int>>,
            /// Every waker invocation, in order (L1).
            pub wakes: Seq<WakerId>,
        }

        // ---------------------------------------------------------------------------------
        // Helpers

        pub open spec fn is_program(self, i: Id) -> bool {
            self.leases.contains_key(i) && self.leases[i].is_program()
        }

        pub open spec fn is_request(self, i: Id) -> bool {
            self.leases.contains_key(i) && self.leases[i].is_request()
        }

        pub open spec fn in_flight(self, i: Id) -> bool {
            self.leases.contains_key(i) && self.leases[i].in_flight()
        }

        pub open spec fn req(self, i: Id) -> Req {
            self.leases[i].holder->req
        }

        /// `mem` overlaps no lease but (possibly) lease `except`.
        pub open spec fn free_except(self, mem: ISet<int>, except: Option<Id>) -> bool {
            forall|j: Id| #[trigger] self.leases.contains_key(j) && except != Some(j)
                ==> self.leases[j].mem.disjoint(mem)
        }

        // Who may touch byte `a` (what S1 is about).

        /// The program may read byte `a` (through a `BufMut` or a `Buf`).
        pub open spec fn program_reads(self, a: int) -> bool {
            exists|i: Id| #[trigger] self.leases.contains_key(i) && self.leases[i].is_program()
                && self.leases[i].mem.contains(a)
        }

        /// The program may write byte `a` (through a `BufMut`).
        pub open spec fn program_writes(self, a: int) -> bool {
            exists|i: Id| #[trigger] self.leases.contains_key(i) && self.leases[i].is_program()
                && self.leases[i].unique && self.leases[i].mem.contains(a)
        }

        /// The NIC or the peer may read byte `a`.
        pub open spec fn nic_reads(self, a: int) -> bool {
            exists|i: Id| #[trigger] self.leases.contains_key(i) && self.leases[i].nic_may_read()
                && self.leases[i].mem.contains(a)
        }

        /// The NIC or the peer may write byte `a`.
        pub open spec fn nic_writes(self, a: int) -> bool {
            exists|i: Id| #[trigger] self.leases.contains_key(i) && self.leases[i].nic_may_write()
                && self.leases[i].mem.contains(a)
        }

        // ---------------------------------------------------------------------------------
        // Start

        init! {
            start() {
                init qps = Set::empty();
                init leases = Map::empty();
                init next_id = 0;
                init outcomes = Map::empty();
                init posted = Map::empty();
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
        /// its lease is released) before the queue pair goes. This is how R1's "at the latest
        /// when its QP is dropped" holds.
        transition! {
            drop_qp(q: QpId) {
                require pre.qps.contains(q);
                require forall|i: Id| #[trigger] pre.leases.contains_key(i) && pre.leases[i].is_request() ==> pre.req(i).qp != q;
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
            split(i: Id, left: ISet<int>) {
                require pre.is_program(i) && pre.leases[i].unique;
                require left.subset_of(pre.leases[i].mem);
                let l = pre.leases[i];
                update leases = pre.leases.remove(i)
                    .insert(pre.next_id, Lease { mem: left, unique: true, any_bytes: l.any_bytes, holder: Holder::Program })
                    .insert(pre.next_id + 1, Lease { mem: l.mem.difference(left), unique: true, any_bytes: l.any_bytes, holder: Holder::Program });
                update next_id = pre.next_id + 2;
            }
        }

        /// `BufMut::freeze`: the buffer becomes shared and read-only.
        transition! {
            freeze(i: Id) {
                require pre.is_program(i) && pre.leases[i].unique;
                let l = pre.leases[i];
                update leases = pre.leases.insert(i,
                    Lease { mem: l.mem, unique: false, any_bytes: l.any_bytes, holder: Holder::Program });
            }
        }

        /// `Buf::clone` and `Buf::slice`: another read-only view of (part of) a shared buffer.
        transition! {
            share(i: Id, sub: ISet<int>) {
                require pre.is_program(i) && !pre.leases[i].unique;
                require sub.subset_of(pre.leases[i].mem);
                let l = pre.leases[i];
                update leases = pre.leases.insert(pre.next_id,
                    Lease { mem: sub, unique: false, any_bytes: l.any_bytes, holder: Holder::Program });
                update next_id = pre.next_id + 1;
            }
        }

        /// `Buf::try_thaw` succeeds: no other view and no request or exposure holds the bytes.
        transition! {
            thaw(i: Id) {
                require pre.is_program(i) && !pre.leases[i].unique;
                require pre.free_except(pre.leases[i].mem, Some(i));
                let l = pre.leases[i];
                update leases = pre.leases.insert(i,
                    Lease { mem: l.mem, unique: true, any_bytes: l.any_bytes, holder: Holder::Program });
            }
        }

        /// Dropping a `BufMut` or a `Buf` view.
        transition! {
            drop(i: Id) {
                require pre.is_program(i);
                update leases = pre.leases.remove(i);
            }
        }

        // ---------------------------------------------------------------------------------
        // Requests

        /// Posting a request (a send, write, receive or read; one entry of a batch, sink or
        /// stream). The buffer's lease moves into the request; the NIC may only write into
        /// a unique buffer whose type accepts any bytes.
        transition! {
            post_request(i: Id, q: QpId, dir: Dir) {
                require pre.is_program(i) && pre.qps.contains(q);
                let l = pre.leases[i];
                require dir is In ==> l.unique && l.any_bytes;
                let r = pre.next_id;
                let req = Req { qp: q, dir, phase: Phase::InFlight, waker: None, detached: false };
                update leases = pre.leases.remove(i).insert(r, l.with_req(req));
                update posted = pre.posted.insert(r, l.mem);
                update next_id = r + 1;
            }
        }

        /// The NIC finished request `r` with outcome `o` (the environment's move; the library
        /// learns it by polling the CQ). A registered waker is invoked; a detached request's
        /// lease is released.
        transition! {
            complete(r: Id, o: Outcome) {
                require pre.in_flight(r);
                let l = pre.leases[r];
                let req = pre.req(r);
                update outcomes = pre.outcomes.insert(r, o);
                update wakes = if req.waker is Some { pre.wakes.push(req.waker->0) } else { pre.wakes };
                update leases = if req.detached { pre.leases.remove(r) }
                    else { pre.leases.insert(r, l.with_req(req.completed(o))) };
            }
        }

        /// The future of `r` was polled and returned `Pending`, registering waker `w`.
        transition! {
            register(r: Id, w: WakerId) {
                require pre.in_flight(r) && !pre.req(r).detached;
                update leases = pre.leases.insert(r, pre.leases[r].with_req(pre.req(r).with_waker(w)));
            }
        }

        /// The future of `r` returned `Ready(o)`: the client takes the outcome and gets the
        /// buffer back.
        transition! {
            take(r: Id, o: Outcome) {
                require pre.is_request(r) && !pre.req(r).detached;
                require pre.req(r).phase == Phase::Complete(o);
                update leases = pre.leases.remove(r).insert(pre.next_id,
                    pre.leases[r].with_holder(Holder::Program));
                update next_id = pre.next_id + 1;
            }
        }

        /// The future of `r` was dropped. A complete request's lease is released now, an
        /// in-flight one's when it completes; nothing is cancelled.
        transition! {
            detach(r: Id) {
                require pre.is_request(r) && !pre.req(r).detached;
                update leases = if pre.req(r).phase is Complete { pre.leases.remove(r) }
                    else { pre.leases.insert(r, pre.leases[r].with_req(pre.req(r).detach())) };
            }
        }

        // ---------------------------------------------------------------------------------
        // Remote access

        /// Registering a buffer for remote access and handing its rkey to the peer. Remote
        /// write needs a unique buffer whose type accepts any bytes.
        transition! {
            expose(i: Id, remote_write: bool) {
                require pre.is_program(i);
                require remote_write ==> pre.leases[i].unique && pre.leases[i].any_bytes;
                update leases = pre.leases.insert(i, pre.leases[i].with_holder(Holder::Exposed { remote_write }));
            }
        }

        /// Deregistering it (the rkey stops working): the program gets the buffer back.
        transition! {
            unexpose(i: Id) {
                require pre.leases.contains_key(i) && pre.leases[i].holder is Exposed;
                update leases = pre.leases.insert(i, pre.leases[i].with_holder(Holder::Program));
            }
        }

        // ---------------------------------------------------------------------------------
        // Invariants

        /// Ids below `next_id` only.
        #[invariant]
        pub fn inv_ids(&self) -> bool {
            &&& forall|i: Id| #[trigger] self.leases.contains_key(i) ==> i < self.next_id
            &&& forall|i: Id| #[trigger] self.outcomes.contains_key(i) ==> i < self.next_id
        }

        /// The heart of S1: a unique lease overlaps no other lease.
        #[invariant]
        pub fn inv_unique(&self) -> bool {
            forall|i: Id, j: Id| #![trigger self.leases[i], self.leases[j]]
                self.leases.contains_key(i) && self.leases.contains_key(j) && i != j
                    && self.leases[i].unique ==> self.leases[i].mem.disjoint(self.leases[j].mem)
        }

        /// Memory the NIC may write is held uniquely and accepts any bytes.
        #[invariant]
        pub fn inv_nic_writes(&self) -> bool {
            forall|i: Id| #[trigger] self.leases.contains_key(i) ==> {
                let l = self.leases[i];
                (l.is_request() && l.holder->req.dir is In) || (l.holder is Exposed && l.holder->remote_write)
                    ==> l.unique && l.any_bytes
            }
        }

        /// A request keeps the bytes it was posted with, lives on a queue pair that exists,
        /// and has an outcome exactly when it is complete (the one recorded).
        #[invariant]
        pub fn inv_requests(&self) -> bool {
            forall|r: Id| #[trigger] self.leases.contains_key(r) && self.leases[r].is_request() ==> {
                let req = self.req(r);
                &&& self.posted.contains_key(r) && self.posted[r] == self.leases[r].mem
                &&& self.qps.contains(req.qp)
                &&& req.phase is InFlight <==> !self.outcomes.contains_key(r)
                &&& req.phase is Complete ==> self.outcomes[r] == req.phase->0
            }
        }

        /// The heart of L1: a complete or detached request holds no waker.
        #[invariant]
        pub fn inv_wakers(&self) -> bool {
            forall|r: Id| #[trigger] self.leases.contains_key(r) && self.leases[r].is_request() ==> {
                let req = self.req(r);
                (req.phase is Complete || req.detached) ==> req.waker is None
            }
        }

        // ---------------------------------------------------------------------------------
        // Inductiveness

        #[inductive(start)]
        fn start_inductive(post: Self) {}

        #[inductive(create_qp)]
        fn create_qp_inductive(pre: Self, post: Self, q: QpId) {}

        #[inductive(drop_qp)]
        fn drop_qp_inductive(pre: Self, post: Self, q: QpId) {}

        #[inductive(alloc)]
        fn alloc_inductive(pre: Self, post: Self, mem: ISet<int>, any_bytes: bool) {}

        #[inductive(split)]
        fn split_inductive(pre: Self, post: Self, i: Id, left: ISet<int>) {}

        #[inductive(freeze)]
        fn freeze_inductive(pre: Self, post: Self, i: Id) {}

        #[inductive(share)]
        fn share_inductive(pre: Self, post: Self, i: Id, sub: ISet<int>) {}

        #[inductive(thaw)]
        fn thaw_inductive(pre: Self, post: Self, i: Id) {}

        #[inductive(drop)]
        fn drop_inductive(pre: Self, post: Self, i: Id) {}

        #[inductive(post_request)]
        fn post_request_inductive(pre: Self, post: Self, i: Id, q: QpId, dir: Dir) {}

        #[inductive(complete)]
        fn complete_inductive(pre: Self, post: Self, r: Id, o: Outcome) {}

        #[inductive(register)]
        fn register_inductive(pre: Self, post: Self, r: Id, w: WakerId) {}

        #[inductive(take)]
        fn take_inductive(pre: Self, post: Self, r: Id, o: Outcome) {}

        #[inductive(detach)]
        fn detach_inductive(pre: Self, post: Self, r: Id) {}

        #[inductive(expose)]
        fn expose_inductive(pre: Self, post: Self, i: Id, remote_write: bool) {}

        #[inductive(unexpose)]
        fn unexpose_inductive(pre: Self, post: Self, i: Id) {}
    }
}
