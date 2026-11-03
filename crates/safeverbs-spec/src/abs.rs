//! **A**: the abstract SafeVerbs machine, the specification users rely on.
//!
//! A knows nothing about queues, doorbells, CQEs or ring slots. It sees *leases*: a set of
//! bytes and what may be done to them. Leases come in ordered *groups*, each held by
//! someone: the program holds each buffer as a group of one lease (a `BufMut` is a unique
//! lease, every `Buf` view a shared one), a posted request holds the group it was given
//! (its *members*; a batch joins several buffers), and a region registered for remote
//! access is held by the peer. Every client action of the library is a transition of A that
//! moves groups between holders; the NIC's only move is `complete`, which gives a request
//! its outcome.
//!
//! The guarantees S1, F1, R1 and L1 are invariants of this machine (proved here and in
//! [`crate::guarantees`]); the refinement theorem ([`crate::refinement`]) states what the
//! library proves to behave like A.
//!
//! The module is called `abs` because `abstract` is a reserved word in Rust. The proofs that
//! its invariants are inductive are in [`crate::abs_proofs`].
#![allow(unused_imports)]

use crate::types::*;
use verus_state_machines_macros::state_machine;
use vstd::prelude::*;

verus! {

/// Names leases and groups (a request is named by its group). Never reused: every new name
/// takes `next_id`.
pub type Id = nat;

/// Names a `core::task::Waker` (two wakers of the same task may share a name).
pub type WakerId = nat;

/// Which way the NIC touches a member of a request.
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

/// A request posted to a queue pair: one future (or one entry of a stream). It has one
/// outcome and one waker for its whole group and a direction for each member, so a READ
/// destination and a shared SEND source may be members of one request.
pub struct Req {
    pub qp: QpId,
    /// How the NIC touches each member.
    pub dirs: Seq<Dir>,
    pub phase: Phase,
    /// The waker the future registered when it last returned `Pending`.
    pub waker: Option<WakerId>,
    /// The future was dropped. Dropping never cancels: the request stays in flight and its
    /// leases are released when it completes.
    pub detached: bool,
}

/// Who holds a group.
pub enum Holder {
    /// The program: through a `BufMut` (a unique lease) or a `Buf` view.
    Program,
    /// A request holds it until the client takes the outcome (or, if detached, until it
    /// completes).
    Request { req: Req },
    /// The peer may access it by rkey: read it, and write it too if `remote_write`.
    Exposed { remote_write: bool },
}

/// A set of bytes and what may be done to them. Its identity survives joining, posting and
/// taking; a shared clone gets a fresh one, even when it covers exactly the same bytes.
pub struct Lease {
    pub id: Id,
    pub mem: ISet<int>,
    /// No other lease overlaps it (a `BufMut`, or what one became).
    pub unique: bool,
    /// The element type accepts every bit pattern (`zerocopy::FromBytes`), so the NIC may
    /// write anything into it.
    pub any_bytes: bool,
}

/// An ordered group of leases, its members, with one holder. Each member keeps its own
/// permissions: one group may hold unique and shared leases.
pub struct Group {
    pub members: Seq<Lease>,
    pub holder: Holder,
}

impl Group {
    /// A group of one lease, held by the program.
    pub open spec fn singleton(id: Id, mem: ISet<int>, unique: bool, any_bytes: bool) -> Group {
        Group { members: seq![Lease { id, mem, unique, any_bytes }], holder: Holder::Program }
    }

    pub open spec fn is_program(self) -> bool { self.holder is Program }
    pub open spec fn is_request(self) -> bool { self.holder is Request }
    pub open spec fn in_flight(self) -> bool {
        self.is_request() && self.holder->req.phase is InFlight
    }
    /// Some member covers byte `x`.
    pub open spec fn contains(self, x: int) -> bool {
        exists|k: int| 0 <= k < self.members.len() && (#[trigger] self.members[k]).mem.contains(x)
    }
    /// The program holds the group and a unique member covers byte `x`.
    pub open spec fn writes_program(self, x: int) -> bool {
        self.is_program() && exists|k: int| 0 <= k < self.members.len()
            && (#[trigger] self.members[k]).unique && self.members[k].mem.contains(x)
    }
    /// The NIC or the peer may read byte `x` through a member.
    pub open spec fn nic_may_read(self, x: int) -> bool {
        exists|k: int| 0 <= k < self.members.len()
            && (#[trigger] self.members[k]).mem.contains(x) && self.read_member(k)
    }
    /// The NIC or the peer may write byte `x` through a member.
    pub open spec fn nic_may_write(self, x: int) -> bool {
        exists|k: int| 0 <= k < self.members.len()
            && (#[trigger] self.members[k]).mem.contains(x) && self.write_member(k)
    }
    /// The NIC writes member `k` of an in-flight request, or the peer an exposed region.
    pub open spec fn write_member(self, k: int) -> bool {
        (self.in_flight() && self.holder->req.dirs[k] is In)
            || (self.holder is Exposed && self.holder->remote_write)
    }
    /// The NIC reads member `k` of an in-flight request, or the peer an exposed region.
    pub open spec fn read_member(self, k: int) -> bool {
        (self.in_flight() && self.holder->req.dirs[k] is Out) || self.holder is Exposed
    }
    /// A request may hold the group with these directions: the NIC writes only unique
    /// members that accept any bytes.
    pub open spec fn allows(self, dirs: Seq<Dir>) -> bool {
        &&& dirs.len() == self.members.len()
        &&& forall|k: int| 0 <= k < dirs.len() && #[trigger] dirs[k] is In
            ==> self.members[k].unique && self.members[k].any_bytes
    }
    pub open spec fn all_writable(self) -> bool {
        forall|k: int| 0 <= k < self.members.len()
            ==> (#[trigger] self.members[k]).unique && self.members[k].any_bytes
    }
    pub open spec fn with_holder(self, holder: Holder) -> Group {
        Group { members: self.members, holder }
    }
    pub open spec fn with_req(self, req: Req) -> Group {
        self.with_holder(Holder::Request { req })
    }
    /// Two groups' leases have distinct identities, and a unique lease of one overlaps no
    /// lease of the other.
    pub open spec fn compatible(self, other: Group) -> bool {
        forall|k: int, l: int| #![trigger self.members[k], other.members[l]]
            0 <= k < self.members.len() && 0 <= l < other.members.len() ==> {
                &&& self.members[k].id != other.members[l].id
                &&& self.members[k].unique || other.members[l].unique
                    ==> self.members[k].mem.disjoint(other.members[l].mem)
            }
    }
    /// The same within the group: distinct identities, and a unique member overlaps no other.
    pub open spec fn wf(self) -> bool {
        forall|k: int, l: int| #![trigger self.members[k], self.members[l]]
            0 <= k < self.members.len() && 0 <= l < self.members.len() && k != l ==> {
                &&& self.members[k].id != self.members[l].id
                &&& self.members[k].unique || self.members[l].unique
                    ==> self.members[k].mem.disjoint(self.members[l].mem)
            }
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
            /// Every group, by its handle.
            pub groups: Map<Id, Group>,
            /// The next fresh name.
            pub next_id: Id,
            /// Every outcome ever produced, by request: written once (F1).
            pub outcomes: Map<Id, Outcome>,
            /// The members each request was posted with (F1: they come back with its outcome).
            pub posted: Map<Id, Seq<Lease>>,
            /// Every waker invocation, in order (L1).
            pub wakes: Seq<WakerId>,
        }

        // ---------------------------------------------------------------------------------
        // Helpers

        pub open spec fn is_program(self, i: Id) -> bool {
            self.groups.contains_key(i) && self.groups[i].is_program()
        }

        pub open spec fn is_request(self, i: Id) -> bool {
            self.groups.contains_key(i) && self.groups[i].is_request()
        }

        pub open spec fn in_flight(self, i: Id) -> bool {
            self.groups.contains_key(i) && self.groups[i].in_flight()
        }

        pub open spec fn req(self, i: Id) -> Req {
            self.groups[i].holder->req
        }

        /// `mem` overlaps no lease but (possibly) those of group `except`.
        pub open spec fn free_except(self, mem: ISet<int>, except: Option<Id>) -> bool {
            forall|j: Id| #[trigger] self.groups.contains_key(j) && except != Some(j)
                ==> forall|k: int| 0 <= k < self.groups[j].members.len() ==> (#[trigger] self.groups[j].members[k]).mem.disjoint(mem)
        }

        // Who may touch byte `a` (what S1 is about).

        /// The program may read byte `a` (through a `BufMut` or a `Buf`).
        pub open spec fn program_reads(self, a: int) -> bool {
            exists|i: Id| #[trigger] self.groups.contains_key(i) && self.groups[i].is_program()
                && self.groups[i].contains(a)
        }

        /// The program may write byte `a` (through a `BufMut`).
        pub open spec fn program_writes(self, a: int) -> bool {
            exists|i: Id| #[trigger] self.groups.contains_key(i) && self.groups[i].is_program()
                && self.groups[i].writes_program(a)
        }

        /// The NIC or the peer may read byte `a`.
        pub open spec fn nic_reads(self, a: int) -> bool {
            exists|i: Id| #[trigger] self.groups.contains_key(i) && self.groups[i].nic_may_read(a)
        }

        /// The NIC or the peer may write byte `a`.
        pub open spec fn nic_writes(self, a: int) -> bool {
            exists|i: Id| #[trigger] self.groups.contains_key(i) && self.groups[i].nic_may_write(a)
        }

        // ---------------------------------------------------------------------------------
        // Start

        init! {
            start() {
                init qps = Set::empty();
                init groups = Map::empty();
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
                require forall|i: Id| #[trigger] pre.groups.contains_key(i) && pre.groups[i].is_request() ==> pre.req(i).qp != q;
                update qps = pre.qps.remove(q);
            }
        }

        // ---------------------------------------------------------------------------------
        // Buffers

        /// An empty group, for a request with no buffers (bookkeeping, no allocation).
        transition! {
            empty() {
                update groups = pre.groups.insert(pre.next_id,
                    Group { members: Seq::empty(), holder: Holder::Program });
                update next_id = pre.next_id + 1;
            }
        }

        /// Joining two of the program's groups into one, in order: every lease keeps its
        /// identity and permissions, and the group gets a fresh handle.
        transition! {
            join(i: Id, j: Id) {
                require i != j && pre.is_program(i) && pre.is_program(j);
                update groups = pre.groups.remove(i).remove(j).insert(pre.next_id,
                    Group { members: pre.groups[i].members + pre.groups[j].members, holder: Holder::Program });
                update next_id = pre.next_id + 1;
            }
        }

        /// Separating one of the program's groups at `at` (a returned batch, say): every
        /// lease keeps its identity and permissions, and both groups get fresh handles.
        transition! {
            separate(i: Id, at: int) {
                require pre.is_program(i) && 0 <= at <= pre.groups[i].members.len();
                update groups = pre.groups.remove(i)
                    .insert(pre.next_id, Group { members: pre.groups[i].members.take(at), holder: Holder::Program })
                    .insert(pre.next_id + 1, Group { members: pre.groups[i].members.skip(at), holder: Holder::Program });
                update next_id = pre.next_id + 2;
            }
        }

        /// `pd.alloc::<T>(n)`: fresh memory becomes a `BufMut<T>`.
        transition! {
            alloc(mem: ISet<int>, any_bytes: bool) {
                require pre.free_except(mem, None);
                update groups = pre.groups.insert(pre.next_id,
                    Group::singleton(pre.next_id, mem, true, any_bytes));
                update next_id = pre.next_id + 1;
            }
        }

        /// `BufMut::split_at` (and `chunks`): one unique lease becomes two.
        transition! {
            split(i: Id, left: ISet<int>) {
                require pre.is_program(i) && pre.groups[i].members.len() == 1 && pre.groups[i].members[0].unique;
                require left.subset_of(pre.groups[i].members[0].mem);
                let l = pre.groups[i];
                update groups = pre.groups.remove(i)
                    .insert(pre.next_id, Group::singleton(pre.next_id, left, true, l.members[0].any_bytes))
                    .insert(pre.next_id + 1, Group::singleton(pre.next_id + 1, l.members[0].mem.difference(left), true, l.members[0].any_bytes));
                update next_id = pre.next_id + 2;
            }
        }

        /// `BufMut::freeze`: the buffer becomes shared and read-only.
        transition! {
            freeze(i: Id) {
                require pre.is_program(i) && pre.groups[i].members.len() == 1 && pre.groups[i].members[0].unique;
                let l = pre.groups[i];
                update groups = pre.groups.insert(i,
                    Group::singleton(l.members[0].id, l.members[0].mem, false, l.members[0].any_bytes));
            }
        }

        /// `Buf::clone` and `Buf::slice`: another read-only view of (part of) a shared buffer.
        transition! {
            share(i: Id, sub: ISet<int>) {
                require pre.is_program(i) && pre.groups[i].members.len() == 1 && !pre.groups[i].members[0].unique;
                require sub.subset_of(pre.groups[i].members[0].mem);
                let l = pre.groups[i];
                update groups = pre.groups.insert(pre.next_id,
                    Group::singleton(pre.next_id, sub, false, l.members[0].any_bytes));
                update next_id = pre.next_id + 1;
            }
        }

        /// `Buf::try_thaw` succeeds: no other view and no request or exposure holds the bytes.
        transition! {
            thaw(i: Id) {
                require pre.is_program(i) && pre.groups[i].members.len() == 1 && !pre.groups[i].members[0].unique;
                require pre.free_except(pre.groups[i].members[0].mem, Some(i));
                let l = pre.groups[i];
                update groups = pre.groups.insert(i,
                    Group::singleton(l.members[0].id, l.members[0].mem, true, l.members[0].any_bytes));
            }
        }

        /// Dropping a `BufMut` or a `Buf` view.
        transition! {
            drop(i: Id) {
                require pre.is_program(i);
                update groups = pre.groups.remove(i);
            }
        }

        // ---------------------------------------------------------------------------------
        // Requests

        /// Posting a request (a send, write, receive or read, a batch, one entry of a sink
        /// or stream): one of the program's groups moves into it, with a direction for each
        /// member. The NIC may only write into a unique lease whose type accepts any bytes.
        transition! {
            post_request(i: Id, q: QpId, dirs: Seq<Dir>) {
                require pre.is_program(i) && pre.qps.contains(q);
                let l = pre.groups[i];
                require l.allows(dirs);
                let r = pre.next_id;
                let req = Req { qp: q, dirs, phase: Phase::InFlight, waker: None, detached: false };
                update groups = pre.groups.remove(i).insert(r, l.with_req(req));
                update posted = pre.posted.insert(r, l.members);
                update next_id = r + 1;
            }
        }

        /// The NIC finished request `r` with outcome `o` (the environment's move; the library
        /// learns it by polling the CQ). A registered waker is invoked; a detached request's
        /// leases are released.
        transition! {
            complete(r: Id, o: Outcome) {
                require pre.in_flight(r);
                let l = pre.groups[r];
                let req = pre.req(r);
                update outcomes = pre.outcomes.insert(r, o);
                update wakes = if req.waker is Some { pre.wakes.push(req.waker->0) } else { pre.wakes };
                update groups = if req.detached { pre.groups.remove(r) }
                    else { pre.groups.insert(r, l.with_req(req.completed(o))) };
            }
        }

        /// The future of `r` was polled and returned `Pending`, registering waker `w`.
        transition! {
            register(r: Id, w: WakerId) {
                require pre.in_flight(r) && !pre.req(r).detached;
                update groups = pre.groups.insert(r, pre.groups[r].with_req(pre.req(r).with_waker(w)));
            }
        }

        /// The future of `r` returned `Ready(o)`: the client takes the outcome and gets the
        /// request's group back, under a fresh handle.
        transition! {
            take(r: Id, o: Outcome) {
                require pre.is_request(r) && !pre.req(r).detached;
                require pre.req(r).phase == Phase::Complete(o);
                update groups = pre.groups.remove(r).insert(pre.next_id,
                    pre.groups[r].with_holder(Holder::Program));
                update next_id = pre.next_id + 1;
            }
        }

        /// The future of `r` was dropped. A complete request's leases are released now, an
        /// in-flight one's when it completes; nothing is cancelled.
        transition! {
            detach(r: Id) {
                require pre.is_request(r) && !pre.req(r).detached;
                update groups = if pre.req(r).phase is Complete { pre.groups.remove(r) }
                    else { pre.groups.insert(r, pre.groups[r].with_req(pre.req(r).detach())) };
            }
        }

        // ---------------------------------------------------------------------------------
        // Remote access

        /// Registering a buffer for remote access and handing its rkey to the peer. Remote
        /// write needs a unique buffer whose type accepts any bytes.
        transition! {
            expose(i: Id, remote_write: bool) {
                require pre.is_program(i);
                require remote_write ==> pre.groups[i].all_writable();
                update groups = pre.groups.insert(i, pre.groups[i].with_holder(Holder::Exposed { remote_write }));
            }
        }

        /// Deregistering it (the rkey stops working): the program gets the buffer back.
        transition! {
            unexpose(i: Id) {
                require pre.groups.contains_key(i) && pre.groups[i].holder is Exposed;
                update groups = pre.groups.insert(i, pre.groups[i].with_holder(Holder::Program));
            }
        }

        // ---------------------------------------------------------------------------------
        // Invariants

        /// Names below `next_id` only: groups, their leases and requests with an outcome.
        #[invariant]
        pub fn inv_ids(&self) -> bool {
            &&& forall|i: Id| #[trigger] self.groups.contains_key(i) ==> {
                &&& i < self.next_id
                &&& forall|k: int| 0 <= k < self.groups[i].members.len()
                    ==> (#[trigger] self.groups[i].members[k]).id < self.next_id
            }
            &&& forall|i: Id| #[trigger] self.outcomes.contains_key(i) ==> i < self.next_id
        }

        /// The heart of S1: a unique lease overlaps no other lease, in its group or another,
        /// and no two leases share an identity.
        #[invariant]
        pub fn inv_unique(&self) -> bool {
            &&& forall|i: Id| #[trigger] self.groups.contains_key(i) ==> self.groups[i].wf()
            &&& forall|i: Id, j: Id| #![trigger self.groups[i], self.groups[j]]
                self.groups.contains_key(i) && self.groups.contains_key(j) && i != j
                    ==> self.groups[i].compatible(self.groups[j])
        }

        /// Memory the NIC may write is in unique leases that accept any bytes.
        #[invariant]
        pub fn inv_nic_writes(&self) -> bool {
            forall|i: Id| #[trigger] self.groups.contains_key(i) ==> {
                let g = self.groups[i];
                &&& g.is_request() ==> g.allows(g.holder->req.dirs)
                &&& g.holder is Exposed && g.holder->remote_write ==> g.all_writable()
            }
        }

        /// A request keeps the members it was posted with, lives on a queue pair that exists,
        /// and has an outcome exactly when it is complete (the one recorded).
        #[invariant]
        pub fn inv_requests(&self) -> bool {
            forall|r: Id| #[trigger] self.groups.contains_key(r) && self.groups[r].is_request() ==> {
                let req = self.req(r);
                &&& self.posted.contains_key(r) && self.posted[r] == self.groups[r].members
                &&& self.qps.contains(req.qp)
                &&& req.phase is InFlight <==> !self.outcomes.contains_key(r)
                &&& req.phase is Complete ==> self.outcomes[r] == req.phase->0
            }
        }

        /// A complete or detached request holds no waker.
        #[invariant]
        pub fn inv_wakers(&self) -> bool {
            forall|r: Id| #[trigger] self.groups.contains_key(r) && self.groups[r].is_request() ==> {
                let req = self.req(r);
                (req.phase is Complete || req.detached) ==> req.waker is None
            }
        }

        // ---------------------------------------------------------------------------------
        // Inductiveness

        #[inductive(empty)]
        pub fn empty_inductive(pre: Self, post: Self) {}

        #[inductive(join)]
        pub fn join_inductive(pre: Self, post: Self, i: Id, j: Id) {
            crate::abs_proofs::lemma_join(pre, post, i, j);
        }

        #[inductive(separate)]
        pub fn separate_inductive(pre: Self, post: Self, i: Id, at: int) {
            crate::abs_proofs::lemma_separate(pre, post, i, at);
        }

        #[inductive(start)]
        pub fn start_inductive(post: Self) {}

        #[inductive(create_qp)]
        pub fn create_qp_inductive(pre: Self, post: Self, q: QpId) {}

        #[inductive(drop_qp)]
        pub fn drop_qp_inductive(pre: Self, post: Self, q: QpId) {}

        #[inductive(alloc)]
        pub fn alloc_inductive(pre: Self, post: Self, mem: ISet<int>, any_bytes: bool) {}

        #[inductive(split)]
        pub fn split_inductive(pre: Self, post: Self, i: Id, left: ISet<int>) {}

        #[inductive(freeze)]
        pub fn freeze_inductive(pre: Self, post: Self, i: Id) {}

        #[inductive(share)]
        pub fn share_inductive(pre: Self, post: Self, i: Id, sub: ISet<int>) {}

        #[inductive(thaw)]
        pub fn thaw_inductive(pre: Self, post: Self, i: Id) {}

        #[inductive(drop)]
        pub fn drop_inductive(pre: Self, post: Self, i: Id) {}

        #[inductive(post_request)]
        pub fn post_request_inductive(pre: Self, post: Self, i: Id, q: QpId, dirs: Seq<Dir>) {}

        #[inductive(complete)]
        pub fn complete_inductive(pre: Self, post: Self, r: Id, o: Outcome) {}

        #[inductive(register)]
        pub fn register_inductive(pre: Self, post: Self, r: Id, w: WakerId) {}

        #[inductive(take)]
        pub fn take_inductive(pre: Self, post: Self, r: Id, o: Outcome) {}

        #[inductive(detach)]
        pub fn detach_inductive(pre: Self, post: Self, r: Id) {}

        #[inductive(expose)]
        pub fn expose_inductive(pre: Self, post: Self, i: Id, remote_write: bool) {}

        #[inductive(unexpose)]
        pub fn unexpose_inductive(pre: Self, post: Self, i: Id) {}
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
        Abs::Step::empty() => Abs::State::empty(pre, post),
        Abs::Step::join(i, j) => Abs::State::join(pre, post, i, j),
        Abs::Step::separate(i, at) => Abs::State::separate(pre, post, i, at),
        Abs::Step::create_qp(q) => Abs::State::create_qp(pre, post, q),
        Abs::Step::drop_qp(q) => Abs::State::drop_qp(pre, post, q),
        Abs::Step::alloc(mem, any_bytes) => Abs::State::alloc(pre, post, mem, any_bytes),
        Abs::Step::split(i, left) => Abs::State::split(pre, post, i, left),
        Abs::Step::freeze(i) => Abs::State::freeze(pre, post, i),
        Abs::Step::share(i, sub) => Abs::State::share(pre, post, i, sub),
        Abs::Step::thaw(i) => Abs::State::thaw(pre, post, i),
        Abs::Step::drop(i) => Abs::State::drop(pre, post, i),
        Abs::Step::post_request(i, q, dirs) => Abs::State::post_request(pre, post, i, q, dirs),
        Abs::Step::complete(r, o) => Abs::State::complete(pre, post, r, o),
        Abs::Step::register(r, w) => Abs::State::register(pre, post, r, w),
        Abs::Step::take(r, o) => Abs::State::take(pre, post, r, o),
        Abs::Step::detach(r) => Abs::State::detach(pre, post, r),
        Abs::Step::expose(i, remote_write) => Abs::State::expose(pre, post, i, remote_write),
        Abs::Step::unexpose(i) => Abs::State::unexpose(pre, post, i),
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
