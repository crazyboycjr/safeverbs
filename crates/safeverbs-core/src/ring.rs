//! The completion ring of one work queue: which requests are in flight, what
//! they hold, and when they retire.
//!
//! This module is bookkeeping only: no FFI, no `unsafe`, no closures or
//! iterator chains. It is written inside `verus!` and proved: Verus checks
//! that every normal return keeps the invariants below and that arithmetic
//! and indexing never panic or overflow (`scripts/verify.sh`; a plain `cargo build` erases the proofs).
//! The worker (`crate::worker`) drives it next to the hardware queue.
//!
//! # Requests, WQEs and sequence numbers
//!
//! A *request* is what one future (or one synchronous post) waits for: one
//! work request, or a batch of them rung with one doorbell. Each request
//! takes one *slot* and is numbered by its [`Seq`], the per-queue sequence
//! number of requests. A request is made of `1..=max_wqes` WQEs (work queue
//! elements), which are numbered by a second per-queue sequence: the WQE
//! sequence number is the `wr_id` the hardware gets and reports back.
//!
//! A request moves through these states:
//!
//! ```text
//!             stage                 commit            complete
//!   Free ───────────────▶ Staged ───────▶ InFlight ─────────▶ Retired
//!   ▲ ▲ ▲                    │ abort                │ detached     │ take / detach
//!   │ │ └────────────────────┼──────────────────────┴──────────────┘
//!   │ │  detached, or kept   │ abort, owned
//!   │ └──────────────────────┤
//!   │                        ▼
//!   └────────────────────  Refused
//!        take / detach
//! ```
//!
//! No slot stores its state: the counters say where a request is (`[head, retired)` retired,
//! `[retired, committed)` in flight, `[committed, posted)` staged), a slot without leases is
//! free, and a refused request's outcome says so ([`Ring::state`]). So committing is one store
//! of each counter, whatever the number of requests.
//!
//! *Staged* requests have their WQEs written but the doorbell not rung
//! (the WQEs of a batch or a sink before its flush); they are not posted
//! yet, and enter the abstract machine A and the NIC model M of
//! `crates/safeverbs-spec` only when committed;
//! [`Ring::commit`] rings it (all staged requests at once). [`Ring::abort`]
//! says the device refused the doorbell: every staged request becomes
//! *Refused*, a result that never reached the NIC, which the result
//! says to its owner, or is freed if it has none; sequence numbers are never
//! reused, and the WQE numbers the NIC never saw go to the next requests.
//! *Retired* and *Refused* requests have their outcome and still hold their
//! buffer leases until the owner takes them ([`Ring::take`]). A *detached*
//! request has no owner (its future was dropped, or it was posted without
//! one): it releases its leases when it retires and frees its slot at once.
//!
//! # In-order completion
//!
//! The ring is for queues that complete in posting order: the send queues
//! of RC and UC QPs and receive queues without an SRQ. A completion for WQE
//! `n` therefore means that every WQE before `n` has completed too, so it
//! retires every request whose WQEs all lie at or before `n`. A request
//! whose last WQE is not signaled produces no completion of its own and is
//! retired by a later one, with the outcome made up at posting
//! (`Slot::outcome`). An error completion is produced for unsignaled WQEs
//! as well; one for a WQE inside a batch is recorded on the batch, which
//! retires with that error once its last WQE completes (normally with a
//! flush error, after the QP entered the error state). Out-of-order queues
//! (UD over several paths, SRQs) need a different structure and are not
//! handled here.
//!
//! # Room and signaling
//!
//! At most `depth` WQEs are in flight (posted and not retired), so the
//! hardware queue never overflows; posting past that is refused and the
//! caller waits (backpressure). WQEs after the last signaled request can
//! only be retired by a later signaled one, so the ring signals a request
//! itself when that tail would exceed `depth - max_wqes`: whatever is in
//! flight, once the signaled requests retire there is room for a request of
//! `max_wqes` WQEs. With single-WQE requests (`max_wqes == 1`) that is a
//! signaled WQE at least every `depth - 1` posts.
//!
//! # Invariants
//!
//! Proved, not checked: [`Ring::new`] establishes `Ring::inv` (the
//! specification of the list below) and every operation requires and keeps
//! it. [`Ring::invariant`] checks the same list at run time, for tests.
//!
//! 1. `head <= retired <= committed <= posted` and `posted - head <= slots`:
//!    the occupied slots form the window `[head, posted)`, with the retired
//!    ones in `[head, retired)`, the ones in flight (and the refused ones
//!    among them) in `[retired, committed)` and the staged ones in
//!    `[committed, posted)`; every slot outside the window is free and
//!    holds nothing.
//! 2. `wqe_retired <= wqe_committed <= wqe_posted` and `wqe_posted -
//!    wqe_retired <= depth`: never more than `depth` WQEs in flight.
//! 3. The requests of `[retired, posted)` cover consecutive WQE ranges
//!    `[first_wqe, end_wqe)`, starting at `wqe_retired`, the committed ones
//!    ending at `wqe_committed`: `1..=max_wqes` WQEs for a request in flight
//!    or staged, none for a refused one (the NIC never saw its WQEs).
//! 4. A slot retires exactly once: only an in-flight slot retires, and
//!    `retired` only grows (past refused slots too, which have nothing to
//!    retire).
//! 5. Leases are released exactly once, only when their slot retires
//!    (detached), is taken, is detached after retiring or being refused, is
//!    refused without an owner, or the QP is destroyed
//!    ([`Ring::release_all`]); [`Ring::abort_keeping`] hands one request's
//!    back. Every staged, in-flight, refused or retired-but-not-taken slot
//!    holds its leases (`Some`), every free slot holds none.
//! 6. A retired, refused or free slot holds no waker: the waker registered
//!    on a slot is woken when it retires or is refused.
//! 7. A detached slot holds no waker, and a retired or refused slot is
//!    never detached (it is freed instead); a refused slot's outcome is a
//!    flush error.
//! 8. Outside an open request, the WQEs posted after the last signaled
//!    request (and not retired) number at most `depth - max_wqes`.
//!
//! No counter overflows: [`Ring::has_free_slot`] refuses a new request once
//! the WQE or request numbers could not hold `max_wqes` more (after 2^64
//! of them, which no NIC reaches). The proofs also rely on a few facts about wakers that
//! Verus takes on trust: [`core::task::Waker`] is opaque to them, and waking,
//! comparing and cloning one are the standard library's (see the TCB in
//! `docs/verification.md`). User wake panics are retained until the ring operation
//! finishes; a worker shares that retention across the whole already-polled CQ batch.
//! The panic then propagates with the bookkeeping consistent.

use core::task::Waker;

use safeverbs_spec::abs::{Phase, WakerId};
use vstd::prelude::*;

use crate::wc::{Completed, CompletionError, CompletionResult, Wc};

mod payload;
mod separation;
pub(crate) mod wake;

pub use payload::Payload;

use wake::WakeBatch;

verus! {

/// A request's sequence number in its queue.
pub type Seq = u64;

// =============================================================================
// Wakers (trusted: these are the standard library's)

/// `Waker` is opaque to the proofs.
#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
#[verifier::external_body]
pub struct ExWaker(Waker);

/// The name A gives a waker: wakers of the same task share it.
pub uninterp spec fn waker_id(waker: Waker) -> WakerId;

/// Whether `a` and `b` would wake the same task (then they share a name).
#[verifier::external_body]
fn will_wake(a: &Waker, b: &Waker) -> (same: bool)
    ensures
        same ==> waker_id(*a) == waker_id(*b),
{
    a.will_wake(b)
}

/// Forgets a value that holds nothing: its destructor has nothing to do.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn forget_free<T>(free: Option<T>)
    requires
        free is None,
{
    core::mem::forget(free);
}

/// Another handle to the task `waker` belongs to.
#[verifier::external_body]
fn clone_waker(waker: &Waker) -> (clone: Waker)
    ensures
        waker_id(clone) == waker_id(*waker),
{
    waker.clone()
}

/// The name of the waker in `waker`, if any.
pub open spec fn waker_name(waker: Option<Waker>) -> Option<WakerId> {
    match waker {
        Some(w) => Some(waker_id(w)),
        None => None,
    }
}

// =============================================================================
// What the abstract machine sees

/// A request of the ring as the abstract machine A sees it
/// (`safeverbs_spec::abs::Req`, less its queue pair, which the queue pair
/// knows), with what it holds.
pub ghost struct ReqView<L> {
    /// In flight, or complete with its outcome.
    pub phase: Phase,
    /// The waker its future registered.
    pub waker: Option<WakerId>,
    /// Its future was dropped (or it never had one).
    pub detached: bool,
    /// Its leases: the memory the NIC may use while it is in flight.
    pub leases: L,
}

impl<L> ReqView<L> {
    /// A newly written request: in flight once posted, no waker yet.
    pub open spec fn new(detached: bool, leases: L) -> ReqView<L> {
        ReqView { phase: Phase::InFlight, waker: None, detached, leases }
    }

    /// A's `register`: the future registered `waker`.
    pub open spec fn with_waker(self, waker: WakerId) -> ReqView<L> {
        ReqView { waker: Some(waker), ..self }
    }

    /// A's `detach` of a request not complete: no owner, no waker.
    pub open spec fn detach(self) -> ReqView<L> {
        ReqView { waker: None, detached: true, ..self }
    }

    /// A's `complete` of a request with an owner: the outcome, no waker.
    pub open spec fn completed(self, outcome: Wc) -> ReqView<L> {
        ReqView { phase: Phase::Complete(outcome.outcome()), waker: None, ..self }
    }
}

/// The interval and outcome bookkeeping of one ring request.
/// This observation creates neither an abstract request nor a WQE identity.
pub ghost struct RequestMeta {
    /// First WQE of this native request, inclusive.
    pub first: u64,
    /// End of its WQE interval, exclusive.
    pub end: u64,
    /// Operation-defined success used when a later CQE retires the request.
    pub implied: Wc,
    /// Its first observed error, or its final outcome after retirement.
    pub seen: Option<Wc>,
}

/// What [`Ring::take`] hands back of a retired or refused request.
pub struct Taken<L> {
    /// Its outcome: [`CompletionError::Refused`] if the NIC never saw it.
    pub outcome: CompletionResult,
    /// What it held.
    pub leases: L,
}

/// The first error of a request without an owner, until someone takes it.
#[derive(Clone, Copy)]
pub struct DetachedRecord {
    /// The request.
    pub seq: Seq,
    /// Why it did not complete successfully.
    pub error: CompletionError,
    /// Its first WQE (erased: only the proofs read it).
    pub first_wqe: Ghost<u64>,
    /// One past its last WQE (erased too).
    pub end_wqe: Ghost<u64>,
}

// =============================================================================
// Slots

/// Where a slot is in its life (see the module documentation).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    /// Holds nothing.
    Free,
    /// WQEs written, doorbell not rung.
    Staged,
    /// Posted, not completed.
    InFlight,
    /// Completed; outcome and leases waiting for the owner.
    Retired,
    /// Never posted: the doorbell that would have posted it was refused.
    /// Its outcome (a flush error) and leases wait for the owner.
    Refused,
}

/// One request's entry. Free when it holds no leases.
pub(crate) struct Slot<L> {
    /// One past the WQE sequence number of its last WQE; a refused request has no WQEs, and
    /// this is where they would have started.
    end_wqe: u64,
    /// The WQE sequence number of its first WQE (erased: the ring's `wqe_retired` is the
    /// oldest request's).
    first_wqe: Ghost<u64>,
    /// What it reports. Staged or in flight, the success a later completion would retire it
    /// with, or the first error recorded on it; then its outcome.
    pub(crate) outcome: CompletionResult,
    /// No owner: release the leases at retirement.
    detached: bool,
    /// What keeps its memory alive while the NIC uses it.
    pub(crate) leases: Option<L>,
}

impl<L> Slot<L> {
    /// Holds nothing: no outcome, no leases, no waker.
    spec fn is_empty(&self) -> bool {
        &&& self.state == State::Free
        &&& self.outcome is None
        &&& self.leases is None
        &&& self.waker is None
    }

    /// The slot with `state`.
    spec fn with_state(self, state: State) -> Slot<L> {
        Slot { state, ..self }
    }

    /// The slot freed of everything it holds.
    spec fn emptied(self) -> Slot<L> {
        Slot { state: State::Free, outcome: None, leases: None, waker: None, ..self }
    }

    /// The slot freed of everything it holds.
    fn free() -> (slot: Slot<L>)
    {
        Slot {
            end_wqe: 0,
            first_wqe: Ghost(0),
            outcome: Ok(Completed::Sent),
            detached: false,
            leases: None,
        }
    }
}

/// A staged slot after a refused doorbell ([`Ring::abort_keeping`]): its
/// WQEs gone (an empty range at `wqe`), its waker woken; freed if `keep` or
/// detached, else holding a flush error and its leases for the owner.
spec fn refuse<L>(slot: Slot<L>, keep: bool, wqe: u64) -> Slot<L> {
    if keep || slot.detached {
        Slot {
            state: State::Free,
            first_wqe: wqe,
            end_wqe: wqe,
            outcome: None,
            leases: None,
            waker: None,
            ..slot
        }
    } else {
        Slot {
            state: State::Refused,
            first_wqe: wqe,
            end_wqe: wqe,
            outcome: Some(Wc { status: crate::wc::Status::WR_FLUSH_ERR, ..slot.implied }),
            waker: None,
            ..slot
        }
    }
}

// =============================================================================
// The ring

/// The ring of slots of one work queue.
///
/// `L` is what a request holds while the NIC may use its memory (the buffer
/// leases); the ring moves it in when the request is posted and out exactly
/// once.
pub struct Ring<L> {
    /// The slots, a power of two of them; request `seq` lives in `slots[seq & mask]`.
    slots: Vec<Slot<L>>,
    /// The number of slots, less one.
    mask: u64,
    /// The oldest request whose slot is not free yet.
    head: Seq,
    /// The oldest request not retired.
    retired: Seq,
    /// The oldest request not committed (the first staged one).
    committed: Seq,
    /// The next request's sequence number.
    posted: Seq,
    /// The most WQEs in flight at once (the hardware queue's depth).
    depth: u64,
    /// The most WQEs of one request.
    max_wqes: u64,
    /// One past the last retired WQE.
    wqe_retired: u64,
    /// One past the last committed WQE.
    wqe_committed: u64,
    /// One past the last WQE written.
    wqe_posted: u64,
    /// One past the last WQE of the last signaled request posted.
    sig_end: u64,
    /// `sig_end` as of the last commit, for [`Ring::abort`].
    committed_sig_end: u64,
    /// Whether an error completion has retired a request.
    failed: bool,
    /// The first error of a detached request, until someone takes it.
    detached_error: Option<DetachedRecord>,
    /// Who to wake when each slot's request retires: by slot, empty until a waker is first
    /// registered, so that a ring nobody waits on holds none.
    wakers: Vec<Option<Waker>>,
    /// The names of the wakers woken so far, in order (ghost).
    #[allow(dead_code)]
    woken: Ghost<vstd::seq::Seq<WakerId>>,
}

/// Requests less than `n` apart live in different slots of a ring of `n`.
proof fn lemma_distinct(n: int, a: int, b: int)
    requires
        1 <= n,
        0 <= a < b < a + n,
    ensures
        a % n != b % n,
{
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod(a, n);
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod(b, n);
    if a % n == b % n {
        let k = b / n - a / n;
        assert(b - a == n * k) by (nonlinear_arith)
            requires
                a == n * (a / n) + a % n,
                b == n * (b / n) + b % n,
                a % n == b % n,
                k == b / n - a / n,
        ;
        if k >= 1 {
            assert(n * k >= n) by (nonlinear_arith)
                requires
                    k >= 1,
                    n >= 1,
            ;
        } else {
            assert(n * k <= 0) by (nonlinear_arith)
                requires
                    k <= 0,
                    n >= 1,
            ;
        }
    }
}

/// Neither of two numbers exceeds their bitwise or.
proof fn lemma_or_bounds(a: u64, b: u64)
    ensures
        a <= (a | b),
        b <= (a | b),
{
    assert(a <= (a | b) && b <= (a | b)) by (bit_vector);
}

/// Two different requests less than `n` apart live in different slots.
proof fn lemma_other(n: int, a: int, b: int)
    requires
        1 <= n,
        0 <= a,
        0 <= b,
        a != b,
        a < b + n,
        b < a + n,
    ensures
        a % n != b % n,
{
    if a < b {
        lemma_distinct(n, a, b);
    } else {
        lemma_distinct(n, b, a);
    }
}

impl<L> Ring<L> {
    // -------------------------------------------------------------------------
    // The invariants, as specifications

    /// The number of slots.
    closed spec fn n(&self) -> int {
        self.slots@.len() as int
    }

    /// The slot request `seq` lives in.
    closed spec fn slot(&self, seq: int) -> Slot<L> {
        self.slots@[seq % self.n()]
    }

    /// One past the last WQE of the closed requests: every WQE handed out
    /// but the open request's.
    closed spec fn closed_end(&self) -> int {
        self.wqe_posted - self.open_wqes
    }

    /// Where the unsignaled tail starts, for the last signaled request
    /// ending at `sig_end`: after it, or after the last retired WQE.
    closed spec fn tail_start(&self, sig_end: u64) -> int {
        if sig_end > self.wqe_retired {
            sig_end as int
        } else {
            self.wqe_retired as int
        }
    }

    /// 1 and 2: the windows of requests and WQEs, and the bounds that keep
    /// the counters from overflowing.
    closed spec fn inv_counters(&self) -> bool {
        &&& self.n() >= 1
        &&& 1 <= self.max_wqes <= self.depth <= u32::MAX
        &&& self.head <= self.retired <= self.committed <= self.posted
        &&& self.posted - self.head <= self.n()
        // The open request has a slot waiting for it.
        &&& self.open ==> self.posted - self.head < self.n()
        &&& self.wqe_retired <= self.wqe_committed <= self.closed_end()
        &&& self.wqe_posted - self.wqe_retired <= self.depth
        &&& self.open_wqes <= self.max_wqes
        &&& !self.open ==> self.open_wqes == 0
        // The open request can take `max_wqes` WQEs, and a sequence number,
        // without overflow.
        &&& self.open ==> self.closed_end() + self.max_wqes <= u64::MAX
        &&& self.open ==> self.posted < u64::MAX
    }

    /// A slot holding the result of request `s`, retired or refused, for its
    /// owner: the outcome (a flush error if refused) and the leases, no
    /// waker.
    closed spec fn result_ok(&self, s: int, state: State) -> bool {
        let slot = self.slot(s);
        &&& slot.state == state
        &&& slot.seq == s
        &&& !slot.detached
        &&& slot.outcome is Some
        &&& slot.leases is Some
        &&& slot.waker is None
        &&& state == State::Refused ==> slot.outcome->0.status == crate::wc::Status::WR_FLUSH_ERR
    }

    /// 5, 6 and 7 for a request in `[head, retired)`: its slot is free, or
    /// holds its result.
    closed spec fn done_ok(&self, s: int) -> bool {
        let slot = self.slot(s);
        slot.is_empty() || self.result_ok(s, State::Retired) || self.result_ok(s, State::Refused)
    }

    /// 3 for a refused request in `[retired, committed)`: no WQEs (the NIC
    /// never saw them), between the retired and the committed ones; its
    /// slot is free or holds its result.
    closed spec fn void_ok(&self, s: int) -> bool {
        let slot = self.slot(s);
        &&& slot.first_wqe == slot.end_wqe
        &&& self.wqe_retired <= slot.first_wqe <= self.wqe_committed
        &&& slot.is_empty() || self.result_ok(s, State::Refused)
    }

    /// 3, 5 and 7 for a request in `[retired, posted)`: in flight (if
    /// committed) or staged, holding its leases, over the WQEs
    /// `[first_wqe, end_wqe)`; or refused (if committed and not in flight).
    closed spec fn live_ok(&self, s: int) -> bool {
        if s < self.committed && self.slot(s).state != State::InFlight {
            self.void_ok(s)
        } else {
            self.posted_ok(s)
        }
    }

    /// [`Ring::live_ok`] for a request in flight or staged.
    closed spec fn posted_ok(&self, s: int) -> bool {
        let slot = self.slot(s);
        &&& slot.seq == s
        &&& if s < self.committed {
            slot.state == State::InFlight
        } else {
            slot.state == State::Staged && slot.outcome is None
        }
        &&& slot.leases is Some
        &&& slot.detached ==> slot.waker is None
        &&& self.wqe_retired <= slot.first_wqe < slot.end_wqe <= self.closed_end()
        &&& slot.end_wqe - slot.first_wqe <= self.max_wqes
        &&& if s < self.committed {
            slot.end_wqe <= self.wqe_committed
        } else {
            self.wqe_committed <= slot.first_wqe
        }
    }

    /// 1 and 5 for a slot outside the window `[head, posted)`: it holds
    /// nothing.
    closed spec fn free_ok(&self, s: int) -> bool {
        self.slot(s).is_empty()
    }

    /// Every slot, by where its request is.
    #[verifier::opaque]
    closed spec fn inv_slots(&self) -> bool {
        &&& forall|s: int| self.head <= s < self.retired ==> #[trigger] self.done_ok(s)
        &&& forall|s: int| self.retired <= s < self.posted ==> #[trigger] self.live_ok(s)
        &&& forall|s: int| self.posted <= s < self.head + self.n() ==> #[trigger] self.free_ok(s)
    }

    /// 3: the requests not retired cover consecutive WQE ranges from
    /// `wqe_retired`, the committed ones up to `wqe_committed`, all up to
    /// the open request's.
    #[verifier::opaque]
    closed spec fn inv_wqes(&self) -> bool {
        &&& self.retired < self.posted ==> self.slot(self.retired as int).first_wqe
            == self.wqe_retired
        &&& forall|s: int|
            self.retired < s < self.posted ==> #[trigger] self.slot(s).first_wqe == self.slot(
                s - 1,
            ).end_wqe
        &&& if self.retired < self.committed {
            self.slot(self.committed - 1).end_wqe == self.wqe_committed
        } else {
            self.wqe_committed == self.wqe_retired
        }
        &&& if self.committed < self.posted {
            self.slot(self.posted - 1).end_wqe == self.closed_end()
        } else {
            self.wqe_committed == self.closed_end()
        }
    }

    /// 8: the WQEs posted after the last signaled request and not retired
    /// number at most `depth - max_wqes`, and so they do after an abort.
    closed spec fn inv_signal(&self) -> bool {
        &&& self.sig_end <= self.closed_end()
        &&& self.committed_sig_end <= self.wqe_committed
        &&& self.closed_end() - self.tail_start(self.sig_end) <= self.depth - self.max_wqes
        &&& self.wqe_committed - self.tail_start(self.committed_sig_end) <= self.depth
            - self.max_wqes
    }

    /// The head is advanced past the free slots at the front of the window.
    closed spec fn head_ok(&self) -> bool {
        self.head < self.retired ==> self.slot(self.head as int).state != State::Free
    }

    /// Everything but [`Ring::head_ok`].
    closed spec fn inv_body(&self) -> bool {
        &&& self.inv_counters()
        &&& self.inv_slots()
        &&& self.inv_wqes()
        &&& self.inv_signal()
    }

    /// The invariants of the module documentation.
    pub closed spec fn inv(&self) -> bool {
        self.inv_body() && self.head_ok()
    }

    /// The state of request `seq`'s slot, as [`Ring::state`] reports it.
    pub closed spec fn state_of(&self, seq: int) -> State {
        if seq < self.head || seq >= self.posted || self.slot(seq).seq != seq {
            State::Free
        } else {
            self.slot(seq).state
        }
    }

    /// What slot `s` holds, as a request.
    closed spec fn view_of(&self, s: int) -> ReqView<L> {
        let slot = self.slot(s);
        ReqView {
            phase: if slot.state == State::Retired {
                Phase::Complete(slot.outcome->0.outcome())
            } else {
                Phase::InFlight
            },
            waker: waker_name(slot.waker),
            detached: slot.detached,
            leases: slot.leases->0,
        }
    }

    /// Request `s` as A sees it, if A sees it: posted to the NIC (committed)
    /// and not freed (retired and taken, or detached).
    pub closed spec fn req(&self, s: int) -> Option<ReqView<L>> {
        if self.head <= s < self.committed && self.slot(s).state != State::Free && self.slot(
            s,
        ).state != State::Refused {
            Some(self.view_of(s))
        } else {
            None
        }
    }

    /// The leases of request `s` if it was refused (its doorbell refused)
    /// and its owner has not taken it. A never saw it: its leases are the
    /// program's, which the ring holds for the owner.
    pub closed spec fn refused(&self, s: int) -> Option<L> {
        if self.head <= s < self.committed && self.slot(s).state == State::Refused {
            Some(self.slot(s).leases->0)
        } else {
            None
        }
    }

    /// Request `s` if it is staged: written but not posted. A does not see
    /// it yet (its leases are still the program's).
    pub closed spec fn staged(&self, s: int) -> Option<ReqView<L>> {
        if self.committed <= s < self.posted {
            Some(self.view_of(s))
        } else {
            None
        }
    }

    /// The native WQE interval, inferred success and cached first error.
    /// Refused results are excluded even if their old WQE numbers are reused.
    pub closed spec fn meta(&self, s: int) -> Option<RequestMeta> {
        if self.req(s) is Some || self.staged(s) is Some {
            let slot = self.slot(s);
            Some(RequestMeta { first: slot.first_wqe, end: slot.end_wqe,
                implied: slot.implied, seen: slot.outcome })
        } else { None }
    }

    /// A stuttering operation leaves native intervals and outcome caches alone.
    pub open spec fn same_meta(&self, pre: &Self) -> bool {
        forall|s: int| #[trigger] self.meta(s) == pre.meta(s)
    }

    /// Metadata after retiring a request, before its slot is reused.
    pub closed spec fn completed_meta(&self, s: int, outcome: Wc) -> Option<RequestMeta> {
        if self.slot(s).detached || self.slot(s).state != State::InFlight { None }
        else { Some(RequestMeta { seen: Some(outcome), ..self.meta(s)->0 }) }
    }

    /// A partial error records the first failure and leaves it unchanged later.
    pub closed spec fn recorded_meta(&self, s: int, wc: Wc) -> Option<RequestMeta> {
        Some(RequestMeta { seen: Some(wc), ..self.meta(s)->0 })
    }

    /// Inspect the native bookkeeping of a live or staged request.
    pub proof fn lemma_meta(&self, s: int)
        requires self.inv(),
        ensures
            self.meta(s) is Some <==> self.req(s) is Some || self.staged(s) is Some,
            self.meta(s) is Some && (self.req(s) is Some && self.req(s)->0.phase is InFlight
                || self.staged(s) is Some)
                ==> self.spec_wqe_retired() <= self.meta(s)->0.first < self.meta(s)->0.end
                    <= self.spec_closed_end(),
            self.req(s) is Some && self.req(s)->0.phase is Complete
                ==> self.meta(s)->0.seen is Some
                    && self.req(s)->0.phase == Phase::Complete(self.meta(s)->0.seen->0.outcome()),
            self.staged(s) is Some ==> self.meta(s)->0.seen is None,
            self.staged(s) is Some ==> self.spec_wqe_committed() <= self.meta(s)->0.first,
            self.req(s) is Some && self.req(s)->0.phase is InFlight
                ==> self.meta(s)->0.end <= self.spec_wqe_committed(),
    {
        self.lemma_view();
        reveal(Ring::inv_slots);
        if self.meta(s) is Some {
            if s < self.retired { assert(self.done_ok(s)); }
            else { assert(self.live_ok(s)); }
        }
    }

    /// The live request retaining WQE `w`. Refused and retired slots
    /// never authorize DMA, even when a refused WQE number is reused.
    pub closed spec fn dma(&self, s: int, w: int) -> Option<L> {
        if self.head <= s < self.posted
            && (self.slot(s).state == State::InFlight || self.slot(s).state == State::Staged)
            && self.slot(s).first_wqe <= w < self.slot(s).end_wqe {
            self.slot(s).leases
        } else { None }
    }

    /// The end of the closed requests, excluding the request being gathered.
    pub open spec fn spec_closed_end(&self) -> int {
        self.spec_wqe_posted() - self.spec_open_wqes()
    }

    /// All still-live WQEs retain their original request and exact resource value.
    pub open spec fn keeps_dma(&self, pre: &Self) -> bool {
        forall|s: int, w: int| pre.dma(s, w) is Some
            && self.spec_wqe_retired() <= w < self.spec_closed_end()
            ==> #[trigger] self.dma(s, w) == pre.dma(s, w)
    }

    /// Every live WQE retains exactly the same request and resource value.
    pub open spec fn same_dma(&self, pre: &Self) -> bool {
        forall|s: int, w: int| #[trigger] self.dma(s, w) == pre.dma(s, w)
    }

    /// A DMA observer names an in-flight or staged request, with its
    /// exact resources, and never a result retained after refusal.
    pub proof fn lemma_dma(&self, s: int, w: int)
        requires self.inv(), self.dma(s, w) is Some,
        ensures
            self.spec_retired() <= s < self.spec_posted(),
            self.spec_wqe_retired() <= w < self.spec_closed_end(),
            (self.req(s) is Some && self.req(s)->0.phase is InFlight
                && self.req(s)->0.leases == self.dma(s, w)->0)
                || (self.staged(s) is Some && self.staged(s)->0.leases == self.dma(s, w)->0),
            self.refused(s) is None,
            w < self.spec_wqe_committed() ==> self.req(s) is Some
                && self.req(s)->0.phase is InFlight && self.req(s)->0.leases == self.dma(s, w)->0,
            self.spec_wqe_committed() <= w ==> self.staged(s) is Some
                && self.staged(s)->0.leases == self.dma(s, w)->0,
    {
        reveal(Ring::inv_slots);
        if s < self.retired { assert(self.done_ok(s)); }
        assert(self.live_ok(s));
    }

    /// Two live requests cannot authorize the same WQE number. This is
    /// independent of wr_id encoding and excludes historical refused ledgers.
    pub proof fn lemma_dma_unique(&self, s: int, t: int, w: int)
        requires self.inv(), self.dma(s, w) is Some, self.dma(t, w) is Some,
        ensures s == t,
    {
        self.lemma_dma(s, w);
        self.lemma_dma(t, w);
        if s < t { self.lemma_ordered(s, t); }
        if t < s { self.lemma_ordered(t, s); }
    }

    /// The names of the wakers the ring has woken, in order.
    pub closed spec fn woken(&self) -> vstd::seq::Seq<WakerId> {
        self.woken@
    }

    /// A sees the same requests in `self` and `other`, with the same staged
    /// ones and wakers woken: a stuttering step.
    pub open spec fn same_view(&self, other: &Self) -> bool {
        &&& forall|s: int| #[trigger] self.req(s) == other.req(s)
        &&& forall|s: int| #[trigger] self.staged(s) == other.staged(s)
        &&& forall|s: int| #[trigger] self.refused(s) == other.refused(s)
        &&& self.woken() == other.woken()
        &&& self.same_dma(other)
        &&& self.same_meta(other)
    }

    /// What A sees of the ring, as functions of the sequence number.
    pub open spec fn view(&self) -> crate::refine::View<L> {
        crate::refine::View {
            req: |s: int| self.req(s),
            staged: |s: int| self.staged(s),
            refused: |s: int| self.refused(s),
        }
    }

    /// The ring's view is well formed (`crate::refine::wf`): requests in
    /// `[retired, committed)` are in flight, those before complete or gone,
    /// those in `[committed, posted)` staged.
    pub proof fn lemma_view(&self)
        requires
            self.inv(),
        ensures
            crate::refine::wf(self.view()),
            self.spec_head() <= self.spec_retired() <= self.spec_committed() <= self.spec_posted(),
            forall|s: int|
                (#[trigger] self.staged(s) is Some) == (self.spec_committed() <= s
                    < self.spec_posted()),
            forall|s: int|
                #[trigger] self.req(s) is Some ==> self.spec_head() <= s < self.spec_committed() && (
                self.req(s)->0.phase is InFlight) == (self.spec_retired() <= s),
            forall|s: int|
                #[trigger] self.refused(s) is Some ==> self.spec_head() <= s < self.spec_committed()
                    && self.req(s) is None,
    {
        reveal(Ring::inv_slots);
        assert forall|s: int| #[trigger] crate::refine::wf_at(self.view(), s) by {
            if self.req(s) is Some {
                if s < self.retired {
                    assert(self.done_ok(s));
                } else {
                    assert(self.live_ok(s));
                }
            }
            if self.staged(s) is Some {
                assert(self.live_ok(s));
            }
        }
        assert forall|s: int| #[trigger] self.req(s) is Some implies self.spec_head() <= s
            < self.spec_committed() && (self.req(s)->0.phase is InFlight) == (self.spec_retired()
            <= s) by {
            if s < self.retired {
                assert(self.done_ok(s));
            } else {
                assert(self.live_ok(s));
            }
        }
    }

    /// A stuttering step changes nothing A sees.
    pub proof fn lemma_same_view(&self, other: &Self)
        requires
            self.same_view(other),
        ensures
            crate::refine::same(other.view(), self.view()),
    {
    }

    /// A request in flight retires as A's `complete` says.
    pub proof fn lemma_retired_view(&self, s: int, outcome: Wc)
        requires
            self.inv(),
            self.spec_retired() <= s < self.spec_committed(),
        ensures
            self.retired_view(s, outcome) == match self.req(s) {
                None => None,
                Some(r) => if r.detached {
                    None
                } else {
                    Some(r.completed(outcome))
                },
            },
    {
        reveal(Ring::inv_slots);
        assert(self.live_ok(s));
    }

    /// The wakers of requests in flight, as A sees them.
    pub proof fn lemma_wakers(&self, from: int, to: int)
        requires
            self.inv(),
            self.spec_retired() <= from <= to <= self.spec_committed(),
        ensures
            self.wakers(from, to) == crate::refine::wakers(self.view(), from, to),
        decreases to - from,
    {
        reveal(Ring::inv_slots);
        if from < to {
            self.lemma_wakers(from, to - 1);
            assert(self.live_ok(to - 1));
        }
    }

    /// `self` and `other` agree on everything but the slots.
    closed spec fn same_counters(&self, other: &Self) -> bool {
        &&& self.same_counters_but_woken(other)
        &&& self.woken == other.woken
    }

    /// `self` and `other` agree on everything but the slots and the wakers
    /// woken.
    closed spec fn same_counters_but_woken(&self, other: &Self) -> bool {
        &&& self.head == other.head
        &&& self.retired == other.retired
        &&& self.committed == other.committed
        &&& self.posted == other.posted
        &&& self.depth == other.depth
        &&& self.max_wqes == other.max_wqes
        &&& self.wqe_retired == other.wqe_retired
        &&& self.wqe_committed == other.wqe_committed
        &&& self.wqe_posted == other.wqe_posted
        &&& self.sig_end == other.sig_end
        &&& self.committed_sig_end == other.committed_sig_end
        &&& self.open == other.open
        &&& self.open_wqes == other.open_wqes
    }

    /// The slot invariants depend on the slots and the window counters only.
    proof fn lemma_frame(pre: &Self, post: &Self)
        requires
            pre.inv_slots(),
            pre.inv_wqes(),
            post.slots@ == pre.slots@,
            post.head == pre.head,
            post.retired == pre.retired,
            post.committed == pre.committed,
            post.posted == pre.posted,
            post.max_wqes == pre.max_wqes,
            post.wqe_retired == pre.wqe_retired,
            post.wqe_committed == pre.wqe_committed,
            post.closed_end() == pre.closed_end(),
        ensures
            post.inv_slots(),
            post.inv_wqes(),
            pre.head_ok() ==> post.head_ok(),
            post.same_dma(pre),
            post.same_meta(pre),
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        assert forall|s: int| post.head <= s < post.retired implies #[trigger] post.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| post.retired <= s < post.posted implies #[trigger] post.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| post.posted <= s < post.head + post.n() implies #[trigger] post.free_ok(
            s,
        ) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| post.retired < s < post.posted implies #[trigger] post.slot(s).first_wqe
            == post.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe == pre.slot(s - 1).end_wqe);
        }
    }

    /// The WQE ranges of the requests not retired are in order: a later
    /// request's WQEs come after an earlier one's.
    proof fn lemma_ordered(&self, s: int, t: int)
        requires
            self.inv_body(),
            self.retired <= s < t < self.posted,
        ensures
            self.slot(s).end_wqe <= self.slot(t).first_wqe,
        decreases t - s,
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        assert(self.slot(t).first_wqe == self.slot(t - 1).end_wqe);
        if s < t - 1 {
            self.lemma_ordered(s, t - 1);
            assert(self.live_ok(t - 1));
        }
    }

    // -------------------------------------------------------------------------
    // Construction and observers

    /// A ring of at least `slots` slots (the next power of two) for a queue of `depth` WQEs,
    /// whose requests have at most `max_wqes` WQEs.
    ///
    /// # Panics
    ///
    /// Unless `slots >= 1` and `1 <= max_wqes <= depth` (checked at run
    /// time too, for callers outside the proofs).
    pub fn new(slots: usize, depth: u32, max_wqes: u32) -> (ring: Ring<L>)
    {
        assert!(slots >= 1, "a ring needs a slot");
        assert!(slots <= 1 << 40, "a ring of at most 2^40 slots");
        assert!(max_wqes >= 1 && max_wqes <= depth, "a request needs 1..=depth WQEs");
        let mut n: usize = 1;
        while n < slots
        {
            n *= 2;
        }
        let mut v: Vec<Slot<L>> = Vec::with_capacity(n);
        let mut i = 0;
        while i < n
        {
            v.push(Slot::free());
            i += 1;
        }
        Ring {
            slots: v,
            mask: (n - 1) as u64,
            head: 0,
            retired: 0,
            committed: 0,
            posted: 0,
            depth: depth as u64,
            max_wqes: max_wqes as u64,
            wqe_retired: 0,
            wqe_committed: 0,
            wqe_posted: 0,
            sig_end: 0,
            committed_sig_end: 0,
            failed: false,
            detached_error: None,
            wakers: Vec::new(),
            woken: Ghost(vstd::seq::Seq::empty()),
        }
    }

    #[inline(always)]
    fn index(&self, seq: Seq) -> (i: usize)
    {
        (seq & self.mask) as usize
    }

    /// The queue's depth in WQEs.
    pub fn depth(&self) -> (depth: u32)
        requires
            self.inv(),
        ensures
            depth == self.spec_depth(),
    {
        self.depth as u32
    }

    /// The most WQEs of one request.
    pub fn max_wqes(&self) -> u32
        requires
            self.inv(),
    {
        self.max_wqes as u32
    }

    /// The oldest request whose slot is not free (retired and not taken,
    /// or not retired); equal to [`Ring::posted`] when the ring is empty.
    #[verifier::when_used_as_spec(spec_head)]
    pub fn head(&self) -> (seq: Seq)
        ensures
            seq == self.spec_head(),
    {
        self.head
    }

    /// [`Ring::head`], as a specification.
    pub closed spec fn spec_head(&self) -> Seq {
        self.head
    }

    /// The next request's sequence number.
    #[verifier::when_used_as_spec(spec_posted)]
    pub fn posted(&self) -> (seq: Seq)
        ensures
            seq == self.spec_posted(),
    {
        self.posted
    }

    /// [`Ring::posted`], as a specification.
    pub closed spec fn spec_posted(&self) -> Seq {
        self.posted
    }

    /// The oldest request not retired: every request before it has retired.
    #[verifier::when_used_as_spec(spec_retired)]
    pub fn retired(&self) -> (seq: Seq)
        ensures
            seq == self.spec_retired(),
    {
        self.retired
    }

    /// [`Ring::retired`], as a specification.
    pub closed spec fn spec_retired(&self) -> Seq {
        self.retired
    }

    /// The oldest staged request: every request before it is committed.
    #[verifier::when_used_as_spec(spec_committed)]
    pub fn committed(&self) -> (seq: Seq)
        ensures
            seq == self.spec_committed(),
    {
        self.committed
    }

    /// [`Ring::committed`], as a specification.
    pub closed spec fn spec_committed(&self) -> Seq {
        self.committed
    }

    /// [`Ring::wqe_retired`], as a specification: one past the last retired
    /// WQE.
    pub closed spec fn spec_wqe_retired(&self) -> u64 {
        self.wqe_retired
    }

    /// One past the last WQE handed out (the open request's included).
    pub closed spec fn spec_wqe_posted(&self) -> u64 {
        self.wqe_posted
    }

    /// [`Ring::depth`], as a specification.
    pub closed spec fn spec_depth(&self) -> u64 {
        self.depth
    }

    /// [`Ring::max_wqes`], as a specification.
    pub closed spec fn spec_max_wqes(&self) -> u64 {
        self.max_wqes
    }

    /// `self` and `other` agree on the WQE accounting: what the hardware
    /// queue holds is the same.
    pub open spec fn same_wqes(&self, other: &Self) -> bool {
        &&& self.spec_wqe_retired() == other.spec_wqe_retired()
        &&& self.spec_wqe_committed() == other.spec_wqe_committed()
        &&& self.spec_wqe_posted() == other.spec_wqe_posted()
        &&& self.spec_depth() == other.spec_depth()
    }

    /// `self` and `other` agree on the open request: whether there is one,
    /// and its WQEs.
    pub open spec fn same_open(&self, other: &Self) -> bool {
        &&& self.spec_is_open() == other.spec_is_open()
        &&& self.spec_open_wqes() == other.spec_open_wqes()
    }

    /// The WQE accounting (invariant 2): the WQEs retired, committed and
    /// handed out come in that order, at most `depth` of them in flight, and
    /// the open request's are the last ones handed out.
    pub proof fn lemma_wqes(&self)
        requires
            self.inv(),
        ensures
            self.spec_wqe_retired() <= self.spec_wqe_committed() <= self.spec_wqe_posted(),
            self.spec_wqe_posted() - self.spec_wqe_retired() <= self.spec_depth(),
            1 <= self.spec_depth() <= u32::MAX,
            self.spec_has_wqe_room() ==> self.spec_wqe_posted() - self.spec_wqe_retired()
                < self.spec_depth(),
            self.spec_open_wqes() as int <= self.spec_wqe_posted() - self.spec_wqe_committed(),
            !self.spec_is_open() ==> self.spec_open_wqes() == 0,
            1 <= self.spec_max_wqes() <= self.spec_depth(),
            self.spec_has_wqe_room() <==> (self.spec_is_open() && self.spec_wqe_posted()
                - self.spec_wqe_retired() < self.spec_depth() && (self.spec_open_wqes() as int)
                < self.spec_max_wqes()),
    {
    }

    /// WQEs posted and not retired.
    pub fn in_flight_wqes(&self) -> (n: u32)
        requires
            self.inv(),
        ensures
            n == self.spec_wqe_posted() - self.spec_wqe_retired(),
    {
        (self.wqe_posted - self.wqe_retired) as u32
    }

    /// Whether some request is staged (written but not committed).
    pub fn has_staged(&self) -> bool {
        self.committed < self.posted
    }

    /// [`Ring::is_open`], as a specification.
    pub closed spec fn spec_is_open(&self) -> bool {
        self.open
    }

    /// Whether a slot is free for the next request.
    ///
    /// It also refuses once the WQE or request sequence numbers are nearly
    /// exhausted (after 2^64 - `max_wqes` of them, centuries at any rate a
    /// NIC can reach), so that no counter can overflow. One comparison
    /// covers both: neither number exceeds their bitwise or.
    #[inline(always)]
    pub fn has_free_slot(&self) -> (free: bool)
    {
        self.posted - self.head <= self.mask && (self.wqe_posted | self.posted)
            <= u64::MAX - self.max_wqes
    }

    /// [`Ring::has_free_slot`], as a specification.
    pub closed spec fn spec_has_free_slot(&self) -> bool {
        self.posted - self.head < self.n() && (self.wqe_posted | self.posted) + self.max_wqes
            <= u64::MAX
    }

    /// One past the last retired WQE: every WQE before it has completed (or
    /// was never seen by the NIC). A completion names a WQE at most
    /// [`Ring::depth`] after it.
    pub fn wqe_retired(&self) -> (wqe: u64)
        ensures
            wqe == self.spec_wqe_retired(),
    {
        self.wqe_retired
    }

    /// Whether a new request of one WQE could be posted now: a free slot and
    /// room for a WQE.
    #[inline(always)]
    pub fn can_post(&self) -> (can: bool)
    {
        self.has_free_slot() && self.wqe_posted - self.wqe_retired < self.depth
    }

    /// Whether every slot is taken by a retired request whose owner has not
    /// taken it: waiting for completions would not free one.
    pub fn is_clogged(&self) -> bool
        requires
            self.inv(),
    {
        !self.has_free_slot() && self.head < self.retired
    }

    /// Whether an error completion has retired a request.
    pub fn failed(&self) -> bool {
        self.failed
    }

    /// The detached error waiting to be taken.
    pub closed spec fn spec_detached_error(&self) -> Option<DetachedRecord> {
        self.detached_error
    }

    /// What a refused doorbell does to the detached error: it stays or, if
    /// there was none, a refused request without an owner has it.
    pub open spec fn detached_refused(&self, pre: &Self) -> bool {
        ||| self.spec_detached_error() == pre.spec_detached_error()
        ||| pre.spec_detached_error() is None && self.spec_detached_error() is Some
            && self.spec_detached_error()->0.refused
            && self.spec_detached_error()->0.wc.spec_status() == crate::wc::Status::WR_FLUSH_ERR
    }

    /// What the completion of WQE `wqe` with `wc` does to the detached error,
    /// which is `post` after it, having retired requests up to `to`: it stays
    /// or, if there was none, one of the requests without an owner it retires
    /// with an error has it.
    pub open spec fn detached_completed(&self, post: Option<DetachedRecord>, wqe: u64, wc: Wc, to: int) -> bool {
        ||| post == self.spec_detached_error()
        ||| self.spec_detached_error() is None && post is Some && {
            let d = post->0;
            let s = d.seq as int;
            &&& self.spec_retired() <= s < to
            &&& self.req(s) is Some && self.req(s)->0.phase is InFlight && self.req(s)->0.detached
            &&& d.wc == self.retire_outcome(s, wqe, wc)
            &&& !d.wc.spec_ok()
            &&& d.first_wqe == self.meta(s)->0.first
            &&& d.end_wqe@ == self.meta(s)->0.end
            &&& !d.refused
        }
    }

    /// `t` is what [`Ring::take`] hands back of request `s`: a refused one
    /// with a flush error, or a complete one with its outcome and its leases.
    pub open spec fn taken_from(&self, s: int, t: Taken<L>) -> bool {
        if t.refused {
            &&& self.req(s) is None
            &&& self.refused(s) == Some(t.leases)
            &&& t.wc.spec_status() == crate::wc::Status::WR_FLUSH_ERR
        } else {
            &&& self.req(s) == Some(
                ReqView { phase: Phase::Complete(t.wc.outcome()), waker: None, detached: false, leases: t.leases },
            )
            &&& self.meta(s)->0.first == t.first_wqe
            &&& self.meta(s)->0.seen == Some(t.wc)
        }
    }

    /// The first error completion of a detached request, once.
    pub fn take_detached_error(&mut self) -> (error: Option<DetachedRecord>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_view(old(self)),
            final(self).same_wqes(old(self)),
            final(self).same_open(old(self)),
            error == old(self).spec_detached_error(),
            final(self).spec_detached_error() is None,
    {
        let error = self.detached_error.take();
        proof {
            Self::lemma_frame(old(self), final(self));
        }
        error
    }

    /// The state of request `seq`: `Free` once it was taken, freed or never posted.
    pub fn state(&self, seq: Seq) -> (state: State)
    {
        if seq < self.head || seq >= self.posted {
            return State::Free;
        }
        let slot = &self.slots[self.index(seq)];
        if slot.leases.is_none() {
            return State::Free;
        }
        if seq >= self.committed {
            return State::Staged;
        }
        if matches!(slot.outcome, Err(CompletionError::Refused)) {
            return State::Refused;
        }
        if seq < self.retired {
            State::Retired
        } else {
            State::InFlight
        }
    }

    // -------------------------------------------------------------------------
    // Writing requests

    /// How many more WQEs the queue may take now.
    #[inline]
    pub fn wqe_room(&self) -> (room: u32)
    {
        (self.depth - (self.wqe_posted - self.wqe_retired)) as u32
    }

    /// Whether the open request may take another WQE now.
    pub closed spec fn spec_has_wqe_room(&self) -> bool {
        self.open && self.wqe_posted - self.wqe_retired < self.depth && self.open_wqes
            < self.max_wqes
    }

    /// [`Ring::open_wqes`], as a specification.
    pub closed spec fn spec_open_wqes(&self) -> u32 {
        self.open_wqes as u32
    }

    /// The WQE sequence number the next WQE written takes (its `wr_id`).
    #[inline(always)]
    pub fn next_wqe(&self) -> (id: u64)
    {
        self.wqe_posted
    }

    /// Whether a new request of `wqes` WQEs must be signaled: with them, the unsignaled tail
    /// would exceed `depth - max_wqes`.
    #[inline]
    pub fn must_signal(&self, wqes: u64) -> (must: bool)
    {
        let base = if self.sig_end > self.wqe_retired {
            self.sig_end
        } else {
            self.wqe_retired
        };
        self.wqe_posted + wqes - base > self.depth - self.max_wqes
    }

    /// [`Ring::must_signal`], as a specification.
    pub closed spec fn spec_must_signal(&self) -> bool {
        self.wqe_posted - self.tail_start(self.sig_end) > self.depth - self.max_wqes
    }

    /// Stages a new request of `wqes` WQEs in its slot, with `leases`: the caller wrote its WQEs,
    /// numbered from [`Ring::next_wqe`], and asked for a completion of its last one if `signaled`
    /// ([`Ring::must_signal`] says when it must). `implied` is its outcome if a later completion
    /// retires it. A `detached` request has no owner. The caller checked that a slot is free and
    /// the queue has room ([`Ring::can_post`], [`Ring::wqe_room`]).
    ///
    /// Returns its sequence number.
    #[inline(always)]
    pub fn stage(&mut self, wqes: u64, signaled: bool, detached: bool, implied: CompletionResult, leases: L) -> (seq: Seq)
    {
        self.fill(wqes, detached, implied, leases);
        self.stage_filled(wqes, signaled)
    }

    /// The first half of [`Ring::stage`], for a caller that writes the WQEs after the slot holds
    /// what keeps their memory alive: the next request's slot gets it all. Then
    /// [`Ring::stage_filled`] stages it, or [`Ring::unfill`] takes it back.
    #[inline(always)]
    pub fn fill(&mut self, wqes: u64, detached: bool, implied: CompletionResult, leases: L)
    {
        let first_wqe = self.wqe_posted;
        let i = self.index(self.posted);
        let slot = &mut self.slots[i];
        slot.end_wqe = first_wqe + wqes;
        slot.first_wqe = Ghost(first_wqe);
        slot.outcome = implied;
        slot.detached = detached;
        forget_free(slot.leases.replace(leases));
    }

    /// The second half of [`Ring::stage`]: the request of the slot just filled is staged.
    #[inline(always)]
    pub fn stage_filled(&mut self, wqes: u64, signaled: bool) -> (seq: Seq)
    {
        let seq = self.posted;
        let end_wqe = self.wqe_posted + wqes;
        self.posted = seq + 1;
        self.wqe_posted = end_wqe;
        if signaled {
            self.sig_end = end_wqe;
        }
        seq
    }

    /// Takes back what [`Ring::fill`] put in the next request's slot: the request was not posted.
    #[cold]
    #[inline(never)]
    pub fn unfill(&mut self) -> (leases: Option<L>)
    {
        let i = self.index(self.posted);
        self.slots[i].leases.take()
    }

    proof fn lemma_end(&self, pre: &Self, p: int)
        requires
            pre.inv(),
            pre.open,
            pre.open_wqes >= 1,
            p == pre.posted,
            self.slots@ == pre.slots@.update(p % pre.n(), self.slot(p)),
            self.slot(p).state == State::Staged,
            self.slot(p).seq == p,
            self.slot(p).first_wqe == pre.closed_end(),
            self.slot(p).end_wqe == pre.wqe_posted,
            self.slot(p).outcome is None,
            self.slot(p).leases is Some,
            self.slot(p).waker is None,
            self.head == pre.head,
            self.retired == pre.retired,
            self.committed == pre.committed,
            self.posted == p + 1,
            self.depth == pre.depth,
            self.max_wqes == pre.max_wqes,
            self.wqe_retired == pre.wqe_retired,
            self.wqe_committed == pre.wqe_committed,
            self.wqe_posted == pre.wqe_posted,
            self.sig_end == pre.sig_end || (self.sig_end == pre.wqe_posted),
            self.sig_end == pre.sig_end ==> pre.wqe_posted - pre.tail_start(pre.sig_end)
                <= pre.depth - pre.max_wqes,
            self.committed_sig_end == pre.committed_sig_end,
            !self.open,
            self.open_wqes == 0,
            self.woken == pre.woken,
        ensures
            self.inv(),
            forall|s: int| #[trigger] self.req(s) == pre.req(s),
            forall|s: int|
                #[trigger] self.staged(s) == if s == p {
                    Some(self.view_of(p))
                } else {
                    pre.staged(s)
                },
            forall|s: int| #[trigger] self.refused(s) == pre.refused(s),
            forall|s: int, w: int| #[trigger] self.dma(s, w) ==
                if s == pre.spec_posted() && pre.spec_closed_end() <= w < pre.spec_wqe_posted() {
                    self.slot(p).leases
                } else { pre.dma(s, w) },
            forall|s: int| #[trigger] self.meta(s) == if s == p {
                Some(RequestMeta { first: pre.spec_closed_end() as u64,
                    end: pre.spec_wqe_posted(), implied: self.slot(p).implied, seen: None })
            } else { pre.meta(s) },
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        let n = pre.n();
        assert forall|s: int| pre.head <= s < pre.head + n && s != p implies self.slot(s) == pre.slot(
            s,
        ) by {
            lemma_other(n, p, s);
        }
        assert forall|s: int| #[trigger] self.req(s) == pre.req(s) by {
            if pre.head <= s < pre.committed {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.refused(s) == pre.refused(s) by {
            if pre.head <= s < pre.committed {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int|
            #[trigger] self.staged(s) == if s == p {
                Some(self.view_of(p))
            } else {
                pre.staged(s)
            } by {
            if pre.committed <= s < p {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            if s != p {
                assert(pre.live_ok(s));
            }
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() implies #[trigger] self.free_ok(
            s,
        ) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe
            == self.slot(s - 1).end_wqe by {
            if s != p {
                assert(pre.slot(s).first_wqe == pre.slot(s - 1).end_wqe);
            } else if pre.committed < pre.posted {
            } else {
                assert(pre.live_ok(s - 1));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Committing and aborting

    /// The doorbell rang: every staged request is in flight.
    #[inline(always)]
    pub fn commit(&mut self)
    {
        self.committed = self.posted;
        self.wqe_committed = self.wqe_posted;
        self.committed_sig_end = self.sig_end;
    }

    proof fn lemma_commit(&self, pre: &Self)
        requires
            pre.inv(),
            !pre.open,
            self.slots@.len() == pre.slots@.len(),
            forall|s: int|
                pre.head <= s < pre.head + pre.n() ==> #[trigger] self.slot(s) == if pre.committed
                    <= s < pre.posted {
                    pre.slot(s).with_state(State::InFlight)
                } else {
                    pre.slot(s)
                },
            self.head == pre.head,
            self.retired == pre.retired,
            self.committed == pre.posted,
            self.posted == pre.posted,
            self.depth == pre.depth,
            self.max_wqes == pre.max_wqes,
            self.wqe_retired == pre.wqe_retired,
            self.wqe_committed == pre.wqe_posted,
            self.wqe_posted == pre.wqe_posted,
            self.sig_end == pre.sig_end,
            self.committed_sig_end == pre.sig_end,
            self.open == pre.open,
            self.open_wqes == pre.open_wqes,
        ensures
            self.inv(),
            forall|s: int|
                #[trigger] self.req(s) == if pre.staged(s) is Some {
                    pre.staged(s)
                } else {
                    pre.req(s)
                },
            forall|s: int| #[trigger] self.staged(s) is None,
            forall|s: int| #[trigger] self.refused(s) == pre.refused(s),
            self.same_dma(pre),
            self.same_meta(pre),
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| #[trigger] self.refused(s) == pre.refused(s) by {
            if pre.head <= s < pre.posted {
                if pre.committed <= s {
                    assert(pre.live_ok(s));
                }
            }
        }
        assert forall|s: int|
            #[trigger] self.req(s) == if pre.staged(s) is Some {
                pre.staged(s)
            } else {
                pre.req(s)
            } by {
            if pre.head <= s < pre.posted {
                assert(self.slot(s) == if pre.committed <= s < pre.posted {
                    pre.slot(s).with_state(State::InFlight)
                } else {
                    pre.slot(s)
                });
                if pre.committed <= s {
                    assert(pre.live_ok(s));
                }
            }
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() implies #[trigger] self.free_ok(
            s,
        ) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe == pre.slot(s - 1).end_wqe);
        }
        if self.retired < self.posted {
            assert(pre.slot(self.retired as int) == self.slot(self.retired as int).with_state(
                pre.slot(self.retired as int).state,
            ));
            assert(pre.slot(self.posted - 1).end_wqe == self.slot(self.posted - 1).end_wqe);
        }
        if self.head < self.retired {
            assert(pre.slot(self.head as int) == self.slot(self.head as int));
        }
        assert forall|s: int, w: int| #[trigger] self.dma(s, w) == pre.dma(s, w) by {
            if pre.head <= s < pre.posted {
                assert(self.slot(s) == if pre.committed <= s < pre.posted {
                    pre.slot(s).with_state(State::InFlight)
                } else { pre.slot(s) });
                if pre.committed <= s { assert(pre.live_ok(s)); }
            }
        }

    }

    /// The hardware discarded everything since the last commit (the device
    /// refused the doorbell): drops the open request (if any) and refuses
    /// every staged request. Returns how many staged requests were refused.
    ///
    /// A refused request keeps its sequence number, which is never reused:
    /// one with an owner retires at once with a flush error, which its owner
    /// takes with its leases ([`Ring::take`]); one without an owner releases
    /// its leases (see [`Ring::abort_keeping`]). The WQEs of the refused
    /// requests were never seen by the NIC: they leave the WQE accounting,
    /// and their numbers go to the next requests. Wakers registered on the
    /// staged requests are woken.
    pub fn abort(&mut self) -> (dropped: u64)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            !final(self).is_open(),
            final(self).spec_posted() == old(self).spec_posted(),
            forall|s: int| #[trigger] final(self).req(s) == old(self).req(s),
            forall|s: int| #[trigger] final(self).staged(s) is None,
            forall|s: int|
                #[trigger] final(self).refused(s) == if old(self).staged(s) is Some && !old(
                    self,
                ).staged(s)->0.detached {
                    Some(old(self).staged(s)->0.leases)
                } else {
                    old(self).refused(s)
                },
            final(self).woken() == old(self).woken() + old(self).wakers(
                old(self).spec_committed() as int,
                old(self).spec_posted() as int,
            ),
            final(self).spec_wqe_retired() == old(self).spec_wqe_retired(),
            final(self).spec_wqe_committed() == old(self).spec_wqe_committed(),
            final(self).spec_wqe_posted() == old(self).spec_wqe_committed(),
            final(self).spec_depth() == old(self).spec_depth(),
            final(self).keeps_dma(old(self)),
            forall|s: int| #[trigger] final(self).meta(s) ==
                if old(self).staged(s) is Some { None } else { old(self).meta(s) },
            final(self).detached_refused(old(self)),
    {
        let mut wakes = WakeBatch::new();
        let result = self.abort_with_wakes(&mut wakes);
        wakes.finish();
        result
    }

    pub(crate) fn abort_with_wakes(&mut self, wakes: &mut WakeBatch) -> (dropped: u64)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            !final(self).is_open(),
            final(self).spec_posted() == old(self).spec_posted(),
            forall|s: int| #[trigger] final(self).req(s) == old(self).req(s),
            forall|s: int| #[trigger] final(self).staged(s) is None,
            forall|s: int|
                #[trigger] final(self).refused(s) == if old(self).staged(s) is Some && !old(
                    self,
                ).staged(s)->0.detached {
                    Some(old(self).staged(s)->0.leases)
                } else {
                    old(self).refused(s)
                },
            final(self).woken() == old(self).woken() + old(self).wakers(
                old(self).spec_committed() as int,
                old(self).spec_posted() as int,
            ),
            final(self).spec_wqe_retired() == old(self).spec_wqe_retired(),
            final(self).spec_wqe_committed() == old(self).spec_wqe_committed(),
            final(self).spec_wqe_posted() == old(self).spec_wqe_committed(),
            final(self).spec_depth() == old(self).spec_depth(),
            final(self).keeps_dma(old(self)),
            forall|s: int| #[trigger] final(self).meta(s) ==
                if old(self).staged(s) is Some { None } else { old(self).meta(s) },
            final(self).detached_refused(old(self)),
    {
        let dropped = self.posted - self.committed;
        // No request is staged at `posted`: none is handed back.
        let _kept = self.abort_keeping_with_wakes(self.posted, wakes);
        dropped
    }

    /// Like [`Ring::abort`], but hands back the leases of staged request
    /// `keep` instead of keeping them for its owner (`None` if it is not
    /// staged); its slot is freed.
    ///
    /// A request without an owner learns of nothing but through the
    /// detached error: if one is dropped, and no detached error waits, it
    /// becomes a flush error of that request (it was not executed), so that
    /// the loss is reported, not taken for success.
    pub fn abort_keeping(&mut self, keep: Seq) -> (kept: Option<L>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            !final(self).is_open(),
            final(self).spec_posted() == old(self).spec_posted(),
            forall|s: int| #[trigger] final(self).req(s) == old(self).req(s),
            forall|s: int| #[trigger] final(self).staged(s) is None,
            forall|s: int|
                #[trigger] final(self).refused(s) == if old(self).staged(s) is Some && s != keep
                    && !old(self).staged(s)->0.detached {
                    Some(old(self).staged(s)->0.leases)
                } else {
                    old(self).refused(s)
                },
            final(self).woken() == old(self).woken() + old(self).wakers(
                old(self).spec_committed() as int,
                old(self).spec_posted() as int,
            ),
            kept == match old(self).staged(keep as int) {
                Some(r) => Some(r.leases),
                None => None,
            },
            final(self).spec_wqe_retired() == old(self).spec_wqe_retired(),
            final(self).spec_wqe_committed() == old(self).spec_wqe_committed(),
            final(self).spec_wqe_posted() == old(self).spec_wqe_committed(),
            final(self).spec_depth() == old(self).spec_depth(),
            final(self).spec_detached_error() is None ==> forall|s: int|
                #[trigger] old(self).staged(s) is Some && s != keep ==> !old(self).staged(
                    s,
                )->0.detached,
            final(self).keeps_dma(old(self)),
            forall|s: int| #[trigger] final(self).meta(s) ==
                if old(self).staged(s) is Some { None } else { old(self).meta(s) },
            final(self).detached_refused(old(self)),
    {
        let mut wakes = WakeBatch::new();
        let result = self.abort_keeping_with_wakes(keep, &mut wakes);
        wakes.finish();
        result
    }

    pub(crate) fn abort_keeping_with_wakes(&mut self, keep: Seq, wakes: &mut WakeBatch) -> (kept: Option<L>)
    {
        let wqe = self.wqe_committed;
        let mut kept = None;
        let mut seq = self.committed;
        while seq < self.posted
        {
            let i = self.index(seq);
            let waker = self.take_waker(i);
            let slot = &mut self.slots[i];
            slot.first_wqe = Ghost(wqe);
            slot.end_wqe = wqe;
            if seq == keep {
                kept = slot.leases.take();
            } else if slot.detached {
                // Releases the leases.
                slot.leases = None;
                if self.detached_error.is_none() {
                    self.detached_error = Some(DetachedRecord {
                        seq,
                        error: CompletionError::Refused,
                        first_wqe: Ghost(wqe),
                        end_wqe: Ghost(wqe),
                    });
                }
            } else {
                slot.outcome = Err(CompletionError::Refused);
            }
            if let Some(waker) = waker {
                wakes.wake(waker);
            }
            seq += 1;
        }
        self.committed = self.posted;
        self.wqe_posted = self.wqe_committed;
        self.sig_end = self.committed_sig_end;
        self.skip_refused();
        self.advance_head();
        kept
    }

    proof fn lemma_abort(&self, pre: &Self, keep: int)
        requires
            pre.inv(),
            self.slots@.len() == pre.slots@.len(),
            forall|s: int|
                pre.head <= s < pre.head + pre.n() ==> #[trigger] self.slot(s) == if pre.committed
                    <= s < pre.posted {
                    refuse(pre.slot(s), s == keep, pre.wqe_committed)
                } else {
                    pre.slot(s)
                },
            self.head == pre.head,
            self.retired == pre.retired,
            self.committed == pre.posted,
            self.posted == pre.posted,
            self.depth == pre.depth,
            self.max_wqes == pre.max_wqes,
            self.wqe_retired == pre.wqe_retired,
            self.wqe_committed == pre.wqe_committed,
            self.wqe_posted == pre.wqe_committed,
            self.sig_end == pre.committed_sig_end,
            self.committed_sig_end == pre.committed_sig_end,
            !self.open,
            self.open_wqes == 0,
        ensures
            self.inv_body(),
            forall|s: int| #[trigger] self.req(s) == pre.req(s),
            forall|s: int| #[trigger] self.staged(s) is None,
            forall|s: int|
                #[trigger] self.refused(s) == if pre.staged(s) is Some && s != keep && !pre.staged(
                    s,
                )->0.detached {
                    Some(pre.staged(s)->0.leases)
                } else {
                    pre.refused(s)
                },
            self.keeps_dma(pre),
            forall|s: int| #[trigger] self.meta(s) ==
                if pre.staged(s) is Some { None } else { pre.meta(s) },
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() implies #[trigger] self.free_ok(
            s,
        ) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| #[trigger] self.req(s) == pre.req(s) by {
            if pre.head <= s < pre.posted {
                if pre.committed <= s {
                    assert(pre.live_ok(s));
                }
            }
        }
        assert forall|s: int|
            #[trigger] self.refused(s) == if pre.staged(s) is Some && s != keep && !pre.staged(
                s,
            )->0.detached {
                Some(pre.staged(s)->0.leases)
            } else {
                pre.refused(s)
            } by {
            if pre.head <= s < pre.posted {
                if pre.committed <= s {
                    assert(pre.live_ok(s));
                }
            }
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe
            == self.slot(s - 1).end_wqe by {
            if s < pre.committed {
                assert(pre.slot(s).first_wqe == pre.slot(s - 1).end_wqe);
            } else if s - 1 < pre.committed {
                assert(pre.slot(s - 1).end_wqe == pre.wqe_committed);
            }
        }
        if self.retired < self.posted {
            if pre.retired < pre.committed {
                assert(pre.slot(self.retired as int) == self.slot(self.retired as int));
            }
        }
        if self.retired < self.committed {
            if pre.committed == pre.posted {
                assert(pre.slot(self.committed - 1) == self.slot(self.committed - 1));
            }
        }
        if self.head < self.retired {
            assert(pre.slot(self.head as int) == self.slot(self.head as int));
        }
        assert forall|s: int, w: int| pre.dma(s, w) is Some
            && self.spec_wqe_retired() <= w < self.spec_closed_end()
            implies #[trigger] self.dma(s, w) == pre.dma(s, w) by {
            if s < pre.retired { assert(pre.done_ok(s)); }
            assert(pre.live_ok(s));
            if s >= pre.committed { assert(pre.posted_ok(s)); }
            assert(s < pre.committed);
            assert(self.slot(s) == pre.slot(s));
        }

    }

    /// Moves `retired` past the requests at the front of the committed ones that have no WQEs:
    /// no completion would retire them.
    fn skip_refused(&mut self)
    {
        while self.retired < self.committed && self.slots[self.index(self.retired)].end_wqe == self.wqe_retired
        {
            self.retired += 1;
        }
    }

    /// Moving `retired` changes nothing A sees.
    proof fn lemma_retired_moved(&self, pre: &Self)
        requires
            self.slots@ == pre.slots@,
            self.head == pre.head,
            self.committed == pre.committed,
            self.posted == pre.posted,
            self.woken == pre.woken,
        ensures
            self.same_view(pre),
    {
        assert forall|s: int| #[trigger] self.req(s) == pre.req(s) by {}
        assert forall|s: int| #[trigger] self.staged(s) == pre.staged(s) by {}
        assert forall|s: int| #[trigger] self.refused(s) == pre.refused(s) by {}
    }

    /// Moving `retired` past a refused request keeps the invariants.
    proof fn lemma_skip(&self, pre: &Self)
        requires
            pre.inv_body(),
            pre.retired < pre.committed,
            pre.slot(pre.retired as int).state != State::InFlight,
            self.slots@ == pre.slots@,
            self.head == pre.head,
            self.retired == pre.retired + 1,
            self.committed == pre.committed,
            self.posted == pre.posted,
            self.depth == pre.depth,
            self.max_wqes == pre.max_wqes,
            self.wqe_retired == pre.wqe_retired,
            self.wqe_committed == pre.wqe_committed,
            self.wqe_posted == pre.wqe_posted,
            self.sig_end == pre.sig_end,
            self.committed_sig_end == pre.committed_sig_end,
            self.open == pre.open,
            self.open_wqes == pre.open_wqes,
        ensures
            self.inv_body(),
            pre.slot(pre.retired as int).waker is None,
            pre.req(pre.retired as int) is None,
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        let r = pre.retired as int;
        assert(pre.live_ok(r));
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            if s != r {
                assert(pre.done_ok(s));
            }
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() implies #[trigger] self.free_ok(
            s,
        ) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe == pre.slot(s - 1).end_wqe);
        }
        if self.retired < self.posted {
            assert(pre.slot(r + 1).first_wqe == pre.slot(r).end_wqe);
        }
    }

    // -------------------------------------------------------------------------
    // Completions

    /// One past the last committed WQE.
    pub closed spec fn spec_wqe_committed(&self) -> u64 {
        self.wqe_committed
    }

    /// The outcome request `s` retires with when WQE `wqe` completes with
    /// `wc`: an error recorded on it before, else `wc` if it ends at `wqe`,
    /// else the outcome implied at posting.
    pub closed spec fn retire_outcome(&self, s: int, wqe: u64, wc: Wc) -> Wc {
        let slot = self.slot(s);
        match slot.outcome {
            Some(recorded) => recorded,
            None => if slot.end_wqe == wqe + 1 {
                wc
            } else {
                slot.implied
            },
        }
    }

    /// What A sees of request `s` once it retired with `outcome` (A's
    /// `complete`): a detached request is gone (its leases released), an
    /// owned one is complete; a refused one A never saw.
    pub closed spec fn retired_view(&self, s: int, outcome: Wc) -> Option<ReqView<L>> {
        if self.slot(s).state != State::InFlight || self.slot(s).detached {
            None
        } else {
            Some(self.view_of(s).completed(outcome))
        }
    }

    /// The names of the wakers held by requests `[from, to)`, in order.
    pub closed spec fn wakers(&self, from: int, to: int) -> vstd::seq::Seq<WakerId>
        decreases to - from,
    {
        if to <= from {
            vstd::seq::Seq::empty()
        } else {
            let before = self.wakers(from, to - 1);
            match self.slot(to - 1).waker {
                Some(w) => before.push(waker_id(w)),
                None => before,
            }
        }
    }

    /// The metadata change of a completion: full requests retire, while the
    /// next request may cache its first error from a proper WQE prefix.
    pub closed spec fn completion_meta(&self, s: int, wqe: u64, wc: Wc, to: int) -> Option<RequestMeta> {
        if self.retired <= s < to {
            self.completed_meta(s, self.retire_outcome(s, wqe, wc))
        } else if s == to && self.req(s) is Some && self.req(s)->0.phase is InFlight
            && self.meta(s)->0.first <= wqe && wqe as int + 1 < self.meta(s)->0.end
            && !wc.spec_ok() && self.meta(s)->0.seen is None {
            self.recorded_meta(s, wc)
        } else { self.meta(s) }
    }

    /// Public observations of the native cache and a completion's metadata frame.
    pub proof fn lemma_completion_observation(&self, s: int, wqe: u64, wc: Wc, to: int)
        requires self.inv(), self.spec_retired() <= to <= self.spec_committed(),
        ensures
            self.completion_meta(s, wqe, wc, to) is Some ==> self.meta(s) is Some
                && self.completion_meta(s, wqe, wc, to)->0.first == self.meta(s)->0.first
                && self.completion_meta(s, wqe, wc, to)->0.end == self.meta(s)->0.end
                && self.completion_meta(s, wqe, wc, to)->0.implied == self.meta(s)->0.implied,
            s < self.spec_retired() ==> self.completion_meta(s, wqe, wc, to) == self.meta(s),
            self.req(s) is Some && to <= s
                && (wc.spec_ok() || self.meta(s)->0.seen is Some
                    || wqe < self.meta(s)->0.first || self.meta(s)->0.end as int <= wqe as int + 1)
                ==> self.completion_meta(s, wqe, wc, to) == self.meta(s),
            self.req(s) is Some ==> self.retire_outcome(s, wqe, wc)
                == match self.meta(s)->0.seen {
                    Some(prior) => prior,
                    None => if self.meta(s)->0.end as int == wqe as int + 1 { wc }
                        else { self.meta(s)->0.implied },
                },
            self.spec_retired() <= s < to && self.req(s) is Some && !self.req(s)->0.detached
                ==> self.completion_meta(s, wqe, wc, to)->0.seen == Some(self.retire_outcome(s, wqe, wc)),
    {
        self.lemma_meta(s);
        self.lemma_view();
        reveal(Ring::inv_slots);
        if self.retired <= s < to { assert(self.live_ok(s)); }
    }

    proof fn lemma_ignored_meta(&self, wqe: u64, wc: Wc)
        requires self.inv(), wqe >= self.spec_wqe_committed() || wqe < self.spec_wqe_retired(),
        ensures forall|s: int| #[trigger] self.meta(s)
            == self.completion_meta(s, wqe, wc, self.spec_retired() as int),
    {
        self.lemma_view();
        assert forall|s: int| #[trigger] self.meta(s)
            == self.completion_meta(s, wqe, wc, self.spec_retired() as int) by {
            self.lemma_meta(s);
        }
    }

    /// After WQE `wqe` completed with `wc`, each request still in flight caches `wc` if it
    /// is an error inside its interval and none was cached, and keeps its cache otherwise.
    #[verifier::opaque]
    pub open spec fn pending_cache(&self, pre: Self, wqe: u64, wc: Wc) -> bool {
        forall|s: int| #[trigger] self.req(s) is Some && self.req(s)->0.phase is InFlight
            ==> self.meta(s)->0.seen == if pre.meta(s)->0.seen is None
                && !wc.spec_ok() && pre.meta(s)->0.first <= wqe {
                Some(wc)
            } else { pre.meta(s)->0.seen }
    }

    /// [`Ring::pending_cache`] for request `s`.
    pub proof fn lemma_pending_cache_observation(&self, pre: Self, wqe: u64, wc: Wc, s: int)
        requires self.pending_cache(pre, wqe, wc),
            self.req(s) is Some, self.req(s)->0.phase is InFlight,
        ensures self.meta(s)->0.seen == if pre.meta(s)->0.seen is None
            && !wc.spec_ok() && pre.meta(s)->0.first <= wqe {
            Some(wc)
        } else { pre.meta(s)->0.seen },
    { reveal(Ring::pending_cache); }

    /// Records the completion of WQE `wqe` of a send queue: retires every committed request whose
    /// WQEs all lie at or before it. Wakes the wakers of the requests that retire.
    pub fn complete(&mut self, wqe: u64, wc: Wc) -> (known: bool)
    {
        let mut wakes = WakeBatch::new();
        let result = self.complete_with_wakes(false, wqe, &wc, &mut wakes);
        wakes.finish();
        result
    }

    /// Records the completion of WQE `wqe` (its `wr_id`), keeping what it retires in the slots:
    /// retires every committed request whose WQEs all lie at or before it.
    ///
    /// The request ending at `wqe` retires with `wc` if that is an error (or with an error
    /// recorded on it before) and otherwise with its success: `wc`'s on a receive queue (`recv`),
    /// the one given at posting on a send queue. Those before it retire with their own. An error
    /// for a WQE inside a request is recorded on it. A completion for an already retired WQE is
    /// ignored. Returns `false`, and changes nothing, for a WQE that was never committed.
    #[inline(always)]
    pub(crate) fn complete_with_wakes(&mut self, recv: bool, wqe: u64, wc: &Wc, wakes: &mut WakeBatch) -> (known: bool)
    {
        if wqe >= self.wqe_committed {
            return false;
        }
        if wqe < self.wqe_retired {
            return true;
        }
        #[allow(clippy::while_let_loop)] // Verus has no `while let`.
        loop
        {
            match self.retire_next(recv, wqe, wc, wakes) {
                Some((_, false)) => {},
                _ => break,
            }
        }
        // Only a request without an owner, or nothing, leaves its slot free as it retires.
        if self.head < self.retired && self.slots[self.index(self.head)].leases.is_none() {
            self.advance_head();
        }
        true
    }

    /// Whether WQE `wqe` is in flight: one comparison, since a WQE before the oldest in flight wraps
    /// far ahead.
    #[inline(always)]
    pub(crate) fn wqe_in_flight(&self, wqe: u64) -> bool {
        wqe.wrapping_sub(self.wqe_retired) < self.wqe_committed - self.wqe_retired
    }

    /// Moves the head past request `seq`, just retired, if it is at it; `false` if that is not all
    /// there is to it, and [`Ring::advance_head`] has to look once the request's slot is free.
    #[inline(always)]
    pub(crate) fn pass_retired(&mut self, seq: Seq) -> (passed: bool) {
        if self.head == seq && self.retired == seq + 1 {
            self.head = seq + 1;
            true
        } else {
            false
        }
    }

    /// Recording an error on the oldest request in flight keeps the
    /// invariants.
    proof fn lemma_record(&self, pre: &Self)
        requires
            pre.inv_body(),
            pre.retired < pre.committed,
            pre.slot(pre.retired as int).state == State::InFlight,
            self.same_counters(pre),
            self.slots@ == pre.slots@.update(
                (pre.retired as int) % pre.n(),
                Slot { outcome: self.slot(pre.retired as int).outcome, ..pre.slot(pre.retired as int) },
            ),
            self.slot(pre.retired as int).outcome is Some,
        ensures
            self.inv_body(),
            forall|s: int| #[trigger] self.req(s) == pre.req(s),
            forall|s: int| #[trigger] self.staged(s) == pre.staged(s),
            forall|s: int| #[trigger] self.refused(s) == pre.refused(s),
            self.same_dma(pre),
            forall|s: int| #[trigger] self.meta(s) == if s == pre.retired {
                pre.recorded_meta(s, self.slot(s).outcome->0)
            } else { pre.meta(s) },
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        let r = pre.retired as int;
        let n = pre.n();
        assert forall|s: int| pre.head <= s < pre.head + n && s != r implies self.slot(s) == pre.slot(
            s,
        ) by {
            lemma_other(n, r, s);
        }
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() implies #[trigger] self.free_ok(
            s,
        ) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe == pre.slot(s - 1).end_wqe);
        }
        assert(pre.live_ok(r));
        assert forall|s: int| #[trigger] self.req(s) == pre.req(s) by {
            if pre.head <= s < pre.committed && s != r {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.staged(s) == pre.staged(s) by {
            if pre.committed <= s < pre.posted {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.refused(s) == pre.refused(s) by {
            if pre.head <= s < pre.committed && s != r {
                assert(self.slot(s) == pre.slot(s));
            }
        }
    }

    /// The step of a completion's walk: on the oldest request not retired, whose WQEs start at
    /// or before WQE `wqe`, a WQE in flight. Retires it, if it ends at or before `wqe`; returns its
    /// slot and whether it ends exactly there, so that the completion is spent. Otherwise WQE `wqe`
    /// lies inside it: an error is recorded on it, if it is its first, and this returns `None`.
    #[inline(always)]
    pub(crate) fn retire_next(&mut self, recv: bool, wqe: u64, wc: &Wc, wakes: &mut WakeBatch) -> (r: Option<(usize, bool)>)
    {
        let seq = self.retired;
        let i = self.index(seq);
        let first = self.wqe_retired;
        let slot = &mut self.slots[i];
        let end = slot.end_wqe;
        let last = wqe + 1;
        if end > last {
            if !wc.is_ok() && slot.outcome.is_ok() {
                slot.outcome = Err(wc.failure((wqe - first) as u32));
            }
            return None;
        }
        let ended = end == last;
        if ended {
            if wc.is_ok() {
                if recv && slot.outcome.is_ok() {
                    slot.outcome = Ok(Completed::Received(wc.received()));
                }
            } else if slot.outcome.is_ok() {
                slot.outcome = Err(wc.failure((wqe - first) as u32));
            }
        }
        self.retired = seq + 1;
        self.wqe_retired = end;
        if slot.detached || slot.outcome.is_err() {
            Self::retired_cold(slot, seq, first, &mut self.failed, &mut self.detached_error);
        }
        if !self.wakers.is_empty() {
            let waker = self.wakers[i].take();
            if let Some(waker) = waker {
                wakes.wake(waker);
            }
        }
        // Refused requests after the last WQE in flight: no completion would retire them.
        if end == self.wqe_committed && self.retired != self.committed {
            self.skip_refused();
        }
        Some((i, ended))
    }

    /// The slot of a request [`Ring::retire_next`] just retired, for what it holds.
    #[inline(always)]
    pub(crate) fn retired_slot(&mut self, i: usize) -> &mut Slot<L> {
        &mut self.slots[i]
    }

    /// What retiring request `seq`, in `slot`, with first WQE `first`, takes beyond the common
    /// case: it failed, or it has no owner.
    #[cold]
    #[inline(never)]
    #[allow(unused_variables)] // `first` and `end` are ghost.
    fn retired_cold(slot: &mut Slot<L>, seq: Seq, first: u64, failed: &mut bool, detached_error: &mut Option<DetachedRecord>)
    {
        let end = slot.end_wqe;
        if let Err(error) = slot.outcome {
            if matches!(error, CompletionError::Failed { .. }) {
                *failed = true;
            }
            if slot.detached && detached_error.is_none() && slot.leases.is_some() {
                *detached_error = Some(DetachedRecord {
                    seq,
                    error,
                    first_wqe: Ghost(first),
                    end_wqe: Ghost(end),
                });
            }
        }
        if slot.detached {
            // Releases the leases.
            slot.leases = None;
        }
    }

    /// Takes the waker of slot `i`, if any.
    #[inline]
    fn take_waker(&mut self, i: usize) -> (waker: Option<Waker>)
    {
        if self.wakers.is_empty() {
            None
        } else {
            self.wakers[i].take()
        }
    }

    proof fn lemma_retire(&self, pre: &Self, outcome: Wc)
        requires
            pre.inv_body(),
            pre.retired < pre.committed,
            pre.slot(pre.retired as int).state == State::InFlight,
            self.slots@.len() == pre.slots@.len(),
            ({
                let r = pre.retired as int;
                let old_slot = pre.slot(r);
                let new_slot = self.slot(r);
                &&& self.slots@ == pre.slots@.update(r % pre.n(), new_slot)
                &&& new_slot.waker is None
                &&& if old_slot.detached {
                    new_slot.is_empty()
                } else {
                    &&& new_slot.state == State::Retired
                    &&& new_slot.outcome == Some(outcome)
                    &&& new_slot.leases == old_slot.leases
                    &&& new_slot.seq == old_slot.seq
                    &&& new_slot.detached == old_slot.detached
                }
            }),
            self.head == pre.head,
            self.retired == pre.retired + 1,
            self.committed == pre.committed,
            self.posted == pre.posted,
            self.depth == pre.depth,
            self.max_wqes == pre.max_wqes,
            self.wqe_retired == pre.slot(pre.retired as int).end_wqe,
            self.wqe_committed == pre.wqe_committed,
            self.wqe_posted == pre.wqe_posted,
            self.sig_end == pre.sig_end,
            self.committed_sig_end == pre.committed_sig_end,
            self.open == pre.open,
            self.open_wqes == pre.open_wqes,
            self.slot(pre.retired as int).first_wqe == pre.slot(pre.retired as int).first_wqe,
            self.slot(pre.retired as int).end_wqe == pre.slot(pre.retired as int).end_wqe,
            self.slot(pre.retired as int).implied == pre.slot(pre.retired as int).implied,
        ensures
            self.inv_body(),
            forall|s: int|
                #[trigger] self.req(s) == if s == pre.retired {
                    pre.retired_view(s, outcome)
                } else {
                    pre.req(s)
                },
            forall|s: int| #[trigger] self.staged(s) == pre.staged(s),
            forall|s: int| #[trigger] self.refused(s) == pre.refused(s),
            forall|s: int|
                pre.head <= s < pre.head + pre.n() && s != pre.retired ==> #[trigger] self.slot(s)
                    == pre.slot(s),
            self.keeps_dma(pre),
            forall|s: int| #[trigger] self.meta(s) == if s == pre.retired {
                pre.completed_meta(s, outcome)
            } else { pre.meta(s) },
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        let r = pre.retired as int;
        let n = pre.n();
        assert(pre.live_ok(r));
        assert forall|s: int| pre.head <= s < pre.head + n && s != r implies self.slot(s) == pre.slot(
            s,
        ) by {
            lemma_other(n, r, s);
        }
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            if s != r {
                assert(pre.done_ok(s));
            }
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
            pre.lemma_ordered(r, s);
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() implies #[trigger] self.free_ok(
            s,
        ) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe == pre.slot(s - 1).end_wqe);
        }
        if self.retired < self.posted {
            assert(pre.slot(r + 1).first_wqe == pre.slot(r).end_wqe);
        }
        if self.retired < self.committed {
            assert(pre.slot(self.committed - 1) == self.slot(self.committed - 1));
        }
        if self.committed < self.posted {
            assert(pre.slot(self.posted - 1) == self.slot(self.posted - 1));
        }
        assert forall|s: int|
            #[trigger] self.req(s) == if s == pre.retired {
                pre.retired_view(s, outcome)
            } else {
                pre.req(s)
            } by {
            if pre.head <= s < pre.committed && s != r {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.staged(s) == pre.staged(s) by {
            if pre.committed <= s < pre.posted {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.refused(s) == pre.refused(s) by {
            if pre.head <= s < pre.committed && s != r {
                assert(self.slot(s) == pre.slot(s));
            }
        }
    }

    /// Moves `head` past the free slots at the front of the window.
    #[inline(never)]
    pub(crate) fn advance_head(&mut self)
    {
        while self.head < self.retired && self.slots[self.index(self.head)].leases.is_none()
        {
            self.head += 1;
        }
    }

    /// Moving `head` past a free slot changes nothing A sees.
    proof fn lemma_head_moved(&self, pre: &Self)
        requires
            pre.head < pre.retired,
            pre.slot(pre.head as int).state == State::Free,
            self.slots@ == pre.slots@,
            self.head == pre.head + 1,
            self.retired == pre.retired,
            self.committed == pre.committed,
            self.posted == pre.posted,
            self.woken == pre.woken,
            pre.retired <= pre.committed,
        ensures
            self.same_view(pre),
    {
        let h = pre.head as int;
        assert forall|s: int| #[trigger] self.req(s) == pre.req(s) by {}
        assert forall|s: int| #[trigger] self.staged(s) == pre.staged(s) by {}
        assert forall|s: int| #[trigger] self.refused(s) == pre.refused(s) by {}
        assert forall|s: int, w: int| #[trigger] self.dma(s, w) == pre.dma(s, w) by {}
        assert forall|s: int| #[trigger] self.meta(s) == pre.meta(s) by {
            if s == h {
                assert(pre.staged(s) is None);
            }
        }
    }

    proof fn lemma_head(&self, pre: &Self)
        requires
            pre.inv_body(),
            pre.head < pre.retired,
            pre.slot(pre.head as int).state == State::Free,
            self.slots@ == pre.slots@,
            self.head == pre.head + 1,
            self.retired == pre.retired,
            self.committed == pre.committed,
            self.posted == pre.posted,
            self.depth == pre.depth,
            self.max_wqes == pre.max_wqes,
            self.wqe_retired == pre.wqe_retired,
            self.wqe_committed == pre.wqe_committed,
            self.wqe_posted == pre.wqe_posted,
            self.sig_end == pre.sig_end,
            self.committed_sig_end == pre.committed_sig_end,
            self.open == pre.open,
            self.open_wqes == pre.open_wqes,
        ensures
            self.inv_body(),
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        let n = pre.n();
        let h = pre.head as int;
        assert(pre.done_ok(h));
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() implies #[trigger] self.free_ok(
            s,
        ) by {
            if s < h + n {
                assert(pre.free_ok(s));
            } else {
                assert((h + n) % n == h % n) by {
                    vstd::arithmetic::div_mod::lemma_mod_add_multiples_vanish(h, n);
                }
            }
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe == pre.slot(s - 1).end_wqe);
        }
    }

    // -------------------------------------------------------------------------
    // Owners

    /// Takes the outcome and leases of retired or refused request `seq`, freeing its slot; `None`
    /// unless it is retired or refused and not taken yet.
    #[inline(always)]
    pub fn take(&mut self, seq: Seq) -> (taken: Option<Taken<L>>)
    {
        if seq < self.head || seq >= self.committed {
            return None;
        }
        let i = self.index(seq);
        let slot = &mut self.slots[i];
        if seq >= self.retired && !matches!(slot.outcome, Err(CompletionError::Refused)) {
            return None;
        }
        // The oldest request, as usual: the head moves past it, with nothing to look for.
        if self.head == seq && self.retired == seq + 1 {
            self.head = seq + 1;
            let leases = slot.leases.take();
            let outcome = slot.outcome;
            return match leases {
                Some(leases) => Some(Taken { outcome, leases }),
                None => None,
            };
        }
        self.take_other(i)
    }

    /// [`Ring::take`] of a request after the oldest, or that leaves free ones behind it.
    #[inline(never)]
    fn take_other(&mut self, i: usize) -> (taken: Option<Taken<L>>)
    {
        let slot = &mut self.slots[i];
        let leases = slot.leases.take();
        let outcome = slot.outcome;
        self.advance_head();
        match leases {
            Some(leases) => Some(Taken { outcome, leases }),
            None => None,
        }
    }

    /// What [`Ring::state`] reports, in terms of what A sees.
    proof fn lemma_state_views(&self, seq: int)
        requires
            self.inv(),
        ensures
            (self.state_of(seq) == State::Retired) == (self.req(seq) is Some && self.req(
                seq,
            )->0.phase is Complete),
            (self.state_of(seq) == State::InFlight) == (self.req(seq) is Some && self.req(
                seq,
            )->0.phase is InFlight),
            (self.state_of(seq) == State::Staged) == (self.staged(seq) is Some),
            (self.state_of(seq) == State::Refused) == (self.refused(seq) is Some),
            self.req(seq) is Some ==> self.staged(seq) is None && self.refused(seq) is None,
            self.refused(seq) is Some ==> self.staged(seq) is None,
    {
        reveal(Ring::inv_slots);
        if self.head <= seq < self.retired {
            assert(self.done_ok(seq));
        } else if self.retired <= seq < self.posted {
            assert(self.live_ok(seq));
        }
    }

    /// A request reported `Retired` or `Refused` holds its result: retired,
    /// or refused (before `retired`, or after it without WQEs).
    proof fn lemma_result_slot(&self, seq: int)
        requires
            self.inv(),
            self.state_of(seq) == State::Retired || self.state_of(seq) == State::Refused,
        ensures
            self.head <= seq < self.committed,
            seq >= self.retired ==> self.slot(seq).state == State::Refused,
            self.slot(seq).state == self.state_of(seq),
            self.slot(seq).outcome is Some,
            self.slot(seq).leases is Some,
            self.slot(seq).waker is None,
            !self.slot(seq).detached,
            self.slot(seq).state == State::Refused ==> self.slot(seq).outcome->0.status
                == crate::wc::Status::WR_FLUSH_ERR,
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        if seq >= self.retired {
            assert(self.live_ok(seq));
        } else {
            assert(self.done_ok(seq));
        }
    }

    /// Emptying the slot of a retired or refused request keeps the
    /// invariants but the head's.
    proof fn lemma_free(&self, pre: &Self, seq: int)
        requires
            pre.inv(),
            pre.head <= seq < pre.committed,
            seq >= pre.retired ==> pre.slot(seq).state == State::Refused,
            pre.slot(seq).state == State::Retired || pre.slot(seq).state == State::Refused,
            self.same_counters(pre),
            self.slots@ == pre.slots@.update(seq % pre.n(), self.slot(seq)),
            self.slot(seq).is_empty(),
            self.slot(seq).first_wqe == pre.slot(seq).first_wqe,
            self.slot(seq).end_wqe == pre.slot(seq).end_wqe,
        ensures
            self.inv_body(),
            forall|s: int|
                #[trigger] self.req(s) == if s == seq {
                    None
                } else {
                    pre.req(s)
                },
            forall|s: int| #[trigger] self.staged(s) == pre.staged(s),
            forall|s: int|
                #[trigger] self.refused(s) == if s == seq {
                    None
                } else {
                    pre.refused(s)
                },
            self.same_dma(pre),
            forall|s: int| #[trigger] self.meta(s) == if s == seq { None } else { pre.meta(s) },
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        let n = pre.n();
        assert forall|s: int| pre.head <= s < pre.head + n && s != seq implies self.slot(s)
            == pre.slot(s) by {
            lemma_other(n, seq, s);
        }
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() implies #[trigger] self.free_ok(
            s,
        ) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe == pre.slot(s - 1).end_wqe);
        }
        if self.retired < self.posted {
            assert(pre.slot(self.retired as int).first_wqe == self.slot(
                self.retired as int,
            ).first_wqe);
        }
        if self.retired < self.committed {
            assert(pre.slot(self.committed - 1).end_wqe == self.slot(self.committed - 1).end_wqe);
        }
        if self.committed < self.posted {
            assert(pre.slot(self.posted - 1) == self.slot(self.posted - 1));
        }
        if self.head < self.retired && self.head != seq {
            assert(pre.slot(self.head as int) == self.slot(self.head as int));
        }
        assert forall|s: int|
            #[trigger] self.req(s) == if s == seq {
                None
            } else {
                pre.req(s)
            } by {
            if pre.head <= s < pre.committed && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.staged(s) == pre.staged(s) by {
            if pre.committed <= s < pre.posted {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int|
            #[trigger] self.refused(s) == if s == seq {
                None
            } else {
                pre.refused(s)
            } by {
            if pre.head <= s < pre.committed && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
    }

    /// Gives up ownership of request `seq`: a retired or refused request is
    /// freed and its leases released now, a staged or in-flight one when it
    /// retires.
    pub fn detach(&mut self, seq: Seq)
    {
        if seq < self.head || seq >= self.posted {
            return;
        }
        let i = self.index(seq);
        let waker = self.take_waker(i);
        let retired = seq < self.retired;
        let slot = &mut self.slots[i];
        if retired || (seq < self.committed && matches!(slot.outcome, Err(CompletionError::Refused))) {
            // Releases the leases.
            slot.leases = None;
            self.advance_head();
        } else {
            slot.detached = true;
        }
        // The future it would wake is gone.
        drop(waker);
    }

    /// A request reported `Staged` or `InFlight` is not retired, and its
    /// slot is in that state.
    proof fn lemma_live_slot(&self, seq: int)
        requires
            self.inv(),
            self.state_of(seq) == State::Staged || self.state_of(seq) == State::InFlight,
        ensures
            self.retired <= seq < self.posted,
            self.slot(seq).state == self.state_of(seq),
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        if seq < self.retired {
            assert(self.done_ok(seq));
        }
    }

    /// Changing the owner or waker of a request in flight or staged keeps
    /// the invariants, as long as a detached request has no waker.
    proof fn lemma_live_update(&self, pre: &Self, seq: int)
        requires
            pre.inv(),
            pre.retired <= seq < pre.posted,
            pre.slot(seq).state == State::Staged || pre.slot(seq).state == State::InFlight,
            self.same_counters(pre),
            ({
                let old_slot = pre.slot(seq);
                let new_slot = self.slot(seq);
                &&& self.slots@ == pre.slots@.update(seq % pre.n(), new_slot)
                &&& new_slot == Slot {
                    detached: new_slot.detached,
                    waker: new_slot.waker,
                    ..old_slot
                }
                &&& new_slot.detached ==> new_slot.waker is None
            }),
        ensures
            self.inv(),
            forall|s: int|
                #[trigger] self.req(s) == if s == seq && pre.req(s) is Some {
                    Some(self.view_of(s))
                } else {
                    pre.req(s)
                },
            forall|s: int|
                #[trigger] self.staged(s) == if s == seq && pre.staged(s) is Some {
                    Some(self.view_of(s))
                } else {
                    pre.staged(s)
                },
            forall|s: int| #[trigger] self.refused(s) == pre.refused(s),
            self.same_dma(pre),
            self.same_meta(pre),
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        let n = pre.n();
        assert forall|s: int| pre.head <= s < pre.head + n && s != seq implies self.slot(s)
            == pre.slot(s) by {
            lemma_other(n, seq, s);
        }
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() implies #[trigger] self.free_ok(
            s,
        ) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe == pre.slot(s - 1).end_wqe);
        }
        if self.retired < self.posted {
            assert(pre.slot(self.retired as int).first_wqe == self.slot(
                self.retired as int,
            ).first_wqe);
        }
        if self.retired < self.committed {
            assert(pre.slot(self.committed - 1).end_wqe == self.slot(self.committed - 1).end_wqe);
        }
        if self.committed < self.posted {
            assert(pre.slot(self.posted - 1).end_wqe == self.slot(self.posted - 1).end_wqe);
        }
        if self.head < self.retired {
            assert(pre.slot(self.head as int) == self.slot(self.head as int));
        }
        assert forall|s: int|
            #[trigger] self.req(s) == if s == seq && pre.req(s) is Some {
                Some(self.view_of(s))
            } else {
                pre.req(s)
            } by {
            if pre.head <= s < pre.committed && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int|
            #[trigger] self.staged(s) == if s == seq && pre.staged(s) is Some {
                Some(self.view_of(s))
            } else {
                pre.staged(s)
            } by {
            if pre.committed <= s < pre.posted && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.refused(s) == pre.refused(s) by {
            if pre.head <= s < pre.committed && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
    }

    /// Registers `waker` to be woken when request `seq` retires (a staged or
    /// in-flight request with an owner); replaces the one before unless it
    /// would wake the same task.
    pub fn set_waker(&mut self, seq: Seq, waker: &Waker)
    {
        let state = self.state(seq);
        if !matches!(state, State::Staged) && !matches!(state, State::InFlight) {
            return;
        }
        let i = self.index(seq);
        if self.slots[i].detached {
            return;
        }
        if self.wakers.is_empty() {
            let mut wakers = Vec::with_capacity(self.slots.len());
            let mut k = 0;
            while k < self.slots.len()
            {
                wakers.push(None);
                k += 1;
            }
            self.wakers = wakers;
        }
        let same = match &self.wakers[i] {
            Some(old) => will_wake(old, waker),
            None => false,
        };
        if !same {
            self.wakers[i] = Some(clone_waker(waker));
        }
    }
    // -------------------------------------------------------------------------
    // Destruction

    /// The QP is destroyed: the NIC will not touch any memory again, so
    /// every slot releases its leases and the ring is empty. Wakers are
    /// dropped (nobody can wait on a destroyed QP).
    pub fn release_all(&mut self)
    {
        let mut i = 0;
        while i < self.slots.len()
        {
            // Releases the leases.
            self.slots[i].leases = None;
            let _waker = self.take_waker(i);
            i += 1;
        }
        self.head = self.posted;
        self.retired = self.posted;
        self.committed = self.posted;
        self.wqe_retired = self.wqe_posted;
        self.wqe_committed = self.wqe_posted;
        self.sig_end = self.wqe_posted;
        self.committed_sig_end = self.wqe_posted;
    }
}

} // verus!

// The checks of the invariants at run time, outside the proofs: tests use them to
// cross-check the proved invariants on random sequences.
impl<L> Ring<L> {
    /// Checks the invariants of the module documentation at run time. Tests call it on random
    /// executions.
    pub fn invariant(&self) -> bool {
        let n = self.slots.len() as u64;
        if !n.is_power_of_two() || self.mask != n - 1 {
            return false;
        }
        if !self.wakers.is_empty() && self.wakers.len() != self.slots.len() {
            return false;
        }
        // 1. The windows.
        if !(self.head <= self.retired && self.retired <= self.committed) {
            return false;
        }
        if !(self.committed <= self.posted && self.posted - self.head <= n) {
            return false;
        }
        // 2. WQE accounting.
        if !(self.wqe_retired <= self.wqe_committed && self.wqe_committed <= self.wqe_posted) {
            return false;
        }
        if self.wqe_posted - self.wqe_retired > self.depth {
            return false;
        }
        // The head is advanced past free slots.
        if self.head < self.retired && self.slots[self.index(self.head)].leases.is_none() {
            return false;
        }
        // Slots in the window.
        let mut next_wqe = self.wqe_retired;
        let mut seq = self.head;
        while seq < self.posted {
            let i = self.index(seq);
            let slot = &self.slots[i];
            let refused = matches!(slot.outcome, Err(CompletionError::Refused));
            if seq < self.retired {
                // A result for its owner, or nothing; no waker, no detachment.
                if slot.leases.is_some() && slot.detached {
                    return false;
                }
                if self.has_waker(i) {
                    return false;
                }
            } else if seq < self.committed && slot.end_wqe == next_wqe {
                // Refused, or freed since, without WQEs: a result for its owner or nothing.
                if slot.leases.is_some() && !refused {
                    return false;
                }
                if self.has_waker(i) {
                    return false;
                }
            } else {
                if slot.leases.is_none() {
                    return false;
                }
                if slot.detached && self.has_waker(i) {
                    return false;
                }
                if seq >= self.committed && slot.outcome.is_err() {
                    return false;
                }
                // 3. Consecutive WQE ranges.
                if slot.end_wqe <= next_wqe || slot.end_wqe - next_wqe > self.max_wqes {
                    return false;
                }
                next_wqe = slot.end_wqe;
            }
            if seq + 1 == self.committed && next_wqe != self.wqe_committed {
                return false;
            }
            seq += 1;
        }
        if self.committed == self.retired && self.wqe_committed != self.wqe_retired {
            return false;
        }
        if next_wqe != self.wqe_posted {
            return false;
        }
        // Slots outside the window hold nothing.
        let mut seq = self.posted;
        while seq < self.head + n {
            let i = self.index(seq);
            if self.slots[i].leases.is_some() || self.has_waker(i) {
                return false;
            }
            seq += 1;
        }
        // 8. The unsignaled tail.
        if self.sig_end > self.wqe_posted || self.committed_sig_end > self.wqe_committed {
            return false;
        }
        let base = if self.sig_end > self.wqe_retired {
            self.sig_end
        } else {
            self.wqe_retired
        };
        if base > self.wqe_posted || self.wqe_posted - base > self.depth - self.max_wqes {
            return false;
        }
        true
    }

    /// Whether slot `i` holds a waker.
    fn has_waker(&self, i: usize) -> bool {
        !self.wakers.is_empty() && self.wakers[i].is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wc::{Opcode, Status};
    use std::cell::Cell;
    use std::rc::Rc;

    /// A lease that counts its releases.
    struct Token {
        released: Rc<Cell<u32>>,
    }

    impl Drop for Token {
        fn drop(&mut self) {
            self.released.set(self.released.get() + 1);
        }
    }

    fn token(counter: &Rc<Cell<u32>>) -> Token {
        Token {
            released: counter.clone(),
        }
    }

    fn ok() -> Wc {
        Wc::success(Opcode::SEND, 8)
    }

    fn err(status: Status) -> Wc {
        Wc { status, ..ok() }
    }

    /// What a successful send reports.
    const SENT: CompletionResult = Ok(Completed::Sent);

    /// Stages a request of `wqes` WQEs; returns its sequence number, last WQE and whether it is
    /// signaled.
    fn stage(
        ring: &mut Ring<Token>,
        c: &Rc<Cell<u32>>,
        wqes: u32,
        signal: bool,
        detached: bool,
    ) -> (Seq, u64, bool) {
        let signaled = signal || ring.must_signal(wqes as u64);
        let last = ring.next_wqe() + wqes as u64 - 1;
        let seq = ring.stage(wqes as u64, signaled, detached, SENT, token(c));
        (seq, last, signaled)
    }

    /// Posts and commits a request of `wqes` WQEs; returns its sequence
    /// number, last WQE and whether it is signaled.
    fn post(
        ring: &mut Ring<Token>,
        c: &Rc<Cell<u32>>,
        wqes: u32,
        signal: bool,
        detached: bool,
    ) -> (Seq, u64, bool) {
        let r = stage(ring, c, wqes, signal, detached);
        ring.commit();
        r
    }

    #[test]
    fn signaled_completion_retires_everything_before_it() {
        let c = Rc::new(Cell::new(0));
        let mut ring = Ring::new(8, 8, 1);
        let (a, _, sa) = post(&mut ring, &c, 1, false, false);
        let (b, _, sb) = post(&mut ring, &c, 1, false, true);
        let (d, wd, sd) = post(&mut ring, &c, 1, true, false);
        assert!(!sa && !sb && sd);
        assert!(ring.complete(wd, ok()));
        assert_eq!(ring.retired(), 3);
        // The detached request released its lease at retirement.
        assert_eq!(c.get(), 1);
        assert_eq!(ring.take(a).unwrap().outcome, SENT);
        assert!(ring.take(a).is_none());
        assert!(ring.take(b).is_none());
        assert!(ring.take(d).is_some());
        assert_eq!(c.get(), 3);
        assert!(ring.invariant());
    }

    #[test]
    fn forced_signal_every_depth_minus_max_wqes() {
        let c = Rc::new(Cell::new(0));
        let mut ring = Ring::new(64, 8, 1);
        let mut signals = 0;
        for _ in 0..7 {
            let (_, _, s) = post(&mut ring, &c, 1, false, true);
            signals += s as u32;
        }
        assert_eq!(signals, 0);
        let (_, w, s) = post(&mut ring, &c, 1, false, true);
        assert!(s, "the eighth post must be signaled");
        assert!(!ring.can_post());
        ring.complete(w, ok());
        assert_eq!(c.get(), 8);
        assert!(ring.can_post());
    }

    #[test]
    fn batches_keep_room_for_a_full_request() {
        let c = Rc::new(Cell::new(0));
        let mut ring = Ring::new(64, 8, 4);
        // Tail at most depth - max_wqes = 4.
        let (_, _, s1) = post(&mut ring, &c, 3, false, true);
        let (_, w2, s2) = post(&mut ring, &c, 2, false, true);
        assert!(!s1 && s2);
        ring.complete(w2, ok());
        assert_eq!(ring.in_flight_wqes(), 0);
        let (_, _, s3) = post(&mut ring, &c, 4, false, true);
        assert!(!s3);
        // A full request still fits.
        assert!(ring.can_post());
        assert_eq!(ring.wqe_room(), 4);
    }

    #[test]
    fn error_inside_a_batch_is_kept_until_its_flush() {
        let c = Rc::new(Cell::new(0));
        let mut ring = Ring::new(8, 16, 4);
        let (a, wa, _) = post(&mut ring, &c, 1, false, false);
        let (b, wb, _) = post(&mut ring, &c, 4, true, false);
        // An error on the second WQE of b retires a (successful).
        ring.complete(wa + 2, err(Status::REM_ACCESS_ERR));
        assert_eq!(ring.retired(), 1);
        assert!(ring.take(a).unwrap().outcome.is_ok());
        // The flushes of b's other WQEs, the last retires it with the error.
        ring.complete(wa + 3, err(Status::WR_FLUSH_ERR));
        assert_eq!(ring.retired(), 1);
        ring.complete(wb, err(Status::WR_FLUSH_ERR));
        let taken = ring.take(b).unwrap();
        // At its position: the second of the WQEs of b, whose first follows a's.
        assert_eq!(
            taken.outcome,
            Err(CompletionError::Failed {
                status: Status::REM_ACCESS_ERR,
                vendor_err: 0,
                position: 1
            })
        );
        drop(taken);
        assert!(ring.failed());
        // Late duplicates are ignored, uncommitted WQEs refused.
        assert!(ring.complete(wa, ok()));
        assert!(!ring.complete(wb + 1, ok()));
        assert_eq!(c.get(), 2);
    }

    #[test]
    fn abort_refuses_staged_requests() {
        let c = Rc::new(Cell::new(0));
        let mut ring = Ring::new(8, 8, 2);
        post(&mut ring, &c, 1, true, false);
        let (owned, _, _) = stage(&mut ring, &c, 1, false, false);
        stage(&mut ring, &c, 1, false, true);
        assert_eq!(ring.abort(), 2);
        // The detached one is released and reported; the owned one waits for
        // its owner with a flush error; its number is not reused.
        assert_eq!(c.get(), 1);
        assert_eq!(ring.posted(), 3);
        assert_eq!(ring.in_flight_wqes(), 1);
        assert_eq!(ring.state(owned), State::Refused);
        let detached = ring.take_detached_error().unwrap();
        assert_eq!(detached.error, CompletionError::Refused);
        assert_eq!(detached.seq, 2);
        assert!(ring.invariant());
        let (seq, wqe, _) = post(&mut ring, &c, 1, true, false);
        assert_eq!(seq, 3);
        assert_eq!(wqe, 1, "the refused WQEs' numbers go to the next request");
        let taken = ring.take(owned).unwrap();
        assert_eq!(taken.outcome, Err(CompletionError::Refused));
        drop(taken);
        assert!(ring.take(owned).is_none());
        assert_eq!(c.get(), 2);
        assert!(ring.invariant());
        ring.release_all();
        assert_eq!(c.get(), 4);
    }

    #[test]
    fn refused_requests_retire_with_the_ones_before() {
        let c = Rc::new(Cell::new(0));
        let mut ring = Ring::new(8, 8, 1);
        let (a, wa, _) = post(&mut ring, &c, 1, false, false);
        let (r, _, _) = stage(&mut ring, &c, 1, true, false);
        ring.abort();
        // Refused behind a request in flight: taken at once, out of order.
        assert_eq!(ring.retired(), 0);
        assert!(ring.take(r).is_some());
        let (b, wb, _) = post(&mut ring, &c, 1, true, false);
        assert_eq!(wb, wa + 1);
        assert!(ring.complete(wb, ok()));
        assert_eq!(ring.retired(), 3);
        assert!(ring.take(a).unwrap().outcome.is_ok());
        assert!(ring.take(b).unwrap().outcome.is_ok());
        assert_eq!(ring.head(), ring.posted());
        assert_eq!(c.get(), 3);
        assert!(ring.invariant());
    }

    #[test]
    fn detach_before_and_after_retirement() {
        let c = Rc::new(Cell::new(0));
        let mut ring = Ring::new(4, 4, 1);
        let (a, _, _) = post(&mut ring, &c, 1, true, false);
        let (b, wb, _) = post(&mut ring, &c, 1, true, false);
        ring.detach(a);
        assert_eq!(c.get(), 0, "an in-flight request keeps its lease");
        ring.complete(wb, ok());
        assert_eq!(c.get(), 1);
        ring.detach(b);
        assert_eq!(c.get(), 2);
        assert_eq!(ring.state(b), State::Free);
    }

    #[test]
    fn retirement_wakes_the_registered_waker() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::task::Wake;

        struct Count(AtomicU32);
        impl Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let count = Arc::new(Count(AtomicU32::new(0)));
        let waker = Waker::from(count.clone());
        let c = Rc::new(Cell::new(0));
        let mut ring = Ring::new(4, 4, 1);
        let (a, wa, _) = post(&mut ring, &c, 1, true, false);
        ring.set_waker(a, &waker);
        ring.set_waker(a, &waker);
        ring.complete(wa, ok());
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn uncollected_results_clog_the_ring() {
        let c = Rc::new(Cell::new(0));
        let mut ring = Ring::new(2, 8, 1);
        let (a, _, _) = post(&mut ring, &c, 1, true, false);
        let (_, wb, _) = post(&mut ring, &c, 1, true, false);
        assert!(!ring.has_free_slot());
        assert!(!ring.is_clogged());
        ring.complete(wb, ok());
        assert!(ring.is_clogged());
        ring.take(a).unwrap();
        assert!(ring.can_post());
    }

    /// A deterministic generator for the random sequences.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Random posts, commits, aborts, completions (in order, with errors and
    /// flushes), takes and detaches against a simple model of the hardware
    /// queue; every lease must be released exactly once, the invariant hold
    /// throughout, and requests retire in order.
    #[test]
    fn random_sequences_release_every_lease_once() {
        for seed in 1..400u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let c = Rc::new(Cell::new(0));
            let slots = 1 + rng.below(8) as usize;
            let depth = 1 + rng.below(16) as u32;
            let max_wqes = 1 + rng.below(depth as u64) as u32;
            let mut ring: Ring<Token> = Ring::new(slots, depth, max_wqes);
            let mut made = 0u32;
            // The hardware's view: committed WQEs not completed yet, with
            // whether each asks for a completion.
            let mut hw: Vec<(u64, bool)> = Vec::new();
            let mut staged: Vec<(u64, bool)> = Vec::new();
            let mut owned: Vec<Seq> = Vec::new();
            let mut error = false;
            for _ in 0..300 {
                match rng.below(7) {
                    0 | 1 => {
                        if ring.can_post() {
                            let want = (1 + rng.below(max_wqes as u64) as u32).min(ring.wqe_room());
                            let detached = rng.below(3) == 0;
                            let (seq, last, signaled) =
                                stage(&mut ring, &c, want, rng.below(2) == 0, detached);
                            made += 1;
                            for id in last + 1 - want as u64..=last {
                                staged.push((id, signaled && id == last));
                            }
                            if !detached {
                                owned.push(seq);
                            }
                        }
                    }
                    2 => {
                        ring.commit();
                        hw.append(&mut staged);
                    }
                    3 => {
                        if rng.below(8) == 0 {
                            let staged_seqs = ring.committed()..ring.posted();
                            ring.abort();
                            staged.clear();
                            for s in staged_seqs {
                                if owned.contains(&s) {
                                    assert_eq!(ring.state(s), State::Refused, "seed {seed}");
                                }
                            }
                        }
                    }
                    4 => {
                        // The NIC completes WQEs in order; a signaled or failed
                        // one produces a completion. Once an error happened every
                        // later WQE is flushed with a completion of its own.
                        let n = rng.below(hw.len() as u64 + 1) as usize;
                        for _ in 0..n {
                            let (id, signaled) = hw.remove(0);
                            if error {
                                ring.complete(id, err(Status::WR_FLUSH_ERR));
                            } else if rng.below(20) == 0 {
                                error = true;
                                ring.complete(id, err(Status::REM_ACCESS_ERR));
                            } else if signaled {
                                ring.complete(id, ok());
                            }
                        }
                    }
                    5 => {
                        if !owned.is_empty() {
                            let k = rng.below(owned.len() as u64) as usize;
                            let seq = owned[k];
                            if ring.take(seq).is_some() {
                                owned.remove(k);
                            }
                        }
                    }
                    _ => {
                        if !owned.is_empty() {
                            let k = rng.below(owned.len() as u64) as usize;
                            ring.detach(owned.remove(k));
                        }
                    }
                }
                assert!(ring.invariant(), "seed {seed}");
                assert!(c.get() <= made);
            }
            ring.release_all();
            assert_eq!(c.get(), made, "seed {seed}: every lease released once");
        }
    }
}
