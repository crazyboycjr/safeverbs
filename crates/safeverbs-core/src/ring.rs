//! The completion ring of one work queue: which requests are in flight, what
//! they hold, and when they retire.
//!
//! This module is bookkeeping only: no FFI, no `unsafe`, no closures or
//! iterator chains. It is written inside `verus!` and proved: Verus checks
//! that every normal return keeps the invariants below and that arithmetic
//! and indexing never panic or overflow (`scripts/verify.sh`; a plain `cargo build` erases the proofs).
//! The engine (`crate::engine`) drives it next to the hardware queue.
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
//! the WQE or request numbers reach [`COUNTERS_END`], which no NIC does. The proofs also rely on a few facts about wakers that
//! Verus takes on trust: [`core::task::Waker`] is opaque to them, and waking,
//! comparing and cloning one are the standard library's (see the TCB in
//! `docs/verification.md`). User wake panics are retained until the ring operation
//! finishes; an engine shares that retention across the whole already-polled CQ batch.
//! The panic then propagates with the bookkeeping consistent.

use core::task::Waker;

use safeverbs_spec::abs::{Phase, WakerId};
use vstd::prelude::*;

use crate::buf::{forget_free, replace_value};
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

/// The WQE and request numbers a ring refuses to go past: far from overflowing with one request
/// more of any size.
pub const COUNTERS_END: u64 = 0x4000_0000_0000_0000;

/// Slot `i` of `slots`, for an `i` from [`Ring::index`], to read, with no bounds check.
#[inline(always)]
fn slot_of<L>(slots: &Vec<Slot<L>>, i: usize) -> (slot: &Slot<L>)
    requires
        i < slots@.len(),
    ensures
        *slot == slots@[i as int],
{
    // SAFETY: `i` is below the length (the precondition).
    unsafe { slots.as_slice().get_unchecked(i) }
}

/// Slot `i` of `slots`, for an `i` from [`Ring::index`], with no bounds check: the data path holds
/// a request's buffers at this point, and a panic edge would keep them in memory.
#[inline(always)]
fn slot_in<L>(slots: &mut Vec<Slot<L>>, i: usize) -> (slot: &mut Slot<L>)
    requires
        i < old(slots)@.len(),
    ensures
        *slot == old(slots)@[i as int],
        final(slots)@ == old(slots)@.update(i as int, *final(slot)),
{
    // SAFETY: `i` is below the length (the precondition).
    unsafe { slots.as_mut_slice().get_unchecked_mut(i) }
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
    pub open spec fn completed(self, outcome: safeverbs_spec::types::Outcome) -> ReqView<L> {
        ReqView { phase: Phase::Complete(outcome), waker: None, ..self }
    }
}

/// The interval and outcome bookkeeping of one ring request.
/// This observation creates neither an abstract request nor a WQE identity.
pub ghost struct RequestMeta {
    /// First WQE of this native request, inclusive.
    pub first: u64,
    /// End of its WQE interval, exclusive.
    pub end: u64,
    /// What it reports: staged or in flight, the success a later completion would retire it with,
    /// or the first error recorded on it; then its final outcome.
    pub outcome: CompletionResult,
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
    /// Its outcome and leases wait for the owner.
    Refused,
}

/// One request's entry. Free when it holds no leases.
pub(crate) struct Slot<L> {
    /// One past the WQE sequence number of its last WQE; a refused request has no WQEs, and
    /// this is where they would have started.
    pub(crate) end_wqe: u64,
    /// The WQE sequence number of its first WQE (erased: the ring's `wqe_retired` is the
    /// oldest request's).
    pub(crate) first_wqe: Ghost<u64>,
    /// What it reports. Staged or in flight, the success a later completion would retire it
    /// with, or the first error recorded on it; then its outcome.
    pub(crate) outcome: CompletionResult,
    /// No owner: release the leases at retirement.
    pub(crate) detached: bool,
    /// Its retirement takes the general path: it has no owner, or holds more than the common case
    /// does ([`Ring::take_general_path`]).
    pub(crate) general: bool,
    /// What keeps its memory alive while the NIC uses it.
    pub(crate) leases: Option<L>,
}

impl<L> Slot<L> {
    /// Whether its request ends just before WQE number `end`.
    #[inline(always)]
    pub(crate) fn end_is(&self, end: u64) -> (is: bool)
        ensures
            is == (self.end_wqe == end),
    {
        self.end_wqe == end
    }

    /// The slot without its leases.
    pub(crate) closed spec fn emptied(self) -> Slot<L> {
        Slot { leases: None, ..self }
    }

    /// Takes what it holds.
    #[inline(always)]
    pub(crate) fn take_leases(&mut self) -> (leases: Option<L>)
        ensures
            leases == old(self).leases,
            *final(self) == old(self).emptied(),
    {
        self.leases.take()
    }

    /// Whether it says that the doorbell of its request was refused.
    spec fn is_refused(&self) -> bool {
        self.outcome matches Err(CompletionError::Refused)
    }

    /// The slot freed of everything it holds.
    fn free() -> (slot: Slot<L>)
        ensures
            slot.leases is None,
    {
        Slot {
            end_wqe: 0,
            first_wqe: Ghost(0),
            outcome: Ok(Completed::Sent),
            detached: false,
            general: false,
            leases: None,
        }
    }
}

/// A staged slot after a refused doorbell ([`Ring::abort_keeping`]): its WQEs gone (an empty range
/// at `wqe`); freed if `keep` or detached, else holding its refusal and its leases for the owner.
spec fn refuse<L>(slot: Slot<L>, keep: bool, wqe: u64) -> Slot<L> {
    if keep {
        Slot {
            first_wqe: Ghost(wqe),
            end_wqe: wqe,
            outcome: Err(CompletionError::Refused),
            leases: None,
            ..slot
        }
    } else if slot.detached {
        Slot { first_wqe: Ghost(wqe), end_wqe: wqe, leases: None, ..slot }
    } else {
        Slot {
            first_wqe: Ghost(wqe),
            end_wqe: wqe,
            outcome: Err(CompletionError::Refused),
            ..slot
        }
    }
}

/// A slot as a request's retirement with `outcome` leaves it: without an owner it holds nothing.
spec fn retired_slot_of<L>(slot: Slot<L>, outcome: CompletionResult) -> Slot<L> {
    Slot { outcome, leases: if slot.detached { None } else { slot.leases }, ..slot }
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
    /// The number of slots is two to this power (erased).
    #[allow(dead_code)]
    bits: Ghost<nat>,
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
    /// Whether refused requests may be left among the committed ones, so that retiring has more
    /// to look at. It decides when the slots of refused requests are looked at, and nothing the
    /// proofs need.
    refused: bool,
    /// The first error of a detached request, until someone takes it.
    detached_error: Option<DetachedRecord>,
    /// Who to wake when each slot's request retires: by slot, empty until a waker is first
    /// registered, so that a ring nobody waits on holds none.
    wakers: Vec<Option<Waker>>,
    /// The names of the wakers woken so far, in order (ghost).
    #[allow(dead_code)]
    woken: Ghost<vstd::seq::Seq<WakerId>>,
    /// One past the last WQE a completion spent on this ring named (ghost): M may have
    /// retired more, of completions not routed yet.
    #[allow(dead_code)]
    routed: Ghost<nat>,
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

/// With `mask` one less than a power of two, masking is the remainder.
proof fn lemma_mask(x: u64, mask: u64, n: int, bits: nat)
    requires
        n == vstd::arithmetic::power2::pow2(bits),
        bits <= 41,
        mask as int == n - 1,
    ensures
        (x & mask) as int == x as int % n,
        (x & mask) < n,
{
    vstd::arithmetic::power2::lemma_pow2_strictly_increases(bits, 64);
    vstd::arithmetic::power2::lemma2_to64();
    vstd::arithmetic::power2::lemma_pow2_pos(bits);
    vstd::bits::lemma_u64_low_bits_mask_is_mod(x, bits);
    vstd::arithmetic::div_mod::lemma_mod_bound(x as int, n);
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

    /// The waker registered on request `seq`.
    closed spec fn waker(&self, seq: int) -> Option<Waker> {
        if self.wakers@.len() == 0 {
            None
        } else {
            self.wakers@[seq % self.n()]
        }
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
        &&& self.n() == vstd::arithmetic::power2::pow2(self.bits@)
        &&& self.bits@ <= 41
        &&& self.mask as int == self.n() - 1
        &&& self.wakers@.len() == 0 || self.wakers@.len() == self.n()
        &&& 1 <= self.max_wqes <= self.depth <= u32::MAX
        &&& self.head <= self.retired <= self.committed <= self.posted
        &&& self.posted - self.head <= self.n()
        &&& self.posted <= COUNTERS_END
        &&& self.wqe_retired <= self.wqe_committed <= self.wqe_posted
        &&& self.wqe_posted - self.wqe_retired <= self.depth
        &&& self.wqe_posted <= COUNTERS_END + u32::MAX
    }

    /// 5, 6 and 7 for a request in `[head, retired)`: its slot is free, or
    /// holds its result for its owner, and no waker.
    closed spec fn done_ok(&self, s: int) -> bool {
        let slot = self.slot(s);
        &&& self.waker(s) is None
        &&& slot.leases is Some ==> !slot.detached
    }

    /// Request `s` is committed and the NIC never saw it: its doorbell was refused, and its
    /// result, if it has one, says so.
    closed spec fn is_void(&self, s: int) -> bool {
        s < self.committed && (self.slot(s).leases is None || self.slot(s).is_refused())
    }

    /// 3 for a refused request in `[retired, committed)`: no WQEs (the NIC
    /// never saw them), between the retired and the committed ones; its
    /// slot is free or holds its result, and no waker.
    closed spec fn void_ok(&self, s: int) -> bool {
        let slot = self.slot(s);
        &&& slot.first_wqe@ == slot.end_wqe
        &&& self.wqe_retired <= slot.first_wqe@ <= self.wqe_committed
        &&& self.waker(s) is None
        &&& slot.leases is Some ==> !slot.detached
        &&& slot.outcome is Ok ==> slot.detached
        &&& slot.detached ==> slot.general
    }

    /// 3, 5 and 7 for a request in flight (if committed) or staged, holding
    /// its leases, over the WQEs `[first_wqe, end_wqe)`.
    closed spec fn posted_ok(&self, s: int) -> bool {
        let slot = self.slot(s);
        &&& slot.leases is Some
        &&& !slot.is_refused()
        &&& slot.detached ==> self.waker(s) is None && slot.general
        &&& self.wqe_retired <= slot.first_wqe@ < slot.end_wqe <= self.wqe_posted
        &&& slot.end_wqe - slot.first_wqe@ <= self.max_wqes
        &&& if s < self.committed {
            slot.end_wqe <= self.wqe_committed
        } else {
            self.wqe_committed <= slot.first_wqe@ && slot.outcome is Ok
        }
    }

    /// 3, 5 and 7 for a request in `[retired, posted)`.
    closed spec fn live_ok(&self, s: int) -> bool {
        if self.is_void(s) {
            self.void_ok(s)
        } else {
            self.posted_ok(s)
        }
    }

    /// 1 and 5 for a slot outside the window `[head, posted)`: it holds
    /// nothing.
    closed spec fn free_ok(&self, s: int) -> bool {
        self.slot(s).leases is None && self.waker(s) is None
    }

    /// Every slot, by where its request is, but free slot `except`, which may hold what is about
    /// to become a request or to leave.
    #[verifier::opaque]
    closed spec fn inv_slots_but(&self, except: int) -> bool {
        &&& forall|s: int| self.head <= s < self.retired ==> #[trigger] self.done_ok(s)
        &&& forall|s: int| self.retired <= s < self.posted ==> #[trigger] self.live_ok(s)
        &&& forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != except
            ==> #[trigger] self.free_ok(s)
    }

    /// Every slot, by where its request is.
    closed spec fn inv_slots(&self) -> bool {
        self.inv_slots_but(-1)
    }

    /// 3: the requests not retired cover consecutive WQE ranges from
    /// `wqe_retired`, the committed ones up to `wqe_committed`, all up to `wqe_posted`.
    #[verifier::opaque]
    closed spec fn inv_wqes(&self) -> bool {
        &&& self.retired < self.posted ==> self.slot(self.retired as int).first_wqe@
            == self.wqe_retired
        &&& forall|s: int|
            self.retired < s < self.posted ==> #[trigger] self.slot(s).first_wqe@ == self.slot(
                s - 1,
            ).end_wqe
        &&& if self.retired < self.committed {
            self.slot(self.committed - 1).end_wqe == self.wqe_committed
        } else {
            self.wqe_committed == self.wqe_retired
        }
        &&& if self.committed < self.posted {
            self.slot(self.posted - 1).end_wqe == self.wqe_posted
        } else {
            self.wqe_committed == self.wqe_posted
        }
    }

    /// 8: the WQEs posted after the last signaled request and not retired
    /// number at most `depth - max_wqes`, and so they do after an abort.
    closed spec fn inv_signal(&self) -> bool {
        &&& self.sig_end <= self.wqe_posted
        &&& self.committed_sig_end <= self.wqe_committed
        &&& self.wqe_posted - self.tail_start(self.sig_end) <= self.depth - self.max_wqes
        &&& self.wqe_committed - self.tail_start(self.committed_sig_end) <= self.depth
            - self.max_wqes
    }

    /// The head is advanced past the free slots at the front of the window.
    closed spec fn head_ok(&self) -> bool {
        self.head < self.retired ==> self.slot(self.head as int).leases is Some
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
        if seq < self.head || seq >= self.posted || self.slot(seq).leases is None {
            State::Free
        } else if seq >= self.committed {
            State::Staged
        } else if self.slot(seq).is_refused() {
            State::Refused
        } else if seq < self.retired {
            State::Retired
        } else {
            State::InFlight
        }
    }

    /// What slot `s` holds, as a request.
    closed spec fn view_of(&self, s: int) -> ReqView<L> {
        let slot = self.slot(s);
        ReqView {
            phase: if s < self.retired {
                Phase::Complete(crate::wc::result_model(slot.outcome))
            } else {
                Phase::InFlight
            },
            waker: waker_name(self.waker(s)),
            detached: slot.detached,
            leases: slot.leases->0,
        }
    }

    /// Request `s` as A sees it, if A sees it: posted to the NIC (committed)
    /// and not freed (retired and taken, or detached).
    pub closed spec fn req(&self, s: int) -> Option<ReqView<L>> {
        if self.head <= s < self.committed && self.slot(s).leases is Some && !self.slot(
            s,
        ).is_refused() {
            Some(self.view_of(s))
        } else {
            None
        }
    }

    /// The leases of request `s` if it was refused (its doorbell refused)
    /// and its owner has not taken it. A never saw it: its leases are the
    /// program's, which the ring holds for the owner.
    pub closed spec fn refused(&self, s: int) -> Option<L> {
        if self.head <= s < self.committed && self.slot(s).leases is Some && self.slot(s).is_refused() {
            self.slot(s).leases
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

    /// The native WQE interval and what it reports so far.
    /// Refused results are excluded even if their old WQE numbers are reused.
    pub closed spec fn meta(&self, s: int) -> Option<RequestMeta> {
        if self.req(s) is Some || self.staged(s) is Some {
            let slot = self.slot(s);
            Some(RequestMeta { first: slot.first_wqe@, end: slot.end_wqe, outcome: slot.outcome })
        } else {
            None
        }
    }

    /// A stuttering operation leaves native intervals and outcome caches alone.
    pub open spec fn same_meta(&self, pre: &Self) -> bool {
        forall|s: int| #[trigger] self.meta(s) == pre.meta(s)
    }

    /// Metadata after retiring a request, before its slot is reused.
    pub closed spec fn completed_meta(&self, s: int, outcome: CompletionResult) -> Option<RequestMeta> {
        if self.req(s) is None || self.slot(s).detached {
            None
        } else {
            Some(RequestMeta { outcome, ..self.meta(s)->0 })
        }
    }

    /// A partial error records the first failure and leaves it unchanged later.
    pub closed spec fn recorded_meta(&self, s: int, error: CompletionError) -> Option<RequestMeta> {
        Some(RequestMeta { outcome: Err(error), ..self.meta(s)->0 })
    }

    /// Inspect the native bookkeeping of a live or staged request.
    pub proof fn lemma_meta(&self, s: int)
        requires
            self.inv(),
        ensures
            self.meta(s) is Some <==> self.req(s) is Some || self.staged(s) is Some,
            self.meta(s) is Some && (self.req(s) is Some && self.req(s)->0.phase is InFlight
                || self.staged(s) is Some) ==> self.spec_wqe_retired() <= self.meta(s)->0.first
                < self.meta(s)->0.end <= self.spec_wqe_posted(),
            self.meta(s) is Some && (self.req(s) is Some && self.req(s)->0.phase is InFlight
                || self.staged(s) is Some) ==> self.meta(s)->0.end - self.meta(s)->0.first
                <= self.spec_max_wqes(),
            self.req(s) is Some && self.req(s)->0.phase is Complete
                ==> self.req(s)->0.phase == Phase::Complete(crate::wc::result_model(self.meta(s)->0.outcome)),
            self.req(s) is Some ==> !(self.meta(s)->0.outcome matches Err(CompletionError::Refused)),
            self.staged(s) is Some ==> self.meta(s)->0.outcome is Ok,
            self.staged(s) is Some ==> self.spec_wqe_committed() <= self.meta(s)->0.first,
            self.req(s) is Some && self.req(s)->0.phase is InFlight ==> self.meta(s)->0.end
                <= self.spec_wqe_committed(),
    {
        self.lemma_view();
        reveal(Ring::inv_slots_but);
        if self.meta(s) is Some {
            if s < self.retired {
                assert(self.done_ok(s));
            } else {
                assert(self.live_ok(s));
            }
        }
    }

    /// The live request retaining WQE `w`. Refused and retired slots
    /// never authorize DMA, even when a refused WQE number is reused.
    pub closed spec fn dma(&self, s: int, w: int) -> Option<L> {
        if self.retired <= s < self.posted && self.slot(s).first_wqe@ <= w < self.slot(s).end_wqe {
            self.slot(s).leases
        } else {
            None
        }
    }

    /// All still-live WQEs retain their original request and exact resource value.
    pub open spec fn keeps_dma(&self, pre: &Self) -> bool {
        forall|s: int, w: int|
            pre.dma(s, w) is Some && self.spec_wqe_retired() <= w < self.spec_wqe_posted()
                ==> #[trigger] self.dma(s, w) == pre.dma(s, w)
    }

    /// Every live WQE retains exactly the same request and resource value.
    pub open spec fn same_dma(&self, pre: &Self) -> bool {
        forall|s: int, w: int| #[trigger] self.dma(s, w) == pre.dma(s, w)
    }

    /// A DMA observer names an in-flight or staged request, with its
    /// exact resources, and never a result retained after refusal.
    pub proof fn lemma_dma(&self, s: int, w: int)
        requires
            self.inv(),
            self.dma(s, w) is Some,
        ensures
            self.spec_retired() <= s < self.spec_posted(),
            self.spec_wqe_retired() <= w < self.spec_wqe_posted(),
            (self.req(s) is Some && self.req(s)->0.phase is InFlight && self.req(s)->0.leases
                == self.dma(s, w)->0) || (self.staged(s) is Some && self.staged(s)->0.leases
                == self.dma(s, w)->0),
            self.refused(s) is None,
            w < self.spec_wqe_committed() ==> self.req(s) is Some && self.req(s)->0.phase is InFlight
                && self.req(s)->0.leases == self.dma(s, w)->0,
            self.spec_wqe_committed() <= w ==> self.staged(s) is Some && self.staged(s)->0.leases
                == self.dma(s, w)->0,
    {
        reveal(Ring::inv_slots_but);
        assert(self.live_ok(s));
    }

    /// Two live requests cannot authorize the same WQE number. This is
    /// independent of wr_id encoding and excludes historical refused ledgers.
    pub proof fn lemma_dma_unique(&self, s: int, t: int, w: int)
        requires
            self.inv(),
            self.dma(s, w) is Some,
            self.dma(t, w) is Some,
        ensures
            s == t,
    {
        self.lemma_dma(s, w);
        self.lemma_dma(t, w);
        if s < t {
            self.lemma_ordered(s, t);
        }
        if t < s {
            self.lemma_ordered(t, s);
        }
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
        reveal(Ring::inv_slots_but);
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
    pub proof fn lemma_retired_view(&self, s: int, outcome: CompletionResult)
        requires
            self.inv(),
            self.spec_retired() <= s < self.spec_committed(),
        ensures
            self.retired_view(s, outcome) == match self.req(s) {
                None => None,
                Some(r) => if r.detached {
                    None
                } else {
                    Some(r.completed(crate::wc::result_model(outcome)))
                },
            },
    {
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
        reveal(Ring::inv_slots_but);
        if from < to {
            self.lemma_wakers(from, to - 1);
            assert(self.live_ok(to - 1));
        }
    }

    /// `self` and `other` agree on the counters, whatever the slots and the wakers.
    closed spec fn same_counters(&self, other: &Self) -> bool {
        &&& self.same_counters_but_woken(other)
        &&& self.woken == other.woken
    }

    /// `self` and `other` agree on the counters, whatever the slots, the wakers and those woken.
    closed spec fn same_counters_but_woken(&self, other: &Self) -> bool {
        &&& self.mask == other.mask
        &&& self.bits == other.bits
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
    }

    /// The slot invariants depend on the slots, the wakers and the window counters only.
    proof fn lemma_frame(pre: &Self, post: &Self)
        requires
            pre.inv_slots(),
            pre.inv_wqes(),
            post.slots@ == pre.slots@,
            post.wakers@ == pre.wakers@,
            post.head == pre.head,
            post.retired == pre.retired,
            post.committed == pre.committed,
            post.posted == pre.posted,
            post.max_wqes == pre.max_wqes,
            post.wqe_retired == pre.wqe_retired,
            post.wqe_committed == pre.wqe_committed,
            post.wqe_posted == pre.wqe_posted,
        ensures
            post.inv_slots(),
            post.inv_wqes(),
            pre.head_ok() ==> post.head_ok(),
            post.same_dma(pre),
            post.same_meta(pre),
            post.woken == pre.woken ==> post.same_view(pre),
    {
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        assert forall|s: int| post.head <= s < post.retired implies #[trigger] post.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| post.retired <= s < post.posted implies #[trigger] post.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| post.posted <= s < post.head + post.n() && s % post.n() != -1
            implies #[trigger] post.free_ok(s) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| post.retired < s < post.posted implies #[trigger] post.slot(s).first_wqe@
            == post.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
        }
    }

    /// The WQE ranges of the requests not retired are in order: a later
    /// request's WQEs come after an earlier one's.
    proof fn lemma_ordered(&self, s: int, t: int)
        requires
            self.inv_body(),
            self.retired <= s < t < self.posted,
        ensures
            self.slot(s).end_wqe <= self.slot(t).first_wqe@,
        decreases t - s,
    {
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        assert(self.slot(t).first_wqe@ == self.slot(t - 1).end_wqe);
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
        requires
            1 <= slots <= 0x100_0000_0000usize,
            1 <= max_wqes <= depth,
        ensures
            ring.inv(),
            ring.spec_wqe_retired() == 0,
            ring.spec_wqe_committed() == 0,
            ring.spec_wqe_posted() == 0,
            ring.spec_depth() == depth,
            ring.spec_max_wqes() == max_wqes,
            forall|s: int| #[trigger] ring.req(s) is None,
            forall|s: int| #[trigger] ring.staged(s) is None,
            forall|s: int| #[trigger] ring.refused(s) is None,
            forall|s: int| #[trigger] ring.meta(s) is None,
            forall|s: int, w: int| #[trigger] ring.dma(s, w) is None,
            ring.woken() == vstd::seq::Seq::<WakerId>::empty(),
            ring.spec_detached_error() is None,
    {
        assert!(slots >= 1, "a ring needs a slot");
        assert!(slots <= 0x100_0000_0000, "a ring of at most 2^40 slots");
        assert!(max_wqes >= 1 && max_wqes <= depth, "a request needs 1..=depth WQEs");
        let mut n: usize = 1;
        let ghost mut bits: nat = 0;
        proof {
            vstd::arithmetic::power2::lemma2_to64();
        }
        while n < slots
            invariant
                n == vstd::arithmetic::power2::pow2(bits),
                slots <= 0x100_0000_0000usize,
                1 <= n < 2 * slots,
                bits <= 41,
            decreases 2 * slots - n,
        {
            proof {
                vstd::arithmetic::power2::lemma_pow2_unfold(bits + 1);
                if bits >= 41 {
                    vstd::arithmetic::power2::lemma_pow2_strictly_increases(41, bits + 1);
                    vstd::arithmetic::power2::lemma2_to64_rest();
                    if bits > 41 {
                        vstd::arithmetic::power2::lemma_pow2_strictly_increases(41, bits);
                    }
                    assert(false);
                }
            }
            n *= 2;
            proof {
                bits = bits + 1;
            }
        }
        let mut v: Vec<Slot<L>> = Vec::with_capacity(n);
        let mut i = 0;
        while i < n
            invariant
                v@.len() == i,
                i <= n,
                forall|j: int| 0 <= j < i ==> (#[trigger] v@[j]).leases is None,
            decreases n - i,
        {
            v.push(Slot::free());
            i += 1;
        }
        let ring = Ring {
            slots: v,
            mask: (n - 1) as u64,
            bits: Ghost(bits),
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
            refused: false,
            detached_error: None,
            wakers: Vec::new(),
            woken: Ghost(vstd::seq::Seq::empty()),
            routed: Ghost(0),
        };
        proof {
            reveal(Ring::inv_slots_but);
            reveal(Ring::inv_wqes);
            assert forall|s: int| ring.posted <= s < ring.head + ring.n() && s % ring.n() != -1
                implies #[trigger] ring.free_ok(s) by {
                vstd::arithmetic::div_mod::lemma_mod_bound(s, ring.n());
            }
        }
        ring
    }

    #[inline(always)]
    fn index(&self, seq: Seq) -> (i: usize)
        requires
            self.n() >= 1,
            self.n() == vstd::arithmetic::power2::pow2(self.bits@),
            self.bits@ <= 41,
            self.mask as int == self.n() - 1,
        ensures
            i == seq as int % self.n(),
            i < self.slots@.len(),
    {
        proof {
            lemma_mask(seq, self.mask, self.n(), self.bits@);
        }
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
    pub fn max_wqes(&self) -> (max: u32)
        requires
            self.inv(),
        ensures
            max == self.spec_max_wqes(),
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

    /// One past the last WQE written.
    pub closed spec fn spec_wqe_posted(&self) -> u64 {
        self.wqe_posted
    }

    /// One past the last WQE a completion spent on this ring named.
    pub closed spec fn spec_routed(&self) -> nat {
        self.routed@
    }

    /// One past the last committed WQE.
    pub closed spec fn spec_wqe_committed(&self) -> u64 {
        self.wqe_committed
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
        &&& self.spec_max_wqes() == other.spec_max_wqes()
        &&& self.spec_routed() == other.spec_routed()
    }

    /// The WQE accounting (invariant 2): the WQEs retired, committed and
    /// written come in that order, at most `depth` of them in flight.
    pub proof fn lemma_wqes(&self)
        requires
            self.inv(),
        ensures
            self.spec_wqe_retired() <= self.spec_wqe_committed() <= self.spec_wqe_posted(),
            self.spec_wqe_posted() - self.spec_wqe_retired() <= self.spec_depth(),
            1 <= self.spec_depth() <= u32::MAX,
            1 <= self.spec_max_wqes() <= self.spec_depth(),
            self.spec_wqe_posted() <= COUNTERS_END + u32::MAX,
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

    /// Whether a slot is free for the next request.
    ///
    /// It also refuses once the WQE or request sequence numbers are nearly
    /// exhausted (after [`COUNTERS_END`] of them, centuries at any rate a
    /// NIC can reach), so that no counter can overflow. One comparison
    /// covers both: neither number exceeds their bitwise or.
    #[inline(always)]
    pub fn has_free_slot(&self) -> (free: bool)
        requires
            self.inv(),
        ensures
            free == self.spec_has_free_slot(),
    {
        self.posted - self.head <= self.mask && (self.wqe_posted | self.posted) < COUNTERS_END
    }

    /// [`Ring::has_free_slot`], as a specification.
    pub closed spec fn spec_has_free_slot(&self) -> bool {
        self.posted - self.head < self.n() && (self.wqe_posted | self.posted) < COUNTERS_END
    }

    /// One past the last retired WQE: every WQE before it has completed (or
    /// was never seen by the NIC). A completion names a WQE at most
    /// [`Ring::depth`] after it.
    #[inline(always)]
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
        requires
            self.inv(),
        ensures
            can == (self.spec_has_free_slot() && self.spec_wqe_posted() - self.spec_wqe_retired()
                < self.spec_depth()),
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
            && self.spec_detached_error()->0.error is Refused
    }

    /// What the completion of WQE `wqe` with `wc` does to the detached error,
    /// which is `post` after it, having retired requests up to `to`: it stays
    /// or, if there was none, one of the requests without an owner it retires
    /// with an error has it.
    pub open spec fn detached_completed(&self, post: Option<DetachedRecord>, recv: bool, wqe: u64, wc: Wc, to: int) -> bool {
        ||| post == self.spec_detached_error()
        ||| self.spec_detached_error() is None && post is Some && {
            let d = post->0;
            let s = d.seq as int;
            &&& self.spec_retired() <= s < to
            &&& self.req(s) is Some && self.req(s)->0.phase is InFlight && self.req(s)->0.detached
            &&& self.retire_outcome(s, recv, wqe, wc) == Err::<Completed, CompletionError>(d.error)
            &&& d.first_wqe@ == self.meta(s)->0.first
            &&& d.end_wqe@ == self.meta(s)->0.end
            &&& d.error is Failed
        }
    }

    /// `t` is what [`Ring::take`] hands back of request `s`: a refused one
    /// saying so, or a complete one with its outcome and its leases.
    pub open spec fn taken_from(&self, s: int, t: Taken<L>) -> bool {
        if t.outcome matches Err(CompletionError::Refused) {
            &&& self.req(s) is None
            &&& self.refused(s) == Some(t.leases)
        } else {
            &&& self.req(s) == Some(
                ReqView { phase: Phase::Complete(crate::wc::result_model(t.outcome)), waker: None, detached: false, leases: t.leases },
            )
            &&& self.meta(s)->0.outcome == t.outcome
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
            final(self).spec_posted() == old(self).spec_posted(),
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
        requires
            self.inv(),
        ensures
            state == self.state_of(seq as int),
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
        requires
            self.inv(),
        ensures
            room == self.spec_depth() - (self.spec_wqe_posted() - self.spec_wqe_retired()),
    {
        (self.depth - (self.wqe_posted - self.wqe_retired)) as u32
    }

    /// The WQE sequence number the next WQE written takes (its `wr_id`).
    #[inline(always)]
    pub fn next_wqe(&self) -> (id: u64)
        ensures
            id == self.spec_wqe_posted(),
    {
        self.wqe_posted
    }

    /// Whether a new request of `wqes` WQEs must be signaled: with them, the unsignaled tail
    /// would exceed `depth - max_wqes`.
    #[inline]
    pub fn must_signal(&self, wqes: u64) -> (must: bool)
        requires
            self.inv(),
            wqes <= u32::MAX,
        ensures
            must == self.spec_must_signal(wqes),
    {
        let base = if self.sig_end > self.wqe_retired {
            self.sig_end
        } else {
            self.wqe_retired
        };
        self.wqe_posted + wqes - base > self.depth - self.max_wqes
    }

    /// [`Ring::must_signal`], as a specification.
    pub closed spec fn spec_must_signal(&self, wqes: u64) -> bool {
        self.wqe_posted + wqes - self.tail_start(self.sig_end) > self.depth - self.max_wqes
    }

    /// A request of `wqes` WQEs fits in the next slot, and in the queue.
    pub open spec fn fits(&self, wqes: u64) -> bool {
        &&& self.spec_has_free_slot()
        &&& 1 <= wqes <= self.spec_max_wqes()
        &&& wqes <= self.spec_depth() - (self.spec_wqe_posted() - self.spec_wqe_retired())
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
        requires
            old(self).inv(),
            old(self).fits(wqes),
            signaled || !old(self).spec_must_signal(wqes),
            implied is Ok,
        ensures
            seq == old(self).spec_posted(),
            final(self).inv(),
            final(self).spec_head() == old(self).spec_head(),
            final(self).spec_retired() == old(self).spec_retired(),
            final(self).spec_committed() == old(self).spec_committed(),
            final(self).spec_posted() == seq + 1,
            final(self).spec_wqe_retired() == old(self).spec_wqe_retired(),
            final(self).spec_wqe_committed() == old(self).spec_wqe_committed(),
            final(self).spec_wqe_posted() == old(self).spec_wqe_posted() + wqes,
            final(self).spec_depth() == old(self).spec_depth(),
            final(self).spec_max_wqes() == old(self).spec_max_wqes(),
            final(self).woken() == old(self).woken(),
            final(self).spec_detached_error() == old(self).spec_detached_error(),
            forall|s: int| #[trigger] final(self).req(s) == old(self).req(s),
            forall|s: int| #[trigger] final(self).refused(s) == old(self).refused(s),
            forall|s: int| #[trigger] final(self).staged(s) == if s == seq {
                Some(ReqView::new(detached, leases))
            } else {
                old(self).staged(s)
            },
            forall|s: int, w: int| #[trigger] final(self).dma(s, w) == if s == seq
                && old(self).spec_wqe_posted() <= w < old(self).spec_wqe_posted() + wqes {
                Some(leases)
            } else {
                old(self).dma(s, w)
            },
            forall|s: int| #[trigger] final(self).meta(s) == if s == seq {
                Some(RequestMeta {
                    first: old(self).spec_wqe_posted(),
                    end: (old(self).spec_wqe_posted() + wqes) as u64,
                    outcome: implied,
                })
            } else {
                old(self).meta(s)
            },
    {
        self.fill(wqes, detached, implied, leases);
        self.stage_filled(wqes, signaled)
    }

    /// Everything of the invariant but that the next request's slot holds what [`Ring::fill`] put
    /// there.
    pub closed spec fn is_filled(&self) -> bool {
        &&& self.inv_counters()
        &&& self.inv_slots_but(self.posted as int % self.n())
        &&& self.inv_wqes()
        &&& self.inv_signal()
        &&& self.head_ok()
        &&& self.posted - self.head < self.n()
        &&& self.slot(self.posted as int).leases is Some
        &&& self.waker(self.posted as int) is None
        &&& self.slot(self.posted as int).first_wqe@ == self.wqe_posted
        &&& self.wqe_posted < self.slot(self.posted as int).end_wqe
        &&& self.slot(self.posted as int).end_wqe - self.wqe_posted <= self.max_wqes
        &&& self.slot(self.posted as int).end_wqe - self.wqe_retired <= self.depth
        &&& self.slot(self.posted as int).end_wqe <= COUNTERS_END + u32::MAX
        &&& self.posted < COUNTERS_END
        &&& self.slot(self.posted as int).outcome is Ok
        &&& self.slot(self.posted as int).detached ==> self.slot(self.posted as int).general
    }

    /// The request in the next slot, as [`Ring::fill`] left it.
    pub closed spec fn filled_view(&self) -> ReqView<L> {
        self.view_of(self.posted as int)
    }

    /// One past the last WQE of the request in the next slot, as [`Ring::fill`] left it.
    pub closed spec fn filled_end(&self) -> u64 {
        self.slot(self.posted as int).end_wqe
    }

    /// The bookkeeping of the request in the next slot, as [`Ring::fill`] left it.
    pub closed spec fn filled_meta(&self) -> RequestMeta {
        let slot = self.slot(self.posted as int);
        RequestMeta { first: slot.first_wqe@, end: slot.end_wqe, outcome: slot.outcome }
    }

    /// Nothing A sees changed, nor any counter: only the free slots hold something else.
    pub open spec fn same_but_free(&self, other: &Self) -> bool {
        &&& self.same_view(other)
        &&& self.same_wqes(other)
        &&& self.spec_head() == other.spec_head()
        &&& self.spec_retired() == other.spec_retired()
        &&& self.spec_committed() == other.spec_committed()
        &&& self.spec_posted() == other.spec_posted()
        &&& self.spec_detached_error() == other.spec_detached_error()
        &&& self.spec_must_signal(1) == other.spec_must_signal(1)
    }

    /// The first half of [`Ring::stage`], for a caller that writes the WQEs after the slot holds
    /// what keeps their memory alive: the next request's slot gets it all. Then
    /// [`Ring::stage_filled`] stages it, or [`Ring::unfill`] takes it back.
    #[inline(always)]
    pub fn fill(&mut self, wqes: u64, detached: bool, implied: CompletionResult, leases: L)
        requires
            old(self).inv(),
            old(self).fits(wqes),
            implied is Ok,
        ensures
            final(self).is_filled(),
            final(self).same_but_free(old(self)),
            forall|k: u64| #[trigger] final(self).spec_must_signal(k) == old(self).spec_must_signal(k),
            final(self).filled_view() == ReqView::new(detached, leases),
            final(self).filled_end() == old(self).spec_wqe_posted() + wqes,
            final(self).filled_meta() == (RequestMeta {
                first: old(self).spec_wqe_posted(),
                end: (old(self).spec_wqe_posted() + wqes) as u64,
                outcome: implied,
            }),
    {
        proof {
            reveal(Ring::inv_slots_but);
            lemma_or_bounds(self.wqe_posted, self.posted);
        }
        let first_wqe = self.wqe_posted;
        let i = self.index(self.posted);
        let slot = slot_in(&mut self.slots, i);
        slot.end_wqe = first_wqe + wqes;
        slot.first_wqe = Ghost(first_wqe);
        slot.outcome = implied;
        slot.detached = detached;
        slot.general = detached;
        proof {
            let p = old(self).posted as int;
            assert(old(self).free_ok(p));
        }
        let free = replace_value(&mut slot.leases, Some(leases));
        forget_free(free, Ghost(free is None));
        proof {
            let pre = old(self);
            let p = pre.posted as int;
            let n = pre.n();
            assert forall|s: int| pre.head <= s < pre.head + n && s != p implies final(self).slot(s) == pre.slot(s) by {
                lemma_other(n, p, s);
            }
            assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
                assert(pre.done_ok(s));
            }
            assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
                assert(pre.live_ok(s));
            }
            assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != p % self.n()
                implies #[trigger] self.free_ok(s) by {
                assert(pre.free_ok(s));
            }
            reveal(Ring::inv_wqes);
            assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe@
                == self.slot(s - 1).end_wqe by {
                assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
            }
            if self.retired < self.posted {
                assert(self.slot(self.retired as int) == pre.slot(self.retired as int));
            }
            if self.retired < self.committed {
                assert(self.slot(self.committed - 1) == pre.slot(self.committed - 1));
            }
            if self.committed < self.posted {
                assert(self.slot(self.posted - 1) == pre.slot(self.posted - 1));
            }
            if self.head < self.retired {
                assert(self.slot(self.head as int) == pre.slot(self.head as int));
            }
            assert forall|s: int| #[trigger] self.req(s) == pre.req(s) by {
                if pre.head <= s < pre.committed {
                    assert(self.slot(s) == pre.slot(s));
                }
            }
            assert forall|s: int| #[trigger] self.staged(s) == pre.staged(s) by {
                if pre.committed <= s < pre.posted {
                    assert(self.slot(s) == pre.slot(s));
                }
            }
            assert forall|s: int| #[trigger] self.refused(s) == pre.refused(s) by {
                if pre.head <= s < pre.committed {
                    assert(self.slot(s) == pre.slot(s));
                }
            }
            assert forall|s: int, w: int| #[trigger] self.dma(s, w) == pre.dma(s, w) by {
                if pre.retired <= s < pre.posted {
                    assert(self.slot(s) == pre.slot(s));
                }
            }
            assert forall|s: int| #[trigger] self.meta(s) == pre.meta(s) by {
                if pre.head <= s < pre.posted {
                    assert(self.slot(s) == pre.slot(s));
                }
            }
        }
    }

    /// Request `seq`, just staged, takes the general path when it retires.
    #[cold]
    #[inline(never)]
    pub(crate) fn take_general_path(&mut self, seq: Seq)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).same_but_free(old(self)),
    {
        let i = self.index(seq);
        self.slots[i].general = true;
        proof {
            let pre = old(self);
            let p = seq as int;
            let n = pre.n();
            assert(self.slot(p) == Slot { general: true, ..pre.slot(p) });
            lemma_general_kept(pre, final(self), p);
        }
    }

    /// The second half of [`Ring::stage`]: the request of the slot just filled is staged.
    #[inline(always)]
    pub fn stage_filled(&mut self, wqes: u64, signaled: bool) -> (seq: Seq)
        requires
            old(self).is_filled(),
            old(self).filled_end() == old(self).spec_wqe_posted() + wqes,
            signaled || !old(self).spec_must_signal(wqes),
        ensures
            seq == old(self).spec_posted(),
            final(self).inv(),
            final(self).spec_head() == old(self).spec_head(),
            final(self).spec_retired() == old(self).spec_retired(),
            final(self).spec_committed() == old(self).spec_committed(),
            final(self).spec_posted() == seq + 1,
            final(self).spec_wqe_retired() == old(self).spec_wqe_retired(),
            final(self).spec_wqe_committed() == old(self).spec_wqe_committed(),
            final(self).spec_wqe_posted() == old(self).spec_wqe_posted() + wqes,
            final(self).spec_depth() == old(self).spec_depth(),
            final(self).spec_max_wqes() == old(self).spec_max_wqes(),
            final(self).woken() == old(self).woken(),
            final(self).spec_detached_error() == old(self).spec_detached_error(),
            forall|s: int| #[trigger] final(self).req(s) == old(self).req(s),
            forall|s: int| #[trigger] final(self).refused(s) == old(self).refused(s),
            forall|s: int| #[trigger] final(self).staged(s) == if s == seq {
                Some(old(self).filled_view())
            } else {
                old(self).staged(s)
            },
            forall|s: int, w: int| #[trigger] final(self).dma(s, w) == if s == seq
                && old(self).spec_wqe_posted() <= w < old(self).filled_end() {
                Some(old(self).filled_view().leases)
            } else {
                old(self).dma(s, w)
            },
            forall|s: int| #[trigger] final(self).meta(s) == if s == seq {
                Some(old(self).filled_meta())
            } else {
                old(self).meta(s)
            },
    {
        let seq = self.posted;
        let end_wqe = self.wqe_posted + wqes;
        self.posted = seq + 1;
        self.wqe_posted = end_wqe;
        if signaled {
            self.sig_end = end_wqe;
        }
        proof {
            self.lemma_staged(old(self));
        }
        seq
    }

    /// Takes back what [`Ring::fill`] put in the next request's slot: the request was not posted.
    #[cold]
    #[inline(never)]
    pub fn unfill(&mut self) -> (leases: Option<L>)
        requires
            old(self).is_filled(),
        ensures
            leases == Some(old(self).filled_view().leases),
            final(self).inv(),
            final(self).same_but_free(old(self)),
    {
        let i = self.index(self.posted);
        let leases = self.slots[i].leases.take();
        proof {
            let pre = old(self);
            let p = pre.posted as int;
            let n = pre.n();
            reveal(Ring::inv_slots_but);
            reveal(Ring::inv_wqes);
            assert forall|s: int| pre.head <= s < pre.head + n && s != p implies self.slot(s) == pre.slot(s) by {
                lemma_other(n, p, s);
            }
            assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
                assert(pre.done_ok(s));
            }
            assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
                assert(pre.live_ok(s));
            }
            assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != -1
                implies #[trigger] self.free_ok(s) by {
                if s == p {
                } else {
                    lemma_other(n, p, s);
                    assert(pre.free_ok(s));
                }
            }
            assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe@
                == self.slot(s - 1).end_wqe by {
                assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
            }
            if self.retired < self.posted {
                assert(self.slot(self.retired as int) == pre.slot(self.retired as int));
            }
            if self.retired < self.committed {
                assert(self.slot(self.committed - 1) == pre.slot(self.committed - 1));
            }
            if self.committed < self.posted {
                assert(self.slot(self.posted - 1) == pre.slot(self.posted - 1));
            }
            if self.head < self.retired {
                assert(self.slot(self.head as int) == pre.slot(self.head as int));
            }
            assert forall|s: int| #[trigger] self.req(s) == pre.req(s) by {
                if pre.head <= s < pre.committed {
                    assert(self.slot(s) == pre.slot(s));
                }
            }
            assert forall|s: int| #[trigger] self.staged(s) == pre.staged(s) by {
                if pre.committed <= s < pre.posted {
                    assert(self.slot(s) == pre.slot(s));
                }
            }
            assert forall|s: int| #[trigger] self.refused(s) == pre.refused(s) by {
                if pre.head <= s < pre.committed {
                    assert(self.slot(s) == pre.slot(s));
                }
            }
            assert forall|s: int, w: int| #[trigger] self.dma(s, w) == pre.dma(s, w) by {
                if pre.retired <= s < pre.posted {
                    assert(self.slot(s) == pre.slot(s));
                }
            }
            assert forall|s: int| #[trigger] self.meta(s) == pre.meta(s) by {
                if pre.head <= s < pre.posted {
                    assert(self.slot(s) == pre.slot(s));
                }
            }
        }
        leases
    }

    /// Staging the request of the slot just filled keeps the invariants.
    proof fn lemma_staged(&self, pre: &Self)
        requires
            pre.is_filled(),
            self.slots@ == pre.slots@,
            self.wakers@ == pre.wakers@,
            self.mask == pre.mask,
            self.bits == pre.bits,
            self.head == pre.head,
            self.retired == pre.retired,
            self.committed == pre.committed,
            self.posted == pre.posted + 1,
            self.depth == pre.depth,
            self.max_wqes == pre.max_wqes,
            self.wqe_retired == pre.wqe_retired,
            self.wqe_committed == pre.wqe_committed,
            self.wqe_posted == pre.filled_end(),
            self.sig_end == pre.sig_end || self.sig_end == pre.filled_end(),
            self.sig_end == pre.sig_end ==> !pre.spec_must_signal((pre.filled_end() - pre.wqe_posted) as u64),
            self.committed_sig_end == pre.committed_sig_end,
            self.woken == pre.woken,
        ensures
            self.inv(),
            forall|s: int| #[trigger] self.req(s) == pre.req(s),
            forall|s: int|
                #[trigger] self.staged(s) == if s == pre.posted {
                    Some(pre.filled_view())
                } else {
                    pre.staged(s)
                },
            forall|s: int| #[trigger] self.refused(s) == pre.refused(s),
            forall|s: int, w: int| #[trigger] self.dma(s, w) ==
                if s == pre.posted && pre.wqe_posted <= w < pre.filled_end() {
                    Some(pre.filled_view().leases)
                } else {
                    pre.dma(s, w)
                },
            forall|s: int| #[trigger] self.meta(s) == if s == pre.posted {
                Some(pre.filled_meta())
            } else {
                pre.meta(s)
            },
    {
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        let p = pre.posted as int;
        let n = pre.n();
        assert forall|s: int| #[trigger] self.req(s) == pre.req(s) by {}
        assert forall|s: int| #[trigger] self.refused(s) == pre.refused(s) by {}
        assert forall|s: int|
            #[trigger] self.staged(s) == if s == p {
                Some(pre.filled_view())
            } else {
                pre.staged(s)
            } by {}
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            if s != p {
                assert(pre.live_ok(s));
                if !self.is_void(s) {
                    assert(pre.posted_ok(s));
                }
            }
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != -1
            implies #[trigger] self.free_ok(s) by {
            lemma_other(n, p, s);
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe@
            == self.slot(s - 1).end_wqe by {
            if s != p {
                assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
            } else if pre.committed < pre.posted {
            } else if pre.retired < pre.committed {
            }
        }
    }

    // -------------------------------------------------------------------------
    // Committing and aborting

    /// The doorbell rang: every staged request is in flight.
    #[inline(always)]
    pub fn commit(&mut self)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).spec_head() == old(self).spec_head(),
            final(self).spec_retired() == old(self).spec_retired(),
            final(self).spec_committed() == old(self).spec_posted(),
            final(self).spec_posted() == old(self).spec_posted(),
            forall|s: int|
                #[trigger] final(self).req(s) == if old(self).staged(s) is Some {
                    old(self).staged(s)
                } else {
                    old(self).req(s)
                },
            forall|s: int| #[trigger] final(self).staged(s) is None,
            forall|s: int| #[trigger] final(self).refused(s) == old(self).refused(s),
            final(self).woken() == old(self).woken(),
            final(self).spec_wqe_retired() == old(self).spec_wqe_retired(),
            final(self).spec_wqe_committed() == old(self).spec_wqe_posted(),
            final(self).spec_wqe_posted() == old(self).spec_wqe_posted(),
            final(self).spec_depth() == old(self).spec_depth(),
            final(self).spec_max_wqes() == old(self).spec_max_wqes(),
            final(self).same_dma(old(self)),
            final(self).same_meta(old(self)),
            final(self).spec_detached_error() == old(self).spec_detached_error(),
    {
        self.committed = self.posted;
        self.wqe_committed = self.wqe_posted;
        self.committed_sig_end = self.sig_end;
        proof {
            self.lemma_commit(old(self));
        }
    }

    proof fn lemma_commit(&self, pre: &Self)
        requires
            pre.inv(),
            self.slots@ == pre.slots@,
            self.wakers@ == pre.wakers@,
            self.mask == pre.mask,
            self.bits == pre.bits,
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
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
            if pre.committed <= s {
                assert(pre.posted_ok(s));
            }
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != -1
            implies #[trigger] self.free_ok(s) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| #[trigger] self.refused(s) == pre.refused(s) by {
            if pre.committed <= s < pre.posted {
                assert(pre.live_ok(s));
                assert(pre.posted_ok(s));
            }
        }
        assert forall|s: int|
            #[trigger] self.req(s) == if pre.staged(s) is Some {
                pre.staged(s)
            } else {
                pre.req(s)
            } by {
            if pre.committed <= s < pre.posted {
                assert(pre.live_ok(s));
                assert(pre.posted_ok(s));
            }
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe@
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
        }
        assert forall|s: int| #[trigger] self.meta(s) == pre.meta(s) by {
            if pre.committed <= s < pre.posted {
                assert(pre.live_ok(s));
                assert(pre.posted_ok(s));
            }
        }
    }

    /// The hardware discarded everything since the last commit (the device
    /// refused the doorbell): refuses every staged request. Returns how many staged requests were
    /// refused.
    ///
    /// A refused request keeps its sequence number, which is never reused:
    /// one with an owner retires at once with its refusal, which its owner
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
            final(self).spec_max_wqes() == old(self).spec_max_wqes(),
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

    /// [`Ring::abort`], its wakers' panics kept in `wakes`.
    pub(crate) fn abort_with_wakes(&mut self, wakes: &mut WakeBatch) -> (dropped: u64)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
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
            final(self).spec_max_wqes() == old(self).spec_max_wqes(),
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
    /// becomes the refusal of that request, so that
    /// the loss is reported, not taken for success.
    pub fn abort_keeping(&mut self, keep: Seq) -> (kept: Option<L>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
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
            final(self).spec_max_wqes() == old(self).spec_max_wqes(),
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

    /// [`Ring::abort_keeping`], its wakers' panics kept in `wakes`.
    pub(crate) fn abort_keeping_with_wakes(&mut self, keep: Seq, wakes: &mut WakeBatch) -> (kept: Option<L>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
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
            final(self).spec_max_wqes() == old(self).spec_max_wqes(),
            final(self).spec_detached_error() is None ==> forall|s: int|
                #[trigger] old(self).staged(s) is Some && s != keep ==> !old(self).staged(
                    s,
                )->0.detached,
            final(self).keeps_dma(old(self)),
            forall|s: int| #[trigger] final(self).meta(s) ==
                if old(self).staged(s) is Some { None } else { old(self).meta(s) },
            final(self).detached_refused(old(self)),
    {
        let ghost pre = *self;
        let wqe = self.wqe_committed;
        let mut kept = None;
        let mut seq = self.committed;
        while seq < self.posted
            invariant
                pre.inv(),
                self.same_counters_but_woken(&pre),
                wqe == pre.wqe_committed,
                self.woken() == pre.woken() + pre.wakers(pre.committed as int, seq as int),
                self.detached_error is None ==> forall|s: int|
                    pre.committed <= s < seq && s != keep ==> !(#[trigger] pre.slot(s)).detached,
                self.detached_refused(&pre),
                self.slots@.len() == pre.slots@.len(),
                self.wakers@.len() == pre.wakers@.len(),
                pre.committed <= seq <= pre.posted,
                forall|s: int|
                    pre.head <= s < pre.head + pre.n() ==> #[trigger] self.slot(s) == if pre.committed
                        <= s < seq {
                        refuse(pre.slot(s), s == keep, wqe)
                    } else {
                        pre.slot(s)
                    },
                forall|s: int|
                    pre.head <= s < pre.head + pre.n() ==> #[trigger] self.waker(s) == if pre.committed
                        <= s < seq {
                        None
                    } else {
                        pre.waker(s)
                    },
                kept == if pre.committed <= keep < seq {
                    pre.slot(keep as int).leases
                } else {
                    None
                },
            decreases pre.posted - seq,
        {
            let i = self.index(seq);
            let ghost before = *self;
            proof {
                reveal(Ring::inv_slots_but);
                assert(pre.live_ok(seq as int));
                assert(pre.posted_ok(seq as int));
                assert(before.slot(seq as int) == pre.slot(seq as int));
                assert(before.waker(seq as int) == pre.waker(seq as int));
                if pre.committed <= keep < pre.posted {
                    assert(pre.live_ok(keep as int));
                }
            }
            let waker = self.take_waker(i);
            let slot = &mut self.slots[i];
            slot.first_wqe = Ghost(wqe);
            slot.end_wqe = wqe;
            if seq == keep {
                kept = slot.leases.take();
                slot.outcome = Err(CompletionError::Refused);
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
            let ghost woken = self.woken@;
            self.woken = Ghost(
                match waker_name(waker) {
                    Some(id) => woken.push(id),
                    None => woken,
                },
            );
            proof {
                assert forall|s: int| pre.head <= s < pre.head + pre.n() implies #[trigger] self.slot(s)
                    == if pre.committed <= s < seq + 1 {
                    refuse(pre.slot(s), s == keep, wqe)
                } else {
                    pre.slot(s)
                } by {
                    if s != seq {
                        lemma_other(pre.n(), seq as int, s);
                        assert(self.slot(s) == before.slot(s));
                    }
                }
                assert forall|s: int| pre.head <= s < pre.head + pre.n() implies #[trigger] self.waker(s)
                    == if pre.committed <= s < seq + 1 {
                    None
                } else {
                    pre.waker(s)
                } by {
                    if s != seq {
                        lemma_other(pre.n(), seq as int, s);
                        assert(self.waker(s) == before.waker(s));
                    }
                }
                assert(pre.wakers(pre.committed as int, seq + 1) == match pre.waker(seq as int) {
                    Some(w) => pre.wakers(pre.committed as int, seq as int).push(waker_id(w)),
                    None => pre.wakers(pre.committed as int, seq as int),
                });
                assert(self.woken() =~= pre.woken() + pre.wakers(pre.committed as int, seq + 1));
            }
            if let Some(waker) = waker {
                wakes.wake(waker);
            }
            seq += 1;
        }
        let ghost looped = *self;
        self.refused = self.committed != self.posted || self.refused;
        self.committed = self.posted;
        self.wqe_posted = self.wqe_committed;
        self.sig_end = self.committed_sig_end;
        proof {
            assert forall|s: int| pre.head <= s < pre.head + pre.n() implies #[trigger] self.slot(s)
                == looped.slot(s) by {}
            assert forall|s: int| pre.head <= s < pre.head + pre.n() implies #[trigger] self.waker(s)
                == looped.waker(s) by {}
            self.lemma_abort(&pre, keep as int);
            if pre.committed <= keep < pre.posted {
                reveal(Ring::inv_slots_but);
                assert(pre.live_ok(keep as int));
            }
        }
        let ghost aborted = *self;
        self.skip_refused();
        self.advance_head();
        proof {
            assert(self.same_view(&aborted));
        }
        kept
    }

    proof fn lemma_abort(&self, pre: &Self, keep: int)
        requires
            pre.inv(),
            self.slots@.len() == pre.slots@.len(),
            self.wakers@.len() == pre.wakers@.len(),
            forall|s: int|
                pre.head <= s < pre.head + pre.n() ==> #[trigger] self.slot(s) == if pre.committed
                    <= s < pre.posted {
                    refuse(pre.slot(s), s == keep, pre.wqe_committed)
                } else {
                    pre.slot(s)
                },
            forall|s: int|
                pre.head <= s < pre.head + pre.n() ==> #[trigger] self.waker(s) == if pre.committed
                    <= s < pre.posted {
                    None
                } else {
                    pre.waker(s)
                },
            self.mask == pre.mask,
            self.bits == pre.bits,
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
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != -1
            implies #[trigger] self.free_ok(s) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| #[trigger] self.req(s) == pre.req(s) by {
            if pre.committed <= s < pre.posted {
                assert(pre.live_ok(s));
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
            if pre.committed <= s < pre.posted {
                assert(pre.live_ok(s));
            }
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe@
            == self.slot(s - 1).end_wqe by {
            if s < pre.committed {
                assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
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
            && self.spec_wqe_retired() <= w < self.spec_wqe_posted()
            implies #[trigger] self.dma(s, w) == pre.dma(s, w) by {
            assert(pre.live_ok(s));
            if s >= pre.committed {
                assert(pre.posted_ok(s));
            }
            assert(s < pre.committed);
            assert(self.slot(s) == pre.slot(s));
        }
        assert forall|s: int| #[trigger] self.meta(s) ==
            if pre.staged(s) is Some { None } else { pre.meta(s) } by {
            if pre.committed <= s < pre.posted {
                assert(pre.live_ok(s));
                assert(self.req(s) is None);
                assert(self.staged(s) is None);
            }
        }
    }

    /// Moves `retired` past the requests at the front of the committed ones that have no WQEs:
    /// no completion would retire them.
    fn skip_refused(&mut self)
        requires
            old(self).inv_body(),
        ensures
            final(self).inv_body(),
            final(self).slots@ == old(self).slots@,
            final(self).wakers@ == old(self).wakers@,
            final(self).head == old(self).head,
            final(self).retired >= old(self).retired,
            final(self).committed == old(self).committed,
            final(self).posted == old(self).posted,
            final(self).wqe_retired == old(self).wqe_retired,
            final(self).wqe_committed == old(self).wqe_committed,
            final(self).wqe_posted == old(self).wqe_posted,
            final(self).depth == old(self).depth,
            final(self).max_wqes == old(self).max_wqes,
            final(self).mask == old(self).mask,
            final(self).bits == old(self).bits,
            final(self).detached_error == old(self).detached_error,
            final(self).routed == old(self).routed,
            final(self).sig_end == old(self).sig_end,
            final(self).committed_sig_end == old(self).committed_sig_end,
            final(self).same_view(old(self)),
            forall|s: int| old(self).retired <= s < final(self).retired ==> #[trigger] old(self).is_void(s),
    {
        let ghost pre = *self;
        while self.retired < self.committed && self.slots[self.index(self.retired)].end_wqe == self.wqe_retired
            invariant
                self.inv_body(),
                pre.inv_body(),
                self.slots@ == pre.slots@,
                self.wakers@ == pre.wakers@,
                self.head == pre.head,
                self.retired >= pre.retired,
                self.committed == pre.committed,
                self.posted == pre.posted,
                self.wqe_retired == pre.wqe_retired,
                self.wqe_committed == pre.wqe_committed,
                self.wqe_posted == pre.wqe_posted,
                self.depth == pre.depth,
                self.max_wqes == pre.max_wqes,
                self.mask == pre.mask,
                self.bits == pre.bits,
                self.detached_error == pre.detached_error,
                self.routed == pre.routed,
                self.sig_end == pre.sig_end,
                self.committed_sig_end == pre.committed_sig_end,
                self.same_view(&pre),
                forall|s: int| pre.retired <= s < self.retired ==> #[trigger] pre.is_void(s),
            decreases self.committed - self.retired,
        {
            let ghost before = *self;
            proof {
                reveal(Ring::inv_slots_but);
                reveal(Ring::inv_wqes);
                let r = before.retired as int;
                assert(before.live_ok(r));
                assert(before.is_void(r));
                assert(pre.is_void(r));
            }
            self.retired += 1;
            proof {
                self.lemma_skip(&before);
                self.lemma_retired_moved(&before);
            }
        }
    }

    /// Moving `retired` changes nothing A sees, when it moves past a request A does not see.
    proof fn lemma_retired_moved(&self, pre: &Self)
        requires
            pre.inv_body(),
            pre.retired < pre.committed,
            self.slots@ == pre.slots@,
            self.wakers@ == pre.wakers@,
            self.head == pre.head,
            self.committed == pre.committed,
            self.posted == pre.posted,
            self.woken == pre.woken,
            self.retired == pre.retired + 1,
            pre.is_void(pre.retired as int),
        ensures
            self.same_view(pre),
    {
        let r = pre.retired as int;
        reveal(Ring::inv_slots_but);
        assert(pre.live_ok(r));
        assert forall|s: int| #[trigger] self.req(s) == pre.req(s) by {}
        assert forall|s: int| #[trigger] self.staged(s) == pre.staged(s) by {}
        assert forall|s: int| #[trigger] self.refused(s) == pre.refused(s) by {}
        assert forall|s: int, w: int| #[trigger] self.dma(s, w) == pre.dma(s, w) by {}
        assert forall|s: int| #[trigger] self.meta(s) == pre.meta(s) by {}
    }

    /// Moving `retired` past a refused request keeps the invariants.
    proof fn lemma_skip(&self, pre: &Self)
        requires
            pre.inv_body(),
            pre.retired < pre.committed,
            pre.is_void(pre.retired as int),
            self.slots@ == pre.slots@,
            self.wakers@ == pre.wakers@,
            self.mask == pre.mask,
            self.bits == pre.bits,
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
        ensures
            self.inv_body(),
            pre.waker(pre.retired as int) is None,
            pre.req(pre.retired as int) is None || pre.slot(pre.retired as int).is_refused(),
    {
        reveal(Ring::inv_slots_but);
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
        assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != -1
            implies #[trigger] self.free_ok(s) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe@
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
        }
        if self.retired < self.posted {
            assert(pre.slot(r + 1).first_wqe@ == pre.slot(r).end_wqe);
        }
    }

    // -------------------------------------------------------------------------
    // Completions

    /// The outcome request `s` retires with when WQE `wqe` completes with
    /// `wc`: if it ends at `wqe`, an error of `wc`, at its position, unless it has one already, or on
    /// a receive queue what `wc` received; else the outcome it has: the one implied at posting, or
    /// an error recorded on it before.
    pub closed spec fn retire_outcome(&self, s: int, recv: bool, wqe: u64, wc: Wc) -> CompletionResult {
        let slot = self.slot(s);
        if slot.end_wqe == wqe + 1 {
            if wc.spec_ok() {
                if recv {
                    Ok(Completed::Received(wc.spec_received()))
                } else {
                    slot.outcome
                }
            } else if slot.outcome is Ok {
                Err(wc.spec_failure((wqe - slot.first_wqe@) as u32))
            } else {
                slot.outcome
            }
        } else {
            slot.outcome
        }
    }

    /// What A sees of request `s` once it retired with `outcome` (A's
    /// `complete`): a detached request is gone (its leases released), an
    /// owned one is complete; a refused one A never saw.
    pub closed spec fn retired_view(&self, s: int, outcome: CompletionResult) -> Option<ReqView<L>> {
        if self.req(s) is None || self.slot(s).detached {
            None
        } else {
            Some(self.view_of(s).completed(crate::wc::result_model(outcome)))
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
            match self.waker(to - 1) {
                Some(w) => before.push(waker_id(w)),
                None => before,
            }
        }
    }

    /// The waker of one request.
    proof fn lemma_wakers_one(&self, s: int)
        ensures
            self.wakers(s, s + 1) == match self.waker(s) {
                Some(w) => vstd::seq::Seq::<WakerId>::empty().push(waker_id(w)),
                None => vstd::seq::Seq::<WakerId>::empty(),
            },
    {
        assert(self.wakers(s, s) == vstd::seq::Seq::<WakerId>::empty());
    }

    /// The wakers of `[from, to)` come after those of `[from, mid)`, those of `[mid, to)`.
    proof fn lemma_wakers_split(&self, from: int, mid: int, to: int)
        requires
            from <= mid <= to,
        ensures
            self.wakers(from, to) == self.wakers(from, mid) + self.wakers(mid, to),
        decreases to - mid,
    {
        if mid == to {
            assert(self.wakers(from, mid) + self.wakers(mid, to) =~= self.wakers(from, mid));
        } else {
            self.lemma_wakers_split(from, mid, to - 1);
            assert(self.wakers(from, to) =~= self.wakers(from, mid) + self.wakers(mid, to));
        }
    }

    /// The wakers of `[from, to)` depend on theirs alone.
    proof fn lemma_wakers_same(&self, other: &Self, from: int, to: int)
        requires
            forall|s: int| from <= s < to ==> #[trigger] self.waker(s) == other.waker(s),
        ensures
            self.wakers(from, to) == other.wakers(from, to),
        decreases to - from,
    {
        if from < to {
            self.lemma_wakers_same(other, from, to - 1);
        }
    }

    /// Requests without wakers have none to wake.
    proof fn lemma_wakers_none(&self, from: int, to: int)
        requires
            forall|s: int| from <= s < to ==> #[trigger] self.waker(s) is None,
        ensures
            self.wakers(from, to) == vstd::seq::Seq::<WakerId>::empty(),
        decreases to - from,
    {
        if from < to {
            self.lemma_wakers_none(from, to - 1);
        }
    }

    /// The metadata change of a completion, having retired requests up to `to` and, if `spent`,
    /// done with it: full requests retire, while the next request may record its first error from
    /// a proper WQE prefix.
    pub closed spec fn completion_meta(&self, s: int, recv: bool, wqe: u64, wc: Wc, to: int, spent: bool) -> Option<RequestMeta> {
        if self.retired <= s < to {
            self.completed_meta(s, self.retire_outcome(s, recv, wqe, wc))
        } else if spent && s == to && self.req(s) is Some && self.req(s)->0.phase is InFlight
            && self.meta(s)->0.first <= wqe && wqe as int + 1 < self.meta(s)->0.end
            && !wc.spec_ok() && self.meta(s)->0.outcome is Ok {
            self.recorded_meta(s, wc.spec_failure((wqe - self.meta(s)->0.first) as u32))
        } else {
            self.meta(s)
        }
    }

    /// Public observations of the native cache and a completion's metadata frame.
    pub proof fn lemma_completion_observation(&self, s: int, recv: bool, wqe: u64, wc: Wc, to: int, spent: bool)
        requires
            self.inv(),
            self.spec_retired() <= to <= self.spec_committed(),
        ensures
            self.completion_meta(s, recv, wqe, wc, to, spent) is Some ==> self.meta(s) is Some
                && self.completion_meta(s, recv, wqe, wc, to, spent)->0.first == self.meta(s)->0.first
                && self.completion_meta(s, recv, wqe, wc, to, spent)->0.end == self.meta(s)->0.end,
            s < self.spec_retired() ==> self.completion_meta(s, recv, wqe, wc, to, spent) == self.meta(s),
            self.req(s) is Some && to <= s
                && (!spent || wc.spec_ok() || self.meta(s)->0.outcome is Err
                    || wqe < self.meta(s)->0.first || self.meta(s)->0.end as int <= wqe as int + 1)
                ==> self.completion_meta(s, recv, wqe, wc, to, spent) == self.meta(s),
            self.req(s) is Some ==> self.retire_outcome(s, recv, wqe, wc)
                == if self.meta(s)->0.end as int == wqe as int + 1 {
                    if wc.spec_ok() {
                        if recv {
                            Ok(Completed::Received(wc.spec_received()))
                        } else {
                            self.meta(s)->0.outcome
                        }
                    } else if self.meta(s)->0.outcome is Ok {
                        Err(wc.spec_failure((wqe - self.meta(s)->0.first) as u32))
                    } else {
                        self.meta(s)->0.outcome
                    }
                } else {
                    self.meta(s)->0.outcome
                },
            self.spec_retired() <= s < to && self.req(s) is Some && !self.req(s)->0.detached
                ==> self.completion_meta(s, recv, wqe, wc, to, spent)->0.outcome
                    == self.retire_outcome(s, recv, wqe, wc),
            self.spec_retired() <= s < to && self.req(s) is Some && self.req(s)->0.detached
                ==> self.completion_meta(s, recv, wqe, wc, to, spent) is None,
    {
        self.lemma_meta(s);
        self.lemma_view();
    }

    /// After WQE `wqe` completed with `wc`, each request still in flight records `wc` if it
    /// is an error inside its interval and none was recorded, and keeps what it has otherwise.
    #[verifier::opaque]
    pub open spec fn pending_cache(&self, pre: Self, wqe: u64, wc: Wc) -> bool {
        forall|s: int| #[trigger] self.req(s) is Some && self.req(s)->0.phase is InFlight
            ==> self.meta(s)->0.outcome == if pre.meta(s)->0.outcome is Ok
                && !wc.spec_ok() && pre.meta(s)->0.first <= wqe {
                Err(wc.spec_failure((wqe - pre.meta(s)->0.first) as u32))
            } else {
                pre.meta(s)->0.outcome
            }
    }

    /// [`Ring::pending_cache`] for request `s`.
    pub proof fn lemma_pending_cache_observation(&self, pre: Self, wqe: u64, wc: Wc, s: int)
        requires
            self.pending_cache(pre, wqe, wc),
            self.req(s) is Some,
            self.req(s)->0.phase is InFlight,
        ensures
            self.meta(s)->0.outcome == if pre.meta(s)->0.outcome is Ok
                && !wc.spec_ok() && pre.meta(s)->0.first <= wqe {
                Err(wc.spec_failure((wqe - pre.meta(s)->0.first) as u32))
            } else {
                pre.meta(s)->0.outcome
            },
    {
        reveal(Ring::pending_cache);
    }

    /// Records the completion of WQE `wqe` of a send queue: retires every committed request whose
    /// WQEs all lie at or before it. Wakes the wakers of the requests that retire.
    pub fn complete(&mut self, wqe: u64, wc: Wc) -> (known: bool)
        requires
            old(self).inv(),
        ensures
            final(self).completed_with(old(self), false, wqe, wc, true),
            known == (wqe < old(self).spec_wqe_committed()),
    {
        let mut wakes = WakeBatch::new();
        let result = self.complete_with_wakes(false, wqe, &wc, &mut wakes);
        wakes.finish();
        result
    }

    /// `self` is `pre` after some or, if `spent`, all of what the completion of WQE `wqe` with `wc`
    /// retires.
    pub open spec fn completed_with(&self, pre: &Self, recv: bool, wqe: u64, wc: Wc, spent: bool) -> bool {
        &&& self.inv()
        &&& pre.spec_retired() <= self.spec_retired() <= pre.spec_committed()
        &&& self.spec_committed() == pre.spec_committed()
        &&& self.spec_posted() == pre.spec_posted()
        &&& self.spec_wqe_committed() == pre.spec_wqe_committed()
        &&& self.spec_wqe_posted() == pre.spec_wqe_posted()
        &&& self.spec_depth() == pre.spec_depth()
        &&& self.spec_max_wqes() == pre.spec_max_wqes()
        &&& pre.spec_wqe_retired() <= self.spec_wqe_retired()
        &&& wqe >= pre.spec_wqe_retired() ==> self.spec_wqe_retired() <= wqe + 1
        &&& self.spec_routed() == if spent && pre.spec_wqe_retired() <= wqe < pre.spec_wqe_committed() {
            wqe + 1
        } else {
            pre.spec_routed() as int
        }
        &&& forall|s: int|
            #[trigger] self.req(s) == if pre.spec_retired() <= s < self.spec_retired() {
                pre.retired_view(s, pre.retire_outcome(s, recv, wqe, wc))
            } else {
                pre.req(s)
            }
        &&& forall|s: int| #[trigger] self.staged(s) == pre.staged(s)
        &&& forall|s: int| #[trigger] self.refused(s) == pre.refused(s)
        &&& self.woken() == pre.woken() + pre.wakers(
            pre.spec_retired() as int,
            self.spec_retired() as int,
        )
        &&& self.keeps_dma(pre)
        &&& forall|s: int| #[trigger] self.meta(s)
            == pre.completion_meta(s, recv, wqe, wc, self.spec_retired() as int, spent)
        &&& forall|s: int| pre.spec_retired() <= s < self.spec_retired()
            && #[trigger] pre.req(s) is Some
            ==> pre.meta(s)->0.end as int <= wqe as int + 1
        &&& spent && pre.spec_wqe_retired() <= wqe < pre.spec_wqe_committed() ==> forall|s: int|
            #[trigger] self.req(s) is Some && self.req(s)->0.phase is InFlight
            ==> self.meta(s)->0.end > wqe as int + 1
        &&& spent && pre.spec_wqe_retired() <= wqe < pre.spec_wqe_committed()
            ==> self.pending_cache(*pre, wqe, wc)
        &&& pre.detached_completed(self.spec_detached_error(), recv, wqe, wc,
            self.spec_retired() as int)
    }

    /// A completion that names no WQE in flight changes nothing.
    proof fn lemma_ignored(&self, recv: bool, wqe: u64, wc: Wc)
        requires
            self.inv(),
            wqe >= self.spec_wqe_committed() || wqe < self.spec_wqe_retired(),
        ensures
            self.completed_with(self, recv, wqe, wc, true),
    {
        self.lemma_view();
        assert forall|s: int| #[trigger] self.meta(s)
            == self.completion_meta(s, recv, wqe, wc, self.spec_retired() as int, true) by {
            self.lemma_meta(s);
        }
        assert(self.woken() =~= self.woken() + self.wakers(self.retired as int, self.retired as int));
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
        requires
            old(self).inv(),
        ensures
            final(self).completed_with(old(self), recv, wqe, *wc, true),
            known == (wqe < old(self).spec_wqe_committed()),
    {
        if wqe >= self.wqe_committed {
            proof {
                self.lemma_ignored(recv, wqe, *wc);
            }
            return false;
        }
        if wqe < self.wqe_retired {
            proof {
                self.lemma_ignored(recv, wqe, *wc);
            }
            return true;
        }
        let ghost pre = *self;
        proof {
            pre.lemma_no_steps(recv, wqe, *wc);
        }
        #[allow(clippy::while_let_loop)] // Verus has no `while let`.
        loop
            invariant_except_break
                self.spec_wqe_retired() <= wqe,
                self.walk_left(&pre),
                self.completed_with(&pre, recv, wqe, *wc, false),
            invariant
                pre.inv(),
                pre.spec_wqe_retired() <= wqe < pre.spec_wqe_committed(),
            ensures
                self.completed_with(&pre, recv, wqe, *wc, true),
            decreases pre.spec_committed() - self.spec_retired(),
        {
            let ghost before = *self;
            let step = self.retire_next(recv, wqe, wc, wakes);
            proof {
                self.lemma_walk_step(&before, &pre, recv, wqe, *wc, step);
            }
            match step {
                Some(false) => {},
                _ => break,
            }
        }
        true
    }

    /// What a walk has yet to retire, it has left as it was.
    pub(crate) closed spec fn walk_left(&self, pre: &Self) -> bool {
        &&& self.mask == pre.mask
        &&& self.bits == pre.bits
        &&& self.slots@.len() == pre.slots@.len()
        &&& pre.retired <= self.retired
        &&& pre.head <= self.head
        &&& forall|s: int| self.retired <= s < self.posted ==> #[trigger] self.slot(s) == pre.slot(s)
        &&& forall|s: int| self.retired <= s < self.posted ==> #[trigger] self.waker(s) == pre.waker(s)
    }

    /// A walk starts with nothing retired, in front of the oldest request in flight.
    proof fn lemma_no_steps(&self, recv: bool, wqe: u64, wc: Wc)
        requires
            self.inv(),
            self.wqe_retired <= wqe < self.wqe_committed,
        ensures
            self.walk_left(self),
            self.completed_with(self, recv, wqe, wc, false),
    {
        reveal(Ring::inv_wqes);
        self.lemma_view();
        assert forall|s: int| #[trigger] self.meta(s)
            == self.completion_meta(s, recv, wqe, wc, self.retired as int, false) by {
            self.lemma_meta(s);
        }
        assert(self.woken() =~= self.woken() + self.wakers(self.retired as int, self.retired as int));
    }

    /// One step of a walk, on `before`, is a step more of the walk from `pre`.
    proof fn lemma_walk_step(&self, before: &Self, pre: &Self, recv: bool, wqe: u64, wc: Wc, step: Option<bool>)
        requires
            pre.inv(),
            pre.wqe_retired <= wqe < pre.wqe_committed,
            before.wqe_retired <= wqe,
            before.walk_left(pre),
            before.completed_with(pre, recv, wqe, wc, false),
            self.completed_with(before, recv, wqe, wc, step is None || step->0),
            step is Some ==> self.walk_left(before),
            step is None ==> self.retired == before.retired,
            step is Some ==> before.retired < self.retired,
            step == Some(false) ==> self.wqe_retired <= wqe,
        ensures
            self.completed_with(pre, recv, wqe, wc, step is None || step->0),
            step == Some(false) ==> self.walk_left(pre),
    {
        let spent = step is None || step->0;
        let (r0, r1, r2) = (pre.retired as int, before.retired as int, self.retired as int);
        reveal(Ring::pending_cache);
        pre.lemma_view();
        before.lemma_view();
        self.lemma_view();
        // The requests yet to retire are as they were at the start.
        assert forall|s: int| r1 <= s < pre.posted implies before.slot(s) == pre.slot(s)
            && before.waker(s) == pre.waker(s) by {}
        assert forall|s: int| r1 <= s < pre.posted
            implies #[trigger] before.retire_outcome(s, recv, wqe, wc) == pre.retire_outcome(s, recv, wqe, wc)
                && before.req(s) == pre.req(s)
                && before.retired_view(s, before.retire_outcome(s, recv, wqe, wc))
                    == pre.retired_view(s, pre.retire_outcome(s, recv, wqe, wc)) by {
        }
        assert forall|s: int| #[trigger] self.req(s) == if pre.spec_retired() <= s < self.spec_retired() {
            pre.retired_view(s, pre.retire_outcome(s, recv, wqe, wc))
        } else {
            pre.req(s)
        } by {
        }
        assert forall|s: int| #[trigger] self.meta(s)
            == pre.completion_meta(s, recv, wqe, wc, self.spec_retired() as int, spent) by {
            pre.lemma_meta(s);
            before.lemma_meta(s);
            if r1 <= s {
                before.lemma_completion_observation(s, recv, wqe, wc, r2, spent);
                pre.lemma_completion_observation(s, recv, wqe, wc, r2, spent);
            }
        }
        before.lemma_wakers_same(pre, r1, r2);
        pre.lemma_wakers_split(r0, r1, r2);
        assert(self.woken() =~= pre.woken() + pre.wakers(r0, r2));
        assert forall|s: int| r0 <= s < r2 && #[trigger] pre.req(s) is Some
            implies pre.meta(s)->0.end as int <= wqe as int + 1 by {
            if r1 <= s {
                assert(before.req(s) == pre.req(s));
            }
        }
        if spent {
            assert forall|s: int| #[trigger] self.req(s) is Some && self.req(s)->0.phase is InFlight
                implies self.meta(s)->0.outcome == if pre.meta(s)->0.outcome is Ok
                    && !wc.spec_ok() && pre.meta(s)->0.first <= wqe {
                    Err(wc.spec_failure((wqe - pre.meta(s)->0.first) as u32))
                } else {
                    pre.meta(s)->0.outcome
                } by {
                pre.lemma_meta(s);
                before.lemma_meta(s);
                self.lemma_meta(s);
                assert(pre.req(s) is Some);
                pre.lemma_completion_observation(s, recv, wqe, wc, r2, spent);
                if r2 < s {
                    reveal(Ring::inv_wqes);
                    self.lemma_after(s);
                }
            }
        }
        if !(self.spec_detached_error() == pre.spec_detached_error()) {
            assert(before.spec_detached_error() is None || before.spec_detached_error() == self.spec_detached_error());
        }
    }

    /// Whether WQE `wqe` is in flight: one comparison, since a WQE before the oldest in flight wraps
    /// far ahead.
    #[inline(always)]
    pub(crate) fn wqe_in_flight(&self, wqe: u64) -> (in_flight: bool)
        requires
            self.inv(),
        ensures
            in_flight == (self.spec_wqe_retired() <= wqe < self.spec_wqe_committed()),
    {
        wqe.wrapping_sub(self.wqe_retired) < self.wqe_committed - self.wqe_retired
    }

    /// A request in flight or staged after request `r`, in front of it, comes with WQEs after `r`'s.
    proof fn lemma_after(&self, s: int)
        requires
            self.inv(),
            self.retired < s < self.posted,
        ensures
            self.slot(self.retired as int).end_wqe <= self.slot(s).first_wqe@,
    {
        self.lemma_ordered(self.retired as int, s);
    }

    /// A completion's walk, in front of the oldest request not retired: what the invariants say
    /// of it.
    proof fn lemma_front(&self, wqe: u64)
        requires
            self.inv(),
            self.wqe_retired <= wqe < self.wqe_committed,
        ensures
            self.retired < self.committed,
            self.slot(self.retired as int).first_wqe@ == self.wqe_retired,
            self.live_ok(self.retired as int),
            self.is_void(self.retired as int) ==> self.slot(self.retired as int).end_wqe <= wqe,
            !self.is_void(self.retired as int) ==> self.posted_ok(self.retired as int),
            self.slot(self.retired as int).end_wqe > wqe + 1
                ==> wqe - self.wqe_retired < self.max_wqes,
    {
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        assert(self.live_ok(self.retired as int));
    }

    /// Recording an error on the oldest request in flight keeps the
    /// invariants.
    proof fn lemma_record(&self, pre: &Self)
        requires
            pre.inv(),
            pre.retired < pre.committed,
            !pre.is_void(pre.retired as int),
            self.same_counters(pre),
            self.wakers@ == pre.wakers@,
            self.slots@ == pre.slots@.update(
                (pre.retired as int) % pre.n(),
                Slot { outcome: self.slot(pre.retired as int).outcome, ..pre.slot(pre.retired as int) },
            ),
            !self.slot(pre.retired as int).is_refused(),
        ensures
            self.inv(),
            forall|s: int| #[trigger] self.req(s) == pre.req(s),
            forall|s: int| #[trigger] self.staged(s) == pre.staged(s),
            forall|s: int| #[trigger] self.refused(s) == pre.refused(s),
            self.same_dma(pre),
            forall|s: int| #[trigger] self.meta(s) == if s == pre.retired {
                pre.recorded_or_kept(s, self.slot(s).outcome)
            } else {
                pre.meta(s)
            },
            forall|s: int| pre.retired < s < pre.posted ==> #[trigger] self.slot(s) == pre.slot(s),
    {
        reveal(Ring::inv_slots_but);
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
        assert(pre.live_ok(r));
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != -1
            implies #[trigger] self.free_ok(s) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe@
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
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
        assert forall|s: int, w: int| #[trigger] self.dma(s, w) == pre.dma(s, w) by {
            if pre.retired <= s < pre.posted && s != r {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.meta(s) == if s == pre.retired {
            pre.recorded_or_kept(s, self.slot(s).outcome)
        } else {
            pre.meta(s)
        } by {
            if pre.head <= s < pre.posted && s != r {
                assert(self.slot(s) == pre.slot(s));
            }
        }
    }

    /// The metadata of request `s` with `outcome`.
    closed spec fn recorded_or_kept(&self, s: int, outcome: CompletionResult) -> Option<RequestMeta> {
        Some(RequestMeta { outcome, ..self.meta(s)->0 })
    }

    /// The step of a completion's walk: on the oldest request not retired, whose WQEs start at
    /// or before WQE `wqe`, a WQE in flight. Retires it, if it ends at or before `wqe`; returns its
    /// slot and whether it ends exactly there, so that the completion is spent. Otherwise WQE `wqe`
    /// lies inside it: an error is recorded on it, if it is its first, and this returns `None`.
    #[inline(always)]
    pub(crate) fn retire_next(&mut self, recv: bool, wqe: u64, wc: &Wc, wakes: &mut WakeBatch) -> (r: Option<bool>)
        requires
            old(self).inv(),
            old(self).spec_wqe_retired() <= wqe < old(self).spec_wqe_committed(),
        ensures
            final(self).completed_with(old(self), recv, wqe, *wc, r is None || r->0),
            r is Some ==> final(self).walk_left(old(self)),
            r is None ==> final(self).spec_retired() == old(self).spec_retired()
                && final(self).spec_wqe_retired() == old(self).spec_wqe_retired()
                && final(self).spec_head() == old(self).spec_head(),
            r is Some ==> old(self).spec_retired() < final(self).spec_retired(),
            r == Some(true) ==> final(self).spec_wqe_retired() == wqe + 1,
            r == Some(false) ==> final(self).spec_wqe_retired() <= wqe,
    {
        let ghost pre = *self;
        proof {
            pre.lemma_front(wqe);
            reveal(Ring::inv_slots_but);
            reveal(Ring::inv_wqes);
        }
        let seq = self.retired;
        let i = self.index(seq);
        let first = self.wqe_retired;
        let slot = slot_in(&mut self.slots, i);
        let end = slot.end_wqe;
        let last = wqe + 1;
        if end > last {
            if !wc.is_ok() && slot.outcome.is_ok() {
                slot.outcome = Err(wc.failure((wqe - first) as u32));
            }
            self.routed = Ghost((wqe + 1) as nat);
            proof {
                self.lemma_recorded(&pre, recv, wqe, *wc);
            }
            return None;
        }
        let ended = end == last;
        if ended {
            if wc.is_ok() {
                // A receive has one WQE, so nothing was recorded on it before.
                if recv {
                    slot.outcome = Ok(Completed::Received(wc.received()));
                }
            } else if slot.outcome.is_ok() {
                slot.outcome = Err(wc.failure((wqe - first) as u32));
            }
        }
        self.retired = seq + 1;
        self.wqe_retired = end;
        self.routed = Ghost(if ended { (wqe + 1) as nat } else { pre.routed@ });
        if !self.wakers.is_empty() {
            let waker = self.wakers[i].take();
            let ghost woken = self.woken@;
            self.woken = Ghost(
                match waker_name(waker) {
                    Some(id) => woken.push(id),
                    None => woken,
                },
            );
            if let Some(waker) = waker {
                wakes.wake(waker);
            }
        }
        proof {
            pre.lemma_wakers_one(seq as int);
            assert(self.woken@ =~= pre.woken() + pre.wakers(seq as int, seq + 1));
        }
        let ghost mut m = pre;
        if slot.detached || slot.outcome.is_err() {
            Self::retired_cold(slot, seq, first, &mut self.failed, &mut self.detached_error);
            proof {
                m = *self;
                self.lemma_retire(&pre, recv, wqe, *wc);
            }
            // A request without an owner leaves its slot free as it retires.
            self.advance_head();
        } else {
            proof {
                m = *self;
                self.lemma_retire(&pre, recv, wqe, *wc);
            }
        }
        let ghost headed = *self;
        // Refused requests after the last WQE in flight: no completion would retire them.
        if self.refused && end == self.wqe_committed {
            self.skip_refused();
            self.advance_head();
        }
        proof {
            assert forall|s: int| m.retired <= s < self.retired implies #[trigger] m.is_void(s) by {
                assert(headed.is_void(s));
            }
            self.lemma_retired_next(&pre, &m, recv, wqe, *wc, ended);
        }
        Some(ended)
    }

    /// The common case of [`Ring::retire_next`] for the event loop's path, all in one test: a
    /// success on the oldest request, which ends at or before WQE `wqe`, has an owner nobody waits
    /// on, and frees the oldest slot in use, with no refused request around. Otherwise this
    /// changes nothing and the general path takes it. Returns the request's slot, for the caller
    /// to empty.
    #[inline(always)]
    pub(crate) fn retire_oldest(&mut self, recv: bool, wqe: u64, wc: &Wc) -> (r: Option<&mut Slot<L>>)
        requires
            old(self).inv(),
            old(self).spec_wqe_retired() <= wqe < old(self).spec_wqe_committed(),
        ensures
            r is None ==> *final(self) == *old(self),
            r matches Some(slot) ==> {
                let s = old(self).spec_retired() as int;
                &&& wc.spec_ok()
                &&& old(self).req(s) is Some
                &&& old(self).req(s)->0.phase is InFlight
                &&& !old(self).req(s)->0.detached
                &&& old(self).meta(s)->0.end as int <= wqe as int + 1
                &&& slot.end_wqe == old(self).meta(s)->0.end
                &&& slot.outcome == old(self).retire_outcome(s, recv, wqe, *wc)
                &&& slot.leases == Some(old(self).req(s)->0.leases)
                &&& *final(slot) == slot.emptied() ==> final(self).retired_and_taken(old(self))
                &&& *final(slot) == slot.emptied() ==> final(self).spec_routed()
                    == if slot.end_wqe == wqe + 1 { wqe + 1 } else { old(self).spec_routed() as int }
            },
    {
        let ghost pre = *self;
        let seq = self.retired;
        let i = self.index(seq);
        proof {
            pre.lemma_front(wqe);
        }
        let front = slot_of(&self.slots, i);
        let end = front.end_wqe;
        let last = wqe + 1;
        if end <= last && wc.is_ok() && !front.general && front.outcome.is_ok()
            && self.head == seq && !self.refused && self.wakers.is_empty()
        {
            let slot = slot_in(&mut self.slots, i);
            if recv && end == last {
                slot.outcome = Ok(Completed::Received(wc.received()));
            }
            self.retired = seq + 1;
            self.head = seq + 1;
            self.wqe_retired = end;
            self.routed = Ghost(if end == last { (wqe + 1) as nat } else { pre.routed@ });
            proof {
                pre.lemma_oldest(*final(slot), recv, wqe, *wc);
            }
            Some(slot)
        } else {
            None
        }
    }

    /// What a step of a completion's walk leaves, the requests it retired perhaps gone already:
    /// each request still there, retired or not, is what that step makes of it.
    pub open spec fn walked(&self, pre: &Self, recv: bool, wqe: u64, wc: Wc, spent: bool) -> bool {
        &&& self.inv()
        &&& pre.spec_retired() <= self.spec_retired() <= pre.spec_committed()
        &&& self.spec_committed() == pre.spec_committed()
        &&& self.spec_posted() == pre.spec_posted()
        &&& self.spec_wqe_committed() == pre.spec_wqe_committed()
        &&& self.spec_wqe_posted() == pre.spec_wqe_posted()
        &&& self.spec_depth() == pre.spec_depth()
        &&& self.spec_max_wqes() == pre.spec_max_wqes()
        &&& pre.spec_wqe_retired() <= self.spec_wqe_retired() <= wqe + 1
        &&& self.spec_routed() == if spent { wqe + 1 } else { pre.spec_routed() as int }
        &&& forall|s: int| #[trigger] self.req(s) is Some ==> self.req(s)
            == if pre.spec_retired() <= s < self.spec_retired() {
                pre.retired_view(s, pre.retire_outcome(s, recv, wqe, wc))
            } else {
                pre.req(s)
            }
        &&& forall|s: int| #[trigger] self.staged(s) == pre.staged(s)
        &&& forall|s: int| #[trigger] self.refused(s) == pre.refused(s)
        &&& self.keeps_dma(pre)
        &&& forall|s: int| #[trigger] self.meta(s) is Some ==> self.meta(s)
            == pre.completion_meta(s, recv, wqe, wc, self.spec_retired() as int, spent)
        &&& forall|s: int| pre.spec_retired() <= s < self.spec_retired()
            && #[trigger] pre.req(s) is Some
            ==> pre.meta(s)->0.end as int <= wqe as int + 1
        &&& spent ==> forall|s: int|
            #[trigger] self.req(s) is Some && self.req(s)->0.phase is InFlight
            ==> self.meta(s)->0.end > wqe as int + 1
        &&& spent ==> self.pending_cache(*pre, wqe, wc)
        &&& pre.detached_completed(self.spec_detached_error(), recv, wqe, wc,
            self.spec_retired() as int)
    }

    /// A completion's walk, retired requests still there, is a walk.
    pub proof fn lemma_completed_walked(&self, pre: &Self, recv: bool, wqe: u64, wc: Wc, spent: bool)
        requires
            self.completed_with(pre, recv, wqe, wc, spent),
            pre.spec_wqe_retired() <= wqe < pre.spec_wqe_committed(),
        ensures
            self.walked(pre, recv, wqe, wc, spent),
    {
    }

    /// The oldest request retired and taken is a step of a walk.
    pub proof fn lemma_oldest_walked(&self, pre: &Self, recv: bool, wqe: u64, wc: Wc, spent: bool)
        requires
            self.retired_and_taken(pre),
            pre.inv(),
            pre.spec_wqe_retired() <= wqe < pre.spec_wqe_committed(),
            wc.spec_ok(),
            pre.req(pre.spec_retired() as int) is Some,
            !pre.req(pre.spec_retired() as int)->0.detached,
            pre.meta(pre.spec_retired() as int)->0.end as int <= wqe as int + 1,
            spent == (pre.meta(pre.spec_retired() as int)->0.end as int == wqe as int + 1),
            self.spec_routed() == if spent { wqe + 1 } else { pre.spec_routed() as int },
        ensures
            self.walked(pre, recv, wqe, wc, spent),
    {
        reveal(Ring::pending_cache);
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        let r = pre.retired as int;
        let to = self.retired as int;
        pre.lemma_view();
        self.lemma_view();
        pre.lemma_front(wqe);
        assert forall|s: int| #[trigger] self.meta(s) is Some implies self.meta(s)
            == pre.completion_meta(s, recv, wqe, wc, to, spent) by {
            pre.lemma_meta(s);
            pre.lemma_completion_observation(s, recv, wqe, wc, to, spent);
        }
        assert forall|s: int| r <= s < to && #[trigger] pre.req(s) is Some
            implies pre.meta(s)->0.end as int <= wqe as int + 1 by {
        }
        if spent {
            assert forall|s: int| #[trigger] self.req(s) is Some && self.req(s)->0.phase is InFlight
                implies self.meta(s)->0.end > wqe as int + 1 by {
                self.lemma_meta(s);
            }
        }
        assert forall|s: int| #[trigger] self.req(s) is Some && self.req(s)->0.phase is InFlight
            implies self.meta(s)->0.outcome == if pre.meta(s)->0.outcome is Ok
                && !wc.spec_ok() && pre.meta(s)->0.first <= wqe {
                Err(wc.spec_failure((wqe - pre.meta(s)->0.first) as u32))
            } else {
                pre.meta(s)->0.outcome
            } by {
        }
    }

    /// `self` is `pre` with its oldest request in flight retired and taken.
    pub closed spec fn retired_and_taken(&self, pre: &Self) -> bool {
        let r = pre.retired as int;
        &&& self.inv()
        &&& self.spec_retired() == pre.retired + 1
        &&& self.spec_head() == pre.retired + 1
        &&& self.spec_committed() == pre.committed
        &&& self.spec_posted() == pre.posted
        &&& self.spec_wqe_retired() == pre.slot(r).end_wqe
        &&& self.spec_wqe_committed() == pre.wqe_committed
        &&& self.spec_wqe_posted() == pre.wqe_posted
        &&& self.spec_depth() == pre.depth
        &&& self.spec_max_wqes() == pre.max_wqes
        &&& self.spec_detached_error() == pre.detached_error
        &&& self.woken() == pre.woken()
        &&& forall|s: int| #[trigger] self.req(s) == if s == r { None } else { pre.req(s) }
        &&& forall|s: int| #[trigger] self.staged(s) == pre.staged(s)
        &&& forall|s: int| #[trigger] self.refused(s) == pre.refused(s)
        &&& forall|s: int| #[trigger] self.meta(s) == if s == r { None } else { pre.meta(s) }
        &&& self.keeps_dma(pre)
    }

    /// What [`Ring::retired_and_taken`] says, for the proofs of its callers.
    pub proof fn lemma_retired_and_taken(&self, pre: &Self)
        requires
            self.retired_and_taken(pre),
        ensures
            self.inv(),
            self.spec_retired() == pre.spec_retired() + 1,
            self.spec_head() == pre.spec_retired() + 1,
            self.spec_committed() == pre.spec_committed(),
            self.spec_posted() == pre.spec_posted(),
            self.spec_wqe_committed() == pre.spec_wqe_committed(),
            self.spec_wqe_posted() == pre.spec_wqe_posted(),
            self.spec_depth() == pre.spec_depth(),
            self.spec_max_wqes() == pre.spec_max_wqes(),
            self.spec_detached_error() == pre.spec_detached_error(),
            self.woken() == pre.woken(),
            forall|s: int| #[trigger] self.req(s) == if s == pre.spec_retired() { None } else { pre.req(s) },
            forall|s: int| #[trigger] self.staged(s) == pre.staged(s),
            forall|s: int| #[trigger] self.refused(s) == pre.refused(s),
            forall|s: int| #[trigger] self.meta(s) == if s == pre.spec_retired() { None } else { pre.meta(s) },
            self.keeps_dma(pre),
            pre.meta(pre.spec_retired() as int) is Some ==> self.spec_wqe_retired() == pre.meta(pre.spec_retired() as int)->0.end,
    {
    }

    /// What [`Ring::retire_oldest`] does, with the slot emptied by its caller, keeps the invariants.
    proof fn lemma_oldest(&self, f: Slot<L>, recv: bool, wqe: u64, wc: Wc)
        requires
            self.inv(),
            self.wqe_retired <= wqe < self.wqe_committed,
            self.wakers@.len() == 0,
            self.head == self.retired,
            self.slot(self.retired as int).end_wqe <= wqe + 1,
            wc.spec_ok(),
            !self.slot(self.retired as int).general,
            self.slot(self.retired as int).outcome is Ok,
        ensures
            self.req(self.retired as int) is Some,
            self.req(self.retired as int)->0.phase is InFlight,
            !self.req(self.retired as int)->0.detached,
            self.meta(self.retired as int)->0.end == self.slot(self.retired as int).end_wqe,
            self.slot(self.retired as int).leases == Some(self.req(self.retired as int)->0.leases),
            forall|post: Self|
                f == (Slot { outcome: self.retire_outcome(self.retired as int, recv, wqe, wc), leases: None, ..self.slot(self.retired as int) })
                && post.slots@ == self.slots@.update(self.retired as int % self.n(), f)
                && post.head == self.retired + 1
                && post.retired == self.retired + 1
                && post.wqe_retired == self.slot(self.retired as int).end_wqe
                && post.same_but_front(self)
                ==> #[trigger] post.retired_and_taken(self),
    {
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        let r = self.retired as int;
        let n = self.n();
        self.lemma_front(wqe);
        assert(!self.is_void(r));
        assert forall|post: Self|
            f == (Slot { outcome: self.retire_outcome(r, recv, wqe, wc), leases: None, ..self.slot(r) })
            && post.slots@ == self.slots@.update(r % n, f)
            && post.head == self.retired + 1
            && post.retired == self.retired + 1
            && post.wqe_retired == self.slot(r).end_wqe
            && post.same_but_front(self)
            implies #[trigger] post.retired_and_taken(self) by {
            assert(post.slot(r) == f);
            assert forall|s: int| self.head <= s < self.head + n && s != r implies post.slot(s) == self.slot(s) by {
                lemma_other(n, r, s);
            }
            assert forall|s: int| post.head <= s < post.retired implies #[trigger] post.done_ok(s) by {}
            assert forall|s: int| post.retired <= s < post.posted implies #[trigger] post.live_ok(s) by {
                assert(self.live_ok(s));
                self.lemma_ordered(r, s);
            }
            assert forall|s: int| post.posted <= s < post.head + post.n() && s % post.n() != -1
                implies #[trigger] post.free_ok(s) by {
                if s < r + n {
                    assert(self.free_ok(s));
                } else {
                    assert((r + n) % n == r % n) by {
                        vstd::arithmetic::div_mod::lemma_mod_add_multiples_vanish(r, n);
                    }
                }
            }
            assert forall|s: int| post.retired < s < post.posted implies #[trigger] post.slot(s).first_wqe@
                == post.slot(s - 1).end_wqe by {
                assert(self.slot(s).first_wqe@ == self.slot(s - 1).end_wqe);
            }
            if post.retired < post.posted {
                assert(self.slot(r + 1).first_wqe@ == self.slot(r).end_wqe);
            }
            if post.retired < post.committed {
                assert(self.slot(post.committed - 1) == post.slot(post.committed - 1));
            }
            if post.committed < post.posted {
                assert(self.slot(post.posted - 1) == post.slot(post.posted - 1));
            }
            assert forall|s: int| #[trigger] post.req(s) == if s == r { None } else { self.req(s) } by {
                if self.head <= s < self.committed && s != r {
                    assert(post.slot(s) == self.slot(s));
                }
            }
            assert forall|s: int| #[trigger] post.staged(s) == self.staged(s) by {
                if self.committed <= s < self.posted {
                    assert(post.slot(s) == self.slot(s));
                }
            }
            assert forall|s: int| #[trigger] post.refused(s) == self.refused(s) by {
                if self.head <= s < self.committed && s != r {
                    assert(post.slot(s) == self.slot(s));
                }
            }
            assert forall|s: int| #[trigger] post.meta(s) == if s == r { None } else { self.meta(s) } by {
                if self.head <= s < self.posted && s != r {
                    assert(post.slot(s) == self.slot(s));
                }
            }
            assert forall|s: int, w: int| self.dma(s, w) is Some
                && post.spec_wqe_retired() <= w < post.spec_wqe_posted()
                implies #[trigger] post.dma(s, w) == self.dma(s, w) by {
                if s != r {
                    assert(post.slot(s) == self.slot(s));
                }
            }
        }
    }

    /// `self` and `pre` agree on what the front of the window does not change.
    closed spec fn same_but_front(&self, pre: &Self) -> bool {
        &&& self.mask == pre.mask
        &&& self.bits == pre.bits
        &&& self.committed == pre.committed
        &&& self.posted == pre.posted
        &&& self.depth == pre.depth
        &&& self.max_wqes == pre.max_wqes
        &&& self.wqe_committed == pre.wqe_committed
        &&& self.wqe_posted == pre.wqe_posted
        &&& self.sig_end == pre.sig_end
        &&& self.committed_sig_end == pre.committed_sig_end
        &&& self.wakers@ == pre.wakers@
        &&& self.woken == pre.woken
        &&& self.detached_error == pre.detached_error
    }

    /// What [`Ring::retire_next`] does to a slot beyond the common case: it failed, or it has no
    /// owner.
    #[cold]
    #[inline(never)]
    #[allow(unused_variables)] // `first` and `end` are ghost.
    fn retired_cold(slot: &mut Slot<L>, seq: Seq, first: u64, failed: &mut bool, detached_error: &mut Option<DetachedRecord>)
        ensures
            *final(slot) == retired_slot_of(*old(slot), old(slot).outcome),
            *final(detached_error) == if old(slot).detached && old(slot).leases is Some && old(slot).outcome is Err && *old(detached_error) is None {
                Some(DetachedRecord { seq, error: old(slot).outcome->Err_0, first_wqe: Ghost(first), end_wqe: Ghost(old(slot).end_wqe) })
            } else {
                *old(detached_error)
            },
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
        requires
            old(self).wakers@.len() == 0 || old(self).wakers@.len() == old(self).n(),
            i < old(self).n(),
        ensures
            waker == if old(self).wakers@.len() == 0 { None } else { old(self).wakers@[i as int] },
            final(self).wakers@.len() == old(self).wakers@.len(),
            final(self).wakers@ == if old(self).wakers@.len() == 0 {
                old(self).wakers@
            } else {
                old(self).wakers@.update(i as int, None)
            },
            final(self).slots == old(self).slots,
            final(self).same_counters(old(self)),
            final(self).detached_error == old(self).detached_error,
            final(self).refused == old(self).refused,
            final(self).failed == old(self).failed,
            final(self).routed == old(self).routed,
    {
        if self.wakers.is_empty() {
            None
        } else {
            self.wakers[i].take()
        }
    }

    /// What [`Ring::retire_next`] does when WQE `wqe` lies inside the oldest request.
    proof fn lemma_recorded(&self, pre: &Self, recv: bool, wqe: u64, wc: Wc)
        requires
            pre.inv(),
            pre.wqe_retired <= wqe < pre.wqe_committed,
            pre.slot(pre.retired as int).end_wqe > wqe + 1,
            self.same_counters(pre),
            self.wakers@ == pre.wakers@,
            self.detached_error == pre.detached_error,
            self.routed@ == wqe + 1,
            self.slots@ == pre.slots@.update(
                (pre.retired as int) % pre.n(),
                Slot {
                    outcome: if !wc.spec_ok() && pre.slot(pre.retired as int).outcome is Ok {
                        Err(wc.spec_failure((wqe - pre.wqe_retired) as u32))
                    } else {
                        pre.slot(pre.retired as int).outcome
                    },
                    ..pre.slot(pre.retired as int)
                },
            ),
        ensures
            self.completed_with(pre, recv, wqe, wc, true),
    {
        let r = pre.retired as int;
        pre.lemma_front(wqe);
        pre.lemma_view();
        assert(!pre.is_void(r));
        wc.lemma_failure((wqe - pre.wqe_retired) as u32);
        self.lemma_record(pre);
        assert(self.woken() =~= pre.woken() + pre.wakers(r, r));
        assert forall|s: int| #[trigger] self.meta(s)
            == pre.completion_meta(s, recv, wqe, wc, r, true) by {
            pre.lemma_meta(s);
        }
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        assert forall|s: int| #[trigger] self.req(s) is Some && self.req(s)->0.phase is InFlight
            implies self.meta(s)->0.end > wqe as int + 1 by {
            self.lemma_meta(s);
            if r < s {
                pre.lemma_ordered(r, s);
                assert(pre.live_ok(s));
                assert(self.slot(s) == pre.slot(s));
            }
        }
        reveal(Ring::pending_cache);
        assert forall|s: int| #[trigger] self.req(s) is Some && self.req(s)->0.phase is InFlight
            implies self.meta(s)->0.outcome == if pre.meta(s)->0.outcome is Ok
                && !wc.spec_ok() && pre.meta(s)->0.first <= wqe {
                Err(wc.spec_failure((wqe - pre.meta(s)->0.first) as u32))
            } else {
                pre.meta(s)->0.outcome
            } by {
            pre.lemma_meta(s);
            if r < s {
                pre.lemma_ordered(r, s);
                assert(pre.live_ok(s));
            }
        }
    }

    /// A request's retirement, with what it left in its slot and its waker gone, keeps the
    /// invariants but the head's.
    proof fn lemma_retire(&self, pre: &Self, recv: bool, wqe: u64, wc: Wc)
        requires
            pre.inv(),
            pre.wqe_retired <= wqe < pre.wqe_committed,
            pre.slot(pre.retired as int).end_wqe <= wqe + 1,
            self.slots@ == pre.slots@.update(
                (pre.retired as int) % pre.n(),
                retired_slot_of(pre.slot(pre.retired as int), pre.retire_outcome(pre.retired as int, recv, wqe, wc)),
            ),
            self.wakers@.len() == pre.wakers@.len(),
            self.wakers@ == if pre.wakers@.len() == 0 {
                pre.wakers@
            } else {
                pre.wakers@.update((pre.retired as int) % pre.n(), None)
            },
            self.mask == pre.mask,
            self.bits == pre.bits,
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
        ensures
            self.inv_body(),
            self.slot(pre.retired as int).leases is Some ==> self.head_ok(),
            forall|s: int|
                #[trigger] self.req(s) == if s == pre.retired {
                    pre.retired_view(s, pre.retire_outcome(s, recv, wqe, wc))
                } else {
                    pre.req(s)
                },
            forall|s: int| #[trigger] self.staged(s) == pre.staged(s),
            forall|s: int| #[trigger] self.refused(s) == pre.refused(s),
            forall|s: int|
                pre.head <= s < pre.head + pre.n() && s != pre.retired ==> #[trigger] self.slot(s)
                    == pre.slot(s),
            forall|s: int|
                pre.head <= s < pre.head + pre.n() && s != pre.retired ==> #[trigger] self.waker(s)
                    == pre.waker(s),
            pre.req(pre.retired as int) is Some ==> pre.meta(pre.retired as int)->0.end as int <= wqe as int + 1,
            self.keeps_dma(pre),
            forall|s: int| #[trigger] self.meta(s) == if s == pre.retired {
                pre.completed_meta(s, pre.retire_outcome(s, recv, wqe, wc))
            } else {
                pre.meta(s)
            },
    {
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        let r = pre.retired as int;
        let n = pre.n();
        let outcome = pre.retire_outcome(r, recv, wqe, wc);
        assert(pre.live_ok(r));
        pre.lemma_front(wqe);
        wc.lemma_failure((wqe - pre.slot(r).first_wqe@) as u32);
        assert(self.slot(r) == retired_slot_of(pre.slot(r), outcome));
        assert forall|s: int| pre.head <= s < pre.head + n && s != r implies self.slot(s) == pre.slot(
            s,
        ) && self.waker(s) == pre.waker(s) by {
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
        assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != -1
            implies #[trigger] self.free_ok(s) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe@
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
        }
        if self.retired < self.posted {
            assert(pre.slot(r + 1).first_wqe@ == pre.slot(r).end_wqe);
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
        assert forall|s: int, w: int| pre.dma(s, w) is Some
            && self.spec_wqe_retired() <= w < self.spec_wqe_posted()
            implies #[trigger] self.dma(s, w) == pre.dma(s, w) by {
            if s != r {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.meta(s) == if s == pre.retired {
            pre.completed_meta(s, outcome)
        } else {
            pre.meta(s)
        } by {
            if pre.head <= s < pre.posted && s != r {
                assert(self.slot(s) == pre.slot(s));
            }
        }
    }

    /// One retirement and then the refused requests behind it, if any, are the steps of a walk.
    proof fn lemma_retired_next(&self, pre: &Self, m: &Self, recv: bool, wqe: u64, wc: Wc, ended: bool)
        requires
            pre.inv(),
            pre.wqe_retired <= wqe < pre.wqe_committed,
            pre.slot(pre.retired as int).end_wqe <= wqe + 1,
            ended == (pre.slot(pre.retired as int).end_wqe == wqe + 1),
            m.retired == pre.retired + 1,
            m.wqe_retired == pre.slot(pre.retired as int).end_wqe,
            m.committed == pre.committed,
            m.posted == pre.posted,
            forall|s: int|
                #[trigger] m.req(s) == if s == pre.retired {
                    pre.retired_view(s, pre.retire_outcome(s, recv, wqe, wc))
                } else {
                    pre.req(s)
                },
            forall|s: int| #[trigger] m.staged(s) == pre.staged(s),
            forall|s: int| #[trigger] m.refused(s) == pre.refused(s),
            forall|s: int|
                pre.head <= s < pre.head + pre.n() && s != pre.retired ==> #[trigger] m.slot(s) == pre.slot(s),
            forall|s: int|
                pre.head <= s < pre.head + pre.n() && s != pre.retired ==> #[trigger] m.waker(s) == pre.waker(s),
            m.keeps_dma(pre),
            forall|s: int| #[trigger] m.meta(s) == if s == pre.retired {
                pre.completed_meta(s, pre.retire_outcome(s, recv, wqe, wc))
            } else {
                pre.meta(s)
            },
            m.woken() == pre.woken() + pre.wakers(pre.retired as int, pre.retired + 1),
            m.detached_error == if pre.slot(pre.retired as int).detached && pre.slot(pre.retired as int).leases is Some
                && pre.retire_outcome(pre.retired as int, recv, wqe, wc) is Err && pre.detached_error is None {
                Some(DetachedRecord {
                    seq: pre.retired,
                    error: pre.retire_outcome(pre.retired as int, recv, wqe, wc)->Err_0,
                    first_wqe: Ghost(pre.wqe_retired),
                    end_wqe: Ghost(pre.slot(pre.retired as int).end_wqe),
                })
            } else {
                pre.detached_error
            },
            self.inv(),
            self.same_view(m),
            self.detached_error == m.detached_error,
            self.retired >= m.retired,
            self.committed == m.committed,
            self.posted == m.posted,
            self.wqe_retired == m.wqe_retired,
            self.wqe_committed == pre.wqe_committed,
            self.wqe_posted == pre.wqe_posted,
            self.depth == pre.depth,
            self.max_wqes == pre.max_wqes,
            self.mask == pre.mask,
            self.bits == pre.bits,
            self.slots@ == m.slots@,
            self.wakers@ == m.wakers@,
            self.head >= pre.head,
            self.routed@ == if ended { (wqe + 1) as nat } else { pre.routed@ },
            forall|s: int| m.retired <= s < self.retired ==> #[trigger] m.is_void(s),
        ensures
            self.completed_with(pre, recv, wqe, wc, ended),
            self.walk_left(pre),
            pre.retired < self.retired,
            ended ==> self.wqe_retired == wqe + 1,
            !ended ==> self.wqe_retired <= wqe,
    {
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        reveal(Ring::pending_cache);
        let r = pre.retired as int;
        let to = self.retired as int;
        pre.lemma_view();
        pre.lemma_front(wqe);
        self.lemma_view();
        // The requests walked past since the first have nothing A sees, and no waker.
        assert forall|s: int| r < s < to implies #[trigger] pre.is_void(s) && pre.waker(s) is None by {
            assert(m.is_void(s));
            assert(m.slot(s) == pre.slot(s));
            assert(pre.live_ok(s));
        }
        assert forall|s: int| #[trigger] self.req(s) == if pre.retired <= s < self.retired {
            pre.retired_view(s, pre.retire_outcome(s, recv, wqe, wc))
        } else {
            pre.req(s)
        } by {}
        assert forall|s: int| #[trigger] self.meta(s)
            == pre.completion_meta(s, recv, wqe, wc, to, ended) by {
            pre.lemma_meta(s);
            if s == to && ended && pre.req(s) is Some && pre.req(s)->0.phase is InFlight {
                pre.lemma_ordered(r, s);
            }
        }
        assert forall|s: int| r + 1 <= s < to implies #[trigger] pre.waker(s) is None by {
            assert(pre.is_void(s));
        }
        pre.lemma_wakers_split(r, r + 1, to);
        pre.lemma_wakers_none(r + 1, to);
        assert(pre.wakers(r, to) =~= pre.wakers(r, r + 1));
        assert forall|s: int| r <= s < to && #[trigger] pre.req(s) is Some
            implies pre.meta(s)->0.end as int <= wqe as int + 1 by {
            if s > r {
                assert(pre.is_void(s));
            }
        }
        if ended {
            assert forall|s: int| #[trigger] self.req(s) is Some && self.req(s)->0.phase is InFlight
                implies self.meta(s)->0.end > wqe as int + 1 by {
                self.lemma_meta(s);
            }
            assert forall|s: int| #[trigger] self.req(s) is Some && self.req(s)->0.phase is InFlight
                implies self.meta(s)->0.outcome == if pre.meta(s)->0.outcome is Ok
                    && !wc.spec_ok() && pre.meta(s)->0.first <= wqe {
                    Err(wc.spec_failure((wqe - pre.meta(s)->0.first) as u32))
                } else {
                    pre.meta(s)->0.outcome
                } by {
                pre.lemma_meta(s);
                pre.lemma_ordered(r, s);
            }
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.slot(s) == pre.slot(s) by {
            assert(m.slot(s) == pre.slot(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.waker(s) == pre.waker(s) by {
            assert(m.waker(s) == pre.waker(s));
        }
        wc.lemma_failure((wqe - pre.slot(r).first_wqe@) as u32);
        assert(pre.detached_completed(self.spec_detached_error(), recv, wqe, wc, to));
        assert(self.keeps_dma(pre));
        assert(self.woken() == pre.woken() + pre.wakers(r, to));
    }

    /// Moves `head` past the free slots at the front of the window.
    #[inline(never)]
    fn advance_head(&mut self)
        requires
            old(self).inv_body(),
        ensures
            final(self).inv(),
            final(self).slots@ == old(self).slots@,
            final(self).wakers@ == old(self).wakers@,
            final(self).head >= old(self).head,
            final(self).retired == old(self).retired,
            final(self).committed == old(self).committed,
            final(self).posted == old(self).posted,
            final(self).wqe_retired == old(self).wqe_retired,
            final(self).wqe_committed == old(self).wqe_committed,
            final(self).wqe_posted == old(self).wqe_posted,
            final(self).depth == old(self).depth,
            final(self).max_wqes == old(self).max_wqes,
            final(self).mask == old(self).mask,
            final(self).bits == old(self).bits,
            final(self).sig_end == old(self).sig_end,
            final(self).committed_sig_end == old(self).committed_sig_end,
            final(self).detached_error == old(self).detached_error,
            final(self).routed == old(self).routed,
            final(self).same_view(old(self)),
    {
        let ghost pre = *self;
        while self.head < self.retired && self.slots[self.index(self.head)].leases.is_none()
            invariant
                self.inv_body(),
                pre.inv_body(),
                self.slots@ == pre.slots@,
                self.wakers@ == pre.wakers@,
                self.head >= pre.head,
                self.retired == pre.retired,
                self.committed == pre.committed,
                self.posted == pre.posted,
                self.wqe_retired == pre.wqe_retired,
                self.wqe_committed == pre.wqe_committed,
                self.wqe_posted == pre.wqe_posted,
                self.depth == pre.depth,
                self.max_wqes == pre.max_wqes,
                self.mask == pre.mask,
                self.bits == pre.bits,
                self.sig_end == pre.sig_end,
                self.committed_sig_end == pre.committed_sig_end,
                self.detached_error == pre.detached_error,
                self.routed == pre.routed,
                self.same_view(&pre),
            decreases self.retired - self.head,
        {
            let ghost before = *self;
            self.head += 1;
            proof {
                self.lemma_head(&before);
                self.lemma_head_moved(&before);
            }
        }
        if self.head == self.committed {
            let ghost before = *self;
            self.refused = false;
            proof {
                Self::lemma_frame(&before, self);
            }
        }
    }

    /// Moving `head` past a free slot changes nothing A sees.
    proof fn lemma_head_moved(&self, pre: &Self)
        requires
            pre.head < pre.retired,
            pre.slot(pre.head as int).leases is None,
            self.slots@ == pre.slots@,
            self.wakers@ == pre.wakers@,
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
            pre.slot(pre.head as int).leases is None,
            self.slots@ == pre.slots@,
            self.wakers@ == pre.wakers@,
            self.mask == pre.mask,
            self.bits == pre.bits,
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
        ensures
            self.inv_body(),
    {
        reveal(Ring::inv_slots_but);
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
        assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != -1
            implies #[trigger] self.free_ok(s) by {
            if s < h + n {
                assert(pre.free_ok(s));
            } else {
                assert((h + n) % n == h % n) by {
                    vstd::arithmetic::div_mod::lemma_mod_add_multiples_vanish(h, n);
                }
            }
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe@
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
        }
    }

    // -------------------------------------------------------------------------
    // Owners

    /// Takes the outcome and leases of retired or refused request `seq`, freeing its slot; `None`
    /// unless it is retired or refused and not taken yet.
    #[inline(always)]
    pub fn take(&mut self, seq: Seq) -> (taken: Option<Taken<L>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            match taken {
                Some(t) => old(self).taken_from(seq as int, t),
                None => !(old(self).req(seq as int) is Some && old(self).req(
                    seq as int,
                )->0.phase is Complete) && old(self).refused(seq as int) is None,
            },
            final(self).left_by_take(old(self), seq as int, taken is Some),
    {
        let ghost pre = *self;
        if seq < self.head || seq >= self.committed {
            proof {
                self.lemma_nothing_taken(seq as int);
            }
            return None;
        }
        let i = self.index(seq);
        let slot = slot_in(&mut self.slots, i);
        if seq >= self.retired && !matches!(slot.outcome, Err(CompletionError::Refused)) {
            proof {
                assert(self.slots@ =~= pre.slots@);
                Self::lemma_frame(&pre, self);
                pre.lemma_in_flight_not_taken(seq as int);
            }
            return None;
        }
        // The oldest request, as usual: the head moves past it, with nothing to look for.
        if self.head == seq && self.retired == seq + 1 {
            self.head = seq + 1;
            let leases = slot.leases.take();
            let outcome = slot.outcome;
            proof {
                self.lemma_taken(&pre, seq as int);
            }
            return match leases {
                Some(leases) => Some(Taken { outcome, leases }),
                None => None,
            };
        }
        proof {
            assert(self.slots@ =~= pre.slots@);
            Self::lemma_frame(&pre, self);
        }
        self.take_other(i, Ghost(seq))
    }

    /// What [`Ring::take`] leaves.
    pub open spec fn left_by_take(&self, pre: &Self, seq: int, taken: bool) -> bool {
        &&& forall|s: int| #[trigger] self.req(s) == if taken && s == seq { None } else { pre.req(s) }
        &&& forall|s: int| #[trigger] self.staged(s) == pre.staged(s)
        &&& forall|s: int| #[trigger] self.refused(s) == if s == seq { None } else { pre.refused(s) }
        &&& self.woken() == pre.woken()
        &&& self.same_wqes(pre)
        &&& self.spec_retired() == pre.spec_retired()
        &&& self.spec_committed() == pre.spec_committed()
        &&& self.spec_posted() == pre.spec_posted()
        &&& self.same_dma(pre)
        &&& forall|s: int| #[trigger] self.meta(s) == if taken && s == seq { None } else { pre.meta(s) }
        &&& self.spec_detached_error() == pre.spec_detached_error()
    }

    /// [`Ring::take`] of a request just retired, whose slot nothing else can hold yet: as long as
    /// refused requests may be around, the head looks for the next slot in use.
    #[inline(always)]
    pub(crate) fn take_retired(&mut self, seq: Seq) -> (taken: Option<Taken<L>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            match taken {
                Some(t) => old(self).taken_from(seq as int, t),
                None => !(old(self).req(seq as int) is Some && old(self).req(
                    seq as int,
                )->0.phase is Complete) && old(self).refused(seq as int) is None,
            },
            final(self).left_by_take(old(self), seq as int, taken is Some),
    {
        if self.refused && self.head <= seq && seq < self.retired {
            let i = self.index(seq);
            self.take_other(i, Ghost(seq))
        } else {
            self.take(seq)
        }
    }

    /// [`Ring::take`] of a request after the oldest, or that leaves free ones behind it.
    #[inline(never)]
    fn take_other(&mut self, i: usize, Ghost(seq): Ghost<Seq>) -> (taken: Option<Taken<L>>)
        requires
            old(self).inv(),
            old(self).head <= seq < old(self).committed,
            i == seq as int % old(self).n(),
            seq >= old(self).retired ==> old(self).slot(seq as int).is_refused(),
        ensures
            final(self).inv(),
            match taken {
                Some(t) => old(self).taken_from(seq as int, t),
                None => !(old(self).req(seq as int) is Some && old(self).req(
                    seq as int,
                )->0.phase is Complete) && old(self).refused(seq as int) is None,
            },
            final(self).left_by_take(old(self), seq as int, taken is Some),
    {
        let ghost pre = *self;
        let slot = &mut self.slots[i];
        let leases = slot.leases.take();
        let outcome = slot.outcome;
        proof {
            pre.lemma_result_view(seq as int);
            self.lemma_freed(&pre, seq as int);
        }
        self.advance_head();
        proof {
            assert(self.left_by_take(&pre, seq as int, leases is Some));
        }
        match leases {
            Some(leases) => Some(Taken { outcome, leases }),
            None => None,
        }
    }

    /// What a slot holds of a request retired or refused.
    proof fn lemma_result_view(&self, seq: int)
        requires
            self.inv(),
            self.head <= seq < self.committed,
            seq >= self.retired ==> self.slot(seq).is_refused(),
        ensures
            self.slot(seq).leases is Some && self.slot(seq).is_refused()
                ==> self.req(seq) is None && self.refused(seq) == self.slot(seq).leases,
            self.slot(seq).leases is Some && !self.slot(seq).is_refused() ==> self.req(seq) == Some(
                ReqView { phase: Phase::Complete(crate::wc::result_model(self.slot(seq).outcome)), waker: None, detached: false, leases: self.slot(seq).leases->0 },
            ) && self.meta(seq)->0.outcome == self.slot(seq).outcome,
            self.slot(seq).leases is None ==> self.req(seq) is None && self.refused(seq) is None,
    {
        reveal(Ring::inv_slots_but);
        if seq >= self.retired {
            assert(self.live_ok(seq));
        } else {
            assert(self.done_ok(seq));
        }
    }

    /// Nothing is taken of a request outside `[head, committed)`.
    proof fn lemma_nothing_taken(&self, seq: int)
        requires
            self.inv(),
            seq < self.head || seq >= self.committed,
        ensures
            self.req(seq) is None,
            self.refused(seq) is None,
            self.left_by_take(self, seq, false),
    {
        assert forall|s: int| #[trigger] self.refused(s) == if s == seq { None } else { self.refused(s) } by {}
    }

    /// A request in flight is not taken.
    proof fn lemma_in_flight_not_taken(&self, seq: int)
        requires
            self.inv(),
            self.retired <= seq < self.committed,
            !self.slot(seq).is_refused(),
        ensures
            !(self.req(seq) is Some && self.req(seq)->0.phase is Complete),
            self.refused(seq) is None,
            self.left_by_take(self, seq, false),
    {
        assert forall|s: int| #[trigger] self.refused(s) == if s == seq { None } else { self.refused(s) } by {}
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
        reveal(Ring::inv_slots_but);
        if self.head <= seq < self.retired {
            assert(self.done_ok(seq));
        } else if self.retired <= seq < self.posted {
            assert(self.live_ok(seq));
        }
    }

    /// Emptying the slot of a retired or refused request keeps the
    /// invariants but the head's.
    proof fn lemma_freed(&self, pre: &Self, seq: int)
        requires
            pre.inv(),
            pre.head <= seq < pre.committed,
            seq >= pre.retired ==> pre.slot(seq).is_refused(),
            self.same_counters(pre),
            self.wakers@.len() == pre.wakers@.len(),
            forall|s: int| pre.head <= s < pre.head + pre.n() ==> #[trigger] self.waker(s) == pre.waker(s),
            self.routed == pre.routed,
            self.slots@ == pre.slots@.update(seq % pre.n(), Slot { leases: None, ..pre.slot(seq) }),
            self.detached_error == pre.detached_error,
        ensures
            self.inv_body(),
            forall|s: int|
                #[trigger] self.req(s) == if s == seq && pre.slot(seq).leases is Some {
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
            forall|s: int| #[trigger] self.meta(s) == if s == seq && pre.slot(seq).leases is Some {
                None
            } else {
                pre.meta(s)
            },
            forall|s: int| pre.head <= s < pre.head + pre.n() && s != seq ==> #[trigger] self.slot(s) == pre.slot(s),
    {
        reveal(Ring::inv_slots_but);
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
        assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != -1
            implies #[trigger] self.free_ok(s) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe@
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
        }
        if self.retired < self.posted {
            assert(pre.slot(self.retired as int).first_wqe@ == self.slot(
                self.retired as int,
            ).first_wqe@);
        }
        if self.retired < self.committed {
            assert(pre.slot(self.committed - 1).end_wqe == self.slot(self.committed - 1).end_wqe);
        }
        if self.committed < self.posted {
            assert(pre.slot(self.posted - 1) == self.slot(self.posted - 1));
        }
        assert forall|s: int|
            #[trigger] self.req(s) == if s == seq && pre.slot(seq).leases is Some {
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
        assert forall|s: int, w: int| #[trigger] self.dma(s, w) == pre.dma(s, w) by {
            if pre.retired <= s < pre.posted {
                assert(pre.live_ok(s));
                if s != seq {
                    assert(self.slot(s) == pre.slot(s));
                }
            }
        }
        assert forall|s: int| #[trigger] self.meta(s) == if s == seq && pre.slot(seq).leases is Some {
            None
        } else {
            pre.meta(s)
        } by {
            if pre.head <= s < pre.posted && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
    }

    /// The fast way of [`Ring::take`], on the oldest slot in use, keeps the invariants.
    proof fn lemma_taken(&self, pre: &Self, seq: int)
        requires
            pre.inv(),
            pre.head == seq,
            pre.retired == seq + 1,
            self.head == seq + 1,
            self.retired == pre.retired,
            self.committed == pre.committed,
            self.posted == pre.posted,
            self.mask == pre.mask,
            self.bits == pre.bits,
            self.depth == pre.depth,
            self.max_wqes == pre.max_wqes,
            self.wqe_retired == pre.wqe_retired,
            self.wqe_committed == pre.wqe_committed,
            self.wqe_posted == pre.wqe_posted,
            self.sig_end == pre.sig_end,
            self.committed_sig_end == pre.committed_sig_end,
            self.woken == pre.woken,
            self.routed == pre.routed,
            self.wakers@ == pre.wakers@,
            self.detached_error == pre.detached_error,
            self.slots@ == pre.slots@.update(seq % pre.n(), Slot { leases: None, ..pre.slot(seq) }),
        ensures
            self.inv(),
            pre.slot(seq).leases is Some,
            pre.slot(seq).is_refused() ==> pre.refused(seq) == pre.slot(seq).leases,
            !pre.slot(seq).is_refused() ==> pre.req(seq) == Some(
                ReqView { phase: Phase::Complete(crate::wc::result_model(pre.slot(seq).outcome)), waker: None, detached: false, leases: pre.slot(seq).leases->0 },
            ) && pre.meta(seq)->0.outcome == pre.slot(seq).outcome,
            self.left_by_take(pre, seq, true),
    {
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        let n = pre.n();
        assert(pre.done_ok(seq));
        assert forall|s: int| pre.head <= s < pre.head + n && s != seq implies self.slot(s)
            == pre.slot(s) by {
            lemma_other(n, seq, s);
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != -1
            implies #[trigger] self.free_ok(s) by {
            if s < seq + n {
                assert(pre.free_ok(s));
            } else {
                assert((seq + n) % n == seq % n) by {
                    vstd::arithmetic::div_mod::lemma_mod_add_multiples_vanish(seq, n);
                }
            }
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe@
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
        }
        if self.retired < self.posted {
            assert(pre.slot(self.retired as int) == self.slot(self.retired as int));
        }
        if self.retired < self.committed {
            assert(pre.slot(self.committed - 1) == self.slot(self.committed - 1));
        }
        if self.committed < self.posted {
            assert(pre.slot(self.posted - 1) == self.slot(self.posted - 1));
        }
        assert forall|s: int| #[trigger] self.req(s) == if s == seq { None } else { pre.req(s) } by {
            if pre.head <= s < pre.committed && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.staged(s) == pre.staged(s) by {
            if pre.committed <= s < pre.posted {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.refused(s) == if s == seq { None } else { pre.refused(s) } by {
            if pre.head <= s < pre.committed && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int, w: int| #[trigger] self.dma(s, w) == pre.dma(s, w) by {
            if pre.retired <= s < pre.posted {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.meta(s) == if s == seq { None } else { pre.meta(s) } by {
            if pre.head <= s < pre.posted && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
    }

    /// Gives up ownership of request `seq`: a retired or refused request is
    /// freed and its leases released now, a staged or in-flight one when it
    /// retires. Hands back its waker, whose destructor is the program's: the future it would wake
    /// is gone.
    pub fn detach(&mut self, seq: Seq) -> (waker: Option<Waker>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            forall|s: int|
                #[trigger] final(self).req(s) == if s == seq && old(self).req(s) is Some {
                    if old(self).req(s)->0.phase is Complete {
                        None
                    } else {
                        Some(old(self).req(s)->0.detach())
                    }
                } else {
                    old(self).req(s)
                },
            forall|s: int|
                #[trigger] final(self).staged(s) == if s == seq && old(self).staged(s) is Some {
                    Some(old(self).staged(s)->0.detach())
                } else {
                    old(self).staged(s)
                },
            forall|s: int|
                #[trigger] final(self).refused(s) == if s == seq {
                    None
                } else {
                    old(self).refused(s)
                },
            final(self).woken() == old(self).woken(),
            final(self).same_wqes(old(self)),
            final(self).spec_retired() == old(self).spec_retired(),
            final(self).spec_committed() == old(self).spec_committed(),
            final(self).spec_posted() == old(self).spec_posted(),
            final(self).same_dma(old(self)),
            forall|s: int| #[trigger] final(self).meta(s) == if s == seq && old(self).req(s) is Some && old(self).req(s)->0.phase is Complete {
                None
            } else {
                old(self).meta(s)
            },
            final(self).spec_detached_error() == old(self).spec_detached_error(),
    {
        let ghost pre = *self;
        if seq < self.head || seq >= self.posted {
            proof {
                assert forall|s: int| #[trigger] self.refused(s) == if s == seq { None } else { self.refused(s) } by {}
            }
            return None;
        }
        let i = self.index(seq);
        let waker = self.take_waker(i);
        let retired = seq < self.retired;
        let slot = &mut self.slots[i];
        if retired || (seq < self.committed && matches!(slot.outcome, Err(CompletionError::Refused))) {
            // Releases the leases.
            slot.leases = None;
            proof {
                pre.lemma_done_or_void(seq as int);
                self.lemma_waker_gone(&pre, seq as int);
                self.lemma_freed(&pre, seq as int);
            }
            self.advance_head();
        } else {
            slot.detached = true;
            slot.general = true;
            proof {
                self.lemma_waker_gone(&pre, seq as int);
                self.lemma_detached(&pre, seq as int);
            }
        }
        // The future it would wake is gone. Its destructor is the program's, so the caller runs it.
        waker
    }

    /// With the waker of request `seq` taken, the others' stay.
    proof fn lemma_waker_gone(&self, pre: &Self, seq: int)
        requires
            pre.inv_counters(),
            pre.head <= seq < pre.head + pre.n(),
            0 <= pre.head,
            self.slots@.len() == pre.slots@.len(),
            self.wakers@ == if pre.wakers@.len() == 0 {
                pre.wakers@
            } else {
                pre.wakers@.update(seq % pre.n(), None)
            },
        ensures
            self.wakers@.len() == pre.wakers@.len(),
            self.waker(seq) is None,
            forall|s: int| pre.head <= s < pre.head + pre.n() && s != seq ==> #[trigger] self.waker(s) == pre.waker(s),
            pre.waker(seq) is None ==> forall|s: int| #[trigger] self.waker(s) == pre.waker(s),
    {
        let n = pre.n();
        assert forall|s: int| pre.head <= s < pre.head + n && s != seq implies #[trigger] self.waker(s) == pre.waker(s) by {
            lemma_other(n, seq, s);
        }
        if pre.waker(seq) is None {
            assert(self.wakers@ =~= pre.wakers@);
        }
    }

    /// A retired or refused request holds no waker.
    proof fn lemma_done_or_void(&self, seq: int)
        requires
            self.inv(),
            self.head <= seq < self.posted,
            seq < self.retired || (seq < self.committed && self.slot(seq).is_refused()),
        ensures
            self.waker(seq) is None,
    {
        reveal(Ring::inv_slots_but);
        if seq < self.retired {
            assert(self.done_ok(seq));
        } else {
            assert(self.live_ok(seq));
        }
    }

    /// Giving up the owner of a request in flight or staged, or that was refused and has nothing
    /// left, keeps the invariants.
    proof fn lemma_detached(&self, pre: &Self, seq: int)
        requires
            pre.inv(),
            pre.retired <= seq < pre.posted,
            !(seq < pre.committed && pre.slot(seq).is_refused()),
            self.same_counters(pre),
            self.detached_error == pre.detached_error,
            self.routed == pre.routed,
            self.wakers@.len() == pre.wakers@.len(),
            self.waker(seq) is None,
            forall|s: int| pre.head <= s < pre.head + pre.n() && s != seq ==> #[trigger] self.waker(s) == pre.waker(s),
            self.slots@ == pre.slots@.update(seq % pre.n(), Slot { detached: true, general: true, ..pre.slot(seq) }),
        ensures
            self.inv(),
            forall|s: int|
                #[trigger] self.req(s) == if s == seq && pre.req(s) is Some {
                    Some(pre.req(s)->0.detach())
                } else {
                    pre.req(s)
                },
            forall|s: int|
                #[trigger] self.staged(s) == if s == seq && pre.staged(s) is Some {
                    Some(pre.staged(s)->0.detach())
                } else {
                    pre.staged(s)
                },
            forall|s: int| #[trigger] self.refused(s) == pre.refused(s),
            self.same_dma(pre),
            self.same_meta(pre),
    {
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        let n = pre.n();
        assert forall|s: int| pre.head <= s < pre.head + n && s != seq implies self.slot(s)
            == pre.slot(s) by {
            lemma_other(n, seq, s);
        }
        assert(pre.live_ok(seq));
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != -1
            implies #[trigger] self.free_ok(s) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe@
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
        }
        if self.retired < self.posted {
            assert(pre.slot(self.retired as int).first_wqe@ == self.slot(self.retired as int).first_wqe@);
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
                Some(pre.req(s)->0.detach())
            } else {
                pre.req(s)
            } by {
            if pre.head <= s < pre.committed && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int|
            #[trigger] self.staged(s) == if s == seq && pre.staged(s) is Some {
                Some(pre.staged(s)->0.detach())
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
        assert forall|s: int, w: int| #[trigger] self.dma(s, w) == pre.dma(s, w) by {
            if pre.retired <= s < pre.posted && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.meta(s) == pre.meta(s) by {
            if pre.head <= s < pre.posted && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
    }

    /// Registers `waker` to be woken when request `seq` retires (a staged or
    /// in-flight request with an owner); replaces the one before unless it
    /// would wake the same task. Hands back the one it replaced, whose destructor is the program's.
    pub fn set_waker(&mut self, seq: Seq, waker: &Waker) -> (replaced: Option<Waker>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            forall|s: int|
                #[trigger] final(self).req(s) == if s == seq && old(self).req(s) is Some && old(self).req(s)->0.phase is InFlight && !old(self).req(s)->0.detached {
                    Some(old(self).req(s)->0.with_waker(waker_id(*waker)))
                } else {
                    old(self).req(s)
                },
            forall|s: int|
                #[trigger] final(self).staged(s) == if s == seq && old(self).staged(s) is Some
                    && !old(self).staged(s)->0.detached {
                    Some(old(self).staged(s)->0.with_waker(waker_id(*waker)))
                } else {
                    old(self).staged(s)
                },
            forall|s: int| #[trigger] final(self).refused(s) == old(self).refused(s),
            final(self).woken() == old(self).woken(),
            final(self).same_wqes(old(self)),
            final(self).spec_retired() == old(self).spec_retired(),
            final(self).spec_committed() == old(self).spec_committed(),
            final(self).spec_posted() == old(self).spec_posted(),
            final(self).same_dma(old(self)),
            final(self).same_meta(old(self)),
            final(self).spec_detached_error() == old(self).spec_detached_error(),
    {
        let ghost pre = *self;
        proof {
            self.lemma_state_views(seq as int);
        }
        let state = self.state(seq);
        if !matches!(state, State::Staged) && !matches!(state, State::InFlight) {
            return None;
        }
        let i = self.index(seq);
        if self.slots[i].detached {
            proof {
                pre.lemma_live_slot(seq as int);
            }
            return None;
        }
        if self.wakers.is_empty() {
            let mut wakers = Vec::with_capacity(self.slots.len());
            let mut k = 0;
            while k < self.slots.len()
                invariant
                    wakers@.len() == k,
                    k <= self.slots@.len(),
                    forall|j: int| 0 <= j < k ==> (#[trigger] wakers@[j]) is None,
                decreases self.slots@.len() - k,
            {
                wakers.push(None);
                k += 1;
            }
            self.wakers = wakers;
            proof {
                pre.lemma_wakers_made(self);
            }
        }
        let ghost made = *self;
        let same = match &self.wakers[i] {
            Some(old) => will_wake(old, waker),
            None => false,
        };
        proof {
            pre.lemma_live_slot(seq as int);
        }
        if same {
            proof {
                self.lemma_waker_set(&pre, seq as int, waker_id(*waker));
            }
            None
        } else {
            // Its destructor is the program's, so the caller runs it.
            let replaced = replace_value(&mut self.wakers[i], Some(clone_waker(waker)));
            proof {
                assert forall|s: int| pre.head <= s < pre.head + pre.n() && s != seq
                    implies #[trigger] self.waker(s) == pre.waker(s) by {
                    lemma_other(pre.n(), seq as int, s);
                    assert(made.waker(s) == pre.waker(s));
                }
                self.lemma_waker_set(&pre, seq as int, waker_id(*waker));
            }
            replaced
        }
    }

    /// A request reported `Staged` or `InFlight` is not retired, and holds its leases.
    proof fn lemma_live_slot(&self, seq: int)
        requires
            self.inv(),
            self.state_of(seq) == State::Staged || self.state_of(seq) == State::InFlight,
        ensures
            self.retired <= seq < self.posted,
            self.posted_ok(seq),
    {
        reveal(Ring::inv_slots_but);
        assert(self.live_ok(seq));
    }

    /// An empty list of wakers and one with none are the same.
    proof fn lemma_wakers_made(&self, post: &Self)
        requires
            self.inv(),
            self.wakers@.len() == 0,
            post.wakers@.len() == self.n(),
            forall|j: int| 0 <= j < post.wakers@.len() ==> (#[trigger] post.wakers@[j]) is None,
            post.slots@ == self.slots@,
            post.same_counters(self),
            post.detached_error == self.detached_error,
        ensures
            post.inv(),
            post.same_view(self),
            forall|s: int| 0 <= s ==> #[trigger] post.waker(s) == self.waker(s),
    {
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        assert forall|s: int| #[trigger] post.waker(s) == self.waker(s) by {
            vstd::arithmetic::div_mod::lemma_mod_bound(s, self.n());
        }
        assert forall|s: int| post.head <= s < post.retired implies #[trigger] post.done_ok(s) by {
            assert(self.done_ok(s));
        }
        assert forall|s: int| post.retired <= s < post.posted implies #[trigger] post.live_ok(s) by {
            assert(self.live_ok(s));
        }
        assert forall|s: int| post.posted <= s < post.head + post.n() && s % post.n() != -1
            implies #[trigger] post.free_ok(s) by {
            assert(self.free_ok(s));
        }
        assert forall|s: int| post.retired < s < post.posted implies #[trigger] post.slot(s).first_wqe@
            == post.slot(s - 1).end_wqe by {
            assert(self.slot(s).first_wqe@ == self.slot(s - 1).end_wqe);
        }
    }

    /// Registering a waker on request `seq`, staged or in flight, with an owner, keeps the invariants.
    proof fn lemma_waker_set(&self, pre: &Self, seq: int, id: WakerId)
        requires
            pre.inv(),
            pre.retired <= seq < pre.posted,
            pre.posted_ok(seq),
            !pre.slot(seq).detached,
            self.slots@ == pre.slots@,
            self.same_counters(pre),
            self.detached_error == pre.detached_error,
            self.routed == pre.routed,
            self.wakers@.len() == pre.n(),
            self.waker(seq) is Some,
            waker_id(self.waker(seq)->0) == id,
            forall|s: int| pre.head <= s < pre.head + pre.n() && s != seq ==> #[trigger] self.waker(s) == pre.waker(s),
        ensures
            self.inv(),
            forall|s: int|
                #[trigger] self.req(s) == if s == seq && pre.req(s) is Some && pre.req(s)->0.phase is InFlight && !pre.req(s)->0.detached {
                    Some(pre.req(s)->0.with_waker(id))
                } else {
                    pre.req(s)
                },
            forall|s: int|
                #[trigger] self.staged(s) == if s == seq && pre.staged(s) is Some && !pre.staged(s)->0.detached {
                    Some(pre.staged(s)->0.with_waker(id))
                } else {
                    pre.staged(s)
                },
            forall|s: int| #[trigger] self.refused(s) == pre.refused(s),
            self.same_dma(pre),
            self.same_meta(pre),
    {
        reveal(Ring::inv_slots_but);
        reveal(Ring::inv_wqes);
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != -1
            implies #[trigger] self.free_ok(s) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe@
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
        }
    }

    // -------------------------------------------------------------------------
    // Destruction

    /// The QP is destroyed: the NIC will not touch any memory again, so
    /// every slot releases its leases and the ring is empty. Wakers are
    /// dropped (nobody can wait on a destroyed QP).
    pub fn release_all(&mut self)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            forall|s: int| #[trigger] final(self).req(s) is None,
            forall|s: int| #[trigger] final(self).staged(s) is None,
            forall|s: int| #[trigger] final(self).refused(s) is None,
            final(self).woken() == old(self).woken(),
            final(self).spec_wqe_retired() == old(self).spec_wqe_posted(),
            final(self).spec_wqe_committed() == old(self).spec_wqe_posted(),
            final(self).spec_wqe_posted() == old(self).spec_wqe_posted(),
            final(self).spec_depth() == old(self).spec_depth(),
            final(self).keeps_dma(old(self)),
            forall|s: int| #[trigger] final(self).meta(s) is None,
    {
        let ghost pre = *self;
        let mut i = 0;
        while i < self.slots.len()
            invariant
                pre.inv(),
                self.same_counters(&pre),
                self.slots@.len() == pre.slots@.len(),
                self.wakers@.len() == pre.wakers@.len(),
                self.wakers@.len() == 0 || self.wakers@.len() == self.slots@.len(),
                forall|j: int| 0 <= j < i ==> (#[trigger] self.slots@[j]).leases is None,
                forall|j: int| 0 <= j < i && self.wakers@.len() != 0 ==> (#[trigger] self.wakers@[j]) is None,
            decreases self.slots@.len() - i,
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
        proof {
            reveal(Ring::inv_slots_but);
            reveal(Ring::inv_wqes);
            assert forall|s: int| self.posted <= s < self.head + self.n() && s % self.n() != -1
                implies #[trigger] self.free_ok(s) by {
                vstd::arithmetic::div_mod::lemma_mod_bound(s, self.n());
            }
        }
    }
}

/// Changing whether a slot takes the general path changes nothing else.
proof fn lemma_general_kept<L>(pre: &Ring<L>, post: &Ring<L>, seq: int)
    requires
        pre.inv(),
        post.same_counters(pre),
        post.wakers@ == pre.wakers@,
        post.detached_error == pre.detached_error,
        post.routed == pre.routed,
        post.slots@ == pre.slots@.update(seq % pre.n(), Slot { general: true, ..pre.slot(seq) }),
    ensures
        post.inv(),
        post.same_but_free(pre),
{
    reveal(Ring::inv_slots_but);
    reveal(Ring::inv_wqes);
    let n = pre.n();
    assert forall|s: int| s % n != seq % n implies post.slot(s) == pre.slot(s) by {
        vstd::arithmetic::div_mod::lemma_mod_bound(s, n);
    }
    assert forall|s: int| s % n == seq % n implies post.slot(s) == (Slot { general: true, ..pre.slot(s) }) by {
    }
    assert forall|s: int| post.head <= s < post.retired implies #[trigger] post.done_ok(s) by {
        assert(pre.done_ok(s));
    }
    assert forall|s: int| post.retired <= s < post.posted implies #[trigger] post.live_ok(s) by {
        assert(pre.live_ok(s));
    }
    assert forall|s: int| post.posted <= s < post.head + post.n() && s % post.n() != -1
        implies #[trigger] post.free_ok(s) by {
        assert(pre.free_ok(s));
    }
    assert forall|s: int| post.retired < s < post.posted implies #[trigger] post.slot(s).first_wqe@
        == post.slot(s - 1).end_wqe by {
        assert(pre.slot(s).first_wqe@ == pre.slot(s - 1).end_wqe);
    }
    if post.retired < post.posted {
        assert(pre.slot(post.retired as int).first_wqe@ == post.slot(post.retired as int).first_wqe@);
    }
    if post.retired < post.committed {
        assert(pre.slot(post.committed - 1).end_wqe == post.slot(post.committed - 1).end_wqe);
    }
    if post.committed < post.posted {
        assert(pre.slot(post.posted - 1).end_wqe == post.slot(post.posted - 1).end_wqe);
    }
    if post.head < post.retired {
        assert(pre.slot(post.head as int).leases == post.slot(post.head as int).leases);
    }
}

} // verus!

// The checks of the invariants at run time, outside the proofs: tests use them to
// cross-check the proved invariants on random sequences.
impl<L> Ring<L> {
    /// The bytes of a slot, for the measurements.
    #[doc(hidden)]
    pub const SLOT_BYTES: usize = core::mem::size_of::<Slot<L>>();

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
