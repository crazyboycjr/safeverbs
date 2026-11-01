//! The completion ring of one work queue: which requests are in flight, what
//! they hold, and when they retire.
//!
//! This module is bookkeeping only: no FFI, no `unsafe`, no closures or
//! iterator chains, so that it can be moved into `verus!` and proved. The
//! queue pair (`crate::qp`) drives it next to the hardware queue.
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
//! A slot moves through these states:
//!
//! ```text
//!           begin / push_wqe / end        commit            complete
//!   Free ───────────────────────▶ Staged ───────▶ InFlight ─────────▶ Retired
//!     ▲                              │ abort                │ detached     │ take / detach
//!     └──────────────────────────────┴──────────────────────┴──────────────┘
//! ```
//!
//! *Staged* requests have their WQEs written but the doorbell not rung;
//! [`Ring::commit`] rings it (all staged requests at once), [`Ring::abort`]
//! discards them. *Retired* requests have their outcome and still hold their
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
//! ([`Slot::implied`]). An error completion is produced for unsignaled WQEs
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
//! Checked by [`Ring::invariant`] after every operation in debug builds:
//!
//! 1. `head <= retired <= committed <= posted` and `posted - head <= slots`:
//!    the occupied slots form the window `[head, posted)`, with the retired
//!    ones in `[head, retired)`, the ones in flight in `[retired,
//!    committed)` and the staged ones in `[committed, posted)`; every slot
//!    outside the window is free and holds nothing.
//! 2. `wqe_retired <= wqe_committed <= wqe_posted` and `wqe_posted -
//!    wqe_retired <= depth`: never more than `depth` WQEs in flight.
//! 3. The requests of `[retired, posted)` cover consecutive WQE ranges
//!    `[first_wqe, end_wqe)` of `1..=max_wqes` WQEs, starting at
//!    `wqe_retired`, the committed ones ending at `wqe_committed`.
//! 4. A slot retires exactly once: only an in-flight slot retires, and
//!    `retired` only grows.
//! 5. Leases are released exactly once, only when their slot retires
//!    (detached), is taken, is detached after retiring, is aborted, or the
//!    QP is destroyed ([`Ring::release_all`]): every staged, in-flight or
//!    retired-but-not-taken slot holds its leases (`Some`), every free slot
//!    holds none.
//! 6. A retired or free slot holds no waker: the waker registered on a slot
//!    is woken when it retires.
//! 7. A detached slot holds no waker, and a retired slot is never detached
//!    (it is freed instead).
//! 8. Outside an open request, the WQEs posted after the last signaled
//!    request (and not retired) number at most `depth - max_wqes`.

use core::task::Waker;

use crate::wc::Wc;

/// A request's sequence number in its queue.
pub type Seq = u64;

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
}

/// One request's entry.
struct Slot<L> {
    /// The state.
    state: State,
    /// The sequence number of the request occupying it (valid unless free).
    seq: Seq,
    /// The WQE sequence number of its first WQE.
    first_wqe: u64,
    /// One past the WQE sequence number of its last WQE.
    end_wqe: u64,
    /// Whether its last WQE asks for a completion.
    signaled: bool,
    /// No owner: release the leases at retirement.
    detached: bool,
    /// The outcome if a later completion retires it without an error of
    /// its own.
    implied: Wc,
    /// An error recorded on it before it retired, then its outcome.
    outcome: Option<Wc>,
    /// What keeps its memory alive while the NIC uses it.
    leases: Option<L>,
    /// Who to wake when it retires.
    waker: Option<Waker>,
}

impl<L> Slot<L> {
    fn free() -> Slot<L> {
        Slot {
            state: State::Free,
            seq: 0,
            first_wqe: 0,
            end_wqe: 0,
            signaled: false,
            detached: false,
            implied: Wc::success(crate::wc::Opcode(0), 0),
            outcome: None,
            leases: None,
            waker: None,
        }
    }
}

/// The ring of slots of one work queue.
///
/// `L` is what a request holds while the NIC may use its memory (the buffer
/// leases); the ring moves it in when the request is posted and out exactly
/// once.
pub struct Ring<L> {
    /// The slots; request `seq` lives in `slots[seq % slots.len()]`.
    slots: Vec<Slot<L>>,
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
    /// One past the last WQE handed out (including an open request's).
    wqe_posted: u64,
    /// One past the last WQE of the last signaled request posted.
    sig_end: u64,
    /// `sig_end` as of the last commit, for [`Ring::abort`].
    committed_sig_end: u64,
    /// Whether a request is being built (between `begin` and `end`).
    open: bool,
    /// WQEs handed out to the open request.
    open_wqes: u64,
    /// Whether an error completion has retired a request.
    failed: bool,
    /// The first error of a detached request, until someone takes it.
    detached_error: Option<Wc>,
}

impl<L> Ring<L> {
    /// A ring of `slots` slots for a queue of `depth` WQEs, whose requests
    /// have at most `max_wqes` WQEs.
    ///
    /// # Panics
    ///
    /// Unless `slots >= 1` and `1 <= max_wqes <= depth`.
    pub fn new(slots: usize, depth: u32, max_wqes: u32) -> Ring<L> {
        assert!(slots >= 1, "a ring needs a slot");
        assert!(
            max_wqes >= 1 && max_wqes <= depth,
            "a request needs 1..=depth WQEs"
        );
        let mut v = Vec::with_capacity(slots);
        let mut i = 0;
        while i < slots {
            v.push(Slot::free());
            i += 1;
        }
        let ring = Ring {
            slots: v,
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
            open: false,
            open_wqes: 0,
            failed: false,
            detached_error: None,
        };
        debug_assert!(ring.invariant());
        ring
    }

    fn index(&self, seq: Seq) -> usize {
        (seq % self.slots.len() as u64) as usize
    }

    /// The queue's depth in WQEs.
    pub fn depth(&self) -> u32 {
        self.depth as u32
    }

    /// The most WQEs of one request.
    pub fn max_wqes(&self) -> u32 {
        self.max_wqes as u32
    }

    /// The next request's sequence number.
    pub fn posted(&self) -> Seq {
        self.posted
    }

    /// The oldest request not retired: every request before it has retired.
    pub fn retired(&self) -> Seq {
        self.retired
    }

    /// The oldest staged request: every request before it is committed.
    pub fn committed(&self) -> Seq {
        self.committed
    }

    /// WQEs posted and not retired.
    pub fn in_flight_wqes(&self) -> u32 {
        (self.wqe_posted - self.wqe_retired) as u32
    }

    /// Whether some request is staged (written but not committed).
    pub fn has_staged(&self) -> bool {
        self.committed < self.posted
    }

    /// Whether a request is open (between [`Ring::begin`] and
    /// [`Ring::end`]).
    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Whether a slot is free for the next request.
    pub fn has_free_slot(&self) -> bool {
        self.posted - self.head < self.slots.len() as u64
    }

    /// Whether a new request of one WQE could be posted now: a free slot and
    /// room for a WQE.
    pub fn can_post(&self) -> bool {
        !self.open && self.has_free_slot() && self.wqe_posted - self.wqe_retired < self.depth
    }

    /// Whether every slot is taken by a retired request whose owner has not
    /// taken it: waiting for completions would not free one.
    pub fn is_clogged(&self) -> bool {
        !self.has_free_slot() && self.head < self.retired
    }

    /// Whether an error completion has retired a request.
    pub fn failed(&self) -> bool {
        self.failed
    }

    /// The first error completion of a detached request, once.
    pub fn take_detached_error(&mut self) -> Option<Wc> {
        self.detached_error.take()
    }

    /// The state of request `seq`'s slot: `Free` once it was taken, freed or
    /// never posted.
    pub fn state(&self, seq: Seq) -> State {
        if seq < self.head || seq >= self.posted {
            return State::Free;
        }
        let slot = &self.slots[self.index(seq)];
        if slot.seq != seq {
            return State::Free;
        }
        slot.state
    }

    /// Opens a request; returns its sequence number, or `None` without a
    /// free slot or while another request is open.
    pub fn begin(&mut self) -> Option<Seq> {
        if self.open || !self.has_free_slot() {
            return None;
        }
        self.open = true;
        self.open_wqes = 0;
        debug_assert!(self.invariant());
        Some(self.posted)
    }

    /// How many more WQEs the open request may take now: limited by the
    /// queue's room and by `max_wqes`.
    pub fn wqe_room(&self) -> u32 {
        if !self.open {
            return 0;
        }
        let room = self.depth - (self.wqe_posted - self.wqe_retired);
        let left = self.max_wqes - self.open_wqes;
        if room < left {
            room as u32
        } else {
            left as u32
        }
    }

    /// WQEs handed out to the open request.
    pub fn open_wqes(&self) -> u32 {
        self.open_wqes as u32
    }

    /// Hands out the next WQE sequence number (the `wr_id`) to the open
    /// request, or `None` without room.
    pub fn push_wqe(&mut self) -> Option<u64> {
        if self.wqe_room() == 0 {
            return None;
        }
        let id = self.wqe_posted;
        self.wqe_posted += 1;
        self.open_wqes += 1;
        debug_assert!(self.invariant());
        Some(id)
    }

    /// Whether the open request must be signaled: with its WQEs, the
    /// unsignaled tail would exceed `depth - max_wqes`.
    pub fn must_signal(&self) -> bool {
        let base = if self.sig_end > self.wqe_retired {
            self.sig_end
        } else {
            self.wqe_retired
        };
        self.wqe_posted - base > self.depth - self.max_wqes
    }

    /// Closes the open request and stages it in its slot with `leases`.
    ///
    /// It is signaled if `want_signal` or [`Ring::must_signal`]; the caller
    /// writes its last WQE with that flag. `implied` is its outcome if a
    /// later completion retires it. A `detached` request has no owner.
    /// Returns its sequence number and whether it is signaled.
    ///
    /// # Panics
    ///
    /// Without an open request of at least one WQE.
    pub fn end(
        &mut self,
        want_signal: bool,
        detached: bool,
        implied: Wc,
        leases: L,
    ) -> (Seq, bool) {
        assert!(
            self.open && self.open_wqes >= 1,
            "no open request with a WQE"
        );
        let signaled = want_signal || self.must_signal();
        let seq = self.posted;
        let i = self.index(seq);
        let slot = &mut self.slots[i];
        slot.state = State::Staged;
        slot.seq = seq;
        slot.first_wqe = self.wqe_posted - self.open_wqes;
        slot.end_wqe = self.wqe_posted;
        slot.signaled = signaled;
        slot.detached = detached;
        slot.implied = implied;
        slot.outcome = None;
        slot.leases = Some(leases);
        slot.waker = None;
        self.posted += 1;
        if signaled {
            self.sig_end = self.wqe_posted;
        }
        self.open = false;
        self.open_wqes = 0;
        debug_assert!(self.invariant());
        (seq, signaled)
    }

    /// The doorbell rang: every staged request is in flight.
    ///
    /// # Panics
    ///
    /// While a request is open.
    pub fn commit(&mut self) {
        assert!(!self.open, "commit with an open request");
        let mut seq = self.committed;
        while seq < self.posted {
            let i = self.index(seq);
            self.slots[i].state = State::InFlight;
            seq += 1;
        }
        self.committed = self.posted;
        self.wqe_committed = self.wqe_posted;
        self.committed_sig_end = self.sig_end;
        debug_assert!(self.invariant());
    }

    /// The hardware discarded everything since the last commit: drops the
    /// open request (if any) and every staged request, releasing their
    /// leases. Returns how many staged requests were dropped.
    pub fn abort(&mut self) -> u64 {
        let dropped = self.posted - self.committed;
        let mut seq = self.committed;
        while seq < self.posted {
            let i = self.index(seq);
            let slot = &mut self.slots[i];
            slot.state = State::Free;
            slot.outcome = None;
            slot.waker = None;
            let leases = slot.leases.take();
            drop(leases);
            seq += 1;
        }
        self.posted = self.committed;
        self.wqe_posted = self.wqe_committed;
        self.sig_end = self.committed_sig_end;
        self.open = false;
        self.open_wqes = 0;
        debug_assert!(self.invariant());
        dropped
    }

    /// Like [`Ring::abort`], but hands back the leases of staged request
    /// `keep` instead of releasing them (`None` if it is not staged).
    pub fn abort_keeping(&mut self, keep: Seq) -> Option<L> {
        let mut kept = None;
        if self.state(keep) == State::Staged {
            let i = self.index(keep);
            kept = self.slots[i].leases.take();
            self.slots[i].state = State::Free;
            self.slots[i].waker = None;
        }
        let mut seq = self.committed;
        while seq < self.posted {
            let i = self.index(seq);
            let slot = &mut self.slots[i];
            slot.state = State::Free;
            slot.outcome = None;
            slot.waker = None;
            let leases = slot.leases.take();
            drop(leases);
            seq += 1;
        }
        self.posted = self.committed;
        self.wqe_posted = self.wqe_committed;
        self.sig_end = self.committed_sig_end;
        self.open = false;
        self.open_wqes = 0;
        debug_assert!(self.invariant());
        kept
    }

    /// Records the completion of WQE `wqe` (its `wr_id`): retires every
    /// committed request whose WQEs all lie at or before it.
    ///
    /// The request ending at `wqe` retires with `wc` (or with an error
    /// recorded on it before), those before it with their implied outcome
    /// (or a recorded error). An error for a WQE inside a request is
    /// recorded on it. A completion for an already retired WQE is ignored.
    /// Returns `false`, and changes nothing, for a WQE that was never
    /// committed.
    pub fn complete(&mut self, wqe: u64, wc: Wc) -> bool {
        if wqe >= self.wqe_committed {
            return false;
        }
        if wqe < self.wqe_retired {
            return true;
        }
        while self.retired < self.committed {
            let i = self.index(self.retired);
            let end = self.slots[i].end_wqe;
            if end <= wqe + 1 {
                let outcome = match self.slots[i].outcome {
                    Some(recorded) => recorded,
                    None => {
                        if end == wqe + 1 {
                            wc
                        } else {
                            self.slots[i].implied
                        }
                    }
                };
                self.retire(i, outcome);
            } else {
                if !wc.is_ok() && self.slots[i].outcome.is_none() {
                    self.slots[i].outcome = Some(wc);
                }
                break;
            }
        }
        self.advance_head();
        debug_assert!(self.invariant());
        true
    }

    /// Retires the oldest in-flight request, in slot `i`, with `outcome`.
    fn retire(&mut self, i: usize, outcome: Wc) {
        debug_assert!(self.slots[i].state == State::InFlight);
        debug_assert!(self.slots[i].seq == self.retired);
        if !outcome.is_ok() {
            self.failed = true;
        }
        self.wqe_retired = self.slots[i].end_wqe;
        self.retired += 1;
        let waker = self.slots[i].waker.take();
        if self.slots[i].detached {
            if !outcome.is_ok() && self.detached_error.is_none() {
                self.detached_error = Some(outcome);
            }
            let slot = &mut self.slots[i];
            slot.state = State::Free;
            slot.outcome = None;
            let leases = slot.leases.take();
            drop(leases);
        } else {
            let slot = &mut self.slots[i];
            slot.state = State::Retired;
            slot.outcome = Some(outcome);
        }
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Moves `head` past the free slots at the front of the window.
    fn advance_head(&mut self) {
        while self.head < self.retired {
            let i = self.index(self.head);
            if self.slots[i].state != State::Free {
                break;
            }
            self.head += 1;
        }
    }

    /// Takes the outcome and leases of retired request `seq`, freeing its
    /// slot; `None` unless it is retired and not taken yet.
    pub fn take(&mut self, seq: Seq) -> Option<(Wc, L)> {
        if self.state(seq) != State::Retired {
            return None;
        }
        let i = self.index(seq);
        let slot = &mut self.slots[i];
        let outcome = slot.outcome.take();
        let leases = slot.leases.take();
        slot.state = State::Free;
        self.advance_head();
        debug_assert!(self.invariant());
        match (outcome, leases) {
            (Some(outcome), Some(leases)) => Some((outcome, leases)),
            _ => None,
        }
    }

    /// Gives up ownership of request `seq`: a retired request is freed and
    /// its leases released now, a staged or in-flight one when it retires.
    pub fn detach(&mut self, seq: Seq) {
        let state = self.state(seq);
        let i = self.index(seq);
        if state == State::Retired {
            let slot = &mut self.slots[i];
            slot.state = State::Free;
            slot.outcome = None;
            let leases = slot.leases.take();
            drop(leases);
            self.advance_head();
        } else if state == State::Staged || state == State::InFlight {
            let slot = &mut self.slots[i];
            slot.detached = true;
            slot.waker = None;
        }
        debug_assert!(self.invariant());
    }

    /// Registers `waker` to be woken when request `seq` retires (a staged or
    /// in-flight request with an owner); replaces the one before unless it
    /// would wake the same task.
    pub fn set_waker(&mut self, seq: Seq, waker: &Waker) {
        let state = self.state(seq);
        if state != State::Staged && state != State::InFlight {
            return;
        }
        let i = self.index(seq);
        let slot = &mut self.slots[i];
        if slot.detached {
            return;
        }
        let same = match &slot.waker {
            Some(old) => old.will_wake(waker),
            None => false,
        };
        if !same {
            slot.waker = Some(waker.clone());
        }
        debug_assert!(self.invariant());
    }

    /// The QP is destroyed: the NIC will not touch any memory again, so
    /// every slot releases its leases and the ring is empty. Wakers are
    /// dropped (nobody can wait on a destroyed QP).
    pub fn release_all(&mut self) {
        let mut i = 0;
        while i < self.slots.len() {
            let slot = &mut self.slots[i];
            slot.state = State::Free;
            slot.outcome = None;
            slot.waker = None;
            let leases = slot.leases.take();
            drop(leases);
            i += 1;
        }
        self.head = self.posted;
        self.retired = self.posted;
        self.committed = self.posted;
        self.wqe_retired = self.wqe_posted;
        self.wqe_committed = self.wqe_posted;
        self.sig_end = self.wqe_posted;
        self.committed_sig_end = self.wqe_posted;
        self.open = false;
        self.open_wqes = 0;
        debug_assert!(self.invariant());
    }

    /// Checks the invariants of the module documentation.
    pub fn invariant(&self) -> bool {
        let n = self.slots.len() as u64;
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
        if !self.open && self.open_wqes != 0 {
            return false;
        }
        if self.open_wqes > self.max_wqes || self.open_wqes > self.wqe_posted - self.wqe_committed {
            return false;
        }
        // The head is advanced past free slots.
        if self.head < self.retired && self.slots[self.index(self.head)].state == State::Free {
            return false;
        }
        // Slots in the window.
        let mut next_wqe = self.wqe_retired;
        let mut seq = self.head;
        while seq < self.posted {
            let slot = &self.slots[self.index(seq)];
            if seq < self.retired {
                // Retired (owned, waiting) or freed.
                if slot.state == State::Free {
                    if slot.leases.is_some() || slot.outcome.is_some() || slot.waker.is_some() {
                        return false;
                    }
                } else {
                    if slot.state != State::Retired || slot.seq != seq || slot.detached {
                        return false;
                    }
                    if slot.leases.is_none() || slot.outcome.is_none() || slot.waker.is_some() {
                        return false;
                    }
                }
            } else {
                let expected = if seq < self.committed {
                    State::InFlight
                } else {
                    State::Staged
                };
                if slot.state != expected || slot.seq != seq || slot.leases.is_none() {
                    return false;
                }
                if slot.detached && slot.waker.is_some() {
                    return false;
                }
                if expected == State::Staged && slot.outcome.is_some() {
                    return false;
                }
                // 3. Consecutive WQE ranges.
                if slot.first_wqe != next_wqe || slot.end_wqe <= slot.first_wqe {
                    return false;
                }
                if slot.end_wqe - slot.first_wqe > self.max_wqes {
                    return false;
                }
                next_wqe = slot.end_wqe;
                if seq + 1 == self.committed && next_wqe != self.wqe_committed {
                    return false;
                }
            }
            seq += 1;
        }
        if self.committed == self.retired && self.wqe_committed != self.wqe_retired {
            return false;
        }
        if next_wqe + self.open_wqes != self.wqe_posted {
            return false;
        }
        // Slots outside the window are free and hold nothing.
        let mut seq = self.posted;
        while seq < self.head + n {
            let slot = &self.slots[self.index(seq)];
            if slot.state != State::Free || slot.leases.is_some() || slot.waker.is_some() {
                return false;
            }
            seq += 1;
        }
        // 8. The unsignaled tail.
        let closed = self.wqe_posted - self.open_wqes;
        if self.sig_end > closed || self.committed_sig_end > self.wqe_committed {
            return false;
        }
        let base = if self.sig_end > self.wqe_retired {
            self.sig_end
        } else {
            self.wqe_retired
        };
        if base > closed || closed - base > self.depth - self.max_wqes {
            return false;
        }
        true
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

    /// Posts and commits a request of `wqes` WQEs; returns its sequence
    /// number, last WQE and whether it is signaled.
    fn post(
        ring: &mut Ring<Token>,
        c: &Rc<Cell<u32>>,
        wqes: u32,
        signal: bool,
        detached: bool,
    ) -> (Seq, u64, bool) {
        ring.begin().unwrap();
        let mut last = 0;
        for _ in 0..wqes {
            last = ring.push_wqe().unwrap();
        }
        let (seq, signaled) = ring.end(signal, detached, ok(), token(c));
        ring.commit();
        (seq, last, signaled)
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
        let (wa, _) = ring.take(a).unwrap();
        assert_eq!(wa, ok());
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
        ring.begin().unwrap();
        assert_eq!(ring.wqe_room(), 4);
        ring.abort();
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
        assert!(ring.take(a).unwrap().0.is_ok());
        // The flushes of b's other WQEs, the last retires it with the error.
        ring.complete(wa + 3, err(Status::WR_FLUSH_ERR));
        assert_eq!(ring.retired(), 1);
        ring.complete(wb, err(Status::WR_FLUSH_ERR));
        let (wc, _) = ring.take(b).unwrap();
        assert_eq!(wc.status, Status::REM_ACCESS_ERR);
        assert!(ring.failed());
        // Late duplicates are ignored, uncommitted WQEs refused.
        assert!(ring.complete(wa, ok()));
        assert!(!ring.complete(wb + 1, ok()));
        assert_eq!(c.get(), 2);
    }

    #[test]
    fn abort_drops_staged_requests() {
        let c = Rc::new(Cell::new(0));
        let mut ring = Ring::new(8, 8, 2);
        post(&mut ring, &c, 1, true, false);
        ring.begin().unwrap();
        ring.push_wqe().unwrap();
        ring.end(false, false, ok(), token(&c));
        ring.begin().unwrap();
        ring.push_wqe().unwrap();
        assert_eq!(ring.abort(), 1);
        assert_eq!(c.get(), 1);
        assert_eq!(ring.posted(), 1);
        assert_eq!(ring.in_flight_wqes(), 1);
        ring.release_all();
        assert_eq!(c.get(), 2);
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
                        if ring.begin().is_some() {
                            let want = 1 + rng.below(max_wqes as u64) as u32;
                            let mut ids = Vec::new();
                            while (ids.len() as u32) < want {
                                match ring.push_wqe() {
                                    Some(id) => ids.push(id),
                                    None => break,
                                }
                            }
                            if ids.is_empty() {
                                ring.abort();
                                staged.clear();
                                owned.retain(|&s| s < ring.posted());
                                continue;
                            }
                            let detached = rng.below(3) == 0;
                            let (seq, signaled) =
                                ring.end(rng.below(2) == 0, detached, ok(), token(&c));
                            made += 1;
                            let last = *ids.last().unwrap();
                            for id in ids {
                                staged.push((id, signaled && id == last));
                            }
                            if !detached {
                                owned.push(seq);
                            }
                        }
                    }
                    2 => {
                        if !ring.is_open() {
                            ring.commit();
                            hw.append(&mut staged);
                        }
                    }
                    3 => {
                        if rng.below(8) == 0 {
                            ring.abort();
                            staged.clear();
                            owned.retain(|&s| s < ring.posted());
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
            if ring.is_open() {
                ring.abort();
            }
            ring.release_all();
            assert_eq!(c.get(), made, "seed {seed}: every lease released once");
        }
    }
}
