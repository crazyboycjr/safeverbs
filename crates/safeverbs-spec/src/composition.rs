//! Finite interleavings of the existing M/A share rules.
//!
//! These are conditional trace lemmas: the caller supplies one coherent sequence of global
//! states and the actual transition at every edge. They lift the one-step worker guarantee
//! to a whole rely interval for an idle observer. The other workers' shares may change
//! between edges, including acquisition of A's fresh group handles. The observer's share
//! and pins stay fixed for the interval; end the interval when either changes.
//!
//! This does not construct that coherent trace from independently created `NicToken`s,
//! split a linear device token, prove buffer permissions, establish `coupled`, or implement
//! `Refines`. In particular, separate existential rely witnesses returned to different
//! workers are not evidence that they describe the same global execution.
//!
//! An A step must be valid in the current global state, not merely in an old local view:
//! foreign cloning can invalidate thaw. Ownership transfer is a change of the ghost share
//! ledger, not a step of A; use `abs_share::lemma_transfer` and start a new interval for
//! either participant. Worker/CQ creation and destruction likewise delimit its lifetime.
//!
//! To obtain a whole-device implementation result, a scheduler invariant must supply the
//! shared trace, pairwise disjoint live shares, well-formed ownership updates and each
//! worker's observation position in that trace. Apply these lemmas to the intervals between
//! its own actions, and compose those intervals with its local simulation steps. Supplying
//! legal edges is an obligation on that simulation, not a proof of their concrete origin.
#![allow(unused_imports)]

use crate::abs::*;
use crate::abs_share;
use crate::nic::*;
use crate::refinement::*;
use crate::share;
use crate::types::*;
use vstd::prelude::*;

verus! {

/// The actual M step, with the acting worker's share or an explicit environment step.
/// Environment includes control-path verbs, whose interference must respect the pins.
pub enum MEvent {
    Worker(share::Share, ISet<Key>, Nic::Step),
    Environment(Nic::Step),
}

impl MEvent {
    pub open spec fn step(self) -> Nic::Step {
        match self {
            MEvent::Worker(_, _, step) => step,
            MEvent::Environment(step) => step,
        }
    }

    /// Classify interference without assuming the conclusion `foreign` for worker verbs.
    pub open spec fn compatible(self, observer: share::Share, pins: ISet<Key>) -> bool {
        match self {
            MEvent::Worker(owner, owner_pins, step) => {
                share::disjoint(owner, observer) && share::mine(owner, owner_pins, step)
            },
            MEvent::Environment(step) => share::foreign(observer, pins, step),
        }
    }
}

/// A coherent interval with no action by the observer. Legal M transitions are local
/// simulation obligations; their endpoints must belong to this same global trace.
pub open spec fn m_interleaving(
    observer: share::Share,
    pins: ISet<Key>,
    run: Seq<Nic::State>,
    events: Seq<MEvent>,
) -> bool {
    &&& run.len() > 0
    &&& events.len() == run.len() - 1
    &&& forall|i: int| 0 <= i < events.len() ==> {
        let event = #[trigger] events[i];
        event.compatible(observer, pins) && nic_step(run[i], run[i + 1], event.step())
    }
}

/// Every finite interleaving of disjoint workers and permitted environment actions is
/// exactly a rely execution of the existing M. No restriction or transition is added to M.
pub proof fn lemma_m_interleaving(
    observer: share::Share,
    pins: ISet<Key>,
    run: Seq<Nic::State>,
    events: Seq<MEvent>,
)
    requires
        m_interleaving(observer, pins, run, events),
        nic_inv(run[0]),
        share::share_wf(run[0], observer),
    ensures
        nic_run(run),
        nic_reaches(run[0], run.last()),
        share::rely_reaches(observer, pins, run[0], run.last()),
        nic_inv(run.last()),
        share::share_wf(run.last(), observer),
        share::framed(observer, pins, run[0], run.last()),
{
    let steps = Seq::new(events.len(), |i: int| events[i].step());
    assert forall|i: int| 0 <= i < steps.len() implies
        share::foreign(observer, pins, #[trigger] steps[i])
            && nic_step(run[i], run[i + 1], steps[i]) by {
        let event = events[i];
        assert(event.compatible(observer, pins));
        match event {
            MEvent::Worker(owner, owner_pins, step) => {
                share::lemma_mine_is_foreign(owner, owner_pins, observer, pins, step);
            },
            MEvent::Environment(_) => {},
        }
    }
    assert(share::rely_run(observer, pins, run, steps));
    assert(share::rely_reaches(observer, pins, run[0], run.last()));
    share::lemma_rely_frame(observer, pins, run[0], run.last());
    assert forall|i: int| 0 <= i < run.len() - 1 implies
        Nic::State::next(#[trigger] run[i], run[i + 1]) by {
        crate::nic::lemma_nic_step_is_next(run[i], run[i + 1], steps[i]);
    }
}

/// The M-side precondition checked in the observer's old view survives the entire
/// interleaving, including foreign control verbs that respect its pins.
pub proof fn lemma_m_interleaving_keeps_enabled(
    observer: share::Share,
    pins: ISet<Key>,
    run: Seq<Nic::State>,
    events: Seq<MEvent>,
    verb: Nic::Step,
)
    requires
        m_interleaving(observer, pins, run, events),
        nic_inv(run[0]),
        share::share_wf(run[0], observer),
        share::mine(observer, pins, verb),
        nic_enabled(run[0], verb),
    ensures
        nic_enabled(run.last(), verb),
{
    lemma_m_interleaving(observer, pins, run, events);
    share::lemma_rely_keeps_enabled(observer, pins, run[0], run.last(), verb);
}

/// An actual A step with its acting owner's current share. This records the proposed
/// group/QP ownership, not concrete byte permissions; readonly byte overlap is unrestricted.
pub struct AEvent {
    pub owner: abs_share::Share,
    pub step: Abs::Step,
}

/// Unlike M enabledness, A enabledness must be established at each actual global edge.
/// Requiring a legal A step here does not derive it from a stale worker snapshot.
pub open spec fn a_interleaving(
    observer: abs_share::Share,
    run: Seq<Abs::State>,
    events: Seq<AEvent>,
) -> bool {
    &&& run.len() > 0
    &&& events.len() == run.len() - 1
    &&& forall|i: int| 0 <= i < events.len() ==> {
        let event = #[trigger] events[i];
        &&& event.owner.disjoint(observer)
        &&& abs_share::mine(event.owner, event.step)
        &&& abs_step(run[i], run[i + 1], event.step)
    }
}

/// All finite foreign-worker A interleavings preserve the idle observer's groups,
/// requests, wakers and outcomes. This theorem deliberately promises no A enabledness.
pub proof fn lemma_a_interleaving(
    observer: abs_share::Share,
    run: Seq<Abs::State>,
    events: Seq<AEvent>,
)
    requires
        a_interleaving(observer, run, events),
        abs_inv(run[0]),
        abs_share::share_wf(run[0], observer),
    ensures
        abs_run(run),
        abs_reaches(run[0], run.last()),
        abs_share::rely_reaches(observer, run[0], run.last()),
        abs_inv(run.last()),
        abs_share::share_wf(run.last(), observer),
        abs_share::framed(observer, run[0], run.last()),
{
    let steps = Seq::new(events.len(), |i: int| events[i].step);
    assert forall|i: int| 0 <= i < steps.len() implies
        abs_share::foreign(observer, #[trigger] steps[i])
            && abs_step(run[i], run[i + 1], steps[i]) by {
        let event = events[i];
        assert(event.owner.disjoint(observer));
        assert(abs_share::mine(event.owner, event.step));
        abs_share::lemma_mine_is_foreign(event.owner, observer, event.step);
    }
    assert(abs_share::rely_run(observer, run, steps));
    assert(abs_share::rely_reaches(observer, run[0], run.last()));
    abs_share::lemma_rely_run(observer, run, steps);
    abs_share::lemma_rely_is_abs_run(observer, run, steps);
}

} // verus!
