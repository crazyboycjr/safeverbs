//! Keep user wake panics until a completion batch has restored its invariants.
//!
//! The normal path has no allocation. Panic payloads are retained only when a
//! waker unwinds; they are released after the driver finishes its bookkeeping.
use core::any::Any;
use core::task::Waker;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use vstd::prelude::*;

/// A panic payload owned by the standard unwinder.
pub(crate) struct WakePanic(Box<dyn Any + Send>);

verus! {

#[cfg(verus_keep_ghost)]
#[verifier::external_type_specification]
#[verifier::external_body]
pub(crate) struct ExWakePanic(WakePanic);

/// Invoke the user waker, retaining any unwind at the standard-library boundary.
#[verifier::external_body]
fn try_wake(waker: Waker) -> Option<WakePanic>
    no_unwind
{
    catch_unwind(AssertUnwindSafe(|| waker.wake())).err().map(WakePanic)
}

/// Resume an existing panic after all internal state has been restored.
#[verifier::external_body]
fn resume(panic: WakePanic)
    ensures false,
{
    resume_unwind(panic.0)
}

/// A stack-local batch of wake failures, shared by all CQEs in one poll.
pub(crate) struct WakeBatch {
    panics: Vec<WakePanic>,
}

impl WakeBatch {
    pub(crate) fn new() -> Self {
        Self { panics: Vec::new() }
    }

    /// Attempt every notification even if an earlier user waker panicked.
    pub(crate) fn wake(&mut self, waker: Waker) {
        if let Some(panic) = try_wake(waker) {
            self.panics.push(panic);
        }
    }

    /// Propagate the first wake panic only after the caller is consistent.
    /// Secondary payload destructors also run at this safe boundary.
    pub(crate) fn finish(mut self) {
        if !self.panics.is_empty() {
            let first = self.panics.remove(0);
            {
                // End the secondary payloads' scope before resuming the first.
                let _secondary = self.panics;
            }
            resume(first);
        }
    }
}

} // verus!
