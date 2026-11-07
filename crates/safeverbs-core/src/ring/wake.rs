//! Keep user wake panics until a completion batch has restored its invariants.
//!
//! The normal path has no allocation. Panic payloads are retained only when a
//! waker unwinds; they are released after the driver finishes its bookkeeping.
use core::any::Any;
use core::task::Waker;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use vstd::prelude::*;

use crate::trusted::{vec_new, vec_push};

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

/// A stack-local batch of wake failures, shared by all CQEs in one poll. It allocates
/// only once a waker panics.
pub(crate) struct WakeBatch {
    panics: Option<Vec<WakePanic>>,
}

impl WakeBatch {
    #[inline]
    pub(crate) fn new() -> Self
        no_unwind
    {
        Self { panics: None }
    }

    /// Attempt every notification even if an earlier user waker panicked.
    #[inline(never)]
    pub(crate) fn wake(&mut self, waker: Waker)
        no_unwind
    {
        if let Some(panic) = try_wake(waker) {
            let mut panics = match self.panics.take() {
                Some(panics) => panics,
                None => vec_new(),
            };
            vec_push(&mut panics, panic);
            self.panics = Some(panics);
        }
    }

    /// Whether a waker panicked.
    #[inline(always)]
    pub(crate) fn pending(&self) -> bool
        no_unwind
    {
        self.panics.is_some()
    }

    /// Propagate the first wake panic only after the caller is consistent. Without one,
    /// this is a test and nothing else.
    #[inline]
    pub(crate) fn finish(self) {
        if let Some(panics) = self.panics {
            resume_first(panics);
        }
    }
}

/// Resume the first wake panic. The other payloads' destructors run first, here, at this
/// safe boundary.
#[cold]
#[inline(never)]
fn resume_first(mut panics: Vec<WakePanic>) {
    if !panics.is_empty() {
        let first = panics.remove(0);
        {
            // End the other payloads' scope before resuming the first.
            let _others = panics;
        }
        resume(first);
    }
}

} // verus!
