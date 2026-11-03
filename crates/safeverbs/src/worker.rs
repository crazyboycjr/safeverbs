//! A worker of one thread for the asynchronous API: one CQ shared by the
//! thread's QPs.

use core::cell::RefCell;
use std::rc::Rc;

use safeverbs_core::{Nic, Pd, Result, Worker};

use crate::Verbs;
use crate::qp::State;

/// A worker for one thread: a CQ and the QPs attached to it
/// ([`Worker`](safeverbs_core::Worker)), behind a reference-counted
/// `RefCell`. Neither `Send` nor `Sync`; cheap to clone (a handle). Its
/// requests have no context (`Worker<()>`): each future is its own request's
/// identity.
///
/// Build QPs on it with
/// [`QueuePairBuilder::build_on`](crate::QueuePairBuilder::build_on). Every
/// future of every QP of the worker drives the shared CQ when polled: one
/// poll retires the completed requests of all the worker's QPs and wakes
/// their futures. This is the thread-per-core pattern: one worker per
/// thread (a tokio `current_thread` runtime or `LocalSet`, or a plain
/// `block_on`), every QP of the thread on it. A QP built without a worker
/// ([`QueuePairBuilder::build`](crate::QueuePairBuilder::build)) gets one of
/// its own.
///
/// The worker lives as long as any handle to it or any of its QPs.
pub struct LocalWorker<N: Nic = Verbs> {
    state: Rc<RefCell<State<N>>>,
}

impl<N: Nic> Clone for LocalWorker<N> {
    fn clone(&self) -> Self {
        LocalWorker {
            state: self.state.clone(),
        }
    }
}

impl<N: Nic> LocalWorker<N> {
    /// A worker of `pd` with a CQ of `cq_size` entries: the QPs built on
    /// it may hold at most that many WQEs in all their queues together.
    pub fn new(pd: &Pd<N>, cq_size: u32) -> Result<Self> {
        Ok(LocalWorker {
            state: Rc::new(RefCell::new(State::new(Worker::new(pd, cq_size)?))),
        })
    }

    pub(crate) fn from_state(state: Rc<RefCell<State<N>>>) -> Self {
        LocalWorker { state }
    }

    pub(crate) fn state(&self) -> &Rc<RefCell<State<N>>> {
        &self.state
    }

    /// Polls the CQ once, retiring what completed on any QP of the worker
    /// and waking the futures waiting on it; returns the number of
    /// completions.
    pub fn poll(&self) -> usize {
        self.state.borrow_mut().worker.progress()
    }

    /// Runs `f` on the synchronous worker. On a worker the futures drive,
    /// take results by sequence number
    /// ([`Worker::take_send`](safeverbs_core::Worker::take_send)), not with
    /// [`Worker::poll`](safeverbs_core::Worker::poll), which would hand out
    /// the futures' results too.
    ///
    /// # Panics
    ///
    /// If called from inside a method of one of its QPs (the `RefCell` is
    /// borrowed).
    pub fn with<R>(&self, f: impl FnOnce(&mut Worker<(), N>) -> R) -> R {
        f(&mut self.state.borrow_mut().worker)
    }

    /// Whether its QPs are created without the driver's locks (on a thread
    /// domain; not every device has them).
    pub fn is_lock_free(&self) -> bool {
        self.state.borrow().worker.is_lock_free()
    }
}
