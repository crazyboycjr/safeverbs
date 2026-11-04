//! A completion queue for the futures of one thread, and what the two kinds of completion queue
//! have in common.

use core::cell::{RefCell, RefMut};
use std::rc::Rc;

use safeverbs_core::{Endpoint, Engine, Nic, QpKey, Result, Transport};

use crate::Verbs;
use crate::qp::{AsyncQp, QpGuard, State};

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// A completion queue a QP can be created on: a [`LocalCq`](crate::LocalCq) for an event loop, or
/// an [`AsyncCq`] for futures. Which one decides the handle the QP gets.
pub trait Cq: sealed::Sealed + Clone {
    /// The device.
    type Nic: Nic;
    /// What each request carries in its slot.
    #[doc(hidden)]
    type Ctx;
    /// The handle of a connected QP on this CQ.
    type Qp<T: Transport>;
    /// Runs `f` on the engine under the handle.
    #[doc(hidden)]
    fn with_engine<R>(&self, f: impl FnOnce(&mut Engine<Self::Ctx, Self::Nic>) -> R) -> R;
    /// A QP was created on the engine.
    #[doc(hidden)]
    fn adopt<T: Transport>(&self, key: QpKey<T>);
    /// Removes a QP from the engine.
    #[doc(hidden)]
    fn remove<T: Transport>(&self, key: QpKey<T>);
    /// The handle of a connected QP.
    #[doc(hidden)]
    fn into_qp<T: Transport>(self, key: QpKey<T>, endpoint: Endpoint) -> Self::Qp<T>;
}

/// A completion queue for one thread's futures: a CQ and the QPs attached to it,
/// behind a reference-counted `RefCell`. Neither `Send` nor `Sync`; cheap to clone (a handle). Its
/// requests have no `wr_id`: each future is its own request's identity.
///
/// Create QPs on it with [`Pd::create_qp`](crate::Pd::create_qp) or
/// [`QueuePairBuilder::build_on`](crate::QueuePairBuilder::build_on). Every
/// future of every QP drives the shared CQ when polled: one poll completes the finished requests
/// of all its QPs and wakes their futures. This is the thread-per-core pattern: one CQ per
/// thread (a tokio `current_thread` runtime or `LocalSet`, or a plain `block_on`), every QP of the
/// thread on it. A QP built without a CQ
/// ([`QueuePairBuilder::build`](crate::QueuePairBuilder::build)) gets one of its own.
///
/// A thread may own one of these and a [`LocalCq`](crate::LocalCq); a CQ is driven one way, which
/// these types enforce. It lives as long as any handle to it or any of its QPs.
pub struct AsyncCq<N: Nic = Verbs> {
    state: Rc<RefCell<State<N>>>,
}

impl<N: Nic> Clone for AsyncCq<N> {
    fn clone(&self) -> Self {
        AsyncCq {
            state: self.state.clone(),
        }
    }
}

impl<N: Nic> sealed::Sealed for AsyncCq<N> {}

impl<N: Nic> Cq for AsyncCq<N> {
    type Nic = N;
    type Ctx = ();
    type Qp<T: Transport> = AsyncQp<T, N>;

    fn with_engine<R>(&self, f: impl FnOnce(&mut Engine<(), N>) -> R) -> R {
        f(&mut self.state.borrow_mut().engine)
    }

    fn adopt<T: Transport>(&self, key: QpKey<T>) {
        self.state.borrow_mut().adopt(key);
    }

    fn remove<T: Transport>(&self, key: QpKey<T>) {
        let left_behind = self.state.borrow_mut().remove_qp(key);
        drop(left_behind);
    }

    fn into_qp<T: Transport>(self, key: QpKey<T>, endpoint: Endpoint) -> AsyncQp<T, N> {
        AsyncQp::new(QpGuard::new(self.state, key), endpoint)
    }
}

impl<N: Nic> AsyncCq<N> {
    pub(crate) fn new(ctx: &safeverbs_core::Context<N>, entries: u32) -> Result<Self> {
        Ok(AsyncCq {
            state: Rc::new(RefCell::new(State::new(Engine::new(ctx, entries)?))),
        })
    }

    pub(crate) fn from_state(state: Rc<RefCell<State<N>>>) -> Self {
        AsyncCq { state }
    }

    /// Polls the CQ once, completing what finished on any of its QPs and waking the futures
    /// waiting on it; returns the number of completions.
    pub fn poll(&self) -> usize {
        self.state.borrow_mut().engine.progress()
    }

    /// The engine under this CQ, with its QPs by index, for what the handles do not offer. On a
    /// CQ the futures drive, take results by sequence number
    /// ([`Engine::take_send`](safeverbs_core::Engine::take_send)), not with
    /// [`Engine::poll`](safeverbs_core::Engine::poll), which would hand out the futures' results
    /// too.
    ///
    /// # Panics
    ///
    /// If called while the handle's other methods are running (the `RefCell` is borrowed).
    pub fn as_raw(&self) -> RefMut<'_, Engine<(), N>> {
        RefMut::map(self.state.borrow_mut(), |state| &mut state.engine)
    }

    /// Runs `f` on the engine under this CQ, as [`AsyncCq::as_raw`] lends it.
    pub fn with<R>(&self, f: impl FnOnce(&mut Engine<(), N>) -> R) -> R {
        f(&mut self.as_raw())
    }

    /// Whether its QPs are created without the driver's locks (on a thread
    /// domain; not every device has them).
    pub fn is_lock_free(&self) -> bool {
        self.state.borrow().engine.is_lock_free()
    }
}
