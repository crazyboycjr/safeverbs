//! Devices and protection domains, with the completion queues and queue pairs they make.

use core::ops::Deref;

use safeverbs_core::{Nic, QpCaps, Result, Transport};

use crate::Verbs;
use crate::builder::{PreparedQp, QueuePairBuilder};
use crate::cq::{AsyncCq, Cq};
use crate::local::LocalCq;

/// An open RDMA device. Cheap to clone. Everything else of
/// [`safeverbs_core::Context`] (its name, ports and GIDs) is reachable through it.
pub struct Context<N: Nic = Verbs> {
    inner: safeverbs_core::Context<N>,
}

impl<N: Nic> Clone for Context<N> {
    fn clone(&self) -> Self {
        Context {
            inner: self.inner.clone(),
        }
    }
}

impl<N: Nic> Deref for Context<N> {
    type Target = safeverbs_core::Context<N>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Context<Verbs> {
    /// Opens the RDMA device called `name`, or the first one if `None`.
    pub fn open(name: Option<&str>) -> Result<Context<Verbs>> {
        Ok(Context {
            inner: safeverbs_core::Context::open(name)?,
        })
    }
}

impl<N: Nic> Context<N> {
    /// Wraps an open device.
    pub fn new(nic: N) -> Context<N> {
        Context {
            inner: safeverbs_core::Context::new(nic),
        }
    }

    /// Allocates a protection domain.
    pub fn alloc_pd(&self) -> Result<Pd<N>> {
        Ok(Pd {
            inner: self.inner.alloc_pd()?,
        })
    }

    /// A completion queue of `entries` entries for this thread's event loop, with values of the
    /// type `W` as `wr_id`s: the QPs created on it may hold at most that many WQEs in all their
    /// queues together.
    pub fn create_local_cq<W>(&self, entries: u32) -> Result<LocalCq<W, N>> {
        LocalCq::new(&self.inner, entries)
    }

    /// A completion queue of `entries` entries for this thread's futures: the QPs created on it
    /// may hold at most that many WQEs in all their queues together.
    pub fn create_async_cq(&self, entries: u32) -> Result<AsyncCq<N>> {
        AsyncCq::new(&self.inner, entries)
    }
}

/// A protection domain: memory registered in it can only be used by QPs of
/// the same domain. Cheap to clone. Allocate memory with
/// [`alloc`](safeverbs_core::Pd::alloc) and [`ring_buf`](safeverbs_core::Pd::ring_buf).
pub struct Pd<N: Nic = Verbs> {
    inner: safeverbs_core::Pd<N>,
}

impl<N: Nic> Clone for Pd<N> {
    fn clone(&self) -> Self {
        Pd {
            inner: self.inner.clone(),
        }
    }
}

impl<N: Nic> Deref for Pd<N> {
    type Target = safeverbs_core::Pd<N>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<N: Nic> Pd<N> {
    /// Creates a QP of this domain on `cq`, with queues of `caps` and the builder's defaults for
    /// the rest ([`QueuePairBuilder`] sets them): the QP refers to the CQ, as in verbs. Send its
    /// [`endpoint`](PreparedQp::endpoint) to the peer and [`handshake`](PreparedQp::handshake)
    /// with the peer's to get a handle that posts.
    pub fn create_qp<T: Transport, C: Cq<Nic = N>>(
        &self,
        cq: &C,
        caps: QpCaps,
    ) -> Result<PreparedQp<T, C>> {
        let mut builder = QueuePairBuilder::<T, N>::new(self);
        builder.set_caps(caps);
        builder.build_on(cq)
    }

    pub(crate) fn from_raw(inner: safeverbs_core::Pd<N>) -> Self {
        Pd { inner }
    }

    /// The domain as the core knows it, for its index-based [`raw`](crate::raw) API.
    pub fn as_raw(&self) -> &safeverbs_core::Pd<N> {
        &self.inner
    }
}
