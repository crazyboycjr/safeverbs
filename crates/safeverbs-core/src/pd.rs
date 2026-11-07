//! Devices and protection domains, reference counted along their
//! dependency chain (a PD keeps its device open, a region its PD).

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::nic::{GidEntry, Nic, PortInfo};
use crate::verbs::Verbs;

/// Identities are never reused, including after a failed hardware allocation.
/// Reserve `u64::MAX` as the exhausted state instead of wrapping to old PDs.
fn reserve_pd_id(ids: &AtomicU64) -> Result<u64> {
    ids.try_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
        next.checked_add(1)
    })
    .map_err(|_| Error::Invalid("protection-domain identities exhausted"))
}

/// An open RDMA device. Cheap to clone.
pub struct Context<N: Nic = Verbs> {
    nic: Arc<N>,
}

impl<N: Nic> Clone for Context<N> {
    fn clone(&self) -> Self {
        Context {
            nic: self.nic.clone(),
        }
    }
}

impl Context<Verbs> {
    /// Opens the RDMA device called `name`, or the first one if `None`.
    pub fn open(name: Option<&str>) -> Result<Context<Verbs>> {
        Ok(Context::new(Verbs::open(name)?))
    }
}

impl<N: Nic> Context<N> {
    /// Wraps an open device.
    pub fn new(nic: N) -> Context<N> {
        Context { nic: Arc::new(nic) }
    }

    /// The device.
    pub fn nic(&self) -> &N {
        &self.nic
    }

    /// Whether `other` is a handle to the same open device.
    pub fn same_device(&self, other: &Context<N>) -> bool {
        Arc::ptr_eq(&self.nic, &other.nic)
    }

    /// The device's name.
    pub fn name(&self) -> &str {
        self.nic.name()
    }

    /// A port's attributes.
    pub fn port(&self, port: u8) -> Result<PortInfo> {
        self.nic.port(port)
    }

    /// A port's GID table.
    pub fn gids(&self, port: u8) -> Vec<GidEntry> {
        self.nic.gids(port)
    }

    /// The GID index to address a RoCE port by: the first RoCE v2 GID
    /// holding an IPv4 address, else the first RoCE v2 GID, else the first
    /// GID; `None` on InfiniBand (addressed by LID).
    pub fn default_gid_index(&self, port: u8) -> Result<Option<u8>> {
        if !self.port(port)?.ethernet {
            return Ok(None);
        }
        let gids = self.gids(port);
        let mut v2 = None;
        for entry in &gids {
            if entry.roce_v2 && entry.gid.ipv4().is_some() {
                return Ok(Some(entry.index));
            }
            if entry.roce_v2 && v2.is_none() {
                v2 = Some(entry.index);
            }
        }
        match (v2, gids.first()) {
            (Some(i), _) => Ok(Some(i)),
            (None, Some(e)) => Ok(Some(e.index)),
            (None, None) => Err(Error::Unsupported("a RoCE port without GIDs")),
        }
    }

    /// Allocates a protection domain.
    pub fn alloc_pd(&self) -> Result<Pd<N>> {
        static IDS: AtomicU64 = AtomicU64::new(1);
        let id = reserve_pd_id(&IDS)?;
        // SAFETY: the `Pd` keeps the device open, and everything that depends on it keeps the `Pd`.
        let pd = unsafe { self.nic.alloc_pd() }?;
        Ok(Pd {
            inner: Arc::new(PdInner {
                pd,
                id,
                ctx: self.clone(),
            }),
        })
    }
}

/// A protection domain: memory registered in it can only be used by QPs of
/// the same domain. Cheap to clone.
pub struct Pd<N: Nic = Verbs> {
    inner: Arc<PdInner<N>>,
}

/// Fields drop in order: the PD before the device.
struct PdInner<N: Nic> {
    pd: N::Pd,
    id: u64,
    ctx: Context<N>,
}

impl<N: Nic> Clone for Pd<N> {
    fn clone(&self) -> Self {
        Pd {
            inner: self.inner.clone(),
        }
    }
}

impl<N: Nic> Pd<N> {
    /// The device.
    pub fn context(&self) -> &Context<N> {
        &self.inner.ctx
    }

    /// The device's handle of the PD.
    pub(crate) fn raw(&self) -> &N::Pd {
        &self.inner.pd
    }

    /// A number unique to this PD in the process, which buffers and QPs
    /// compare to check they belong together.
    #[inline]
    pub fn id(&self) -> u64 {
        self.inner.id
    }
}

#[cfg(test)]
mod revocation_tests {
    use super::*;
    use crate::model::Model;

    #[test]
    fn exhausted_pd_identity_never_wraps_or_reuses_a_name() {
        let ids = AtomicU64::new(u64::MAX - 1);
        assert_eq!(reserve_pd_id(&ids).unwrap(), u64::MAX - 1);
        for _ in 0..2 {
            assert_eq!(
                reserve_pd_id(&ids),
                Err(Error::Invalid("protection-domain identities exhausted"))
            );
            assert_eq!(ids.load(Ordering::Relaxed), u64::MAX);
        }
    }

    #[test]
    fn failed_base_revocation_keeps_the_pd_and_device_alive() {
        let model = Model::new(54);
        let ctx = Context::new(model.clone());
        let pd = ctx.alloc_pd().unwrap();
        let pd_lifetime = Arc::downgrade(&pd.inner);
        let device_lifetime = Arc::downgrade(&ctx.nic);
        let part = pd.alloc::<u8>(16).unwrap().into_part();
        model.refuse_mr_closes(part.sge.lkey, 1);
        drop(part);
        drop(pd);
        drop(ctx);
        assert_eq!(model.registered(), 1);
        assert!(pd_lifetime.upgrade().is_some());
        assert!(device_lifetime.upgrade().is_some());
    }

    #[test]
    fn successful_base_revocation_releases_the_dependency_chain() {
        let model = Model::new(55);
        let ctx = Context::new(model.clone());
        let pd = ctx.alloc_pd().unwrap();
        let pd_lifetime = Arc::downgrade(&pd.inner);
        let device_lifetime = Arc::downgrade(&ctx.nic);
        drop(pd.alloc::<u8>(16).unwrap());
        drop(pd);
        drop(ctx);
        assert_eq!(model.registered(), 0);
        assert!(pd_lifetime.upgrade().is_none());
        assert!(device_lifetime.upgrade().is_none());
    }
}
