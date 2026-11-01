//! Devices and protection domains, reference counted along their
//! dependency chain (a PD keeps its device open, a region its PD).

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::nic::{GidEntry, Nic, PortInfo};
use crate::verbs::Verbs;

/// An open RDMA device. Cheap to clone.
pub struct Context<N: Nic> {
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
        let pd = self.nic.alloc_pd()?;
        Ok(Pd {
            inner: Arc::new(PdInner {
                pd,
                id: IDS.fetch_add(1, Ordering::Relaxed),
                ctx: self.clone(),
            }),
        })
    }
}

/// A protection domain: memory registered in it can only be used by QPs of
/// the same domain. Cheap to clone.
pub struct Pd<N: Nic> {
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
    pub fn id(&self) -> u64 {
        self.inner.id
    }
}
