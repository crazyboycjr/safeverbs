#![allow(non_camel_case_types)]
#![allow(clippy::too_many_arguments)]

use crate::ffi;
use crate::ibv;
use crate::interval_tree::IntervalTree;
use core::any::TypeId;
use core::mem;
use core::ops::Deref;
use core::ops::{Range, RangeBounds};
use core::ptr;
use core::slice;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Poll, Waker};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

// Re-exports
pub use ibv::{devices, Device, DeviceList, DeviceListIter, Gid, Guid, RemoteKey};

#[derive(Clone)]
pub struct Context {
    ctx: Arc<ibv::Context>,
}

impl AsRef<ibv::Context> for Context {
    fn as_ref(&self) -> &ibv::Context {
        &self.ctx
    }
}

impl Deref for Context {
    type Target = ibv::Context;
    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl Context {
    pub fn with_device(dev: &Device) -> io::Result<Context> {
        let ctx = dev.open()?;
        Ok(Context { ctx: Arc::new(ctx) })
    }

    pub fn create_cq(&self, min_cq_entries: i32, id: isize) -> io::Result<CompletionQueue> {
        let inner = self.ctx.create_cq(min_cq_entries, id)?;
        // SAFETY: This is safe as long as `cq: Arc<ibv::CompletionQueue<'static>>` cannot
        // be constructed from the outside by any means.
        let cq: ibv::CompletionQueue<'static> = unsafe { mem::transmute(inner) };
        Ok(CompletionQueue(Arc::new(CompletionQueueInner {
            cq,
            _ctx: self.clone(),
            wr_slab: sharded_slab::Slab::new(),
        })))
    }

    pub fn alloc_pd(&self) -> io::Result<ProtectionDomain> {
        let inner = self.ctx.alloc_pd()?;
        // SAFETY: This is safe as long as `cq: Arc<ibv::ProtectionDomain<'static>>` cannot
        // be constructed from the outside by any means.
        let pd: ibv::ProtectionDomain<'static> = unsafe { mem::transmute(inner) };
        Ok(ProtectionDomain(Arc::new(ProtectionDomainInner {
            pd,
            _ctx: self.clone(),
        })))
    }
}

struct CompletionQueueInner {
    cq: ibv::CompletionQueue<'static>,
    _ctx: Context,
    wr_slab: sharded_slab::Slab<WorkRequestInner>,
}

#[derive(Clone)]
pub struct CompletionQueue(Arc<CompletionQueueInner>);

impl<'a> AsRef<ibv::CompletionQueue<'a>> for CompletionQueue {
    fn as_ref(&self) -> &ibv::CompletionQueue<'a> {
        &self.0.cq
    }
}

// impl Deref for CompletionQueue {
//     type Target = ibv::CompletionQueue<'static>;
//     fn deref(&self) -> &Self::Target {
//         &self.0.cq
//     }
// }

impl CompletionQueue {
    #[inline]
    pub fn poll<'c>(
        &self,
        completions: &'c mut [ffi::ibv_wc],
    ) -> io::Result<&'c mut [ffi::ibv_wc]> {
        let comp = self.0.cq.poll(completions)?;
        for c in comp.iter_mut() {
            let wr = self
                .0
                .wr_slab
                .get(c.wr_id() as usize)
                .expect("something gets wrong");
            // mem::swap(&mut c.wr_id, &mut wr.wr_id);
            c.wr_id = wr.wr_id;
            // A completion should only be fetched from one CQ, so set
            // should return successfully.
            wr.wc.set(*c).unwrap();
            if let Some(waker) = wr.waker.get() {
                waker.wake_by_ref();
            }
        }
        Ok(comp)
    }
}

struct ProtectionDomainInner {
    pd: ibv::ProtectionDomain<'static>,
    _ctx: Context,
}

#[derive(Clone)]
pub struct ProtectionDomain(Arc<ProtectionDomainInner>);

impl<'a> AsRef<ibv::ProtectionDomain<'a>> for ProtectionDomain {
    fn as_ref(&self) -> &ibv::ProtectionDomain<'a> {
        &self.0.pd
    }
}

impl Deref for ProtectionDomain {
    type Target = ibv::ProtectionDomain<'static>;
    fn deref(&self) -> &Self::Target {
        &self.0.pd
    }
}

// NOTE(cjr): Perhaps I need both the builder pattern and the type state pattern.

impl ProtectionDomain {
    pub fn create_qp<T: ToQpType>(
        &self,
        qp_init_attr: &QpInitAttr<T>,
    ) -> io::Result<QueuePair<T, RESET>> {
        let mut attr = qp_init_attr.to_ibv_qp_init_attr();
        // SAFETY: FFI calls, FFI object (PD) obtained from a safe object is
        // guaranteed to be valid. ibv_qp_init_attr is also valid because we
        // take the reference from QpInitAttr.
        let qp = unsafe { OwnedQueuePair::create(self.0.pd.pd(), &mut attr) }?;
        Ok(QueuePair {
            inner: Arc::new(QueuePairInner {
                qp,
                pd: self.clone(),
                send_cq: qp_init_attr.send_cq.clone(),
                recv_cq: qp_init_attr.recv_cq.clone(),
            }),
            _marker: PhantomData,
        })
    }

    pub fn allocate<T: Sized + Clone + Default + zerocopy::FromBytes>(
        &self,
        n: usize,
        access: ffi::ibv_access_flags,
    ) -> io::Result<MemoryRegion<T>> {
        assert!(n > 0);
        assert!(mem::size_of::<T>() > 0);

        let mut data = Vec::with_capacity(n);
        data.resize(n, T::default());

        let mr = unsafe {
            ffi::ibv_reg_mr(
                self.0.pd.pd(),
                data.as_mut_ptr() as *mut _,
                n * mem::size_of::<T>(),
                access.0 as i32,
            )
        };

        if mr.is_null() {
            Err(io::Error::last_os_error())
        } else {
            let slice = data.into_boxed_slice();
            let leaked = Box::leak(slice);
            // SAFETY: memory is obtained by Vec::with_capacity. Downcasting it to [u8]
            // still satisfies the alignment requirement for [u8].
            let bytes = unsafe {
                slice::from_raw_parts_mut(leaked.as_mut_ptr().cast(), mem::size_of_val(leaked))
            };
            Ok(MemoryRegion {
                inner: Arc::new(OwnedMemoryRegionOpaque {
                    mr,
                    // SAFETY: `bytes` is previously leaked from Box<[T]>.
                    data: unsafe { Box::from_raw(bytes) },
                    intervals: Mutex::new(IntervalTree::new()),
                }),
                _marker: PhantomData,
            })
        }
    }
}

/// The type of QP used for communication.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QpType(pub ffi::ibv_qp_type::Type);

pub trait ToQpType: 'static + Clone + Copy {
    fn to_qp_type() -> QpType;
}
pub trait ConnectionOrientedQpType: ToQpType {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RC;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UC;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UD;

impl ConnectionOrientedQpType for UC {}
impl ConnectionOrientedQpType for RC {}

#[inline]
fn is<S: ToQpType, T: ToQpType>() -> bool {
    TypeId::of::<S>() == TypeId::of::<T>()
}

impl ToQpType for RC {
    fn to_qp_type() -> QpType {
        QpType(ffi::ibv_qp_type::IBV_QPT_RC)
    }
}
impl ToQpType for UC {
    fn to_qp_type() -> QpType {
        QpType(ffi::ibv_qp_type::IBV_QPT_UC)
    }
}
impl ToQpType for UD {
    fn to_qp_type() -> QpType {
        QpType(ffi::ibv_qp_type::IBV_QPT_UD)
    }
}

/// QP capabilities.
#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
pub struct QpCapability(pub ffi::ibv_qp_cap);

/// The attributes to initialize a QP.
#[derive(Clone)]
pub struct QpInitAttr<T: ToQpType> {
    /// Associated user context of the QP.
    pub qp_context: usize,
    /// CQ to be associated with the Send Queue (SQ).
    pub send_cq: CompletionQueue,
    /// CQ to be associated with the Receive Queue (RQ).
    pub recv_cq: CompletionQueue,
    /// Qp capabilities.
    pub cap: QpCapability,
    /// QP Transport Service Type: IBV_QPT_RC, IBV_QPT_UC, iBV_QPT_UD.
    pub qp_type: T,
    /// If set, each Work Request (WR) submitted to the SQ generates a completion entry.
    pub sq_sig_all: bool,
}

impl<T: ToQpType> QpInitAttr<T> {
    pub fn to_ibv_qp_init_attr(&self) -> ffi::ibv_qp_init_attr {
        ffi::ibv_qp_init_attr {
            qp_context: self.qp_context as *mut _,
            send_cq: self.send_cq.as_ref().cq(),
            recv_cq: self.recv_cq.as_ref().cq(),
            srq: ptr::null_mut(),
            cap: self.cap.0,
            qp_type: T::to_qp_type().0,
            sq_sig_all: self.sq_sig_all as i32,
        }
    }
}

/// The state of QP used for communication.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QpState(pub ffi::ibv_qp_state::Type);

pub trait ToQpState: 'static + Clone + Copy {
    fn to_qp_state() -> QpState;
}

macro_rules! define_qp_state {
    ($state_name:ident, $variant:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $state_name;

        impl ToQpState for $state_name {
            fn to_qp_state() -> QpState {
                QpState(ffi::ibv_qp_state::$variant)
            }
        }
    };
}

define_qp_state!(RESET, IBV_QPS_RESET);
define_qp_state!(INIT, IBV_QPS_INIT);
define_qp_state!(RTR, IBV_QPS_RTR);
define_qp_state!(RTS, IBV_QPS_RTS);
define_qp_state!(ERR, IBV_QPS_ERR);

// Type aliases
pub type UcQueuePairBuilder = QueuePairBuilder<UC>;
pub type RcQueuePairBuilder = QueuePairBuilder<RC>;
pub type UdQueuePairBuilder = QueuePairBuilder<UD>;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct QueuePairSetting {
    /// valid for RC and UC
    pub access: ffi::ibv_access_flags,
    /// valid for RC and UC
    pub path_mtu: u32,
    /// valid for RC and UC
    pub rq_psn: u32,
    /// valid for RC and UC
    pub sq_psn: u32,
    /// only valid for RC
    pub timeout: Option<u8>,
    /// only valid for RC
    pub retry_count: Option<u8>,
    /// only valid for RC
    pub rnr_retry: Option<u8>,
    /// only valid for RC
    pub min_rnr_timer: Option<u8>,
    /// only valid for RC
    pub max_rd_atomic: Option<u8>,
    /// only valid for RC
    pub max_dest_rd_atomic: Option<u8>,
    /// traffic class, the default value is 0.
    /// Note that the setting can be overwritten by a global system setting.
    pub traffic_class: u8,
    /// source gid_index. IB: None, ETH: default 0.
    /// See `show_gids` for more information.
    pub gid_index: Option<u8>,
}

/// An unconfigured `QueuePair`.
///
/// A `QueuePairBuilder<T>` is used to configure a `QueuePair` before it is allocated and initialized.
/// See also [RDMAmojo] for many more details.
///
/// [RDMAmojo]: http://www.rdmamojo.com/2013/01/12/ibv_modify_qp/
#[derive(Clone)]
pub struct QueuePairBuilder<T: ConnectionOrientedQpType> {
    // Required fields
    pd: ProtectionDomain,
    qp_init_attr: QpInitAttr<T>,
    // carried along to handshake phase
    setting: QueuePairSetting,
}

impl<T: ConnectionOrientedQpType> QueuePairBuilder<T> {
    pub fn new(pd: ProtectionDomain, qp_init_attr: QpInitAttr<T>) -> QueuePairBuilder<T> {
        let port_attr = pd.context().port_attr().unwrap();
        QueuePairBuilder {
            pd: pd.clone(),
            qp_init_attr,
            setting: QueuePairSetting {
                access: ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE,
                path_mtu: port_attr.active_mtu,
                rq_psn: 0,
                sq_psn: 0,
                min_rnr_timer: is::<T, RC>().then_some(16),
                retry_count: is::<T, RC>().then_some(6),
                rnr_retry: is::<T, RC>().then_some(6),
                timeout: is::<T, RC>().then_some(4),
                max_rd_atomic: is::<T, RC>().then_some(1),
                max_dest_rd_atomic: is::<T, RC>().then_some(1),
                traffic_class: 0,
                gid_index: (port_attr.link_layer == ffi::IBV_LINK_LAYER_ETHERNET as u8)
                    .then_some(0),
            },
        }
    }

    pub fn set_access(&mut self, access: ffi::ibv_access_flags) -> &mut Self {
        self.setting.access = access;
        self
    }

    pub fn allow_remote_rw(&mut self) -> &mut Self {
        assert!(is::<T, RC>() || is::<T, UC>());
        self.setting.access = self.setting.access
            | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_READ;
        self
    }

    pub fn set_path_mtu(&mut self, path_mtu: u32) -> &mut Self {
        assert!((1..=5).contains(&path_mtu));
        self.setting.path_mtu = path_mtu;
        self
    }

    pub fn set_rq_psn(&mut self, rq_psn: u32) -> &mut Self {
        self.setting.rq_psn = rq_psn;
        self
    }

    pub fn set_sq_psn(&mut self, sq_psn: u32) -> &mut Self {
        self.setting.sq_psn = sq_psn;
        self
    }

    pub fn set_traffic_class(&mut self, traffic_class: u8) -> &mut Self {
        self.setting.traffic_class = traffic_class;
        self
    }

    pub fn set_qp_context(&mut self, qp_context: usize) -> &mut Self {
        self.qp_init_attr.qp_context = qp_context;
        self
    }

    pub fn set_gid_index(&mut self, gid_index: u8) -> &mut Self {
        self.setting.gid_index = Some(gid_index);
        self
    }

    pub fn build(&self) -> io::Result<PreparedQueuePair<T>> {
        let qp = self.pd.create_qp(&self.qp_init_attr)?;
        let gid = self
            .setting
            .gid_index
            .map(|index| self.pd.context().gid(index as u32))
            .transpose()?;
        Ok(PreparedQueuePair {
            port_attr: self.pd.context().port_attr()?,
            gid,
            qp,
            setting: self.setting.clone(),
        })
    }
}

impl QueuePairBuilder<RC> {
    pub fn set_min_rnr_timer(&mut self, timer: u8) -> &mut Self {
        self.setting.min_rnr_timer = Some(timer);
        self
    }

    pub fn set_timeout(&mut self, timeout: u8) -> &mut Self {
        self.setting.timeout = Some(timeout);
        self
    }

    pub fn set_retry_count(&mut self, count: u8) -> &mut Self {
        assert!(count <= 7);
        self.setting.retry_count = Some(count);
        self
    }

    pub fn set_rnr_retry(&mut self, n: u8) -> &mut Self {
        assert!(n <= 7);
        self.setting.rnr_retry = Some(n);
        self
    }

    pub fn set_max_rd_atomic(&mut self, max_rd_atomic: u8) -> &mut Self {
        self.setting.max_rd_atomic = Some(max_rd_atomic);
        self
    }

    pub fn set_max_dest_rd_atomic(&mut self, max_dest_rd_atomic: u8) -> &mut Self {
        self.setting.max_dest_rd_atomic = Some(max_dest_rd_atomic);
        self
    }
}

pub struct PreparedQueuePair<T: ConnectionOrientedQpType> {
    port_attr: ffi::ibv_port_attr,
    gid: Option<Gid>,
    qp: QueuePair<T, RESET>,

    // carried along from builder
    setting: QueuePairSetting,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct QueuePairEndpoint {
    /// the `QueuePair`'s `qp_num`
    pub num: u32,
    /// the context's `lid`
    pub lid: u16,
    /// the context's `gid`, used for global routing
    pub gid: Option<Gid>,
}

impl<T: ConnectionOrientedQpType> PreparedQueuePair<T> {
    pub fn endpoint(&self) -> QueuePairEndpoint {
        // SAFETY: QP cannot be dropped when there's outstanding references,
        // so it is safe to access qp_num field.
        let num = unsafe { &*self.qp.inner.qp.qp }.qp_num;

        QueuePairEndpoint {
            num,
            lid: self.port_attr.lid,
            gid: self.gid,
        }
    }
}

impl PreparedQueuePair<RC> {
    pub fn handshake(self, remote: QueuePairEndpoint) -> io::Result<QueuePair<RC, RTS>> {
        let init_qp = self.qp.modify_to_init(0, 1, self.setting.access)?;
        let mut ah_attr = ffi::ibv_ah_attr {
            dlid: remote.lid,
            sl: 0,
            src_path_bits: 0,
            port_num: 1,
            grh: Default::default(),
            ..Default::default()
        };
        if let Some(gid) = remote.gid {
            ah_attr.is_global = 1;
            ah_attr.grh = ffi::ibv_global_route {
                dgid: gid.into(),
                flow_label: 0,
                sgid_index: self.setting.gid_index.expect("expect GRH"),
                hop_limit: 0xff,
                traffic_class: self.setting.traffic_class,
            };
        }
        let rtr_qp = init_qp.modify_to_rtr(
            ah_attr,
            self.setting.path_mtu,
            remote.num,
            self.setting.rq_psn,
            self.setting.max_dest_rd_atomic.expect("RC"),
            self.setting.min_rnr_timer.expect("RC"),
            None, // pkey_index already set
            None, // access_flags already set
            None,
        )?;
        let rts_qp = rtr_qp.modify_to_rts(
            self.setting.sq_psn,
            self.setting.timeout.expect("RC"),
            self.setting.retry_count.expect("RC"),
            self.setting.rnr_retry.expect("RC"),
            self.setting.max_rd_atomic.expect("RC"),
            None, // cur_qp_state
            None, // access_flags already set
            None, // min_rnr_timer already set
            None,
            None,
        )?;
        Ok(rts_qp)
    }
}

impl PreparedQueuePair<UC> {
    pub fn handshake(self, remote: QueuePairEndpoint) -> io::Result<QueuePair<UC, RTS>> {
        let init_qp = self.qp.modify_to_init(0, 1, self.setting.access)?;
        let mut ah_attr = ffi::ibv_ah_attr {
            dlid: remote.lid,
            sl: 0,
            src_path_bits: 0,
            port_num: 1,
            grh: Default::default(),
            ..Default::default()
        };
        if let Some(gid) = remote.gid {
            ah_attr.is_global = 1;
            ah_attr.grh = ffi::ibv_global_route {
                dgid: gid.into(),
                flow_label: 0,
                sgid_index: self.setting.gid_index.expect("expect GRH"),
                hop_limit: 0xff,
                traffic_class: self.setting.traffic_class,
            };
        }
        let rtr_qp = init_qp.modify_to_rtr(
            ah_attr,
            self.setting.path_mtu,
            remote.num,
            self.setting.rq_psn,
            None, // pkey_index already set
            None, // access_flags already set
            None, // alt_path
        )?;
        let rts_qp = rtr_qp.modify_to_rts(self.setting.sq_psn, None, None, None, None)?;
        Ok(rts_qp)
    }
}

#[repr(transparent)]
struct OwnedQueuePair {
    qp: *mut ffi::ibv_qp,
}

unsafe impl Send for OwnedQueuePair {}
unsafe impl Sync for OwnedQueuePair {}

impl OwnedQueuePair {
    /// Creates a new `OwnedQueuePair`.
    ///
    /// # Safety
    ///
    /// The ibv_pd object and ibv_qp_init_attr object must be valid. Otherwise, undefined
    /// behavior will happen.
    ///
    /// We leave the various checks to the underlying ibverbs library and driver, and returns
    /// errors in case the input is not valid.
    unsafe fn create(
        pd: *mut ffi::ibv_pd,
        attr: *mut ffi::ibv_qp_init_attr,
    ) -> io::Result<OwnedQueuePair> {
        assert!(!pd.is_null() && !attr.is_null());
        let qp = unsafe { ffi::ibv_create_qp(pd, attr) };
        if qp.is_null() {
            Err(io::Error::last_os_error())
        } else {
            // SAFETY: The qp will be managed by us. There's no extra alias to it.
            // let qp: ibv::QueuePair<'static> = unsafe { ibv::QueuePair::from_raw_qp(qp) };
            Ok(OwnedQueuePair { qp })
        }
    }

    unsafe fn post_send_gather(
        &self,
        ms: &[MemorySegmentOpaque<RO>],
        wr_id: u64,
        flags: ffi::ibv_send_flags,
    ) -> io::Result<()> {
        let mut sglist = Vec::with_capacity(ms.len());
        for m in ms {
            let slice = &m.mr.data.as_ref()[m.range.start..m.range.end];
            sglist.push(ffi::ibv_sge {
                addr: slice.as_ptr() as u64,
                length: mem::size_of_val(slice) as u32,
                // SAFETY: mr is guarded by Arc, so it should be valid.
                lkey: m.mr.lkey(),
            });
        }
        let mut wr = ffi::ibv_send_wr {
            wr_id,
            next: ptr::null_mut(),
            sg_list: sglist.as_mut_ptr(),
            num_sge: sglist.len() as i32,
            opcode: ffi::ibv_wr_opcode::IBV_WR_SEND,
            send_flags: flags.0,
            ..Default::default()
        };
        self.post_send_batch(&mut wr)
    }

    unsafe fn post_write_gather(
        &self,
        ms: &[MemorySegmentOpaque<RO>],
        remote_memory: &RemoteMemory,
        wr_id: u64,
        flags: ffi::ibv_send_flags,
    ) -> io::Result<()> {
        let mut sglist = Vec::with_capacity(ms.len());
        for m in ms {
            let slice = &m.mr.data.as_ref()[m.range.start..m.range.end];
            sglist.push(ffi::ibv_sge {
                addr: slice.as_ptr() as u64,
                length: mem::size_of_val(slice) as u32,
                // SAFETY: mr is guarded by Arc, so it should be valid.
                lkey: m.mr.lkey(),
            });
        }
        let mut wr = ffi::ibv_send_wr {
            wr_id,
            next: ptr::null_mut(),
            sg_list: sglist.as_mut_ptr(),
            num_sge: sglist.len() as i32,
            opcode: ffi::ibv_wr_opcode::IBV_WR_RDMA_WRITE,
            send_flags: flags.0,
            wr: ffi::ibv_send_wr__bindgen_ty_2 {
                rdma: ffi::ibv_send_wr__bindgen_ty_2__bindgen_ty_1 {
                    remote_addr: remote_memory.addr,
                    rkey: remote_memory.rkey.key,
                },
            },
            ..Default::default()
        };
        self.post_send_batch(&mut wr)
    }

    unsafe fn post_recv_scatter(
        &self,
        ms: &[MemorySegmentOpaque<RW>],
        wr_id: u64,
    ) -> io::Result<()> {
        let mut sglist = Vec::with_capacity(ms.len());
        for m in ms {
            let slice = &m.mr.data.as_ref()[m.range.start..m.range.end];
            sglist.push(ffi::ibv_sge {
                addr: slice.as_ptr() as u64,
                length: mem::size_of_val(slice) as u32,
                // SAFETY: mr is guarded by Arc, so it should be valid.
                lkey: m.mr.lkey(),
            });
        }
        let mut wr = ffi::ibv_recv_wr {
            wr_id,
            next: ptr::null_mut(),
            sg_list: sglist.as_mut_ptr(),
            num_sge: sglist.len() as i32,
        };
        let mut bad_wr = ptr::null_mut();

        let ctx = (*self.qp).context;
        let ops = &mut (*ctx).ops;
        let errno = ops.post_recv.as_mut().unwrap()(self.qp, &mut wr, &mut bad_wr);
        if errno != 0 {
            Err(io::Error::from_raw_os_error(errno))
        } else {
            Ok(())
        }
    }

    unsafe fn post_send_batch(&self, wr_list: &mut ffi::ibv_send_wr) -> io::Result<()> {
        let mut bad_wr = ptr::null_mut();

        let ctx = (*self.qp).context;
        let ops = &mut (*ctx).ops;
        let errno = ops.post_send.as_mut().unwrap()(self.qp, wr_list, &mut bad_wr);
        if errno != 0 {
            Err(io::Error::from_raw_os_error(errno))
        } else {
            Ok(())
        }
    }
}

impl Drop for OwnedQueuePair {
    fn drop(&mut self) {
        // TODO: ibv_destroy_qp() fails if the QP is attached to a multicast group.
        let errno = unsafe { ffi::ibv_destroy_qp(self.qp) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            eprintln!("Failed to destroy qp: {}", e);
        }
    }
}

struct QueuePairInner {
    qp: OwnedQueuePair,
    pd: ProtectionDomain,
    send_cq: CompletionQueue,
    recv_cq: CompletionQueue,
}

#[derive(Clone)]
pub struct QueuePair<T: ToQpType, S: ToQpState> {
    inner: Arc<QueuePairInner>,
    _marker: PhantomData<(T, S)>,
}

// Type aliases
pub type UcQueuePair<S> = QueuePair<UC, S>;
pub type RcQueuePair<S> = QueuePair<RC, S>;
pub type UdQueuePair<S> = QueuePair<UD, S>;

impl<T: ToQpType, S: ToQpState> QueuePair<T, S> {
    #[inline]
    pub fn send_cq(&self) -> &CompletionQueue {
        &self.inner.send_cq
    }

    #[inline]
    pub fn recv_cq(&self) -> &CompletionQueue {
        &self.inner.recv_cq
    }

    #[inline]
    pub fn pd(&self) -> &ProtectionDomain {
        &self.inner.pd
    }

    /// IB: no gid. ETH: required
    pub fn endpoint(&self, gid_index: Option<u32>) -> io::Result<QueuePairEndpoint> {
        let port_attr = self.inner.pd.context().port_attr()?;
        let gid = gid_index
            .map(|index| self.inner.pd.context().gid(index))
            .transpose()?;
        // SAFETY: QP cannot be dropped when there's outstanding references,
        // so it is safe to access qp_num field.
        let num = unsafe { &*self.inner.qp.qp }.qp_num;

        Ok(QueuePairEndpoint {
            num,
            lid: port_attr.lid,
            gid,
        })
    }

    /// Bioperlate code to `ibv_modify_qp`.
    unsafe fn modify<D: ToQpState>(
        self,
        attr: *mut ffi::ibv_qp_attr,
        mask: ffi::ibv_qp_attr_mask,
    ) -> io::Result<QueuePair<T, D>> {
        let errno = unsafe { ffi::ibv_modify_qp(self.inner.qp.qp, attr, mask.0 as i32) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(QueuePair {
            inner: self.inner,
            _marker: PhantomData,
        })
    }

    pub fn modify_to_reset(self) -> io::Result<QueuePair<T, RESET>> {
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RESET,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE;
        // SAFETY: FFI call. The FFI object obtained from a valid QueuePair object is
        // guaranteed to be valid.
        unsafe { self.modify(&mut attr, mask) }
    }

    pub fn modify_to_err(self) -> io::Result<QueuePair<T, ERR>> {
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_ERR,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE;
        // SAFETY: FFI call. The FFI object obtained from a valid QueuePair object is
        // guaranteed to be valid.
        unsafe { self.modify(&mut attr, mask) }
    }
}

fn modify_reset_to_init<T: ToQpType>(
    qp: QueuePair<T, RESET>,
    pkey_index: u16,
    port_num: u8,
    access: Option<ffi::ibv_access_flags>,
    qkey: Option<u32>,
) -> io::Result<QueuePair<T, INIT>> {
    let mut attr = ffi::ibv_qp_attr {
        qp_state: ffi::ibv_qp_state::IBV_QPS_INIT,
        pkey_index,
        port_num,
        ..Default::default()
    };
    let mut mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
        | ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
        | ffi::ibv_qp_attr_mask::IBV_QP_PORT;

    if is::<T, RC>() || is::<T, UC>() {
        attr.qp_access_flags = access.expect("RC or UC must set ACCESS_FLAGS").0;
        mask |= ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
    }
    if is::<T, UD>() {
        attr.qkey = qkey.expect("UD must set qkey");
        mask |= ffi::ibv_qp_attr_mask::IBV_QP_QKEY;
    }
    // SAFETY: FFI call. The FFI object obtained from a valid QueuePair object is
    // guaranteed to be valid.
    unsafe { qp.modify(&mut attr, mask) }
}

fn modify_init_to_init<T: ToQpType>(
    qp: QueuePair<T, INIT>,
    pkey_index: Option<u16>,
    port_num: Option<u8>,
    access: Option<ffi::ibv_access_flags>,
    qkey: Option<u32>,
) -> io::Result<QueuePair<T, INIT>> {
    let mut attr = ffi::ibv_qp_attr {
        qp_state: ffi::ibv_qp_state::IBV_QPS_INIT,
        ..Default::default()
    };
    let mut mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE;

    if let Some(pkey_index) = pkey_index {
        attr.pkey_index = pkey_index;
        mask |= ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX;
    }
    if let Some(port_num) = port_num {
        attr.port_num = port_num;
        mask |= ffi::ibv_qp_attr_mask::IBV_QP_PORT;
    }
    if is::<T, RC>() || is::<T, UC>() {
        if let Some(access) = access {
            attr.qp_access_flags = access.0;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        }
    }
    if is::<T, UD>() {
        if let Some(qkey) = qkey {
            attr.qkey = qkey;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_QKEY;
        }
    }
    // SAFETY: FFI call. The FFI object obtained from a valid QueuePair object is
    // guaranteed to be valid.
    unsafe { qp.modify(&mut attr, mask) }
}

macro_rules! impl_modify_qp {
    ($func_name:ident, $qp_state_begin:ident, $qp_state_end:ident,
        $(required: ($($qpt:ident),+ $(,)?) [$mask_tag:ident] $field_name:ident: $field_type:ty,)*
        $(optional: ($($qpt_opt:ident),+ $(,)?) [$mask_tag_opt:ident] $field_name_opt:ident: $field_type_opt:ty,)*
        $(alt_path: ($($qpt_alt_path:ident),+ $(,)?) [$mask_tag_alt_path:ident] $field_name_alt_path:ident: $field_type_alt_path:ty,)?
    ) => {
        fn $func_name<T: ToQpType>(
            qp: QueuePair<T, $qp_state_begin>,
            $($field_name: $field_type,)*
            $($field_name_opt: $field_type_opt,)*
            $($field_name_alt_path: $field_type_alt_path,)?
        ) -> io::Result<QueuePair<T, $qp_state_end>> {
            let mut attr = ffi::ibv_qp_attr {
                qp_state: $qp_state_end::to_qp_state().0,
                ..Default::default()
            };
            let mut mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE;
            // build required fields
            $(
            // for each qp type in [RC, UC, UD]
            $(
            if is::<T, $qpt>() {
                attr.$field_name = $field_name.expect(concat!(stringify!($field_name), " is required"));
                mask |= ffi::ibv_qp_attr_mask::$mask_tag;
            })*
            )*
            // build optional fields
            $(
            // for each qp type in [RC, UC, UD]
            $(
            if is::<T, $qpt_opt>() {
                if let Some($field_name_opt) = $field_name_opt {
                    attr.$field_name_opt = $field_name_opt;
                    mask |= ffi::ibv_qp_attr_mask::$mask_tag_opt;
                }
            })*
            )*
            // build fields for alternative path
            $(
            // for each qp type in [RC, UC, UD]
            $(
            if is::<T, $qpt_alt_path>() {
                if let Some(alt_path) = $field_name_alt_path {
                    attr.alt_ah_attr = alt_path.alt_ah_attr;
                    attr.alt_pkey_index = alt_path.alt_pkey_index;
                    attr.alt_port_num = alt_path.alt_port_num;
                    attr.alt_timeout = alt_path.alt_timeout;
                    mask |= ffi::ibv_qp_attr_mask::$mask_tag_alt_path;
                }
            }
            )*
            )?
            // SAFETY: FFI call. The FFI object obtained from a valid QueuePair object is
            // guaranteed to be valid.
            unsafe { qp.modify(&mut attr, mask) }
        }
    };
}

impl_modify_qp!(modify_init_to_rtr, INIT, RTR,
    required: (RC, UC) [IBV_QP_AV] ah_attr: Option<ffi::ibv_ah_attr>,
    required: (RC, UC) [IBV_QP_PATH_MTU] path_mtu: Option<u32>,
    required: (RC, UC) [IBV_QP_DEST_QPN] dest_qp_num: Option<u32>,
    required: (RC, UC) [IBV_QP_RQ_PSN] rq_psn: Option<u32>,
    required: (RC) [IBV_QP_MAX_DEST_RD_ATOMIC] max_dest_rd_atomic: Option<u8>,
    required: (RC) [IBV_QP_MIN_RNR_TIMER] min_rnr_timer: Option<u8>,
    optional: (RC, UC, UD) [IBV_QP_PKEY_INDEX] pkey_index: Option<u16>,
    optional: (RC, UC) [IBV_QP_ACCESS_FLAGS] qp_access_flags: Option<u32>,
    optional: (UD) [IBV_QP_QKEY] qkey: Option<u32>,
    alt_path: (RC, UC) [IBV_QP_ALT_PATH] alt_path: Option<AltPath>,
);

impl_modify_qp!(modify_rtr_to_rts, RTR, RTS,
    required: (RC, UC, UD) [IBV_QP_SQ_PSN] sq_psn: Option<u32>,
    required: (RC) [IBV_QP_TIMEOUT] timeout: Option<u8>,
    required: (RC) [IBV_QP_RETRY_CNT] retry_cnt: Option<u8>,
    required: (RC) [IBV_QP_RNR_RETRY] rnr_retry: Option<u8>,
    required: (RC) [IBV_QP_MAX_QP_RD_ATOMIC] max_rd_atomic: Option<u8>,

    optional: (RC, UC, UD) [IBV_QP_CUR_STATE] cur_qp_state: Option<ffi::ibv_qp_state::Type>,
    optional: (RC, UC) [IBV_QP_ACCESS_FLAGS] qp_access_flags: Option<u32>,
    optional: (RC, UC) [IBV_QP_PATH_MIG_STATE] path_mig_state: Option<ffi::ibv_mig_state>,
    optional: (RC) [IBV_QP_MIN_RNR_TIMER] min_rnr_timer: Option<u8>,
    optional: (UD) [IBV_QP_QKEY] qkey: Option<u32>,

    alt_path: (RC, UC) [IBV_QP_ALT_PATH] alt_path: Option<AltPath>,
);

impl_modify_qp!(modify_rts_to_rts, RTS, RTS,
    optional: (RC, UC, UD) [IBV_QP_CUR_STATE] cur_qp_state: Option<ffi::ibv_qp_state::Type>,
    optional: (RC, UC) [IBV_QP_ACCESS_FLAGS] qp_access_flags: Option<u32>,
    optional: (RC, UC) [IBV_QP_PATH_MIG_STATE] path_mig_state: Option<ffi::ibv_mig_state>,
    optional: (RC) [IBV_QP_MIN_RNR_TIMER] min_rnr_timer: Option<u8>,
    optional: (UD) [IBV_QP_QKEY] qkey: Option<u32>,

    alt_path: (RC, UC) [IBV_QP_ALT_PATH] alt_path: Option<AltPath>,
);

// RESET -> INIT
impl QueuePair<UC, RESET> {
    pub fn modify_to_init(
        self,
        pkey_index: u16,
        port_num: u8,
        access: ffi::ibv_access_flags,
    ) -> io::Result<QueuePair<UC, INIT>> {
        modify_reset_to_init(self, pkey_index, port_num, Some(access), None)
    }
}

impl QueuePair<RC, RESET> {
    pub fn modify_to_init(
        self,
        pkey_index: u16,
        port_num: u8,
        access: ffi::ibv_access_flags,
    ) -> io::Result<QueuePair<RC, INIT>> {
        modify_reset_to_init(self, pkey_index, port_num, Some(access), None)
    }
}

impl QueuePair<UD, RESET> {
    pub fn modify_to_init(
        self,
        pkey_index: u16,
        port_num: u8,
        qkey: u32,
    ) -> io::Result<QueuePair<UD, INIT>> {
        modify_reset_to_init(self, pkey_index, port_num, None, Some(qkey))
    }
}

// INIT -> INIT
impl QueuePair<UC, INIT> {
    pub fn modify_to_init(
        self,
        pkey_index: Option<u16>,
        port_num: Option<u8>,
        access: Option<ffi::ibv_access_flags>,
    ) -> io::Result<QueuePair<UC, INIT>> {
        modify_init_to_init(self, pkey_index, port_num, access, None)
    }
}

impl QueuePair<RC, INIT> {
    pub fn modify_to_init(
        self,
        pkey_index: Option<u16>,
        port_num: Option<u8>,
        access: Option<ffi::ibv_access_flags>,
    ) -> io::Result<QueuePair<RC, INIT>> {
        modify_init_to_init(self, pkey_index, port_num, access, None)
    }
}

impl QueuePair<UD, INIT> {
    pub fn modify_to_init(
        self,
        pkey_index: Option<u16>,
        port_num: Option<u8>,
        qkey: Option<u32>,
    ) -> io::Result<QueuePair<UD, INIT>> {
        modify_init_to_init(self, pkey_index, port_num, None, qkey)
    }
}

#[derive(Clone, Copy)]
pub struct AltPath {
    alt_ah_attr: ffi::ibv_ah_attr,
    alt_pkey_index: u16,
    alt_port_num: u8,
    alt_timeout: u8,
}

// INIT -> RTR
impl QueuePair<UC, INIT> {
    pub fn modify_to_rtr(
        self,
        ah_attr: ffi::ibv_ah_attr,
        path_mtu: u32,
        dest_qpn: u32,
        rq_psn: u32,
        pkey_index: Option<u16>,
        access: Option<ffi::ibv_access_flags>,
        alt_path: Option<AltPath>,
    ) -> io::Result<QueuePair<UC, RTR>> {
        modify_init_to_rtr(
            self,
            Some(ah_attr),
            Some(path_mtu),
            Some(dest_qpn),
            Some(rq_psn),
            None, // max_dest_rd_atomic
            None, // min_rnr_timer
            pkey_index,
            access.map(|x| x.0),
            None, // qkey (optional fields for UD only)
            alt_path,
        )
    }
}

impl QueuePair<RC, INIT> {
    pub fn modify_to_rtr(
        self,
        ah_attr: ffi::ibv_ah_attr,
        path_mtu: u32,
        dest_qpn: u32,
        rq_psn: u32,
        max_dest_rd_atomic: u8,
        min_rnr_timer: u8,
        pkey_index: Option<u16>,
        access: Option<ffi::ibv_access_flags>,
        alt_path: Option<AltPath>,
    ) -> io::Result<QueuePair<RC, RTR>> {
        modify_init_to_rtr(
            self,
            Some(ah_attr),
            Some(path_mtu),
            Some(dest_qpn),
            Some(rq_psn),
            Some(max_dest_rd_atomic),
            Some(min_rnr_timer),
            pkey_index,
            access.map(|x| x.0),
            None, // qkey (optional fields for UD only)
            alt_path,
        )
    }
}

impl QueuePair<UD, INIT> {
    pub fn modify_to_rtr(
        self,
        pkey_index: Option<u16>,
        qkey: Option<u32>,
    ) -> io::Result<QueuePair<UD, RTR>> {
        modify_init_to_rtr(
            self, None, None, None, None, None, None, pkey_index, None, qkey, None,
        )
    }
}

// RTR -> RTS
impl QueuePair<UC, RTR> {
    pub fn modify_to_rts(
        self,
        sq_psn: u32,
        cur_qp_state: Option<ffi::ibv_qp_state::Type>,
        access: Option<ffi::ibv_access_flags>,
        alt_path: Option<AltPath>,
        path_mig_state: Option<ffi::ibv_mig_state>,
    ) -> io::Result<QueuePair<UC, RTS>> {
        modify_rtr_to_rts(
            self,
            Some(sq_psn),
            None,
            None,
            None,
            None,
            cur_qp_state,
            access.map(|x| x.0),
            path_mig_state,
            None, // min_rnr_timer
            None, // qkey
            alt_path,
        )
    }
}

impl QueuePair<RC, RTR> {
    pub fn modify_to_rts(
        self,
        sq_psn: u32,
        timeout: u8,
        retry_cnt: u8,
        rnr_retry: u8,
        max_rd_atomic: u8,
        cur_qp_state: Option<ffi::ibv_qp_state::Type>,
        access: Option<ffi::ibv_access_flags>,
        min_rnr_timer: Option<u8>,
        alt_path: Option<AltPath>,
        path_mig_state: Option<ffi::ibv_mig_state>,
    ) -> io::Result<QueuePair<RC, RTS>> {
        modify_rtr_to_rts(
            self,
            Some(sq_psn),
            Some(timeout),
            Some(retry_cnt),
            Some(rnr_retry),
            Some(max_rd_atomic),
            cur_qp_state,
            access.map(|x| x.0),
            path_mig_state,
            min_rnr_timer,
            None, // qkey
            alt_path,
        )
    }
}

impl QueuePair<UD, RTR> {
    pub fn modify_to_rts(
        self,
        sq_psn: u32,
        cur_qp_state: Option<ffi::ibv_qp_state::Type>,
        qkey: Option<u32>,
    ) -> io::Result<QueuePair<UD, RTS>> {
        modify_rtr_to_rts(
            self,
            Some(sq_psn),
            None,
            None,
            None,
            None,
            cur_qp_state,
            None,
            None,
            None,
            qkey,
            None,
        )
    }
}

// RTS -> RTS
impl QueuePair<UC, RTS> {
    pub fn modify_to_rts(
        self,
        cur_qp_state: Option<ffi::ibv_qp_state::Type>,
        access: Option<ffi::ibv_access_flags>,
        alt_path: Option<AltPath>,
        path_mig_state: Option<ffi::ibv_mig_state>,
    ) -> io::Result<QueuePair<UC, RTS>> {
        modify_rts_to_rts(
            self,
            cur_qp_state,
            access.map(|x| x.0),
            path_mig_state,
            None, // min_rnr_timer
            None, // qkey
            alt_path,
        )
    }
}

impl QueuePair<RC, RTS> {
    pub fn modify_to_rts(
        self,
        cur_qp_state: Option<ffi::ibv_qp_state::Type>,
        access: Option<ffi::ibv_access_flags>,
        min_rnr_timer: Option<u8>,
        alt_path: Option<AltPath>,
        path_mig_state: Option<ffi::ibv_mig_state>,
    ) -> io::Result<QueuePair<RC, RTS>> {
        modify_rts_to_rts(
            self,
            cur_qp_state,
            access.map(|x| x.0),
            path_mig_state,
            min_rnr_timer,
            None, // qkey
            alt_path,
        )
    }
}

impl QueuePair<UD, RTS> {
    pub fn modify_to_rts(
        self,
        cur_qp_state: Option<ffi::ibv_qp_state::Type>,
        qkey: Option<u32>,
    ) -> io::Result<QueuePair<UD, RTS>> {
        modify_rts_to_rts(self, cur_qp_state, None, None, None, qkey, None)
    }
}

struct WorkRequestInner {
    wr_id: u64,
    wc: OnceLock<ffi::ibv_wc>,
    waker: OnceLock<Waker>,
}

pub struct SendRequest {
    qp: Arc<QueuePairInner>,
    wr_key: usize,
    _ms: Vec<MemorySegmentOpaque<RO>>,
}

pub struct RecvRequest {
    qp: Arc<QueuePairInner>,
    wr_key: usize,
    _ms: Vec<MemorySegmentOpaque<RW>>,
}

impl Future for SendRequest {
    type Output = ffi::ibv_wc;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // NOTE(cjr): Do not use the slab.take because it may block.
        // Simply get and remove would be nicer here.

        // `wr_key`` should exists otherwise something is wrong
        let inner = self.qp.send_cq.0.wr_slab.get(self.wr_key).unwrap();
        if let Some(wc) = inner.wc.get() {
            // NOTE: Now wr_id must be the wr_key in the slab
            let removed = self.qp.send_cq.0.wr_slab.remove(self.wr_key);
            assert!(removed);
            Poll::Ready(*wc)
        } else {
            // The inner waker should only be set by one thread
            inner.waker.get_or_init(|| cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Future for RecvRequest {
    type Output = ffi::ibv_wc;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // NOTE(cjr): Do not use the slab.take because it may block.
        // Simply get and remove would be nicer here.

        // `wr_key`` should exists otherwise something is wrong
        let inner = self.qp.recv_cq.0.wr_slab.get(self.wr_key).unwrap();
        if let Some(wc) = inner.wc.get() {
            // NOTE: Now wr_id must be the wr_key in the slab
            let removed = self.qp.recv_cq.0.wr_slab.remove(self.wr_key);
            assert!(removed);
            Poll::Ready(*wc)
        } else {
            // The inner waker should only be set by one thread
            inner.waker.get_or_init(|| cx.waker().clone());
            Poll::Pending
        }
    }
}

// Data-verbs
macro_rules! impl_post_recv {
    ($qp_state: ident) => {
        impl<T: ToQpType> QueuePair<T, $qp_state> {
            #[inline]
            pub fn post_recv<V>(
                &self,
                ms: &MemorySegment<V, RW>,
                wr_id: u64,
            ) -> io::Result<RecvRequest> {
                let ms = vec![ms.get_opaque()];
                self.post_recv_scatter(&ms, wr_id)
            }

            #[inline]
            pub fn post_recv_scatter(
                &self,
                ms: &[MemorySegmentOpaque<RW>],
                wr_id: u64,
            ) -> io::Result<RecvRequest> {
                let inner = WorkRequestInner {
                    wr_id,
                    wc: OnceLock::new(),
                    waker: OnceLock::new(),
                };
                // Panics if too many items in the slab.
                let wr_key = self.inner.recv_cq.0.wr_slab.insert(inner).unwrap();
                unsafe {
                    self.inner.qp.post_recv_scatter(ms, wr_key as u64)?;
                }
                let wr = RecvRequest {
                    qp: Arc::clone(&self.inner),
                    wr_key,
                    _ms: ms.to_vec(),
                };
                Ok(wr)
            }
        }
    };
}

impl_post_recv!(RTR);
impl_post_recv!(RTS);

impl<T: ToQpType> QueuePair<T, RTS> {
    #[inline]
    pub fn post_send<V>(&self, ms: &MemorySegment<V, RO>, wr_id: u64) -> io::Result<SendRequest> {
        let ms = vec![ms.get_opaque()];
        self.post_send_gather(&ms, wr_id)
    }

    #[inline]
    pub fn post_send_gather(
        &self,
        ms: &[MemorySegmentOpaque<RO>],
        wr_id: u64,
    ) -> io::Result<SendRequest> {
        let inner = WorkRequestInner {
            wr_id,
            wc: OnceLock::new(),
            waker: OnceLock::new(),
        };
        // Panics if too many items in the slab.
        let wr_key = self.inner.send_cq.0.wr_slab.insert(inner).unwrap();
        unsafe {
            self.inner.qp.post_send_gather(
                ms,
                wr_key as u64,
                ffi::ibv_send_flags::IBV_SEND_SIGNALED,
            )?;
        }
        let wr = SendRequest {
            qp: Arc::clone(&self.inner),
            wr_key,
            _ms: ms.to_vec(),
        };
        Ok(wr)
    }

    /// Post a send request without generating a completion.
    ///
    /// # Safety
    ///
    /// This API posts a send request to the NIC, without generating a completion, so there
    /// is no direct way to know when the operation will be finished.
    /// Therefore, the user must ensure the relevant QP, MemorySegment, and any other relevant
    /// resources are not dropped before the completion of the operation.
    #[inline]
    pub unsafe fn post_send_gather_unsignaled(
        &self,
        ms: &[MemorySegmentOpaque<RO>],
        wr_id: u64,
    ) -> io::Result<()> {
        self.inner
            .qp
            .post_send_gather(ms, wr_id, ffi::ibv_send_flags(0))?;
        Ok(())
    }

    /// Post a batch of work request, only the last wr generating a completion.
    ///
    /// # Safety
    ///
    /// The user must ensure all the memory regions/segments, QPs, and relevant resources are
    /// valid and remain unchanged before the completion of the last work request in the batch.
    #[inline]
    pub unsafe fn post_send_batch(&self, sr: &mut WorkRequestBatch) -> io::Result<SendRequest> {
        assert!(sr.enclosed());
        let inner = WorkRequestInner {
            wr_id: sr.wr_id().expect("Empty wr_list"),
            wc: OnceLock::new(),
            waker: OnceLock::new(),
        };
        // Panics if too many items in the slab.
        let wr_key = self.inner.send_cq.0.wr_slab.insert(inner).unwrap();
        sr.set_wr_id(wr_key as u64);
        unsafe {
            self.inner.qp.post_send_batch(sr.to_ibv_send_wr())?;
        }
        let wr = SendRequest {
            qp: Arc::clone(&self.inner),
            wr_key,
            _ms: vec![],
        };
        Ok(wr)
    }
}

pub struct WorkRequestBatch {
    wr_list: Vec<Box<ffi::ibv_send_wr>>,
    sglists: Vec<Vec<ffi::ibv_sge>>,
}

unsafe impl Send for WorkRequestBatch {}
unsafe impl Sync for WorkRequestBatch {}

impl WorkRequestBatch {
    #[inline]
    pub const fn new() -> Self {
        WorkRequestBatch {
            wr_list: Vec::new(),
            sglists: Vec::new(),
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        WorkRequestBatch {
            wr_list: Vec::with_capacity(capacity),
            sglists: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn wr_id(&self) -> Option<u64> {
        self.wr_list
            .last()
            .map(|last| last.wr_id)
    }

    fn set_wr_id(&mut self, wr_id: u64) {
        if let Some(last) = self.wr_list.last_mut() {
            last.wr_id = wr_id;
        }
    }

    #[inline]
    pub fn prepost_send(
        &mut self,
        ms: &[MemorySegmentOpaque<RO>],
        wr_id: u64,
        flags: ffi::ibv_send_flags,
    ) {
        let mut sglist = Vec::with_capacity(ms.len());
        for m in ms {
            let slice = &m.mr.data.as_ref()[m.range.start..m.range.end];
            sglist.push(ffi::ibv_sge {
                addr: slice.as_ptr() as u64,
                length: mem::size_of_val(slice) as u32,
                lkey: m.mr.lkey(),
            });
        }
        let wr = Box::new(ffi::ibv_send_wr {
            wr_id,
            next: ptr::null_mut(),
            sg_list: sglist.as_mut_ptr(),
            num_sge: sglist.len() as i32,
            opcode: ffi::ibv_wr_opcode::IBV_WR_SEND,
            send_flags: flags.0,
            ..Default::default()
        });
        self.push(wr, sglist);
    }

    #[inline]
    pub fn prepost_write(
        &mut self,
        ms: &[MemorySegmentOpaque<RO>],
        remote_memory: &RemoteMemory,
        wr_id: u64,
        flags: ffi::ibv_send_flags,
    ) {
        let mut sglist = Vec::with_capacity(ms.len());
        for m in ms {
            let slice = &m.mr.data.as_ref()[m.range.start..m.range.end];
            sglist.push(ffi::ibv_sge {
                addr: slice.as_ptr() as u64,
                length: mem::size_of_val(slice) as u32,
                lkey: m.mr.lkey(),
            });
        }
        let wr = Box::new(ffi::ibv_send_wr {
            wr_id,
            next: ptr::null_mut(),
            sg_list: sglist.as_mut_ptr(),
            num_sge: sglist.len() as i32,
            opcode: ffi::ibv_wr_opcode::IBV_WR_RDMA_WRITE,
            send_flags: flags.0,
            wr: ffi::ibv_send_wr__bindgen_ty_2 {
                rdma: ffi::ibv_send_wr__bindgen_ty_2__bindgen_ty_1 {
                    remote_addr: remote_memory.addr,
                    rkey: remote_memory.rkey.key,
                },
            },
            ..Default::default()
        });
        self.push(wr, sglist);
    }

    fn push(&mut self, mut wr: Box<ffi::ibv_send_wr>, sglist: Vec<ffi::ibv_sge>) {
        if let Some(last) = self.wr_list.last_mut() {
            last.send_flags &= !ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
            last.next = &raw mut *wr;
        }

        self.wr_list.push(wr);
        self.sglists.push(sglist);
    }

    #[inline]
    pub fn clear(&mut self) {
        self.wr_list.clear();
        self.sglists.clear();
    }

    #[inline]
    pub fn enclose(&mut self) {
        self.wr_list
            .last_mut()
            .expect("Empty wr_list")
            .send_flags |= ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
    }

    fn enclosed(&self) -> bool {
        self.wr_list.last().expect("Empty wr_list").send_flags
            & ffi::ibv_send_flags::IBV_SEND_SIGNALED.0
            != 0
    }

    unsafe fn to_ibv_send_wr(&mut self) -> &mut ffi::ibv_send_wr {
        &mut *self.wr_list.first_mut().expect("Empty wr_list")
    }
}

impl<T: ConnectionOrientedQpType> QueuePair<T, RTS> {
    #[inline]
    pub fn post_write_gather(
        &self,
        ms: &[MemorySegmentOpaque<RO>],
        remote_memory: &RemoteMemory,
        wr_id: u64,
    ) -> io::Result<SendRequest> {
        let inner = WorkRequestInner {
            wr_id,
            wc: OnceLock::new(),
            waker: OnceLock::new(),
        };
        // Panics if too many items in the slab.
        let wr_key = self.inner.send_cq.0.wr_slab.insert(inner).unwrap();
        unsafe {
            self.inner.qp.post_write_gather(
                ms,
                remote_memory,
                wr_key as u64,
                ffi::ibv_send_flags::IBV_SEND_SIGNALED,
            )?;
        }
        let wr = SendRequest {
            qp: Arc::clone(&self.inner),
            wr_key,
            _ms: ms.to_vec(),
        };
        Ok(wr)
    }

    /// Post a send request without generating a completion.
    ///
    /// # Safety
    ///
    /// This API posts a send request to the NIC, without generating a completion, so there
    /// is no direct way to know when the operation will be finished.
    /// Therefore, the user must ensure the relevant QP, MemorySegment, and any other relevant
    /// resources are not dropped before the completion of the operation.
    #[inline]
    pub unsafe fn post_write_gather_unsignaled(
        &self,
        ms: &[MemorySegmentOpaque<RO>],
        remote_memory: &RemoteMemory,
        wr_id: u64,
    ) -> io::Result<()> {
        self.inner
            .qp
            .post_write_gather(ms, remote_memory, wr_id, ffi::ibv_send_flags(0))?;
        Ok(())
    }
}

struct OwnedMemoryRegionOpaque {
    mr: *mut ffi::ibv_mr,
    data: Box<[u8]>,
    intervals: Mutex<IntervalTree>,
}

unsafe impl Send for OwnedMemoryRegionOpaque {}
unsafe impl Sync for OwnedMemoryRegionOpaque {}

impl Drop for OwnedMemoryRegionOpaque {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_dereg_mr(self.mr) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{}", e);
        }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy)]
pub struct RemoteMemory {
    pub addr: u64,
    pub rkey: RemoteKey,
}

pub struct MemoryRegion<T> {
    inner: Arc<OwnedMemoryRegionOpaque>,
    _marker: PhantomData<T>,
}

impl<T> Clone for MemoryRegion<T> {
    fn clone(&self) -> Self {
        MemoryRegion {
            inner: Arc::clone(&self.inner),
            _marker: PhantomData,
        }
    }
}

pub struct MemorySegmentOpaque<P: 'static> {
    mr: Arc<OwnedMemoryRegionOpaque>,
    // Range in bytes
    range: Range<usize>,
    _permission: PhantomData<P>,
}

impl<P> Clone for MemorySegmentOpaque<P> {
    fn clone(&self) -> Self {
        MemorySegmentOpaque {
            mr: Arc::clone(&self.mr),
            range: self.range.clone(),
            _permission: PhantomData,
        }
    }
}

impl<P: 'static> Drop for MemorySegmentOpaque<P> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.mr) == 1 {
            let mut intervals = self.mr.intervals.lock().unwrap();
            intervals.remove::<P>(&self.range).unwrap();
        }
    }
}

impl OwnedMemoryRegionOpaque {
    /// Get the remote authentication key used to allow direct remote access to this memory region.
    fn rkey(&self) -> RemoteKey {
        RemoteKey {
            key: unsafe { &*self.mr }.rkey,
        }
    }

    /// Get the local key.
    fn lkey(&self) -> u32 {
        unsafe { &*self.mr }.lkey
    }

    /// Returns a `MemorySegment` that may overlap with other `MemorySegment`s.
    ///
    /// # Safety
    ///
    /// The user must ensure a read-write memory segment never overlaps with any other memory segemnt,
    /// whether they are read-only or read-write.
    ///
    /// The user must also ensure the memory region has proper alignment for T. Other than that, `byte_range`
    /// must also be within the memory region.
    #[inline]
    unsafe fn get_unchecked<T, P: Permission>(
        self: Arc<Self>,
        byte_range: Range<usize>,
    ) -> MemorySegment<T, P> {
        MemorySegment {
            opaque: MemorySegmentOpaque {
                mr: self,
                range: byte_range,
                _permission: PhantomData,
            },
            _marker: PhantomData,
        }
    }

    #[inline]
    fn try_insert_readonly(&self, range: Range<usize>) -> Result<(), ()> {
        let mut intervals = self.intervals.lock().unwrap();
        intervals.try_insert_readonly(range)
    }

    #[inline]
    fn try_insert_readwrite(&self, range: Range<usize>) -> Result<(), ()> {
        let mut intervals = self.intervals.lock().unwrap();
        intervals.try_insert_readwrite(range)
    }
}

pub struct MemorySegment<T, P: 'static> {
    opaque: MemorySegmentOpaque<P>,
    _marker: PhantomData<T>,
}

pub struct RW;
pub struct RO;

mod sealed {
    pub trait Sealed {}
}
use sealed::Sealed;
pub trait Permission: 'static + Sealed {}
impl Sealed for RO {}
impl Sealed for RW {}
impl Permission for RO {}
impl Permission for RW {}

// COMMENT(cjr): Keep this in case I forget: when post_send/recv holds references,
// it is okay to impl Drop for the high-level MmeorySegment<T, P> instead of impl Drop
// for the underlying type MemorySegmentOpaque<P>; when post_send/recv does not hold references
// but instead, it increments the reference counts to the input arguments, the impl Drop
// should only be implemented to the underlying type.
// impl<T, P: 'static> Drop for MemorySegment<T, P> {
//     fn drop(&mut self) {
//         let mut intervals = self.opaque.mr.intervals.lock().unwrap();
//         intervals.remove::<P>(&self.opaque.range).unwrap();
//     }
// }

impl<T> AsRef<[T]> for MemorySegment<T, RO> {
    fn as_ref(&self) -> &[T] {
        self.deref().as_ref()
    }
}

impl<T> Deref for MemorySegment<T, RO> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        // SAFETY: data must have correct alignment for T.
        // This is guaranteed because OwnedMemoryRegionOpaque is created
        // by OwnedMemoryRegion<T>.
        unsafe {
            slice::from_raw_parts(
                self.opaque
                    .mr
                    .data
                    .as_ptr()
                    .byte_offset(self.opaque.range.start as isize)
                    .cast(),
                (self.opaque.range.end - self.opaque.range.start) / mem::size_of::<T>(),
            )
        }
    }
}

// MemorySegment<T, RW> only implements AsMut<[T]>.
impl<T> AsMut<[T]> for MemorySegment<T, RW> {
    /// Creates a mutable slice from MemorySegment<T, O>.
    fn as_mut(&mut self) -> &mut [T] {
        // SAFETY: MemorySegment<T, RW> does not overlap with any other memory segment,
        // so it should be okay to obtain a mutable pointer regardless of how many
        // owners of self.opaque.mr.
        unsafe {
            slice::from_raw_parts_mut(
                self.opaque
                    .mr
                    .data
                    .as_ptr()
                    .cast_mut()
                    .byte_offset(self.opaque.range.start as isize)
                    .cast(),
                (self.opaque.range.end - self.opaque.range.start) / mem::size_of::<T>(),
            )
        }
    }
}

impl<T, P> MemorySegment<T, P> {
    #[inline]
    pub fn get_opaque(&self) -> MemorySegmentOpaque<P> {
        self.opaque.clone()
    }
}

impl<T> MemorySegment<T, RO> {
    /// Get the remote authentication key used to allow direct remote access to this memory region.
    #[inline]
    pub fn rkey(&self) -> RemoteKey {
        self.opaque.mr.rkey()
    }

    fn byte_range(range: Range<usize>) -> Range<usize> {
        Range {
            start: range.start * size_of::<T>(),
            end: range.end * size_of::<T>(),
        }
    }

    fn check_range(&self, range: &Range<usize>) {
        if range.start > range.end {
            mr_index_order_fail(range.start, range.end);
        } else if range.end * mem::size_of::<T>() > self.opaque.range.end {
            mr_index_len_fail(range.end, self.opaque.range.end / mem::size_of::<T>());
        }
    }

    /// To use with [`get_readonly_unchecked`] and [`try_insert_readonly`].
    #[inline]
    pub fn convert_to_range<R: RangeBounds<usize>>(&self, range: R) -> Range<usize> {
        use core::ops::Bound;
        let start = match range.start_bound() {
            Bound::Included(s) => *s,
            Bound::Excluded(_) => unreachable!(),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(e) => *e + 1,
            Bound::Excluded(e) => *e,
            Bound::Unbounded => self.opaque.range.end / mem::size_of::<T>(),
        };
        Range { start, end }
    }

    /// Returns a read-only `MemorySegment` that may overlap with other `MemorySegment`s.
    ///
    /// # Safety
    ///
    /// Refer to [`Self::get_unchecked`].
    #[inline]
    pub unsafe fn get_readonly_unchecked(&self, range: Range<usize>) -> MemorySegment<T, RO> {
        let byte_range = Self::byte_range(range);
        let range = Range {
            start: self.opaque.range.start + byte_range.start,
            end: self.opaque.range.start + byte_range.end,
        };
        Arc::clone(&self.opaque.mr).get_unchecked(range)
    }

    #[inline]
    pub fn insert_readonly(&self, range: Range<usize>) {
        self.opaque
            .mr
            .try_insert_readonly(range)
            .expect("This operation should never fail");
    }

    #[inline]
    pub fn get_readonly<R: RangeBounds<usize>>(&self, range: R) -> MemorySegment<T, RO> {
        let range = self.convert_to_range(range);
        self.check_range(&range);
        self.insert_readonly(range.clone());
        // SAFETY: This operation should never fail
        unsafe { self.get_readonly_unchecked(range) }
    }
}

impl<T> MemorySegment<T, RW> {
    #[inline]
    pub fn remote_memory(&self) -> RemoteMemory {
        RemoteMemory {
            addr: self.opaque.mr.data.as_ptr() as u64,
            rkey: self.opaque.mr.rkey(),
        }
    }

    /// Consume an read-write memory segment and make it read-only.
    #[inline]
    pub fn freeze(self) -> MemorySegment<T, RO> {
        // move rw_interval to ro_interval
        let range = self.opaque.range.clone();
        let mut intervals = self.opaque.mr.intervals.lock().unwrap();
        intervals.remove::<RW>(&range).unwrap();
        intervals.try_insert_readonly(range).unwrap();
        drop(intervals);
        // SAFETY: This is okay because they have the same layout.
        unsafe { mem::transmute(self) }
    }
}

impl<T> MemoryRegion<T> {
    /// Get the remote authentication key used to allow direct remote access to this memory region.
    #[inline]
    pub fn rkey(&self) -> RemoteKey {
        self.inner.rkey()
    }

    fn byte_range(range: Range<usize>) -> Range<usize> {
        Range {
            start: range.start * size_of::<T>(),
            end: range.end * size_of::<T>(),
        }
    }

    fn check_range(&self, range: &Range<usize>) {
        if range.start > range.end {
            mr_index_order_fail(range.start, range.end);
        } else if range.end * mem::size_of::<T>() > self.inner.data.len() {
            mr_index_len_fail(range.end, self.inner.data.len() / mem::size_of::<T>());
        }
    }

    #[inline]
    pub fn convert_to_range<R: RangeBounds<usize>>(&self, range: R) -> Range<usize> {
        use core::ops::Bound;
        let start = match range.start_bound() {
            Bound::Included(s) => *s,
            Bound::Excluded(_) => unreachable!(),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(e) => *e + 1,
            Bound::Excluded(e) => *e,
            Bound::Unbounded => self.inner.data.len() / mem::size_of::<T>(),
        };
        Range { start, end }
    }

    /// Returns a `MemorySegment` that may overlap with other `MemorySegment`s.
    ///
    /// # Safety
    ///
    /// The user must ensure a read-write memory segment never overlaps with any other memory segemnt,
    /// whether they are read-only or read-write.
    #[inline]
    pub unsafe fn get_unchecked<P: Permission>(&self, range: Range<usize>) -> MemorySegment<T, P> {
        Arc::clone(&self.inner).get_unchecked(Self::byte_range(range))
    }

    /// Returns a read-only `MemorySegment` that may overlap with other `MemorySegment`s.
    ///
    /// # Safety
    ///
    /// Refer to [`Self::get_unchecked`].
    #[inline]
    pub unsafe fn get_readonly_unchecked(&self, range: Range<usize>) -> MemorySegment<T, RO> {
        self.get_unchecked(range)
    }

    /// Returns a read-write `MemorySegment` that may overlap with other `MemorySegment`s.
    ///
    /// # Safety
    ///
    /// Refer to [`Self::get_unchecked`].
    #[inline]
    pub unsafe fn get_readwrite_unchecked(&self, range: Range<usize>) -> MemorySegment<T, RW> {
        self.get_unchecked(range)
    }

    #[inline]
    pub fn try_insert_readonly(&self, range: Range<usize>) -> Result<(), ()> {
        self.inner.try_insert_readonly(range)
    }

    #[inline]
    pub fn try_insert_readwrite(&self, range: Range<usize>) -> Result<(), ()> {
        self.inner.try_insert_readwrite(range)
    }

    #[inline]
    pub fn get_readonly<R: RangeBounds<usize>>(&self, range: R) -> Option<MemorySegment<T, RO>> {
        let range = self.convert_to_range(range);
        self.check_range(&range);
        self.try_insert_readonly(range.clone())
            .ok()
            .map(|_| unsafe { self.get_readonly_unchecked(range) })
    }

    #[inline]
    pub fn get_readwrite<R: RangeBounds<usize>>(&self, range: R) -> Option<MemorySegment<T, RW>> {
        let range = self.convert_to_range(range);
        self.check_range(&range);
        self.try_insert_readwrite(range.clone())
            .ok()
            .map(|_| unsafe { self.get_readwrite_unchecked(range) })
    }
}

#[inline(never)]
#[cold]
fn mr_index_order_fail(index: usize, end: usize) -> ! {
    panic!("MemoryRegion index starts at {} but ends at {}", index, end);
}

#[inline(never)]
#[cold]
fn mr_index_len_fail(index: usize, len: usize) -> ! {
    panic!(
        "index {} out of range for MemoryRegion of length {}",
        index, len
    );
}
