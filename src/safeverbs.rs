#![allow(non_camel_case_types)]

use crate::{ffi, ibv};
use core::any::TypeId;
use core::mem;
use core::ptr;
use std::io;
use std::marker::PhantomData;
use std::sync::Arc;

// Re-exports
pub use ibv::{devices, Device, DeviceList, DeviceListIter, Gid, Guid, QueuePairEndpoint};

#[derive(Clone)]
pub struct Context {
    ctx: Arc<ibv::Context>,
}

impl AsRef<ibv::Context> for Context {
    fn as_ref(&self) -> &ibv::Context {
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
        Ok(CompletionQueue {
            cq: Arc::new(cq),
            _ctx: self.clone(),
        })
    }

    pub fn alloc_pd(&self) -> io::Result<ProtectionDomain> {
        let inner = self.ctx.alloc_pd()?;
        // SAFETY: This is safe as long as `cq: Arc<ibv::ProtectionDomain<'static>>` cannot
        // be constructed from the outside by any means.
        let pd: ibv::ProtectionDomain<'static> = unsafe { mem::transmute(inner) };
        Ok(ProtectionDomain {
            pd: Arc::new(pd),
            _ctx: self.clone(),
        })
    }
}

#[derive(Clone)]
pub struct CompletionQueue {
    cq: Arc<ibv::CompletionQueue<'static>>,
    _ctx: Context,
}

impl<'a> AsRef<ibv::CompletionQueue<'a>> for CompletionQueue {
    fn as_ref(&self) -> &ibv::CompletionQueue<'a> {
        &self.cq
    }
}

#[derive(Clone)]
pub struct ProtectionDomain {
    pd: Arc<ibv::ProtectionDomain<'static>>,
    _ctx: Context,
}

impl<'a> AsRef<ibv::ProtectionDomain<'a>> for ProtectionDomain {
    fn as_ref(&self) -> &ibv::ProtectionDomain<'a> {
        &self.pd
    }
}

// NOTE(cjr): Perhaps I need both the builder pattern and the type state pattern.

impl ProtectionDomain {
    pub fn create_qp<T: ToQpType>(
        &self,
        qp_init_attr: QpInitAttr<T>,
    ) -> io::Result<QueuePair<T, RESET>> {
        let mut attr = qp_init_attr.to_ibv_qp_init_attr();
        // SAFETY: FFI calls, FFI object (PD) obtained from a safe object is
        // guaranteed to be valid. ibv_qp_init_attr is also valid because we
        // take the reference from QpInitAttr.
        let qp = unsafe { OwnedQueuePair::create(self.pd.pd(), &mut attr) }?;
        Ok(QueuePair {
            qp: Arc::new(qp),
            _marker: PhantomData,
        })
    }
}

/// The type of QP used for communication.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QpType(pub ffi::ibv_qp_type::Type);

pub trait ToQpType: 'static {
    fn as_qp_type() -> QpType;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RC;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UC;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UD;

impl ToQpType for RC {
    fn as_qp_type() -> QpType {
        QpType(ffi::ibv_qp_type::IBV_QPT_RC)
    }
}
impl ToQpType for UC {
    fn as_qp_type() -> QpType {
        QpType(ffi::ibv_qp_type::IBV_QPT_UC)
    }
}
impl ToQpType for UD {
    fn as_qp_type() -> QpType {
        QpType(ffi::ibv_qp_type::IBV_QPT_UD)
    }
}

/// QP capabilities.
#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
pub struct QpCapability(pub ffi::ibv_qp_cap);

/// The attributes to initialize a QP.
pub struct QpInitAttr<T: ToQpType> {
    /// Associated user context of the QP.
    pub qp_context: usize,
    /// CQ to be associated with the Send Queue (SQ).
    pub send_cq: Option<CompletionQueue>,
    /// CQ to be associated with the Receive Queue (RQ).
    pub recv_cq: Option<CompletionQueue>,
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
            send_cq: self
                .send_cq
                .as_ref()
                .map_or(ptr::null_mut(), |cq| cq.cq.cq()),
            recv_cq: self
                .recv_cq
                .as_ref()
                .map_or(ptr::null_mut(), |cq| cq.cq.cq()),
            srq: ptr::null_mut(),
            cap: self.cap.0,
            qp_type: T::as_qp_type().0,
            sq_sig_all: self.sq_sig_all as i32,
        }
    }
}

/// The state of QP used for communication.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QpState(pub ffi::ibv_qp_state::Type);

pub trait ToQpState: 'static {
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

// #[derive(Clone)]
// pub struct QueuePairBuilder<T: ToQpType> {
//     builder: Arc<ibv::QueuePairBuilder<'static>>,
//     _pd: ProtectionDomain,
//     _send_cq: CompletionQueue,
//     _recv_cq: CompletionQueue,
//     _marker: PhantomData<T>,
// }

// impl<T: ToQpType> QueuePairBuilder<T> {
//     pub fn new(
//         pd: ProtectionDomain,
//         send_cq: CompletionQueue,
//         max_send_wr: u32,
//         recv_cq: CompletionQueue,
//         max_recv_wr: u32,
//     ) -> QueuePairBuilder<T> {
//         let inner = ibv::QueuePairBuilder::new(pd.as_ref(), send_cq.as_ref(), max_send_wr, recv_cq.as_ref(), max_recv_wr, T::as_ibv_qp_type());
//         todo!()
//     }
// }

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

pub struct QueuePair<T: ToQpType, S: ToQpState> {
    qp: Arc<OwnedQueuePair>,
    _marker: PhantomData<(T, S)>,
}

impl<T: ToQpType, S: ToQpState> QueuePair<T, S> {
    pub fn modify_to_reset(self) -> io::Result<QueuePair<T, RESET>> {
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RESET,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE;
        // SAFETY: FFI call. The FFI object obtained from a valid QueuePair object is
        // guaranteed to be valid.
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr, mask.0 as i32) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(QueuePair {
            qp: self.qp,
            _marker: PhantomData,
        })
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

    if TypeId::of::<T>() == TypeId::of::<RC>() || TypeId::of::<T>() == TypeId::of::<UC>() {
        attr.qp_access_flags = access.expect("RC or UC must set ACCESS_FLAGS").0;
        mask |= ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
    }
    if TypeId::of::<T>() == TypeId::of::<UD>() {
        attr.qkey = qkey.expect("UD must set qkey");
        mask |= ffi::ibv_qp_attr_mask::IBV_QP_QKEY;
    }
    // SAFETY: FFI call. The FFI object obtained from a valid QueuePair object is
    // guaranteed to be valid.
    let errno = unsafe { ffi::ibv_modify_qp(qp.qp.qp, &mut attr, mask.0 as i32) };
    if errno != 0 {
        return Err(io::Error::from_raw_os_error(errno));
    }
    Ok(QueuePair {
        qp: qp.qp,
        _marker: PhantomData,
    })
}

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
