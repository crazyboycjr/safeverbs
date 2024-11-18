#![allow(non_camel_case_types)]
#![allow(clippy::too_many_arguments)]

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
    fn to_qp_type() -> QpType;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RC;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UC;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UD;

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
            qp_type: T::to_qp_type().0,
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
    /// Bioperlate code to `ibv_modify_qp`.
    unsafe fn modify<D: ToQpState>(
        self,
        attr: *mut ffi::ibv_qp_attr,
        mask: ffi::ibv_qp_attr_mask,
    ) -> io::Result<QueuePair<T, D>> {
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, attr, mask.0 as i32) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(QueuePair {
            qp: self.qp,
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
                qp_state: ffi::ibv_qp_state::IBV_QPS_RTR,
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

// fn modify_init_to_rtr2<T: ToQpType>(
//     qp: QueuePair<T, INIT>,
//     ah_attr: ffi::ibv_ah_attr,
//     path_mtu: u32,
//     dest_qpn: u32,
//     rq_psn: u32,
//     max_dest_rd_atomic: Option<u8>,
//     min_rnr_timer: Option<u8>,
//     pkey_index: Option<u16>,
//     access: Option<ffi::ibv_access_flags>,
//     alt_path: Option<AltPath>,
// ) -> io::Result<QueuePair<T, RTR>> {
//     let mut attr = ffi::ibv_qp_attr {
//         qp_state: ffi::ibv_qp_state::IBV_QPS_RTR,
//         ..Default::default()
//     };
//     let mut mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE;
//     if is::<T, RC>() {
//         if let Some(max_dest_rd_atomic) = max_dest_rd_atomic {
//             attr.max_dest_rd_atomic = max_dest_rd_atomic;
//             mask |= ffi::ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC;
//         }
//     }
//     // SAFETY: FFI call. The FFI object obtained from a valid QueuePair object is
//     // guaranteed to be valid.
//     unsafe { qp.modify(&mut attr, mask) }
// }

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
