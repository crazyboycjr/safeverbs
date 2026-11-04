//! The real NIC: [`Nic`] over libibverbs.
//!
//! QPs are created with `ibv_create_qp_ex` and post through the extended
//! work request API (`ibv_wr_start`, `ibv_wr_*`, `ibv_wr_complete`); CQs with
//! `ibv_create_cq_ex` and are polled with `ibv_start_poll`, `ibv_next_poll`
//! and `ibv_end_poll`. On the data path the `static inline` helpers of
//! `verbs.h` are reimplemented here as calls through the provider's function
//! pointers in `ibv_qp_ex` and `ibv_cq_ex`, which is all they do.
//!
//! An engine asks for a thread domain (`ibv_alloc_td`, [`VerbsTd`]) and, for each protection
//! domain of its QPs, a parent domain over both ([`VerbsDomain`]), on which mlx5 drops the locks
//! of the QPs created there and gives them a doorbell of their own, which they share; on devices
//! without thread domains (rxe) its QPs are plain ones. CQs polled by one thread at a time are
//! created `IBV_CREATE_CQ_ATTR_SINGLE_THREADED`.

use core::ffi::CStr;
use core::mem::zeroed;
use core::ptr::{self, null_mut};

use safeverbs_sys as ffi;

use crate::error::{Error, Result};
use crate::nic::{
    Access, ConnectAttr, DeviceCaps, Endpoint, Gid, GidEntry, Nic, NicCq, NicQp, PortInfo, QpCaps,
    QpType, Sge, WrOp,
};
use crate::wc::{Opcode, Status, Wc};

/// Linux's `ENOENT`: `ibv_start_poll` on an empty CQ.
const ENOENT: i32 = 2;

/// The names of the RDMA devices of this host.
pub fn devices() -> Result<Vec<String>> {
    // SAFETY: the list is read before it is freed; names are copied.
    unsafe {
        let mut n = 0;
        let list = ffi::ibv_get_device_list(&mut n);
        if list.is_null() {
            return Err(Error::last_os_error());
        }
        let mut names = Vec::with_capacity(n.max(0) as usize);
        let mut i = 0;
        while i < n.max(0) as usize {
            let dev = *list.add(i);
            names.push(
                CStr::from_ptr(ffi::ibv_get_device_name(dev))
                    .to_string_lossy()
                    .into_owned(),
            );
            i += 1;
        }
        ffi::ibv_free_device_list(list);
        Ok(names)
    }
}

/// An open RDMA device (`struct ibv_context`).
pub struct Verbs {
    ctx: *mut ffi::ibv_context,
    name: String,
    caps: DeviceCaps,
}

// SAFETY: libibverbs contexts may be used from any thread.
unsafe impl Send for Verbs {}
// SAFETY: as above; every method takes `&self` and the verbs it calls are
// thread safe.
unsafe impl Sync for Verbs {}

impl Verbs {
    /// Opens the device called `name`, or the first device if `None`.
    pub fn open(name: Option<&str>) -> Result<Verbs> {
        // SAFETY: the device pointer is used before the list is freed; the
        // context outlives it.
        unsafe {
            let mut n = 0;
            let list = ffi::ibv_get_device_list(&mut n);
            if list.is_null() {
                return Err(Error::last_os_error());
            }
            let mut found = null_mut();
            let mut found_name = String::new();
            let mut i = 0;
            while i < n.max(0) as usize {
                let dev = *list.add(i);
                let dev_name = CStr::from_ptr(ffi::ibv_get_device_name(dev))
                    .to_string_lossy()
                    .into_owned();
                if name.is_none() || name == Some(dev_name.as_str()) {
                    found = dev;
                    found_name = dev_name;
                    break;
                }
                i += 1;
            }
            if found.is_null() {
                ffi::ibv_free_device_list(list);
                return Err(Error::NoDevice);
            }
            let ctx = ffi::ibv_open_device(found);
            let open_error = Error::last_os_error();
            ffi::ibv_free_device_list(list);
            if ctx.is_null() {
                return Err(open_error);
            }
            let mut attr: ffi::ibv_device_attr = zeroed();
            let rc = ffi::ibv_query_device(ctx, &mut attr);
            if rc != 0 {
                ffi::ibv_close_device(ctx);
                return Err(Error::Os(rc));
            }
            // The longest message every port carries.
            let mut max_msg = u32::MAX;
            let mut port = 1;
            while port <= attr.phys_port_cnt {
                let mut pattr: ffi::ibv_port_attr = zeroed();
                if ffi::___ibv_query_port(ctx, port, &mut pattr) == 0 {
                    max_msg = max_msg.min(pattr.max_msg_sz);
                }
                port += 1;
            }
            let caps = DeviceCaps {
                max_qp_wr: attr.max_qp_wr.max(0) as u32,
                max_sge: attr.max_sge.max(0) as u32,
                max_cqe: attr.max_cqe.max(0) as u32,
                max_qp_rd_atom: attr.max_qp_rd_atom.max(0) as u32,
                atomics: attr.atomic_cap != ffi::IBV_ATOMIC_NONE,
                max_msg,
            };
            Ok(Verbs {
                ctx,
                name: found_name,
                caps,
            })
        }
    }
}

impl Drop for Verbs {
    fn drop(&mut self) {
        // SAFETY: every resource of the device holds it alive (the safe
        // wrappers keep an `Arc` of it), so none is left.
        unsafe { ffi::ibv_close_device(self.ctx) };
    }
}

/// A protection domain (`struct ibv_pd`).
pub struct VerbsPd {
    pd: *mut ffi::ibv_pd,
}

// SAFETY: a PD is a handle the verbs use from any thread.
unsafe impl Send for VerbsPd {}
// SAFETY: as above.
unsafe impl Sync for VerbsPd {}

impl Drop for VerbsPd {
    fn drop(&mut self) {
        // SAFETY: its regions and QPs are gone (the wrappers keep the order).
        unsafe { ffi::ibv_dealloc_pd(self.pd) };
    }
}

/// A registered memory region (`struct ibv_mr`); closed explicitly or on drop.
/// Drop aborts if deregistration fails: this handle cannot retain the caller's
/// backing memory. Owners that recover from close failure must retain both.
pub struct VerbsMr {
    /// Null only after successful deregistration.
    mr: *mut ffi::ibv_mr,
}

// SAFETY: a region is a handle; its keys are read only.
unsafe impl Send for VerbsMr {}
// SAFETY: as above.
unsafe impl Sync for VerbsMr {}

impl Drop for VerbsMr {
    fn drop(&mut self) {
        if Verbs::close_mr(self).is_err() {
            // Returning or unwinding could free the caller's backing memory
            // while its registration is still live. The owner must intercept
            // a recoverable close failure before dropping this handle.
            std::process::abort();
        }
    }
}

/// A completion queue (`struct ibv_cq_ex`).
pub struct VerbsCq {
    cq: *mut ffi::ibv_cq_ex,
}

// SAFETY: a CQ may move between threads; `poll` takes `&mut self`.
unsafe impl Send for VerbsCq {}

impl Drop for VerbsCq {
    fn drop(&mut self) {
        // SAFETY: its QPs are destroyed first (the QP owns its CQs and drops
        // them after itself).
        unsafe { ffi::ibv_destroy_cq(ffi::ibv_cq_ex_to_cq(self.cq)) };
    }
}

impl NicCq for VerbsCq {
    #[inline]
    fn poll(&mut self, out: &mut [Wc]) -> usize {
        if out.is_empty() {
            return 0;
        }
        let cq = self.cq;
        // SAFETY: what `ibv_start_poll`, `ibv_next_poll`, `ibv_end_poll` and
        // the `ibv_wc_read_*` helpers do, through the provider's pointers,
        // which an extended CQ always sets.
        unsafe {
            let mut attr = ffi::ibv_poll_cq_attr { comp_mask: 0 };
            let rc = ((*cq).start_poll.unwrap_unchecked())(cq, &mut attr);
            if rc != 0 {
                debug_assert!(rc == ENOENT, "ibv_start_poll: {rc}");
                return 0;
            }
            let mut n = 0;
            loop {
                let status = Status((*cq).status);
                let mut wc = Wc {
                    wr_id: (*cq).wr_id,
                    status,
                    opcode: Opcode(0),
                    byte_len: 0,
                    imm: None,
                    vendor_err: 0,
                };
                if status.is_ok() {
                    wc.opcode = Opcode(((*cq).read_opcode.unwrap_unchecked())(cq));
                    if wc.opcode.has_byte_len() {
                        wc.byte_len = ((*cq).read_byte_len.unwrap_unchecked())(cq);
                    }
                    if wc.opcode.is_receive() {
                        let flags = ((*cq).read_wc_flags.unwrap_unchecked())(cq);
                        if flags & ffi::ibv_wc_flags::IBV_WC_WITH_IMM.0 != 0 {
                            wc.imm =
                                Some(u32::from_be(((*cq).read_imm_data.unwrap_unchecked())(cq)));
                        }
                    }
                } else {
                    wc.vendor_err = ((*cq).read_vendor_err.unwrap_unchecked())(cq);
                }
                out[n] = wc;
                n += 1;
                if n == out.len() || ((*cq).next_poll.unwrap_unchecked())(cq) != 0 {
                    break;
                }
            }
            ((*cq).end_poll.unwrap_unchecked())(cq);
            n
        }
    }
}

/// A thread domain (`struct ibv_td`).
pub struct VerbsTd {
    td: *mut ffi::ibv_td,
}

// SAFETY: the domain is a handle; it moves with the QPs created on it,
// which are used by one thread at a time.
unsafe impl Send for VerbsTd {}

impl Drop for VerbsTd {
    fn drop(&mut self) {
        // SAFETY: the parent domains over it and the QPs on them are gone (the engine drops
        // them first).
        unsafe { ffi::ibv_dealloc_td(self.td) };
    }
}

/// A parent domain over a PD and a thread domain (the `struct ibv_pd` of
/// `ibv_alloc_parent_domain`).
pub struct VerbsDomain {
    parent: *mut ffi::ibv_pd,
}

// SAFETY: as for the thread domain.
unsafe impl Send for VerbsDomain {}

impl Drop for VerbsDomain {
    fn drop(&mut self) {
        // SAFETY: the QPs created on it are gone (the engine drops them first); it goes
        // before its PD and its thread domain.
        unsafe { ffi::ibv_dealloc_pd(self.parent) };
    }
}

/// A queue pair (`struct ibv_qp_ex`), possibly on a thread domain.
pub struct VerbsQp {
    qp: *mut ffi::ibv_qp,
    qpx: *mut ffi::ibv_qp_ex,
    /// Whether it was created on a thread domain.
    lock_free: bool,
    qp_type: QpType,
}

// SAFETY: a QP may move between threads; posting takes `&mut self`.
unsafe impl Send for VerbsQp {}

impl Drop for VerbsQp {
    fn drop(&mut self) {
        // SAFETY: a live QP, destroyed once; its domain outlives it.
        unsafe {
            ffi::ibv_destroy_qp(self.qp);
        }
    }
}

impl VerbsQp {
    fn modify(&mut self, attr: &mut ffi::ibv_qp_attr, mask: ffi::ibv_qp_attr_mask) -> Result<()> {
        // SAFETY: a live QP and a filled attribute.
        let rc = unsafe { ffi::ibv_modify_qp(self.qp, attr, mask.0 as i32) };
        if rc == 0 { Ok(()) } else { Err(Error::Os(rc)) }
    }
}

impl NicQp for VerbsQp {
    fn qp_num(&self) -> u32 {
        // SAFETY: a live QP.
        unsafe { (*self.qp).qp_num }
    }

    fn is_lock_free(&self) -> bool {
        self.lock_free
    }

    fn connect(&mut self, a: &ConnectAttr, remote: &Endpoint) -> Result<()> {
        use ffi::ibv_qp_attr_mask as M;
        let rc = self.qp_type == QpType::Rc;

        // SAFETY (all three): zeroed attributes are valid C structs.
        let mut attr: ffi::ibv_qp_attr = unsafe { zeroed() };
        attr.qp_state = ffi::ibv_qp_state::IBV_QPS_INIT;
        attr.pkey_index = 0;
        attr.port_num = a.port;
        attr.qp_access_flags = a.access.0;
        self.modify(
            &mut attr,
            M::IBV_QP_STATE | M::IBV_QP_PKEY_INDEX | M::IBV_QP_PORT | M::IBV_QP_ACCESS_FLAGS,
        )?;

        let mut attr: ffi::ibv_qp_attr = unsafe { zeroed() };
        attr.qp_state = ffi::ibv_qp_state::IBV_QPS_RTR;
        attr.path_mtu = a.path_mtu;
        attr.dest_qp_num = remote.qp_num;
        attr.rq_psn = a.rq_psn;
        attr.ah_attr.port_num = a.port;
        attr.ah_attr.dlid = remote.lid;
        if let Some(gid) = remote.gid {
            attr.ah_attr.is_global = 1;
            attr.ah_attr.grh.dgid.raw = gid.0;
            attr.ah_attr.grh.sgid_index = a.gid_index.unwrap_or(0);
            attr.ah_attr.grh.hop_limit = 0xff;
            attr.ah_attr.grh.traffic_class = a.traffic_class;
        }
        let mut mask = M::IBV_QP_STATE
            | M::IBV_QP_AV
            | M::IBV_QP_PATH_MTU
            | M::IBV_QP_DEST_QPN
            | M::IBV_QP_RQ_PSN;
        if rc {
            attr.max_dest_rd_atomic = a.max_dest_rd_atomic;
            attr.min_rnr_timer = a.min_rnr_timer;
            mask = mask | M::IBV_QP_MAX_DEST_RD_ATOMIC | M::IBV_QP_MIN_RNR_TIMER;
        }
        self.modify(&mut attr, mask)?;

        let mut attr: ffi::ibv_qp_attr = unsafe { zeroed() };
        attr.qp_state = ffi::ibv_qp_state::IBV_QPS_RTS;
        attr.sq_psn = a.sq_psn;
        let mut mask = M::IBV_QP_STATE | M::IBV_QP_SQ_PSN;
        if rc {
            attr.timeout = a.timeout;
            attr.retry_cnt = a.retry_count;
            attr.rnr_retry = a.rnr_retry;
            attr.max_rd_atomic = a.max_rd_atomic;
            mask = mask
                | M::IBV_QP_TIMEOUT
                | M::IBV_QP_RETRY_CNT
                | M::IBV_QP_RNR_RETRY
                | M::IBV_QP_MAX_QP_RD_ATOMIC;
        }
        self.modify(&mut attr, mask)
    }

    fn to_error(&mut self) -> Result<()> {
        // SAFETY: a zeroed attribute is a valid C struct.
        let mut attr: ffi::ibv_qp_attr = unsafe { zeroed() };
        attr.qp_state = ffi::ibv_qp_state::IBV_QPS_ERR;
        self.modify(&mut attr, ffi::ibv_qp_attr_mask::IBV_QP_STATE)
    }

    #[inline]
    unsafe fn start(&mut self) {
        let q = self.qpx;
        // SAFETY: `ibv_wr_start`; the pointer is set for extended QPs.
        unsafe { ((*q).wr_start.unwrap_unchecked())(q) };
    }

    #[inline]
    unsafe fn post(&mut self, wr_id: u64, signaled: bool, op: &WrOp, sges: &[Sge]) {
        let q = self.qpx;
        // SAFETY: the `ibv_wr_*` helpers; the QP was created with the send
        // ops its transport allows (READ and atomics only on RC, which the
        // types above enforce), so their pointers are set. The caller keeps
        // the memory valid.
        unsafe {
            (*q).wr_id = wr_id;
            (*q).wr_flags = if signaled {
                ffi::ibv_send_flags::IBV_SEND_SIGNALED.0
            } else {
                0
            };
            match *op {
                WrOp::Send => ((*q).wr_send.unwrap_unchecked())(q),
                WrOp::SendImm(imm) => ((*q).wr_send_imm.unwrap_unchecked())(q, imm.to_be()),
                WrOp::Write(r) => ((*q).wr_rdma_write.unwrap_unchecked())(q, r.rkey, r.addr),
                WrOp::WriteImm(r, imm) => {
                    ((*q).wr_rdma_write_imm.unwrap_unchecked())(q, r.rkey, r.addr, imm.to_be())
                }
                WrOp::Read(r) => ((*q).wr_rdma_read.unwrap_unchecked())(q, r.rkey, r.addr),
                WrOp::FetchAdd { remote, add } => {
                    ((*q).wr_atomic_fetch_add.unwrap_unchecked())(q, remote.rkey, remote.addr, add)
                }
                WrOp::CompareSwap {
                    remote,
                    compare,
                    swap,
                } => ((*q).wr_atomic_cmp_swp.unwrap_unchecked())(
                    q,
                    remote.rkey,
                    remote.addr,
                    compare,
                    swap,
                ),
            }
            if sges.len() == 1 {
                ((*q).wr_set_sge.unwrap_unchecked())(q, sges[0].lkey, sges[0].addr, sges[0].length);
            } else {
                // `Sge` has the layout of `struct ibv_sge` (checked below).
                ((*q).wr_set_sge_list.unwrap_unchecked())(q, sges.len(), sges.as_ptr().cast());
            }
        }
    }

    #[inline]
    unsafe fn complete(&mut self) -> Result<()> {
        let q = self.qpx;
        // SAFETY: `ibv_wr_complete`.
        let rc = unsafe { ((*q).wr_complete.unwrap_unchecked())(q) };
        if rc == 0 { Ok(()) } else { Err(Error::Os(rc)) }
    }

    #[inline]
    unsafe fn abort(&mut self) {
        let q = self.qpx;
        // SAFETY: `ibv_wr_abort`.
        unsafe { ((*q).wr_abort.unwrap_unchecked())(q) };
    }

    #[inline]
    unsafe fn post_recv(&mut self, wr_id: u64, sges: &[Sge]) -> Result<()> {
        let mut wr = ffi::ibv_recv_wr {
            wr_id,
            next: null_mut(),
            sg_list: sges.as_ptr().cast_mut().cast(),
            num_sge: sges.len() as i32,
        };
        let mut bad = null_mut();
        // SAFETY: `ibv_post_recv` through the context's operations; the
        // provider only reads the list.
        let rc = unsafe {
            let ctx = (*self.qp).context;
            ((*ctx).ops.post_recv.unwrap_unchecked())(self.qp, &mut wr, &mut bad)
        };
        if rc == 0 { Ok(()) } else { Err(Error::Os(rc)) }
    }
}

const _: () = {
    assert!(core::mem::size_of::<Sge>() == core::mem::size_of::<ffi::ibv_sge>());
    assert!(core::mem::offset_of!(Sge, addr) == core::mem::offset_of!(ffi::ibv_sge, addr));
    assert!(core::mem::offset_of!(Sge, length) == core::mem::offset_of!(ffi::ibv_sge, length));
    assert!(core::mem::offset_of!(Sge, lkey) == core::mem::offset_of!(ffi::ibv_sge, lkey));
};

// SAFETY: the provider owns its verbs resources, reports the hardware's
// completions, and quiesces QPs before the caller releases their memory.
// The data-path methods preserve the provider's WQE and CQ ordering.
unsafe impl Nic for Verbs {
    type Pd = VerbsPd;
    type Mr = VerbsMr;
    type Cq = VerbsCq;
    type Qp = VerbsQp;
    type Td = VerbsTd;
    type Domain = VerbsDomain;

    fn name(&self) -> &str {
        &self.name
    }

    fn caps(&self) -> DeviceCaps {
        self.caps
    }

    fn port(&self, port: u8) -> Result<PortInfo> {
        // SAFETY: a live context and a zeroed C struct.
        unsafe {
            let mut attr: ffi::ibv_port_attr = zeroed();
            let rc = ffi::___ibv_query_port(self.ctx, port, &mut attr);
            if rc != 0 {
                return Err(Error::Os(rc));
            }
            Ok(PortInfo {
                lid: attr.lid,
                active_mtu: attr.active_mtu,
                ethernet: attr.link_layer == ffi::IBV_LINK_LAYER_ETHERNET as u8,
                active: attr.state == ffi::ibv_port_state::IBV_PORT_ACTIVE,
            })
        }
    }

    fn gids(&self, port: u8) -> Vec<GidEntry> {
        let mut out = Vec::new();
        // SAFETY: a live context and zeroed C structs.
        unsafe {
            let mut attr: ffi::ibv_port_attr = zeroed();
            if ffi::___ibv_query_port(self.ctx, port, &mut attr) != 0 {
                return out;
            }
            let mut index = 0;
            while index < attr.gid_tbl_len.clamp(0, 256) as u32 {
                let mut entry: ffi::ibv_gid_entry = zeroed();
                if ffi::ibv_query_gid_ex(self.ctx, port as u32, index, &mut entry, 0) == 0 {
                    out.push(GidEntry {
                        index: index as u8,
                        gid: Gid(entry.gid.raw),
                        roce_v2: entry.gid_type == ffi::IBV_GID_TYPE_ROCE_V2,
                    });
                }
                index += 1;
            }
        }
        out
    }

    fn alloc_pd(&self) -> Result<VerbsPd> {
        // SAFETY: a live context.
        let pd = unsafe { ffi::ibv_alloc_pd(self.ctx) };
        if pd.is_null() {
            Err(Error::last_os_error())
        } else {
            Ok(VerbsPd { pd })
        }
    }

    unsafe fn reg_mr(
        &self,
        pd: &VerbsPd,
        addr: *mut u8,
        len: usize,
        access: Access,
    ) -> Result<VerbsMr> {
        // SAFETY: the caller keeps the memory allocated while registered.
        let mr = unsafe { ffi::ibv_reg_mr(pd.pd, addr.cast(), len, access.0 as i32) };
        if mr.is_null() {
            Err(Error::last_os_error())
        } else {
            Ok(VerbsMr { mr })
        }
    }

    fn mr_keys(mr: &VerbsMr) -> (u32, u32) {
        assert!(!mr.mr.is_null(), "verbs: keys of a closed MR");
        // SAFETY: a live region.
        unsafe { ((*mr.mr).lkey, (*mr.mr).rkey) }
    }

    fn close_mr(mr: &mut VerbsMr) -> Result<()> {
        if mr.mr.is_null() {
            return Ok(());
        }
        // SAFETY: a live MR; its owner keeps its memory, PD and device alive,
        // and has retired local work using it, as for dropping the region.
        // rdma-core 58e40801, libibverbs/verbs.c and providers/{mlx5,rxe}:
        // dereg_mr returns errno on failure and retains the provider's MR.
        let rc = unsafe { ffi::ibv_dereg_mr(mr.mr) };
        if rc != 0 {
            return Err(Error::Os(rc));
        }
        mr.mr = null_mut();
        Ok(())
    }

    fn create_cq(&self, entries: u32, single_threaded: bool) -> Result<VerbsCq> {
        // SAFETY: a live context and a filled attribute.
        unsafe {
            let mut attr: ffi::ibv_cq_init_attr_ex = zeroed();
            attr.cqe = entries;
            attr.wc_flags = ffi::IBV_WC_STANDARD_FLAGS as u64;
            if single_threaded {
                attr.comp_mask = ffi::ibv_cq_init_attr_mask::IBV_CQ_INIT_ATTR_MASK_FLAGS.0;
                attr.flags = ffi::ibv_create_cq_attr_flags::IBV_CREATE_CQ_ATTR_SINGLE_THREADED.0;
            }
            let mut cq = ffi::ibv_create_cq_ex(self.ctx, &mut attr);
            if cq.is_null() && single_threaded {
                attr.comp_mask = 0;
                attr.flags = 0;
                cq = ffi::ibv_create_cq_ex(self.ctx, &mut attr);
            }
            if cq.is_null() {
                Err(Error::last_os_error())
            } else {
                Ok(VerbsCq { cq })
            }
        }
    }

    fn alloc_td(&self) -> Option<VerbsTd> {
        // SAFETY: a live context and a filled attribute.
        let td = unsafe {
            let mut attr = ffi::ibv_td_init_attr { comp_mask: 0 };
            ffi::ibv_alloc_td(self.ctx, &mut attr)
        };
        if td.is_null() {
            None
        } else {
            Some(VerbsTd { td })
        }
    }

    fn alloc_domain(&self, pd: &VerbsPd, td: &VerbsTd) -> Option<VerbsDomain> {
        // SAFETY: a live context, PD and thread domain, and a filled attribute.
        let parent = unsafe {
            let mut attr: ffi::ibv_parent_domain_init_attr = zeroed();
            attr.pd = pd.pd;
            attr.td = td.td;
            ffi::ibv_alloc_parent_domain(self.ctx, &mut attr)
        };
        if parent.is_null() {
            None
        } else {
            Some(VerbsDomain { parent })
        }
    }

    fn create_qp(
        &self,
        pd: &VerbsPd,
        domain: Option<&VerbsDomain>,
        send_cq: &VerbsCq,
        recv_cq: &VerbsCq,
        qp_type: QpType,
        caps: &QpCaps,
    ) -> Result<VerbsQp> {
        use ffi::ibv_qp_create_send_ops_flags as F;
        let mut ops = F::IBV_QP_EX_WITH_SEND
            | F::IBV_QP_EX_WITH_SEND_WITH_IMM
            | F::IBV_QP_EX_WITH_RDMA_WRITE
            | F::IBV_QP_EX_WITH_RDMA_WRITE_WITH_IMM;
        if qp_type == QpType::Rc {
            ops |= F::IBV_QP_EX_WITH_RDMA_READ;
            if self.caps.atomics {
                ops = ops
                    | F::IBV_QP_EX_WITH_ATOMIC_CMP_AND_SWP
                    | F::IBV_QP_EX_WITH_ATOMIC_FETCH_AND_ADD;
            }
        }
        // SAFETY: live resources and filled attributes; the QP is destroyed
        // on the failure path.
        unsafe {
            let mut attr: ffi::ibv_qp_init_attr_ex = zeroed();
            attr.send_cq = ffi::ibv_cq_ex_to_cq(send_cq.cq);
            attr.recv_cq = ffi::ibv_cq_ex_to_cq(recv_cq.cq);
            attr.srq = ptr::null_mut();
            attr.cap = ffi::ibv_qp_cap {
                max_send_wr: caps.max_send_wr,
                max_recv_wr: caps.max_recv_wr,
                max_send_sge: caps.max_send_sge,
                max_recv_sge: caps.max_recv_sge,
                max_inline_data: caps.max_inline_data,
            };
            attr.qp_type = match qp_type {
                QpType::Rc => ffi::ibv_qp_type::IBV_QPT_RC,
                QpType::Uc => ffi::ibv_qp_type::IBV_QPT_UC,
            };
            attr.sq_sig_all = 0;
            attr.comp_mask = (ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_PD
                | ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_SEND_OPS_FLAGS)
                .0;
            attr.send_ops_flags = ops.0 as u64;
            let mut lock_free = false;
            let mut qp = null_mut();
            if let Some(domain) = domain {
                attr.pd = domain.parent;
                qp = ffi::ibv_create_qp_ex(self.ctx, &mut attr);
                lock_free = !qp.is_null();
            }
            if qp.is_null() {
                attr.pd = pd.pd;
                qp = ffi::ibv_create_qp_ex(self.ctx, &mut attr);
            }
            if qp.is_null() {
                return Err(Error::last_os_error());
            }
            let qpx = ffi::ibv_qp_to_qp_ex(qp);
            if qpx.is_null() {
                ffi::ibv_destroy_qp(qp);
                return Err(Error::Unsupported("extended QP"));
            }
            Ok(VerbsQp {
                qp,
                qpx,
                lock_free,
                qp_type,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use core::mem::ManuallyDrop;

    use super::*;
    use crate::wc::{Completed, CompletionError, Received};

    const OPCODE: u32 = 1;
    const LENGTH: u32 = 2;
    const FLAGS: u32 = 4;
    const IMMEDIATE: u32 = 8;
    const VENDOR: u32 = 16;
    const END: u32 = 32;

    /// A provider whose read callbacks record every field accessed.
    #[repr(C)]
    struct TestCq {
        cq: ffi::ibv_cq_ex,
        opcode: Opcode,
        flags: u32,
        reads: u32,
    }

    impl TestCq {
        fn new(opcode: Opcode, status: Status, flags: u32) -> Self {
            TestCq {
                cq: ffi::ibv_cq_ex {
                    status: status.0,
                    wr_id: 42,
                    start_poll: Some(start),
                    next_poll: Some(next),
                    end_poll: Some(end),
                    read_opcode: Some(read_opcode),
                    read_byte_len: Some(read_length),
                    read_wc_flags: Some(read_flags),
                    read_imm_data: Some(read_immediate),
                    read_vendor_err: Some(read_vendor),
                    ..Default::default()
                },
                opcode,
                flags,
                reads: 0,
            }
        }

        fn poll(&mut self) -> Wc {
            // This is a Rust-owned test provider, not an allocated verbs
            // resource: its CQ must not call ibv_destroy_cq on drop.
            let mut cq = ManuallyDrop::new(VerbsCq { cq: &mut self.cq });
            let mut out = [Wc::default(); 2];
            assert_eq!(cq.poll(&mut out), 1);
            out[0]
        }
    }

    unsafe extern "C" fn start(_: *mut ffi::ibv_cq_ex, _: *mut ffi::ibv_poll_cq_attr) -> i32 {
        0
    }

    unsafe extern "C" fn next(_: *mut ffi::ibv_cq_ex) -> i32 {
        ENOENT
    }

    unsafe extern "C" fn end(cq: *mut ffi::ibv_cq_ex) {
        // SAFETY: repr(C) puts cq first in the live TestCq passed to poll.
        unsafe { (*cq.cast::<TestCq>()).reads |= END };
    }

    unsafe extern "C" fn read_opcode(cq: *mut ffi::ibv_cq_ex) -> u32 {
        // SAFETY: repr(C) puts cq first in the live TestCq passed to poll.
        let test = unsafe { &mut *cq.cast::<TestCq>() };
        test.reads |= OPCODE;
        test.opcode.0
    }

    unsafe extern "C" fn read_length(cq: *mut ffi::ibv_cq_ex) -> u32 {
        // SAFETY: repr(C) puts cq first in the live TestCq passed to poll.
        unsafe { (*cq.cast::<TestCq>()).reads |= LENGTH };
        4096
    }

    unsafe extern "C" fn read_flags(cq: *mut ffi::ibv_cq_ex) -> u32 {
        // SAFETY: repr(C) puts cq first in the live TestCq passed to poll.
        let test = unsafe { &mut *cq.cast::<TestCq>() };
        test.reads |= FLAGS;
        test.flags
    }

    unsafe extern "C" fn read_immediate(cq: *mut ffi::ibv_cq_ex) -> u32 {
        // SAFETY: repr(C) puts cq first in the live TestCq passed to poll.
        unsafe { (*cq.cast::<TestCq>()).reads |= IMMEDIATE };
        0x1234_5678u32.to_be()
    }

    unsafe extern "C" fn read_vendor(cq: *mut ffi::ibv_cq_ex) -> u32 {
        // SAFETY: repr(C) puts cq first in the live TestCq passed to poll.
        unsafe { (*cq.cast::<TestCq>()).reads |= VENDOR };
        7
    }

    #[test]
    fn poll_reads_only_defined_success_fields() {
        let with_imm = ffi::ibv_wc_flags::IBV_WC_WITH_IMM.0;
        for (opcode, has_len, recv) in [
            (Opcode::SEND, false, false),
            (Opcode::RDMA_WRITE, false, false),
            (Opcode::RDMA_READ, true, false),
            (Opcode::COMP_SWAP, true, false),
            (Opcode::FETCH_ADD, true, false),
            (Opcode::RECV, true, true),
            (Opcode::RECV_RDMA_WITH_IMM, true, true),
            (Opcode(u32::MAX), false, false),
        ] {
            for flags in [0, with_imm] {
                let mut provider = TestCq::new(opcode, Status::SUCCESS, flags);
                let wc = provider.poll();
                let imm = (flags != 0).then_some(0x1234_5678);
                assert_eq!(
                    wc.result(),
                    Ok(match opcode {
                        Opcode::RDMA_READ | Opcode::COMP_SWAP | Opcode::FETCH_ADD => {
                            Completed::Read { len: 4096 }
                        }
                        Opcode::RECV => Completed::Received(Received::Message { len: 4096, imm }),
                        Opcode::RECV_RDMA_WITH_IMM => {
                            Completed::Received(Received::WriteNotification {
                                imm: imm.unwrap_or(0),
                                len: 4096,
                            })
                        }
                        _ => Completed::Sent,
                    }),
                    "{opcode:?} flags {flags}"
                );
                assert_eq!((wc.wr_id, wc.vendor_err), (42, 0));
                let expected = OPCODE
                    | END
                    | if has_len { LENGTH } else { 0 }
                    | if recv { FLAGS } else { 0 }
                    | if recv && flags != 0 { IMMEDIATE } else { 0 };
                assert_eq!(provider.reads, expected, "{opcode:?} flags {flags}");
            }
        }
    }

    #[test]
    fn poll_errors_never_read_opcode_length_flags_or_immediate() {
        for status in (1..=21).map(Status).chain([Status(u32::MAX)]) {
            let mut provider = TestCq::new(Opcode::RECV, status, u32::MAX);
            let wc = provider.poll();
            assert_eq!(wc.wr_id, 42);
            assert_eq!(
                wc.result(),
                Err(CompletionError::Failed {
                    status,
                    vendor_err: 7,
                    position: 0
                })
            );
            assert_eq!(provider.reads, VENDOR | END);
        }
    }
}
