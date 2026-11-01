//! Smoke tests over the raw bindings on one RDMA device: an RC loopback (two
//! QPs of one device connected to each other) with SEND/RECV and RDMA WRITE,
//! and a probe of the extended verbs (thread domains, extended CQs and QPs,
//! `ibv_wr_*`, `ibv_start_poll`), which prints what the device supports.
//!
//! The tests pass without doing anything when the host has no RDMA device.
//! `scripts/rxe.sh up` creates a Soft-RoCE one. `SAFEVERBS_TEST_DEVICE` picks
//! a device by name (else an rxe device, else the first) and
//! `SAFEVERBS_TEST_GID_INDEX` a GID (else the RoCE v2 GID of an IPv4 address).
//! Run with `--nocapture` to see the probe's table.

use core::ffi::CStr;
use core::mem::zeroed;
use core::ptr::null_mut;
use std::io;
use std::time::{Duration, Instant};

use safeverbs_sys::*;

/// The port every test uses.
const PORT: u8 = 1;
/// Bytes in each half of the test buffer.
const LEN: usize = 4096;
/// Linux's `ENOENT`, which `ibv_start_poll` returns on an empty CQ.
const ENOENT: i32 = 2;

/// An open device with the port's attributes and the address QPs use.
struct Device {
    ctx: *mut ibv_context,
    name: String,
    port: ibv_port_attr,
    /// The GID index for RoCE, `None` on InfiniBand (addressed by LID).
    gid_index: Option<u8>,
    gid: ibv_gid,
}

impl Device {
    /// Opens the test device, or `None` when the host has no RDMA device.
    fn open() -> Option<Device> {
        unsafe {
            let mut n = 0;
            let list = ibv_get_device_list(&mut n);
            if list.is_null() || n == 0 {
                eprintln!("no RDMA device: skipped (scripts/rxe.sh up creates one)");
                if !list.is_null() {
                    ibv_free_device_list(list);
                }
                return None;
            }
            let devices = core::slice::from_raw_parts(list, n as usize);
            let name_of = |d: *mut ibv_device| {
                CStr::from_ptr(ibv_get_device_name(d))
                    .to_string_lossy()
                    .into_owned()
            };
            let wanted = std::env::var("SAFEVERBS_TEST_DEVICE").ok();
            let dev = match &wanted {
                Some(w) => devices.iter().copied().find(|&d| name_of(d) == *w),
                None => devices
                    .iter()
                    .copied()
                    .find(|&d| name_of(d).contains("rxe"))
                    .or(devices.first().copied()),
            }
            .unwrap_or_else(|| panic!("no RDMA device named {wanted:?}"));
            let name = name_of(dev);
            let ctx = ibv_open_device(dev);
            assert!(
                !ctx.is_null(),
                "ibv_open_device: {}",
                io::Error::last_os_error()
            );
            ibv_free_device_list(list);

            let mut port: ibv_port_attr = zeroed();
            assert_eq!(___ibv_query_port(ctx, PORT, &mut port), 0, "query_port");
            assert_eq!(
                port.state,
                ibv_port_state::IBV_PORT_ACTIVE,
                "port {PORT} of {name} is not active"
            );
            let (gid_index, gid) = if port.link_layer == IBV_LINK_LAYER_ETHERNET as u8 {
                let (i, gid) = roce_gid(ctx, &port);
                (Some(i), gid)
            } else {
                (None, zeroed())
            };
            Some(Device {
                ctx,
                name,
                port,
                gid_index,
                gid,
            })
        }
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        unsafe { ibv_close_device(self.ctx) };
    }
}

/// The GID to use on a RoCE port: `SAFEVERBS_TEST_GID_INDEX`, else the first
/// RoCE v2 GID holding an IPv4 address (`::ffff:a.b.c.d`).
unsafe fn roce_gid(ctx: *mut ibv_context, port: &ibv_port_attr) -> (u8, ibv_gid) {
    let wanted: Option<u32> = std::env::var("SAFEVERBS_TEST_GID_INDEX")
        .ok()
        .map(|s| s.parse().expect("SAFEVERBS_TEST_GID_INDEX"));
    for index in 0..port.gid_tbl_len as u32 {
        let mut entry: ibv_gid_entry = unsafe { zeroed() };
        if unsafe { ibv_query_gid_ex(ctx, PORT as u32, index, &mut entry, 0) } != 0 {
            continue;
        }
        let raw = unsafe { entry.gid.raw };
        let ipv4 = raw[..10].iter().all(|&b| b == 0) && raw[10..12] == [0xff, 0xff];
        let pick = match wanted {
            Some(w) => index == w,
            None => entry.gid_type == IBV_GID_TYPE_ROCE_V2 && ipv4,
        };
        if pick {
            return (index as u8, entry.gid);
        }
    }
    panic!("no usable GID (set SAFEVERBS_TEST_GID_INDEX)");
}

/// A protection domain with one registered buffer: the first half is what
/// is sent, the second where it lands.
struct Memory {
    pd: *mut ibv_pd,
    mr: *mut ibv_mr,
    buf: Box<[u8; 2 * LEN]>,
}

impl Memory {
    fn new(dev: &Device) -> Memory {
        unsafe {
            let pd = ibv_alloc_pd(dev.ctx);
            assert!(
                !pd.is_null(),
                "ibv_alloc_pd: {}",
                io::Error::last_os_error()
            );
            let mut buf = Box::new([0u8; 2 * LEN]);
            let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ;
            let mr = ibv_reg_mr(pd, buf.as_mut_ptr().cast(), buf.len(), access.0 as i32);
            assert!(!mr.is_null(), "ibv_reg_mr: {}", io::Error::last_os_error());
            Memory { pd, mr, buf }
        }
    }

    fn src(&self) -> u64 {
        self.buf.as_ptr() as u64
    }

    fn dst(&self) -> u64 {
        self.src() + LEN as u64
    }

    fn lkey(&self) -> u32 {
        unsafe { (*self.mr).lkey }
    }

    fn rkey(&self) -> u32 {
        unsafe { (*self.mr).rkey }
    }

    /// Fills the source half with a pattern and clears the destination half.
    fn reset(&mut self, seed: u8) {
        for (i, b) in self.buf[..LEN].iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(31).wrapping_add(seed);
        }
        self.buf[LEN..].fill(0);
    }

    /// Whether the first `len` bytes arrived. Read through a volatile view,
    /// since the device wrote them behind the compiler's back.
    fn arrived(&self, len: usize) -> bool {
        let base = self.buf.as_ptr();
        (0..len).all(|i| unsafe {
            core::ptr::read_volatile(base.add(LEN + i)) == core::ptr::read_volatile(base.add(i))
        })
    }
}

impl Drop for Memory {
    fn drop(&mut self) {
        let (mr, pd) = unsafe { (ibv_dereg_mr(self.mr), ibv_dealloc_pd(self.pd)) };
        // A failed test leaves its QPs behind, which keep these busy.
        if !std::thread::panicking() {
            assert_eq!((mr, pd), (0, 0), "dereg_mr, dealloc_pd");
        }
    }
}

fn qp_caps() -> ibv_qp_cap {
    ibv_qp_cap {
        max_send_wr: 16,
        max_recv_wr: 16,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
    }
}

/// Creates a plain RC QP on `pd` with both queues on `cq`.
unsafe fn create_rc_qp(pd: *mut ibv_pd, cq: *mut ibv_cq) -> *mut ibv_qp {
    let mut attr: ibv_qp_init_attr = unsafe { zeroed() };
    attr.send_cq = cq;
    attr.recv_cq = cq;
    attr.cap = qp_caps();
    attr.qp_type = ibv_qp_type::IBV_QPT_RC;
    attr.sq_sig_all = 0;
    let qp = unsafe { ibv_create_qp(pd, &mut attr) };
    assert!(
        !qp.is_null(),
        "ibv_create_qp: {}",
        io::Error::last_os_error()
    );
    qp
}

/// Moves `qp` through INIT, RTR and RTS, connected to `remote_qpn` on the
/// same device.
unsafe fn connect(dev: &Device, qp: *mut ibv_qp, remote_qpn: u32) {
    use ibv_qp_attr_mask as M;
    let modify = |attr: &mut ibv_qp_attr, mask: ibv_qp_attr_mask, step: &str| {
        let rc = unsafe { ibv_modify_qp(qp, attr, mask.0 as i32) };
        assert_eq!(
            rc,
            0,
            "modify_qp to {step}: {}",
            io::Error::from_raw_os_error(rc)
        );
    };

    let mut attr: ibv_qp_attr = unsafe { zeroed() };
    attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = PORT;
    attr.qp_access_flags = (ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ)
        .0;
    modify(
        &mut attr,
        M::IBV_QP_STATE | M::IBV_QP_PKEY_INDEX | M::IBV_QP_PORT | M::IBV_QP_ACCESS_FLAGS,
        "INIT",
    );

    let mut attr: ibv_qp_attr = unsafe { zeroed() };
    attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
    attr.path_mtu = dev.port.active_mtu;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.port_num = PORT;
    match dev.gid_index {
        Some(index) => {
            attr.ah_attr.is_global = 1;
            attr.ah_attr.grh.dgid = dev.gid;
            attr.ah_attr.grh.sgid_index = index;
            attr.ah_attr.grh.hop_limit = 1;
        }
        None => attr.ah_attr.dlid = dev.port.lid,
    }
    modify(
        &mut attr,
        M::IBV_QP_STATE
            | M::IBV_QP_AV
            | M::IBV_QP_PATH_MTU
            | M::IBV_QP_DEST_QPN
            | M::IBV_QP_RQ_PSN
            | M::IBV_QP_MAX_DEST_RD_ATOMIC
            | M::IBV_QP_MIN_RNR_TIMER,
        "RTR",
    );

    let mut attr: ibv_qp_attr = unsafe { zeroed() };
    attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;
    modify(
        &mut attr,
        M::IBV_QP_STATE
            | M::IBV_QP_TIMEOUT
            | M::IBV_QP_RETRY_CNT
            | M::IBV_QP_RNR_RETRY
            | M::IBV_QP_SQ_PSN
            | M::IBV_QP_MAX_QP_RD_ATOMIC,
        "RTS",
    );
}

/// Posts a receive of `len` bytes into the destination half.
unsafe fn post_recv(qp: *mut ibv_qp, mem: &Memory, len: usize, wr_id: u64) {
    let mut sge = ibv_sge {
        addr: mem.dst(),
        length: len as u32,
        lkey: mem.lkey(),
    };
    let mut wr: ibv_recv_wr = unsafe { zeroed() };
    wr.wr_id = wr_id;
    wr.sg_list = &mut sge;
    wr.num_sge = 1;
    let mut bad = null_mut();
    let rc = unsafe { ibv_post_recv(qp, &mut wr, &mut bad) };
    assert_eq!(rc, 0, "ibv_post_recv: {}", io::Error::from_raw_os_error(rc));
}

/// Posts a signaled SEND, or an RDMA WRITE into the destination half, of
/// `len` bytes from the source half.
unsafe fn post_send(qp: *mut ibv_qp, mem: &Memory, len: usize, wr_id: u64, write: bool) {
    let mut sge = ibv_sge {
        addr: mem.src(),
        length: len as u32,
        lkey: mem.lkey(),
    };
    let mut wr: ibv_send_wr = unsafe { zeroed() };
    wr.wr_id = wr_id;
    wr.sg_list = &mut sge;
    wr.num_sge = 1;
    wr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
    if write {
        wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
        wr.wr.rdma.remote_addr = mem.dst();
        wr.wr.rdma.rkey = mem.rkey();
    } else {
        wr.opcode = ibv_wr_opcode::IBV_WR_SEND;
    }
    let mut bad = null_mut();
    let rc = unsafe { ibv_post_send(qp, &mut wr, &mut bad) };
    assert_eq!(rc, 0, "ibv_post_send: {}", io::Error::from_raw_os_error(rc));
}

/// Polls `cq` until `n` completions arrive (two seconds at most) and returns
/// them.
unsafe fn poll(cq: *mut ibv_cq, n: usize) -> Vec<ibv_wc> {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut out = Vec::new();
    while out.len() < n {
        let mut wc: ibv_wc = unsafe { zeroed() };
        let got = unsafe { ibv_poll_cq(cq, 1, &mut wc) };
        assert!(got >= 0, "ibv_poll_cq failed");
        if got == 1 {
            assert_eq!(
                wc.status,
                ibv_wc_status::IBV_WC_SUCCESS,
                "completion {} failed: {}",
                wc.wr_id,
                unsafe { CStr::from_ptr(ibv_wc_status_str(wc.status)) }.to_string_lossy()
            );
            out.push(wc);
        }
        assert!(
            Instant::now() < deadline,
            "timed out with {} of {n} completions",
            out.len()
        );
    }
    out
}

#[test]
fn rc_loopback_send_recv_and_write() {
    let Some(dev) = Device::open() else { return };
    let mut mem = Memory::new(&dev);
    unsafe {
        let cq = ibv_create_cq(dev.ctx, 32, null_mut(), null_mut(), 0);
        assert!(
            !cq.is_null(),
            "ibv_create_cq: {}",
            io::Error::last_os_error()
        );
        let a = create_rc_qp(mem.pd, cq);
        let b = create_rc_qp(mem.pd, cq);
        connect(&dev, a, (*b).qp_num);
        connect(&dev, b, (*a).qp_num);
        println!(
            "{}: QP {} <-> QP {}, GID index {:?}, MTU enum {}",
            dev.name,
            (*a).qp_num,
            (*b).qp_num,
            dev.gid_index,
            dev.port.active_mtu
        );

        // SEND from a, RECV on b.
        mem.reset(1);
        post_recv(b, &mem, LEN, 10);
        post_send(a, &mem, LEN, 11, false);
        let wcs = poll(cq, 2);
        let recv = wcs
            .iter()
            .find(|wc| wc.wr_id == 10)
            .expect("recv completion");
        assert_eq!(recv.opcode, ibv_wc_opcode::IBV_WC_RECV);
        assert_eq!(recv.byte_len as usize, LEN);
        assert_eq!(recv.qp_num, (*b).qp_num);
        assert!(mem.arrived(LEN), "SEND payload differs");
        println!("SEND/RECV of {LEN} bytes: ok");

        // RDMA WRITE from a into the same buffer through b.
        mem.reset(2);
        post_send(a, &mem, LEN, 12, true);
        let wcs = poll(cq, 1);
        assert_eq!(wcs[0].wr_id, 12);
        assert_eq!(wcs[0].opcode, ibv_wc_opcode::IBV_WC_RDMA_WRITE);
        assert!(mem.arrived(LEN), "RDMA WRITE payload differs");
        println!("RDMA WRITE of {LEN} bytes: ok");

        assert_eq!(ibv_destroy_qp(a), 0);
        assert_eq!(ibv_destroy_qp(b), 0);
        assert_eq!(ibv_destroy_cq(cq), 0);
    }
}

/// What one probe found: `Ok` with a note, or the error.
type Outcome = Result<String, String>;

fn errno() -> String {
    io::Error::last_os_error().to_string()
}

/// The outcome of a verb that returns a pointer, null on failure.
fn made<T>(object: *mut T) -> Outcome {
    if object.is_null() {
        Err(errno())
    } else {
        Ok(String::new())
    }
}

fn code(rc: i32) -> String {
    io::Error::from_raw_os_error(rc).to_string()
}

#[test]
fn probe_extended_verbs() {
    let Some(dev) = Device::open() else { return };
    let mut mem = Memory::new(&dev);
    let mut table: Vec<(&str, Outcome)> = Vec::new();
    unsafe {
        // Thread domain and a parent domain over it.
        let mut td_attr = ibv_td_init_attr { comp_mask: 0 };
        let td = ibv_alloc_td(dev.ctx, &mut td_attr);
        table.push(("ibv_alloc_td", made(td)));
        let mut pd_attr: ibv_parent_domain_init_attr = zeroed();
        pd_attr.pd = mem.pd;
        pd_attr.td = td;
        let parent = ibv_alloc_parent_domain(dev.ctx, &mut pd_attr);
        table.push((
            "ibv_alloc_parent_domain",
            if parent.is_null() {
                Err(errno())
            } else if td.is_null() {
                Ok("without a thread domain".into())
            } else {
                Ok("over the thread domain".into())
            },
        ));

        // Extended CQ, single threaded.
        let mut cq_attr: ibv_cq_init_attr_ex = zeroed();
        cq_attr.cqe = 32;
        cq_attr.wc_flags = IBV_WC_STANDARD_FLAGS as u64;
        cq_attr.comp_mask = ibv_cq_init_attr_mask::IBV_CQ_INIT_ATTR_MASK_FLAGS.0;
        cq_attr.flags = ibv_create_cq_attr_flags::IBV_CREATE_CQ_ATTR_SINGLE_THREADED.0;
        let mut cqx = ibv_create_cq_ex(dev.ctx, &mut cq_attr);
        table.push(("ibv_create_cq_ex SINGLE_THREADED", made(cqx)));
        if cqx.is_null() {
            cq_attr.comp_mask = 0;
            cq_attr.flags = 0;
            cqx = ibv_create_cq_ex(dev.ctx, &mut cq_attr);
            table.push(("ibv_create_cq_ex", made(cqx)));
        }
        // On mlx5 a CQ on a parent domain with a thread domain drops its lock.
        if !parent.is_null() {
            let mut attr = cq_attr;
            attr.comp_mask |= ibv_cq_init_attr_mask::IBV_CQ_INIT_ATTR_MASK_PD.0;
            attr.parent_domain = parent;
            let cq = ibv_create_cq_ex(dev.ctx, &mut attr);
            table.push(("ibv_create_cq_ex on the parent domain", made(cq)));
            if !cq.is_null() {
                assert_eq!(ibv_destroy_cq(ibv_cq_ex_to_cq(cq)), 0);
            }
        }
        let send_cq = if cqx.is_null() {
            ibv_create_cq(dev.ctx, 32, null_mut(), null_mut(), 0)
        } else {
            ibv_cq_ex_to_cq(cqx)
        };
        assert!(!send_cq.is_null(), "no send CQ: {}", errno());
        let recv_cq = ibv_create_cq(dev.ctx, 32, null_mut(), null_mut(), 0);
        assert!(!recv_cq.is_null(), "ibv_create_cq: {}", errno());

        // Extended QP with the ibv_wr_* send ops, on the parent domain when
        // there is one, else on the plain PD.
        let mut qp_attr: ibv_qp_init_attr_ex = zeroed();
        qp_attr.send_cq = send_cq;
        qp_attr.recv_cq = recv_cq;
        qp_attr.cap = qp_caps();
        qp_attr.qp_type = ibv_qp_type::IBV_QPT_RC;
        qp_attr.comp_mask = (ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_PD
            | ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_SEND_OPS_FLAGS)
            .0;
        qp_attr.send_ops_flags = (ibv_qp_create_send_ops_flags::IBV_QP_EX_WITH_SEND
            | ibv_qp_create_send_ops_flags::IBV_QP_EX_WITH_RDMA_WRITE)
            .0 as u64;
        let mut a = null_mut();
        if !parent.is_null() {
            qp_attr.pd = parent;
            a = ibv_create_qp_ex(dev.ctx, &mut qp_attr);
            table.push(("ibv_create_qp_ex on the parent domain", made(a)));
        }
        if a.is_null() {
            qp_attr.pd = mem.pd;
            a = ibv_create_qp_ex(dev.ctx, &mut qp_attr);
            table.push(("ibv_create_qp_ex with send ops", made(a)));
        }

        if !a.is_null() {
            let b = create_rc_qp(mem.pd, recv_cq);
            connect(&dev, a, (*b).qp_num);
            connect(&dev, b, (*a).qp_num);
            let qpx = ibv_qp_to_qp_ex(a);
            assert!(!qpx.is_null(), "ibv_qp_to_qp_ex");

            // A SEND and an RDMA WRITE posted in one batch.
            mem.reset(3);
            post_recv(b, &mem, LEN, 20);
            ibv_wr_start(qpx);
            (*qpx).wr_id = 21;
            (*qpx).wr_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
            ibv_wr_send(qpx);
            ibv_wr_set_sge(qpx, mem.lkey(), mem.src(), LEN as u32);
            (*qpx).wr_id = 22;
            (*qpx).wr_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
            ibv_wr_rdma_write(qpx, mem.rkey(), mem.dst());
            ibv_wr_set_sge(qpx, mem.lkey(), mem.src(), LEN as u32);
            let rc = ibv_wr_complete(qpx);
            table.push((
                "ibv_wr_start/send/rdma_write/set_sge/complete",
                if rc == 0 {
                    Ok(String::new())
                } else {
                    Err(code(rc))
                },
            ));
            if rc == 0 {
                let recv = poll(recv_cq, 1);
                assert_eq!(recv[0].wr_id, 20);
                if cqx.is_null() {
                    let ids: Vec<u64> = poll(send_cq, 2).iter().map(|wc| wc.wr_id).collect();
                    assert_eq!(ids, [21, 22]);
                } else {
                    let outcome = poll_ex(cqx, 2).and_then(|ids| {
                        if ids == [21, 22] {
                            Ok(String::new())
                        } else {
                            Err(format!("wrong completions, wr_id {ids:?}"))
                        }
                    });
                    table.push(("ibv_start_poll/next_poll/end_poll", outcome));
                }
                assert!(mem.arrived(LEN), "payload differs");
            }
            assert_eq!(ibv_destroy_qp(b), 0);
            assert_eq!(ibv_destroy_qp(a), 0);
        }

        assert_eq!(ibv_destroy_cq(recv_cq), 0);
        assert_eq!(ibv_destroy_cq(send_cq), 0);
        if !parent.is_null() {
            assert_eq!(ibv_dealloc_pd(parent), 0);
        }
        if !td.is_null() {
            assert_eq!(ibv_dealloc_td(td), 0);
        }
    }

    println!("extended verbs on {}:", dev.name);
    for (name, outcome) in &table {
        match outcome {
            Ok(note) if note.is_empty() => println!("  {name:48} yes"),
            Ok(note) => println!("  {name:48} yes ({note})"),
            Err(e) => println!("  {name:48} no ({e})"),
        }
    }
}

/// Polls an extended CQ with `ibv_start_poll`, `ibv_next_poll` and
/// `ibv_end_poll` until `n` completions arrive; returns their `wr_id`s.
unsafe fn poll_ex(cq: *mut ibv_cq_ex, n: usize) -> Result<Vec<u64>, String> {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut ids = Vec::new();
    while ids.len() < n {
        let mut attr = ibv_poll_cq_attr { comp_mask: 0 };
        let rc = unsafe { ibv_start_poll(cq, &mut attr) };
        if rc == ENOENT {
            assert!(
                Instant::now() < deadline,
                "timed out polling the extended CQ"
            );
            continue;
        }
        if rc != 0 {
            return Err(format!("ibv_start_poll: {}", code(rc)));
        }
        loop {
            let status = unsafe { (*cq).status };
            if status != ibv_wc_status::IBV_WC_SUCCESS {
                unsafe { ibv_end_poll(cq) };
                return Err(format!("completion status {status}"));
            }
            ids.push(unsafe { (*cq).wr_id });
            if ids.len() == n || unsafe { ibv_next_poll(cq) } != 0 {
                break;
            }
        }
        unsafe { ibv_end_poll(cq) };
    }
    Ok(ids)
}
