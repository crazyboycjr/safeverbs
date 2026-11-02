//! The NIC behind the queue pairs: a trait with two implementations, the
//! real one over libibverbs ([`crate::verbs::Verbs`]) and a simulated one in
//! pure Rust ([`crate::model::Model`]), so that everything above it can be
//! tested without hardware.
//!
//! The trait is implemented by the type of an open device (a context); its
//! associated types are the device's resources, each released when dropped.
//! Everything is monomorphized: there is no dynamic dispatch on the data
//! path. The data-path methods are `unsafe`: they hand the NIC raw addresses
//! that it reads or writes until the work request completes, which the
//! completion ring and the buffer leases above make sound.

use core::fmt;

use crate::error::{Error, Result};
use crate::wc::Wc;

/// A scatter/gather element: a range of registered memory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Sge {
    /// The address of its first byte.
    pub addr: u64,
    /// Its length in bytes.
    pub length: u32,
    /// The local key of the memory region holding it.
    pub lkey: u32,
}

/// A range of a peer's memory, as the peer exposed it: what RDMA READ, WRITE
/// and atomics address.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RemoteBuf {
    /// The address of its first byte in the peer's address space.
    pub addr: u64,
    /// Its length in bytes.
    pub len: u64,
    /// The remote key that grants access to it.
    pub rkey: u32,
}

impl RemoteBuf {
    /// The part starting `bytes` into this one, to its end.
    ///
    /// # Panics
    ///
    /// If `bytes` is past the end.
    pub fn offset(&self, bytes: u64) -> RemoteBuf {
        assert!(
            bytes <= self.len,
            "offset {bytes} past a remote buffer of {} bytes",
            self.len
        );
        RemoteBuf {
            addr: self.addr + bytes,
            len: self.len - bytes,
            rkey: self.rkey,
        }
    }

    /// The bytes `range` of this one.
    ///
    /// # Panics
    ///
    /// If the range is not within it.
    pub fn slice(&self, range: impl core::ops::RangeBounds<u64>) -> RemoteBuf {
        use core::ops::Bound;
        let start = match range.start_bound() {
            Bound::Included(&s) => s,
            Bound::Excluded(&s) => s + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&e) => e + 1,
            Bound::Excluded(&e) => e,
            Bound::Unbounded => self.len,
        };
        assert!(
            start <= end && end <= self.len,
            "range {start}..{end} outside a remote buffer of {} bytes",
            self.len
        );
        RemoteBuf {
            addr: self.addr + start,
            len: end - start,
            rkey: self.rkey,
        }
    }
}

impl fmt::Debug for RemoteBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RemoteBuf({:#x}+{}, rkey {:#x})",
            self.addr, self.len, self.rkey
        )
    }
}

/// What a send-queue work request does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WrOp {
    /// SEND: the local data into the peer's next receive.
    Send,
    /// SEND with immediate data.
    SendImm(u32),
    /// RDMA WRITE of the local data into the remote buffer.
    Write(RemoteBuf),
    /// RDMA WRITE with immediate data, which also consumes a receive.
    WriteImm(RemoteBuf, u32),
    /// RDMA READ of the remote buffer into the local one.
    Read(RemoteBuf),
    /// Atomic fetch and add on the 8 bytes at the remote address; the old
    /// value lands in the local buffer.
    FetchAdd {
        /// The remote 8 bytes (8-byte aligned).
        remote: RemoteBuf,
        /// What to add.
        add: u64,
    },
    /// Atomic compare and swap on the 8 bytes at the remote address; the old
    /// value lands in the local buffer.
    CompareSwap {
        /// The remote 8 bytes (8-byte aligned).
        remote: RemoteBuf,
        /// The value expected there.
        compare: u64,
        /// The value written if it matched.
        swap: u64,
    },
}

/// Memory access rights (`enum ibv_access_flags`).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct Access(pub u32);

impl Access {
    /// The NIC may write the memory (receives, READ responses).
    pub const LOCAL_WRITE: Access = Access(1);
    /// Peers may RDMA WRITE it.
    pub const REMOTE_WRITE: Access = Access(1 << 1);
    /// Peers may RDMA READ it.
    pub const REMOTE_READ: Access = Access(1 << 2);
    /// Peers may run atomics on it.
    pub const REMOTE_ATOMIC: Access = Access(1 << 3);

    /// Whether every right of `other` is in `self`.
    pub const fn contains(self, other: Access) -> bool {
        self.0 & other.0 == other.0
    }
}

impl core::ops::BitOr for Access {
    type Output = Access;
    fn bitor(self, rhs: Access) -> Access {
        Access(self.0 | rhs.0)
    }
}

/// The transport service of a QP.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum QpType {
    /// Reliable connection.
    Rc,
    /// Unreliable connection.
    Uc,
}

/// The sizes of a QP's queues.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QpCaps {
    /// WQEs in the send queue.
    pub max_send_wr: u32,
    /// WQEs in the receive queue.
    pub max_recv_wr: u32,
    /// Scatter/gather elements of a send WQE.
    pub max_send_sge: u32,
    /// Scatter/gather elements of a receive WQE.
    pub max_recv_sge: u32,
    /// Bytes of inline data in a send WQE.
    pub max_inline_data: u32,
}

/// What a device supports (from `ibv_query_device`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceCaps {
    /// WQEs per work queue.
    pub max_qp_wr: u32,
    /// Scatter/gather elements per WQE.
    pub max_sge: u32,
    /// Entries per completion queue.
    pub max_cqe: u32,
    /// Outstanding RDMA READs and atomics a QP may initiate.
    pub max_qp_rd_atom: u32,
    /// Whether atomics are supported.
    pub atomics: bool,
    /// Bytes of one message, at most (the least `max_msg_sz` of its
    /// ports).
    pub max_msg: u32,
}

/// A port's attributes (from `ibv_query_port`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PortInfo {
    /// The port's local identifier (InfiniBand).
    pub lid: u16,
    /// The active MTU (`enum ibv_mtu`, 1 = 256 bytes ... 5 = 4096 bytes).
    pub active_mtu: u32,
    /// Whether the link layer is Ethernet (RoCE), else InfiniBand.
    pub ethernet: bool,
    /// Whether the port is active.
    pub active: bool,
}

/// A global identifier: an IPv6 address (or an IPv4 address mapped into
/// one) naming a port.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Gid(pub [u8; 16]);

impl Gid {
    /// The IPv4 address, if this GID is one mapped into IPv6
    /// (`::ffff:a.b.c.d`).
    pub fn ipv4(&self) -> Option<[u8; 4]> {
        let r = &self.0;
        let mut i = 0;
        while i < 10 {
            if r[i] != 0 {
                return None;
            }
            i += 1;
        }
        if r[10] == 0xff && r[11] == 0xff {
            Some([r[12], r[13], r[14], r[15]])
        } else {
            None
        }
    }
}

impl fmt::Debug for Gid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some([a, b, c, d]) = self.ipv4() {
            return write!(f, "::ffff:{a}.{b}.{c}.{d}");
        }
        let mut i = 0;
        while i < 16 {
            if i > 0 {
                f.write_str(":")?;
            }
            write!(f, "{:02x}{:02x}", self.0[i], self.0[i + 1])?;
            i += 2;
        }
        Ok(())
    }
}

/// One entry of a port's GID table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GidEntry {
    /// Its index, which QPs name as their source GID.
    pub index: u8,
    /// The GID.
    pub gid: Gid,
    /// Whether it is a RoCE v2 GID (routable over UDP/IP).
    pub roce_v2: bool,
}

/// How peers reach a QP: what the two sides exchange before connecting.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Endpoint {
    /// The QP number.
    pub qp_num: u32,
    /// The port's LID (InfiniBand).
    pub lid: u16,
    /// The port's GID (RoCE, or InfiniBand across subnets).
    pub gid: Option<Gid>,
}

/// The attributes that connect a QP to its peer (INIT, RTR and RTS).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectAttr {
    /// The port.
    pub port: u8,
    /// What the peer may do to this QP's memory (`IBV_QP_ACCESS_FLAGS`).
    pub access: Access,
    /// The path MTU (`enum ibv_mtu`).
    pub path_mtu: u32,
    /// The receive queue's starting packet sequence number.
    pub rq_psn: u32,
    /// The send queue's starting packet sequence number.
    pub sq_psn: u32,
    /// The source GID index, for addressing by GID.
    pub gid_index: Option<u8>,
    /// The traffic class with a GRH.
    pub traffic_class: u8,
    /// RC: the local ACK timeout (4.096 µs × 2^timeout).
    pub timeout: u8,
    /// RC: transport retries.
    pub retry_count: u8,
    /// RC: receiver-not-ready retries (7 = infinite).
    pub rnr_retry: u8,
    /// RC: the receiver-not-ready NAK timer.
    pub min_rnr_timer: u8,
    /// RC: outstanding RDMA READs and atomics this QP may initiate.
    pub max_rd_atomic: u8,
    /// RC: outstanding RDMA READs and atomics the peer may initiate here.
    pub max_dest_rd_atomic: u8,
}

/// An open RDMA device and its resources.
///
/// # Safety of the resources
///
/// The associated types are released when dropped, and must be dropped in
/// dependency order: QPs before their CQs and PD, memory regions before
/// their PD, everything before the device. The safe wrappers in this crate
/// keep that order.
/// # Safety
///
/// Implementations and their associated resources must enforce the access ranges,
/// protection domains and permissions of registered memory. Completions must truthfully
/// identify retired work: reporting retirement permits the safe core to release its
/// buffers immediately. An in-order completion must imply all earlier work on that queue
/// has stopped accessing local memory. Successful deregistration must revoke access
/// before returning; a failed close must retain the registration and its handle.
/// Resource destruction must quiesce access or prevent backing memory from being
/// released if deregistration fails. Associated queue methods must honor their
/// documented posting, cancellation and completion contracts, including
/// rejecting invalid remote descriptors rather than accessing arbitrary process memory.
/// A backend is part of the trusted boundary even when its implementation is software.
pub unsafe trait Nic: Send + Sync + Sized + 'static {
    /// A protection domain.
    type Pd: Send + Sync + 'static;
    /// A registered memory region; dropping it must revoke access or prevent
    /// backing memory from being released if revocation fails.
    type Mr: Send + Sync + 'static;
    /// A completion queue.
    type Cq: NicCq + 'static;
    /// A queue pair.
    type Qp: NicQp + 'static;
    /// A thread domain with a parent domain over a PD: QPs created on it
    /// are used by one thread at a time, so the driver may skip their locks
    /// (and give them a doorbell of their own, shared among them).
    type Domain: Send + 'static;

    /// The device's name.
    fn name(&self) -> &str;

    /// What the device supports.
    fn caps(&self) -> DeviceCaps;

    /// A port's attributes.
    fn port(&self, port: u8) -> Result<PortInfo>;

    /// A port's GID table.
    fn gids(&self, port: u8) -> Vec<GidEntry>;

    /// Allocates a protection domain.
    fn alloc_pd(&self) -> Result<Self::Pd>;

    /// Registers `len` bytes at `addr` in `pd` with `access`.
    ///
    /// # Safety
    ///
    /// The memory must stay allocated until the region is successfully closed
    /// or dropped. With remote or local-write access nothing may read it as
    /// Rust values while the NIC may write it.
    unsafe fn reg_mr(
        &self,
        pd: &Self::Pd,
        addr: *mut u8,
        len: usize,
        access: Access,
    ) -> Result<Self::Mr>;

    /// The local and remote keys of a region. Must not dereference a released
    /// handle if the region has already been closed; implementations may panic.
    fn mr_keys(mr: &Self::Mr) -> (u32, u32);

    /// Tries to deregister a region without consuming its handle.
    ///
    /// `Ok(())` means its keys are revoked, the NIC can no longer access its
    /// memory, and the handle is closed. Closing an already closed handle
    /// succeeds without another deregistration. `Err` leaves the registration
    /// and handle alive for retry: its memory, PD and device must stay alive.
    /// As when dropping a region, local work using it must have retired first.
    ///
    /// A caller retaining resources after an error must retain the MR itself
    /// too, without running its destructor. Dropping it may retry and abort if
    /// revocation still fails, since the MR alone does not own its backing memory.
    /// Backends without an explicit close keep their existing destructor contract;
    /// this default reports no revocation and leaves the handle untouched.
    fn close_mr(_mr: &mut Self::Mr) -> Result<()> {
        Err(Error::Unsupported("explicit MR close"))
    }

    /// Creates a completion queue of at least `entries` entries, polled by
    /// one thread at a time when `single_threaded`.
    fn create_cq(&self, entries: u32, single_threaded: bool) -> Result<Self::Cq>;

    /// Allocates a thread domain and a parent domain over `pd` (with them,
    /// mlx5 drops the locks of the QPs created there); `None` where the
    /// device has none (rxe).
    fn alloc_domain(&self, pd: &Self::Pd) -> Option<Self::Domain>;

    /// Creates a QP in `pd` with its queues on the two CQs (they may be the
    /// same). On `domain`, which must be over `pd`, the driver may skip its
    /// own locks: the caller serializes every use of the QP and of the
    /// other QPs of the domain. If the device refuses a QP on the domain,
    /// it is created without.
    fn create_qp(
        &self,
        pd: &Self::Pd,
        domain: Option<&Self::Domain>,
        send_cq: &Self::Cq,
        recv_cq: &Self::Cq,
        qp_type: QpType,
        caps: &QpCaps,
    ) -> Result<Self::Qp>;
}

/// A queue pair of a [`Nic`].
pub trait NicQp: Send {
    /// The QP number.
    fn qp_num(&self) -> u32;

    /// Whether the QP was created without the driver's locks (on a thread
    /// domain).
    fn is_lock_free(&self) -> bool;

    /// Moves the QP from RESET through INIT and RTR to RTS, connected to
    /// `remote`.
    fn connect(&mut self, attr: &ConnectAttr, remote: &Endpoint) -> Result<()>;

    /// Moves the QP to the error state: outstanding work requests complete
    /// with flush errors.
    fn to_error(&mut self) -> Result<()>;

    /// Starts writing WQEs (`ibv_wr_start`).
    ///
    /// # Safety
    ///
    /// Not while a previous start is pending.
    unsafe fn start(&mut self);

    /// Writes one WQE (`ibv_wr_*` and `ibv_wr_set_sge(_list)`) with id
    /// `wr_id`, asking for a completion if `signaled`.
    ///
    /// # Safety
    ///
    /// Between [`NicQp::start`] and [`NicQp::complete`] or [`NicQp::abort`].
    /// The local memory of `sges` must stay registered and valid until the
    /// WQE completes; memory the
    /// NIC writes must not be read or written by Rust code meanwhile. The
    /// queue must have room.
    /// A remote descriptor may be stale or rejected by the peer; this must
    /// produce an operation error, never a local memory-safety violation.
    unsafe fn post(&mut self, wr_id: u64, signaled: bool, op: &WrOp, sges: &[Sge]);

    /// Rings the doorbell for the WQEs written since [`NicQp::start`]
    /// (`ibv_wr_complete`). On error none of them is posted.
    ///
    /// # Safety
    ///
    /// After [`NicQp::start`].
    unsafe fn complete(&mut self) -> Result<()>;

    /// Discards the WQEs written since [`NicQp::start`] (`ibv_wr_abort`).
    ///
    /// # Safety
    ///
    /// After [`NicQp::start`].
    unsafe fn abort(&mut self);

    /// Posts a receive into `sges` with id `wr_id`.
    ///
    /// # Safety
    ///
    /// As for [`NicQp::post`]: the memory stays valid and untouched until
    /// the receive completes, and the queue has room.
    unsafe fn post_recv(&mut self, wr_id: u64, sges: &[Sge]) -> Result<()>;
}

/// A completion queue of a [`Nic`].
pub trait NicCq: Send {
    /// Takes up to `out.len()` completions; returns how many.
    fn poll(&mut self, out: &mut [Wc]) -> usize;
}
