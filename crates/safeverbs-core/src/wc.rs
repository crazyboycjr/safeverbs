//! Work completions as plain data: what the completion queue reports for a
//! work request, independent of the NIC behind it.

use core::fmt;

/// The status of a work completion (`enum ibv_wc_status`).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Status(pub u32);

impl Status {
    /// The work request completed.
    pub const SUCCESS: Status = Status(0);
    /// Local length error: a message longer than the receive buffer, or a
    /// gather list longer than the port allows.
    pub const LOC_LEN_ERR: Status = Status(1);
    /// Local QP operation error.
    pub const LOC_QP_OP_ERR: Status = Status(2);
    /// Local protection error: a local key or address the NIC refused.
    pub const LOC_PROT_ERR: Status = Status(4);
    /// The QP entered the error state before this work request ran.
    pub const WR_FLUSH_ERR: Status = Status(5);
    /// Bad response from the remote side.
    pub const BAD_RESP_ERR: Status = Status(7);
    /// Local access error.
    pub const LOC_ACCESS_ERR: Status = Status(8);
    /// The remote side found the request invalid (for example a SEND larger
    /// than its receive buffer).
    pub const REM_INV_REQ_ERR: Status = Status(9);
    /// Remote access error: a remote key, address or permission the remote
    /// NIC refused.
    pub const REM_ACCESS_ERR: Status = Status(10);
    /// Remote operation error.
    pub const REM_OP_ERR: Status = Status(11);
    /// The transport retry counter was exceeded.
    pub const RETRY_EXC_ERR: Status = Status(12);
    /// The receiver-not-ready retry counter was exceeded.
    pub const RNR_RETRY_EXC_ERR: Status = Status(13);
    /// Remote aborted error.
    pub const REM_ABORT_ERR: Status = Status(16);
    /// Fatal error.
    pub const FATAL_ERR: Status = Status(19);
    /// Response timeout.
    pub const RESP_TIMEOUT_ERR: Status = Status(20);
    /// General error.
    pub const GENERAL_ERR: Status = Status(21);

    /// Whether the work request completed without error.
    pub const fn is_ok(self) -> bool {
        self.0 == 0
    }

    /// The status's name, as `ibv_wc_status_str` would give it.
    pub const fn name(self) -> &'static str {
        match self.0 {
            0 => "success",
            1 => "local length error",
            2 => "local QP operation error",
            3 => "local EE context operation error",
            4 => "local protection error",
            5 => "work request flushed",
            6 => "memory window bind error",
            7 => "bad response",
            8 => "local access error",
            9 => "remote invalid request",
            10 => "remote access error",
            11 => "remote operation error",
            12 => "transport retry counter exceeded",
            13 => "RNR retry counter exceeded",
            14 => "local RDD violation",
            15 => "remote invalid RD request",
            16 => "remote aborted",
            17 => "invalid EE context number",
            18 => "invalid EE context state",
            19 => "fatal error",
            20 => "response timeout",
            21 => "general error",
            _ => "unknown status",
        }
    }
}

impl fmt::Debug for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.name(), self.0)
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// What a completed work request did (`enum ibv_wc_opcode`).
///
/// Only meaningful for successful completions: the NIC leaves it undefined
/// on an error.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Opcode(pub u32);

impl Opcode {
    /// A SEND (with or without immediate data).
    pub const SEND: Opcode = Opcode(0);
    /// An RDMA WRITE (with or without immediate data).
    pub const RDMA_WRITE: Opcode = Opcode(1);
    /// An RDMA READ.
    pub const RDMA_READ: Opcode = Opcode(2);
    /// An atomic compare and swap.
    pub const COMP_SWAP: Opcode = Opcode(3);
    /// An atomic fetch and add.
    pub const FETCH_ADD: Opcode = Opcode(4);
    /// A receive that a SEND consumed.
    pub const RECV: Opcode = Opcode(128);
    /// A receive that an RDMA WRITE with immediate data consumed.
    pub const RECV_RDMA_WITH_IMM: Opcode = Opcode(129);
}

/// A work completion: the outcome of one work request.
///
/// Requests posted without a completion of their own (unsignaled) and
/// retired by a later one get a completion made up from what was posted:
/// success, the request's opcode and its length.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Wc {
    /// The work request's id: its WQE sequence number in its queue.
    pub wr_id: u64,
    /// Whether it succeeded, and if not why.
    pub status: Status,
    /// What it did (successful completions only).
    pub opcode: Opcode,
    /// Bytes transferred: received for a receive, else what was posted.
    pub byte_len: u32,
    /// The immediate data of a SEND or WRITE with immediate, on the receiver.
    pub imm: Option<u32>,
    /// The vendor's error code (error completions only).
    pub vendor_err: u32,
    /// The QP the work request was posted on.
    pub qp_num: u32,
}

impl Wc {
    /// A successful completion of `opcode` with `byte_len` bytes, for work
    /// requests that complete without a completion of their own.
    pub const fn success(opcode: Opcode, byte_len: u32) -> Wc {
        Wc {
            wr_id: 0,
            status: Status::SUCCESS,
            opcode,
            byte_len,
            imm: None,
            vendor_err: 0,
            qp_num: 0,
        }
    }

    /// Whether the work request succeeded.
    pub const fn is_ok(&self) -> bool {
        self.status.is_ok()
    }
}
