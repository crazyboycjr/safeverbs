//! Work completions as plain data: what the completion queue reports for a
//! work request, independent of the NIC behind it.

use core::fmt;

use vstd::prelude::*;

verus! {

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
    pub const fn is_ok(self) -> (ok: bool)
        ensures
            ok == (self.0 == 0),
    {
        self.0 == 0
    }

    /// The status as the formal model names it (`safeverbs_spec::types::Status`): success,
    /// a flush, the responder's refusal, the two retry counters, or else a local error.
    pub open spec fn model(self) -> safeverbs_spec::types::Status {
        if self.0 == 0 {
            safeverbs_spec::types::Status::Success
        } else if self.0 == 5 {
            safeverbs_spec::types::Status::FlushErr
        } else if self.0 == 7 || self.0 == 9 || self.0 == 10 || self.0 == 11 || self.0 == 16 {
            safeverbs_spec::types::Status::RemoteErr
        } else if self.0 == 13 {
            safeverbs_spec::types::Status::RnrRetryExceeded
        } else if self.0 == 12 || self.0 == 20 {
            safeverbs_spec::types::Status::RetryExceeded
        } else {
            safeverbs_spec::types::Status::LocalErr
        }
    }
}

} // verus!

impl Status {
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

verus! {

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

    /// Whether a successful completion defines a transferred byte count.
    pub(crate) const fn has_byte_len(self) -> (has: bool)
        ensures
            has == (self.0 == 2 || self.0 == 3 || self.0 == 4 || self.0 == 128 || self.0 == 129),
    {
        self.0 == 2 || self.0 == 3 || self.0 == 4 || self.0 == 128 || self.0 == 129
    }

    /// Whether a successful completion can carry received immediate data.
    pub(crate) const fn is_receive(self) -> (recv: bool)
        ensures
            recv == (self.0 == 128 || self.0 == 129),
    {
        self.0 == 128 || self.0 == 129
    }
}

/// A work completion: the outcome of one work request.
///
/// Requests posted without a completion of their own (unsignaled) and
/// retired by a later one get a completion made up from what was posted:
/// success and the request's opcode, with the same metadata as a signaled
/// completion. SEND and WRITE report status only. Metadata getters return
/// `None` wherever the NIC leaves a field undefined, including on errors.
/// Raw metadata cannot be accessed directly:
///
/// ```compile_fail
/// use safeverbs_core::{Opcode, Wc};
/// let wc = Wc::success(Opcode::SEND, 64);
/// let _ = wc.byte_len;
/// ```
///
/// ```compile_fail
/// use safeverbs_core::{Opcode, Wc};
/// let wc = Wc::success(Opcode::SEND, 64);
/// let _ = wc.opcode;
/// ```
///
/// ```compile_fail
/// use safeverbs_core::{Opcode, Wc};
/// let wc = Wc::success(Opcode::RECV, 64);
/// let _ = wc.imm;
/// ```
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Wc {
    /// The work request's id: its WQE sequence number in its queue.
    pub wr_id: u64,
    /// Whether it succeeded, and if not why.
    pub status: Status,
    /// What it did (successful completions only).
    pub(crate) opcode: Opcode,
    /// The NIC's byte count, meaningful only for successful receives,
    /// RDMA READs and atomics.
    pub(crate) byte_len: u32,
    /// The immediate data of a SEND or WRITE with immediate, on the receiver.
    pub(crate) imm: Option<u32>,
    /// The vendor's error code (error completions only).
    pub vendor_err: u32,
    /// The QP the work request was posted on.
    pub qp_num: u32,
}

impl Wc {
    /// A successful completion of `opcode`, for work requests that retire
    /// without a completion of their own. SEND and WRITE ignore `byte_len`.
    pub const fn success(opcode: Opcode, byte_len: u32) -> (wc: Wc)
        ensures
            wc.spec_ok(),
            wc.spec_opcode() == Some(opcode),
            wc.spec_byte_len() == if opcode.0 == 2 || opcode.0 == 3 || opcode.0 == 4
                || opcode.0 == 128 || opcode.0 == 129 { Some(byte_len) } else { None },
            wc.outcome().imm is None,
            wc.outcome().canonical(),
            wc.outcome().status is Success,
            wc.outcome().kind == if opcode.0 == 128 { safeverbs_spec::types::CompletionKind::Receive }
                else if opcode.0 == 129 { safeverbs_spec::types::CompletionKind::WriteNotification }
                else if opcode.0 == 2 || opcode.0 == 3 || opcode.0 == 4 { safeverbs_spec::types::CompletionKind::Read }
                else { safeverbs_spec::types::CompletionKind::StatusOnly },
            wc.outcome().byte_len == if opcode.0 == 2 || opcode.0 == 3 || opcode.0 == 4
                || opcode.0 == 128 || opcode.0 == 129 { byte_len as nat } else { 0 },
    {
        let wc = Wc {
            wr_id: 0,
            status: Status::SUCCESS,
            opcode,
            byte_len: if opcode.has_byte_len() { byte_len } else { 0 },
            imm: None,
            vendor_err: 0,
            qp_num: 0,
        };
        proof { wc.lemma_outcome_canonical(); }
        wc
    }

    /// Whether the work request succeeded.
    pub const fn is_ok(&self) -> (ok: bool)
        ensures
            ok == self.spec_ok(),
    {
        self.status.is_ok()
    }

    /// What the work request did, only when it succeeded.
    pub const fn opcode(&self) -> (opcode: Option<Opcode>)
        ensures
            opcode == self.spec_opcode(),
            (opcode is Some) == self.spec_ok(),
            self.outcome().canonical(),
    {
        proof { self.lemma_outcome_canonical(); }
        if self.status.is_ok() { Some(self.opcode) } else { None }
    }

    /// Bytes transferred by a successful receive, RDMA READ or atomic.
    ///
    /// For [`Opcode::RECV_RDMA_WITH_IMM`], this counts the remote WRITE,
    /// whose data went to the remote address, not the posted receive
    /// buffer. Use [`Self::received_len`] when slicing a receive buffer.
    /// SEND and WRITE completions have no defined byte count.
    pub const fn byte_len(&self) -> (len: Option<u32>)
        ensures
            len == self.spec_byte_len(),
            len is Some ==> self.spec_ok(),
            len is Some ==> self.outcome().byte_len == len->Some_0 as nat,
            len is None ==> self.outcome().byte_len == 0,
            self.outcome().canonical(),
    {
        proof { self.lemma_outcome_canonical(); }
        if self.status.is_ok() && self.opcode.has_byte_len() {
            Some(self.byte_len)
        } else {
            None
        }
    }

    /// Bytes placed in the posted receive buffer by a successful SEND.
    ///
    /// A WRITE_WITH_IMM notification consumes a receive without writing
    /// its buffer, so it returns `None` even if the remote WRITE has data.
    pub const fn received_len(&self) -> (len: Option<u32>)
        ensures
            len == self.spec_received_len(),
            (len is Some) == (self.spec_ok() && self.outcome().kind is Receive),
            len is Some ==> self.outcome().byte_len == len->Some_0 as nat,
            self.outcome().canonical(),
    {
        proof { self.lemma_outcome_canonical(); }
        if self.status.is_ok() && self.opcode.0 == 128 {
            Some(self.byte_len)
        } else {
            None
        }
    }

    /// Received immediate data, only on a successful receive carrying it.
    pub const fn imm(&self) -> (imm: Option<u32>)
        ensures
            imm == self.spec_imm(),
            imm == self.outcome().imm,
            imm is Some ==> self.spec_ok(),
            self.outcome().canonical(),
    {
        proof { self.lemma_outcome_canonical(); }
        if self.status.is_ok() && self.opcode.is_receive() {
            self.imm
        } else {
            None
        }
    }

    /// Whether the work request succeeded (for proofs).
    pub closed spec fn spec_ok(&self) -> bool {
        self.status.0 == 0
    }

    /// The completion status, exposed without unfolding the private representation.
    pub closed spec fn spec_status(&self) -> Status {
        self.status
    }

    /// Internal proofs may connect the public observation to the private representation.
    pub(crate) proof fn lemma_spec_status(&self)
        ensures
            self.spec_status() == self.status,
    {
    }

    /// The defined opcode, for accessor contracts.
    pub closed spec fn spec_opcode(&self) -> Option<Opcode> {
        if self.spec_ok() { Some(self.opcode) } else { None }
    }

    /// The defined transfer length, for accessor contracts.
    pub closed spec fn spec_byte_len(&self) -> Option<u32> {
        if self.spec_ok() && (self.opcode.0 == 2 || self.opcode.0 == 3
            || self.opcode.0 == 4 || self.opcode.0 == 128 || self.opcode.0 == 129) {
            Some(self.byte_len)
        } else {
            None
        }
    }

    /// The defined posted-buffer length, for accessor contracts.
    pub closed spec fn spec_received_len(&self) -> Option<u32> {
        if self.spec_ok() && self.opcode.0 == 128 { Some(self.byte_len) } else { None }
    }

    /// The defined immediate data, for accessor contracts.
    pub closed spec fn spec_imm(&self) -> Option<u32> {
        if self.spec_ok() && (self.opcode.0 == 128 || self.opcode.0 == 129) {
            self.imm
        } else {
            None
        }
    }

    /// The outcome as the formal model sees it (`safeverbs_spec::types::Outcome`).
    pub closed spec fn outcome(&self) -> safeverbs_spec::types::Outcome {
        let kind = if !self.spec_ok() {
            safeverbs_spec::types::CompletionKind::StatusOnly
        } else if self.opcode.0 == 128 {
            safeverbs_spec::types::CompletionKind::Receive
        } else if self.opcode.0 == 129 {
            safeverbs_spec::types::CompletionKind::WriteNotification
        } else if self.opcode.0 == 2 || self.opcode.0 == 3 || self.opcode.0 == 4 {
            safeverbs_spec::types::CompletionKind::Read
        } else {
            safeverbs_spec::types::CompletionKind::StatusOnly
        };
        safeverbs_spec::types::Outcome {
            status: self.status.model(),
            kind,
            byte_len: if kind is StatusOnly { 0 } else { self.byte_len as nat },
            imm: if kind is Receive || kind is WriteNotification { self.imm } else { None },
        }
    }

    /// Discarding undefined metadata always gives a canonical outcome.
    pub proof fn lemma_outcome_canonical(&self)
        ensures
            self.outcome().canonical(),
    {
        if !self.spec_ok() {
            assert(self.outcome().kind is StatusOnly);
        } else if self.opcode.0 == 128 {
            assert(self.outcome().kind is Receive);
        } else if self.opcode.0 == 129 {
            assert(self.outcome().kind is WriteNotification);
        } else if self.opcode.0 == 2 || self.opcode.0 == 3 || self.opcode.0 == 4 {
            assert(self.outcome().kind is Read);
        } else {
            assert(self.outcome().kind is StatusOnly);
        }
    }
}

} // verus!

impl fmt::Debug for Wc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wc")
            .field("wr_id", &self.wr_id)
            .field("status", &self.status)
            .field("opcode", &self.opcode())
            .field("byte_len", &self.byte_len())
            .field("imm", &self.imm())
            .field("vendor_err", &self.vendor_err)
            .field("qp_num", &self.qp_num)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OPCODES: [Opcode; 8] = [
        Opcode::SEND,
        Opcode::RDMA_WRITE,
        Opcode::RDMA_READ,
        Opcode::COMP_SWAP,
        Opcode::FETCH_ADD,
        Opcode::RECV,
        Opcode::RECV_RDMA_WITH_IMM,
        Opcode(u32::MAX),
    ];

    /// Deliberately populates fields the provider may leave undefined.
    fn raw(opcode: Opcode, status: Status) -> Wc {
        Wc {
            wr_id: 42,
            status,
            opcode,
            byte_len: 4096,
            imm: Some(0xdead_beef),
            vendor_err: 7,
            qp_num: 9,
        }
    }

    #[test]
    fn successful_metadata_depends_on_operation() {
        for opcode in OPCODES {
            let wc = raw(opcode, Status::SUCCESS);
            assert_eq!(wc.opcode(), Some(opcode));
            let len = match opcode {
                Opcode::RDMA_READ
                | Opcode::COMP_SWAP
                | Opcode::FETCH_ADD
                | Opcode::RECV
                | Opcode::RECV_RDMA_WITH_IMM => Some(4096),
                _ => None,
            };
            assert_eq!(wc.byte_len(), len, "{opcode:?}");
            assert_eq!(
                wc.received_len(),
                if opcode == Opcode::RECV {
                    Some(4096)
                } else {
                    None
                }
            );
            assert_eq!(
                wc.imm(),
                match opcode {
                    Opcode::RECV | Opcode::RECV_RDMA_WITH_IMM => Some(0xdead_beef),
                    _ => None,
                }
            );
        }
    }

    #[test]
    fn errors_hide_all_undefined_metadata() {
        for opcode in OPCODES {
            for status in (1..=21).map(Status).chain([Status(u32::MAX)]) {
                let wc = raw(opcode, status);
                assert!(!wc.is_ok());
                assert_eq!(wc.opcode(), None);
                assert_eq!(wc.byte_len(), None);
                assert_eq!(wc.received_len(), None);
                assert_eq!(wc.imm(), None);
                assert_eq!(
                    (wc.wr_id, wc.status, wc.vendor_err, wc.qp_num),
                    (42, status, 7, 9)
                );
                let debug = format!("{wc:?}");
                assert!(debug.contains("opcode: None, byte_len: None, imm: None"));
            }
        }
    }

    #[test]
    fn inferred_success_matches_signaled_metadata() {
        for opcode in OPCODES {
            let inferred = Wc::success(opcode, 4096);
            let signaled = Wc {
                imm: None,
                ..raw(opcode, Status::SUCCESS)
            };
            assert!(inferred.is_ok());
            assert_eq!(inferred.opcode(), signaled.opcode());
            assert_eq!(inferred.byte_len(), signaled.byte_len());
            assert_eq!(inferred.received_len(), signaled.received_len());
            assert_eq!(inferred.imm(), signaled.imm());
            if matches!(opcode, Opcode::SEND | Opcode::RDMA_WRITE) {
                assert_eq!(inferred.byte_len, 0);
            }
        }
    }

    #[test]
    fn zero_length_receive_is_distinct_from_write_notification() {
        let recv = Wc::success(Opcode::RECV, 0);
        assert_eq!(recv.byte_len(), Some(0));
        assert_eq!(recv.received_len(), Some(0));
        let notification = raw(Opcode::RECV_RDMA_WITH_IMM, Status::SUCCESS);
        assert_eq!(notification.byte_len(), Some(4096));
        assert_eq!(notification.received_len(), None);
        assert_eq!(notification.imm(), Some(0xdead_beef));
    }
}
