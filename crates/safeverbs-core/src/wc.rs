//! Work completions as plain data: what the completion queue reports for a
//! work request, independent of the NIC behind it.

// Verus generates undocumented field accessors for the public enums below.
#![cfg_attr(verus_keep_ghost, allow(missing_docs))]

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
    /// The QP was in the error state when the NIC came to this work request.
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

/// What a completed work request did (`enum ibv_wc_opcode`), as the NIC
/// reports it. Only meaningful for successful completions: the NIC leaves it
/// undefined on an error.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct Opcode(pub u32);

impl Opcode {
    /// A SEND (with or without immediate data).
    pub(crate) const SEND: Opcode = Opcode(0);
    /// An RDMA WRITE (with or without immediate data).
    pub(crate) const RDMA_WRITE: Opcode = Opcode(1);
    /// An RDMA READ.
    pub(crate) const RDMA_READ: Opcode = Opcode(2);
    /// An atomic compare and swap.
    pub(crate) const COMP_SWAP: Opcode = Opcode(3);
    /// An atomic fetch and add.
    pub(crate) const FETCH_ADD: Opcode = Opcode(4);
    /// A receive that a SEND consumed.
    pub(crate) const RECV: Opcode = Opcode(128);
    /// A receive that an RDMA WRITE with immediate data consumed.
    pub(crate) const RECV_RDMA_WITH_IMM: Opcode = Opcode(129);

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

/// What a receive that succeeded got.
///
/// The two cases are apart because they mean different things for the
/// receive's buffer. There is no length to misuse: a notification's `len`
/// counts bytes that went elsewhere, and a message's is the only one to slice
/// the buffer with.
///
/// ```
/// use safeverbs_core::Received;
/// fn payload<'a>(received: Received, buf: &'a [u8]) -> &'a [u8] {
///     match received {
///         Received::Message { len, .. } => &buf[..len as usize],
///         Received::WriteNotification { .. } => &[],
///     }
/// }
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[allow(inconsistent_fields)] // consumed by `verus!`: `imm` differs between the cases
pub enum Received {
    /// A SEND arrived: `len` bytes are in the receive's buffer (zero is a
    /// message too), with immediate data if it was sent with some.
    Message {
        /// The bytes in the buffer.
        len: u32,
        /// The immediate data of a SEND with immediate.
        imm: Option<u32>,
    },
    /// A peer's WRITE with immediate landed in memory it was given access to
    /// and consumed this receive. The receive's buffer is untouched.
    WriteNotification {
        /// The immediate data.
        imm: u32,
        /// The length of the remote WRITE, which went to the exposed memory.
        len: u32,
    },
}

/// What a request that succeeded did: every WQE of it completed successfully,
/// and this is what the last one reports, with only the fields its operation
/// defines.
///
/// A SEND has no length to read by mistake:
///
/// ```compile_fail,E0026
/// use safeverbs_core::Completed;
/// fn length(done: Completed) -> u32 {
///     match done {
///         Completed::Sent { len } => len,
///         _ => 0,
///     }
/// }
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Completed {
    /// A SEND or WRITE, with or without immediate data: the NIC reports its
    /// status alone. Local completion says the source may be reused, not that
    /// a remote application has processed the data (on an unreliable
    /// connection, not even that it arrived).
    Sent,
    /// An RDMA READ or an atomic: `len` bytes landed in its buffer. A batch of
    /// READs reports the length of its last READ, not a sum.
    Read {
        /// The bytes of its last WQE.
        len: u32,
    },
    /// A receive.
    Received(Received),
}

impl Completed {
    /// The outcome the formal model M names for it.
    pub open spec fn model(self) -> safeverbs_spec::types::Outcome {
        let (kind, byte_len, imm) = match self {
            Completed::Sent => (safeverbs_spec::types::CompletionKind::StatusOnly, 0, None),
            Completed::Read { len } => (safeverbs_spec::types::CompletionKind::Read, len as nat, None),
            Completed::Received(Received::Message { len, imm }) =>
                (safeverbs_spec::types::CompletionKind::Receive, len as nat, imm),
            Completed::Received(Received::WriteNotification { imm, len }) =>
                (safeverbs_spec::types::CompletionKind::WriteNotification, len as nat, Some(imm)),
        };
        safeverbs_spec::types::Outcome {
            status: safeverbs_spec::types::Status::Success, kind, byte_len, imm,
        }
    }
}

/// What a request reports: what it did, or why it did not.
pub type CompletionResult = Result<Completed, CompletionError>;

/// Why a request did not complete successfully. Its context and buffers come
/// back with the error all the same.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompletionError {
    /// The NIC reported an error for the request's WQE at `position` (the
    /// first is 0; a request of one WQE always says 0).
    ///
    /// The WQEs before `position` completed successfully. That WQE and those
    /// after it did not complete successfully, and their remote effects are
    /// unknown: a later WRITE may land at the peer before an earlier READ
    /// fails, and a lost acknowledgement can hide a WRITE that landed. To
    /// recover, ask the peer what it has, or write again what may be written
    /// twice.
    Failed {
        /// The status of that WQE, the first in the request that failed.
        status: Status,
        /// The vendor's error code.
        vendor_err: u32,
        /// The WQE's position in its request.
        position: u32,
    },
    /// The doorbell that would have posted the request was refused: the NIC
    /// never saw it, so nothing of it happened, here or at the peer.
    Refused,
}

impl CompletionError {
    /// The status of the WQE that failed; `None` for a refused request, which
    /// no status describes.
    pub const fn status(self) -> Option<Status> {
        match self {
            CompletionError::Failed { status, .. } => Some(status),
            CompletionError::Refused => None,
        }
    }
}

/// A work completion as the NIC's completion queue gives it: the outcome of
/// one WQE, what the rings keep for a request, and the only place undefined
/// fields exist. What leaves the core is a [`Completed`] or a
/// [`CompletionError`], which have none of them.
///
/// Requests posted without a completion of their own (unsignaled) and completed
/// by a later one get a completion made up from what was posted: success and
/// the request's opcode, with the same metadata as a signaled completion.
///
/// The `wr_id` is the core's own, for routing: requests are told apart by their
/// contexts.
///
/// ```compile_fail,E0616
/// fn id(wc: &safeverbs_core::Wc) -> u64 {
///     wc.wr_id
/// }
/// ```
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Wc {
    /// The id the WQE was posted with: its QP, queue and number, for routing.
    pub(crate) wr_id: u64,
    /// Whether it succeeded, and if not why.
    pub(crate) status: Status,
    /// What it did (successful completions only).
    pub(crate) opcode: Opcode,
    /// The NIC's byte count, meaningful only for successful receives,
    /// RDMA READs and atomics.
    pub(crate) byte_len: u32,
    /// The immediate data of a SEND or WRITE with immediate, on the receiver.
    pub(crate) imm: Option<u32>,
    /// The vendor's error code (error completions only).
    pub(crate) vendor_err: u32,
}

impl Wc {
    /// A successful completion of `opcode`, for work requests that complete
    /// without a completion of their own. SEND and WRITE ignore `byte_len`.
    pub(crate) const fn success(opcode: Opcode, byte_len: u32) -> (wc: Wc)
        ensures
            wc.spec_ok(),
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
        };
        proof { wc.lemma_outcome_canonical(); }
        wc
    }

    /// Whether the work request succeeded.
    #[inline(always)]
    pub(crate) const fn is_ok(&self) -> (ok: bool)
        ensures
            ok == self.spec_ok(),
    {
        self.status.is_ok()
    }

    /// What a successful completion reports, as M's outcome says. A
    /// notification carries its immediate data by M's rule for receives
    /// (`RecvWr::allows_outcome`); without it, the proof says nothing.
    #[inline(always)]
    pub(crate) const fn completed(&self) -> (done: Completed)
        requires
            self.spec_ok(),
        ensures
            self.outcome().kind is WriteNotification && self.outcome().imm is None
                || done.model() == self.outcome(),
    {
        let len = self.byte_len;
        if self.opcode.0 == 128 {
            Completed::Received(Received::Message { len, imm: self.imm })
        } else if self.opcode.0 == 129 {
            let imm = match self.imm {
                Some(imm) => imm,
                None => 0,
            };
            Completed::Received(Received::WriteNotification { imm, len })
        } else if self.opcode.0 == 2 || self.opcode.0 == 3 || self.opcode.0 == 4 {
            Completed::Read { len }
        } else {
            Completed::Sent
        }
    }

    /// What a successful completion of a receive reports: `completed`, for the receive queue, where
    /// M allows nothing but a message or a notification.
    #[inline(always)]
    pub(crate) const fn received(&self) -> (done: Received)
        requires
            self.spec_ok(),
    {
        let len = self.byte_len;
        if self.opcode.0 == 129 {
            let imm = match self.imm {
                Some(imm) => imm,
                None => 0,
            };
            Received::WriteNotification { imm, len }
        } else {
            Received::Message { len, imm: self.imm }
        }
    }

    /// The error of a failed completion, of the WQE at `position` in its
    /// request.
    #[inline(always)]
    pub(crate) const fn failure(&self, position: u32) -> (error: CompletionError)
        ensures
            error matches CompletionError::Failed { status, position: at, .. }
                && status == self.spec_status() && at == position,
    {
        CompletionError::Failed { status: self.status, vendor_err: self.vendor_err, position }
    }

    /// What this completion reports on its own, for callers of the raw
    /// [`NicCq::poll`](crate::nic::NicCq::poll): a WQE that failed is at
    /// position 0, as it would be in a request of its own.
    pub fn result(&self) -> Result<Completed, CompletionError> {
        if self.is_ok() {
            Ok(self.completed())
        } else {
            Err(self.failure(0))
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
            self.outcome().status == self.status.model(),
    {
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

    /// The outcome is a success exactly when the completion is.
    pub proof fn lemma_outcome_ok(&self)
        ensures
            self.outcome().status is Success <==> self.spec_ok(),
    {
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

impl Wc {
    /// The `wr_id` the NIC reported, for programs that post on the NIC's trait themselves
    /// (the hand-written baselines of `scripts/measure.sh`). The library's own is internal.
    #[doc(hidden)]
    pub fn probe_wr_id(&self) -> u64 {
        self.wr_id
    }
}

impl fmt::Display for CompletionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompletionError::Failed {
                status, position, ..
            } => write!(f, "WQE {position} of the request failed: {status}"),
            CompletionError::Refused => {
                f.write_str("the doorbell was refused: the NIC never saw the request")
            }
        }
    }
}

impl std::error::Error for CompletionError {}

impl Default for Wc {
    /// A placeholder for [`NicCq::poll`](crate::nic::NicCq::poll) to fill.
    fn default() -> Wc {
        crate::hw::EMPTY_WC
    }
}

impl fmt::Debug for Wc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Wc({:?})", self.result())
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
        }
    }

    #[test]
    fn a_success_has_only_what_its_operation_defines() {
        for opcode in OPCODES {
            let expected = match opcode {
                Opcode::RDMA_READ | Opcode::COMP_SWAP | Opcode::FETCH_ADD => {
                    Completed::Read { len: 4096 }
                }
                Opcode::RECV => Completed::Received(Received::Message {
                    len: 4096,
                    imm: Some(0xdead_beef),
                }),
                Opcode::RECV_RDMA_WITH_IMM => Completed::Received(Received::WriteNotification {
                    imm: 0xdead_beef,
                    len: 4096,
                }),
                _ => Completed::Sent,
            };
            assert_eq!(
                raw(opcode, Status::SUCCESS).result(),
                Ok(expected),
                "{opcode:?}"
            );
        }
    }

    #[test]
    fn an_error_has_its_status_alone() {
        for opcode in OPCODES {
            for status in (1..=21).map(Status).chain([Status(u32::MAX)]) {
                let wc = raw(opcode, status);
                assert!(!wc.is_ok());
                assert_eq!(
                    wc.result(),
                    Err(CompletionError::Failed {
                        status,
                        vendor_err: 7,
                        position: 0
                    })
                );
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
            // A notification always has immediate data on a real NIC; this
            // made-up one is never inferred (a receive has its own CQE).
            if opcode != Opcode::RECV_RDMA_WITH_IMM {
                assert_eq!(inferred.result(), signaled.result());
            }
            if matches!(opcode, Opcode::SEND | Opcode::RDMA_WRITE) {
                assert_eq!(inferred.byte_len, 0);
            }
        }
    }

    #[test]
    fn zero_length_receive_is_distinct_from_write_notification() {
        assert_eq!(
            Wc::success(Opcode::RECV, 0).result(),
            Ok(Completed::Received(Received::Message { len: 0, imm: None }))
        );
        assert_eq!(
            raw(Opcode::RECV_RDMA_WITH_IMM, Status::SUCCESS).result(),
            Ok(Completed::Received(Received::WriteNotification {
                imm: 0xdead_beef,
                len: 4096
            }))
        );
    }
}
