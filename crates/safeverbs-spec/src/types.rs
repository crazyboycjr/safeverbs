//! Types shared by the NIC model (`nic`, M) and the abstract machine (`abs`, A).
//!
//! Memory is modelled as byte addresses (`int`). A set of addresses says which bytes a
//! party may touch; the contents of memory are not modelled, because no guarantee here
//! depends on them: the NIC and the peer may write any bytes they like.
use vstd::prelude::*;

verus! {

/// A queue pair, named by its number.
pub type QpId = nat;

/// A completion queue.
pub type CqId = nat;

/// A protection domain.
pub type PdId = nat;

/// A memory region key: the lkey the local side names it by (its rkey is the same number
/// in this model).
pub type Key = nat;

/// A contiguous byte range `[start, start + len)`.
pub struct Span {
    pub start: int,
    pub len: nat,
}

impl Span {
    pub open spec fn end(self) -> int {
        self.start + self.len
    }

    pub open spec fn contains(self, a: int) -> bool {
        self.start <= a < self.end()
    }

    /// `self` lies inside `outer`.
    pub open spec fn within(self, outer: Span) -> bool {
        outer.start <= self.start && self.end() <= outer.end()
    }

    /// The bytes of the range, as a set.
    pub open spec fn bytes(self) -> ISet<int> {
        ISet::new(|a: int| self.contains(a))
    }
}

/// The status of a work completion, as `ibv_wc_status` reports it (the cases that matter here).
pub enum Status {
    Success,
    /// The QP was in the error state: the WQE did not complete successfully, and its remote
    /// effects are unknown.
    FlushErr,
    /// Local errors: length, protection (bad lkey or range), QP operation.
    LocalErr,
    /// The responder found a protection error on the remote buffer of an RDMA operation: a
    /// bad rkey, a range outside its region, or rights the region does not give
    /// (`IBV_WC_REM_ACCESS_ERR`; IB 11.6.2, "Remote Access Error").
    RemoteAccess,
    /// The responder found the request invalid: an operation its receive queue does not
    /// support, too many RDMA READs or atomics outstanding, or a message longer than its
    /// receive (`IBV_WC_REM_INV_REQ_ERR`; IB 11.6.2, "Remote Invalid Request Error").
    RemoteInvalidRequest,
    /// The responder could not complete the request for a reason of its own, such as a
    /// malformed receive WQE (`IBV_WC_REM_OP_ERR`; IB 11.6.2, "Remote Operation Error").
    RemoteOperation,
    /// The rest of what comes from the other end: a response with an unexpected opcode
    /// (`IBV_WC_BAD_RESP_ERR`, IB 11.6.2 "Bad Response Error") or an operation aborted
    /// (`IBV_WC_REM_ABORT_ERR`, IB 11.6.2 "Aborted Error").
    RemoteOther,
    /// The responder had no receive posted and the RNR retries ran out (RC).
    RnrRetryExceeded,
    /// No acknowledgement after the transport retries (peer gone, link down).
    RetryExceeded,
}

impl Status {
    pub open spec fn is_err(self) -> bool {
        !(self is Success)
    }

    /// What the other end reported (IB 11.6.2): its refusal of the request, or a response the
    /// requester could not use.
    pub open spec fn is_remote(self) -> bool {
        self is RemoteAccess || self is RemoteInvalidRequest || self is RemoteOperation
            || self is RemoteOther
    }
}

/// What a completion reports besides its status.
pub enum CompletionKind {
    /// Nothing: a SEND or WRITE acknowledgement, or any error.
    StatusOnly,
    /// The length an RDMA read transferred.
    Read,
    /// A message that arrived in the receive's buffers: its length and immediate.
    Receive,
    /// A peer's WRITE with immediate, which consumed the receive but wrote an exposed
    /// region: the write's length and the immediate.
    WriteNotification,
}

/// What the NIC reports for one work request: the fields of `ibv_wc` that reach the user,
/// without those the hardware leaves undefined.
pub struct Outcome {
    pub status: Status,
    pub kind: CompletionKind,
    /// Bytes received or read, or the length of a notified write; zero when undefined.
    pub byte_len: nat,
    /// Immediate data, for receives completed by a send or write with immediate.
    pub imm: Option<u32>,
}

impl Outcome {
    /// Undefined fields have one value: an error reports its status only, as a status-only
    /// success does (no length, no immediate), and a read has no immediate.
    pub open spec fn canonical(self) -> bool {
        &&& self.status.is_err() ==> self.kind is StatusOnly
        &&& self.kind is StatusOnly ==> self.byte_len == 0 && self.imm is None
        &&& self.kind is Read ==> self.imm is None
    }
}

} // verus!
