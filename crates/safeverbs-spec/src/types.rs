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
    /// The QP was in the error state: the request was not executed.
    FlushErr,
    /// Local errors: length, protection (bad lkey or range), QP operation.
    LocalErr,
    /// The responder rejected the request: remote access (bad rkey or range) or invalid request.
    RemoteErr,
    /// The responder had no receive posted and the RNR retries ran out (RC).
    RnrRetryExceeded,
    /// No acknowledgement after the transport retries (peer gone, link down).
    RetryExceeded,
}

impl Status {
    pub open spec fn is_err(self) -> bool {
        !(self is Success)
    }
}

/// What the NIC reports for one work request: the fields of `ibv_wc` that reach the user.
pub struct Outcome {
    pub status: Status,
    /// Bytes received (receives) or transferred (RDMA read).
    pub byte_len: nat,
    /// Immediate data, for receives completed by a send or write with immediate.
    pub imm: Option<u32>,
}

} // verus!
