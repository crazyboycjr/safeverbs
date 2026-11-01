//! Transport services as types, so that what a QP may do is checked at
//! compile time (RDMA READ and atomics only on reliable connections).

use crate::nic::QpType;

/// A transport service of a connected QP.
pub trait Transport: Send + Sync + 'static {
    /// The service.
    const QP_TYPE: QpType;
    /// Whether the send queue completes in posting order, so that one
    /// signaled completion retires every request before it. Only such
    /// transports are supported for now: the completion ring
    /// (`crate::ring`) relies on it. Out-of-order transports (UD over
    /// several paths, SRQs) need another structure behind the same QP API.
    const IN_ORDER_COMPLETION: bool;
}

/// A transport that delivers reliably: RDMA READ and atomics are available.
pub trait Reliable: Transport {}

/// Reliable connection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RC;

/// Unreliable connection: SEND and RDMA WRITE, no acknowledgements (a lost
/// message is silently dropped, the sender's request still succeeds).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UC;

impl Transport for RC {
    const QP_TYPE: QpType = QpType::Rc;
    const IN_ORDER_COMPLETION: bool = true;
}

impl Reliable for RC {}

impl Transport for UC {
    const QP_TYPE: QpType = QpType::Uc;
    const IN_ORDER_COMPLETION: bool = true;
}
