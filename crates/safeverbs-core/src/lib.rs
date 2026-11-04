//! The core of safeverbs: everything `unsafe` behind a small, safe API.
//!
//! Applications use the [`safeverbs`](https://docs.rs/safeverbs) crate,
//! which builds futures, sinks and streams on this one and forbids `unsafe`
//! code. This crate holds:
//!
//! - [`nic`]: the NIC as a trait, [`Nic`], with two implementations: the
//!   real one over libibverbs ([`verbs::Verbs`]) and a simulated one for
//!   tests ([`model::Model`]);
//! - [`pd`]: devices and protection domains;
//! - [`buf`]: registered memory as Rust values ([`BufMut`], [`Buf`],
//!   [`Exposed`], [`RingBuf`]) and the leases that keep it alive while the
//!   NIC uses it;
//! - [`ring`]: the completion ring of a work queue, pure bookkeeping
//!   without FFI or `unsafe`, verified with Verus; [`refine`] proves that
//!   it refines the abstract machine of `safeverbs-spec`;
//! - the internal `trusted` module: verbs with contracts from the NIC model;
//! - the internal `wr_id` module: the `wr_id` of a WQE and how a completion is
//!   routed back by it, verified;
//! - the internal `hw` module: the hardware side of an engine (its CQ, its QPs' queues and
//!   rings, its share of the NIC model), verified against the trusted verbs:
//!   every verb meets the model's precondition, the rings stay coupled to the
//!   model's queues, and QPs are destroyed only once drained;
//! - [`engine`]: a completion queue and the queue pairs attached to it,
//!   driven synchronously and by index ([`Engine`]): post requests into the rings and the
//!   hardware queues, commit them with one doorbell, poll the CQ, and get
//!   each request's context, result and buffers back.
//!
//! # Soundness in one paragraph
//!
//! The NIC reads and writes memory behind the compiler's back from the
//! moment a work request is posted until it completes. Every request
//! therefore holds leases of the memory it names, and those leases live in
//! the request's ring slot until the request retires (a completion for it
//! or a later request of the same in-order queue, including error and flush
//! completions); a QP is destroyed only once it is drained (moved to the
//! error state and polled until the NIC has flushed every work request);
//! nothing else can release them. Memory the NIC writes is lent as a unique buffer (moved into the
//! slot, unreachable until it comes back); memory it reads is lent as a
//! shared frozen buffer (read only until every handle is gone); no reference
//! to memory a peer may write is ever formed, only a volatile copy of it into
//! a type that accepts any bytes. Rings never let more
//! WQEs be in flight than the queues hold, and an engine's CQ reserves room
//! for every completion its QPs' queues may produce.

#![warn(missing_docs)]

pub mod buf;
pub mod engine;
pub mod error;
pub(crate) mod hw;
pub mod model;
pub mod nic;
pub mod pd;
pub mod refine;
pub mod ring;
pub mod transport;
pub(crate) mod trusted;
pub mod verbs;
pub mod wc;
pub(crate) mod wr_id;

pub use buf::{Buf, BufError, BufMut, Chunks, Exposed, Leases, Part, RingBuf};
pub use engine::{Completion, DetachedError, Engine, PostError, QpKey, Queue, Request, Unposted};
pub use error::{Error, Result};
pub use nic::{
    Access, ConnectAttr, Endpoint, Gid, GidEntry, Nic, PortInfo, QpCaps, RemoteBuf, WrOp,
};
pub use pd::{Context, Pd};
pub use ring::Seq;
pub use transport::{RC, Reliable, Transport, UC};
pub use wc::{Completed, CompletionError, CompletionResult, Received, Status, Wc};
