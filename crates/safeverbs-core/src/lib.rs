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
//! - [`qp`]: a connected queue pair driven synchronously ([`QueuePair`]):
//!   post requests into the ring and the hardware queue, commit them with
//!   one doorbell, poll, and take each request's completion and buffers
//!   back.
//!
//! # Soundness in one paragraph
//!
//! The NIC reads and writes memory behind the compiler's back from the
//! moment a work request is posted until it completes. Every request
//! therefore holds leases of the memory it names, and those leases live in
//! the request's ring slot until the request retires (a completion for it
//! or a later request of the same in-order queue, including error and flush
//! completions) or the hardware QP is destroyed; nothing else can release
//! them. Memory the NIC writes is lent as a unique buffer (moved into the
//! slot, unreachable until it comes back); memory it reads is lent as a
//! shared frozen buffer (read only until every handle is gone); memory a
//! peer may write is never readable as Rust values. Rings never let more
//! WQEs be in flight than the queues and CQs hold.

#![warn(missing_docs)]

pub mod buf;
pub mod error;
pub mod model;
pub mod nic;
pub mod pd;
pub mod qp;
pub mod refine;
pub mod ring;
pub mod transport;
pub mod verbs;
pub mod wc;

pub use buf::{Buf, BufError, BufMut, Chunks, Exposed, Leases, Part, RingBuf};
pub use error::{Error, Result};
pub use nic::{
    Access, ConnectAttr, Endpoint, Gid, GidEntry, Nic, PortInfo, QpCaps, RemoteBuf, WrOp,
};
pub use pd::{Context, Pd};
pub use qp::{QueuePair, Request};
pub use ring::Seq;
pub use transport::{RC, Reliable, Transport, UC};
pub use wc::{Opcode, Status, Wc};
