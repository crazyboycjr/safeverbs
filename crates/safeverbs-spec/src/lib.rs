//! The formal model of safeverbs and its guarantees, checked by Verus.
//!
//! safeverbs promises one thing: under a model **M** of the RDMA NIC, whatever the peer does,
//! the library behaves like a small abstract machine **A**. This crate holds M, A, the
//! guarantees as lemmas about A, and the refinement theorem the library's core proves.
//! Read it in this order:
//!
//! 1. [`abs`]: **A**, the abstract machine. Buffers are *leases*, each held by the program,
//!    by a posted request or by the peer; every library call moves leases.
//! 2. [`guarantees`]: **S1** (no conflicting access between the program and the NIC),
//!    **F1** (each request gets its own outcome, once, with its own buffers), **R1** (the
//!    library never loses track of a buffer: one place at a time, released once, after the
//!    NIC is done) and **L1** (no lost wakeup), proved about every state A can reach.
//! 3. [`nic`]: **M**, the trusted model of the NIC as one endpoint sees it. Each verb's
//!    precondition is what **S2** (no local misuse) promises the library meets.
//! 4. [`refinement`]: **Theorem 1**, as the interface the core must prove, and how A and M
//!    fit together.
//! 5. [`share`]: each worker's share of M, and what the threads of one device rely on and
//!    guarantee each other.
//! 6. [`compose`]: several engines on one device, and the buffers exposed to a peer, as one state
//!    of A coupled to M.
//! 7. [`live`]: **liveness**, that outcomes come, under a premise about the device: it executes
//!    the WQEs it holds, and the CQEs are polled.
//! 8. [`examples`]: concrete executions of A and M.
//!
//! [`types`] holds what A and M share; [`abs_proofs`] and [`nic_proofs`] hold the proofs
//! that their invariants are inductive.
//!
//! Everything here is ghost code: a plain `cargo build` compiles it away, and
//! `scripts/verify.sh` checks it. `docs/verification.md` says what is proved and what is
//! assumed; `docs/design.md` explains the design.
#![no_std]
// `state_machine!` names its modules after the machines (`Nic`, `Abs`).
#![allow(non_snake_case)]

pub mod abs;
pub mod abs_proofs;
pub mod compose;
pub mod examples;
pub mod guarantees;
pub mod live;
pub mod nic;
pub mod nic_proofs;
pub mod refinement;
pub mod share;
pub mod types;
