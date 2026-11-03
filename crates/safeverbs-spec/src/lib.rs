//! The formal model of safeverbs and its guarantees, checked by Verus.
//!
//! safeverbs promises one thing: under a model **M** of the RDMA NIC, whatever the peer does,
//! the library behaves like a small abstract machine **A**. This crate holds M, A, the
//! guarantees as lemmas about A, and the refinement theorem the library's core proves.
//! Read it in this order:
//!
//! 1. [`abs`]: **A**, the abstract machine. Buffers are *leases* held by the program, by a
//!    posted request or by the peer; every library call moves leases.
//! 2. [`guarantees`]: **S1** (no conflicting access between the program and the NIC),
//!    **F1** (each request gets its own outcome, once, with its own buffers), **R1** (no
//!    buffer released early or twice, none outliving its queue pair) and **L1** (no lost
//!    wakeup), proved about every state A can reach.
//! 3. [`nic`]: **M**, the trusted model of the NIC as one endpoint sees it. Each verb's
//!    precondition is what **S2** (no local misuse) promises the library meets.
//! 4. [`refinement`]: **Theorem 1**, as the interface the core must prove, and how A and M
//!    fit together.
//! 5. [`share`]: each worker's share of M, and what the threads of one device rely on and
//!    guarantee each other.
//! 6. [`examples`]: concrete executions of A and M.
//!
//! [`types`] holds what A and M share; [`nic_proofs`] holds the proofs about M.
//!
//! Everything here is ghost code: a plain `cargo build` compiles it away, and
//! `scripts/verify.sh` checks it. `docs/verification.md` says what is proved and what is
//! assumed; `docs/design.md` explains the design.
#![no_std]
// `state_machine!` names its modules after the machines (`Nic`, `Abs`).
#![allow(non_snake_case)]

pub mod abs;
pub mod examples;
pub mod guarantees;
pub mod nic;
pub mod nic_proofs;
pub mod refinement;
pub mod share;
pub mod types;
