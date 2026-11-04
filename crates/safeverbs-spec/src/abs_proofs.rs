//! The proof that A's invariants are inductive.
//!
//! Nothing here is needed to read A: each `#[inductive]` block in `abs.rs` proves its step, and
//! this gathers them.
#![allow(unused_imports)]

use crate::abs::*;
use crate::types::*;
use vstd::prelude::*;

verus! {

/// Every step keeps A's invariants.
pub proof fn lemma_next(pre: Abs::State, post: Abs::State)
    requires
        pre.invariant(),
        Abs::State::next(pre, post),
    ensures
        post.invariant(),
{
    reveal(Abs::State::next);
    reveal(Abs::State::next_by);
    let step = choose|step: Abs::Step| Abs::State::next_by(pre, post, step);
    match step {
        Abs::Step::create_qp(q) => Abs::State::create_qp_inductive(pre, post, q),
        Abs::Step::drop_qp(q) => Abs::State::drop_qp_inductive(pre, post, q),
        Abs::Step::alloc(mem, any_bytes) => Abs::State::alloc_inductive(pre, post, mem, any_bytes),
        Abs::Step::split(l, left) => Abs::State::split_inductive(pre, post, l, left),
        Abs::Step::freeze(l) => Abs::State::freeze_inductive(pre, post, l),
        Abs::Step::share(l, sub) => Abs::State::share_inductive(pre, post, l, sub),
        Abs::Step::thaw(l) => Abs::State::thaw_inductive(pre, post, l),
        Abs::Step::drop(l) => Abs::State::drop_inductive(pre, post, l),
        Abs::Step::post_request(dirs, q) => Abs::State::post_request_inductive(pre, post, dirs, q),
        Abs::Step::complete(r, o) => Abs::State::complete_inductive(pre, post, r, o),
        Abs::Step::register(r, w) => Abs::State::register_inductive(pre, post, r, w),
        Abs::Step::take(r, o) => Abs::State::take_inductive(pre, post, r, o),
        Abs::Step::detach(r) => Abs::State::detach_inductive(pre, post, r),
        Abs::Step::expose(l, remote_write) => Abs::State::expose_inductive(pre, post, l, remote_write),
        Abs::Step::unexpose(l) => Abs::State::unexpose_inductive(pre, post, l),
        Abs::Step::dummy_to_use_type_params(_) => {},
    }
}

} // verus!
