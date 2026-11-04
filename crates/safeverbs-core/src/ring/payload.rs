//! What a slot of a worker's rings holds for a request: its buffers and, if it
//! has an owner, its context. To the ring this is one value, its generic `L`:
//! that it comes back exactly once, with its own request's outcome, is what the
//! ring proves of every `L`.
//!
//! A request gives up its context when it gives up its owner ([`Ring::give_up`]),
//! so that a slot without an owner never holds one. Retiring such a request
//! during a poll therefore runs no destructor of the user's, and none can
//! unwind through a poll with completions still to record.

use super::*;
use crate::buf::Leases;
use crate::nic::Nic;

verus! {

/// What a slot of a worker's rings holds for a request.
#[verifier::reject_recursive_types(N)]
pub struct Payload<C, N: Nic> {
    /// The owner's context, which comes back once with the request's outcome;
    /// `None` for a request without an owner.
    pub ctx: Option<C>,
    /// Its buffers.
    pub leases: Leases<N>,
}

impl<C, N: Nic> Payload<C, N> {
    /// The same buffers, without a context.
    pub open spec fn without_ctx(self) -> Self {
        Payload { ctx: None, ..self }
    }
}

impl<L> Ring<L> {
    /// Changing the payload of a request that holds one changes nothing else.
    proof fn lemma_payload_update(&self, pre: &Self, seq: int, new: L)
        requires
            pre.inv(),
            self.same_counters(pre),
            pre.head <= seq < pre.posted,
            pre.slot(seq).seq == seq,
            pre.slot(seq).state != State::Free,
            pre.slot(seq).leases is Some,
            self.slots@ == pre.slots@.update(seq % pre.n(), Slot { leases: Some(new), ..pre.slot(seq) }),
        ensures
            self.inv(),
            forall|s: int| #[trigger] self.req(s) == if s == seq && pre.req(s) is Some {
                Some(ReqView { leases: new, ..pre.req(s)->0 })
            } else { pre.req(s) },
            forall|s: int| #[trigger] self.staged(s) == if s == seq && pre.staged(s) is Some {
                Some(ReqView { leases: new, ..pre.staged(s)->0 })
            } else { pre.staged(s) },
            forall|s: int| #[trigger] self.refused(s) == if s == seq && pre.refused(s) is Some {
                Some(new)
            } else { pre.refused(s) },
            forall|s: int, w: int| #[trigger] self.dma(s, w) == if s == seq && pre.dma(s, w) is Some {
                Some(new)
            } else { pre.dma(s, w) },
            self.same_meta(pre),
    {
        reveal(Ring::inv_slots);
        reveal(Ring::inv_wqes);
        let n = pre.n();
        assert(self.slot(seq) == Slot { leases: Some(new), ..pre.slot(seq) });
        assert forall|s: int| pre.head <= s < pre.head + n && s != seq implies self.slot(s) == pre.slot(s) by {
            lemma_other(n, seq, s);
        }
        assert forall|s: int| self.head <= s < self.retired implies #[trigger] self.done_ok(s) by {
            assert(pre.done_ok(s));
        }
        assert forall|s: int| self.retired <= s < self.posted implies #[trigger] self.live_ok(s) by {
            assert(pre.live_ok(s));
        }
        assert forall|s: int| self.posted <= s < self.head + self.n() implies #[trigger] self.free_ok(s) by {
            assert(pre.free_ok(s));
        }
        assert forall|s: int| self.retired < s < self.posted implies #[trigger] self.slot(s).first_wqe
            == self.slot(s - 1).end_wqe by {
            assert(pre.slot(s).first_wqe == pre.slot(s - 1).end_wqe);
        }
        if self.retired < self.posted {
            assert(pre.slot(self.retired as int).first_wqe == self.slot(self.retired as int).first_wqe);
        }
        if self.retired < self.committed {
            assert(pre.slot(self.committed - 1).end_wqe == self.slot(self.committed - 1).end_wqe);
        }
        if self.committed < self.posted {
            assert(pre.slot(self.posted - 1).end_wqe == self.slot(self.posted - 1).end_wqe);
        }
        if self.head < self.retired {
            assert(pre.slot(self.head as int).state == self.slot(self.head as int).state);
        }
        assert forall|s: int| #[trigger] self.req(s) == if s == seq && pre.req(s) is Some {
            Some(ReqView { leases: new, ..pre.req(s)->0 })
        } else { pre.req(s) } by {
            if pre.head <= s < pre.committed && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.staged(s) == if s == seq && pre.staged(s) is Some {
            Some(ReqView { leases: new, ..pre.staged(s)->0 })
        } else { pre.staged(s) } by {
            if pre.committed <= s < pre.posted && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.refused(s) == if s == seq && pre.refused(s) is Some {
            Some(new)
        } else { pre.refused(s) } by {
            if pre.head <= s < pre.committed && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int, w: int| #[trigger] self.dma(s, w) == if s == seq && pre.dma(s, w) is Some {
            Some(new)
        } else { pre.dma(s, w) } by {
            if pre.head <= s < pre.posted && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
        assert forall|s: int| #[trigger] self.meta(s) == pre.meta(s) by {
            if pre.head <= s < pre.posted && s != seq {
                assert(self.slot(s) == pre.slot(s));
            }
        }
    }
}

impl<C, N: Nic> Ring<Payload<C, N>> {
    /// Gives up ownership of request `seq`, as [`Ring::detach`] does, and hands
    /// back its context, if it has one.
    pub fn give_up(&mut self, seq: Seq) -> (ctx: Option<C>)
    {
        let ctx = if seq < self.head || seq >= self.posted {
            None
        } else {
            let i = self.index(seq);
            match &mut self.slots[i].leases {
                Some(payload) => payload.ctx.take(),
                None => None,
            }
        };
        self.detach(seq);
        ctx
    }
}

} // verus!
