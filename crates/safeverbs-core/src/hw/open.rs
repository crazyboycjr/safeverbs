//! A request being written: nothing of it reaches the hardware or the ring until it is closed,
//! when its signaling is known, so that neither has a state for it, and the program may do
//! anything between two of its WQEs.
//!
//! What it is written into comes from its QP and goes back there (`Hw::open_request`,
//! `Hw::give_back`): nothing is allocated per request. WQEs of one frozen buffer keep one reference
//! to it, and a list of several buffers is reused too, by the requests that hand it back.
//!
//! Its invariant (`OpenRequest::inv`) says that its WQEs are what its ledger of inputs says, each
//! input kept alive by what it holds. It is a value of its own, so what a WQE asks of a QP is asked
//! again of the QP it goes out on (`HwQp::takes`).
#![allow(unused_imports)]
use super::*;
#[cfg(verus_keep_ghost)]
use crate::buf::covers_inputs;
use crate::buf::{Lend, Loan, Peek, WqeLoans};
use crate::nic::RemoteBuf;

verus! {

/// A WQE of a request being written: its operation and its scatter/gather list, which is its
/// `sge` if it has one, as WQEs mostly do, else lies in `OpenRequest::gathered` from `first_sge`.
#[derive(Clone, Copy)]
pub struct OpenWqe {
    pub(super) op: WrOp,
    pub(super) sge: Sge,
    pub(super) first_sge: u32,
    pub(super) sges: u32,
}

/// What `OpenRequest::wqes` is filled with until it is written.
pub(super) const EMPTY_WQE: OpenWqe = OpenWqe {
    op: WrOp::Send,
    sge: Sge { addr: 0, length: 0, lkey: 0 },
    first_sge: 0,
    sges: 0,
};

/// A request being written, a value of its own: `Engine::open_request` makes one, its WQEs go in
/// without the engine while all is well, and `Engine::close_open` or `Engine::submit_open` send
/// it. Dropping it drops its WQEs, which never left it.
#[verifier::reject_recursive_types(N)]
pub struct OpenRequest<N: Device> {
    /// Its WQEs, the first `n` of them; as long as its QP's requests may be.
    pub(super) wqes: Vec<OpenWqe>,
    /// How many it has.
    pub(super) n: usize,
    /// How many it may have before its QP has to be asked: 0 sends every WQE there.
    pub(super) limit: usize,
    /// The longest message of its QP.
    pub(super) max_len: u32,
    /// The protection domain of its QP.
    pub(super) pd_id: u64,
    /// What keeps its buffers alive.
    pub(super) kept: Kept<N>,
    /// The scatter/gather lists of its WQEs of several elements.
    pub(super) gathered: Vec<Sge>,
    /// Whether some WQE of it has the NIC write its buffers, which asks for a reliable QP.
    pub(super) reads: bool,
    /// The longest scatter/gather list of its WQEs.
    pub(super) max_sges: u32,
    /// What each of its WQEs lends (erased).
    #[allow(dead_code)]
    pub(super) ledger: Ghost<Seq<WqeLoans>>,
}

/// Whether a WQE doing `op` on `len` bytes needs nothing more of its QP than the length and the
/// protection domain.
#[inline(always)]
pub(super) fn plain_op(op: &WrOp, len: u64) -> (plain: bool)
    ensures
        plain ==> !opcode_model(*op).writes_local(),
{
    match *op {
        WrOp::Send | WrOp::SendImm(_) => true,
        WrOp::Write(r) | WrOp::WriteImm(r, _) => len <= r.len,
        WrOp::Read(_) | WrOp::FetchAdd { .. } | WrOp::CompareSwap { .. } => false,
    }
}

/// `core::slice::from_ref`, which vstd does not specify.
#[verifier::external_body]
#[inline(always)]
pub(super) fn one_of<T>(value: &T) -> (list: &[T])
    ensures
        list@ == seq![*value],
{
    core::slice::from_ref(value)
}

/// The WR of one element: what it names and its bytes.
pub(super) proof fn lemma_one_wr(id: u64, signaled: bool, op: WrOp, sges: Seq<Sge>, sge: Sge)
    requires
        sges == seq![sge],
    ensures
        send_wr(id, signaled, op, sges).sges == seq![sge_model(sge)],
        total_len(send_wr(id, signaled, op, sges).sges) == sge.length,
{
    let wr = send_wr(id, signaled, op, sges);
    assert(wr.sges =~= seq![sge_model(sge)]);
    assert(wr.sges.drop_last() =~= Seq::<safeverbs_spec::nic::Sge>::empty());
    reveal_with_fuel(total_len, 2);
}

/// The scatter/gather list of `parts`, in their order.
pub(super) open spec fn parts_sges<N: Device>(parts: Seq<Part<N>>) -> Seq<Sge> {
    parts.map_values(|part: Part<N>| part_sge(part))
}

/// The list of one part.
pub(super) proof fn lemma_parts_one<N: Device>(part: Part<N>)
    ensures
        parts_sges(seq![part]) == seq![part_sge(part)],
        sges_len(parts_sges(seq![part])) == part_sge(part).length,
{
    assert(parts_sges(seq![part]) =~= seq![part_sge(part)]);
    lemma_sges_len_one(part_sge(part));
}

/// The bytes of a list of one element.
pub(super) proof fn lemma_sges_len_one(sge: Sge)
    ensures
        sges_len(seq![sge]) == sge.length,
{
    let f = |sge: Sge| sge_model(sge);
    let m = seq![sge].map_values(f);
    assert(m.drop_last() =~= Seq::<safeverbs_spec::nic::Sge>::empty());
    assert(m.last() == sge_model(sge));
    reveal_with_fuel(total_len, 2);
}

/// What matched the inputs still does with more of them.
pub(super) proof fn lemma_matches_more<N: Device>(entry: WqeLoans, loans: Seq<Loan<N>>, more: Seq<Loan<N>>)
    requires
        entry.matches(loans),
        loans.len() <= more.len(),
        forall|i: int| 0 <= i < loans.len() ==> #[trigger] more[i] == loans[i],
    ensures
        entry.matches(more),
{
    assert forall|j: int| 0 <= j < entry.sges.len() implies {
        let loan = #[trigger] more[entry.end() - 1 - j];
        &&& loan.part.spec_sge() == entry.sges[j]
        &&& loan.access == Some(entry.access())
    } by {
        assert(more[entry.end() - 1 - j] == loans[entry.end() - 1 - j]);
    }
}

impl<N: Device> OpenRequest<N> {
    /// The scatter/gather list of WQE `k`.
    pub open(super) spec fn sges_at(&self, k: int) -> Seq<Sge> {
        let w = self.wqes@[k];
        if w.sges == 1 {
            seq![w.sge]
        } else {
            self.gathered@.subrange(w.first_sge as int, w.first_sge as int + w.sges as int)
        }
    }

    /// WQE `k` as its ledger has it, what it asks of its QP in the fields that say so.
    pub open(super) spec fn wqe_ok(&self, k: int) -> bool {
        let w = self.wqes@[k];
        let entry = self.ledger@[k];
        &&& w.sges >= 1
        &&& w.sges != 1 ==> w.first_sge as int + w.sges as int <= self.gathered@.len()
        &&& entry.opcode == Some(opcode_model(w.op))
        &&& entry.sges == self.sges_at(k)
        &&& entry.matches(self.kept.loans())
        &&& sges_len(entry.sges) <= self.max_len
        &&& opcode_model(w.op).writes_local() ==> self.reads
        &&& w.sges <= self.max_sges
    }

    /// Its WQEs, their inputs and what keeps those alive: every input belongs to a WQE, each WQE
    /// has its own, all of its protection domain.
    pub open(super) spec fn inv(&self) -> bool {
        &&& self.n <= self.wqes@.len()
        &&& self.limit <= self.wqes@.len()
        &&& self.ledger@.len() == self.n
        &&& self.kept.inv()
        &&& 1 <= self.max_sges
        &&& forall|k: int| 0 <= k < self.n ==> #[trigger] self.wqe_ok(k)
        &&& forall|i: int, j: int| 0 <= i < j < self.n
            ==> (#[trigger] self.ledger@[i]).end() <= (#[trigger] self.ledger@[j]).first
        &&& covers_inputs(self.ledger@, self.kept.loans().len() as int)
        &&& forall|i: int| 0 <= i < self.kept.loans().len()
            ==> (#[trigger] self.kept.loans()[i]).part.spec_pd() == self.pd_id
    }

    /// One more WQE at the end, with inputs of its own, keeps what is said of the others.
    pub(super) proof fn lemma_added(&self, pre: &Self)
        requires
            pre.inv(),
            self.n == pre.n + 1,
            self.n <= self.wqes@.len(),
            self.limit <= self.wqes@.len(),
            self.kept.inv(),
            1 <= self.max_sges,
            forall|k: int| 0 <= k < pre.n ==> #[trigger] self.wqes@[k] == pre.wqes@[k],
            pre.gathered@.len() <= self.gathered@.len(),
            forall|i: int| 0 <= i < pre.gathered@.len() ==> #[trigger] self.gathered@[i] == pre.gathered@[i],
            pre.kept.loans().len() <= self.kept.loans().len(),
            forall|i: int| 0 <= i < pre.kept.loans().len() ==> #[trigger] self.kept.loans()[i] == pre.kept.loans()[i],
            self.ledger@ == pre.ledger@.push(self.ledger@[pre.n as int]),
            self.ledger@[pre.n as int].first == pre.kept.loans().len(),
            self.ledger@[pre.n as int].end() == self.kept.loans().len(),
            self.wqe_ok(pre.n as int),
            self.max_len == pre.max_len,
            self.pd_id == pre.pd_id,
            pre.reads ==> self.reads,
            pre.max_sges <= self.max_sges,
            forall|i: int| pre.kept.loans().len() <= i < self.kept.loans().len()
                ==> (#[trigger] self.kept.loans()[i]).part.spec_pd() == self.pd_id,
        ensures
            self.inv(),
    {
        let loans = pre.kept.loans();
        let more = self.kept.loans();
        let new = self.ledger@[pre.n as int];
        assert forall|k: int| 0 <= k < self.n implies #[trigger] self.wqe_ok(k) by {
            if k < pre.n {
                assert(pre.wqe_ok(k));
                assert(self.ledger@[k] == pre.ledger@[k]);
                assert(self.wqes@[k] == pre.wqes@[k]);
                let w = self.wqes@[k];
                if w.sges != 1 {
                    assert(self.gathered@.subrange(w.first_sge as int, w.first_sge as int + w.sges as int)
                        =~= pre.gathered@.subrange(w.first_sge as int, w.first_sge as int + w.sges as int));
                }
                assert(self.sges_at(k) == pre.sges_at(k));
                lemma_matches_more(pre.ledger@[k], loans, more);
            }
        }
        assert forall|i: int, j: int| 0 <= i < j < self.n
            implies (#[trigger] self.ledger@[i]).end() <= (#[trigger] self.ledger@[j]).first by {
            assert(self.ledger@[i] == pre.ledger@[i]);
            assert(pre.wqe_ok(i));
            if j < pre.n {
                assert(self.ledger@[j] == pre.ledger@[j]);
            }
        }
        assert forall|i: int| 0 <= i < more.len() implies #[trigger] crate::buf::input_is_covered(self.ledger@, i) by {
            if i < loans.len() {
                assert(crate::buf::input_is_covered(pre.ledger@, i));
                let w = choose|w: int| 0 <= w < pre.ledger@.len()
                    && (#[trigger] pre.ledger@[w]).first <= i < pre.ledger@[w].end();
                assert(self.ledger@[w] == pre.ledger@[w]);
            } else {
                assert(self.ledger@[pre.n as int] == new);
            }
        }
        assert forall|i: int| 0 <= i < more.len() implies (#[trigger] more[i]).part.spec_pd() == self.pd_id by {
            if i < loans.len() {
                assert(more[i] == loans[i]);
            }
        }
    }

    /// It has no WQE.
    pub open(super) spec fn has_no_wqe(&self) -> bool {
        self.n == 0
    }

    /// Its inputs.
    pub open(super) spec fn loans(&self) -> Seq<Loan<N>> {
        self.kept.loans()
    }

    /// Without a WQE it has no input.
    pub(super) proof fn lemma_no_inputs(&self)
        requires
            self.inv(),
            self.n == 0,
        ensures
            self.kept.loans().len() == 0,
    {
        if self.kept.loans().len() > 0 {
            assert(crate::buf::input_is_covered(self.ledger@, 0));
        }
    }

    /// One that holds nothing and takes nothing.
    pub fn closed() -> (r: Self)
        ensures
            r.inv(),
            r.has_no_wqe(),
    {
        OpenRequest {
            wqes: Vec::new(),
            n: 0,
            limit: 0,
            max_len: 0,
            pd_id: 0,
            kept: Kept::new(),
            gathered: Vec::new(),
            reads: false,
            max_sges: 1,
            ledger: Ghost(Seq::empty()),
        }
    }

    /// How many WQEs it has.
    #[inline(always)]
    pub fn len(&self) -> (n: usize) {
        self.n
    }

    /// Whether it has no WQE.
    #[inline(always)]
    pub fn is_empty(&self) -> (empty: bool) {
        self.n == 0
    }

    /// Its regions are registered (its leases say so).
    #[inline(always)]
    pub(super) fn keeps_registered(&self, pd: &Pd<N>, Tracked(token): Tracked<&mut NicToken>)
        requires
            forall|j: int| 0 <= j < self.kept.loans().len()
                ==> part_pd((#[trigger] self.kept.loans()[j]).part) == pd_id(*pd),
        ensures
            final(token).same_engine(old(token)),
            final(token).same_writings(old(token)),
            rely_reaches(old(token).share(), pinned(*old(token)), old(token).nic(), final(token).nic()),
            forall|j: int|
                0 <= j < self.kept.loans().len() ==> final(token).nic().registered(
                    pd_of(*pd),
                    sge_model(part_sge((#[trigger] self.kept.loans()[j]).part)),
                    part_writable(self.kept.loans()[j].part),
                ),
    {
        registered_all(&self.kept, pd, Tracked(token));
    }

    /// Whether it still holds what it keeps from its QP.
    #[inline(always)]
    pub(crate) fn holds_storage(&self) -> (holds: bool) {
        self.wqes.len() > 0
    }

    /// Whether the next WQE goes in without its QP being asked.
    #[inline(always)]
    pub fn has_room(&self) -> (room: bool) {
        self.n < self.limit
    }

    /// From now on every WQE goes to its QP.
    pub fn stop(&mut self)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
    {
        let ghost before = *self;
        self.limit = 0;
        proof {
            self.lemma_same_wqes(&before);
        }
    }

    /// What is said of its WQEs depends on them, their lists, their inputs and limits.
    pub(super) proof fn lemma_same_wqes(&self, pre: &Self)
        requires
            pre.inv(),
            self.n == pre.n,
            self.wqes@ == pre.wqes@,
            self.limit <= self.wqes@.len(),
            self.gathered@ == pre.gathered@,
            self.kept == pre.kept,
            self.ledger == pre.ledger,
            self.max_len == pre.max_len,
            self.pd_id == pre.pd_id,
            self.reads == pre.reads,
            self.max_sges == pre.max_sges,
        ensures
            self.inv(),
    {
        assert forall|k: int| 0 <= k < self.n implies #[trigger] self.wqe_ok(k) by {
            assert(pre.wqe_ok(k));
        }
    }

    /// What WQE `i`, one of those it has, does.
    #[inline(always)]
    pub(super) fn op_of(&self, i: usize) -> (op: &WrOp)
        requires
            self.inv(),
            i < self.n,
        ensures
            *op == self.wqes@[i as int].op,
    {
        // SAFETY: `i < n`, and `n` is at most the length of `wqes`.
        unsafe { &self.wqes.as_slice().get_unchecked(i).op }
    }

    /// The scatter/gather list of WQE `i`, one of those it has.
    #[inline(always)]
    pub(super) fn sges_of(&self, i: usize) -> (sges: &[Sge])
        requires
            self.inv(),
            i < self.n,
        ensures
            sges@ == self.sges_at(i as int),
    {
        proof {
            assert(self.wqe_ok(i as int));
        }
        // SAFETY: `i < n`, and `n` is at most the length of `wqes`.
        let wqe = unsafe { self.wqes.as_slice().get_unchecked(i) };
        if wqe.sges == 1 {
            one_of(&wqe.sge)
        } else {
            let from = wqe.first_sge as usize;
            vstd::slice::slice_subrange(self.gathered.as_slice(), from, from + wqe.sges as usize)
        }
    }

    /// The bytes of its last WQE, which has a gather list.
    pub(super) fn last_gathered_bytes(&self) -> (bytes: u32)
        requires
            self.inv(),
            self.n > 0,
            self.wqes@[self.n - 1].sges != 1,
        ensures
            bytes == sges_len(self.sges_at(self.n - 1)),
    {
        proof {
            assert(self.wqe_ok(self.n - 1));
        }
        let ghost k = self.n - 1;
        let ghost list = self.sges_at(k);
        let last = &self.wqes[self.n - 1];
        let from = last.first_sge as usize;
        let mut bytes = 0u32;
        let mut i = 0;
        proof {
            assert(list.take(0).map_values(|s: Sge| sge_model(s)) =~= Seq::<safeverbs_spec::nic::Sge>::empty());
        }
        while i < last.sges as usize
            invariant
                self.inv(),
                k == self.n - 1,
                self.wqe_ok(k),
                last == self.wqes@[k],
                from == last.first_sge,
                list == self.sges_at(k),
                last.sges != 1,
                i <= last.sges,
                bytes == sges_len(list.take(i as int)),
            decreases last.sges - i,
        {
            proof {
                lemma_sges_len_step(list, i as int);
                lemma_sges_len_prefix(list, (i + 1) as int);
                assert(list[i as int] == self.gathered@[from + i]);
            }
            bytes += self.gathered[from + i].length;
            i += 1;
        }
        proof {
            assert(list.take(i as int) =~= list);
        }
        bytes
    }

    /// Its buffers, leaving it none, for the WQEs `wqes` it was written as. Starting a list of
    /// several takes the room in `spare`.
    #[inline(always)]
    pub(super) fn take_leases_with(&mut self, spare: &mut Vec<Lease<N>>, Ghost(wqes): Ghost<Seq<WqeLoans>>) -> (leases: Leases<N>)
        requires
            old(self).inv(),
            old(spare)@.len() == 0,
            forall|i: int| 0 <= i < wqes.len() ==> (#[trigger] wqes[i]).matches(old(self).kept.loans()),
            forall|i: int, j: int| 0 <= i < j < wqes.len() ==> (#[trigger] wqes[i]).end() <= (#[trigger] wqes[j]).first,
        ensures
            final(self).inv(),
            final(self).n == 0,
            final(self).wqes@ == old(self).wqes@,
            final(self).limit == old(self).limit,
            final(self).pd_id == old(self).pd_id,
            final(self).max_len == old(self).max_len,
            final(self).reads == old(self).reads,
            final(self).max_sges == old(self).max_sges,
            final(self).gathered@ == old(self).gathered@,
            final(self).kept.loans().len() == 0,
            final(spare)@.len() == 0,
            leases.inv(),
            leases.loans() == old(self).kept.loans(),
            leases.wqes() == wqes,
    {
        self.n = 0;
        self.ledger = Ghost(Seq::empty());
        self.kept.take(spare, Ghost(wqes))
    }

    /// Its buffers, leaving it none (of a request that is not going out).
    pub fn take_leases(&mut self) -> (leases: Leases<N>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
    {
        self.take_leases_with(&mut Vec::new(), Ghost(Seq::empty()))
    }

    /// Adds a WQE doing `op` on `part` if nothing is out of the common: the checks pass, it has
    /// room, and keeping `part` takes no allocation. Otherwise `part` comes back and nothing
    /// changed, for its QP to say (`Engine::push_open`).
    #[inline(always)]
    pub fn try_push(&mut self, op: WrOp, part: Part<N>) -> (r: core::result::Result<(), Part<N>>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            r matches Err(back) ==> back == part,
    {
        let sge = part_sge_of(&part);
        let k = self.n;
        if k < self.limit && plain_op(&op, sge.length as u64) && part_pd_of(&part) == self.pd_id
            && sge.length <= self.max_len
        {
            let ghost before = *self;
            match self.kept.try_keep(part, Ghost(safeverbs_spec::abs::Dir::Out)) {
                Ok(()) => {
                    write_wqe(&mut self.wqes, k, OpenWqe { op, sge, first_sge: 0, sges: 1 });
                    self.n = k + 1;
                    let ghost entry = WqeLoans {
                        wr_id: 0,
                        opcode: Some(opcode_model(op)),
                        sges: seq![sge],
                        first: before.kept.loans().len() as int,
                    };
                    self.ledger = Ghost(before.ledger@.push(entry));
                    proof {
                        self.lemma_one_more(&before, part, safeverbs_spec::abs::Dir::Out, entry);
                    }
                    Ok(())
                },
                Err(part) => {
                    proof {
                        self.lemma_kept_same(&before);
                    }
                    Err(part)
                },
            }
        } else {
            Err(part)
        }
    }

    /// The same, whatever fields of it the program can tell: what closing it keeps.
    pub open(super) spec fn same_fields(&self, pre: &Self) -> bool {
        &&& self.inv()
        &&& self.pd_id == pre.pd_id
        &&& self.max_len == pre.max_len
        &&& self.reads == pre.reads
        &&& self.max_sges == pre.max_sges
        &&& self.wqes@.len() == pre.wqes@.len()
    }

    /// The WR of WQE `k`, whose number is `first + k`.
    pub(super) open spec fn wr_of(&self, k: int, index: u16, first: u64, signaled: bool) -> SendWr {
        send_wr(spec_wr_id(index, false, (first + k) as u64), signaled, self.wqes@[k].op, self.sges_at(k))
    }

    /// The WRs of its WQEs, numbered from `first`, the last signaled if `signaled`.
    pub(super) open spec fn posted_wrs(&self, index: u16, first: u64, signaled: bool) -> Seq<SendWr> {
        Seq::new(self.n as nat, |k: int| self.wr_of(k, index, first, signaled && k == self.n - 1))
    }

    /// Its ledger, its WQEs numbered from `first`.
    pub(super) open spec fn posted_ledger(&self, index: u16, first: u64) -> Seq<WqeLoans> {
        Seq::new(self.n as nat, |k: int| WqeLoans {
            wr_id: spec_wr_id(index, false, (first + k) as u64),
            ..self.ledger@[k]
        })
    }

    /// What M asks of its WRs, once the regions of its inputs are registered and its QP takes it;
    /// and its ledger, with their numbers, is one for the ring.
    pub(super) proof fn lemma_posted(&self, index: u16, first: u64, signaled: bool, m: Nic::State, attr: QpAttr)
        requires
            self.inv(),
            1 <= self.n,
            forall|j: int| 0 <= j < self.kept.loans().len() ==> m.registered(
                attr.pd,
                sge_model(part_sge((#[trigger] self.kept.loans()[j]).part)),
                part_writable(self.kept.loans()[j].part),
            ),
            self.max_len <= m.caps.max_msg,
            self.max_sges <= attr.max_sge,
            self.reads ==> attr.transport == Transport::Rc,
            attr.transport == Transport::Rc || attr.transport == Transport::Uc,
        ensures ({
            let more = self.posted_wrs(index, first, signaled);
            let wqes = self.posted_ledger(index, first);
            let loans = self.kept.loans();
            &&& more.len() == self.n
            &&& wqes.len() == self.n
            &&& forall|j: int| 0 <= j < more.len() ==> {
                let wr = #[trigger] more[j];
                &&& wr.wr_id == spec_wr_id(index, false, (first + j) as u64)
                &&& m.send_wr_ok(attr, wr)
                &&& !wr.inline
            }
            &&& more.last().opcode == opcode_model(self.wqes@[self.n - 1].op)
            &&& total_len(more.last().sges) == sges_len(self.sges_at(self.n - 1))
            &&& forall|i: int| 0 <= i < wqes.len() ==> (#[trigger] wqes[i]).matches(loans)
            &&& forall|i: int, j: int| 0 <= i < j < wqes.len() ==> (#[trigger] wqes[i]).end() <= (#[trigger] wqes[j]).first
            &&& covers_inputs(wqes, loans.len() as int)
        }),
    {
        let more = self.posted_wrs(index, first, signaled);
        let wqes = self.posted_ledger(index, first);
        let loans = self.kept.loans();
        self.kept.lemma_loans();
        assert forall|j: int| 0 <= j < more.len() implies {
            let wr = #[trigger] more[j];
            &&& wr.wr_id == spec_wr_id(index, false, (first + j) as u64)
            &&& m.send_wr_ok(attr, wr)
            &&& !wr.inline
        } by {
            assert(self.wqe_ok(j));
            let entry = self.ledger@[j];
            let wr = more[j];
            let list = self.sges_at(j);
            let f = |sge: Sge| sge_model(sge);
            assert(wr.sges =~= list.map_values(f));
            assert(sges_len(list) == total_len(wr.sges));
            assert(entry.sges == list);
            assert(wr.opcode == opcode_model(self.wqes@[j].op));
            assert forall|i: int| 0 <= i < wr.sges.len()
                implies m.registered(attr.pd, #[trigger] wr.sges[i], wr.opcode.writes_local()) by {
                let loan = loans[entry.end() - 1 - i];
                assert(loan.part.spec_sge() == entry.sges[i]);
                assert(wr.sges[i] == sge_model(list[i]));
                assert(m.registered(attr.pd, sge_model(part_sge(loan.part)), part_writable(loan.part)));
            }
            assert(list.len() == self.wqes@[j].sges) by {
                if self.wqes@[j].sges == 1 {
                    assert(list =~= seq![self.wqes@[j].sge]);
                }
            }
            assert(wr.sges.len() == list.len());
        }
        let last = self.n - 1;
        assert(more.last() == more[last as int]);
        let f = |sge: Sge| sge_model(sge);
        assert(more[last as int].sges =~= self.sges_at(last as int).map_values(f));
        assert forall|i: int| 0 <= i < wqes.len() implies (#[trigger] wqes[i]).matches(loans) by {
            assert(self.wqe_ok(i));
            assert(wqes[i].sges == self.ledger@[i].sges && wqes[i].first == self.ledger@[i].first
                && wqes[i].opcode == self.ledger@[i].opcode);
        }
        assert forall|i: int, j: int| 0 <= i < j < wqes.len()
            implies (#[trigger] wqes[i]).end() <= (#[trigger] wqes[j]).first by {
            assert(self.ledger@[i].end() <= self.ledger@[j].first);
        }
        assert forall|i: int| 0 <= i < loans.len() implies #[trigger] crate::buf::input_is_covered(wqes, i) by {
            assert(crate::buf::input_is_covered(self.ledger@, i));
            let w = choose|w: int| 0 <= w < self.ledger@.len()
                && (#[trigger] self.ledger@[w]).first <= i < self.ledger@[w].end();
            assert(wqes[w].first == self.ledger@[w].first && wqes[w].sges == self.ledger@[w].sges);
        }
    }

    /// Each of its WRs is in the ledger of `leases`, which recorded it.
    pub(super) proof fn lemma_ledger_of_posted(&self, leases: Leases<N>, index: u16, first: u64, signaled: bool)
        requires
            self.inv(),
            leases.wqes() == self.posted_ledger(index, first),
        ensures ({
            let more = self.posted_wrs(index, first, signaled);
            forall|j: int| 0 <= j < more.len() ==> ledger_has(
                leases,
                (#[trigger] more[j]).wr_id,
                Some(more[j].opcode),
                more[j].sges,
            )
        }),
    {
        let more = self.posted_wrs(index, first, signaled);
        assert forall|j: int| 0 <= j < more.len() implies ledger_has(
            leases,
            (#[trigger] more[j]).wr_id,
            Some(more[j].opcode),
            more[j].sges,
        ) by {
            assert(self.wqe_ok(j));
            lemma_ledger_wr(leases, j, more[j].wr_id, signaled && j == self.n - 1, self.wqes@[j].op, self.sges_at(j));
        }
    }

    /// Its list of elements may be another as long as none of its WQEs has it.
    pub(super) proof fn lemma_gathered_swapped(&self, pre: &Self)
        requires
            pre.inv(),
            self.gathered@ == pre.gathered@ || pre.gathered@.len() == 0,
            *self == (OpenRequest { gathered: self.gathered, ..*pre }),
        ensures
            self.inv(),
    {
        assert forall|k: int| 0 <= k < self.n implies #[trigger] self.wqe_ok(k) by {
            assert(pre.wqe_ok(k));
            if self.wqes@[k].sges != 1 {
                assert(self.gathered@ == pre.gathered@);
            }
            assert(self.sges_at(k) == pre.sges_at(k));
        }
    }

    /// A WQE of several elements in: its list at the end of `gathered`, its inputs at the end of
    /// its loans, the last element first.
    pub(super) proof fn lemma_gathered_more(&self, pre: &Self, built: Seq<Part<N>>, access: safeverbs_spec::abs::Dir, entry: WqeLoans, base: Seq<Sge>)
        requires
            pre.inv(),
            self.n == pre.n + 1,
            self.n <= self.wqes@.len(),
            self.limit == pre.limit,
            self.wqes@ == pre.wqes@.update(pre.n as int, self.wqes@[pre.n as int]),
            1 <= built.len() <= MAX_SGES,
            base == pre.gathered@,
            self.wqes@[pre.n as int].sges == built.len(),
            self.wqes@[pre.n as int].first_sge == base.len(),
            self.wqes@[pre.n as int].sge == part_sge(built[0]),
            self.gathered@.len() == base.len() + built.len(),
            forall|k: int| 0 <= k < base.len() ==> #[trigger] self.gathered@[k] == base[k],
            forall|k: int| 0 <= k < built.len() ==> #[trigger] self.gathered@[base.len() + k] == part_sge(built[k]),
            base.len() + built.len() <= u32::MAX,
            entry.opcode == Some(opcode_model(self.wqes@[pre.n as int].op)),
            entry.sges == parts_sges(built),
            entry.first == pre.kept.loans().len(),
            self.kept.inv(),
            self.kept.loans().len() == pre.kept.loans().len() + built.len(),
            forall|j: int| 0 <= j < pre.kept.loans().len() ==> #[trigger] self.kept.loans()[j] == pre.kept.loans()[j],
            forall|j: int| 0 <= j < built.len() ==> #[trigger] self.kept.loans()[pre.kept.loans().len() + (built.len() - 1 - j)]
                == (Loan { part: built[j], access: Some(access) }),
            access == entry.access(),
            self.ledger@ == pre.ledger@.push(entry),
            sges_len(parts_sges(built)) <= pre.max_len,
            forall|j: int| 0 <= j < built.len() ==> part_pd(#[trigger] built[j]) == pre.pd_id,
            self.max_len == pre.max_len,
            self.pd_id == pre.pd_id,
            pre.reads ==> self.reads,
            opcode_model(self.wqes@[pre.n as int].op).writes_local() ==> self.reads,
            pre.max_sges <= self.max_sges,
            built.len() <= self.max_sges,
        ensures
            self.inv(),
    {
        let k = pre.n as int;
        let w = self.wqes@[k];
        let list = parts_sges(built);
        if w.sges == 1 {
            assert(list =~= seq![w.sge]);
        } else {
            let sub = self.gathered@.subrange(w.first_sge as int, w.first_sge as int + w.sges as int);
            assert forall|j: int| 0 <= j < list.len() implies sub[j] == list[j] by {
                assert(self.gathered@[base.len() + j] == part_sge(built[j]));
            }
            assert(sub =~= list);
        }
        assert(self.sges_at(k) == entry.sges);
        assert(self.ledger@[k] == entry);
        assert(entry.end() == self.kept.loans().len());
        assert forall|j: int| 0 <= j < entry.sges.len() implies {
            let loan = #[trigger] self.kept.loans()[entry.end() - 1 - j];
            &&& loan.part.spec_sge() == entry.sges[j]
            &&& loan.access == Some(entry.access())
        } by {
            assert(self.kept.loans()[pre.kept.loans().len() + (built.len() - 1 - j)]
                == (Loan { part: built[j], access: Some(access) }));
        }
        assert(self.wqe_ok(k));
        assert forall|j: int| 0 <= j < pre.n implies #[trigger] self.wqes@[j] == pre.wqes@[j] by {}
        assert forall|i: int| pre.kept.loans().len() <= i < self.kept.loans().len()
            implies (#[trigger] self.kept.loans()[i]).part.spec_pd() == self.pd_id by {
            let j = built.len() - 1 - (i - pre.kept.loans().len());
            assert(self.kept.loans()[pre.kept.loans().len() + (built.len() - 1 - j)]
                == (Loan { part: built[j], access: Some(access) }));
        }
        self.lemma_added(pre);
    }

    /// Keeping nothing new leaves what is said of its WQEs as it was.
    proof fn lemma_kept_same(&self, pre: &Self)
        requires
            pre.inv(),
            self.kept.inv(),
            self.kept.loans() == pre.kept.loans(),
            self.n == pre.n,
            self.wqes@ == pre.wqes@,
            self.limit == pre.limit,
            self.gathered@ == pre.gathered@,
            self.ledger == pre.ledger,
            self.max_len == pre.max_len,
            self.pd_id == pre.pd_id,
            self.reads == pre.reads,
            self.max_sges == pre.max_sges,
        ensures
            self.inv(),
    {
        assert forall|k: int| 0 <= k < self.n implies #[trigger] self.wqe_ok(k) by {
            assert(pre.wqe_ok(k));
        }
    }

    /// A WQE of one element, whose part is the input it just got, keeps the invariant.
    pub(super) proof fn lemma_one_more(&self, pre: &Self, part: Part<N>, access: safeverbs_spec::abs::Dir, entry: WqeLoans)
        requires
            pre.inv(),
            self.n == pre.n + 1,
            self.n <= self.wqes@.len(),
            self.limit == pre.limit,
            self.wqes@ == pre.wqes@.update(pre.n as int, self.wqes@[pre.n as int]),
            self.wqes@[pre.n as int].sges == 1,
            self.wqes@[pre.n as int].sge == part_sge(part),
            entry.opcode == Some(opcode_model(self.wqes@[pre.n as int].op)),
            entry.sges == seq![part_sge(part)],
            entry.first == pre.kept.loans().len(),
            self.gathered@ == pre.gathered@,
            self.kept.inv(),
            self.kept.loans() == pre.kept.loans().push(Loan { part, access: Some(access) }),
            access == entry.access(),
            self.ledger@ == pre.ledger@.push(entry),
            part_sge(part).length <= pre.max_len,
            part_pd(part) == pre.pd_id,
            self.max_len == pre.max_len,
            self.pd_id == pre.pd_id,
            pre.reads ==> self.reads,
            self.max_sges == pre.max_sges,
            opcode_model(self.wqes@[pre.n as int].op).writes_local() ==> self.reads,
        ensures
            self.inv(),
    {
        let k = pre.n as int;
        lemma_sges_len_one(part_sge(part));
        assert(self.kept.loans().len() == entry.first + 1);
        assert(self.kept.loans()[entry.first] == (Loan { part, access: Some(access) }));
        assert(entry.end() == entry.first + 1);
        assert(entry.matches(self.kept.loans())) by {
            assert forall|j: int| 0 <= j < entry.sges.len() implies {
                let loan = #[trigger] self.kept.loans()[entry.end() - 1 - j];
                &&& loan.part.spec_sge() == entry.sges[j]
                &&& loan.access == Some(entry.access())
            } by {
                assert(j == 0);
            }
        }
        assert(self.ledger@[k] == entry);
        assert(self.sges_at(k) == entry.sges);
        assert(self.wqe_ok(k));
        assert forall|j: int| 0 <= j < pre.n implies #[trigger] self.wqes@[j] == pre.wqes@[j] by {}
        assert forall|i: int| pre.kept.loans().len() <= i < self.kept.loans().len()
            implies (#[trigger] self.kept.loans()[i]).part.spec_pd() == self.pd_id by {
            assert(i == entry.first);
        }
        assert forall|i: int| 0 <= i < pre.kept.loans().len() implies #[trigger] self.kept.loans()[i] == pre.kept.loans()[i] by {}
        self.lemma_added(pre);
    }

    /// Whether a WQE of `peek`'s bytes may go in with nothing more to keep: they are of the frozen
    /// allocation of its last WQE, of its protection domain then, and it has room.
    #[inline(always)]
    fn takes_peek(&self, peek: &Peek<'_, N>) -> (takes: bool)
        ensures
            takes ==> self.n < self.limit && peek.bytes <= self.max_len
                && self.kept.keeps_frozen(**peek.frozen),
    {
        self.n < self.limit && peek.bytes <= self.max_len as usize && self.kept.keeps_peek(peek)
    }

    /// [`OpenRequest::try_push`] of bytes of the frozen allocation of its last WQE, which is
    /// already kept alive: nothing to count, and nothing to store but the WQE. Whether it did.
    #[inline(always)]
    pub(crate) fn try_push_peek(&mut self, op: WrOp, peek: Peek<'_, N>) -> (pushed: bool)
        requires
            old(self).inv(),
            peek.inv(),
        ensures
            final(self).inv(),
    {
        if self.takes_peek(&peek) && plain_op(&op, peek.bytes as u64) {
            let k = self.n;
            let ghost before = *self;
            let sge = peek.sge();
            self.kept.note_peek(&peek, Ghost(self.pd_id));
            write_wqe(&mut self.wqes, k, OpenWqe { op, sge, first_sge: 0, sges: 1 });
            self.n = k + 1;
            let ghost part = peek.part(self.pd_id);
            let ghost entry = WqeLoans {
                wr_id: 0,
                opcode: Some(opcode_model(op)),
                sges: seq![sge],
                first: before.kept.loans().len() as int,
            };
            self.ledger = Ghost(before.ledger@.push(entry));
            proof {
                peek.lemma_part(self.pd_id);
                self.lemma_one_more(&before, part, safeverbs_spec::abs::Dir::Out, entry);
            }
            true
        } else {
            false
        }
    }

    /// [`OpenRequest::try_push_peek`] of a plain WRITE to `dst`, written where it stays.
    #[inline(always)]
    pub(crate) fn try_push_peek_write(&mut self, dst: RemoteBuf, peek: Peek<'_, N>) -> (pushed: bool)
        requires
            old(self).inv(),
            peek.inv(),
        ensures
            final(self).inv(),
    {
        if self.takes_peek(&peek) && peek.bytes as u64 <= dst.len {
            let k = self.n;
            let ghost before = *self;
            let sge = peek.sge();
            self.kept.note_peek(&peek, Ghost(self.pd_id));
            let wqe = wqe_at(&mut self.wqes, k);
            wqe.op = WrOp::Write(dst);
            wqe.sge = sge;
            wqe.sges = 1;
            self.n = k + 1;
            let ghost part = peek.part(self.pd_id);
            let ghost entry = WqeLoans {
                wr_id: 0,
                opcode: Some(opcode_model(WrOp::Write(dst))),
                sges: seq![sge],
                first: before.kept.loans().len() as int,
            };
            self.ledger = Ghost(before.ledger@.push(entry));
            proof {
                peek.lemma_part(self.pd_id);
                self.lemma_one_more(&before, part, safeverbs_spec::abs::Dir::Out, entry);
            }
            true
        } else {
            false
        }
    }
}

/// WQE `k`, which `k < limit <= wqes.len()` allows: no bounds check, and so no panic edge with the
/// program's buffer in hand.
#[inline(always)]
pub(super) fn wqe_at(wqes: &mut Vec<OpenWqe>, k: usize) -> (wqe: &mut OpenWqe)
    requires
        k < old(wqes)@.len(),
    ensures
        *wqe == old(wqes)@[k as int],
        final(wqes)@ == old(wqes)@.update(k as int, *final(wqe)),
{
    // SAFETY: `k` is below the length (the precondition).
    unsafe { wqes.as_mut_slice().get_unchecked_mut(k) }
}

/// Writes WQE `k`, as [`wqe_at`] allows.
#[inline(always)]
pub(super) fn write_wqe(wqes: &mut Vec<OpenWqe>, k: usize, wqe: OpenWqe)
    requires
        k < old(wqes)@.len(),
    ensures
        final(wqes)@ == old(wqes)@.update(k as int, wqe),
{
    *wqe_at(wqes, k) = wqe;
}

} // verus!

impl<N: Device> OpenRequest<N> {
    /// Adds a WQE of `op` over what `buf` lends, when it is of the frozen allocation of the last
    /// WQE, which is already kept alive: nothing to count, and nothing to store but the WQE.
    /// Whether it did.
    #[inline(always)]
    pub fn try_push_lent<B: Lend<N>>(&mut self, op: WrOp, buf: &B) -> bool {
        self.try_push_peek(op, buf.peek())
    }

    /// [`OpenRequest::try_push_lent`] of a plain WRITE to `dst`, written where it stays.
    #[inline(always)]
    pub fn try_push_write<B: Lend<N>>(&mut self, dst: RemoteBuf, buf: &B) -> bool {
        self.try_push_peek_write(dst, buf.peek())
    }
}
