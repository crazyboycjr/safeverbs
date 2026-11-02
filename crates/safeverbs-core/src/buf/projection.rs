//! A's members derived from the actual logical inputs retained by `Leases`.
//!
//! Logical inputs, not deduplicated physical guards, determine member order,
//! byte spans, exclusivity and operation access. All items here are erased.
//! Names and arbitrary-byte validity come from the caller: neither fresh identities
//! nor typed validity follow from retaining a resource. This adapter establishes no
//! allocation permission, registration, nonaliasing, `Group::wf` or A transition.
#![allow(unused_imports)]

use safeverbs_spec::abs::{Dir, Group, Holder, Id, Lease, Req};
use safeverbs_spec::types::Span;
use vstd::prelude::*;

use super::lease::*;
use crate::nic::Nic;
use crate::refine::*;

verus! {

/// Exactly the actual input SGE's byte geometry, with mathematical end arithmetic.
/// This says nothing about allocation bounds or the SGE's registration key.
pub open spec fn loan_span<N: Nic>(loan: Loan<N>) -> Span {
    let sge = loan.part.spec_sge();
    Span { start: sge.addr as int, len: sge.length as nat }
}

/// The logical input for SGE `j`, following the actual reverse-order pop loop.
/// The bounds and correspondence are established by `lemma_wqe_member`.
pub open spec fn wqe_loan_index(wqe: WqeLoans, j: int) -> int {
    wqe.end() - 1 - j
}

/// Exactly the recorded SGE's byte geometry, independent of registration.
pub open spec fn wqe_span(wqe: WqeLoans, j: int) -> Span {
    let sge = wqe.sges[j];
    Span { start: sge.addr as int, len: sge.length as nat }
}

/// A byte in any recorded SGE of this WQE. An empty SGE list covers no bytes.
pub open spec fn wqe_contains(wqe: WqeLoans, addr: int) -> bool {
    exists|j: int| 0 <= j < wqe.sges.len()
        && #[trigger] wqe_span(wqe, j).contains(addr)
}

/// Every logical input has the supplied operation access. Unposted inputs with
/// `None` cannot satisfy this predicate, except for an empty group.
pub open spec fn access_matches<N: Nic>(leases: Leases<N>, dirs: Seq<Dir>) -> bool {
    &&& dirs.len() == leases.loans().len()
    &&& forall|i: int| 0 <= i < dirs.len()
        ==> (#[trigger] leases.loans()[i]).access == Some(dirs[i])
}

/// The bytes named by actual logical inputs with this operation access.
pub open spec fn loan_accesses<N: Nic>(leases: Leases<N>, dir: Dir, addr: int) -> bool {
    exists|i: int| 0 <= i < leases.loans().len()
        && (#[trigger] leases.loans()[i]).access == Some(dir)
        && loan_span(leases.loans()[i]).contains(addr)
}

/// The union of recorded WQE SGE spans with the required local access role.
/// These are this request's records, not the WQEs in M's current queues.
pub open spec fn recorded_accesses<N: Nic>(leases: Leases<N>, dir: Dir, addr: int) -> bool {
    exists|w: int| 0 <= w < leases.wqes().len()
        && (#[trigger] leases.wqes()[w]).access() == dir
        && wqe_contains(leases.wqes()[w], addr)
}

/// Full input coverage makes every loan access an access of a recorded WQE.
/// Inverting its reverse-order indexing recovers an actual SGE witness.
pub proof fn lemma_loan_recorded<N: Nic>(leases: Leases<N>, dir: Dir, addr: int)
    requires leases.inv(), leases.fully_recorded(), loan_accesses(leases, dir, addr),
    ensures recorded_accesses(leases, dir, addr),
{
    let i = choose|i: int| 0 <= i < leases.loans().len()
        && leases.loans()[i].access == Some(dir)
        && loan_span(leases.loans()[i]).contains(addr);
    assert(covers_inputs(leases.wqes(), leases.loans().len() as int));
    assert(input_is_covered(leases.wqes(), i));
    let w = choose|w: int| 0 <= w < leases.wqes().len()
        && (#[trigger] leases.wqes()[w]).first <= i < leases.wqes()[w].end();
    leases.lemma_wqes();
    let wqe = leases.wqes()[w];
    assert(wqe.matches(leases.loans()));
    let j = wqe.end() - 1 - i;
    assert(0 <= j < wqe.sges.len());
    assert(wqe_loan_index(wqe, j) == i);
    assert(leases.loans()[i].part.spec_sge() == wqe.sges[j]);
    assert(leases.loans()[i].access == Some(wqe.access()));
    assert(wqe.access() == dir);
    assert(loan_span(leases.loans()[i]) == wqe_span(wqe, j));
    assert(wqe_span(wqe, j).contains(addr));
    assert(wqe_contains(wqe, addr));
    assert(recorded_accesses(leases, dir, addr));
}

/// Caller-supplied metadata that the current buffer proof does not establish.
/// Request/member positions name logical loans even when one Arc guards several.
pub ghost struct Projection {
    /// Stable member names, indexed by request sequence and logical input position.
    /// The caller must justify fresh creation in A and preserve these names when
    /// resources move. This function need not be injective; no freshness is inferred.
    pub names: spec_fn(int, int) -> Id,
    /// Whether each member's type accepts arbitrary bytes. A future buffer-token
    /// proof must justify these values; `Part::spec_writable` alone does not.
    pub any_bytes: spec_fn(int, int) -> bool,
}

impl Projection {
    /// One member per actual input, including repeated views of a shared buffer.
    pub open spec fn members<N: Nic>(self, s: int, leases: Leases<N>) -> Seq<Lease> {
        Seq::new(leases.loans().len(), |i: int| Lease {
            id: (self.names)(s, i),
            mem: loan_span(leases.loans()[i]).bytes(),
            unique: leases.loans()[i].part.spec_writable(),
            any_bytes: (self.any_bytes)(s, i),
        })
    }

    /// A program-held description, not evidence that this group exists in A.
    pub open spec fn group<N: Nic>(self, s: int, leases: Leases<N>) -> Group {
        Group { members: self.members(s, leases), holder: Holder::Program }
    }

    /// The concrete resource interpretation expected by the ring adapters.
    /// Directions still have to satisfy `access_matches` for each held request;
    /// arbitrary maps do not establish `refine::placed`.
    pub open spec fn permissions<N: Nic>(self, access: Map<int, Seq<Dir>>) -> Permissions<Leases<N>> {
        Permissions { access, resources: |s: int, leases: Leases<N>| self.members(s, leases) }
    }

    /// The additional validity obligation needed by `Group::allows`, expressed
    /// separately from the exclusive-write fact that `lemma_loans` proves.
    pub open spec fn write_validity<N: Nic>(self, s: int, leases: Leases<N>) -> bool {
        forall|i: int| 0 <= i < leases.loans().len()
            && (#[trigger] leases.loans()[i]).access == Some(Dir::In)
            ==> (self.any_bytes)(s, i)
    }

    /// Exact metadata for every member, without a well-formedness assumption.
    pub proof fn lemma_members<N: Nic>(self, s: int, leases: Leases<N>)
        requires leases.inv(),
        ensures
            self.members(s, leases).len() == leases.loans().len(),
            forall|i: int| 0 <= i < leases.loans().len() ==> {
                let member = #[trigger] self.members(s, leases)[i];
                &&& member.id == (self.names)(s, i)
                &&& member.mem == loan_span(leases.loans()[i]).bytes()
                &&& member.unique == leases.loans()[i].part.spec_writable()
                &&& member.any_bytes == (self.any_bytes)(s, i)
            },
    {
        assert forall|i: int| 0 <= i < leases.loans().len() implies {
            let member = #[trigger] self.members(s, leases)[i];
            &&& member.id == (self.names)(s, i)
            &&& member.mem == loan_span(leases.loans()[i]).bytes()
            &&& member.unique == leases.loans()[i].part.spec_writable()
            &&& member.any_bytes == (self.any_bytes)(s, i)
        } by {
            assert(self.members(s, leases)[i] == (Lease {
                id: (self.names)(s, i),
                mem: loan_span(leases.loans()[i]).bytes(),
                unique: leases.loans()[i].part.spec_writable(),
                any_bytes: (self.any_bytes)(s, i),
            }));
        }
    }

    /// Each projected member has an actual retained lifetime guard, and each
    /// actual incoming write is exclusive. Guard indices need not be distinct.
    /// No claim that the guard owns the SGE's bytes is made here.
    pub proof fn lemma_backing<N: Nic>(self, s: int, leases: Leases<N>)
        requires leases.inv(),
        ensures
            self.members(s, leases).len() == leases.loans().len(),
            forall|i: int| 0 <= i < self.members(s, leases).len() ==> {
                let member = #[trigger] self.members(s, leases)[i];
                &&& exists|j: int| 0 <= j < leases.view().len()
                    && (#[trigger] leases.view()[j]).same_backing(leases.loans()[i].part.resource())
                &&& leases.loans()[i].access == Some(Dir::In) ==> member.unique
            },
    {
        leases.lemma_loans();
        self.lemma_members(s, leases);
        assert forall|i: int| 0 <= i < self.members(s, leases).len() implies {
            let member = #[trigger] self.members(s, leases)[i];
            &&& exists|j: int| 0 <= j < leases.view().len()
                && (#[trigger] leases.view()[j]).same_backing(leases.loans()[i].part.resource())
            &&& leases.loans()[i].access == Some(Dir::In) ==> member.unique
        } by {
            let loan = leases.loans()[i];
            let j = choose|j: int| 0 <= j < leases.view().len()
                && leases.view()[j].same_backing(loan.part.resource());
            assert(self.members(s, leases)[i].unique == loan.part.spec_writable());
            assert(loan.access == Some(Dir::In) ==> loan.part.spec_writable());
        }
    }

    /// Each actual WQE SGE names the exact reverse-indexed loan and member,
    /// with the operation's required access and an actual physical guard.
    /// Incoming writes are exclusive; guard indices need not be distinct.
    pub proof fn lemma_wqe_member<N: Nic>(self, s: int, leases: Leases<N>, w: int, j: int)
        requires
            leases.inv(), 0 <= w < leases.wqes().len(),
            0 <= j < leases.wqes()[w].sges.len(),
        ensures {
            let wqe = leases.wqes()[w];
            let i = wqe_loan_index(wqe, j);
            &&& wqe.first <= i < wqe.end()
            &&& 0 <= i < leases.loans().len()
            &&& i < self.members(s, leases).len()
            &&& leases.loans()[i].part.spec_sge() == wqe.sges[j]
            &&& loan_span(leases.loans()[i]) == wqe_span(wqe, j)
            &&& leases.loans()[i].access == Some(wqe.access())
            &&& self.members(s, leases)[i].mem == wqe_span(wqe, j).bytes()
            &&& self.members(s, leases)[i].unique == leases.loans()[i].part.spec_writable()
            &&& wqe.access() is In ==> self.members(s, leases)[i].unique
            &&& exists|g: int| 0 <= g < leases.view().len()
                && (#[trigger] leases.view()[g]).same_backing(leases.loans()[i].part.resource())
        },
    {
        leases.lemma_wqes();
        self.lemma_members(s, leases);
        self.lemma_backing(s, leases);
        let wqe = leases.wqes()[w];
        let i = wqe_loan_index(wqe, j);
        assert(wqe.matches(leases.loans()));
        assert(wqe.first <= i < wqe.end());
        assert(0 <= i < leases.loans().len());
        assert(leases.loans()[i].part.spec_sge() == wqe.sges[j]);
        assert(leases.loans()[i].access == Some(wqe.access()));
        assert(loan_span(leases.loans()[i]) == wqe_span(wqe, j));
        assert(self.members(s, leases)[i].mem == loan_span(leases.loans()[i]).bytes());
        assert(self.members(s, leases)[i].unique == leases.loans()[i].part.spec_writable());
        if wqe.access() is In {
            assert(leases.loans()[i].access == Some(Dir::In));
            assert(self.members(s, leases)[i].unique);
        }
        let g = choose|g: int| 0 <= g < leases.view().len()
            && leases.view()[g].same_backing(leases.loans()[i].part.resource());
    }

    /// Every byte in a recorded WQE's SGEs is covered by the projected request
    /// with the recorded operation's role: receives and local destinations permit
    /// writes, while send/write sources permit reads. Extra loans are permitted.
    /// This is a request-local ledger result, not coupling to M's current queues.
    pub proof fn lemma_wqe_access<N: Nic>(self, s: int, leases: Leases<N>, req: Req, w: int, addr: int)
        requires
            leases.inv(), access_matches(leases, req.dirs), req.phase is InFlight,
            0 <= w < leases.wqes().len(), wqe_contains(leases.wqes()[w], addr),
        ensures
            leases.wqes()[w].access() is Out
                ==> self.group(s, leases).with_req(req).nic_may_read(addr),
            leases.wqes()[w].access() is In
                ==> self.group(s, leases).with_req(req).nic_may_write(addr),
    {
        let wqe = leases.wqes()[w];
        let j = choose|j: int| 0 <= j < wqe.sges.len()
            && wqe_span(wqe, j).contains(addr);
        self.lemma_wqe_member(s, leases, w, j);
        let i = wqe_loan_index(wqe, j);
        assert(loan_span(leases.loans()[i]).contains(addr));
        assert(leases.loans()[i].access == Some(wqe.access()));
        assert(req.dirs[i] == wqe.access());
        self.lemma_operation_access(s, leases, req, addr);
        match wqe.access() {
            Dir::Out => { assert(loan_accesses(leases, Dir::Out, addr)); },
            Dir::In => { assert(loan_accesses(leases, Dir::In, addr)); },
        }
    }

    /// With every loan recorded, projected request access is exactly the union
    /// of the corresponding recorded WQE spans. This needs no fresh-name,
    /// byte-token or registration premise and establishes no global M coupling.
    pub proof fn lemma_recorded_access<N: Nic>(self, s: int, leases: Leases<N>, req: Req, addr: int)
        requires
            leases.inv(), leases.fully_recorded(),
            access_matches(leases, req.dirs), req.phase is InFlight,
        ensures
            self.group(s, leases).with_req(req).nic_may_read(addr)
                <==> recorded_accesses(leases, Dir::Out, addr),
            self.group(s, leases).with_req(req).nic_may_write(addr)
                <==> recorded_accesses(leases, Dir::In, addr),
    {
        self.lemma_operation_access(s, leases, req, addr);
        let group = self.group(s, leases).with_req(req);
        if group.nic_may_read(addr) {
            assert(loan_accesses(leases, Dir::Out, addr));
            lemma_loan_recorded(leases, Dir::Out, addr);
        }
        if recorded_accesses(leases, Dir::Out, addr) {
            let w = choose|w: int| 0 <= w < leases.wqes().len()
                && leases.wqes()[w].access() == Dir::Out
                && wqe_contains(leases.wqes()[w], addr);
            self.lemma_wqe_access(s, leases, req, w, addr);
            assert(group.nic_may_read(addr));
        }
        if group.nic_may_write(addr) {
            assert(loan_accesses(leases, Dir::In, addr));
            lemma_loan_recorded(leases, Dir::In, addr);
        }
        if recorded_accesses(leases, Dir::In, addr) {
            let w = choose|w: int| 0 <= w < leases.wqes().len()
                && leases.wqes()[w].access() == Dir::In
                && wqe_contains(leases.wqes()[w], addr);
            self.lemma_wqe_access(s, leases, req, w, addr);
            assert(group.nic_may_write(addr));
        }
    }

    /// The actual operation's access is allowed once arbitrary-byte validity is
    /// supplied. Exclusivity is derived from `Leases`, not assumed for the group.
    pub proof fn lemma_allows<N: Nic>(self, s: int, leases: Leases<N>, dirs: Seq<Dir>)
        requires leases.inv(), access_matches(leases, dirs), self.write_validity(s, leases),
        ensures self.group(s, leases).allows(dirs),
    {
        self.lemma_backing(s, leases);
        self.lemma_members(s, leases);
        let members = self.members(s, leases);
        assert forall|i: int| 0 <= i < dirs.len() && #[trigger] dirs[i] is In
            implies members[i].unique && members[i].any_bytes by {
            assert(leases.loans()[i].access == Some(dirs[i]));
            assert(leases.loans()[i].access == Some(Dir::In));
            assert(members[i].unique);
            assert((self.any_bytes)(s, i));
        }
    }

    /// A real `Permissions<Leases<N>>` uses precisely this description, with no
    /// new assumption about A's state, other requests or resource ownership.
    pub proof fn lemma_permissions<N: Nic>(self, s: int, leases: Leases<N>, access: Map<int, Seq<Dir>>)
        requires
            leases.inv(), access.contains_key(s), access_matches(leases, access[s]),
            self.write_validity(s, leases),
        ensures
            self.permissions::<N>(access).access == access,
            (self.permissions::<N>(access).resources)(s, leases) == self.group(s, leases).members,
            self.group(s, leases).allows(self.permissions::<N>(access).access[s]),
    {
        self.lemma_allows(s, leases, access[s]);
    }

    /// Projected NIC access is exactly the bytes of loans carrying that access.
    /// This lemma uses loan access alone; `lemma_wqe_access` additionally uses
    /// the recorded WQE-to-loan correspondence to cover its SGE spans.
    pub proof fn lemma_operation_access<N: Nic>(self, s: int, leases: Leases<N>, req: Req, addr: int)
        requires leases.inv(), access_matches(leases, req.dirs), req.phase is InFlight,
        ensures
            self.group(s, leases).with_req(req).nic_may_read(addr)
                <==> loan_accesses(leases, Dir::Out, addr),
            self.group(s, leases).with_req(req).nic_may_write(addr)
                <==> loan_accesses(leases, Dir::In, addr),
    {
        self.lemma_members(s, leases);
        let group = self.group(s, leases).with_req(req);
        if group.nic_may_read(addr) {
            let i = choose|i: int| 0 <= i < group.members.len()
                && group.members[i].mem.contains(addr) && group.read_member(i);
            assert(req.dirs[i] is Out);
            assert(leases.loans()[i].access == Some(Dir::Out));
            assert(loan_span(leases.loans()[i]).contains(addr));
            assert(loan_accesses(leases, Dir::Out, addr));
        }
        if loan_accesses(leases, Dir::Out, addr) {
            let i = choose|i: int| 0 <= i < leases.loans().len()
                && leases.loans()[i].access == Some(Dir::Out)
                && loan_span(leases.loans()[i]).contains(addr);
            assert(req.dirs[i] is Out);
            assert(group.members[i].mem.contains(addr));
            assert(group.read_member(i));
            assert(group.nic_may_read(addr));
        }
        if group.nic_may_write(addr) {
            let i = choose|i: int| 0 <= i < group.members.len()
                && group.members[i].mem.contains(addr) && group.write_member(i);
            assert(req.dirs[i] is In);
            assert(leases.loans()[i].access == Some(Dir::In));
            assert(loan_span(leases.loans()[i]).contains(addr));
            assert(loan_accesses(leases, Dir::In, addr));
        }
        if loan_accesses(leases, Dir::In, addr) {
            let i = choose|i: int| 0 <= i < leases.loans().len()
                && leases.loans()[i].access == Some(Dir::In)
                && loan_span(leases.loans()[i]).contains(addr);
            assert(req.dirs[i] is In);
            assert(group.members[i].mem.contains(addr));
            assert(group.write_member(i));
            assert(group.nic_may_write(addr));
        }
    }
}

} // verus!
