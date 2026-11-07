//! An engine: one completion queue and the queue pairs attached to it, the
//! unit of thread affinity, driven synchronously and by index.
//!
//! Applications use the handles of the `safeverbs` crate, `LocalCq` and `LocalQp`, or its
//! futures: thin, safe wrappers over this type. It stays public as an escape hatch for anyone
//! who wants neither a reference count nor a `RefCell` on the way.
//!
//! Everything of an engine that the NIC model M sees (its CQ, its QPs'
//! hardware queues and rings, its share of M) lives in the internal `Hw`,
//! which is verified: every call into the NIC meets M's precondition, the
//! rings stay coupled to M's queues, completions are routed to the WQEs they
//! name, and QPs are destroyed only drained. An engine's invariant is a type invariant: no program
//! has to promise anything, and Verus checks that each method keeps it and that nothing unwinds
//! while it does not hold. So each method here is a step of Theorem 1, and any engine a program
//! holds, whatever it did with it, is the end of a run of the theorem. What stays outside `verus!`
//! is what calls these methods and no more: there is nothing for it to get wrong.
//!
//! The efficient way to use RDMA is one CQ per thread, shared by all of that
//! thread's QPs, and a loop that polls it, dispatches each completion and
//! posts new work. An [`Engine`] is that CQ with its QPs, as one value: `Send`
//! but not `Sync`, so it moves between threads as a whole and is used by one
//! thread at a time. Its CQ is created `IBV_CREATE_CQ_ATTR_SINGLE_THREADED`,
//! and, where the device has thread domains, its QPs are created on one
//! thread domain of the engine's: mlx5 then takes no QP locks and gives the
//! engine's QPs a doorbell of their own, which they share. Elsewhere (rxe)
//! they are plain QPs.
//!
//! # QPs and their keys
//!
//! [`Engine::create_qp`] creates a QP attached to the engine's CQ, in a protection domain of
//! the same device, and returns its [`QpKey`], a typed index: every per-QP method takes one. A
//! QP reserves CQ capacity for every completion it may produce, its send and receive depths, and
//! creating one that does not fit is an error ([`Error::CqFull`]), never a possible overrun: each
//! WQE in flight produces at most one completion, and each QP's rings never let more WQEs be in
//! flight than its queues hold. This is the rule of the NIC model M (`create_qp` reserves the
//! depths on the CQ).
//!
//! # Completions
//!
//! Every completion is routed to its QP and queue by its `wr_id`, which
//! holds the QP's index in the engine (16 bits), the queue (1 bit) and the
//! low 47 bits of the WQE's number in that queue's ring. The ring's number
//! is recovered from the WQEs in flight, which span at most the queue's
//! depth, so the 47 bits never wrap ambiguously. Routing therefore costs a
//! shift and an array index per completion, with no lookup; the queue's bit
//! is needed in any case, since a failed completion's opcode is undefined.
//! The QP's ring then completes its requests in order.
//!
//! There are two ways to drive an engine:
//!
//! - **The event loop** ([`Engine::poll`]): reads the CQEs, updates the CQ's consumer index and
//!   then, in one pass over them, routes each one, walks its ring up to it and hands every
//!   request that completes (with an owner) as a [`Completion`] (its context, result and buffers,
//!   with its QP, queue and sequence number) to a callback that may post new work on any QP. Nothing
//!   is allocated.
//! - **Owners that take their results** ([`Engine::progress`] and [`Engine::take_send`]): polling
//!   completes requests into their rings, where their results wait, and wakes their wakers; each
//!   owner takes its request's result by sequence number. The futures of `safeverbs` work this
//!   way.
//!
//! An engine driven one way should not be driven the other: `poll` hands out the results owners
//! would wait for.
//!
//! # Contexts
//!
//! An `Engine<C>` keeps a value of the type `C` with each request that has an
//! owner: whatever tells the program which request just finished. It lives in
//! the request's ring slot and comes back exactly once, with its result and
//! its buffers:
//!
//! ```no_run
//! # use safeverbs_core::{Buf, Completed, CompletionError, Context, Engine, QpKey, RC};
//! enum Job {
//!     Reply { client: u32 },
//!     Log,
//! }
//!
//! # fn f(ctx: Context, qp: QpKey<RC>, buf: Buf<u8>) -> safeverbs_core::Result<()> {
//! let mut w: Engine<Job> = Engine::new(&ctx, 4096)?;
//! w.send(qp, buf.slice(..), Job::Reply { client: 7 })?;
//! w.poll(32, |w, c| match (c.wr_id, c.result) {
//!     (Job::Reply { client }, Ok(_)) => println!("replied to {client}"),
//!     (Job::Log, Ok(_)) => {}
//!     (job, Err(e)) => {
//!         // The WQEs before `e`'s position completed; the others did not, and
//!         // what they did at the peer is unknown.
//!         let _ = (job, e);
//!     }
//! });
//! # Ok(()) }
//! ```
//!
//! C programs would use a `u64`, which is `Engine<u64>`. The default, `()`,
//! costs nothing; the futures of `safeverbs` use it, being their own identity.
//! A batch is one request, with one context ([`Request::submit`]). Requests
//! without an owner hold none.
//!
//! Giving up a request hands its context back ([`Engine::detach_send`]).
//! Removing a QP or dropping the engine gives up every request: the contexts
//! of the requests not delivered yet are dropped then, and their buffers
//! released once the NIC is done with them. A context's destructor is never run
//! by a poll.
//!
//! # Removing QPs
//!
//! The NIC may read and write the buffers of every WQE posted and not yet
//! completed, and M (`destroy_qp`) allows destroying a QP only when its queues
//! are empty. So a QP is never destroyed with work in flight: removing it
//! ([`Engine::remove_qp`], and dropping the engine, for each of its QPs)
//! first moves it to the error state, where the NIC completes every WQE
//! left with a flush error, then posts a signaled *drain* WQE on its send
//! queue (a SEND of nothing, which the error state flushes too: it completes
//! the unsignaled WQEs that succeeded before the error and would never get
//! a completion of their own) and polls the CQ until both rings have
//! nothing in flight. Only then is the QP destroyed, and only after that
//! are its buffers released. The other QPs' completions polled meanwhile
//! go to their rings as [`Engine::progress`] sends them.
//!
//! A device that never flushes is a fatal error, and no buffer is ever
//! freed while the NIC may access it: if no completion arrives for
//! [`DRAIN_TIMEOUT`], the QP is kept as it is, with its buffers, its index
//! and its share of the CQ, and [`Engine::remove_qp`] fails with
//! [`Error::NotFlushed`]. Dropping the engine tries once more; a QP still
//! not drained then is leaked with its buffers, and so are the CQ and the
//! thread domain it is attached to.

// The code keeps the plain forms the proofs reason about (explicit matches).
#![allow(clippy::question_mark, clippy::manual_ok_err)]

use core::marker::PhantomData;
use core::task::Waker;
use std::time::{Duration, Instant};

use vstd::prelude::*;
use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::buf::{Buf, BufError, BufMut, Leases, Part, release};
use crate::error::{Error, Result};
use crate::hw::{Hw, Refusal};
pub use crate::hw::{LetGo, OpenRequest, Unposted};
use crate::nic::{ConnectAttr, Endpoint, Nic, QpCaps, QpType, RemoteBuf, WrOp};
use crate::pd::{Context, Pd};
use crate::ring::wake::WakeBatch;
use crate::ring::{Payload, Ring, Seq};
use crate::transport::{Reliable, Transport};
use crate::trusted::{
    alloc_domain, alloc_td, clone_context, clone_pd, no_context, no_qp, pd_id_of, same_device,
    vec_push,
};
#[cfg(verus_keep_ghost)]
use crate::trusted::{domain_over, pd_id};
use crate::verbs::Verbs;
#[cfg(doc)]
use crate::wc::CompletionError;

pub use crate::hw::{Completion, DetachedError, MAX_QPS, MAX_SGES, POLL_MAX, Side as Queue};

/// How long removing a QP waits for a completion while the device flushes
/// its work, before it gives up: then the QP and its buffers are kept, never
/// freed (see the module documentation).
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

/// Empty polls of the CQ between two looks at the clock while draining.
const DRAIN_SPINS: u32 = 1024;

verus! {

broadcast use {
    crate::hw::refines::lemma_moved_chain,
    crate::hw::refines::lemma_same_table_chain,
    crate::hw::refines::lemma_kept_qp,
};

/// [`Transport`], without specifications of its own: an engine reads no more of a key than its
/// index.
#[cfg(verus_keep_ghost)]
#[verifier::external_trait_specification]
pub trait ExTransport: Send + Sync + 'static {
    /// The trait specified.
    type ExternalTraitSpecificationFor: Transport;
}

/// The QP type of a transport, which must complete in order (a build error otherwise). No meaning
/// in M beyond the type itself, and what a WQE asks of its QP is compared against it at run time.
#[verifier::external_body]
#[inline(always)]
fn transport_of<T: Transport>() -> QpType {
    const {
        assert!(
            T::IN_ORDER_COMPLETION,
            "only in-order transports are supported"
        );
    }
    T::QP_TYPE
}

/// The hardware side of an engine about to go: this is the last use of it (`Engine::drop`), so the
/// engine's invariant need not hold of what it leaves.
#[verifier::external_body]
fn hw_at_end<C, N: Nic>(engine: &mut Engine<C, N>) -> (hw: &mut Hw<C, N>)
    ensures
        *hw == old(engine).state(),
{
    &mut engine.hw
}

/// A drain's clock. The time means nothing in M: whatever it says, a QP is destroyed only with
/// nothing in flight.
#[verifier::external_body]
struct DrainClock {
    deadline: Instant,
    idle: u32,
}

/// A clock with [`DRAIN_TIMEOUT`] to go.
#[verifier::external_body]
fn clock_start() -> DrainClock {
    DrainClock {
        deadline: Instant::now() + DRAIN_TIMEOUT,
        idle: 0,
    }
}

/// A completion came: [`DRAIN_TIMEOUT`] to go again.
#[verifier::external_body]
fn clock_renew(clock: &mut DrainClock) {
    clock.deadline = Instant::now() + DRAIN_TIMEOUT;
    clock.idle = 0;
}

/// No completion came: yields now and then, and says whether the time is up.
#[verifier::external_body]
fn clock_idle(clock: &mut DrainClock) -> bool {
    clock.idle = clock.idle.wrapping_add(1);
    if clock.idle.is_multiple_of(DRAIN_SPINS) {
        if Instant::now() > clock.deadline {
            return true;
        }
        std::thread::yield_now();
    } else {
        core::hint::spin_loop();
    }
    false
}

/// A QP of an engine: its index there, typed by its transport. Cheap to copy.
///
/// A key names a QP of the engine that created it, until the QP is removed;
/// using it with another engine, or after the removal, names another QP or
/// none (and the methods panic). That is a logic error, not a memory-safety
/// one: the engine checks what each WQE may do against the QP it is posted
/// on.
pub struct QpKey<T: Transport> {
    index: u16,
    _t: PhantomData<T>,
}

impl<T: Transport> QpKey<T> {
    /// [`QpKey::index`], as a specification.
    pub closed spec fn spec_index(self) -> usize {
        self.index as usize
    }

    /// The QP's index in its engine, as [`Completion::qp`] reports it.
    #[inline(always)]
    pub fn index(self) -> (index: usize)
        ensures
            index == self.spec_index(),
            index < MAX_QPS,
    {
        self.index as usize
    }
}

impl<T: Transport> Clone for QpKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Transport> Copy for QpKey<T> {}

/// A request that could not be posted: why, and its context and buffers.
#[verifier::reject_recursive_types(N)]
pub struct PostError<C = (), N: Nic = Verbs> {
    /// What went wrong.
    pub error: Error,
    /// The value it was given (its context).
    pub wr_id: C,
    /// Its buffers.
    pub leases: Leases<N>,
}

/// A receive [`Engine::try_post_recv`] hands back: its buffer and its context.
pub type ReceiveBack<C, N> = (Part<N>, C);

/// A refusal of a request given a context, with that context.
#[cold]
#[inline(never)]
fn owned<C, N: Nic>(refused: Refusal<C, N>) -> (e: PostError<C, N>)
    requires
        refused.ctx is Some,
    ensures
        e.wr_id == refused.ctx->0,
{
    PostError {
        error: refused.error,
        wr_id: refused.ctx.expect("the context it was given"),
        leases: refused.leases,
    }
}

/// A parent domain over a protection domain and the engine's thread domain.
#[verifier::reject_recursive_types(N)]
struct Domain<N: Nic> {
    /// Dropped before its protection domain.
    domain: N::Domain,
    pd: Pd<N>,
}

/// A completion queue and the QPs attached to it (the synchronous layer), with
/// a context of type `C` kept with each request (see the module documentation).
///
/// Dropping it drains its QPs (as [`Engine::remove_qp`] does, all at once),
/// destroys the QPs, releases the buffers their requests held, destroys the CQ
/// and the thread domain, then drops the contexts of the requests not delivered.
/// A QP the device never flushes is leaked with its buffers, and so are the CQ
/// and the domain.
#[verifier::reject_recursive_types(C)]
#[verifier::reject_recursive_types(N)]
pub struct Engine<C = (), N: Nic = Verbs> {
    /// The parent domains its QPs are created on, one per protection domain. The QPs go first, by
    /// hand, in `drop`, and these before the thread domain.
    domains: Vec<Domain<N>>,
    /// The thread domain of its QPs, where the device has them: it goes before the device, which
    /// `hw` keeps open.
    td: Option<N::Td>,
    /// The verified hardware side; its QPs and CQ go by hand, in `drop`.
    hw: Hw<C, N>,
    /// What `hw` was when the engine was made (erased).
    #[allow(dead_code)]
    first: Ghost<Hw<C, N>>,
}

impl<C, N: Nic> Engine<C, N> {
    /// **What every engine a program holds is, whatever it did with it:** its hardware side keeps
    /// [`Hw::inv`], and it came from the first state by what [`Hw::moved_from`] says, which is a
    /// step of Theorem 1 (`hw::refines`). Verus checks this after every change of `hw`, and that no
    /// such change can unwind halfway; so the program never has to promise anything, even after
    /// catching a panic.
    #[verifier::type_invariant]
    closed spec fn inv(self) -> bool {
        &&& self.hw.inv()
        &&& self.hw.moved_from(&self.first@)
        &&& self.first@.born()
        &&& forall|i: int| 0 <= i < self.domains@.len()
            ==> domain_over(#[trigger] self.domains@[i].domain) == pd_id(self.domains@[i].pd)
    }

    /// What it stands for in Theorem 1: its hardware side, a state of `EngineState`.
    pub closed spec fn state(&self) -> Hw<C, N> {
        self.hw
    }

    /// `self` is `older` after calls of the engine: its hardware side moved as [`Hw::moved_from`]
    /// says, which is a step of Theorem 1.
    pub open spec fn moved_from(&self, older: &Self) -> bool {
        self.state().moved_from(&older.state())
    }

    /// What a program's engine is, for the proofs: the type invariant, said of its state, with the
    /// first state it came from.
    #[cfg(verus_keep_ghost)]
    pub(crate) fn stands(&self) -> (first: Ghost<Hw<C, N>>)
        ensures
            self.state().inv(),
            self.state().moved_from(&first@),
            first@.born(),
    {
        proof {
            use_type_invariant(self);
        }
        self.first
    }

    /// Its hardware side, as Theorem 1 reads it.
    #[cfg(verus_keep_ghost)]
    pub(crate) fn hw(&self) -> (hw: &Hw<C, N>)
        ensures
            *hw == self.state(),
            hw.inv(),
    {
        proof {
            use_type_invariant(self);
        }
        &self.hw
    }

    /// An engine on the device of `ctx` with a CQ of `cq_size` entries, on a thread domain of
    /// its own where the device has them.
    pub fn new(ctx: &Context<N>, cq_size: u32) -> (r: Result<Engine<C, N>>)
        ensures
            r matches Ok(engine) ==> engine.state().spec_reserved() == 0 && engine.state().spec_cq_size() == cq_size,
    {
        let hw = match Hw::new(clone_context(ctx), cq_size) {
            Ok(hw) => hw,
            Err(e) => return Err(e),
        };
        let ghost first = hw;
        proof {
            first.lemma_moved_refl();
        }
        Ok(Engine { domains: Vec::new(), td: alloc_td(ctx), hw, first: Ghost(first) })
    }

    /// The device of its QPs.
    pub fn context(&self) -> &Context<N> {
        self.hw.context()
    }

    /// Whether its QPs are created on a thread domain (so the driver takes
    /// no locks for them); not every device has thread domains.
    pub fn is_lock_free(&self) -> bool {
        self.td.is_some()
    }

    /// The entries of its CQ.
    pub fn cq_size(&self) -> u32 {
        self.hw.cq_size()
    }

    /// The CQ entries its QPs reserve (the sum of their queue depths).
    pub fn reserved(&self) -> u32 {
        self.hw.reserved()
    }

    /// The parent domain to create a QP of `pd` on: the engine's for that protection domain,
    /// made the first time; `None` where the device has no thread domains.
    fn domain_index(&mut self, pd: &Pd<N>) -> (i: Option<usize>)
        ensures
            final(self).state() == old(self).state(),
            i matches Some(k) ==> k < final(self).domains@.len() && domain_over(final(self).domains@[k as int].domain) == pd_id(*pd),
    {
        proof {
            use_type_invariant(&*self);
        }
        let td = match &self.td {
            Some(td) => td,
            None => return None,
        };
        let id = pd_id_of(pd);
        let mut i = 0;
        while i < self.domains.len()
            invariant
                i <= self.domains@.len(),
                id == pd_id(*pd),
                forall|k: int| 0 <= k < self.domains@.len()
                    ==> domain_over(#[trigger] self.domains@[k].domain) == pd_id(self.domains@[k].pd),
            decreases self.domains@.len() - i,
        {
            if pd_id_of(&self.domains[i].pd) == id {
                return Some(i);
            }
            i += 1;
        }
        let domain = match alloc_domain(pd, td) {
            Some(domain) => domain,
            None => return None,
        };
        let ghost before = self.domains@;
        vec_push(&mut self.domains, Domain { domain, pd: clone_pd(pd) });
        proof {
            use_type_invariant(&*self);
            assert(self.domains@[before.len() as int].domain == domain);
        }
        Some(self.domains.len() - 1)
    }

    /// Creates a QP (in the RESET state) in `pd` with queues of `caps` and requests of at most
    /// `max_batch` WQEs, attached to the engine's CQ.
    ///
    /// `pd` must be of the engine's device. The sizes are checked against what the device
    /// supports, and the QP's depths must fit in what the CQ has not reserved yet. M has one
    /// limit of scatter/gather elements per QP: every WQE is held to the smaller of
    /// `max_send_sge` and `max_recv_sge`.
    pub fn create_qp<T: Transport>(&mut self, pd: &Pd<N>, caps: QpCaps, max_batch: u32) -> (r: Result<QpKey<T>>)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
        }
        let qp_type = transport_of::<T>();
        if !same_device(self.hw.context(), pd) {
            proof {
                self.hw.lemma_moved_refl();
            }
            return Err(Error::Invalid("a protection domain of another device"));
        }
        let at = self.domain_index(pd);
        proof {
            use_type_invariant(&*self);
        }
        let domain = match at {
            Some(i) => Some(&self.domains[i].domain),
            None => None,
        };
        match self.hw.create_qp(pd, domain, qp_type, caps, max_batch) {
            Ok(index) => Ok(QpKey { index: index as u16, _t: PhantomData }),
            Err(e) => Err(e),
        }
    }

    /// Removes a QP: drops the contexts of its requests not delivered, drains
    /// it, destroys it, then releases the buffers its requests still hold (none
    /// is handed out) and its share of the CQ.
    ///
    /// The NIC may still use the buffers of the WQEs in flight, so the QP is
    /// drained first (see the module documentation): moved to the error
    /// state, given a signaled drain WQE, and the CQ polled until nothing of
    /// it is in flight, the other QPs' completions going to their rings as
    /// [`Engine::progress`] sends them. No completion of the QP is left in
    /// the CQ then, so another QP may take its index and its share at once.
    ///
    /// If the device does not flush (no completion for [`DRAIN_TIMEOUT`]),
    /// the QP is kept with its buffers, index and share, never destroyed or
    /// freed, and this fails with [`Error::NotFlushed`]; dropping the
    /// engine tries again. Either way the key names no QP afterwards.
    pub fn remove_qp<T: Transport>(&mut self, qp: QpKey<T>) -> (r: Result<()>)
        ensures
            final(self).moved_from(old(self)),
            r is Err ==> final(self).state().keeps_qp(qp.spec_index() as int),
    {
        proof {
            use_type_invariant(&*self);
            self.hw.lemma_moved_refl();
        }
        let index = qp.index();
        if !self.hw.is_qp(index) {
            return Ok(());
        }
        self.hw.start_drain(index);
        let let_go = self.hw.take_let_go();
        release(let_go);
        let r = self.drain_and_destroy(index);
        self.resume_panics();
        r
    }

    /// [`Engine::remove_qp`] for a caller that holds the engine behind a borrow or a lock: nothing
    /// whose destructor is the program's is dropped here. What it let go of comes back, for the
    /// caller to drop once it has let go of the engine, since a `wr_id` may hold a handle to a QP.
    pub fn remove_qp_later<T: Transport>(&mut self, qp: QpKey<T>) -> (r: (Result<()>, LetGo<C>))
        ensures
            final(self).moved_from(old(self)),
            r.0 is Err ==> final(self).state().keeps_qp(qp.spec_index() as int),
    {
        proof {
            use_type_invariant(&*self);
            self.hw.lemma_moved_refl();
        }
        let index = qp.index();
        if !self.hw.is_qp(index) {
            return (Ok(()), LetGo::new());
        }
        self.hw.start_drain(index);
        let let_go = self.hw.take_let_go();
        (self.drain_and_destroy(index), let_go)
    }

    /// The rest of a removal: drained, then destroyed, or kept (see [`Engine::remove_qp`]).
    fn drain_and_destroy(&mut self, index: usize) -> (r: Result<()>)
        requires
            index < MAX_QPS,
            old(self).state().keeps_qp(index as int),
        ensures
            final(self).moved_from(old(self)),
            r is Err ==> final(self).state().keeps_qp(index as int),
    {
        proof {
            use_type_invariant(&*self);
        }
        let drained = self.drain(index, index + 1);
        proof {
            use_type_invariant(&*self);
        }
        if drained && self.hw.destroy(index) {
            Ok(())
        } else {
            proof {
                assert(self.state().keeps_qp(index as int));
            }
            Err(Error::NotFlushed)
        }
    }

    /// Polls the CQ until the QPs being removed among those from `from` up to `to` have nothing in
    /// flight; `false` if no completion came for [`DRAIN_TIMEOUT`] while some still had. Neither
    /// answer frees anything: what frees a QP is [`Hw::destroy`], which looks at its rings itself.
    #[verifier::exec_allows_no_decreases_clause]
    fn drain(&mut self, from: usize, to: usize) -> (drained: bool)
        ensures
            final(self).moved_from(old(self)),
            final(self).state().same_table(&old(self).state()),
    {
        let ghost start = self.state();
        proof {
            use_type_invariant(&*self);
            start.lemma_moved_refl();
            start.lemma_same_table_refl();
        }
        let mut clock = clock_start();
        loop
            invariant
                self.state().moved_from(&start),
                self.state().same_table(&start),
                start == old(self).state(),
        {
            let mut busy = false;
            let mut index = from;
            while index < to
                invariant
                    self.state().moved_from(&start),
                    self.state().same_table(&start),
                    start == old(self).state(),
                decreases to - index,
            {
                proof {
                    use_type_invariant(&*self);
                }
                self.hw.post_drain(index);
                proof {
                    use_type_invariant(&*self);
                }
                if self.hw.draining(index) {
                    busy = true;
                }
                index += 1;
            }
            if !busy {
                return true;
            }
            proof {
                use_type_invariant(&*self);
            }
            if self.hw.progress() > 0 {
                clock_renew(&mut clock);
            } else if clock_idle(&mut clock) {
                return false;
            }
        }
    }

    /// Everything an engine does as it goes, but for its CQ: gives up every request and drains every
    /// QP, destroys the drained ones and releases their buffers. What the program must get rid of
    /// comes back: the contexts and wakers given up, and the panics of the wakers run.
    fn end(&mut self) -> (r: (LetGo<C>, WakeBatch))
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
            self.hw.lemma_moved_refl();
        }
        let slots = self.hw.qp_slots();
        let mut index = 0;
        while index < slots
            invariant
                self.state().moved_from(&old(self).state()),
            decreases slots - index,
        {
            proof {
                use_type_invariant(&*self);
            }
            self.hw.start_drain(index);
            index += 1;
        }
        proof {
            use_type_invariant(&*self);
        }
        let let_go = self.hw.take_let_go();
        let _ = self.drain(0, slots);
        let mut index = 0;
        while index < slots
            invariant
                self.state().moved_from(&old(self).state()),
            decreases slots - index,
        {
            proof {
                use_type_invariant(&*self);
            }
            if !self.hw.draining(index) {
                let _ = self.hw.destroy(index);
            }
            index += 1;
        }
        proof {
            use_type_invariant(&*self);
        }
        (let_go, self.hw.take_panics())
    }

    /// Destroys its CQ if no QP is left; `false`, and the CQ stays, if some QP the device never
    /// flushed is.
    fn close(&mut self) -> (closed: bool) {
        proof {
            use_type_invariant(&*self);
        }
        hw_at_end(self).finish()
    }

    /// Resumes the first panic of a waker it ran. Here, and not in the hardware side: nothing can
    /// unwind while the engine's invariant is being restored.
    #[inline(always)]
    fn resume_panics(&mut self)
        ensures
            final(self).moved_from(old(self)),
            final(self).state().same_table(&old(self).state()),
    {
        proof {
            use_type_invariant(&*self);
            self.hw.lemma_moved_refl();
            self.hw.lemma_same_table_refl();
        }
        if self.hw.panicked() {
            self.resume_taken();
        }
    }

    /// [`Engine::resume_panics`], when there is one.
    #[cold]
    #[inline(never)]
    fn resume_taken(&mut self)
        ensures
            final(self).moved_from(old(self)),
            final(self).state().same_table(&old(self).state()),
    {
        proof {
            use_type_invariant(&*self);
        }
        let panics = self.hw.take_panics();
        panics.finish();
    }

    /// Panics unless `index` names a QP of the engine.
    fn check(&self, index: usize) {
        proof {
            use_type_invariant(self);
        }
        if !self.hw.is_qp(index) {
            no_qp(index);
        }
    }

    // -------------------------------------------------------------------------
    // Polling

    /// Polls the CQ once, recording what completed in the rings of its QPs
    /// (and waking the wakers registered there); returns the number of
    /// completions. Owners take their results by sequence number
    /// ([`Engine::take_send`], [`Engine::take_recv`]).
    ///
    /// If a registered waker panics, every CQE already polled is still
    /// recorded and every registered notification is attempted before the
    /// panic resumes. Catching that panic does not lose those results.
    #[inline]
    pub fn progress(&mut self) -> (n: usize)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
        }
        let n = self.hw.progress();
        self.resume_panics();
        n
    }

    /// The first half of [`Engine::poll`], for a caller who cannot lend the engine to a callback:
    /// reads at most `n` CQEs and updates the CQ's consumer index, unless some read before are not
    /// routed yet. Returns how many it read. A panic of a waker the last CQEs ran resumes here, with
    /// nothing lost.
    #[inline(always)]
    pub fn read_completions(&mut self, n: usize) -> (read: usize)
        ensures
            final(self).moved_from(old(self)),
    {
        self.resume_panics();
        proof {
            use_type_invariant(&*self);
        }
        self.hw.read_completions(n)
    }

    /// The second half of [`Engine::poll`]: the next request that completed with an owner, `None`
    /// when nothing is left of what was read.
    #[inline(always)]
    pub fn next_completion(&mut self) -> (r: Option<Completion<C, N>>)
        ensures
            final(self).moved_from(old(self)),
            r matches Some(c) ==> old(self).state().hands_out(c),
    {
        proof {
            use_type_invariant(&*self);
        }
        self.hw.next_completion()
    }

    /// The event loop's poll: reads at most `n` CQEs ([`POLL_MAX`] at most), updates the CQ's
    /// consumer index, and then hands every request they complete with an owner (and that was not
    /// taken otherwise) to `f`, with the engine, in each queue's order, and every request refused
    /// with its doorbell at once; returns the number of requests handed out. `f` may post new work
    /// on any QP, take or detach requests, even poll or remove QPs. Nothing is allocated.
    ///
    /// Requests without an owner (detached) release their buffers when they
    /// complete; their first error is reported by [`Engine::take_send_error`].
    /// An unsignaled request completes with the next signaled one of its
    /// queue: a loop that posts unsignaled requests also posts signaled
    /// ones.
    ///
    /// That every `F` may be called is what a program's closure is: only Verus can write one that
    /// asks more.
    #[inline]
    #[verifier::exec_allows_no_decreases_clause]
    pub fn poll<F: FnMut(&mut Engine<C, N>, Completion<C, N>)>(&mut self, n: usize, mut f: F) -> usize
        requires
            forall|any: F, e: &mut Engine<C, N>, c: Completion<C, N>| any.requires((e, c)),
    {
        let _ = self.read_completions(n);
        let mut handed = 0usize;
        loop
            invariant
                forall|any: F, e: &mut Engine<C, N>, c: Completion<C, N>| any.requires((e, c)),
        {
            match self.next_completion() {
                Some(done) => {
                    f(self, done);
                    handed = handed.wrapping_add(1);
                },
                None => return handed,
            }
        }
    }

    // -------------------------------------------------------------------------
    // One QP

    /// Connects the QP to `remote` (RESET to INIT, RTR and RTS). A QP
    /// connects once: from RESET, as created. Until it is connected (or
    /// moved to the error state) it takes no work: doorbells are refused
    /// and receives are not posted. If connecting fails part way, the QP
    /// takes no work until it is moved to the error state.
    pub fn connect<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        attr: &ConnectAttr,
        remote: &Endpoint,
    ) -> (r: Result<()>)
        ensures
            final(self).moved_from(old(self)),
    {
        self.check(qp.index());
        proof {
            use_type_invariant(&*self);
        }
        self.hw.connect(qp.index(), attr, remote)
    }

    /// Moves the QP to the error state: every request in flight completes
    /// with a flush error and hands its buffers back. Work posted from then
    /// on is flushed too.
    pub fn to_error<T: Transport>(&mut self, qp: QpKey<T>) -> (r: Result<()>)
        ensures
            final(self).moved_from(old(self)),
    {
        self.check(qp.index());
        proof {
            use_type_invariant(&*self);
        }
        self.hw.set_error(qp.index())
    }

    /// A request to write on the QP, as a value of its own: no borrow of the engine is held while
    /// its WQEs go in ([`OpenRequest::try_push`], [`OpenRequest::try_push_lent`]; what those hand
    /// back goes through [`Engine::push_open`]), and nothing is allocated for it. Nothing reaches
    /// the hardware or the rings until [`Engine::close_open`] or [`Engine::submit_open`], which look
    /// at the QP's queue then: they say there is no room, or wait for some.
    #[inline(always)]
    pub fn open_request<T: Transport>(&mut self, qp: QpKey<T>) -> (open: OpenRequest<N>)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
        }
        OpenRequest::of(self.hw.open_request(qp.index()))
    }

    /// Adds a WQE to `open`, whatever it takes: what [`OpenRequest::try_push`] could not tell is
    /// checked, and if the request is full it goes out as a request of its own, signaled and
    /// without an owner, waiting for room (polling the CQ). On error nothing is added and the part
    /// is handed back.
    pub fn push_open<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        open: &mut OpenRequest<N>,
        op: WrOp,
        part: Part<N>,
    ) -> (r: Result<(), BufError<Part<N>>>)
        ensures
            final(self).moved_from(old(self)),
            r matches Err(e) ==> e.buf == part,
    {
        self.check(qp.index());
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&*open);
        }
        self.hw.push_other(qp.index(), &mut open.writing, op, part)
    }

    /// Whether `open`'s WQEs fit in the QP's queue now.
    #[inline(always)]
    pub fn open_fits<T: Transport>(&self, qp: QpKey<T>, open: &OpenRequest<N>) -> bool {
        proof {
            use_type_invariant(self);
            use_type_invariant(open);
        }
        self.hw.open_fits(qp.index(), &open.writing)
    }

    /// Writes `open`'s WQEs as a request, signaled if `signal` (else only if the ring needs it),
    /// with `ctx` to come back with its completion, if it has one; it goes out at the next
    /// [`Engine::commit`]. `open` is empty then and takes more. Returns its sequence number; if it
    /// has no WQE, the queue no room ([`Engine::open_fits`] says) or the QP does not take what it
    /// was written for (another protection domain, more WQEs than its requests may have, a READ on
    /// an unreliable connection), `ctx` back and `open` as it was.
    pub fn close_open<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        open: &mut OpenRequest<N>,
        signal: bool,
        ctx: Option<C>,
    ) -> (r: core::result::Result<Seq, Option<C>>)
        ensures
            final(self).moved_from(old(self)),
            r matches Err(back) ==> back == ctx,
    {
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&*open);
        }
        self.hw.close(qp.index(), &mut open.writing, signal, ctx)
    }

    /// `open` as a request with `ctx`, and the doorbell; what it keeps from the QP goes back. If the
    /// queue has no room this waits for some, polling the CQ. On an error its context and buffers
    /// come back.
    #[inline(always)]
    pub fn submit_open<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        open: &mut OpenRequest<N>,
        signal: bool,
        ctx: C,
    ) -> (r: Result<Seq, PostError<C, N>>)
        ensures
            final(self).moved_from(old(self)),
            r matches Err(e) ==> e.wr_id == ctx,
    {
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&*open);
        }
        match self.hw.submit_open(qp.index(), &mut open.writing, signal, ctx) {
            Ok(seq) => Ok(seq),
            Err(refused) => Err(owned(refused)),
        }
    }

    /// What `open` holds goes out as a request without an owner, signaled, and the doorbell; what
    /// it keeps from the QP goes back. If the queue has no room this waits for some, polling the CQ.
    pub fn submit_open_detached<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        open: &mut OpenRequest<N>,
    ) -> (r: Result<()>)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&*open);
            self.hw.lemma_moved_refl();
        }
        let index = qp.index();
        if !open.is_empty() {
            match self.hw.close_waiting(index, &mut open.writing, true, None) {
                Ok(_) => {},
                Err(_) => return Err(Error::QueueFull),
            }
        }
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&*open);
        }
        self.hw.give_back(index, &mut open.writing);
        proof {
            use_type_invariant(&*self);
        }
        match self.hw.commit(index, None) {
            Ok(()) => Ok(()),
            Err((e, _)) => Err(e),
        }
    }

    /// For an owner about to drop `leases`, those of a completed request of the QP: frozen buffers
    /// are released here, and a list of several goes back to the QP for the next such request, so
    /// that it allocates nothing.
    pub fn hand_back_leases<T: Transport>(&mut self, qp: QpKey<T>, leases: &mut Leases<N>)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
        }
        self.hw.hand_back_leases(qp.index(), leases);
    }

    /// What `open` keeps from the QP goes back there, for the next request, if it has no WQE: the
    /// WQEs it has stay in it.
    pub fn give_back<T: Transport>(&mut self, qp: QpKey<T>, open: &mut OpenRequest<N>)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&*open);
        }
        self.hw.give_back(qp.index(), &mut open.writing);
    }

    /// One WQE as a request of its own, signaled, and the doorbell; if the queue is full it waits
    /// for room, polling the CQ.
    #[inline(always)]
    fn post_one<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        op: WrOp,
        part: Part<N>,
        ctx: C,
    ) -> (r: Result<Seq, PostError<C, N>>)
        ensures
            final(self).moved_from(old(self)),
            r matches Err(e) ==> e.wr_id == ctx,
    {
        proof {
            use_type_invariant(&*self);
        }
        match self.hw.post_one(qp.index(), op, part, Some(ctx), true, true) {
            Ok(Ok(seq)) => Ok(seq),
            Ok(Err(refused)) => Err(owned(refused)),
            Err(unposted) => self.post_slow(qp.index(), unposted, true, true),
        }
    }

    /// [`Engine::post_one`] out of the common.
    #[cold]
    #[inline(never)]
    fn post_slow(
        &mut self,
        index: usize,
        unposted: Unposted<C, N>,
        signal: bool,
        ring: bool,
    ) -> (r: Result<Seq, PostError<C, N>>)
        requires
            unposted.ctx is Some,
        ensures
            final(self).moved_from(old(self)),
            r matches Err(e) ==> e.wr_id == unposted.ctx->0,
    {
        self.check(index);
        proof {
            use_type_invariant(&*self);
        }
        match self.hw.post_one_slow(index, unposted, signal, ring) {
            Ok(seq) => Ok(seq),
            Err(refused) => Err(owned(refused)),
        }
    }

    /// One WQE doing `op` on `part` as a signaled request of its own, and the doorbell, if nothing
    /// is out of the common: the QP takes work, nothing is being gathered, there is room and the
    /// checks pass. Otherwise everything comes back untouched, for
    /// [`Engine::post_unposted`] or for later.
    #[inline(always)]
    pub fn try_post<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        op: WrOp,
        part: Part<N>,
        ctx: C,
    ) -> (r: Result<Result<Seq, PostError<C, N>>, Unposted<C, N>>)
        ensures
            final(self).moved_from(old(self)),
            r matches Ok(Err(e)) ==> e.wr_id == ctx,
            r matches Err(back) ==> back.op == op && back.part == part && back.ctx == Some(ctx),
    {
        proof {
            use_type_invariant(&*self);
        }
        match self.hw.post_one(qp.index(), op, part, Some(ctx), true, true) {
            Ok(Ok(seq)) => Ok(Ok(seq)),
            Ok(Err(refused)) => Ok(Err(owned(refused))),
            Err(unposted) => Err(unposted),
        }
    }

    /// [`Engine::try_post`] without the doorbell, as [`Engine::stage`] writes it: signaled if
    /// `signal`, else only if the ring needs it.
    #[inline(always)]
    pub fn try_stage<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        op: WrOp,
        part: Part<N>,
        ctx: C,
        signal: bool,
    ) -> (r: Result<Result<Seq, PostError<C, N>>, Unposted<C, N>>)
        ensures
            final(self).moved_from(old(self)),
            r matches Ok(Err(e)) ==> e.wr_id == ctx,
            r matches Err(back) ==> back.op == op && back.part == part && back.ctx == Some(ctx),
    {
        proof {
            use_type_invariant(&*self);
        }
        match self.hw.post_one(qp.index(), op, part, Some(ctx), signal, false) {
            Ok(Ok(seq)) => Ok(Ok(seq)),
            Ok(Err(refused)) => Ok(Err(owned(refused))),
            Err(unposted) => Err(unposted),
        }
    }

    /// What [`Engine::try_post`] or [`Engine::try_stage`] handed back, as a request of the general
    /// kind, signaled if `signal`, and the doorbell if `ring`: it waits for room if it must,
    /// polling the CQ.
    ///
    /// # Panics
    ///
    /// If it is refused and the program took the context out of `unposted`, so that there is none
    /// to give back.
    pub fn post_unposted<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        unposted: Unposted<C, N>,
        signal: bool,
        ring: bool,
    ) -> (r: Result<Seq, PostError<C, N>>)
        ensures
            final(self).moved_from(old(self)),
    {
        self.check(qp.index());
        proof {
            use_type_invariant(&*self);
        }
        match self.hw.post_one_slow(qp.index(), unposted, signal, ring) {
            Ok(seq) => Ok(seq),
            Err(refused) => {
                if refused.ctx.is_none() {
                    no_context();
                }
                Err(owned(refused))
            },
        }
    }

    /// Writes one WQE doing `op` on `part` as a request of its own, with `ctx` to come back with
    /// its completion, signaled if `signal` (else only if the ring needs it). Its WQE goes out at
    /// the next [`Engine::commit`], with those written before it.
    #[inline]
    pub fn stage<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        op: WrOp,
        part: Part<N>,
        ctx: C,
        signal: bool,
    ) -> (r: Result<Seq, PostError<C, N>>)
        ensures
            final(self).moved_from(old(self)),
            r matches Err(e) ==> e.wr_id == ctx,
    {
        proof {
            use_type_invariant(&*self);
        }
        match self.hw.post_one(qp.index(), op, part, Some(ctx), signal, false) {
            Ok(Ok(seq)) => Ok(seq),
            Ok(Err(refused)) => Err(owned(refused)),
            Err(unposted) => self.post_slow(qp.index(), unposted, signal, false),
        }
    }

    /// [`Engine::stage`] of a request without an owner: nobody will take it back, its buffers are
    /// released when it completes, and its first error is reported by
    /// [`Engine::take_send_error`].
    #[inline]
    pub fn stage_detached<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        op: WrOp,
        part: Part<N>,
        signal: bool,
    ) -> (r: Result<Seq, BufError<Leases<N>>>)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
        }
        let done = match self.hw.post_one(qp.index(), op, part, None, signal, false) {
            Ok(done) => done,
            Err(unposted) => {
                self.check(qp.index());
                proof {
                    use_type_invariant(&*self);
                }
                self.hw.post_one_slow(qp.index(), unposted, signal, false)
            },
        };
        match done {
            Ok(seq) => Ok(seq),
            Err(refused) => Err(BufError { error: refused.error, buf: refused.leases }),
        }
    }

    /// Rings the QP's doorbell for every request written since the last
    /// one. If the device refuses, none of them is posted: each one with an
    /// owner completes at once as [`CompletionError::Refused`], which its owner
    /// takes with its context and buffers ([`Engine::take_send`], or
    /// [`Engine::poll`]); each one without an owner releases its buffers and is
    /// reported by [`Engine::take_send_error`]. Sequence numbers are never
    /// reused. [`Request::submit`] hands the context and buffers of its own
    /// request back with the error instead.
    /// A waker panic during refusal propagates only after all staged
    /// requests have their refusal result and notifications are attempted.
    #[inline]
    pub fn commit<T: Transport>(&mut self, qp: QpKey<T>) -> (r: Result<()>)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
        }
        match self.hw.commit(qp.index(), None) {
            Ok(()) => Ok(()),
            Err((e, _)) => self.refused(qp.index(), e),
        }
    }

    /// A doorbell was refused: what `commit` says of it, the program's own error if the key names
    /// no QP. A waker's panic resumes now.
    #[cold]
    #[inline(never)]
    fn refused(&mut self, index: usize, error: Error) -> (r: Result<()>)
        ensures
            final(self).moved_from(old(self)),
    {
        self.check(index);
        self.resume_panics();
        Err(error)
    }

    /// Takes the context, result and buffers of completed (or refused) request `seq` of `queue`.
    #[inline(always)]
    pub fn take<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        queue: Queue,
        seq: Seq,
    ) -> (r: Option<Completion<C, N>>)
        ensures
            final(self).moved_from(old(self)),
            old(self).state().takeable(qp.spec_index(), queue, seq) ==> r is Some,
            queue is Recv ==> (r matches Some(c) ==> old(self).state().recv_taken_matches(qp.spec_index(), seq, c)),
            queue is Send ==> (r matches Some(c) ==> old(self).state().send_taken_matches(qp.spec_index(), seq, c)),
    {
        proof {
            use_type_invariant(&*self);
        }
        self.hw.take(qp.index(), queue, seq)
    }

    /// Takes the context, result and buffers of completed (or refused) send
    /// request `seq`.
    #[inline]
    pub fn take_send<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) -> (r: Option<Completion<C, N>>)
        ensures
            final(self).moved_from(old(self)),
            old(self).state().takeable(qp.spec_index(), Queue::Send, seq) ==> r is Some,
            r matches Some(c) ==> old(self).state().send_taken_matches(qp.spec_index(), seq, c),
    {
        proof {
            use_type_invariant(&*self);
        }
        self.hw.take(qp.index(), Queue::Send, seq)
    }

    /// Takes the context, result and buffer of completed receive `seq`.
    #[inline]
    pub fn take_recv<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) -> (r: Option<Completion<C, N>>)
        ensures
            final(self).moved_from(old(self)),
            old(self).state().takeable(qp.spec_index(), Queue::Recv, seq) ==> r is Some,
            r matches Some(c) ==> old(self).state().recv_taken_matches(qp.spec_index(), seq, c),
    {
        proof {
            use_type_invariant(&*self);
        }
        self.hw.take(qp.index(), Queue::Recv, seq)
    }

    /// Gives up send request `seq`: its buffers are released when it completes
    /// (now, if it has), and its context comes back, if it has one still.
    pub fn detach_send<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) -> (ctx: Option<C>)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
        }
        let (ctx, _waker) = self.hw.detach(qp.index(), Queue::Send, seq);
        ctx
    }

    /// Gives up receive `seq`: its buffer is released when it completes (now, if
    /// it has), and its context comes back, if it has one still.
    pub fn detach_recv<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) -> (ctx: Option<C>)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
        }
        let (ctx, _waker) = self.hw.detach(qp.index(), Queue::Recv, seq);
        ctx
    }

    /// Gives up request `seq` of `queue`, as [`Engine::detach_send`] and [`Engine::detach_recv`] do,
    /// for a caller that holds the engine behind a borrow or a lock: its context and its waker come
    /// back, to drop once it has let go of the engine.
    pub fn give_up<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        queue: Queue,
        seq: Seq,
    ) -> (given_up: (Option<C>, Option<Waker>))
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
        }
        self.hw.detach(qp.index(), queue, seq)
    }

    /// The first error of a send request without an owner (a dropped future's,
    /// a sink's, a split batch's part), once.
    pub fn take_send_error<T: Transport>(&mut self, qp: QpKey<T>) -> (r: Option<DetachedError>)
        ensures
            final(self).moved_from(old(self)),
            r matches Some(error) ==> old(self).state().detached_taken_matches(qp.spec_index(), error),
    {
        proof {
            use_type_invariant(&*self);
        }
        self.hw.take_send_error(qp.index())
    }

    /// Wakes `waker` when send request `seq` completes; the waker it replaces comes back.
    pub fn set_send_waker<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        seq: Seq,
        waker: &Waker,
    ) -> (replaced: Option<Waker>)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
        }
        self.hw.set_waker(qp.index(), Queue::Send, seq, waker)
    }

    /// Wakes `waker` when receive `seq` completes; the waker it replaces comes back.
    pub fn set_recv_waker<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        seq: Seq,
        waker: &Waker,
    ) -> (replaced: Option<Waker>)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
        }
        self.hw.set_waker(qp.index(), Queue::Recv, seq, waker)
    }

    /// Waits until a new send request of at least one WQE fits on the QP:
    /// rings its doorbell for what is written and polls the CQ until a slot
    /// and a WQE are free. Fails if every slot holds a result nobody took.
    pub fn make_send_room<T: Transport>(&mut self, qp: QpKey<T>) -> (r: Result<()>)
        ensures
            final(self).moved_from(old(self)),
    {
        self.check(qp.index());
        proof {
            use_type_invariant(&*self);
        }
        let r = self.hw.make_room(qp.index());
        self.resume_panics();
        r
    }

    /// Posts a receive into `part`, a unique buffer, on the QP, with `ctx` for
    /// its owner. Refused, with `part` and `ctx`, when the receive queue is
    /// full or the QP takes no work.
    #[inline(always)]
    pub fn post_recv<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        part: Part<N>,
        ctx: C,
    ) -> (r: Result<Seq, PostError<C, N>>)
        ensures
            final(self).moved_from(old(self)),
            r matches Err(e) ==> e.wr_id == ctx,
    {
        match self.try_post_recv(qp, part, ctx) {
            Ok(posted) => posted,
            Err((part, ctx)) => Err(self.recv_error(qp, part, ctx)),
        }
    }

    /// [`Engine::post_recv`] if nothing is out of the common: the QP takes work, the checks pass
    /// and there is room. Otherwise the buffer and `ctx` come back untouched, for
    /// [`Engine::recv_error`] to say why.
    #[inline(always)]
    pub fn try_post_recv<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        part: Part<N>,
        ctx: C,
    ) -> (r: Result<Result<Seq, PostError<C, N>>, ReceiveBack<C, N>>)
        ensures
            final(self).moved_from(old(self)),
            r matches Ok(Err(e)) ==> e.wr_id == ctx,
            r matches Err((back, c)) ==> back == part && c == ctx,
    {
        proof {
            use_type_invariant(&*self);
        }
        match self.hw.post_recv(qp.index(), part, Some(ctx)) {
            Ok(Ok(seq)) => Ok(Ok(seq)),
            Ok(Err(refused)) => Ok(Err(owned(refused))),
            Err((part, ctx)) => Err((part, ctx.expect("the context it was given"))),
        }
    }

    /// Why [`Engine::try_post_recv`] handed a receive back.
    #[cold]
    #[inline(never)]
    pub fn recv_error<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        part: Part<N>,
        ctx: C,
    ) -> (e: PostError<C, N>)
        ensures
            final(self).moved_from(old(self)),
            e.wr_id == ctx,
    {
        self.check(qp.index());
        proof {
            use_type_invariant(&*self);
        }
        let refused = self.hw.recv_refused(qp.index(), part, Some(ctx));
        proof {
            self.hw.lemma_moved_refl();
        }
        owned(refused)
    }

    /// [`Engine::post_recv`] of a receive without an owner: its buffer is released when it
    /// completes, and its first error goes with the detached send errors' own.
    pub fn post_recv_detached<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        part: Part<N>,
    ) -> (r: Result<Seq, BufError<Leases<N>>>)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
        }
        let refused = match self.hw.post_recv(qp.index(), part, None) {
            Ok(Ok(seq)) => return Ok(seq),
            Ok(Err(refused)) => refused,
            Err((part, ctx)) => {
                self.check(qp.index());
                proof {
                    use_type_invariant(&*self);
                }
                self.hw.recv_refused(qp.index(), part, ctx)
            },
        };
        Err(BufError { error: refused.error, buf: refused.leases })
    }

    /// The QP number.
    pub fn qp_num<T: Transport>(&self, qp: QpKey<T>) -> u32 {
        self.check(qp.index());
        proof {
            use_type_invariant(self);
        }
        match self.hw.qp_num(qp.index()) {
            Some((num, _)) => num,
            None => 0,
        }
    }

    /// The protection domain of the QP.
    pub fn pd<T: Transport>(&self, qp: QpKey<T>) -> &Pd<N> {
        match self.hw.qp_pd(qp.index()) {
            Some(pd) => pd,
            None => {
                no_qp(qp.index());
                vstd::pervasive::unreached()
            },
        }
    }

    /// The queues' sizes.
    pub fn caps<T: Transport>(&self, qp: QpKey<T>) -> QpCaps {
        proof {
            use_type_invariant(self);
        }
        match self.hw.qp_caps(qp.index()) {
            Some(caps) => caps,
            None => {
                no_qp(qp.index());
                vstd::pervasive::unreached()
            },
        }
    }

    /// Whether the QP was created without the driver's locks (on the
    /// engine's thread domain).
    pub fn qp_is_lock_free<T: Transport>(&self, qp: QpKey<T>) -> bool {
        self.check(qp.index());
        proof {
            use_type_invariant(self);
        }
        match self.hw.qp_num(qp.index()) {
            Some((_, free)) => free,
            None => false,
        }
    }

    /// The send queue's ring.
    #[inline]
    pub fn send_ring<T: Transport>(&self, qp: QpKey<T>) -> &Ring<Payload<C, N>> {
        proof {
            use_type_invariant(self);
        }
        match self.hw.ring(qp.index(), Queue::Send) {
            Some(ring) => ring,
            None => {
                no_qp(qp.index());
                vstd::pervasive::unreached()
            },
        }
    }

    /// The receive queue's ring.
    #[inline]
    pub fn recv_ring<T: Transport>(&self, qp: QpKey<T>) -> &Ring<Payload<C, N>> {
        proof {
            use_type_invariant(self);
        }
        match self.hw.ring(qp.index(), Queue::Recv) {
            Some(ring) => ring,
            None => {
                no_qp(qp.index());
                vstd::pervasive::unreached()
            },
        }
    }

    /// Starts a request on the QP's send queue: add its WQEs, then give it its
    /// context when it is finished ([`Request::submit`], [`Request::finish`]).
    /// Nothing reaches the hardware until then; its WQEs go out at the next
    /// [`Engine::commit`] of the QP.
    #[inline]
    pub fn request<T: Transport>(&mut self, qp: QpKey<T>) -> (r: Request<'_, T, C, N>) {
        let open = self.open_request(qp);
        Request { w: self, qp, open }
    }
}

/// A request being written on a QP of an engine: one or more WQEs that
/// complete together.
///
/// Nothing reaches the hardware or the ring until it is finished, when its signaling is known: its
/// WQEs and their buffers are a value of its own ([`OpenRequest`]), in storage that comes from the
/// QP and goes back to it. A request longer than the QP's `max_batch` WQEs is split: the part so
/// far goes out as a request of its own (signaled, without an owner) and the rest continues once
/// there is room. A full send queue shows when it is finished. Dropping an unfinished request
/// finishes it without an owner.
#[verifier::reject_recursive_types(C)]
#[verifier::reject_recursive_types(N)]
pub struct Request<'w, T: Transport, C = (), N: Nic = Verbs> {
    w: &'w mut Engine<C, N>,
    qp: QpKey<T>,
    open: OpenRequest<N>,
}

impl<T: Transport, C, N: Nic> Request<'_, T, C, N> {
    /// Adds a WQE doing `op` on `part`; on error nothing is added and the
    /// part is handed back.
    #[inline]
    pub fn push(&mut self, op: WrOp, part: Part<N>) -> (r: Result<(), BufError<Part<N>>>) {
        match self.open.try_push(op, part) {
            Ok(()) => Ok(()),
            Err(part) => self.w.push_open(self.qp, &mut self.open, op, part),
        }
    }

    /// Whether no WQE was added.
    pub fn is_empty(&self) -> bool {
        self.open.is_empty()
    }

    /// Finishes the request, signaled if `signal` (else only if the ring needs
    /// it), with `ctx` to come back with its completion. Returns its sequence
    /// number, or `ctx` if it has no WQE. It goes out at the next commit; if the
    /// device refuses that doorbell, it completes as
    /// [`CompletionError::Refused`] (see [`Engine::commit`]).
    #[inline]
    pub fn finish(mut self, signal: bool, ctx: C) -> (r: core::result::Result<Seq, C>)
        ensures
            r matches Err(back) ==> back == ctx,
    {
        match self.w.close_waiting(self.qp, &mut self.open, signal, Some(ctx)) {
            Ok(seq) => Ok(seq),
            Err(back) => Err(back.expect("the context it was given")),
        }
    }

    /// Finishes the request without an owner: nobody will take it back, and its
    /// buffers are released when it completes. Returns its sequence number,
    /// `None` if it has no WQE.
    pub fn finish_detached(mut self, signal: bool) -> Option<Seq> {
        match self.w.close_waiting(self.qp, &mut self.open, signal, None) {
            Ok(seq) => Some(seq),
            Err(_) => None,
        }
    }

    /// Finishes the request with `ctx`, as [`Request::finish`], and rings the
    /// doorbell. If the device refuses, or it has no WQE, its context and
    /// buffers come back with the error.
    #[inline]
    pub fn submit(mut self, signal: bool, ctx: C) -> (r: Result<Seq, PostError<C, N>>)
        ensures
            r matches Err(e) ==> e.wr_id == ctx,
    {
        self.w.submit_open(self.qp, &mut self.open, signal, ctx)
    }

    /// Finishes the request without an owner and rings the doorbell.
    pub fn submit_detached(mut self) -> Result<()> {
        let _ = self.w.close_waiting(self.qp, &mut self.open, true, None);
        self.w.commit(self.qp)
    }

    /// What `drop` does: an unfinished request goes out without an owner.
    #[inline]
    fn finish_dropped(&mut self) {
        if !self.open.is_empty() {
            let _ = self.w.close_waiting(self.qp, &mut self.open, true, None);
        }
        if self.open.holds_storage() {
            self.w.give_back(self.qp, &mut self.open);
        }
    }
}

impl<C, N: Nic> Engine<C, N> {
    /// `open`, once there is room for it, as a request (this polls the CQ).
    #[inline]
    fn close_waiting<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        open: &mut OpenRequest<N>,
        signal: bool,
        ctx: Option<C>,
    ) -> (r: core::result::Result<Seq, Option<C>>)
        ensures
            final(self).moved_from(old(self)),
            r matches Err(back) ==> back == ctx,
    {
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&*open);
        }
        self.hw.close_waiting(qp.index(), &mut open.writing, signal, ctx)
    }

    /// One more part for the WQE being gathered on the QP.
    fn gather<T: Transport>(&mut self, qp: QpKey<T>, part: Part<N>) -> (r: Result<(), BufError<Part<N>>>)
        ensures
            final(self).moved_from(old(self)),
    {
        self.check(qp.index());
        proof {
            use_type_invariant(&*self);
        }
        self.hw.gather(qp.index(), part)
    }

    /// Drops what was being gathered on the QP.
    fn drop_gathered<T: Transport>(&mut self, qp: QpKey<T>)
        ensures
            final(self).moved_from(old(self)),
    {
        proof {
            use_type_invariant(&*self);
        }
        self.hw.drop_gathered(qp.index());
    }

    /// The WQE gathered on the QP, in `open`.
    fn push_gathered<T: Transport>(&mut self, qp: QpKey<T>, open: &mut OpenRequest<N>, op: WrOp) -> (r: Result<()>)
        ensures
            final(self).moved_from(old(self)),
    {
        self.check(qp.index());
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&*open);
        }
        self.hw.push_gathered(qp.index(), &mut open.writing, op)
    }
}

} // verus!

impl<T: Transport> PartialEq for QpKey<T> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl<T: Transport> Eq for QpKey<T> {}

impl<T: Transport> core::fmt::Debug for QpKey<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "QpKey({})", self.index)
    }
}

impl<C, N: Nic> PostError<C, N> {
    /// Takes back its unique buffer (of a receive, READ or atomic) as a `BufMut<T>`.
    pub fn buf_mut<T: FromBytes>(&mut self) -> Option<BufMut<T, N>> {
        self.leases.take_buf_mut()
    }
}

impl<C, N: Nic> Completion<C, N> {
    /// The number of the QP it was posted on.
    #[inline(always)]
    pub fn qp_num(&self) -> u32 {
        self.qp_num
    }

    /// Takes back its unique buffer (of a receive, READ or atomic) as a `BufMut<T>`; `None` if it
    /// has none or its bytes cannot hold `[T]`.
    #[inline(always)]
    pub fn buf_mut<T: FromBytes>(&mut self) -> Option<BufMut<T, N>> {
        self.leases.take_buf_mut()
    }
}

impl<C, N: Nic> core::fmt::Debug for Completion<C, N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Completion")
            .field("result", &self.result)
            .field("qp_num", &self.qp_num)
            .finish_non_exhaustive()
    }
}

impl<C, N: Nic> core::fmt::Debug for PostError<C, N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PostError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl<C, N: Nic> core::fmt::Display for PostError<C, N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Display::fmt(&self.error, f)
    }
}

impl<C, N: Nic> std::error::Error for PostError<C, N> {}

impl<C, N: Nic> From<PostError<C, N>> for Error {
    fn from(e: PostError<C, N>) -> Error {
        e.error
    }
}

impl<C, N: Nic> From<PostError<C, N>> for std::io::Error {
    fn from(e: PostError<C, N>) -> std::io::Error {
        e.error.into()
    }
}

impl<C, N: Nic> Engine<C, N> {
    /// The bytes of a QP in the table, for the measurements.
    #[doc(hidden)]
    pub const QP_BYTES: usize = Hw::<C, N>::QP_BYTES;

    /// How peers reach the QP through `port`, by GID `gid_index` if given.
    pub fn endpoint<T: Transport>(
        &self,
        qp: QpKey<T>,
        port: u8,
        gid_index: Option<u8>,
    ) -> Result<Endpoint> {
        let ctx = self.context();
        let info = ctx.port(port)?;
        let gid = match gid_index {
            Some(index) => {
                let mut found = None;
                for entry in ctx.gids(port) {
                    if entry.index == index {
                        found = Some(entry.gid);
                    }
                }
                Some(found.ok_or(Error::Invalid("no GID at that index"))?)
            }
            None => None,
        };
        Ok(Endpoint {
            qp_num: self.qp_num(qp),
            lid: info.lid,
            gid,
        })
    }

    /// SENDs `buf` into the peer's next receive, as a signaled request of its
    /// own, and rings the doorbell. `ctx` comes back with its completion.
    #[inline]
    pub fn send<T: Transport, U: IntoBytes + Immutable>(
        &mut self,
        qp: QpKey<T>,
        buf: Buf<U, N>,
        ctx: C,
    ) -> Result<Seq, PostError<C, N>> {
        self.post_one(qp, WrOp::Send, buf.into_part(), ctx)
    }

    /// [`Engine::send`] with immediate data `imm`.
    #[inline]
    pub fn send_imm<T: Transport, U: IntoBytes + Immutable>(
        &mut self,
        qp: QpKey<T>,
        buf: Buf<U, N>,
        imm: u32,
        ctx: C,
    ) -> Result<Seq, PostError<C, N>> {
        self.post_one(qp, WrOp::SendImm(imm), buf.into_part(), ctx)
    }

    /// RDMA WRITEs `buf` to `dst` in the peer's memory, as a signaled request
    /// of its own, and rings the doorbell. `ctx` comes back with its completion.
    #[inline]
    pub fn write<T: Transport, U: IntoBytes + Immutable>(
        &mut self,
        qp: QpKey<T>,
        buf: Buf<U, N>,
        dst: RemoteBuf,
        ctx: C,
    ) -> Result<Seq, PostError<C, N>> {
        self.post_one(qp, WrOp::Write(dst), buf.into_part(), ctx)
    }

    /// [`Engine::write`] with immediate data `imm`, which also consumes one of
    /// the peer's receives.
    #[inline]
    pub fn write_imm<T: Transport, U: IntoBytes + Immutable>(
        &mut self,
        qp: QpKey<T>,
        buf: Buf<U, N>,
        dst: RemoteBuf,
        imm: u32,
        ctx: C,
    ) -> Result<Seq, PostError<C, N>> {
        self.post_one(qp, WrOp::WriteImm(dst, imm), buf.into_part(), ctx)
    }

    /// RDMA READs `src` from the peer's memory into `buf`, as a signaled
    /// request of its own, and rings the doorbell. `ctx` comes back with its
    /// completion and the buffer.
    #[inline]
    pub fn read<T: Reliable, U: FromBytes + IntoBytes>(
        &mut self,
        qp: QpKey<T>,
        buf: BufMut<U, N>,
        src: RemoteBuf,
        ctx: C,
    ) -> Result<Seq, PostError<C, N>> {
        self.post_one(qp, WrOp::Read(src), buf.into_part(), ctx)
    }

    /// Posts a receive into `buf`. `ctx` comes back with its completion and the
    /// buffer.
    #[inline(always)]
    pub fn recv<T: Transport, U: FromBytes + IntoBytes>(
        &mut self,
        qp: QpKey<T>,
        buf: BufMut<U, N>,
        ctx: C,
    ) -> Result<Seq, PostError<C, N>> {
        self.post_recv(qp, buf.into_part(), ctx)
    }
}

impl<C, N: Nic> Drop for Engine<C, N> {
    /// Gives up every request and drains every QP (all at once, as [`Engine::remove_qp`] does one),
    /// destroys the drained ones and releases their buffers, then destroys the CQ and, as the fields
    /// go, the parent domains, the thread domain and the device. If the device does not flush some
    /// QP, that QP is leaked with its buffers, and so are the CQ and the domains (destroying them
    /// would need the QP gone). The contexts and wakers given up go last, and a waker's panic: what
    /// they run is the program's, and if it unwinds, the fields are all there is left.
    fn drop(&mut self) {
        let (let_go, panics) = self.end();
        if !self.close() {
            self.hw.leak();
            core::mem::forget(core::mem::take(&mut self.domains));
            core::mem::forget(self.td.take());
        }
        drop(let_go);
        panics.finish();
    }
}

impl<T: Transport, C, N: Nic> Request<'_, T, C, N> {
    /// Adds a WQE doing `op` on the concatenation of `parts` (a gather
    /// list of at most the QP's `max_send_sge` parts). On error nothing is added and the parts
    /// are dropped.
    pub fn push_gather<I>(&mut self, op: WrOp, parts: I) -> Result<()>
    where
        I: IntoIterator<Item = Part<N>>,
    {
        // What an iterator that panicked left behind is not part of this WQE.
        self.w.drop_gathered(self.qp);
        for (i, part) in parts.into_iter().enumerate() {
            let n = i + 1;
            if let Err(e) = self.w.gather(self.qp, part) {
                self.w.drop_gathered(self.qp);
                let max = match e.error {
                    Error::TooManySges { max, .. } => max,
                    _ => MAX_SGES,
                };
                return Err(Error::TooManySges { got: n, max });
            }
        }
        self.w.push_gathered(self.qp, &mut self.open, op)
    }
}

impl<T: Transport, C, N: Nic> Drop for Request<'_, T, C, N> {
    #[inline]
    fn drop(&mut self) {
        self.finish_dropped();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Model;
    use crate::nic::{ConnectAttr, QpCaps};
    use crate::pd::Context;
    use crate::transport::{RC, UC};
    use crate::wc::CompletionError;
    use crate::wr_id::{WQE_MASK, index_of, is_recv, wqe_of, wr_id};

    fn caps(depth: u32) -> QpCaps {
        QpCaps {
            max_send_wr: depth,
            max_recv_wr: depth,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 0,
        }
    }

    #[test]
    fn wr_ids_round_trip() {
        for &(index, recv, wqe) in &[
            (0u16, false, 0u64),
            (65535, true, WQE_MASK),
            (7, false, (1 << 50) + 3),
        ] {
            let id = wr_id(index, recv, wqe);
            assert_eq!(index_of(id), index as usize);
            assert_eq!(is_recv(id), recv);
            // Any WQE at most 2^47 after the oldest in flight comes back.
            for retired in [wqe, wqe.saturating_sub(1), wqe.saturating_sub(4096)] {
                assert_eq!(wqe_of(id, retired), wqe);
            }
        }
        // A WQE before the oldest in flight maps far ahead.
        assert!(wqe_of(wr_id(0, false, 5), 6) > 6 + (1 << 46));
    }

    /// What routing a completion costs: the `wr_id` decoding the engine
    /// does, against finding the QP by its number (a linear scan, as a
    /// engine of a few QPs would, and a hash map). Run with `cargo test
    /// --release -p safeverbs-core -- --ignored --nocapture demux_cost`.
    #[test]
    #[ignore = "a measurement"]
    fn demux_cost() {
        use core::hint::black_box;
        use std::collections::HashMap;
        use std::time::Instant;
        const N: usize = 1 << 24;
        for qps in [1usize, 4, 16, 64, 256] {
            // Completions of random QPs and queues, with their QP numbers.
            let mut x = 0x9E37_79B9_7F4A_7C15u64;
            let wcs: Vec<(u64, u32)> = (0..4096)
                .map(|i| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let q = (x as usize) % qps;
                    (wr_id(q as u16, x & 1 == 0, i), 0x100 + 7 * q as u32)
                })
                .collect();
            let nums: Vec<u32> = (0..qps).map(|q| 0x100 + 7 * q as u32).collect();
            let map: HashMap<u32, usize> = nums.iter().enumerate().map(|(i, &n)| (n, i)).collect();
            let time = |f: &dyn Fn(u64, u32) -> u64| {
                let start = Instant::now();
                let mut sum = 0u64;
                for i in 0..N {
                    let (id, num) = wcs[i % wcs.len()];
                    sum = sum.wrapping_add(f(black_box(id), black_box(num)));
                }
                black_box(sum);
                start.elapsed().as_nanos() as f64 / N as f64
            };
            let by_wr_id = time(&|id, _| {
                let index = index_of(id) as u64;
                let recv = is_recv(id);
                index * 2 + recv as u64 + wqe_of(id, 0)
            });
            let by_scan = time(&|id, num| {
                let index = nums.iter().position(|&n| n == num).unwrap_or(0) as u64;
                let recv = is_recv(id);
                index * 2 + recv as u64 + wqe_of(id, 0)
            });
            let by_map = time(&|id, num| {
                let index = map.get(&num).copied().unwrap_or(0) as u64;
                let recv = is_recv(id);
                index * 2 + recv as u64 + wqe_of(id, 0)
            });
            println!(
                "{qps:4} QPs: wr_id {by_wr_id:.2} ns, qp_num scan {by_scan:.2} ns, qp_num hash map {by_map:.2} ns per completion"
            );
        }
    }

    /// The CQ's capacity is reserved per QP; a QP that does not fit is
    /// refused, and removing one frees its share.
    #[test]
    fn qps_reserve_the_cq() {
        let pd = Context::new(Model::new(3)).alloc_pd().unwrap();
        let mut w: Engine<(), _> = Engine::new(pd.context(), 64).unwrap();
        let a = w.create_qp::<RC>(&pd, caps(16), 4).unwrap();
        let b = w.create_qp::<RC>(&pd, caps(16), 4).unwrap();
        assert_eq!(w.reserved(), 64);
        assert_eq!(
            w.create_qp::<RC>(&pd, caps(1), 1).err(),
            Some(Error::CqFull { need: 2, free: 0 })
        );
        w.remove_qp(a).unwrap();
        assert_eq!(w.reserved(), 32);
        let c = w.create_qp::<RC>(&pd, caps(16), 4).unwrap();
        assert_eq!(c.index(), a.index(), "the index is reused");
        assert_ne!(w.qp_num(c), w.qp_num(b));
    }

    /// Two RC QPs of one engine connected to each other.
    fn connected_pair(
        w: &mut Engine<(), Model>,
        pd: &Pd<Model>,
        depth: u32,
        batch: u32,
    ) -> (QpKey<RC>, QpKey<RC>) {
        let a = w.create_qp::<RC>(pd, caps(depth), batch).unwrap();
        let b = w.create_qp::<RC>(pd, caps(depth), batch).unwrap();
        let attr = ConnectAttr {
            port: 1,
            access: crate::nic::Access::LOCAL_WRITE,
            path_mtu: 5,
            rq_psn: 0,
            sq_psn: 0,
            gid_index: None,
            traffic_class: 0,
            timeout: 14,
            retry_count: 7,
            rnr_retry: 7,
            min_rnr_timer: 12,
            max_rd_atomic: 1,
            max_dest_rd_atomic: 1,
        };
        let (ea, eb) = (
            w.endpoint(a, 1, None).unwrap(),
            w.endpoint(b, 1, None).unwrap(),
        );
        w.connect(a, &attr, &eb).unwrap();
        w.connect(b, &attr, &ea).unwrap();
        (a, b)
    }

    /// Removing QPs with work in flight drains them first: receives that
    /// will never match, and unsignaled sends that succeeded but have no
    /// completion of their own (the drain WQE completes them). The model
    /// panics if a QP is destroyed with a WQE not completed (M's
    /// `destroy_qp`); every buffer comes back.
    #[test]
    fn remove_qp_drains_work_in_flight() {
        let model = Model::new(11);
        let pd = Context::new(model.clone()).alloc_pd().unwrap();
        let mut w = Engine::new(pd.context(), 64).unwrap();
        let (a, b) = connected_pair(&mut w, &pd, 8, 1);
        let frozen = pd.alloc::<u8>(64).unwrap().freeze();
        // Receives on both sides; the sends of `a` take two of `b`'s.
        for qp in [a, a, b, b, b] {
            let buf = pd.alloc::<u8>(64).unwrap();
            w.post_recv(qp, buf.into_part(), ()).unwrap();
        }
        // A signaled send, then an unsignaled one: once both ran, the last
        // has no completion and only a later one could retire it.
        let mut req = w.request(a);
        req.push(WrOp::Send, frozen.slice(..8).into_part()).unwrap();
        req.submit(true, ()).unwrap();
        let mut req = w.request(a);
        req.push(WrOp::Send, frozen.slice(..8).into_part()).unwrap();
        req.submit(false, ()).unwrap();
        model.run();
        w.progress();
        assert_eq!(model.pending(), 3, "the sends ran, three receives wait");
        assert_eq!(w.send_ring(a).in_flight_wqes(), 1, "the unsignaled send");
        w.remove_qp(a).unwrap();
        assert_eq!(w.reserved(), 16);
        w.remove_qp(b).unwrap();
        assert_eq!(w.reserved(), 0);
        assert!(frozen.try_thaw().is_ok(), "the sends' buffer is back");
        drop(w);
        drop(pd);
        assert_eq!(model.registered(), 0);
    }

    /// A ring whose counters have reached their end takes no more requests and no completion would
    /// give it room: waiting for room says so, and does not spin for ever.
    #[test]
    fn counters_at_their_end_fail_who_waits_for_room() {
        let model = Model::new(11);
        let pd = Context::new(model.clone()).alloc_pd().unwrap();
        let mut w = Engine::<(), _>::new(pd.context(), 64).unwrap();
        let (a, _) = connected_pair(&mut w, &pd, 8, 1);
        w.hw.send_ring_mut(a.index()).at_counters_end();
        assert!(matches!(w.make_send_room(a), Err(Error::Clogged)));
    }

    /// Dropping an engine drains every QP before destroying it.
    #[test]
    fn dropping_an_engine_drains_its_qps() {
        let model = Model::new(13);
        model.set_pace(900, 1);
        let pd = Context::new(model.clone()).alloc_pd().unwrap();
        let mut w = Engine::new(pd.context(), 64).unwrap();
        let (a, b) = connected_pair(&mut w, &pd, 8, 6);
        let frozen = pd.alloc::<u8>(64).unwrap().freeze();
        for _ in 0..4 {
            let buf = pd.alloc::<u8>(64).unwrap();
            w.post_recv_detached(b, buf.into_part()).unwrap();
        }
        let mut req = w.request(a);
        for _ in 0..6 {
            req.push(WrOp::Send, frozen.slice(..8).into_part()).unwrap();
        }
        req.submit(false, ()).unwrap();
        drop(w);
        assert!(frozen.try_thaw().is_ok(), "the sends' buffer is back");
        drop(pd);
        assert_eq!(model.registered(), 0);
    }

    /// A device that never flushes: removing the QP fails, and its buffers
    /// are kept, never freed, with its index and share of the CQ; once the
    /// device moves again, dropping the engine drains and releases them.
    /// If it never does, the engine leaks the QP, its buffers and its CQ.
    #[test]
    fn a_device_that_never_flushes_keeps_the_buffers() {
        for recovers in [true, false] {
            let model = Model::new(17);
            let pd = Context::new(model.clone()).alloc_pd().unwrap();
            let mut w = Engine::new(pd.context(), 64).unwrap();
            let (a, b) = connected_pair(&mut w, &pd, 8, 1);
            let buf = pd.alloc::<u8>(64).unwrap();
            let ptr = buf.as_ptr() as usize;
            w.post_recv(a, buf.into_part(), ()).unwrap();
            model.halt(true);
            assert_eq!(w.remove_qp(a), Err(Error::NotFlushed));
            assert_eq!(w.reserved(), 32, "its share of the CQ stays taken");
            let c = w.create_qp::<RC>(&pd, caps(8), 8).unwrap();
            assert_ne!(c.index(), a.index(), "its index stays taken");
            assert_eq!(model.registered(), 1, "its buffer stays registered");
            let _ = (b, ptr);
            if recovers {
                model.halt(false);
            }
            drop(w);
            drop(pd);
            assert_eq!(model.registered(), if recovers { 0 } else { 1 });
        }
    }

    /// A request handed back to its QP while it has WQEs keeps them, and they still go out.
    #[test]
    fn giving_back_leaves_the_wqes_written() {
        let model = Model::new(19);
        let pd = Context::new(model.clone()).alloc_pd().unwrap();
        let mut w = Engine::new(pd.context(), 64).unwrap();
        let (a, _b) = connected_pair(&mut w, &pd, 8, 4);
        let buf = pd.alloc::<u8>(64).unwrap().freeze();
        let mut open = w.open_request(a);
        assert!(open.try_push(WrOp::Send, buf.clone().into_part()).is_ok());
        assert!(open.try_push(WrOp::Send, buf.into_part()).is_ok());
        w.give_back(a, &mut open);
        assert_eq!(open.len(), 2);
        assert!(w.submit_open(a, &mut open, true, ()).is_ok());
        assert!(open.is_empty());
        assert_eq!(w.send_ring(a).in_flight_wqes(), 2);
    }

    /// A request is a value of its own, and it goes out only on a QP that takes what it was written
    /// for: not on one of another protection domain, nor on an unreliable one if the NIC writes its
    /// buffers, nor on one whose requests have fewer WQEs. It stays whole for its own QP.
    #[test]
    fn a_request_goes_out_only_on_a_qp_that_takes_it() {
        let model = Model::new(23);
        let pd = Context::new(model.clone()).alloc_pd().unwrap();
        let other_pd = pd.context().alloc_pd().unwrap();
        let mut w = Engine::new(pd.context(), 256).unwrap();
        let (a, _b) = connected_pair(&mut w, &pd, 8, 4);
        let other = w.create_qp::<RC>(&other_pd, caps(8), 4).unwrap();
        let unreliable = w.create_qp::<UC>(&pd, caps(8), 4).unwrap();
        let short = w.create_qp::<RC>(&pd, caps(8), 1).unwrap();
        let remote = RemoteBuf {
            addr: 0,
            len: 64,
            rkey: 0,
        };
        let mut open = w.open_request(a);
        w.push_open(
            a,
            &mut open,
            WrOp::Read(remote),
            pd.alloc::<u8>(64).unwrap().into_part(),
        )
        .unwrap();
        let buf = pd.alloc::<u8>(64).unwrap().freeze();
        assert!(open.try_push(WrOp::Send, buf.into_part()).is_ok());
        for refused in [
            w.close_open(other, &mut open, true, None).is_err(),
            w.close_open(unreliable, &mut open, true, None).is_err(),
            w.close_open(short, &mut open, true, None).is_err(),
        ] {
            assert!(refused);
        }
        assert_eq!(
            w.submit_open_detached(short, &mut open),
            Err(Error::QueueFull),
            "a request without an owner that cannot go out says so"
        );
        assert_eq!(open.len(), 2);
        // Waiting for room on one of them never ends well: it says so, with the buffers.
        let error = w.submit_open(short, &mut open, true, ()).unwrap_err();
        assert_eq!(
            error.error,
            Error::Invalid("a request written for another QP")
        );
        assert_eq!(error.leases.len(), 2);
        assert!(open.is_empty());
    }

    /// A request that holds no storage takes no WQE: pushing to one says so and hands the buffer
    /// back.
    #[test]
    fn pushing_to_a_request_without_storage_is_refused() {
        let model = Model::new(29);
        let pd = Context::new(model.clone()).alloc_pd().unwrap();
        let mut w = Engine::new(pd.context(), 64).unwrap();
        let (a, _b) = connected_pair(&mut w, &pd, 8, 4);
        let buf = pd.alloc::<u8>(64).unwrap().freeze();
        let mut closed = OpenRequest::closed();
        let refused = w
            .push_open(a, &mut closed, WrOp::Send, buf.clone().into_part())
            .unwrap_err();
        assert_eq!(
            refused.error,
            Error::Invalid("a request written for another QP")
        );
        let mut open = w.open_request(a);
        w.give_back(a, &mut open);
        let refused = w
            .push_open(a, &mut open, WrOp::Send, buf.into_part())
            .unwrap_err();
        assert_eq!(refused.error, Error::Invalid("a request that takes no WQE"));
    }

    /// A refused doorbell drops the requests written before it: one
    /// without an owner is reported as refused, one with an owner completes
    /// as refused too (and `poll` hands it out), and numbers are not reused.
    #[test]
    fn refused_doorbell_retires_staged_requests() {
        let pd = Context::new(Model::new(7)).alloc_pd().unwrap();
        let mut w: Engine<u32, _> = Engine::new(pd.context(), 64).unwrap();
        // Not connected: the engine refuses the doorbell, as a device would.
        let qp = w.create_qp::<RC>(&pd, caps(8), 4).unwrap();
        let buf = pd.alloc::<u8>(64).unwrap().freeze();
        let mut req = w.request(qp);
        req.push(WrOp::Send, buf.clone().into_part()).unwrap();
        assert_eq!(req.finish_detached(false), Some(0));
        let mut req = w.request(qp);
        req.push(WrOp::Send, buf.clone().into_part()).unwrap();
        let owned = req.finish(true, 11).unwrap();
        let mut req = w.request(qp);
        req.push(WrOp::Send, buf.into_part()).unwrap();
        let refused = req.submit(true, 22).unwrap_err();
        assert_eq!(refused.error, Error::NotConnected);
        assert_eq!(refused.wr_id, 22);
        assert_eq!(refused.leases.len(), 1);
        assert_eq!(w.send_ring(qp).posted(), 3);
        assert_eq!(w.send_ring(qp).in_flight_wqes(), 0);
        assert_eq!(
            w.take_send_error(qp),
            Some(DetachedError {
                seq: 0,
                error: CompletionError::Refused
            }),
            "the dropped request is reported"
        );
        assert!(w.take_send_error(qp).is_none());
        let mut got = Vec::new();
        w.poll(32, |_, c| {
            got.push((c.qp, c.queue, c.seq, c.wr_id, c.result, c.leases.len()))
        });
        assert_eq!(
            got,
            [(
                qp.index(),
                Queue::Send,
                owned,
                11,
                Err(CompletionError::Refused),
                1
            )]
        );
    }
}
