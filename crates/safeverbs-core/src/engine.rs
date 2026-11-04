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
//! name, and QPs are destroyed only drained. This module is the unverified
//! shell around it: keys, thread domains and the drain's timing.
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

use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::task::Waker;
use std::time::{Duration, Instant};

use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::buf::{Buf, BufError, BufMut, Leases, Part};
use crate::error::{Error, Result};
pub use crate::hw::Unposted;
use crate::hw::{Hw, Refusal};
use crate::nic::{ConnectAttr, Endpoint, Nic, QpCaps, RemoteBuf, WrOp};
use crate::pd::{Context, Pd};
use crate::ring::{Payload, Ring, Seq};
use crate::transport::{Reliable, Transport};
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

/// A QP of an engine: its index there, typed by its transport. Cheap to copy.
///
/// A key names a QP of the engine that created it, until the QP is removed;
/// using it with another engine, or after the removal, names another QP or
/// none (and the methods panic). That is a logic error, not a memory-safety
/// one: the engine checks what each WQE may do against the QP it is posted
/// on.
pub struct QpKey<T: Transport> {
    index: u16,
    _t: PhantomData<fn() -> T>,
}

impl<T: Transport> QpKey<T> {
    /// The QP's index in its engine, as [`Completion::qp`] reports it.
    #[inline(always)]
    pub fn index(self) -> usize {
        self.index as usize
    }
}

impl<T: Transport> Clone for QpKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Transport> Copy for QpKey<T> {}

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

/// A request that could not be posted: why, and its context and buffers.
pub struct PostError<C = (), N: Nic = Verbs> {
    /// What went wrong.
    pub error: Error,
    /// The value it was given (its context).
    pub wr_id: C,
    /// Its buffers.
    pub leases: Leases<N>,
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

/// A receive [`Engine::try_post_recv`] hands back: its buffer and its context.
pub type ReceiveBack<C, N> = (Part<N>, C);

/// A refusal of a request given a context, with that context.
#[cold]
#[inline(never)]
fn owned<C, N: Nic>(refused: Refusal<C, N>) -> PostError<C, N> {
    PostError {
        error: refused.error,
        wr_id: refused.ctx.expect("the context it was given"),
        leases: refused.leases,
    }
}

/// A parent domain over a protection domain and the engine's thread domain.
struct Domain<N: Nic> {
    /// Dropped before its protection domain.
    domain: N::Domain,
    pd: Pd<N>,
}

/// A completion queue and the QPs attached to it (the synchronous layer), with
/// a context of type `C` kept with each request (see the module documentation).
///
/// Dropping it drains its QPs (as [`Engine::remove_qp`] does, all at once),
/// drops the contexts of the requests not delivered, destroys the QPs, releases
/// the buffers their requests held, then destroys the CQ and the thread domain.
/// A QP the device never flushes is leaked with its buffers, and so are the CQ
/// and the domain.
pub struct Engine<C = (), N: Nic = Verbs> {
    /// The verified hardware side; dropped by hand, in `drop`.
    hw: ManuallyDrop<Hw<C, N>>,
    /// The parent domains its QPs are created on, one per protection domain; dropped by hand.
    domains: ManuallyDrop<Vec<Domain<N>>>,
    /// The thread domain of its QPs, where the device has them; dropped by hand, last.
    td: ManuallyDrop<Option<N::Td>>,
}

impl<C, N: Nic> Engine<C, N> {
    /// An engine on the device of `ctx` with a CQ of `cq_size` entries, on a thread domain of
    /// its own where the device has them.
    pub fn new(ctx: &Context<N>, cq_size: u32) -> Result<Engine<C, N>> {
        let hw = Hw::new(ctx.clone(), cq_size)?;
        Ok(Engine {
            hw: ManuallyDrop::new(hw),
            domains: ManuallyDrop::new(Vec::new()),
            td: ManuallyDrop::new(ctx.nic().alloc_td()),
        })
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

    /// Panics unless `index` names a QP of the engine.
    fn check(&self, index: usize) {
        assert!(self.hw.is_qp(index), "no QP {index} in this engine");
    }

    /// The parent domain to create a QP of `pd` on: the engine's for that protection domain,
    /// made the first time; `None` where the device has no thread domains.
    fn domain_index(&mut self, pd: &Pd<N>) -> Option<usize> {
        let td = (*self.td).as_ref()?;
        if let Some(i) = self.domains.iter().position(|d| d.pd.id() == pd.id()) {
            return Some(i);
        }
        let domain = pd.context().nic().alloc_domain(pd.raw(), td)?;
        self.domains.push(Domain {
            domain,
            pd: pd.clone(),
        });
        Some(self.domains.len() - 1)
    }

    /// Creates a QP (in the RESET state) in `pd` with queues of `caps` and requests of at most
    /// `max_batch` WQEs, attached to the engine's CQ.
    ///
    /// `pd` must be of the engine's device. The sizes are checked against what the device
    /// supports, and the QP's depths must fit in what the CQ has not reserved yet. M has one
    /// limit of scatter/gather elements per QP: every WQE is held to the smaller of
    /// `max_send_sge` and `max_recv_sge`.
    pub fn create_qp<T: Transport>(
        &mut self,
        pd: &Pd<N>,
        caps: QpCaps,
        max_batch: u32,
    ) -> Result<QpKey<T>> {
        const {
            assert!(
                T::IN_ORDER_COMPLETION,
                "only in-order transports are supported"
            );
        }
        if !self.context().same_device(pd.context()) {
            return Err(Error::Invalid("a protection domain of another device"));
        }
        let domain = self.domain_index(pd);
        let domain = domain.map(|i| &self.domains[i].domain);
        let index = self.hw.create_qp(pd, domain, T::QP_TYPE, caps, max_batch)?;
        Ok(QpKey {
            index: index as u16,
            _t: PhantomData,
        })
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
    pub fn remove_qp<T: Transport>(&mut self, qp: QpKey<T>) -> Result<()> {
        let index = qp.index();
        if !self.hw.is_qp(index) {
            return Ok(());
        }
        self.hw.start_drain(index);
        if self.drain(Some(index)) && self.hw.destroy(index) {
            Ok(())
        } else {
            Err(Error::NotFlushed)
        }
    }

    /// Polls the CQ until the QPs being removed (`only` that one, or all)
    /// have nothing in flight; `false` if no completion came for
    /// [`DRAIN_TIMEOUT`] while some still had.
    fn drain(&mut self, only: Option<usize>) -> bool {
        let (from, to) = match only {
            Some(index) => (index, index + 1),
            None => (0, self.hw.qp_slots()),
        };
        let mut deadline = Instant::now() + DRAIN_TIMEOUT;
        let mut idle = 0u32;
        loop {
            let mut busy = false;
            for index in from..to {
                self.hw.post_drain(index);
                busy |= self.hw.draining(index);
            }
            if !busy {
                return true;
            }
            if self.hw.progress() > 0 {
                deadline = Instant::now() + DRAIN_TIMEOUT;
                idle = 0;
                continue;
            }
            idle += 1;
            if idle.is_multiple_of(DRAIN_SPINS) {
                if Instant::now() > deadline {
                    return false;
                }
                std::thread::yield_now();
            } else {
                core::hint::spin_loop();
            }
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
    pub fn progress(&mut self) -> usize {
        self.hw.progress()
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
    #[inline]
    pub fn poll<F: FnMut(&mut Engine<C, N>, Completion<C, N>)>(
        &mut self,
        n: usize,
        mut f: F,
    ) -> usize {
        self.hw.read_completions(n);
        let mut handed = 0;
        while let Some(done) = self.hw.next_completion() {
            f(self, done);
            handed += 1;
        }
        handed
    }

    /// The first half of [`Engine::poll`], for a caller who cannot lend the engine to a callback:
    /// reads at most `n` CQEs and updates the CQ's consumer index, unless some read before are not
    /// routed yet. Returns how many it read.
    #[inline(always)]
    pub fn read_completions(&mut self, n: usize) -> usize {
        self.hw.read_completions(n)
    }

    /// The second half of [`Engine::poll`]: the next request that completed with an owner, `None`
    /// when nothing is left of what was read.
    #[inline(always)]
    pub fn next_completion(&mut self) -> Option<Completion<C, N>> {
        self.hw.next_completion()
    }

    // -------------------------------------------------------------------------
    // One QP

    /// The QP number.
    pub fn qp_num<T: Transport>(&self, qp: QpKey<T>) -> u32 {
        self.check(qp.index());
        self.hw.qp_num(qp.index()).map_or(0, |(num, _)| num)
    }

    /// The protection domain of the QP.
    pub fn pd<T: Transport>(&self, qp: QpKey<T>) -> &Pd<N> {
        self.check(qp.index());
        self.hw.qp_pd(qp.index()).expect("a QP of this engine")
    }

    /// The queues' sizes.
    pub fn caps<T: Transport>(&self, qp: QpKey<T>) -> QpCaps {
        self.check(qp.index());
        self.hw.qp_caps(qp.index()).expect("a QP of this engine")
    }

    /// Whether the QP was created without the driver's locks (on the
    /// engine's thread domain).
    pub fn qp_is_lock_free<T: Transport>(&self, qp: QpKey<T>) -> bool {
        self.check(qp.index());
        self.hw.qp_num(qp.index()).is_some_and(|(_, free)| free)
    }

    /// The send queue's ring.
    #[inline]
    pub fn send_ring<T: Transport>(&self, qp: QpKey<T>) -> &Ring<Payload<C, N>> {
        match self.hw.ring(qp.index(), Queue::Send) {
            Some(ring) => ring,
            None => panic!("no QP {} in this engine", qp.index()),
        }
    }

    /// The receive queue's ring.
    #[inline]
    pub fn recv_ring<T: Transport>(&self, qp: QpKey<T>) -> &Ring<Payload<C, N>> {
        match self.hw.ring(qp.index(), Queue::Recv) {
            Some(ring) => ring,
            None => panic!("no QP {} in this engine", qp.index()),
        }
    }

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
    ) -> Result<()> {
        self.check(qp.index());
        self.hw.connect(qp.index(), attr, remote)
    }

    /// Moves the QP to the error state: every request in flight completes
    /// with a flush error and hands its buffers back. Work posted from then
    /// on is flushed too.
    pub fn to_error<T: Transport>(&mut self, qp: QpKey<T>) -> Result<()> {
        self.check(qp.index());
        self.hw.set_error(qp.index())
    }

    /// Starts a request on the QP's send queue: add its WQEs, then give it its
    /// context when it is finished ([`Request::submit`], [`Request::finish`]).
    /// Nothing reaches the hardware until then; its WQEs go out at the next
    /// [`Engine::commit`] of the QP.
    #[inline]
    pub fn request<T: Transport>(&mut self, qp: QpKey<T>) -> Request<'_, T, C, N> {
        Request {
            w: self,
            index: qp.index(),
            _t: PhantomData,
        }
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
    ) -> Result<Seq, PostError<C, N>> {
        match self
            .hw
            .post_one(qp.index(), op, part, Some(ctx), true, true)
        {
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
    ) -> Result<Seq, PostError<C, N>> {
        self.hw
            .post_one_slow(index, unposted, signal, ring)
            .map_err(owned)
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
    ) -> Result<Result<Seq, PostError<C, N>>, Unposted<C, N>> {
        match self
            .hw
            .post_one(qp.index(), op, part, Some(ctx), true, true)
        {
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
    ) -> Result<Result<Seq, PostError<C, N>>, Unposted<C, N>> {
        match self
            .hw
            .post_one(qp.index(), op, part, Some(ctx), signal, false)
        {
            Ok(Ok(seq)) => Ok(Ok(seq)),
            Ok(Err(refused)) => Ok(Err(owned(refused))),
            Err(unposted) => Err(unposted),
        }
    }

    /// What [`Engine::try_post`] or [`Engine::try_stage`] handed back, as a request of the general
    /// kind, signaled if `signal`, and the doorbell if `ring`: it waits for room if it must,
    /// polling the CQ.
    pub fn post_unposted<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        unposted: Unposted<C, N>,
        signal: bool,
        ring: bool,
    ) -> Result<Seq, PostError<C, N>> {
        self.post_slow(qp.index(), unposted, signal, ring)
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
    ) -> Result<Seq, PostError<C, N>> {
        match self
            .hw
            .post_one(qp.index(), op, part, Some(ctx), signal, false)
        {
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
    ) -> Result<Seq, BufError<Leases<N>>> {
        let done = match self.hw.post_one(qp.index(), op, part, None, signal, false) {
            Ok(done) => done,
            Err(unposted) => self.hw.post_one_slow(qp.index(), unposted, signal, false),
        };
        done.map_err(|refused| BufError {
            error: refused.error,
            buf: refused.leases,
        })
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
    pub fn commit<T: Transport>(&mut self, qp: QpKey<T>) -> Result<()> {
        self.hw.commit(qp.index(), None).map_err(|(e, _)| e)
    }

    /// Takes the context, result and buffers of completed (or refused) request `seq` of `queue`.
    #[inline(always)]
    pub fn take<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        queue: Queue,
        seq: Seq,
    ) -> Option<Completion<C, N>> {
        self.hw.take(qp.index(), queue, seq)
    }

    /// Takes the context, result and buffers of completed (or refused) send
    /// request `seq`.
    #[inline]
    pub fn take_send<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) -> Option<Completion<C, N>> {
        self.hw.take(qp.index(), Queue::Send, seq)
    }

    /// Takes the context, result and buffer of completed receive `seq`.
    #[inline]
    pub fn take_recv<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) -> Option<Completion<C, N>> {
        self.hw.take(qp.index(), Queue::Recv, seq)
    }

    /// Gives up send request `seq`: its buffers are released when it completes
    /// (now, if it has), and its context comes back, if it has one still.
    pub fn detach_send<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) -> Option<C> {
        self.hw.detach(qp.index(), Queue::Send, seq)
    }

    /// Gives up receive `seq`: its buffer is released when it completes (now, if
    /// it has), and its context comes back, if it has one still.
    pub fn detach_recv<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq) -> Option<C> {
        self.hw.detach(qp.index(), Queue::Recv, seq)
    }

    /// The first error of a send request without an owner (a dropped future's,
    /// a sink's, a split batch's part), once.
    pub fn take_send_error<T: Transport>(&mut self, qp: QpKey<T>) -> Option<DetachedError> {
        self.hw.take_send_error(qp.index())
    }

    /// Wakes `waker` when send request `seq` completes.
    pub fn set_send_waker<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq, waker: &Waker) {
        self.hw.set_waker(qp.index(), Queue::Send, seq, waker);
    }

    /// Wakes `waker` when receive `seq` completes.
    pub fn set_recv_waker<T: Transport>(&mut self, qp: QpKey<T>, seq: Seq, waker: &Waker) {
        self.hw.set_waker(qp.index(), Queue::Recv, seq, waker);
    }

    /// Waits until a new send request of at least one WQE fits on the QP:
    /// rings its doorbell for what is written and polls the CQ until a slot
    /// and a WQE are free. Fails if every slot holds a result nobody took.
    pub fn make_send_room<T: Transport>(&mut self, qp: QpKey<T>) -> Result<()> {
        self.hw.make_room(qp.index())
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
    ) -> Result<Seq, PostError<C, N>> {
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
    ) -> Result<Result<Seq, PostError<C, N>>, ReceiveBack<C, N>> {
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
    ) -> PostError<C, N> {
        owned(self.hw.recv_refused(qp.index(), part, Some(ctx)))
    }

    /// [`Engine::post_recv`] of a receive without an owner: its buffer is released when it
    /// completes, and its first error goes with the detached send errors' own.
    pub fn post_recv_detached<T: Transport>(
        &mut self,
        qp: QpKey<T>,
        part: Part<N>,
    ) -> Result<Seq, BufError<Leases<N>>> {
        let refused = match self.hw.post_recv(qp.index(), part, None) {
            Ok(Ok(seq)) => return Ok(seq),
            Ok(Err(refused)) => refused,
            Err((part, ctx)) => self.hw.recv_refused(qp.index(), part, ctx),
        };
        Err(BufError {
            error: refused.error,
            buf: refused.leases,
        })
    }
}

impl<C, N: Nic> Drop for Engine<C, N> {
    /// Drops the contexts of the requests not delivered and drains every QP
    /// (all at once, as [`Engine::remove_qp`] does one), destroys the drained
    /// ones and releases their buffers, then destroys the CQ and the thread
    /// domain. If the device does not flush some QP,
    /// that QP is leaked with its buffers, and so are the CQ and the domain
    /// (destroying them would need the QP gone).
    fn drop(&mut self) {
        let slots = self.hw.qp_slots();
        for index in 0..slots {
            self.hw.start_drain(index);
        }
        self.drain(None);
        for index in 0..slots {
            if !self.hw.draining(index) {
                self.hw.destroy(index);
            }
        }
        // SAFETY: taken once, here; the fields are not used again.
        let (hw, domains, td) = unsafe {
            (
                ManuallyDrop::take(&mut self.hw),
                ManuallyDrop::take(&mut self.domains),
                ManuallyDrop::take(&mut self.td),
            )
        };
        match hw.finish() {
            Ok(()) => {
                drop(domains);
                drop(td);
            }
            // A QP the NIC may still use: nothing of it is ever freed.
            Err(hw) => core::mem::forget((hw, domains, td)),
        }
    }
}

/// A request being written on a QP of an engine: one or more WQEs that
/// complete together.
///
/// Nothing reaches the hardware until it is finished, when its signaling is known. A request
/// longer than the QP's `max_batch` WQEs, or than the room in the send queue, is split: the part
/// so far goes out as a request of its own (signaled, without an owner) and the rest continues
/// once there is room. Dropping an unfinished request finishes it without an owner. The WQEs and
/// their buffers are kept by the engine's verified hardware side.
pub struct Request<'w, T: Transport, C = (), N: Nic = Verbs> {
    w: &'w mut Engine<C, N>,
    index: usize,
    _t: PhantomData<fn() -> T>,
}

impl<T: Transport, C, N: Nic> Request<'_, T, C, N> {
    /// Adds a WQE doing `op` on `part`; on error nothing is added and the
    /// part is handed back.
    #[inline]
    pub fn push(&mut self, op: WrOp, part: Part<N>) -> Result<(), BufError<Part<N>>> {
        self.w.hw.push(self.index, op, part)
    }

    /// Adds a WQE doing `op` on the concatenation of `parts` (a gather
    /// list for sends and WRITEs, a scatter list for READs); on error
    /// nothing is added and the parts are dropped.
    pub fn push_gather<I>(&mut self, op: WrOp, parts: I) -> Result<()>
    where
        I: IntoIterator<Item = Part<N>>,
    {
        for (i, part) in parts.into_iter().enumerate() {
            let n = i + 1;
            if let Err(e) = self.w.hw.gather(self.index, part) {
                self.w.hw.drop_gathered(self.index);
                let max = match e.error {
                    Error::TooManySges { max, .. } => max,
                    _ => MAX_SGES,
                };
                return Err(Error::TooManySges { got: n, max });
            }
        }
        self.w.hw.push_gathered(self.index, op)
    }

    /// Whether no WQE was added.
    pub fn is_empty(&self) -> bool {
        !self.w.hw.is_open(self.index)
    }

    /// Finishes the request, signaled if `signal` (else only if the ring needs
    /// it), with `ctx` to come back with its completion. Returns its sequence
    /// number, or `ctx` if it has no WQE. It goes out at the next commit; if the
    /// device refuses that doorbell, it completes as
    /// [`CompletionError::Refused`] (see [`Engine::commit`]).
    #[inline]
    pub fn finish(self, signal: bool, ctx: C) -> core::result::Result<Seq, C> {
        self.w
            .hw
            .close(self.index, signal, Some(ctx))
            .map_err(|ctx| ctx.expect("the context it was given"))
    }

    /// Finishes the request without an owner: nobody will take it back, and its
    /// buffers are released when it completes. Returns its sequence number,
    /// `None` if it has no WQE.
    pub fn finish_detached(self, signal: bool) -> Option<Seq> {
        self.w.hw.close(self.index, signal, None).ok()
    }

    /// Finishes the request with `ctx`, as [`Request::finish`], and rings the
    /// doorbell. If the device refuses, or it has no WQE, its context and
    /// buffers come back with the error.
    #[inline]
    pub fn submit(self, signal: bool, ctx: C) -> Result<Seq, PostError<C, N>> {
        let seq = match self.w.hw.close(self.index, signal, Some(ctx)) {
            Ok(seq) => seq,
            Err(ctx) => {
                return Err(PostError {
                    error: Error::Invalid("a request without a WQE"),
                    wr_id: ctx.expect("the context it was given"),
                    leases: Leases::default(),
                });
            }
        };
        match self.w.hw.commit_own(self.index, seq) {
            Ok(seq) => Ok(seq),
            Err(refused) => Err(owned(refused)),
        }
    }

    /// Finishes the request without an owner (signaled) and rings the
    /// doorbell.
    pub fn submit_detached(self) -> Result<()> {
        let _ = self.w.hw.close(self.index, true, None);
        self.w.hw.commit(self.index, None).map_err(|(e, _)| e)
    }
}

impl<T: Transport, C, N: Nic> Drop for Request<'_, T, C, N> {
    fn drop(&mut self) {
        // An unfinished request goes out without an owner (a no-op once
        // finished).
        let _ = self.w.hw.close(self.index, true, None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Model;
    use crate::nic::{ConnectAttr, QpCaps};
    use crate::pd::Context;
    use crate::transport::RC;
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
