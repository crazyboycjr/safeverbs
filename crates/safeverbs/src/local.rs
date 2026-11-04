//! A completion queue and queue pairs for one thread's event loop, shaped like verbs: the `wr_id`
//! type belongs to the CQ, a QP refers to its CQ, and a polling loop decides the dispatch.

use core::cell::{RefCell, RefMut};
use std::rc::Rc;

use safeverbs_core::{
    Buf, BufMut, Completion, Endpoint, Engine, Error, Leases, Nic, Part, QpKey, Reliable,
    RemoteBuf, Result, Transport, Unposted, WrOp,
};
use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::batch::Batch;
use crate::cq::{Cq, sealed};
use crate::{Pd, Verbs};

/// CQEs [`LocalCq::poll_one`] reads at a time: the others wait for the next calls.
const POLL_ONE_READS: usize = 16;

/// A completion queue for one thread's event loop, whose requests carry a `wr_id` of the type
/// `W`, as in C but of any type: it comes back exactly once, with its completion.
///
/// Neither `Send` nor `Sync`: created and used on one thread, and so is every QP on it, where the
/// device has thread domains on one of them (the driver takes no QP lock, its QPs share one
/// dedicated doorbell, and the CQ is single-threaded). This layer adds a reference count and a
/// `RefCell` flag: no atomics, no locks. Cheap to clone (a handle). It lives as long as any handle
/// to it or any of its QPs.
///
/// ```no_run
/// # use safeverbs::{Context, LocalCq, LocalQp, QpCaps, RC, Result, Buf};
/// enum Req {
///     Reply { client: u32 },
///     Log,
/// }
///
/// # fn f(ctx: Context, qp: LocalQp<RC, Req>, cq: LocalCq<Req>, buf: Buf<u8>) -> Result<()> {
/// qp.post_send(buf.slice(..), Req::Reply { client: 7 })?;
/// cq.poll(32, |wc| match (wc.wr_id, wc.result) {
///     (Req::Reply { client }, Ok(_)) => println!("replied to {client}"),
///     (Req::Log, Ok(_)) => {}
///     // Before the error's position every WQE completed; from it on, none did, and what those
///     // did at the peer is unknown.
///     (_, Err(error)) => eprintln!("a request failed: {error}"),
/// });
/// # Ok(()) }
/// ```
pub struct LocalCq<W = u64, N: Nic = Verbs> {
    engine: Rc<RefCell<Engine<W, N>>>,
}

impl<W, N: Nic> Clone for LocalCq<W, N> {
    fn clone(&self) -> Self {
        LocalCq {
            engine: self.engine.clone(),
        }
    }
}

impl<W, N: Nic> sealed::Sealed for LocalCq<W, N> {}

impl<W, N: Nic> Cq for LocalCq<W, N> {
    type Nic = N;
    type Ctx = W;
    type Qp<T: Transport> = LocalQp<T, W, N>;

    fn with_engine<R>(&self, f: impl FnOnce(&mut Engine<W, N>) -> R) -> R {
        f(&mut self.engine.borrow_mut())
    }

    fn adopt<T: Transport>(&self, _key: QpKey<T>) {}

    fn remove<T: Transport>(&self, key: QpKey<T>) {
        // A QP the device does not flush stays in the engine with its buffers
        // (`Error::NotFlushed`): nothing more to do here.
        let _ = self.engine.borrow_mut().remove_qp(key);
    }

    fn into_qp<T: Transport>(self, key: QpKey<T>, endpoint: Endpoint) -> LocalQp<T, W, N> {
        LocalQp {
            cq: self,
            key,
            endpoint,
        }
    }
}

impl<W, N: Nic> LocalCq<W, N> {
    pub(crate) fn new(ctx: &safeverbs_core::Context<N>, entries: u32) -> Result<Self> {
        Ok(LocalCq {
            engine: Rc::new(RefCell::new(Engine::new(ctx, entries)?)),
        })
    }

    /// Reads at most `n` CQEs (1 to [`POLL_MAX`](crate::raw::POLL_MAX), which also bounds the time
    /// a loop spends on completions), like `ibv_poll_cq(cq, n, wcs)` with the array inside, and hands every request
    /// they complete to `f`, in each queue's order; returns how many it handed out. An unsignaled
    /// request completes with the next signaled one of its queue, so that may be more than `n`.
    ///
    /// The CQEs are read and the CQ's consumer index updated before `f` gets any of them, and
    /// no borrow is held while `f` runs, so `f` may post on any QP, poll this CQ again (the next
    /// CQEs are handed out in order) or drop QPs.
    #[inline]
    pub fn poll(&self, n: usize, mut f: impl FnMut(Wc<W, N>)) -> usize {
        self.engine.borrow_mut().read_completions(n);
        let mut handed = 0;
        loop {
            let next = self.engine.borrow_mut().next_completion();
            match next {
                Some(done) => {
                    f(done);
                    handed += 1;
                }
                None => return handed,
            }
        }
    }

    /// The next completed request, if any, for loops that want `break`, `?` or `.await`, which
    /// [`LocalCq::poll`]'s closure makes awkward. It reads several CQEs at once when it has none
    /// left, and the next calls hand out what they complete.
    #[inline]
    pub fn poll_one(&self) -> Option<Wc<W, N>> {
        let mut engine = self.engine.borrow_mut();
        match engine.next_completion() {
            Some(done) => Some(done),
            None => {
                engine.read_completions(POLL_ONE_READS);
                engine.next_completion()
            }
        }
    }

    /// The entries of the CQ.
    pub fn size(&self) -> u32 {
        self.engine.borrow().cq_size()
    }

    /// The CQ entries its QPs reserve (the sum of their queue depths).
    pub fn reserved(&self) -> u32 {
        self.engine.borrow().reserved()
    }

    /// Whether its QPs are created without the driver's locks (on a thread
    /// domain; not every device has them).
    pub fn is_lock_free(&self) -> bool {
        self.engine.borrow().is_lock_free()
    }

    /// The engine under this CQ, with its QPs by index ([`LocalQp::as_raw`] gives a QP's key), for
    /// what the handles do not offer.
    ///
    /// # Panics
    ///
    /// If called while the handle's other methods are running (the `RefCell` is borrowed).
    pub fn as_raw(&self) -> RefMut<'_, Engine<W, N>> {
        self.engine.borrow_mut()
    }
}

/// A request that completed, as [`LocalCq::poll`] hands it out: an owned value, with only what
/// its operation defines: the `wr_id` it was posted with, what it did or why it did not
/// (`result`), [`qp_num`](Completion::qp_num) and its buffers
/// ([`buf_mut`](Completion::buf_mut), `leases`).
pub type Wc<W = u64, N = Verbs> = Completion<W, N>;

/// A request that could not be posted: why (`error`), and its `wr_id` and buffers
/// ([`buf_mut`](safeverbs_core::PostError::buf_mut), `leases`).
pub type PostError<W = u64, N = Verbs> = safeverbs_core::PostError<W, N>;

/// A connected QP on a [`LocalCq`], whose requests carry a `wr_id` of the CQ's type: neither `Send`
/// nor `Sync`. The handle goes anywhere on the thread (a `Conn` struct, a map).
///
/// Posting checks what it must, one comparison each: the buffer is of the QP's protection domain,
/// fits, and the queue has room. A full queue is an error here, since nothing polls for it: the
/// `wr_id` and the buffers come back with it ([`PostError`]). Each request stays in its QP's ring
/// with its buffers until a completion on the CQ hands it out; dropping the QP drains it (moves it
/// to the error state and waits until the NIC has flushed every work request left) before
/// destroying it and releasing the buffers.
pub struct LocalQp<T: Transport, W = u64, N: Nic = Verbs> {
    cq: LocalCq<W, N>,
    key: QpKey<T>,
    endpoint: Endpoint,
}

impl<T: Transport, W, N: Nic> Drop for LocalQp<T, W, N> {
    fn drop(&mut self) {
        self.cq.remove(self.key);
    }
}

/// `Ok` of a posted request.
type Posted<W, N> = core::result::Result<(), PostError<W, N>>;

/// A request of one WQE that did not take the fast path: a full queue, which is an error here, or
/// whatever else was out of the common, which the engine's general path decides.
#[cold]
#[inline(never)]
fn not_common<T: Transport, W, N: Nic>(
    engine: &mut Engine<W, N>,
    key: QpKey<T>,
    unposted: Unposted<W, N>,
    signal: bool,
    ring: bool,
) -> Posted<W, N> {
    let send = engine.send_ring(key);
    let error = if send.can_post() {
        return engine
            .post_unposted(key, unposted, signal, ring)
            .map(|_| ());
    } else if send.is_clogged() {
        Error::Clogged
    } else {
        Error::QueueFull
    };
    Err(PostError {
        error,
        wr_id: unposted.ctx.expect("the value it was given"),
        leases: Leases::from(unposted.part),
    })
}

impl<T: Transport, W, N: Nic> LocalQp<T, W, N> {
    /// A signaled request of one WQE, and the doorbell. Unlike the engine's general path it never
    /// waits for room. The caller borrowed the engine first and made `part` after, so that no
    /// `Part` crosses the borrow's panic: that would keep it in memory.
    #[inline(always)]
    fn post(
        &self,
        mut engine: RefMut<'_, Engine<W, N>>,
        op: WrOp,
        part: Part<N>,
        wr_id: W,
    ) -> Posted<W, N> {
        match engine.try_post(self.key, op, part, wr_id) {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(unposted) => not_common(&mut engine, self.key, unposted, true, true),
        }
    }

    /// The engine under the CQ.
    #[inline(always)]
    fn engine(&self) -> RefMut<'_, Engine<W, N>> {
        self.cq.engine.borrow_mut()
    }

    /// SENDs `buf` into the peer's next receive and rings the doorbell. `wr_id` comes back with
    /// its completion.
    #[inline(always)]
    pub fn post_send<U: IntoBytes + Immutable>(&self, buf: Buf<U, N>, wr_id: W) -> Posted<W, N> {
        let engine = self.engine();
        self.post(engine, WrOp::Send, buf.into_part(), wr_id)
    }

    /// [`LocalQp::post_send`] with immediate data `imm`.
    #[inline(always)]
    pub fn post_send_imm<U: IntoBytes + Immutable>(
        &self,
        buf: Buf<U, N>,
        imm: u32,
        wr_id: W,
    ) -> Posted<W, N> {
        let engine = self.engine();
        self.post(engine, WrOp::SendImm(imm), buf.into_part(), wr_id)
    }

    /// RDMA WRITEs `buf` to `dst` in the peer's memory and rings the doorbell. `wr_id` comes back
    /// with its completion.
    #[inline(always)]
    pub fn post_write<U: IntoBytes + Immutable>(
        &self,
        buf: Buf<U, N>,
        dst: RemoteBuf,
        wr_id: W,
    ) -> Posted<W, N> {
        let engine = self.engine();
        self.post(engine, WrOp::Write(dst), buf.into_part(), wr_id)
    }

    /// [`LocalQp::post_write`] with immediate data `imm`, which also consumes one of the peer's
    /// receives.
    #[inline(always)]
    pub fn post_write_imm<U: IntoBytes + Immutable>(
        &self,
        buf: Buf<U, N>,
        dst: RemoteBuf,
        imm: u32,
        wr_id: W,
    ) -> Posted<W, N> {
        let engine = self.engine();
        self.post(engine, WrOp::WriteImm(dst, imm), buf.into_part(), wr_id)
    }

    /// Posts a receive into `buf`. `wr_id` comes back with its completion and the buffer.
    #[inline(always)]
    pub fn post_recv<U: FromBytes + IntoBytes>(&self, buf: BufMut<U, N>, wr_id: W) -> Posted<W, N> {
        let mut engine = self.engine();
        match engine.try_post_recv(self.key, buf.into_part(), wr_id) {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err((part, wr_id)) => Err(engine.recv_error(self.key, part, wr_id)),
        }
    }

    /// SENDs the concatenation of `bufs` as one message (a gather list of at most the QP's
    /// `max_send_sge` buffers) and rings the doorbell. Since a gather is out of the common, a full
    /// queue waits for room by polling, keeping the completions of others for the next
    /// [`LocalCq::poll`].
    pub fn post_send_gather<U: IntoBytes + Immutable>(
        &self,
        bufs: impl IntoIterator<Item = Buf<U, N>>,
        wr_id: W,
    ) -> Posted<W, N> {
        let parts = bufs.into_iter().map(Buf::into_part);
        let mut engine = self.cq.engine.borrow_mut();
        let mut req = engine.request(self.key);
        match req.push_gather(WrOp::Send, parts) {
            Ok(()) => req.submit(true, wr_id).map(|_| ()),
            Err(error) => Err(PostError {
                error,
                wr_id,
                leases: Leases::default(),
            }),
        }
    }

    /// Posts the SENDs and WRITEs `f` adds as one request, of one doorbell and one completion,
    /// with one `wr_id`. A batch longer than the QP's `max_batch` is split into requests of that
    /// size, those before the last without a `wr_id` (which may wait for room by polling, keeping
    /// the completions of others for the next [`LocalCq::poll`]). `f` runs with the CQ borrowed: it
    /// must not use this QP, another QP of the CQ, or the CQ.
    pub fn post_batch(&self, wr_id: W, f: impl FnOnce(&mut Batch<'_, T, N, W>)) -> Posted<W, N> {
        let mut engine = self.cq.engine.borrow_mut();
        let mut batch = Batch::direct(engine.request(self.key));
        f(&mut batch);
        match batch.post(wr_id) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Writes one WQE doing `op` on `part` as a request of its own, without ringing the doorbell:
    /// [`LocalQp::ring`] sends every request written. It asks for a completion if `signaled`, or
    /// if the queue needs one; without one, it completes with the next signaled request of this
    /// queue, and only then does `wr_id` come back, with its buffers. A loop that posts unsignaled
    /// requests also posts signaled ones. A full queue is an error, as for [`LocalQp::post_send`].
    #[inline]
    pub fn stage(&self, op: WrOp, part: Part<N>, wr_id: W, signaled: bool) -> Posted<W, N> {
        let mut engine = self.cq.engine.borrow_mut();
        match engine.try_stage(self.key, op, part, wr_id, signaled) {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(unposted) => not_common(&mut engine, self.key, unposted, signaled, false),
        }
    }

    /// Rings the doorbell for every request written with [`LocalQp::stage`]. If the device refuses,
    /// none of them is posted, and each comes back from [`LocalCq::poll`] as
    /// [`CompletionError::Refused`](crate::CompletionError::Refused): the NIC never saw it.
    #[inline]
    pub fn ring(&self) -> Result<()> {
        self.cq.engine.borrow_mut().commit(self.key)
    }

    /// Whether a new request of one WQE fits now.
    #[inline]
    pub fn can_post(&self) -> bool {
        self.cq.engine.borrow().send_ring(self.key).can_post()
    }

    /// Whether a new receive fits now.
    #[inline]
    pub fn can_post_recv(&self) -> bool {
        self.cq.engine.borrow().recv_ring(self.key).can_post()
    }

    /// WQEs posted on the send queue that did not complete yet.
    #[inline]
    pub fn in_flight(&self) -> u32 {
        self.cq.engine.borrow().send_ring(self.key).in_flight_wqes()
    }

    /// How the peer reaches this QP (what the handshake exchanged).
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    /// The QP number.
    pub fn qp_num(&self) -> u32 {
        self.endpoint.qp_num
    }

    /// The protection domain.
    pub fn pd(&self) -> Pd<N> {
        Pd::from_raw(self.cq.engine.borrow().pd(self.key).clone())
    }

    /// The CQ it is on.
    pub fn cq(&self) -> LocalCq<W, N> {
        self.cq.clone()
    }

    /// Whether the QP was created without the driver's locks (on a thread domain; not every device
    /// has them).
    pub fn is_lock_free(&self) -> bool {
        self.cq.engine.borrow().qp_is_lock_free(self.key)
    }

    /// Moves the QP to the error state: every request in flight completes with a flush error,
    /// handing its buffers back.
    pub fn force_error(&self) -> Result<()> {
        self.cq.engine.borrow_mut().to_error(self.key)
    }

    /// The QP's key in the engine under its CQ ([`LocalCq::as_raw`]).
    pub fn as_raw(&self) -> QpKey<T> {
        self.key
    }
}

impl<T: Reliable, W, N: Nic> LocalQp<T, W, N> {
    /// RDMA READs `src` from the peer's memory into `buf` and rings the doorbell. `wr_id` comes back
    /// with its completion and the buffer.
    #[inline(always)]
    pub fn post_read<U: FromBytes + IntoBytes>(
        &self,
        buf: BufMut<U, N>,
        src: RemoteBuf,
        wr_id: W,
    ) -> Posted<W, N> {
        let engine = self.engine();
        self.post(engine, WrOp::Read(src), buf.into_part(), wr_id)
    }

    /// Atomically adds `add` to the 8 bytes at `dst` in the peer's memory; their old value lands
    /// in `old` (one `u64`), which comes back with its completion.
    pub fn post_fetch_add(
        &self,
        old: BufMut<u64, N>,
        dst: RemoteBuf,
        add: u64,
        wr_id: W,
    ) -> Posted<W, N> {
        let engine = self.engine();
        self.post(
            engine,
            WrOp::FetchAdd { remote: dst, add },
            old.into_part(),
            wr_id,
        )
    }

    /// Atomically replaces the 8 bytes at `dst` in the peer's memory with `swap` if they equal
    /// `compare`; their old value lands in `old` (one `u64`), which comes back with its completion.
    pub fn post_compare_swap(
        &self,
        old: BufMut<u64, N>,
        dst: RemoteBuf,
        compare: u64,
        swap: u64,
        wr_id: W,
    ) -> Posted<W, N> {
        let op = WrOp::CompareSwap {
            remote: dst,
            compare,
            swap,
        };
        let engine = self.engine();
        self.post(engine, op, old.into_part(), wr_id)
    }
}
