//! Backpressure: work that does not fit in a full queue waits here, in
//! order, and is posted by whichever future polls the QP next once there is
//! room. A future of waiting work holds a ticket; when its work is posted,
//! the ticket's result (the request's sequence number, or an error with the
//! buffers) waits for it here.

use std::collections::VecDeque;

use safeverbs_core::qp::{MAX_SGES, QueuePair};
use safeverbs_core::{Error, Leases, Nic, Part, Seq, Transport, WrOp};

/// Work for one request.
pub(crate) enum Work<N: Nic> {
    /// One send-queue WQE.
    One(WrOp, Part<N>),
    /// One send-queue WQE with a gather list.
    Gather(WrOp, [Option<Part<N>>; MAX_SGES]),
    /// A batch of send-queue WQEs.
    Batch(Vec<(WrOp, Part<N>)>),
    /// A receive.
    Recv(Part<N>),
}

/// What posting work gave: the request's sequence number, or an error and
/// the buffers.
pub(crate) type Posted<N> = Result<Seq, (Error, Leases<N>)>;

struct Entry<N: Nic> {
    /// The work, until posted.
    work: Option<Work<N>>,
    /// The outcome of posting it, until its future takes it.
    result: Option<Posted<N>>,
    /// Its future took the result or was dropped.
    gone: bool,
}

/// Waiting work of one queue, in posting order.
pub(crate) struct Backlog<N: Nic> {
    /// The ticket of `entries[0]`.
    base: u64,
    entries: VecDeque<Entry<N>>,
}

impl<N: Nic> Backlog<N> {
    pub(crate) fn new() -> Self {
        Backlog {
            base: 0,
            entries: VecDeque::new(),
        }
    }

    /// Whether no work waits (new work may be posted at once).
    pub(crate) fn is_idle(&self) -> bool {
        let mut i = 0;
        while i < self.entries.len() {
            if self.entries[i].work.is_some() {
                return false;
            }
            i += 1;
        }
        true
    }

    /// Queues `work`; returns its ticket.
    pub(crate) fn push(&mut self, work: Work<N>) -> u64 {
        let ticket = self.base + self.entries.len() as u64;
        self.entries.push_back(Entry {
            work: Some(work),
            result: None,
            gone: false,
        });
        ticket
    }

    /// Posts waiting work in order while it fits.
    pub(crate) fn drain<T: Transport>(&mut self, qp: &mut QueuePair<T, N>) {
        let mut i = 0;
        while i < self.entries.len() {
            if self.entries[i].work.is_some() {
                let fits = match &self.entries[i].work {
                    Some(work) => fits(qp, work),
                    None => true,
                };
                if !fits {
                    break;
                }
                // A dropped future's work was dropped with it, so whoever
                // waits here still wants the result.
                let work = self.entries[i].work.take().expect("work");
                self.entries[i].result = Some(post(qp, work));
            }
            i += 1;
        }
        self.trim();
    }

    /// Takes the result for `ticket`, once its work is posted.
    pub(crate) fn take(&mut self, ticket: u64) -> Option<Posted<N>> {
        let i = (ticket - self.base) as usize;
        let result = self.entries[i].result.take();
        if result.is_some() {
            self.entries[i].gone = true;
            self.trim();
        }
        result
    }

    /// The future of `ticket` is dropped: its work, if not posted, is
    /// dropped with it; a posted request is returned to be detached.
    pub(crate) fn cancel(&mut self, ticket: u64) -> Option<Seq> {
        let i = (ticket - self.base) as usize;
        let entry = &mut self.entries[i];
        entry.gone = true;
        entry.work = None;
        let posted = match entry.result.take() {
            Some(Ok(seq)) => Some(seq),
            _ => None,
        };
        self.trim();
        posted
    }

    /// Drops finished entries from the front.
    fn trim(&mut self) {
        while let Some(front) = self.entries.front() {
            if !front.gone || front.work.is_some() || front.result.is_some() {
                break;
            }
            self.entries.pop_front();
            self.base += 1;
        }
    }
}

/// Whether `work` can be posted now without waiting.
pub(crate) fn fits<T: Transport, N: Nic>(qp: &QueuePair<T, N>, work: &Work<N>) -> bool {
    match work {
        Work::One(..) | Work::Gather(..) => {
            qp.send_ring().can_post() || qp.send_ring().is_clogged()
        }
        Work::Batch(ops) => {
            let ring = qp.send_ring();
            let want = ops.len().min(ring.max_wqes() as usize) as u32;
            (ring.can_post() && ring.depth() - ring.in_flight_wqes() >= want) || ring.is_clogged()
        }
        Work::Recv(_) => qp.recv_ring().can_post() || qp.recv_ring().is_clogged(),
    }
}

/// Posts `work` now (and rings the doorbell).
pub(crate) fn post<T: Transport, N: Nic>(qp: &mut QueuePair<T, N>, work: Work<N>) -> Posted<N> {
    match work {
        Work::One(op, part) => {
            let mut req = qp.request();
            if let Err(e) = req.push(op, part) {
                return Err((e.error, Leases::from(e.buf)));
            }
            req.submit(true).expect("a WQE was added")
        }
        Work::Gather(op, parts) => {
            let mut req = qp.request();
            if let Err(e) = req.push_gather(op, parts.into_iter().flatten()) {
                return Err((e, Leases::default()));
            }
            req.submit(true).expect("a WQE was added")
        }
        Work::Batch(ops) => {
            let mut req = qp.request();
            let mut error = None;
            for (op, part) in ops {
                if error.is_none() {
                    if let Err(e) = req.push(op, part) {
                        error = Some(e.error);
                    }
                }
            }
            match error {
                Some(error) => {
                    // What was added before the error goes out, owned by
                    // nobody.
                    let _ = req.submit_detached();
                    Err((error, Leases::default()))
                }
                None => req
                    .submit(true)
                    .unwrap_or(Err((Error::Invalid("an empty batch"), Leases::default()))),
            }
        }
        Work::Recv(part) => qp
            .post_recv(part, false)
            .map_err(|e| (e.error, Leases::from(e.buf))),
    }
}
