//! Backpressure: work that does not fit in a full queue waits here, in
//! order, and is posted by whichever future polls the QP next once there is
//! room. A future of waiting work holds a ticket; when its work is posted,
//! the ticket's result (the request's sequence number, or an error with the
//! buffers) waits for it here.

use std::collections::VecDeque;

use safeverbs_core::engine::OpenRequest;
use safeverbs_core::{Engine, Error, Leases, Nic, Part, QpKey, Seq, Transport, WrOp};

/// Work for one request, while it waits or takes the general path.
pub(crate) enum Work<N: Nic> {
    /// One send-queue WQE.
    One(WrOp, Part<N>),
    /// One send-queue WQE with a gather list.
    Gather(WrOp, Vec<Part<N>>),
    /// A batch of send-queue WQEs, collected.
    Open(OpenRequest<N>),
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

    /// The entries it holds.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
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
    pub(crate) fn drain<T: Transport>(&mut self, w: &mut Engine<(), N>, qp: QpKey<T>) {
        let mut i = 0;
        while i < self.entries.len() {
            if self.entries[i].work.is_some() {
                let fits = match &self.entries[i].work {
                    Some(work) => fits(w, qp, work),
                    None => true,
                };
                if !fits {
                    break;
                }
                // A dropped future's work was dropped with it, so whoever
                // waits here still wants the result.
                let work = self.entries[i].work.take().expect("work");
                self.entries[i].result = Some(post(w, qp, work));
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

/// Whether `work` can be posted now without waiting. Work waits both for
/// room in the queue and for a free slot: when every slot holds a result
/// that its future has not taken yet, it waits for those futures to be
/// polled.
pub(crate) fn fits<T: Transport, N: Nic>(w: &Engine<(), N>, qp: QpKey<T>, work: &Work<N>) -> bool {
    match work {
        Work::One(..) | Work::Gather(..) => w.send_ring(qp).can_post(),
        Work::Open(open) => w.open_fits(qp, open),
        Work::Recv(_) => w.recv_ring(qp).can_post(),
    }
}

/// Posts `work` now on QP `qp` of `w` (and rings the doorbell).
pub(crate) fn post<T: Transport, N: Nic>(
    w: &mut Engine<(), N>,
    qp: QpKey<T>,
    work: Work<N>,
) -> Posted<N> {
    match work {
        Work::One(op, part) => {
            let mut req = w.request(qp);
            if let Err(e) = req.push(op, part) {
                return Err((e.error, Leases::from(e.buf)));
            }
            req.submit(true, ()).map_err(|e| (e.error, e.leases))
        }
        Work::Gather(op, parts) => {
            let mut req = w.request(qp);
            if let Err(e) = req.push_gather(op, parts) {
                return Err((e, Leases::default()));
            }
            req.submit(true, ()).map_err(|e| (e.error, e.leases))
        }
        Work::Open(mut open) => w
            .submit_open(qp, &mut open, true, ())
            .map_err(|e| (e.error, e.leases)),
        Work::Recv(part) => w.post_recv(qp, part, ()).map_err(|e| (e.error, e.leases)),
    }
}
