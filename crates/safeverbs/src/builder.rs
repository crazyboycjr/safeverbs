//! Connection setup in typestate: a [`QueuePairBuilder`] configures a QP,
//! [`QueuePairBuilder::build_on`] creates it on a completion queue (or
//! [`QueuePairBuilder::build`] on one of its own) as a [`PreparedQp`]
//! whose [`Endpoint`] goes to the peer, and [`PreparedQp::handshake`] with
//! the peer's endpoint connects it into a [`LocalQp`](crate::LocalQp) or an
//! [`AsyncQp`](crate::AsyncQp), by the kind of the CQ. Only connected QPs
//! can post, and settings that only apply to RC exist only on RC builders.

use core::marker::PhantomData;

use safeverbs_core::{
    Access, ConnectAttr, Endpoint, Engine, Nic, QpCaps, QpKey, RC, Result, Transport, UC,
};

use crate::cq::{AsyncCq, Cq};
use crate::{Pd, Verbs};

/// A builder of RC QPs.
pub type RcQueuePairBuilder<N = Verbs> = QueuePairBuilder<RC, N>;
/// A builder of UC QPs.
pub type UcQueuePairBuilder<N = Verbs> = QueuePairBuilder<UC, N>;

/// Configures a QP before it is created. See [RDMAmojo] for what each
/// setting does.
///
/// [RDMAmojo]: http://www.rdmamojo.com/2013/01/12/ibv_modify_qp/
#[derive(Clone)]
pub struct QueuePairBuilder<T: Transport, N: Nic = Verbs> {
    pd: Pd<N>,
    caps: QpCaps,
    max_batch: Option<u32>,
    port: u8,
    gid_index: Option<Option<u8>>,
    access: Access,
    path_mtu: Option<u32>,
    rq_psn: u32,
    sq_psn: u32,
    traffic_class: u8,
    timeout: u8,
    retry_count: u8,
    rnr_retry: u8,
    min_rnr_timer: u8,
    max_rd_atomic: Option<u8>,
    max_dest_rd_atomic: Option<u8>,
    _t: PhantomData<T>,
}

impl<T: Transport, N: Nic> QueuePairBuilder<T, N> {
    /// A builder of QPs in `pd` with the defaults: 256 WQEs in each queue,
    /// one scatter/gather element per WQE, batches of up to half the send
    /// queue, port 1 addressed by its default GID on RoCE, the path MTU of
    /// the port, and peers allowed to WRITE, READ and run atomics (on
    /// memory they were given a remote key for).
    pub fn new(pd: &Pd<N>) -> Self {
        let mut access = Access::LOCAL_WRITE | Access::REMOTE_WRITE | Access::REMOTE_READ;
        if T::QP_TYPE == safeverbs_core::nic::QpType::Rc {
            access = access | Access::REMOTE_ATOMIC;
        }
        QueuePairBuilder {
            pd: pd.clone(),
            caps: QpCaps {
                max_send_wr: 256,
                max_recv_wr: 256,
                max_send_sge: 1,
                max_recv_sge: 1,
                max_inline_data: 0,
            },
            max_batch: None,
            port: 1,
            gid_index: None,
            access,
            path_mtu: None,
            rq_psn: 0,
            sq_psn: 0,
            traffic_class: 0,
            timeout: 14,
            retry_count: 7,
            rnr_retry: 7,
            min_rnr_timer: 12,
            max_rd_atomic: None,
            max_dest_rd_atomic: None,
            _t: PhantomData,
        }
    }

    /// The sizes of the QP's queues.
    pub fn set_caps(&mut self, caps: QpCaps) -> &mut Self {
        self.caps = caps;
        self
    }

    /// WQEs in the send queue (`max_send_wr`).
    pub fn set_send_depth(&mut self, depth: u32) -> &mut Self {
        self.caps.max_send_wr = depth;
        self
    }

    /// WQEs in the receive queue (`max_recv_wr`).
    pub fn set_recv_depth(&mut self, depth: u32) -> &mut Self {
        self.caps.max_recv_wr = depth;
        self
    }

    /// Scatter/gather elements of a send WQE (`max_send_sge`).
    pub fn set_max_send_sge(&mut self, n: u32) -> &mut Self {
        self.caps.max_send_sge = n;
        self
    }

    /// Scatter/gather elements of a receive WQE (`max_recv_sge`).
    pub fn set_max_recv_sge(&mut self, n: u32) -> &mut Self {
        self.caps.max_recv_sge = n;
        self
    }

    /// The most WQEs of one request (a batch); longer batches are split.
    /// The ring keeps a signaled WQE at least every `send depth - max
    /// batch` WQEs.
    pub fn set_max_batch(&mut self, n: u32) -> &mut Self {
        self.max_batch = Some(n);
        self
    }

    /// The port.
    pub fn set_port(&mut self, port: u8) -> &mut Self {
        self.port = port;
        self
    }

    /// The source GID index (RoCE); see `show_gids` or
    /// [`Context::gids`](safeverbs_core::Context::gids).
    pub fn set_gid_index(&mut self, gid_index: u8) -> &mut Self {
        self.gid_index = Some(Some(gid_index));
        self
    }

    /// What the peer may do through this QP (`IBV_QP_ACCESS_FLAGS`).
    pub fn set_access(&mut self, access: Access) -> &mut Self {
        self.access = access;
        self
    }

    /// The path MTU (`enum ibv_mtu`, 1 = 256 bytes ... 5 = 4096 bytes).
    pub fn set_path_mtu(&mut self, path_mtu: u32) -> &mut Self {
        assert!((1..=5).contains(&path_mtu), "path MTU 1..=5");
        self.path_mtu = Some(path_mtu);
        self
    }

    /// The receive queue's starting packet sequence number.
    pub fn set_rq_psn(&mut self, psn: u32) -> &mut Self {
        self.rq_psn = psn;
        self
    }

    /// The send queue's starting packet sequence number.
    pub fn set_sq_psn(&mut self, psn: u32) -> &mut Self {
        self.sq_psn = psn;
        self
    }

    /// The traffic class with a GRH (a system setting may override it).
    pub fn set_traffic_class(&mut self, traffic_class: u8) -> &mut Self {
        self.traffic_class = traffic_class;
        self
    }

    /// Creates the QP on a completion queue of its own for futures, which holds exactly the
    /// QP's queues, with the queues' sizes checked against the device.
    pub fn build(&self) -> Result<PreparedQp<T, AsyncCq<N>>> {
        let cq_size = self.caps.max_send_wr.saturating_add(self.caps.max_recv_wr);
        let cq = AsyncCq::new(self.pd.context(), cq_size)?;
        self.build_on(&cq)
    }

    /// Creates the QP on `cq`, attached to it, with the queues' sizes checked against the device.
    /// Fails with [`Error::CqFull`](crate::Error::CqFull) if the CQ has no room left
    /// for the QP's queues, and with [`Error::Invalid`](crate::Error::Invalid) if the CQ is of
    /// another device.
    pub fn build_on<C: Cq<Nic = N>>(&self, cq: &C) -> Result<PreparedQp<T, C>> {
        let (key, endpoint, attr) = cq.with_engine(|engine| self.build_in(engine))?;
        cq.adopt(key);
        Ok(PreparedQp {
            cq: Some(cq.clone()),
            key,
            attr,
            endpoint,
        })
    }

    /// Creates the QP on an engine, with the queues' sizes checked against the device; returns its
    /// key, how the peer reaches it, and the attributes to connect it with
    /// ([`Engine::connect`]) once the peer's endpoint is known. Fails as
    /// [`QueuePairBuilder::build_on`] does.
    pub fn build_in<C>(&self, w: &mut Engine<C, N>) -> Result<(QpKey<T>, Endpoint, ConnectAttr)> {
        let ctx = self.pd.context();
        let port = ctx.port(self.port)?;
        let gid_index = match self.gid_index {
            Some(g) => g,
            None => ctx.default_gid_index(self.port)?,
        };
        let max_batch = self.max_batch.unwrap_or((self.caps.max_send_wr / 2).max(1));
        let key = w.create_qp::<T>(self.pd.as_raw(), self.caps, max_batch)?;
        let endpoint = match w.endpoint(key, self.port, gid_index) {
            Ok(e) => e,
            Err(e) => {
                // Nothing was posted: nothing to flush.
                let _ = w.remove_qp(key);
                return Err(e);
            }
        };
        let rd = ctx.nic().caps().max_qp_rd_atom.clamp(1, 16) as u8;
        let attr = ConnectAttr {
            port: self.port,
            access: self.access,
            path_mtu: self.path_mtu.unwrap_or(port.active_mtu),
            rq_psn: self.rq_psn,
            sq_psn: self.sq_psn,
            gid_index,
            traffic_class: self.traffic_class,
            timeout: self.timeout,
            retry_count: self.retry_count,
            rnr_retry: self.rnr_retry,
            min_rnr_timer: self.min_rnr_timer,
            max_rd_atomic: self.max_rd_atomic.unwrap_or(rd),
            max_dest_rd_atomic: self.max_dest_rd_atomic.unwrap_or(rd),
        };
        Ok((key, endpoint, attr))
    }
}

impl<N: Nic> QueuePairBuilder<RC, N> {
    /// The local ACK timeout, 4.096 µs × 2^`timeout` (0 = infinite).
    pub fn set_timeout(&mut self, timeout: u8) -> &mut Self {
        self.timeout = timeout;
        self
    }

    /// Transport retries before an error (at most 7).
    pub fn set_retry_count(&mut self, count: u8) -> &mut Self {
        assert!(count <= 7, "retry count 0..=7");
        self.retry_count = count;
        self
    }

    /// Receiver-not-ready retries before an error (7 = infinite).
    pub fn set_rnr_retry(&mut self, n: u8) -> &mut Self {
        assert!(n <= 7, "RNR retry 0..=7");
        self.rnr_retry = n;
        self
    }

    /// The receiver-not-ready NAK timer this side asks peers to wait.
    pub fn set_min_rnr_timer(&mut self, timer: u8) -> &mut Self {
        self.min_rnr_timer = timer;
        self
    }

    /// Outstanding RDMA READs and atomics this QP may initiate.
    pub fn set_max_rd_atomic(&mut self, n: u8) -> &mut Self {
        self.max_rd_atomic = Some(n);
        self
    }

    /// Outstanding RDMA READs and atomics the peer may initiate here.
    pub fn set_max_dest_rd_atomic(&mut self, n: u8) -> &mut Self {
        self.max_dest_rd_atomic = Some(n);
        self
    }
}

/// A created QP, not connected yet: send its [`PreparedQp::endpoint`] to
/// the peer and [`PreparedQp::handshake`] with the peer's.
///
/// Dropping it removes the QP from its CQ.
pub struct PreparedQp<T: Transport, C: Cq> {
    /// The CQ; `None` once handed on.
    cq: Option<C>,
    key: QpKey<T>,
    attr: ConnectAttr,
    endpoint: Endpoint,
}

impl<T: Transport, C: Cq> Drop for PreparedQp<T, C> {
    fn drop(&mut self) {
        if let Some(cq) = self.cq.take() {
            cq.remove(self.key);
        }
    }
}

impl<T: Transport, C: Cq> PreparedQp<T, C> {
    /// How the peer reaches this QP.
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    /// Connects to the peer at `remote` (INIT, RTR, RTS), which gives the handle that posts.
    pub fn handshake(mut self, remote: Endpoint) -> Result<C::Qp<T>> {
        let key = self.key;
        let attr = self.attr;
        self.cq
            .as_ref()
            .expect("a QP's completion queue")
            .with_engine(|engine| engine.connect(key, &attr, &remote))?;
        let cq = self.cq.take().expect("a QP's completion queue");
        Ok(cq.into_qp(key, self.endpoint))
    }
}
