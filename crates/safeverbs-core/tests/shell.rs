//! What an engine does around its data path: the order its resources go in, what a program's panic
//! leaves, and keys that name no QP.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use safeverbs_core::model::Model;
use safeverbs_core::nic::{DeviceCaps, PortInfo, QpType};
use safeverbs_core::{
    Access, ConnectAttr, Context, Endpoint, Engine, GidEntry, Nic, QpCaps, QpKey, RC, Result, WrOp,
};

type Log = Arc<Mutex<Vec<&'static str>>>;

/// A device that says in what order its device and domains go.
struct Logged {
    model: Model,
    log: Log,
}

impl Drop for Logged {
    fn drop(&mut self) {
        self.log.lock().unwrap().push("device");
    }
}

/// A thread domain that says when it goes.
struct LoggedTd {
    log: Log,
}

impl Drop for LoggedTd {
    fn drop(&mut self) {
        self.log.lock().unwrap().push("thread domain");
    }
}

/// A parent domain that says when it goes.
struct LoggedDomain {
    log: Log,
}

impl Drop for LoggedDomain {
    fn drop(&mut self) {
        self.log.lock().unwrap().push("parent domain");
    }
}

// SAFETY: what the simulated NIC does, and it logs the drops.
unsafe impl Nic for Logged {
    type Pd = <Model as Nic>::Pd;
    type Mr = <Model as Nic>::Mr;
    type Cq = <Model as Nic>::Cq;
    type Qp = <Model as Nic>::Qp;
    type Td = LoggedTd;
    type Domain = LoggedDomain;

    fn name(&self) -> &str {
        self.model.name()
    }

    fn caps(&self) -> DeviceCaps {
        self.model.caps()
    }

    fn port(&self, port: u8) -> Result<PortInfo> {
        self.model.port(port)
    }

    fn gids(&self, port: u8) -> Vec<GidEntry> {
        self.model.gids(port)
    }

    unsafe fn alloc_pd(&self) -> Result<Self::Pd> {
        // SAFETY: as its caller promises.
        unsafe { self.model.alloc_pd() }
    }

    unsafe fn reg_mr(
        &self,
        pd: &Self::Pd,
        addr: *mut u8,
        len: usize,
        access: Access,
    ) -> Result<Self::Mr> {
        // SAFETY: as its caller promises.
        unsafe { self.model.reg_mr(pd, addr, len, access) }
    }

    fn mr_keys(mr: &Self::Mr) -> (u32, u32) {
        Model::mr_keys(mr)
    }

    fn close_mr(mr: &mut Self::Mr) -> Result<()> {
        Model::close_mr(mr)
    }

    unsafe fn create_cq(&self, entries: u32, single_threaded: bool) -> Result<Self::Cq> {
        // SAFETY: as its caller promises.
        unsafe { self.model.create_cq(entries, single_threaded) }
    }

    unsafe fn alloc_td(&self) -> Option<LoggedTd> {
        Some(LoggedTd {
            log: self.log.clone(),
        })
    }

    unsafe fn alloc_domain(&self, _pd: &Self::Pd, _td: &LoggedTd) -> Option<LoggedDomain> {
        Some(LoggedDomain {
            log: self.log.clone(),
        })
    }

    unsafe fn create_qp(
        &self,
        pd: &Self::Pd,
        _domain: Option<&LoggedDomain>,
        send_cq: &Self::Cq,
        recv_cq: &Self::Cq,
        qp_type: QpType,
        caps: &QpCaps,
    ) -> Result<Self::Qp> {
        // SAFETY: as its caller promises.
        unsafe {
            self.model
                .create_qp(pd, None, send_cq, recv_cq, qp_type, caps)
        }
    }
}

fn caps(sges: u32) -> QpCaps {
    QpCaps {
        max_send_wr: 8,
        max_recv_wr: 8,
        max_send_sge: sges,
        max_recv_sge: sges,
        max_inline_data: 0,
    }
}

fn attr() -> ConnectAttr {
    ConnectAttr {
        port: 1,
        access: Access::LOCAL_WRITE,
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
    }
}

/// With the last handles to a device in an engine, its thread domain still goes before the device:
/// `ibv_dealloc_td` reads the context.
#[test]
fn an_engines_domains_go_before_its_device() {
    let log = Log::default();
    let ctx = Context::new(Logged {
        model: Model::new(61),
        log: log.clone(),
    });
    let pd = ctx.alloc_pd().unwrap();
    let mut engine: Engine<(), Logged> = Engine::new(&ctx, 64).unwrap();
    let _qp = engine.create_qp::<RC>(&pd, caps(1), 1).unwrap();
    drop(pd);
    drop(ctx);
    assert!(log.lock().unwrap().is_empty());
    drop(engine);
    assert_eq!(
        *log.lock().unwrap(),
        ["parent domain", "thread domain", "device"]
    );
}

/// An engine with no QP, the device's last handle in it: the thread domain goes first too.
#[test]
fn an_engine_without_qps_lets_go_of_its_domain_first() {
    let log = Log::default();
    let ctx = Context::new(Logged {
        model: Model::new(62),
        log: log.clone(),
    });
    let engine: Engine<(), Logged> = Engine::new(&ctx, 64).unwrap();
    drop(ctx);
    drop(engine);
    assert_eq!(*log.lock().unwrap(), ["thread domain", "device"]);
}

fn connected_pair(
    engine: &mut Engine<(), Model>,
    pd: &safeverbs_core::Pd<Model>,
    sges: u32,
) -> (QpKey<RC>, QpKey<RC>) {
    let a = engine.create_qp::<RC>(pd, caps(sges), 4).unwrap();
    let b = engine.create_qp::<RC>(pd, caps(sges), 4).unwrap();
    let end = |qp_num| Endpoint {
        qp_num,
        lid: 1,
        gid: None,
    };
    let (an, bn) = (engine.qp_num(a), engine.qp_num(b));
    engine.connect(a, &attr(), &end(bn)).unwrap();
    engine.connect(b, &attr(), &end(an)).unwrap();
    (a, b)
}

/// What an iterator gave before it panicked is not part of the next gather list: on a QP of one
/// scatter/gather element, the next WQE of one part would not fit.
#[test]
fn a_gather_list_starts_empty_after_an_iterator_panicked() {
    let pd = Context::new(Model::new(63)).alloc_pd().unwrap();
    let mut engine: Engine<(), Model> = Engine::new(pd.context(), 64).unwrap();
    let (a, _b) = connected_pair(&mut engine, &pd, 1);
    let buf = pd.alloc::<u8>(64).unwrap().freeze();
    let first = buf.slice(..8).into_part();
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let mut req = engine.request(a);
        let parts = std::iter::once(first).chain(std::iter::from_fn(|| panic!("the program's")));
        let _ = req.push_gather(WrOp::Send, parts);
    }));
    assert!(panicked.is_err());
    let mut req = engine.request(a);
    req.push_gather(WrOp::Send, [buf.slice(..8).into_part()])
        .unwrap();
    assert!(req.submit(true, ()).is_ok());
    assert_eq!(engine.send_ring(a).in_flight_wqes(), 1);
}

/// A program's closure that panics in a poll leaves an engine like any other: the requests that
/// completed come out, and it drains as it goes.
#[test]
fn an_engine_goes_on_after_a_closure_panicked_in_a_poll() {
    let model = Model::new(64);
    let pd = Context::new(model.clone()).alloc_pd().unwrap();
    let mut engine: Engine<u32, Model> = Engine::new(pd.context(), 64).unwrap();
    let a = engine.create_qp::<RC>(&pd, caps(1), 4).unwrap();
    let b = engine.create_qp::<RC>(&pd, caps(1), 4).unwrap();
    let end = |qp_num| Endpoint {
        qp_num,
        lid: 1,
        gid: None,
    };
    let (an, bn) = (engine.qp_num(a), engine.qp_num(b));
    engine.connect(a, &attr(), &end(bn)).unwrap();
    engine.connect(b, &attr(), &end(an)).unwrap();
    let frozen = pd.alloc::<u8>(8).unwrap().freeze();
    for wr_id in 0..2 {
        let buf = pd.alloc::<u8>(8).unwrap();
        engine.post_recv(b, buf.into_part(), 10 + wr_id).unwrap();
        engine
            .post_recv(a, pd.alloc::<u8>(8).unwrap().into_part(), 20 + wr_id)
            .unwrap();
        engine.send(a, frozen.clone(), wr_id).unwrap();
    }
    model.run();
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        engine.poll(32, |_, _| panic!("the program's"));
    }));
    assert!(panicked.is_err());
    let mut got = 0;
    while got < 3 {
        model.run();
        got += engine.poll(32, |_, _| {});
    }
    engine.remove_qp(a).unwrap();
    engine.remove_qp(b).unwrap();
    drop(engine);
    drop(pd);
    assert!(frozen.try_thaw().is_ok(), "everything it held is back");
    assert_eq!(model.registered(), 0);
}

/// A key that names no QP is the program's error, and the engine panics, as it did from under its
/// hardware side.
#[test]
fn a_key_that_names_no_qp_panics() {
    let pd = Context::new(Model::new(65)).alloc_pd().unwrap();
    let mut engine: Engine<(), Model> = Engine::new(pd.context(), 64).unwrap();
    let (a, b) = connected_pair(&mut engine, &pd, 1);
    engine.remove_qp(a).unwrap();
    let buf = pd.alloc::<u8>(8).unwrap().freeze();
    let mut open = engine.open_request(b);
    assert!(catch_unwind(AssertUnwindSafe(|| engine.commit(a))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| engine.to_error(a))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| engine.make_send_room(a))).is_err());
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let part = buf.clone().into_part();
            let _ = engine.push_open(a, &mut open, WrOp::Send, part);
        }))
        .is_err()
    );
    assert!(catch_unwind(AssertUnwindSafe(|| engine.qp_num(a))).is_err());
    // It goes on: the panic came before anything changed.
    assert_eq!(engine.commit(b), Ok(()));
    engine.give_back(b, &mut open);
    assert_eq!(engine.remove_qp(b), Ok(()));
}
