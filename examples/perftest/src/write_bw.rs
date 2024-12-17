use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use minstant::Instant;
use perftest::cli::get_args;
use perftest::{exchange_endpoint, find_and_open_device, qp_capability};
use safeverbs::{MemoryRegion, QpInitAttr, QueuePair, QueuePairBuilder, RC, UC};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

fn run_sender(server_addr: &str) -> anyhow::Result<()> {
    let context = find_and_open_device()?;
    let pd = context.alloc_pd()?;
    let cq = context.create_cq(128, 0)?;
    let qp_init_attr = QpInitAttr {
        qp_context: 0,
        send_cq: cq.clone(),
        recv_cq: cq.clone(),
        cap: qp_capability(),
        qp_type: UC,
        sq_sig_all: false,
    };

    let args = get_args();
    let mut stream = TcpStream::connect((server_addr, args.port))?;

    let prepared_qp = QueuePairBuilder::new(pd.clone(), qp_init_attr).build()?;
    let endpoint = prepared_qp.endpoint();
    println!("Client endpoint: {:?}", endpoint);

    let remote = exchange_endpoint(&mut stream, endpoint)?;
    println!("Server endpoint: {:?}", remote);

    let qp = prepared_qp.handshake(remote)?;
    println!("QP handshake finished.");

    // rendezvous remote memory
    let mut len_buf = [0u8; 4];
    stream.read_exact(len_buf.as_mut())?;
    let len = u32::from_be_bytes(len_buf);
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf)?;
    let remote_memory = bincode::deserialize(&buf)?;

    let mr: MemoryRegion<u8> = pd.allocate(
        args.size,
        safeverbs::ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE,
    )?;
    let mut ms = mr.get_readwrite(..).unwrap();
    ms.as_mut().fill(42);
    let ms = ms.freeze();

    let mut completions = Vec::with_capacity(32);
    unsafe { completions.set_len(completions.capacity()) };

    let mut scnt = 0;
    let mut rcnt = 0;
    let mut last_rcnt = 0;
    let mut last_ts = Instant::now();
    let mut nbytes = 0;
    let mut last_nbytes = 0;
    let mut wr_set = FuturesUnordered::new();
    let tx_depth = args.tx_depth as usize;

    while rcnt < args.num_iters + args.warmup {
        while scnt < rcnt + tx_depth && scnt < args.num_iters + args.warmup {
            if (scnt + 1) % tx_depth == 0 || scnt + 1 == args.num_iters + args.warmup {
                let wr = qp.post_write_gather(&[ms.get_opaque()], &remote_memory, scnt as _)?;
                wr_set.push(wr.fuse());
            } else {
                // SAFETY: QP and MS are not dropped so it should be okay.
                unsafe {
                    qp.post_write_gather_unsignaled(&[ms.get_opaque()], &remote_memory, scnt as _)?;
                }
            }
            scnt += 1;
        }

        futures::select! {
            wc = wr_set.next() => {
                // println!("{:?}", wc);
                let wc = wc.expect("FuturesUnordered has been drained");
                // it's not always tx_depth but it is okay because it will only affect the last iteration
                if !wc.is_valid() {
                    panic!("wc failed: {:?}", wc);
                }
                rcnt += tx_depth;
                nbytes += ms.as_ref().len() * tx_depth;
            }
            complete => break,
            default => {
                cq.poll(&mut completions)?;

                let last_dura = last_ts.elapsed();
                if last_dura > std::time::Duration::from_secs(1) {
                    let rps = (rcnt - last_rcnt) as f64 / last_dura.as_secs_f64();
                    let bw_gbps = 8e-9 * (nbytes - last_nbytes) as f64 / last_dura.as_secs_f64();
                    if rcnt > args.warmup {
                        println!("{:.2} rps, {:.2} Gb/s", rps, bw_gbps);
                    }
                    last_ts = Instant::now();
                    last_rcnt = rcnt;
                    last_nbytes = nbytes;
                }
            }
        };
    }

    Ok(())
}

fn run_receiver() -> anyhow::Result<()> {
    let context = find_and_open_device()?;
    let pd = context.alloc_pd()?;
    let cq = context.create_cq(128, 0)?;
    let qp_init_attr = QpInitAttr {
        qp_context: 0,
        send_cq: cq.clone(),
        recv_cq: cq.clone(),
        cap: qp_capability(),
        qp_type: UC,
        sq_sig_all: false,
    };

    let args = get_args();
    let listener = TcpListener::bind(("0.0.0.0", args.port))?;
    let (mut stream, _) = listener.accept()?;

    let prepared_qp = QueuePairBuilder::new(pd.clone(), qp_init_attr.clone()).build()?;
    let endpoint = prepared_qp.endpoint();
    println!("Client endpoint: {:?}", endpoint);

    let remote = exchange_endpoint(&mut stream, endpoint)?;
    println!("Server endpoint: {:?}", remote);

    let _qp = prepared_qp.handshake(remote)?;
    println!("QP handshake finished.");

    // allocate memory for RDMA remote write
    let mr: MemoryRegion<u8> = pd.allocate(
        args.size,
        safeverbs::ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | safeverbs::ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE,
    )?;
    let ms = mr.get_readwrite(..).unwrap();
    let memory_for_remote_write = ms.remote_memory();

    // rendezvous remote memory
    let buf = bincode::serialize(&memory_for_remote_write)?;
    stream.write_all(&(buf.len() as u32).to_be_bytes())?;
    stream.write_all(&buf)?;

    let mut buf = vec![0u8; 1];
    stream.read(&mut buf)?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = get_args();
    match &args.connect {
        Some(connect) => run_sender(&connect)?,
        None => run_receiver()?,
    }
    Ok(())
}
