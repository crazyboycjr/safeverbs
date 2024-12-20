use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use minstant::Instant;
use perftest::cli::get_args;
use perftest::{exchange_endpoint, find_and_open_device, qp_capability};
use safeverbs::{MemoryRegion, QpInitAttr, QueuePairBuilder, WorkRequestBatch, RC, UC};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

macro_rules! impl_run_sender {
    ($QPT: ident, $func_name: ident) => {
        fn $func_name(server_addr: &str) -> anyhow::Result<()> {
            let context = find_and_open_device()?;
            let pd = context.alloc_pd()?;
            let cq = context.create_cq(128, 0)?;
            let qp_init_attr = QpInitAttr {
                qp_context: 0,
                send_cq: cq.clone(),
                recv_cq: cq.clone(),
                cap: qp_capability(),
                qp_type: $QPT,
                sq_sig_all: false,
            };

            let args = get_args();
            let mut qp_builder = QueuePairBuilder::new(pd.clone(), qp_init_attr);
            if let Some(gid_index) = args.gid_index {
                qp_builder.set_gid_index(gid_index);
            }
            let prepared_qp = qp_builder
                .set_traffic_class(args.traffic_class)
                .build()?;
            let endpoint = prepared_qp.endpoint();
            println!("Client endpoint: {:?}", endpoint);

            let mut stream = TcpStream::connect((server_addr, args.port))?;
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
            let mut batch = WorkRequestBatch::new();
            let total_iters = args.num_iters + args.warmup;

            while scnt < rcnt + tx_depth && scnt < total_iters {
                batch.prepost_write(&[ms.get_opaque()], &remote_memory, scnt as _, safeverbs::ffi::ibv_send_flags(0));
                if (scnt + 1) % tx_depth == 0 || scnt + 1 == total_iters {
                    batch.enclose();
                    let wr = unsafe { qp.post_send_batch(&mut batch)? };
                    wr_set.push(wr.fuse());
                }
                scnt += 1;
            }

            while rcnt < total_iters {
                // while scnt < rcnt + tx_depth && scnt < total_iters {
                    // This achieves ~5.5 million ops/s.
                    // if (scnt + 1) % tx_depth == 0 || scnt + 1 == total_iters {
                    //     let wr = qp.post_write_gather(&[ms.get_opaque()], &remote_memory, scnt as _)?;
                    //     wr_set.push(wr.fuse());
                    // } else {
                    //     // SAFETY: QP and MS are not dropped so it should be okay.
                    //     unsafe {
                    //         qp.post_write_gather_unsignaled(&[ms.get_opaque()], &remote_memory, scnt as _)?;
                    //     }
                    // }
                    // Post in batch achieves ~7 million ops/s.
                    // batch.prepost_write(&[ms.get_opaque()], &remote_memory, scnt as _, safeverbs::ffi::ibv_send_flags(0));
                    // if (scnt + 1) % tx_depth == 0 || scnt + 1 == total_iters {
                    //     batch.enclose();
                    //     let wr = unsafe { qp.post_send_batch(&mut batch)? };
                    //     wr_set.push(wr.fuse());
                    // }
                    // scnt += 1;
                // }

                if scnt < rcnt + tx_depth && scnt < total_iters {
                    if scnt + tx_depth > total_iters {
                        batch.clear();
                        while scnt < rcnt + tx_depth && scnt < total_iters {
                            batch.prepost_write(&[ms.get_opaque()], &remote_memory, scnt as _, safeverbs::ffi::ibv_send_flags(0));
                            if (scnt + 1) % tx_depth == 0 || scnt + 1 == total_iters {
                                batch.enclose();
                                let wr = unsafe { qp.post_send_batch(&mut batch)? };
                                wr_set.push(wr.fuse());
                            }
                            scnt += 1;
                        }
                    } else {
                        // Avoid reconstruct the batch every time
                        // THis achieves ~12 million ops/s
                        let wr = unsafe { qp.post_send_batch(&mut batch)? };
                        wr_set.push(wr.fuse());
                        scnt += tx_depth;
                    }
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
    }
}

impl_run_sender!(RC, run_sender_for_rc);
impl_run_sender!(UC, run_sender_for_uc);

macro_rules! impl_run_receiver {
    ($QPT: ident, $func_name: ident) => {
        fn $func_name() -> anyhow::Result<()> {
            let context = find_and_open_device()?;
            let pd = context.alloc_pd()?;
            let cq = context.create_cq(128, 0)?;
            let qp_init_attr = QpInitAttr {
                qp_context: 0,
                send_cq: cq.clone(),
                recv_cq: cq.clone(),
                cap: qp_capability(),
                qp_type: $QPT,
                sq_sig_all: false,
            };

            let args = get_args();
            let listener = TcpListener::bind(("0.0.0.0", args.port))?;
            let (mut stream, _) = listener.accept()?;

            let mut qp_builder = QueuePairBuilder::new(pd.clone(), qp_init_attr);
            if let Some(gid_index) = args.gid_index {
                qp_builder.set_gid_index(gid_index);
            }
            let prepared_qp = qp_builder
                .set_access(
                    safeverbs::ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                        | safeverbs::ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE,
                )
                .set_traffic_class(args.traffic_class)
                .build()?;
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
    };
}

impl_run_receiver!(RC, run_receiver_for_rc);
impl_run_receiver!(UC, run_receiver_for_uc);

fn main() -> anyhow::Result<()> {
    let args = get_args();
    match &args.host {
        Some(host) => {
            if args.connection == "RC" {
                run_sender_for_rc(&host)?;
            } else if args.connection == "UC" {
                run_sender_for_uc(&host)?;
            } else {
                panic!("unknown connection type: {}", args.connection)
            }
        }
        None => {
            if args.connection == "RC" {
                run_receiver_for_rc()?;
            } else if args.connection == "UC" {
                run_receiver_for_uc()?;
            } else {
                panic!("unknown connection type: {}", args.connection)
            }
        }
    }
    Ok(())
}
