// The takeaway is that in C, there is no existing construct representing an ongoing operation.
// All the resource (which may have complex dependency) must be explicitly managed.
// In Rust, safeverbs naturally maps an on going post_send or post_recv operation to a Future,
// and maintains resource dependencies with reference counting, without sacrificing performance.

fn server_main_loop_impl_1() {
    let mut completions = Vec::with_capacity(32);
    unsafe { completions.set_len(completions.capacity()) };
    let mut wr = wr.fuse();
    let mut wc_len = 0;

    loop {
        futures::select! {
            wc = wr => {
                println!("{:?}", wc);
                wc_len = wc.len();
            }
            complete => break,
            default => {
                qp.recv_cq().poll(&mut completions)?;
            }
        };
    }
    println!("{}", std::str::from_utf8(&ms.freeze()[..wc_len])?);
}

fn server_main_loop_impl_2() {
    let mut completions = Vec::with_capacity(32);
    unsafe { completions.set_len(completions.capacity()) };
    let mut wr = wr.fuse();
    let mut wc_len = 0;

    use futures::task::{LocalSpawnExt, SpawnExt};
    let mut pool = futures::executor::LocalPool::new();
    pool.spawner().spawn(async move {
        loop {
            qp.recv_cq().poll(&mut completions).unwrap();
            yield_now().await;
        }
    })?;
    let handle = pool.spawner().spawn_local_with_handle(async move {
        let wc = wr.await;
        println!(
            "{:?}",
            std::str::from_utf8(&ms.freeze()[..wc.len()]).unwrap()
        );
    })?;
    pool.run_until(handle);
}

fn server_main_loop_in_uchan() {
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

    loop {
        // refill (post new send requests)
        while scnt < rcnt + tx_depth && scnt < args.num_iters + args.warmup {
            if (scnt + 1) % tx_depth == 0 {
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

        // check completions
        futures::select! {
            wc = wr_set.next() => {
                // println!("{:?}", wc);
                let _wc = wc.unwrap_or_else(|| panic!("wc failed: {:?}", wc));
                rcnt += tx_depth;
                nbytes += ms.as_ref().len() * tx_depth;
            }
            complete => break,
            default => {
                cq.poll(&mut completions)?;

                if rcnt >= args.num_iters + args.warmup {
                    break;
                }
                // print statistics
                let last_dura = last_ts.elapsed();
                if last_dura > std::time::Duration::from_secs(1) {
                    let rps = (rcnt - last_rcnt) as f64 / last_dura.as_secs_f64();
                    let bw_gbps = 8e-9 * (nbytes - last_nbytes) as f64 / last_dura.as_secs_f64();
                    if rcnt > args.warmup {
                        println!("{} rps, {} Gb/s", rps, bw_gbps);
                    }
                    last_ts = Instant::now();
                    last_rcnt = rcnt;
                    last_nbytes = nbytes;
                }
            }
        };
    }
}