use futures::FutureExt;
use safeverbs::{MemoryRegion, QpInitAttr, QueuePairBuilder, UC};
use std::io::{Read, Write};
use std::net::TcpStream;

mod config;
const MSG: &str = "Hello SafeVerbs";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = config::find_and_open_device()?;
    let pd = context.alloc_pd()?;
    let cq = context.create_cq(128, 0)?;
    let qp_init_attr = QpInitAttr {
        qp_context: 0,
        send_cq: cq.clone(),
        recv_cq: cq.clone(),
        cap: config::qp_capability(),
        qp_type: UC,
        sq_sig_all: false,
    };

    let mut stream = TcpStream::connect(("127.0.0.1", config::SERVER_PORT))?;

    let prepared_qp = QueuePairBuilder::new(pd.clone(), qp_init_attr).build()?;
    let endpoint = prepared_qp.endpoint();
    println!("Client endpoint: {:?}", endpoint);

    let remote = config::exchange_endpoint(&mut stream, endpoint)?;
    println!("Server endpoint: {:?}", remote);

    let qp = prepared_qp.handshake(remote)?;
    println!("QP handshake finished.");

    // synchronization
    let mut buf = [0x1u8; 1];
    stream.write_all(&buf)?;
    stream.read_exact(&mut buf)?;

    let mr: MemoryRegion<u8> = pd.allocate(
        MSG.len(),
        safeverbs::ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE,
    )?;
    let mut ms = mr.get_readwrite(..).unwrap();
    ms.as_mut().copy_from_slice(MSG.as_bytes());
    let ms = ms.freeze();
    let wr = qp.post_send(&ms, 114)?;

    let mut wr = wr.fuse();
    let mut completions = Vec::with_capacity(32);
    unsafe { completions.set_len(completions.capacity()) };
    loop {
        futures::select! {
            wc = wr => println!("{:?}", wc),
            complete => break,
            default => {
                cq.poll(&mut completions)?;
            }
        };
    }

    Ok(())
}
