use futures::FutureExt;
use safeverbs::{MemoryRegion, ProtectionDomain, QpInitAttr, QueuePairBuilder, UC};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

mod config;

fn handle_new_client(
    mut stream: TcpStream,
    pd: ProtectionDomain,
    qp_init_attr: QpInitAttr<UC>,
) -> Result<(), Box<dyn std::error::Error>> {
    let prepared_qp = QueuePairBuilder::new(pd.clone(), qp_init_attr.clone()).build()?;
    let endpoint = prepared_qp.endpoint();
    println!("Server endpoint: {:?}", endpoint);

    let remote = config::exchange_endpoint(&mut stream, endpoint)?;
    println!("Client endpoint: {:?}", remote);

    let qp = prepared_qp.handshake(remote)?;
    println!("QP handshake finished.");

    let mr: MemoryRegion<u8> = pd.allocate(
        1024,
        safeverbs::ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE,
    )?;
    let ms = mr.get_readwrite(..).unwrap();
    let wr = qp.post_recv(&ms, 0)?;

    // synchronization
    let mut buf = [0x1u8; 1];
    stream.write_all(&buf)?;
    stream.read_exact(&mut buf)?;

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
    println!("{:?}", std::str::from_utf8(&ms.freeze()[..wc_len]).unwrap());
    Ok(())
}

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

    let listener = TcpListener::bind(("0.0.0.0", config::SERVER_PORT))?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_new_client(stream, pd.clone(), qp_init_attr.clone())?;
            }
            Err(err) => eprintln!("{}", err),
        }
    }
    Ok(())
}
