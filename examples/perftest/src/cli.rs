use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use minstant::Instant;
use safeverbs::{
    Context, MemoryRegion, QpCapability, QpInitAttr, QueuePairBuilder, QueuePairEndpoint, UC,
};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

#[derive(Parser, Debug)]
#[command(about = "Perftest for SafeVerbs.")]
pub struct Args {
    /// The address to connect, can be an IP address or domain name.
    /// If not specified, the binary runs as a server that listens on 0.0.0.0.
    #[arg(short, long)]
    pub connect: Option<String>,

    /// The port number to use.
    #[arg(short, long, default_value = "5000")]
    pub port: u16,

    /// The IB device to use.
    #[arg(short, long, default_value = "mlx5_0")]
    pub device: String,

    /// Send queue depth (max_send_wr).
    #[arg(short, long, default_value = "128")]
    pub tx_depth: u32,

    /// Total number of iterations.
    #[arg(short, long, default_value = "5000")]
    pub num_iters: usize,

    /// Number of warmup iterations.
    #[arg(short, long, default_value = "100")]
    pub warmup: usize,

    /// Message size.
    #[arg(short, long, default_value = "65536")]
    pub size: usize,
    // /// Number of QPs in each thread.
    // #[arg(long, default_value = "1")]
    // pub num_qp: usize,

    // /// Number of threads. num_threads * num_qp is mapped to to num_server_qps
    // /// in a round-robin way.
    // #[arg(long, default_value = "1")]
    // pub num_threads: usize,

    // /// Number of server QPs.
    // #[arg(long, default_value = "1")]
    // pub num_server_qps: usize,
}

#[inline]
pub fn get_args() -> &'static Args {
    use std::sync::OnceLock;
    static ARGS: OnceLock<Args> = OnceLock::new();

    let args: &Args = ARGS.get_or_init(|| {
        let args = Args::parse();
        println!("args: {:#?}", args);
        args
    });

    args
}