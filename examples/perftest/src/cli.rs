//! Command-line arguments.

use std::sync::OnceLock;

use clap::Parser;

/// Perftest for SafeVerbs.
#[derive(Parser, Debug)]
#[command(about = "RDMA WRITE bandwidth test for SafeVerbs.")]
pub struct Args {
    /// The host to connect to, an IP address or a name. Without it the
    /// binary runs as the server and listens on 0.0.0.0.
    pub host: Option<String>,

    /// The TCP port to use.
    #[arg(short, long, default_value = "5000")]
    pub port: u16,

    /// The RDMA device to use (default: the first).
    #[arg(short, long)]
    pub device: Option<String>,

    /// Send queue depth (max_send_wr).
    #[arg(short, long, default_value = "128")]
    pub tx_depth: u32,

    /// Number of measured iterations.
    #[arg(short, long, default_value = "5000")]
    pub num_iters: usize,

    /// Number of warmup iterations.
    #[arg(short, long, default_value = "100")]
    pub warmup: usize,

    /// Message size in bytes.
    #[arg(short, long, default_value = "65536")]
    pub size: usize,

    /// Connection type, RC or UC.
    #[arg(short, long, default_value = "RC")]
    pub connection: String,

    /// The GID index (perftest's -x); default: the first RoCE v2 GID with an
    /// IPv4 address.
    #[arg(short = 'x', long)]
    pub gid_index: Option<u8>,

    /// Traffic class with a GRH.
    #[arg(long, default_value = "0")]
    pub traffic_class: u8,
}

/// The arguments, parsed once.
pub fn get_args() -> &'static Args {
    static ARGS: OnceLock<Args> = OnceLock::new();
    ARGS.get_or_init(|| {
        let args = Args::parse();
        println!("args: {args:#?}");
        args
    })
}
