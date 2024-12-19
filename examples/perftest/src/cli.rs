use clap::Parser;

#[derive(Parser, Debug)]
#[command(about = "Perftest for SafeVerbs.")]
pub struct Args {
    /// The host address to connect, can be an IP address or domain name.
    /// If not specified, the binary runs as a server that listens on 0.0.0.0.
    pub host: Option<String>,

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

    /// Connection type, RC/UC.
    #[arg(short, long, default_value = "RC")]
    pub connection: String,

    /// Uses GID with GID index. Equivalent to -x flags in perftest.
    #[arg(short = 'x', long)]
    pub gid_index: Option<u8>,

    /// Traffic class if GRH is in use
    #[arg(long, default_value = "0")]
    pub traffic_class: u8,
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
