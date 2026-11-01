# Development setup

How to build safeverbs, get an RDMA device on any Linux machine with
Soft-RoCE, and run the tests.

## Prerequisites

- Rust through rustup: `rust-toolchain.toml` pins the toolchain and rustup
  installs it on first use.
- For rdma-core and the bindings: a C compiler, `make`, CMake 3.18.1 or
  newer, `pkg-config`, Python 3, libclang (for bindgen) and the libnl 3
  headers (`libnl-3.0` and `libnl-route-3.0`).

  | Distribution  | Packages                                                                              |
  | ------------- | ------------------------------------------------------------------------------------- |
  | Debian/Ubuntu | `build-essential cmake pkg-config python3 libclang-dev libnl-3-dev libnl-route-3-dev` |
  | Fedora        | `gcc make cmake pkgconf python3 clang-devel libnl3-devel`                             |
  | Arch          | `base-devel cmake python clang libnl`                                                 |

  bindgen finds libclang by itself; if `LIBCLANG_PATH` is set, it must point
  at the directory of a working `libclang.so`.

- For the Soft-RoCE device: Docker (membership in the `docker` group), and a
  kernel with the `rdma_rxe` and `ib_uverbs` modules (most distribution
  kernels have them).
- Locked memory: registering memory pins it, so `ulimit -l` must be large
  (`unlimited` is simplest), for example through `/etc/security/limits.conf`.

## Source

rdma-core is a submodule of `crates/safeverbs-sys`, pinned to a release tag:

```sh
git submodule update --init crates/safeverbs-sys/vendor/rdma-core
```

## Building

```sh
cargo build -p safeverbs-sys
cargo test -p safeverbs-sys
```

`safeverbs-sys` holds the raw FFI for libibverbs and mlx5dv. Its build script
(`crates/safeverbs-sys/build.rs`) does three things:

1. With the `vendored` feature (the default), builds rdma-core from the
   submodule with CMake, configured `IN_PLACE` (runnable from its build
   directory): only libibverbs and the `mlx5` (ConnectX) and `rxe`
   (Soft-RoCE) providers, no man pages, no pyverbs, no daemons. The result
   is in `target/<profile>/build/safeverbs-sys-*/out/build/` (`lib`,
   `include`, `etc/libibverbs.d`).
2. Runs bindgen over `infiniband/verbs.h` and `infiniband/mlx5dv.h` for
   everything named `ibv_*`, `mlx5dv_*`, `mlx5_*` and their constants.
3. Compiles C wrappers of the headers' `static inline` functions
   (`ibv_post_send`, `ibv_poll_cq`, `ibv_create_qp_ex`, `ibv_wr_*`,
   `ibv_start_poll`, ...) with bindgen's `wrap_static_fns`, so that Rust can
   call them by their names.

The CMake build directory keeps its cache between builds. After changing
the options in `build.rs`, run `cargo clean -p safeverbs-sys` so that no
stale option survives.

### Finding the vendored libraries at run time

An `IN_PLACE` libibverbs has the absolute paths of its build directory
compiled in: it reads its provider list from `build/etc/libibverbs.d` and
loads the providers from `build/lib`, so no environment variable is needed
once the program loads that libibverbs rather than the system's. The
build script gives this crate's own tests and examples a run path to
`build/lib` (`-Wl,-rpath`).

Cargo passes such linker arguments only to the package that prints them,
and passes build-script metadata only to direct dependents. So the
directory travels down the crates, each giving its own tests a run path
and handing it on (each has a `links` key for this, though only
`safeverbs-sys` links a library):

- `safeverbs-sys` prints it as `DEP_RDMA_CORE_LIB_DIR`;
- `safeverbs-core` reads that and prints `DEP_SAFEVERBS_CORE_LIB_DIR`;
- `safeverbs` reads that and prints `DEP_SAFEVERBS_LIB_DIR`.

A crate depending on `safeverbs` (the examples, the demos, an
application) gives its binaries the run path in its `build.rs`:

```rust
fn main() {
    if let Ok(dir) = std::env::var("DEP_SAFEVERBS_LIB_DIR") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
    }
}
```

Without it, a binary would load the system's libibverbs, if there is one,
with the system's providers.

### System rdma-core (MLNX_OFED, DOCA-OFED, distribution packages)

```sh
cargo build -p safeverbs-sys --no-default-features
```

Without `vendored`, the build script finds `libibverbs` and `libmlx5`
through pkg-config, generates the bindings from the installed headers and
links the installed libraries, which then load the installed providers.
Use this on hosts with MLNX_OFED or DOCA-OFED, which replace the
distribution's rdma-core with their own build, so that the library, its
providers and the installed driver stack agree. If pkg-config does not find
them, add the directory with their `.pc` files to `PKG_CONFIG_PATH`.

The bindings follow the installed headers, so an older rdma-core lacks
newer verbs. Old releases also have bugs: rdma-core 41's rxe provider
returns the same completions again from `ibv_start_poll` and
`ibv_next_poll` (fixed upstream by "rxe: fix completion queue consumer index
overrun"), which the probe below reports.

## Soft-RoCE: an RDMA device anywhere

`scripts/rxe.sh` creates an rxe device on a private dummy interface:

```sh
scripts/rxe.sh up       # load rdma_rxe, create sv-rxe0 and the device svrxe0
scripts/rxe.sh status   # the device, its interface, its uverbs node, its GIDs
scripts/rxe.sh down     # remove the device and the interface
```

Root comes from a privileged container (`--privileged --net=host` with
`/dev` and `/lib/modules` mounted), so `sudo` is not needed. `up` loads
`ib_uverbs` and `rdma_rxe`, creates the dummy interface `sv-rxe0` with
`10.233.0.1/24` (refused if a route of the host other than the default
covers that address), runs `rdma link add svrxe0 type rxe netdev sv-rxe0`
and makes the device's `/dev/infiniband/uverbs*` node usable by everyone.
`SV_RXE_NETDEV`, `SV_RXE_DEV` and `SV_RXE_ADDR` change the names and the
address. `down` leaves the module loaded. The device does not survive a
reboot.

A dummy interface is enough for the tests because they connect QPs of the
same device: rxe delivers a packet addressed to its own interface inside
the driver (its loopback path), without handing it to the interface, which
as a dummy would drop it. Traffic between two rxe devices, or two hosts,
needs an interface that carries packets, such as a veth pair or a real
NIC.

The device has two GIDs, both RoCE v2: index 0 the interface's IPv6
link-local address and index 1 its IPv4 address (`::ffff:10.233.0.1`), which
the tests use.

## Tests

```sh
scripts/rxe.sh up
cargo test --workspace
```

- `safeverbs-sys` (`crates/safeverbs-sys/tests/loopback.rs`): smoke tests
  over the raw FFI, below.
- `safeverbs-core`: unit tests of the completion ring (`ring.rs`), among
  them random sequences of posts, commits, aborts, in-order completions
  with errors and flushes, takes and detaches, checking the invariant after
  every step and that every lease is released exactly once.
- `safeverbs` (`crates/safeverbs/tests/model.rs`): the public API on the
  simulated NIC (`Model`), which needs no device: every operation, the sink
  and the stream, dropped futures, errors and flushes, and random
  operation sequences with injected errors after which every buffer is
  back or released once and nothing stays registered.
- `safeverbs` (`crates/safeverbs/tests/rxe.rs`): one test per feature on a
  real device, two QPs of it connected to each other.
- The crate documentation of `safeverbs` runs its example on the model
  and has `compile_fail` tests for what the types rule out.

The tests on a device, the raw ones included, pass without doing anything
on a host without RDMA devices. They pick an rxe device if there is one,
else the first device; `SAFEVERBS_TEST_DEVICE` names one. On a RoCE port
they use the first RoCE v2 GID that holds an IPv4 address;
`SAFEVERBS_TEST_GID_INDEX` picks another. On an InfiniBand port they
address by LID. Every wait has a deadline, so a test fails rather than
hangs.

The examples (`examples/`) and demos (`demo/`) run on Soft-RoCE as they
are, client and server on the same device, for example:

```sh
cargo run -p greeter --bin greeter_server &
cargo run -p greeter --bin greeter_client
cargo run -p perftest --bin safeverbs_write_bw &
cargo run -p perftest --bin safeverbs_write_bw -- 127.0.0.1
cargo run -p demo --bin datapath            # on the simulated NIC
SAFEVERBS_DEVICE=svrxe0 cargo run -p demo --bin datapath
```

### The raw smoke tests

`crates/safeverbs-sys/tests/loopback.rs` has two tests over the raw FFI:

- `rc_loopback_send_recv_and_write`: two RC QPs of one device connected to
  each other; a 4 KiB SEND into a posted RECV, then a 4 KiB RDMA WRITE, each
  payload compared.
- `probe_extended_verbs`: tries the extended verbs and prints which work
  (table below, `cargo test -p safeverbs-sys -- --nocapture`), then posts a
  SEND and an RDMA WRITE in one `ibv_wr_start` ... `ibv_wr_complete` batch
  and polls them with `ibv_start_poll`, `ibv_next_poll` and
  `ibv_end_poll`.

### What rxe supports

From `probe_extended_verbs` with the vendored rdma-core v65.0 on a Linux 7.2
kernel:

| Verb                                                                                    | rxe | Notes                                                                            |
| --------------------------------------------------------------------------------------- | --- | -------------------------------------------------------------------------------- |
| RC SEND/RECV, RDMA WRITE (`ibv_post_send`, `ibv_post_recv`, `ibv_poll_cq`)              | yes |                                                                                  |
| `ibv_alloc_td`                                                                          | no  | `EOPNOTSUPP`                                                                     |
| `ibv_alloc_parent_domain`                                                               | no  | `EOPNOTSUPP`, with or without a thread domain; so no QP or CQ on a parent domain |
| `ibv_create_cq_ex` with `IBV_CREATE_CQ_ATTR_SINGLE_THREADED`                            | yes | accepted and ignored: rxe's CQ keeps its lock                                    |
| `ibv_create_qp_ex` with `IBV_QP_INIT_ATTR_SEND_OPS_FLAGS` (plain PD)                    | yes |                                                                                  |
| `ibv_wr_start`, `ibv_wr_send`, `ibv_wr_rdma_write`, `ibv_wr_set_sge`, `ibv_wr_complete` | yes |                                                                                  |
| `ibv_start_poll`, `ibv_next_poll`, `ibv_end_poll`                                       | yes | broken in rdma-core 41's rxe (see above)                                         |

So on rxe the lock-free path is not available: code that creates QPs on a
thread domain must fall back to a normal QP when `ibv_alloc_td` or
`ibv_alloc_parent_domain` fails.

## Real hardware (ConnectX-5, 6, 7)

ConnectX-5 and later use the `mlx5` provider, which both builds have.

- RoCE: pick the GID index of a RoCE v2 GID; `ibv_devinfo -v` or
  `/sys/class/infiniband/<device>/ports/<port>/gids/` and `gid_attrs/types/`
  list them. InfiniBand ports address by LID instead.
- mlx5 implements thread domains: a QP or CQ created on a parent domain
  over an `ibv_td` has no locks, which is what the extended-verbs probe
  checks first (`ibv_create_qp_ex on the parent domain`, `ibv_create_cq_ex
on the parent domain`). Run the probe on the target hardware to confirm.
- `mlx5dv` (the direct verbs: `mlx5dv_init_obj`, the WQE and CQE layouts)
  is in the same bindings; the vendored build links `libmlx5` directly for
  it.
- With MLNX_OFED or DOCA-OFED installed, build with
  `--no-default-features` (see above).
