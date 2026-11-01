//! Finds rdma-core, generates the bindings and compiles the static inline
//! functions of its headers into a small C library.
//!
//! With the `vendored` feature (the default) rdma-core comes from the
//! `vendor/rdma-core` submodule, configured `IN_PLACE` so that the built
//! libibverbs loads its providers (mlx5, rxe) from the build directory; only
//! libibverbs and those two providers are built. Without it, the system's
//! libibverbs and libmlx5 are found through pkg-config.
//!
//! Derived from the build script of rust-ibverbs's `ibverbs-sys`
//! (<https://github.com/jonhoo/rust-ibverbs>, MIT OR Apache-2.0).

use std::env;
use std::path::PathBuf;
#[cfg(feature = "vendored")]
use std::{fs, path::Path};

/// The providers built from the submodule: mlx5 for ConnectX hardware and rxe
/// (Soft-RoCE) for tests.
#[cfg(feature = "vendored")]
const PROVIDERS: &[&str] = &["mlx5", "rxe"];

/// Where to find rdma-core's headers and libraries.
struct RdmaCore {
    /// Directories holding `infiniband/verbs.h` and `infiniband/mlx5dv.h`.
    include_dirs: Vec<PathBuf>,
    /// Directories to search for the libraries at link time.
    link_dirs: Vec<PathBuf>,
    /// Directory to put in the run path of this crate's tests and examples.
    rpath: Option<PathBuf>,
}

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=wrapper.h");

    #[cfg(feature = "vendored")]
    let rdma = vendored(&manifest_dir);
    #[cfg(not(feature = "vendored"))]
    let rdma = system();

    let header = manifest_dir.join("wrapper.h");
    let static_fns = out_dir.join("static_fns");
    let bindings = bindgen::Builder::default()
        .header(header.to_str().expect("path is not UTF-8"))
        .clang_args(
            rdma.include_dirs
                .iter()
                .map(|d| format!("-I{}", d.display())),
        )
        .rust_edition(bindgen::RustEdition::Edition2024)
        .use_core()
        .allowlist_item("_*(ibv|mlx5dv|mlx5)_.*")
        .allowlist_item("(IBV|MLX5DV|MLX5)_.*")
        // Packed and 4-aligned in C, which Rust cannot say: defined in lib.rs.
        .blocklist_type("mlx5_wqe_ctrl_seg")
        .bitfield_enum("ibv_access_flags")
        .bitfield_enum("ibv_qp_attr_mask")
        .bitfield_enum("ibv_wc_flags")
        .bitfield_enum("ibv_send_flags")
        .bitfield_enum("ibv_port_cap_flags")
        .bitfield_enum("ibv_qp_init_attr_mask")
        .bitfield_enum("ibv_qp_create_send_ops_flags")
        .bitfield_enum("ibv_cq_init_attr_mask")
        .bitfield_enum("ibv_create_cq_attr_flags")
        .bitfield_enum("ibv_create_cq_wc_flags")
        .bitfield_enum("ibv_parent_domain_init_attr_mask")
        .constified_enum_module("ibv_qp_type")
        .constified_enum_module("ibv_qp_state")
        .constified_enum_module("ibv_port_state")
        .constified_enum_module("ibv_mtu")
        .constified_enum_module("ibv_wc_opcode")
        .constified_enum_module("ibv_wr_opcode")
        .constified_enum_module("ibv_wc_status")
        .derive_default(true)
        .derive_debug(true)
        .prepend_enum_name(false)
        .size_t_is_usize(true)
        .wrap_static_fns(true)
        .wrap_static_fns_path(&static_fns)
        .generate()
        .expect("unable to generate the bindings");
    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("could not write the bindings");

    // The static inline functions, made callable. Compiled before the
    // libraries are named so that the linker sees its references first.
    cc::Build::new()
        .file(static_fns.with_extension("c"))
        .include(&manifest_dir)
        .includes(&rdma.include_dirs)
        .compile("safeverbs_static_fns");

    for dir in &rdma.link_dirs {
        println!("cargo:rustc-link-search=native={}", dir.display());
    }
    println!("cargo:rustc-link-lib=dylib=ibverbs");
    println!("cargo:rustc-link-lib=dylib=mlx5");
    if let Some(rpath) = &rdma.rpath {
        // Only this crate's tests and examples get it; dependents read
        // `DEP_RDMA_CORE_LIB_DIR` and add their own (see docs/dev-setup.md).
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", rpath.display());
        println!("cargo:lib_dir={}", rpath.display());
    }
    if let Some(include) = rdma.include_dirs.first() {
        println!("cargo:include={}", include.display());
    }
}

/// Builds libibverbs and [`PROVIDERS`] from the submodule.
#[cfg(feature = "vendored")]
fn vendored(manifest_dir: &Path) -> RdmaCore {
    let src = manifest_dir.join("vendor/rdma-core");
    assert!(
        src.join("CMakeLists.txt").exists(),
        "{} is empty: run `git submodule update --init`",
        src.display()
    );
    println!("cargo:rerun-if-changed={}", src.display());

    // ENABLE_RESOLVE_NEIGH links libnl, which libibverbs also uses to list
    // devices over netlink: without it devices come from sysfs alone and a
    // software device is matched to its provider by name, so an rxe device
    // not named `rxe*` is never found.
    let mut config = cmake::Config::new(&src);
    config
        .define("IN_PLACE", "1")
        .define("NO_MAN_PAGES", "1")
        .define("NO_PYVERBS", "1")
        .define("ENABLE_RESOLVE_NEIGH", "1")
        .define("ENABLE_VALGRIND", "0")
        .define("ENABLE_STATIC", "0")
        .profile("RelWithDebInfo");
    let pabi = provider_abi(&src);
    let mut out = PathBuf::new();
    for provider in PROVIDERS {
        // A shared provider (mlx5) is its own target; a plugin is
        // `<name>-rdmav<ABI>`.
        let target = if *provider == "mlx5" {
            provider.to_string()
        } else {
            format!("{provider}-rdmav{pabi}")
        };
        out = config.build_target(&target).build();
    }
    let build = out.join("build");
    prune_drivers(&build.join("etc/libibverbs.d"));
    let lib = build.join("lib");
    RdmaCore {
        include_dirs: vec![build.join("include")],
        link_dirs: vec![lib.clone()],
        rpath: Some(lib),
    }
}

/// Reads the provider ABI version (`IBVERBS_PABI_VERSION`) from rdma-core's
/// top `CMakeLists.txt`.
#[cfg(feature = "vendored")]
fn provider_abi(src: &Path) -> String {
    let cmake = fs::read_to_string(src.join("CMakeLists.txt")).expect("read CMakeLists.txt");
    cmake
        .lines()
        .find_map(|line| line.trim().strip_prefix("set(IBVERBS_PABI_VERSION \""))
        .and_then(|rest| rest.split('"').next())
        .expect("IBVERBS_PABI_VERSION not found in CMakeLists.txt")
        .to_string()
}

/// Removes the driver files of the providers that were not built, which the
/// configure step writes for every provider; libibverbs would warn about each
/// one it cannot load.
#[cfg(feature = "vendored")]
fn prune_drivers(dir: &Path) {
    for entry in fs::read_dir(dir).expect("read libibverbs.d") {
        let path = entry.expect("read libibverbs.d").path();
        let keep = path
            .file_stem()
            .and_then(|s| s.to_str())
            .is_some_and(|name| PROVIDERS.contains(&name));
        if !keep {
            fs::remove_file(&path).expect("remove a driver file");
        }
    }
}

/// Finds the system's libibverbs and libmlx5 through pkg-config.
#[cfg(not(feature = "vendored"))]
fn system() -> RdmaCore {
    let mut include_dirs = Vec::new();
    let mut link_dirs = Vec::new();
    for name in ["libibverbs", "libmlx5"] {
        let lib = pkg_config::Config::new()
            .cargo_metadata(false)
            .probe(name)
            .unwrap_or_else(|e| panic!("{name} not found: {e}"));
        include_dirs.extend(lib.include_paths);
        link_dirs.extend(lib.link_paths);
    }
    include_dirs.dedup();
    link_dirs.dedup();
    RdmaCore {
        include_dirs,
        link_dirs,
        rpath: None,
    }
}
