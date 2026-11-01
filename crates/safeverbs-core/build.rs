//! Gives this crate's tests a run path to the vendored libibverbs and passes
//! the directory on to dependents as `DEP_SAFEVERBS_CORE_LIB_DIR` (see
//! `docs/dev-setup.md`).

fn main() {
    if let Ok(dir) = std::env::var("DEP_RDMA_CORE_LIB_DIR") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
        println!("cargo:lib_dir={dir}");
    }
}
