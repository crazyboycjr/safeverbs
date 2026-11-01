//! Gives the binaries a run path to the vendored libibverbs (see
//! `docs/dev-setup.md`).

fn main() {
    if let Ok(dir) = std::env::var("DEP_SAFEVERBS_LIB_DIR") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
    }
}
