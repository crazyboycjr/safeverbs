pub use ibverbs as ibv;
pub use ibverbs_sys as ffi;

pub mod safeverbs;

/// Because `std::slice::SliceIndex` is still unstable, we follow @alexcrichton's suggestion in
/// https://github.com/rust-lang/rust/issues/35729 and implement it ourselves.
mod sliceindex;