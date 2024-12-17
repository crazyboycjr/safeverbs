pub use ibverbs as ibv;
pub use ibverbs_sys as ffi;

pub mod safeverbs;
pub use safeverbs::*;

pub(crate) mod interval_tree;