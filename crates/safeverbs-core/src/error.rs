//! The errors of safeverbs.

use core::fmt;

use crate::wc::{Status, Wc};

/// What went wrong.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    /// A verb failed with this `errno`.
    Os(i32),
    /// A work request completed with an error.
    Completion(Wc),
    /// A buffer is registered in another protection domain than the QP's.
    PdMismatch,
    /// More scatter/gather elements than the QP allows.
    TooManySges {
        /// Elements given.
        got: usize,
        /// Elements allowed.
        max: usize,
    },
    /// Data longer than its destination (a remote buffer, an atomic's 8
    /// bytes, or what a work request can carry).
    TooLong {
        /// Bytes given.
        len: u64,
        /// Bytes that fit.
        max: u64,
    },
    /// The receive queue is full.
    QueueFull,
    /// A worker's CQ has no room for another QP's queues: every QP reserves
    /// a CQ entry for each WQE its queues hold.
    CqFull {
        /// Entries the QP needs (its send and receive depths).
        need: u64,
        /// Entries not reserved yet.
        free: u64,
    },
    /// Every slot of the ring holds a result nobody took: too many
    /// operations whose futures were neither awaited nor dropped.
    Clogged,
    /// The QP is in the error state: work requests would only be flushed.
    QpFailed,
    /// The device does not support this.
    Unsupported(&'static str),
    /// An argument out of range.
    Invalid(&'static str),
    /// No RDMA device (with that name).
    NoDevice,
}

impl Error {
    /// The completion status, for a work request that failed.
    pub fn status(&self) -> Option<Status> {
        match self {
            Error::Completion(wc) => Some(wc.status),
            _ => None,
        }
    }

    /// The error of the last OS call (`errno`).
    pub fn last_os_error() -> Error {
        Error::Os(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Os(errno) => write!(f, "{}", std::io::Error::from_raw_os_error(*errno)),
            Error::Completion(wc) => write!(f, "work request {} failed: {}", wc.wr_id, wc.status),
            Error::PdMismatch => f.write_str("buffer registered in another protection domain"),
            Error::TooManySges { got, max } => {
                write!(f, "{got} scatter/gather elements, at most {max}")
            }
            Error::TooLong { len, max } => write!(f, "{len} bytes where {max} fit"),
            Error::QueueFull => f.write_str("receive queue full"),
            Error::CqFull { need, free } => {
                write!(f, "the CQ has {free} entries left, the QP needs {need}")
            }
            Error::Clogged => f.write_str("every slot holds a result nobody took"),
            Error::QpFailed => f.write_str("the queue pair is in the error state"),
            Error::Unsupported(what) => write!(f, "not supported: {what}"),
            Error::Invalid(what) => write!(f, "invalid argument: {what}"),
            Error::NoDevice => f.write_str("no RDMA device"),
        }
    }
}

impl std::error::Error for Error {}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> std::io::Error {
        match e {
            Error::Os(errno) => std::io::Error::from_raw_os_error(errno),
            other => std::io::Error::other(other),
        }
    }
}

/// A result of safeverbs.
pub type Result<T, E = Error> = core::result::Result<T, E>;
