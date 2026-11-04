//! The errors of safeverbs.

use core::fmt;

use crate::wc::{CompletionError, Status};

/// What went wrong.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    /// A verb failed with this `errno`.
    Os(i32),
    /// A request did not complete successfully.
    Completion(CompletionError),
    /// An item sent through a sink did not complete successfully.
    SinkItem {
        /// Its position among the items sent through that sink, from 0.
        item: u64,
        /// Why.
        error: CompletionError,
    },
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
    /// An engine's CQ has no room for another QP's queues: every QP reserves
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
    /// The QP takes no work: it is not connected (nor moved to the error
    /// state), so the device would not accept its work requests.
    NotConnected,
    /// The device did not flush a QP being removed: no completion came
    /// while it still had work in flight. The QP is kept with the buffers
    /// its work requests name, never destroyed or freed (a leak, not a use
    /// after free).
    NotFlushed,
    /// The device does not support this.
    Unsupported(&'static str),
    /// An argument out of range.
    Invalid(&'static str),
    /// No RDMA device (with that name).
    NoDevice,
}

impl Error {
    /// The completion status of the WQE that failed, if the NIC reported one.
    pub fn status(&self) -> Option<Status> {
        match self {
            Error::Completion(error) | Error::SinkItem { error, .. } => error.status(),
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
            Error::Completion(error) => write!(f, "{error}"),
            Error::SinkItem { item, error } => write!(f, "item {item} of the sink: {error}"),
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
            Error::NotConnected => f.write_str("the queue pair is not connected"),
            Error::NotFlushed => {
                f.write_str("the device did not flush the queue pair; it is kept with its buffers")
            }
            Error::Unsupported(what) => write!(f, "not supported: {what}"),
            Error::Invalid(what) => write!(f, "invalid argument: {what}"),
            Error::NoDevice => f.write_str("no RDMA device"),
        }
    }
}

impl std::error::Error for Error {}

impl From<CompletionError> for Error {
    fn from(error: CompletionError) -> Error {
        Error::Completion(error)
    }
}

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
