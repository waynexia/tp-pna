use std::{error, fmt, result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Rpc(labrpc::Error),
    NoLeader,
    Timeout,
    Others,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Error::Rpc(ref e) => Some(e),
            Error::NoLeader => None,
            Error::Timeout => None,
            Error::Others => None,
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
