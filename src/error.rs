use serde_json::error as serde_json_error;
use std::error;
use std::fmt;
use std::io;
use std::result;

///
#[derive(Debug)]
pub enum KvsError {
    IO(io::Error),
    Serde(serde_json_error::Error),
    Kvs(Box<String>),
}

///
pub type Result<T> = result::Result<T, KvsError>;

impl fmt::Display for KvsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KvsError::IO(ref err) => write!(f, "IO error {}", err),
            KvsError::Serde(ref err) => write!(f, "Serde error {}", err),
            KvsError::Kvs(ref desc) => write!(f, "Kvs inner error {}", desc),
        }
    }
}

impl error::Error for KvsError {
    fn description(&self) -> &str {
        match *self {
            KvsError::IO(ref err) => err.description(),
            KvsError::Serde(ref err) => err.description(),
            KvsError::Kvs(ref desc) => &desc,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            KvsError::IO(ref err) => Some(err),
            KvsError::Serde(ref err) => Some(err),
            // KvsError::Kvs(ref desc) => Some(&KvsError::from(*desc.deref())),
            KvsError::Kvs(ref _err) => None,
        }
    }
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::IO(err)
    }
}

impl From<serde_json_error::Error> for KvsError {
    fn from(err: serde_json_error::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

impl From<String> for KvsError {
    fn from(err: String) -> KvsError {
        KvsError::Kvs(Box::from(err))
    }
}
