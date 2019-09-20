use serde_json::error as serde_json_error;
use std::error;
use std::fmt;
use std::io;
use std::result;

/// Error types for kvs
#[derive(Debug)]
pub enum KvsError {
    /// IO error
    IO(io::Error),
    /// create `Serde`'s error
    Serde(serde_json_error::Error),
    /// Cannot find a given key in store
    KeyNotFound,
    /// Cannot deserialize something in log file
    Undeserialized,
}

/// Result alias
pub type Result<T> = result::Result<T, KvsError>;

impl fmt::Display for KvsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KvsError::IO(ref err) => write!(f, "IO error {}", err),
            KvsError::Serde(ref err) => write!(f, "Serde error {}", err),
            KvsError::KeyNotFound => write!(f, "Key not found"),
            KvsError::Undeserialized => write!(f, "Unable to deserialize log file"),
        }
    }
}

impl error::Error for KvsError {
    fn description(&self) -> &str {
        match *self {
            KvsError::IO(ref err) => err.description(),
            KvsError::Serde(ref err) => err.description(),
            KvsError::KeyNotFound => "Key not found",
            KvsError::Undeserialized => "Unable to deserialize log file",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            KvsError::IO(ref err) => Some(err),
            KvsError::Serde(ref err) => Some(err),
            KvsError::KeyNotFound => None,
            KvsError::Undeserialized => None,
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
