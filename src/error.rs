use std::io;
use std::result;

///
#[derive(Debug)]
pub enum KVSError {
    IO(io::Error),
}

///
pub type Result<T> = result::Result<T, KVSError>;

impl From<io::Error> for KVSError {
    fn from(err: io::Error) -> KVSError {
        KVSError::IO(err)
    }
}
