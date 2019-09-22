#![deny(missing_docs)]
//! kvs is a key-value store implementation.
//! Both key and value's types are `String`.
//! At this stage, it's implemented with `HashMap`.
//! Data is stored in memory and is nonpersistent.

pub mod engine;
mod error;
mod protocol;

pub use engine::{KvStore, KvsEngine};
pub use error::{KvsError, Result};
pub use protocol::{Protocol, Status};
