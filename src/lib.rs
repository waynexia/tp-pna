#![deny(missing_docs)]
//! kvs is a key-value store library.
//!
//! Includes a trait `KvsEngine` which defines a kvs engine's behavior
//! with two engines `KvStore` and `SledKvsEngine` that implemented this trait;
//! error type `KvsError` for error handle in this library
//! and struct `Protocol` for client-server communication.

pub mod engine;
mod error;
mod protocol;
mod thread_pool;

pub use engine::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{KvsError, Result};
pub use protocol::{Command, OpType, Protocol, Status};
pub use thread_pool::ThreadPool;
