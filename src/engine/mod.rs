//!

use crate::error::Result;

mod kvstore;
mod sled_engine;
pub use kvstore::KvStore;
pub use sled_engine::SledKvsEngine;

/// The `KvsEngine` trait allows to access data in a key-value storage
/// Struct which implemented this trait can be called `engine`.
/// 
/// A kvs(Key-Value Storage) engine's should support three operation: 
/// `get`, `set` and `remove`. Both types of key and value are `String`.
/// And return value is `Result<T,KvsError>`.
/// 
/// # Example
/// See document of KvStore or SledKvsEngine for example.
pub trait KvsEngine {
    /// Returns a value corresponding to the key.
    ///
    /// # Error
    /// - Try to get a non-existent key should return Ok(None)
    /// - Only return Err for other inner unexpectation.
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Inserts a key-value pair into the store.
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Removes a key from the map.
    ///
    /// # Error
    /// Removing of a non-existent key is considered an error.
    fn remove(&mut self, key: String) -> Result<()>;
}
