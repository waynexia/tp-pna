//!

use crate::error::Result;

mod kvstore;
pub use kvstore::KvStore;

///
pub trait KvsEngine {
    ///
    fn get(&mut self, key: String) -> Result<Option<String>>;
    ///
    fn set(&mut self, key: String, value: String) -> Result<()>;
    ///
    fn remove(&mut self, key: String) -> Result<()>;
}
