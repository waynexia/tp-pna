use sled::Db;
extern crate bincode;

use super::KvsEngine;
use crate::error::{KvsError, Result};
use std::path::Path;

/// SledKvsEngine handle.
/// SledKvsEngine is a key-value stroage based on crate sled.
///
/// # Example
/// ```rust
/// use kvs::SledKvsEngine;
/// use tempfile::TempDir;
/// use crate::kvs::KvsEngine;
///
/// // open a SledKvsEngine
/// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
/// let mut store = SledKvsEngine::open(temp_dir.path()).unwrap();
///
/// // set and get data
/// store.set("key1".to_owned(), "value1".to_owned()).unwrap();
/// assert_eq!(store.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
///
/// // re-open then check data
/// drop(store);
/// let mut store = SledKvsEngine::open(temp_dir.path()).unwrap();
/// assert_eq!(store.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
///
/// // remove key-value pair
/// assert!(store.remove("key1".to_owned()).is_ok());
/// assert_eq!(store.get("key1".to_owned()).unwrap(), None);
/// ```
#[derive(Clone)]
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    /// Open or load a sled engine located in given path.
    pub fn open(path: &Path) -> Result<SledKvsEngine> {
        Ok(SledKvsEngine {
            db: sled::Db::open(path)?,
        })
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&self, key: String) -> Result<Option<String>> {
        match self.db.get(key.as_bytes())? {
            Some(value) => {
                return Ok(Some(String::from_utf8(value.to_vec()).unwrap()));
            }
            None => return Ok(None),
        }
    }
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key.as_bytes(), value.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }
    fn remove(&self, key: String) -> Result<()> {
        match self.db.remove(key.as_bytes())? {
            Some(_) => {
                self.db.flush()?;
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        }
    }
}
