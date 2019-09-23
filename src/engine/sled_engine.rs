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
/// 
/// // open a SledKvsEngine
/// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
/// let mut store = SledKvsEngine::open(temp_dir.path())?;
/// 
/// // set and get data
/// store.set("key1".to_owned(), "value1".to_owned())?;
/// assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
/// 
/// // re-open then check data
/// drop(store);
/// let mut store = SledKvsEngine::open(temp_dir.path())?;
/// assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
/// 
/// // remove key-value pair
/// assert!(store.remove("key1".to_owned()).is_ok());
/// assert_eq!(store.get("key1".to_owned())?, None);
/// ```
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    /// Open or load a sled engine located in given path.
    pub fn open(path: &Path) -> Result<SledKvsEngine> {
        Ok(SledKvsEngine {
            db: sled::Db::open(path).unwrap(),
        })
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.db.get(key.as_bytes()).unwrap() {
            Some(value) => {
                return Ok(Some(String::from_utf8(value.to_vec()).unwrap()));
            }
            None => return Ok(None),
        }
    }
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key.as_bytes(), value.as_bytes()).unwrap();
        self.db.flush().unwrap();
        Ok(())
    }
    fn remove(&mut self, key: String) -> Result<()> {
        match self.db.remove(key.as_bytes()).unwrap() {
            Some(_) => {
                self.db.flush().unwrap();
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        }
    }
}
