use sled::Db;
extern crate bincode;

use super::KvsEngine;
use crate::error::{KvsError, Result};
use std::path::Path;

///
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    ///
    pub fn open(path: &Path) -> Result<SledKvsEngine> {
        Ok(SledKvsEngine {
            db: sled::Db::start_default(path).unwrap(),
        })
    }
}

impl KvsEngine for SledKvsEngine {
    ///
    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.db.get(key.as_bytes()).unwrap() {
            Some(value) => {
                return Ok(Some(String::from_utf8(value.to_vec()).unwrap()));
            }
            None => return Ok(None),
        }
    }
    ///
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key.as_bytes(), value.as_bytes()).unwrap();
        self.db.flush();
        Ok(())
    }
    ///
    fn remove(&mut self, key: String) -> Result<()> {
        match self.db.remove(key.as_bytes()).unwrap() {
            Some(key) => {
                self.db.flush();
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        }
    }
}
