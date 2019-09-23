use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{metadata, rename, File, OpenOptions};
use std::io::{prelude::*, BufReader, SeekFrom};
use std::path::Path;

use super::KvsEngine;
use crate::error::{KvsError, Result};

#[derive(Serialize, Deserialize, Debug)]
enum OpType {
    Set,
    Get,
    Remove,
}

#[derive(Serialize, Deserialize, Debug)]
struct Record {
    op: OpType,
    key: String,
    value: Option<String>,
}

/// KvStore handle.
/// KvStore is a key-value stroage based on kvs engine.
///
/// # Example
/// ```rust
/// use kvs::KvStore;
/// use tempfile::TempDir;
///
/// // open a KvStore
/// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
/// let mut store = KvStore::open(temp_dir.path())?;
///
/// // set and get data
/// store.set("key1".to_owned(), "value1".to_owned())?;
/// assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
///
/// // re-open then check data
/// drop(store);
/// let mut store = KvStore::open(temp_dir.path())?;
/// assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
///
/// // remove key-value pair
/// assert!(store.remove("key1".to_owned()).is_ok());
/// assert_eq!(store.get("key1".to_owned())?, None);
/// ```
pub struct KvStore {
    index: HashMap<String, usize>,
    db_file_path: Box<Path>,
}

impl KvStore {
    /// Open or load a kvs engine located in given path.
    pub fn open(path: &Path) -> Result<KvStore> {
        let db_file_path = path.join(Path::new("record.db"));
        if !db_file_path.exists() {
            File::create(&db_file_path)?;
        }
        let index = KvStore::build_index(&db_file_path)?;
        Ok(KvStore {
            index,
            db_file_path: Box::from(db_file_path),
        })
    }

    fn build_index(db_file_path: &Path) -> Result<HashMap<String, usize>> {
        let mut index: HashMap<String, usize> = HashMap::new();

        let mut iter =
            serde_json::Deserializer::from_reader(BufReader::new(File::open(db_file_path)?))
                .into_iter::<Record>();

        loop {
            let offset = iter.byte_offset();
            match iter.next() {
                Some(item) => {
                    let record = item?;
                    match record.op {
                        OpType::Set => index.insert(record.key.clone(), offset.to_owned()),
                        OpType::Remove => index.remove(&record.key),
                        _ => {
                            return Err(KvsError::Undeserialized);
                        }
                    }
                }
                None => break,
            };
        }
        Ok(index)
    }

    fn get_value_by_offset(&self, offset: &usize) -> Result<Option<String>> {
        let mut file = File::open(&self.db_file_path)?;
        file.seek(SeekFrom::Start(*offset as u64))?;
        let record = serde_json::Deserializer::from_reader(BufReader::new(file))
            .into_iter::<Record>()
            .next()
            .ok_or(KvsError::Undeserialized)?;
        Ok(record?.value)
    }

    fn write_log(&self, record: Record) -> Result<usize> {
        /* try to use enviroment variable instead? */
        let log_size_limit = 10_0000;

        let mut size = metadata(&self.db_file_path)?.len();
        if size > log_size_limit {
            self.compact_log()?;
            size = metadata(&self.db_file_path)?.len();
        }

        let mut file = OpenOptions::new().append(true).open(&self.db_file_path)?;
        serde_json::to_writer(&mut file, &record)?;

        Ok(size as usize)
    }

    /*
        strategy: just copy log entry that still alive into a new file
        then use it to overwrite the old log file.
    */
    fn compact_log(&self) -> Result<()> {
        let something = &format!("{}_compacted", self.db_file_path.to_str().unwrap());
        let new_log_path = Path::new(something);
        File::create(&new_log_path)?;
        let mut out_file = OpenOptions::new().append(true).open(new_log_path)?;
        let mut in_file = File::open(&self.db_file_path)?;

        for (_, offset) in self.index.iter() {
            in_file.seek(SeekFrom::Start(*offset as u64))?;
            let record = serde_json::Deserializer::from_reader(BufReader::new(&in_file))
                .into_iter::<Record>()
                .next()
                .ok_or(KvsError::Undeserialized)?;
            serde_json::to_writer(&mut out_file, &record?)?;
        }

        rename(&new_log_path, &self.db_file_path)?;
        Ok(())
    }
}
impl KvsEngine for KvStore {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        self.get_value_by_offset(match self.index.get(&key) {
            Some(item) => item,
            None => return Ok(None),
        })
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let record = Record {
            op: OpType::Set,
            key: key.clone(),
            value: Some(value),
        };
        let offset = self.write_log(record)?;
        self.index.insert(key, offset);
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.index.get(&key).ok_or(KvsError::KeyNotFound)?;
        let record = Record {
            op: OpType::Remove,
            key: key.clone(),
            value: None,
        };
        self.write_log(record)?;
        self.index.remove(&key);
        Ok(())
    }
}