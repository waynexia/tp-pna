#![deny(missing_docs)]
//! kvs is a key-value store implementation.
//! Both key and value's types are `String`.
//! At this stage, it's implemented with `HashMap`.
//! Data is stored in memory and is nonpersistent.
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::BufReader;

mod error;
use error::KvsError;
///
pub use error::Result;
use std::path::Path;

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

/// Used to create and operate a `KvStore` instance.
pub struct KvStore {
    // data: HashMap<String, String>,
    // writer: io::BufWriter<File>,
    // file: File,
    db_file_path: Box<Path>,
}

impl KvStore {
    ///
    pub fn open(path: &Path) -> Result<KvStore> {
        let db_file_path = path.join(Path::new("record.db"));
        if !db_file_path.exists() {
            File::create(&db_file_path)?;
        }
        // let file = OpenOptions::new().append(true).open(db_file_path)?;
        // let writer = io::BufWriter::new(file);
        Ok(KvStore {
            // data: HashMap::new(),
            // writer,
            // file,
            db_file_path: Box::from(db_file_path),
        })
    }

    /// Returns a value corresponding to the key.
    ///
    /// # Example:
    /// ```rust
    /// use kvs::KvStore;
    ///
    /// let mut kvs = KvStore::new();
    /// kvs.set("key1".to_owned(), "value1".to_owned());
    /// assert_eq!(kvs.get("key1".to_owned()), Some("value1".to_owned()));
    /// assert_eq!(kvs.get("key2".to_owned()), None);
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let index = self.build_index()?;
        Ok(index.get(&key).cloned())
    }

    /// Inserts a key-value pair into the store.
    ///
    /// # Example
    /// ```rust
    /// use kvs::KvStore;
    ///
    /// let mut kvs = KvStore::new();
    /// kvs.set("key1".to_owned(), "value1".to_owned());
    /// assert_eq!(kvs.get("key1".to_owned()), Some("value1".to_owned()));
    ///
    /// kvs.set("key1".to_owned(), "value2".to_owned());
    /// assert_eq!(kvs.get("key1".to_owned()), Some("value2".to_owned()))
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let record = Record {
            op: OpType::Set,
            key,
            value: Some(value),
        };
        let mut file = OpenOptions::new().append(true).open(&self.db_file_path)?;
        serde_json::to_writer(&mut file, &record)?;
        Ok(())
    }

    /// Removes a key from the map.
    ///
    /// # Example
    /// ```rust
    /// use kvs::KvStore;
    ///
    /// let mut kvs = KvStore::new();
    /// kvs.set("key1".to_owned(), "value1".to_owned());
    /// kvs.remove("key1".to_owned());
    /// assert_eq!(kvs.get("key1".to_owned()),None);
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        let index = self.build_index()?;
        index
            .get(&key)
            .ok_or(KvsError::from("Try to remove a non-exist key".to_owned()))?;
        let record = Record {
            op: OpType::Remove,
            key,
            value: None,
        };
        let mut file = OpenOptions::new().append(true).open(&self.db_file_path)?;
        serde_json::to_writer(&mut file, &record)?;
        Ok(())
    }

    fn build_index(&self) -> Result<HashMap<String, String>> {
        let mut index: HashMap<String, String> = HashMap::new();
        let file = File::open(&self.db_file_path)?;
        let reader = BufReader::new(file);
        let der = serde_json::Deserializer::from_reader(reader);
        let iter = der.into_iter::<Record>();
        for record_result in iter {
            let record = record_result?;
            match record.op {
                OpType::Set => index.insert(
                    record.key.clone(),
                    record
                        .value
                        .ok_or(KvsError::from("Expect a value in log file".to_owned()))?
                        .clone(),
                ),
                OpType::Remove => index.remove(&record.key),
                _ => {
                    return Err(KvsError::from(
                        "Unexpected command: `Get` in log file".to_owned(),
                    ))
                }
            };
        }
        Ok(index)
    }
}
