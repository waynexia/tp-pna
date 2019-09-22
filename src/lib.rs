#![deny(missing_docs)]
//! kvs is a key-value store implementation.
//! Both key and value's types are `String`.
//! At this stage, it's implemented with `HashMap`.
//! Data is stored in memory and is nonpersistent.
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{metadata, rename, File, OpenOptions};
use std::io::{prelude::*, BufReader, SeekFrom};

mod error;
mod protocol;
pub use error::{KvsError, Result};
pub use protocol::{Protocol, Status};
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

///
pub struct KvsEngine {}

/// Used to create and operate a `KvStore` instance.
pub struct KvStore {
    index: HashMap<String, usize>,
    db_file_path: Box<Path>,
}

impl KvStore {
    /// Open a kvs located in given path.
    /// If no log file existing here it will be created.
    /// And if there is one, kvs will use it to recover previous status.
    ///
    /// # Example:
    /// ```rust
    /// use kvs::KvStore;
    /// use std::path::Path;
    ///
    /// let mut kvs = KvStore::open(Path::new("./")).unwrap();
    /// ```
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

    /// Returns a value corresponding to the key.
    ///
    /// # Example:
    /// ```rust
    /// use kvs::KvStore;
    /// use std::path::Path;
    ///
    /// let mut kvs = KvStore::open(Path::new("./")).unwrap();
    /// kvs.set("key1".to_owned(), "value1".to_owned());
    /// assert_eq!(kvs.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
    /// assert_eq!(kvs.get("key2".to_owned()).unwrap(), None);
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // maybe can use some combinator here
        self.get_value_by_offset(match self.index.get(&key) {
            Some(item) => item,
            None => return Ok(None),
        })
    }

    /// Inserts a key-value pair into the store.
    ///
    /// # Example
    /// ```rust
    /// use kvs::KvStore;
    /// use std::path::Path;
    ///
    /// let mut kvs = KvStore::open(Path::new("./")).unwrap();
    /// kvs.set("key1".to_owned(), "value1".to_owned()).unwrap();
    /// assert_eq!(kvs.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
    ///
    /// kvs.set("key1".to_owned(), "value2".to_owned()).unwrap();
    /// assert_eq!(kvs.get("key1".to_owned()).unwrap(), Some("value2".to_owned()))
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let record = Record {
            op: OpType::Set,
            key: key.clone(),
            value: Some(value),
        };
        let offset = self.write_log(record)?;
        self.index.insert(key, offset);
        Ok(())
    }

    /// Removes a key from the map.
    ///
    /// # Example
    /// ```rust
    /// use kvs::KvStore;
    /// use std::path::Path;
    ///
    /// let mut kvs = KvStore::open(Path::new("./")).unwrap();
    /// kvs.set("key1".to_owned(), "value1".to_owned()).unwrap();
    /// kvs.remove("key1".to_owned()).unwrap();
    /// assert_eq!(kvs.get("key1".to_owned()).unwrap(),None);
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
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
        // try to use enviroment variable instead?
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

    // strategy: just copy log entry that still alive into a new file
    // then use it to overwrite the old log file.
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
