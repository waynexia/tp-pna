use std::collections::HashMap;
use std::fs::{create_dir_all, metadata, rename, File, OpenOptions};
use std::io::{prelude::*, BufReader, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use super::KvsEngine;
use crate::error::{KvsError, Result};
use crate::protocol::{Command, OpType};

const MAX_REDUNDANCY: u64 = 1 << 16;

/// KvStore handle.
/// KvStore is a key-value stroage based on kvs engine.
///
/// # Example
/// ```rust
/// use kvs::{KvStore,KvsEngine};
/// use tempfile::TempDir;
///
/// // open a KvStore
/// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
/// let mut store = KvStore::open(temp_dir.path()).unwrap();
///
/// // set and get data
/// store.set("key1".to_owned(), "value1".to_owned()).unwrap();
/// assert_eq!(store.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
///
/// // re-open then check data
/// drop(store);
/// let mut store = KvStore::open(temp_dir.path()).unwrap();
/// assert_eq!(store.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
///
/// // remove key-value pair
/// assert!(store.remove("key1".to_owned()).is_ok());
/// assert_eq!(store.get("key1".to_owned()).unwrap(), None);
/// ```
#[derive(Clone)]
pub struct KvStore {
    index_keeper: Arc<Mutex<HashMap<String, usize>>>,
    // db_file_path: Box<Path>,
    file_handle_keeper: Arc<Mutex<DBFileHandle>>,
}

impl KvStore {
    /// Open or load a kvs engine located in given path.
    pub fn open(path: &Path) -> Result<KvStore> {
        let db_file_path = path.join(Path::new("record.db"));
        if !db_file_path.exists() {
            create_dir_all(&path)?;
            File::create(&db_file_path)?;
        }
        let file_handle = DBFileHandle {
            file_path: Box::from(db_file_path),
            redundancy: 0,
        };
        let index = file_handle.build_index()?;
        Ok(KvStore {
            index_keeper: Arc::new(Mutex::new(index)),
            file_handle_keeper: Arc::new(Mutex::new(file_handle)),
        })
    }
}
impl KvsEngine for KvStore {
    fn get(&self, key: String) -> Result<Option<String>> {
        let file_handle_locker = Arc::clone(&self.file_handle_keeper);
        let file_handle = file_handle_locker.lock().unwrap();
        let index_locker = Arc::clone(&self.index_keeper);
        let index = index_locker.lock().unwrap();

        file_handle.get_value_by_offset(match index.get(&key) {
            Some(item) => item,
            None => return Ok(None),
        })
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let record = Command {
            op: OpType::Set,
            key: key.clone(),
            value: Some(value),
        };

        let file_handle_locker = Arc::clone(&self.file_handle_keeper);
        let mut file_handle = file_handle_locker.lock().unwrap();
        let index_locker = Arc::clone(&self.index_keeper);
        let index = index_locker.lock().unwrap();

        let offset = file_handle.write_log(record, index)?;
        let mut index = index_locker.lock().unwrap();
        index.insert(key, offset);
        Ok(())
    }

    fn remove(&self, key: String) -> Result<()> {
        let file_handle_locker = Arc::clone(&self.file_handle_keeper);
        let mut file_handle = file_handle_locker.lock().unwrap();
        let index_locker = Arc::clone(&self.index_keeper);
        let index = index_locker.lock().unwrap();

        index.get(&key).ok_or(KvsError::KeyNotFound)?;
        let record = Command {
            op: OpType::Remove,
            key: key.clone(),
            value: None,
        };
        file_handle.write_log(record, index)?;
        let mut index = index_locker.lock().unwrap();
        index.remove(&key);
        Ok(())
    }
}

#[derive(Clone)]
struct DBFileHandle {
    file_path: Box<Path>,
    redundancy: u64,
}

impl DBFileHandle {
    pub fn build_index(&self) -> Result<HashMap<String, usize>> {
        let mut index: HashMap<String, usize> = HashMap::new();

        let mut iter = serde_json::Deserializer::from_reader(BufReader::new(File::open(
            self.file_path.to_owned(),
        )?))
        .into_iter::<Command>();

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

    pub fn get_value_by_offset(&self, offset: &usize) -> Result<Option<String>> {
        let mut file = File::open(&self.file_path)?;
        file.seek(SeekFrom::Start(*offset as u64))?;
        let record = serde_json::Deserializer::from_reader(BufReader::new(file))
            .into_iter::<Command>()
            .next()
            .ok_or(KvsError::Undeserialized)?;
        Ok(record?.value)
    }

    pub fn write_log(
        &mut self,
        record: Command,
        mut index: MutexGuard<HashMap<String, usize>>,
    ) -> Result<usize> {
        /* maintain self.redundancy */
        if record.op == OpType::Remove
            || (record.op == OpType::Set && index.get(&record.key) != None)
        {
            let redundant_record = self.get_value_by_offset(index.get(&record.key).unwrap())?;
            self.redundancy += serde_json::to_string(&redundant_record)?.len() as u64;
        }
        if self.redundancy > MAX_REDUNDANCY {
            self.compact_log(&index)?;
            /* update index (can refine) */
            index.clear();
            for (key, value) in self.build_index()? {
                index.insert(key, value);
            }
        }
        let size = metadata(&self.file_path)?.len();
        let mut file = OpenOptions::new().append(true).open(&self.file_path)?;
        serde_json::to_writer(&mut file, &record)?;

        Ok(size as usize)
    }

    /*
        strategy: copy log entry that still alive into a new file
        then use it to overwrite the old log file.
    */
    pub fn compact_log(&mut self, index: &HashMap<String, usize>) -> Result<()> {
        let something = &format!("{}_compacted", self.file_path.to_str().unwrap());
        let new_log_path = Path::new(something);
        File::create(&new_log_path)?;
        let mut out_file = OpenOptions::new().append(true).open(new_log_path)?;
        let mut in_file = File::open(&self.file_path)?;

        for (_, offset) in index.iter() {
            in_file.seek(SeekFrom::Start(*offset as u64))?;
            let record = serde_json::Deserializer::from_reader(BufReader::new(&in_file))
                .into_iter::<Command>()
                .next()
                .ok_or(KvsError::Undeserialized)?;
            serde_json::to_writer(&mut out_file, &record?)?;
        }

        rename(&new_log_path, &self.file_path)?;
        self.redundancy = 0;
        Ok(())
    }
}
