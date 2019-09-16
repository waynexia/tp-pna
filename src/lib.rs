#![deny(missing_docs)]
//! kvs is a key-value store implementation.
//! Both key and value's types are `String`.
//! At this stage, it's implemented with `HashMap`.
//! Data is stored in memory and is nonpersistent.
use std::collections::HashMap;

/// Used to create and operate a `KvStore` instance.
pub struct KvStore {
    data: HashMap<String, String>,
}

impl KvStore {
    /// Creates an empty KvStore.
    ///
    /// # Example:
    /// ```rust
    /// use kvs::KvStore;
    /// let mut kvs = KvStore::new();
    /// ```
    pub fn new() -> KvStore {
        KvStore {
            data: HashMap::new(),
        }
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
    pub fn get(&mut self, key: String) -> Option<String> {
        self.data.get(&key).cloned()
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
    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
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
    pub fn remove(&mut self, key: String) {
        self.data.remove(&key);
    }
}
