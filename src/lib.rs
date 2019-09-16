use std::collections::HashMap;

pub struct KvStore{
    data: HashMap<String,String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            data: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        self.data.get(&key).cloned()
    }

    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key,value);
    }

    pub fn remove(&mut self, key: String) {
        self.data.remove(&key);
    }
}
