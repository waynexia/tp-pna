pub struct KvStore {}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {}
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        unimplemented!();
    }

    pub fn set(&mut self, key: String, value: String) {
        unimplemented!();
    }

    pub fn remove(&mut self, key: String) {
        unimplemented!();
    }
}
