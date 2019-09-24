use crate::error::{KvsError, Result};

///
pub struct ThreadPool {}

impl ThreadPool {
    ///
    pub fn new(threads: u32) -> Result<ThreadPool> {
        panic!();
    }

    ///
    pub fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
    }
}
