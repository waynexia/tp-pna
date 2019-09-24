use crate::error::{KvsError, Result};
use std::thread;

///
pub struct ThreadPool {}

impl ThreadPool {
    ///
    pub fn new(threads: u32) -> Result<ThreadPool> {
        Ok(ThreadPool{})
    }

    ///
    pub fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job).join().unwrap();
    }
}
