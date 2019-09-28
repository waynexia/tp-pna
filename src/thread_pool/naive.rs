use super::ThreadPool;
use crate::error::Result;
use std::thread;

/// A naive "thread pool". No thread re-use at all. Just a wrapper of
/// std::thread.
pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<NaiveThreadPool> {
        Ok(NaiveThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
