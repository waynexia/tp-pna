//!

use crate::error::Result;

mod naive;
mod rayon;
mod shared_queue;
pub use self::rayon::RayonThreadPool;
pub use naive::NaiveThreadPool;
pub use shared_queue::SharedQueueThreadPool;

/// Trait `ThreadPool` is for multithread jobs. Required functions
/// are `new` and `spawn`. 
pub trait ThreadPool {
    /// Construct a new thread pool with total threads number `threads`.
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    /// Submit a new workload to execute.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
