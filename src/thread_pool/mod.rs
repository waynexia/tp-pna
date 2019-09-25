//!

use crate::error::Result;

mod naive;
mod rayon;
mod shared_queue;
pub use self::rayon::RayonThreadPool;
pub use naive::NaiveThreadPool;
pub use shared_queue::SharedQueueThreadPool;

///
pub trait ThreadPool {
    ///
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    ///
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
