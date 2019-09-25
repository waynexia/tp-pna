use super::ThreadPool;
use crate::error::Result;
use rayon;

///
pub struct RayonThreadPool {
    thread_pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<RayonThreadPool> {
        Ok(RayonThreadPool {
            thread_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads as usize)
                .build()
                .unwrap(),
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.thread_pool.spawn(job);
    }
}
