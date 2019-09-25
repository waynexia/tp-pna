use crate::error::Result;
use crossbeam::deque::{Steal, Stealer, Worker};
use std::process::exit;
use std::thread;

use super::ThreadPool;

///
pub struct SharedQueueThreadPool {
    workers: Worker<ThreadPoolMessage>,
    join_handles: Vec<thread::JoinHandle<()>>,
}

impl ThreadPool for SharedQueueThreadPool {
    ///
    fn new(threads: u32) -> Result<SharedQueueThreadPool> {
        let workers = Worker::<ThreadPoolMessage>::new_fifo();
        let mut join_handles = Vec::new();
        for _ in 0..threads {
            let stealer = workers.stealer();
            let handle = thread::spawn(move || get_job_and_exec(stealer));
            join_handles.push(handle);
        }

        Ok(SharedQueueThreadPool {
            workers,
            join_handles,
        })
    }

    ///
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.workers.push(ThreadPoolMessage::RunJob(Box::new(job)));
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.join_handles.len() {
            self.workers.push(ThreadPoolMessage::Shutdown);
        }
    }
}

enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

fn get_job_and_exec(stealer: Stealer<ThreadPoolMessage>) {
    loop {
        if let Steal::Success(message) = stealer.steal() {
            match message {
                ThreadPoolMessage::RunJob(job) => {
                    job();
                }
                ThreadPoolMessage::Shutdown => {
                    exit(0);
                }
            }
        }
    }
}
