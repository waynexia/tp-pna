use super::ThreadPool;
use crate::error::Result;
use crossbeam::deque::{Steal, Stealer, Worker};
use std::process::exit;
use std::sync::mpsc::{channel, Sender};
use std::thread;

///
pub struct SharedQueueThreadPool {
    tx_worker: Sender<ThreadPoolMessage>,
    threads: u32,
}

impl ThreadPool for SharedQueueThreadPool {
    /// This constructor will spwan n(`threads`) + 1 threads. One more thread is
    /// to monitor others. Re-spawn thread when one thread is paniced or forward
    /// job and shutdown (`ThreadPoolMessage`) to other `n` threads.
    fn new(threads: u32) -> Result<SharedQueueThreadPool> {
        let workers = Worker::<ThreadPoolMessage>::new_fifo();
        /*
            Comment for following two mpsc channels.
            The one with ending `monitor` is for monitor thread to know whether
            a thread is paniced. Every worker thread will keep a sender of this
            channel. When worker thread exit it will send its status to monitor.

            Another one with ending `worker` is for forwarding messages between
            monitor and the object `self`. As variable `workers` defined above
            cannot move between thread, and monitor thread need it to product
            `Stealer` for new worker thread, the ownership should be moved into
            monitor thread rather than be kept by object `self`.
        */
        let (tx_monitor, rx_monitor) = channel::<QuitStatus>();
        let (tx_worker, rx_worker) = channel::<ThreadPoolMessage>();
        for _ in 0..threads {
            let stealer = workers.stealer();
            let sender = tx_monitor.clone();
            thread::spawn(move || {
                let _monitor = ThreadMonitor { sender };
                get_job_and_exec(stealer);
            });
        }

        thread::spawn(move || loop {
            if let Some(status) = rx_monitor.try_recv().ok() {
                match status {
                    QuitStatus::Panic => {
                        let sender = tx_monitor.clone();
                        let stealer = workers.stealer();
                        /* If one worker thread paniced, let it go and spawn a new one */
                        thread::spawn(move || {
                            let _monitor = ThreadMonitor { sender };
                            get_job_and_exec(stealer);
                        });
                    }
                    QuitStatus::Normal => {}
                };
            }
            if let Some(message) = rx_worker.try_recv().ok() {
                match message {
                    ThreadPoolMessage::RunJob(job) => workers.push(ThreadPoolMessage::RunJob(job)),
                    ThreadPoolMessage::Shutdown => {
                        workers.push(ThreadPoolMessage::Shutdown);
                        /* stop to monitor other threads */
                        break;
                    }
                }
            }
        });

        Ok(SharedQueueThreadPool { tx_worker, threads })
    }

    /// Send job via a mpsc channel to monitor thread
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.tx_worker
            .send(ThreadPoolMessage::RunJob(Box::new(job)))
            .unwrap();
    }
}

impl Drop for SharedQueueThreadPool {
    /// Send shutdown message to monitor thread, it will forward this message
    /// to all worker threads.
    fn drop(&mut self) {
        for _ in 0..self.threads {
            self.tx_worker.send(ThreadPoolMessage::Shutdown).unwrap();
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

enum QuitStatus {
    Panic,
    Normal,
}

/// When one instance of this struct dropped, it stands for a worker thread
/// is ended (panic or normal exit). The drop() function then will tell
/// monitor about this.
struct ThreadMonitor {
    sender: Sender<QuitStatus>,
}

impl Drop for ThreadMonitor {
    fn drop(&mut self) {
        if thread::panicking() {
            self.sender.send(QuitStatus::Panic).unwrap();
        } else {
            self.sender.send(QuitStatus::Normal).unwrap();
        }
    }
}
