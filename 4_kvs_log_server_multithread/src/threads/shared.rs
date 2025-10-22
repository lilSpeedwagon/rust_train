use crossbeam::deque;

use crate::threads::base;
use crate::models;

type Job = Box<dyn FnOnce() + Send + 'static>;

enum SharedMessage {
    NewJob(Job),
    Terminate,
}

pub struct SharedThreadPool {
    injector: std::sync::Arc<deque::Injector<SharedMessage>>,
    threads: Vec<std::thread::JoinHandle<()>>,
}


impl SharedThreadPool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0, "ThreadPool size must be greater than zero");

        let injector = std::sync::Arc::new(deque::Injector::<SharedMessage>::new());

        let mut threads = Vec::with_capacity(size);
        for _ in 0..size {
            let injector_ptr = injector.clone();
            let thread_handle = std::thread::spawn(
                move || {
                    loop {
                        match injector_ptr.steal() {
                            deque::Steal::Empty | deque::Steal::Retry => {
                                std::thread::sleep(std::time::Duration::from_millis(10));
                            },
                            deque::Steal::Success(msg) => {
                                match msg {
                                    SharedMessage::NewJob(job) => {
                                        job();
                                    },
                                    SharedMessage::Terminate => {
                                        break;
                                    },
                                }
                            }
                        }
                    }
                }
            );
            threads.push(thread_handle);
        }

        SharedThreadPool {
            injector: injector,
            threads: threads,
        }
    }
}

impl base::ThreadPool for SharedThreadPool {
    fn spawn<F>(&mut self, job: F) -> models::Result<()> where F: FnOnce() + Send + 'static {
        let job = Box::new(job);
        self.injector.push(SharedMessage::NewJob(job));
        Ok(())
    }
}

impl Drop for SharedThreadPool {
    fn drop(&mut self) {
        for _ in &self.threads {
            self.injector.push(SharedMessage::Terminate);
        }

        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}
