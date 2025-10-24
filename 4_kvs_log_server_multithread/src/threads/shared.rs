use crossbeam::deque;
use log;

use crate::threads::base::{Job, ThreadPool};
use crate::models;

enum SharedMessage {
    NewJob(Job),
    Terminate,
}

pub struct SharedThreadPool {
    injector: std::sync::Arc<deque::Injector<SharedMessage>>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

fn steal_msg(shared_injector: &std::sync::Arc<deque::Injector<SharedMessage>>) -> Option<SharedMessage> {
    match shared_injector.steal() {
        deque::Steal::Empty | deque::Steal::Retry => None,
        deque::Steal::Success(msg) => Some(msg),
    }
}


/// A single thread pool worker function.
/// In polls the injector dequeue for new jobs in a loop.
/// If a terminate message is received, it exits.
fn thread_handle(shared_injector: std::sync::Arc<deque::Injector<SharedMessage>>) {
    loop {
        if let Some(msg) = steal_msg(&shared_injector) {
            match msg {
                SharedMessage::NewJob(job) => {
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job())) {
                        Ok(_) => {},
                        Err(err) => {
                            log::error!("Job in threapool panicked: {}", err.downcast_ref::<&str>().unwrap_or(&""));
                        }
                    }
                },
                SharedMessage::Terminate => {
                    log::debug!("Thread pool worker received the terminate signal. Exiting.");
                    return
                },
            }
        } else {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}


/// A shared thread pool of constant size.
/// Worker threads are preallocated on startup.
/// A concurrent deque is used to distribute jobs between workers.
impl SharedThreadPool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0, "ThreadPool size must be greater than zero");

        let injector = std::sync::Arc::new(deque::Injector::<SharedMessage>::new());

        let mut threads = Vec::with_capacity(size);
        for _ in 0..size {
            let injector_ptr = injector.clone();
            let thread_handle = std::thread::spawn(move || thread_handle(injector_ptr));
            threads.push(thread_handle);
        }

        SharedThreadPool {
            injector: injector,
            threads: threads,
        }
    }
}

impl ThreadPool for SharedThreadPool {
    fn spawn(&mut self, job: Job) -> models::Result<()> {
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
            match thread.join() {
                Ok(_) => {},
                Err(err) => {
                    log::warn!("Thread finished with err: {}", err.downcast_ref::<&str>().unwrap_or(&""));
                }
            }
        }
    }
}


#[test]
fn test_shared_thread_pool_executes_tasks() {
    let pool_size = 4;
    let mut pool = SharedThreadPool::new(pool_size);

    let result = std::sync::Arc::new(std::sync::Mutex::new(0));
    let result_clone = std::sync::Arc::clone(&result);

    pool.spawn(Box::new(move || {
        let mut num = result_clone.lock().unwrap();
        *num = 42;
    })).unwrap();

    // Wait for the task to complete
    std::thread::sleep(std::time::Duration::from_millis(50));

    assert_eq!(*result.lock().unwrap(), 42);
}

#[test]
fn test_shared_thread_pool_multiple_tasks() {
    let pool_size = 2;
    let mut pool = SharedThreadPool::new(pool_size);

    let counter = std::sync::Arc::new(std::sync::Mutex::new(0));

    for _ in 0..10 {
        let counter_clone = std::sync::Arc::clone(&counter);
        pool.spawn(Box::new(move || {
            let mut num = counter_clone.lock().unwrap();
            *num += 1;
        })).unwrap();
    }

    // Wait for all tasks to complete
    std::thread::sleep(std::time::Duration::from_millis(100));

    assert_eq!(*counter.lock().unwrap(), 10);
}

#[test]
fn test_shared_thread_pool_teardown() {
    let pool_size = 2;
    let flag = std::sync::Arc::new(std::sync::Mutex::new(false));
    let flag_clone = std::sync::Arc::clone(&flag);

    {
        let mut pool = SharedThreadPool::new(pool_size);
        pool.spawn(Box::new(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            let mut done = flag_clone.lock().unwrap();
            *done = true;
        })).unwrap();
    }
    
    // Make sure even if the pool is dropped the job is finished.
    assert_eq!(*flag.lock().unwrap(), true);
}

/// Even if a spawned job panics, we still should be able to continue using the pool.
#[test]
fn test_shared_thread_pool_panic() {
    let pool_size = 1;
    {
        // Run a panicking job.
        let mut pool = SharedThreadPool::new(pool_size);
        pool.spawn(Box::new(move || { panic!("Thread panicking!"); })).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        // Make sure we still can run a regular job.
        let result = std::sync::Arc::new(std::sync::Mutex::new(0));
        let result_clone = std::sync::Arc::clone(&result);

        pool.spawn(Box::new(move || {
            let mut num = result_clone.lock().unwrap();
            *num = 42;
        })).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        assert_eq!(*result.lock().unwrap(), 42);
    }
}

