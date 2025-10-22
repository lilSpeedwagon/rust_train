use crossbeam::deque;

use crate::threads::base::ThreadPool;
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

impl ThreadPool for SharedThreadPool {
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


#[test]
fn test_shared_thread_pool_executes_tasks() {
    let pool_size = 4;
    let mut pool = SharedThreadPool::new(pool_size);

    let result = std::sync::Arc::new(std::sync::Mutex::new(0));
    let result_clone = std::sync::Arc::clone(&result);

    pool.spawn(move || {
        let mut num = result_clone.lock().unwrap();
        *num = 42;
    }).unwrap();

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
        pool.spawn(move || {
            let mut num = counter_clone.lock().unwrap();
            *num += 1;
        }).unwrap();
    }

    // Wait for all tasks to complete
    std::thread::sleep(std::time::Duration::from_millis(100));

    assert_eq!(*counter.lock().unwrap(), 10);
}

#[test]
fn test_shared_thread_pool_teardown() {
    let pool_size = 2;
    {
        let mut pool = SharedThreadPool::new(pool_size);

        let flag = std::sync::Arc::new(std::sync::Mutex::new(false));
        let flag_clone = std::sync::Arc::clone(&flag);

        pool.spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            let mut done = flag_clone.lock().unwrap();
            *done = true;
        }).unwrap();

        // Make sure even if the pool is dropped the job is finished.
        drop(pool);

        assert_eq!(*flag.lock().unwrap(), true);
    }
    // If we reach here without panic, teardown worked
}
