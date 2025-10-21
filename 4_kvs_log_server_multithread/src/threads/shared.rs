use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};

use crate::threads::base;
use crate::models;

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct SimpleThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}


impl SimpleThreadPool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0, "ThreadPool size must be greater than zero");

        let (sender, receiver) = mpsc::channel::<Message>();
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        SimpleThreadPool { workers, sender }
    }
}

impl base::ThreadPool for SimpleThreadPool {
    fn spawn<F>(&self, job: F) -> models::Result<()> where F: FnOnce() + Send + 'static {
        let job = Box::new(job);
        let _ = self.sender.send(Message::NewJob(job));
        Ok(())
    }
}

impl Drop for SimpleThreadPool {
    fn drop(&mut self) {
        // send terminate message for each worker
        for _ in &self.workers {
            let _ = self.sender.send(Message::Terminate);
        }

        // join all worker threads
        for worker in &mut self.workers {
            if let Some(handle) = worker.thread.take() {
                let _ = handle.join();
            }
        }
    }
}

enum Message {
    NewJob(Job),
    Terminate,
}

struct Worker {
    id: usize,
    thread: Option<JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = {
                // lock and receive
                let lock = receiver.lock().expect("Worker receiver lock poisoned");
                lock.recv()
            };

            match message {
                Ok(Message::NewJob(job)) => {
                    // execute the job
                    job();
                }
                Ok(Message::Terminate) => {
                    break;
                }
                Err(_) => {
                    // channel disconnected, exit
                    break;
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}