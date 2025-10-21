use crate::threads::base;
use crate::models;


pub struct NaiveThreadPool {
    thread_handlers: std::sync::Mutex<Vec<std::thread::JoinHandle<()>>>,
}


impl NaiveThreadPool {
    pub fn new() -> Self {
        NaiveThreadPool { thread_handlers: std::sync::Mutex::new(Vec::new()) }
    }
}

impl base::ThreadPool for NaiveThreadPool {
    fn spawn<F>(&self, job: F) -> models::Result<()> where F: FnOnce() + Send + 'static {
        let handle = std::thread::spawn(job);
        let mut handlers_list = self.thread_handlers.lock().unwrap_or_else(|e| e.into_inner());
        handlers_list.push(handle);
        Ok(())
    }
}

impl Drop for NaiveThreadPool {
    fn drop(&mut self) {
        // Drain and join all remaining handles
        let mut handles = self.thread_handlers.lock().unwrap_or_else(|e| e.into_inner());
        while let Some(handle) = handles.pop() {
            if let Err(err) = handle.join() {
                log::error!("Thread panicked: {:?}", err);
            }
        }
    }
}
