use crate::threads::base;


pub struct NaiveThreadPool {}


impl NaiveThreadPool {
    pub fn new() -> Self {
        NaiveThreadPool {}
    }
}

impl base::ThreadPool for NaiveThreadPool {
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static{
        std::thread::spawn(job);
    }
}
