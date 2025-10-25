use crate::threads::base;
use crate::models;


/// Dummy thread pool without actual threads.
/// Each task is blocking and executed in the same thread.
/// If the job panics, it is propagated to the current thread.
pub struct NoneThreadPool {}


impl NoneThreadPool {
    pub fn new() -> Self {
        NoneThreadPool {}
    }
}

impl base::ThreadPool for NoneThreadPool {
    fn spawn(&mut self, job: base::Job) -> models::Result<()> {
        job();
        Ok(())
    }
}
