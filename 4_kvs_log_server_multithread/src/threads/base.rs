use crate::models;

pub type Job = Box<dyn FnOnce() + Send + 'static>;

pub trait ThreadPool {
    fn spawn(&mut self, job: Job) -> models::Result<()>;
}
