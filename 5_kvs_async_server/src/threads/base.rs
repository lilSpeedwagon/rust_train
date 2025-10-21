use crate::models;

pub trait ThreadPool {
    fn spawn<F>(&self, job: F) -> models::Result<()> where F: FnOnce() + Send + 'static;
}
