pub trait ThreadPool {
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}
