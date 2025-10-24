use rayon;

use crate::threads::base::{Job, ThreadPool};
use crate::models;

pub struct RayonThreadPool {
    internal_pool: rayon::ThreadPool,
}

/// A thread pool wrapper for the `rayon` implementation. 
impl RayonThreadPool {
    pub fn new(size: usize) -> models::Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new().num_threads(size).build()?;
        Ok(RayonThreadPool { internal_pool: pool })
    }
}

impl ThreadPool for RayonThreadPool {
    fn spawn(&mut self, job: Job) -> models::Result<()> {
        self.internal_pool.install(job);
        Ok(())
    }
}
