use std::error::Error;

#[derive(Clone)]
pub enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;
