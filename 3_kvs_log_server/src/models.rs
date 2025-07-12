use std::error::Error;
use std::fmt;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Clone)]
pub enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Set {key, value} => write!(f, "Set<key={}, value={}>", key, value),
            Command::Get {key} => write!(f, "Get<key={}>", key),
            Command::Remove {key} => write!(f, "Remove<key={}>", key),
        }
    }
}

pub struct RequestHeader {
    pub version: u8,
    pub keep_alive: u8,
    pub command_count: u16,
    pub body_size: u32,
    pub reserved: u32,
}

pub struct Request {
    pub header: RequestHeader,
    pub commands: Vec<Command>,
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "<version={}; keep_alive={}; command_count={}, body_size={}>",
            self.header.version,
            self.header.keep_alive,
            self.header.command_count,
            self.header.body_size,
        )
    }
}

pub struct ResponseHeader {
    pub version: u8,
    pub reserved_1: u8,
    pub command_count: u16,
    pub body_size: u32,
    pub reserved_2: u32,
}

pub enum ResponseCommand {
    Set {},
    Get { value: String },
    Remove {},
}

pub struct Response {
    pub header: ResponseHeader,
    pub commands: Vec<ResponseCommand>,
}

impl fmt::Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "<version={}; command_count={}; body_size={}>",
            self.header.version,
            self.header.command_count,
            self.header.body_size,
        )
    }
}
