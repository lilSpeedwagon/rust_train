use std::io;
use std::result;
use std::mem;

use crate::models::{Command, Result};


pub trait ReadFromStream {
    fn deserialize(stream: &mut dyn io::Read) -> result::Result<Self, io::Error> where Self: Sized;
}


macro_rules! impl_read_from_stream {
    ($($t:ty),*) => {
        $(
            impl ReadFromStream for $t {
                fn deserialize(stream: &mut dyn io::Read) -> result::Result<Self, io::Error> {
                    const TYPE_SIZE: usize = mem::size_of::<$t>();
                    let mut buffer = [0u8; TYPE_SIZE];

                    let bytes_count = stream.read(&mut buffer)?;
                    if bytes_count != TYPE_SIZE {
                        return Err(
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("Not enough bytes to read {}", std::any::type_name::<$t>()),
                            ),
                        );
                    }

                    Ok(<$t>::from_be_bytes(buffer))
                }
            }
        )*
    };
}

impl_read_from_stream!(u8, u16, u32, u64);


pub trait WriteToBuffer {
    fn serialize(&self, buffer: &mut Vec<u8>) -> result::Result<(), io::Error>;
}

macro_rules! impl_write_to_stream {
    ($($t:ty),*) => {
        $(
            impl WriteToBuffer for $t {
                fn serialize(&self, buffer: &mut Vec<u8>) -> result::Result<(), io::Error> {
                    buffer.extend(self.to_be_bytes());
                    Ok(())
                }
            }
        )*
    };
}

impl_write_to_stream!(u8, u16, u32, u64);


pub fn serialize_str(s: &String, buffer: &mut Vec<u8>) {
    let len = s.len() as u32;
    buffer.extend(len.to_be_bytes());
    buffer.extend(s.as_bytes());
}


pub fn deserialize_str<T: io::Read>(reader: &mut T) -> Result<String> {
    let mut size_buffer = [0u8; 4];
    reader.read_exact(&mut size_buffer)?;
    let size = u32::from_be_bytes(size_buffer) as usize;

    let mut str_buffer = vec![0u8; size];
    str_buffer.reserve(size);
    reader.read_exact(&mut str_buffer[..])?;

    Ok(String::from_utf8(str_buffer)?)
}


pub fn serialize(command: &Command) -> Vec<u8> {
    match command {
        Command::Set { key, value } => {
            let mut buffer: Vec<u8> = Vec::new();
            buffer.extend(b"s");
            serialize_str(key, &mut buffer);
            serialize_str(value, &mut buffer);
            return buffer;
        },
        Command::Get { key } => {
            let mut buffer: Vec<u8> = Vec::new();
            buffer.extend(b"g");
            serialize_str(key, &mut buffer);
            return buffer;
        },
        Command::Remove { key } => {
            let mut buffer: Vec<u8> = Vec::new();
            buffer.extend(b"r");
            serialize_str(key, &mut buffer);
            return buffer;
        },
        Command::Reset { } => {
            let mut buffer: Vec<u8> = Vec::new();
            buffer.extend(b"z");
            return buffer;
        },
    }
}


pub fn get_value_offset(command: &Command) -> Option<u64> {
    match command {
        Command::Set { key, value: _ } => Some((b"s".len() + size_of::<u32>() + key.len()) as u64),
        _ => None,
    }
}


pub fn deserialize<T: io::Read>(reader: &mut T) -> Result<Option<Command>> {
    let mut command_buffer = [0u8; 1];
    let bytes_count = reader.read(&mut command_buffer)?;
    if bytes_count == 0 {
        return Ok(None)
    }

    let command_code = u8::from_be_bytes(command_buffer);
    match command_code {
        b's' => {
            let key = deserialize_str(reader)?;
            let value = deserialize_str(reader)?;
            return Ok(Some(Command::Set { key: key, value: value }))
        },
        b'r' => {
            let key = deserialize_str(reader)?;
            return Ok(Some(Command::Remove { key: key }))
        },
        b'g' => {
            let key = deserialize_str(reader)?;
            return Ok(Some(Command::Get { key: key }))
        },
        b'z' => {
            return Ok(Some(Command::Reset {}))
        },
        _ => {
            return Err(
                Box::new(io::Error::new(io::ErrorKind::Other, format!("Unknown command {}", command_code)))
            );
        }
    }
}
