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


impl<T: ReadFromStream> ReadFromStream for Option<T> {
    fn deserialize(stream: &mut dyn io::Read) -> result::Result<Option<T>, io::Error> {
        let mut buffer = [0u8; 1];
        let bytes_count = stream.read(&mut buffer)?;
        if bytes_count != 1 {
            return Err(
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Not enough bytes to read {}", std::any::type_name::<T>()),
                ),
            );
        }

        let has_value = buffer[0] != 0;
        if has_value {
            let value = T::deserialize(stream)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
}


impl ReadFromStream for String {
    fn deserialize(stream: &mut dyn io::Read) -> result::Result<String, io::Error> {
        let mut size_buffer = [0u8; 4];
        stream.read_exact(&mut size_buffer)?;
        let size = u32::from_be_bytes(size_buffer) as usize;

        let mut str_buffer = vec![0u8; size];
        str_buffer.reserve(size);
        stream.read_exact(&mut str_buffer[..])?;

        match String::from_utf8(str_buffer) {
            Ok(result) => Ok(result),
            Err(err) => Err(io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
        }
    }
}


pub trait WriteToStream {
    fn serialize(&self, buffer: &mut Vec<u8>) -> result::Result<(), io::Error>;
}

macro_rules! impl_write_to_stream {
    ($($t:ty),*) => {
        $(
            impl WriteToStream for $t {
                fn serialize(&self, buffer: &mut Vec<u8>) -> result::Result<(), io::Error> {
                    buffer.extend(self.to_be_bytes());
                    Ok(())
                }
            }
        )*
    };
}

impl_write_to_stream!(u8, u16, u32, u64);


impl<T: WriteToStream> WriteToStream for Option<T> {
    fn serialize(&self, buffer: &mut Vec<u8>) -> result::Result<(), io::Error> {
        let has_value = self.is_some();
        let bytes = if has_value {[1u8]} else {[0u8]};
        buffer.extend(bytes);

        if self.is_some() {
            self.as_ref().unwrap().serialize(buffer)?;
        }
        Ok(())
    }
}


impl WriteToStream for String {
    fn serialize(&self, buffer: &mut Vec<u8>) -> result::Result<(), io::Error> {
        let len = self.len() as u32;
        buffer.extend(len.to_be_bytes());
        buffer.extend(self.as_bytes());
        Ok(())
    }
}


pub fn serialize(command: &Command) -> result::Result<Vec<u8>, io::Error> {
    match command {
        Command::Set { key, value } => {
            let mut buffer: Vec<u8> = Vec::new();
            buffer.extend(b"s");
            key.serialize(&mut buffer)?;
            value.serialize(&mut buffer)?;
            return Ok(buffer);
        },
        Command::Get { key } => {
            let mut buffer: Vec<u8> = Vec::new();
            buffer.extend(b"g");
            key.serialize(&mut buffer)?;
            return Ok(buffer);
        },
        Command::Remove { key } => {
            let mut buffer: Vec<u8> = Vec::new();
            buffer.extend(b"r");
            key.serialize(&mut buffer)?;
            return Ok(buffer);
        },
        Command::Reset { } => {
            let mut buffer: Vec<u8> = Vec::new();
            buffer.extend(b"z");
            return Ok(buffer);
        },
    }
}


pub fn get_value_offset(command: &Command) -> Option<u64> {
    // Get offset in bytes from the serialized command start till it's stored value if some.
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
            let key = String::deserialize(reader)?;
            let value = String::deserialize(reader)?;
            return Ok(Some(Command::Set { key: key, value: value }))
        },
        b'r' => {
            let key = String::deserialize(reader)?;
            return Ok(Some(Command::Remove { key: key }))
        },
        b'g' => {
            let key = String::deserialize(reader)?;
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
