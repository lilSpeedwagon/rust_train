use std::{fmt::format, io::{Error, ErrorKind, Read}};
use crate::models::{Command, Result};


fn serialize_str(s: &String, buffer: &mut Vec<u8>) {
    let len = s.len() as u16;
    buffer.extend(len.to_be_bytes());
    buffer.extend(s.as_bytes());
}


fn deserialize_str<T: Read>(reader: &mut T) -> Result<String> {
    let mut size_buffer = [0u8, 2];
    reader.read_exact(&mut size_buffer)?;
    let size = u16::from_be_bytes(size_buffer) as usize;

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
            return Vec::new();
        },
        Command::Remove { key } => {
            let mut buffer: Vec<u8> = Vec::new();
            buffer.extend(b"r");
            serialize_str(key, &mut buffer);
            return buffer;
        }
    }
}


pub fn deserialize<T: Read>(reader: &mut T) -> Result<Option<Command>> {
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
        _ => {
            return Err(Box::new(Error::new(ErrorKind::Other, format!("Unknown command {}", command_code))));
        }
    }
}
