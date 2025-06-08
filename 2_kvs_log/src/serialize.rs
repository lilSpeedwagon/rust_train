use crate::models::Command;


fn serialize_str(s: &String, buffer: &mut Vec<u8>) {
    let len = s.len() as u16;
    buffer.extend(len.to_be_bytes());
    buffer.extend(s.as_bytes());
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
