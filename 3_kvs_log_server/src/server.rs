use std::net;
use std::io;
use std::io::{Read, Write};

use crate::models;
use crate::serialize;
use crate::serialize::WriteToBuffer;

const SERVER_VERSION: u8 = 1u8;

pub struct KvsServer {
    
}

impl KvsServer {
    pub fn new() -> KvsServer {
        return KvsServer {}
    }

    fn read_header(stream: &mut dyn io::Read) -> models::Result<models::RequestHeader> {
        Ok(
            models::RequestHeader{
                version: serialize::ReadFromStream::deserialize(stream)?,
                keep_alive: serialize::ReadFromStream::deserialize(stream)?,
                command_count: serialize::ReadFromStream::deserialize(stream)?,
                body_size: serialize::ReadFromStream::deserialize(stream)?,
                reserved: serialize::ReadFromStream::deserialize(stream)?,
            }
        )
    }

    fn serialize_response(responses: Vec<models::ResponseCommand>) -> models::Result<Vec<u8>> {
        let command_count = responses.len();
        let mut body_buffer = Vec::new();
        for response in responses {
            match response {
                models::ResponseCommand::Get { value } => {
                    body_buffer.write(&[b'g'])?;
                    serialize::serialize_str(&value, &mut body_buffer);
                },
                models::ResponseCommand::Set {} => {
                    body_buffer.write(&[b's'])?;
                },
                models::ResponseCommand::Remove {} => {
                    body_buffer.write(&[b'r'])?;
                },
            };
        }

        let header =  models::ResponseHeader{
            version: SERVER_VERSION,
            reserved_1: 0u8,
            command_count: command_count as u16,
            body_size: body_buffer.len() as u32,
            reserved_2: 0u32,
        };

        let mut response_buffer = Vec::new();
        response_buffer.reserve(size_of::<models::ResponseHeader>() + body_buffer.len());
        header.version.serialize(&mut response_buffer)?;
        header.reserved_1.serialize(&mut response_buffer)?;
        header.command_count.serialize(&mut response_buffer)?;
        header.body_size.serialize(&mut response_buffer)?;
        header.reserved_2.serialize(&mut response_buffer)?;
        response_buffer.extend(body_buffer.iter());

        Ok(response_buffer)
    }

    fn handle_request(request: models::Request) -> models::Result<Vec<models::ResponseCommand>> {
        let mut responses = Vec::new();
        for command in request.commands {
            log::info!("Hanlding command {}", command);
            let response_command = match command {
                models::Command::Get { key: _ } => models::ResponseCommand::Get{value: String::from("value")},
                models::Command::Set { key: _, value: _ } => models::ResponseCommand::Set{},
                models::Command::Remove { key: _ } => models::ResponseCommand::Remove{},
            };
            responses.push(response_command);
        }

        Ok(responses)
    }

    fn handle_connection(mut stream: &net::TcpStream) -> models::Result<()> {
        log::debug!("Handling incoming connection");

        loop {
            let mut reader = io::BufReader::new(stream);
            let header = Self::read_header(&mut reader)?;
            if header.version > SERVER_VERSION {
                return Err(
                    Box::from(
                        format!("Unsupported request version {}, server version: {}", header.version, SERVER_VERSION)
                    )
                )
            }
            let keep_alive = header.keep_alive != 0;

            log::debug!("Body size {}", header.body_size);
            
            let mut body_buffer = Vec::new();
            body_buffer.resize(header.body_size as usize, 0u8);
            reader.read_exact(body_buffer.as_mut_slice())?;
            drop(reader);
            
            let mut body_reader = io::Cursor::new(body_buffer);
            let mut commands = Vec::new();
            for _ in 0..header.command_count {
                let cmd = serialize::deserialize(&mut body_reader)?;
                if cmd.is_none() {
                    return Err(
                        Box::from(
                            format!("Expected {} commands, found {}", header.command_count, commands.len())
                        )
                    );
                }
                commands.push(cmd.unwrap());
            }
            drop(body_reader);

            let request = models::Request{
                header: header,
                commands: commands,
            };
            log::debug!("Handling request {}", request);
            let responses = Self::handle_request(request)?;

            let response_data = Self::serialize_response(responses)?;
            log::debug!("{}", String::from_utf8_lossy(&response_data));
            let mut writer = io::BufWriter::new(&mut stream);
            writer.write(response_data.as_slice())?;
            writer.flush()?;
            drop(writer);

            if keep_alive {
                log::debug!("Request handled, keep connection alive");
                continue;
            } else {
                log::debug!("Request handled, close connection");
                return Ok(())
            }
        }
    }

    pub fn listen(&self, host: String, port: u32) -> models::Result<()> {
        let addr = format!("{}:{}", host, port);
        let listener = net::TcpListener::bind(addr)?;

        for connection_result in listener.incoming() {
            match connection_result {
                Ok(mut stream) => {
                    match Self::handle_connection(&mut stream) {
                        Ok(_) => {},
                        Err(err) => {
                            log::error!("Request handling error: {}", err);
                        }
                    }
                    match stream.shutdown(std::net::Shutdown::Both) {
                        Ok(_) => {},
                        Err(err) => {
                            log::error!("Cannot close TCP stream: {}", err);
                        }
                    }
                },
                Err(err) => {
                    log::error!("Cannot handle incoming connection: {}", err);
                }
            }
        }

        Ok(())
    }
}
