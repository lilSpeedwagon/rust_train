use std::net;
use std::io;
use std::io::{Read, Write};

use crate::serialize;
use crate::models;

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

    fn handle_request(request: models::Request) -> models::Result<()> {
        for cmd in request.commands {
            log::info!("Hanlding command {}", cmd);
        }

        Ok(())
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

            Self::handle_request(request)?;

            let mut writer = io::BufWriter::new(&mut stream);
            let response = b"";
            writer.write(response.as_slice())?;
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
