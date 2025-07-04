use std::fmt::Display;
use std::io::BufRead;
use std::io::Read;
use std::io::Write;
use std::net;
use std::io;
use std::mem;
use std::fmt;

use crate::{Result, Command};
use crate::serialize;
use crate::models;

const SERVER_VERSION: u8 = 1u8;

pub struct KvsServer {
    
}

impl KvsServer {
    pub fn new() -> KvsServer {
        return KvsServer {}
    }

    fn read_header(stream: &mut dyn io::Read) -> Result<models::RequestHeader> {
        Ok(
            models::RequestHeader{
                version: serialize::ReadFromStream::deserialize(stream)?,
                keep_alive: serialize::ReadFromStream::deserialize(stream)?,
                command_count: serialize::ReadFromStream::deserialize(stream)?,
                reserved: serialize::ReadFromStream::deserialize(stream)?,
            }
        )
    }

    fn read_command(stream: &mut io::BufReader<&net::TcpStream>) -> Result<Option<Command>> {
        serialize::deserialize(stream)
    }

    fn handle_request(request: models::Request) -> Result<()> {
        for cmd in request.commands {
            log::info!("Hanlding command {}", cmd);
        }

        Ok(())
    }

    fn handle_connection(mut stream: &net::TcpStream) -> Result<()> {
        log::debug!("Handling incoming connection");

        loop {
            let mut reader = io::BufReader::new(stream);
            let header = Self::read_header(&mut reader)?;
            if header.version > SERVER_VERSION {
                return Err(
                    Box::from(
                        format!("Unsupported request versin {}, server version: {}", header.version, SERVER_VERSION)
                    )
                )
            }
            let keep_alive = header.keep_alive != 0;
            let mut commands = Vec::new();
            for _ in 0..header.command_count {
                let cmd = Self::read_command(&mut reader)?;
                if cmd.is_none() {
                    return Err(
                        Box::from(
                            format!("Expected {} commands, found {}", header.command_count, commands.len())
                        )
                    );
                }
                commands.push(cmd.unwrap());
            }
            drop(reader);

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

    pub fn listen(&self, host: String, port: u32) -> Result<()> {
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
