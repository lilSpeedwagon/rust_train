use std::io::BufRead;
use std::io::Read;
use std::io::Write;
use std::net;
use std::io;

use crate::Result;

pub struct KvsServer {
    
}

struct Request {
    content: Vec<u8>,
}

struct Response {
    content: Vec<u8>,
}

impl KvsServer {
    pub fn new() -> KvsServer {
        return KvsServer {}
    }

    fn handle(&self, request: Request) -> Result<Response> {
        // Handle as simple echo server.
        Ok(Response { content: request.content })
    }

    pub fn listen(&self, host: String, port: u32) -> Result<()> {
        let addr = format!("{}:{}", host, port);
        let listener = net::TcpListener::bind(addr)?;

        for connection_result in listener.incoming() {
            match connection_result {
                Ok(mut stream) => {
                    log::debug!("Handling incoming connection");

                    let mut reader = io::BufReader::new(&mut stream);
                    let mut buffer = Vec::new();
                    match reader.read_until(0, &mut buffer) {
                        Ok(_) => {},
                        Err(err) => {
                            log::error!("Cannot read from stream: {}", err);
                        }
                    }
                    drop(reader);

                    log::debug!("Incoming: {}", String::from_utf8(buffer.clone())?);

                    match self.handle(Request { content: buffer }) {
                        Ok(response) => { 
                            log::debug!("Response: {}", String::from_utf8(response.content.clone())?);
                            
                            let mut writer = io::BufWriter::new(&mut stream);
                            match writer.write(response.content.as_slice()) {
                                Ok(_) => {},
                                Err(err) => {
                                    log::error!("Cannot write response to stream: {}", err);
                                }
                            }
                        },
                        Err(e) => {
                            log::error!("Request handling error: {}", e);
                        }
                    }

                    match stream.flush() {
                        Ok(_) => {},
                        Err(err) => {
                            log::error!("Cannot flush data to TCP stream: {}", err);
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
