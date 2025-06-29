use std::io::Read;
use std::io::Write;
use std::net;
use std::io;

use crate::{Result, Command};


pub struct KvsClient {
    socket_opt: Option<net::TcpStream>,
}

impl Drop for KvsClient {
    fn drop(&mut self) {
        if let Some(err) = self.close().err() {
            log::error!("Cannot close client: {}", err);
        }
    }
}

impl KvsClient {
    pub fn new() -> Self {
        KvsClient { socket_opt: None }
    }

    pub fn connect(&mut self, host: String, port: u32) -> Result<()> {
        let addr = format!("{}:{}", host, port);
        log::debug!("Connecting to {}...", addr);
        let socket = net::TcpStream::connect(addr)?;
        self.socket_opt = Some(socket);
        log::debug!("Connected");
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        if !self.is_connected() {
            return Ok(());
        }

        let socket = self.socket_opt.as_mut().unwrap();
        socket.flush()?;
        socket.shutdown(net::Shutdown::Both)?;
        self.socket_opt = None;

        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        return self.socket_opt.is_some();
    }

    pub fn send(&mut self, data: Vec<u8>) -> Result<Vec<u8>> {
        if !self.is_connected() {
            return Err(Box::from(format!("Client is not ready")));
        }

        let mut socket = self.socket_opt.as_mut().unwrap();

        log::debug!("Request: {:?}", data);
        
        let mut writer = io::BufWriter::new(&mut socket);
        writer.write(data.as_slice())?;
        writer.write(&[0u8; 1])?;
        writer.flush()?;
        drop(writer);

        let mut reader = io::BufReader::new(&mut socket);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        log::debug!("Response: {:?}", buffer);
        
        Ok(buffer)
    }
}
