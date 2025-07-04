use std::io::Read;
use std::io::Write;
use std::net;
use std::io;

use crate::models;
use crate::models::Request;
use crate::serialize;
use crate::serialize::WriteToBuffer;
use crate::{Result, Command};


const CLIENT_VERSION: u8 = 1u8;

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

    fn serialize_request(request: Request) -> Result<Vec<u8>> {
        let mut buffer = vec!();
        request.header.version.serialize(&mut buffer)?;
        request.header.keep_alive.serialize(&mut buffer)?;
        request.header.command_count.serialize(&mut buffer)?;
        request.header.reserved.serialize(&mut buffer)?;

        for cmd in request.commands {
            let data = serialize::serialize(&cmd);
            buffer.extend(data);
        }

        Ok(buffer)
    }

    pub fn execute_one(&mut self, command: Command, keep_alive: bool) -> Result<()> {
        let commands = vec![command];
        self.execute(commands, keep_alive)
    }

    pub fn execute(&mut self, commands: Vec<Command>, keep_alive: bool) -> Result<()> {
        let mut keep_alive_value = 1u8;
        if !keep_alive {
            keep_alive_value = 0u8;
        }
        let header = models::RequestHeader {
            version: CLIENT_VERSION,
            keep_alive: keep_alive_value,
            command_count: commands.len() as u16,
            reserved: 0u32,
        };
        let request = models::Request{
            header: header,
            commands: commands,
        };
        self.send(request)?;

        // TODO keepalive
        
        Ok(())
    }
    
    pub fn send(&mut self, request: models::Request) -> Result<models::Response> {
        if !self.is_connected() {
            // TODO autoconnect/disconnect
            return Err(Box::from(format!("Client is not ready")));
        }

        let mut socket = self.socket_opt.as_mut().unwrap();
        
        log::debug!("Request: {}", request);
        
        let serialized_request = Self::serialize_request(request)?;
        
        let mut writer = io::BufWriter::new(&mut socket);
        writer.write(serialized_request.as_slice())?;
        writer.flush()?;
        drop(writer);

        let mut reader = io::BufReader::new(&mut socket);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        log::debug!("Response: {:?}", buffer);
        
        Ok(models::Response{})
    }
}
