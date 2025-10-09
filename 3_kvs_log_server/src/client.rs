use std::io::Write;
use std::net;
use std::io;
use std::time;

use crate::models;
use crate::serialize;
use crate::serialize::WriteToBuffer;


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

    pub fn connect(&mut self, host: String, port: u32, timeout: time::Duration) -> models::Result<()> {
        let addr = format!("{}:{}", host, port);
        log::debug!("Connecting to {}...", addr);
        let socket = net::TcpStream::connect(addr)?;
        socket.set_read_timeout(Some(timeout))?;
        self.socket_opt = Some(socket);
        log::debug!("Connected. Read timeout {}s", timeout.as_secs_f32());
        Ok(())
    }

    pub fn close(&mut self) -> models::Result<()> {
        if !self.is_connected() {
            return Ok(());
        }

        let socket = self.socket_opt.as_mut().unwrap();
        let _ = socket.flush();
        let _ = socket.shutdown(net::Shutdown::Both);
        self.socket_opt = None;

        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        return self.socket_opt.is_some();
    }

    fn serialize_request(commands: Vec<models::Command>, keep_alive: bool) -> models::Result<Vec<u8>> {
        let cmd_count = commands.len();
        let mut cmd_buffer = vec!();
        for cmd in commands {
            let data = serialize::serialize(&cmd);
            cmd_buffer.extend(data);
        }

        let mut keep_alive_value = 1u8;
        if !keep_alive {
            keep_alive_value = 0u8;
        }

        let header = models::RequestHeader{
            version: CLIENT_VERSION,
            keep_alive: keep_alive_value,
            command_count: cmd_count as u16,
            body_size: cmd_buffer.len() as u32,
            reserved: 0,
        };

        let mut buffer = vec!();
        buffer.reserve(size_of::<models::RequestHeader>() + cmd_buffer.len());
        header.version.serialize(&mut buffer)?;
        header.keep_alive.serialize(&mut buffer)?;
        header.command_count.serialize(&mut buffer)?;
        header.body_size.serialize(&mut buffer)?;
        header.reserved.serialize(&mut buffer)?;
        buffer.extend(cmd_buffer);

        Ok(buffer)
    }

    fn read_response(stream: &mut dyn io::Read) -> models::Result<models::Response> {
        let header =  models::ResponseHeader{
            version: serialize::ReadFromStream::deserialize(stream)?,
            reserved_1: serialize::ReadFromStream::deserialize(stream)?,
            command_count: serialize::ReadFromStream::deserialize(stream)?,
            body_size: serialize::ReadFromStream::deserialize(stream)?,
            reserved_2: serialize::ReadFromStream::deserialize(stream)?,
        };
        
        let mut body_buffer = Vec::new();
        body_buffer.resize(header.body_size as usize, 0u8);
        stream.read_exact(body_buffer.as_mut_slice())?;
        let mut body_reader = io::Cursor::new(&mut body_buffer);

        let mut commands= Vec::new();
        commands.reserve(header.command_count as usize);
        for _ in 0..header.command_count {
            let cmd_type: u8 = serialize::ReadFromStream::deserialize(&mut body_reader)?;
            match cmd_type {
                b's' => {
                    commands.push(models::ResponseCommand::Set {});
                },
                b'r' => {
                    commands.push(models::ResponseCommand::Remove {});
                },
                b'g' => {
                    let value = serialize::deserialize_str(&mut body_reader)?;
                    commands.push(models::ResponseCommand::Get { value: value });
                },
                b'z' => {
                    commands.push(models::ResponseCommand::Reset {});
                },
                _ => {
                    return Err(Box::new(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Unknown response command {}", cmd_type)
                    )));
                }
            }
        }
        
        Ok(
            models::Response{
                header: header,
                commands: commands,
            }
        )
    }

    pub fn execute_one(&mut self, command: models::Command, keep_alive: bool) -> models::Result<models::Response> {
        let commands = vec![command];
        self.execute(commands, keep_alive)
    }

    pub fn execute(&mut self, commands: Vec<models::Command>, keep_alive: bool) -> models::Result<models::Response> {
        let serialized_request = Self::serialize_request(commands, keep_alive)?;
        let response = self.send(serialized_request)?;

        if !keep_alive {
            self.close()?;
        }
        
        Ok(response)
    }
    
    pub fn send(&mut self, request_data: Vec<u8>) -> models::Result<models::Response> {
        if !self.is_connected() {
            // TODO autoconnect/disconnect
            return Err(Box::from(format!("Client is not ready")));
        }

        log::debug!("{}", String::from_utf8_lossy(&request_data));

        let mut socket = self.socket_opt.as_mut().unwrap();
        
        let mut writer = io::BufWriter::new(&mut socket);
        writer.write(request_data.as_slice())?;
        writer.flush()?;
        drop(writer);

        let mut reader = io::BufReader::new(&mut socket);
        let response = Self::read_response(&mut reader)?;
        drop(reader);

        log::debug!("Response: {}", response);
        
        Ok(response)
    }
}
