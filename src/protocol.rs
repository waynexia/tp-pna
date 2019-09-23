extern crate bincode;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::io::prelude::*;
use std::net::TcpStream;

/// Stands for three possible operations defined in `KvsEngine`
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum OpType {
    /// For `set` operation
    Set,
    /// For `get` operation
    Get,
    /// For `rm` or `remove` operation
    Remove,
}

/// Command body struct
#[derive(Serialize, Deserialize, Debug)]
pub struct Command {
    /// Specify what current operation is
    pub op: OpType,
    /// The key in this command / operation. 
    pub key: String,
    /// The value. This field could be None as
    /// `get` and `remove` opertaion need not to provide it.
    pub value: Option<String>,
}

/// This enum is used in server-to-client communication flow.
/// Implies the status or execution result.
pub enum Status {
    /// Command executes successfully
    Ok,
    /// A error is reported 
    Error,
    /// No error found, system sends some message
    Message,
}

/// This struct is for communication between kvs client and server.
///
/// It provieds basic `send` and `receive` function based on `std::net::TcpStream`.
/// Command and result's constructor and parser are also included.
pub struct Protocol<'a> {
    stream: &'a TcpStream,
}

impl<'a> Protocol<'a> {
    /// Create a protocol handle.
    pub fn new(stream: &mut TcpStream) -> Protocol {
        Protocol { stream }
    }

    /// Send a serializable content.
    pub fn send<T: Serialize>(&mut self, content: &T) -> Result<&'a mut Protocol> {
        let serialized = bincode::serialize(content).unwrap();
        let len = bincode::serialize(&serialized.len()).unwrap();
        self.stream.write(&len)?;
        self.stream.write(&serialized)?;
        Ok(self)
    }

    /// To receive a object in given type.
    /// This function will block until read enough data.
    pub fn receive<T>(&mut self) -> Result<T>
    where
        for<'de> T: Deserialize<'de>,
    {
        let mut rcv_message = vec![0; 8];
        self.stream.read_exact(&mut rcv_message)?;
        let len: usize = bincode::deserialize(&rcv_message).unwrap();
        rcv_message.resize(len, 0);
        self.stream.read_exact(&mut rcv_message)?;
        let de: T = bincode::deserialize(&rcv_message).unwrap();
        Ok(de)
    }

    /// Parse the returned message from server for client.
    pub fn parse_result(result: &str) -> Result<(Status, String)> {
        match result.chars().nth(0) {
            Some('+') => return Ok((Status::Ok, result.get(1..).unwrap().to_string())),
            Some('-') => return Ok((Status::Error, result.get(1..).unwrap().to_string())),
            Some('*') => return Ok((Status::Message, result.get(1..).unwrap().to_string())),
            _ => unreachable!(),
        }
    }
}
