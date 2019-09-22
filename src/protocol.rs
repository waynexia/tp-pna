extern crate bincode;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::io::prelude::*;
use std::net::TcpStream;

#[derive(Serialize, Deserialize, Debug)]
enum OpType {
    Set,
    Get,
    Remove,
}

#[derive(Serialize, Deserialize, Debug)]
struct Command {
    op: OpType,
    key: String,
    value: Option<String>,
}

///
pub enum Status {
    ///
    Ok,
    ///
    Error,
    ///
    Message,
}

///
pub struct Protocol<'a> {
    stream: &'a TcpStream,
}

impl<'a> Protocol<'a> {
    ///
    pub fn new(stream: &mut TcpStream) -> Protocol {
        Protocol { stream }
    }

    ///
    pub fn send<T: Serialize>(&mut self, content: &T) -> Result<&'a mut Protocol> {
        let serialized = bincode::serialize(content).unwrap();
        let len = bincode::serialize(&serialized.len()).unwrap();
        self.stream.write(&len)?;
        self.stream.write(&serialized)?;
        Ok(self)
    }

    ///
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

    ///
    pub fn parse_result(result: &str) -> Result<(Status, String)> {
        match result.chars().nth(0) {
            Some('+') => return Ok((Status::Ok, result.get(1..).unwrap().to_string())),
            Some('-') => return Ok((Status::Error, result.get(1..).unwrap().to_string())),
            Some('*') => return Ok((Status::Message, result.get(1..).unwrap().to_string())),
            _ => unreachable!(),
        }
    }
}
