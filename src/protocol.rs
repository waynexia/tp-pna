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
        println!("sended!");
        Ok(self)
    }

    ///
    pub fn receive<T>(&mut self) -> Result<T>
    where
        for<'de> T: Deserialize<'de>,
    {
        let mut rcv_message = vec![0; 8];
        println!("{:?}", rcv_message);
        self.stream.read_exact(&mut rcv_message)?;
        println!("{:?}", rcv_message);
        let len: usize = bincode::deserialize(&rcv_message).unwrap();
        println!("{}", len);
        rcv_message.resize(len, 0);
        self.stream.read_exact(&mut rcv_message)?;
        let de: T = bincode::deserialize(&rcv_message).unwrap();
        Ok(de)
    }
}
