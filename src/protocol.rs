extern crate bincode;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

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
pub fn protocol_send<T: Serialize>(stream: &mut TcpStream, content: &T) -> Result<()> {
    let serialized = bincode::serialize(content).unwrap();
    let len = bincode::serialize(&serialized.len()).unwrap();
    stream.write(&len)?;
    stream.write(&serialized)?;
    println!("sended!");
    Ok(())
}

///
pub fn protocol_receive<T>(stream: &mut TcpStream) -> Result<T>
where
    for<'de> T: Deserialize<'de>,
{
    let mut rcv_message = vec![0; 8];
    println!("{:?}", rcv_message);
    stream.read_exact(&mut rcv_message)?;
    println!("{:?}", rcv_message);
    let len: usize = bincode::deserialize(&rcv_message).unwrap();
    println!("{}", len);
    rcv_message.resize(len, 0);
    stream.read_exact(&mut rcv_message)?;
    let de: T = bincode::deserialize(&rcv_message).unwrap();
    Ok(de)
}
