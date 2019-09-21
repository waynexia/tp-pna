extern crate clap;
use serde::{Deserialize, Serialize};
use clap::App;
use kvs::{KvsError, KvStore, Result};
use std::path::Path;
use std::process::exit;
use std::net::TcpStream;
use std::io::prelude::*;

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

fn main() -> Result<()> {
    /* load clap config from yaml file */
    let yaml = clap::load_yaml!("kvs-client-clap.yml");
    let matches = App::from_yaml(yaml)
        .name(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .get_matches();

    let mut kvstore = KvStore::open(&Path::new("./"))?;


    let mut command = Command{
        op: OpType::Get,
        key: String::new(),
        value: None,
    };
    let mut addr = String::new();

    match matches.subcommand() {
        ("get", Some(sub_m)) => {
            // match kvstore.get(sub_m.value_of("key").unwrap().to_owned())? {
            //     Some(value) => println!("{}", value),
            //     None => println!("Key not found"),
            // };
            command.key = sub_m.value_of("key").unwrap().to_owned();
            addr = sub_m.value_of("addr").unwrap_or("127.0.0.1:4000").to_owned();
        }
        ("set", Some(sub_m)) => {
            // kvstore.set(
            //     sub_m.value_of("key").unwrap().to_owned(),
            //     sub_m.value_of("value").unwrap().to_owned(),
            // )?;

            command.op = OpType::Set;
            command.key = sub_m.value_of("key").unwrap().to_owned();
            command.value = Some(sub_m.value_of("value").unwrap().to_owned());
            addr = sub_m.value_of("addr").unwrap_or("127.0.0.1:4000").to_owned();
        }
        ("rm", Some(sub_m)) => {
            // match kvstore.remove(sub_m.value_of("key").unwrap().to_owned()) {
            //     Err(KvsError::KeyNotFound) => {
            //         println!("Key not found");
            //         exit(1);
            //     }
            //     Ok(()) => {},
            //     Err(e) => return Err(e),
            // }
            command.op = OpType::Remove;
            command.key = sub_m.value_of("key").unwrap().to_owned();
            addr = sub_m.value_of("addr").unwrap_or("127.0.0.1:4000").to_owned();
        }

        _ => unreachable!(),
    }


    let mut stream = TcpStream::connect(&addr)?;
    let content = serde_json::to_vec(&command)?;
    let len = serde_json::to_vec(&content.len())?;
    stream.write(&len)?;
    stream.write(&content)?;
    println!("sended!");
    let mut rcv_message = vec![];
    stream.read_to_end(&mut rcv_message)?;
    let de : Command = serde_json::from_slice(&rcv_message)?;
    println!("{:?}",de);

    Ok(())
}
