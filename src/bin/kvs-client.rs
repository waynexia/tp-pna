extern crate bincode;
extern crate clap;
use clap::App;
use kvs::{protocol_receive, protocol_send, KvStore, KvsError, Result};
use serde::{Deserialize, Serialize};
use std::io::prelude::*;
use std::net::TcpStream;
use std::path::Path;
use std::process::exit;

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

    let mut command = Command {
        op: OpType::Get,
        key: String::new(),
        value: None,
    };
    let mut addr = String::new();

    match matches.subcommand() {
        ("get", Some(sub_m)) => {
            command.key = sub_m.value_of("key").unwrap().to_owned();
            addr = sub_m
                .value_of("addr")
                .unwrap_or("127.0.0.1:4000")
                .to_owned();
        }
        ("set", Some(sub_m)) => {
            command.op = OpType::Set;
            command.key = sub_m.value_of("key").unwrap().to_owned();
            command.value = Some(sub_m.value_of("value").unwrap().to_owned());
            addr = sub_m
                .value_of("addr")
                .unwrap_or("127.0.0.1:4000")
                .to_owned();
        }
        ("rm", Some(sub_m)) => {
            command.op = OpType::Remove;
            command.key = sub_m.value_of("key").unwrap().to_owned();
            addr = sub_m
                .value_of("addr")
                .unwrap_or("127.0.0.1:4000")
                .to_owned();
        }

        _ => unreachable!(),
    }

    let mut stream = TcpStream::connect(&addr)?;
    protocol_send(&mut stream, &command)?;

    let de: String = protocol_receive(&mut stream)?;
    println!("{:?}", de);

    Ok(())
}
