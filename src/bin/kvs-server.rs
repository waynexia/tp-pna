extern crate clap;
use clap::App;
use kvs::{protocol_receive, protocol_send, KvStore, KvsError, Result};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use slog::Drain;

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
    let yaml = clap::load_yaml!("kvs-server-clap.yml");
    let _matches = App::from_yaml(yaml)
        .name(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .get_matches();

    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let log = slog::Logger::root(drain, o!());

    info!(log, "start");

    let mut kvstore = KvStore::open(&Path::new("./"))?;
    // debug!(log, "debug");
    // warn!(log, "warn");
    let listener = TcpListener::bind("127.0.0.1:4000")?;
    for s in listener.incoming() {
        let mut stream = s?;
        let command: Command = protocol_receive(&mut stream)?;
        info!(log, "received command {:?}", command);

        let mut ret_str = String::new();
        match command.op {
            OpType::Get => {
                match kvstore.get(command.key.to_owned())? {
                    Some(value) => ret_str.push_str(&format!("+{}", value)),
                    None => ret_str.push_str("-Key not found"),
                };
            }
            OpType::Set => {
                kvstore.set(command.key.to_owned(), command.value.unwrap().to_owned())?;
                ret_str.push_str("*Done");
            }
            OpType::Remove => {
                match kvstore.remove(command.key.to_owned()) {
                    Err(KvsError::KeyNotFound) => {
                        ret_str.push_str("-Key not found");
                    }
                    Ok(()) => {}
                    Err(e) => ret_str.push_str(&format!("-{}", e.description())),
                };
            }
        }

        info!(log, "will be returned {}", ret_str);
        protocol_send(&mut stream, &ret_str)?;
    }

    Ok(())
}
/*
+ for result
- for error message
* for system message
*/
