extern crate clap;
use clap::App;
use kvs::Result;
use serde::{Deserialize, Serialize};
use std::net::{TcpListener, TcpStream};
use std::io::prelude::*;
#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate slog_async;

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
    // debug!(log, "debug");
    // warn!(log, "warn");
    let listener = TcpListener::bind("127.0.0.1:4000")?;
    for stream in listener.incoming() {
        let mut s = stream?;
        let mut buf = vec![0,8];
        println!("received!");
        s.read_exact(&mut buf)?;
        let len :usize = serde_json::from_slice(&buf)?;
        buf.resize(len,0);
        s.read_exact(&mut buf);
        let something : Command = serde_json::from_slice(&buf)?;
        info!(log,"{:?}",something);
        s.write(&buf)?;
    }

    Ok(())
}