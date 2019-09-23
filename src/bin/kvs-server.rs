extern crate clap;
use clap::App;
use kvs::engine::KvsEngine;
use kvs::{KvStore, KvsError, Protocol, Result, SledKvsEngine};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::File;
use std::net::TcpListener;
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
    let matches = App::from_yaml(yaml)
        .name(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .get_matches();

    let decorator = slog_term::PlainDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let log = slog::Logger::root(drain, o!());

    info!(log, "start");

    let addr = matches.value_of("addr").unwrap_or("127.0.0.1:4000");
    let default_engine_name = get_default_engine();
    let engine_name = matches.value_of("engine").unwrap_or(&default_engine_name);
    setup_engine_flag(&engine_name);
    info!(
        log,
        "{} (ver {}), engine: {}, start listening on {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        engine_name,
        addr
    );

    match engine_name {
        "kvs" => run(KvStore::open(&Path::new("./"))?, addr, log)?,
        "sled" => run(SledKvsEngine::open(&std::env::current_dir()?)?, addr, log)?,
        _ => panic!("invalid engine name, must be kvs or sled!"),
    }

    Ok(())
}

fn run<T: KvsEngine>(mut store: T, addr: &str, log: slog::Logger) -> Result<()> {
    let listener = TcpListener::bind(addr)?;
    for s in listener.incoming() {
        let mut stream = s?;
        let mut protocol = Protocol::new(&mut stream);
        let command: Command = protocol.receive()?;
        info!(log, "received command {:?}", command);

        let mut ret_str = String::new();
        match command.op {
            OpType::Get => {
                match store.get(command.key.to_owned())? {
                    Some(value) => ret_str.push_str(&format!("+{}", value)),
                    None => ret_str.push_str("-Key not found"),
                };
            }
            OpType::Set => {
                store.set(command.key.to_owned(), command.value.unwrap().to_owned())?;
                ret_str.push_str("*Done");
            }
            OpType::Remove => {
                match store.remove(command.key.to_owned()) {
                    Err(KvsError::KeyNotFound) => {
                        ret_str.push_str("-Key not found");
                    }
                    Ok(()) => ret_str.push_str("*Done"),
                    Err(e) => ret_str.push_str(&format!("-{}", e.description())),
                };
            }
        }
        info!(log, "execute result: {}", ret_str);
        protocol.send(&ret_str)?;
    }
    Ok(())
}

fn get_default_engine() -> String {
    if Path::new("./").join(".sled_flag").exists() {
        return "sled".to_owned();
    } else {
        return "kvs".to_owned();
    }
}

fn setup_engine_flag(engine_name: &str) {
    match engine_name {
        "kvs" => {
            if Path::new("./.sled_flag").exists() {
                panic!("different engine than previously used");
            }
            if !Path::new("./.kvs_flag").exists() {
                File::create(&Path::new("./.kvs_flag")).unwrap();
            }
        }
        "sled" => {
            if Path::new("./.kvs_flag").exists() {
                panic!("different engine than previously used");
            }
            if !Path::new("./.sled_flag").exists() {
                File::create(&Path::new("./.sled_flag")).unwrap();
            }
        }
        _ => panic!(),
    }
}
