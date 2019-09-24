extern crate bincode;
extern crate clap;
use clap::App;
use kvs::{Command, OpType, Protocol, Result, Status};
use std::net::TcpStream;
use std::process::exit;

fn main() -> Result<()> {
    /* load clap config from yaml file */
    let yaml = clap::load_yaml!("kvs-client-clap.yml");
    let matches = App::from_yaml(yaml)
        .name(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .get_matches();

    let mut command = Command {
        op: OpType::Get,
        key: String::new(),
        value: None,
    };
    let addr: String;

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
    let mut protocol = Protocol::new(&mut stream);
    let de: String = protocol.send(&command)?.receive()?;
    let (status, result) = Protocol::parse_result(&de)?;
    match status {
        Status::Ok => println!("{}", result),
        Status::Error => {
            if command.op == OpType::Remove {
                eprintln!("Error: {}", result);
                exit(1);
            } else {
                println!("Error: {}", result);
            }
        }
        Status::Message => {}
    }

    Ok(())
}
