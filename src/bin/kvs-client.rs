extern crate bincode;
extern crate clap;
use clap::App;
use kvs::{Command, OpType, Protocol, Result, Status};
use std::net::TcpStream;
use std::process::exit;

const DEFAULT_ADDR: &str = "127.0.0.1:4000";

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

    /* parse common parts of args */
    if let (_, Some(sub_m)) = matches.subcommand() {
        addr = sub_m.value_of("addr").unwrap_or(DEFAULT_ADDR).to_owned();
        command.key = sub_m.value_of("key").unwrap().to_owned();
    } else {
        eprintln!("Expect a sub command!");
        exit(1);
    }

    match matches.subcommand() {
        ("get", Some(_sub_m)) => {
            /* OpType::Get is default value */
            // command.op = OpType::Get;
        }
        ("set", Some(sub_m)) => {
            command.op = OpType::Set;
            command.value = Some(sub_m.value_of("value").unwrap().to_owned());
        }
        ("rm", Some(_sub_m)) => {
            command.op = OpType::Remove;
        }

        _ => unreachable!(),
    }

    let mut stream = TcpStream::connect(&addr)?;
    let mut protocol = Protocol::new(&mut stream);
    let response: String = protocol.send(&command)?.receive()?;
    let (status, result) = Protocol::parse_result(&response)?;
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
