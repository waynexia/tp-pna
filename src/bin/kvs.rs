extern crate clap;
use clap::App;
use kvs::{KvStore, Result};
use std::path::Path;
use std::process::exit;

fn main() -> Result<()> {
    /* load clap config from yaml file */
    let yaml = clap::load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml)
        .name(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .get_matches();

    let mut kvstore = KvStore::open(&Path::new("./"))?;

    match matches.subcommand() {
        ("get", Some(sub_m)) => {
            match kvstore.get(sub_m.value_of("key").unwrap().to_owned())? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            };
        }
        ("set", Some(sub_m)) => {
            kvstore.set(
                sub_m.value_of("key").unwrap().to_owned(),
                sub_m.value_of("value").unwrap().to_owned(),
            )?;
        }
        ("rm", Some(sub_m)) => {
            if kvstore.get(sub_m.value_of("key").unwrap().to_owned())? == None {
                println!("Key not found");
                exit(1);
            } else {
                kvstore.remove(sub_m.value_of("key").unwrap().to_owned())?;
            }
        }

        _ => unreachable!(),
    }

    Ok(())
}
