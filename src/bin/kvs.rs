extern crate clap;
use clap::App;
use std::process::exit;
// use kvs::KvStore;

fn main() {
    /* load clap config from yaml file */
    let yaml = clap::load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml)
        .name(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .get_matches();

    // let mut kvstore = KvStore::new();

    match matches.subcommand() {
        ("get", Some(sub_m)) => {
            sub_m.value_of("key").unwrap();
            eprintln!("unimplemented!");
            exit(1);
        }
        ("set", Some(_sub_m)) => {
            eprintln!("unimplemented!");
            exit(1);
        }
        ("rm", Some(_sub_m)) => {
            eprintln!("unimplemented!");
            exit(1);
        }

        _ => unreachable!(),
    }
}
