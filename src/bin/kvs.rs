extern crate clap;
use clap::App;

fn main() {
    let yaml = clap::load_yaml!("cli.yml");
    let m = App::from_yaml(yaml)
        .name(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .get_matches();

    println!("{:?}", m)
}
