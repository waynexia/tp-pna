#![feature(integer_atomics)]
#![deny(clippy::all)]
// for atomic fetch_max
#![feature(atomic_min_max)]

#[allow(unused_imports)]
#[macro_use]
extern crate log;
#[allow(unused_imports)]
#[macro_use]
extern crate prost_derive;

pub mod kvraft;
mod proto;
pub mod raft;

// / A place holder for suppressing unused_variables warning.
// fn your_code_here<T>(_: T) -> ! {
//     unimplemented!()
// }
