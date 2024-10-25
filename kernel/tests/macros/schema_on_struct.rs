extern crate delta_kernel_derive;
use delta_kernel_derive::Schema;

mod actions;
mod schema;

#[derive(Schema)]
pub struct WithFields {
    some_long_name: String,
}

fn main() {}
