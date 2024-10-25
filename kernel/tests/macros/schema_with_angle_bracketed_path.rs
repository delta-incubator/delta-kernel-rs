extern crate delta_kernel_derive;
use delta_kernel_derive::Schema;
use std::collections::HashMap;

mod actions;
mod schema;

#[derive(Schema)]
pub struct WithAngleBracketPath {
    map_field: HashMap<String, String>,
}

fn main() {}
