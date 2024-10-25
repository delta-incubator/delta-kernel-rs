extern crate delta_kernel_derive;
use delta_kernel_derive::Schema;
use std::collections::HashMap;

mod actions;
mod schema;

#[derive(Schema)]
pub struct WithAngleBracketPath {
    #[drop_null_container_values]
    map_field: HashMap<String, String>,
}

fn main() {}
