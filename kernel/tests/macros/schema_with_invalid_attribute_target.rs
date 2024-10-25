extern crate delta_kernel_derive;
use delta_kernel_derive::Schema;

mod actions;
mod schema;

#[derive(Schema)]
pub struct WithInvalidAttribute {
    #[drop_null_container_values]
    some_long_name: String,
}

fn main() {}
