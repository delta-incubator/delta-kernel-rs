extern crate delta_kernel_derive;
use delta_kernel_derive::Schema;

mod actions;
mod schema;

#[derive(Schema)]
pub struct NoFields;

fn main() {}
