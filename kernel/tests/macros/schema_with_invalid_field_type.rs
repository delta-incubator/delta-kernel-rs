extern crate delta_kernel_derive;
use delta_kernel_derive::Schema;
use syn::Token;

mod actions;
mod schema;

#[derive(Schema)]
pub struct WithMacro {
    token: Token![struct],
}

fn main() {}
