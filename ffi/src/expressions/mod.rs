//! This module holds functionality for moving expressions across the FFI boundary, both from
//! engine to kernel, and from kernel to engine.
use delta_kernel::Expression;
use delta_kernel_ffi_macros::handle_descriptor;

pub mod engine_expr_visitor;
pub mod kernel_expr_visitor;

#[handle_descriptor(target=Expression, mutable=false, sized=true)]
pub struct SharedExpression;
