use crate::schema::Schema;

use std::any::Any;

/// This module defines an interface for the kernel to transer data between a client (engine) and
/// the kernel itself. (TODO: Explain more)

pub trait DataReceiver {
    // Receive some data from a call to `extract`. The data in the Vec should not be assumed to live
    // beyond the call to this funtion (i.e. it should be copied if needed)
    fn recv(&mut self, vals: Vec<Option<&dyn Any>>) -> ();
}

// A data extractor can take whatever the engine defines as its `EngineData` type and can call back
// into kernel with rows extracted from that data.
pub trait DataExtractor<EngineData> {
    // This method _must_ extract the leaf data of the schema, construct a Vec of it *in schema
    // order*, and then call the recv method of the passed visitor for each row in `blob`.
    fn extract(&self, blob: EngineData, schema: Schema, visitor: &mut dyn DataReceiver) -> ();
    // Return the number of items (rows?) in blob
    fn length(&self, blob: EngineData) -> usize;
}

