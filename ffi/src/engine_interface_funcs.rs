//! Generate functions to perform the "normal" engine interface operations

use crate::{handle::ArcHandle, ExternEngineInterfaceHandle};

pub extern "C" fn parse_json(extern_table_client: *const ExternEngineInterfaceHandle) {
    let extern_table_client = unsafe { ArcHandle::clone_as_arc(extern_table_client) };
    //extern_table_client.table_client().parse_json()
}
