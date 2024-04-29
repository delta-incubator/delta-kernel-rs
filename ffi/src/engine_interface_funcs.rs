//! Generate functions to perform the "normal" engine interface operations

use crate::{handle::ArcHandle, ExternEngineInterfaceHandle, KernelStringSlice};

#[repr(C)]
pub struct FileMeta {
    pub path: KernelStringSlice,
    pub last_modified: i64,
    pub size: usize,
}

impl Into<delta_kernel::FileMeta> for FileMeta {
    fn into(self) -> delta_kernel::FileMeta {
        todo!()
    }
}

pub extern "C" fn read_parquet_files(
    extern_table_client: *const ExternEngineInterfaceHandle,
    file: &FileMeta
) {
    let extern_table_client = unsafe { ArcHandle::clone_as_arc(extern_table_client) };
    let interface = extern_table_client.table_client();
}
