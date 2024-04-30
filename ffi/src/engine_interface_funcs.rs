//! Generate functions to perform the "normal" engine interface operations

use delta_kernel::{schema::SchemaRef, Error};

use crate::{handle::ArcHandle, unwrap_and_parse_path_as_url, ExternEngineInterfaceHandle, KernelStringSlice};

#[repr(C)]
pub struct FileMeta {
    pub path: KernelStringSlice,
    pub last_modified: i64,
    pub size: usize,
}

impl TryFrom<&FileMeta> for delta_kernel::FileMeta {
    type Error = Error;

    fn try_from(fm: &FileMeta) -> Result<Self, Error> {
        let location = unsafe { unwrap_and_parse_path_as_url(fm.path.clone()) }?;
        Ok(delta_kernel::FileMeta {
            location,
            last_modified: fm.last_modified,
            size: fm.size,
        })
    }
}


#[no_mangle]
pub extern "C" fn read_parquet_files(
    extern_table_client: *const ExternEngineInterfaceHandle,
    file: &FileMeta,
    physical_schema: &SchemaRef,
) {
    let extern_table_client = unsafe { ArcHandle::clone_as_arc(extern_table_client) };
    let interface = extern_table_client.table_client();
    let delta_fm: delta_kernel::FileMeta = file.try_into().expect("Can't convert");
    let parquet_handler = interface.get_parquet_handler();
    let res_iter = parquet_handler.read_parquet_files(
        &[delta_fm],
        physical_schema.clone(),
        None
    ).unwrap();
    for res in res_iter {
        println!("Made it here: {}", res.is_ok());
    }
}
