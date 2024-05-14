//! Generate functions to perform the "normal" engine interface operations

use std::sync::Arc;

use delta_kernel::{schema::Schema, DeltaResult, Error, FileDataReadResultIterator};
use tracing::debug;

use crate::{
    handle::{ArcHandle, BoxHandle},
    scan::EngineDataHandle,
    unwrap_and_parse_path_as_url, ExternEngine, ExternEngineHandle, ExternResult, IntoExternResult,
    KernelStringSlice, NullableCvoid,
};

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

// Intentionally opaque to the engine.
pub struct FileReadResultIterator {
    // Box -> Wrap its unsized content this struct is fixed-size with thin pointers.
    // Item = Box<dyn EngineData>, see above, Vec<bool> -> can become a KernelBoolSlice
    data: FileDataReadResultIterator,

    // Also keep a reference to the external engine for its error allocator.
    // Parquet and Json handlers don't hold any reference to the tokio reactor, so the iterator
    // terminates early if the last engine goes out of scope.
    engine: Arc<dyn ExternEngine>,
}

impl BoxHandle for FileReadResultIterator {}

impl Drop for FileReadResultIterator {
    fn drop(&mut self) {
        debug!("dropping FileReadResultIterator");
    }
}

/// Call the engine back with the next `EngingeData` batch read by Parquet/Json handler
/// # Safety
///
/// The iterator must be valid (returned by [`read_parquet_files`]) and not yet freed by
/// [`free_read_result_iter`]. The visitor function pointer must be non-null.
#[no_mangle]
pub unsafe extern "C" fn read_result_next(
    data: &mut FileReadResultIterator,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        engine_data: *mut EngineDataHandle,
    ),
) -> ExternResult<bool> {
    read_result_next_impl(data, engine_context, engine_visitor)
        .into_extern_result(data.engine.error_allocator())
}

fn read_result_next_impl(
    data: &mut FileReadResultIterator,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        engine_data: *mut EngineDataHandle,
    ),
) -> DeltaResult<bool> {
    if let Some(data) = data.data.next().transpose()? {
        let data_handle = BoxHandle::into_handle(EngineDataHandle { data });
        (engine_visitor)(engine_context, data_handle);
        // TODO: calling `into_raw` in the visitor causes this to segfault
        //       perhaps the callback needs to indicate if it took ownership or not
        // unsafe { BoxHandle::drop_handle(data_handle) };
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Free the memory from the passed read result iterator
/// # Safety
///
/// Caller is responsible for (at most once) passing a valid pointer returned by a call to
/// [`read_parquet_files`].
#[no_mangle]
pub unsafe extern "C" fn free_read_result_iter(data: *mut FileReadResultIterator) {
    BoxHandle::drop_handle(data);
}

/// Use the specified engine's [`delta_kernel::ParquetHandler`] to read the specified file.
///
/// # Safety
/// Caller is responsible for calling with a valid `ExternEngineHandle` and `FileMeta`
#[no_mangle]
pub unsafe extern "C" fn read_parquet_files(
    extern_engine: *const ExternEngineHandle,
    file: &FileMeta,
    physical_schema: &Schema,
) -> ExternResult<*mut FileReadResultIterator> {
    read_parquet_files_impl(extern_engine, file, physical_schema).into_extern_result(extern_engine)
}

unsafe fn read_parquet_files_impl(
    extern_engine: *const ExternEngineHandle,
    file: &FileMeta,
    physical_schema: &Schema,
) -> DeltaResult<*mut FileReadResultIterator> {
    let extern_engine = unsafe { ArcHandle::clone_as_arc(extern_engine) };
    let engine = extern_engine.engine();
    let delta_fm: delta_kernel::FileMeta = file.try_into()?;
    let parquet_handler = engine.get_parquet_handler();
    let data =
        parquet_handler.read_parquet_files(&[delta_fm], Arc::new(physical_schema.clone()), None)?;
    let res = FileReadResultIterator {
        data,
        engine: extern_engine.clone(),
    };
    Ok(res.into_handle())
}
