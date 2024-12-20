//! Generate functions to perform the "normal" engine operations

use std::sync::Arc;

use delta_kernel::{
    schema::{DataType, Schema, SchemaRef},
    DeltaResult, EngineData, Expression, ExpressionEvaluator, FileDataReadResultIterator,
};
use delta_kernel_ffi_macros::handle_descriptor;
use tracing::debug;
use url::Url;

use crate::{
    scan::SharedSchema, ExclusiveEngineData, ExternEngine, ExternResult, IntoExternResult,
    KernelStringSlice, NullableCvoid, SharedExternEngine, TryFromStringSlice,
};

use super::handle::Handle;

#[repr(C)]
pub struct FileMeta {
    pub path: KernelStringSlice,
    pub last_modified: i64,
    pub size: usize,
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

#[handle_descriptor(target=FileReadResultIterator, mutable=true, sized=true)]
pub struct ExclusiveFileReadResultIterator;

impl Drop for FileReadResultIterator {
    fn drop(&mut self) {
        debug!("dropping FileReadResultIterator");
    }
}

/// Call the engine back with the next `EngingeData` batch read by Parquet/Json handler. The
/// _engine_ "owns" the data that is passed into the `engine_visitor`, since it is allocated by the
/// `Engine` being used for log-replay. If the engine wants the kernel to free this data, it _must_
/// call [`free_engine_data`] on it.
///
/// # Safety
///
/// The iterator must be valid (returned by [`read_parquet_file`]) and not yet freed by
/// [`free_read_result_iter`]. The visitor function pointer must be non-null.
#[no_mangle]
pub unsafe extern "C" fn read_result_next(
    mut data: Handle<ExclusiveFileReadResultIterator>,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        engine_data: Handle<ExclusiveEngineData>,
    ),
) -> ExternResult<bool> {
    let iter = unsafe { data.as_mut() };
    read_result_next_impl(iter, engine_context, engine_visitor)
        .into_extern_result(iter.engine.error_allocator())
}

fn read_result_next_impl(
    iter: &mut FileReadResultIterator,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        engine_data: Handle<ExclusiveEngineData>,
    ),
) -> DeltaResult<bool> {
    if let Some(data) = iter.data.next().transpose()? {
        (engine_visitor)(engine_context, data.into());
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Free the memory from the passed read result iterator
/// # Safety
///
/// Caller is responsible for (at most once) passing a valid pointer returned by a call to
/// [`read_parquet_file`].
#[no_mangle]
pub unsafe extern "C" fn free_read_result_iter(data: Handle<ExclusiveFileReadResultIterator>) {
    data.drop_handle();
}

/// Use the specified engine's [`delta_kernel::ParquetHandler`] to read the specified file.
///
/// # Safety
/// Caller is responsible for calling with a valid `ExternEngineHandle` and `FileMeta`
#[no_mangle]
pub unsafe extern "C" fn read_parquet_file(
    engine: Handle<SharedExternEngine>, // TODO Does this cause a free?
    file: &FileMeta,
    physical_schema: Handle<SharedSchema>,
) -> ExternResult<Handle<ExclusiveFileReadResultIterator>> {
    let engine = unsafe { engine.clone_as_arc() };
    let physical_schema = unsafe { physical_schema.clone_as_arc() };
    let path = unsafe { TryFromStringSlice::try_from_slice(&file.path) };
    let res = read_parquet_file_impl(engine.clone(), path, file, physical_schema);
    res.into_extern_result(&engine.as_ref())
}

fn read_parquet_file_impl(
    extern_engine: Arc<dyn ExternEngine>,
    path: DeltaResult<&str>,
    file: &FileMeta,
    physical_schema: Arc<Schema>,
) -> DeltaResult<Handle<ExclusiveFileReadResultIterator>> {
    let engine = extern_engine.engine();
    let parquet_handler = engine.get_parquet_handler();
    let location = Url::parse(path?)?;
    let delta_fm = delta_kernel::FileMeta {
        location,
        last_modified: file.last_modified,
        size: file.size,
    };
    // TODO: Plumb the predicate through the FFI?
    let data = parquet_handler.read_parquet_files(&[delta_fm], physical_schema, None)?;
    let res = Box::new(FileReadResultIterator {
        data,
        engine: extern_engine,
    });
    Ok(res.into())
}

// Expression Eval

#[handle_descriptor(target=dyn ExpressionEvaluator, mutable=false)]
pub struct SharedExpressionEvaluator;

#[no_mangle]
pub unsafe extern "C" fn get_evaluator(
    engine: Handle<SharedExternEngine>,
    input_schema: Handle<SharedSchema>,
    expression: &Expression,
    // TODO: Make this a data_type, and give a way for c code to go between schema <-> datatype
    output_type: Handle<SharedSchema>,
) -> Handle<SharedExpressionEvaluator> {
    let engine = unsafe { engine.clone_as_arc() };
    let input_schema = unsafe { input_schema.clone_as_arc() };
    let output_type: DataType = output_type.as_ref().clone().into();
    get_evaluator_impl(engine, input_schema, expression, output_type)
}

fn get_evaluator_impl(
    extern_engine: Arc<dyn ExternEngine>,
    input_schema: SchemaRef,
    expression: &Expression,
    output_type: DataType,
) -> Handle<SharedExpressionEvaluator> {
    let engine = extern_engine.engine();
    let evaluator = engine.get_expression_handler().get_evaluator(
        input_schema,
        expression.clone(),
        output_type,
    );
    evaluator.into()
}

/// Free an evaluator
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_evaluator(evaluator: Handle<SharedExpressionEvaluator>) {
    debug!("engine released evaluator");
    evaluator.drop_handle();
}


#[no_mangle]
pub unsafe extern "C" fn evaluate(
    engine: Handle<SharedExternEngine>,
    batch: &mut Handle<ExclusiveEngineData>,
    evaluator: Handle<SharedExpressionEvaluator>,
) -> ExternResult<Handle<ExclusiveEngineData>> {
    let engine = unsafe { engine.clone_as_arc() };
    let batch = unsafe { batch.as_mut() };
    let evaluator = unsafe { evaluator.clone_as_arc() };
    let res = evaluate_impl(batch, evaluator.as_ref());
    res.into_extern_result(&engine.as_ref())
}

fn evaluate_impl(
    batch: &dyn EngineData,
    evaluator: &dyn ExpressionEvaluator,
) -> DeltaResult<Handle<ExclusiveEngineData>> {
    let res = evaluator.evaluate(batch);
    res.map(|d| d.into())
}

#[cfg(test)]
mod tests {
    use super::get_evaluator;
    use crate::{free_engine, tests::get_default_engine};
    use delta_kernel::{
        schema::{DataType, StructField, StructType},
        Expression,
    };
    use std::sync::Arc;

    #[test]
    fn test_get_evaluator() {
        let engine = get_default_engine();
        let in_schema = Arc::new(StructType::new(vec![StructField::new(
            "a",
            DataType::LONG,
            true,
        )]));
        let expr = Expression::literal(1);
        let output_type = in_schema.clone();
        unsafe {
            get_evaluator(
                engine.shallow_copy(),
                in_schema.into(),
                &expr,
                output_type.into(),
            );
            free_engine(engine);
        }
    }
}
