/// FFI interface for the delta kernel
///
/// Exposes that an engine needs to call from C/C++ to interface with kernel
#[cfg(feature = "default-engine")]
use std::collections::HashMap;
use std::default::Default;
use std::os::raw::{c_char, c_void};
use std::ptr::NonNull;
use std::sync::Arc;
use tracing::{debug, warn};
use url::Url;

use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Engine, EngineData, Error, Table};
use delta_kernel_ffi_macros::handle_descriptor;

// cbindgen doesn't understand our use of feature flags here, and by default it parses `mod handle`
// twice. So we tell it to ignore one of the declarations to avoid double-definition errors.
/// cbindgen:ignore
#[cfg(feature = "developer-visibility")]
pub mod handle;
#[cfg(not(feature = "developer-visibility"))]
pub(crate) mod handle;

use handle::Handle;

// The handle_descriptor macro needs this, because it needs to emit fully qualified type names. THe
// actual prod code could use `crate::`, but doc tests can't because they're not "inside" the crate.
// relies on `crate::`
extern crate self as delta_kernel_ffi;

pub mod engine_funcs;
pub mod expressions;
pub mod scan;
pub mod schema;
#[cfg(feature = "test-ffi")]
pub mod test_ffi;

pub(crate) type NullableCvoid = Option<NonNull<c_void>>;

/// Model iterators. This allows an engine to specify iteration however it likes, and we simply wrap
/// the engine functions. The engine retains ownership of the iterator.
#[repr(C)]
pub struct EngineIterator {
    // Opaque data that will be iterated over. This data will be passed to the get_next function
    // each time a next item is requested from the iterator
    data: NonNull<c_void>,
    /// A function that should advance the iterator and return the next time from the data
    /// If the iterator is complete, it should return null. It should be safe to
    /// call `get_next()` multiple times if it returns null.
    get_next: extern "C" fn(data: NonNull<c_void>) -> *const c_void,
}

impl Iterator for EngineIterator {
    // Todo: Figure out item type
    type Item = *const c_void;

    fn next(&mut self) -> Option<Self::Item> {
        let next_item = (self.get_next)(self.data);
        if next_item.is_null() {
            None
        } else {
            Some(next_item)
        }
    }
}

/// A non-owned slice of a UTF8 string, intended for arg-passing between kernel and engine. The
/// slice is only valid until the function it was passed into returns, and should not be copied.
///
/// # Safety
///
/// Intentionally not Copy, Clone, Send, nor Sync.
///
/// Whoever instantiates the struct must ensure it does not outlive the data it points to. The
/// compiler cannot help us here, because raw pointers don't have lifetimes. A good rule of thumb is
/// to always use the [`kernel_string_slice`] macro to create string slices, and to avoid returning
/// a string slice from a code block or function.
///
/// Meanwhile, the callee must assume that the slice is only valid until the function returns, and
/// must not retain any references to the slice or its data that might outlive the function call.
#[repr(C)]
pub struct KernelStringSlice {
    ptr: *const c_char,
    len: usize,
}
impl KernelStringSlice {
    /// Creates a new string slice from a source string. This method is dangerous and can easily
    /// lead to use-after-free scenarios. The [`kernel_string_slice`] macro should be preferred as a
    /// much safer alternative.
    ///
    /// # Safety
    ///
    /// Caller affirms that the source will outlive the new slice created from it. The compiler
    /// cannot help as raw pointers do not have lifetimes that the compiler verify.
    pub(crate) unsafe fn new_unsafe(source: impl AsRef<str>) -> Self {
        let source = source.as_ref().as_bytes();
        Self {
            ptr: source.as_ptr().cast(),
            len: source.len(),
        }
    }
}

/// Creates a new [`KernelStringSlice`] from a source object (which must be an identifier, to ensure
/// it is not immediately dropped). This is the safest way to create a kernel string slice.
macro_rules! kernel_string_slice {
    ( $source:ident ) => {{
        // Safety: A named source cannot immediately go out of scope. Any dangerous situations will
        // arise from the _use_ of this string slice, not from its creation.
        //
        // NOTE: the `do_it` wrapper avoids an "unnecessary `unsafe` block clippy warning, since
        // annotating an expression with #[allow(unused_unsafe)] is not stable rust.
        fn do_it(s: impl AsRef<str>) -> $crate::KernelStringSlice {
            unsafe { $crate::KernelStringSlice::new_unsafe(s) }
        }
        do_it($source)
    }};
}
pub(crate) use kernel_string_slice;

trait TryFromStringSlice<'a>: Sized {
    unsafe fn try_from_slice(slice: &'a KernelStringSlice) -> DeltaResult<Self>;
}

impl<'a> TryFromStringSlice<'a> for String {
    /// Converts a kernel string slice into a `String`.
    ///
    /// # Safety
    ///
    /// The slice must be a valid (non-null) pointer, and must point to the indicated number of
    /// valid utf8 bytes.
    unsafe fn try_from_slice(slice: &'a KernelStringSlice) -> DeltaResult<Self> {
        let slice: &str = unsafe { TryFromStringSlice::try_from_slice(slice)? };
        Ok(slice.into())
    }
}

impl<'a> TryFromStringSlice<'a> for &'a str {
    /// Converts a kernel string slice into a borrowed `str`. The result does not outlive the kernel
    /// string slice it came from.
    ///
    /// # Safety
    ///
    /// The slice must be a valid (non-null) pointer, and must point to the indicated number of
    /// valid utf8 bytes.
    unsafe fn try_from_slice(slice: &'a KernelStringSlice) -> DeltaResult<Self> {
        let slice = unsafe { std::slice::from_raw_parts(slice.ptr.cast(), slice.len) };
        Ok(std::str::from_utf8(slice)?)
    }
}

/// Allow engines to allocate strings of their own type. the contract of calling a passed allocate
/// function is that `kernel_str` is _only_ valid until the return from this function
pub type AllocateStringFn = extern "C" fn(kernel_str: KernelStringSlice) -> NullableCvoid;

// Put KernelBoolSlice in a sub-module, with non-public members, so rust code cannot instantiate it
// directly. It can only be created by converting `From<Vec<bool>>`.
mod private {
    use std::ptr::NonNull;

    /// Represents an owned slice of boolean values allocated by the kernel. Any time the engine
    /// receives a `KernelBoolSlice` as a return value from a kernel method, engine is responsible
    /// to free that slice, by calling [super::free_bool_slice] exactly once.
    #[repr(C)]
    pub struct KernelBoolSlice {
        ptr: NonNull<bool>,
        len: usize,
    }

    /// An owned slice of u64 row indexes allocated by the kernel. The engine is responsible for
    /// freeing this slice by calling [super::free_row_indexes] once.
    #[repr(C)]
    pub struct KernelRowIndexArray {
        ptr: NonNull<u64>,
        len: usize,
    }

    impl KernelBoolSlice {
        /// Creates an empty slice.
        pub fn empty() -> KernelBoolSlice {
            KernelBoolSlice {
                ptr: NonNull::dangling(),
                len: 0,
            }
        }

        /// Converts this slice back into a `Vec<bool>`.
        ///
        /// # Safety
        ///
        /// The slice must have been originally created `From<Vec<bool>>`, and must not have been
        /// already been consumed by a previous call to this method.
        pub unsafe fn as_ref(&self) -> &[bool] {
            if self.len == 0 {
                Default::default()
            } else {
                unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
            }
        }

        /// Converts this slice back into a `Vec<bool>`.
        ///
        /// # Safety
        ///
        /// The slice must have been originally created `From<Vec<bool>>`, and must not have been
        /// already been consumed by a previous call to this method.
        pub unsafe fn into_vec(self) -> Vec<bool> {
            if self.len == 0 {
                Default::default()
            } else {
                Vec::from_raw_parts(self.ptr.as_ptr(), self.len, self.len)
            }
        }
    }

    impl From<Vec<bool>> for KernelBoolSlice {
        fn from(val: Vec<bool>) -> Self {
            let len = val.len();
            let boxed = val.into_boxed_slice();
            let leaked_ptr = Box::leak(boxed).as_mut_ptr();
            let ptr = NonNull::new(leaked_ptr)
                .expect("This should never be non-null please report this bug.");
            KernelBoolSlice { ptr, len }
        }
    }

    /// # Safety
    ///
    /// Whenever kernel passes a [KernelBoolSlice] to engine, engine assumes ownership of the slice
    /// memory, but must only free it by calling [super::free_bool_slice]. Since the global
    /// allocator is threadsafe, it doesn't matter which engine thread invokes that method.
    unsafe impl Send for KernelBoolSlice {}
    /// # Safety
    ///
    /// This follows the same contract as KernelBoolSlice above, engine assumes ownership of the
    /// slice memory, but must only free it by calling [super::free_row_indexes]. It does not matter
    /// from which thread the engine invoke that method
    unsafe impl Send for KernelRowIndexArray {}

    /// # Safety
    ///
    /// If engine chooses to leverage concurrency, engine is responsible to prevent data races.
    unsafe impl Sync for KernelBoolSlice {}
    /// # Safety
    ///
    /// If engine chooses to leverage concurrency, engine is responsible to prevent data races.
    /// Same contract as KernelBoolSlice above
    unsafe impl Sync for KernelRowIndexArray {}

    impl KernelRowIndexArray {
        /// Converts this slice back into a `Vec<u64>`.
        ///
        /// # Safety
        ///
        /// The slice must have been originally created `From<Vec<u64>>`, and must not have
        /// already been consumed by a previous call to this method.
        pub unsafe fn into_vec(self) -> Vec<u64> {
            Vec::from_raw_parts(self.ptr.as_ptr(), self.len, self.len)
        }

        /// Creates an empty slice.
        pub fn empty() -> KernelRowIndexArray {
            Self {
                ptr: NonNull::dangling(),
                len: 0,
            }
        }
    }

    impl From<Vec<u64>> for KernelRowIndexArray {
        fn from(vec: Vec<u64>) -> Self {
            let len = vec.len();
            let boxed = vec.into_boxed_slice();
            let leaked_ptr = Box::leak(boxed).as_mut_ptr();
            let ptr = NonNull::new(leaked_ptr)
                .expect("This should never be non-null please report this bug.");
            KernelRowIndexArray { ptr, len }
        }
    }
}
pub use private::KernelBoolSlice;
pub use private::KernelRowIndexArray;

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_bool_slice(slice: KernelBoolSlice) {
    let vec = unsafe { slice.into_vec() };
    debug!("Dropping bool slice. It is {vec:#?}");
}

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_row_indexes(slice: KernelRowIndexArray) {
    let _ = slice.into_vec();
}

// TODO: Do we want this handle at all? Perhaps we should just _always_ pass raw *mut c_void pointers
// that are the engine data? Even if we want the type, should it be a shared handle instead?
/// an opaque struct that encapsulates data read by an engine. this handle can be passed back into
/// some kernel calls to operate on the data, or can be converted into the raw data as read by the
/// [`delta_kernel::Engine`] by calling [`get_raw_engine_data`]
#[handle_descriptor(target=dyn EngineData, mutable=true, sized=false)]
pub struct ExclusiveEngineData;

/// Drop an `ExclusiveEngineData`.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle as engine_data
#[no_mangle]
pub unsafe extern "C" fn free_engine_data(engine_data: Handle<ExclusiveEngineData>) {
    engine_data.drop_handle();
}

#[repr(C)]
#[derive(Debug)]
pub enum KernelError {
    UnknownError, // catch-all for unrecognized kernel Error types
    FFIError,     // errors encountered in the code layer that supports FFI
    #[cfg(any(feature = "default-engine", feature = "sync-engine"))]
    ArrowError,
    EngineDataTypeError,
    ExtractError,
    GenericError,
    IOErrorError,
    #[cfg(any(feature = "default-engine", feature = "sync-engine"))]
    ParquetError,
    #[cfg(feature = "default-engine")]
    ObjectStoreError,
    #[cfg(feature = "default-engine")]
    ObjectStorePathError,
    #[cfg(feature = "default-engine")]
    ReqwestError,
    FileNotFoundError,
    MissingColumnError,
    UnexpectedColumnTypeError,
    MissingDataError,
    MissingVersionError,
    DeletionVectorError,
    InvalidUrlError,
    MalformedJsonError,
    MissingMetadataError,
    MissingProtocolError,
    MissingMetadataAndProtocolError,
    ParseError,
    JoinFailureError,
    Utf8Error,
    ParseIntError,
    InvalidColumnMappingModeError,
    InvalidTableLocationError,
    InvalidDecimalError,
    InvalidStructDataError,
    InternalError,
    InvalidExpression,
    InvalidLogPath,
    InvalidCommitInfo,
    FileAlreadyExists,
    MissingCommitInfo,
}

impl From<Error> for KernelError {
    fn from(e: Error) -> Self {
        match e {
            // NOTE: By definition, no kernel Error maps to FFIError
            #[cfg(any(feature = "default-engine", feature = "sync-engine"))]
            Error::Arrow(_) => KernelError::ArrowError,
            Error::EngineDataType(_) => KernelError::EngineDataTypeError,
            Error::Extract(..) => KernelError::ExtractError,
            Error::Generic(_) => KernelError::GenericError,
            Error::GenericError { .. } => KernelError::GenericError,
            Error::IOError(_) => KernelError::IOErrorError,
            #[cfg(any(feature = "default-engine", feature = "sync-engine"))]
            Error::Parquet(_) => KernelError::ParquetError,
            #[cfg(feature = "default-engine")]
            Error::ObjectStore(_) => KernelError::ObjectStoreError,
            #[cfg(feature = "default-engine")]
            Error::ObjectStorePath(_) => KernelError::ObjectStorePathError,
            #[cfg(feature = "default-engine")]
            Error::Reqwest(_) => KernelError::ReqwestError,
            Error::FileNotFound(_) => KernelError::FileNotFoundError,
            Error::MissingColumn(_) => KernelError::MissingColumnError,
            Error::UnexpectedColumnType(_) => KernelError::UnexpectedColumnTypeError,
            Error::MissingData(_) => KernelError::MissingDataError,
            Error::MissingVersion => KernelError::MissingVersionError,
            Error::DeletionVector(_) => KernelError::DeletionVectorError,
            Error::InvalidUrl(_) => KernelError::InvalidUrlError,
            Error::MalformedJson(_) => KernelError::MalformedJsonError,
            Error::MissingMetadata => KernelError::MissingMetadataError,
            Error::MissingProtocol => KernelError::MissingProtocolError,
            Error::MissingMetadataAndProtocol => KernelError::MissingMetadataAndProtocolError,
            Error::ParseError(..) => KernelError::ParseError,
            Error::JoinFailure(_) => KernelError::JoinFailureError,
            Error::Utf8Error(_) => KernelError::Utf8Error,
            Error::ParseIntError(_) => KernelError::ParseIntError,
            Error::InvalidColumnMappingMode(_) => KernelError::InvalidColumnMappingModeError,
            Error::InvalidTableLocation(_) => KernelError::InvalidTableLocationError,
            Error::InvalidDecimal(_) => KernelError::InvalidDecimalError,
            Error::InvalidStructData(_) => KernelError::InvalidStructDataError,
            Error::InternalError(_) => KernelError::InternalError,
            Error::Backtraced {
                source,
                backtrace: _,
            } => Self::from(*source),
            Error::InvalidExpressionEvaluation(_) => KernelError::InvalidExpression,
            Error::InvalidLogPath(_) => KernelError::InvalidLogPath,
            Error::InvalidCommitInfo(_) => KernelError::InvalidCommitInfo,
            Error::FileAlreadyExists(_) => KernelError::FileAlreadyExists,
            Error::MissingCommitInfo => KernelError::MissingCommitInfo,
        }
    }
}

/// An error that can be returned to the engine. Engines that wish to associate additional
/// information can define and use any type that is [pointer
/// interconvertible](https://en.cppreference.com/w/cpp/language/static_cast#pointer-interconvertible)
/// with this one -- e.g. by subclassing this struct or by embedding this struct as the first member
/// of a [standard layout](https://en.cppreference.com/w/cpp/language/data_members#Standard-layout)
/// class.
#[repr(C)]
pub struct EngineError {
    etype: KernelError,
}

/// Semantics: Kernel will always immediately return the leaked engine error to the engine (if it
/// allocated one at all), and engine is responsible for freeing it.
#[repr(C)]
pub enum ExternResult<T> {
    Ok(T),
    Err(*mut EngineError),
}

pub type AllocateErrorFn =
    extern "C" fn(etype: KernelError, msg: KernelStringSlice) -> *mut EngineError;

// NOTE: We can't "just" impl From<DeltaResult<T>> because we require an error allocator.
impl<T> ExternResult<T> {
    pub fn is_ok(&self) -> bool {
        match self {
            Self::Ok(_) => true,
            Self::Err(_) => false,
        }
    }
    pub fn is_err(&self) -> bool {
        !self.is_ok()
    }
}

/// Represents an engine error allocator. Ultimately all implementations will fall back to an
/// [`AllocateErrorFn`] provided by the engine, but the trait allows us to conveniently access the
/// allocator in various types that may wrap it.
pub trait AllocateError {
    /// Allocates a new error in engine memory and returns the resulting pointer. The engine is
    /// expected to copy the passed-in message, which is only guaranteed to remain valid until the
    /// call returns. Kernel will always immediately return the result of this method to the engine.
    ///
    /// # Safety
    ///
    /// The string slice must be valid until the call returns, and the error allocator must also be
    /// valid.
    unsafe fn allocate_error(&self, etype: KernelError, msg: KernelStringSlice)
        -> *mut EngineError;
}

impl AllocateError for AllocateErrorFn {
    unsafe fn allocate_error(
        &self,
        etype: KernelError,
        msg: KernelStringSlice,
    ) -> *mut EngineError {
        self(etype, msg)
    }
}

impl AllocateError for &dyn ExternEngine {
    /// # Safety
    ///
    /// In addition to the usual requirements, the engine handle must be valid.
    unsafe fn allocate_error(
        &self,
        etype: KernelError,
        msg: KernelStringSlice,
    ) -> *mut EngineError {
        self.error_allocator().allocate_error(etype, msg)
    }
}

/// Converts a [DeltaResult] into an [ExternResult], using the engine's error allocator.
///
/// # Safety
///
/// The allocator must be valid.
trait IntoExternResult<T> {
    unsafe fn into_extern_result(self, alloc: &dyn AllocateError) -> ExternResult<T>;
}

impl<T> IntoExternResult<T> for DeltaResult<T> {
    unsafe fn into_extern_result(self, alloc: &dyn AllocateError) -> ExternResult<T> {
        match self {
            Ok(ok) => ExternResult::Ok(ok),
            Err(err) => {
                let msg = format!("{}", err);
                let err = unsafe { alloc.allocate_error(err.into(), kernel_string_slice!(msg)) };
                ExternResult::Err(err)
            }
        }
    }
}

// A wrapper for Engine which defines additional FFI-specific methods.
pub trait ExternEngine: Send + Sync {
    fn engine(&self) -> Arc<dyn Engine>;
    fn error_allocator(&self) -> &dyn AllocateError;
}

#[handle_descriptor(target=dyn ExternEngine, mutable=false)]
pub struct SharedExternEngine;

struct ExternEngineVtable {
    // Actual engine instance to use
    engine: Arc<dyn Engine>,
    allocate_error: AllocateErrorFn,
}

impl Drop for ExternEngineVtable {
    fn drop(&mut self) {
        debug!("dropping engine interface");
    }
}

/// # Safety
///
/// Kernel doesn't use any threading or concurrency. If engine chooses to do so, engine is
/// responsible for handling  any races that could result.
unsafe impl Send for ExternEngineVtable {}

/// # Safety
///
/// Kernel doesn't use any threading or concurrency. If engine chooses to do so, engine is
/// responsible for handling any races that could result.
///
/// These are needed because anything wrapped in Arc "should" implement it
/// Basically, by failing to implement these traits, we forbid the engine from being able to declare
/// its thread-safety (because rust assumes it is not threadsafe). By implementing them, we leave it
/// up to the engine to enforce thread safety if engine chooses to use threads at all.
unsafe impl Sync for ExternEngineVtable {}

impl ExternEngine for ExternEngineVtable {
    fn engine(&self) -> Arc<dyn Engine> {
        self.engine.clone()
    }
    fn error_allocator(&self) -> &dyn AllocateError {
        &self.allocate_error
    }
}

/// # Safety
///
/// Caller is responsible for passing a valid path pointer.
unsafe fn unwrap_and_parse_path_as_url(path: KernelStringSlice) -> DeltaResult<Url> {
    let path: &str = unsafe { TryFromStringSlice::try_from_slice(&path) }?;
    let table = Table::try_from_uri(path)?;
    Ok(table.location().clone())
}

/// A builder that allows setting options on the `Engine` before actually building it
#[cfg(feature = "default-engine")]
pub struct EngineBuilder {
    url: Url,
    allocate_fn: AllocateErrorFn,
    options: HashMap<String, String>,
}

#[cfg(feature = "default-engine")]
impl EngineBuilder {
    fn set_option(&mut self, key: String, val: String) {
        self.options.insert(key, val);
    }
}

/// Get a "builder" that can be used to construct an engine. The function
/// [`set_builder_option`] can be used to set options on the builder prior to constructing the
/// actual engine
///
/// # Safety
/// Caller is responsible for passing a valid path pointer.
#[cfg(feature = "default-engine")]
#[no_mangle]
pub unsafe extern "C" fn get_engine_builder(
    path: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<*mut EngineBuilder> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    get_engine_builder_impl(url, allocate_error).into_extern_result(&allocate_error)
}

#[cfg(feature = "default-engine")]
fn get_engine_builder_impl(
    url: DeltaResult<Url>,
    allocate_fn: AllocateErrorFn,
) -> DeltaResult<*mut EngineBuilder> {
    let builder = Box::new(EngineBuilder {
        url: url?,
        allocate_fn,
        options: HashMap::default(),
    });
    Ok(Box::into_raw(builder))
}

/// Set an option on the builder
///
/// # Safety
///
/// Caller must pass a valid EngineBuilder pointer, and valid slices for key and value
#[cfg(feature = "default-engine")]
#[no_mangle]
pub unsafe extern "C" fn set_builder_option(
    builder: &mut EngineBuilder,
    key: KernelStringSlice,
    value: KernelStringSlice,
) {
    let key = unsafe { String::try_from_slice(&key) };
    let value = unsafe { String::try_from_slice(&value) };
    // TODO: Return ExternalError if key or value is invalid? (builder has an error allocator)
    let key = match key {
        Ok(key) => key,
        Err(err) => {
            warn!("Ignoring invalid option key: {err}");
            return;
        }
    };
    let value = match value {
        Ok(value) => value,
        Err(err) => {
            warn!("Ignoring invalid option value: {err}");
            return;
        }
    };
    builder.set_option(key, value);
}

/// Consume the builder and return a `default` engine. After calling, the passed pointer is _no
/// longer valid_.
///
///
/// # Safety
///
/// Caller is responsible to pass a valid EngineBuilder pointer, and to not use it again afterwards
#[cfg(feature = "default-engine")]
#[no_mangle]
pub unsafe extern "C" fn builder_build(
    builder: *mut EngineBuilder,
) -> ExternResult<Handle<SharedExternEngine>> {
    let builder_box = unsafe { Box::from_raw(builder) };
    get_default_engine_impl(
        builder_box.url,
        builder_box.options,
        builder_box.allocate_fn,
    )
    .into_extern_result(&builder_box.allocate_fn)
}

/// # Safety
///
/// Caller is responsible for passing a valid path pointer.
#[cfg(feature = "default-engine")]
#[no_mangle]
pub unsafe extern "C" fn get_default_engine(
    path: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedExternEngine>> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    get_default_default_engine_impl(url, allocate_error).into_extern_result(&allocate_error)
}

// get the default version of the default engine :)
#[cfg(feature = "default-engine")]
fn get_default_default_engine_impl(
    url: DeltaResult<Url>,
    allocate_error: AllocateErrorFn,
) -> DeltaResult<Handle<SharedExternEngine>> {
    get_default_engine_impl(url?, Default::default(), allocate_error)
}

/// # Safety
///
/// Caller is responsible for passing a valid path pointer.
#[cfg(feature = "sync-engine")]
#[no_mangle]
pub unsafe extern "C" fn get_sync_engine(
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedExternEngine>> {
    get_sync_engine_impl(allocate_error).into_extern_result(&allocate_error)
}

#[cfg(feature = "default-engine")]
fn get_default_engine_impl(
    url: Url,
    options: HashMap<String, String>,
    allocate_error: AllocateErrorFn,
) -> DeltaResult<Handle<SharedExternEngine>> {
    use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
    use delta_kernel::engine::default::DefaultEngine;
    let engine = DefaultEngine::<TokioBackgroundExecutor>::try_new(
        &url,
        options,
        Arc::new(TokioBackgroundExecutor::new()),
    );
    let engine: Arc<dyn ExternEngine> = Arc::new(ExternEngineVtable {
        engine: Arc::new(engine?),
        allocate_error,
    });
    Ok(engine.into())
}

#[cfg(feature = "sync-engine")]
fn get_sync_engine_impl(
    allocate_error: AllocateErrorFn,
) -> DeltaResult<Handle<SharedExternEngine>> {
    let engine = delta_kernel::engine::sync::SyncEngine::new();
    let engine: Arc<dyn ExternEngine> = Arc::new(ExternEngineVtable {
        engine: Arc::new(engine),
        allocate_error,
    });
    Ok(engine.into())
}

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_engine(engine: Handle<SharedExternEngine>) {
    debug!("engine released engine");
    engine.drop_handle();
}

#[handle_descriptor(target=Snapshot, mutable=false, sized=true)]
pub struct SharedSnapshot;

/// Get the latest snapshot from the specified table
///
/// # Safety
///
/// Caller is responsible for passing valid handles and path pointer.
#[no_mangle]
pub unsafe extern "C" fn snapshot(
    path: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedSnapshot>> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    let engine = unsafe { engine.as_ref() };
    snapshot_impl(url, engine).into_extern_result(&engine)
}

fn snapshot_impl(
    url: DeltaResult<Url>,
    extern_engine: &dyn ExternEngine,
) -> DeltaResult<Handle<SharedSnapshot>> {
    let snapshot = Snapshot::try_new(url?, extern_engine.engine().as_ref(), None)?;
    Ok(Arc::new(snapshot).into())
}

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_snapshot(snapshot: Handle<SharedSnapshot>) {
    debug!("engine released snapshot");
    snapshot.drop_handle();
}

/// Get the version of the specified snapshot
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn version(snapshot: Handle<SharedSnapshot>) -> u64 {
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot.version()
}

/// Get the resolved root of the table. This should be used in any future calls that require
/// constructing a path
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_table_root(
    snapshot: Handle<SharedSnapshot>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let snapshot = unsafe { snapshot.as_ref() };
    let table_root = snapshot.table_root().to_string();
    allocate_fn(kernel_string_slice!(table_root))
}

type StringIter = dyn Iterator<Item = String> + Send;

#[handle_descriptor(target=StringIter, mutable=true, sized=false)]
pub struct StringSliceIterator;

/// # Safety
///
/// The iterator must be valid (returned by [kernel_scan_data_init]) and not yet freed by
/// [kernel_scan_data_free]. The visitor function pointer must be non-null.
#[no_mangle]
pub unsafe extern "C" fn string_slice_next(
    data: Handle<StringSliceIterator>,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(engine_context: NullableCvoid, slice: KernelStringSlice),
) -> bool {
    string_slice_next_impl(data, engine_context, engine_visitor)
}

fn string_slice_next_impl(
    mut data: Handle<StringSliceIterator>,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(engine_context: NullableCvoid, slice: KernelStringSlice),
) -> bool {
    let data = unsafe { data.as_mut() };
    if let Some(data) = data.next() {
        (engine_visitor)(engine_context, kernel_string_slice!(data));
        true
    } else {
        false
    }
}

/// # Safety
///
/// Caller is responsible for (at most once) passing a valid pointer to a [`StringSliceIterator`]
#[no_mangle]
pub unsafe extern "C" fn free_string_slice_data(data: Handle<StringSliceIterator>) {
    data.drop_handle();
}

// A set that can identify its contents by address
pub struct ReferenceSet<T> {
    map: std::collections::HashMap<usize, T>,
    next_id: usize,
}

impl<T> ReferenceSet<T> {
    pub fn new() -> Self {
        Default::default()
    }

    // Inserts a new value into the set. This always creates a new entry
    // because the new value cannot have the same address as any existing value.
    // Returns a raw pointer to the value. This pointer serves as a key that
    // can be used later to take() from the set, and should NOT be dereferenced.
    pub fn insert(&mut self, value: T) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        self.map.insert(id, value);
        id
    }

    // Attempts to remove a value from the set, if present.
    pub fn take(&mut self, i: usize) -> Option<T> {
        self.map.remove(&i)
    }

    // True if the set contains an object whose address matches the pointer.
    pub fn contains(&self, id: usize) -> bool {
        self.map.contains_key(&id)
    }

    // The current size of the set.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl<T> Default for ReferenceSet<T> {
    fn default() -> Self {
        Self {
            map: Default::default(),
            // NOTE: 0 is interpreted as None
            next_id: 1,
        }
    }
}
