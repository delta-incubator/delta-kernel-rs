//! A set of traits and types to help safely and ergonically pass rust objects across the FFI
//! boundary as thin pointer "handles". A handle is an opaque type that aids type safety by uniquely
//! identifying some Rust type that may not even be representable (such as traits). There are three
//! kinds of handles:
//!
//! * [BoxHandle] represents the content of a `Box<T>`, where `T` is a sized type.
//! * [ArcHandle] represents the content of an `Arc<T>`, where `T` may not be sized.
//! * [SizedArcHandle] specializes [ArcHandle] to handle sized types efficiently.
//!
//! Box handles are useful for passing owned sized data across the FFI boundary, while Arc handles
//! are more suitable for shared and/or unsized data.
use std::sync::Arc;

mod unconstructable {
    /// A struct that cannot be instantiated by any rust code because this module exposes no public
    /// constructor for it. Intentionally _NOT_ a zero-sized type, to avoid weirdness in C/C++ land.
    pub struct Unconstructable {
        _private: usize,
    }
}

// Make it easy to use (the module was only used to enforce member privacy).
pub type Unconstructable = unconstructable::Unconstructable;


/// Helper trait that simplifies passing a leaked instance of a (possibly unsized) type across the
/// FFI boundary as a leaked thin pointer. Does not include any reference-counting capability, so
/// the engine is responsible to do whatever reference counting the engine may need. Engine is also
/// responsible not to drop an in-use handle, so that kernel code can safely dereference the
/// pointer.
pub trait BoxHandle: Sized {
    /// The target type this handle represents.
    type Target: ?Sized;

    /// Converts the target into a (leaked) "handle" that can cross the FFI boundary. The handle
    /// remains valid until passed to [Self::drop_handle].
    fn into_handle(target: impl Into<Box<Self::Target>>) -> *mut Self {
        // NOTE: We need a double-box to ensure the handle is a thin pointer.
        Box::into_raw(Box::new(target.into())) as _
    }

    /// Obtains a shared reference to the underlying
    unsafe fn as_ref<'a>(handle: &'a *const Self) -> &'a Self::Target {
        let ptr = *handle as *const Box<Self::Target>;
        unsafe { & *ptr }
    }

    /// Obtains a mutable reference to the underlying
    unsafe fn as_mut<'a>(handle: &'a *mut Self) -> &'a mut Self::Target {
        let ptr = *handle as *mut Box<Self::Target>;
        unsafe { &mut *ptr }
    }

    /// Drops the handle, invalidating it and freeing the underlying object.
    ///
    /// # Safety
    ///
    /// Caller of this method asserts that `handle` satisfies all of the following conditions:
    ///
    /// * Obtained by calling [into_handle]
    /// * Not previously passed to [drop_handle]
    /// * Has no other live references
    unsafe fn drop_handle(handle: *mut Self) {
        let ptr = handle as *mut Box<Self::Target>;
        let _ = unsafe { Box::from_raw(ptr) };
    }

}

/// A marker trait that allows a [Sized] type to act as its own [BoxHandle]. This is more efficient
/// and also produces a pointer of the same type that can be used directly (no type aliasing).
pub trait SizedBoxHandle: Sized { }

// A blanket implementation of `BoxHandle` for all types satisfying `SizedBoxHandle`.
//
// Unlike the default (unsized) implementation, which must wrap the `Box` in second `Box` in order
// to obtain a thin pointer, the sized implementation can directly return a pointer to the
// underlying type. This blanket implementation applies automatically to all types that implement
// `SizedBoxHandle`, so a type that implements `SizedBoxHandle` cannot directly implement
// `BoxHandle` (which is a Good Thing, because it preserves the important type safety invariant that
// every handle has exactly one `Target` type):
//
// This blanket implementation applies automatically to all types that implement `SizedBoxHandle`,
// so a type that implements `SizedBoxHandle` cannot directly implement `BoxHandle` (which is a Good
// Thing, because it preserves the important type safety invariant that every handle has exactly one
// `Target` type):
//
// ```
// error[E0119]: conflicting implementations of trait `BoxHandle` for type `FooHandle`
//   --> src/main.rs:55:1
//    |
// 28 | impl<H, T> BoxHandle for H where H: SizedBoxHandle<Target = T> {
//    | -------------------------------------------------------------- first implementation here
// ...
// 55 | impl BoxHandle for FooHandle {
//    | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^ conflicting implementation for `FooHandle`
// ```
impl<H: SizedBoxHandle> BoxHandle for H {
    type Target = Self; // not actually needed, but required by the trait

    fn into_handle(target: impl Into<Box<Self>>) -> *mut Self {
        // One layer of boxing is enough, we just need it to be heap-allocated
        Box::into_raw(target.into())
    }

    unsafe fn as_ref<'a>(handle: &'a *const Self) -> &'a Self::Target {
        unsafe { & **handle }
    }

    unsafe fn as_mut<'a>(handle: &'a *mut Self) -> &'a mut Self::Target {
        unsafe { &mut **handle }
    }
}

/// Helper trait that allows passing an `Arc<Target>` across the FFI boundary as a thin-pointer
/// handle type. The pointer remains valid until freed by a call to [ArcHandle::drop_handle]. The
/// handle is strongly typed, in the sense that each handle type maps to a single `Target` type.
///
/// This trait tracks a shared reference to the underlying, which differs from `BoxHandle` in that
/// the latter tracks an owned reference. Thus, constructing an `ArcHandle` only leaks a reference
/// count (which in turn may cause the underlying to leak), and it is easy to clone additional `Arc`
/// from a given `ArcHandle` as needed.
///
/// Typically, the handle (`Self`) is an opaque struct (_not_ `repr(C)`) with an FFI-friendly name
/// name, containing an [Unconstructable] member so rust code cannot legally instantiate it.
///
/// # Examples
///
/// To define and use a handle for a trait or other unsized type:
/// ```
/// // The (unsized) trait to pass across the FFI boundary
/// trait MyTrait { ... }
///
/// // The handle that will represent `MyStruct` externally
/// struct MyTraitHandle {
///     _unconstructable: Unconstructable,
/// }
///
/// // Connect the two
/// impl ArcHandle for MyTraitHandle {
///     type Target = dyn MyTrait;
/// }
///
/// fn unsized_handle_example(val: Arc<dyn MyTrait>) {
///     let handle: *const MyTraitHandle = ArcHandle::into_handle(val);
///     let val2 = unsafe { ArcHandle::clone_as_arc(handle) };
///     unsafe { ArcHandle::drop_handle(handle) };
///     // `handle` is no longer safe to use, but `val2` (a cloned Arc) is still valid
/// }
/// ```
///
/// To define and use a handle optimized for a sized type, just impl [SizedArcHandle] (which
/// automatically impl [ArcHandle]), and use the [ArcHandle] API in the same way as for unsized:
///
/// ```
/// // The (sized) struct to pass across the FFI boundary struct
/// MyStruct { ... }
///
/// // The handle that will represent `MyStruct` externally
/// struct MyStructHandle {
///     _unconstructable: Unconstructable,
/// }
///
/// // Connect the two
/// impl SizedArcHandle for MyStructHandle {
///     type Target = MyStruct;
/// }
///
/// fn sized_handle_example(val: Arc<MyStruct>) {
///     let handle: *const MyStructHandle = ArcHandle::into_handle(val);
///     let val2 = unsafe { ArcHandle::clone_as_arc(handle) };
///     unsafe { ArcHandle::drop_handle(handle) };
///     // `handle` is no longer safe to use, but `val2` (a cloned Arc) is still valid
/// }
/// ```
///
/// # Safety
///
/// In addition to being a raw pointer, a handle always points to an underlying allocation of some
/// other type. Thus, it is _ALWAYS_ incorrect to dereference (`*`) a handle or cast it to any other
/// type, (including `Target`). The only safe operations on a handle are to copy the pointer itself
/// by value and to invoke associated functions of `ArcHandle`.
///
/// Additionally, this trait intentionally does _NOT_ expose anything like an `as_ref()` function,
/// because the resulting reference would have an arbitrary (= unnecessarily dangerous) lifetime
/// that cannot be safely enforced, even if we aggressively bound its lifetime. For example:
/// ```
/// let handle: *const MyHandle = ArcHandle::into_handle(...);
/// let handle_copy = handle; // does NOT increment the Arc refcount
/// let my_ref = ArcHandle::as_ref(&handle);
/// ArcHandle::drop_handle(handle_copy); // still-borrowed `handle` can't prevent this
/// my_ref.some_access(); // Illegal access to dangling reference
/// ```
///
/// If a borrowed reference is needed, call [ArcHandle::clone_as_arc] and invoke [Arc::as_ref] on
/// the result.
/// ```
/// let handle: *const MyHandle = ArcHandle::into_handle(...);
/// let handle_copy = handle; // does NOT increment the Arc refcount
/// let my_ref = ArcHandle::clone_as_arc(handle); // increments refcount
/// ArcHandle::drop_handle(handle);
/// my_ref.some_access(); // No problem.
/// ```
///
/// # Synchronization
///
/// The handle (a raw pointer) is neither [Send] nor [Sync] by default:
///
/// It is ONLY safe to manually implement [Send] for a handle (or a type that embeds it) if all of
/// the following conditions hold:
///
/// * The handle's `Target` type is `Send`
///
/// * Calls to [ArcHandle::clone_as_arc] and [ArcHandle::drop_handle] always obey their respective
///   safety requirements. In particular, this means proving (or at least affirming) that no handle
///   is ever accessed after passing it to [ArcHandle::drop_handle].
///
/// It is ONLY safe to implement [Sync] for a handle (or type embedding it) if all of the following
/// conditions hold:
///
/// * The `Target` type is `Sync`
///
/// * The handle is (correctly) `Send`
pub trait ArcHandle: Sized {
    /// The target type this handle represents.
    type Target: ?Sized;

    /// Converts the target Arc into a (leaked) "handle" that can cross the FFI boundary. The Arc
    /// refcount does not change. The handle remains valid until passed to [Self::drop_handle].
    fn into_handle(target: impl Into<Arc<Self::Target>>) -> *const Self {
        Box::into_raw(Box::new(target.into())) as _
    }

    /// Obtains a shared reference to the underlying
    unsafe fn as_ref<'a>(handle: &'a *const Self) -> &'a Self::Target {
        let ptr = *handle as *const Arc<Self::Target>;
        unsafe { & *ptr }
    }

    /// Clones an Arc from an FFI handle for kernel use, without dropping the handle. The Arc
    /// refcount increases by one.
    ///
    /// # Safety
    ///
    /// Caller of this method asserts that `handle` satisfies all of the following conditions:
    ///
    /// * Obtained by calling [into_handle]
    /// * Never cast to any other type nor dereferenced
    /// * Not previously passed to [drop_handle]
    unsafe fn clone_as_arc(handle: *const Self) -> Arc<Self::Target> {
        let ptr = handle as *const Arc<Self::Target>;
        let arc = unsafe { & *ptr };
        arc.clone()
    }

    /// Drops the handle, invalidating it. The Arc refcount decreases by one, which may drop the
    /// underlying trait instance as well.
    ///
    /// # Safety
    ///
    /// Caller of this method asserts that `handle` satisfies all of the following conditions:
    ///
    /// * Obtained by calling [into_handle]
    /// * Never cast to any other type nor dereferenced
    /// * Not previously passed to [drop_handle]
    /// * Has no other live references
    unsafe fn drop_handle(handle: *const Self) {
        let ptr = handle as *mut Arc<Self::Target>;
        let _ = unsafe { Box::from_raw(ptr) };
    }
}

/// A marker trait that allows a [Sized] type to act as its own [ArcHandle]. This is more efficient
/// and also produces a pointer of the same type that can be used directly (no type aliasing).
pub trait SizedArcHandle: Sized { }

// A blanket implementation of `ArcHandle` for all types satisfying `SizedArcHandle`.
//
// Unlike the default (unsized) implementation, which must wrap the `Arc` in a `Box` in order to
// obtain a thin pointer, the sized implementation can directly return a pointer to the underlying
// type. This blanket implementation applies automatically to all types that implement
// `SizedArcHandle`, so a type that implements `SizedArcHandle` cannot directly implement
// `ArcHandle` (which is a Good Thing, because it preserves the important type safety invariant that
// every handle has exactly one `Target` type):
//
// ```
// error[E0119]: conflicting implementations of trait `ArcHandle` for type `FooHandle`
//   --> src/main.rs:55:1
//    |
// 28 | impl<H, T> ArcHandle for H where H: SizedArcHandle<Target = T> {
//    | -------------------------------------------------------------- first implementation here
// ...
// 55 | impl ArcHandle for FooHandle {
//    | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^ conflicting implementation for `FooHandle`
// ```
impl<H: SizedArcHandle> ArcHandle for H {
    type Target = Self; // not actually needed, but required by the trait

    fn into_handle(target: impl Into<Arc<Self>>) -> *const Self {
        Arc::into_raw(target.into())
    }

    unsafe fn as_ref<'a>(handle: &'a *const Self) -> &'a Self::Target {
        unsafe { & **handle }
    }

    unsafe fn clone_as_arc(handle: *const Self) -> Arc<Self> {
        unsafe { Arc::increment_strong_count(handle) };
        unsafe { Arc::from_raw(handle) }
    }

    unsafe fn drop_handle(handle: *const Self) {
        let _ = unsafe { Arc::from_raw(handle) };
    }
}
