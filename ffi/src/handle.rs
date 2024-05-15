//! Primitives for conspicuously transferring ownership of rust objects across an FFI boundary.
//!
//! Creating a handle always implies some kind of ownership transfer. A mutable handle takes
//! ownership of the object itself (analagous to `Box<T>`), while a non-mutable (shared) handle
//! takes ownership of a shared reference to the object (analagous to one instance instance of an
//! `Arc<T>`). Thus, a created handle remains valid, and its underlying object remains accessible,
//! until the handle is explicitly dropped. Dropping a mutable handle always drops the underlying
//! object, while dropping a shared handle only drops the underlying object if the handle was the
//! last reference to that object.
//!
//! Because handles carry ownership semantics, and lifetime information is not preserved across the
//! FFI boundary, handles are always opaque types to avoid confusiong them with normal shared
//! references (`&Foo`, `*const Foo`, etc) that are only valid until the FFI call returns.
use std::ptr::NonNull;
use std::sync::Arc;

/// Used for mutable access to a mutable handle's underlying object.
/// Similar to [AsMut], but unsafe and hard-wired to the handle's target type.
pub trait HandleAsMut {
    type Target: ?Sized;

    /// Obtains a mutable reference to the handle's underlying object.
    unsafe fn as_mut(&mut self) -> &mut Self::Target;
}

/// Used for [Arc] access to a shared handle's underlying object.
pub trait HandleAsArc {
    type Target: ?Sized;

    /// Creates a new [Arc] from the handle, increasing the underlying object's refcount by one.
    unsafe fn as_arc(&self) -> Arc<Self::Target>;
}

/// Describes the kind of handle a given opaque pointer type represents.
pub trait HandleDescriptor {
    /// The actual type of the handle's underlying object
    type Target: ?Sized + Send + Sync;
    /// If true, the handle owns the object (`Box`-like); otherwise, the handle owns a reference to
    /// the object (`Arc`-like).
    type Mutable: Boolean;
    /// True if the handle's [Target] is [Sized].
    type Sized: Boolean;
}

mod private {

    use std::mem::ManuallyDrop;
    use std::sync::Arc;

    use super::*;

    /// Represents an object that crosses the FFI boundary and which outlives the scope that created
    /// it. It remains valid until explicitly dropped by a call to [Handle::drop_handle].
    ///
    /// cbindgen:transparent-typedef
    #[repr(transparent)]
    pub struct Handle<H: HandleDescriptor> {
        ptr: NonNull<H>,
    }

    unsafe impl<H: HandleDescriptor> Send for Handle<H> {}
    unsafe impl<H: HandleDescriptor> Sync for Handle<H> {}

    impl<T, M, S, H> Handle<H>
    where
        T: ?Sized,
        H: HandleDescriptor<Target=T, Mutable=M, Sized=S> + HandleOps<T, M, S>,
    {
        /// Obtains a shared reference to this handle's underlying object.
        pub unsafe fn as_ref(&self) -> &H::Target {
            H::as_ref(&self.ptr)
        }

        pub unsafe fn into_inner(self) -> H::From {
            H::into_inner(self.ptr)
        }
        /// Destroys this handle.
        pub unsafe fn drop_handle(self) {
            drop(self.into_inner())
        }

    }

    impl<T, S, H> From<Box<T>> for Handle<H>
    where
        T: ?Sized,
        H: HandleDescriptor<Target=T, Mutable=True, Sized=S> + HandleOps<T, True, S, From=Box<T>>,
    {
        fn from(val: Box<T>) -> Handle<H> {
            let ptr = H::into_handle(val).cast();
            Handle { ptr }
        }
    }

    impl<T, S, H> From<Arc<T>> for Handle<H>
    where
        T: ?Sized,
        H: HandleDescriptor<Target=T, Mutable=False, Sized=S> + HandleOps<T, False, S, From=Arc<T>>,
    {
        fn from(val: Arc<T>) -> Handle<H> {
            let ptr = H::into_handle(val).cast();
            Handle { ptr }
        }
    }

    impl<T, S, H> HandleAsMut for Handle<H>
    where
        T: ?Sized,
        H: HandleDescriptor<Target=T, Mutable=True, Sized=S> + HandleOps<T, True, S>,
    {
        type Target = T;

        unsafe fn as_mut(&mut self) -> &mut T {
            H::as_mut(&self.ptr)
        }
    }

    impl<T, S, H> HandleAsArc for Handle<H>
    where
        T: ?Sized,
        H: HandleDescriptor<Target=T, Mutable=False, Sized=S> + HandleOps<T, False, S>,
    {
        type Target = T;

        unsafe fn as_arc(&self) -> Arc<T> {
            H::clone_arc(self.ptr)
        }
    }


    pub trait HandleOps<T: ?Sized, M, S> {
        type From: Sized;
        type Raw: Sized;

        fn into_handle(val: Self::From) -> NonNull<Self::Raw>;

        unsafe fn as_ref(ptr: &NonNull<Self>) -> &T;

        // WARNING: unimplemented when M=False
        unsafe fn as_mut(ptr: &NonNull<Self>) -> &mut T;

        // WARNING: unimplemented when M=True
        unsafe fn clone_arc(ptr: NonNull<Self>) -> Arc<T>;

        unsafe fn into_inner(ptr: NonNull<Self>) -> Self::From;
    }

    // Acts like Box<T: Sized> -- can directly use the input Box
    impl<T, H> HandleOps<T, True, True> for H
    where
          H: HandleDescriptor<Target=T, Mutable=True, Sized=True>,
    {
        type From = Box<T>;
        type Raw = T;

        fn into_handle(val: Box<T>) -> NonNull<T> {
            let val = ManuallyDrop::new(val);
            val.as_ref().into()
        }
        unsafe fn as_ref<'a>(ptr: &'a NonNull<H>) -> &'a T {
            let ptr = ptr.cast();
            unsafe { ptr.as_ref() }
        }
        unsafe fn as_mut<'a>(ptr: &'a NonNull<H>) -> &'a mut T {
            let mut ptr = ptr.cast();
            unsafe { ptr.as_mut() }
        }
        unsafe fn clone_arc(_: NonNull<H>) -> Arc<T> {
            unimplemented!()
        }
        unsafe fn into_inner(ptr: NonNull<H>) -> Box<T> {
            let ptr = ptr.cast().as_ptr();
            unsafe { Box::from_raw(ptr) }
        }
    }

    // Acts like Arc<T: Sized> -- can directly use the input Arc
    impl<T, H> HandleOps<T, False, True> for H
    where
        H: HandleDescriptor<Target=T, Mutable=False, Sized=True>,
    {
        type From = Arc<T>;
        type Raw = T;

        fn into_handle(val: Arc<T>) -> NonNull<T> {
            let val = ManuallyDrop::new(val);
            val.as_ref().into()
        }
        unsafe fn as_ref<'a>(ptr: &'a NonNull<H>) -> &'a T {
            let ptr = ptr.cast();
            unsafe { ptr.as_ref() }
        }
        unsafe fn as_mut(_: &NonNull<H>) -> &mut T {
            unimplemented!()
        }
        unsafe fn clone_arc(ptr: NonNull<H>) -> Arc<T> {
            let ptr = ptr.cast().as_ptr();
            unsafe { Arc::increment_strong_count(ptr) };
            unsafe { Arc::from_raw(ptr) }
        }
        unsafe fn into_inner(ptr: NonNull<H>) -> Arc<T> {
            let ptr = ptr.cast().as_ptr();
            unsafe { Arc::from_raw(ptr) }
        }
    }

    // Acts like Box<T: ?Sized> -- could be a fat pointer, so have to box it up
    impl<T, H> HandleOps<T, True, False> for H
    where
        T: ?Sized,
        H: HandleDescriptor<Target=T, Mutable=True, Sized=False>,
    {
        type From = Box<T>;
        type Raw = Box<T>;

        fn into_handle(val: Box<T>) -> NonNull<Box<T>> {
            // Double-boxing needed in order to obtain a thin pointer
            let val = ManuallyDrop::new(Box::new(val));
            val.as_ref().into()
        }
        unsafe fn as_ref<'a>(ptr: &'a NonNull<H>) -> &'a T {
            let ptr = ptr.cast();
            let boxed: &Box<T> = unsafe { ptr.as_ref() };
            boxed.as_ref()
        }
        unsafe fn as_mut<'a>(ptr: &'a NonNull<H>) -> &'a mut T {
            let mut ptr = ptr.cast();
            let boxed: &mut Box<T> = unsafe { ptr.as_mut() };
            boxed.as_mut()
        }
        unsafe fn clone_arc(_: NonNull<H>) -> Arc<T> {
            unimplemented!()
        }
        unsafe fn into_inner(ptr: NonNull<H>) -> Box<T> {
            let ptr = ptr.cast().as_ptr();
            let boxed = unsafe { Box::from_raw(ptr) };
            *boxed
        }
    }

    // Acts like Arc<T: ?Sized> -- could be a fat pointer, so have to box it up
    impl<T, H> HandleOps<T, False, False> for H
    where
        T: ?Sized,
        H: HandleDescriptor<Target=T, Mutable=False, Sized=False>,
    {
        type From = Arc<T>;
        type Raw = Arc<T>;

        fn into_handle(val: Arc<T>) -> NonNull<Arc<T>> {
            // Double-boxing needed in order to obtain a thin pointer
            let val = ManuallyDrop::new(Box::new(val));
            val.as_ref().into()
        }
        unsafe fn as_ref<'a>(ptr: &'a NonNull<H>) -> &'a T {
            let ptr = ptr.cast();
            let arc: &Arc<T> = unsafe { ptr.as_ref() };
            arc.as_ref()
        }
        unsafe fn as_mut(_: &NonNull<H>) -> &mut T {
            unimplemented!()
        }
        unsafe fn clone_arc(ptr: NonNull<H>) -> Arc<T> {
            let ptr = ptr.cast();
            let arc: &Arc<T> = unsafe { ptr.as_ref() };
            arc.clone()
        }
        unsafe fn into_inner(ptr: NonNull<H>) -> Arc<T> {
            let ptr = ptr.cast().as_ptr();
            let boxed = unsafe { Box::from_raw(ptr) };
            *boxed
        }
    }

    /// A struct that cannot be instantiated by any rust code because this module exposes no public
    /// constructor for it. Intentionally _NOT_ a zero-sized type, to avoid FFI weirdness.
    pub struct Unconstructable {
        _private: usize,
    }

    // workaround for #![feature(associated_const_equality)]
    pub struct True {}
    pub struct False {}

    pub trait Boolean: Sealed {}
    impl Boolean for True {}
    impl Boolean for False {}

    pub trait Sealed {}
    impl Sealed for True {}
    impl Sealed for False {}
}

pub use private::{Boolean, False, Handle, True, Unconstructable};

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    pub struct Foo {
        pub x: usize,
        pub y: String,
    }

    pub struct MutableFoo {
        _unused: usize,
    }

    #[derive(Debug)]
    pub struct Bar {
        pub x: usize,
        pub y: String,
    }

    pub struct SharedBar {
        _unused: usize,
    }


    impl HandleDescriptor for MutableFoo {
        type Target = Foo;
        type Mutable = True;
        type Sized = True;
    }

    impl HandleDescriptor for SharedBar {
        type Target = Bar;
        type Mutable = False;
        type Sized = True;
    }

    pub trait Baz: Send + Sync {
        fn squawk(&self);
    }

    impl Baz for Bar {
        fn squawk(&self) {
            println!("Bar!")
        }
    }

    pub struct MutableBaz {
        _unused: usize,
    }

    pub struct SharedBaz {
        _unused: usize,
    }


    impl HandleDescriptor for MutableBaz {
        type Target = dyn Baz;
        type Mutable = True;
        type Sized = False;
    }

    impl HandleDescriptor for SharedBaz {
        type Target = dyn Baz;
        type Mutable = False;
        type Sized = False;
    }

    #[test]
    fn test_handle_use_cases_compile() {
        let f = Foo { x: 10, y: "hi".into() };
        let mut h: Handle<MutableFoo> = Box::new(f).into();
        let r = unsafe { h.as_mut() };
        println!("{r:?}");

        // error[E0599]: the method `clone_arc` exists for struct `Handle<FooHandle>`, but its trait bounds were not satisfied
        //let _ = unsafe { h.clone_arc() };

        unsafe { h.drop_handle() };

        // error[E0382]: borrow of moved value: `h`
        // let _ = unsafe { h.as_mut() };

        // error[E0451]: field `ptr` of struct `Handle` is private
        // let h = Handle::<FooHandle>{ ptr: std::ptr::null() };

        let b = Bar { x: 10, y: "hi".into() };
        let h: Handle<SharedBar> = Arc::new(b).into();
        let r = unsafe { h.as_ref() };
        println!("{r:?}");

        // error[E0599]: the method `as_mut` exists for struct `Handle<BarHandle>`, but its trait bounds were not satisfied
        // let r = unsafe { h.as_mut() };

        let r = unsafe { h.as_arc() };
        println!("{r:?}");
        unsafe { h.drop_handle() };

        // error[E0382]: borrow of moved value: `h`
        // let _ = unsafe { h.as_ref() };

        let b = Bar { x: 10, y: "hello".into() };
        let t: Box<dyn Baz> = Box::new(b);
        let mut h: Handle<MutableBaz> = t.into();
        let r = unsafe { h.as_mut() };
        r.squawk();
        let r = unsafe { h.as_ref() };
        r.squawk();

        // error[E0599]: the method `clone_arc` exists for struct `Handle<FooHandle>`, but its trait bounds were not satisfied
        //let _ = unsafe { h.clone_arc() };


        let b = Bar { x: 10, y: "hello".into() };
        let t: Arc<dyn Baz> = Arc::new(b);
        let h: Handle<SharedBaz> = t.into();
        let r = unsafe { h.as_ref() };
        r.squawk();
        let r = unsafe { h.as_arc() };
        r.squawk();

    }
}

/*
/// Helper trait that simplifies passing a leaked instance of a (possibly unsized) type across the
/// FFI boundary as a leaked thin pointer. Does not include any reference-counting capability, so
/// the engine is responsible to do whatever reference counting the engine may need. Engine is also
/// responsible not to drop an in-use handle, so that kernel code can safely dereference the
/// pointer.
pub trait BoxHandle: Sized {
    /// The target type this handle represents.
    type Target: ?Sized + Send + Sync;

    /// Converts the target into a (leaked) "handle" that can cross the FFI boundary. The handle
    /// remains valid until passed to [Self::drop_handle].
    fn into_handle(target: impl Into<Box<Self::Target>>) -> *mut Self {
        // NOTE: We need a double-box to ensure the handle is a thin pointer.
        Box::into_raw(Box::new(target.into())) as _
    }

    /// Obtains a shared reference to the underlying
    unsafe fn as_ref(handle: &*const Self) -> &Self::Target {
        let ptr = *handle as *const Box<Self::Target>;
        unsafe { & *ptr }
    }

    /// Obtains a mutable reference to the underlying
    unsafe fn as_mut(handle: &*mut Self) -> &mut Self::Target {
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
pub trait SizedBoxHandle: Sized {
    type Target: Sized + Send + Sync;
}

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
impl<H: SizedBoxHandle + Send + Sync> BoxHandle for H {
    type Target = Self; // not actually needed, but required by the trait

    fn into_handle(target: impl Into<Box<Self>>) -> *mut Self {
        // One layer of boxing is enough, we just need it to be heap-allocated
        Box::into_raw(target.into())
    }

    unsafe fn as_ref(handle: &*const Self) -> &Self::Target {
        unsafe { & **handle }
    }

    unsafe fn as_mut(handle: &*mut Self) -> &mut Self::Target {
        unsafe { &mut **handle }
    }
    unsafe fn drop_handle(handle: *mut Self) {
        let ptr = handle as *mut Self::Target;
        let _ = unsafe { Box::from_raw(ptr) };
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
    type Target: ?Sized + Send + Sync;

    /// Converts the target Arc into a (leaked) "handle" that can cross the FFI boundary. The Arc
    /// refcount does not change. The handle remains valid until passed to [Self::drop_handle].
    fn into_handle(target: impl Into<Arc<Self::Target>>) -> *const Self {
        Box::into_raw(Box::new(target.into())) as _
    }

    /// Obtains a shared reference to the underlying
    unsafe fn as_ref(handle: &*const Self) -> &Self::Target {
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
pub trait SizedArcHandle: Sized {
    type Target: Sized + Send + Sync;
}

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
impl<H, T> ArcHandle for H
where
    H: SizedArcHandle<Target = T>,
    T: Send + Sync
{
    type Target = T;

    fn into_handle(target: impl Into<Arc<Self::Target>>) -> *const Self {
        Arc::into_raw(target.into()) as _
    }

    unsafe fn as_ref(handle: &*const Self) -> &Self::Target {
        let ptr = *handle as *const Self::Target;
        unsafe { & *ptr }
    }

    unsafe fn clone_as_arc(handle: *const Self) -> Arc<Self::Target> {
        let ptr = handle as *const Self::Target;
        unsafe { Arc::increment_strong_count(ptr) };
        unsafe { Arc::from_raw(ptr) }
    }

    unsafe fn drop_handle(handle: *const Self) {
        let ptr = handle as *const Self::Target;
        let _ = unsafe { Arc::from_raw(ptr) };
    }
}

*/
