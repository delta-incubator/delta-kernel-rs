//! [`Handle`] is an idiom for visibly transferring ownership of opaque rust objects across the FFI
//! boundary.
//!
//! Creating a [`Handle<T>`] always implies some kind of ownership transfer. A mutable handle takes
//! ownership of the object itself (analagous to [`Box<T>`]), while a non-mutable (shared) handle
//! takes ownership of a shared reference to the object (analagous to [`Arc<T>`]). Thus, a created
//! handle remains [valid][Handle#Validity], and its underlying object remains accessible, until the
//! handle is explicitly dropped or consumed. Dropping a mutable handle always drops the underlying
//! object as well; dropping a shared handle only drops the underlying object if the handle was the
//! last reference to that object.
//!
//! Because handles carry ownership semantics, and lifetime information is not preserved across the
//! FFI boundary, handles are always opaque types to avoid confusiong them with normal references
//! and pointers (`&Foo`, `*const Foo`, etc) that are possibly only valid until the FFI call
//! returns. For the same reason, mutable handles implement neither [`Copy`] nor [`Clone`]. However,
//! this only helps on the Rust side, because handles appear as simple pointers in the FFI and are
//! thus easily duplicated there. In order to improve safety, external (non-Rust) code is strongly
//! advised to maintain "unique pointer" semantics for mutable handles.
//!
//! NOTE: While shared handles could conceptually impl [`Clone`], cloning would require unsafe code
//! and so we can't actually implement the trait. Use [`CloneHandle::clone_handle`] instead.
use std::ptr::NonNull;
use std::sync::Arc;

/// Provides mutable access to the underlying object of a mutable [`Handle`] .
/// Similar to [`AsMut`], but unsafe and hard-wired to the handle's target type.
pub trait HandleAsMut {
    /// The true type of the underlying object.
    type Target: ?Sized;

    /// Obtains a mutable reference to the handle's underlying object.
    ///
    /// # Safety
    ///
    /// Caller asserts that this handle obeys [mutable reference
    /// semantics](https://doc.rust-lang.org/book/ch04-02-references-and-borrowing.html#the-rules-of-references),
    /// namely:
    ///
    /// * The handle is [valid][Handle#Validity].
    ///
    /// * No other references (neither shared nor mutable) can overlap with the returned reference.
    unsafe fn as_mut(&mut self) -> &mut Self::Target;
}

/// Clones a a shared [`Handle`]. Similar to [`Clone`], but unsafe. Also allows to clone the handle
/// as an [`Arc`] for easy access to the handle's underlying object.
pub trait CloneHandle {
    /// The true type of the underlying object.
    type Target: ?Sized;

    /// Clones this handle, increasing the underlying object's refcount by one. The new handle is
    /// independent of the source handle, and should be dropped when it is no longer needed. This is
    /// equivalent to calling `clone_as_arc().into_handle()`.
    ///
    /// # Safety
    ///
    /// Caller asserts that the handle is [valid][Handle#Validity].
    unsafe fn clone_handle(&self) -> Self;

    /// Creates a new `Arc<T>` from this handle, increasing the underlying object's refcount by
    /// one. This is equivalent to calling `clone_handle().into_inner()`.
    ///
    /// # Safety
    ///
    /// Caller asserts that the handle is [valid][Handle#Validity].
    unsafe fn clone_as_arc(&self) -> Arc<Self::Target>;
}

/// Describes the kind of handle a given opaque pointer type represents.
///
/// It is not normally necessary to implement this trait directly; instead, use the provided
/// attribute macro `#[handle_descriptor]` to generate the implementation automatically, e.g.
/// ```
/// pub struct Foo {
///     x: usize,
/// }
///
/// #[handle_descriptor(target = "Foo", mutable = true, sized = true)]
/// pub struct MutableFoo;
/// ```
pub trait HandleDescriptor {
    /// The true type of the handle's underlying object
    type Target: ?Sized + Send + Sync;

    /// If `True`, the handle owns the underlying object (`Box`-like); otherwise, the handle owns a
    /// shared reference to the underlying object (`Arc`-like).
    type Mutable: Boolean;

    /// `True` iff the handle's `Target` is [`Sized`].
    type Sized: Boolean;
}

mod private {

    use std::mem::ManuallyDrop;
    use std::sync::Arc;

    use super::*;

    /// Represents an object that crosses the FFI boundary and which outlives the scope that created
    /// it. It can be passed freely between rust code and external code.
    ///
    /// # Validity
    ///
    /// A `Handle` is _valid_ if all of the following hold:
    ///
    /// * It was created by a call to [`Handle::from`]
    /// * Not dropped by a call to [`Handle::drop_handle`]
    /// * Not consumed by a call to [`Handle::into_inner`]
    /// * All concurrent uses in external code uphold [`Send`] and [`Sync`] semantics
    ///
    /// cbindgen:transparent-typedef
    #[repr(transparent)]
    pub struct Handle<H: HandleDescriptor> {
        ptr: NonNull<H>,
    }

    /// # Safety
    ///
    /// Kernel does not directly employ concurrency, but is nevertheless concurrency-friendly. The
    /// underlying type of a `Handle` is always `Send`, so Rust code will handle it correctly even
    /// if the external code calls into kernel from a different thread than the one that created the
    /// handle. External code that uses concurrency must use appropriate mutual exclusion to uphold
    /// the [`Send`
    /// contract](https://doc.rust-lang.org/book/ch16-04-extensible-concurrency-sync-and-send.html#allowing-transference-of-ownership-between-threads-with-send).
    unsafe impl<H: HandleDescriptor> Send for Handle<H> {}

    /// # Safety
    ///
    /// Kernel does not directly employ concurrency, but is nevertheless concurrency-friendly. The
    /// underlying type of a `Handle` is always `Sync`, so Rust code will handle it correctly even
    /// if the external code calls into kernel from a different thread than the one that created the
    /// handle. External code that uses concurrency must use appropriate mutual exclusion to uphold
    /// the [`Sync`
    /// contract](https://doc.rust-lang.org/book/ch16-04-extensible-concurrency-sync-and-send.html#allowing-access-from-multiple-threads-with-sync).
    unsafe impl<H: HandleDescriptor> Sync for Handle<H> {}

    impl<T, M, S, H> Handle<H>
    where
        T: ?Sized,
        H: HandleDescriptor<Target = T, Mutable = M, Sized = S> + HandleOps<T, M, S>,
    {
        /// Obtains a shared reference to this handle's underlying object.
        ///
        /// # Safety
        ///
        /// Caller asserts that this handle obeys [shared reference
        /// semantics](https://doc.rust-lang.org/book/ch04-02-references-and-borrowing.html#the-rules-of-references),
        /// namely:
        ///
        /// * The handle is [valid][Handle#Validity].
        ///
        /// * No mutable references can overlap with the returned reference.
        pub unsafe fn as_ref(&self) -> &H::Target {
            H::as_ref(self.ptr.cast().as_ptr())
        }

        /// Consumes this handle and returns the resource it was originally created `From` ([`Box<T>`]
        /// for mutable handles, and [`Arc<T>`] for shared handles).
        ///
        /// # Safety
        ///
        /// Caller asserts that the handle is [valid][Handle#Validity].
        pub unsafe fn into_inner(self) -> H::From {
            H::into_inner(self.ptr.cast().as_ptr())
        }
        /// Drops this handle. Dropping a mutable handle always drops the underlying object as well;
        /// dropping a shared handle only drops the underlying object if the handle was the last
        /// reference to that object.
        ///
        /// # Safety
        ///
        /// Caller asserts that the handle is [valid][Handle#Validity].
        pub unsafe fn drop_handle(self) {
            drop(self.into_inner())
        }
    }

    // NOTE: While it would be really convenient to `impl From<T> for Handle<H>`, that definition is
    // not disjoint and conflicts with the standard `impl From<T> for T` (since there's no way to
    // prove categorically that T != H). Ditto for `impl<X: Into<Box<T>>> From<X> for Handle<H>`.
    impl<T, S, H> From<Box<T>> for Handle<H>
    where
        T: ?Sized + Send + Sync,
        H: HandleDescriptor<Target = T, Mutable = True, Sized = S>
            + HandleOps<T, True, S, From = Box<T>>,
    {
        fn from(val: Box<T>) -> Handle<H> {
            let ptr = H::into_handle_ptr(val).cast();
            Handle { ptr }
        }
    }

    impl<T, S, H> From<Arc<T>> for Handle<H>
    where
        T: ?Sized + Send + Sync,
        H: HandleDescriptor<Target = T, Mutable = False, Sized = S>
            + HandleOps<T, False, S, From = Arc<T>>,
    {
        fn from(val: Arc<T>) -> Handle<H> {
            let ptr = H::into_handle_ptr(val).cast();
            Handle { ptr }
        }
    }

    impl<T, S, H> HandleAsMut for Handle<H>
    where
        T: ?Sized + Send + Sync,
        H: HandleDescriptor<Target = T, Mutable = True, Sized = S> + HandleOps<T, True, S>,
    {
        type Target = T;

        unsafe fn as_mut(&mut self) -> &mut T {
            H::as_mut(self.ptr.cast().as_ptr())
        }
    }

    impl<T, S, H> CloneHandle for Handle<H>
    where
        T: ?Sized + Send + Sync,
        H: HandleDescriptor<Target = T, Mutable = False, Sized = S>
            + HandleOps<T, False, S, From = Arc<T>>,
    {
        type Target = T;

        unsafe fn clone_handle(&self) -> Self {
            self.clone_as_arc().into()
        }
        unsafe fn clone_as_arc(&self) -> Arc<T> {
            H::clone_arc(self.ptr.cast().as_ptr())
        }
    }

    // Helper for [`Handle`] that encapsulates the various operations for each specialization. By
    // taking `M` (mutable) and `S` (sized) as generic parameters, we can define disjoint generic
    // impl for each of the four combinations of `M` and `S`, without risk of ambiguity.
    //
    // NOTE: This trait is only public because otherwise `Handle` cannot impl it. It is not part of
    // stable public API.
    #[doc(hidden)]
    pub trait HandleOps<T: ?Sized, M, S> {
        type From: Sized;
        type Raw: Sized;

        fn into_handle_ptr(val: Self::From) -> NonNull<Self::Raw>;

        // # Safety
        //
        // This method is VERY unsafe to call directly, because the returned reference receives an
        // arbitrary lifetime provided by its caller. Assuming the safety requirements of
        // [`Handle::as_ref`] are met, that method will ensure safety by assigning a lifetime that
        // does not outlive the source `Handle` itself, and can thus safely call this method..
        unsafe fn as_ref<'a>(ptr: *const Self::Raw) -> &'a T;

        // This method is only required when `M=True`, and implementations with `M=False` should
        // leave it `unimplemented!`.
        //
        // # Safety
        //
        // This method is VERY unsafe to call directly, because the returned reference receives an
        // arbitrary lifetime provided by its caller. Assuming the safety requirements of
        // [`HandleAsMut::as_mut`] are met, that method will ensure safety by assigning a lifetime
        // that does not outlive the source `Handle` itself, and can thus safely call this method.
        unsafe fn as_mut<'a>(ptr: *mut Self::Raw) -> &'a mut T;

        // This method is only required when `M=False`, and implementations with `M=True` should
        // leave it `unimplemented!`.
        //
        // # Safety
        //
        // Assuming the safety requirements of [`CloneHandle::clone_as_arc`] are met, that method
        // can safely call this method.
        unsafe fn clone_arc(ptr: *const Self::Raw) -> Arc<T>;

        // # Safety
        //
        // Assuming the safety requirements of [`Handle::into_inner`] are met, that method can
        // safely call this method.
        unsafe fn into_inner(ptr: *mut Self::Raw) -> Self::From;
    }

    // Acts like `Box<T: Sized>` -- we can directly use the input `Box`
    impl<T, H> HandleOps<T, True, True> for H
    where
        H: HandleDescriptor<Target = T, Mutable = True, Sized = True>,
    {
        type From = Box<T>;
        type Raw = T;

        fn into_handle_ptr(val: Box<T>) -> NonNull<T> {
            let val = ManuallyDrop::new(val);
            val.as_ref().into()
        }
        unsafe fn as_ref<'a>(ptr: *const T) -> &'a T {
            &*ptr
        }
        unsafe fn as_mut<'a>(ptr: *mut T) -> &'a mut T {
            &mut *ptr
        }
        unsafe fn clone_arc(_: *const T) -> Arc<T> {
            unimplemented!()
        }
        unsafe fn into_inner(ptr: *mut T) -> Box<T> {
            Box::from_raw(ptr)
        }
    }

    // Acts like `Arc<T: Sized>` -- we can directly use the input `Arc`
    impl<T, H> HandleOps<T, False, True> for H
    where
        H: HandleDescriptor<Target = T, Mutable = False, Sized = True>,
    {
        type From = Arc<T>;
        type Raw = T;

        fn into_handle_ptr(val: Arc<T>) -> NonNull<T> {
            let val = ManuallyDrop::new(val);
            val.as_ref().into()
        }
        unsafe fn as_ref<'a>(ptr: *const T) -> &'a T {
            &*ptr
        }
        unsafe fn as_mut<'a>(_: *mut T) -> &'a mut T {
            unimplemented!()
        }
        unsafe fn clone_arc(ptr: *const T) -> Arc<T> {
            Arc::increment_strong_count(ptr);
            Arc::from_raw(ptr)
        }
        unsafe fn into_inner(ptr: *mut T) -> Arc<T> {
            Arc::from_raw(ptr)
        }
    }

    // Acts like `Box<T: ?Sized>` -- could be a fat pointer, so we have to box it up
    impl<T, H> HandleOps<T, True, False> for H
    where
        T: ?Sized,
        H: HandleDescriptor<Target = T, Mutable = True, Sized = False>,
    {
        type From = Box<T>;
        type Raw = Box<T>;

        fn into_handle_ptr(val: Box<T>) -> NonNull<Box<T>> {
            // Double-boxing needed in order to obtain a thin pointer
            let val = ManuallyDrop::new(Box::new(val));
            val.as_ref().into()
        }
        unsafe fn as_ref<'a>(ptr: *const Box<T>) -> &'a T {
            let boxed = unsafe { &*ptr };
            boxed.as_ref()
        }
        unsafe fn as_mut<'a>(ptr: *mut Box<T>) -> &'a mut T {
            let boxed = unsafe { &mut *ptr };
            boxed.as_mut()
        }
        unsafe fn clone_arc(_: *const Box<T>) -> Arc<T> {
            unimplemented!()
        }
        unsafe fn into_inner(ptr: *mut Box<T>) -> Box<T> {
            *Box::from_raw(ptr)
        }
    }

    // Acts like `Arc<T: ?Sized>` -- could be a fat pointer, so we have to box it up
    impl<T, H> HandleOps<T, False, False> for H
    where
        T: ?Sized,
        H: HandleDescriptor<Target = T, Mutable = False, Sized = False>,
    {
        type From = Arc<T>;
        type Raw = Arc<T>;

        fn into_handle_ptr(val: Arc<T>) -> NonNull<Arc<T>> {
            // Double-boxing needed in order to obtain a thin pointer
            let val = ManuallyDrop::new(Box::new(val));
            val.as_ref().into()
        }
        unsafe fn as_ref<'a>(ptr: *const Arc<T>) -> &'a T {
            let arc = unsafe { &*ptr };
            arc.as_ref()
        }
        unsafe fn as_mut<'a>(_: *mut Arc<T>) -> &'a mut T {
            unimplemented!()
        }
        unsafe fn clone_arc(ptr: *const Arc<T>) -> Arc<T> {
            let arc = unsafe { &*ptr };
            arc.clone()
        }
        unsafe fn into_inner(ptr: *mut Arc<T>) -> Arc<T> {
            *Box::from_raw(ptr)
        }
    }

    // A NZST struct that cannot be instantiated by any Rust code because this module exposes no
    // public constructor for it. Intentionally _NOT_ a zero-sized type, to avoid FFI weirdness.
    #[doc(hidden)]
    pub struct Unconstructable {
        _private: usize,
    }

    // This trait is a workaround until
    // [`#![feature(associated_const_equality)]`](https://github.com/rust-lang/rust/labels/F-associated_const_equality)
    // stabilizes. Ideally, the `M` and `S` parameters would be actual constant boolean values, but
    // we cannot specialize generics based on constant values. So instead we define this sealed
    // `Boolean` trait along with a couple implementing structs that represent true and false.
    #[doc(hidden)]
    pub trait Boolean: Sealed {}
    #[doc(hidden)]
    pub trait Sealed {}

    #[doc(hidden)]
    pub struct True {}
    #[doc(hidden)]
    pub struct False {}

    impl Boolean for True {}
    impl Boolean for False {}

    impl Sealed for True {}
    impl Sealed for False {}
}

pub use private::{Boolean, False, Handle, True, Unconstructable};

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel_ffi_macros::handle_descriptor;

    #[derive(Debug)]
    pub struct Foo {
        pub x: usize,
        pub y: String,
    }

    #[handle_descriptor(target=Foo, mutable=true, sized=true)]
    pub struct MutableFoo;

    #[derive(Debug)]
    pub struct Bar {
        pub x: usize,
        pub y: String,
    }

    #[handle_descriptor(target=Bar, mutable=false, sized=true)]
    pub struct SharedBar;

    pub trait Baz: Send + Sync {
        fn squawk(&self) -> String;
    }

    impl Baz for Bar {
        fn squawk(&self) -> String {
            format!("{self:?}")
        }
    }

    #[handle_descriptor(target=dyn Baz, mutable=true)]
    pub struct MutableBaz;

    #[handle_descriptor(target=dyn Baz, mutable=false)]
    pub struct SharedBaz;

    #[test]
    fn test_handle_use_cases_compile() {
        let randstr = rand::random::<usize>().to_string();
        let randint = rand::random::<usize>();

        let f = Foo {
            x: randint,
            y: randstr.clone(),
        };
        let s = format!("{f:?}");
        let mut h: Handle<MutableFoo> = Box::new(f).into();
        let r = unsafe { h.as_mut() };
        assert_eq!(format!("{r:?}"), s);

        // error[E0599]: the method `clone_as_arc` exists for struct `Handle<FooHandle>`,
        //               but its trait bounds were not satisfied
        //let _ = unsafe { h.clone_as_arc() };

        unsafe { h.drop_handle() };

        // error[E0382]: borrow of moved value: `h`
        // let _ = unsafe { h.as_mut() };

        // error[E0451]: field `ptr` of struct `Handle` is private
        // let h = Handle::<FooHandle>{ ptr: std::ptr::null() };

        let randstr = rand::random::<usize>().to_string();
        let randint = rand::random::<usize>();

        let b = Bar {
            x: randint,
            y: randstr.clone(),
        };
        let s = format!("{b:?}");
        let h: Handle<SharedBar> = Arc::new(b).into();
        let r = unsafe { h.as_ref() };
        assert_eq!(format!("{r:?}"), s);

        // error[E0599]: the method `as_mut` exists for struct `Handle<BarHandle>`,
        //               but its trait bounds were not satisfied
        // let r = unsafe { h.as_mut() };

        let r = unsafe { h.clone_as_arc() };
        assert_eq!(format!("{r:?}"), s);
        unsafe { h.drop_handle() };

        // error[E0382]: borrow of moved value: `h`
        // let _ = unsafe { h.as_ref() };

        let randstr = rand::random::<usize>().to_string();
        let randint = rand::random::<usize>();

        let b = Bar {
            x: randint,
            y: randstr.clone(),
        };
        let s = format!("{b:?}");
        let t: Arc<dyn Baz> = Arc::new(b);
        let h: Handle<SharedBaz> = t.into();
        let r = unsafe { h.as_ref() };
        r.squawk();
        let r = unsafe { h.clone_as_arc() };
        r.squawk();

        let h2 = unsafe { h.clone_handle() };
        unsafe { h.drop_handle() };

        let b = Bar {
            x: 10,
            y: "hello".into(),
        };
        let t: Box<dyn Baz> = Box::new(b);
        let mut h: Handle<MutableBaz> = t.into();
        let r = unsafe { h.as_mut() };
        r.squawk();
        let r = unsafe { h.as_ref() };
        r.squawk();

        unsafe { h.drop_handle() };

        // error[E0599]: the method `clone_as_arc` exists for struct `Handle<FooHandle>`,
        //               but its trait bounds were not satisfied
        //let _ = unsafe { h.clone_as_arc() };

        let r = unsafe { h2.as_ref() };
        assert_eq!(r.squawk(), s);
        unsafe { h2.drop_handle() };
    }
}
