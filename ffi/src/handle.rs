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
    type Target: ?Sized + Send + Sync;

    /// Obtains a mutable reference to the handle's underlying object.
    unsafe fn as_mut(&mut self) -> &mut Self::Target;
}

/// Used for [Arc] access to a shared handle's underlying object.
pub trait HandleAsArc {
    type Target: ?Sized + Send + Sync;

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
        H: HandleDescriptor<Target = T, Mutable = M, Sized = S> + HandleOps<T, M, S>,
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
        T: ?Sized + Send + Sync,
        H: HandleDescriptor<Target = T, Mutable = True, Sized = S>
            + HandleOps<T, True, S, From = Box<T>>,
    {
        fn from(val: Box<T>) -> Handle<H> {
            let ptr = H::into_handle(val).cast();
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
            let ptr = H::into_handle(val).cast();
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
            H::as_mut(&mut self.ptr)
        }
    }

    impl<T, S, H> HandleAsArc for Handle<H>
    where
        T: ?Sized + Send + Sync,
        H: HandleDescriptor<Target = T, Mutable = False, Sized = S> + HandleOps<T, False, S>,
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
        unsafe fn as_mut(ptr: &mut NonNull<Self>) -> &mut T;

        // WARNING: unimplemented when M=True
        unsafe fn clone_arc(ptr: NonNull<Self>) -> Arc<T>;

        unsafe fn into_inner(ptr: NonNull<Self>) -> Self::From;
    }

    // Acts like Box<T: Sized> -- can directly use the input Box
    impl<T, H> HandleOps<T, True, True> for H
    where
        H: HandleDescriptor<Target = T, Mutable = True, Sized = True>,
    {
        type From = Box<T>;
        type Raw = T;

        fn into_handle(val: Box<T>) -> NonNull<T> {
            let val = ManuallyDrop::new(val);
            val.as_ref().into()
        }
        unsafe fn as_ref(ptr: &NonNull<H>) -> &T {
            let ptr = ptr.cast();
            unsafe { ptr.as_ref() }
        }
        unsafe fn as_mut(ptr: &mut NonNull<H>) -> &mut T {
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
        H: HandleDescriptor<Target = T, Mutable = False, Sized = True>,
    {
        type From = Arc<T>;
        type Raw = T;

        fn into_handle(val: Arc<T>) -> NonNull<T> {
            let val = ManuallyDrop::new(val);
            val.as_ref().into()
        }
        unsafe fn as_ref(ptr: &NonNull<H>) -> &T {
            let ptr = ptr.cast();
            unsafe { ptr.as_ref() }
        }
        unsafe fn as_mut(_: &mut NonNull<H>) -> &mut T {
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
        H: HandleDescriptor<Target = T, Mutable = True, Sized = False>,
    {
        type From = Box<T>;
        type Raw = Box<T>;

        fn into_handle(val: Box<T>) -> NonNull<Box<T>> {
            // Double-boxing needed in order to obtain a thin pointer
            let val = ManuallyDrop::new(Box::new(val));
            val.as_ref().into()
        }
        unsafe fn as_ref(ptr: &NonNull<H>) -> &T {
            let ptr: NonNull<Box<T>> = ptr.cast();
            let boxed = unsafe { ptr.as_ref() };
            boxed.as_ref()
        }
        unsafe fn as_mut(ptr: &mut NonNull<H>) -> &mut T {
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
        H: HandleDescriptor<Target = T, Mutable = False, Sized = False>,
    {
        type From = Arc<T>;
        type Raw = Arc<T>;

        fn into_handle(val: Arc<T>) -> NonNull<Arc<T>> {
            // Double-boxing needed in order to obtain a thin pointer
            let val = ManuallyDrop::new(Box::new(val));
            val.as_ref().into()
        }
        unsafe fn as_ref(ptr: &NonNull<H>) -> &T {
            let ptr: NonNull<Arc<T>> = ptr.cast();
            let arc = unsafe { ptr.as_ref() };
            arc.as_ref()
        }
        unsafe fn as_mut(_: &mut NonNull<H>) -> &mut T {
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
        let f = Foo {
            x: 10,
            y: "hi".into(),
        };
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

        let b = Bar {
            x: 10,
            y: "hi".into(),
        };
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

        // error[E0599]: the method `clone_arc` exists for struct `Handle<FooHandle>`, but its trait bounds were not satisfied
        //let _ = unsafe { h.clone_arc() };

        let b = Bar {
            x: 10,
            y: "hello".into(),
        };
        let t: Arc<dyn Baz> = Arc::new(b);
        let h: Handle<SharedBaz> = t.into();
        let r = unsafe { h.as_ref() };
        r.squawk();
        let r = unsafe { h.as_arc() };
        r.squawk();
    }
}
