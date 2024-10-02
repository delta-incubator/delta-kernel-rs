//! Various utility functions/macros used throughout the kernel

/// convenient way to return an error if a condition isn't true
macro_rules! require {
    ( $cond:expr, $err:expr ) => {
        if !($cond) {
            return Err($err);
        }
    };
}
pub(crate) trait MapAndThen: Iterator {
    fn map_and_then<F, T, U, E>(self, mut f: F) -> impl Iterator<Item = Result<U, E>>
    where
        Self: Iterator<Item = Result<T, E>> + Sized,
        F: FnMut(T) -> Result<U, E>,
    {
        // NOTE: FnMut is-a FnOnce, but using it that way consumes `f`. Fortunately,
        // &mut FnMut is _also_ FnOnce, and using it that way does _not_ consume `f`.
        self.map(move |result| result.and_then(&mut f))
    }
}

impl<T: Iterator> MapAndThen for T {}

pub(crate) use require;
