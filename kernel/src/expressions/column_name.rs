use std::borrow::Borrow;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
/// A (possibly nested) column name.
// TODO: Track name as a path rather than a single string
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct ColumnName {
    path: String,
}

impl ColumnName {
    /// Constructs a new column name from an iterator of field names. The field names are joined
    /// together to make a single path.
    pub fn new(path: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let path: Vec<_> = path.into_iter().map(Into::into).collect();
        let path = path.join(".");
        Self { path }
    }

    /// Joins two column names, concatenating their fields into a single nested column path.
    pub fn join(left: ColumnName, right: ColumnName) -> ColumnName {
        [left, right].into_iter().collect()
    }

    /// The path of field names for this column name
    pub fn path(&self) -> &String {
        &self.path
    }

    /// Consumes this column name and returns the path of field names.
    pub fn into_inner(self) -> String {
        self.path
    }
}

/// Creates a new column name from a path of field names. Each field name is taken as-is, and may
/// contain arbitrary characters (including periods, spaces, etc.).
impl<A: Into<String>> FromIterator<A> for ColumnName {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = A>,
    {
        Self::new(iter)
    }
}

/// Creates a new column name by joining multiple column names together.
impl FromIterator<ColumnName> for ColumnName {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = ColumnName>,
    {
        Self::new(iter.into_iter().map(ColumnName::into_inner))
    }
}

impl Display for ColumnName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

impl Deref for ColumnName {
    type Target = String;

    fn deref(&self) -> &String {
        &self.path
    }
}

// Allows searching collections of `ColumnName` without an owned key value
impl Borrow<String> for ColumnName {
    fn borrow(&self) -> &String {
        self
    }
}

// Allows searching collections of `&ColumnName` without an owned key value. Needed because there is
// apparently no blanket `impl<U, T> Borrow<U> for &T where T: Borrow<U>`, even tho `Eq` [1] and
// `Hash` [2] both have blanket impl for treating `&T` like `T`.
//
// [1] https://doc.rust-lang.org/std/cmp/trait.Eq.html#impl-Eq-for-%26A
// [2] https://doc.rust-lang.org/std/hash/trait.Hash.html#impl-Hash-for-%26T
impl Borrow<String> for &ColumnName {
    fn borrow(&self) -> &String {
        self
    }
}

impl Hash for ColumnName {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        (**self).hash(hasher)
    }
}

/// Creates a simple (non-nested) column name.
///
/// To avoid accidental misuse, the argument must be a string literal and must not contain any
/// dots. Thus, the following invocations would both fail to compile:
///
/// ```fail_compile
/// # use delta_kernel::expressions::simple_column_name;
/// let s = "a";
/// let name = simple_column_name!(s); // not a string literal
/// ```
///
/// ```fail_compile
/// # use delta_kernel::expressions::simple_column_name;
/// let name = simple_column_name!("a.b"); // contains dots
/// ```
///
/// NOTE: `simple_column_name!("a.b.c")` produces a non-nested column whose name happens to contain
/// two period characters. To interpret the column name as nested with field names "a", "b" and "c",
/// use [`nested_column_name!`] instead.
// NOTE: Macros are only public if exported, which defines them at the root of the crate. But we
// don't want it there. So, we export a hidden macro and pub use it here where we actually want it.
#[macro_export]
#[doc(hidden)]
macro_rules! __simple_column_name {
    ( $($name:tt)* ) => {
        $crate::expressions::ColumnName::new(delta_kernel_derive::parse_simple_column_name!($($name)*))
    };
}
#[doc(inline)]
pub use __simple_column_name as simple_column_name;

/// Creates a nested column name whose field names are all simple column names. This macro is
/// provided as a convenience for the common case where the caller knows the column contains only
/// simple field names and that splitting by periods is safe.
///
/// To avoid accidental misuse, the argument must be a string literal. Thus, the following
/// invocation would fail to compile:
///
/// ```fail_compile
/// # use delta_kernel::expressions::nested_column_name;
/// let s = "a.b";
/// let name = nested_column_name!(s);
/// ```
///
/// NOTE: `nested_column_name!("a.b.c")` produces a path with field names "a", "b", and "c".
#[macro_export]
#[doc(hidden)]
macro_rules! __nested_column_name {
    ( $($name:tt)* ) => {
        $crate::expressions::ColumnName::new(delta_kernel_derive::parse_nested_column_name!($($name)*))
    };
}
#[doc(inline)]
pub use __nested_column_name as nested_column_name;

/// Joins two column names together, when one or both inputs might be literal strings representing
/// simple (non-nested) column names. For example, the invocation `joined_column_name!("a", "b")` is
/// equivalent to `ColumnName::join(simple_column_name!("a"), simple_column_name!("b"))`
///
/// To avoid accidental misuse, at least one argument must be a string literal. Thus, the following
/// invocation would fail to compile:
///
/// ```fail_compile
/// # use delta_kernel::expressions::joined_column_name;
/// let s = "s";
/// let name = joined_column_name!(s, s);
/// ```
///
/// NOTE: `joined_column_name!("a.b", "c")` produces a path with field names "a.b" and "c". If
/// either argument is a nested path delimited by periods, it should be split first and the result
/// passed to `joined_column_name!`, e.g. `joined_column_name!(nested_column_name!("a.b"), "c")`.
#[macro_export]
#[doc(hidden)]
macro_rules! __joined_column_name {
    ( $left:literal, $right:literal ) => {
        $crate::expressions::ColumnName::join(
            $crate::__simple_column_name!($left),
            $crate::__simple_column_name!($right),
        )
    };
    ( $left:literal, $right:expr ) => {
        $crate::expressions::ColumnName::join($crate::__simple_column_name!($left), $right)
    };
    ( $left:expr, $right:literal) => {
        $crate::expressions::ColumnName::join($left, $crate::__simple_column_name!($right))
    };
    ( $($other:tt)* ) => {
        compile_error!("joined_column_name!() requires at least one string literal input")
    };
}
#[doc(inline)]
pub use __joined_column_name as joined_column_name;

#[macro_export]
#[doc(hidden)]
macro_rules! __simple_column_expr {
    ( $($name:tt)* ) => {
        $crate::expressions::Expression::from($crate::expressions::simple_column_name!($($name)*))
    };
}
#[doc(inline)]
pub use __simple_column_expr as simple_column;

#[macro_export]
#[doc(hidden)]
macro_rules! __nested_column_expr {
    ( $($name:tt)* ) => {
        $crate::expressions::Expression::from($crate::expressions::nested_column_name!($($name)*))
    };
}
#[doc(inline)]
pub use __nested_column_expr as nested_column;

#[macro_export]
#[doc(hidden)]
macro_rules! __joined_column_expr {
    ( $($name:tt)* ) => {
        $crate::expressions::Expression::from($crate::expressions::joined_column_name!($($name)*))
    };
}
#[doc(inline)]
pub use __joined_column_expr as joined_column;

#[cfg(test)]
mod test {
    use super::*;
    use delta_kernel_derive::{parse_nested_column_name, parse_simple_column_name};

    #[test]
    fn test_parse_column_name_macros() {
        assert_eq!(parse_simple_column_name!("a"), ["a"]);

        assert_eq!(parse_nested_column_name!("a"), ["a"]);
        assert_eq!(parse_nested_column_name!("a.b"), ["a", "b"]);
        assert_eq!(parse_nested_column_name!("a.b.c"), ["a", "b", "c"]);
    }

    #[test]
    fn test_column_name_macros() {
        let simple = simple_column_name!("x");
        let nested = nested_column_name!("x.y");

        // fails to compile (argument is not a literal string, or contains illegal characters)
        //simple_column_name!("a.b");
        //simple_column_name!("a b");
        //simple_column_name!(simple);
        //simple_column_name!(simple.clone());

        assert_eq!(simple_column_name!("a"), ColumnName::new(["a"]));

        // fails to compile (argument is not a literal string, or contains illegal characters)
        //nested_column_name!("a b.c d");
        //nested_column_name!(simple);
        //nested_column_name!(simple.clone());

        assert_eq!(nested_column_name!("a"), ColumnName::new(["a"])); // Silly, but legal.
        assert_eq!(nested_column_name!("a.b"), ColumnName::new(["a", "b"]));
        assert_eq!(
            nested_column_name!("a.b.c"),
            ColumnName::new(["a", "b", "c"])
        );

        assert_eq!(joined_column_name!("a", "b"), ColumnName::new(["a", "b"]));
        assert_eq!(joined_column_name!("a", "b"), ColumnName::new(["a", "b"]));

        assert_eq!(
            joined_column_name!(simple.clone(), "b"),
            ColumnName::new(["x", "b"])
        );
        assert_eq!(
            joined_column_name!(nested.clone(), "b"),
            ColumnName::new(["x.y", "b"])
        );

        assert_eq!(
            joined_column_name!("a", simple.clone()),
            ColumnName::new(["a", "x"])
        );
        assert_eq!(
            joined_column_name!("a", nested.clone()),
            ColumnName::new(["a", "x.y"])
        );

        // fails to compile (none of the arguments is a literal, or one of the literals contains illegal characters)
        //joined_column_name!(simple, simple);
        //joined_column_name!(simple.clone(), simple.clone());
        //joined_column_name!("a.b", "c");
        //joined_column_name!("a", "b.c");
        //joined_column_name!("a.b", "c.d");
    }

    #[test]
    fn test_column_name_methods() {
        let simple = simple_column_name!("x");
        let nested = nested_column_name!("x.y");

        // path()
        assert_eq!(simple.path(), "x");
        assert_eq!(nested.path(), "x.y");

        // into_inner()
        assert_eq!(simple.clone().into_inner(), "x");
        assert_eq!(nested.clone().into_inner(), "x.y");

        // impl Deref
        let name: &str = &nested;
        assert_eq!(name, "x.y");

        // impl<A: Into<String>> FromIterator<A>
        let name: ColumnName = ["x", "y"].into_iter().collect();
        assert_eq!(name, nested);

        // impl FromIterator<ColumnName>
        let name: ColumnName = [nested, simple].into_iter().collect();
        assert_eq!(name, nested_column_name!("x.y.x"));
    }
}
