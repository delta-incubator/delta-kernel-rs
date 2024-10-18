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
    /// Creates a simple (non-nested) column name.
    // TODO: A simple path name containing periods will be treated as nested right now.
    pub fn simple(name: impl Into<String>) -> Self {
        Self { path: name.into() }
    }

    /// Parses a nested column name by splitting it at periods. Caller affirms that field names in
    /// the input string do not contain any periods.
    pub fn split(path: impl AsRef<str>) -> Self {
        // TODO: Actually parse this!
        Self::simple(path.as_ref())
    }

    /// Joins two column names
    pub fn join(left: impl Into<ColumnName>, right: impl Into<ColumnName>) -> ColumnName {
        Self::simple(format!("{}.{}", left.into(), right.into()))
    }

    /// The path of field names for this column name
    pub fn path(&self) -> &String {
        &self.path
    }

    /// Consumes this column name and returns the path of field names as a vector.
    pub fn into_inner(self) -> String {
        self.path
    }
}

impl<T: Into<String>> From<T> for ColumnName {
    fn from(value: T) -> Self {
        Self::simple(value)
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_column_name() {
        assert_eq!(
            ColumnName::join(ColumnName::split("a.b.c"), "d"),
            ColumnName::split("a.b.c.d")
        );
    }
}
