use std::borrow::Borrow;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
/// A (possibly nested) column name.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct ColumnName {
    path: Vec<String>,
}

impl ColumnName {
    /// Creates a new column name from input satisfying `FromIterator for ColumnName`. The provided
    /// field names are concatenated into a single path.
    pub fn new<A>(iter: impl IntoIterator<Item = A>) -> Self
    where
        Self: FromIterator<A>,
    {
        iter.into_iter().collect()
    }

    /// Naively splits a string at dots to create a column name.
    ///
    /// This method is _NOT_ recommended for production use, as it does not attempt to interpret
    /// special characters in field names. For example, many systems would interpret the field name
    /// `"a.b".c` as equivalent to `ColumnName::new(["\"a.b\"", "c"])` (two fields), but this method
    /// would return the equivalent of `ColumnName::new(["\"a", "b\"", "c"])` (three fields).
    pub fn from_naive_str_split(name: impl AsRef<str>) -> Self {
        Self::new(name.as_ref().split('.'))
    }

    /// Naively converts a column name into a string by joining its fields with dots. This is a
    /// potentially lossy conversion if any field name contains a dot or other special characters.
    pub fn to_string_lossy(&self) -> String {
        self.path.join(".")
    }

    /// Joins this column with another, concatenating their fields into a single nested column path.
    ///
    /// NOTE: This is a convenience method that copies two arguments without consuming them. If more
    /// arguments are needed, or if performance is a concern, it is recommended to use
    /// [`FromIterator for ColumnName`](#impl-FromIterator<ColumnName>-for-ColumnName) instead:
    ///
    /// ```
    /// # use delta_kernel::expressions::ColumnName;
    /// let x = ColumnName::new(["a", "b"]);
    /// let y = ColumnName::new(["c", "d"]);
    /// let joined: ColumnName = [x, y].into_iter().collect();
    /// assert_eq!(joined, ColumnName::new(["a", "b", "c", "d"]));
    /// ```
    pub fn join(&self, right: &ColumnName) -> ColumnName {
        [self.clone(), right.clone()].into_iter().collect()
    }

    /// The path of field names for this column name
    pub fn path(&self) -> &[String] {
        &self.path
    }

    /// Consumes this column name and returns the path of field names.
    pub fn into_inner(self) -> Vec<String> {
        self.path
    }
}

/// Creates a new column name from a path of field names. Each field name is taken as-is, and may
/// contain arbitrary characters (including periods, spaces, etc.).
impl<A: Into<String>> FromIterator<A> for ColumnName {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let path = iter.into_iter().map(|s| s.into()).collect();
        Self { path }
    }
}

/// Creates a new column name by joining multiple column names together.
impl FromIterator<ColumnName> for ColumnName {
    fn from_iter<T: IntoIterator<Item = ColumnName>>(iter: T) -> Self {
        let path = iter.into_iter().flat_map(|c| c.into_iter()).collect();
        Self { path }
    }
}

impl IntoIterator for ColumnName {
    type Item = String;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.path.into_iter()
    }
}

impl Display for ColumnName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // TODO: Handle non-simple field names better?
        f.write_str(&self.to_string_lossy())
    }
}

impl Deref for ColumnName {
    type Target = [String];

    fn deref(&self) -> &[String] {
        &self.path
    }
}

// Allows searching collections of `ColumnName` without an owned key value
impl Borrow<[String]> for ColumnName {
    fn borrow(&self) -> &[String] {
        self
    }
}

// Allows searching collections of `&ColumnName` without an owned key value. Needed because there is
// apparently no blanket `impl<U, T> Borrow<U> for &T where T: Borrow<U>`, even tho `Eq` [1] and
// `Hash` [2] both have blanket impl for treating `&T` like `T`.
//
// [1] https://doc.rust-lang.org/std/cmp/trait.Eq.html#impl-Eq-for-%26A
// [2] https://doc.rust-lang.org/std/hash/trait.Hash.html#impl-Hash-for-%26T
impl Borrow<[String]> for &ColumnName {
    fn borrow(&self) -> &[String] {
        self
    }
}

impl Hash for ColumnName {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        (**self).hash(hasher)
    }
}

/// A wrapper for `ColumnName` that pretty-prints the column name with fields delimited by periods
/// and enclosed in square brackets, e.g. `column_name!("a.b.c")` would print as `"[a].[b].[c]"`.
// TODO: Marked as test-only for now, as a demonstration. Should this be Display for ColumnName, so
// `ColumnName::new(["a", "b.c", "d"])` would print as `"a.[b.c].d"` instead of merely `"a.b.c.d"`?
#[cfg(test)]
struct FancyColumnName(ColumnName);

#[cfg(test)]
impl Display for FancyColumnName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut delim = None;
        for s in self.0.iter() {
            use std::fmt::Write as _;

            // Only emit the period delimiter after the first iteration
            if let Some(d) = delim {
                f.write_char(d)?;
            } else {
                delim = Some('.');
            }

            let digit_char = |c: char| c.is_ascii_digit();
            let illegal_char = |c: char| !c.is_ascii_alphanumeric() && c != '_';
            if s.is_empty() || s.starts_with(digit_char) || s.contains(illegal_char) {
                // Special situation detected. For safety, surround the field name with brackets
                // (with proper escaping if the field name itself contains brackets).
                f.write_char('[')?;
                for c in s.chars() {
                    match c {
                        '[' => f.write_str("[[")?,
                        ']' => f.write_str("]]")?,
                        _ => f.write_char(c)?,
                    }
                }
                f.write_char(']')?;
            } else {
                // The fild name contains no special characters, so emit it as-is.
                f.write_str(s)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
use crate::error::{DeltaResult, Error};

/// Parses a column name from a string. Field names are separated by dots. Field names enclosed in
/// square brackets may contain arbitrary characters, including periods and spaces. To include a
/// literal square bracket in a field name, escape it by doubling it, e.g. `"[foo]]bar]"` would
/// parse as `foo]bar`.
// TODO: Marked as test-only for now, as a demonstration. Should we officially support it?
#[cfg(test)]
impl std::str::FromStr for ColumnName {
    type Err = Error;

    fn from_str(s: &str) -> DeltaResult<Self> {
        column_name_from_str(s)
    }
}

/// See `impl FromStr for ColumnName`.
#[cfg(test)]
fn column_name_from_str(s: &str) -> DeltaResult<ColumnName> {
    type Chars<'a> = std::iter::Peekable<std::str::Chars<'a>>;

    fn parse_simple(chars: &mut Chars<'_>) -> DeltaResult<(String, bool)> {
        let mut name = String::new();
        let mut allow_digits = false; // first character cannot be a digit
        for c in chars {
            match c {
                '.' => return Ok((name, false)),
                '_' | 'a'..='z' | 'A'..='Z' => name.push(c),
                '0'..='9' => {
                    if allow_digits {
                        name.push(c);
                    } else {
                        return Err(Error::generic(format!(
                            "Unescaped field name cannot start with a digit '{c}'"
                        )));
                    }
                }
                _ => {
                    return Err(Error::generic(format!(
                        "Invalid character '{c}' in unescaped field name"
                    )))
                }
            };
            allow_digits = true;
        }
        Ok((name, true)) // EOF
    }

    fn parse_escaped(chars: &mut Chars<'_>) -> DeltaResult<(String, bool)> {
        let mut name = String::new();
        while let Some(c) = chars.next() {
            match c {
                '[' => match chars.next() {
                    Some('[') => name.push('['), // escaped delimiter (keep going)
                    _ => return Err(Error::generic(
                        "Unescaped '[' delimiter inside escaped field name")),
                },
                ']' => match chars.next() {
                    Some(']') => name.push(']'), // escaped delimiter (keep going)
                    Some('.') => return Ok((name, false)),
                    Some(c) => {
                        return Err(Error::generic(format!(
                            "Invalid character '{c}' after field name"
                        )))
                    }
                    None => return Ok((name, true)), // EOF
                },
                _ => name.push(c),
            }
        }
        Err(Error::generic("Escaped field name lacks a closing ']' delimiter"))
    }

    // Ambiguous case: The empty string `""`could reasonably parse as either `ColumnName::new([""])`
    // or `ColumnName::new([])`. However, `ColumnName::new([""]).to_string()` is `"[]"` and
    // `ColumnName::new([]).to_string()` is `""`, so we choose the latter because it produces a
    // lossless round trip from `ColumnName` to  `String` and back.
    let mut chars = s.chars().peekable();
    if chars.peek().is_none() {
        return Ok(ColumnName::new(&[] as &[String]));
    }

    let mut path = vec![];
    loop {
        let (field_name, done) = if chars.next_if_eq(&'[').is_none() {
            parse_simple(&mut chars)?
        } else {
            parse_escaped(&mut chars)?
        };
        path.push(field_name);
        if done {
            return Ok(ColumnName::new(path));
        }
    }
}

/// Creates a nested column name whose field names are all simple column names (containing only
/// alphanumeric characters and underscores), delimited by dots. This macro is provided as a
/// convenience for the common case where the caller knows the column name contains only simple
/// field names and that splitting by periods is safe:
///
/// ```
/// # use delta_kernel::expressions::{column_name, ColumnName};
/// assert_eq!(column_name!("a.b.c"), ColumnName::new(["a", "b", "c"]));
/// ```
///
/// To avoid accidental misuse, the argument must be a string literal, so the compiler can validate
/// the safety conditions. Thus, the following uses would fail to compile:
///
/// ```fail_compile
/// # use delta_kernel::expressions::column_name;
/// let s = "a.b";
/// let name = column_name!(s); // not a string literal
/// ```
///
/// ```fail_compile
/// # use delta_kernel::expressions::simple_column_name;
/// let name = simple_column_name!("a b"); // non-alphanumeric character
/// ```
// NOTE: Macros are only public if exported, which defines them at the root of the crate. But we
// don't want it there. So, we export a hidden macro and pub use it here where we actually want it.
#[macro_export]
#[doc(hidden)]
macro_rules! __column_name {
    ( $($name:tt)* ) => {
        $crate::expressions::ColumnName::new($crate::delta_kernel_derive::parse_column_name!($($name)*))
    };
}
#[doc(inline)]
pub use __column_name as column_name;

/// Joins two column names together, when one or both inputs might be literal strings representing
/// simple (non-nested) column names. For example:
///
/// ```
/// # use delta_kernel::expressions::{column_name, joined_column_name};
/// assert_eq!(joined_column_name!("a.b", "c"), column_name!("a.b").join(&column_name!("c")))
/// ```
///
/// To avoid accidental misuse, at least one argument must be a string literal. Thus, the following
/// invocation would fail to compile:
///
/// ```fail_compile
/// # use delta_kernel::expressions::joined_column_name;
/// let s = "s";
/// let name = joined_column_name!(s, s);
/// ```
#[macro_export]
#[doc(hidden)]
macro_rules! __joined_column_name {
    ( $left:literal, $right:literal ) => {
        $crate::__column_name!($left).join(&$crate::__column_name!($right))
    };
    ( $left:literal, $right:expr ) => {
        $crate::__column_name!($left).join(&$right)
    };
    ( $left:expr, $right:literal) => {
        $left.join(&$crate::__column_name!($right))
    };
    ( $($other:tt)* ) => {
        compile_error!("joined_column_name!() requires at least one string literal input")
    };
}
#[doc(inline)]
pub use __joined_column_name as joined_column_name;

#[macro_export]
#[doc(hidden)]
macro_rules! __column_expr {
    ( $($name:tt)* ) => {
        $crate::expressions::Expression::from($crate::__column_name!($($name)*))
    };
}
#[doc(inline)]
pub use __column_expr as column_expr;

#[macro_export]
#[doc(hidden)]
macro_rules! __joined_column_expr {
    ( $($name:tt)* ) => {
        $crate::expressions::Expression::from($crate::__joined_column_name!($($name)*))
    };
}
#[doc(inline)]
pub use __joined_column_expr as joined_column_expr;

#[cfg(test)]
mod test {
    use super::*;
    use delta_kernel_derive::parse_column_name;

    #[test]
    fn test_parse_column_name_macros() {
        assert_eq!(parse_column_name!("a"), ["a"]);

        assert_eq!(parse_column_name!("a"), ["a"]);
        assert_eq!(parse_column_name!("a.b"), ["a", "b"]);
        assert_eq!(parse_column_name!("a.b.c"), ["a", "b", "c"]);
    }

    #[test]
    fn test_column_name_macros() {
        let simple = column_name!("x");
        let nested = column_name!("x.y");

        assert_eq!(column_name!("a"), ColumnName::new(["a"]));
        assert_eq!(column_name!("a.b"), ColumnName::new(["a", "b"]));
        assert_eq!(column_name!("a.b.c"), ColumnName::new(["a", "b", "c"]));

        assert_eq!(joined_column_name!("a", "b"), ColumnName::new(["a", "b"]));
        assert_eq!(joined_column_name!("a", "b"), ColumnName::new(["a", "b"]));

        assert_eq!(
            joined_column_name!(simple, "b"),
            ColumnName::new(["x", "b"])
        );
        assert_eq!(
            joined_column_name!(nested, "b"),
            ColumnName::new(["x", "y", "b"])
        );

        assert_eq!(
            joined_column_name!("a", &simple),
            ColumnName::new(["a", "x"])
        );
        assert_eq!(
            joined_column_name!("a", &nested),
            ColumnName::new(["a", "x", "y"])
        );
    }

    #[test]
    fn test_column_name_methods() {
        let simple = column_name!("x");
        let nested = column_name!("x.y");

        // path()
        assert_eq!(simple.path(), ["x"]);
        assert_eq!(nested.path(), ["x", "y"]);

        // into_inner()
        assert_eq!(simple.clone().into_inner(), ["x"]);
        assert_eq!(nested.clone().into_inner(), ["x", "y"]);

        // impl Deref
        let name: &[String] = &nested;
        assert_eq!(name, &["x", "y"]);

        // impl<A: Into<String>> FromIterator<A>
        let name: ColumnName = ["x", "y"].into_iter().collect();
        assert_eq!(name, nested);

        // impl FromIterator<ColumnName>
        let name: ColumnName = [&nested, &simple].into_iter().cloned().collect();
        assert_eq!(name, column_name!("x.y.x"));

        // ColumnName::new
        let name = ColumnName::new([nested, simple]);
        assert_eq!(name, column_name!("x.y.x"));

        let name = ColumnName::new(["x", "y"]);
        assert_eq!(name, column_name!("x.y"));

        // ColumnName::into_iter()
        let name = column_name!("x.y.z");
        let name = ColumnName::new(name);
        assert_eq!(name, column_name!("x.y.z"));
    }

    #[test]
    fn test_column_name_from_str() {
        let cases = [
            ("", Some(ColumnName::new(&[] as &[String]))), // the ambiguous case!
            (".", Some(ColumnName::new(["", ""]))),
            (" ", None),
            ("0", None),
            (".a", Some(ColumnName::new(["", "a"]))),
            ("a.", Some(ColumnName::new(["a", ""]))),
            ("a..b", Some(ColumnName::new(["a", "", "b"]))),
            ("[a", None),
            ("a]", None),
            ("a[b]", None),
            ("[a]b", None),
            ("[a][b]", None),
            ("a", Some(ColumnName::new(["a"]))),
            ("a0", Some(ColumnName::new(["a0"]))),
            ("[a]", Some(ColumnName::new(["a"]))),
            ("[ ]", Some(ColumnName::new([" "]))),
            ("[0]", Some(ColumnName::new(["0"]))),
            ("[.]", Some(ColumnName::new(["."]))),
            ("[.].[.]", Some(ColumnName::new([".", "."]))),
            ("[ ].[ ]", Some(ColumnName::new([" ", " "]))),
            ("a.b", Some(ColumnName::new(["a", "b"]))),
            ("a.[b]", Some(ColumnName::new(["a", "b"]))),
            ("[a].b", Some(ColumnName::new(["a", "b"]))),
            ("[a].[b]", Some(ColumnName::new(["a", "b"]))),
            ("[a].[b].[c]", Some(ColumnName::new(["a", "b", "c"]))),
            ("[a[].[b]]]", None),
            ("[a[[].[b]]", None),
            ("[a[].[b]]]", None),
            ("[a[[].[b]]", None),
            ("[a[[].[b]]]", Some(ColumnName::new(["a[", "b]"]))),
            ("[a.[b]].c]", None),
            ("[a.[[b].c]", None),
            ("[a.[[b]].c]", Some(ColumnName::new(["a.[b].c"]))),
            ("a[.b]]", None),
        ];
        for (input, expected_output) in cases {
            let output: DeltaResult<ColumnName> = input.parse();
            match (&output, &expected_output) {
                (Ok(output), Some(expected_output)) => assert_eq!(output, expected_output),
                (Err(_), None) => {}
                _ => panic!("Expected {input} to parse as {expected_output:?}, got {output:?}"),
            }
        }
    }

    #[test]
    fn test_to_string_lossy() {
        let cases = [
            ("", ColumnName::new(&[] as &[String])), // ambiguous case!
            ("a.b.c", ColumnName::new(["a", "b", "c"])),
            ("a.b_c.d", ColumnName::new(["a", "b_c", "d"])),
            ("a.b c.d", ColumnName::new(["a", "b c", "d"])),
            ("a.b.c.d", ColumnName::new(["a", "b.c", "d"])),
        ];
        for (expected_output, input) in cases {
            assert_eq!(input.to_string_lossy(), expected_output);
        }
    }

    #[test]
    fn test_column_name_to_string() {
        let cases = [
            ("", ColumnName::new(&[] as &[String])), // the ambiguous case!
            ("[].[]", ColumnName::new(["", ""])),
            ("[].a", ColumnName::new(["", "a"])),
            ("a.[]", ColumnName::new(["a", ""])),
            ("a.[].b", ColumnName::new(["a", "", "b"])),
            ("a", ColumnName::new(["a"])),
            ("a0", ColumnName::new(["a0"])),
            ("[a ]", ColumnName::new(["a "])),
            ("[ ]", ColumnName::new([" "])),
            ("[0]", ColumnName::new(["0"])),
            ("[.]", ColumnName::new(["."])),
            ("[.].[.]", ColumnName::new([".", "."])),
            ("[ ].[ ]", ColumnName::new([" ", " "])),
            ("a.b", ColumnName::new(["a", "b"])),
            ("a.b.c", ColumnName::new(["a", "b", "c"])),
            ("a.[b.c].d", ColumnName::new(["a", "b.c", "d"])),
            ("[a[[].[b]]]", ColumnName::new(["a[", "b]"])),
        ];
        for (expected_output, input) in cases {
            let output = FancyColumnName(input.clone()).to_string();
            assert_eq!(output, expected_output);

            let parsed: ColumnName = output.parse().expect(&output);
            assert_eq!(parsed, input);
        }

        // Ensure unnecessary escaping is tolerated
        let cases = [
            ("[a]", "a", ColumnName::new(["a"])),
            ("[a0]", "a0", ColumnName::new(["a0"])),
            ("[a].[b]", "a.b", ColumnName::new(["a", "b"])),
        ];
        for (input, expected_output, expected_parsed) in cases {
            let parsed: ColumnName = input.parse().unwrap();
            assert_eq!(parsed, expected_parsed);
            assert_eq!(parsed.to_string(), expected_output);
        }
    }
}
