//! Definitions and functions to create and manipulate kernel expressions

use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};

use itertools::Itertools;

pub use self::column_names::{
    column_expr, column_name, joined_column_expr, joined_column_name, ColumnName,
};
pub use self::scalars::{ArrayData, Scalar, StructData};
use crate::DataType;

mod column_names;
mod scalars;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// A binary operator.
pub enum BinaryOperator {
    /// Arithmetic Plus
    Plus,
    /// Arithmetic Minus
    Minus,
    /// Arithmetic Multiply
    Multiply,
    /// Arithmetic Divide
    Divide,
    /// Comparison Less Than
    LessThan,
    /// Comparison Less Than Or Equal
    LessThanOrEqual,
    /// Comparison Greater Than
    GreaterThan,
    /// Comparison Greater Than Or Equal
    GreaterThanOrEqual,
    /// Comparison Equal
    Equal,
    /// Comparison Not Equal
    NotEqual,
    /// Distinct
    Distinct,
    /// IN
    In,
    /// NOT IN
    NotIn,
}

impl BinaryOperator {
    /// Returns `<op2>` (if any) such that `B <op2> A` is equivalent to `A <op> B`.
    pub(crate) fn commute(&self) -> Option<BinaryOperator> {
        use BinaryOperator::*;
        match self {
            GreaterThan => Some(LessThan),
            GreaterThanOrEqual => Some(LessThanOrEqual),
            LessThan => Some(GreaterThan),
            LessThanOrEqual => Some(GreaterThanOrEqual),
            Equal | NotEqual | Distinct | Plus | Multiply => Some(*self),
            In | NotIn | Minus | Divide => None, // not commutative
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VariadicOperator {
    And,
    Or,
}

impl VariadicOperator {
    pub(crate) fn invert(&self) -> VariadicOperator {
        use VariadicOperator::*;
        match self {
            And => Or,
            Or => And,
        }
    }
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
            Self::Multiply => write!(f, "*"),
            Self::Divide => write!(f, "/"),
            Self::LessThan => write!(f, "<"),
            Self::LessThanOrEqual => write!(f, "<="),
            Self::GreaterThan => write!(f, ">"),
            Self::GreaterThanOrEqual => write!(f, ">="),
            Self::Equal => write!(f, "="),
            Self::NotEqual => write!(f, "!="),
            // TODO(roeap): AFAIK DISTINCT does not have a commonly used operator symbol
            // so ideally this would not be used as we use Display for rendering expressions
            // in our code we take care of this, but theirs might not ...
            Self::Distinct => write!(f, "DISTINCT"),
            Self::In => write!(f, "IN"),
            Self::NotIn => write!(f, "NOT IN"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
/// A unary operator.
pub enum UnaryOperator {
    /// Unary Not
    Not,
    /// Unary Is Null
    IsNull,
}

pub type ExpressionRef = std::sync::Arc<Expression>;

#[derive(Clone, Debug, PartialEq)]
pub struct UnaryExpression {
    /// The operator.
    pub op: UnaryOperator,
    /// The expression.
    pub expr: Box<Expression>,
}
impl UnaryExpression {
    fn new(op: UnaryOperator, expr: impl Into<Expression>) -> Self {
        let expr = Box::new(expr.into());
        Self { op, expr }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BinaryExpression {
    /// The operator.
    pub op: BinaryOperator,
    /// The left-hand side of the operation.
    pub left: Box<Expression>,
    /// The right-hand side of the operation.
    pub right: Box<Expression>,
}
impl BinaryExpression {
    fn new(op: BinaryOperator, left: impl Into<Expression>, right: impl Into<Expression>) -> Self {
        let left = Box::new(left.into());
        let right = Box::new(right.into());
        Self { op, left, right }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VariadicExpression {
    /// The operator.
    pub op: VariadicOperator,
    /// The expressions.
    pub exprs: Vec<Expression>,
}
impl VariadicExpression {
    fn new(op: VariadicOperator, exprs: Vec<Expression>) -> Self {
        Self { op, exprs }
    }
}

/// A SQL expression.
///
/// These expressions do not track or validate data types, other than the type
/// of literals. It is up to the expression evaluator to validate the
/// expression against a schema and add appropriate casts as required.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// A literal value.
    Literal(Scalar),
    /// A column reference by name.
    Column(ColumnName),
    /// A struct computed from a Vec of expressions
    Struct(Vec<Expression>),
    /// A unary operation.
    Unary(UnaryExpression),
    /// A binary operation.
    Binary(BinaryExpression),
    /// A variadic operation.
    Variadic(VariadicExpression),
    // TODO: support more expressions, such as IS IN, LIKE, etc.
}

impl<T: Into<Scalar>> From<T> for Expression {
    fn from(value: T) -> Self {
        Self::literal(value)
    }
}

impl From<ColumnName> for Expression {
    fn from(value: ColumnName) -> Self {
        Self::Column(value)
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(l) => write!(f, "{l}"),
            Self::Column(name) => write!(f, "Column({name})"),
            Self::Struct(exprs) => write!(
                f,
                "Struct({})",
                &exprs.iter().map(|e| format!("{e}")).join(", ")
            ),
            Self::Binary(BinaryExpression {
                op: BinaryOperator::Distinct,
                left,
                right,
            }) => write!(f, "DISTINCT({left}, {right})"),
            Self::Binary(BinaryExpression { op, left, right }) => write!(f, "{left} {op} {right}"),
            Self::Unary(UnaryExpression { op, expr }) => match op {
                UnaryOperator::Not => write!(f, "NOT {expr}"),
                UnaryOperator::IsNull => write!(f, "{expr} IS NULL"),
            },
            Self::Variadic(VariadicExpression { op, exprs }) => {
                let exprs = &exprs.iter().map(|e| format!("{e}")).join(", ");
                let op = match op {
                    VariadicOperator::And => "AND",
                    VariadicOperator::Or => "OR",
                };
                write!(f, "{op}({exprs})")
            }
        }
    }
}

impl Expression {
    /// Returns a set of columns referenced by this expression.
    pub fn references(&self) -> HashSet<&ColumnName> {
        let mut set = HashSet::new();

        for expr in self.walk() {
            if let Self::Column(name) = expr {
                set.insert(name);
            }
        }

        set
    }

    /// Create a new column name expression from input satisfying `FromIterator for ColumnName`.
    pub fn column<A>(field_names: impl IntoIterator<Item = A>) -> Expression
    where
        ColumnName: FromIterator<A>,
    {
        ColumnName::new(field_names).into()
    }

    /// Create a new expression for a literal value
    pub fn literal(value: impl Into<Scalar>) -> Self {
        Self::Literal(value.into())
    }

    pub fn null_literal(data_type: DataType) -> Self {
        Self::Literal(Scalar::Null(data_type))
    }

    /// Create a new struct expression
    pub fn struct_from(exprs: impl IntoIterator<Item = Self>) -> Self {
        Self::Struct(exprs.into_iter().collect())
    }

    /// Creates a new unary expression OP expr
    pub fn unary(op: UnaryOperator, expr: impl Into<Expression>) -> Self {
        Self::Unary(UnaryExpression {
            op,
            expr: Box::new(expr.into()),
        })
    }

    /// Creates a new binary expression lhs OP rhs
    pub fn binary(
        op: BinaryOperator,
        lhs: impl Into<Expression>,
        rhs: impl Into<Expression>,
    ) -> Self {
        Self::Binary(BinaryExpression {
            op,
            left: Box::new(lhs.into()),
            right: Box::new(rhs.into()),
        })
    }

    /// Creates a new variadic expression OP(exprs...)
    pub fn variadic(op: VariadicOperator, exprs: impl IntoIterator<Item = Self>) -> Self {
        let exprs = exprs.into_iter().collect::<Vec<_>>();
        Self::Variadic(VariadicExpression { op, exprs })
    }

    /// Creates a new expression AND(exprs...)
    pub fn and_from(exprs: impl IntoIterator<Item = Self>) -> Self {
        Self::variadic(VariadicOperator::And, exprs)
    }

    /// Creates a new expression OR(exprs...)
    pub fn or_from(exprs: impl IntoIterator<Item = Self>) -> Self {
        Self::variadic(VariadicOperator::Or, exprs)
    }

    /// Create a new expression `self IS NULL`
    pub fn is_null(self) -> Self {
        Self::unary(UnaryOperator::IsNull, self)
    }

    /// Create a new expression `self IS NOT NULL`
    pub fn is_not_null(self) -> Self {
        !Self::is_null(self)
    }

    /// Create a new expression `self == other`
    pub fn eq(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::Equal, self, other)
    }

    /// Create a new expression `self != other`
    pub fn ne(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::NotEqual, self, other)
    }

    /// Create a new expression `self <= other`
    pub fn le(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::LessThanOrEqual, self, other)
    }

    /// Create a new expression `self < other`
    pub fn lt(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::LessThan, self, other)
    }

    /// Create a new expression `self >= other`
    pub fn ge(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::GreaterThanOrEqual, self, other)
    }

    /// Create a new expression `self > other`
    pub fn gt(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::GreaterThan, self, other)
    }

    /// Create a new expression `self >= other`
    pub fn gt_eq(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::GreaterThanOrEqual, self, other)
    }

    /// Create a new expression `self <= other`
    pub fn lt_eq(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::LessThanOrEqual, self, other)
    }

    /// Create a new expression `self AND other`
    pub fn and(self, other: impl Into<Self>) -> Self {
        Self::and_from([self, other.into()])
    }

    /// Create a new expression `self OR other`
    pub fn or(self, other: impl Into<Self>) -> Self {
        Self::or_from([self, other.into()])
    }

    /// Create a new expression `DISTINCT(self, other)`
    pub fn distinct(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::Distinct, self, other)
    }

    fn walk(&self) -> impl Iterator<Item = &Self> + '_ {
        use Expression::*;
        let mut stack = vec![self];
        std::iter::from_fn(move || {
            let expr = stack.pop()?;
            match expr {
                Literal(_) => {}
                Column { .. } => {}
                Struct(exprs) => stack.extend(exprs),
                Unary(UnaryExpression { expr, .. }) => stack.push(expr),
                Binary(BinaryExpression { left, right, .. }) => {
                    stack.push(left);
                    stack.push(right);
                }
                Variadic(VariadicExpression { exprs, .. }) => stack.extend(exprs),
            }
            Some(expr)
        })
    }
}

/// Generic framework for recursive bottom-up expression transforms. Transformations return
/// `Option<Cow>` with the following semantics:
///
/// * `Some(Cow::Owned)` -- The input was transformed and the parent should be updated with it.
/// * `Some(Cow::Borrowed)` -- The input was not transformed.
/// * `None` -- The input was filtered out and the parent should be updated to not reference it.
///
/// The transform can start from the generic [`Self::transform`], or directly from a specific
/// expression variant (e.g. [`Self::transform_binary`] to start with [`BinaryExpression`]).
///
/// The provided `transform_xxx` methods all default to no-op (returning their input as
/// `Some(Cow::Borrowed)`), and implementations should selectively override specific `transform_xxx`
/// methods as needed for the task at hand.
///
/// The provided `recurse_into_xxx` methods encapsulate the boilerplate work of recursing into the
/// children of each expression variant. Implementations can call these as needed but will generally
/// not need to override them.
pub trait ExpressionTransform {
    /// Called for each literal encountered during the expression traversal.
    fn transform_literal<'a>(&mut self, value: &'a Scalar) -> Option<Cow<'a, Scalar>> {
        Some(Cow::Borrowed(value))
    }

    /// Called for each column reference encountered during the expression traversal.
    fn transform_column<'a>(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
        Some(Cow::Borrowed(name))
    }

    /// Called for each [`StructExpression`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_struct`] if they wish to recursively transform child expressions.
    fn transform_struct<'a>(
        &mut self,
        fields: &'a Vec<Expression>,
    ) -> Option<Cow<'a, Vec<Expression>>> {
        self.recurse_into_struct(fields)
    }

    /// Called for each [`UnaryExpression`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_unary`] if they wish to recursively transform the child.
    fn transform_unary<'a>(
        &mut self,
        expr: &'a UnaryExpression,
    ) -> Option<Cow<'a, UnaryExpression>> {
        self.recurse_into_unary(expr)
    }

    /// Called for each [`BinaryExpression`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_binary`] if they wish to recursively transform the children.
    fn transform_binary<'a>(
        &mut self,
        expr: &'a BinaryExpression,
    ) -> Option<Cow<'a, BinaryExpression>> {
        self.recurse_into_binary(expr)
    }

    /// Called for each [`VariadicExpression`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_variadic`] if they wish to recursively transform the children.
    fn transform_variadic<'a>(
        &mut self,
        expr: &'a VariadicExpression,
    ) -> Option<Cow<'a, VariadicExpression>> {
        self.recurse_into_variadic(expr)
    }

    /// General entry point for transforming an expression. This method will dispatch to the
    /// specific transform for each expression variant. Also invoked internally in order to recurse
    /// on the child(ren) of non-leaf variants.
    fn transform<'a>(&mut self, expr: &'a Expression) -> Option<Cow<'a, Expression>> {
        use Cow::*;
        let expr = match expr {
            Expression::Literal(s) => match self.transform_literal(s)? {
                Borrowed(_) => Borrowed(expr),
                Owned(s) => Owned(Expression::Literal(s)),
            },
            Expression::Column(c) => match self.transform_column(c)? {
                Borrowed(_) => Borrowed(expr),
                Owned(c) => Owned(Expression::Column(c)),
            },
            Expression::Struct(s) => match self.transform_struct(s)? {
                Borrowed(_) => Borrowed(expr),
                Owned(s) => Owned(Expression::Struct(s)),
            },
            Expression::Unary(u) => match self.transform_unary(u)? {
                Borrowed(_) => Borrowed(expr),
                Owned(u) => Owned(Expression::Unary(u)),
            },
            Expression::Binary(b) => match self.transform_binary(b)? {
                Borrowed(_) => Borrowed(expr),
                Owned(b) => Owned(Expression::Binary(b)),
            },
            Expression::Variadic(v) => match self.transform_variadic(v)? {
                Borrowed(_) => Borrowed(expr),
                Owned(v) => Owned(Expression::Variadic(v)),
            },
        };
        Some(expr)
    }

    /// Recursively transforms a struct's child expressions. Returns `None` if all children were
    /// removed, `Some(Cow::Owned)` if at least one child was changed or removed, and
    /// `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_struct<'a>(
        &mut self,
        fields: &'a Vec<Expression>,
    ) -> Option<Cow<'a, Vec<Expression>>> {
        let mut num_borrowed = 0;
        let new_fields: Vec<_> = fields
            .iter()
            .filter_map(|f| self.transform(f))
            .inspect(|f| {
                if matches!(f, Cow::Borrowed(_)) {
                    num_borrowed += 1;
                }
            })
            .collect();

        if new_fields.is_empty() {
            None // all children filtered out
        } else if num_borrowed < fields.len() {
            // At least one child was changed or filtered out, so make a new struct
            Some(Cow::Owned(
                new_fields.into_iter().map(|f| f.into_owned()).collect(),
            ))
        } else {
            Some(Cow::Borrowed(fields))
        }
    }

    /// Recursively transforms a unary expression's child. Returns `None` if the child was removed,
    /// `Some(Cow::Owned)` if the child was changed, and `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_unary<'a>(
        &mut self,
        u: &'a UnaryExpression,
    ) -> Option<Cow<'a, UnaryExpression>> {
        use Cow::*;
        let u = match self.transform(&u.expr)? {
            Borrowed(_) => Borrowed(u),
            Owned(expr) => Owned(UnaryExpression::new(u.op, expr)),
        };
        Some(u)
    }

    /// Recursively transforms a binary expression's children. Returns `None` if both children were
    /// removed, `Some(Cow::Owned)` if at least one child was changed or removed, and
    /// `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_binary<'a>(
        &mut self,
        b: &'a BinaryExpression,
    ) -> Option<Cow<'a, BinaryExpression>> {
        use Cow::*;
        let left = self.transform(&b.left)?;
        let right = self.transform(&b.right)?;
        let b = match (&left, &right) {
            (Borrowed(_), Borrowed(_)) => Borrowed(b),
            _ => Owned(BinaryExpression::new(
                b.op,
                left.into_owned(),
                right.into_owned(),
            )),
        };
        Some(b)
    }

    /// Recursively transforms a variadic expression's children. Returns `None` if all children were
    /// removed, `Some(Cow::Owned)` if at least one child was changed or removed, and
    /// `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_variadic<'a>(
        &mut self,
        v: &'a VariadicExpression,
    ) -> Option<Cow<'a, VariadicExpression>> {
        use Cow::*;
        let v = match self.recurse_into_struct(&v.exprs)? {
            Borrowed(_) => Borrowed(v),
            Owned(exprs) => Owned(VariadicExpression::new(v.op, exprs)),
        };
        Some(v)
    }
}

impl std::ops::Not for Expression {
    type Output = Self;

    fn not(self) -> Self {
        Self::unary(UnaryOperator::Not, self)
    }
}

impl<R: Into<Expression>> std::ops::Add<R> for Expression {
    type Output = Self;

    fn add(self, rhs: R) -> Self::Output {
        Self::binary(BinaryOperator::Plus, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Sub<R> for Expression {
    type Output = Self;

    fn sub(self, rhs: R) -> Self {
        Self::binary(BinaryOperator::Minus, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Mul<R> for Expression {
    type Output = Self;

    fn mul(self, rhs: R) -> Self {
        Self::binary(BinaryOperator::Multiply, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Div<R> for Expression {
    type Output = Self;

    fn div(self, rhs: R) -> Self {
        Self::binary(BinaryOperator::Divide, self, rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::{column_expr, Expression as Expr};

    #[test]
    fn test_expression_format() {
        let col_ref = column_expr!("x");
        let cases = [
            (col_ref.clone(), "Column(x)"),
            (col_ref.clone().eq(2), "Column(x) = 2"),
            ((col_ref.clone() - 4).lt(10), "Column(x) - 4 < 10"),
            ((col_ref.clone() + 4) / 10 * 42, "Column(x) + 4 / 10 * 42"),
            (
                col_ref.clone().gt_eq(2).and(col_ref.clone().lt_eq(10)),
                "AND(Column(x) >= 2, Column(x) <= 10)",
            ),
            (
                Expr::and_from([
                    col_ref.clone().gt_eq(2),
                    col_ref.clone().lt_eq(10),
                    col_ref.clone().lt_eq(100),
                ]),
                "AND(Column(x) >= 2, Column(x) <= 10, Column(x) <= 100)",
            ),
            (
                col_ref.clone().gt(2).or(col_ref.clone().lt(10)),
                "OR(Column(x) > 2, Column(x) < 10)",
            ),
            (col_ref.eq("foo"), "Column(x) = 'foo'"),
        ];

        for (expr, expected) in cases {
            let result = format!("{}", expr);
            assert_eq!(result, expected);
        }
    }
}
