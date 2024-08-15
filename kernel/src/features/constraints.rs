/// A constraint in a check constraint
#[derive(Eq, PartialEq, Debug, Default, Clone)]
pub struct Constraint {
    /// The full path to the field.
    pub name: String,
    /// The SQL string that must always evaluate to true.
    pub expr: String,
}

impl Constraint {
    /// Create a new invariant
    pub fn new(field_name: impl ToString, invariant_sql: impl ToString) -> Self {
        Self {
            name: field_name.to_string(),
            expr: invariant_sql.to_string(),
        }
    }
}
