use std::sync::Arc;

use crate::{
    scan::{get_state_info, ColumnType},
    schema::{SchemaRef, StructType},
    DeltaResult, ExpressionRef,
};

use super::TableChanges;

/// The result of building a [`TableChanges`] scan over a table. This can be used to get a change
/// data feed from the table
#[allow(unused)]
pub struct TableChangesScan {
    table_changes: Arc<TableChanges>,
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    predicate: Option<ExpressionRef>,
    all_fields: Vec<ColumnType>,
    have_partition_cols: bool,
}
/// Builder to read the `TableChanges` of a table.
pub struct TableChangesScanBuilder {
    table_changes: Arc<TableChanges>,
    schema: Option<SchemaRef>,
    predicate: Option<ExpressionRef>,
}

impl TableChangesScanBuilder {
    /// Create a new [`TableChangesScanBuilder`] instance.
    pub fn new(table_changes: impl Into<Arc<TableChanges>>) -> Self {
        Self {
            table_changes: table_changes.into(),
            schema: None,
            predicate: None,
        }
    }

    /// Provide [`Schema`] for columns to select from the [`TableChanges`].
    ///
    /// A table with columns `[a, b, c]` could have a scan which reads only the first
    /// two columns by using the schema `[a, b]`.
    ///
    /// [`Schema`]: crate::schema::Schema
    /// [`TableChanges`]: crate::table_changes:TableChanges:
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Optionally provide a [`SchemaRef`] for columns to select from the [`TableChanges`]. See
    /// [`TableChangesScanBuilder::with_schema`] for details. If `schema_opt` is `None` this is a no-op.
    pub fn with_schema_opt(self, schema_opt: Option<SchemaRef>) -> Self {
        match schema_opt {
            Some(schema) => self.with_schema(schema),
            None => self,
        }
    }

    /// Optionally provide an expression to filter rows. For example, using the predicate `x <
    /// 4` to return a subset of the rows in the scan which satisfy the filter. If `predicate_opt`
    /// is `None`, this is a no-op.
    ///
    /// NOTE: The filtering is best-effort and can produce false positives (rows that should should
    /// have been filtered out but were kept).
    pub fn with_predicate(mut self, predicate: impl Into<Option<ExpressionRef>>) -> Self {
        self.predicate = predicate.into();
        self
    }

    /// Build the [`TableChangesScan`].
    ///
    /// This does not scan the table at this point, but does do some work to ensure that the
    /// provided schema make sense, and to prepare some metadata that the scan will need.  The
    /// [`TableChangesScan`] type itself can be used to fetch the files and associated metadata required to
    /// perform actual data reads.
    pub fn build(self) -> DeltaResult<TableChangesScan> {
        // if no schema is provided, use snapshot's entire schema (e.g. SELECT *)
        let logical_schema = self
            .schema
            .unwrap_or_else(|| self.table_changes.schema.clone().into());
        let (all_fields, read_fields, have_partition_cols) = get_state_info(
            logical_schema.as_ref(),
            &self.table_changes.metadata.partition_columns,
            self.table_changes.column_mapping_mode,
        )?;
        let physical_schema = Arc::new(StructType::new(read_fields));
        Ok(TableChangesScan {
            table_changes: self.table_changes,
            logical_schema,
            physical_schema,
            predicate: self.predicate,
            all_fields,
            have_partition_cols,
        })
    }
}
