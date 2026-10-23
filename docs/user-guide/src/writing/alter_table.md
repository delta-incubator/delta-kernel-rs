# Altering a table

To add a column to an existing Delta table, you build an `AlterTableTransaction`
from a `Snapshot`, queue one or more schema operations, and commit. The result
updates the table's schema. You can commit without rewriting files, or rewrite files while
preserving the table's logical data.

Before reading this page, make sure you understand
[Creating a Table](./create_table.md) and
[Appending Data](./append.md).

## ALTER TABLE versus schema evolution

Use **ALTER TABLE** for schema changes that preserve logical data. You can add a nullable column
without rewriting files, or replace files using the evolved schema without changing logical rows.

Use **UPDATE** for data operations, including schema evolution with backfills or appended rows.
UPDATE also handles operations without schema changes, including OPTIMIZE rewrites that preserve
logical data.

The `dataChange` flag indicates whether file actions change the table's logical data.

| Schema change | Logical data change | Builder entry point | `dataChange` |
|---------------|---------------------|---------------------|--------------|
| Yes | No | `alter_table()` | Always `false` |
| Yes | Yes | `update_table()` | `true` |
| No | Yes | `update_table()` | `true` |
| No | No | `update_table()`, such as OPTIMIZE | `false` |

Both builders use `TransactionBuilder<T>` and share schema operations and common setters.
You queue all schema operations before `build()`; Kernel validates and applies them during
`build()`. Both workflows let you stage data-file changes on the returned `Transaction`, whose
`WriteState` uses the evolved schema. On clustered tables, `build()` may perform log replay to read
clustering columns for write statistics.

For schema evolution during a write, supply `data_change` when constructing the update-table builder.
Kernel preserves this value even for empty commits. For rewrites without schema changes, use
`snapshot.update_table(UpdateTableOperation::Optimize, engine_info, false)`.

> [!NOTE]
> The supported operations are adding columns and making columns nullable. Other schema operations
> (drop column, rename, type changes) are not yet available through the
> transaction builder.

## Adding a column

Suppose your table has the canonical schema `name STRING, age INTEGER, city
STRING` with rows for Alice, Bob, and Carol, and you want to add a `country`
column. The flow is:

1. Load a `Snapshot` of the table.
2. Call `snapshot.alter_table("my-app/1.0")` to get an `AlterTableTransactionBuilder`.
3. Call `add_column()` with the new field.
4. Call `build()` to produce an `AlterTableTransaction`.
5. Call `commit()` to atomically apply the schema change.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::schema::{DataType, StructField};
# use delta_kernel::transaction::CommitResult;
# use delta_kernel::{KernelResult, Snapshot};
# fn example() -> KernelResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
// 1. Load a snapshot of the existing table.
let snapshot = Snapshot::builder_for(url).build(&engine)?;

// 2. Build and commit an alter-table transaction that adds a new column.
let result = snapshot
    .alter_table("my-app/1.0")
    .add_column(StructField::nullable("country", DataType::STRING))
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;

match result {
    CommitResult::Committed(committed) => {
        println!("Schema altered at version {}", committed.commit_version());
    }
    _ => eprintln!("alter table did not succeed"),
}
# Ok(())
# }
```

After this commit, the table schema has four fields. Existing rows for Alice,
Bob, and Carol read back `NULL` for `country`. New writes can populate the
column by including it in the `RecordBatch` they pass to
`engine.write_parquet()`.

If the table declares CHECK constraints, acknowledge that your connector enforces them before you
commit. See [Check constraints](./check_constraints.md) for the acknowledgement.

## Validation rules

`add_column()` checks the new field at `build()` time. If any rule is violated,
`build()` returns an error and no commit is attempted.

| Rule | Why |
|------|-----|
| The field name must not already exist (case-insensitive) | Delta column names are unique within a struct. |
| The field must be nullable | Existing files do not contain the new column. They read back `NULL`, which would violate a `NOT NULL` constraint. |
| The table must support writes | Tables with unsupported writer features cannot be altered. |
| The evolved schema must not require protocol features the table does not enable | For example, adding a `TIMESTAMP_NTZ` column to a table without the `timestampNtz` feature fails. |
| Column mapping tables must be protocol-valid | When column mapping is enabled, Kernel assigns or preserves column-mapping IDs and physical names for the added column and updates `delta.columnMapping.maxColumnId`. |

> [!NOTE]
> Kernel rejects schema changes at `build()` when `icebergCompatV2`, `icebergCompatV3`, or
> `allowColumnDefaults` is enabled. This restriction applies to both workflows, regardless of the
> transaction operation or `dataChange` value.

## Chaining multiple operations

`add_column()` can be called more than once to add several columns in a single
commit. The operations are applied in order, and the resulting schema is
validated as a whole before the commit is constructed:

```rust,ignore
let result = snapshot
    .alter_table("my-app/1.0")
    .add_column(StructField::nullable("country", DataType::STRING))
    .add_column(StructField::nullable("postal_code", DataType::STRING))
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;
```

The builder uses a type-state pattern to enforce that at least one operation is
queued before `build()` is callable. Calling `.build()` directly on
`snapshot.alter_table("my-app/1.0")` without first queuing a schema operation is a compile
error.

## Rewriting files without changing logical data

After building an `AlterTableTransaction`, get its `write_state()` and bind a write context. Write
replacement files using that context's evolved schema, register their metadata with `add_files()`,
and pass the replaced files' scan metadata to `remove_files()`. Commit the additions, removals, and
schema change together.

`AlterTableTransaction` implements `SupportsDataFiles`, sharing these methods and statistics
configuration with UPDATE. It also accepts replacement deletion-vector descriptors through
`update_deletion_vectors()`, such as when you relocate the same deletion bitmap.
All emitted Add and Remove actions use `dataChange = false`. Your connector must preserve the logical
rows across file and deletion-vector changes; Kernel doesn't compare their contents.

On row-tracked tables, preserve Stable Row IDs and Stable Row Commit Versions in replacement files.
Bind their materialized columns through the write-context builder, and call
`ack_row_tracking_preservation()` before committing removals or deletion-vector updates.

The ALTER builder has no `with_data_change()` or `with_blind_append()` setter. Manifest writes,
user domain removals, and explicit row-tracking high-water overrides remain UPDATE-only.
Changing an existing table's partitioning or clustering isn't supported by this builder.

## Evolving a schema while writing

Select a data operation, such as `UpdateTableOperation::Write`, when the same transaction evolves
the schema and changes logical data. Choose `data_change` based on whether the logical data changes,
not merely whether files are written. Kernel applies the schema operations
during `build()`, so the transaction's `WriteState` uses the evolved schema.

```rust,ignore
let mut transaction = snapshot
    .update_table(UpdateTableOperation::Write, "my-app/1.0", /* data_change */ true)
    .add_column(StructField::nullable("country", DataType::STRING))
    .build(&engine, Box::new(FileSystemCommitter::new()))?;

let write_state = transaction.write_state()?;
let write_context = write_state.write_context_builder().build()?;
// Write files using the evolved schema, add their metadata, and commit.
```

## What's next

- [Appending Data](./append.md) walks through writing data to the evolved
  table.
- [Creating a Table](./create_table.md) covers creating a new table with the
  schema you want from the start.
- [Schemas and Data Types](../concepts/schema_and_types.md) describes
  Kernel's type system in detail.
