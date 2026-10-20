# Altering a table

Configure schema, property, and clustering changes on the existing-table transaction builder.
Kernel validates the combined result at `build()` and commits it atomically. A standalone ALTER
changes table metadata without rewriting files; a data operation can apply the same changes
alongside a write.

Before reading this page, make sure you understand
[Creating a Table](./create_table.md) and
[Appending Data](./append.md).

## ALTER TABLE versus schema evolution

Use **ALTER TABLE** for a standalone, metadata-only schema change. For example, you can add a nullable
column without writing or removing data files.

Use **SCHEMA EVOLUTION** to change the schema while writing or rewriting data files in the same
transaction. For example, you can add a nullable column and append rows containing values for that
column atomically.

The `dataChange` flag indicates whether file actions change the table's logical data.
Pass it explicitly when you construct the builder. The table shows typical workflows.

| Transaction Component | ALTER TABLE | SCHEMA EVOLUTION |
|--------|------------------------|---------------------------------|
| Operation | `UpdateTableOperation::AlterTable` | The data operation, such as `UpdateTableOperation::Write` |
| Table changes | At least one schema, property, or clustering request | Optional |
| Data-file changes | None staged | Staged on the built transaction |
| `data_change` constructor argument | `false` | `true` if records change; `false` otherwise |

Both workflows use `UpdateTableTransactionBuilder`. You queue all schema operations before `build()`;
Kernel validates and applies them during `build()`. For schema evolution, you stage data-file changes
on the returned `Transaction`, whose `WriteState` uses the evolved schema.

`ALTER TABLE` requires `dataChange = false`. Kernel checks this at `build()` and retains a defensive
check at `commit()`. Commit validation also rejects staged file additions, removals, and
deletion-vector updates, including empty or fully unselected batches. Staged manifest writes are
also rejected.
The supplied value cannot be changed and is preserved even when no file batches are staged.

For a rewrite that preserves logical data, select the data operation, such as
`UpdateTableOperation::Optimize`, and pass `false` to `update_table()`.

> [!NOTE]
> Incremental schema operations add columns and relax nullability. Use `with_updated_schema()`
> for complete-schema changes on column-mapped tables, including drops, renames, and supported widening.

## Adding a column

Suppose your table has the canonical schema `name STRING, age INTEGER, city
STRING` with rows for Alice, Bob, and Carol, and you want to add a `country`
column. The flow is:

1. Load a `Snapshot` of the table.
2. Call `snapshot.update_table(UpdateTableOperation::AlterTable, "my-app/1.0", false)` to get an
   `UpdateTableTransactionBuilder`.
3. Call `add_column()` with the new field.
4. Call `build()` to produce a `Transaction`.
5. Call `commit()` to atomically apply the schema change.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::schema::{DataType, StructField};
# use delta_kernel::transaction::{CommitResult, UpdateTableOperation};
# use delta_kernel::{KernelResult, Snapshot};
# fn example() -> KernelResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
// 1. Load a snapshot of the existing table.
let snapshot = Snapshot::builder_for(url).build(&engine)?;

// 2. Build and commit an alter-table transaction that adds a new column.
let result = snapshot
    .update_table(UpdateTableOperation::AlterTable, "my-app/1.0", /* data_change */ false)
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
> Incremental schema operations are rejected when `icebergCompatV2`, `icebergCompatV3`, or
> `allowColumnDefaults` is enabled. Complete-schema updates validate supported transitions against
> the final feature configuration. Unsupported writer features remain rejected.

## Chaining multiple operations

`add_column()` can be called more than once to add several columns in a single
commit. The operations are applied in order, and the resulting schema is
validated as a whole before the commit is constructed:

```rust,ignore
let result = snapshot
    .update_table(UpdateTableOperation::AlterTable, "my-app/1.0", /* data_change */ false)
    .add_column(StructField::nullable("country", DataType::STRING))
    .add_column(StructField::nullable("postal_code", DataType::STRING))
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;
```

An `AlterTable` transaction requires a schema, property, or clustering request. Accepted requests
whose values are unchanged may still commit. Blind append cannot include any of these table changes.

## Complete schema, properties, and clustering

The following setters accumulate intent and validate together at `build()`, regardless of setter
order. Each call replaces the previous request for that setter. Complete-schema replacement cannot
be mixed with incremental schema operations.

| Setter | Contract |
|--------|----------|
| `with_updated_schema(schema)` | Requires column mapping. Surviving fields retain mapping IDs and physical names. Supports nullable additions, drops, renames, reordering, comments, relaxed nullability, permitted widening, and supported literal-default changes on existing fields. |
| `with_table_properties_added(properties)` | Merges ordinary properties, canonicalizes recognized Delta keys, and adds required protocol features. Rejects protected or unsupported Delta settings. |
| `with_table_properties_removed(keys)` | Removes ordinary properties, including absent keys. Rejects all `delta.*` keys and overlap with additions. |
| `with_clustering_columns(columns)` | Replaces logical keys in supplied order. Omission retains keys; an explicit empty list clears them. |

Kernel preserves type-change history, mapping allocation, row-tracking names, and in-commit
timestamp bookkeeping. New fields must omit physical names; Kernel allocates fresh names so they
cannot expose data from dropped columns. Omitted physical names on surviving fields are restored.
Retained statistics selections follow column IDs through renames and drops.
Mapping-mode migrations, row-tracking enablement/disablement, and core IcebergCompatV3 toggles on
existing tables are unsupported. Feature requests never implicitly remove protocol support.
Core Iceberg compatibility does not establish Iceberg writer-compatibility support.

Clustering keys resolve against the final schema. Dropping a retained key fails; dropping it while
replacing or clearing the keys succeeds. A mapped rename preserves the physical reference.
Clustering requests require an unpartitioned Delta table and valid statistics-eligible keys.
Every explicit request enables clustering protocol support, even an empty list on an unclustered
table. Omit the setter if you want to avoid this upgrade.

Connectors must ensure historical files have statistics for newly introduced keys. Kernel enforces
statistics for new files but does not backfill historical statistics or physically recluster data.

```rust,no_run
# use delta_kernel::{Engine, KernelResult, SnapshotRef};
# use delta_kernel::schema::SchemaRef;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::expressions::column_name;
# use delta_kernel::transaction::UpdateTableOperation;
# fn configure(snapshot: SnapshotRef, schema: SchemaRef, engine: impl Engine) -> KernelResult<()> {
let transaction = snapshot
    .update_table(UpdateTableOperation::AlterTable, "my-app/1.0", false)
    .with_updated_schema(schema)
    .with_table_properties_added([("application.owner", "analytics")])
    .with_table_properties_removed(["application.legacy"])
    .with_clustering_columns(vec![column_name!("event_time")])
    .build(&engine, Box::new(FileSystemCommitter::new()))?;
# let _ = transaction;
# Ok(())
# }
```

Build before obtaining write state or staging files. Validation failures publish nothing. After a
commit conflict, load a fresh snapshot and prepare the intent again; do not reuse stale metadata.
The Unity Catalog committer rejects existing-table metadata, protocol, and clustering updates.

## Evolving a schema while writing

Select a data operation, such as `UpdateTableOperation::Write`, rather than `AlterTable` when the
same transaction evolves the schema and writes files. Choose `data_change` based on whether the
logical data changes, not merely whether files are written. Kernel applies the schema operations
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
