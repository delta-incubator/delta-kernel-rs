update-dv
=========

C FFI example for connector-authored deletion vector updates. The example assumes the
connector has already written a DV file (or built inline DV bytes) and knows the descriptor
fields to install in the Delta log.

It demonstrates:

- `dv_descriptor_map_new`
- `dv_descriptor_new`
- `dv_descriptor_map_insert`
- `scan_metadata_iter_init`
- `update_table_txn_update_deletion_vectors`
- `update_table_txn_commit`

# Building

```bash
# from repo root
$ cargo build -p delta_kernel_ffi
# from this directory
$ mkdir build && cd build && cmake .. && make
```

# Running

```bash
$ ./update_dv /path/to/table data-file.parquet p file:///tmp/table/dv.bin 1 36 2
```

Arguments:

1. Table path
2. Data file path exactly as it appears in scan metadata / the Add action
3. Storage type: `u` (persisted relative), `i` (inline), or `p` (persisted absolute)
4. `pathOrInlineDv`
5. Offset, or `-` to omit it
6. `sizeInBytes`
7. Cardinality

The table must have the `deletionVectors` reader/writer feature and
`delta.enableDeletionVectors=true`. The example does not write the DV file itself; it only
installs the descriptor and lets the kernel stage the matching remove/add action pair.

# Kernel-authored DV files

Connectors can use `write_deletion_vectors` instead of serializing DV files themselves.
It accepts a borrowed `FfiSlice<FfiDeletionVectorUpdate>` with one entry per active file:
the exact Add-action path and a borrowed slice of physical row indexes. Indexes are
zero-based across the whole file. Duplicate indexes within an entry are deduplicated;
duplicate file entries are rejected.

Start an existing-table transaction with
`new_update_table_txn_builder(snapshot, operation, engine_info, data_change, engine)`
from the snapshot used to resolve physical row locations. Supply the operation and
connector engine information at construction, with `data_change = true` for row
changes, then build with `update_table_txn_builder_build`.
Supply that snapshot and a bound write context from the same transaction to
`write_deletion_vectors`.
The function reads snapshot file metadata once, validates all requested locations,
preserves each existing DV, and uploads one Kernel-serialized DV file containing the
batch's vectors using the Engine's storage handler. The vectors are prepared in memory;
each descriptor identifies its own offset in the shared file. It does not open the
business Parquet files, modify a transaction, or commit anything.

The returned descriptor map is owned by the caller. Pass it to
`update_table_txn_update_deletion_vectors` together with a metadata iterator from the same
snapshot. Stage new-file metadata with `update_table_txn_add_files` on that transaction
and call `update_table_txn_commit` once. If preparation is abandoned, release the map
with `free_dv_descriptor_map`.
The snapshot, write context, engine, and all input slices remain caller-owned.

An empty batch returns an empty map without uploading files. Missing files, unsupported
DV features, missing `numRecords`, and out-of-bounds indexes fail explicitly. Input
validation, including reading and validating all existing DVs, finishes before upload.
A validation or serialization failure therefore uploads no DV file. An upload failure
can still leave an uncommitted DV file. Do not treat that file as published or delete
it while another operation could still use it; apply the connector's safe
orphan-retention policy.

After a commit conflict, load a fresh snapshot and resolve physical row locations again.
Reusing a descriptor map built against stale file locations can invalidate the wrong
rows after compaction.
