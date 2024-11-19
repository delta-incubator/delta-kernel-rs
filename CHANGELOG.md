# Changelog


test change revert me

## [v0.4.1](https://github.com/delta-io/delta-kernel-rs/tree/v0.4.1/) (2024-10-28)


[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.4.0...v0.4.1)

**API Changes**

None.

**Fixed bugs:**

- **Disabled missing-column row group skipping**: The optimization to treat a physically missing
column as all-null is unsound, if the schema was not already verified to prove that the table's
logical schema actually includes the missing column. We disable it until we can add the necessary
validation. [\#435]

[\#435]: https://github.com/delta-io/delta-kernel-rs/pull/435

## [v0.4.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.4.0/) (2024-10-23)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.3.1...v0.4.0)

**API Changes**

*Breaking*

1. `pub ScanResult.mask` field made private and only accessible as `ScanResult.raw_mask()` method [\#374]
2. new `ReaderFeatures` enum variant: `TypeWidening` and `TypeWideningPreview` [\#335]
3. new `WriterFeatures` enum variant: `TypeWidening` and `TypeWideningPreview` [\#335]
4. new `Error` enum variant: `InvalidLogPath` when kernel is unable to parse the name of a log path [\#347]
5. Module moved: `mod delta_kernel::transaction` -> `mod delta_kernel::actions::set_transaction` [\#386]
6. change `default-feature` to be none (removed `sync-engine` by default. If downstream users relied on this, turn on `sync-engine` feature or specific arrow-related feature flags to pull in the pieces needed) [\#339]
7. `Scan`'s `execute(..)` method now returns a lazy iterator instead of materializing a `Vec<ScanResult>`. You can trivially migrate to the new API (and force eager materialization by using `.collect()` or the like on the returned iterator) [\#340]
8. schema and expression FFI moved to their own `mod delta_kernel_ffi::schema` and `mod delta_kernel_ffi::expressions` [\#360]
9. Parquet and JSON readers in `Engine` trait now take `Arc<Expression>` (aliased to `ExpressionRef`) instead of `Expression` [\#364]
10. `StructType::new(..)` now takes an `impl IntoIterator<Item = StructField>` instead of `Vec<StructField>` [\#385]
11. `DataType::struct_type(..)` now takes an `impl IntoIterator<Item = StructField>` instead of `Vec<StructField>` [\#385]
12. removed `DataType::array_type(..)` API: there is already an `impl From<ArrayType> for DataType` [\#385]
13. `Expression::struct_expr(..)` renamed to `Expression::struct_from(..)` [\#399]
14. lots of expressions take `impl Into<Self>` or `impl Into<Expression>` instead of just `Self`/`Expression` now [\#399]
15. remove `log_replay_iter` and `process_batch` APIs in `scan::log_replay` [\#402]

*Additions*

1. remove feature flag requirement for `impl GetData` on `()` [\#334]
2. new `full_mask()` method on `ScanResult` [\#374]
3. `StructType::try_new(fields: impl IntoIterator<Item = StructField>)` [\#385]
4. `DataType::try_struct_type(fields: impl IntoIterator<Item = StructField>)` [\#385]
5. `StructField.metadata_with_string_values(&self) -> HashMap<String, String>` to materialize and return our metadata into a hashmap [\#331]

**Implemented enhancements:**

- support reading tables with type widening in default engine [\#335]
- add predicate to protocol and metadata log replay for pushdown [\#336] and [\#343]
- support annotation (macro) for nullable values in a container (for `#[derive(Schema)]`) [\#342]
- new `ParsedLogPath` type for better log path parsing [\#347]
- implemented row group skipping for default engine parquet readers and new utility trait for stats-based skipping logic [\#357], [\#362], [\#381]
- depend on wider arrow versions and add arrow integration testing [\#366] and [\#413]
- added semver testing to CI [\#369], [\#383], [\#384]
- new `SchemaTransform` trait and usage in column mapping and data skipping [\#395] and [\#398]
- arrow expression evaluation improvements [\#401]
- replace panics with `to_compiler_error` in macros [\#409]

**Fixed bugs:**

- output of arrow expression evaluation now applies/validates output schema in default arrow expression handler [\#331]
- add `arrow-buffer` to `arrow-expression` feature [\#332]
- fix bug with out-of-date last checkpoint [\#354]
- fixed broken sync engine json parsing and harmonized sync/async json parsing [\#373]
- filesystem client now always returns a sorted list [\#344]

[\#331]: https://github.com/delta-io/delta-kernel-rs/pull/331
[\#332]: https://github.com/delta-io/delta-kernel-rs/pull/332
[\#334]: https://github.com/delta-io/delta-kernel-rs/pull/334
[\#335]: https://github.com/delta-io/delta-kernel-rs/pull/335
[\#336]: https://github.com/delta-io/delta-kernel-rs/pull/336
[\#337]: https://github.com/delta-io/delta-kernel-rs/pull/337
[\#339]: https://github.com/delta-io/delta-kernel-rs/pull/339
[\#340]: https://github.com/delta-io/delta-kernel-rs/pull/340
[\#342]: https://github.com/delta-io/delta-kernel-rs/pull/342
[\#343]: https://github.com/delta-io/delta-kernel-rs/pull/343
[\#344]: https://github.com/delta-io/delta-kernel-rs/pull/344
[\#347]: https://github.com/delta-io/delta-kernel-rs/pull/347
[\#354]: https://github.com/delta-io/delta-kernel-rs/pull/354
[\#357]: https://github.com/delta-io/delta-kernel-rs/pull/357
[\#360]: https://github.com/delta-io/delta-kernel-rs/pull/360
[\#362]: https://github.com/delta-io/delta-kernel-rs/pull/362
[\#364]: https://github.com/delta-io/delta-kernel-rs/pull/364
[\#366]: https://github.com/delta-io/delta-kernel-rs/pull/366
[\#369]: https://github.com/delta-io/delta-kernel-rs/pull/369
[\#373]: https://github.com/delta-io/delta-kernel-rs/pull/373
[\#374]: https://github.com/delta-io/delta-kernel-rs/pull/374
[\#381]: https://github.com/delta-io/delta-kernel-rs/pull/381
[\#383]: https://github.com/delta-io/delta-kernel-rs/pull/383
[\#384]: https://github.com/delta-io/delta-kernel-rs/pull/384
[\#385]: https://github.com/delta-io/delta-kernel-rs/pull/385
[\#386]: https://github.com/delta-io/delta-kernel-rs/pull/386
[\#395]: https://github.com/delta-io/delta-kernel-rs/pull/395
[\#398]: https://github.com/delta-io/delta-kernel-rs/pull/398
[\#399]: https://github.com/delta-io/delta-kernel-rs/pull/399
[\#401]: https://github.com/delta-io/delta-kernel-rs/pull/401
[\#402]: https://github.com/delta-io/delta-kernel-rs/pull/402
[\#409]: https://github.com/delta-io/delta-kernel-rs/pull/409
[\#413]: https://github.com/delta-io/delta-kernel-rs/pull/413


## [v0.3.1](https://github.com/delta-io/delta-kernel-rs/tree/v0.3.1/) (2024-09-10)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.3.0...v0.3.1)

**API Changes**

*Additions*

1. Two new binary expressions: `In` and `NotIn`, as well as a new `Scalar::Array` variant to represent arrays in the expression framework [\#270](https://github.com/delta-io/delta-kernel-rs/pull/270) NOTE: exact API for these expressions is still evolving.

**Implemented enhancements:**

- Enabled more golden table tests [\#301](https://github.com/delta-io/delta-kernel-rs/pull/301)

**Fixed bugs:**

- Allow kernel to read tables with invalid `_last_checkpoint` [\#311](https://github.com/delta-io/delta-kernel-rs/pull/311)
- List log files with checkpoint hint when constructing latest snapshot (when version requested is `None`) [\#312](https://github.com/delta-io/delta-kernel-rs/pull/312)
- Fix incorrect offset value when computing list offsets [\#327](https://github.com/delta-io/delta-kernel-rs/pull/327)
- Fix metadata string conversion in default engine arrow conversion [\#328](https://github.com/delta-io/delta-kernel-rs/pull/328)

## [v0.3.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.3.0/) (2024-08-07)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.2.0...v0.3.0)

**API Changes**

*Breaking*

1. `delta_kernel::column_mapping` module moved to `delta_kernel::features::column_mapping` [\#222](https://github.com/delta-io/delta-kernel-rs/pull/297)


*Additions*

1. New deletion vector API `row_indexes` (and accompanying FFI) to get row indexes instead of seletion vector of deleted rows. This can be more efficient for sparse DVs. [\#215](https://github.com/delta-io/delta-kernel-rs/pull/215)
2. Typed table features: `ReaderFeatures`, `WriterFeatures` enums and `has_reader_feature`/`has_writer_feature` API [\#222](https://github.com/delta-io/delta-kernel-rs/pull/297)

**Implemented enhancements:**

- Add `--limit` option to example `read-table-multi-threaded` [\#297](https://github.com/delta-io/delta-kernel-rs/pull/297)
- FFI now built with cmake. Move to using the read-test example as an ffi-test. And building on macos. [\#288](https://github.com/delta-io/delta-kernel-rs/pull/288)
- Golden table tests migrated from delta-spark/delta-kernel java [\#295](https://github.com/delta-io/delta-kernel-rs/pull/295)
- Code coverage implemented via [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov) and reported with [codecov](https://app.codecov.io/github/delta-io/delta-kernel-rs) [\#287](https://github.com/delta-io/delta-kernel-rs/pull/287)
- All tests enabled to run in CI [\#284](https://github.com/delta-io/delta-kernel-rs/pull/284)
- Updated DAT to 0.3 [\#290](https://github.com/delta-io/delta-kernel-rs/pull/290)

**Fixed bugs:**

- Evaluate timestamps as "UTC" instead of "+00:00" for timezone [\#295](https://github.com/delta-io/delta-kernel-rs/pull/295)
- Make Map arrow type field naming consistent with parquet field naming [\#299](https://github.com/delta-io/delta-kernel-rs/pull/299)


## [v0.2.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.2.0/) (2024-07-17)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.1.1...v0.2.0)

**API Changes**

*Breaking*

1. The scan callback if using `visit_scan_files` now takes an extra `Option<Stats>` argument, holding top level
   stats for associated scan file. You will need to add this argument to your callback.

    Likewise, the callback in the ffi code also needs to take a new argument which is a pointer to a
   `Stats` struct, and which can be null if no stats are present.

*Additions*

1. You can call `scan_builder()` directly on a snapshot, for more convenience.
2. You can pass a `URL` starting with `"hdfs"` or `"viewfs"` to the default client to read using `hdfs_native_store`

**Implemented enhancements:**

- Handle nested structs in `schemaString` (allows reading iceberg compat tables) [\#257](https://github.com/delta-io/delta-kernel-rs/pull/257)
- Expose top level stats in scans [\#227](https://github.com/delta-io/delta-kernel-rs/pull/227)
- Hugely expanded C-FFI example [\#203](https://github.com/delta-io/delta-kernel-rs/pull/203)
- Add `scan_builder` function to `Snapshot` [\#273](https://github.com/delta-io/delta-kernel-rs/pull/273)
- Add `hdfs_native_store` support [\#273](https://github.com/delta-io/delta-kernel-rs/pull/274)
- Proper reading of Parquet files, including only reading requested leaves, type casting, and reordering [\#271](https://github.com/delta-io/delta-kernel-rs/pull/271)
- Allow building the package if you are behind an https proxy [\#282](https://github.com/delta-io/delta-kernel-rs/pull/282)

**Fixed bugs:**

- Don't error if more fields exist than expected in a struct expression [\#267](https://github.com/delta-io/delta-kernel-rs/pull/267)
- Handle cases where the deletion vector length is less than the total number of rows in the chunk [\#276](https://github.com/delta-io/delta-kernel-rs/pull/276)
- Fix partition map indexing if column mapping is in effect [\#278](https://github.com/delta-io/delta-kernel-rs/pull/278)


## [v0.1.1](https://github.com/delta-io/delta-kernel-rs/tree/v0.1.0/) (2024-06-03)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.1.0...v0.1.1)

**Implemented enhancements:**

- Support unary `NOT` and `IsNull` for data skipping [\#231](https://github.com/delta-io/delta-kernel-rs/pull/231)
- Add unary visitors to c ffi [\#247](https://github.com/delta-io/delta-kernel-rs/pull/247)
- Minor other QOL improvements


## [v0.1.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.1.0/) (2024-06-12)

Initial public release