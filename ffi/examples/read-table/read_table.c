#include <stdio.h>
#include <string.h>

#include "read_table.h"
#include "schema.h"

#ifdef PRINT_ARROW_DATA
#include "arrow.h"
#endif

// Print the content of a selection vector if `VERBOSE` is defined in read_table.h
void print_selection_vector(const char* indent, const KernelBoolSlice* selection_vec) {
#ifdef VERBOSE
  for (int i = 0; i < selection_vec->len; i++) {
    printf("%ssel[%i] = %b\n", indent, i, selection_vec->ptr[i]);
  }
#endif
}

// Print info about table partitions if `VERBOSE` is defined in read_table.h
void print_partition_info(struct EngineContext* context, CStringMap* partition_values) {
#ifdef VERBOSE
  for (int i = 0; i < context->partition_cols->len; i++) {
    char* col = context->partition_cols->cols[i];
    KernelStringSlice key = { col, strlen(col) };
    char* partition_val = get_from_map(partition_values, key, allocate_string);
    if (partition_val) {
      print_diag("  partition '%s' here: %s\n", col, partition_val);
      free(partition_val);
    } else {
      print_diag("  no partition here\n");
    }
  }
#endif
}

// kernel will call this to allocate our errors. This can be used to create an "engine native" type
// error
EngineError* allocate_error(KernelError etype, const KernelStringSlice msg) {
  Error* error = malloc(sizeof(Error));
  error->etype.etype = etype;
  char* charmsg = allocate_string(msg);
  error->msg = charmsg;
  return (EngineError*)error;
}

// utility function to convert key/val into slices and set them on a builder
void set_builder_opt(EngineBuilder* engine_builder, char* key, char* val) {
  KernelStringSlice key_slice = { key, strlen(key) };
  KernelStringSlice val_slice = { val, strlen(val) };
  set_builder_option(engine_builder, key_slice, val_slice);
}

// Kernel will call this function for each file that should be scanned. The arguments include enough
// context to constuct the correct logical data from the physically read parquet
void scan_row_callback(void* engine_context,
                       const KernelStringSlice path,
                       long size,
                       const DvInfo* dv_info,
                       CStringMap* partition_values) {
  struct EngineContext* context = engine_context;
  print_diag("Called back to read file: %.*s\n", (int)path.len, path.ptr);
  ExternResultKernelBoolSlice selection_vector_res =
    selection_vector_from_dv(dv_info, context->engine, context->global_state);
  if (selection_vector_res.tag != OkKernelBoolSlice) {
    printf("Could not get selection vector from kernel\n");
    exit(-1);
  }
  KernelBoolSlice selection_vector = selection_vector_res.ok;
  if (selection_vector.len > 0) {
    print_diag("  Selection vector for this file:\n");
    print_selection_vector("    ", &selection_vector);
  } else {
    print_diag("  No selection vector for this file\n");
  }
  context->partition_values = partition_values;
  print_partition_info(context, partition_values);
#ifdef PRINT_ARROW_DATA
  read_parquet_file(context, path, selection_vector);
#endif
  drop_bool_slice(selection_vector);
  context->partition_values = NULL;
}

// For each chunk of scan data (which may contain multiple files to scan), kernel will call this
// function (named do_visit_scan_data to avoid conflict with visit_scan_data exported by kernel)
void do_visit_scan_data(void* engine_context,
                        EngineDataHandle* engine_data,
                        KernelBoolSlice selection_vec) {
  print_diag("\nScan iterator found some data to read\n  Of this data, here is "
             "a selection vector\n");
  print_selection_vector("    ", &selection_vec);
  // Ask kernel to iterate each individual file and call us back with extracted metadata
  print_diag("Asking kernel to call us back for each scan row (file to read)\n");
  visit_scan_data(engine_data, selection_vec, engine_context, scan_row_callback);
  drop_bool_slice(selection_vec);
}

// Called for each element of the partition StringSliceIterator. We just turn the slice into a
// `char*` and append it to our list. We knew the total number of partitions up front, so this
// assumes that `list->cols` has been allocated with enough space to store the pointer.
void visit_partition(void* context, const KernelStringSlice partition) {
  PartitionList* list = context;
  char* col = allocate_string(partition);
  list->cols[list->len] = col;
  list->len++;
}

// Build a list of partition column names.
PartitionList* get_partition_list(GlobalScanState* state) {
  print_diag("Building list of partition columns\n");
  int count = get_partition_column_count(state);
  PartitionList* list = malloc(sizeof(PartitionList));
  // We set the `len` to 0 here and use it to track how many items we've added to the list
  list->len = 0;
  list->cols = malloc(sizeof(char*) * count);
  StringSliceIterator* part_iter = get_partition_columns(state);
  for (;;) {
    bool has_next = string_slice_next(part_iter, list, visit_partition);
    if (!has_next) {
      print_diag("Done iterating partition columns\n");
      break;
    }
  }
  if (list->len != count) {
    printf("Error, partition iterator did not return get_partition_column_count columns\n");
    exit(-1);
  }
  if (list->len > 0) {
    print_diag("Partition columns are:\n");
    for (int i = 0; i < list->len; i++) {
      print_diag("  - %s\n", list->cols[i]);
    }
  } else {
    print_diag("Table has no partition columns\n");
  }
  return list;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Usage: %s table/path\n", argv[0]);
    return -1;
  }

  char* table_path = argv[1];
  printf("Reading table at %s\n", table_path);

  KernelStringSlice table_path_slice = { table_path, strlen(table_path) };

  ExternResultEngineBuilder engine_builder_res =
    get_engine_builder(table_path_slice, allocate_error);
  if (engine_builder_res.tag != OkEngineBuilder) {
    print_error("Could not get engine builder.", (Error*)engine_builder_res.err);
    free_error((Error*)engine_builder_res.err);
    return -1;
  }

  // an example of using a builder to set options when building an engine
  EngineBuilder* engine_builder = engine_builder_res.ok;
  set_builder_opt(engine_builder, "aws_region", "us-west-2");
  // potentially set credentials here
  // set_builder_opt(engine_builder, "aws_access_key_id" , "[redacted]");
  // set_builder_opt(engine_builder, "aws_secret_access_key", "[redacted]");
  ExternResultExternEngineHandle engine_res = builder_build(engine_builder);

  // alternately if we don't care to set any options on the builder:
  // ExternResultExternEngineHandle engine_res =
  //   get_default_engine(table_path_slice, NULL);

  if (engine_res.tag != OkExternEngineHandle) {
    print_error("File to get engine", (Error*)engine_builder_res.err);
    free_error((Error*)engine_builder_res.err);
    return -1;
  }

  const ExternEngineHandle* engine = engine_res.ok;

  ExternResultSnapshotHandle snapshot_handle_res = snapshot(table_path_slice, engine);
  if (snapshot_handle_res.tag != OkSnapshotHandle) {
    print_error("Failed to create snapshot.", (Error*)snapshot_handle_res.err);
    free_error((Error*)snapshot_handle_res.err);
    return -1;
  }

  const SnapshotHandle* snapshot_handle = snapshot_handle_res.ok;

  uint64_t v = version(snapshot_handle);
  printf("version: %llu\n\n", v);
  print_schema(snapshot_handle);

  print_diag("Starting table scan\n\n");

  ExternResultScan scan_res = scan(snapshot_handle, engine, NULL);
  if (scan_res.tag != OkScan) {
    print_error("Failed to create scan.", (Error*)scan_res.err);
    free_error((Error*)scan_res.err);
    return -1;
  }

  Scan* scan = scan_res.ok;
  GlobalScanState* global_state = get_global_scan_state(scan);
  PartitionList* partition_cols = get_partition_list(global_state);
  struct EngineContext context = {
    global_state,
    table_path,
    engine,
    partition_cols,
    .partition_values = NULL,
#ifdef PRINT_ARROW_DATA
    .arrow_context = init_arrow_context(),
#endif
  };

  ExternResultKernelScanDataIterator data_iter_res = kernel_scan_data_init(engine, scan);
  if (data_iter_res.tag != OkKernelScanDataIterator) {
    print_error("Failed to construct scan data iterator.", (Error*)data_iter_res.err);
    free_error((Error*)data_iter_res.err);
    return -1;
  }

  KernelScanDataIterator* data_iter = data_iter_res.ok;

  print_diag("\nIterating scan data\n");

  // iterate scan files
  for (;;) {
    ExternResultbool ok_res = kernel_scan_data_next(data_iter, &context, do_visit_scan_data);
    if (ok_res.tag != Okbool) {
      print_error("Failed to iterate scan data.", (Error*)ok_res.err);
      free_error((Error*)ok_res.err);
      return -1;
    } else if (!ok_res.ok) {
      print_diag("Scan data iterator done\n");
      break;
    }
  }

  print_diag("All done reading table data\n");

#ifdef PRINT_ARROW_DATA
  print_arrow_context(context.arrow_context);
#endif

  kernel_scan_data_free(data_iter);
  drop_global_scan_state(global_state);
  drop_snapshot(snapshot_handle);
  drop_engine(engine);

  return 0;
}
