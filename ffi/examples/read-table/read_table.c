#include <stdio.h>
#include <string.h>

#include <delta_kernel_ffi.h>

#include "read_table.h"
#include "schema.h"

#ifdef PRINT_ARROW_DATA
#include "arrow.h"
#endif

struct EngineContext {
  GlobalScanState* global_state;
  char* table_root;
  const ExternEngineHandle* engine;
  PartitionList* partition_cols;
  CStringMap* partition_values;
#ifdef PRINT_ARROW_DATA
  ArrowContext* arrow_context;
#endif
};

// This is how we represent our errors. The kernel will ask us to contruct this struct whenever it
// enounters an error, and then return the contructed EngineError to us
typedef struct Error {
  struct EngineError etype;
  char* msg;
} Error;

void print_selection_vector(const char* indent, const KernelBoolSlice* selection_vec) {
#ifdef VERBOSE
  for (int i = 0; i < selection_vec->len; i++) {
    printf("%ssel[%i] = %b\n", indent, i, selection_vec->ptr[i]);
  }
#endif
}

EngineError* allocate_error(KernelError etype, const KernelStringSlice msg) {
  Error* error = malloc(sizeof(Error));
  error->etype.etype = etype;
  char* charmsg = allocate_string(msg);
  error->msg = charmsg;
  return (EngineError*)error;
}

void free_error(Error* error) {
  free(error->msg);
  free(error);
}

void print_error(const char* indent, Error* err) {
  printf("%sCode: %i\n", indent, err->etype);
  printf("%sMsg: %s\n", indent, err->msg);
}

void set_builder_opt(EngineBuilder* engine_builder, char* key, char* val) {
  KernelStringSlice key_slice = { key, strlen(key) };
  KernelStringSlice val_slice = { val, strlen(val) };
  set_builder_option(engine_builder, key_slice, val_slice);
}

#ifdef PRINT_ARROW_DATA
// convert to a garrow boolean array. can't use garrow_boolean_array_builder_append_values as that
// expects a gboolean*, which is actually an int* which is 4 bytes, but our slice is a C99 _Bool*
// which is 1 byte
GArrowBooleanArray* slice_to_arrow_bool_array(const KernelBoolSlice slice) {
  GArrowBooleanArrayBuilder* builder = garrow_boolean_array_builder_new();
  for (int i = 0; i < slice.len; i++) {
    gboolean val = slice.ptr[i] ? TRUE : FALSE;
    garrow_boolean_array_builder_append_value(builder, val, NULL);
  }
  GArrowArray* ret = garrow_array_builder_finish((GArrowArrayBuilder*)builder, NULL);
  if (ret == NULL) {
    printf("ERROR IN BUILDING\n");
  }
  return (GArrowBooleanArray*)ret;
}

void visit_read_data(void* vcontext, EngineDataHandle* data) {
  print_diag("  Converting read data to arrow and adding to context\n");
  struct EngineContext* context = vcontext;
  ExternResultArrowFFIData arrow_res = get_raw_arrow_data(data, context->engine);
  if (arrow_res.tag != OkArrowFFIData) {
    printf("Failed to get arrow data\n");
    print_error("  ", (Error*)arrow_res.err);
    free_error((Error*)arrow_res.err);
    exit(-1);
  }
  ArrowFFIData* arrow_data = arrow_res.ok;
  add_batch_to_context(
    context->arrow_context, arrow_data, context->partition_cols, context->partition_values);
}

void read_parquet_file(struct EngineContext* context,
                       const KernelStringSlice path,
                       const KernelBoolSlice selection_vector) {
  int full_len = strlen(context->table_root) + path.len + 1;
  char* full_path = malloc(sizeof(char) * full_len);
  snprintf(full_path, full_len, "%s%.*s", context->table_root, path.len, path.ptr);
  print_diag("  Reading parquet file at %s\n", full_path);
  KernelStringSlice path_slice = { full_path, full_len };
  Schema* read_schema = get_global_read_schema(context->global_state);
  FileMeta meta = {
    .path = path_slice,
  };
  ExternResultFileReadResultIterator read_res =
    read_parquet_files(context->engine, &meta, read_schema);
  if (read_res.tag != OkFileReadResultIterator) {
    printf("Couldn't read data\n");
    return;
  }
  if (selection_vector.len > 0) {
    GArrowBooleanArray* sel_array = slice_to_arrow_bool_array(selection_vector);
    context->arrow_context->cur_filter = sel_array;
  }
  FileReadResultIterator* read_iter = read_res.ok;
  for (;;) {
    ExternResultbool ok_res = read_result_next(read_iter, context, visit_read_data);
    if (ok_res.tag != Okbool) {
      printf("Failed to iterate read data\n");
      print_error("  ", (Error*)ok_res.err);
      free_error((Error*)ok_res.err);
      exit(-1);
    } else if (!ok_res.ok) {
      print_diag("  Done reading parquet file\n");
      break;
    }
  }
}
#endif

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

void visit_callback(void* engine_context,
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

void visit_data(void* engine_context,
                EngineDataHandle* engine_data,
                KernelBoolSlice selection_vec) {
  print_diag("\nScan iterator found some data to read\n  Of this data, here is "
             "a selection vector\n");
  print_selection_vector("    ", &selection_vec);
  visit_scan_data(engine_data, selection_vec, engine_context, visit_callback);
  drop_bool_slice(selection_vec);
}

void visit_partition(void* context, const KernelStringSlice partition) {
  PartitionList* list = context;
  char* col = allocate_string(partition);
  list->cols[list->len] = col;
  list->len++;
}

PartitionList* get_partition_list(GlobalScanState* state) {
  int count = get_partition_column_count(state);
  PartitionList* list = malloc(sizeof(PartitionList));
  list->len = 0;
  list->cols = malloc(sizeof(char*) * count);
  StringSliceIterator* part_iter = get_partition_columns(state);
  for (;;) {
    bool has_next = string_slice_next(part_iter, list, visit_partition);
    if (!has_next) {
      print_diag("Done iterating partition columns");
      break;
    }
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
    printf("Could not get engine builder.\n");
    print_error("  ", (Error*)engine_builder_res.err);
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
    printf("Failed to get engine\n");
    print_error("  ", (Error*)engine_builder_res.err);
    free_error((Error*)engine_builder_res.err);
    return -1;
  }

  const ExternEngineHandle* engine = engine_res.ok;

  ExternResultSnapshotHandle snapshot_handle_res = snapshot(table_path_slice, engine);
  if (snapshot_handle_res.tag != OkSnapshotHandle) {
    printf("Failed to create snapshot\n");
    print_error("  ", (Error*)snapshot_handle_res.err);
    free_error((Error*)snapshot_handle_res.err);
    return -1;
  }

  const SnapshotHandle* snapshot_handle = snapshot_handle_res.ok;

  uint64_t v = version(snapshot_handle);
  printf("version: %llu\n\n", v);
  print_schema(snapshot_handle);

  ExternResultScan scan_res = scan(snapshot_handle, engine, NULL);
  if (scan_res.tag != OkScan) {
    printf("Failed to create scan\n");
    print_error("  ", (Error*)scan_res.err);
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
    printf("Failed to construct scan data iterator\n");
    print_error("  ", (Error*)data_iter_res.err);
    free_error((Error*)data_iter_res.err);
    return -1;
  }

  KernelScanDataIterator* data_iter = data_iter_res.ok;

  // iterate scan files
  for (;;) {
    ExternResultbool ok_res = kernel_scan_data_next(data_iter, &context, visit_data);
    if (ok_res.tag != Okbool) {
      printf("Failed to iterate scan data\n");
      print_error("  ", (Error*)ok_res.err);
      free_error((Error*)ok_res.err);
      return -1;
    } else if (!ok_res.ok) {
      print_diag("Iterator done\n");
      break;
    }
  }

  print_diag("All done\n");

#ifdef PRINT_ARROW_DATA
  print_arrow_context(context.arrow_context);
#endif

  kernel_scan_data_free(data_iter);
  drop_global_scan_state(global_state);
  drop_snapshot(snapshot_handle);
  drop_engine(engine);

  return 0;
}
