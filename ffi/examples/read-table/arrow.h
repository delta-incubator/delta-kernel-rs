// This file contains code to work with arrow data. Used when we are actually reading and printing
// the content of the table

#include "delta_kernel_ffi.h"
#include "read_table.h"
#include <arrow-glib/arrow-glib.h>

typedef struct ArrowContext {
  gsize num_batches;
  GArrowRecordBatch** batches;
  GArrowBooleanArray* cur_filter;
} ArrowContext;

ArrowContext* init_arrow_context() {
  ArrowContext* context = malloc(sizeof(ArrowContext));
  context->num_batches = 0;
  context->batches = calloc(0, sizeof(GArrowRecordBatch*));
  context->cur_filter = NULL;
  return context;
}

// Turn ffi formatted schema data into a GArrowSchema
static GArrowSchema* get_schema(FFI_ArrowSchema* schema) {
  GError* error = NULL;
  GArrowSchema* garrow_schema = garrow_schema_import((gpointer)schema, &error);
  if (error != NULL) {
    fprintf(stderr, "Can't get schema: %s\n", error->message);
    g_error_free(error);
  }
  return garrow_schema;
}

// Turn ffi formatted record batch data into a GArrowRecordBatch
static GArrowRecordBatch* get_record_batch(FFI_ArrowArray* array, GArrowSchema* schema) {
  GError* error = NULL;
  GArrowRecordBatch* record_batch = garrow_record_batch_import((gpointer)array, schema, &error);
  if (error != NULL) {
    fprintf(stderr, "Can't get record batch: %s\n", error->message);
    g_error_free(error);
  }
  return record_batch;
}

// Add columns to a record batch for each partition
static GArrowRecordBatch* add_partition_columns(GArrowRecordBatch* record_batch,
                                                PartitionList* partition_cols,
                                                CStringMap* partition_values) {
  gint64 rows = garrow_record_batch_get_n_rows(record_batch);
  gint64 cols = garrow_record_batch_get_n_columns(record_batch);
  GArrowRecordBatch* new_record_batch = record_batch;
  for (int i = 0; i < partition_cols->len; i++) {
    char* col = partition_cols->cols[i];
    guint pos = cols + i;
    KernelStringSlice key = { col, strlen(col) };
    char* partition_val = get_from_map(partition_values, key, allocate_string);
    if (partition_val) {
      print_diag(
        "  Adding partition column '%s' with value '%s' at column %u\n", col, partition_val, pos);
      GArrowStringArrayBuilder* builder = garrow_string_array_builder_new();
      for (gint64 i = 0; i < rows; i++) {
        garrow_string_array_builder_append_string(builder, partition_val, NULL);
      }
      GArrowArray* ret = garrow_array_builder_finish((GArrowArrayBuilder*)builder, NULL);
      GArrowField* field = garrow_field_new(col, (GArrowDataType*)garrow_string_data_type_new());
      GError* error = NULL;
      new_record_batch = garrow_record_batch_add_column(new_record_batch, pos, field, ret, &error);
      if (new_record_batch == NULL) {
        if (error != NULL) {
          // Report error to user, and free error
          fprintf(stderr, "Could not add column at %u: %s\n", pos, error->message);
          g_error_free(error);
        }
      }
      free(partition_val);
    } else {
      printf("Error: Did not find value for expected partition column '%s'\n", col);
      exit(-1);
    }
  }
  return new_record_batch;
}

// append a batch to our context
static void add_batch_to_context(ArrowContext* context,
                                 ArrowFFIData* arrow_data,
                                 PartitionList* partition_cols,
                                 CStringMap* partition_values) {
  GArrowSchema* schema = get_schema(&arrow_data->schema);
  GArrowRecordBatch* record_batch = get_record_batch(&arrow_data->array, schema);
  if (context->cur_filter != NULL) {
    record_batch = garrow_record_batch_filter(record_batch, context->cur_filter, NULL, NULL);
    context->cur_filter = NULL; // the filter is now part of the record batch (TODO: is this true?)
  }
  record_batch = add_partition_columns(record_batch, partition_cols, partition_values);
  context->batches =
    realloc(context->batches, sizeof(GArrowRecordBatch*) * (context->num_batches + 1));
  context->batches[context->num_batches] = record_batch;
  context->num_batches++;
  print_diag("  Added batch to arrow context, have %i batches in context now\n",
             context->num_batches);
}

// convert to a garrow boolean array. can't use garrow_boolean_array_builder_append_values as that
// expects a gboolean*, which is actually an int* which is 4 bytes, but our slice is a C99 _Bool*
// which is 1 byte
static GArrowBooleanArray* slice_to_arrow_bool_array(const KernelBoolSlice slice) {
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

// This is the callback that will be called for each chunk of data read from the parquet file
static void visit_read_data(void* vcontext, EngineDataHandle* data) {
  print_diag("  Converting read data to arrow\n");
  struct EngineContext* context = vcontext;
  ExternResultArrowFFIData arrow_res = get_raw_arrow_data(data, context->engine);
  if (arrow_res.tag != OkArrowFFIData) {
    print_error("Failed to get arrow data.", (Error*)arrow_res.err);
    free_error((Error*)arrow_res.err);
    exit(-1);
  }
  ArrowFFIData* arrow_data = arrow_res.ok;
  add_batch_to_context(
    context->arrow_context, arrow_data, context->partition_cols, context->partition_values);
}

// We call this for each file we get called back to read in read_table.c::visit_callback
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
      print_error("Failed to iterate read data.", (Error*)ok_res.err);
      free_error((Error*)ok_res.err);
      exit(-1);
    } else if (!ok_res.ok) {
      print_diag("  Done reading parquet file\n");
      break;
    }
  }
}

// Concat all our batches into a `GArrowTable`, call to_string on it, and print the result
void print_arrow_context(ArrowContext* context) {
  if (context->num_batches > 0) {
    GError* error = NULL;
    GArrowSchema* schema = garrow_record_batch_get_schema(context->batches[0]);
    GArrowTable* table =
      garrow_table_new_record_batches(schema, context->batches, context->num_batches, &error);
    if (error != NULL) {
      // Report error to user, and free error
      fprintf(stderr, "Can't create table from batches: %s\n", error->message);
      g_error_free(error);
    }
    gchar* out = garrow_table_to_string(table, &error);
    if (error != NULL) {
      fprintf(stderr, "Can't turn table into string: %s\n", error->message);
      g_error_free(error);
    } else {
      printf("\nTable Data:\n-----------\n\n%s\n", out);
      g_free(out);
    }
  } else {
    printf("[No data]\n");
  }
}
