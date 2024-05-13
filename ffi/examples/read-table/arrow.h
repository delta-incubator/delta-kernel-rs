#include <arrow-glib/arrow-glib.h>
#include "read_table.h"
#include "delta_kernel_ffi.h"

static GArrowSchema* get_schema(FFI_ArrowSchema *schema) {
  GError *error = NULL;
  GArrowSchema *garrow_schema = garrow_schema_import((gpointer)schema, &error);
  if (error != NULL) {
    fprintf(stderr, "Can't get schema: %s\n", error->message);
    g_error_free(error);
  }
  return garrow_schema;
}

static GArrowRecordBatch* get_record_batch(FFI_ArrowArray *array, GArrowSchema *schema) {
  GError *error = NULL;
  GArrowRecordBatch *record_batch = garrow_record_batch_import((gpointer)array, schema, &error);
  if (error != NULL) {
    fprintf(stderr, "Can't get record batch: %s\n", error->message);
    g_error_free(error);
  }
  return record_batch;
}

typedef struct ArrowContext {
  gsize num_batches;
  GArrowRecordBatch **batches;
  GArrowBooleanArray *cur_filter;
} ArrowContext;

ArrowContext* init_arrow_context() {
  ArrowContext *context = malloc(sizeof(ArrowContext));
  context->num_batches = 0;
  context->batches = calloc(0, sizeof(GArrowRecordBatch*));
  context->cur_filter = NULL;
  return context;
}

static GArrowRecordBatch* add_partition_columns(
  GArrowRecordBatch *record_batch,
  PartitionList *partition_cols,
  CStringMap *partition_values
) {
  gint64 rows = garrow_record_batch_get_n_rows(record_batch);
  gint64 cols = garrow_record_batch_get_n_columns(record_batch);
  GArrowRecordBatch *new_record_batch = record_batch;
  for (int i = 0; i < partition_cols->len; i++) {
    char *col = partition_cols->cols[i];
    guint pos = cols + i;
    KernelStringSlice key = {col, strlen(col)};
    char *partition_val = get_from_map(partition_values, key, allocate_string);
    if (partition_val) {
      print_diag("  Adding partition column '%s' with value '%s' at %u\n", col, partition_val, pos);
      GArrowStringArrayBuilder* builder = garrow_string_array_builder_new();
      for (gint64 i = 0; i < rows; i++) {
	garrow_string_array_builder_append_string(
          builder,
          partition_val,
          NULL
	);
      }
      GArrowArray *ret = garrow_array_builder_finish((GArrowArrayBuilder*)builder, NULL);
      GArrowField *field = garrow_field_new(col, (GArrowDataType*)garrow_string_data_type_new());
      GError *error = NULL;
      new_record_batch = garrow_record_batch_add_column(
	new_record_batch,
	pos,
	field,
	ret,
	&error
      );
      if (new_record_batch == NULL) {
	if (error != NULL) {
	  // Report error to user, and free error
	  fprintf(stderr, "Could not add column at %u: %s\n", pos, error->message);
	  g_error_free(error);
	}
      }
      free(partition_val);
    } else {
      print_diag("  no partition here\n");
    }
  }
  return new_record_batch;
}

void add_batch_to_context(
  ArrowContext *context,
  ArrowFFIData *arrow_data,
  PartitionList *partition_cols,
  CStringMap *partition_values
) {
  GArrowSchema *schema = get_schema(&arrow_data->schema);
  GArrowRecordBatch *record_batch = get_record_batch(&arrow_data->array, schema);
  if (context->cur_filter != NULL) {
    record_batch = garrow_record_batch_filter(
      record_batch,
      context->cur_filter,
      NULL,
      NULL
    );
    context->cur_filter = NULL; // the filter is now part of the record batch (TODO: is this true?)
  }
  record_batch = add_partition_columns(record_batch, partition_cols, partition_values);
  context->batches = realloc(context->batches, sizeof(GArrowRecordBatch*) * (context->num_batches+1));
  context->batches[context->num_batches] = record_batch;
  context->num_batches++;
  print_diag("  Added batch to arrow context, have %i in context now\n", context->num_batches);
}

void print_arrow_context(ArrowContext *context) {
  if (context->num_batches > 0) {
    GError *error = NULL;
    GArrowSchema *schema = garrow_record_batch_get_schema(context->batches[0]);
    GArrowTable* table = garrow_table_new_record_batches(
      schema,
      context->batches,
      context->num_batches,
      &error);
    if (error != NULL) {
      // Report error to user, and free error
      fprintf(stderr, "Can't create table from batches: %s\n", error->message);
      g_error_free(error);
    }
    gchar *out = garrow_table_to_string(table, &error);
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
