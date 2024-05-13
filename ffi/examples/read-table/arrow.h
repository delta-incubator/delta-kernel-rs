#include <arrow-glib/arrow-glib.h>
#include "delta_kernel_ffi.h"

static GArrowSchema* get_schema(FFI_ArrowSchema *schema) {
  GError *error = NULL;
  GArrowSchema *garrow_schema = garrow_schema_import((gpointer)schema, &error);
  if (error != NULL) {
    // Report error to user, and free error
    fprintf (stderr, "Can't get schema: %s\n", error->message);
    g_error_free (error);
  }
  return garrow_schema;
}

static GArrowRecordBatch* get_record_batch(FFI_ArrowArray *array, GArrowSchema *schema) {
  GError *error = NULL;
  GArrowRecordBatch *record_batch = garrow_record_batch_import((gpointer)array, schema, &error);
  if (error != NULL) {
    // Report error to user, and free error
    fprintf (stderr, "Can't get record batch: %s\n", error->message);
    g_error_free (error);
  }
  return record_batch;
}

typedef struct ArrowContext {
  gsize num_batches;
  GArrowRecordBatch **batches;
} ArrowContext;

ArrowContext* init_arrow_context() {
  ArrowContext *context = malloc(sizeof(ArrowContext));
  context->num_batches = 0;
  context->batches = calloc(0, sizeof(GArrowRecordBatch*));
  return context;
}

void add_batch_to_context(ArrowContext *context, ArrowFFIData *arrow_data) {
  GArrowSchema *schema = get_schema(&arrow_data->schema);
  GArrowRecordBatch *record_batch = get_record_batch(&arrow_data->array, schema);
  context->batches = realloc(context->batches, sizeof(GArrowRecordBatch*) * (context->num_batches+1));
  context->batches[context->num_batches] = record_batch;
  context->num_batches++;
  printf("Added to context, have %i now\n", context->num_batches);
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
      fprintf (stderr, "Can't create table from batches: %s\n", error->message);
      g_error_free (error);
    }
    gchar *out = garrow_table_to_string(table, &error);
    if (error != NULL) {
      fprintf (stderr, "Can't turn table into string: %s\n", error->message);
      g_error_free (error);
    } else {
      printf("%s\n", out);
      g_free(out);
    }
  } else {
    printf("[No data]\n");
  }
}
