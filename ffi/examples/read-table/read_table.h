#pragma once

// uncomment below for more diagnotic messages
// #define VERBOSE

#include <delta_kernel_ffi.h>

void print_diag(char* fmt, ...) {
#ifdef VERBOSE
  va_list args;
  va_start(args, fmt);
  vprintf(fmt, args);
  va_end(args);
#endif
}

// A list of partition column names
typedef struct PartitionList {
  int len;
  char** cols;
} PartitionList;

// This is information we want to pass between callbacks. The kernel will generally take a `void*`
// "context" argument, and then pass it back when calling a callback.
struct EngineContext {
  GlobalScanState* global_state;
  char* table_root;
  const ExternEngineHandle* engine;
  PartitionList* partition_cols;
  CStringMap* partition_values;
#ifdef PRINT_ARROW_DATA
  struct ArrowContext* arrow_context;
#endif
};

// This is how we represent our errors. The kernel will ask us to contruct this struct whenever it
// enounters an error, and then return the contructed EngineError to us
typedef struct Error {
  struct EngineError etype;
  char* msg;
} Error;

// Print out an error message, plus the code and kernel message of an error
void print_error(const char* msg, Error* err) {
  printf("[ERROR] %s\n", msg);
  printf("  Kernel Code: %i\n", err->etype);
  printf("  Kernel Msg: %s\n", err->msg);
}

// free an error
void free_error(Error* error) {
  free(error->msg);
  free(error);
}

// create a char* from a KernelStringSlice
void* allocate_string(const KernelStringSlice slice) {
  return strndup(slice.ptr, slice.len);
}
