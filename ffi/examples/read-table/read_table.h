#pragma once

// uncomment below for more diagnotic messages
// #define VERBOSE

// create a char* from a KernelStringSlice
void* allocate_string(const KernelStringSlice slice) {
  return strndup(slice.ptr, slice.len);
}

void print_diag(char* fmt, ...) {
#ifdef VERBOSE
  va_list args;
  va_start(args, fmt);
  vprintf(fmt, args);
  va_end(args);
#endif
}

typedef struct PartitionList {
  int len;
  char** cols;
} PartitionList;
