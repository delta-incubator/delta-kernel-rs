#include <stdint.h>
#include "delta_kernel_ffi.h"

/**
 * This module defines a very simple model of a schema, used only to be able to print the schema of
 * a table. It consists of a "SchemaBuilder" which is our user data that gets passed into each visit_x
 * call. This simply keeps track of all the lists we are asked to allocate.
 *
 * Each list is a "SchemaItemList", which tracks its length an an array of "SchemaItem"s.
 *
 * Each "SchemaItem" has a name and a type, which are just strings. It can also have a list which is
 * its children. This is initially always UINTPTR_MAX, but when visiting a struct, map, or array, we
 * point this at the list id specified in the callback, which allows us to traverse the schema when
 * printing it.
 */

// If you want the visitor to print out what it's being asked to do at each step, uncomment the
// following line
//#define PRINT_VISITS

typedef struct SchemaItemList SchemaItemList;

typedef struct {
  char* name;
  char* type;
  uintptr_t children;
} SchemaItem;

typedef struct SchemaItemList {
  uint32_t len;
  SchemaItem* list;
} SchemaItemList;

typedef struct {
  int list_count;
  SchemaItemList* lists;
} SchemaBuilder;

char* allocate_name(const KernelStringSlice slice) {
  return strndup(slice.ptr, slice.len);
}

// lists are preallocated to have exactly enough space, so we just fill in the next open slot and
// increment our length
SchemaItem* add_to_list(SchemaItemList *list, char* name, char* type) {
  int idx = list->len;
  list->list[idx].name = name;
  list->list[idx].type = type;
  list->len++;
  return list->list+idx;
}

// print out all items in a list, recursing into any children they may have
void print_list(SchemaBuilder* builder, uintptr_t list_id, int indent, bool parent_on_last) {
  SchemaItemList *list = builder->lists+list_id;
  for (int i = 0; i < list->len; i++) {
    bool is_last = i == list->len - 1;
    for (int j = 0; j < indent; j++) {
      if (parent_on_last && j == indent - 1) {
	// don't print a dangling | on my parent's last item
	printf("   ");
      } else {
	printf("│  ");
      }
    }
    SchemaItem* item = &list->list[i];
    char* prefix = is_last? "└" : "├";
    printf("%s─ %s: %s\n", prefix, item->name, item->type);
    if (list->list[i].children != UINTPTR_MAX) {
      print_list(builder, list->list[i].children, indent+1, is_last);
    }
  }
}

// declare all our visitor methods
uintptr_t make_field_list(void *data, uintptr_t reserve) {
  SchemaBuilder *builder = data;
  int id = builder->list_count;
#ifdef PRINT_VISITS
  printf("Making a list of lenth %i with id %i\n", reserve, id);
#endif
  builder->list_count++;
  builder->lists = realloc(builder->lists, sizeof(SchemaItemList) * builder->list_count);
  SchemaItem* list = calloc(reserve, sizeof(SchemaItem));
  for (int i = 0; i < reserve; i++) {
    list[i].children = UINTPTR_MAX;
  }
  builder->lists[id].len = 0;
  builder->lists[id].list = list;
  return id;
}

void visit_struct(void *data,
                  uintptr_t sibling_list_id,
                  struct KernelStringSlice name,
                  uintptr_t child_list_id) {
  SchemaBuilder *builder = data;
  char* name_ptr = allocate_name(name);
#ifdef PRINT_VISITS
  printf("Asked to visit a struct, belonging to list %i for %s. Children are in %i\n", sibling_list_id, name_ptr, child_list_id);
#endif
  SchemaItem* struct_item = add_to_list(builder->lists+sibling_list_id, name_ptr, "struct");
  struct_item->children = child_list_id;
}
void visit_array(void *data,
                 uintptr_t sibling_list_id,
                 struct KernelStringSlice name,
                 bool contains_null,
                 uintptr_t child_list_id) {
  SchemaBuilder *builder = data;
  char* name_ptr = allocate_name(name);
#ifdef PRINT_VISITS
  printf("Asked to visit array, belonging to list %i for %s. Types are in %i\n", sibling_list_id, name_ptr, child_list_id);
#endif
  SchemaItem* array_item = add_to_list(builder->lists+sibling_list_id, name_ptr, "array");
  array_item->children = child_list_id;
}
void visit_map(void *data,
               uintptr_t sibling_list_id,
               struct KernelStringSlice name,
               uintptr_t child_list_id) {
  SchemaBuilder *builder = data;
  char* name_ptr = allocate_name(name);
#ifdef PRINT_VISITS
  printf("Asked to visit map, belonging to list %i for %s. Types are in %i\n", sibling_list_id, name_ptr, child_list_id);
#endif
  SchemaItem* map_item = add_to_list(builder->lists+sibling_list_id, name_ptr, "map");
  map_item->children = child_list_id;
}

void visit_decimal(void *data,
                   uintptr_t sibling_list_id,
                   struct KernelStringSlice name,
                   uint8_t precision,
                   int8_t scale) {
#ifdef PRINT_VISITS
  printf("Asked to visit decimal with precision %i and scale %i, belonging to list %i\n", sibling_list_id);
#endif
  SchemaBuilder *builder = data;
  char* name_ptr = allocate_name(name);
  char* type = malloc(16 * sizeof(char));
  snprintf(type, 16, "decimal(%i)(%i)", precision, scale);
  add_to_list(builder->lists+sibling_list_id, name_ptr, type);
}



void visit_simple_type(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name, char* type) {
  SchemaBuilder *builder = data;
  char* name_ptr = allocate_name(name);
#ifdef PRINT_VISITS
  printf("Asked to visit a(n) %s belonging to list %i for %s\n", type, sibling_list_id, name_ptr);
#endif
  add_to_list(builder->lists+sibling_list_id, name_ptr, type);
}

#define DEFINE_VISIT_SIMPLE_TYPE(typename) \
  void visit_##typename(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name) { \
    visit_simple_type(data, sibling_list_id, name, #typename);		\
  }

DEFINE_VISIT_SIMPLE_TYPE(string);
DEFINE_VISIT_SIMPLE_TYPE(integer);
DEFINE_VISIT_SIMPLE_TYPE(short);
DEFINE_VISIT_SIMPLE_TYPE(byte);
DEFINE_VISIT_SIMPLE_TYPE(float);
DEFINE_VISIT_SIMPLE_TYPE(double);
DEFINE_VISIT_SIMPLE_TYPE(boolean);
DEFINE_VISIT_SIMPLE_TYPE(binary);
DEFINE_VISIT_SIMPLE_TYPE(date);
DEFINE_VISIT_SIMPLE_TYPE(timestamp);
DEFINE_VISIT_SIMPLE_TYPE(timestamp_ntz);

// free all the data in the builder (but not the builder itself, it's stack allocated)
void free_builder(SchemaBuilder builder) {
  for (int i = 0; i < builder.list_count; i++) {
    SchemaItemList *list = (builder.lists)+i;
    for (int j = 0; j < list->len; j++) {
      SchemaItem *item = list->list+j;
      free(item->name);
      // don't free item->type, those are static strings
      if (!strncmp(item->type, "decimal", 7)) {
        // except decimal types, we malloc'd those :)
        free(item->type);
      }
    }
    free(list->list); // free all the items in this list (we alloc'd them together)
  }
  free(builder.lists);
}

// Print the schema of the snapshot
void print_schema(const SnapshotHandle *snapshot) {
  SchemaBuilder builder = {
    .list_count = 0,
    .lists = calloc(0, sizeof(SchemaItem*)),
  };
  EngineSchemaVisitor visitor = {
    .data = &builder,
    .make_field_list = make_field_list,
    .visit_struct = visit_struct,
    .visit_array = visit_array,
    .visit_map = visit_map,
    .visit_decimal = visit_decimal,
    .visit_string  = visit_string,
    .visit_long = visit_long,
    .visit_integer = visit_integer,
    .visit_short = visit_short,
    .visit_byte = visit_byte,
    .visit_float = visit_float,
    .visit_double = visit_double,
    .visit_boolean = visit_boolean,
    .visit_binary = visit_binary,
    .visit_date = visit_date,
    .visit_timestamp = visit_timestamp,
    .visit_timestamp_ntz = visit_timestamp_ntz
  };
  uintptr_t schema_list_id = visit_schema(snapshot, &visitor);
#ifdef PRINT_VISITS
  printf("Schema returned in list %i\n", schema_list_id);
#endif
  printf("Schema:\n");
  print_list(&builder, schema_list_id, 0, false);
  printf("\n");
  free_builder(builder);
}
