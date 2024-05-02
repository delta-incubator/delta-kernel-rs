#include "delta_kernel_ffi.h"

typedef struct SchemaItemList SchemaItemList;

typedef struct {
  char* name;
  char* type;
  SchemaItemList* children;
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
  char* buf = malloc(sizeof(char) * (slice.len + 1)); // +1 for null
  snprintf(buf, slice.len + 1, "%s", slice.ptr);
  return buf;
}

SchemaItem* add_to_list(SchemaItemList *list, char* name, char* type) {
  int idx = list->len;
  list->list[idx].name = name;
  list->list[idx].type = type;
  list->len++;
  return list->list+idx;
}

void print_list(SchemaItemList *list, int indent, bool last) {
  for (int i = 0; i < list->len; i++) {
    for (int j = 0; j <= indent; j++) {
      if (j == indent) {
	if (i == list->len - 1) {
	  printf("└");
	} else {
	  printf("├");
	}
      } else {
	if (last && j == indent - 1) {
	  // don't print a dangling | for my last item
	  printf("   ");
	} else {
	  printf("│  ");
	}
      }
    }
    printf("─ %s: %s\n", list->list[i].name, list->list[i].type);
    if (list->list[i].children) {
      bool last = i == list->len - 1;
      print_list(list->list[i].children, indent+1, last);
    }
  }
}

// declare all our visitor methods
uintptr_t make_field_list(void *data, uintptr_t reserve) {
  SchemaBuilder *builder = (SchemaBuilder*)data;
  int id = builder->list_count;
  //printf("Asked to make list of len %i, id %i\n", reserve, id);
  builder->list_count++;
  builder->lists = realloc(builder->lists, sizeof(SchemaItemList) * builder->list_count);
  SchemaItem* list = calloc(reserve, sizeof(SchemaItem));
  //list->children = NULL;
  builder->lists[id].len = 0;
  builder->lists[id].list = list;
  return id;
}

void visit_struct(void *data,
		  uintptr_t sibling_list_id,
		  struct KernelStringSlice name,
		  uintptr_t child_list_id) {
  //printf("Asked to visit struct children are in %i, i am in %i\n", child_list_id, sibling_list_id);
  SchemaBuilder *builder = (SchemaBuilder*)data;
  char* name_ptr = allocate_name(name);
  SchemaItem* struct_item = add_to_list(builder->lists+sibling_list_id, name_ptr, "struct");
  struct_item->children = builder->lists+child_list_id;
}
void visit_array(void *data,
		 uintptr_t sibling_list_id,
		 struct KernelStringSlice name,
		 bool contains_null,
		 uintptr_t child_list_id) {
  //printf("Asked to visit array type is in %i, i am in %i\n", child_list_id, sibling_list_id);
  SchemaBuilder *builder = (SchemaBuilder*)data;
  char* name_ptr = allocate_name(name);
  SchemaItem* array_item = add_to_list(builder->lists+sibling_list_id, name_ptr, "array");
  array_item->children = builder->lists+child_list_id;
}
void visit_map(void *data,
	       uintptr_t sibling_list_id,
	       struct KernelStringSlice name,
	       uintptr_t child_list_id) {
  //printf("Asked to visit map, types are in %i, i am in %i\n", child_list_id, sibling_list_id);
  SchemaBuilder *builder = (SchemaBuilder*)data;
  char* name_ptr = allocate_name(name);
  SchemaItem* map_item = add_to_list(builder->lists+sibling_list_id, name_ptr, "map");
  map_item->children = builder->lists+child_list_id;
}

void visit_decimal(void *data,
		   uintptr_t sibling_list_id,
		   struct KernelStringSlice name,
		   uint8_t precision,
		   int8_t scale) {
  SchemaBuilder *builder = (SchemaBuilder*)data;
  char* name_ptr = allocate_name(name);
  char* type = malloc(16 * sizeof(char));
  sprintf(type, "decimal(%i)(%i)", precision, scale);
  add_to_list(builder->lists+sibling_list_id, name_ptr, type);
}

void visit_simple_type(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name, char* type) {
  SchemaBuilder *builder = (SchemaBuilder*)data;
  char* name_ptr = allocate_name(name);
  add_to_list(builder->lists+sibling_list_id, name_ptr, type);
}

void visit_string(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name) {
  visit_simple_type(data, sibling_list_id, name, "string");
}

void visit_long(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name) {
  visit_simple_type(data, sibling_list_id, name, "long");
}

void visit_integer(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name) {
  visit_simple_type(data, sibling_list_id, name, "integer");
}

void visit_short(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name) {
  visit_simple_type(data, sibling_list_id, name, "short");
}

void visit_byte(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name) {
  visit_simple_type(data, sibling_list_id, name, "byte");
}
void visit_float(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name) {
  visit_simple_type(data, sibling_list_id, name, "float");
}

void visit_double(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name) {
  visit_simple_type(data, sibling_list_id, name, "double");
}

void visit_boolean(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name) {
  visit_simple_type(data, sibling_list_id, name, "boolean");
}

void visit_binary(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name) {
  visit_simple_type(data, sibling_list_id, name, "binary");
}

void visit_date(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name) {
  visit_simple_type(data, sibling_list_id, name, "date");
}

void visit_timestamp(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name) {
  visit_simple_type(data, sibling_list_id, name, "timestamp");
}

void visit_timestamp_ntz(void *data, uintptr_t sibling_list_id, struct KernelStringSlice name) {
  visit_simple_type(data, sibling_list_id, name, "timestamp_ntz");
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
  visit_schema(snapshot, &visitor);
  printf("Schema:\n");
  print_list(builder.lists, 0, false);
  printf("\n");
}
